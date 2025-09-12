use anyhow::{Context, Result, anyhow};
#[cfg(target_os = "linux")]
use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use tokio::net::TcpListener;
use tracing::debug;
#[cfg(not(target_os = "linux"))]
use tracing::warn;

#[cfg(target_os = "linux")]
use rustix::process::{pidfd_open, pidfd_getfd, PidfdFlags, PidfdGetfdFlags};
#[cfg(target_os = "linux")]
use rustix::fd::OwnedFd;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Convert a raw file descriptor from another process to a TcpListener in the current process
/// Uses pidfd_getfd() system call on Linux for secure cross-process FD transfer
pub async fn create_listener_from_remote_fd(
    _source_pid: u32,
    _remote_fd: RawFd,
) -> Result<TcpListener> {
    #[cfg(target_os = "linux")]
    {
        debug!(
            "Attempting to duplicate FD {} from process {} using pidfd_getfd",
            _remote_fd, _source_pid
        );

        // Open pidfd for the source process using rustix
        let pidfd = pidfd_open(_source_pid, PidfdFlags::empty())
            .map_err(|e| anyhow!(
                "Failed to open pidfd for process {}: {}",
                _source_pid, e
            ))?;

        // Use pidfd_getfd to duplicate the file descriptor using rustix
        let local_fd = pidfd_getfd(&pidfd, _remote_fd, PidfdGetfdFlags::empty())
            .map_err(|e| anyhow!(
                "Failed to duplicate FD {} from process {}: {}",
                _remote_fd, _source_pid, e
            ))?;

        debug!(
            "Successfully duplicated FD {} from process {} as local FD {}",
            _remote_fd, _source_pid, local_fd.as_raw_fd()
        );

        // Convert the owned FD to a TcpListener
        let std_listener = std::net::TcpListener::from(local_fd);

        // Convert to tokio TcpListener
        let tokio_listener = TcpListener::from_std(std_listener)
            .context("Failed to convert std::net::TcpListener to tokio::net::TcpListener")?;

        Ok(tokio_listener)
    }

    #[cfg(not(target_os = "linux"))]
    {
        warn!(
            "pidfd_getfd() not available on this platform. Hot reload socket transfer not supported."
        );
        Err(anyhow!(
            "Socket file descriptor transfer not supported on this platform"
        ))
    }
}

/// Validate that a file descriptor refers to a TCP socket bound to the expected address
pub async fn validate_socket_fd(listener: &TcpListener, expected_port: u16) -> Result<()> {
    let local_addr = listener
        .local_addr()
        .context("Failed to get local address from listener")?;

    if local_addr.port() != expected_port {
        return Err(anyhow!(
            "Socket port mismatch: expected {}, got {}",
            expected_port,
            local_addr.port()
        ));
    }

    debug!(
        "Validated socket FD: bound to {} (expected port {})",
        local_addr, expected_port
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_socket_fd() {
        // Create a test listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();

        // Should succeed with correct port
        assert!(validate_socket_fd(&listener, port).await.is_ok());

        // Should fail with wrong port
        assert!(validate_socket_fd(&listener, port + 1).await.is_err());
    }
}
