//! utilities for working with file descriptors in hot reload context

use anyhow::{Context, Result};
use std::os::unix::io::OwnedFd;
use tokio::net::TcpListener;

/// Create a TcpListener from an owned file descriptor and return both the listener and port
pub fn create_tcp_listener_from_fd(fd: OwnedFd) -> Result<(TcpListener, u16)> {
    let std_listener = std::net::TcpListener::from(fd);

    std_listener
        .set_nonblocking(true)
        .context("Failed to set listener to non-blocking mode")?;

    let port = std_listener
        .local_addr()
        .context("Failed to get local address from listener socket")?
        .port();

    // Convert to tokio listener
    let tokio_listener = TcpListener::from_std(std_listener)
        .context("Failed to convert std TcpListener to tokio TcpListener")?;

    Ok((tokio_listener, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::os::fd::AsFd;

    #[tokio::test]
    async fn test_create_tcp_listener_from_fd() {
        // Create a test listener
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let expected_port = listener.local_addr().unwrap().port();

        let owned_fd = listener.as_fd().try_clone_to_owned().unwrap();

        // Test our function
        let (tokio_listener, extracted_port) = create_tcp_listener_from_fd(owned_fd).unwrap();
        assert_eq!(expected_port, extracted_port);

        // Verify the tokio listener works
        assert_eq!(tokio_listener.local_addr().unwrap().port(), expected_port);
    }
}
