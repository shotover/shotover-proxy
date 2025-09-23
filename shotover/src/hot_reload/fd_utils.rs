//! Safe utilities for working with file descriptors in hot reload context
//!
//! This module provides a sound safe API around unsafe file descriptor operations
//! needed for the hot reload functionality.

// Allow unsafe code in this module as we implement sound safe API around raw FDs
#![allow(unsafe_code)]

use anyhow::{Context, Result};
use std::os::unix::io::{FromRawFd, OwnedFd, RawFd};
use tokio::net::TcpListener;

pub fn take_ownership_of_fd(fd: RawFd) -> OwnedFd {
    unsafe { OwnedFd::from_raw_fd(fd) }
}

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
    use std::os::unix::io::IntoRawFd;

    #[tokio::test]
    async fn test_create_tcp_listener_from_fd() {
        // Create a test listener
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let expected_port = listener.local_addr().unwrap().port();
        let fd = listener.into_raw_fd();

        // Convert RawFd to OwnedFd
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };

        // Test our function
        let (tokio_listener, extracted_port) = create_tcp_listener_from_fd(owned_fd).unwrap();
        assert_eq!(expected_port, extracted_port);

        // Verify the tokio listener works
        assert_eq!(tokio_listener.local_addr().unwrap().port(), expected_port);
    }
}
