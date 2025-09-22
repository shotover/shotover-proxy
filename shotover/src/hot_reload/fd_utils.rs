//! Safe utilities for working with file descriptors in hot reload context
//!
//! This module provides a sound safe API around unsafe file descriptor operations
//! needed for the hot reload functionality.

// Allow unsafe code in this module as we implement sound safe API around raw FDs
#![allow(unsafe_code)]

use anyhow::{Context, Result};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};

pub fn extract_port_from_listener_fd(fd: RawFd) -> Result<u16> {
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };

    let port = listener
        .local_addr()
        .context("Failed to get local address from listener socket")?
        .port();

    // Convert back to raw FD to avoid dropping the socket
    let recovered_fd = listener.into_raw_fd();

    // Sanity check that we got the same FD back
    debug_assert_eq!(
        fd, recovered_fd,
        "File descriptor should be preserved during conversion"
    );

    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn test_extract_port_from_listener_fd() {
        // Create a test listener
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let expected_port = listener.local_addr().unwrap().port();
        let fd = listener.into_raw_fd();

        // Test our function
        let extracted_port = extract_port_from_listener_fd(fd).unwrap();
        assert_eq!(expected_port, extracted_port);

        // Clean up
        let _listener = unsafe { TcpListener::from_raw_fd(fd) };
    }
}
