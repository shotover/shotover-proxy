use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::os::unix::io::RawFd;
use uds::tokio::UnixSeqpacketConn;

/// Read a complete JSON message from a SEQPACKET socket
pub async fn read_json<T: DeserializeOwned>(conn: &mut UnixSeqpacketConn) -> Result<T> {
    let mut buf = vec![0u8; 65536]; // Large buffer for JSON messages
    let bytes_read = conn
        .recv(&mut buf)
        .await
        .context("Failed to read packet from socket")?;

    // Resize buffer to actual data received
    buf.truncate(bytes_read);

    // Parse the complete JSON message
    serde_json::from_slice(&buf).context("Failed to parse JSON from packet")
}

/// Write a complete JSON message to a SEQPACKET socket
pub async fn write_json<T: Serialize>(conn: &mut UnixSeqpacketConn, data: &T) -> Result<()> {
    let json_bytes = serde_json::to_vec(data).context("Failed to serialize JSON")?;

    conn.send(&json_bytes)
        .await
        .context("Failed to send JSON packet")?;

    Ok(())
}

/// Read a complete JSON message from a SEQPACKET socket with file descriptors
pub async fn read_json_with_fds<T: DeserializeOwned>(
    conn: &mut UnixSeqpacketConn,
) -> Result<(T, Vec<RawFd>)> {
    let mut buf = vec![0u8; 65536]; // Large buffer for JSON messages
    let mut fd_buf = vec![0i32; 16]; // Buffer for file descriptors
    let (bytes_read, _truncated, fds_read) = conn
        .recv_fds(&mut buf, &mut fd_buf)
        .await
        .context("Failed to read packet with FDs from socket")?;

    // Resize buffer to actual data received
    buf.truncate(bytes_read);
    fd_buf.truncate(fds_read);

    // Parse the complete JSON message
    let data = serde_json::from_slice(&buf).context("Failed to parse JSON from packet")?;

    Ok((data, fd_buf))
}

/// Write a complete JSON message to a SEQPACKET socket with file descriptors
pub async fn write_json_with_fds<T: Serialize>(
    conn: &mut UnixSeqpacketConn,
    data: &T,
    fds: &[RawFd],
) -> Result<()> {
    let json_bytes = serde_json::to_vec(data).context("Failed to serialize JSON")?;

    conn.send_fds(&json_bytes, fds)
        .await
        .context("Failed to send JSON packet with FDs")?;

    Ok(())
}
