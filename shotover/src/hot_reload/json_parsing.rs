use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
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
