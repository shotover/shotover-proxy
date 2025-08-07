use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;

pub async fn read_json<T: DeserializeOwned, R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<T> {
    let mut received_bytes = Vec::new();
    loop {
        let mut buf = [0u8; 1024];
        let n = reader
            .read(&mut buf)
            .await
            .context("Failed to read from stream")?;
        if n == 0 {
            return Err(anyhow::anyhow!(
                "Connection closed before full JSON message received"
            ));
        }
        received_bytes.extend_from_slice(&buf[..n]);
        match serde_json::from_slice(&received_bytes) {
            Ok(response) => return Ok(response),
            Err(e) if e.is_eof() => continue,
            Err(e) => return Err(e).context("Failed to parse JSON"),
        }
    }
}
