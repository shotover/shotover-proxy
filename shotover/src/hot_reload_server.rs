use crate::hot_reload::{Request, Response};
use anyhow::{Context, Result};
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

pub struct UnixSocketServer {
    socket_path: String,
    listener: UnixListener,
}

impl UnixSocketServer {
    pub fn new(socket_path: String) -> Result<Self> {
        if Path::new(&socket_path).exists() {
            std::fs::remove_file(&socket_path).with_context(|| {
                format!("Failed to remove existing socket file: {}", socket_path)
            })?;
        }

        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("Failed to bind Unix socket to: {}", socket_path))?;
        info!("Unix socket server listening on: {}", socket_path);

        Ok(Self {
            socket_path,
            listener,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    debug!("New connection received on Unix socket");
                    if let Err(e) = self.handle_connection(stream).await {
                        error!("Error handling connection: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error accepting connection: {:?}", e);
                }
            }
        }
    }

    async fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        let request: Request = read_json(&mut reader).await?;
        debug!("Received request: {:?}", request);

        let response = self.process_request(request).await;

        let response_json =
            serde_json::to_string(&response).context("Failed to serialize response")?;
        writer
            .write_all(response_json.as_bytes())
            .await
            .context("Failed to write response")?;
        debug!("Sent response: {}", response_json);
        Ok(())
    }

    async fn process_request(&self, request: Request) -> Response {
        match request {
            Request::SendListeningSockets => {
                info!("Processing SendListeningSockets request");
                // TODO: In next steps, we'll extract actual file descriptors

                let mut port_to_fd = HashMap::new();
                port_to_fd.insert(6380, crate::hot_reload::FileDescriptor(10));
                Response::SendListeningSockets { port_to_fd }
            }
        }
    }
}

impl Drop for UnixSocketServer {
    fn drop(&mut self) {
        if Path::new(&self.socket_path).exists() {
            if let Err(e) = std::fs::remove_file(&self.socket_path) {
                warn!("Failed to remove socket file {}: {:?}", self.socket_path, e);
            }
        }
    }
}

use serde::de::DeserializeOwned;
async fn read_json<T: DeserializeOwned, R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<T> {
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
            Ok(request) => return Ok(request),
            Err(e) if e.is_eof() => continue,
            Err(e) => return Err(e).context("Failed to parse JSON"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    #[tokio::test]
    async fn test_unix_socket_server_basic() {
        let socket_path = "/tmp/test-shotover-hotreload.sock";

        // Clean up any existing socket
        let _ = std::fs::remove_file(socket_path);
        let server = UnixSocketServer::new(socket_path.to_string()).unwrap();

        // Test that socket file was created
        assert!(Path::new(socket_path).exists());

        drop(server);
        assert!(!Path::new(socket_path).exists());
    }

    #[tokio::test]
    async fn test_request_response() {
        let socket_path = "/tmp/test-shotover-request-response.sock";
        let mut server = UnixSocketServer::new(socket_path.to_string()).unwrap();

        // Start server in background
        let server_handle = tokio::spawn(async move {
            server.run().await.ok();
        });

        let mut stream = None;
        for _ in 0..1000 {
            match UnixStream::connect(socket_path).await {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => {
                    use std::time::Duration;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
        let mut stream =
            stream.expect("Failed to Connect to Hot Reload Unix Socket Server after waiting");

        // Send request
        let request = Request::SendListeningSockets;
        let request_json = serde_json::to_string(&request).unwrap();
        stream.write_all(request_json.as_bytes()).await.unwrap();

        // Read response
        let mut response_data = Vec::new();
        stream.read_to_end(&mut response_data).await.unwrap();
        let response: Response = serde_json::from_slice(&response_data).unwrap();

        // Verify response
        match response {
            Response::SendListeningSockets { port_to_fd } => {
                assert_eq!(port_to_fd.len(), 1);
                assert!(port_to_fd.contains_key(&6380));
            }
            _ => panic!("Wrong response type"),
        }

        // Clean up
        server_handle.abort();
        let _ = std::fs::remove_file(socket_path);
    }
}
