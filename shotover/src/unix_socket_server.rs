use crate::hot_reload::{Request, Response};
use anyhow::{Context, Result};
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};
pub struct UnixSocketServer {
    socket_path: String,
    listener: Option<UnixListener>,
}
impl UnixSocketServer {
    pub fn new(socket_path: String) -> Self {
        Self {
            socket_path,
            listener: None,
        }
    }
    /// Start the Unix socket server
    pub async fn start(&mut self) -> Result<()> {
        // Remove existing socket file if it exists
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path).with_context(|| {
                format!(
                    "Failed to remove existing socket file: {}",
                    self.socket_path
                )
            })?;
        }
        // Create Unix socket listener
        let listener = UnixListener::bind(&self.socket_path)
            .with_context(|| format!("Failed to bind Unix socket to: {}", self.socket_path))?;
        info!("Unix socket server listening on: {}", self.socket_path);
        self.listener = Some(listener);
        Ok(())
    }
    /// Run the server loop (this should be spawned as a background task)
    pub async fn run(&mut self) -> Result<()> {
        let listener = self
            .listener
            .as_ref()
            .context("Server not started - call start() first")?;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    debug!("New connection received on Unix socket");
                    if let Err(e) = self.handle_connection(stream).await {
                        error!("Error handling connection: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error accepting connection: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }
    /// Handle a single connection
    async fn handle_connection(&self, stream: UnixStream) -> Result<()> {
        // Split the stream for reading and writing
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        // Read the request
        reader
            .read_line(&mut line)
            .await
            .context("Failed to read request from Unix socket")?;
        debug!("Received request: {}", line.trim());
        // Parse the request
        let request: Request =
            serde_json::from_str(line.trim()).context("Failed to parse request JSON")?;
        // Process the request and generate response
        let response = self.process_request(request).await;
        // Send the response
        let response_json =
            serde_json::to_string(&response).context("Failed to serialize response")?;
        writer
            .write_all(response_json.as_bytes())
            .await
            .context("Failed to write response")?;
        writer
            .write_all(b"\n")
            .await
            .context("Failed to write newline")?;
        debug!("Sent response: {}", response_json);
        Ok(())
    }
    /// Process a request and return appropriate response
    async fn process_request(&self, request: Request) -> Response {
        match request {
            Request::SendListeningSockets => {
                info!("Processing SendListeningSockets request");
                // TODO: In next steps, we'll extract actual file descriptors
                // For now, return a placeholder response
                let mut port_to_fd = HashMap::new();
                port_to_fd.insert(6380, crate::hot_reload::FileDescriptor(10));
                Response::SendListeningSockets { port_to_fd }
            }
        }
    }
}
impl Drop for UnixSocketServer {
    fn drop(&mut self) {
        // Clean up socket file when server is dropped
        if Path::new(&self.socket_path).exists() {
            if let Err(e) = std::fs::remove_file(&self.socket_path) {
                warn!("Failed to remove socket file {}: {:?}", self.socket_path, e);
            }
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
        let mut server = UnixSocketServer::new(socket_path.to_string());
        server.start().await.expect("Failed to start server");
        // Test that socket file was created
        assert!(Path::new(socket_path).exists());
        // Clean up
        drop(server);
        assert!(!Path::new(socket_path).exists());
    }
    #[tokio::test]
    async fn test_request_response() {
        let socket_path = "/tmp/test-shotover-request-response.sock";
        // Clean up any existing socket
        let _ = std::fs::remove_file(socket_path);
        let mut server = UnixSocketServer::new(socket_path.to_string());
        server.start().await.expect("Failed to start server");
        // Start server in background
        let server_handle = tokio::spawn(async move {
            // Run server for a short time
            tokio::select! {
            _
            = server.run() => {},
            _
            = tokio::time::sleep(tokio::time::Duration::from_secs(2))
            => {}
            }
        });
        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        // Connect as client
        let mut stream = UnixStream::connect(socket_path)
            .await
            .expect("Failed to connect to server");
        // Send request
        let request = Request::SendListeningSockets;
        let request_json = serde_json::to_string(&request).unwrap();
        stream.write_all(request_json.as_bytes()).await.unwrap();
        stream.write_all(b"\n").await.unwrap();
        // Read response
        let mut response_data = Vec::new();
        stream.read_to_end(&mut response_data).await.unwrap();
        let response_str = String::from_utf8(response_data).unwrap();
        // Parse response
        let response: Response = serde_json::from_str(response_str.trim()).unwrap();
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
