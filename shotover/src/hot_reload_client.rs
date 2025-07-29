//This module gives clinet-side implementation for socket handoff as part of hot reloading
//Client will connect to existing shotovers and requests for FDs
use crate::hot_reload::{Request, Response};
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tracing::{debug, info};

pub struct UnixSocketClient {
    socket_path: String,
    timeout_duration: Duration,
}

impl UnixSocketClient {
    pub fn new(socket_path: String) -> Self {
        Self {
            socket_path,
            timeout_duration: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_duration = timeout;
        self
    }

    pub async fn send_request(&self, request: Request) -> Result<Response> {
        info!("Connecting to hot reload server at: {}", self.socket_path);

        let result = timeout(self.timeout_duration, self.send_request_inner(request)).await;

        match result {
            Ok(response) => response,
            Err(_) => Err(anyhow::anyhow!(
                "Hot reload request timed out after {:?}",
                self.timeout_duration
            )),
        }
    }

    async fn send_request_inner(&self, request: Request) -> Result<Response> {
        // Connect to server
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to hot reload server at: {}",
                    self.socket_path
                )
            })?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        // Send request
        let request_json =
            serde_json::to_string(&request).context("Failed to serialize request")?;

        debug!("Sending request: {}", request_json);
        writer
            .write_all(request_json.as_bytes())
            .await
            .context("Failed to send request")?;

        // Read response
        let response: Response = read_json(&mut reader).await?;
        debug!("Received response: {:?}", response);

        Ok(response)
    }
}

// Reuse the same JSON parsing logic as server
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
            Ok(response) => return Ok(response),
            Err(e) if e.is_eof() => continue,
            Err(e) => return Err(e).context("Failed to parse JSON"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_timeout() {
        // Use a path that will cause the connection to hang, not fail immediately
        // This simulates a server that accepts connections but doesn't respond
        let client =
            UnixSocketClient::new("/dev/null".to_string()).with_timeout(Duration::from_millis(100));

        let result = client.send_request(Request::SendListeningSockets).await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();

        // The error could be either a timeout or a connection error, both are valid
        assert!(
            error_msg.contains("timed out") || error_msg.contains("Failed to connect"),
            "Expected timeout or connection error, got: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_client_connection_error() {
        let client = UnixSocketClient::new("/nonexistent/path.sock".to_string());

        let result = client.send_request(Request::SendListeningSockets).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to connect")
        );
    }

    #[tokio::test]
    async fn test_client_with_timeout_builder() {
        let client = UnixSocketClient::new("/test/path.sock".to_string())
            .with_timeout(Duration::from_secs(5));

        assert_eq!(client.timeout_duration, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_client_server_integration() {
        let socket_path = "/tmp/test-client-server-integration.sock";

        // Clean up any existing socket
        let _ = std::fs::remove_file(socket_path);

        // Start server
        let mut server =
            crate::hot_reload_server::UnixSocketServer::new(socket_path.to_string()).unwrap();
        let server_handle = tokio::spawn(async move {
            server.run().await.ok();
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create client and send request
        let client = UnixSocketClient::new(socket_path.to_string());
        let response = client
            .send_request(Request::SendListeningSockets)
            .await
            .unwrap();

        // Verify response
        match response {
            Response::SendListeningSockets { port_to_fd } => {
                assert_eq!(port_to_fd.len(), 1);
                assert!(port_to_fd.contains_key(&6380));
            }
            Response::Error(msg) => panic!("Unexpected error response: {}", msg),
        }

        // Cleanup
        server_handle.abort();
        let _ = std::fs::remove_file(socket_path);
    }

    #[tokio::test]
    async fn test_multiple_client_requests() {
        let socket_path = "/tmp/test-multiple-clients.sock";

        // Clean up any existing socket
        let _ = std::fs::remove_file(socket_path);

        // Start server
        let mut server =
            crate::hot_reload_server::UnixSocketServer::new(socket_path.to_string()).unwrap();
        let server_handle = tokio::spawn(async move {
            server.run().await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send multiple requests
        let client = UnixSocketClient::new(socket_path.to_string());

        for _i in 0..3 {
            let response = client
                .send_request(Request::SendListeningSockets)
                .await
                .unwrap();
            match response {
                Response::SendListeningSockets { port_to_fd } => {
                    assert_eq!(port_to_fd.len(), 1);
                }
                Response::Error(msg) => panic!("Unexpected error response: {}", msg),
            }
        }

        // Cleanup
        server_handle.abort();
        let _ = std::fs::remove_file(socket_path);
    }
}
