//This module gives client-side implementation for socket handoff as part of hot reloading
//Client will connect to existing shotovers and requests for FDs
use crate::hot_reload::{Request, Response};
use crate::json_parsing::read_json;
use anyhow::{Context, Result};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufReader};
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

    #[cfg(test)]
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

/// Request listening sockets from an existing Shotover instance during hot reload
pub async fn request_listening_sockets(socket_path: String) -> Result<()> {
    info!(
        "Hot reload CLIENT will request sockets from existing shotover at: {}",
        socket_path
    );

    let client = UnixSocketClient::new(socket_path.clone());

    match client
        .send_request(crate::hot_reload::Request::SendListeningSockets)
        .await
    {
        Ok(crate::hot_reload::Response::SendListeningSockets { port_to_fd }) => {
            info!(
                "Successfully received {} file descriptors from hot reload server",
                port_to_fd.len()
            );
            for (port, fd) in &port_to_fd {
                info!("Received file descriptor {} for port {}", fd.0, port);
            }
            Ok(())
        }
        Ok(crate::hot_reload::Response::Error(msg)) => {
            Err(anyhow::anyhow!("Hot reload request failed: {}", msg))
        }
        Err(e) => Err(e).context("Failed to communicate with hot reload server"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[cfg(test)]
    async fn wait_for_unix_socket_connection(socket_path: &str, timeout_ms: u64) {
        use tokio::net::UnixStream;
        for _ in 0..timeout_ms / 5 {
            if UnixStream::connect(socket_path).await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!(
            "Failed to connect to Unix socket at {} after waiting",
            socket_path
        );
    }

    #[tokio::test]
    async fn test_client_connection_error() {
        let client = UnixSocketClient::new("/nonexistent/path.sock".to_string());

        let result = client.send_request(Request::SendListeningSockets).await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to connect")
        );
    }

    #[tokio::test]
    async fn test_client_server_integration() {
        let socket_path = "/tmp/test-client-server-integration.sock";

        // Start server
        let mut server =
            crate::hot_reload_server::UnixSocketServer::new(socket_path.to_string()).unwrap();
        let server_handle = tokio::spawn(async move {
            server.run().await.ok();
        });

        // Wait for server to start
        wait_for_unix_socket_connection(socket_path, 2000).await;

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

        wait_for_unix_socket_connection(socket_path, 2000).await;

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
    }
}
