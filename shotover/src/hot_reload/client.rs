//This module gives client-side implementation for socket handoff as part of hot reloading
//Client will connect to existing shotovers and requests for FDs
use crate::hot_reload::fd_utils::create_tcp_listener_from_fd;
use crate::hot_reload::json_parsing::{read_json_with_fds, write_json};
use crate::hot_reload::protocol::{Request, Response};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::os::unix::io::OwnedFd;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tracing::{info, warn};
use uds::tokio::UnixSeqpacketConn;

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

    pub async fn send_request(&self, request: Request) -> Result<HashMap<u16, TcpListener>> {
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

    async fn send_request_inner(&self, request: Request) -> Result<HashMap<u16, TcpListener>> {
        // Connect to server
        let mut stream = UnixSeqpacketConn::connect(&self.socket_path).with_context(|| {
            format!(
                "Failed to connect to hot reload server at: {}",
                self.socket_path
            )
        })?;

        // Send request
        info!("Sending request: {:?}", request);
        write_json(&mut stream, &request).await?;

        // Read response
        let (response, received_fds): (Response, Vec<OwnedFd>) =
            read_json_with_fds(&mut stream).await?;

        let mut listeners_by_port = HashMap::new();

        if !received_fds.is_empty() {
            info!(
                "Received {} file descriptors via ancillary data",
                received_fds.len()
            );

            // Process each owned file descriptor received from the OS
            for owned_fd in received_fds {
                match create_tcp_listener_from_fd(owned_fd) {
                    Ok((listener, port)) => {
                        info!("Created TcpListener from file descriptor for port {}", port);
                        listeners_by_port.insert(port, listener);
                    }
                    Err(e) => {
                        warn!("Failed to create listener from FD: {}", e);
                    }
                }
            }
        }

        info!("Received response: {:?}", response);

        // Handle error responses
        if let Response::Error(err) = response {
            return Err(anyhow::anyhow!("Hot reload server returned error: {}", err));
        }

        Ok(listeners_by_port)
    }
}
pub struct HotReloadClient {
    socket_path: String,
}

impl HotReloadClient {
    pub fn new(socket_path: String) -> Self {
        Self { socket_path }
    }

    /// Request listening sockets from an existing Shotover instance during hot reload
    /// Returns a HashMap mapping port numbers to TcpListener instances
    pub async fn perform_hot_reloading(&self) -> Result<HashMap<u16, TcpListener>> {
        info!(
            "Hot reload CLIENT will request sockets from existing shotover at: {}",
            self.socket_path
        );

        let client = UnixSocketClient::new(self.socket_path.clone());

        let listeners = client
            .send_request(crate::hot_reload::protocol::Request::SendListeningSockets)
            .await?;

        if listeners.is_empty() {
            warn!("No listeners received during hot reload");
        } else {
            info!(
                "Successfully received {} listeners during hot reload",
                listeners.len()
            );
        }

        Ok(listeners)
    }

    /// Request the old Shotover instance to shutdown after hot reload handoff
    /// This should be called after the new instance is fully started and ready to accept connections
    /// The old instance will drain connections gradually 10% every 10 seconds
    pub async fn request_shutdown_old_instance(&self) -> Result<()> {
        info!(
            "Hot reload CLIENT requesting gradual shutdown of old shotover at: {}",
            self.socket_path
        );

        let client = UnixSocketClient::new(self.socket_path.clone());

        // Send the shutdown request - we don't expect any listeners back
        let _listeners = client
            .send_request(crate::hot_reload::protocol::Request::GradualShutdown)
            .await?;

        info!("Successfully sent gradual shutdown request to old shotover instance");

        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::hot_reload::protocol::HotReloadListenerResponse;
    use crate::hot_reload::server::SourceHandle;
    use crate::hot_reload::tests::wait_for_unix_socket_connection;
    use tokio::sync::mpsc::unbounded_channel;
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
        let (tx, mut rx) = unbounded_channel();
        let (gradual_shutdown_tx, _gradual_shutdown_rx) = unbounded_channel();
        let source_handles: Vec<SourceHandle> = vec![SourceHandle {
            name: "foo".to_string(),
            sender: tx,
            gradual_shutdown_tx,
        }];
        let mut server = crate::hot_reload::server::UnixSocketServer::new(
            socket_path.to_string(),
            source_handles,
        )
        .unwrap();
        tokio::spawn(async move {
            rx.recv()
                .await
                .unwrap()
                .return_chan
                .send(HotReloadListenerResponse::HotReloadResponse {
                    port: 6000,
                    // Create a dummy OwnedFd for testing using a Unix socket pair
                    listener_socket_fd: {
                        let (sock, _) = std::os::unix::net::UnixStream::pair().unwrap();
                        sock.into()
                    },
                })
                .unwrap();
        });

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        // Wait for server to start
        wait_for_unix_socket_connection(socket_path, 2000).await;

        // Create client and send request
        let client = UnixSocketClient::new(socket_path.to_string());
        let listeners = client
            .send_request(Request::SendListeningSockets)
            .await
            .unwrap();

        // Verify response
        assert_eq!(listeners.len(), 0); // No actual FDs in test

        // Cleanup
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_multiple_client_requests() {
        let socket_path = "/tmp/test-multiple-clients.sock";

        let (tx, mut rx) = unbounded_channel();
        let (gradual_shutdown_tx, _gradual_shutdown_rx) = unbounded_channel();
        let source_handles: Vec<SourceHandle> = vec![SourceHandle {
            name: "foo".to_string(),
            sender: tx,
            gradual_shutdown_tx,
        }];
        let mut server = crate::hot_reload::server::UnixSocketServer::new(
            socket_path.to_string(),
            source_handles,
        )
        .unwrap();

        tokio::spawn(async move {
            for _i in 0..3 {
                rx.recv()
                    .await
                    .unwrap()
                    .return_chan
                    .send(HotReloadListenerResponse::HotReloadResponse {
                        port: 6000,
                        // Create a dummy OwnedFd for testing using a Unix socket pair
                        listener_socket_fd: {
                            let (sock, _) = std::os::unix::net::UnixStream::pair().unwrap();
                            sock.into()
                        },
                    })
                    .unwrap();
            }
        });

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        wait_for_unix_socket_connection(socket_path, 2000).await;

        // Send multiple requests
        let client = UnixSocketClient::new(socket_path.to_string());

        for _i in 0..3 {
            let listeners = client
                .send_request(Request::SendListeningSockets)
                .await
                .unwrap();
            // Verify we get a valid response
            assert_eq!(listeners.len(), 0);
        }

        // Cleanup
        server_handle.abort();
    }
}
