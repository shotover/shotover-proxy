use crate::hot_reload::protocol::{HotReloadListenerRequest, Request, Response, SocketInfo};
use crate::sources::Source;
use anyhow::{Context, Result};
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uds::tokio::{UnixSeqpacketConn, UnixSeqpacketListener};

pub struct SourceHandle {
    pub name: String,
    pub sender: mpsc::UnboundedSender<HotReloadListenerRequest>,
}

pub struct UnixSocketServer {
    socket_path: String,
    listener: UnixSeqpacketListener,
    sources: Vec<SourceHandle>,
}

impl UnixSocketServer {
    pub fn new(socket_path: String, sources: Vec<SourceHandle>) -> Result<Self> {
        if Path::new(&socket_path).exists() {
            std::fs::remove_file(&socket_path).with_context(|| {
                format!("Failed to remove existing socket file: {}", socket_path)
            })?;
        }
        let listener = UnixSeqpacketListener::bind(&socket_path)
            .with_context(|| format!("Failed to bind Unix seqpacket socket to: {}", socket_path))?;
        info!("Unix seqpacket socket server listening on: {}", socket_path);
        Ok(Self {
            socket_path,
            listener,
            sources,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            match self.listener.accept().await {
                Ok((conn, _)) => {
                    debug!("New connection received on Unix seqpacket socket");
                    if let Err(e) = self.handle_connection(conn).await {
                        error!("Error handling connection: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error accepting connection: {:?}", e);
                }
            }
        }
    }

    async fn handle_connection(&self, mut conn: UnixSeqpacketConn) -> Result<()> {
        // Receive request as a single packet
        let mut request_buf = vec![0u8; 8192]; // Buffer for request
        let len = conn
            .recv(&mut request_buf)
            .await
            .context("Failed to receive request")?;

        request_buf.truncate(len);
        let request: Request =
            serde_json::from_slice(&request_buf).context("Failed to deserialize request")?;
        debug!("Received request: {:?}", request);

        let response = self.process_request(request).await;

        let response_json =
            serde_json::to_string(&response).context("Failed to serialize response")?;

        // Send response as a single packet
        conn.send(response_json.as_bytes())
            .await
            .context("Failed to send response")?;
        debug!("Sent response: {}", response_json);
        Ok(())
    }

    async fn process_request(&self, request: Request) -> Response {
        match request {
            Request::SendListeningSockets => {
                info!("Processing SendListeningSockets request");

                // Send requests to all TcpCodecListener instances and collect responses
                let mut port_to_socket_info: HashMap<u32, SocketInfo> = HashMap::new();
                let current_pid = std::process::id();

                let mut response_futures = Vec::new();

                for source in &self.sources {
                    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                    let hot_reload_request = HotReloadListenerRequest {
                        return_chan: response_tx,
                    };

                    if let Err(e) = source.sender.send(hot_reload_request) {
                        warn!(
                            "Failed to send hot reload request to source {}: {:?}",
                            source.name, e
                        );
                        continue;
                    }

                    response_futures.push((source.name.clone(), response_rx));
                }

                // Collect all responses with timeout
                for (source_name, response_rx) in response_futures {
                    match tokio::time::timeout(std::time::Duration::from_secs(5), response_rx).await
                    {
                        Ok(Ok(response)) => {
                            match response {
                                crate::hot_reload::protocol::HotReloadListenerResponse::HotReloadResponse { port, listener_socket_fd } => {
                                    info!(
                                        "Received FD {} for port {} from source {} (PID: {})",
                                        listener_socket_fd.0, port, source_name, current_pid
                                    );
                                    port_to_socket_info.insert(port as u32, SocketInfo {
                                        fd: listener_socket_fd,
                                        pid: current_pid,
                                    });
                                }
                                crate::hot_reload::protocol::HotReloadListenerResponse::NoListenerAvailable => {
                                    info!("Source {} reported no listener available", source_name);
                                }
                            }
                        }
                        Ok(Err(_)) => {
                            warn!("Source {} dropped response channel", source_name);
                        }
                        Err(_) => {
                            warn!("Timeout waiting for response from source {}", source_name);
                        }
                    }
                }

                info!(
                    "Sending response with {} socket info entries",
                    port_to_socket_info.len()
                );

                Response::SendListeningSockets {
                    port_to_socket_info,
                }
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
pub fn start_hot_reload_server(socket_path: String, sources: &[Source]) {
    let source_handles: Vec<SourceHandle> = sources
        .iter()
        .map(|x| SourceHandle {
            name: x.name().to_string(),
            sender: x.get_hot_reload_tx(),
        })
        .collect();

    tokio::spawn(async move {
        match UnixSocketServer::new(socket_path, source_handles) {
            Ok(mut server) => {
                info!("Unix socket server started for hot reload communication");
                if let Err(e) = server.run().await {
                    error!("Unix socket server error: {:?}", e);
                }
            }
            Err(e) => {
                error!("Failed to start Unix socket server: {:?}", e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_reload::tests::wait_for_unix_socket_connection;
    use uds::tokio::UnixSeqpacketConn;

    #[tokio::test]
    async fn test_unix_socket_server_basic() {
        let socket_path = "/tmp/test-shotover-hotreload.sock";

        let source_handles: Vec<SourceHandle> = vec![];
        let server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        // Test that socket file was created
        assert!(Path::new(socket_path).exists());

        drop(server);
        assert!(!Path::new(socket_path).exists());
    }

    #[tokio::test]
    async fn test_request_response() {
        let socket_path = "/tmp/test-shotover-request-response.sock";
        let source_handles: Vec<SourceHandle> = vec![]; // Empty for test 
        let mut server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        // Start server in background
        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });
        wait_for_unix_socket_connection(socket_path, 2000).await;
        let mut conn = UnixSeqpacketConn::connect(socket_path).unwrap();

        // Send request
        let request = Request::SendListeningSockets;
        let request_json = serde_json::to_string(&request).unwrap();
        conn.send(request_json.as_bytes()).await.unwrap();

        // Read response
        let mut response_buf = vec![0u8; 8192];
        let len = conn.recv(&mut response_buf).await.unwrap();
        response_buf.truncate(len);
        let response: Response = serde_json::from_slice(&response_buf).unwrap();

        // Verify response
        match response {
            Response::SendListeningSockets {
                port_to_socket_info,
            } => {
                assert_eq!(port_to_socket_info.len(), 0);
            }
            _ => panic!("Wrong response type"),
        }

        // Clean up
        server_handle.abort();
    }
}
