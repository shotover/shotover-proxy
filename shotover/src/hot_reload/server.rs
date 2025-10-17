use crate::hot_reload::json_parsing::{read_json, write_json, write_json_with_fds};
use crate::hot_reload::protocol::{HotReloadListenerRequest, Request, Response};
use crate::sources::Source;
use anyhow::{Context, Result};
use std::os::unix::io::{AsRawFd, RawFd};
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
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl UnixSocketServer {
    pub fn new(
        socket_path: String,
        sources: Vec<SourceHandle>,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
    ) -> Result<Self> {
        if Path::new(&socket_path).exists() {
            std::fs::remove_file(&socket_path).with_context(|| {
                format!("Failed to remove existing socket file: {}", socket_path)
            })?;
        }
        let listener = UnixSeqpacketListener::bind(&socket_path)
            .with_context(|| format!("Failed to bind Unix socket to: {}", socket_path))?;
        info!("Unix socket server listening on: {}", socket_path);
        Ok(Self {
            socket_path,
            listener,
            sources,
            shutdown_tx,
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

    async fn handle_connection(&self, mut stream: UnixSeqpacketConn) -> Result<()> {
        let request: Request = read_json(&mut stream).await?;
        debug!("Received request: {:?}", request);

        let (response, fds) = self.process_request(request).await;

        // Send response with file descriptors
        if fds.is_empty() {
            write_json(&mut stream, &response).await?;
            debug!("Sent response without FDs: {:?}", response);
        } else {
            write_json_with_fds(&mut stream, &response, &fds).await?;
            info!(
                "Sent response with {} file descriptors via ancillary data: {:?}",
                fds.len(),
                response
            );
        }

        Ok(())
    }

    async fn process_request(&self, request: Request) -> (Response, Vec<RawFd>) {
        match request {
            Request::SendListeningSockets => {
                info!("Processing SendListeningSockets request");

                // Send requests to all TcpCodecListener instances and collect responses
                let mut collected_owned_fds = Vec::new();

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
                                        "Received OwnedFd for port {} from source {}",
                                        port, source_name
                                    );
                                    collected_owned_fds.push(listener_socket_fd);
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

                // Extract RawFd only for transfer
                let raw_fds: Vec<RawFd> = collected_owned_fds
                    .iter()
                    .map(|owned_fd| owned_fd.as_raw_fd())
                    .collect();

                info!("Sending response with {} file descriptors", raw_fds.len());

                // Transfer ownership to receiver
                // SAFETY: We're transferring these FDs via Unix socket to the new process.
                // The FDs will be duplicated by the kernel during transfer.
                // The new process will take ownership of the duplicated FDs.
                std::mem::forget(collected_owned_fds);

                (Response::SendListeningSockets, raw_fds)
            }
            Request::ShutdownOriginalNode => {
                info!("Processing ShutdownOriginalNode request - initiating immediate shutdown");

                // Trigger shutdown by sending true through the watch channel
                if let Err(e) = self.shutdown_tx.send(true) {
                    error!("Failed to send shutdown signal: {:?}", e);
                    return (
                        Response::Error("Failed to trigger shutdown".to_string()),
                        vec![],
                    );
                }

                info!("Shutdown signal sent successfully");
                (Response::ShutdownOriginalNode, vec![])
            }
            Request::ShutdownOriginalNode => {
                info!("Processing ShutdownOriginalNode request - initiating immediate shutdown");

                // Trigger shutdown by sending true through the watch channel
                if let Err(e) = self.shutdown_tx.send(true) {
                    error!("Failed to send shutdown signal: {:?}", e);
                    return (
                        Response::Error("Failed to trigger shutdown".to_string()),
                        vec![],
                    );
                }

                info!("Shutdown signal sent successfully");
                (Response::ShutdownOriginalNode, vec![])
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
pub fn start_hot_reload_server(
    socket_path: String,
    sources: &[Source],
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) {
    let source_handles: Vec<SourceHandle> = sources
        .iter()
        .map(|x| SourceHandle {
            name: x.name().to_string(),
            sender: x.get_hot_reload_tx(),
        })
        .collect();

    tokio::spawn(async move {
        match UnixSocketServer::new(socket_path, source_handles, shutdown_tx) {
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::hot_reload::tests::wait_for_unix_socket_connection;

    #[tokio::test]
    async fn test_unix_socket_server_basic() {
        let socket_path = "/tmp/test-shotover-hotreload.sock";

        let source_handles: Vec<SourceHandle> = vec![];
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        let server =
            UnixSocketServer::new(socket_path.to_string(), source_handles, shutdown_tx).unwrap();

        // Test that socket file was created
        assert!(Path::new(socket_path).exists());

        drop(server);
        assert!(!Path::new(socket_path).exists());
    }

    #[tokio::test]
    async fn test_request_response() {
        use crate::hot_reload::json_parsing::{read_json, write_json};
        use uds::tokio::UnixSeqpacketConn;

        let socket_path = "/tmp/test-shotover-request-response.sock";
        let source_handles: Vec<SourceHandle> = vec![]; // Empty for test
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        let mut server =
            UnixSocketServer::new(socket_path.to_string(), source_handles, shutdown_tx).unwrap();

        // Start server in background
        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });

        wait_for_unix_socket_connection(socket_path, 2000).await;

        let mut stream = UnixSeqpacketConn::connect(socket_path).unwrap();
        let request = Request::SendListeningSockets;
        write_json(&mut stream, &request).await.unwrap();
        let response: Response = read_json(&mut stream).await.unwrap();

        // Verify response
        match response {
            Response::SendListeningSockets => {
                // Test passes if we get the correct response variant
                // File descriptors are now sent via ancillary data
            }
            Response::ShutdownOriginalNode => {
                panic!("Unexpected ShutdownOriginalNode response");
            }
            Response::Error(err) => panic!("Unexpected error response: {}", err),
        }

        // Clean up
        server_handle.abort();
    }
}
