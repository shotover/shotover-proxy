//! Server-side implementation for hot reload socket communication.

use crate::hot_reload::json_parsing::{read_json, write_json, write_json_with_fds};
use crate::hot_reload::protocol::{
    GradualShutdownRequest, HotReloadListenerRequest, Request, Response,
};
use crate::sources::Source;
use anyhow::{Context, Result};
use std::os::fd::OwnedFd;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uds::tokio::{UnixSeqpacketConn, UnixSeqpacketListener};

#[derive(Clone)]
pub struct SourceHandle {
    pub name: String,
    pub sender: mpsc::UnboundedSender<HotReloadListenerRequest>,
    pub gradual_shutdown_tx: mpsc::UnboundedSender<GradualShutdownRequest>,
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
            .with_context(|| format!("Failed to bind Unix socket to: {}", socket_path))?;
        info!("Unix socket server listening on: {}", socket_path);
        Ok(Self {
            socket_path,
            listener,
            sources,
        })
    }

    pub async fn run(&mut self) -> ! {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    info!("New connection received on Unix socket");
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

    async fn handle_connection(&mut self, mut stream: UnixSeqpacketConn) -> Result<()> {
        let request: Request = read_json(&mut stream).await?;
        info!("Received request: {:?}", request);

        let (response, fds) = self.process_request(request).await;

        // Send response with file descriptors
        if fds.is_empty() {
            write_json(&mut stream, &response).await?;
            debug!("Sent response without FDs: {:?}", response);
        } else {
            let raw_fds: Vec<RawFd> = fds.iter().map(|fd| fd.as_raw_fd()).collect();
            write_json_with_fds(&mut stream, &response, &raw_fds).await?;
            // Explicitly drop the OwnedFds immediately after sending to close the original FDs
            // and unbind the addresses. The kernel has already duplicated the FDs during transfer.
            drop(fds);
            info!(
                "Sent response with {} file descriptors via ancillary data: {:?}",
                raw_fds.len(),
                response
            );
        }

        Ok(())
    }

    async fn process_request(&mut self, request: Request) -> (Response, Vec<OwnedFd>) {
        match request {
            Request::SendListeningSockets => {
                info!("Processing SendListeningSockets request");

                // Send requests to all TcpCodecListener instances and collect responses
                let mut collected_fds = Vec::new();

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
                                    collected_fds.push(listener_socket_fd);
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
                    "Sending response with {} file descriptors",
                    collected_fds.len()
                );
                (Response::SendListeningSockets, collected_fds)
            }
            Request::GradualShutdown { duration } => {
                info!(
                    "Processing GradualShutdown request - initiating gradual connection draining over {:?}",
                    duration
                );

                // Send gradual shutdown requests to all sources
                for source in &self.sources {
                    if let Err(e) = source
                        .gradual_shutdown_tx
                        .send(GradualShutdownRequest { duration })
                    {
                        warn!(
                            "Failed to send gradual shutdown request to source {}: {:?}",
                            source.name, e
                        );
                    }
                }

                info!("Gradual shutdown initiated for all sources");
                (Response::GradualShutdown, Vec::new())
            }
        }
    }
}

impl Drop for UnixSocketServer {
    fn drop(&mut self) {
        if Path::new(&self.socket_path).exists()
            && let Err(e) = std::fs::remove_file(&self.socket_path)
        {
            warn!("Failed to remove socket file {}: {:?}", self.socket_path, e);
        }
    }
}
pub fn start_hot_reload_server(socket_path: String, sources: &[Source]) {
    let source_handles: Vec<SourceHandle> = sources
        .iter()
        .map(|x| SourceHandle {
            name: x.name().to_string(),
            sender: x.get_hot_reload_tx(),
            gradual_shutdown_tx: x.get_gradual_shutdown_tx(),
        })
        .collect();

    tokio::spawn(async move {
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

        loop {
            match UnixSocketServer::new(socket_path.clone(), source_handles.clone()) {
                Ok(mut server) => {
                    info!("Hot reload server created at {}", socket_path);

                    // Run the server - this runs forever until the task is killed
                    server.run().await;
                }
                Err(e) => {
                    info!(
                        "Failed to bind socket at {}: {:?}. \
                         Likely waiting for previous instance to release socket. \
                         Retrying in {:?}...",
                        socket_path, e, RETRY_DELAY
                    );

                    tokio::time::sleep(RETRY_DELAY).await;
                }
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
        let server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

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
        let mut server = UnixSocketServer::new(socket_path.to_string(), source_handles).unwrap();

        // Start server in background - run() never returns, so we just await it indefinitely
        let server_handle = tokio::spawn(async move {
            server.run().await;
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
            Response::GradualShutdown => {
                panic!("Unexpected GradualShutdown response");
            }
            Response::Error(err) => panic!("Unexpected error response: {}", err),
        }

        // Clean up
        server_handle.abort();
    }
}
