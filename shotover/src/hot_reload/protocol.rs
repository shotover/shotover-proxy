use serde::{Deserialize, Serialize};
use std::os::unix::io::RawFd;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDescriptor(pub RawFd);

/// Requests that can be sent from replacement shotover to original shotover
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    SendListeningSockets,
    //BeginDrainingConnections,
    //ShutdownOriginalNode,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    SendListeningSockets,

    //BeginDrainingConnections,
    //ShutdownOriginalNode,
    Error(String),
}

impl Response {
    /// Collect all file descriptors from the response for ancillary data transmission
    pub fn collect_fds(&self) -> Vec<RawFd> {
        match self {
            Response::SendListeningSockets => Vec::new(), // FDs will be passed separately
            Response::Error(_) => Vec::new(),
        }
    }

    pub fn replace_fds_with_received(&mut self, _received_fds: Vec<RawFd>) {}
}

/// Request sent from hot reload server to TcpCodecListener
pub struct HotReloadListenerRequest {
    pub return_chan: tokio::sync::oneshot::Sender<HotReloadListenerResponse>,
}

/// Response sent from TcpCodecListener back to hot reload server
#[derive(Debug)]
pub enum HotReloadListenerResponse {
    HotReloadResponse {
        port: u16,
        listener_socket_fd: FileDescriptor,
    },
    NoListenerAvailable,
}
