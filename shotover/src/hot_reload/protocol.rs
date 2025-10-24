use serde::{Deserialize, Serialize};
use std::os::unix::io::OwnedFd;

/// Requests that can be sent from replacement shotover to original shotover
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    SendListeningSockets,
    ShutdownOriginalNode,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    SendListeningSockets,
    ShutdownOriginalNode,
    Error(String),
}
/// Request sent from hot reload server to TcpCodecListener
pub struct HotReloadListenerRequest {
    pub return_chan: tokio::sync::oneshot::Sender<HotReloadListenerResponse>,
}

/// Response sent from TcpCodecListener back to hot reload server
/// listener_socket_fd is an OwnedFd. It will be converted to RawFd only during transfer.
#[derive(Debug)]
pub enum HotReloadListenerResponse {
    HotReloadResponse {
        port: u16,
        listener_socket_fd: OwnedFd,
    },
    NoListenerAvailable,
}
