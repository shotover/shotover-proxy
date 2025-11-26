//! Protocol definitions for hot reload communication between shotover instances.

use serde::{Deserialize, Serialize};
use std::os::unix::io::OwnedFd;
use std::time::Duration;

/// Requests that can be sent from replacement shotover to original shotover
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    SendListeningSockets,
    /// Request for gradual shutdown
    GradualShutdown {
        /// The total duration in seconds for the shutdown process, specifying how long the original instance should spend draining connections.
        duration_secs: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    SendListeningSockets,
    GradualShutdown,
    Error(String),
}
/// Request sent from hot reload server to TcpCodecListener
pub struct HotReloadListenerRequest {
    pub return_chan: tokio::sync::oneshot::Sender<HotReloadListenerResponse>,
}

/// Request sent from hot reload server to TcpCodecListener for gradual shutdown
pub struct GradualShutdownRequest {
    /// The total duration for the gradual shutdown process
    pub duration: Duration,
}

/// Response sent from TcpCodecListener back to hot reload server
#[derive(Debug)]
pub enum HotReloadListenerResponse {
    HotReloadResponse {
        port: u16,
        /// An OwnedFd that will be converted to RawFd only during transfer
        listener_socket_fd: OwnedFd,
    },
    NoListenerAvailable,
}
