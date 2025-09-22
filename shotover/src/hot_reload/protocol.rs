use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    SendListeningSockets {
        port_to_fd: HashMap<u32, FileDescriptor>,
    },

    //BeginDrainingConnections,
    //ShutdownOriginalNode,
    Error(String),
}

impl Response {
    /// Collect all file descriptors from the response for ancillary data transmission
    pub fn collect_fds(&self) -> Vec<RawFd> {
        match self {
            Response::SendListeningSockets { port_to_fd } => {
                port_to_fd.values().map(|fd| fd.0).collect()
            }
            Response::Error(_) => Vec::new(),
        }
    }

    /// Replace file descriptors in the response with received FDs from ancillary data
    pub fn replace_fds_with_received(&mut self, received_fds: Vec<RawFd>) {
        match self {
            Response::SendListeningSockets { port_to_fd } => {
                let mut fd_iter = received_fds.into_iter();
                for fd in port_to_fd.values_mut() {
                    if let Some(new_fd) = fd_iter.next() {
                        fd.0 = new_fd;
                    }
                }
            }
            Response::Error(_) => {
                // No FDs to replace in error responses
            }
        }
    }
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
