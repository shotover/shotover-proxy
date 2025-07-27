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
