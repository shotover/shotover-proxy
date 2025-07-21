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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]

    fn test_request_serialization() {
        let request = Request::SendListeningSockets;
        let json = serde_json::to_string(&request).unwrap();

        let deserialized: Request = serde_json::from_str(&json).unwrap();
        match deserialized {
            Request::SendListeningSockets => {}
        }
    }
    #[test]
    fn test_response_serialization() {
        let mut port_to_fd = HashMap::new();
        port_to_fd.insert(6380, FileDescriptor(10));
        port_to_fd.insert(6381, FileDescriptor(11));
        let response = Response::SendListeningSockets { port_to_fd };

        let json = serde_json::to_string(&response).unwrap();

        let deserialized: Response = serde_json::from_str(&json).unwrap();

        match deserialized {
            Response::SendListeningSockets { port_to_fd } => {
                assert_eq!(port_to_fd.len(), 2);
                assert_eq!(port_to_fd.get(&6380).unwrap().0, 10);
                assert_eq!(port_to_fd.get(&6381).unwrap().0, 11);
            }
            _ => panic!("Wrong response type"),
        }
    }
    #[test]
    fn test_error_response_serialization() {
        let response = Response::Error("Something went wrong".to_string());

        let json = serde_json::to_string(&response).unwrap();

        let deserialized: Response = serde_json::from_str(&json).unwrap();
        match deserialized {
            Response::Error(msg) => {
                assert_eq!(msg, "Something went wrong");
            }
            _ => panic!("Wrong response type"),
        }
    }
}
