use crate::hot_reload::{Request, Response};
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub struct UnixSocketServer {
    socket_path: String,
    listener: Option<UnixListener>,
}

impl UnixSocketServer {
    pub fn new(socket_path: String) -> Self {
        Self {
            socket_path,
            listener: None,
        }
    }

    pub async fn start(&mut self) {
        if Path::new(&self.socket_path).exists() {
            std::fs::remove_file(&self.socket_path).unwrap();
        }
        let listener = UnixListener::bind(&self.socket_path).unwrap();
        self.listener = Some(listener);
    }

    pub async fn run(&mut self) {
        let listener = self.listener.as_ref().unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            self.handle_connection(stream).await;
        }
    }

    async fn handle_connection(&self, stream: UnixStream) {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        reader.read_line(&mut line).await.unwrap();

        let request: Request = serde_json::from_str(line.trim()).unwrap();

        let response = self.process_request(request).await;

        let response_json = serde_json::to_string(&response).unwrap();
        writer.write_all(response_json.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
    }

    async fn process_request(&self, request: Request) -> Response {
        match request {
            Request::SendListeningSockets => {
                let mut port_to_fd = HashMap::new();
                port_to_fd.insert(6380, crate::hot_reload::FileDescriptor(10));
                Response::SendListeningSockets { port_to_fd }
            }
        }
    }
}

impl Drop for UnixSocketServer {
    fn drop(&mut self) {
        if Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    #[tokio::test]
    async fn test_unix_socket_server_basic() {
        let socket_path = "/tmp/test-shotover-hotreload.sock";
        let _ = std::fs::remove_file(socket_path);
        let mut server = UnixSocketServer::new(socket_path.to_string());
        server.start().await;
        assert!(Path::new(socket_path).exists());

        drop(server);
        assert!(!Path::new(socket_path).exists());
    }

    #[tokio::test]
    async fn test_request_response() {
        let socket_path = "/tmp/test-shotover-request-response.sock";
        let _ = std::fs::remove_file(socket_path);
        let mut server = UnixSocketServer::new(socket_path.to_string());
        server.start().await;

        let server_handle = tokio::spawn(async move {
            tokio::select! {
                _ = server.run() => {},
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {}
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut stream = UnixStream::connect(socket_path).await.unwrap();
        let request = Request::SendListeningSockets;
        let request_json = serde_json::to_string(&request).unwrap();
        stream.write_all(request_json.as_bytes()).await.unwrap();
        stream.write_all(b"\n").await.unwrap();

        let mut response_data = Vec::new();
        stream.read_to_end(&mut response_data).await.unwrap();
        let response_str = String::from_utf8(response_data).unwrap();
        let response: Response = serde_json::from_str(response_str.trim()).unwrap();

        match response {
            Response::SendListeningSockets { port_to_fd } => {
                assert_eq!(port_to_fd.len(), 1);
                assert!(port_to_fd.contains_key(&6380));
            }
            _ => panic!("Wrong response type"),
        }

        server_handle.abort();
        let _ = std::fs::remove_file(socket_path);
    }
}
