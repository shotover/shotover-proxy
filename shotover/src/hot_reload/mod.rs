pub mod client;
pub mod fd_utils;
pub mod json_parsing;
pub mod protocol;
pub mod server;

#[cfg(test)]
pub mod tests {
    use tokio::net::UnixStream;

    pub async fn wait_for_unix_socket_connection(socket_path: &str, timeout_ms: u64) {
        for _ in 0..timeout_ms / 5 {
            if UnixStream::connect(socket_path).await.is_ok() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!(
            "Failed to connect to Unix socket at {} after waiting",
            socket_path
        );
    }
}
