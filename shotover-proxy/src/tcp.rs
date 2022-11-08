use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    time::timeout,
};

pub async fn tcp_stream<A: ToSocketAddrs + std::fmt::Debug>(destination: A) -> Result<TcpStream> {
    timeout(Duration::from_secs(3), TcpStream::connect(&destination))
        .await
        .map_err(|_| {
            anyhow!(
                "destination {destination:?} did not respond to connection attempt within 3 seconds"
            )
        })?
        .map_err(|e| {
            anyhow!(e).context(format!("Failed to connect to destination {destination:?}"))
        })
}
