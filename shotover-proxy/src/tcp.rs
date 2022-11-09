use anyhow::{anyhow, Result};
use std::time::Duration;
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    time::timeout,
};

pub async fn tcp_stream<A: ToSocketAddrs + std::fmt::Debug>(
    connect_timeout: Duration,
    destination: A,
) -> Result<TcpStream> {
    timeout(connect_timeout, TcpStream::connect(&destination))
        .await
        .map_err(|_| {
            anyhow!(
                "destination {destination:?} did not respond to connection attempt within {connect_timeout:?}"
            )
        })?
        .map_err(|e| {
            anyhow!(e).context(format!("Failed to connect to destination {destination:?}"))
        })
}
