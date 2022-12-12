use redis::aio::AsyncStream;
use redis::Client;
use shotover_proxy::tls::{TlsConnector, TlsConnectorConfig};
use std::pin::Pin;
use std::time::Duration;
use tokio_io_timeout::TimeoutStream;

#[allow(dead_code)]
pub fn new(port: u16) -> redis::Connection {
    let address = "127.0.0.1";
    test_helpers::wait_for_socket_to_open(address, port);

    let connection = Client::open((address, port))
        .unwrap()
        .get_connection()
        .unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    connection
}

#[allow(dead_code)]
pub async fn new_async(port: u16) -> redis::aio::Connection {
    let address = "127.0.0.1";
    test_helpers::wait_for_socket_to_open(address, port);

    let stream = Box::pin(
        tokio::net::TcpStream::connect((address, port))
            .await
            .unwrap(),
    );
    new_async_inner(Box::pin(stream) as Pin<Box<dyn AsyncStream + Send + Sync>>).await
}

#[allow(dead_code)]
pub async fn new_async_tls(port: u16, config: TlsConnectorConfig) -> redis::aio::Connection {
    let address = "127.0.0.1";
    test_helpers::wait_for_socket_to_open(address, port);

    let connector = TlsConnector::new(config).unwrap();
    let tls_stream = connector
        .connect(Duration::from_secs(3), (address, port))
        .await
        .unwrap();
    new_async_inner(Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>).await
}

async fn new_async_inner(
    stream: Pin<Box<dyn AsyncStream + Send + Sync>>,
) -> redis::aio::Connection {
    let mut stream_with_timeout = TimeoutStream::new(stream);
    stream_with_timeout.set_read_timeout(Some(Duration::from_secs(10)));

    let connection_info = Default::default();
    redis::aio::Connection::new(
        &connection_info,
        Box::pin(stream_with_timeout) as Pin<Box<dyn AsyncStream + Send + Sync>>,
    )
    .await
    .unwrap()
}
