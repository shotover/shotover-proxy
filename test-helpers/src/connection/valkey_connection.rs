use anyhow::Context;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use redis::Client;
use redis::aio::AsyncStream;
use std::pin::Pin;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_io_timeout::TimeoutStream;
use tokio_openssl::SslStream;

pub fn new(port: u16) -> redis::Connection {
    let address = "127.0.0.1";
    let connection = Client::open((address, port))
        .unwrap()
        .get_connection()
        .with_context(|| format!("Failed to create valkey connection to port {port}"))
        .unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    connection
}

pub async fn new_async(address: &str, port: u16) -> redis::aio::Connection {
    let stream = Box::pin(
        tokio::net::TcpStream::connect((address, port))
            .await
            .with_context(|| format!("Failed to create async valkey connection to port {port}"))
            .unwrap(),
    );
    new_async_inner(Box::pin(stream) as Pin<Box<dyn AsyncStream + Send + Sync>>).await
}

pub async fn new_async_tls(address: &str, port: u16) -> redis::aio::Connection {
    let certificate_authority_path = "tests/test-configs/valkey/tls/certs/localhost_CA.crt";
    let certificate_path = "tests/test-configs/valkey/tls/certs/localhost.crt";
    let private_key_path = "tests/test-configs/valkey/tls/certs/localhost.key";

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file(certificate_authority_path).unwrap();
    builder
        .set_private_key_file(private_key_path, SslFiletype::PEM)
        .unwrap();
    builder
        .set_certificate_chain_file(certificate_path)
        .unwrap();

    let ssl = builder
        .build()
        .configure()
        .unwrap()
        .verify_hostname(false)
        .into_ssl(address)
        .unwrap();

    let tcp_stream = TcpStream::connect((address, port)).await.unwrap();
    let mut tls_stream = SslStream::new(ssl, tcp_stream).unwrap();
    Pin::new(&mut tls_stream).connect().await.unwrap();
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
