use anyhow::Context;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use redis::Client;
use redis::aio::{AsyncStream, MultiplexedConnection};
use std::pin::Pin;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_io_timeout::TimeoutStream;
use tokio_openssl::SslStream;

fn new_sync_connection(address: &str, port: u16) -> redis::Connection {
    let connection = Client::open((address, port))
        .unwrap()
        .get_connection_with_timeout(Duration::from_secs(5))
        .with_context(|| format!("Failed to create valkey connection to port {port}"))
        .unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    connection
}

async fn new_async_connection(address: &str, port: u16) -> MultiplexedConnection {
    Client::open((address, port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

// fn create_tls_client(address: &str, port: u16) -> Client {
//     let root_cert = std::fs::read("tests/test-configs/valkey/tls/certs/localhost_CA.crt").unwrap();
//     let client_cert = std::fs::read("tests/test-configs/valkey/tls/certs/localhost.crt").unwrap();
//     let client_key = std::fs::read("tests/test-configs/valkey/tls/certs/localhost.key").unwrap();
//
//     Client::build_with_tls(
//         format!("rediss://{address}:{port}/#insecure"),
//         TlsCertificates {
//             client_tls: Some(ClientTlsConfig {
//                 client_cert,
//                 client_key,
//             }),
//             root_cert: Some(root_cert),
//         },
//     )
//     .unwrap()
// }

async fn new_async_tls_connection(address: &str, port: u16) -> redis::aio::MultiplexedConnection {
    // let client = create_tls_client(address, port);
    // client.get_multiplexed_async_connection().await.unwrap()
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
) -> redis::aio::MultiplexedConnection {
    let mut stream_with_timeout = TimeoutStream::new(stream);
    stream_with_timeout.set_read_timeout(Some(Duration::from_secs(10)));

    let connection_info = Default::default();
    let (conn, fut) = redis::aio::MultiplexedConnection::new(
        &connection_info,
        Box::pin(stream_with_timeout) as Pin<Box<dyn AsyncStream + Send + Sync>>,
    )
    .await
    .unwrap();

    fut.await;
    conn
}

// fn new_sync_tls_connection(address: &str, port: u16) -> redis::Connection {
// let client = create_tls_client(address, port);
// client.get_connection().unwrap()
// }

pub struct ValkeyConnectionCreator {
    pub address: String,
    pub port: u16,
    pub tls: bool,
}

impl ValkeyConnectionCreator {
    pub async fn new_async(&self) -> redis::aio::MultiplexedConnection {
        if self.tls {
            new_async_tls_connection(&self.address, self.port).await
        } else {
            new_async_connection(&self.address, self.port).await
        }
    }

    pub fn new_sync(&self) -> redis::Connection {
        if self.tls {
            panic!("Not implemented yet");
            // new_sync_tls_connection(&self.address, self.port)
        } else {
            new_sync_connection(&self.address, self.port)
        }
    }
}
