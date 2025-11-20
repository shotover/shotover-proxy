use anyhow::Context;
use redis::aio::MultiplexedConnection;
use redis::{Client, ClientTlsConfig, TlsCertificates};
use rustls::crypto::ring::default_provider;
use std::time::Duration;

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

fn create_tls_client(address: &str, port: u16) -> Client {
    create_tls_client_with_certs(address, port, "tests/test-configs/valkey/tls/certs")
}

fn create_tls_client_with_certs(address: &str, port: u16, cert_dir: &str) -> Client {
    default_provider().install_default().ok();
    let root_cert = std::fs::read(format!("{}/localhost_CA.crt", cert_dir)).unwrap();
    let client_cert = std::fs::read(format!("{}/localhost.crt", cert_dir)).unwrap();
    let client_key = std::fs::read(format!("{}/localhost.key", cert_dir)).unwrap();

    Client::build_with_tls(
        format!("rediss://{address}:{port}/#insecure"),
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert,
                client_key,
            }),
            root_cert: Some(root_cert),
        },
    )
    .unwrap()
}

async fn new_async_tls_connection(address: &str, port: u16) -> redis::aio::MultiplexedConnection {
    let client = create_tls_client(address, port);
    client.get_multiplexed_async_connection().await.unwrap()
}

fn new_sync_tls_connection(address: &str, port: u16) -> redis::Connection {
    let client = create_tls_client(address, port);
    client.get_connection().unwrap()
}

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
            new_sync_tls_connection(&self.address, self.port)
        } else {
            new_sync_connection(&self.address, self.port)
        }
    }
}

/// Create a TLS client with custom certificate directory
pub fn create_tls_valkey_client_from_certs(address: &str, port: u16, cert_dir: &str) -> Client {
    create_tls_client_with_certs(address, port, cert_dir)
}
