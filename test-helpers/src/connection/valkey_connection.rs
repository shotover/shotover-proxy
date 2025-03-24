use anyhow::Context;
use openssl::ssl::{Ssl, SslConnector, SslFiletype, SslMethod};
use redis::aio::{AsyncStream, MultiplexedConnection};
use redis::{
    Client, ClientTlsConfig, Cmd, Commands, ConnectionLike, Pipeline, RedisFuture, RedisResult,
    TlsCertificates, ToRedisArgs, Value,
};
use std::fs::File;
use std::io::{BufReader, Read};
use std::pin::Pin;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_io_timeout::TimeoutStream;
use tokio_openssl::SslStream;

#[deprecated]
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

fn new_sync_connection(address: &str, port: u16) -> redis::Connection {
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

#[deprecated]
pub async fn new_async(address: &str, port: u16) -> redis::aio::Connection {
    let stream = Box::pin(
        tokio::net::TcpStream::connect((address, port))
            .await
            .with_context(|| format!("Failed to create async valkey connection to port {port}"))
            .unwrap(),
    );
    new_async_inner(Box::pin(stream) as Pin<Box<dyn AsyncStream + Send + Sync>>).await
}

async fn new_async_connection(address: &str, port: u16) -> MultiplexedConnection {
    let connection = Client::open((address, port))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    connection
}

#[deprecated]
pub async fn new_async_tls(address: &str, port: u16) -> redis::aio::Connection {
    let ssl = create_ssl(address, port);

    let tcp_stream = TcpStream::connect((address, port)).await.unwrap();
    let mut tls_stream = SslStream::new(ssl, tcp_stream).unwrap();
    Pin::new(&mut tls_stream).connect().await.unwrap();
    new_async_inner(Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>).await
}

fn create_ssl(address: &str, port: u16) -> Ssl {
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

    builder
        .build()
        .configure()
        .unwrap()
        .verify_hostname(false)
        .into_ssl(address)
        .unwrap()
}

fn create_tls_client(address: &str, port: u16) -> Client {
    let root_cert_file =
        File::open("tests/test-configs/valkey/tls/certs/localhost_CA.crt").unwrap();
    let mut root_cert_vec = Vec::new();
    BufReader::new(root_cert_file)
        .read_to_end(&mut root_cert_vec)
        .unwrap();

    let cert_file = File::open("tests/test-configs/valkey/tls/certs/localhost.crt").unwrap();
    let mut client_cert_vec = Vec::new();
    BufReader::new(cert_file)
        .read_to_end(&mut client_cert_vec)
        .unwrap();

    let key_file = File::open("tests/test-configs/valkey/tls/certs/localhost.key").unwrap();
    let mut client_key_vec = Vec::new();
    BufReader::new(key_file)
        .read_to_end(&mut client_key_vec)
        .unwrap();

    Client::build_with_tls(
        format!("rediss://{address}:{port}"),
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert: client_cert_vec,
                client_key: client_key_vec,
            }),
            root_cert: Some(root_cert_vec),
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

pub struct ValkeyConnectionCreator {
    pub address: String,
    pub port: u16,
    pub tls: bool,
    // pub driver: ValkeyDriver,
}

impl ValkeyConnectionCreator {
    // pub fn new(address: &str, port: u16, tls: bool, driver: ValkeyDriver) -> Self {
    //     Self {
    //         address: address.into(),
    //         port,
    //         tls,
    //         driver,
    //     }
    // }
    //
    pub async fn build(&self, driver: ValkeyDriver) -> ValkeyConnection {
        match driver {
            ValkeyDriver::Sync => ValkeyConnection::Sync(self.new_sync()),
            ValkeyDriver::Async => ValkeyConnection::Async(self.new_async().await),
        }
    }

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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ValkeyDriver {
    Sync,
    Async,
}

pub enum ValkeyConnection {
    Sync(redis::Connection),
    Async(redis::aio::MultiplexedConnection),
}

impl redis::ConnectionLike for ValkeyConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        if let ValkeyConnection::Sync(conn) = self {
            conn.req_packed_command(cmd)
        } else {
            unreachable!()
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if let ValkeyConnection::Sync(conn) = self {
            conn.req_packed_commands(cmd, offset, count)
        } else {
            unreachable!()
        }
    }

    fn get_db(&self) -> i64 {
        if let ValkeyConnection::Sync(conn) = self {
            conn.get_db()
        } else {
            unreachable!()
        }
    }

    fn check_connection(&mut self) -> bool {
        if let ValkeyConnection::Sync(conn) = self {
            conn.check_connection()
        } else {
            unreachable!()
        }
    }

    fn is_open(&self) -> bool {
        if let ValkeyConnection::Sync(conn) = self {
            conn.is_open()
        } else {
            unreachable!()
        }
    }
}

impl redis::aio::ConnectionLike for ValkeyConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        if let ValkeyConnection::Async(conn) = self {
            conn.req_packed_command(cmd)
        } else {
            unreachable!()
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        if let ValkeyConnection::Async(conn) = self {
            conn.req_packed_commands(cmd, offset, count)
        } else {
            unreachable!()
        }
    }

    fn get_db(&self) -> i64 {
        if let ValkeyConnection::Async(conn) = self {
            conn.get_db()
        } else {
            unreachable!()
        }
    }
}

// impl ValkeyConnection {
//     pub async fn into_pubsub(self) -> ValkeyPubsub {
//         match self {
//             ValkeyConnection::Sync(mut conn) => ValkeyPubsub::Sync(conn.as_pubsub()),
//             ValkeyConnection::Async(conn) => {
//                 panic!("Does not support converting to async pubsub connection")
//             }
//         }
//     }
//
//     pub async fn publish<K: ToRedisArgs, E: ToRedisArgs>(
//         self,
//         channel: K,
//         message: E,
//     ) -> RedisResult<()> {
//         match self {
//             ValkeyConnection::Sync(mut conn) => conn.publish(channel, message).unwrap()?,
//             ValkeyConnection::Async(mut conn) => conn.publish(channel, message).unwrap()?,
//         }
//     }
// }
//
// pub enum ValkeyPubsub<'a> {
//     Sync(redis::PubSub<'a>),
//     Async(redis::aio::PubSub),
// }
//
// impl ValkeyPubsub {
//     pub async fn into_connection(self) {
//         drop(self);
//     }
//
//     pub async fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
//         match self {
//             ValkeyPubsub::Sync(conn) => Ok(conn.subscribe(channel)?),
//             ValkeyPubsub::Async(conn) => Ok(conn.subscribe(channel).await?),
//         }
//     }
//
//     pub async fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
//         match self {
//             ValkeyPubsub::Sync(conn) => Ok(conn.unsubscribe(channel)?),
//             ValkeyPubsub::Async(conn) => Ok(conn.unsubscribe(channel).await?),
//         }
//     }
//
//     pub async fn psubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
//         match self {
//             ValkeyPubsub::Sync(conn) => Ok(conn.psubscribe(channel)?),
//             ValkeyPubsub::Async(conn) => Ok(conn.psubscribe(channel).await?),
//         }
//     }
// }
