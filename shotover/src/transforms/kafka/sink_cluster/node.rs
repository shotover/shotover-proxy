use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::message::Message;
use crate::tcp;
use crate::tls::TlsConnector;
use crate::transforms::util::cluster_connection_pool::{spawn_read_write_tasks, Connection};
use anyhow::{anyhow, Result};
use kafka_protocol::messages::BrokerId;
use kafka_protocol::protocol::StrBytes;
use std::time::Duration;
use tokio::io::split;

pub struct ConnectionFactory {
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    handshake_message: Option<Message>,
    auth_message: Option<Message>,
}

impl ConnectionFactory {
    pub fn new(tls: Option<TlsConnector>, connect_timeout: Duration) -> Self {
        ConnectionFactory {
            tls,
            connect_timeout,
            handshake_message: None,
            auth_message: None,
        }
    }

    pub async fn create_connection(&self, kafka_address: &KafkaAddress) -> Result<Connection> {
        tracing::info!(
            "creating connection with {:?} {:?}",
            self.handshake_message,
            self.auth_message
        );

        let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkCluster".to_owned());
        let address = (kafka_address.host.to_string(), kafka_address.port as u16);
        if let Some(tls) = self.tls.as_ref() {
            let tls_stream = tls.connect(self.connect_timeout, address).await?;
            let (rx, tx) = split(tls_stream);
            let connection = spawn_read_write_tasks(&codec, rx, tx);
            Ok(connection)
        } else {
            let tcp_stream = tcp::tcp_stream(self.connect_timeout, address).await?;
            let (rx, tx) = tcp_stream.into_split();
            let connection = spawn_read_write_tasks(&codec, rx, tx);
            Ok(connection)
        }
    }
}

pub struct ConnectionFactory {
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
}

impl ConnectionFactory {
    pub fn new(tls: Option<TlsConnector>, connect_timeout: Duration) -> Self {
        ConnectionFactory {
            tls,
            connect_timeout,
        }
    }

    pub async fn create_connection(&self, kafka_address: &KafkaAddress) -> Result<Connection> {
        let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkCluster".to_owned());
        let address = (kafka_address.host.to_string(), kafka_address.port as u16);
        if let Some(tls) = self.tls.as_ref() {
            let tls_stream = tls.connect(self.connect_timeout, address).await?;
            let (rx, tx) = split(tls_stream);
            let connection = spawn_read_write_tasks(&codec, rx, tx);
            Ok(connection)
        } else {
            let tcp_stream = tcp::tcp_stream(self.connect_timeout, address).await?;
            let (rx, tx) = tcp_stream.into_split();
            let connection = spawn_read_write_tasks(&codec, rx, tx);
            Ok(connection)
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct KafkaAddress {
    pub host: StrBytes,
    pub port: i32,
}

impl KafkaAddress {
    pub fn new(host: StrBytes, port: i32) -> Self {
        KafkaAddress { host, port }
    }

    pub fn from_str(address: &str) -> Result<Self> {
        let mut address_iter = address.split(':');
        Ok(KafkaAddress {
            host: StrBytes::from_string(
                address_iter
                    .next()
                    .ok_or_else(|| anyhow!("Address must include ':' seperator"))?
                    .to_owned(),
            ),
            port: address_iter
                .next()
                .ok_or_else(|| anyhow!("Address must include port after ':'"))?
                .parse()
                .map_err(|_| anyhow!("Failed to parse address port as integer"))?,
        })
    }
}

#[derive(Clone)]
pub struct KafkaNode {
    pub broker_id: BrokerId,
    pub kafka_address: KafkaAddress,
    connection: Option<Connection>,
}

impl KafkaNode {
    pub fn new(broker_id: BrokerId, kafka_address: KafkaAddress) -> Self {
        KafkaNode {
            broker_id,
            kafka_address,
            connection: None,
        }
    }

    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
    ) -> Result<&Connection> {
        if self.connection.is_none() {
            self.connection = Some(
                connection_factory
                    .create_connection(&self.kafka_address)
                    .await?,
            );
        }
        Ok(self.connection.as_ref().unwrap())
    }
}
