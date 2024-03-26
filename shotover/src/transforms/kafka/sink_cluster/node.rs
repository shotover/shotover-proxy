use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::message::Message;
use crate::tls::TlsConnector;
use anyhow::{anyhow, Result};
use kafka_protocol::messages::BrokerId;
use kafka_protocol::protocol::StrBytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

pub struct ConnectionFactory {
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    handshake_message: Option<Message>,
    auth_message: Option<Message>,
    force_run_chain: Arc<Notify>,
}

impl ConnectionFactory {
    pub fn new(
        tls: Option<TlsConnector>,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
        force_run_chain: Arc<Notify>,
    ) -> Self {
        ConnectionFactory {
            tls,
            connect_timeout,
            handshake_message: None,
            auth_message: None,
            force_run_chain,
            read_timeout,
        }
    }

    pub fn add_handshake_message(&mut self, message: Message) {
        self.handshake_message = Some(message);
    }

    pub fn add_auth_message(&mut self, message: Message) {
        self.auth_message = Some(message);
    }

    pub async fn create_connection(&self, kafka_address: &KafkaAddress) -> Result<SinkConnection> {
        let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkCluster".to_owned());
        let address = (kafka_address.host.to_string(), kafka_address.port as u16);
        let mut connection = SinkConnection::new(
            address,
            codec,
            &self.tls,
            self.connect_timeout,
            self.force_run_chain.clone(),
            self.read_timeout,
        )
        .await?;

        if let Some(auth_message) = self.auth_message.as_ref() {
            let handshake_msg = self.handshake_message.as_ref().unwrap();

            connection.send(vec![handshake_msg.clone(), auth_message.clone()])?;
            let mut received_count = 0;
            while received_count < 2 {
                received_count += connection.recv().await?.len();
            }
        }

        Ok(connection)
    }
}

#[derive(Clone, PartialEq, Debug)]
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

pub struct KafkaNode {
    pub broker_id: BrokerId,
    pub rack: Option<StrBytes>,
    pub kafka_address: KafkaAddress,
    connection: Option<SinkConnection>,
}

impl Clone for KafkaNode {
    fn clone(&self) -> Self {
        Self {
            broker_id: self.broker_id,
            rack: self.rack.clone(),
            kafka_address: self.kafka_address.clone(),
            connection: None,
        }
    }
}

impl KafkaNode {
    pub fn new(broker_id: BrokerId, kafka_address: KafkaAddress, rack: Option<StrBytes>) -> Self {
        KafkaNode {
            broker_id,
            kafka_address,
            rack,
            connection: None,
        }
    }

    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut SinkConnection> {
        if self.connection.is_none() {
            self.connection = Some(
                connection_factory
                    .create_connection(&self.kafka_address)
                    .await?,
            );
        }
        Ok(self.connection.as_mut().unwrap())
    }

    pub fn get_connection_if_open(&mut self) -> Option<&mut SinkConnection> {
        self.connection.as_mut()
    }
}
