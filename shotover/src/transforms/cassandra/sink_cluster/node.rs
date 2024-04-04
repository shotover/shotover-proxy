use crate::codec::cassandra::CassandraCodecBuilder;
use crate::codec::{CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::frame::Frame;
use crate::message::{Message, Messages};
use crate::tls::{TlsConnector, ToHostname};
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use derivative::Derivative;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct CassandraNode {
    pub address: SocketAddr,
    pub rack: String,
    pub outbound: Option<CassandraConnection>,
    pub host_id: Uuid,
    pub is_up: bool,

    #[derivative(Debug = "ignore")]
    pub tokens: Vec<Murmur3Token>,
}

impl CassandraNode {
    pub fn new(
        address: SocketAddr,
        rack: String,
        tokens: Vec<Murmur3Token>,
        host_id: Uuid,
    ) -> Self {
        Self {
            address,
            rack,
            tokens,
            host_id,
            outbound: None,
            is_up: true,
        }
    }

    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut CassandraConnection> {
        if self.outbound.is_none() {
            self.outbound = Some(connection_factory.new_connection(self.address).await?)
        }

        Ok(self.outbound.as_mut().unwrap())
    }

    pub fn report_issue(&mut self) {
        self.is_up = false;
        self.outbound = None;
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ConnectionFactory {
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    init_handshake: Vec<Message>,
    use_message: Option<Message>,
    #[derivative(Debug = "ignore")]
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    #[derivative(Debug = "ignore")]
    codec_builder: CassandraCodecBuilder,
    version: Option<Version>,
    force_run_chain: Option<Arc<Notify>>,
}

impl Clone for ConnectionFactory {
    fn clone(&self) -> Self {
        Self {
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            init_handshake: self.init_handshake.clone(),
            use_message: None,
            tls: self.tls.clone(),
            pushed_messages_tx: None,
            force_run_chain: None,
            codec_builder: self.codec_builder.clone(),
            version: self.version,
        }
    }
}

impl ConnectionFactory {
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
        tls: Option<TlsConnector>,
    ) -> Self {
        Self {
            connect_timeout,
            read_timeout,
            init_handshake: vec![],
            use_message: None,
            tls,
            pushed_messages_tx: None,
            force_run_chain: None,
            codec_builder: CassandraCodecBuilder::new(
                Direction::Sink,
                "CassandraSinkCluster".to_owned(),
            ),
            version: None,
        }
    }

    /// Create a new instance with the same configuration but a fresh state.
    pub fn new_with_same_config(&self) -> Self {
        Self {
            connect_timeout: self.connect_timeout,
            init_handshake: vec![],
            read_timeout: self.read_timeout,
            use_message: None,
            tls: self.tls.clone(),
            pushed_messages_tx: None,
            force_run_chain: None,
            codec_builder: self.codec_builder.clone(),
            version: None,
        }
    }

    pub async fn new_connection<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        &self,
        address: A,
    ) -> Result<CassandraConnection> {
        let outbound = CassandraConnection::new(
            self.connect_timeout,
            address,
            self.codec_builder.clone(),
            self.tls.clone(),
            self.pushed_messages_tx.clone(),
        )
        .await
        .map_err(|e| e.context("Failed to create new connection"))?;

        for handshake_message in &self.init_handshake {
            outbound
                .send(handshake_message.clone())
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with handshake, tx failed")
                })?
                .await
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with handshake, rx failed")
                })??;
        }

        if let Some(use_message) = &self.use_message {
            outbound
                .send(use_message.clone())
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with use message, tx failed")
                })?
                .await
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with use message, rx failed")
                })??;
        }

        Ok(outbound)
    }

    pub fn push_handshake_message(&mut self, mut request: Message) {
        if self.version.is_none() {
            if let Some(Frame::Cassandra(frame)) = request.frame() {
                self.version = Some(frame.version);
            }
        }
        self.init_handshake.push(request);
    }

    pub async fn new_sink_connection<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        &self,
        address: A,
    ) -> Result<SinkConnection> {
        let mut connection = SinkConnection::new(
            address,
            self.codec_builder.clone(),
            &self.tls,
            self.connect_timeout,
            self.force_run_chain.clone().unwrap(),
            self.read_timeout,
        )
        .await
        .map_err(|e| e.context("Failed to create new connection"))?;

        let requests: Vec<Message> = self
            .init_handshake
            .iter()
            .cloned()
            .chain(self.use_message.clone())
            .collect();
        for request in requests {
            connection.send(vec![request])?;
            connection.recv().await?;
        }

        Ok(connection)
    }

    /// Add a USE statement to the handshake ensures that any new connection
    /// created will have the correct keyspace setup.
    // Existing USE statements should be discarded as we are changing keyspaces
    pub fn set_use_message(&mut self, message: Message) {
        self.use_message = Some(message);
    }

    pub fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }

    pub fn get_version(&self) -> Result<Version> {
        if let Some(version) = self.version {
            Ok(version)
        } else {
            Err(anyhow!(
                "connection version could not be retrieved from the handshake because none of the {} messages in the handshake could be parsed",
                self.init_handshake.len()
            ))
        }
    }

    pub fn set_force_run_chain(&mut self, force_run_chain: Arc<Notify>) {
        self.force_run_chain = Some(force_run_chain);
    }
}
