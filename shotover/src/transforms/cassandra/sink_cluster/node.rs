use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodecBuilder;
use crate::codec::{CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::frame::Frame;
use crate::message::Message;
use crate::tls::{TlsConnector, ToHostname};
use anyhow::{Result, anyhow};
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use derivative::Derivative;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct CassandraNode {
    pub address: SocketAddr,
    pub rack: String,
    pub host_id: Uuid,
    pub is_up: bool,

    #[derivative(Debug = "ignore")]
    pub outbound: Option<CassandraConnection>,
    #[derivative(Debug = "ignore")]
    pub tokens: Vec<Murmur3Token>,
}

impl Clone for CassandraNode {
    fn clone(&self) -> Self {
        Self {
            address: self.address,
            rack: self.rack.clone(),
            outbound: None,
            host_id: self.host_id,
            is_up: self.is_up,
            tokens: self.tokens.clone(),
        }
    }
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

    pub fn try_recv(&mut self, responses: &mut Vec<Message>, version: Version) {
        if let Some(connection) = self.outbound.as_mut()
            && let Err(()) = connection.try_recv(responses, version)
        {
            self.report_issue()
        }
    }

    pub async fn recv_all_pending(&mut self, responses: &mut Vec<Message>, version: Version) {
        if let Some(connection) = self.outbound.as_mut()
            && let Err(()) = connection.recv_all_pending(responses, version).await
        {
            self.report_issue()
        }
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
            force_run_chain: None,
            codec_builder: self.codec_builder.clone(),
            version: None,
        }
    }

    pub async fn new_connection<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        &self,
        address: A,
    ) -> Result<CassandraConnection> {
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

        Ok(CassandraConnection::new(connection))
    }

    pub fn push_handshake_message(&mut self, mut request: Message) {
        if self.version.is_none()
            && let Some(Frame::Cassandra(frame)) = request.frame()
        {
            self.version = Some(frame.version);
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
