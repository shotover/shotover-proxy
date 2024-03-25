use crate::codec::cassandra::CassandraCodecBuilder;
use crate::codec::{CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::frame::{CassandraFrame, Frame};
use crate::message::{Message, Messages};
use crate::tls::{TlsConnector, ToHostname};
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use derivative::Derivative;
use std::iter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct CassandraNode {
    pub address: SocketAddr,
    pub rack: String,
    pub host_id: Uuid,
    pub is_up: bool,
    #[derivative(Debug = "ignore")]
    pub outbound: Option<SinkConnection>,
    pending_request_stream_ids: Vec<i16>,

    #[derivative(Debug = "ignore")]
    pub tokens: Vec<Murmur3Token>,
}

impl Clone for CassandraNode {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            rack: self.rack.clone(),
            outbound: None,
            host_id: self.host_id.clone(),
            is_up: self.is_up.clone(),
            tokens: self.tokens.clone(),
            pending_request_stream_ids: vec![],
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
            pending_request_stream_ids: vec![],
        }
    }

    pub fn send(&mut self, requests: Vec<Message>) -> Result<()> {
        let connection = self.outbound.as_mut().unwrap();
        self.pending_request_stream_ids
            .extend(requests.iter().map(|x| x.stream_id().unwrap()));
        Ok(connection.send(requests)?)
    }

    pub fn try_recv(&mut self, version: Version) -> Option<Vec<Message>> {
        if self.is_up {
            if let Some(connection) = self.outbound.as_mut() {
                match connection.try_recv() {
                    Ok(results) => {
                        for result in &results {
                            let stream_id = result.stream_id().unwrap();
                            let index = self
                                .pending_request_stream_ids
                                .iter()
                                .position(|x| *x == stream_id)
                                .unwrap();
                            self.pending_request_stream_ids.remove(index);
                        }
                        Some(results)
                    }
                    Err(err) => {
                        self.report_issue();
                        Some(
                            self.pending_request_stream_ids
                                .drain(..)
                                .map(|stream_id| {
                                    Message::from_frame(Frame::Cassandra(
                                        CassandraFrame::shotover_error(
                                            stream_id,
                                            version,
                                            &format!("{err}"),
                                        ),
                                    ))
                                })
                                .collect(),
                        )
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut SinkConnection> {
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
    force_run_chain: Option<Arc<Notify>>,
    #[derivative(Debug = "ignore")]
    codec_builder: CassandraCodecBuilder,
}

impl Clone for ConnectionFactory {
    fn clone(&self) -> Self {
        Self {
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout.clone(),
            init_handshake: self.init_handshake.clone(),
            use_message: None,
            tls: self.tls.clone(),
            force_run_chain: None,
            codec_builder: self.codec_builder.clone(),
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
        }
    }

    /// Create a new instance with the same configuration but a fresh state.
    pub fn new_with_same_config(&self) -> Self {
        Self {
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
            init_handshake: vec![],
            use_message: None,
            tls: self.tls.clone(),
            force_run_chain: None,
            codec_builder: self.codec_builder.clone(),
        }
    }

    pub async fn new_connection<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
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
        let mut wait_for = requests.len();
        connection.send(requests)?;

        while wait_for > 0 {
            wait_for -= connection.recv().await?.len();
        }

        Ok(connection)
    }

    pub fn push_handshake_message(&mut self, message: Message) {
        self.init_handshake.push(message);
    }

    /// Add a USE statement to the handshake ensures that any new connection
    /// created will have the correct keyspace setup.
    // Existing USE statements should be discarded as we are changing keyspaces
    pub fn set_use_message(&mut self, message: Message) {
        self.use_message = Some(message);
    }

    pub fn set_force_run_chain(&mut self, force_run_chain: Arc<Notify>) {
        self.force_run_chain = Some(force_run_chain);
    }

    pub fn get_version(&mut self) -> Result<Version> {
        for message in &mut self.init_handshake {
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                return Ok(frame.version);
            }
        }
        Err(anyhow!(
            "connection version could not be retrieved from the handshake because none of the {} messages in the handshake could be parsed",
            self.init_handshake.len()
        ))
    }
}
