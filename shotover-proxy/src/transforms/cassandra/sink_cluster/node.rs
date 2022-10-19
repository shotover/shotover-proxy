use crate::codec::cassandra::CassandraCodec;
use crate::frame::Frame;
use crate::message::{Message, Messages};
use crate::tls::TlsConnector;
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::{anyhow, Result};
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use std::net::SocketAddr;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CassandraNode {
    pub address: SocketAddr,
    pub rack: String,
    pub tokens: Vec<Murmur3Token>,
    pub outbound: Option<CassandraConnection>,
    pub host_id: Uuid,
    pub is_up: bool,
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
}

#[derive(Debug)]
pub struct ConnectionFactory {
    init_handshake: Vec<Message>,
    use_message: Option<Message>,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
}

impl Clone for ConnectionFactory {
    fn clone(&self) -> Self {
        Self {
            init_handshake: self.init_handshake.clone(),
            use_message: None,
            tls: self.tls.clone(),
            pushed_messages_tx: None,
        }
    }
}

impl ConnectionFactory {
    pub fn new(tls: Option<TlsConnector>) -> Self {
        Self {
            init_handshake: vec![],
            use_message: None,
            tls,
            pushed_messages_tx: None,
        }
    }

    /// For when you want to clone the config options for creating new connections but none of the state.
    /// When the transform chain is cloned for a new incoming connection, this method should be used so the state doesn't also get cloned to
    /// the new connection as aswell.
    pub fn new_with_same_config(&self) -> Self {
        Self {
            init_handshake: vec![],
            use_message: None,
            tls: self.tls.clone(),
            pushed_messages_tx: None,
        }
    }

    pub async fn new_connection<A: ToSocketAddrs + std::fmt::Debug>(
        &self,
        address: A,
    ) -> Result<CassandraConnection> {
        let outbound = CassandraConnection::new(
            address,
            CassandraCodec::new(),
            self.tls.clone(),
            self.pushed_messages_tx.clone(),
        )
        .await
        .map_err(|e| e.context("Failed to create new connection"))?;

        for handshake_message in &self.init_handshake {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            outbound
                .send(handshake_message.clone(), return_chan_tx)
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with handshake, tx failed")
                })?;
            return_chan_rx.await.map_err(|e| {
                anyhow!(e).context("Failed to initialize new connection with handshake, rx failed")
            })?;
        }

        if let Some(use_message) = &self.use_message {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            outbound
                .send(use_message.clone(), return_chan_tx)
                .map_err(|e| {
                    anyhow!(e)
                        .context("Failed to initialize new connection with use message, tx failed")
                })?;
            return_chan_rx.await.map_err(|e| {
                anyhow!(e)
                    .context("Failed to initialize new connection with use message, rx failed")
            })?;
        }

        Ok(outbound)
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

    pub fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }

    pub fn get_version(&mut self) -> Result<Version> {
        for message in &mut self.init_handshake {
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                return Ok(frame.version());
            }
        }
        Err(anyhow!(
            "connection version could not be retrieved from the handshake because none of the {} messages in the handshake could be parsed",
            self.init_handshake.len()
        ))
    }
}
