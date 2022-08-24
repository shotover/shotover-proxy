use crate::codec::cassandra::CassandraCodec;
use crate::message::{Message, Messages};
use crate::tls::TlsConnector;
use crate::transforms::cassandra::connection::CassandraConnection;
use anyhow::Result;
use std::net::IpAddr;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct CassandraNode {
    pub address: IpAddr,
    pub _rack: String,
    pub _tokens: Vec<String>,
    pub outbound: Option<CassandraConnection>,
}

impl CassandraNode {
    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
    ) -> Result<&mut CassandraConnection> {
        if self.outbound.is_none() {
            self.outbound = Some(
                connection_factory
                    .new_connection((self.address, 9042))
                    .await?,
            )
        }

        Ok(self.outbound.as_mut().unwrap())
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionFactory {
    init_handshake: Vec<Message>,
    use_message: Option<Message>,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
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

    pub fn clone_config(&self) -> Self {
        Self {
            init_handshake: vec![],
            use_message: None,
            tls: self.tls.clone(),
            pushed_messages_tx: None,
        }
    }

    pub async fn new_connection<A: ToSocketAddrs>(
        &self,
        address: A,
    ) -> Result<CassandraConnection> {
        let outbound = CassandraConnection::new(
            address,
            CassandraCodec::new(),
            self.tls.clone(),
            self.pushed_messages_tx.clone(),
        )
        .await?;

        for handshake_message in &self.init_handshake {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            outbound.send(handshake_message.clone(), return_chan_tx)?;
            return_chan_rx.await?;
        }

        if let Some(use_message) = &self.use_message {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            outbound.send(use_message.clone(), return_chan_tx)?;
            return_chan_rx.await?;
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
}
