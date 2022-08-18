use super::super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::message::{Message, Messages};
use crate::tls::TlsConnector;
use anyhow::Result;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
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
    init_handshake: Arc<RwLock<Vec<Message>>>,
    use_message: Arc<RwLock<Option<Message>>>,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
}

impl ConnectionFactory {
    pub fn new(tls: Option<TlsConnector>) -> Self {
        Self {
            init_handshake: Arc::new(RwLock::new(vec![])),
            use_message: Arc::new(RwLock::new(None)),
            tls,
            pushed_messages_tx: None,
        }
    }

    // When you want to clone from the transform.
    // you don't want state specific to this connection but you need the config options
    pub fn clone_transfrom(&self) -> Self {
        let use_message = self.use_message.read().unwrap().clone();

        Self {
            init_handshake: Arc::new(RwLock::new(vec![])),
            tls: self.tls.clone(),
            use_message: Arc::new(RwLock::new(use_message)),
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

        let handshake_messages = self.init_handshake.read().unwrap().clone();

        for handshake_message in handshake_messages {
            let (return_chan_tx, return_chan_rx) = oneshot::channel();
            outbound.send(handshake_message.clone(), return_chan_tx)?;
            return_chan_rx.await?;
        }

        Ok(outbound)
    }

    pub fn push_handshake_message(&self, message: Message) {
        // TODO should accept array
        let write = self.init_handshake.write().unwrap();
        write.push(message);
    }

    pub fn add_use_message(&self, message: Message) {
        let mut write = self.use_message.write().unwrap();
        *write = Some(message);
    }

    pub fn pop_use_message(&self) {
        let mut write = self.use_message.write().unwrap();
        *write = None;
    }

    pub fn has_use_message(&self) -> bool {
        let read = self.use_message.read().unwrap();
        read.is_some()
    }
}
