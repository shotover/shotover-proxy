use super::super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::message::{Message, Messages};
use crate::tls::TlsConnector;
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
    pub init_handshake: Vec<Message>,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
}

impl ConnectionFactory {
    pub fn new(tls: Option<TlsConnector>) -> Self {
        Self {
            init_handshake: vec![],
            tls,
            pushed_messages_tx: None,
        }
    }

    // When you want to clone from the transform.
    // you don't want state specific to this connection but you need the config options
    pub fn clone_transfrom(&self) -> Self {
        Self {
            init_handshake: vec![],
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

        Ok(outbound)
    }
}
