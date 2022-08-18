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
        handshake: &[Message],
        tls: &Option<TlsConnector>,
        pushed_messages_tx: &Option<mpsc::UnboundedSender<Messages>>,
    ) -> Result<&mut CassandraConnection> {
        if self.outbound.is_none() {
            self.outbound = Some(
                new_connection((self.address, 9042), handshake, tls, pushed_messages_tx).await?,
            )
        }

        Ok(self.outbound.as_mut().unwrap())
    }
}

pub async fn new_connection<A: ToSocketAddrs>(
    address: A,
    handshake: &[Message],
    tls: &Option<TlsConnector>,
    pushed_messages_tx: &Option<mpsc::UnboundedSender<Messages>>,
) -> Result<CassandraConnection> {
    let outbound = CassandraConnection::new(
        address,
        CassandraCodec::new(),
        tls.clone(),
        pushed_messages_tx.clone(),
    )
    .await?;

    for handshake_message in handshake {
        let (return_chan_tx, return_chan_rx) = oneshot::channel();
        outbound.send(handshake_message.clone(), return_chan_tx)?;
        return_chan_rx.await?;
    }

    Ok(outbound)
}
