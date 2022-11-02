use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::tls::{ApplicationProtocol, TlsConnector, TlsConnectorConfig};
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesOrdered;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkSingleConfig {
    #[serde(rename = "remote_address")]
    pub address: String,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkSingleConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self
            .tls
            .clone()
            .map(|c| TlsConnector::new(c, ApplicationProtocol::Cassandra))
            .transpose()?;
        Ok(Transforms::CassandraSinkSingle(CassandraSinkSingle::new(
            self.address.clone(),
            chain_name,
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkSingle {
    address: String,
    outbound: Option<CassandraConnection>,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
}

impl Clone for CassandraSinkSingle {
    fn clone(&self) -> Self {
        CassandraSinkSingle {
            address: self.address.clone(),
            outbound: None,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
        }
    }
}

impl CassandraSinkSingle {
    pub fn new(
        address: String,
        chain_name: String,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkSingle {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkSingle");
        let receive_timeout = timeout.map(Duration::from_secs);

        CassandraSinkSingle {
            address,
            outbound: None,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
        }
    }
}

impl CassandraSinkSingle {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        if self.outbound.is_none() {
            trace!("creating outbound connection {:?}", self.address);
            self.outbound = Some(
                CassandraConnection::new(
                    self.address.clone(),
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
        }
        trace!("sending frame upstream");

        let outbound = self.outbound.as_mut().unwrap();
        let responses_future: Result<FuturesOrdered<oneshot::Receiver<Response>>> = messages
            .into_iter()
            .map(|m| {
                let (return_chan_tx, return_chan_rx) = oneshot::channel();
                outbound.send(m, return_chan_tx)?;

                Ok(return_chan_rx)
            })
            .collect();

        super::connection::receive(self.read_timeout, &self.failed_requests, responses_future?)
            .await
    }
}

#[async_trait]
impl Transform for CassandraSinkSingle {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}
