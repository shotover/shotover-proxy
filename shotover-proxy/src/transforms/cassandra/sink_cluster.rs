use super::connection::CassandraConnection;
use crate::codec::cassandra::CassandraCodec;
use crate::concurrency::FuturesOrdered;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::util::Response;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::trace;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraSinkClusterConfig {
    pub first_contact_points: Vec<String>,
    pub data_center: String,
    pub tls: Option<TlsConnectorConfig>,
    pub read_timeout: Option<u64>,
}

impl CassandraSinkClusterConfig {
    pub async fn get_transform(&self, chain_name: String) -> Result<Transforms> {
        let tls = self.tls.clone().map(TlsConnector::new).transpose()?;
        Ok(Transforms::CassandraSinkCluster(CassandraSinkCluster::new(
            self.first_contact_points.clone(),
            chain_name,
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    outbound: Option<CassandraConnection>,
    chain_name: String,
    failed_requests: Counter,
    tls: Option<TlsConnector>,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            outbound: None,
            chain_name: self.chain_name.clone(),
            tls: self.tls.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
        }
    }
}

impl CassandraSinkCluster {
    pub fn new(
        contact_points: Vec<String>,
        chain_name: String,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkCluster {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        CassandraSinkCluster {
            contact_points,
            outbound: None,
            chain_name,
            failed_requests,
            tls,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        if self.outbound.is_none() {
            trace!("creating outbound connection {:?}", self.contact_points);
            self.outbound = Some(
                CassandraConnection::new(
                    self.contact_points[0].clone(),
                    CassandraCodec::new(),
                    self.tls.clone(),
                    self.pushed_messages_tx.clone(),
                )
                .await?,
            );
        }

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
impl Transform for CassandraSinkCluster {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.send_message(message_wrapper.messages).await
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn add_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        self.pushed_messages_tx = Some(pushed_messages_tx);
    }
}
