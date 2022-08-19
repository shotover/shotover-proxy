use crate::error::ChainResponse;
use crate::message::Messages;
use crate::tls::{TlsConnector, TlsConnectorConfig};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use cql3_parser::common::FQName;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::mpsc;

pub mod node;
pub mod node_pool;

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
            self.data_center.clone(),
            tls,
            self.read_timeout,
        )))
    }
}

pub struct CassandraSinkCluster {
    contact_points: Vec<String>,
    node_pool: node_pool::NodePool,
    chain_name: String,
    failed_requests: Counter,
    pushed_messages_tx: Option<mpsc::UnboundedSender<Messages>>,
    read_timeout: Option<Duration>,
    peer_table: FQName,
    data_center: String,
}

impl Clone for CassandraSinkCluster {
    fn clone(&self) -> Self {
        CassandraSinkCluster {
            contact_points: self.contact_points.clone(),
            node_pool: node_pool::NodePool::new(
                self.contact_points.clone(),
                self.data_center.clone(),
                None,
            ),
            chain_name: self.chain_name.clone(),
            failed_requests: self.failed_requests.clone(),
            pushed_messages_tx: None,
            read_timeout: self.read_timeout,
            peer_table: self.peer_table.clone(),
            data_center: self.data_center.clone(),
        }
    }
}

impl CassandraSinkCluster {
    pub fn new(
        contact_points: Vec<String>,
        chain_name: String,
        data_center: String,
        tls: Option<TlsConnector>,
        timeout: Option<u64>,
    ) -> CassandraSinkCluster {
        let failed_requests = register_counter!("failed_requests", "chain" => chain_name.clone(), "transform" => "CassandraSinkCluster");
        let receive_timeout = timeout.map(Duration::from_secs);

        let node_pool = node_pool::NodePool::new(contact_points.clone(), data_center.clone(), tls);

        CassandraSinkCluster {
            contact_points,
            node_pool,
            chain_name,
            failed_requests,
            pushed_messages_tx: None,
            read_timeout: receive_timeout,
            peer_table: FQName::new("system", "peers"),
            data_center,
        }
    }
}

impl CassandraSinkCluster {
    async fn send_message(&mut self, messages: Messages) -> ChainResponse {
        let responses = self.node_pool.send_messages(messages).await?;
        Ok(responses)
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
