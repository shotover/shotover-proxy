//! Sources used to listen for connections and send/recieve with the client.

use crate::hot_reload::protocol::{HotReloadListenerRequest, SocketInfo};
#[cfg(feature = "cassandra")]
use crate::sources::cassandra::CassandraConfig;
#[cfg(feature = "kafka")]
use crate::sources::kafka::KafkaConfig;
#[cfg(feature = "opensearch")]
use crate::sources::opensearch::OpenSearchConfig;
#[cfg(feature = "valkey")]
use crate::sources::valkey::ValkeyConfig;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(feature = "valkey")]
pub mod valkey;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub enum Transport {
    Tcp,
    WebSocket,
}

#[derive(Debug)]
pub struct Source {
    pub join_handle: JoinHandle<()>,
    pub hot_reload_tx: UnboundedSender<HotReloadListenerRequest>,
    pub name: String,
}

impl Source {
    pub fn new(
        join_handle: JoinHandle<()>,
        hot_reload_tx: UnboundedSender<HotReloadListenerRequest>,
        name: String,
    ) -> Self {
        Self {
            join_handle,
            hot_reload_tx,
            name,
        }
    }

    pub fn into_join_handle(self) -> JoinHandle<()> {
        self.join_handle
    }
    pub fn get_hot_reload_tx(&self) -> UnboundedSender<HotReloadListenerRequest> {
        self.hot_reload_tx.clone()
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum SourceConfig {
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraConfig),
    #[cfg(feature = "valkey")]
    Valkey(ValkeyConfig),
    #[cfg(feature = "kafka")]
    Kafka(KafkaConfig),
    #[cfg(feature = "opensearch")]
    OpenSearch(OpenSearchConfig),
}

impl SourceConfig {
    pub(crate) async fn get_source(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_sockets: Option<&HashMap<u32, SocketInfo>>,
    ) -> Result<Source, Vec<String>> {
        match self {
            #[cfg(feature = "cassandra")]
            SourceConfig::Cassandra(c) => c.get_source(trigger_shutdown_rx, hot_reload_sockets).await,
            #[cfg(feature = "valkey")]
            SourceConfig::Valkey(r) => r.get_source(trigger_shutdown_rx, hot_reload_sockets).await,
            #[cfg(feature = "kafka")]
            SourceConfig::Kafka(r) => r.get_source(trigger_shutdown_rx, hot_reload_sockets).await,
            #[cfg(feature = "opensearch")]
            SourceConfig::OpenSearch(r) => r.get_source(trigger_shutdown_rx, hot_reload_sockets).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            #[cfg(feature = "cassandra")]
            SourceConfig::Cassandra(c) => &c.name,
            #[cfg(feature = "valkey")]
            SourceConfig::Valkey(r) => &r.name,
            #[cfg(feature = "kafka")]
            SourceConfig::Kafka(r) => &r.name,
            #[cfg(feature = "opensearch")]
            SourceConfig::OpenSearch(r) => &r.name,
        }
    }
}
