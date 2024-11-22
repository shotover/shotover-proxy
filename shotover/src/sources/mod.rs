//! Sources used to listen for connections and send/recieve with the client.

#[cfg(feature = "cassandra")]
use crate::sources::cassandra::{CassandraConfig, CassandraSource};
#[cfg(feature = "kafka")]
use crate::sources::kafka::{KafkaConfig, KafkaSource};
#[cfg(feature = "opensearch")]
use crate::sources::opensearch::{OpenSearchConfig, OpenSearchSource};
#[cfg(feature = "redis")]
use crate::sources::valkey::{ValkeyConfig, ValkeySource};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(feature = "redis")]
pub mod valkey;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub enum Transport {
    Tcp,
    WebSocket,
}

#[derive(Debug)]
pub enum Source {
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraSource),
    #[cfg(feature = "redis")]
    Valkey(ValkeySource),
    #[cfg(feature = "kafka")]
    Kafka(KafkaSource),
    #[cfg(feature = "opensearch")]
    OpenSearch(OpenSearchSource),
}

impl Source {
    pub fn into_join_handle(self) -> JoinHandle<()> {
        match self {
            #[cfg(feature = "cassandra")]
            Source::Cassandra(c) => c.join_handle,
            #[cfg(feature = "redis")]
            Source::Valkey(r) => r.join_handle,
            #[cfg(feature = "kafka")]
            Source::Kafka(r) => r.join_handle,
            #[cfg(feature = "opensearch")]
            Source::OpenSearch(o) => o.join_handle,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum SourceConfig {
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraConfig),
    #[cfg(feature = "redis")]
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
    ) -> Result<Source, Vec<String>> {
        match self {
            #[cfg(feature = "cassandra")]
            SourceConfig::Cassandra(c) => c.get_source(trigger_shutdown_rx).await,
            #[cfg(feature = "redis")]
            SourceConfig::Valkey(r) => r.get_source(trigger_shutdown_rx).await,
            #[cfg(feature = "kafka")]
            SourceConfig::Kafka(r) => r.get_source(trigger_shutdown_rx).await,
            #[cfg(feature = "opensearch")]
            SourceConfig::OpenSearch(r) => r.get_source(trigger_shutdown_rx).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            #[cfg(feature = "cassandra")]
            SourceConfig::Cassandra(c) => &c.name,
            #[cfg(feature = "redis")]
            SourceConfig::Valkey(r) => &r.name,
            #[cfg(feature = "kafka")]
            SourceConfig::Kafka(r) => &r.name,
            #[cfg(feature = "opensearch")]
            SourceConfig::OpenSearch(r) => &r.name,
        }
    }
}
