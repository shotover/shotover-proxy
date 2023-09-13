//! Sources used to listen for connections and send/recieve with the client.

use crate::sources::cassandra::{CassandraConfig, CassandraSource};
use crate::sources::kafka::{KafkaConfig, KafkaSource};
use crate::sources::opensearch::{OpenSearchConfig, OpenSearchSource};
use crate::sources::redis::{RedisConfig, RedisSource};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub mod cassandra;
pub mod kafka;
pub mod opensearch;
pub mod redis;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub enum Transport {
    Tcp,
    WebSocket,
}

#[derive(Debug)]
pub enum Source {
    Cassandra(CassandraSource),
    Redis(RedisSource),
    Kafka(KafkaSource),
    OpenSearch(OpenSearchSource),
}

impl Source {
    pub fn into_join_handle(self) -> JoinHandle<()> {
        match self {
            Source::Cassandra(c) => c.join_handle,
            Source::Redis(r) => r.join_handle,
            Source::Kafka(r) => r.join_handle,
            Source::OpenSearch(o) => o.join_handle,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum SourceConfig {
    Cassandra(CassandraConfig),
    Redis(RedisConfig),
    Kafka(KafkaConfig),
    OpenSearch(OpenSearchConfig),
}

impl SourceConfig {
    pub(crate) async fn get_source(
        &self,
        chain_builder: TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source> {
        match self {
            SourceConfig::Cassandra(c) => c.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::Redis(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::Kafka(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::OpenSearch(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
        }
    }
}
