use crate::sources::cassandra::{CassandraConfig, CassandraSource};
use crate::sources::kafka::{KafkaConfig, KafkaSource};
use crate::sources::redis::{RedisConfig, RedisSource};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::Result;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub mod cassandra;
pub mod kafka;
pub mod redis;

#[derive(Debug)]
pub enum Sources {
    Cassandra(CassandraSource),
    Redis(RedisSource),
    Kafka(KafkaSource),
}

impl Sources {
    pub fn into_join_handle(self) -> JoinHandle<()> {
        match self {
            Sources::Cassandra(c) => c.join_handle,
            Sources::Redis(r) => r.join_handle,
            Sources::Kafka(r) => r.join_handle,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub enum SourcesConfig {
    Cassandra(CassandraConfig),
    Redis(RedisConfig),
    Kafka(KafkaConfig),
}

impl SourcesConfig {
    pub(crate) async fn get_source(
        &self,
        chain_builder: TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        match self {
            SourcesConfig::Cassandra(c) => c.get_source(chain_builder, trigger_shutdown_rx).await,
            SourcesConfig::Redis(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
            SourcesConfig::Kafka(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
        }
    }
}
