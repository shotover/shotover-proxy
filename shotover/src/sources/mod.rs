use crate::sources::cassandra::{CassandraConfig, CassandraSource};
use crate::sources::kafka::{KafkaConfig, KafkaSource};
use crate::sources::raw::{RawConfig, RawSource};
use crate::sources::redis::{RedisConfig, RedisSource};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::Result;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub mod cassandra;
pub mod kafka;
pub mod raw;
pub mod redis;

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum Transport {
    Tcp,
    WebSocket,
}

#[derive(Debug)]
pub enum Source {
    Cassandra(CassandraSource),
    Redis(RedisSource),
    Kafka(KafkaSource),
    Raw(RawSource),
}

impl Source {
    pub fn into_join_handle(self) -> JoinHandle<()> {
        match self {
            Source::Cassandra(c) => c.join_handle,
            Source::Redis(r) => r.join_handle,
            Source::Kafka(k) => k.join_handle,
            Source::Raw(r) => r.join_handle,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub enum SourceConfig {
    Cassandra(CassandraConfig),
    Redis(RedisConfig),
    Kafka(KafkaConfig),
    Raw(RawConfig),
}

impl SourceConfig {
    pub(crate) async fn get_source(
        &self,
        chain_builder: TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Source>> {
        match self {
            SourceConfig::Cassandra(c) => c.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::Redis(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::Kafka(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
            SourceConfig::Raw(r) => r.get_source(chain_builder, trigger_shutdown_rx).await,
        }
    }
}
