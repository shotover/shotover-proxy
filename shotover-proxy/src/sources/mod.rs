use crate::config::topology::TopicHolder;
use crate::sources::cassandra_source::{CassandraConfig, CassandraSource};
use crate::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use crate::sources::redis_source::{RedisConfig, RedisSource};
use crate::transforms::chain::TransformChain;
use anyhow::Result;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub mod cassandra_source;
pub mod mpsc_source;
pub mod redis_source;

#[derive(Debug)]
pub enum Sources {
    Cassandra(CassandraSource),
    Mpsc(AsyncMpsc),
    Redis(RedisSource),
}

impl Sources {
    pub fn into_join_handle(self) -> JoinHandle<Result<()>> {
        match self {
            Sources::Cassandra(c) => c.join_handle,
            Sources::Mpsc(m) => m.rx_handle,
            Sources::Redis(r) => r.join_handle,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub enum SourcesConfig {
    Cassandra(CassandraConfig),
    Mpsc(AsyncMpscConfig),
    Redis(RedisConfig),
}

impl SourcesConfig {
    pub(crate) async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        match self {
            SourcesConfig::Cassandra(c) => c.get_source(chain, topics, trigger_shutdown_rx).await,
            SourcesConfig::Mpsc(m) => m.get_source(chain, topics, trigger_shutdown_rx).await,
            SourcesConfig::Redis(r) => r.get_source(chain, topics, trigger_shutdown_rx).await,
        }
    }
}
