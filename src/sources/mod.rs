use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::sources::cassandra_source::{CassandraConfig, CassandraSource};
use crate::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use crate::transforms::chain::TransformChain;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::error::Error;
use tokio::task::JoinHandle;
use crate::sources::redis_source::{RedisSource, RedisConfig};

pub mod cassandra_source;
pub mod mpsc_source;
pub mod redis_source;

pub enum Sources {
    Cassandra(CassandraSource),
    Mpsc(AsyncMpsc),
    Redis(RedisSource)
}

impl Sources {
    pub fn get_join_handles<T>(&self) -> &JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        match self {
            Sources::Cassandra(c) => &c.join_handle,
            Sources::Mpsc(m) => &m.rx_handle,
            Sources::Redis(r) => &r.join_handle,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SourcesConfig {
    Cassandra(CassandraConfig),
    Mpsc(AsyncMpscConfig),
    Redis(RedisConfig)
}

impl SourcesConfig {
    pub(crate) async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        logger: &Logger,
    ) -> Result<Sources, ConfigError> {
        match self {
            SourcesConfig::Cassandra(c) => c.get_source(chain, topics, logger).await,
            SourcesConfig::Mpsc(m) => m.get_source(chain, topics, logger).await,
            SourcesConfig::Redis(r) => r.get_source(chain, topics, logger).await
        }
    }
}

#[async_trait]
pub trait SourcesFromConfig: Send + Sync {
    async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        logger: &Logger,
    ) -> Result<Sources, ConfigError>;
}
