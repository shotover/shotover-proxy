use crate::sources::cassandra_source::{CassandraSource, CassandraConfig};
use crate::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use async_trait::async_trait;
use crate::transforms::chain::TransformChain;
use serde::{Serialize, Deserialize};
use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use tokio::task::JoinHandle;
use std::error::Error;
use slog::Logger;

pub mod cassandra_source;
pub mod mpsc_source;


pub enum Sources {
    Cassandra(CassandraSource),
    Mpsc(AsyncMpsc)
}

impl Sources {
    pub fn get_join_handles<T>(&self) -> &JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        match self {
            Sources::Cassandra(c) => {&c.join_handle},
            Sources::Mpsc(m) => {&m.rx_handle},
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SourcesConfig {
    Cassandra(CassandraConfig),
    Mpsc(AsyncMpscConfig)
}

impl SourcesConfig {
    pub(crate) async fn get_source(&self, chain: &TransformChain, topics: &mut TopicHolder, logger: &Logger) -> Result<Sources, ConfigError> {
        match self {
            SourcesConfig::Cassandra(c) => {
                c.get_source(chain, topics, logger).await
            },
            SourcesConfig::Mpsc(m) => {
                m.get_source(chain, topics, logger).await
            },
        }
    }
}


#[async_trait]
pub trait SourcesFromConfig: Send + Sync {
    async fn get_source(&self, chain: &TransformChain, topics: &mut TopicHolder, logger: &Logger) -> Result<Sources, ConfigError>;
}
