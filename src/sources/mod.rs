use crate::sources::cassandra_source::{CassandraSource, CassandraConfig};
use crate::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use async_trait::async_trait;
use crate::transforms::chain::TransformChain;
use serde::{Serialize, Deserialize};

pub mod cassandra_source;
pub mod mpsc_source;


pub enum Sources {
    Cassandra(CassandraSource),
    Mpsc(AsyncMpsc)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SourcesConfig {
    Cassandra(CassandraConfig),
    Mpsc(AsyncMpscConfig)
}

impl SourcesConfig {
    async fn get_source(&self, chain: TransformChain) -> Sources {
        match self {
            SourcesConfig::Cassandra(c) => {
                c.get_source(chain).await
            },
            SourcesConfig::Mpsc(m) => {
                m.get_source(chain).await
            },
        }
    }
}


#[async_trait]
pub trait SourcesFromConfig: Send + Sync {
    async fn get_source(&self, chain: TransformChain) -> Sources;
}
