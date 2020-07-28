use crate::config::topology::TopicHolder;
use crate::sources::cassandra_source::{CassandraConfig, CassandraSource};
use crate::sources::mpsc_source::{AsyncMpsc, AsyncMpscConfig};
use crate::transforms::chain::{TransformChain};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use crate::sources::redis_source::{RedisSource, RedisConfig};
use tokio::sync::{broadcast, mpsc};

use anyhow::{Result};

pub mod cassandra_source;
pub mod mpsc_source;
pub mod redis_source;

/*
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
 */

#[derive(Debug)]
pub enum Sources {
    Cassandra(CassandraSource),
    Mpsc(AsyncMpsc),
    Redis(RedisSource)
}

impl Sources {
    pub fn get_join_handles<T>(&self) -> &JoinHandle<Result<()>> {
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
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Sources> {
        match self {
            SourcesConfig::Cassandra(c) => c.get_source(chain, topics, notify_shutdown, shutdown_complete_tx).await,
            SourcesConfig::Mpsc(m) => m.get_source(chain, topics, notify_shutdown,  shutdown_complete_tx).await,
            SourcesConfig::Redis(r) => r.get_source(chain, topics,  notify_shutdown, shutdown_complete_tx).await
        }
    }
}

#[async_trait]
pub trait SourcesFromConfig: Send {
    async fn get_source(
        &self,
        chain: &TransformChain,
        topics: &mut TopicHolder,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Sources>;


}
