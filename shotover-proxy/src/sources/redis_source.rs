use crate::transforms::chain::TransformChain;

use crate::config::topology::TopicHolder;
use crate::protocols::redis_codec::RedisCodec;
use crate::server::TcpCodecListener;
use crate::sources::{Sources, SourcesFromConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

use anyhow::Result;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    pub listen_addr: String,
    pub batch_size_hint: u64,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
}

#[async_trait]
impl SourcesFromConfig for RedisConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        trigger_shutdown_tx: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        Ok(vec![Sources::Redis(
            RedisSource::new(
                chain,
                self.listen_addr.clone(),
                self.batch_size_hint,
                trigger_shutdown_tx,
                shutdown_complete_tx,
                self.connection_limit,
                self.hard_connection_limit,
            )
            .await,
        )])
    }
}

#[derive(Debug)]
pub struct RedisSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<()>>,
    pub listen_addr: String,
}

impl RedisSource {
    pub async fn new(
        chain: &TransformChain,
        listen_addr: String,
        batch_hint: u64,
        trigger_shutdown_tx: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
    ) -> RedisSource {
        info!("Starting Redis source on [{}]", listen_addr);
        let name = "Redis Source";

        let mut trigger_shutdown_rx = trigger_shutdown_tx.subscribe();

        let mut listener = TcpCodecListener {
            chain: chain.clone(),
            source_name: name.to_string(),
            listener: None,
            listen_addr: listen_addr.clone(),
            hard_connection_limit: hard_connection_limit.unwrap_or(false),
            codec: RedisCodec::new(false, batch_hint as usize),
            limit_connections: Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_tx,
            shutdown_complete_tx,
        };

        let join_handle = Handle::current().spawn(async move {
            tokio::select! {
                res = listener.run() => {
                    if let Err(err) = res {
                        error!(cause = %err, "failed to accept");
                    }
                }
                _ = trigger_shutdown_rx.recv() => {
                    info!("redis source shutting down")
                }
            }

            let TcpCodecListener {
                trigger_shutdown_tx,
                shutdown_complete_tx,
                ..
            } = listener;

            drop(shutdown_complete_tx);
            drop(trigger_shutdown_tx);

            Ok(())
        });

        RedisSource {
            name,
            join_handle,
            listen_addr,
        }
    }
}
