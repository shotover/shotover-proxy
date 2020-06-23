use crate::transforms::chain::{TransformChain};


use tracing::info;
use tokio::net::{TcpListener};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use crate::protocols::redis_codec::RedisCodec;
use crate::sources::{Sources, SourcesFromConfig};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use crate::config::topology::TopicHolder;
use crate::server::{TcpCodecListener};
use tokio::sync::{broadcast, mpsc, Semaphore};
use std::sync::Arc;

use anyhow::{Result};


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RedisConfig {
    pub listen_addr: String,
}

#[async_trait]
impl SourcesFromConfig for RedisConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Sources> {
        Ok(Sources::Redis(RedisSource::new(
            chain,
            self.listen_addr.clone(),
            notify_shutdown,
            shutdown_complete_tx,
        ).await))
    }
}

#[derive(Debug)]
pub struct RedisSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<()>>,
    pub listen_addr: String,
}

impl RedisSource {
    //"127.0.0.1:9043
    pub async fn new(
        chain: &TransformChain,
        listen_addr: String,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> RedisSource {
        let listener = TcpListener::bind(listen_addr.clone()).await.unwrap();

        info!("Starting Redis source on [{}]", listen_addr);

        let mut listener = TcpCodecListener {
            chain: chain.clone(),
            listener,
            codec: RedisCodec::new(false),
            limit_connections: Arc::new(Semaphore::new(50)),
            notify_shutdown,
            shutdown_complete_tx
        };

        let jh = Handle::current().spawn(async move {
            listener.run().await;

            let TcpCodecListener {
                notify_shutdown,
                shutdown_complete_tx,
                    ..
            } = listener;

            drop(shutdown_complete_tx);
            drop(notify_shutdown);

            // let _ shutd

            Ok(())
        });

        RedisSource {
            name: "Redis",
            join_handle: jh,
            listen_addr: listen_addr.clone(),
        }
    }
}
