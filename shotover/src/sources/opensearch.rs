use crate::codec::{CodecBuilder, Direction, opensearch::OpenSearchCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug)]
pub struct OpenSearchConfig {
    pub name: String,
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub timeout: Option<u64>,
    pub chain: TransformChainConfig,
}

impl OpenSearchConfig {
    pub async fn get_source(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source, Vec<String>> {
        Ok(Source::OpenSearch(
            OpenSearchSource::new(
                self.name.clone(),
                &self.chain,
                self.listen_addr.clone(),
                trigger_shutdown_rx,
                self.connection_limit,
                self.hard_connection_limit,
                self.timeout,
            )
            .await?,
        ))
    }
}

#[derive(Debug)]
pub struct OpenSearchSource {
    pub join_handle: JoinHandle<()>,
}

impl OpenSearchSource {
    pub async fn new(
        name: String,
        chain_config: &TransformChainConfig,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        timeout: Option<u64>,
    ) -> Result<Self, Vec<String>> {
        info!("Starting OpenSearch source on [{}]", listen_addr);

        let mut listener = TcpCodecListener::new(
            chain_config,
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            OpenSearchCodecBuilder::new(Direction::Source, name),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            None,
            timeout.map(Duration::from_secs),
            Transport::Tcp,
        )
        .await?;

        let join_handle = tokio::spawn(async move {
            // Check we didn't receive a shutdown signal before the receiver was created
            if !*trigger_shutdown_rx.borrow() {
                tokio::select! {
                    res = listener.run() => {
                        if let Err(err) = res {
                            error!(cause = %err, "failed to accept");
                        }
                    }
                    _ = trigger_shutdown_rx.changed() => {
                        listener.shutdown().await;
                    }
                }
            }
        });

        Ok(Self { join_handle })
    }
}
