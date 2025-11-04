use crate::codec::{CodecBuilder, Direction, opensearch::OpenSearchCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::hot_reload::protocol::GradualShutdownRequest;
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
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
    pub async fn build(
        &self,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_listeners: &mut HashMap<u16, TcpListener>,
    ) -> Result<Source, Vec<String>> {
        info!("Starting OpenSearch source on [{}]", self.listen_addr);

        let (hot_reload_tx, hot_reload_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gradual_shutdown_tx, mut gradual_shutdown_rx) =
            tokio::sync::mpsc::unbounded_channel::<GradualShutdownRequest>();

        let mut listener = TcpCodecListener::new(
            &self.chain,
            self.name.clone(),
            self.listen_addr.clone(),
            self.hard_connection_limit.unwrap_or(false),
            OpenSearchCodecBuilder::new(Direction::Source, self.name.clone()),
            Arc::new(Semaphore::new(self.connection_limit.unwrap_or(512))),
            None,
            self.timeout.map(Duration::from_secs),
            Transport::Tcp,
            hot_reload_rx,
            hot_reload_listeners,
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
                    Some(GradualShutdownRequest) = gradual_shutdown_rx.recv() => {
                        listener.gradual_shutdown().await;
                    }
                }
            }
        });

        Ok(Source::new(
            join_handle,
            hot_reload_tx,
            gradual_shutdown_tx,
            self.name.clone(),
        ))
    }
}
