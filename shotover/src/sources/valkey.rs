use crate::codec::{CodecBuilder, Direction, valkey::ValkeyCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::hot_reload::protocol::GradualShutdownRequest;
use crate::source_task::SourceTask;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ValkeySourceConfig {
    pub name: String,
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
    pub chain: TransformChainConfig,
}

impl ValkeySourceConfig {
    pub async fn build(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_listeners: &mut HashMap<u16, TcpListener>,
    ) -> Result<Source, Vec<String>> {
        info!("Starting Valkey source on [{}]", self.listen_addr);

        let (hot_reload_tx, hot_reload_rx) = tokio::sync::mpsc::unbounded_channel();
        let (gradual_shutdown_tx, gradual_shutdown_rx) =
            tokio::sync::mpsc::unbounded_channel::<GradualShutdownRequest>();

        let join_handle = SourceTask::start(
            &self.chain,
            self.name.clone(),
            self.listen_addr.clone(),
            self.hard_connection_limit.unwrap_or(false),
            ValkeyCodecBuilder::new(Direction::Source, self.name.clone()),
            Arc::new(Semaphore::new(self.connection_limit.unwrap_or(512))),
            self.tls.as_ref().map(TlsAcceptor::new).transpose()?,
            self.timeout.map(Duration::from_secs),
            Transport::Tcp,
            hot_reload_rx,
            hot_reload_listeners,
            trigger_shutdown_rx,
            gradual_shutdown_rx,
        )
        .await?;

        Ok(Source::new(
            join_handle,
            hot_reload_tx,
            gradual_shutdown_tx,
            self.name.clone(),
        ))
    }
}
