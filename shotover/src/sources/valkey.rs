use crate::codec::{CodecBuilder, Direction, valkey::ValkeyCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ValkeyConfig {
    pub name: String,
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
    pub chain: TransformChainConfig,
}

impl ValkeyConfig {
    pub async fn get_source(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_channel_manager: Option<&mut crate::hot_reload::HotReloadChannelManager>,
    ) -> Result<Source, Vec<String>> {
        Ok(Source::Valkey(
            ValkeySource::new(
                self.name.clone(),
                &self.chain,
                self.listen_addr.clone(),
                trigger_shutdown_rx,
                self.connection_limit,
                self.hard_connection_limit,
                self.tls.clone(),
                self.timeout,
                hot_reload_channel_manager,
            )
            .await?,
        ))
    }
}

#[derive(Debug)]
pub struct ValkeySource {
    pub join_handle: JoinHandle<()>,
}

impl ValkeySource {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        name: String,
        chain_config: &TransformChainConfig,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsAcceptorConfig>,
        timeout: Option<u64>,
        hot_reload_channel_manager: Option<&mut crate::hot_reload::HotReloadChannelManager>,
    ) -> Result<ValkeySource, Vec<String>> {
        info!("Starting Valkey source on [{}]", listen_addr);
        let hot_reload_rx = if let Some(manager) = hot_reload_channel_manager {
            Some(manager.create_channel_for_source(name.clone()))
        } else {
            None
        };

        let mut listener = TcpCodecListener::new(
            chain_config,
            name.clone(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            ValkeyCodecBuilder::new(Direction::Source, name),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.as_ref().map(TlsAcceptor::new).transpose()?,
            timeout.map(Duration::from_secs),
            Transport::Tcp,
            hot_reload_rx,
        )
        .await?;

        let join_handle = tokio::spawn(async move {
            // Check we didn't receive a shutdown signal before the receiver was created
            if !*trigger_shutdown_rx.borrow() {
                tokio::select! {
                    res = listener.run() => {
                        if let Err(err) = res {
                            error!(cause = %err, "failed to accept connection");
                        }
                    }
                    _ = trigger_shutdown_rx.changed() => {
                        listener.shutdown().await;
                    }
                }
            }
        });

        Ok(ValkeySource { join_handle })
    }
}
