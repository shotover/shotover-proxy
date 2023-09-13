use crate::codec::{redis::RedisCodecBuilder, CodecBuilder, Direction};
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
}

impl RedisConfig {
    pub async fn get_source(
        &self,
        chain_builder: TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source> {
        Ok(Source::Redis(
            RedisSource::new(
                chain_builder,
                self.listen_addr.clone(),
                trigger_shutdown_rx,
                self.connection_limit,
                self.hard_connection_limit,
                self.tls.clone(),
                self.timeout,
            )
            .await?,
        ))
    }
}

#[derive(Debug)]
pub struct RedisSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<()>,
    pub listen_addr: String,
}

impl RedisSource {
    pub async fn new(
        chain_builder: TransformChainBuilder,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsAcceptorConfig>,
        timeout: Option<u64>,
    ) -> Result<RedisSource> {
        info!("Starting Redis source on [{}]", listen_addr);
        let name = "RedisSource";

        let mut listener = TcpCodecListener::new(
            chain_builder,
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            RedisCodecBuilder::new(Direction::Source),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.map(TlsAcceptor::new).transpose()?,
            timeout,
            Transport::Tcp,
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

        Ok(RedisSource {
            name,
            join_handle,
            listen_addr,
        })
    }
}
