use crate::codec::Direction;
use crate::codec::{CodecBuilder, cassandra::CassandraCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tracing::{error, info};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CassandraConfig {
    pub name: String,
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
    pub transport: Option<Transport>,
    pub chain: TransformChainConfig,
}

impl CassandraConfig {
    pub async fn get_source(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source, Vec<String>> {
        self.get_source_with_listener(trigger_shutdown_rx, &mut HashMap::new())
            .await
    }

    pub async fn get_source_with_listener(
        &self,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_listeners: &mut HashMap<u16, TcpListener>,
    ) -> Result<Source, Vec<String>> {
        info!("Starting Cassandra source on [{}]", self.listen_addr);

        let (hot_reload_tx, hot_reload_rx) = tokio::sync::mpsc::unbounded_channel();

        // Check if we have a hot reload listener for this port
        let port = self
            .listen_addr
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u16>().ok());

        let hot_reload_listener = port.and_then(|p| hot_reload_listeners.remove(&p));

        if hot_reload_listener.is_some() {
            info!(
                "Using hot reloaded listener for Cassandra source on [{}]",
                self.listen_addr
            );
        }

        let mut listener = TcpCodecListener::new_with_listener(
            &self.chain,
            self.name.clone(),
            self.listen_addr.clone(),
            self.hard_connection_limit.unwrap_or(false),
            CassandraCodecBuilder::new(Direction::Source, self.name.clone()),
            Arc::new(Semaphore::new(self.connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            self.tls.as_ref().map(TlsAcceptor::new).transpose()?,
            self.timeout.map(Duration::from_secs),
            self.transport.unwrap_or(Transport::Tcp),
            hot_reload_rx,
            hot_reload_listener,
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

        Ok(Source::new(join_handle, hot_reload_tx, self.name.clone()))
    }
}
