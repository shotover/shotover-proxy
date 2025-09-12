use crate::codec::{CodecBuilder, Direction, valkey::ValkeyCodecBuilder};
use crate::config::chain::TransformChainConfig;
use crate::hot_reload::protocol::SocketInfo;
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
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
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        hot_reload_sockets: Option<&HashMap<u32, SocketInfo>>,
    ) -> Result<Source, Vec<String>> {
        info!("Starting Valkey source on [{}]", self.listen_addr);

        let (hot_reload_tx, hot_reload_rx) = tokio::sync::mpsc::unbounded_channel();

        // Extract port and find matching hot reload socket
        let port = self
            .listen_addr
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse::<u32>().ok())
            .unwrap_or(0);
        let hot_reload_socket_info = hot_reload_sockets.and_then(|sockets| sockets.get(&port));

        let mut listener = TcpCodecListener::new(
            &self.chain,
            self.name.clone(),
            self.listen_addr.clone(),
            self.hard_connection_limit.unwrap_or(false),
            ValkeyCodecBuilder::new(Direction::Source, self.name.clone()),
            Arc::new(Semaphore::new(self.connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            self.tls.as_ref().map(TlsAcceptor::new).transpose()?,
            self.timeout.map(Duration::from_secs),
            Transport::Tcp,
            hot_reload_rx,
            hot_reload_socket_info,
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

        Ok(Source::new(join_handle, hot_reload_tx, self.name.clone()))
    }
}
