use crate::codec::redis::RedisCodec;
use crate::config::topology::TopicHolder;
use crate::server::TcpCodecListener;
use crate::sources::Sources;
use crate::tls::{TlsAcceptor, TlsConfig};
use crate::transforms::chain::TransformChain;
use anyhow::Result;
use serde::Deserialize;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsConfig>,
}

impl RedisConfig {
    pub async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        RedisSource::new(
            chain,
            self.listen_addr.clone(),
            trigger_shutdown_rx,
            self.connection_limit,
            self.hard_connection_limit,
            self.tls.clone(),
        )
        .await
        .map(|x| vec![Sources::Redis(x)])
    }
}

#[derive(Debug)]
pub struct RedisSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<()>>,
    pub listen_addr: String,
}

impl RedisSource {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain: &TransformChain,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsConfig>,
    ) -> Result<RedisSource> {
        info!("Starting Redis source on [{}]", listen_addr);
        let name = "RedisSource";

        let mut listener = TcpCodecListener::new(
            chain.clone(),
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            RedisCodec::new(),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.map(TlsAcceptor::new).transpose()?,
        )
        .await?;

        let join_handle = Handle::current().spawn(async move {
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

            Ok(())
        });

        Ok(RedisSource {
            name,
            join_handle,
            listen_addr,
        })
    }
}
