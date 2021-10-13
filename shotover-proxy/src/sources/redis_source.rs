use crate::config::topology::TopicHolder;
use crate::protocols::redis_codec::{DecodeType, RedisCodec};
use crate::server::TcpCodecListener;
use crate::sources::{Sources, SourcesFromConfig};
use crate::tls::{TlsAcceptor, TlsConfig};
use crate::transforms::chain::TransformChain;
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub listen_addr: String,
    pub batch_size_hint: u64,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsConfig>,
}

#[async_trait]
impl SourcesFromConfig for RedisConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        trigger_shutdown_rx: watch::Receiver<bool>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        RedisSource::new(
            chain,
            self.listen_addr.clone(),
            self.batch_size_hint,
            trigger_shutdown_rx,
            shutdown_complete_tx,
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
        batch_hint: u64,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        shutdown_complete_tx: mpsc::Sender<()>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsConfig>,
    ) -> Result<RedisSource> {
        info!("Starting Redis source on [{}]", listen_addr);
        let name = "Redis Source";

        let mut listener = TcpCodecListener {
            chain: chain.clone(),
            source_name: name.to_string(),
            listener: None,
            listen_addr: listen_addr.clone(),
            hard_connection_limit: hard_connection_limit.unwrap_or(false),
            codec: RedisCodec::new(DecodeType::Query, batch_hint as usize),
            limit_connections: Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx: trigger_shutdown_rx.clone(),
            shutdown_complete_tx,
            tls: tls.map(TlsAcceptor::new).transpose()?,
        };

        let join_handle = Handle::current().spawn(async move {
            // Check we didn't receive a shutdown signal before the receiver was created
            if !*trigger_shutdown_rx.borrow() {
                tokio::select! {
                    res = listener.run() => {
                        if let Err(err) = res {
                            error!(cause = %err, "failed to accept connection");
                        }
                    }
                    _ =  trigger_shutdown_rx.changed() => {
                        info!("redis source shutting down")
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
