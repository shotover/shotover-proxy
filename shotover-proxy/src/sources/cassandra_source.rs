use crate::codec::cassandra::CassandraCodecBuilder;
use crate::server::TcpCodecListener;
use crate::sources::Sources;
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use crate::transforms::chain::TransformChainBuilder;
use anyhow::Result;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraConfig {
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
}

impl CassandraConfig {
    pub async fn get_source(
        &self,
        chain: &TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Vec<Sources>> {
        Ok(vec![Sources::Cassandra(
            CassandraSource::new(
                chain,
                self.listen_addr.clone(),
                trigger_shutdown_rx,
                self.connection_limit,
                self.hard_connection_limit,
                self.tls.clone(),
                self.timeout,
            )
            .await?,
        )])
    }
}

#[derive(Debug)]
pub struct CassandraSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<()>,
    pub listen_addr: String,
}

impl CassandraSource {
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain: &TransformChainBuilder,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsAcceptorConfig>,
        timeout: Option<u64>,
    ) -> Result<CassandraSource> {
        let name = "CassandraSource";

        info!("Starting Cassandra source on [{}]", listen_addr);

        let mut listener = TcpCodecListener::new(
            chain.clone(),
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            CassandraCodecBuilder::new(),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.map(TlsAcceptor::new).transpose()?,
            timeout,
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

        Ok(CassandraSource {
            name,
            join_handle,
            listen_addr,
        })
    }
}
