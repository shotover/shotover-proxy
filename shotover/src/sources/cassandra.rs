use crate::codec::Direction;
use crate::codec::{cassandra::CassandraCodecBuilder, CodecBuilder};
use crate::server::TcpCodecListener;
use crate::sources::{Source, Transport};
use crate::tls::{TlsAcceptor, TlsAcceptorConfig};
use crate::transforms::chain::TransformChainBuilder;
use crate::transforms::{SourceBuilder, TransformBuilder, TransformConfig};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CassandraSourceConfig {
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
    pub transport: Option<Transport>,
}

#[typetag::deserialize(name = "CassandraSource")]
#[async_trait(?Send)]
impl TransformConfig for CassandraSourceConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<TransformBuilder> {
        Ok(TransformBuilder::Source(Box::new(self.clone())))
    }
}

#[async_trait(?Send)]
impl SourceBuilder for CassandraSourceConfig {
    async fn build(
        &self,
        chain_builder: TransformChainBuilder,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source> {
        Ok(Source::Cassandra(
            CassandraSource::new(
                chain_builder,
                self.listen_addr.clone(),
                trigger_shutdown_rx,
                self.connection_limit,
                self.hard_connection_limit,
                self.tls.clone(),
                self.timeout,
                self.transport,
            )
            .await?,
        ))
    }

    fn get_name(&self) -> &'static str {
        "CassandraSource"
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
        chain_builder: TransformChainBuilder,
        listen_addr: String,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
        tls: Option<TlsAcceptorConfig>,
        timeout: Option<u64>,
        transport: Option<Transport>,
    ) -> Result<Self> {
        let name = "CassandraSource";

        info!("Starting Cassandra source on [{}]", listen_addr);

        let mut listener = TcpCodecListener::new(
            chain_builder,
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            CassandraCodecBuilder::new(Direction::Source),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.map(TlsAcceptor::new).transpose()?,
            timeout,
            transport.unwrap_or(Transport::Tcp),
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

        Ok(Self {
            name,
            join_handle,
            listen_addr,
        })
    }
}
