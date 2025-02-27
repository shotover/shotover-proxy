use crate::codec::{CodecBuilder, Direction, kafka::KafkaCodecBuilder};
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
pub struct KafkaConfig {
    pub name: String,
    pub listen_addr: String,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
    pub tls: Option<TlsAcceptorConfig>,
    pub timeout: Option<u64>,
    pub chain: TransformChainConfig,
}

impl KafkaConfig {
    pub async fn get_source(
        &self,
        trigger_shutdown_rx: watch::Receiver<bool>,
    ) -> Result<Source, Vec<String>> {
        Ok(Source::Kafka(
            KafkaSource::new(
                self.name.clone(),
                &self.chain,
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
pub struct KafkaSource {
    pub join_handle: JoinHandle<()>,
}

impl KafkaSource {
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
    ) -> Result<KafkaSource, Vec<String>> {
        info!("Starting Kafka source on [{}]", listen_addr);

        let mut listener = TcpCodecListener::new(
            chain_config,
            name.to_string(),
            listen_addr.clone(),
            hard_connection_limit.unwrap_or(false),
            KafkaCodecBuilder::new(Direction::Source, name),
            Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            trigger_shutdown_rx.clone(),
            tls.as_ref().map(TlsAcceptor::new).transpose()?,
            timeout.map(Duration::from_secs),
            Transport::Tcp,
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

        Ok(KafkaSource { join_handle })
    }
}
