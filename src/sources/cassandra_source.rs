use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::config::topology::TopicHolder;
use crate::server::TcpCodecListener;
use crate::sources::{Sources, SourcesFromConfig};
use crate::transforms::chain::TransformChain;
use shotover_protocols::cassandra_protocol2::CassandraCodec2;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CassandraConfig {
    pub listen_addr: String,
    pub cassandra_ks: HashMap<String, Vec<String>>,
    pub bypass_query_processing: Option<bool>,
    pub connection_limit: Option<usize>,
    pub hard_connection_limit: Option<bool>,
}

#[async_trait]
impl SourcesFromConfig for CassandraConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        Ok(vec![Sources::Cassandra(
            CassandraSource::new(
                chain,
                self.listen_addr.clone(),
                self.cassandra_ks.clone(),
                notify_shutdown,
                shutdown_complete_tx,
                self.bypass_query_processing.unwrap_or(true),
                self.connection_limit,
                self.hard_connection_limit,
            )
            .await,
        )])
    }
}

#[derive(Debug)]
pub struct CassandraSource {
    pub name: &'static str,
    pub join_handle: JoinHandle<Result<()>>,
    pub listen_addr: String,
}

impl CassandraSource {
    //"127.0.0.1:9043
    pub async fn new(
        chain: &TransformChain,
        listen_addr: String,
        cassandra_ks: HashMap<String, Vec<String>>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        bypass: bool,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
    ) -> CassandraSource {
        // let listener = TcpListener::bind(listen_addr.clone()).await.unwrap();
        let name = "Cassandra Source";

        info!("Starting Cassandra source on [{}]", listen_addr);

        let mut listener = TcpCodecListener {
            chain: chain.clone(),
            source_name: name.to_string(),
            listener: None,
            listen_addr: listen_addr.clone(),
            hard_connection_limit: hard_connection_limit.unwrap_or(false),
            codec: CassandraCodec2::new(cassandra_ks, bypass),
            limit_connections: Arc::new(Semaphore::new(connection_limit.unwrap_or(512))),
            notify_shutdown,
            shutdown_complete_tx,
        };

        let jh = Handle::current().spawn(async move {
            tokio::select! {
                res = listener.run() => {
                    if let Err(err) = res {
                        error!(cause = %err, "failed to accept");
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal received - shutting down")
                }
            }

            // listener.run().await?;

            let TcpCodecListener {
                notify_shutdown,
                shutdown_complete_tx,
                ..
            } = listener;

            drop(shutdown_complete_tx);
            drop(notify_shutdown);

            Ok(())
        });

        CassandraSource {
            name,
            join_handle: jh,
            listen_addr,
        }
    }
}
