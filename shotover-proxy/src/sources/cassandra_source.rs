use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::config::topology::TopicHolder;
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use crate::server::TcpCodecListener;
use crate::sources::{Sources, SourcesFromConfig};
use crate::transforms::chain::TransformChain;

#[derive(Deserialize, Debug, Clone)]
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
        trigger_shutdown_rx: watch::Receiver<bool>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Vec<Sources>> {
        Ok(vec![Sources::Cassandra(
            CassandraSource::new(
                chain,
                self.listen_addr.clone(),
                self.cassandra_ks.clone(),
                trigger_shutdown_rx,
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
    #![allow(clippy::too_many_arguments)]
    pub async fn new(
        chain: &TransformChain,
        listen_addr: String,
        cassandra_ks: HashMap<String, Vec<String>>,
        mut trigger_shutdown_rx: watch::Receiver<bool>,
        shutdown_complete_tx: mpsc::Sender<()>,
        bypass: bool,
        connection_limit: Option<usize>,
        hard_connection_limit: Option<bool>,
    ) -> CassandraSource {
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
            trigger_shutdown_rx: trigger_shutdown_rx.clone(),
            shutdown_complete_tx,
            tls: None,
        };

        let join_handle = Handle::current().spawn(async move {
            // Check we didn't receive a shutdown signal before the receiver was created
            if !*trigger_shutdown_rx.borrow() {
                tokio::select! {
                    res = listener.run() => {
                        if let Err(err) = res {
                            error!(cause = %err, "failed to accept");
                        }
                    }
                    _ = trigger_shutdown_rx.changed() => {
                        info!("cassandra source shutting down")
                    }
                }
            }

            let TcpCodecListener {
                shutdown_complete_tx,
                ..
            } = listener;

            drop(shutdown_complete_tx);

            Ok(())
        });

        CassandraSource {
            name,
            join_handle,
            listen_addr,
        }
    }
}
