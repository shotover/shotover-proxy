use crate::transforms::chain::{TransformChain};


use crate::config::topology::TopicHolder;
use crate::protocols::cassandra_protocol2::CassandraCodec2;
use crate::sources::{Sources, SourcesFromConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;
use std::collections::HashMap;
use tokio::net::{TcpListener};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use crate::server::{TcpCodecListener};
use tokio::sync::{broadcast, mpsc, Semaphore};
use std::sync::Arc;

use anyhow::{Result};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct CassandraConfig {
    pub listen_addr: String,
    pub cassandra_ks: HashMap<String, Vec<String>>,
}

#[async_trait]
impl SourcesFromConfig for CassandraConfig {
    async fn get_source(
        &self,
        chain: &TransformChain,
        _topics: &mut TopicHolder,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Result<Sources> {
        Ok(Sources::Cassandra(CassandraSource::new(
            chain,
            self.listen_addr.clone(),
            self.cassandra_ks.clone(),
            notify_shutdown,
            shutdown_complete_tx,
        ).await))
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
    ) -> CassandraSource {
        let listener = TcpListener::bind(listen_addr.clone()).await.unwrap();

        info!("Starting Cassandra source on [{}]", listen_addr);

        let mut listener = TcpCodecListener {
            chain: chain.clone(),
            listener,
            codec: CassandraCodec2::new(cassandra_ks),
            limit_connections: Arc::new(Semaphore::new(50)),
            notify_shutdown,
            shutdown_complete_tx
        };

        let jh = Handle::current().spawn(async move {
            listener.run().await
        });

        return CassandraSource {
            name: "Cassandra Source",
            join_handle: jh,
            listen_addr,
        }
    }
}
