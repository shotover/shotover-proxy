use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use tokio::sync::mpsc::Sender;

use crate::config::topology::TopicHolder;
use crate::message::{Message, QueryResponse};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;
use futures::TryFutureExt;
use crate::error::{ChainResponse};
use anyhow::{anyhow, Result};

/*
AsyncMPSC Tees and Forwarders should only be created from the AsyncMpsc struct,
It's the thing that owns tx and rx handles :D
 */

#[derive(Debug, Clone)]
pub struct AsyncMpscForwarder {
    pub name: &'static str,
    pub tx: Sender<Message>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscForwarderConfig {
    pub topic_name: String,
}

#[async_trait]
impl TransformsFromConfig for AsyncMpscForwarderConfig {
    async fn get_source(
        &self,
        topics: &TopicHolder,
    ) -> Result<Transforms> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCForwarder(AsyncMpscForwarder {
                name: "forward",
                tx,
            }));
        }
        Err(anyhow!(
                "Could not find the topic {} in [{:#?}]",
                self.topic_name.clone(),
                &topics.topics_rx.keys()
        ))
    }
}

#[derive(Debug, Clone)]
pub struct AsyncMpscTee {
    pub name: &'static str,
    pub tx: Sender<Message>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscTeeConfig {
    pub topic_name: String,
}
#[async_trait]
impl TransformsFromConfig for AsyncMpscTeeConfig {
    async fn get_source(
        &self,
        topics: &TopicHolder,
    ) -> Result<Transforms> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCTee(AsyncMpscTee {
                name: "tee",
                tx,
            }));
        }
        Err(anyhow!(
                "Could not find the topic {} in [{:#?}]",
                &self.topic_name,
                topics.topics_rx.keys()
        ))
    }
}

#[async_trait]
impl Transform for AsyncMpscForwarder {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        self.tx.clone().send(qd.message).map_err(|e| {
            warn!("MPSC error {}", e);
            e
        }).await?;
        return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[async_trait]
impl Transform for AsyncMpscTee {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let m = qd.message.clone();
        self.tx.clone().send(m).map_err(|e| {
            warn!("MPSC error {}", e);
            e
        }).await?;
        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
