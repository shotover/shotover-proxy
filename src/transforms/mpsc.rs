use tokio::sync::mpsc::Sender;

use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::error::ChainResponse;
use crate::message::{Message, Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::TryFutureExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::warn;

/*
AsyncMPSC Tees and Forwarders should only be created from the AsyncMpsc struct,
It's the thing that owns tx and rx handles :D
 */

#[derive(Debug, Clone)]
pub struct Buffer {
    pub name: &'static str,
    pub tx: Sender<ChannelMessage>,
    pub async_mode: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct BufferConfig {
    pub topic_name: String,
    pub async_mode: bool,
}

#[async_trait]
impl TransformsFromConfig for BufferConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCForwarder(Buffer {
                name: "forward",
                tx,
                async_mode: self.async_mode,
            }));
        }
        Err(anyhow!(
            "Could not find the topic {} in [{:#?}]",
            self.topic_name.clone(),
            &topics.topics_rx.keys()
        ))
    }
}

#[async_trait]
impl Transform for Buffer {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        if self.async_mode {
            let expected_responses = qd.message.messages.len();
            self.tx
                .send(ChannelMessage::new_with_no_return(qd.message))
                .map_err(|e| {
                    warn!("MPSC error {}", e);
                    e
                })
                .await?;
            ChainResponse::Ok(Messages {
                messages: (0..expected_responses)
                    .into_iter()
                    .map(|_| Message::new_response(QueryResponse::empty(), true, RawFrame::NONE))
                    .collect_vec(),
            })
        } else {
            let (tx, rx) = oneshot::channel::<ChainResponse>();
            self.tx
                .send(ChannelMessage::new(qd.message, tx))
                .map_err(|e| {
                    warn!("MPSC error {}", e);
                    e
                })
                .await?;
            return rx.await?;
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[derive(Debug, Clone)]
pub struct Tee {
    pub name: &'static str,
    pub tx: Sender<ChannelMessage>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TeeConfig {
    pub topic_name: String,
}
#[async_trait]
impl TransformsFromConfig for TeeConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCTee(Tee { name: "tee", tx }));
        }
        Err(anyhow!(
            "Could not find the topic {} in [{:#?}]",
            &self.topic_name,
            topics.topics_rx.keys()
        ))
    }
}

#[async_trait]
impl Transform for Tee {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let m = qd.message.clone();
        self.tx
            .send(ChannelMessage::new_with_no_return(m))
            .map_err(|e| {
                warn!("MPSC error {}", e);
                e
            })
            .await?;
        qd.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
