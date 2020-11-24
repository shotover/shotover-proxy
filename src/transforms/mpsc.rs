use tokio::sync::mpsc::Sender;

use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::error::ChainResponse;
use crate::message::{Message, Messages, QueryResponse, Value};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::TryFutureExt;
use itertools::Itertools;
use metrics::counter;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{trace, warn};

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
    pub fail_topic: Option<Sender<ChannelMessage>>,
    pub behavior: ConsistencyBehavior,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum ConsistencyBehavior {
    IGNORE,
    FAIL,
    LOG { topic: String },
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TeeConfig {
    pub topic_name: String,
    pub behavior: Option<ConsistencyBehavior>,
}
#[async_trait]
impl TransformsFromConfig for TeeConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            let fail_topic = if let Some(ConsistencyBehavior::LOG { topic }) = &self.behavior {
                let topic_chan = topics.get_tx(topic);
                if topic_chan.is_none() {
                    return Err(anyhow!(
                        "Could not find the fail topic {} in [{:#?}]",
                        topic,
                        topics.topics_rx.keys()
                    ));
                }
                topic_chan
            } else {
                None
            };

            return Ok(Transforms::MPSCTee(Tee {
                name: "tee",
                tx,
                fail_topic,
                behavior: self.behavior.clone().unwrap_or(ConsistencyBehavior::IGNORE),
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
impl Transform for Tee {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let m = qd.message.clone();
        return match self.behavior {
            ConsistencyBehavior::IGNORE => {
                let _ = self
                    .tx
                    .try_send(ChannelMessage::new_with_no_return(m))
                    .map_err(|e| {
                        counter!("tee_dropped_messages", 1, "chain" => self.name);
                        trace!("MPSC error {}", e);
                        e
                    });
                qd.call_next_transform().await
            }
            ConsistencyBehavior::FAIL => {
                let (tx, rx) = oneshot::channel::<ChainResponse>();
                self.tx
                    .send(ChannelMessage::new(m, tx))
                    .map_err(|e| {
                        warn!("MPSC error {}", e);
                        e
                    })
                    .await?;

                let (tee_result, chain_result) = tokio::join!(rx, qd.call_next_transform());
                let tee_response = tee_result??;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    Ok(Messages::new_single_response(
                        QueryResponse::empty_with_error(Some(Value::Strings(
                            "Shotover could not write to both topics via Tee - Behavior is to fail"
                                .to_string(),
                        ))),
                        true,
                        RawFrame::NONE,
                    ))
                } else {
                    Ok(chain_response)
                }
            }
            ConsistencyBehavior::LOG { .. } => {
                let failed_message = m.clone();
                let (tx, rx) = oneshot::channel::<ChainResponse>();
                self.tx
                    .send(ChannelMessage::new(m, tx))
                    .map_err(|e| {
                        // counter!("tee_logged_messages", 1, "chain" => self.name);
                        warn!("MPSC error {}", e);
                        e
                    })
                    .await?;

                let (tee_result, chain_result) = tokio::join!(rx, qd.call_next_transform());
                let tee_response = tee_result??;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    if let Some(topic) = &mut self.fail_topic {
                        topic
                            .send(ChannelMessage::new_with_no_return(failed_message))
                            .map_err(|e| {
                                warn!("MPSC error for logging failed Tee message {}", e);
                                e
                            })
                            .await?;
                    }
                }

                Ok(chain_response)
            }
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
