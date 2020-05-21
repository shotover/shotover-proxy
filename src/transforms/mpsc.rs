use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio::sync::mpsc::{Sender};

use async_trait::async_trait;
use crate::message::{Message, QueryResponse};
use crate::transforms::{Transforms, TransformsFromConfig};
use serde::{Serialize, Deserialize};
use crate::config::ConfigError;
use crate::config::topology::TopicHolder;
use slog::Logger;



/*
AsyncMPSC Tees and Forwarders should only be created from the AsyncMpsc struct,
It's the thing that owns tx and rx handles :D
 */

#[derive(Debug, Clone)]
pub struct AsyncMpscForwarder {
    pub name: &'static str,
    pub tx: Sender<Message>,
    logger: Logger
}


#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscForwarderConfig {
    pub topic_name: String
}

#[async_trait]
impl TransformsFromConfig for AsyncMpscForwarderConfig {
    async fn get_source(&self, topics: &TopicHolder, logger: &Logger) -> Result<Transforms, ConfigError> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCForwarder(AsyncMpscForwarder{
                name: "forward",
                tx,
                logger: logger.clone()
            }));
        }
        Err(ConfigError::new(format!("Could not find the topic {} in [{:#?}]", self.topic_name.clone(), &topics.topics_rx.keys()).as_str()))
    }
}

#[derive(Debug, Clone)]
pub struct AsyncMpscTee {
    pub name: &'static str,
    pub tx: Sender<Message>,
    logger: Logger
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AsyncMpscTeeConfig {
    pub topic_name: String
}
#[async_trait]
impl TransformsFromConfig for AsyncMpscTeeConfig {
    async fn get_source(&self, topics: &TopicHolder, logger: &Logger) -> Result<Transforms, ConfigError> {
        if let Some(tx) = topics.get_tx(&self.topic_name) {
            return Ok(Transforms::MPSCTee(AsyncMpscTee{
                name: "tee",
                tx,
                logger: logger.clone()
            }));
        }
        Err(ConfigError::new(format!("Could not find the topic {} in [{:#?}]", &self.topic_name, topics.topics_rx.keys()).as_str()))
    }
}

#[async_trait]
impl Transform for AsyncMpscForwarder {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
        self.tx.clone().send(qd.message).await;
        return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}


#[async_trait]
impl Transform for AsyncMpscTee {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
        let m = qd.message.clone();
        self.tx.clone().send(m).await;
        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
