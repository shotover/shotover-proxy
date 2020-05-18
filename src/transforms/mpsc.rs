use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use tokio::sync::mpsc::{Sender, Receiver, channel};

use async_trait::async_trait;
use crate::message::{Message, QueryResponse};
use crate::transforms::{Transforms, TransformsFromConfig};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use crate::config::ConfigError;


/*
AsyncMPSC Tees and Forwarders should only be created from the AsyncMpsc struct,
It's the thing that owns tx and rx handles :D
 */

#[derive(Debug, Clone)]
pub struct AsyncMpscForwarder {
    pub name: &'static str,
    pub tx: Sender<Message>,
}


#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AsyncMpscForwarderConfig {
}

#[async_trait]
impl TransformsFromConfig for AsyncMpscForwarderConfig {
    async fn get_source(&self, transforms: &HashMap<String, TransformChain, RandomState>) -> Result<Transforms, ConfigError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct AsyncMpscTee {
    pub name: &'static str,
    pub tx: Sender<Message>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AsyncMpscTeeConfig {
}

#[async_trait]
impl TransformsFromConfig for AsyncMpscTeeConfig {
    async fn get_source(&self, transforms: &HashMap<String, TransformChain>) -> Result<Transforms, ConfigError> {
        unimplemented!()
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
