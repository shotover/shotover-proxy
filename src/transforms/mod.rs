#![feature(box_syntax, box_patterns)]

use crate::config::topology::TopicHolder;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::cassandra_codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::lua::LuaFilterTransform;
use crate::transforms::test_transforms::{ReturnerTransform, RandomDelayTransform};
use crate::transforms::mpsc::{
    AsyncMpscForwarder, AsyncMpscForwarderConfig, AsyncMpscTee, AsyncMpscTeeConfig,
};
use crate::transforms::null::Null;
use crate::transforms::printer::Printer;
use crate::transforms::protect::Protect;
use crate::transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::route::{Route, RouteConfig};
use crate::transforms::scatter::{Scatter, ScatterConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::transforms::redis_codec_destination::{RedisCodecDestination, RedisCodecConfiguration};
use crate::error::{ChainResponse};
use anyhow::{Result};
use std::fmt::Debug;
use serde::export::Formatter;
use core::fmt;
use crate::transforms::tuneable_consistency_scatter::{TuneableConsistency, TuneableConsistencyConfig};
use crate::transforms::response_unifier::{ResponseUnifier, ResponseUnifierConfig};

pub mod chain;
pub mod cassandra_codec_destination;
pub mod redis_codec_destination;
pub mod kafka_destination;
pub mod lua;
pub mod mpsc;
pub mod noop;
pub mod null;
pub mod printer;
pub mod protect;
pub mod query;
pub mod redis_cache;
pub mod route;
pub mod scatter;
pub mod test_transforms;
pub mod tuneable_consistency_scatter;
mod response_unifier;


#[derive(Clone)]
pub enum Transforms {
    CodecDestination(CodecDestination),
    RedisCodecDestination(RedisCodecDestination),
    KafkaDestination(KafkaDestination),
    RedisCache(SimpleRedisCache),
    MPSCTee(AsyncMpscTee),
    MPSCForwarder(AsyncMpscForwarder),
    Route(Route),
    Scatter(Scatter),
    Printer(Printer),
    Null(Null),
    Lua(LuaFilterTransform),
    Protect(Protect),
    TuneableConsistency(TuneableConsistency),
    ResponseUnifier(ResponseUnifier),
    // The below variants are mainly for testing
    RepeatMessage(Box<ReturnerTransform>),
    RandomDelay(RandomDelayTransform)
}

impl Debug for Transforms {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transform: {}", self.get_name())
    }
}

unsafe impl Send for Transforms {}
unsafe impl Sync for Transforms {}

#[async_trait]
impl Transform for Transforms {

    async fn prep_transform_chain(& mut self, t: &mut TransformChain) -> Result<()> {
        match self {
            Transforms::CodecDestination(a) => a.prep_transform_chain(t).await,
            Transforms::RedisCodecDestination(a) => a.prep_transform_chain(t).await,
            Transforms::KafkaDestination(a) => a.prep_transform_chain(t).await,
            Transforms::RedisCache(a) => a.prep_transform_chain(t).await,
            Transforms::MPSCTee(a) => a.prep_transform_chain(t).await,
            Transforms::MPSCForwarder(a) => a.prep_transform_chain(t).await,
            Transforms::Route(a) => a.prep_transform_chain(t).await,
            Transforms::Scatter(a) => a.prep_transform_chain(t).await,
            Transforms::Printer(a) => a.prep_transform_chain(t).await,
            Transforms::Null(a) => a.prep_transform_chain(t).await,
            Transforms::Lua(a) => a.prep_transform_chain(t).await,
            Transforms::Protect(a) => a.prep_transform_chain(t).await,
            Transforms::TuneableConsistency(a) => a.prep_transform_chain(t).await,
            Transforms::ResponseUnifier(a) => a.prep_transform_chain(t).await,
            Transforms::RepeatMessage(a) => a.prep_transform_chain(t).await,
            Transforms::RandomDelay(a) => a.prep_transform_chain(t).await,
        }
    }
    
    
    
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        match self {
            Transforms::CodecDestination(c) => c.transform(qd, t).await,
            Transforms::KafkaDestination(k) => k.transform(qd, t).await,
            Transforms::RedisCache(r) => r.transform(qd, t).await,
            Transforms::MPSCTee(m) => m.transform(qd, t).await,
            Transforms::MPSCForwarder(m) => m.transform(qd, t).await,
            Transforms::Route(r) => r.transform(qd, t).await,
            Transforms::Scatter(s) => s.transform(qd, t).await,
            Transforms::Printer(p) => p.transform(qd, t).await,
            Transforms::Null(n) => n.transform(qd, t).await,
            Transforms::Lua(l) => l.transform(qd, t).await,
            Transforms::Protect(p) => p.transform(qd, t).await,
            Transforms::RepeatMessage(p) => p.transform(qd, t).await,
            Transforms::RandomDelay(p) => p.transform(qd, t).await,
            Transforms::TuneableConsistency(tc) => tc.transform(qd, t).await,
            Transforms::RedisCodecDestination(r) => r.transform(qd, t).await,
            Transforms::ResponseUnifier(r) => r.transform(qd, t).await
        }
    }

    fn get_name(&self) -> &'static str {
        match self {
            Transforms::CodecDestination(c) => c.get_name(),
            Transforms::KafkaDestination(k) => k.get_name(),
            Transforms::RedisCache(r) => r.get_name(),
            Transforms::MPSCTee(m) => m.get_name(),
            Transforms::MPSCForwarder(m) => m.get_name(),
            Transforms::Route(r) => r.get_name(),
            Transforms::Scatter(s) => s.get_name(),
            Transforms::Printer(p) => p.get_name(),
            Transforms::Null(n) => n.get_name(),
            Transforms::Lua(l) => l.get_name(),
            Transforms::Protect(p) => p.get_name(),
            Transforms::TuneableConsistency(t) => t.get_name(),
            Transforms::RepeatMessage(p) => p.get_name(),
            Transforms::RandomDelay(p) => p.get_name(),
            Transforms::RedisCodecDestination(r) => r.get_name(),
            Transforms::ResponseUnifier(r) => r.get_name()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TransformsConfig {
    CodecDestination(CodecConfiguration),
    RedisDestination(RedisCodecConfiguration),
    KafkaDestination(KafkaConfig),
    RedisCache(RedisConfig),
    MPSCTee(AsyncMpscTeeConfig),
    MPSCForwarder(AsyncMpscForwarderConfig),
    Route(RouteConfig),
    ConsistentScatter(TuneableConsistencyConfig),
    ResponseResolver(ResponseUnifierConfig),
    Scatter(ScatterConfig),
}

impl TransformsConfig {
    pub async fn get_transforms(
        &self,
        topics: &TopicHolder,
    ) -> Result<Transforms> {
        match self {
            TransformsConfig::CodecDestination(c) => c.get_source(topics).await,
            TransformsConfig::KafkaDestination(k) => k.get_source(topics).await,
            TransformsConfig::RedisCache(r) => r.get_source(topics).await,
            TransformsConfig::MPSCTee(t) => t.get_source(topics).await,
            TransformsConfig::MPSCForwarder(f) => f.get_source(topics).await,
            TransformsConfig::Route(r) => r.get_source(topics).await,
            TransformsConfig::Scatter(s) => s.get_source(topics).await,
            TransformsConfig::RedisDestination(r) => r.get_source(topics).await,
            TransformsConfig::ConsistentScatter(c) => c.get_source(topics).await,
            TransformsConfig::ResponseResolver(r) => r.get_source(topics).await,
        }
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &[TransformsConfig],
    topics: &TopicHolder,
) -> Result<TransformChain> {
    let mut transforms: Vec<Transforms> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_transforms(topics).await?)
    }
    return Ok(TransformChain::new(transforms, name, topics.get_global_map_handle(),  topics.get_global_tx()));
}

#[async_trait]
pub trait TransformsFromConfig: Send {
    async fn get_source(
        &self,
        topics: &TopicHolder,
    ) -> Result<Transforms>;
}
