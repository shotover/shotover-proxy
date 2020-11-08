use core::fmt;
use std::fmt::Debug;

use anyhow::Result;
use async_trait::async_trait;
use serde::export::Formatter;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use metrics::{counter, timing};

use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::cassandra_codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::chain::TransformChain;
use crate::transforms::distributed::tunable_consistency_scatter::{
    TunableConsistency, TunableConsistencyConfig,
};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::load_balance::{ConnectionBalanceAndPool, ConnectionBalanceAndPoolConfig};
use crate::transforms::lua::LuaFilterTransform;
use crate::transforms::mpsc::{Buffer, BufferConfig, Tee, TeeConfig};
use crate::transforms::null::Null;
use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
use crate::transforms::printer::Printer;
use crate::transforms::protect::Protect;
use crate::transforms::redis_transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::redis_transforms::redis_cluster::{RedisCluster, RedisClusterConfig};
use crate::transforms::redis_transforms::redis_codec_destination::{
    RedisCodecConfiguration, RedisCodecDestination,
};
use crate::transforms::redis_transforms::timestamp_tagging::RedisTimestampTagger;
use crate::transforms::sequential_map::{SequentialMap, SequentialMapConfig};
use crate::transforms::test_transforms::{RandomDelayTransform, ReturnerTransform};
use core::fmt::Display;
use distributed::route::{Route, RouteConfig};
use distributed::scatter::{Scatter, ScatterConfig};
use mlua::UserData;
use tokio::time::Instant;
use tracing::span;
use tracing::{Level, Span};
use tracing_futures::Instrument;

pub mod cassandra_codec_destination;
pub mod chain;
pub mod distributed;
pub mod kafka_destination;
pub mod load_balance;
pub mod lua;
pub mod mpsc;
pub mod noop;
pub mod null;
mod parallel_map;
pub mod printer;
pub mod protect;
pub mod redis_transforms;
pub mod sampler;
pub mod sequential_map;
pub mod test_transforms;

//TODO Generate the trait implementation for this passthrough enum via a macro

#[derive(Clone)]
pub enum Transforms {
    CodecDestination(CodecDestination),
    RedisCodecDestination(RedisCodecDestination),
    KafkaDestination(KafkaDestination),
    RedisCache(SimpleRedisCache),
    MPSCTee(Tee),
    MPSCForwarder(Buffer),
    Route(Route),
    Scatter(Scatter),
    Null(Null),
    Lua(LuaFilterTransform),
    Protect(Protect),
    TunableConsistency(TunableConsistency),
    RedisTimeStampTagger(RedisTimestampTagger),
    RedisCluster(RedisCluster),
    // The below variants are mainly for testing
    RepeatMessage(Box<ReturnerTransform>),
    RandomDelay(RandomDelayTransform),
    Printer(Printer),
    SequentialMap(SequentialMap),
    ParallelMap(ParallelMap),
    PoolConnections(ConnectionBalanceAndPool),
}

impl Debug for Transforms {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transform: {}", self.get_name())
    }
}

#[async_trait]
impl Transform for Transforms {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        match self {
            Transforms::CodecDestination(c) => c.instrumented_transform(qd),
            Transforms::KafkaDestination(k) => k.instrumented_transform(qd),
            Transforms::RedisCache(r) => r.instrumented_transform(qd),
            Transforms::MPSCTee(m) => m.instrumented_transform(qd),
            Transforms::MPSCForwarder(m) => m.instrumented_transform(qd),
            Transforms::Route(r) => r.instrumented_transform(qd),
            Transforms::Scatter(s) => s.instrumented_transform(qd),
            Transforms::Printer(p) => p.instrumented_transform(qd),
            Transforms::Null(n) => n.instrumented_transform(qd),
            Transforms::Lua(l) => l.instrumented_transform(qd),
            Transforms::Protect(p) => p.instrumented_transform(qd),
            Transforms::RepeatMessage(p) => p.instrumented_transform(qd),
            Transforms::RandomDelay(p) => p.instrumented_transform(qd),
            Transforms::TunableConsistency(tc) => tc.instrumented_transform(qd),
            Transforms::RedisCodecDestination(r) => r.instrumented_transform(qd),
            Transforms::RedisTimeStampTagger(r) => r.instrumented_transform(qd),
            Transforms::RedisCluster(r) => r.instrumented_transform(qd),
            Transforms::SequentialMap(s) => s.instrumented_transform(qd),
            Transforms::ParallelMap(s) => s.instrumented_transform(qd),
            Transforms::PoolConnections(s) => s.instrumented_transform(qd),
        }
        .await
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
            Transforms::TunableConsistency(t) => t.get_name(),
            Transforms::RepeatMessage(p) => p.get_name(),
            Transforms::RandomDelay(p) => p.get_name(),
            Transforms::RedisCodecDestination(r) => r.get_name(),
            Transforms::RedisTimeStampTagger(r) => r.get_name(),
            Transforms::RedisCluster(r) => r.get_name(),
            Transforms::SequentialMap(s) => s.get_name(),
            Transforms::ParallelMap(s) => s.get_name(),
            Transforms::PoolConnections(s) => s.get_name(),
        }
    }

    async fn prep_transform_chain(&mut self, t: &mut TransformChain) -> Result<()> {
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
            Transforms::TunableConsistency(a) => a.prep_transform_chain(t).await,
            Transforms::RepeatMessage(a) => a.prep_transform_chain(t).await,
            Transforms::RandomDelay(a) => a.prep_transform_chain(t).await,
            Transforms::RedisTimeStampTagger(a) => a.prep_transform_chain(t).await,
            Transforms::RedisCluster(r) => r.prep_transform_chain(t).await,
            Transforms::SequentialMap(s) => s.prep_transform_chain(t).await,
            Transforms::ParallelMap(s) => s.prep_transform_chain(t).await,
            Transforms::PoolConnections(s) => s.prep_transform_chain(t).await,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum TransformsConfig {
    CodecDestination(CodecConfiguration),
    RedisDestination(RedisCodecConfiguration),
    KafkaDestination(KafkaConfig),
    RedisCache(RedisConfig),
    MPSCTee(TeeConfig),
    MPSCForwarder(BufferConfig),
    Route(RouteConfig),
    ConsistentScatter(TunableConsistencyConfig),
    Scatter(ScatterConfig),
    RedisCluster(RedisClusterConfig),
    RedisTimestampTagger,
    Printer,
    SequentialMap(SequentialMapConfig),
    ParallelMap(ParallelMapConfig),
    PoolConnections(ConnectionBalanceAndPoolConfig),
}

impl TransformsConfig {
    pub async fn get_transforms(&self, topics: &TopicHolder) -> Result<Transforms> {
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
            TransformsConfig::RedisTimestampTagger => {
                Ok(Transforms::RedisTimeStampTagger(RedisTimestampTagger::new()))
            }
            TransformsConfig::Printer => Ok(Transforms::Printer(Printer::new())),
            TransformsConfig::RedisCluster(r) => r.get_source(topics).await,
            TransformsConfig::SequentialMap(s) => s.get_source(topics).await,
            TransformsConfig::ParallelMap(s) => s.get_source(topics).await,
            TransformsConfig::PoolConnections(s) => s.get_source(topics).await,
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
    Ok(TransformChain::new(
        transforms,
        name,
        topics.get_global_map_handle(),
        topics.get_global_tx(),
    ))
}

#[async_trait]
pub trait TransformsFromConfig: Send {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms>;
}

#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

#[derive(Debug)]
pub struct Wrapper<'a> {
    pub message: Messages,
    pub from_client: String,
    transforms: Vec<&'a mut Transforms>,
}

impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            message: self.message.clone(),
            from_client: self.from_client.clone(),
            transforms: vec![],
        }
    }
}

impl<'a> UserData for Wrapper<'a> {}

impl<'a> Display for Wrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("{:#?}", self.message))
    }
}

impl<'a> Wrapper<'a> {
    pub async fn call_next_transform(mut self) -> ChainResponse {
        let t = self.transforms.remove(0);

        let name = t.get_name();
        let start = Instant::now();
        let result;
        {
            result = t.transform(self).await;
        }
        let end = Instant::now();
        counter!("shotover_transform_total", 1, "transform" => name);
        if result.is_err() {
            counter!("shotover_transform_failures", 1, "transform" => name)
        }
        timing!("shotover_transform_latency", start, end, "transform" => name);
        result
    }

    pub fn swap_message(&mut self, mut m: Messages) {
        std::mem::swap(&mut self.message, &mut m);
    }

    pub fn new(m: Messages) -> Self {
        Wrapper {
            message: m,
            from_client: "".to_string(),
            transforms: vec![],
        }
    }

    pub fn reset(&mut self, transforms: Vec<&'a mut Transforms>) {
        self.transforms = transforms;
    }
}

#[derive(Debug)]
struct ResponseData {
    response: Messages,
}

#[async_trait]
pub trait Transform: Send {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse;

    async fn instrumented_transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let name = self.get_name();
        let client = qd.from_client.clone();
        self.transform(qd)
            .instrument(span!(
                Level::DEBUG,
                "processing_transform",
                transform_name = ?name,
                connection = ?client.as_str()
            ))
            .await
    }

    fn get_name(&self) -> &'static str;

    async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
        Ok(())
    }
}
