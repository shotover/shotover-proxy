use core::fmt;
use std::fmt::{Debug, Formatter};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::cassandra::cassandra_codec_destination::{
    CodecConfiguration, CodecDestination,
};
use metrics::{counter, timing};

use crate::transforms::chain::TransformChain;
use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
use crate::transforms::distributed::tunable_consistency_scatter::{
    TunableConsistency, TunableConsistencyConfig,
};
use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::load_balance::{ConnectionBalanceAndPool, ConnectionBalanceAndPoolConfig};
use crate::transforms::lua::LuaFilterTransform;
use crate::transforms::mpsc::{Buffer, BufferConfig, Tee, TeeConfig};
use crate::transforms::null::Null;
use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
use crate::transforms::printer::Printer;
use crate::transforms::protect::Protect;
use crate::transforms::query_counter::{QueryCounter, QueryCounterConfig};
use crate::transforms::redis_transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::redis_transforms::redis_cluster::{RedisCluster, RedisClusterConfig};
use crate::transforms::redis_transforms::redis_codec_destination::{
    RedisCodecConfiguration, RedisCodecDestination,
};
use crate::transforms::redis_transforms::timestamp_tagging::RedisTimestampTagger;
use crate::transforms::test_transforms::{RandomDelayTransform, ReturnerTransform};
use core::fmt::Display;
use distributed::route::{Route, RouteConfig};
use distributed::scatter::{Scatter, ScatterConfig};
use mlua::UserData;
use tokio::time::Instant;

pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod distributed;
pub mod filter;
pub mod kafka_destination;
pub mod load_balance;
pub mod lua;
pub mod mpsc;
pub mod noop;
pub mod null;
mod parallel_map;
pub mod printer;
pub mod protect;
pub mod query_counter;
pub mod redis_transforms;
pub mod sampler;
pub mod test_transforms;
pub mod util;

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
    ParallelMap(ParallelMap),
    PoolConnections(ConnectionBalanceAndPool),
    Coalesce(Coalesce),
    QueryTypeFilter(QueryTypeFilter),
    QueryCounter(QueryCounter),
}

impl Debug for Transforms {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transform: {}", self.get_name())
    }
}

impl Transforms {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        match self {
            Transforms::CodecDestination(c) => c.transform(qd).await,
            Transforms::KafkaDestination(k) => k.transform(qd).await,
            Transforms::RedisCache(r) => r.transform(qd).await,
            Transforms::MPSCTee(m) => m.transform(qd).await,
            Transforms::MPSCForwarder(m) => m.transform(qd).await,
            Transforms::Route(r) => r.transform(qd).await,
            Transforms::Scatter(s) => s.transform(qd).await,
            Transforms::Printer(p) => p.transform(qd).await,
            Transforms::Null(n) => n.transform(qd).await,
            Transforms::Lua(l) => l.transform(qd).await,
            Transforms::Protect(p) => p.transform(qd).await,
            Transforms::RepeatMessage(p) => p.transform(qd).await,
            Transforms::RandomDelay(p) => p.transform(qd).await,
            Transforms::TunableConsistency(tc) => tc.transform(qd).await,
            Transforms::RedisCodecDestination(r) => r.transform(qd).await,
            Transforms::RedisTimeStampTagger(r) => r.transform(qd).await,
            Transforms::RedisCluster(r) => r.transform(qd).await,
            Transforms::ParallelMap(s) => s.transform(qd).await,
            Transforms::PoolConnections(s) => s.transform(qd).await,
            Transforms::Coalesce(s) => s.transform(qd).await,
            Transforms::QueryTypeFilter(s) => s.transform(qd).await,
            Transforms::QueryCounter(s) => s.transform(qd).await,
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
            Transforms::TunableConsistency(t) => t.get_name(),
            Transforms::RepeatMessage(p) => p.get_name(),
            Transforms::RandomDelay(p) => p.get_name(),
            Transforms::RedisCodecDestination(r) => r.get_name(),
            Transforms::RedisTimeStampTagger(r) => r.get_name(),
            Transforms::RedisCluster(r) => r.get_name(),
            Transforms::ParallelMap(s) => s.get_name(),
            Transforms::PoolConnections(s) => s.get_name(),
            Transforms::Coalesce(s) => s.get_name(),
            Transforms::QueryTypeFilter(s) => s.get_name(),
            Transforms::QueryCounter(s) => s.get_name(),
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
            Transforms::ParallelMap(s) => s.prep_transform_chain(t).await,
            Transforms::PoolConnections(s) => s.prep_transform_chain(t).await,
            Transforms::Coalesce(s) => s.prep_transform_chain(t).await,
            Transforms::QueryTypeFilter(s) => s.prep_transform_chain(t).await,
            Transforms::QueryCounter(s) => s.prep_transform_chain(t).await,
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
    ParallelMap(ParallelMapConfig),
    PoolConnections(ConnectionBalanceAndPoolConfig),
    Coalesce(CoalesceConfig),
    QueryTypeFilter(QueryTypeFilterConfig),
    QueryCounter(QueryCounterConfig),
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
            TransformsConfig::ParallelMap(s) => s.get_source(topics).await,
            TransformsConfig::PoolConnections(s) => s.get_source(topics).await,
            TransformsConfig::Coalesce(s) => s.get_source(topics).await,
            TransformsConfig::QueryTypeFilter(s) => s.get_source(topics).await,
            TransformsConfig::QueryCounter(s) => s.get_source(topics).await,
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
    Ok(TransformChain::new(transforms, name))
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
    // pub next_transform: usize,
    transforms: Vec<&'a mut Transforms>,
    pub client_details: String,
    chain_name: String,
}

impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            message: self.message.clone(),
            transforms: vec![],
            client_details: self.client_details.clone(),
            chain_name: self.chain_name.clone(),
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
        let result = t.transform(self).await;
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
            transforms: vec![],
            client_details: "".to_string(),
            chain_name: "".to_string(),
        }
    }

    pub fn new_with_chain_name(m: Messages, chain_name: String) -> Self {
        Wrapper {
            message: m,
            transforms: vec![],
            client_details: "".to_string(),
            chain_name,
        }
    }

    pub fn new_with_client_details(m: Messages, client_details: String) -> Self {
        Wrapper {
            message: m,
            transforms: vec![],
            client_details,
            chain_name: "".to_string(),
        }
    }

    pub fn new_with_next_transform(m: Messages, _next_transform: usize) -> Self {
        Wrapper {
            message: m,
            transforms: vec![],
            client_details: "".to_string(),
            chain_name: "".to_string(),
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

    fn get_name(&self) -> &'static str;

    async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
        Ok(())
    }
}
