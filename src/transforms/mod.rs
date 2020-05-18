use crate::transforms::codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::chain::{Transform, Wrapper, ChainResponse, TransformChain};
use async_trait::async_trait;
use crate::transforms::mpsc::{AsyncMpscForwarder, AsyncMpscTee, AsyncMpscTeeConfig, AsyncMpscForwarderConfig};
use crate::transforms::route::{Route, RouteConfig};
use crate::transforms::scatter::{Scatter, ScatterConfig};
use serde::{Serialize, Deserialize};

pub mod chain;
pub mod codec_destination;
pub mod route;
pub mod scatter;
pub mod noop;
pub mod printer;
pub mod query;
pub mod redis_cache;
pub mod mpsc;
pub mod kafka_destination;

#[derive(Clone)]
pub enum Transforms {
    CodecDestination(CodecDestination),
    KafkaDestination(KafkaDestination),
    RedisCache(SimpleRedisCache),
    MPSCTee(AsyncMpscTee),
    MPSCForwarder(AsyncMpscForwarder),
    Route(Route),
    Scatter(Scatter)
}

#[async_trait]
impl Transform for Transforms {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        match self {
            Transforms::CodecDestination(c) => {c.transform(qd, t).await},
            Transforms::KafkaDestination(k) => {k.transform(qd, t).await},
            Transforms::RedisCache(r) => {r.transform(qd, t).await},
            Transforms::MPSCTee(m) => {m.transform(qd, t).await},
            Transforms::MPSCForwarder(m) => {m.transform(qd, t).await},
            Transforms::Route(r) => {r.transform(qd, t).await},
            Transforms::Scatter(s) => {s.transform(qd, t).await}
        }
    }

    fn get_name(&self) -> &'static str {
        match self {
            Transforms::CodecDestination(c) => {c.get_name()},
            Transforms::KafkaDestination(k) => {k.get_name()},
            Transforms::RedisCache(r) => {r.get_name()},
            Transforms::MPSCTee(m) => {m.get_name()},
            Transforms::MPSCForwarder(m) => {m.get_name()},
            Transforms::Route(r) => {r.get_name()},
            Transforms::Scatter(s) => {s.get_name()}
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TransformsConfig {
    CodecDestination(CodecConfiguration),
    KafkaDestination(KafkaConfig),
    RedisCache(RedisConfig),
    MPSCTee(AsyncMpscTeeConfig),
    MPSCForwarder(AsyncMpscForwarderConfig),
    Route(RouteConfig),
    Scatter(ScatterConfig)
}



#[async_trait]
pub trait TransformsFromConfig: Send + Sync {
    async fn get_source(&self) -> Transforms;
}