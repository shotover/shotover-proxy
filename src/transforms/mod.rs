use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};
use crate::transforms::cassandra_codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::lua::LuaFilterTransform;
use crate::transforms::test_transforms::ReturnerTransform;
use crate::transforms::mpsc::{
    AsyncMpscForwarder, AsyncMpscForwarderConfig, AsyncMpscTee, AsyncMpscTeeConfig,
};
use crate::transforms::null::Null;
use crate::transforms::printer::Printer;
use crate::transforms::protect::Protect;
use crate::transforms::python::PythonFilterTransform;
use crate::transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::route::{Route, RouteConfig};
use crate::transforms::scatter::{Scatter, ScatterConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use slog::Logger;
use crate::transforms::redis_codec_destination::{RedisCodecDestination, RedisCodecConfiguration};


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
pub mod python;
pub mod query;
pub mod redis_cache;
pub mod route;
pub mod scatter;
pub mod test_transforms;

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
    Python(PythonFilterTransform),
    Printer(Printer),
    Null(Null),
    Lua(LuaFilterTransform),
    Protect(Protect),
    // The below variants are mainly for testing
    RepeatMessage(ReturnerTransform)
}

#[async_trait]
impl Transform for Transforms {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        match self {
            Transforms::CodecDestination(c) => c.transform(qd, t).await,
            Transforms::KafkaDestination(k) => k.transform(qd, t).await,
            Transforms::RedisCache(r) => r.transform(qd, t).await,
            Transforms::MPSCTee(m) => m.transform(qd, t).await,
            Transforms::MPSCForwarder(m) => m.transform(qd, t).await,
            Transforms::Route(r) => r.transform(qd, t).await,
            Transforms::Scatter(s) => s.transform(qd, t).await,
            Transforms::Python(r) => r.transform(qd, t).await,
            Transforms::Printer(p) => p.transform(qd, t).await,
            Transforms::Null(n) => n.transform(qd, t).await,
            Transforms::Lua(l) => l.transform(qd, t).await,
            Transforms::Protect(p) => p.transform(qd, t).await,
            Transforms::RepeatMessage(p) => p.transform(qd, t).await,
            Transforms::RedisCodecDestination(r) => r.transform(qd, t).await
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
            Transforms::Python(r) => r.get_name(),
            Transforms::Printer(p) => p.get_name(),
            Transforms::Null(n) => n.get_name(),
            Transforms::Lua(l) => l.get_name(),
            Transforms::Protect(p) => p.get_name(),
            Transforms::RepeatMessage(p) => p.get_name(),
            Transforms::RedisCodecDestination(r) => r.get_name()
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
    Scatter(ScatterConfig),
}

impl TransformsConfig {
    pub async fn get_transforms(
        &self,
        topics: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        match self {
            TransformsConfig::CodecDestination(c) => c.get_source(topics, logger).await,
            TransformsConfig::KafkaDestination(k) => k.get_source(topics, logger).await,
            TransformsConfig::RedisCache(r) => r.get_source(topics, logger).await,
            TransformsConfig::MPSCTee(t) => t.get_source(topics, logger).await,
            TransformsConfig::MPSCForwarder(f) => f.get_source(topics, logger).await,
            TransformsConfig::Route(r) => r.get_source(topics, logger).await,
            TransformsConfig::Scatter(s) => s.get_source(topics, logger).await,
            TransformsConfig::RedisDestination(r) => r.get_source(topics, logger).await,
        }
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &Vec<TransformsConfig>,
    topics: &TopicHolder,
    logger: &Logger,
) -> Result<TransformChain, ConfigError> {
    let mut transforms: Vec<Transforms> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_transforms(topics, logger).await?)
    }
    return Ok(TransformChain::new(transforms, name));
}

#[async_trait]
pub trait TransformsFromConfig: Send + Sync {
    async fn get_source(
        &self,
        topics: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError>;
}
