use crate::transforms::codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::chain::{Transform, Wrapper, ChainResponse, TransformChain};
use async_trait::async_trait;
use crate::transforms::mpsc::{AsyncMpscForwarder, AsyncMpscTee};

pub mod chain;
pub mod codec_destination;
pub mod forward;
pub mod noop;
pub mod printer;
pub mod query;
pub mod redis_cache;
pub mod mpsc;
pub mod kafka_destination;
pub mod cassandra_source;


pub enum TransformConfigs {
    CodecDestination(CodecConfiguration),
    KafkaDestination(KafkaConfig),
    RedisCache(RedisConfig)
}

#[derive(Clone)]
pub enum Transforms {
    CodecDestination(CodecDestination),
    KafkaDestination(KafkaDestination),
    RedisCache(SimpleRedisCache),
    MPSCTee(AsyncMpscTee),
    MPSCForwarder(AsyncMpscForwarder)
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
        }
    }

    fn get_name(&self) -> &'static str {
        match self {
            Transforms::CodecDestination(c) => {c.get_name()},
            Transforms::KafkaDestination(k) => {k.get_name()},
            Transforms::RedisCache(r) => {r.get_name()},
            Transforms::MPSCTee(m) => {m.get_name()},
            Transforms::MPSCForwarder(m) => {m.get_name()},
        }
    }
}
//
// impl Transforms {
//     pub fn get_chain(&self) ->
// }

impl TransformConfigs {
    pub async fn get_transform(&self) -> Transforms {
        match self {
            TransformConfigs::CodecDestination(c) => {
                Transforms::CodecDestination(CodecDestination::new_from_config(c.address.clone()).await)
            },
            TransformConfigs::KafkaDestination(k) => {
                Transforms::KafkaDestination(KafkaDestination::new_from_config(&k.keys))
            },
            TransformConfigs::RedisCache(r) => {
                Transforms::RedisCache(SimpleRedisCache::new_from_config(&r.uri))
            },
        }
    }
}