use core::fmt;
use core::fmt::Display;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use libloading::{Library, Symbol};
use metrics::{counter, timing};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::transforms::cassandra_codec_destination::{CodecConfiguration, CodecDestination};
use crate::transforms::chain::TransformChain;
use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
use crate::transforms::distributed::tunable_consistency_scatter::{
    TunableConsistency, TunableConsistencyConfig,
};
use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
use crate::transforms::load_balance::{ConnectionBalanceAndPool, ConnectionBalanceAndPoolConfig};
use crate::transforms::mpsc::{Buffer, BufferConfig, Tee, TeeConfig};
use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
use crate::transforms::query_counter::{QueryCounter, QueryCounterConfig};
use crate::transforms::redis_transforms::redis_cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::redis_transforms::redis_cluster::{RedisCluster, RedisClusterConfig};
use crate::transforms::redis_transforms::redis_codec_destination::{
    RedisCodecConfiguration, RedisCodecDestination,
};
use crate::transforms::test_transforms::{RandomDelayTransform, ReturnerTransform};

pub mod cassandra_codec_destination;
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
pub mod query_counter;
pub mod redis_transforms;
pub mod sampler;
pub mod test_transforms;
pub mod util;

//TODO Generate the trait implementation for this passthrough enum via a macro

#[derive(Clone, Debug)]
pub struct LibraryTransform {
    lib_path: String,
    lib: LibDeclaration,
}

#[async_trait]
impl Transform for LibraryTransform {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        (self.lib.transform_call)(qd.message).await
    }

    fn get_name(&self) -> &'static str {
        "library transform"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct LibraryTransformConfig {}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for LibraryTransformConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        let libname = "./target/debug/libasync_plugin.so";
        let library = Library::new(libname).unwrap();
        let decl = unsafe {
            library
                .get::<*mut LibDeclaration>(b"lib_declaration\0")
                .unwrap()
                .read()
        };
        return Ok(Box::new(LibraryTransform {
            lib_path: libname.to_string(),
            lib: decl,
        }));
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &[Box<dyn TransformsFromConfig + Send + Sync>],
    topics: &TopicHolder,
) -> Result<TransformChain> {
    let mut transforms: Vec<Box<dyn Transform + Send + Sync>> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_source(topics).await?)
    }
    Ok(TransformChain::new(transforms, name))
}

#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

#[derive(Debug)]
struct ResponseData {
    response: Messages,
}

#[async_trait]
pub trait InternalTransform: Send {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse;

    fn get_name(&self) -> &'static str;

    async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
        Ok(())
    }
}
