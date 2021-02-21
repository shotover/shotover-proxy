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

use crate::transforms::chain::TransformChain;
use shotover_transforms::TopicHolder;
use shotover_transforms::{ChainResponse, TransformsFromConfig};
use shotover_transforms::{LibDeclaration, Transform};
use shotover_transforms::{Messages, Wrapper};
// use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
// use crate::transforms::distributed::tunable_consistency_scatter::{
//     TunableConsistency, TunableConsistencyConfig,
// };
// use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
// use crate::transforms::kafka_destination::{KafkaConfig, KafkaDestination};
// use crate::transforms::load_balance::{ConnectionBalanceAndPool, ConnectionBalanceAndPoolConfig};
// use crate::transforms::mpsc::{Buffer, BufferConfig, Tee, TeeConfig};
// use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
// use crate::transforms::query_counter::{QueryCounter, QueryCounterConfig};
// use crate::transforms::redis_transforms::redis_cache::{RedisConfig, SimpleRedisCache};
// use crate::transforms::redis_transforms::redis_cluster::{RedisCluster, RedisClusterConfig};
// use crate::transforms::redis_transforms::redis_codec_destination::{
//     RedisCodecConfiguration, RedisCodecDestination,
// };
// use crate::transforms::test_transforms::{RandomDelayTransform, ReturnerTransform};

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
pub struct LibraryTransformConfig {
    libname: String,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for LibraryTransformConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        let filename = self.libname.clone();
        let library = Library::new(filename).unwrap();
        unsafe {
            let conf_func: libloading::Symbol<
                unsafe extern "C" fn(
                    config: String,
                )
                    -> Pin<Box<dyn TransformsFromConfig + Send + Sync>>,
            > = library.get(b"get_configurator")?;
            let config_string = "".to_string();
            let conf_struct = conf_func(config_string);
            let fut = conf_struct.get_source_future(topics);
            return fut.await;
        }
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &[Box<dyn TransformsFromConfig + Send + Sync>],
    topics: &TopicHolder,
) -> Result<TransformChain> {
    let mut transforms: Vec<Box<dyn Transform + Send + Sync>> = Vec::new();
    let mut configs: Vec<Box<dyn TransformsFromConfig + Send + Sync>> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_source(topics).await?);
        configs.push(tc.clone());
    }
    Ok(TransformChain::new_with_configs(transforms, name, configs))
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
