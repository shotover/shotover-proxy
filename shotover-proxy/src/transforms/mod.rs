use core::fmt;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;

use anyhow::Result;
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::Future;
use serde::Deserialize;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, Messages};
use crate::transforms::cassandra::sink_single::{CassandraSinkSingle, CassandraSinkSingleConfig};
use metrics::{counter, histogram};

use crate::transforms::chain::TransformChain;
use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
use crate::transforms::debug_printer::DebugPrinter;
use crate::transforms::distributed::consistent_scatter::{
    ConsistentScatter, ConsistentScatterConfig,
};
use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
use crate::transforms::internal_debug_transforms::{
    DebugRandomDelayTransform, DebugReturnerTransform,
};
use crate::transforms::kafka_sink::{KafkaSink, KafkaSinkConfig};
use crate::transforms::load_balance::ConnectionBalanceAndPool;
use crate::transforms::loopback::Loopback;
use crate::transforms::null::Null;
use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
use crate::transforms::protect::Protect;
use crate::transforms::query_counter::{QueryCounter, QueryCounterConfig};
use crate::transforms::redis::cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::redis::cluster_ports_rewrite::{
    RedisClusterPortsRewrite, RedisClusterPortsRewriteConfig,
};
use crate::transforms::redis::sink_cluster::{RedisSinkCluster, RedisSinkClusterConfig};
use crate::transforms::redis::sink_single::{RedisSinkSingle, RedisSinkSingleConfig};
use crate::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use crate::transforms::tee::{Forwarder, ForwarderConfig, Tee, TeeConfig};
use core::fmt::Display;
use tokio::time::Instant;

pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod debug_printer;
pub mod distributed;
pub mod filter;
pub mod internal_debug_transforms;
pub mod kafka_sink;
pub mod load_balance;
pub mod loopback;
pub mod noop;
pub mod null;
mod parallel_map;
pub mod protect;
pub mod query_counter;
pub mod redis;
pub mod sampler;
pub mod tee;
pub mod util;

//TODO Generate the trait implementation for this passthrough enum via a macro

/// The [`crate::transforms::Transforms`] enum is responsible for [`crate::transforms::Transform`] registration and enum dispatch
/// in the transform chain. This is largely a performance optimisation by using enum dispatch rather
/// than using dynamic trait objects.
#[derive(Clone)]
pub enum Transforms {
    CassandraSinkSingle(CassandraSinkSingle),
    RedisSinkSingle(RedisSinkSingle),
    KafkaSink(KafkaSink),
    RedisCache(SimpleRedisCache),
    Tee(Tee),
    Forwarder(Forwarder),
    Null(Null),
    Loopback(Loopback),
    Protect(Protect),
    ConsistentScatter(ConsistentScatter),
    RedisTimestampTagger(RedisTimestampTagger),
    RedisSinkCluster(RedisSinkCluster),
    RedisClusterPortsRewrite(RedisClusterPortsRewrite),
    DebugReturnerTransform(DebugReturnerTransform),
    DebugRandomDelay(DebugRandomDelayTransform),
    DebugPrinter(DebugPrinter),
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
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        match self {
            Transforms::CassandraSinkSingle(c) => c.transform(message_wrapper).await,
            Transforms::KafkaSink(k) => k.transform(message_wrapper).await,
            Transforms::RedisCache(r) => r.transform(message_wrapper).await,
            Transforms::Tee(m) => m.transform(message_wrapper).await,
            Transforms::Forwarder(m) => m.transform(message_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform(message_wrapper).await,
            Transforms::Null(n) => n.transform(message_wrapper).await,
            Transforms::Loopback(n) => n.transform(message_wrapper).await,
            Transforms::Protect(p) => p.transform(message_wrapper).await,
            Transforms::DebugReturnerTransform(p) => p.transform(message_wrapper).await,
            Transforms::DebugRandomDelay(p) => p.transform(message_wrapper).await,
            Transforms::ConsistentScatter(tc) => tc.transform(message_wrapper).await,
            Transforms::RedisSinkSingle(r) => r.transform(message_wrapper).await,
            Transforms::RedisTimestampTagger(r) => r.transform(message_wrapper).await,
            Transforms::RedisClusterPortsRewrite(r) => r.transform(message_wrapper).await,
            Transforms::RedisSinkCluster(r) => r.transform(message_wrapper).await,
            Transforms::ParallelMap(s) => s.transform(message_wrapper).await,
            Transforms::PoolConnections(s) => s.transform(message_wrapper).await,
            Transforms::Coalesce(s) => s.transform(message_wrapper).await,
            Transforms::QueryTypeFilter(s) => s.transform(message_wrapper).await,
            Transforms::QueryCounter(s) => s.transform(message_wrapper).await,
        }
    }

    fn get_name(&self) -> &'static str {
        match self {
            Transforms::CassandraSinkSingle(c) => c.get_name(),
            Transforms::KafkaSink(k) => k.get_name(),
            Transforms::RedisCache(r) => r.get_name(),
            Transforms::Tee(m) => m.get_name(),
            Transforms::Forwarder(m) => m.get_name(),
            Transforms::DebugPrinter(p) => p.get_name(),
            Transforms::Null(n) => n.get_name(),
            Transforms::Loopback(n) => n.get_name(),
            Transforms::Protect(p) => p.get_name(),
            Transforms::ConsistentScatter(t) => t.get_name(),
            Transforms::DebugReturnerTransform(p) => p.get_name(),
            Transforms::DebugRandomDelay(p) => p.get_name(),
            Transforms::RedisSinkSingle(r) => r.get_name(),
            Transforms::RedisClusterPortsRewrite(r) => r.get_name(),
            Transforms::RedisTimestampTagger(r) => r.get_name(),
            Transforms::RedisSinkCluster(r) => r.get_name(),
            Transforms::ParallelMap(s) => s.get_name(),
            Transforms::PoolConnections(s) => s.get_name(),
            Transforms::Coalesce(s) => s.get_name(),
            Transforms::QueryTypeFilter(s) => s.get_name(),
            Transforms::QueryCounter(s) => s.get_name(),
        }
    }

    async fn _prep_transform_chain(&mut self, t: &mut TransformChain) -> Result<()> {
        match self {
            Transforms::CassandraSinkSingle(a) => a.prep_transform_chain(t).await,
            Transforms::RedisSinkSingle(a) => a.prep_transform_chain(t).await,
            Transforms::KafkaSink(a) => a.prep_transform_chain(t).await,
            Transforms::RedisCache(a) => a.prep_transform_chain(t).await,
            Transforms::Tee(a) => a.prep_transform_chain(t).await,
            Transforms::Forwarder(a) => a.prep_transform_chain(t).await,
            Transforms::DebugPrinter(a) => a.prep_transform_chain(t).await,
            Transforms::Null(a) => a.prep_transform_chain(t).await,
            Transforms::Loopback(a) => a.prep_transform_chain(t).await,
            Transforms::Protect(a) => a.prep_transform_chain(t).await,
            Transforms::ConsistentScatter(a) => a.prep_transform_chain(t).await,
            Transforms::DebugReturnerTransform(a) => a.prep_transform_chain(t).await,
            Transforms::DebugRandomDelay(a) => a.prep_transform_chain(t).await,
            Transforms::RedisTimestampTagger(a) => a.prep_transform_chain(t).await,
            Transforms::RedisSinkCluster(r) => r.prep_transform_chain(t).await,
            Transforms::RedisClusterPortsRewrite(r) => r.prep_transform_chain(t).await,
            Transforms::ParallelMap(s) => s.prep_transform_chain(t).await,
            Transforms::PoolConnections(s) => s.prep_transform_chain(t).await,
            Transforms::Coalesce(s) => s.prep_transform_chain(t).await,
            Transforms::QueryTypeFilter(s) => s.prep_transform_chain(t).await,
            Transforms::QueryCounter(s) => s.prep_transform_chain(t).await,
        }
    }

    fn validate(&self, position: usize) -> Vec<String> {
        match self {
            Transforms::CassandraSinkSingle(c) => c.validate(position),
            Transforms::KafkaSink(k) => k.validate(position),
            Transforms::RedisCache(r) => r.validate(position),
            Transforms::MPSCTee(t) => t.validate(position),
            Transforms::MPSCForwarder(f) => f.validate(position),
            Transforms::RedisSinkSingle(r) => r.validate(position),
            Transforms::TunableConsistency(c) => c.validate(position),
            Transforms::RedisTimeStampTagger(r) => r.validate(position),
            Transforms::RedisClusterSlotRewrite(r) => r.validate(position),
            Transforms::Printer(p) => p.validate(position),
            Transforms::Null(n) => n.validate(position),
            Transforms::RedisSinkCluster(r) => r.validate(position),
            Transforms::ParallelMap(s) => s.validate(position),
            Transforms::PoolConnections(s) => s.validate(position),
            Transforms::Coalesce(s) => s.validate(position),
            Transforms::QueryTypeFilter(s) => s.validate(position),
            Transforms::QueryCounter(s) => s.validate(position),
            _ => vec![],
        }
    }
}

/// The TransformsConfig enum is responsible for TransformConfig registration and enum dispatch
/// in the transform chain. Allows you to register your config struct for the config file.
#[derive(Deserialize, Debug, Clone)]
pub enum TransformsConfig {
    CassandraSinkSingle(CassandraSinkSingleConfig),
    RedisSinkSingle(RedisSinkSingleConfig),
    KafkaSink(KafkaSinkConfig),
    RedisCache(RedisConfig),
    Tee(TeeConfig),
    Forwarder(ForwarderConfig),
    ConsistentScatter(ConsistentScatterConfig),
    RedisSinkCluster(RedisSinkClusterConfig),
    RedisClusterPortsRewrite(RedisClusterPortsRewriteConfig),
    RedisTimestampTagger,
    DebugPrinter,
    Null,
    Loopback,
    ParallelMap(ParallelMapConfig),
    //PoolConnections(ConnectionBalanceAndPoolConfig),
    Coalesce(CoalesceConfig),
    QueryTypeFilter(QueryTypeFilterConfig),
    QueryCounter(QueryCounterConfig),
}

impl TransformsConfig {
    #[async_recursion]
    /// Return a new instance of the transform that the config is specifying.
    pub async fn get_transforms(&self, topics: &TopicHolder) -> Result<Transforms> {
        match self {
            TransformsConfig::CassandraSinkSingle(c) => c.get_source(topics).await,
            TransformsConfig::KafkaSink(k) => k.get_source(topics).await,
            TransformsConfig::RedisCache(r) => r.get_source(topics).await,
            TransformsConfig::Tee(t) => t.get_source(topics).await,
            TransformsConfig::Forwarder(f) => f.get_source(topics).await,
            TransformsConfig::RedisSinkSingle(r) => r.get_source(topics).await,
            TransformsConfig::ConsistentScatter(c) => c.get_source(topics).await,
            TransformsConfig::RedisTimestampTagger => {
                Ok(Transforms::RedisTimestampTagger(RedisTimestampTagger::new()))
            }
            TransformsConfig::RedisClusterPortsRewrite(r) => r.get_source(topics).await,
            TransformsConfig::DebugPrinter => Ok(Transforms::DebugPrinter(DebugPrinter::new())),
            TransformsConfig::Null => Ok(Transforms::Null(Null::default())),
            TransformsConfig::Loopback => Ok(Transforms::Loopback(Loopback::default())),
            TransformsConfig::RedisSinkCluster(r) => r.get_source(topics).await,
            TransformsConfig::ParallelMap(s) => s.get_source(topics).await,
            //TransformsConfig::PoolConnections(s) => s.get_source(topics).await,
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

#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`Wrapper.messages`].
#[derive(Debug)]
pub struct Wrapper<'a> {
    pub messages: Messages,
    transforms: Vec<&'a mut Transforms>,
    pub client_details: String,
    chain_name: String,
}

/// [`Wrapper`] will not (cannot) bring the current list of transforms that it needs to traverse with it
/// This is purely to make it convenient to clone all the data within Wrapper rather than it's transform
/// state.
impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            messages: self.messages.clone(),
            transforms: vec![],
            client_details: self.client_details.clone(),
            chain_name: self.chain_name.clone(),
        }
    }
}

impl<'a> Display for Wrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("{:#?}", self.messages))
    }
}

tokio::task_local! {
    pub static CONTEXT_CHAIN_NAME: String;
}

impl<'a> Wrapper<'a> {
    /// This function will take a mutable reference to the next transform out of the [`Wrapper`] structs
    /// vector of transform references. It then sets up the chain name and transform name in the local
    /// thread scope for structured logging.
    ///
    /// It then calls the next [Transform], recording the number of successes and failures in a metrics counter. It also measures
    /// the execution time of the [Transform::transform] function as a metrics latency histogram.
    ///
    /// The result of calling the next transform is then provided as a response.
    pub async fn call_next_transform(mut self) -> ChainResponse {
        if self.transforms.is_empty() {
            panic!("The transform chain does not end with a terminating transform. If you want to throw the messages away use a Null transform, otherwise use a terminating sink transform to send the messages somewhere.");
        }
        let transform = self.transforms.remove(0);

        let transform_name = transform.get_name();
        let chain_name = self.chain_name.clone();

        let start = Instant::now();
        let result = CONTEXT_CHAIN_NAME
            .scope(chain_name, transform.transform(self))
            .await;
        counter!("shotover_transform_total", 1, "transform" => transform_name);
        if result.is_err() {
            counter!("shotover_transform_failures", 1, "transform" => transform_name)
        }
        histogram!("shotover_transform_latency", start.elapsed(),  "transform" => transform_name);
        result
    }

    pub fn new(m: Messages) -> Self {
        Wrapper {
            messages: m,
            transforms: vec![],
            client_details: "".to_string(),
            chain_name: "".to_string(),
        }
    }

    pub fn new_with_chain_name(m: Messages, chain_name: String) -> Self {
        Wrapper {
            messages: m,
            transforms: vec![],
            client_details: "".to_string(),
            chain_name,
        }
    }

    pub fn new_with_client_details(m: Messages, client_details: String) -> Self {
        Wrapper {
            messages: m,
            transforms: vec![],
            client_details,
            chain_name: "".to_string(),
        }
    }

    pub fn new_with_next_transform(m: Messages, _next_transform: usize) -> Self {
        Wrapper {
            messages: m,
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

/// This trait is the primary extension point for Shotover-proxy.
/// A [`Transform`] is a struct that implements the Transform trait and enables you to modify and observe database
/// queries or frames.
/// The trait has one method where you implement the majority of your logic [Transform::transform],
/// however it also includes a setup and naming method.
///
/// Transforms are cloned on a per TCP connection basis from a copy of the struct originally created
/// by the call to [TransformsConfig::get_transforms].
/// This means that each member of your struct that implements this trait can be considered private for
/// each TCP connection or connected client. If you wish to share data between all copies of your struct
/// then wrapping a member in an [`Arc<Mutex<_>>`](std::sync::Mutex) will achieve that.
///
/// Changing the clone behavior of this struct can also control this behavior.
///
/// Once you have created your [`Transform`], you will need to create
/// new enum variants in [Transforms] and [TransformsConfig] to make them configurable in Shotover.
/// Shotover uses a concept called enum dispatch to provide dynamic configuration of transform chains
/// with minimal impact on performance.
///
/// Implementing this trait is usually done using `#[async_trait]` macros.
///
#[async_trait]
pub trait Transform: Send {
    /// This method should be implemented by your transform. The wrapper object contains the queries/
    /// frames in a [`Vec<Message>`](crate::message::Message). Some protocols support multiple queries before a response is expected
    /// for example pipelined Redis queries or batched Cassandra queries.
    ///
    /// Shotover expects the same number of messages in [`wrapper.messages`](crate::transforms::Wrapper) to be returned as was passed
    /// into the method via the parameter message_wrapper. For in order protocols (such as Redis) you will
    /// also need to ensure the order of responses matches the order of the queries.
    ///
    /// You can modify the messages in the wrapper struct to achieve your own designs. Your transform
    /// can also modify the response from `message_wrapper.call_next_transform()` if it needs
    /// to. As long as you return the same number of messages as you received, you won't break behavior
    /// from other transforms.
    ///
    /// ## Invariants
    /// Your transform method at a minimum needs to
    /// * _Non-terminating_ - If your transform does not send the message to an external system or generate its own response to the query,
    /// it will need to call and return the response from `message_wrapper.call_next_transform()`. This ensures that your
    /// transform will call any subsequent downstream transforms without needing to know about what they
    /// do. This type of transform is called an non-terminating transform.
    /// * _Terminating_ - Your transform can also choose not to call `message_wrapper.call_next_transform()` if it sends the
    /// messages to an external system or generates its own response to the query e.g.
    /// [`crate::transforms::cassandra::cassandra_sink_single::CassandraSinkSingle`]. This type of transform
    /// is called a Terminating transform (as no subsequent transforms in the chain will be called).
    /// * _Message count_ - Your transform should return the same number of responses as messages it receives. Transforms that
    /// don't do this explicitly for each call, should return the same number of responses as messages it receives over the lifetime
    /// of the transform chain. A good example of this is the [`crate::transforms::coalesce::Coalesce`] transform. The
    /// [`crate::transforms::sampler::Sampler`] transform is also another example of this, with a slightly different twist.
    /// The number of responses will be the sames as the number of messages, as the sampled messages are sent to a subchain rather than
    /// changing the behavior of the main chain.
    ///
    /// ## Naming
    /// Transforms also have different naming conventions.
    /// * Transforms that interact with an external system are called Sinks.
    /// * Transforms that don't call subsequent chains via `message_wrapper.call_next_transform()` are called terminating transforms.
    /// * Transforms that do call subsquent chains via `message_wrapper.call_next_transform()` are non-terminating transforms.
    ///
    /// You can have have a transforms that is both non-terminating and a sink (see [`crate::transforms::kafka_sink::KafkaSink`]).
    ///
    /// A basic transform that logs query data and counts the number requests it sees could be defined like so:
    /// ```
    /// use shotover_proxy::transforms::{Transform, Wrapper};
    /// use async_trait::async_trait;
    /// use tracing::info;
    /// use shotover_proxy::error::ChainResponse;
    ///
    /// #[derive(Debug, Clone)]
    /// pub struct Printer {
    ///     counter: i32,
    /// }
    ///
    /// impl Default for Printer {
    ///     fn default() -> Self {
    ///         Self::new()
    ///     }
    /// }
    ///
    /// impl Printer {
    ///     pub fn new() -> Printer {
    ///         Printer { counter: 0 }
    ///     }
    /// }
    ///
    /// #[async_trait]
    /// impl Transform for Printer {
    ///     async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
    ///         self.counter += 1;
    ///         info!("{} Request content: {:?}", self.counter, message_wrapper.messages);
    ///         let response = message_wrapper.call_next_transform().await;
    ///         info!("Response content: {:?}", response);
    ///         response
    ///     }
    ///
    ///     fn get_name(&self) -> &'static str {
    ///         "Printer"
    ///     }
    /// }
    /// ```
    ///
    /// In this example `counter` will contain the count of the number of messages seen for this connection.
    /// Wrapping it in an [`Arc<Mutex<_>>`](std::sync::Mutex) would make it a global count of all messages seen by this transform.
    ///
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse;

    /// This method provides an access method for getting the name of the transform.
    fn get_name(&self) -> &'static str;

    /// This method provides a hook into chain setup that allows you to perform any chain setup
    /// needed before receiving traffic. It is generally recommended to do any setup on the first query
    /// as this makes Shotover lazy startup and Shotover / upstream database startup ordering challenges
    /// easier to resolve (e.g. you can start Shotover before the upstream database).
    async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
        Ok(())
    }

    fn is_terminating(&self) -> bool {
        false
    }

    fn validate(&self, position: usize) -> Vec<String> {
        let mut errors = Vec::new();
        if position == 0 && !self.is_terminating() {
            errors.push(format!(
                "Non-terminating transform {:?} is last in chain",
                self.get_name()
            ));
        }

        if position != 0 && self.is_terminating() {
            errors.push(format!(
                "Terminating transform {:?} is not last in chain",
                self.get_name()
            ));
        }
        errors
    }
}

pub type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<(Message, Result<Messages>)>> + std::marker::Send>>;
