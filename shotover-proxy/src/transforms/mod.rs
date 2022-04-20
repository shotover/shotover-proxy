use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::cassandra::peers_rewrite::CassandraPeersRewrite;
use crate::transforms::cassandra::peers_rewrite::CassandraPeersRewriteConfig;
use crate::transforms::cassandra::sink_single::{CassandraSinkSingle, CassandraSinkSingleConfig};
use crate::transforms::chain::TransformChain;
use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
use crate::transforms::debug::force_parse::DebugForceParse;
#[cfg(feature = "alpha-transforms")]
use crate::transforms::debug::force_parse::DebugForceParseConfig;
use crate::transforms::debug::printer::DebugPrinter;
use crate::transforms::debug::random_delay::DebugRandomDelay;
use crate::transforms::debug::returner::{DebugReturner, DebugReturnerConfig};
use crate::transforms::distributed::consistent_scatter::{
    ConsistentScatter, ConsistentScatterConfig,
};
use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
use crate::transforms::load_balance::ConnectionBalanceAndPool;
#[cfg(test)]
use crate::transforms::loopback::Loopback;
use crate::transforms::null::Null;
use crate::transforms::parallel_map::{ParallelMap, ParallelMapConfig};
use crate::transforms::protect::Protect;
#[cfg(feature = "alpha-transforms")]
use crate::transforms::protect::ProtectConfig;
use crate::transforms::query_counter::{QueryCounter, QueryCounterConfig};
use crate::transforms::redis::cache::{RedisConfig, SimpleRedisCache};
use crate::transforms::redis::cluster_ports_rewrite::{
    RedisClusterPortsRewrite, RedisClusterPortsRewriteConfig,
};
use crate::transforms::redis::sink_cluster::{RedisSinkCluster, RedisSinkClusterConfig};
use crate::transforms::redis::sink_single::{RedisSinkSingle, RedisSinkSingleConfig};
use crate::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use crate::transforms::tee::{Tee, TeeConfig};
use crate::transforms::throttling::{RequestThrottling, RequestThrottlingConfig};
use anyhow::Result;
use async_recursion::async_recursion;
use async_trait::async_trait;
use core::fmt;
use core::fmt::Display;
use futures::Future;
use metrics::{counter, histogram};
use serde::Deserialize;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use strum_macros::IntoStaticStr;
use tokio::time::Instant;

pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod debug;
pub mod distributed;
pub mod filter;
pub mod load_balance;
pub mod loopback;
pub mod noop;
pub mod null;
pub mod parallel_map;
pub mod protect;
pub mod query_counter;
pub mod redis;
pub mod sampler;
pub mod tee;
pub mod throttling;
pub mod util;

//TODO Generate the trait implementation for this passthrough enum via a macro

/// The [`crate::transforms::Transforms`] enum is responsible for [`crate::transforms::Transform`] registration and enum dispatch
/// in the transform chain. This is largely a performance optimisation by using enum dispatch rather
/// than using dynamic trait objects.
#[derive(Clone, IntoStaticStr)]
pub enum Transforms {
    CassandraSinkSingle(CassandraSinkSingle),
    RedisSinkSingle(RedisSinkSingle),
    CassandraPeersRewrite(CassandraPeersRewrite),
    RedisCache(SimpleRedisCache),
    Tee(Tee),
    Null(Null),
    #[cfg(test)]
    Loopback(Loopback),
    Protect(Protect),
    ConsistentScatter(ConsistentScatter),
    RedisTimestampTagger(RedisTimestampTagger),
    RedisSinkCluster(RedisSinkCluster),
    RedisClusterPortsRewrite(RedisClusterPortsRewrite),
    DebugReturner(DebugReturner),
    DebugRandomDelay(DebugRandomDelay),
    DebugPrinter(DebugPrinter),
    DebugForceParse(DebugForceParse),
    ParallelMap(ParallelMap),
    PoolConnections(ConnectionBalanceAndPool),
    Coalesce(Coalesce),
    QueryTypeFilter(QueryTypeFilter),
    QueryCounter(QueryCounter),
    RequestThrottling(RequestThrottling),
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
            Transforms::CassandraPeersRewrite(c) => c.transform(message_wrapper).await,
            Transforms::RedisCache(r) => r.transform(message_wrapper).await,
            Transforms::Tee(m) => m.transform(message_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform(message_wrapper).await,
            Transforms::DebugForceParse(p) => p.transform(message_wrapper).await,
            Transforms::Null(n) => n.transform(message_wrapper).await,
            #[cfg(test)]
            Transforms::Loopback(n) => n.transform(message_wrapper).await,
            Transforms::Protect(p) => p.transform(message_wrapper).await,
            Transforms::DebugReturner(p) => p.transform(message_wrapper).await,
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
            Transforms::RequestThrottling(s) => s.transform(message_wrapper).await,
        }
    }

    fn get_name(&self) -> &'static str {
        self.into()
    }

    async fn _prep_transform_chain(&mut self, t: &mut TransformChain) -> Result<()> {
        match self {
            Transforms::CassandraSinkSingle(a) => a.prep_transform_chain(t).await,
            Transforms::CassandraPeersRewrite(c) => c.prep_transform_chain(t).await,
            Transforms::RedisSinkSingle(a) => a.prep_transform_chain(t).await,
            Transforms::RedisCache(a) => a.prep_transform_chain(t).await,
            Transforms::Tee(a) => a.prep_transform_chain(t).await,
            Transforms::DebugPrinter(a) => a.prep_transform_chain(t).await,
            Transforms::DebugForceParse(a) => a.prep_transform_chain(t).await,
            Transforms::Null(a) => a.prep_transform_chain(t).await,
            #[cfg(test)]
            Transforms::Loopback(a) => a.prep_transform_chain(t).await,
            Transforms::Protect(a) => a.prep_transform_chain(t).await,
            Transforms::ConsistentScatter(a) => a.prep_transform_chain(t).await,
            Transforms::DebugReturner(a) => a.prep_transform_chain(t).await,
            Transforms::DebugRandomDelay(a) => a.prep_transform_chain(t).await,
            Transforms::RedisTimestampTagger(a) => a.prep_transform_chain(t).await,
            Transforms::RedisSinkCluster(r) => r.prep_transform_chain(t).await,
            Transforms::RedisClusterPortsRewrite(r) => r.prep_transform_chain(t).await,
            Transforms::ParallelMap(s) => s.prep_transform_chain(t).await,
            Transforms::PoolConnections(s) => s.prep_transform_chain(t).await,
            Transforms::Coalesce(s) => s.prep_transform_chain(t).await,
            Transforms::QueryTypeFilter(s) => s.prep_transform_chain(t).await,
            Transforms::QueryCounter(s) => s.prep_transform_chain(t).await,
            Transforms::RequestThrottling(s) => s.prep_transform_chain(t).await,
        }
    }

    fn validate(&self) -> Vec<String> {
        match self {
            Transforms::CassandraSinkSingle(c) => c.validate(),
            Transforms::CassandraPeersRewrite(c) => c.validate(),
            Transforms::RedisCache(r) => r.validate(),
            Transforms::Tee(t) => t.validate(),
            Transforms::RedisSinkSingle(r) => r.validate(),
            Transforms::ConsistentScatter(c) => c.validate(),
            Transforms::RedisTimestampTagger(r) => r.validate(),
            Transforms::RedisClusterPortsRewrite(r) => r.validate(),
            Transforms::DebugPrinter(p) => p.validate(),
            Transforms::DebugForceParse(p) => p.validate(),
            Transforms::Null(n) => n.validate(),
            Transforms::RedisSinkCluster(r) => r.validate(),
            Transforms::ParallelMap(s) => s.validate(),
            Transforms::PoolConnections(s) => s.validate(),
            Transforms::Coalesce(s) => s.validate(),
            Transforms::QueryTypeFilter(s) => s.validate(),
            Transforms::QueryCounter(s) => s.validate(),
            #[cfg(test)]
            Transforms::Loopback(l) => l.validate(),
            Transforms::Protect(p) => p.validate(),
            Transforms::DebugReturner(d) => d.validate(),
            Transforms::DebugRandomDelay(d) => d.validate(),
            Transforms::RequestThrottling(d) => d.validate(),
        }
    }

    fn is_terminating(&self) -> bool {
        match self {
            Transforms::CassandraSinkSingle(c) => c.is_terminating(),
            Transforms::CassandraPeersRewrite(c) => c.is_terminating(),
            Transforms::RedisCache(r) => r.is_terminating(),
            Transforms::Tee(t) => t.is_terminating(),
            Transforms::RedisSinkSingle(r) => r.is_terminating(),
            Transforms::ConsistentScatter(c) => c.is_terminating(),
            Transforms::RedisTimestampTagger(r) => r.is_terminating(),
            Transforms::RedisClusterPortsRewrite(r) => r.is_terminating(),
            Transforms::DebugPrinter(p) => p.is_terminating(),
            Transforms::DebugForceParse(p) => p.is_terminating(),
            Transforms::Null(n) => n.is_terminating(),
            Transforms::RedisSinkCluster(r) => r.is_terminating(),
            Transforms::ParallelMap(s) => s.is_terminating(),
            Transforms::PoolConnections(s) => s.is_terminating(),
            Transforms::Coalesce(s) => s.is_terminating(),
            Transforms::QueryTypeFilter(s) => s.is_terminating(),
            Transforms::QueryCounter(s) => s.is_terminating(),
            #[cfg(test)]
            Transforms::Loopback(l) => l.is_terminating(),
            Transforms::Protect(p) => p.is_terminating(),
            Transforms::DebugReturner(d) => d.is_terminating(),
            Transforms::DebugRandomDelay(d) => d.is_terminating(),
            Transforms::RequestThrottling(d) => d.is_terminating(),
        }
    }
}

/// The TransformsConfig enum is responsible for TransformConfig registration and enum dispatch
/// in the transform chain. Allows you to register your config struct for the config file.
#[derive(Deserialize, Debug, Clone)]
pub enum TransformsConfig {
    CassandraSinkSingle(CassandraSinkSingleConfig),
    RedisSinkSingle(RedisSinkSingleConfig),
    CassandraPeersRewrite(CassandraPeersRewriteConfig),
    RedisCache(RedisConfig),
    Tee(TeeConfig),
    ConsistentScatter(ConsistentScatterConfig),
    RedisSinkCluster(RedisSinkClusterConfig),
    RedisClusterPortsRewrite(RedisClusterPortsRewriteConfig),
    RedisTimestampTagger,
    DebugPrinter,
    DebugReturner(DebugReturnerConfig),
    Null,
    #[cfg(test)]
    Loopback,
    #[cfg(feature = "alpha-transforms")]
    Protect(ProtectConfig),
    #[cfg(feature = "alpha-transforms")]
    DebugForceParse(DebugForceParseConfig),
    ParallelMap(ParallelMapConfig),
    //PoolConnections(ConnectionBalanceAndPoolConfig),
    Coalesce(CoalesceConfig),
    QueryTypeFilter(QueryTypeFilterConfig),
    QueryCounter(QueryCounterConfig),
    RequestThrottling(RequestThrottlingConfig),
}

impl TransformsConfig {
    #[async_recursion]
    /// Return a new instance of the transform that the config is specifying.
    pub async fn get_transform(
        &self,
        topics: &TopicHolder,
        chain_name: String,
    ) -> Result<Transforms> {
        match self {
            TransformsConfig::CassandraSinkSingle(c) => c.get_transform(chain_name).await,
            TransformsConfig::CassandraPeersRewrite(c) => c.get_transform().await,
            TransformsConfig::RedisCache(r) => r.get_transform(topics).await,
            TransformsConfig::Tee(t) => t.get_transform(topics).await,
            TransformsConfig::RedisSinkSingle(r) => r.get_transform(chain_name).await,
            TransformsConfig::ConsistentScatter(c) => c.get_transform(topics).await,
            TransformsConfig::RedisTimestampTagger => {
                Ok(Transforms::RedisTimestampTagger(RedisTimestampTagger::new()))
            }
            TransformsConfig::RedisClusterPortsRewrite(r) => r.get_transform().await,
            TransformsConfig::DebugPrinter => Ok(Transforms::DebugPrinter(DebugPrinter::new())),
            TransformsConfig::DebugReturner(d) => d.get_transform().await,
            TransformsConfig::Null => Ok(Transforms::Null(Null::default())),
            #[cfg(test)]
            TransformsConfig::Loopback => Ok(Transforms::Loopback(Loopback::default())),
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::Protect(p) => p.get_transform().await,
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::DebugForceParse(d) => d.get_transform().await,
            TransformsConfig::RedisSinkCluster(r) => r.get_transform(chain_name).await,
            TransformsConfig::ParallelMap(s) => s.get_transform(topics).await,
            //TransformsConfig::PoolConnections(s) => s.get_transform(topics).await,
            TransformsConfig::Coalesce(s) => s.get_transform().await,
            TransformsConfig::QueryTypeFilter(s) => s.get_transform().await,
            TransformsConfig::QueryCounter(s) => s.get_transform().await,
            TransformsConfig::RequestThrottling(s) => s.get_transform().await,
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
        transforms.push(tc.get_transform(topics, name.clone()).await?)
    }
    Ok(TransformChain::new(transforms, name))
}

use std::slice::IterMut;
use tracing::debug;

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`Wrapper.messages`].
#[derive(Debug)]
pub struct Wrapper<'a> {
    pub messages: Messages,
    transforms: IterMut<'a, Transforms>,
    pub client_details: String,
    chain_name: String,
    /// When true transforms must flush any buffered messages into the messages field.
    /// This can occur at any time but will always occur before the transform is destroyed due to either
    /// shotover or the transform's chain shutting down.
    pub flush: bool,
}

/// [`Wrapper`] will not (cannot) bring the current list of transforms that it needs to traverse with it
/// This is purely to make it convenient to clone all the data within Wrapper rather than it's transform
/// state.
impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            messages: self.messages.clone(),
            transforms: [].iter_mut(),
            client_details: self.client_details.clone(),
            chain_name: self.chain_name.clone(),
            flush: false,
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
        let transform = match self.transforms.next() {
            Some(transform) => transform,
            None => panic!("The transform chain does not end with a terminating transform. If you want to throw the messages away use a Null transform, otherwise use a terminating sink transform to send the messages somewhere.")
        };

        let transform_name = transform.get_name();
        let chain_name = self.chain_name.clone();
        debug!(
            "call_next_transform calling {} {}",
            transform_name, chain_name
        );

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

    #[cfg(test)]
    pub fn new(m: Messages) -> Self {
        Wrapper {
            messages: m,
            transforms: [].iter_mut(),
            client_details: "".to_string(),
            chain_name: "".to_string(),
            flush: false,
        }
    }

    pub fn new_with_chain_name(m: Messages, chain_name: String) -> Self {
        Wrapper {
            messages: m,
            transforms: [].iter_mut(),
            client_details: "".to_string(),
            chain_name,
            flush: false,
        }
    }

    pub fn flush_with_chain_name(chain_name: String) -> Self {
        Wrapper {
            messages: vec![],
            transforms: [].iter_mut(),
            client_details: "".into(),
            chain_name,
            flush: true,
        }
    }

    pub fn new_with_client_details(
        m: Messages,
        client_details: String,
        chain_name: String,
    ) -> Self {
        Wrapper {
            messages: m,
            transforms: [].iter_mut(),
            client_details,
            chain_name,
            flush: false,
        }
    }

    pub fn reset(&mut self, transforms: &'a mut [Transforms]) {
        self.transforms = transforms.iter_mut();
    }
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
    /// * _Message count_ - message_wrapper.messages will contain 0 or more messages.
    /// Your transform should return the same number of responses as messages received in message_wrapper.messages. Transforms that
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
    /// You can have have a transforms that is both non-terminating and a sink.
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
    /// }
    /// ```
    ///
    /// In this example `counter` will contain the count of the number of messages seen for this connection.
    /// Wrapping it in an [`Arc<Mutex<_>>`](std::sync::Mutex) would make it a global count of all messages seen by this transform.
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse;

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

    fn validate(&self) -> Vec<String> {
        vec![]
    }
}

pub type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + std::marker::Send>>;
