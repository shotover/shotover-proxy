use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::cassandra::peers_rewrite::{
    CassandraPeersRewrite, CassandraPeersRewriteConfig,
};
use crate::transforms::cassandra::sink_cluster::{
    CassandraSinkCluster, CassandraSinkClusterConfig,
};
use crate::transforms::cassandra::sink_single::{CassandraSinkSingle, CassandraSinkSingleConfig};
use crate::transforms::chain::TransformChainBuilder;
use crate::transforms::coalesce::{Coalesce, CoalesceConfig};
use crate::transforms::debug::force_parse::DebugForceParse;
#[cfg(feature = "alpha-transforms")]
use crate::transforms::debug::force_parse::{DebugForceEncodeConfig, DebugForceParseConfig};
use crate::transforms::debug::printer::DebugPrinter;
use crate::transforms::debug::random_delay::DebugRandomDelay;
use crate::transforms::debug::returner::{DebugReturner, DebugReturnerConfig};
use crate::transforms::distributed::tuneable_consistency_scatter::{
    TuneableConsistencyScatterConfig, TuneableConsistentencyScatter,
};
use crate::transforms::filter::{QueryTypeFilter, QueryTypeFilterConfig};
use crate::transforms::kafka::sink_single::KafkaSinkSingle;
#[cfg(feature = "alpha-transforms")]
use crate::transforms::kafka::sink_single::KafkaSinkSingleConfig;
use crate::transforms::load_balance::ConnectionBalanceAndPool;
use crate::transforms::loopback::Loopback;
use crate::transforms::null::NullSink;
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
use anyhow::{anyhow, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
use core::fmt;
use dyn_clone::DynClone;
use futures::Future;
use metrics::{counter, histogram};
use serde::Deserialize;
use std::fmt::{Debug, Formatter};
use std::iter::Rev;
use std::net::SocketAddr;
use std::pin::Pin;
use std::slice::IterMut;
use strum_macros::IntoStaticStr;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod debug;
pub mod distributed;
pub mod filter;
pub mod kafka;
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

// TODO: probably better, semantically, to have this not Clone.
//       Fixing that is left to a followup PR though.
//       It would also affect whether sources pointing into the same chain share state, which will require careful consideration
pub trait TransformBuilder: DynClone + Send {
    fn build(&self) -> Transforms;

    fn get_name(&self) -> &'static str;

    fn validate(&self) -> Vec<String> {
        vec![]
    }

    fn is_terminating(&self) -> bool {
        false
    }
}
dyn_clone::clone_trait_object!(TransformBuilder);

impl Debug for dyn TransformBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transform: {}", self.get_name())
    }
}

//TODO Generate the trait implementation for this passthrough enum via a macro
/// The [`crate::transforms::Transforms`] enum is responsible for [`crate::transforms::Transform`] registration and enum dispatch
/// in the transform chain. This is largely a performance optimisation by using enum dispatch rather
/// than using dynamic trait objects.
#[derive(IntoStaticStr)]
pub enum Transforms {
    KafkaSinkSingle(KafkaSinkSingle),
    CassandraSinkSingle(CassandraSinkSingle),
    CassandraSinkCluster(Box<CassandraSinkCluster>),
    RedisSinkSingle(RedisSinkSingle),
    CassandraPeersRewrite(CassandraPeersRewrite),
    RedisCache(SimpleRedisCache),
    Tee(Tee),
    NullSink(NullSink),
    Loopback(Loopback),
    Protect(Box<Protect>),
    TuneableConsistencyScatter(TuneableConsistentencyScatter),
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
            Transforms::KafkaSinkSingle(c) => c.transform(message_wrapper).await,
            Transforms::CassandraSinkSingle(c) => c.transform(message_wrapper).await,
            Transforms::CassandraSinkCluster(c) => c.transform(message_wrapper).await,
            Transforms::CassandraPeersRewrite(c) => c.transform(message_wrapper).await,
            Transforms::RedisCache(r) => r.transform(message_wrapper).await,
            Transforms::Tee(m) => m.transform(message_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform(message_wrapper).await,
            Transforms::DebugForceParse(p) => p.transform(message_wrapper).await,
            Transforms::NullSink(n) => n.transform(message_wrapper).await,
            Transforms::Loopback(n) => n.transform(message_wrapper).await,
            Transforms::Protect(p) => p.transform(message_wrapper).await,
            Transforms::DebugReturner(p) => p.transform(message_wrapper).await,
            Transforms::DebugRandomDelay(p) => p.transform(message_wrapper).await,
            Transforms::TuneableConsistencyScatter(tc) => tc.transform(message_wrapper).await,
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

    async fn transform_pushed<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        match self {
            Transforms::KafkaSinkSingle(c) => c.transform_pushed(message_wrapper).await,
            Transforms::CassandraSinkSingle(c) => c.transform_pushed(message_wrapper).await,
            Transforms::CassandraSinkCluster(c) => c.transform_pushed(message_wrapper).await,
            Transforms::CassandraPeersRewrite(c) => c.transform_pushed(message_wrapper).await,
            Transforms::RedisCache(r) => r.transform_pushed(message_wrapper).await,
            Transforms::Tee(m) => m.transform_pushed(message_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform_pushed(message_wrapper).await,
            Transforms::DebugForceParse(p) => p.transform_pushed(message_wrapper).await,
            Transforms::NullSink(n) => n.transform_pushed(message_wrapper).await,
            Transforms::Loopback(n) => n.transform_pushed(message_wrapper).await,
            Transforms::Protect(p) => p.transform_pushed(message_wrapper).await,
            Transforms::DebugReturner(p) => p.transform_pushed(message_wrapper).await,
            Transforms::DebugRandomDelay(p) => p.transform_pushed(message_wrapper).await,
            Transforms::TuneableConsistencyScatter(tc) => {
                tc.transform_pushed(message_wrapper).await
            }
            Transforms::RedisSinkSingle(r) => r.transform_pushed(message_wrapper).await,
            Transforms::RedisTimestampTagger(r) => r.transform_pushed(message_wrapper).await,
            Transforms::RedisClusterPortsRewrite(r) => r.transform_pushed(message_wrapper).await,
            Transforms::RedisSinkCluster(r) => r.transform_pushed(message_wrapper).await,
            Transforms::ParallelMap(s) => s.transform_pushed(message_wrapper).await,
            Transforms::PoolConnections(s) => s.transform_pushed(message_wrapper).await,
            Transforms::Coalesce(s) => s.transform_pushed(message_wrapper).await,
            Transforms::QueryTypeFilter(s) => s.transform_pushed(message_wrapper).await,
            Transforms::QueryCounter(s) => s.transform_pushed(message_wrapper).await,
            Transforms::RequestThrottling(s) => s.transform_pushed(message_wrapper).await,
        }
    }

    fn get_name(&self) -> &'static str {
        self.into()
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        match self {
            Transforms::KafkaSinkSingle(c) => c.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::CassandraSinkSingle(c) => c.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::CassandraSinkCluster(c) => c.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::CassandraPeersRewrite(c) => c.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::RedisCache(r) => r.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::Tee(t) => t.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::RedisSinkSingle(r) => r.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::TuneableConsistencyScatter(c) => {
                c.set_pushed_messages_tx(pushed_messages_tx)
            }
            Transforms::RedisTimestampTagger(r) => r.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::RedisClusterPortsRewrite(r) => r.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::DebugPrinter(p) => p.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::DebugForceParse(p) => p.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::NullSink(n) => n.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::RedisSinkCluster(r) => r.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::ParallelMap(s) => s.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::PoolConnections(s) => s.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::Coalesce(s) => s.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::QueryTypeFilter(s) => s.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::QueryCounter(s) => s.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::Loopback(l) => l.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::Protect(p) => p.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::DebugReturner(d) => d.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::DebugRandomDelay(d) => d.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::RequestThrottling(d) => d.set_pushed_messages_tx(pushed_messages_tx),
        }
    }
}

/// The TransformsConfig enum is responsible for TransformConfig registration and enum dispatch
/// in the transform chain. Allows you to register your config struct for the config file.
#[derive(Deserialize, Debug)]
pub enum TransformsConfig {
    #[cfg(feature = "alpha-transforms")]
    KafkaSinkSingle(KafkaSinkSingleConfig),
    CassandraSinkSingle(CassandraSinkSingleConfig),
    CassandraSinkCluster(CassandraSinkClusterConfig),
    RedisSinkSingle(RedisSinkSingleConfig),
    CassandraPeersRewrite(CassandraPeersRewriteConfig),
    RedisCache(RedisConfig),
    Tee(TeeConfig),
    TuneableConsistencyScatter(TuneableConsistencyScatterConfig),
    RedisSinkCluster(RedisSinkClusterConfig),
    RedisClusterPortsRewrite(RedisClusterPortsRewriteConfig),
    RedisTimestampTagger,
    DebugPrinter,
    DebugReturner(DebugReturnerConfig),
    NullSink,
    #[cfg(test)]
    Loopback,
    #[cfg(feature = "alpha-transforms")]
    Protect(ProtectConfig),
    #[cfg(feature = "alpha-transforms")]
    DebugForceParse(DebugForceParseConfig),
    #[cfg(feature = "alpha-transforms")]
    DebugForceEncode(DebugForceEncodeConfig),
    ParallelMap(ParallelMapConfig),
    //PoolConnections(ConnectionBalanceAndPoolConfig),
    Coalesce(CoalesceConfig),
    QueryTypeFilter(QueryTypeFilterConfig),
    QueryCounter(QueryCounterConfig),
    RequestThrottling(RequestThrottlingConfig),
}

impl TransformsConfig {
    #[async_recursion]
    pub async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        match self {
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::KafkaSinkSingle(c) => c.get_builder(chain_name).await,
            TransformsConfig::CassandraSinkSingle(c) => c.get_builder(chain_name).await,
            TransformsConfig::CassandraSinkCluster(c) => c.get_builder(chain_name).await,
            TransformsConfig::CassandraPeersRewrite(c) => c.get_builder().await,
            TransformsConfig::RedisCache(r) => r.get_builder().await,
            TransformsConfig::Tee(t) => t.get_builder().await,
            TransformsConfig::RedisSinkSingle(r) => r.get_builder(chain_name).await,
            TransformsConfig::TuneableConsistencyScatter(c) => c.get_builder().await,
            TransformsConfig::RedisTimestampTagger => {
                Ok(Box::new(RedisTimestampTagger::new()) as Box<dyn TransformBuilder>)
            }
            TransformsConfig::RedisClusterPortsRewrite(r) => r.get_builder().await,
            TransformsConfig::DebugPrinter => {
                Ok(Box::new(DebugPrinter::new()) as Box<dyn TransformBuilder>)
            }
            TransformsConfig::DebugReturner(d) => d.get_builder().await,
            TransformsConfig::NullSink => {
                Ok(Box::<NullSink>::default() as Box<dyn TransformBuilder>)
            }
            #[cfg(test)]
            TransformsConfig::Loopback => {
                Ok(Box::<Loopback>::default() as Box<dyn TransformBuilder>)
            }
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::Protect(p) => p.get_builder().await,
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::DebugForceParse(d) => d.get_builder().await,
            #[cfg(feature = "alpha-transforms")]
            TransformsConfig::DebugForceEncode(d) => d.get_builder().await,
            TransformsConfig::RedisSinkCluster(r) => r.get_builder(chain_name).await,
            TransformsConfig::ParallelMap(s) => s.get_builder().await,
            // TransformsConfig::PoolConnections(s) => s.get_builder().await,
            TransformsConfig::Coalesce(s) => s.get_builder().await,
            TransformsConfig::QueryTypeFilter(s) => s.get_builder().await,
            TransformsConfig::QueryCounter(s) => s.get_builder().await,
            TransformsConfig::RequestThrottling(s) => s.get_builder(),
        }
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &[TransformsConfig],
) -> Result<TransformChainBuilder> {
    let mut transforms: Vec<Box<dyn TransformBuilder>> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_builder(name.clone()).await?)
    }
    Ok(TransformChainBuilder::new(transforms, name))
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`Wrapper.messages`].
#[derive(Debug)]
pub struct Wrapper<'a> {
    pub messages: Messages,
    transforms: TransformIter<'a>,
    pub client_details: String,
    /// Contains the shotover source's ip address and port which the message was received on
    pub local_addr: SocketAddr,
    chain_name: String,
    /// When true transforms must flush any buffered messages into the messages field.
    /// This can occur at any time but will always occur before the transform is destroyed due to either
    /// shotover or the transform's chain shutting down.
    pub flush: bool,
}

#[derive(Debug)]
enum TransformIter<'a> {
    Forwards(IterMut<'a, Transforms>),
    Backwards(Rev<IterMut<'a, Transforms>>),
}

impl<'a> TransformIter<'a> {
    fn new_forwards(transforms: &'a mut [Transforms]) -> TransformIter<'a> {
        TransformIter::Forwards(transforms.iter_mut())
    }

    fn new_backwards(transforms: &'a mut [Transforms]) -> TransformIter<'a> {
        TransformIter::Backwards(transforms.iter_mut().rev())
    }
}

impl<'a> Iterator for TransformIter<'a> {
    type Item = &'a mut Transforms;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TransformIter::Forwards(iter) => iter.next(),
            TransformIter::Backwards(iter) => iter.next(),
        }
    }
}

/// [`Wrapper`] will not (cannot) bring the current list of transforms that it needs to traverse with it
/// This is purely to make it convenient to clone all the data within Wrapper rather than it's transform
/// state.
impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            messages: self.messages.clone(),
            transforms: TransformIter::new_forwards(&mut []),
            client_details: self.client_details.clone(),
            chain_name: self.chain_name.clone(),
            local_addr: self.local_addr,
            flush: self.flush,
        }
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
            None => panic!("The transform chain does not end with a terminating transform. If you want to throw the messages away use a NullSink transform, otherwise use a terminating sink transform to send the messages somewhere.")
        };

        let transform_name = transform.get_name();
        let chain_name = self.chain_name.clone();

        let start = Instant::now();
        let result = CONTEXT_CHAIN_NAME
            .scope(chain_name, transform.transform(self))
            .await
            .map_err(|e| e.context(anyhow!("{transform_name} transform failed")));
        counter!("shotover_transform_total", 1, "transform" => transform_name);
        if result.is_err() {
            counter!("shotover_transform_failures", 1, "transform" => transform_name)
        }
        histogram!("shotover_transform_latency", start.elapsed(),  "transform" => transform_name);
        result
    }

    pub async fn call_next_transform_pushed(mut self) -> ChainResponse {
        let transform = match self.transforms.next() {
            Some(transform) => transform,
            None => return Ok(self.messages),
        };

        let transform_name = transform.get_name();
        let chain_name = self.chain_name.clone();

        let start = Instant::now();
        let result = CONTEXT_CHAIN_NAME
            .scope(chain_name, transform.transform_pushed(self))
            .await;
        counter!("shotover_transform_pushed_total", 1, "transform" => transform_name);
        if result.is_err() {
            counter!("shotover_transform_pushed_failures", 1, "transform" => transform_name)
        }
        histogram!("shotover_transform_pushed_latency", start.elapsed(),  "transform" => transform_name);
        result
    }

    #[cfg(test)]
    pub fn new(m: Messages) -> Self {
        Wrapper {
            messages: m,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_string(),
            local_addr: "127.0.0.1:8000".parse().unwrap(),
            chain_name: "".to_string(),
            flush: false,
        }
    }

    pub fn new_with_chain_name(m: Messages, chain_name: String, local_addr: SocketAddr) -> Self {
        Wrapper {
            messages: m,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_string(),
            local_addr,
            chain_name,
            flush: false,
        }
    }

    pub fn flush_with_chain_name(chain_name: String) -> Self {
        Wrapper {
            messages: vec![],
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".into(),
            // The connection is closed so we need to just fake an address here
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            chain_name,
            flush: true,
        }
    }

    pub fn new_with_client_details(
        m: Messages,
        client_details: String,
        chain_name: String,
        local_addr: SocketAddr,
    ) -> Self {
        Wrapper {
            messages: m,
            transforms: TransformIter::new_forwards(&mut []),
            client_details,
            local_addr,
            chain_name,
            flush: false,
        }
    }

    pub fn reset(&mut self, transforms: &'a mut [Transforms]) {
        self.transforms = TransformIter::new_forwards(transforms);
    }

    pub fn reset_rev(&mut self, transforms: &'a mut [Transforms]) {
        self.transforms = TransformIter::new_backwards(transforms);
    }
}

/// This trait is the primary extension point for Shotover-proxy.
/// A [`Transform`] is a struct that implements the Transform trait and enables you to modify and observe database
/// queries or frames.
/// The trait has one method where you implement the majority of your logic [Transform::transform],
/// however it also includes a setup and naming method.
///
/// Transforms are created on a per TCP connection basis by calling `TransformBuilder::build()`.
/// This means that each member of your struct that implements this trait can be considered private for
/// each TCP connection or connected client. If you wish to share data between all copies of your struct
/// then wrapping a member in an [`Arc<Mutex<_>>`](std::sync::Mutex) will achieve that,
/// but make sure to copy the value from the TransformBuilder to ensure all instances are referring to the same value.
///
/// Once you have created your [`Transform`], you will need to create
/// new enum variants in [Transforms], [TransformBuilder] and [TransformsConfig] to make them configurable in Shotover.
/// Shotover uses a concept called enum dispatch to provide dynamic configuration of transform chains
/// with minimal impact on performance.
///
/// Implementing this trait is usually done using `#[async_trait]` macros.
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
    /// [`crate::transforms::cassandra::sink_single::CassandraSinkSingle`]. This type of transform
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
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse;

    /// This method should be should be implemented by your transform if it is required to process pushed messages (typically events
    /// or messages that your source is subscribed to. The wrapper object contains the queries/frames
    /// in a [`Vec<Message`](crate::message::Message).
    ///
    /// This transform method is not the same request/response model as the other transform method.
    /// This method processes one pushed message before sending it in reverse on the chain back to the source.
    ///
    /// You can modify the messages in the wrapper struct to achieve your own designs. Your transform can
    /// also modify the response from `message_wrapper.call_next_transform_pushed` if it needs to. As long as the message
    /// carries on through the chain, it will function correctly. You are able to add or remove messages as this method is not expecting
    /// request/response pairs.
    ///
    /// ## Invariants
    /// * _Non-terminating_ - Your `transform_pushed` method should not be terminating as the messages should get passed back to the source, where they will terminate.
    async fn transform_pushed<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let response = message_wrapper.call_next_transform_pushed().await?;
        Ok(response)
    }

    fn set_pushed_messages_tx(&mut self, _pushed_messages_tx: mpsc::UnboundedSender<Messages>) {}
}

pub type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + Send + Sync>>;
