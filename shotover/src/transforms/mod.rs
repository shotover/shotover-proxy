//! Various types required for defining a transform

use crate::message::Messages;
use crate::transforms::cassandra::peers_rewrite::CassandraPeersRewrite;
use crate::transforms::cassandra::sink_cluster::CassandraSinkCluster;
use crate::transforms::cassandra::sink_single::CassandraSinkSingle;
use crate::transforms::coalesce::Coalesce;
use crate::transforms::debug::force_parse::DebugForceParse;
use crate::transforms::debug::log_to_file::DebugLogToFile;
use crate::transforms::debug::printer::DebugPrinter;
use crate::transforms::debug::random_delay::DebugRandomDelay;
use crate::transforms::debug::returner::DebugReturner;
use crate::transforms::distributed::tuneable_consistency_scatter::TuneableConsistentencyScatter;
use crate::transforms::filter::QueryTypeFilter;
use crate::transforms::kafka::sink_cluster::KafkaSinkCluster;
use crate::transforms::kafka::sink_single::KafkaSinkSingle;
use crate::transforms::load_balance::ConnectionBalanceAndPool;
use crate::transforms::loopback::Loopback;
use crate::transforms::null::NullSink;
use crate::transforms::parallel_map::ParallelMap;
use crate::transforms::protect::Protect;
use crate::transforms::query_counter::QueryCounter;
use crate::transforms::redis::cache::SimpleRedisCache;
use crate::transforms::redis::cluster_ports_rewrite::RedisClusterPortsRewrite;
use crate::transforms::redis::sink_cluster::RedisSinkCluster;
use crate::transforms::redis::sink_single::RedisSinkSingle;
use crate::transforms::redis::timestamp_tagging::RedisTimestampTagger;
use crate::transforms::tee::Tee;
use crate::transforms::throttling::RequestThrottling;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use core::fmt;
use futures::Future;
use metrics::{counter, histogram};
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

pub trait TransformBuilder: Send + Sync {
    fn build(&self) -> Transforms;

    fn get_name(&self) -> &'static str;

    fn validate(&self) -> Vec<String> {
        vec![]
    }

    fn is_terminating(&self) -> bool {
        false
    }
}

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
    KafkaSinkCluster(KafkaSinkCluster),
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
    DebugLogToFile(DebugLogToFile),
    DebugForceParse(DebugForceParse),
    ParallelMap(ParallelMap),
    PoolConnections(ConnectionBalanceAndPool),
    Coalesce(Coalesce),
    QueryTypeFilter(QueryTypeFilter),
    QueryCounter(QueryCounter),
    RequestThrottling(RequestThrottling),
    Custom(Box<dyn Transform>),
}

impl Debug for Transforms {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transform: {}", self.get_name())
    }
}

impl Transforms {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match self {
            Transforms::KafkaSinkSingle(c) => c.transform(requests_wrapper).await,
            Transforms::KafkaSinkCluster(c) => c.transform(requests_wrapper).await,
            Transforms::CassandraSinkSingle(c) => c.transform(requests_wrapper).await,
            Transforms::CassandraSinkCluster(c) => c.transform(requests_wrapper).await,
            Transforms::CassandraPeersRewrite(c) => c.transform(requests_wrapper).await,
            Transforms::RedisCache(r) => r.transform(requests_wrapper).await,
            Transforms::Tee(m) => m.transform(requests_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform(requests_wrapper).await,
            Transforms::DebugLogToFile(p) => p.transform(requests_wrapper).await,
            Transforms::DebugForceParse(p) => p.transform(requests_wrapper).await,
            Transforms::NullSink(n) => n.transform(requests_wrapper).await,
            Transforms::Loopback(n) => n.transform(requests_wrapper).await,
            Transforms::Protect(p) => p.transform(requests_wrapper).await,
            Transforms::DebugReturner(p) => p.transform(requests_wrapper).await,
            Transforms::DebugRandomDelay(p) => p.transform(requests_wrapper).await,
            Transforms::TuneableConsistencyScatter(tc) => tc.transform(requests_wrapper).await,
            Transforms::RedisSinkSingle(r) => r.transform(requests_wrapper).await,
            Transforms::RedisTimestampTagger(r) => r.transform(requests_wrapper).await,
            Transforms::RedisClusterPortsRewrite(r) => r.transform(requests_wrapper).await,
            Transforms::RedisSinkCluster(r) => r.transform(requests_wrapper).await,
            Transforms::ParallelMap(s) => s.transform(requests_wrapper).await,
            Transforms::PoolConnections(s) => s.transform(requests_wrapper).await,
            Transforms::Coalesce(s) => s.transform(requests_wrapper).await,
            Transforms::QueryTypeFilter(s) => s.transform(requests_wrapper).await,
            Transforms::QueryCounter(s) => s.transform(requests_wrapper).await,
            Transforms::RequestThrottling(s) => s.transform(requests_wrapper).await,
            Transforms::Custom(s) => s.transform(requests_wrapper).await,
        }
    }

    async fn transform_pushed<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match self {
            Transforms::KafkaSinkSingle(c) => c.transform_pushed(requests_wrapper).await,
            Transforms::KafkaSinkCluster(c) => c.transform_pushed(requests_wrapper).await,
            Transforms::CassandraSinkSingle(c) => c.transform_pushed(requests_wrapper).await,
            Transforms::CassandraSinkCluster(c) => c.transform_pushed(requests_wrapper).await,
            Transforms::CassandraPeersRewrite(c) => c.transform_pushed(requests_wrapper).await,
            Transforms::RedisCache(r) => r.transform_pushed(requests_wrapper).await,
            Transforms::Tee(m) => m.transform_pushed(requests_wrapper).await,
            Transforms::DebugPrinter(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::DebugLogToFile(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::DebugForceParse(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::NullSink(n) => n.transform_pushed(requests_wrapper).await,
            Transforms::Loopback(n) => n.transform_pushed(requests_wrapper).await,
            Transforms::Protect(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::DebugReturner(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::DebugRandomDelay(p) => p.transform_pushed(requests_wrapper).await,
            Transforms::TuneableConsistencyScatter(tc) => {
                tc.transform_pushed(requests_wrapper).await
            }
            Transforms::RedisSinkSingle(r) => r.transform_pushed(requests_wrapper).await,
            Transforms::RedisTimestampTagger(r) => r.transform_pushed(requests_wrapper).await,
            Transforms::RedisClusterPortsRewrite(r) => r.transform_pushed(requests_wrapper).await,
            Transforms::RedisSinkCluster(r) => r.transform_pushed(requests_wrapper).await,
            Transforms::ParallelMap(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::PoolConnections(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::Coalesce(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::QueryTypeFilter(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::QueryCounter(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::RequestThrottling(s) => s.transform_pushed(requests_wrapper).await,
            Transforms::Custom(s) => s.transform_pushed(requests_wrapper).await,
        }
    }

    fn get_name(&self) -> &'static str {
        self.into()
    }

    fn set_pushed_messages_tx(&mut self, pushed_messages_tx: mpsc::UnboundedSender<Messages>) {
        match self {
            Transforms::KafkaSinkSingle(c) => c.set_pushed_messages_tx(pushed_messages_tx),
            Transforms::KafkaSinkCluster(c) => c.set_pushed_messages_tx(pushed_messages_tx),
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
            Transforms::DebugLogToFile(p) => p.set_pushed_messages_tx(pushed_messages_tx),
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
            Transforms::Custom(d) => d.set_pushed_messages_tx(pushed_messages_tx),
        }
    }
}

#[typetag::deserialize]
#[async_trait(?Send)]
pub trait TransformConfig: Debug {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>>;
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`wrapper.requests`].
#[derive(Debug)]
pub struct Wrapper<'a> {
    pub requests: Messages,
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
            requests: self.requests.clone(),
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
    pub async fn call_next_transform(mut self) -> Result<Messages> {
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

    pub async fn call_next_transform_pushed(mut self) -> Result<Messages> {
        let transform = match self.transforms.next() {
            Some(transform) => transform,
            None => return Ok(self.requests),
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
    pub fn new(requests: Messages) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_string(),
            local_addr: "127.0.0.1:8000".parse().unwrap(),
            chain_name: "".to_string(),
            flush: false,
        }
    }

    pub fn new_with_chain_name(
        requests: Messages,
        chain_name: String,
        local_addr: SocketAddr,
    ) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_string(),
            local_addr,
            chain_name,
            flush: false,
        }
    }

    pub fn flush_with_chain_name(chain_name: String) -> Self {
        Wrapper {
            requests: vec![],
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".into(),
            // The connection is closed so we need to just fake an address here
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            chain_name,
            flush: true,
        }
    }

    pub fn new_with_client_details(
        requests: Messages,
        client_details: String,
        chain_name: String,
        local_addr: SocketAddr,
    ) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details,
            local_addr,
            chain_name,
            flush: false,
        }
    }

    /// Helper for writing debug logs while testing
    /// feature flagged behind alpha-transforms as this call is too expensive to ever make it into production
    #[cfg(feature = "alpha-transforms")]
    pub fn messages_to_high_level_string(&mut self) -> String {
        let messages = self
            .requests
            .iter_mut()
            .map(|x| x.to_high_level_string())
            .collect::<Vec<_>>();
        format!("{:?}", messages)
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
/// new enum variants in [Transforms].
/// And implement the [TransformBuilder] and [TransformConfig] traits to make them configurable in Shotover.
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
    /// Shotover expects the same number of messages in [`wrapper.requests`](crate::transforms::Wrapper) to be returned as was passed
    /// into the method via the parameter requests_wrapper. For in order protocols (such as Redis) you will
    /// also need to ensure the order of responses matches the order of the queries.
    ///
    /// You can modify the messages in the wrapper struct to achieve your own designs. Your transform
    /// can also modify the response from `requests_wrapper.call_next_transform()` if it needs
    /// to. As long as you return the same number of messages as you received, you won't break behavior
    /// from other transforms.
    ///
    /// ## Invariants
    /// Your transform method at a minimum needs to
    /// * _Non-terminating_ - If your transform does not send the message to an external system or generate its own response to the query,
    /// it will need to call and return the response from `requests_wrapper.call_next_transform()`. This ensures that your
    /// transform will call any subsequent downstream transforms without needing to know about what they
    /// do. This type of transform is called an non-terminating transform.
    /// * _Terminating_ - Your transform can also choose not to call `requests_wrapper.call_next_transform()` if it sends the
    /// messages to an external system or generates its own response to the query e.g.
    /// [`crate::transforms::cassandra::sink_single::CassandraSinkSingle`]. This type of transform
    /// is called a Terminating transform (as no subsequent transforms in the chain will be called).
    /// * _Message count_ - requests_wrapper.requests will contain 0 or more messages.
    /// Your transform should return the same number of responses as messages received in requests_wrapper.requests. Transforms that
    /// don't do this explicitly for each call, should return the same number of responses as messages it receives over the lifetime
    /// of the transform chain. A good example of this is the [`crate::transforms::coalesce::Coalesce`] transform. The
    /// [`crate::transforms::sampler::Sampler`] transform is also another example of this, with a slightly different twist.
    /// The number of responses will be the sames as the number of messages, as the sampled messages are sent to a subchain rather than
    /// changing the behavior of the main chain.
    ///
    /// ## Naming
    /// Transforms also have different naming conventions.
    /// * Transforms that interact with an external system are called Sinks.
    /// * Transforms that don't call subsequent chains via `requests_wrapper.call_next_transform()` are called terminating transforms.
    /// * Transforms that do call subsquent chains via `requests_wrapper.call_next_transform()` are non-terminating transforms.
    ///
    /// You can have have a transforms that is both non-terminating and a sink.
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages>;

    /// This method should be should be implemented by your transform if it is required to process pushed messages (typically events
    /// or messages that your source is subscribed to. The wrapper object contains the queries/frames
    /// in a [`Vec<Message`](crate::message::Message).
    ///
    /// This transform method is not the same request/response model as the other transform method.
    /// This method processes one pushed message before sending it in reverse on the chain back to the source.
    ///
    /// You can modify the messages in the wrapper struct to achieve your own designs. Your transform can
    /// also modify the response from `requests_wrapper.call_next_transform_pushed` if it needs to. As long as the message
    /// carries on through the chain, it will function correctly. You are able to add or remove messages as this method is not expecting
    /// request/response pairs.
    ///
    /// ## Invariants
    /// * _Non-terminating_ - Your `transform_pushed` method should not be terminating as the messages should get passed back to the source, where they will terminate.
    async fn transform_pushed<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let response = requests_wrapper.call_next_transform_pushed().await?;
        Ok(response)
    }

    fn set_pushed_messages_tx(&mut self, _pushed_messages_tx: mpsc::UnboundedSender<Messages>) {}
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + Send + Sync>>;
