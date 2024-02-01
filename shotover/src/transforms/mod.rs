//! Various types required for defining a transform

use crate::message::Messages;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use core::fmt;
use futures::Future;
use std::fmt::{Debug, Formatter};
use std::iter::Rev;
use std::net::SocketAddr;
use std::pin::Pin;
use std::slice::IterMut;
use tokio::sync::mpsc;
use tokio::time::Instant;

use self::chain::TransformAndMetrics;

#[cfg(feature = "cassandra")]
pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod debug;
#[cfg(feature = "redis")]
pub mod distributed;
pub mod filter;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod load_balance;
pub mod loopback;
pub mod noop;
pub mod null;
#[cfg(all(feature = "alpha-transforms", feature = "opensearch"))]
pub mod opensearch;
pub mod parallel_map;
#[cfg(feature = "cassandra")]
pub mod protect;
pub mod query_counter;
#[cfg(feature = "redis")]
pub mod redis;
pub mod sampler;
pub mod tee;
pub mod throttling;
pub mod util;

pub trait TransformBuilder: Send + Sync {
    fn build(&self) -> Box<dyn Transform>;

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

#[typetag::serde]
#[async_trait(?Send)]
pub trait TransformConfig: Debug {
    async fn get_builder(&self, chain_name: String) -> Result<Box<dyn TransformBuilder>>;
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`wrapper.requests`].
pub struct Wrapper<'a> {
    pub requests: Messages,
    transforms: TransformIter<'a>,
    pub client_details: String,
    /// Contains the shotover source's ip address and port which the message was received on
    pub local_addr: SocketAddr,
    /// When true transforms must flush any buffered messages into the messages field.
    /// This can occur at any time but will always occur before the transform is destroyed due to either
    /// shotover or the transform's chain shutting down.
    pub flush: bool,
}

enum TransformIter<'a> {
    Forwards(IterMut<'a, TransformAndMetrics>),
    Backwards(Rev<IterMut<'a, TransformAndMetrics>>),
}

impl<'a> TransformIter<'a> {
    fn new_forwards(transforms: &'a mut [TransformAndMetrics]) -> TransformIter<'a> {
        TransformIter::Forwards(transforms.iter_mut())
    }

    fn new_backwards(transforms: &'a mut [TransformAndMetrics]) -> TransformIter<'a> {
        TransformIter::Backwards(transforms.iter_mut().rev())
    }
}

impl<'a> Iterator for TransformIter<'a> {
    type Item = &'a mut TransformAndMetrics;

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
            local_addr: self.local_addr,
            flush: self.flush,
        }
    }
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
        let TransformAndMetrics {
            transform,
            transform_total,
            transform_failures,
            transform_latency,
            ..
        } = match self.transforms.next() {
            Some(transform) => transform,
            None => panic!("The transform chain does not end with a terminating transform. If you want to throw the messages away use a NullSink transform, otherwise use a terminating sink transform to send the messages somewhere.")
        };

        let transform_name = transform.get_name();

        let start = Instant::now();
        let result = transform
            .transform(self)
            .await
            .map_err(|e| e.context(anyhow!("{transform_name} transform failed")));
        transform_total.increment(1);
        if result.is_err() {
            transform_failures.increment(1);
        }
        transform_latency.record(start.elapsed());
        result
    }

    pub async fn call_next_transform_pushed(mut self) -> Result<Messages> {
        let TransformAndMetrics {
            transform,
            transform_pushed_total,
            transform_pushed_failures,
            transform_pushed_latency,
            ..
        } = match self.transforms.next() {
            Some(transform) => transform,
            None => return Ok(self.requests),
        };

        let transform_name = transform.get_name();

        let start = Instant::now();
        let result = transform
            .transform_pushed(self)
            .await
            .map_err(|e| e.context(anyhow!("{transform_name} transform failed")));
        transform_pushed_total.increment(1);
        if result.is_err() {
            transform_pushed_failures.increment(1);
        }
        transform_pushed_latency.record(start.elapsed());
        result
    }

    #[cfg(test)]
    pub fn new_test(requests: Messages) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_owned(),
            local_addr: "127.0.0.1:8000".parse().unwrap(),
            flush: false,
        }
    }

    pub fn new_with_addr(requests: Messages, local_addr: SocketAddr) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_owned(),
            local_addr,
            flush: false,
        }
    }

    pub fn flush() -> Self {
        Wrapper {
            requests: vec![],
            transforms: TransformIter::new_forwards(&mut []),
            client_details: "".to_owned(),
            // The connection is closed so we need to just fake an address here
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            flush: true,
        }
    }

    pub fn new_with_client_details(
        requests: Messages,
        client_details: String,
        local_addr: SocketAddr,
    ) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            client_details,
            local_addr,
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

    pub fn reset(&mut self, transforms: &'a mut [TransformAndMetrics]) {
        self.transforms = TransformIter::new_forwards(transforms);
    }

    pub fn reset_rev(&mut self, transforms: &'a mut [TransformAndMetrics]) {
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
/// Once you have created your [`Transform`], you will need to implement the [TransformBuilder] and [TransformConfig] traits
/// to make them configurable in Shotover.
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
    /// Your transform should return the same number of responses as messages received in requests_wrapper.requests. Transform that
    /// don't do this explicitly for each call, should return the same number of responses as messages it receives over the lifetime
    /// of the transform chain. A good example of this is the [`crate::transforms::coalesce::Coalesce`] transform. The
    /// [`crate::transforms::sampler::Sampler`] transform is also another example of this, with a slightly different twist.
    /// The number of responses will be the sames as the number of messages, as the sampled messages are sent to a subchain rather than
    /// changing the behavior of the main chain.
    ///
    /// ## Naming
    /// Transform also have different naming conventions.
    /// * Transform that interact with an external system are called Sinks.
    /// * Transform that don't call subsequent chains via `requests_wrapper.call_next_transform()` are called terminating transforms.
    /// * Transform that do call subsquent chains via `requests_wrapper.call_next_transform()` are non-terminating transforms.
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

    fn get_name(&self) -> &'static str;

    fn set_pushed_messages_tx(&mut self, _pushed_messages_tx: mpsc::UnboundedSender<Messages>) {}
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + Send + Sync>>;
