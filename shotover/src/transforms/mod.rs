//! Various types required for defining a transform

use self::chain::TransformAndMetrics;
use crate::frame::MessageType;
use crate::message::{Message, MessageIdMap, Messages};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use core::fmt;
use futures::Future;
use std::fmt::{Debug, Formatter};
use std::iter::Rev;
use std::net::SocketAddr;
use std::pin::Pin;
use std::slice::IterMut;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::time::Instant;

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

/// Provides extra context that may be needed when creating a Transform
#[derive(Clone, Debug)]
pub struct TransformContextBuilder {
    /// The chain is run naturally whenever messages are received from the client.
    /// However, for various reasons, a transform may want to force a chain run.
    /// This can be done by calling `notify_one` on this field.
    ///
    /// For example:
    /// * This must be used when a sink transform has asynchronously received responses in the background
    /// * This should be used when a transform needs to generate or flush messages after some kind of timeout or background process completes.
    pub force_run_chain: Arc<Notify>,
    pub client_details: String,
}

#[allow(clippy::new_without_default)]
impl TransformContextBuilder {
    pub fn new_test() -> Self {
        TransformContextBuilder {
            force_run_chain: Arc::new(Notify::new()),
            client_details: String::new(),
        }
    }
}

pub trait TransformBuilder: Send + Sync {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform>;

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
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>>;
}

/// Provides extra context that may be needed when creating a TransformBuilder
#[derive(Clone)]
pub struct TransformContextConfig {
    pub chain_name: String,
    pub protocol: MessageType,
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`wrapper.requests`].
pub struct Wrapper<'a> {
    pub requests: Messages,
    transforms: TransformIter<'a>,
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

    pub fn clone_requests_into_hashmap(&self, destination: &mut MessageIdMap<Message>) {
        for request in &self.requests {
            destination.insert(request.id(), request.clone());
        }
    }

    #[cfg(test)]
    pub fn new_test(requests: Messages) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            local_addr: "127.0.0.1:8000".parse().unwrap(),
            flush: false,
        }
    }

    pub fn new_with_addr(requests: Messages, local_addr: SocketAddr) -> Self {
        Wrapper {
            requests,
            transforms: TransformIter::new_forwards(&mut []),
            local_addr,
            flush: false,
        }
    }

    pub fn flush() -> Self {
        Wrapper {
            requests: vec![],
            transforms: TransformIter::new_forwards(&mut []),
            // The connection is closed so we need to just fake an address here
            local_addr: "127.0.0.1:10000".parse().unwrap(),
            flush: true,
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
    /// In order to implement your transform you can modify the messages:
    /// * contained in requests_wrapper.requests
    ///     + these are the requests that will flow into the next transform in the chain.
    /// * contained in the return value of `requests_wrapper.call_next_transform()`
    ///     + These are the responses that will flow back to the previous transform in the chain.
    ///
    /// But while doing so, also make sure to follow the below invariants when modifying the messages.
    ///
    /// # Invariants
    ///
    /// * Non-terminating specific invariants
    ///     + If your transform does not send the message to an external system or generate its own response to the query,
    /// it will need to call and return the response from `requests_wrapper.call_next_transform()`.
    ///     + This ensures that your transform will call any subsequent downstream transforms without needing to know about what they
    /// do. This type of transform is called a non-terminating transform.
    ///
    /// * Terminating specific invariants
    ///     + Your transform can also choose not to call `requests_wrapper.call_next_transform()` if it sends the
    /// messages to an external system or generates its own response to the query e.g.
    /// [`crate::transforms::cassandra::sink_single::CassandraSinkSingle`].
    ///     + This type of transform is called a Terminating transform (as no subsequent transforms in the chain will be called).
    ///
    /// * Request/Response invariants:
    ///     + Transforms must ensure that each request that passes through the transform has a corresponding response returned for it.
    ///         - A response/request pair can be identified by calling `request_id()` on a response and matching that to the `id()` of a previous request.
    ///         - The response does not need to be returned within the same call to [`Transform::transform`] that the request was encountered.
    ///           But it must be returned eventually over the lifetime of the transform.
    ///         - If a transform deletes a request it must return a simulated response message with its request_id set to the deleted request.
    ///             * For in order protocols: this simulated message must be in the correct location within the list of responses
    ///                 - The best way to achieve this is storing the [`MessageId`] of the message before the deleted message.
    ///         - If a transform introduces a new request into the requests_wrapper the response must be located and
    ///           removed from the list of returned responses.
    ///     + For in order protocols, transforms must ensure that responses are kept in the same order in which they are received.
    ///         - When writing protocol generic transforms: always ensure this is upheld.
    ///         - When writing a transform specific to a protocol that is out of order: you can disregard this requirement
    ///             * This is currently only cassandra
    ///
    /// # Naming
    /// Transform also have different naming conventions.
    /// * Transform that interact with an external system are called Sinks.
    /// * Transform that don't call subsequent chains via `requests_wrapper.call_next_transform()` are called terminating transforms.
    /// * Transform that do call subsquent chains via `requests_wrapper.call_next_transform()` are non-terminating transforms.
    ///
    /// You can have have a transform that is both non-terminating and a sink.
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages>;

    /// TODO: This method should be removed and integrated with `Transform::transform` once we properly support out of order protocols.
    ///
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
    /// # Invariants
    /// * _Non-terminating_ - Your `transform_pushed` method should not be terminating as the messages should get passed back to the source, where they will terminate.
    async fn transform_pushed<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let response = requests_wrapper.call_next_transform_pushed().await?;
        Ok(response)
    }

    fn get_name(&self) -> &'static str;

    fn set_pushed_messages_tx(&mut self, _pushed_messages_tx: mpsc::UnboundedSender<Messages>) {}
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + Send + Sync>>;
