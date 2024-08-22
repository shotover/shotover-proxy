//! Various types required for defining a transform

use self::chain::TransformAndMetrics;
use crate::frame::MessageType;
use crate::message::{Message, MessageIdMap, Messages};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::Future;
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::slice::IterMut;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::Instant;

#[cfg(feature = "cassandra")]
pub mod cassandra;
pub mod chain;
pub mod coalesce;
pub mod debug;
pub mod filter;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod load_balance;
pub mod loopback;
pub mod null;
#[cfg(all(feature = "alpha-transforms", feature = "opensearch"))]
pub mod opensearch;
pub mod parallel_map;
#[cfg(all(feature = "alpha-transforms", feature = "cassandra"))]
pub mod protect;
pub mod query_counter;
#[cfg(feature = "redis")]
pub mod redis;
pub mod tee;
#[cfg(feature = "cassandra")]
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

    /// IP address of the client
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
    /// Builds a single instance of the transform.
    /// Shotover will create a new transform instance by calling this method for every time this transform is configured in the `topology.yaml`.
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform>;

    /// Name of the transform used in logs and displayed to the user
    fn get_name(&self) -> &'static str;

    /// Transform specific validation can be implemented here.
    /// Any strings returned are considered a validation error that will be logged and cause shotover to fail to startup.
    fn validate(&self) -> Vec<String> {
        vec![]
    }

    // TODO: remove in favor of down_chain_protocol
    fn is_terminating(&self) -> bool {
        false
    }
}

/// Defines the configuration fields of a transform as they appear in the `topology.yaml`,
/// along with other metadata provided as implemented trait methods
#[typetag::serde]
#[async_trait(?Send)]
pub trait TransformConfig: Debug {
    /// Returns the builder used to create instances of this transform.
    /// Shotover will only call this method once.
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>>;

    /// Defines which protocols this transform will:
    /// * Accept requests as
    /// * Send responses as
    /// Shotover will use the results of this method to validate that a transform
    /// is compatible with the other transforms it is connected to.
    /// If the configuration is invalid shotover will log the issue and fail to start.
    fn up_chain_protocol(&self) -> UpChainProtocol;

    /// Defines which protocols this transform will:
    /// * Accept responses as
    /// * Send requests as
    /// Shotover will use the results of this method to validate that a transform
    /// is compatible with the other transforms and sources it is connected to.
    /// If the configuration is invalid shotover will log the issue and fail to start.
    fn down_chain_protocol(&self) -> DownChainProtocol;
}

/// Defines which protocols a transform will:
/// * Accept requests as
/// * Send responses as
pub enum UpChainProtocol {
    /// This transform will only accept the specified protocols from up chain.
    MustBeOneOf(Vec<MessageType>),
    /// This transform will accept any protocol from up chain.
    Any,
}

/// Defines which protocols a transform will:
/// * Accept responses as
/// * Send requests as
pub enum DownChainProtocol {
    /// The protocol sent down the chain will be this protocol.
    TransformedTo(MessageType),
    /// The protocol sent down the chain will be the same protocol received from up chain.
    SameAsUpChain,
    /// The transform is terminating and so it does not send any messages down chain.
    Terminating,
}

/// Provides extra context that may be needed when creating a TransformBuilder
#[derive(Clone)]
pub struct TransformContextConfig {
    /// The name of the chain that this transform is configured in.
    pub chain_name: String,
    // TODO: rename to up_chain_protocol
    /// The protocol that the transform will receive requests in.
    pub protocol: MessageType,
}

/// The [`Wrapper`] struct is passed into each transform and contains a list of mutable references to the
/// remaining transforms that will process the messages attached to this [`Wrapper`].
/// Most [`Transform`] authors will only be interested in [`wrapper.requests`].
pub struct ChainState<'a> {
    /// Requests received from the client
    pub requests: Messages,
    transforms: IterMut<'a, TransformAndMetrics>,
    /// Contains the shotover source's ip address and port which the message was received on
    pub local_addr: SocketAddr,
    /// When true transforms must flush any buffered messages into the messages field.
    /// This can occur at any time but will always occur before the transform is destroyed due to either
    /// shotover or the transform's chain shutting down.
    /// The one exception is if [`Wrapper::close_client_connection`] was set to true, in which case no flush occurs.
    pub flush: bool,
    /// Set to false by default.
    /// Transforms can set this to true to force the connection to the client to be closed after the stack of `Transform::transform` calls returns.
    /// When closed in this way, the chain will not be flushed and no further calls to the chain will be made before it is dropped.
    pub close_client_connection: bool,
}

/// [`Wrapper`] will not (cannot) bring the current list of transforms that it needs to traverse with it
/// This is purely to make it convenient to clone all the data within Wrapper rather than it's transform
/// state.
impl<'a> Clone for ChainState<'a> {
    fn clone(&self) -> Self {
        ChainState {
            requests: self.requests.clone(),
            transforms: [].iter_mut(),
            local_addr: self.local_addr,
            flush: self.flush,
            close_client_connection: self.close_client_connection,
        }
    }
}

impl<'shorter, 'longer: 'shorter> ChainState<'longer> {
    fn take(&mut self) -> Self {
        ChainState {
            requests: std::mem::take(&mut self.requests),
            transforms: std::mem::take(&mut self.transforms),
            local_addr: self.local_addr,
            flush: self.flush,
            close_client_connection: self.close_client_connection,
        }
    }

    /// This function will take a mutable reference to the next transform out of the [`Wrapper`] structs
    /// vector of transform references. It then sets up the chain name and transform name in the local
    /// thread scope for structured logging.
    ///
    /// It then calls the next [Transform], recording the number of successes and failures in a metrics counter. It also measures
    /// the execution time of the [Transform::transform] function as a metrics latency histogram.
    ///
    /// The result of calling the next transform is then provided as a response.
    pub async fn call_next_transform(&'shorter mut self) -> Result<Messages> {
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

    pub fn clone_requests_into_hashmap(&self, destination: &mut MessageIdMap<Message>) {
        for request in &self.requests {
            destination.insert(request.id(), request.clone());
        }
    }

    #[cfg(test)]
    pub fn new_test(requests: Messages) -> Self {
        ChainState {
            requests,
            transforms: [].iter_mut(),
            local_addr: DUMMY_ADDRESS,
            flush: false,
            close_client_connection: false,
        }
    }

    pub fn new_with_addr(requests: Messages, local_addr: SocketAddr) -> Self {
        ChainState {
            requests,
            transforms: [].iter_mut(),
            local_addr,
            flush: false,
            close_client_connection: false,
        }
    }

    pub fn flush() -> Self {
        ChainState {
            requests: vec![],
            transforms: [].iter_mut(),
            // The connection is closed so we need to just fake an address here
            local_addr: DUMMY_ADDRESS,
            flush: true,
            close_client_connection: false,
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

    pub fn reset(&mut self, transforms: &'longer mut [TransformAndMetrics]) {
        self.transforms = transforms.iter_mut();
    }
}

const DUMMY_ADDRESS: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));

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
    /// * contained in chain_state.requests
    ///     + these are the requests that will flow into the next transform in the chain.
    /// * contained in the return value of `chain_state.call_next_transform()`
    ///     + These are the responses that will flow back to the previous transform in the chain.
    ///
    /// But while doing so, also make sure to follow the below invariants when modifying the messages.
    ///
    /// # Invariants
    ///
    /// * Non-terminating specific invariants
    ///     + If your transform does not send the message to an external system or generate its own response to the query,
    /// it will need to call and return the response from `chain_state.call_next_transform()`.
    ///     + This ensures that your transform will call any subsequent downstream transforms without needing to know about what they
    /// do. This type of transform is called a non-terminating transform.
    ///
    /// * Terminating specific invariants
    ///     + Your transform can also choose not to call `chain_state.call_next_transform()` if it sends the
    /// messages to an external system or generates its own response to the query e.g. `CassandraSinkSingle`.
    ///     + This type of transform is called a Terminating transform (as no subsequent transforms in the chain will be called).
    ///
    /// * Request/Response invariants:
    ///     + Transforms must ensure that each request that passes through the transform has a corresponding response returned for it.
    ///         - A response/request pair can be identified by calling `request_id()` on a response and matching that to the `id()` of a previous request.
    ///         - The response does not need to be returned within the same call to [`Transform::transform`] that the request was encountered.
    ///           But it must be returned eventually over the lifetime of the transform.
    ///         - If a transform deletes a request it must return a simulated response message with its request_id set to the deleted request.
    ///             * For in order protocols: this simulated message must be in the correct location within the list of responses
    ///                 - The best way to achieve this is storing the [`crate::message::MessageId`] of the message before the deleted message.
    ///         - If a transform introduces a new request into the chain_state the response must be located and
    ///           removed from the list of returned responses.
    ///     + For in order protocols, transforms must ensure that responses are kept in the same order in which they are received.
    ///         - When writing protocol generic transforms: always ensure this is upheld.
    ///         - When writing a transform specific to a protocol that is out of order: you can disregard this requirement
    ///             * This is currently only cassandra
    ///
    /// * Err response:
    ///     + When [`Transform::transform`] returns `Err`, shotover will never call [`Transform::transform`] on this transform instance again
    ///       and will close the connection with the client after attempting to generate error responses for any pending requests.
    ///       So it is ok to leave the transform in an invalid state when returning `Err`.
    ///
    /// # Naming
    /// Transform also have different naming conventions.
    /// * Transform that interact with an external system are called Sinks.
    /// * Transform that don't call subsequent chains via `chain_state.call_next_transform()` are called terminating transforms.
    /// * Transform that do call subsquent chains via `chain_state.call_next_transform()` are non-terminating transforms.
    ///
    /// You can have have a transform that is both non-terminating and a sink.
    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages>;

    /// Name of the transform used in logs and displayed to the user
    fn get_name(&self) -> &'static str;
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<util::Response>> + Send + Sync>>;
