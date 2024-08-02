use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::config::chain::TransformChainConfig;
use crate::http::HttpServerError;
use crate::message::{Message, MessageIdMap, Messages};
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use atomic_enum::atomic_enum;
use axum::extract::State;
use axum::response::Html;
use axum::Router;
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::Ordering;
use std::{net::SocketAddr, str, sync::Arc};
use tracing::{debug, error, trace, warn};

struct TeeBuilder {
    tx: TransformChainBuilder,
    buffer_size: usize,
    behavior: ConsistencyBehaviorBuilder,
    timeout_micros: Option<u64>,
    dropped_messages: Counter,
    result_source: Arc<AtomicResultSource>,
    protocol_is_inorder: bool,
}

enum ConsistencyBehaviorBuilder {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(TransformChainBuilder),
}

impl TeeBuilder {
    fn new(
        tx: TransformChainBuilder,
        buffer_size: usize,
        behavior: ConsistencyBehaviorBuilder,
        timeout_micros: Option<u64>,
        switch_port: Option<u16>,
        protocol_is_inorder: bool,
    ) -> Self {
        let result_source = Arc::new(AtomicResultSource::new(ResultSource::RegularChain));

        if let Some(switch_port) = switch_port {
            let chain_switch_listener =
                ChainSwitchListener::new(SocketAddr::from(([127, 0, 0, 1], switch_port)));
            tokio::spawn(chain_switch_listener.async_run(result_source.clone()));
        }

        let dropped_messages = counter!("shotover_tee_dropped_messages_count", "chain" => "Tee");

        TeeBuilder {
            tx,
            buffer_size,
            behavior,
            timeout_micros,
            dropped_messages,
            result_source,
            protocol_is_inorder,
        }
    }
}

impl TransformBuilder for TeeBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(Tee {
            tx: self
                .tx
                .build_buffered(self.buffer_size, transform_context.clone()),
            behavior: match &self.behavior {
                ConsistencyBehaviorBuilder::Ignore => ConsistencyBehavior::Ignore,
                ConsistencyBehaviorBuilder::LogWarningOnMismatch => {
                    ConsistencyBehavior::LogWarningOnMismatch
                }
                ConsistencyBehaviorBuilder::FailOnMismatch => ConsistencyBehavior::FailOnMismatch,
                ConsistencyBehaviorBuilder::SubchainOnMismatch(chain) => {
                    ConsistencyBehavior::SubchainOnMismatch(
                        chain.build_buffered(self.buffer_size, transform_context),
                        Default::default(),
                    )
                }
            },
            timeout_micros: self.timeout_micros,
            dropped_messages: self.dropped_messages.clone(),
            result_source: self.result_source.clone(),
            incoming_responses: if self.protocol_is_inorder {
                IncomingResponses::InOrder {
                    tee: VecDeque::new(),
                    chain: VecDeque::new(),
                }
            } else {
                IncomingResponses::OutOfOrder {
                    tee_by_request_id: Default::default(),
                    chain_by_request_id: Default::default(),
                }
            },
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn validate(&self) -> Vec<String> {
        let mut errors = self
            .tx
            .validate()
            .iter()
            .map(|x| format!("  {x}"))
            .collect::<Vec<String>>();

        if let ConsistencyBehaviorBuilder::SubchainOnMismatch(mismatch_chain) = &self.behavior {
            let sub_errors = mismatch_chain
                .validate()
                .iter()
                .map(|x| format!("  {x}"))
                .collect::<Vec<String>>();
            errors.extend(sub_errors)
        }

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.get_name()));
        }

        errors
    }
}

struct Tee {
    tx: BufferedChain,
    behavior: ConsistencyBehavior,
    timeout_micros: Option<u64>,
    dropped_messages: Counter,
    result_source: Arc<AtomicResultSource>,
    incoming_responses: IncomingResponses,
}

#[atomic_enum]
enum ResultSource {
    RegularChain,
    TeeChain,
}

impl fmt::Display for ResultSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultSource::RegularChain => write!(f, "regular-chain"),
            ResultSource::TeeChain => write!(f, "tee-chain"),
        }
    }
}

enum ConsistencyBehavior {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(BufferedChain, MessageIdMap<Message>),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TeeConfig {
    pub behavior: Option<ConsistencyBehaviorConfig>,
    pub timeout_micros: Option<u64>,
    pub chain: TransformChainConfig,
    pub buffer_size: Option<usize>,
    pub switch_port: Option<u16>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum ConsistencyBehaviorConfig {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(TransformChainConfig),
}

const NAME: &str = "Tee";
#[typetag::serde(name = "Tee")]
#[async_trait(?Send)]
impl TransformConfig for TeeConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let buffer_size = self.buffer_size.unwrap_or(5);
        let behavior = match &self.behavior {
            Some(ConsistencyBehaviorConfig::Ignore) => ConsistencyBehaviorBuilder::Ignore,
            Some(ConsistencyBehaviorConfig::FailOnMismatch) => {
                ConsistencyBehaviorBuilder::FailOnMismatch
            }
            Some(ConsistencyBehaviorConfig::LogWarningOnMismatch) => {
                ConsistencyBehaviorBuilder::LogWarningOnMismatch
            }
            Some(ConsistencyBehaviorConfig::SubchainOnMismatch(mismatch_chain)) => {
                ConsistencyBehaviorBuilder::SubchainOnMismatch(
                    mismatch_chain
                        .get_builder(TransformContextConfig {
                            chain_name: "mismatch_chain".to_string(),
                            protocol: transform_context.protocol,
                        })
                        .await?,
                )
            }
            None => ConsistencyBehaviorBuilder::Ignore,
        };
        let tee_chain = self
            .chain
            .get_builder(TransformContextConfig {
                chain_name: "tee_chain".to_string(),
                protocol: transform_context.protocol,
            })
            .await?;

        Ok(Box::new(TeeBuilder::new(
            tee_chain,
            buffer_size,
            behavior,
            self.timeout_micros,
            self.switch_port,
            transform_context.protocol.is_inorder(),
        )))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[async_trait]
impl Transform for Tee {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match &mut self.behavior {
            ConsistencyBehavior::Ignore => self.ignore_behaviour(requests_wrapper).await,
            ConsistencyBehavior::FailOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );

                let keep: ResultSource = self.result_source.load(Ordering::Relaxed);
                let responses = self.incoming_responses.new_responses(
                    tee_result?,
                    chain_result?,
                    keep,
                    |keep_message, mut other_message| {
                        debug!(
                            "Tee mismatch:\nresult-source response: {}\nother response: {}",
                            keep_message.to_high_level_string(),
                            other_message.to_high_level_string()
                        );
                        *keep_message = keep_message.from_response_to_error_response(
                            "ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch".into()
                        ).unwrap();
                    },
                );

                Ok(responses)
            }
            ConsistencyBehavior::SubchainOnMismatch(mismatch_chain, requests) => {
                let mut address = *requests_wrapper.local_addr;
                for request in &requests_wrapper.requests {
                    requests.insert(request.id(), request.clone());
                }
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );

                let mut mismatched_requests = vec![];
                let keep: ResultSource = self.result_source.load(Ordering::Relaxed);
                let responses = self.incoming_responses.new_responses(
                    tee_result?,
                    chain_result?,
                    keep,
                    |keep_message, _| {
                        if let Some(id) = keep_message.request_id() {
                            mismatched_requests.push(requests.remove(&id).unwrap());
                        }
                    },
                );
                mismatch_chain
                    .process_request(
                        Wrapper::new_with_addr(mismatched_requests, &mut address),
                        None,
                    )
                    .await?;

                Ok(responses)
            }
            ConsistencyBehavior::LogWarningOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );

                let keep: ResultSource = self.result_source.load(Ordering::Relaxed);
                let responses = self.incoming_responses.new_responses(
                    tee_result?,
                    chain_result?,
                    keep,
                    |keep_message, mut other_message| {
                        warn!(
                            "Tee mismatch:\nresult-source response: {}\nother response: {}",
                            keep_message.to_high_level_string(),
                            other_message.to_high_level_string()
                        );
                    },
                );

                Ok(responses)
            }
        }
    }
}

enum IncomingResponses {
    /// We must handle in order protocols seperately because we must maintain their order
    InOrder {
        tee: VecDeque<Message>,
        chain: VecDeque<Message>,
    },
    /// We must handle out of order protocols seperately because they could arrive in any order.
    OutOfOrder {
        tee_by_request_id: MessageIdMap<Message>,
        chain_by_request_id: MessageIdMap<Message>,
    },
}

impl IncomingResponses {
    /// Processes incoming responses.
    /// If we have a complete response pair then immediately process and return them.
    /// Otherwise store the individual response so that we may eventually match it up with its pair.
    /// Responses with no corresponding request are immediately returned or dropped as they will not have any pair.
    fn new_responses<F>(
        &mut self,
        tee_responses: Vec<Message>,
        chain_responses: Vec<Message>,
        keep: ResultSource,
        mut on_mismatch: F,
    ) -> Vec<Message>
    where
        F: FnMut(&mut Message, Message),
    {
        let mut result = vec![];
        match self {
            IncomingResponses::InOrder { tee, chain } => {
                tee.extend(tee_responses);
                chain.extend(chain_responses);

                // process all responses where we have received from tee and chain
                while !tee.is_empty() && !chain.is_empty() {
                    // handle responses with no request
                    if tee.front().unwrap().request_id().is_none() {
                        result.push(tee.pop_front().unwrap());
                        // need to start the loop again otherwise we might find there are no responses to pop!
                        continue;
                    }
                    if chain.front().unwrap().request_id().is_none() {
                        result.push(chain.pop_front().unwrap());
                        continue;
                    }

                    let mut tee_response = tee.pop_front().unwrap();
                    let mut chain_response = chain.pop_front().unwrap();
                    match keep {
                        ResultSource::RegularChain => {
                            if tee_response != chain_response {
                                on_mismatch(&mut chain_response, tee_response);
                            }
                            result.push(chain_response);
                        }
                        ResultSource::TeeChain => {
                            if tee_response != chain_response {
                                on_mismatch(&mut tee_response, chain_response);
                            }
                            result.push(tee_response);
                        }
                    }
                }

                // once again, handle responses with no request
                // we need to recheck to ensure we havent left any requestless responses lingering
                if tee
                    .front()
                    .map(|x| x.request_id().is_none())
                    .unwrap_or(false)
                {
                    result.push(tee.pop_front().unwrap());
                }
                if chain
                    .front()
                    .map(|x| x.request_id().is_none())
                    .unwrap_or(false)
                {
                    result.push(chain.pop_front().unwrap());
                }
            }
            IncomingResponses::OutOfOrder {
                tee_by_request_id,
                chain_by_request_id,
            } => {
                // Handle all incoming tee responses that have a matching stored chain response
                for mut tee_response in tee_responses {
                    if let Some(request_id) = tee_response.request_id() {
                        // a requested response, compare against the other chain before sending it on.
                        if let Some(mut chain_response) = chain_by_request_id.remove(&request_id) {
                            match keep {
                                ResultSource::TeeChain => {
                                    if tee_response != chain_response {
                                        on_mismatch(&mut tee_response, chain_response);
                                    }
                                    result.push(tee_response);
                                }
                                ResultSource::RegularChain => {
                                    if tee_response != chain_response {
                                        on_mismatch(&mut chain_response, tee_response);
                                    }
                                    result.push(chain_response);
                                }
                            }
                        } else {
                            tee_by_request_id.insert(request_id, tee_response);
                        }
                    } else {
                        // unrequested response, so just send it on if its from the keep chain.
                        if let ResultSource::TeeChain = keep {
                            result.push(tee_response);
                        }
                    }
                }

                // Handle all incoming chain responses that have a matching tee response which was just added in the previous block
                for mut chain_response in chain_responses {
                    if let Some(request_id) = chain_response.request_id() {
                        // a requested response, compare against the other chain before sending it on.
                        if let Some(mut tee_response) = tee_by_request_id.remove(&request_id) {
                            match keep {
                                ResultSource::RegularChain => {
                                    if tee_response != chain_response {
                                        on_mismatch(&mut chain_response, tee_response);
                                    }
                                    result.push(chain_response);
                                }
                                ResultSource::TeeChain => {
                                    if tee_response != chain_response {
                                        on_mismatch(&mut tee_response, chain_response);
                                    }
                                    result.push(tee_response);
                                }
                            }
                        } else {
                            chain_by_request_id.insert(request_id, chain_response);
                        }
                    } else {
                        // unrequested response, so just send it on if its from the keep chain.
                        if let ResultSource::RegularChain = keep {
                            result.push(chain_response);
                        }
                    }
                }
            }
        }
        result
    }
}

impl Tee {
    async fn ignore_behaviour<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let result_source: ResultSource = self.result_source.load(Ordering::Relaxed);
        match result_source {
            ResultSource::RegularChain => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request_no_return(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );
                if let Err(e) = tee_result {
                    self.dropped_messages.increment(1);
                    trace!("Tee Ignored error {e}");
                }
                chain_result
            }
            ResultSource::TeeChain => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );
                if let Err(e) = chain_result {
                    self.dropped_messages.increment(1);
                    trace!("Tee Ignored error {e}");
                }
                tee_result
            }
        }
    }
}

struct ChainSwitchListener {
    address: SocketAddr,
}

impl ChainSwitchListener {
    fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    async fn async_run(self, result_source: Arc<AtomicResultSource>) {
        if let Err(err) = self.async_run_inner(result_source).await {
            error!("Error in ChainSwitchListener: {}", err);
        }
    }

    async fn async_run_inner(self, result_source: Arc<AtomicResultSource>) -> Result<()> {
        let app = Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/transform/tee/result-source",
                axum::routing::get(get_result_source),
            )
            .route(
                "/transform/tee/result-source",
                axum::routing::put(put_result_source),
            )
            .with_state(AppState { result_source });

        let address = self.address;
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("Failed to bind to {}", address))?;
        axum::serve(listener, app).await.map_err(|e| anyhow!(e))
    }
}

async fn root() -> Html<&'static str> {
    Html("try /transform/tee/result-source")
}

async fn get_result_source(State(state): State<AppState>) -> Html<String> {
    let result_source: ResultSource = state.result_source.load(Ordering::Relaxed);
    Html(result_source.to_string())
}

async fn put_result_source(
    State(state): State<AppState>,
    new_result_source: String,
) -> Result<(), HttpServerError> {
    let new_value = match new_result_source.as_str() {
        "tee-chain" => ResultSource::TeeChain,
        "regular-chain" => ResultSource::RegularChain,
        _ => {
            return Err(HttpServerError(anyhow!(
                r"Invalid value for result source: {:?}, should be 'tee-chain' or 'regular-chain'",
                new_result_source
            )));
        }
    };

    state.result_source.store(new_value, Ordering::Relaxed);
    tracing::info!("result source set to {new_value}");

    Ok(())
}

#[derive(Clone)]
struct AppState {
    result_source: Arc<AtomicResultSource>,
}

#[cfg(all(test, feature = "redis"))]
mod tests {
    use super::*;
    use crate::{frame::MessageType, transforms::null::NullSinkConfig};
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_validate_subchain_valid() {
        let config = TeeConfig {
            behavior: None,
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };

        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_validate_subchain_invalid() {
        let config = TeeConfig {
            behavior: None,
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig), Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };

        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate().join("\n");
        let expected = r#"Tee:
  tee_chain chain:
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain."#;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_validate_behaviour_ignore() {
        let config = TeeConfig {
            behavior: Some(ConsistencyBehaviorConfig::Ignore),
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };
        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_validate_behaviour_fail_on_mismatch() {
        let config = TeeConfig {
            behavior: Some(ConsistencyBehaviorConfig::FailOnMismatch),
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };
        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }

    #[tokio::test]
    async fn test_validate_behaviour_subchain_on_mismatch_invalid() {
        let config = TeeConfig {
            behavior: Some(ConsistencyBehaviorConfig::SubchainOnMismatch(
                TransformChainConfig(vec![Box::new(NullSinkConfig), Box::new(NullSinkConfig)]),
            )),
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };

        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate().join("\n");
        let expected = r#"Tee:
  mismatch_chain chain:
    Terminating transform "NullSink" is not last in chain. Terminating transform must be last in chain."#;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_validate_behaviour_subchain_on_mismatch_valid() {
        let config = TeeConfig {
            behavior: Some(ConsistencyBehaviorConfig::SubchainOnMismatch(
                TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            )),
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };

        let transform_context_config = TransformContextConfig {
            chain_name: "".into(),
            protocol: MessageType::Redis,
        };
        let transform = config.get_builder(transform_context_config).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }
}
