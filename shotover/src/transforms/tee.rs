use super::TransformContextConfig;
use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use atomic_enum::atomic_enum;
use bytes::Bytes;
use hyper::{
    service::{make_service_fn, service_fn},
    Method, Request, StatusCode, {Body, Response, Server},
};
use metrics::{counter, Counter};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::Ordering;
use std::{convert::Infallible, net::SocketAddr, str, sync::Arc};
use tracing::{debug, error, trace, warn};

pub struct TeeBuilder {
    pub tx: TransformChainBuilder,
    pub buffer_size: usize,
    pub behavior: ConsistencyBehaviorBuilder,
    pub timeout_micros: Option<u64>,
    dropped_messages: Counter,
    result_source: Arc<AtomicResultSource>,
}

pub enum ConsistencyBehaviorBuilder {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(TransformChainBuilder),
}

impl TeeBuilder {
    pub fn new(
        tx: TransformChainBuilder,
        buffer_size: usize,
        behavior: ConsistencyBehaviorBuilder,
        timeout_micros: Option<u64>,
        switch_port: Option<u16>,
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
        }
    }
}

impl TransformBuilder for TeeBuilder {
    fn build(&self) -> Box<dyn Transform> {
        Box::new(Tee {
            tx: self.tx.build_buffered(self.buffer_size),
            behavior: match &self.behavior {
                ConsistencyBehaviorBuilder::Ignore => ConsistencyBehavior::Ignore,
                ConsistencyBehaviorBuilder::LogWarningOnMismatch => {
                    ConsistencyBehavior::LogWarningOnMismatch
                }
                ConsistencyBehaviorBuilder::FailOnMismatch => ConsistencyBehavior::FailOnMismatch,
                ConsistencyBehaviorBuilder::SubchainOnMismatch(chain) => {
                    ConsistencyBehavior::SubchainOnMismatch(chain.build_buffered(self.buffer_size))
                }
            },
            buffer_size: self.buffer_size,
            timeout_micros: self.timeout_micros,
            dropped_messages: self.dropped_messages.clone(),
            result_source: self.result_source.clone(),
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

pub struct Tee {
    pub tx: BufferedChain,
    pub buffer_size: usize,
    pub behavior: ConsistencyBehavior,
    pub timeout_micros: Option<u64>,
    dropped_messages: Counter,
    result_source: Arc<AtomicResultSource>,
}

#[atomic_enum]
pub enum ResultSource {
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

pub enum ConsistencyBehavior {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(BufferedChain),
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
        )))
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
                let mut tee_response = tee_result?;
                let mut chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    debug!(
                        "Tee mismatch:\nchain response: {:?}\ntee response: {:?}",
                        chain_response
                            .iter_mut()
                            .map(|m| m.to_high_level_string())
                            .collect::<Vec<_>>(),
                        tee_response
                            .iter_mut()
                            .map(|m| m.to_high_level_string())
                            .collect::<Vec<_>>()
                    );

                    for message in &mut chain_response {
                        *message = message.to_error_response(
                            "ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch".into())?;
                    }
                }

                Ok(self.return_response(tee_response, chain_response).await)
            }
            ConsistencyBehavior::SubchainOnMismatch(mismatch_chain) => {
                let failed_message = requests_wrapper.clone();
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );

                let tee_response = tee_result?;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    mismatch_chain.process_request(failed_message, None).await?;
                }

                Ok(self.return_response(tee_response, chain_response).await)
            }
            ConsistencyBehavior::LogWarningOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );

                let mut tee_response = tee_result?;
                let mut chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    warn!(
                        "Tee mismatch:\nchain response: {:?}\ntee response: {:?}",
                        chain_response
                            .iter_mut()
                            .map(|m| m.to_high_level_string())
                            .collect::<Vec<_>>(),
                        tee_response
                            .iter_mut()
                            .map(|m| m.to_high_level_string())
                            .collect::<Vec<_>>()
                    );
                }
                Ok(self.return_response(tee_response, chain_response).await)
            }
        }
    }
}

impl Tee {
    async fn return_response(&mut self, tee_result: Messages, chain_result: Messages) -> Messages {
        let result_source: ResultSource = self.result_source.load(Ordering::Relaxed);
        match result_source {
            ResultSource::RegularChain => chain_result,
            ResultSource::TeeChain => tee_result,
        }
    }

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

    fn rsp(status: StatusCode, body: impl Into<Body>) -> Response<Body> {
        Response::builder()
            .status(status)
            .body(body.into())
            .expect("builder with known status code must not fail")
    }

    async fn set_result_source_chain(
        body: Bytes,
        result_source: Arc<AtomicResultSource>,
    ) -> Result<()> {
        let new_result_source = str::from_utf8(body.as_ref())?;

        let new_value = match new_result_source {
            "tee-chain" => ResultSource::TeeChain,
            "regular-chain" => ResultSource::RegularChain,
            _ => {
                return Err(anyhow!(
                    r"Invalid value for result source: {}, should be 'tee-chain' or 'regular-chain'",
                    new_result_source
                ))
            }
        };

        debug!("Setting result source to {}", new_value);

        result_source.store(new_value, Ordering::Relaxed);
        Ok(())
    }

    async fn async_run(self, result_source: Arc<AtomicResultSource>) {
        if let Err(err) = self.async_run_inner(result_source).await {
            error!("Error in ChainSwitchListener: {}", err);
        }
    }

    async fn async_run_inner(self, result_source: Arc<AtomicResultSource>) -> Result<()> {
        let make_svc = make_service_fn(move |_| {
            let result_source = result_source.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let result_source = result_source.clone();
                    async move {
                        let response = match (req.method(), req.uri().path()) {
                            (&Method::GET, "/transform/tee/result-source") => {
                                let result_source: ResultSource =
                                    result_source.load(Ordering::Relaxed);
                                Self::rsp(StatusCode::OK, result_source.to_string())
                            }
                            (&Method::PUT, "/transform/tee/result-source") => {
                                match hyper::body::to_bytes(req.into_body()).await {
                                    Ok(body) => {
                                        match Self::set_result_source_chain(
                                            body,
                                            result_source.clone(),
                                        )
                                        .await
                                        {
                                            Err(error) => {
                                                error!(?error, "setting result source failed");
                                                Self::rsp(
                                                    StatusCode::BAD_REQUEST,
                                                    format!(
                                                        "setting result source failed: {error}"
                                                    ),
                                                )
                                            }
                                            Ok(()) => Self::rsp(StatusCode::OK, Body::empty()),
                                        }
                                    }
                                    Err(error) => {
                                        error!(%error, "setting result source failed - Couldn't read bytes");
                                        Self::rsp(
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            format!("{error:?}"),
                                        )
                                    }
                                }
                            }
                            _ => {
                                Self::rsp(StatusCode::NOT_FOUND, "try /tranform/tee/result-source")
                            }
                        };
                        Ok::<_, Infallible>(response)
                    }
                }))
            }
        });

        let address = self.address;
        Server::try_bind(&address)
            .with_context(|| format!("Failed to bind to {}", address))?
            .serve(make_svc)
            .await
            .map_err(|e| anyhow!(e))
    }
}

#[cfg(all(test, feature = "redis"))]
mod tests {
    use super::*;
    use crate::{frame::MessageType, transforms::null::NullSinkConfig};

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
