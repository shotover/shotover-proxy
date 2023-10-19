use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use hyper::{
    service::{make_service_fn, service_fn},
    Method, Request, StatusCode, {Body, Response, Server},
};
use metrics::{register_counter, Counter};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, net::SocketAddr, str, sync::Arc};
use tokio::sync::Mutex;
use tracing::{debug, error, trace, warn};

pub struct TeeBuilder {
    pub tx: TransformChainBuilder,
    pub buffer_size: usize,
    pub behavior: ConsistencyBehaviorBuilder,
    pub timeout_micros: Option<u64>,
    dropped_messages: Counter,
    switch_port: Option<u16>,
    switched_chain: Option<Arc<Mutex<bool>>>,
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
        switched_chain: Option<Arc<Mutex<bool>>>,
    ) -> Self {
        let dropped_messages = register_counter!("tee_dropped_messages", "chain" => "Tee");

        TeeBuilder {
            tx,
            buffer_size,
            behavior,
            timeout_micros,
            dropped_messages,
            switch_port,
            switched_chain,
        }
    }
}

impl TransformBuilder for TeeBuilder {
    fn build(&self) -> Transforms {
        if let Some(switch_port) = self.switch_port {
            let chain_switch_listener =
                ChainSwitchListener::new(SocketAddr::from(([127, 0, 0, 1], switch_port)));
            tokio::spawn(chain_switch_listener.async_run(self.switched_chain.clone().unwrap()));
        }

        Transforms::Tee(Tee {
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
            switched_chain: self.switched_chain.clone(),
        })
    }

    fn get_name(&self) -> &'static str {
        "Tee"
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
    switched_chain: Option<Arc<Mutex<bool>>>,
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

#[typetag::serde(name = "Tee")]
#[async_trait(?Send)]
impl TransformConfig for TeeConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
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
                        .get_builder("mismatch_chain".to_string())
                        .await?,
                )
            }
            None => ConsistencyBehaviorBuilder::Ignore,
        };
        let tee_chain = self.chain.get_builder("tee_chain".to_string()).await?;

        Ok(Box::new(TeeBuilder::new(
            tee_chain,
            buffer_size,
            behavior,
            self.timeout_micros,
            self.switch_port,
            Some(Arc::new(Mutex::new(false))),
        )))
    }
}

#[async_trait]
impl Transform for Tee {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        self.transform_inner(requests_wrapper).await
    }
}

impl Tee {
    async fn is_switched(&mut self) -> bool {
        if let Some(switched_chain) = self.switched_chain.as_ref() {
            let switched = switched_chain.lock().await;
            *switched
        } else {
            false
        }
    }

    async fn return_response(
        &mut self,
        tee_result: Messages,
        chain_result: Messages,
    ) -> Result<Messages> {
        if self.is_switched().await {
            Ok(tee_result)
        } else {
            Ok(chain_result)
        }
    }

    async fn ignore_behaviour_inner<'a>(
        &'a mut self,
        requests_wrapper: Wrapper<'a>,
    ) -> Result<Messages> {
        if self.is_switched().await {
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
        } else {
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
    }

    async fn transform_inner<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match &mut self.behavior {
            ConsistencyBehavior::Ignore => self.ignore_behaviour_inner(requests_wrapper).await,
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
                        "Tee mismatch: \nchain response: {:?} \ntee response: {:?}",
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

                self.return_response(tee_response, chain_response).await
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

                self.return_response(tee_response, chain_response).await
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
                        "Tee mismatch: \nchain response: {:?} \ntee response: {:?}",
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
                self.return_response(tee_response, chain_response).await
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

    async fn set_switched_chain(body: Bytes, switched_chain: Arc<Mutex<bool>>) -> Result<()> {
        let new_switched_chain_state_str = str::from_utf8(body.as_ref())?;
        let new_switched_chain_state = new_switched_chain_state_str.parse::<bool>()?;
        let mut switched = switched_chain.lock().await;
        *switched = new_switched_chain_state;
        Ok(())
    }

    async fn async_run(self, switched_chain: Arc<Mutex<bool>>) {
        if let Err(err) = self.async_run_inner(switched_chain).await {
            error!("Error in ChainSwitchListener: {}", err);
        }
    }

    async fn async_run_inner(self, switched_chain: Arc<Mutex<bool>>) -> Result<()> {
        let make_svc = make_service_fn(move |_| {
            let switched_chain = switched_chain.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let switched_chain = switched_chain.clone();
                    async move {
                        let response = match (req.method(), req.uri().path()) {
                            (&Method::GET, "/switched") => {
                                let switched_chain: bool = *switched_chain.lock().await;
                                Self::rsp(StatusCode::OK, switched_chain.to_string())
                            }
                            (&Method::PUT, "/switch") => {
                                match hyper::body::to_bytes(req.into_body()).await {
                                    Ok(body) => {
                                        match Self::set_switched_chain(body, switched_chain.clone())
                                            .await
                                        {
                                            Err(error) => {
                                                error!(?error, "switching chain failed");
                                                Self::rsp(
                                                    StatusCode::BAD_REQUEST,
                                                    "switching chain failed",
                                                )
                                            }
                                            Ok(()) => Self::rsp(StatusCode::OK, Body::empty()),
                                        }
                                    }
                                    Err(error) => {
                                        error!(%error, "setting filter failed - Couldn't read bytes");
                                        Self::rsp(
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            format!("{error:?}"),
                                        )
                                    }
                                }
                            }
                            _ => Self::rsp(StatusCode::NOT_FOUND, "/switched or /switch"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::null::NullSinkConfig;

    #[tokio::test]
    async fn test_validate_subchain_valid() {
        let config = TeeConfig {
            behavior: None,
            timeout_micros: None,
            chain: TransformChainConfig(vec![Box::new(NullSinkConfig)]),
            buffer_size: None,
            switch_port: None,
        };

        let transform = config.get_builder("".to_owned()).await.unwrap();
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

        let transform = config.get_builder("".to_owned()).await.unwrap();
        let result = transform.validate().join("\n");
        let expected = r#"Tee:
  tee_chain:
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
        let transform = config.get_builder("".to_owned()).await.unwrap();
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
        let transform = config.get_builder("".to_owned()).await.unwrap();
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

        let transform = config.get_builder("".to_owned()).await.unwrap();
        let result = transform.validate().join("\n");
        let expected = r#"Tee:
  mismatch_chain:
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

        let transform = config.get_builder("".to_owned()).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }
}
