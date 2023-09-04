use crate::config::chain::TransformChainConfig;
use crate::message::Messages;
// use itertools::Itertools;
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use tracing::{trace, warn};

pub struct TeeBuilder {
    pub tx: TransformChainBuilder,
    pub buffer_size: usize,
    pub behavior: ConsistencyBehaviorBuilder,
    pub timeout_micros: Option<u64>,
    dropped_messages: Counter,
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
    ) -> Self {
        let dropped_messages = register_counter!("tee_dropped_messages", "chain" => "Tee");

        TeeBuilder {
            tx,
            buffer_size,
            behavior,
            timeout_micros,
            dropped_messages,
        }
    }
}

impl TransformBuilder for TeeBuilder {
    fn build(&self) -> Transforms {
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
}

pub enum ConsistencyBehavior {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(BufferedChain),
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TeeConfig {
    pub behavior: Option<ConsistencyBehaviorConfig>,
    pub timeout_micros: Option<u64>,
    pub chain: TransformChainConfig,
    pub buffer_size: Option<usize>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub enum ConsistencyBehaviorConfig {
    Ignore,
    LogWarningOnMismatch,
    FailOnMismatch,
    SubchainOnMismatch(TransformChainConfig),
}

#[typetag::deserialize(name = "Tee")]
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
        )))
    }
}

#[async_trait]
impl Transform for Tee {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match &mut self.behavior {
            ConsistencyBehavior::Ignore => {
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
            ConsistencyBehavior::FailOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(requests_wrapper.clone(), self.timeout_micros),
                    requests_wrapper.call_next_transform()
                );
                let tee_response = tee_result?;
                let mut chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    for message in &mut chain_response {
                        *message = message.to_error_response(
                            "ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch".into())?;
                    }
                }
                Ok(chain_response)
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

                Ok(chain_response)
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
                Ok(chain_response)
            }
        }
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
        };

        let transform = config.get_builder("".to_owned()).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }
}
