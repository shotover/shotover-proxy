use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::chain::BufferedChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{register_counter, Counter};
use serde::Deserialize;
use tracing::trace;

pub struct Tee {
    pub tx: BufferedChain,
    pub mismatch_chain: Option<BufferedChain>,
    pub buffer_size: usize,
    pub behavior: ConsistencyBehavior,
    pub timeout_micros: Option<u64>,
    dropped_messages: Counter,
}

impl Clone for Tee {
    fn clone(&self) -> Self {
        Tee {
            tx: self.tx.to_new_instance(self.buffer_size),
            mismatch_chain: self
                .mismatch_chain
                .as_ref()
                .map(|x| x.to_new_instance(self.buffer_size)),
            buffer_size: self.buffer_size,
            behavior: self.behavior.clone(),
            timeout_micros: self.timeout_micros,
            dropped_messages: self.dropped_messages.clone(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub enum ConsistencyBehavior {
    Ignore,
    FailOnMismatch,
    SubchainOnMismatch(Vec<TransformsConfig>),
}

#[derive(Deserialize, Debug, Clone)]
pub struct TeeConfig {
    pub behavior: Option<ConsistencyBehavior>,
    pub timeout_micros: Option<u64>,
    pub chain: Vec<TransformsConfig>,
    pub buffer_size: Option<usize>,
}

impl TeeConfig {
    pub async fn get_transform(&self, topics: &TopicHolder) -> Result<Transforms> {
        let buffer_size = self.buffer_size.unwrap_or(5);
        let mismatch_chain =
            if let Some(ConsistencyBehavior::SubchainOnMismatch(mismatch_chain)) = &self.behavior {
                Some(
                    build_chain_from_config("mismatch_chain".to_string(), mismatch_chain, topics)
                        .await?
                        .into_buffered_chain(buffer_size),
                )
            } else {
                None
            };
        let tee_chain =
            build_chain_from_config("tee_chain".to_string(), &self.chain, topics).await?;

        Ok(Transforms::Tee(Tee::new(
            tee_chain.into_buffered_chain(buffer_size),
            mismatch_chain,
            buffer_size,
            self.behavior.clone().unwrap_or(ConsistencyBehavior::Ignore),
            self.timeout_micros,
        )))
    }
}

impl Tee {
    pub fn new(
        tx: BufferedChain,
        mismatch_chain: Option<BufferedChain>,
        buffer_size: usize,
        behavior: ConsistencyBehavior,
        timeout_micros: Option<u64>,
    ) -> Self {
        let dropped_messages = register_counter!("tee_dropped_messages", "chain" => "Tee");

        Tee {
            tx,
            mismatch_chain,
            buffer_size,
            behavior,
            timeout_micros,
            dropped_messages,
        }
    }
}

impl Tee {
    fn get_name(&self) -> &'static str {
        "Tee"
    }
}

#[async_trait]
impl Transform for Tee {
    fn validate(&self) -> Vec<String> {
        if let Some(mismatch_chain) = &self.mismatch_chain {
            let mut errors = mismatch_chain
                .original_chain
                .validate()
                .iter()
                .map(|x| format!("  {x}"))
                .collect::<Vec<String>>();

            if !errors.is_empty() {
                errors.insert(0, format!("{}:", self.get_name()));
            }

            errors
        } else {
            vec![]
        }
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        match self.behavior {
            ConsistencyBehavior::Ignore => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request_no_return(message_wrapper.clone(), self.timeout_micros),
                    message_wrapper.call_next_transform()
                );
                match tee_result {
                    Ok(_) => {}
                    Err(e) => {
                        self.dropped_messages.increment(1);
                        trace!("MPSC error {}", e);
                    }
                }
                chain_result
            }
            ConsistencyBehavior::FailOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(message_wrapper.clone(), self.timeout_micros),
                    message_wrapper.call_next_transform()
                );
                let tee_response = tee_result?;
                let mut chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    for message in &mut chain_response {
                        message.set_error(
                                "ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch".into())
                    }
                }
                Ok(chain_response)
            }
            ConsistencyBehavior::SubchainOnMismatch(_) => {
                let failed_message = message_wrapper.clone();
                let (tee_result, chain_result) = tokio::join!(
                    self.tx
                        .process_request(message_wrapper.clone(), self.timeout_micros),
                    message_wrapper.call_next_transform()
                );

                let tee_response = tee_result?;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    if let Some(topic) = &mut self.mismatch_chain {
                        topic.process_request(failed_message, None).await?;
                    }
                }

                Ok(chain_response)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::topology::TopicHolder;
    use crate::transforms::TransformsConfig;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_validate_no_subchain() {
        let holder = TopicHolder {
            topics_rx: HashMap::new(),
            topics_tx: HashMap::new(),
        };

        {
            let config = TeeConfig {
                behavior: Some(ConsistencyBehavior::Ignore),
                timeout_micros: None,
                chain: vec![TransformsConfig::Null],
                buffer_size: None,
            };
            let transform = config.get_transform(&holder).await.unwrap();
            let result = transform.validate();
            assert_eq!(result, Vec::<String>::new());
        }

        {
            let config = TeeConfig {
                behavior: Some(ConsistencyBehavior::FailOnMismatch),
                timeout_micros: None,
                chain: vec![TransformsConfig::Null],
                buffer_size: None,
            };
            let transform = config.get_transform(&holder).await.unwrap();
            let result = transform.validate();
            assert_eq!(result, Vec::<String>::new());
        }
    }

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let holder = TopicHolder {
            topics_rx: HashMap::new(),
            topics_tx: HashMap::new(),
        };

        let config = TeeConfig {
            behavior: Some(ConsistencyBehavior::SubchainOnMismatch(vec![
                TransformsConfig::Null,
                TransformsConfig::Null,
            ])),
            timeout_micros: None,
            chain: vec![TransformsConfig::Null],
            buffer_size: None,
        };

        let transform = config.get_transform(&holder).await.unwrap();
        let result = transform.validate();
        let expected = vec!["Tee:", "  mismatch_chain:", "    Terminating transform \"Null\" is not last in chain. Terminating transform must be last in chain."];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let holder = TopicHolder {
            topics_rx: HashMap::new(),
            topics_tx: HashMap::new(),
        };

        let config = TeeConfig {
            behavior: Some(ConsistencyBehavior::SubchainOnMismatch(vec![
                TransformsConfig::Null,
            ])),
            timeout_micros: None,
            chain: vec![TransformsConfig::Null],
            buffer_size: None,
        };

        let transform = config.get_transform(&holder).await.unwrap();
        let result = transform.validate();
        assert_eq!(result, Vec::<String>::new());
    }
}
