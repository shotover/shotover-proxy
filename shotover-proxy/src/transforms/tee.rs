use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, QueryResponse, Value};
use crate::protocols::RawFrame;
use crate::transforms::chain::{BufferedChain, TransformChain};
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, register_counter, Unit};
use serde::Deserialize;
use tracing::trace;

#[derive(Debug, Clone)]
pub struct Tee {
    pub tx: BufferedChain,
    pub fail_chain: Option<BufferedChain>,
    pub buffer_size: usize,
    pub chain_to_clone: TransformChain,
    pub behavior: ConsistencyBehavior,
    pub timeout_micros: Option<u64>,
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
    pub async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let buffer_size = self.buffer_size.unwrap_or(5);
        let fail_chain =
            if let Some(ConsistencyBehavior::SubchainOnMismatch(fail_chain)) = &self.behavior {
                Some(
                    build_chain_from_config("fail_chain".to_string(), fail_chain, topics)
                        .await?
                        .into_buffered_chain(buffer_size),
                )
            } else {
                None
            };
        let tee_chain =
            build_chain_from_config("tee_chain".to_string(), &self.chain, topics).await?;

        Ok(Transforms::Tee(Tee::new(
            tee_chain.clone().into_buffered_chain(buffer_size),
            fail_chain,
            buffer_size,
            tee_chain,
            self.behavior.clone().unwrap_or(ConsistencyBehavior::Ignore),
            self.timeout_micros,
        )))
    }
}

impl Tee {
    pub fn new(
        tx: BufferedChain,
        fail_chain: Option<BufferedChain>,
        buffer_size: usize,
        chain_to_clone: TransformChain,
        behavior: ConsistencyBehavior,
        timeout_micros: Option<u64>,
    ) -> Self {
        let tee = Tee {
            tx,
            fail_chain,
            buffer_size,
            chain_to_clone,
            behavior,
            timeout_micros,
        };

        register_counter!("tee_dropped_messages", Unit::Count, "chain" => tee.get_name());

        tee
    }
}

impl Tee {
    fn get_name(&self) -> &'static str {
        "Tee"
    }
}

#[async_trait]
impl Transform for Tee {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        match self.behavior {
            ConsistencyBehavior::Ignore => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx.process_request_no_return(
                        message_wrapper.clone(),
                        "Tee".to_string(),
                        self.timeout_micros
                    ),
                    message_wrapper.call_next_transform()
                );
                match tee_result {
                    Ok(_) => {}
                    Err(e) => {
                        counter!("tee_dropped_messages", 1, "chain" => self.get_name());
                        trace!("MPSC error {}", e);
                    }
                }
                chain_result
            }
            ConsistencyBehavior::FailOnMismatch => {
                let (tee_result, chain_result) = tokio::join!(
                    self.tx.process_request(
                        message_wrapper.clone(),
                        "Tee".to_string(),
                        self.timeout_micros
                    ),
                    message_wrapper.call_next_transform()
                );
                let tee_response = tee_result?;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    Ok(vec!(Message::new(
                        MessageDetails::Response(QueryResponse::empty_with_error(Some(Value::Strings(
                            "The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch"
                                .to_string(),
                        )))),
                        true,
                        RawFrame::None,
                    )))
                } else {
                    Ok(chain_response)
                }
            }
            ConsistencyBehavior::SubchainOnMismatch(_) => {
                let failed_message = message_wrapper.clone();
                let (tee_result, chain_result) = tokio::join!(
                    self.tx.process_request(
                        message_wrapper.clone(),
                        "Tee".to_string(),
                        self.timeout_micros
                    ),
                    message_wrapper.call_next_transform()
                );

                let tee_response = tee_result?;
                let chain_response = chain_result?;

                if !chain_response.eq(&tee_response) {
                    if let Some(topic) = &mut self.fail_chain {
                        topic
                            .process_request(failed_message, "SubchainOnMismatch".to_string(), None)
                            .await?;
                    }
                }

                Ok(chain_response)
            }
        }
    }
}
