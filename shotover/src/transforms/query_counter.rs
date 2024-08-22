use crate::frame::Frame;
use crate::message::Messages;
use crate::transforms::TransformConfig;
use crate::transforms::TransformContextBuilder;
use crate::transforms::{ChainState, Transform, TransformBuilder};
use anyhow::Result;
use async_trait::async_trait;
use metrics::counter;
use metrics::Counter;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

use super::DownChainProtocol;
use super::TransformContextConfig;
use super::UpChainProtocol;

#[derive(Clone)]
pub struct QueryCounter {
    counter_name: &'static str,
    query_to_counter: HashMap<String, Counter>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        // Leaking here is fine since the builder is created only once during shotover startup.
        let counter_name_ref: &'static str = counter_name.leak();

        // Although not incremented, this counter needs to be created to ensure shotover_query_count is 0 on shotover startup.
        let _ = counter!("shotover_query_count", "name" => counter_name_ref);

        QueryCounter {
            counter_name: counter_name_ref,
            query_to_counter: HashMap::new(),
        }
    }

    fn increment_counter(&mut self, query: String, query_type: &'static str) {
        self.query_to_counter.entry(query)
            .or_insert_with_key(|query| counter!("shotover_query_count", "name" => self.counter_name, "query" => query.clone(), "type" => query_type))
            .increment(1);
    }
}

impl TransformBuilder for QueryCounter {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for QueryCounter {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for m in &mut chain_state.requests {
            match m.frame() {
                #[cfg(feature = "cassandra")]
                Some(Frame::Cassandra(frame)) => {
                    for statement in frame.operation.queries() {
                        self.increment_counter(statement.short_name().to_string(), "cassandra");
                    }
                }
                #[cfg(feature = "redis")]
                Some(Frame::Redis(frame)) => {
                    if let Some(query_type) = crate::frame::redis::redis_query_name(frame) {
                        self.increment_counter(query_type, "redis");
                    } else {
                        self.increment_counter("unknown".to_string(), "redis");
                    }
                }
                #[cfg(feature = "kafka")]
                Some(Frame::Kafka(_)) => {
                    self.increment_counter("unknown".to_string(), "kafka");
                }
                Some(Frame::Dummy) => {
                    // Dummy does not count as a message
                }
                #[cfg(feature = "opensearch")]
                Some(Frame::OpenSearch(_)) => {
                    todo!();
                }
                None => {
                    self.increment_counter("unknown".to_string(), "none");
                }
            }
        }

        chain_state.call_next_transform().await
    }
}

const NAME: &str = "QueryCounter";
#[typetag::serde(name = "QueryCounter")]
#[async_trait(?Send)]
impl TransformConfig for QueryCounterConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(QueryCounter::new(self.name.clone())))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}
