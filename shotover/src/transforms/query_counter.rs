use crate::frame::Frame;
use crate::message::Messages;
use crate::transforms::TransformConfig;
use crate::transforms::TransformContextBuilder;
use crate::transforms::{Transform, TransformBuilder, Wrapper};
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
    counters: HashMap<String, Counter>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        let counter_name_ref: &'static str = counter_name.leak();
        let mut counters = HashMap::new();
        let query_counter = counter!("shotover_query_count", "name" => counter_name_ref);
        counters.insert("shotover_query_count".to_string(), query_counter);

        QueryCounter {
            // Leaking here is fine since the builder is created only once during shotover startup.
            counter_name: counter_name_ref,
            counters,
        }
    }

    fn increment_counter(&mut self, query: String, query_type: &'static str) {
        if self.counters.contains_key(query.as_str()) {
            self.counters.get(query.as_str()).unwrap().increment(1);
        } else {
            let query_counter = counter!("shotover_query_count", "name" => self.counter_name, "query" => query.clone(), "type" => query_type);
            query_counter.increment(1);
            self.counters.insert(query, query_counter);
        }
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

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for m in &mut requests_wrapper.requests {
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

        requests_wrapper.call_next_transform().await
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
