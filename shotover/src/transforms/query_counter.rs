use crate::frame::Frame;
use crate::message::Messages;
use crate::transforms::TransformConfig;
use crate::transforms::TransformContextBuilder;
use crate::transforms::{Transform, TransformBuilder, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::counter;
use serde::Deserialize;
use serde::Serialize;

use super::DownChainProtocol;
use super::TransformContextConfig;
use super::UpChainProtocol;

#[derive(Clone)]
pub struct QueryCounter {
    counter_name: &'static str,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        counter!("shotover_query_count", "name" => counter_name.clone());

        QueryCounter {
            counter_name: counter_name.leak(),
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
                        counter!("shotover_query_count", "name" => self.counter_name, "query" => statement.short_name(), "type" => "cassandra").increment(1);
                    }
                }
                #[cfg(feature = "redis")]
                Some(Frame::Redis(frame)) => {
                    if let Some(query_type) = crate::frame::redis::redis_query_name(frame) {
                        counter!("shotover_query_count", "name" => self.counter_name, "query" => query_type, "type" => "redis").increment(1);
                    } else {
                        counter!("shotover_query_count", "name" => self.counter_name, "query" => "unknown", "type" => "redis").increment(1);
                    }
                }
                #[cfg(feature = "kafka")]
                Some(Frame::Kafka(_)) => {
                    counter!("shotover_query_count", "name" => self.counter_name, "query" => "unknown", "type" => "kafka").increment(1);
                }
                Some(Frame::Dummy) => {
                    // Dummy does not count as a message
                }
                #[cfg(feature = "opensearch")]
                Some(Frame::OpenSearch(_)) => {
                    todo!();
                }
                None => {
                    counter!("shotover_query_count", "name" => self.counter_name, "query" => "unknown", "type" => "none").increment(1)
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
