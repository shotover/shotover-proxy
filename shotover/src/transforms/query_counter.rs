use crate::frame::Frame;
use crate::frame::RedisFrame;
use crate::message::Messages;
use crate::transforms::TransformConfig;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, register_counter};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct QueryCounter {
    counter_name: String,
}

#[derive(Deserialize, Debug)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        register_counter!("query_count", "name" => counter_name.clone());

        QueryCounter { counter_name }
    }
}

impl TransformBuilder for QueryCounter {
    fn build(&self) -> Transforms {
        Transforms::QueryCounter(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "QueryCounter"
    }
}

#[async_trait]
impl Transform for QueryCounter {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for m in &mut requests_wrapper.requests {
            match m.frame() {
                Some(Frame::Cassandra(frame)) => {
                    for statement in frame.operation.queries() {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => statement.short_name(), "type" => "cassandra");
                    }
                }
                Some(Frame::Redis(frame)) => {
                    if let Some(query_type) = get_redis_query_type(frame) {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => query_type, "type" => "redis");
                    } else {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "redis");
                    }
                }
                Some(Frame::Kafka(_)) => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "kafka");
                }
                Some(Frame::Dummy) => {
                    // Dummy does not count as a message
                }
                None => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "none")
                }
            }
        }

        requests_wrapper.call_next_transform().await
    }
}

fn get_redis_query_type(frame: &RedisFrame) -> Option<String> {
    if let RedisFrame::Array(array) = frame {
        if let Some(RedisFrame::BulkString(v)) = array.get(0) {
            let upper_bytes = v.to_ascii_uppercase();
            match String::from_utf8(upper_bytes) {
                Ok(query_type) => {
                    return Some(query_type);
                }
                Err(err) => {
                    tracing::error!(
                        "Failed to convert redis bulkstring to string, err: {:?}",
                        err
                    )
                }
            }
        }
    }
    None
}

#[typetag::deserialize(name = "QueryCounter")]
#[async_trait(?Send)]
impl TransformConfig for QueryCounterConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(QueryCounter::new(self.name.clone())))
    }
}
