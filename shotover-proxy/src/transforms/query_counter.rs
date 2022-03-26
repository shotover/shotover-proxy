use crate::error::ChainResponse;
use crate::frame::Frame;
use crate::frame::RedisFrame;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, register_counter};
use serde::Deserialize;
use sqlparser::ast::Statement;

#[derive(Debug, Clone)]
pub struct QueryCounter {
    counter_name: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryCounterConfig {
    pub name: String,
}

impl QueryCounter {
    pub fn new(counter_name: String) -> Self {
        register_counter!("query_count", "name" => counter_name.clone());

        QueryCounter { counter_name }
    }
}

#[async_trait]
impl Transform for QueryCounter {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for m in &mut message_wrapper.messages {
            match m.frame() {
                Some(Frame::Cassandra(frame)) => match frame.operation.queries() {
                    Ok(queries) => {
                        for statement in queries {
                            let query_type = match statement {
                                Statement::Query(_) => "SELECT",
                                Statement::Insert { .. } => "INSERT",
                                Statement::Copy { .. } => "COPY",
                                Statement::Update { .. } => "UPDATE",
                                Statement::Delete { .. } => "DELETE",
                                Statement::CreateTable { .. } => "CREATE TABLE",
                                Statement::AlterTable { .. } => "ALTER TABLE",
                                Statement::Drop { .. } => "DROP",
                                _ => "UNRECOGNISED CQL",
                            };
                            counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => query_type, "type" => "cassandra");
                        }
                    }
                    Err(_) => {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "cassandra");
                    }
                },
                Some(Frame::Redis(frame)) => {
                    if let Some(query_type) = get_redis_query_type(frame) {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => query_type, "type" => "redis");
                    } else {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "redis");
                    }
                }
                Some(Frame::None) | None => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "none")
                }
            }
        }

        message_wrapper.call_next_transform().await
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

impl QueryCounterConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::QueryCounter(QueryCounter::new(
            self.name.clone(),
        )))
    }
}
