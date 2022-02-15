use crate::error::ChainResponse;
use crate::frame::cassandra::{CassandraFrame, CassandraOperation};
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
                Some(Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Query { query, .. },
                    ..
                })) => {
                    for statement in query {
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
                Some(Frame::Cassandra(_)) => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "unknown")
                }
                Some(Frame::Redis(RedisFrame::Array(array))) => {
                    if let Some(v) = array.get(0) {
                        let command = format!("{v:?}");
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => command.to_ascii_uppercase(), "type" => "redis");
                    } else {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "empty", "type" => "redis");
                    }
                }
                Some(Frame::Redis(_)) => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "unknown")
                }
                Some(Frame::None) => {
                    counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "unknown")
                }
                None => {}
            }
        }

        message_wrapper.call_next_transform().await
    }
}

impl QueryCounterConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::QueryCounter(QueryCounter::new(
            self.name.clone(),
        )))
    }
}
