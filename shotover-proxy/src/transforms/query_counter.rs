use crate::error::ChainResponse;
use crate::message::Value::List;
use crate::message::{ASTHolder, MessageDetails, QueryMessage};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use metrics::{counter, register_counter, Unit};
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
        register_counter!("query_count", Unit::Count, "name" => counter_name.clone());

        QueryCounter { counter_name }
    }
}

#[async_trait]
impl Transform for QueryCounter {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        for m in &message_wrapper.messages {
            if let MessageDetails::Query(QueryMessage {
                query_string: _query_string,
                namespace: _namespace,
                primary_key: _primary_key,
                query_values: _query_values,
                projection: _projection,
                query_type: _query_type,
                ast: Some(ast),
            }) = &m.details
            {
                match ast {
                    ASTHolder::SQL(statement) => {
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
                    ASTHolder::Commands(List(commands)) => {
                        if let Some(v) = commands.get(0) {
                            let command = format!("{:?}", v);
                            counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => command.to_ascii_uppercase(), "type" => "redis");
                        } else {
                            counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "empty", "type" => "redis");
                        }
                    }
                    _ => {
                        counter!("query_count", 1, "name" => self.counter_name.clone(), "query" => "unknown", "type" => "unknown");
                    }
                }
            }
        }

        message_wrapper.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        "QueryCounter"
    }
}

impl QueryCounterConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::QueryCounter(QueryCounter::new(
            self.name.clone(),
        )))
    }
}
