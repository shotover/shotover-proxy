use bytes::Bytes;
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::cassandra_protocol::{RawFrame};
use sqlparser::ast::Statement;


#[derive(PartialEq, Debug, Clone)]
pub enum Message<'a> {
   Query(QueryMessage<'a>),
   Response(QueryResponse)
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryMessage<'a> {
    pub original: RawFrame,
    pub query_string: String,
    pub namespace: Vec<String>,
    pub primary_key: HashMap<String, Value>,
    pub query_values: Option<HashMap<String, Value>>,
    pub projection: Option<Vec<String>>,
    pub query_type: QueryType,
    pub ast: Option<&'a Statement>
}

impl QueryMessage<'_> {
    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self.primary_key.iter().map(|(k,v) | {format!("{:?}", v)}).collect();
        return Some(f.join("."));
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryResponse {
    pub original: RawFrame,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

impl QueryResponse {
    pub fn empty() -> Self {
        return QueryResponse {
            original: RawFrame::NONE,
            result: None,
            error: None
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum QueryType {
   Read,
   Write,
   ReadWrite,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Value {
    Bytes(Bytes),
    Strings(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(DateTime<Utc>),
    Rows(Vec<Vec<Value>>),
    Document(HashMap<String, Value>),
}