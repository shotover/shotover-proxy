use bytes::Bytes;
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use chrono::serde::ts_nanoseconds::serialize as to_nano_ts;
use crate::cassandra_protocol::{RawFrame};
use sqlparser::ast::Statement;
use serde::{Serialize, Deserialize};


#[derive(PartialEq, Debug, Clone)]
pub enum Message { 
    Bypass(RawMessage),
    Query(QueryMessage),
    Response(QueryResponse)
}

#[derive(PartialEq, Debug, Clone)]
pub struct RawMessage {
    pub original: RawFrame,
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryMessage {
    pub original: RawFrame,
    pub query_string: String,
    pub namespace: Vec<String>,
    pub primary_key: HashMap<String, Value>,
    pub query_values: Option<HashMap<String, Value>>,
    pub projection: Option<Vec<String>>,
    pub query_type: QueryType,
    pub ast: Option<Statement>
}

impl QueryMessage {
    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self.primary_key.iter().map(|(k,v) | {serde_json::to_string(&v).unwrap()}).collect();
        return Some(f.join("."));
    }

    pub fn get_namespaced_primary_key(&self) -> Option<String> {
        if let Some(pk) = self.get_primary_key() {
            let mut buffer = String::new();
            let f: String = self.namespace.join(".");
            buffer.push_str(f.as_str());
            buffer.push_str(".");
            buffer.push_str(serde_json::to_string(&pk).unwrap().as_str());
            return Some(buffer);
        }
        return None;
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryResponse {
    pub matching_query: Option<QueryMessage>,
    pub original: RawFrame,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

impl QueryResponse {
    pub fn empty() -> Self {
        return QueryResponse {
            matching_query: None,
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
    SchemaChange
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    NULL,
    #[serde(with = "my_bytes")]
    Bytes(Bytes),
    Strings(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    #[serde(serialize_with = "to_nano_ts")]
    Timestamp(DateTime<Utc>),
    Rows(Vec<Vec<Value>>),
    Document(HashMap<String, Value>),
}

mod my_bytes {
    use bytes::{Bytes, Buf};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(val: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        serializer.serialize_bytes(val.bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
        where
            D: Deserializer<'de>,
    {
        let val: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(val))
    }
}