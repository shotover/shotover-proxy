use bytes::Bytes;
use std::collections::HashMap;
use chrono::{DateTime, TimeZone, Utc};
use crate::cassandra_protocol::{RawFrame};
use std::rc::Rc;


#[derive(PartialEq, Debug, Clone)]
pub enum Message {
   Query(QueryMessage),
   Response(QueryResponse)
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryMessage {
    pub original: RawFrame,
    pub query_string: String,
//    pub query_parameters: HashMap<String, Value>,
    pub query_type: QueryType,
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryResponse {
    pub original: RawFrame,
    pub result: Option<Value>,
    pub error: Option<Value>,
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