use crate::protocols::RawFrame;
use bytes::Bytes;
use cassandra_proto::frame::frame_result::{ColSpec, ColType};
use cassandra_proto::types::data_serialization_types::{
    decode_ascii, decode_bigint, decode_boolean, decode_decimal, decode_double, decode_float,
    decode_inet, decode_int, decode_smallint, decode_timestamp, decode_tinyint, decode_varchar,
    decode_varint,
};
use cassandra_proto::types::CBytes;
use chrono::serde::ts_nanoseconds::serialize as to_nano_ts;
use chrono::{DateTime, TimeZone, Utc};
use mlua::UserData;
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::net::IpAddr;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Bypass(RawMessage),
    Query(QueryMessage),
    Response(QueryResponse),
    Modified(Box<Message>), //The box is to put the nested Message on the heap so we can have a recursive Message
}

impl UserData for Message {}

impl UserData for QueryMessage {}
impl UserData for QueryResponse {}
impl UserData for RawMessage {}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct RawMessage {
    pub original: RawFrame,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryMessage {
    pub original: RawFrame,
    pub query_string: String,
    pub namespace: Vec<String>,
    pub primary_key: HashMap<String, Value>,
    pub query_values: Option<HashMap<String, Value>>,
    pub projection: Option<Vec<String>>,
    pub query_type: QueryType,
    #[serde(skip)]
    pub ast: Option<Statement>,
}

impl QueryMessage {
    pub fn get_namespace(&self) -> Vec<String> {
        return self.namespace.clone();
    }

    pub fn set_namespace_elem(&mut self, index: usize, elem: String) -> String {
        let old = self.namespace.remove(index);
        self.namespace.insert(index, elem);
        return old;
    }

    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self
            .primary_key
            .iter()
            .map(|(_, v)| serde_json::to_string(&v).unwrap())
            .collect();
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub matching_query: Option<QueryMessage>,
    pub original: RawFrame,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

//TODO this could use a Builder
impl QueryResponse {
    pub fn empty() -> Self {
        return QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: None,
            error: None,
        };
    }

    pub fn empty_with_error(error: Option<Value>) -> Self {
        return QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: None,
            error,
        };
    }

    pub fn just_result(result: Value) -> Self {
        return QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: Some(result),
            error: None,
        };
    }

    pub fn result_with_matching(matching: Option<QueryMessage>, result: Value) -> Self {
        return QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result: Some(result),
            error: None,
        };
    }

    pub fn result_error_with_matching(
        matching: Option<QueryMessage>,
        result: Option<Value>,
        error: Option<Value>,
    ) -> Self {
        return QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result: result,
            error: error,
        };
    }

    pub fn error_with_matching(matching: Option<QueryMessage>, error: Value) -> Self {
        return QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result: None,
            error: Some(error),
        };
    }

    pub fn empty_with_matching(original: QueryMessage) -> Self {
        return QueryResponse {
            matching_query: Some(original),
            original: RawFrame::NONE,
            result: None,
            error: None,
        };
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum QueryType {
    Read,
    Write,
    ReadWrite,
    SchemaChange,
    PubSubMessage,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    NULL,
    None,
    #[serde(with = "my_bytes")]
    Bytes(Bytes),
    Strings(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    #[serde(serialize_with = "to_nano_ts")]
    Timestamp(DateTime<Utc>),
    Inet(IpAddr),
    List(Vec<Value>),
    Rows(Vec<Vec<Value>>),
    NamedRows(Vec<HashMap<String, Value>>),
    Document(HashMap<String, Value>),
    FragmentedResponese(Vec<Value>),
}

impl Into<Frame> for Value {
    fn into(self) -> Frame {
        match self {
            Value::NULL => Frame::Null,
            Value::None => unimplemented!(),
            Value::Bytes(b) => Frame::BulkString(b.to_vec()),
            Value::Strings(s) => Frame::SimpleString(s),
            Value::Integer(i) => Frame::Integer(i),
            Value::Float(f) => Frame::SimpleString(f.to_string()),
            Value::Boolean(b) => Frame::Integer(i64::from(b)),
            Value::Timestamp(t) => Frame::SimpleString(t.to_rfc2822()),
            Value::Inet(i) => Frame::SimpleString(i.to_string()),
            Value::List(l) => Frame::Array(l.iter().cloned().map(|v| v.into()).collect()),
            Value::Rows(r) => {
                Frame::Array(r.iter().cloned().map(|v| Value::List(v).into()).collect())
            }
            Value::NamedRows(_) => unimplemented!(),
            Value::Document(_) => unimplemented!(),
            Value::FragmentedResponese(l) => {
                Frame::Array(l.iter().cloned().map(|v| v.into()).collect())
            }
        }
    }
}

impl Value {
    pub fn build_value_from_cstar_col_type(spec: &ColSpec, data: &CBytes) -> Value {
        if let Some(actual_bytes) = data.as_slice() {
            return match spec.col_type.id {
                ColType::Ascii => return Value::Strings(decode_ascii(actual_bytes).unwrap()),
                ColType::Bigint => return Value::Integer(decode_bigint(actual_bytes).unwrap()),
                ColType::Blob => return Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Boolean => return Value::Boolean(decode_boolean(actual_bytes).unwrap()),
                ColType::Counter => Value::Integer(decode_int(actual_bytes).unwrap() as i64),
                ColType::Decimal => Value::Float(decode_decimal(actual_bytes).unwrap().as_plain()),
                ColType::Double => Value::Float(decode_double(actual_bytes).unwrap()),
                ColType::Float => Value::Float(decode_float(actual_bytes).unwrap() as f64),
                ColType::Int => Value::Integer(decode_int(actual_bytes).unwrap() as i64),
                ColType::Timestamp => {
                    Value::Timestamp(Utc.timestamp_nanos(decode_timestamp(actual_bytes).unwrap()))
                }
                ColType::Uuid => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Varchar => Value::Strings(decode_varchar(actual_bytes).unwrap()),
                ColType::Varint => Value::Integer(decode_varint(actual_bytes).unwrap()),
                ColType::Timeuuid => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Inet => Value::Inet(decode_inet(actual_bytes).unwrap()),
                ColType::Date => Value::NULL,
                ColType::Time => Value::NULL,
                ColType::Smallint => Value::Integer(decode_smallint(actual_bytes).unwrap() as i64),
                ColType::Tinyint => Value::Integer(decode_tinyint(actual_bytes).unwrap() as i64),
                _ => {
                    Value::NULL
                    // todo: process collection types based on ColTypeOption
                    // (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L569)
                }
            };
        }
        Value::NULL
    }
}

impl Into<cassandra_proto::types::value::Bytes> for Value {
    fn into(self) -> cassandra_proto::types::value::Bytes {
        return match self {
            Value::NULL => (-1).into(),
            Value::None => cassandra_proto::types::value::Bytes::new(vec![]),
            Value::Bytes(b) => cassandra_proto::types::value::Bytes::new(b.to_vec()),
            Value::Strings(s) => s.into(),
            Value::Integer(i) => i.into(),
            Value::Float(f) => f.into(),
            Value::Boolean(b) => b.into(),
            Value::Timestamp(t) => t.timestamp().into(),
            Value::List(l) => cassandra_proto::types::value::Bytes::from(l),
            Value::Rows(r) => cassandra_proto::types::value::Bytes::from(r),
            Value::NamedRows(n) => cassandra_proto::types::value::Bytes::from(n),
            Value::Document(d) => cassandra_proto::types::value::Bytes::from(d),
            Value::Inet(i) => i.into(),
            Value::FragmentedResponese(l) => cassandra_proto::types::value::Bytes::from(l),
        };
    }
}

mod my_bytes {
    use bytes::{Buf, Bytes};
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
