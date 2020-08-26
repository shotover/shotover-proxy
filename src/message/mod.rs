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
use itertools::Itertools;
use mlua::UserData;
use redis::{RedisResult, RedisWrite, Value as RValue};
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::net::IpAddr;

// TODO: Clippy says this is bad due to large variation - also almost 1k in size on the stack
// Should move the message type to just be bulk..
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Bypass(RawMessage),
    Query(QueryMessage),
    Response(QueryResponse),
    Modified(Box<Message>), //The box is to put the nested Message on the heap so we can have a recursive Message
    Bulk(Vec<Message>), // TODO: This enum is not the best place to put this, but it was the easiest integration point to build out pipelining support
}

impl Message {
    pub fn new_mod(message: Message) -> Message {
        Message::Modified(Box::new(message))
    }
}

impl UserData for Message {}

impl UserData for QueryMessage {}
impl UserData for QueryResponse {}
impl UserData for RawMessage {}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct RawMessage {
    pub original: RawFrame,
}

// Transforms should not try to directly serialize the AST - it's purely an in-memory representation
// query_string is also mainly there from a debugging / logging perspective as its a utf8
// encoded represntation of the query.
// Statement can be "serialized"/rendered through it's display methods
// Commands can be serialized by getting the underlying Value

#[derive(PartialEq, Debug, Clone)]
pub enum ASTHolder {
    SQL(Statement),
    Commands(Value), // A flexible representation of a structured query that will naturally convert into the required type via into/from traits
}

impl ASTHolder {
    pub fn get_command(&self) -> String {
        match self {
            ASTHolder::SQL(statement) => {
                return match statement {
                    Statement::Query(_) => "SELECT",
                    Statement::Insert { .. } => "INSERT",
                    Statement::Update { .. } => "UPDATE",
                    Statement::Delete { .. } => "DELETE",
                    Statement::CreateView { .. } => "CREATE VIEW",
                    Statement::CreateTable { .. } => "CREATE TABLE",
                    Statement::AlterTable { .. } => "ALTER TABLE",
                    Statement::Drop { .. } => "DROP",
                    _ => "UKNOWN",
                }
                .to_string();
            }
            ASTHolder::Commands(commands) => {
                if let Value::List(coms) = commands {
                    if let Some(Value::Bytes(b)) = coms.get(0) {
                        return String::from_utf8(b.to_vec())
                            .unwrap_or_else(|_| "couldn't decode".to_string());
                    }
                }
            }
        }
        "UNKNOWN".to_string()
    }
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
    pub ast: Option<ASTHolder>,
}

impl QueryMessage {
    pub fn get_namespace(&self) -> Vec<String> {
        self.namespace.clone()
    }

    pub fn set_namespace_elem(&mut self, index: usize, elem: String) -> String {
        let old = self.namespace.remove(index);
        self.namespace.insert(index, elem);
        old
    }

    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self
            .primary_key
            .iter()
            .map(|(_, v)| serde_json::to_string(&v).unwrap())
            .collect();
        Some(f.join("."))
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
        None
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub matching_query: Option<QueryMessage>,
    pub original: RawFrame,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub response_meta: Option<Value>,
}

//TODO this could use a Builder
impl QueryResponse {
    pub fn empty() -> Self {
        QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: None,
            error: None,
            response_meta: None,
        }
    }

    pub fn empty_with_error(error: Option<Value>) -> Self {
        QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: None,
            error,
            response_meta: None,
        }
    }

    pub fn just_result(result: Value) -> Self {
        QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: Some(result),
            error: None,
            response_meta: None,
        }
    }

    pub fn result_with_matching(matching: Option<QueryMessage>, result: Value) -> Self {
        QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result: Some(result),
            error: None,
            response_meta: None,
        }
    }

    pub fn result_error_with_matching(
        matching: Option<QueryMessage>,
        result: Option<Value>,
        error: Option<Value>,
    ) -> Self {
        QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result,
            error,
            response_meta: None,
        }
    }

    pub fn error_with_matching(matching: Option<QueryMessage>, error: Value) -> Self {
        QueryResponse {
            matching_query: matching,
            original: RawFrame::NONE,
            result: None,
            error: Some(error),
            response_meta: None,
        }
    }

    pub fn empty_with_matching(original: QueryMessage) -> Self {
        QueryResponse {
            matching_query: Some(original),
            original: RawFrame::NONE,
            result: None,
            error: None,
            response_meta: None,
        }
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

fn parse_redis(v: &RValue) -> Value {
    match v {
        RValue::Nil => Value::NULL,
        RValue::Int(i) => Value::Integer(*i),
        RValue::Data(d) => Value::Bytes(Bytes::from(d.clone())),
        RValue::Bulk(b) => Value::List(b.iter().map(|v| parse_redis(v)).collect_vec()),
        RValue::Status(s) => Value::Strings(s.clone()),
        RValue::Okay => Value::Strings("OK".to_string()),
    }
}

impl redis::FromRedisValue for Value {
    fn from_redis_value(v: &RValue) -> RedisResult<Self> {
        RedisResult::Ok(parse_redis(v))
    }
}

impl redis::ToRedisArgs for Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Value::NULL => {}
            Value::None => {}
            Value::Bytes(b) => out.write_arg(b),
            Value::Strings(s) => s.write_redis_args(out),
            Value::Integer(i) => i.write_redis_args(out),
            Value::Float(f) => f.write_redis_args(out),
            Value::Boolean(b) => b.write_redis_args(out),
            Value::Timestamp(t) => format!("{}", t).write_redis_args(out),
            Value::Inet(i) => format!("{}", i).write_redis_args(out),
            Value::List(l) => l.write_redis_args(out),
            Value::Rows(r) => r.write_redis_args(out),
            _ => unreachable!(),
        }
    }
}

impl From<Frame> for Value {
    fn from(f: Frame) -> Self {
        // panic!("Aug 07 15:56:49.621  INFO instaproxy::transforms::tuneable_consistency_scatter: Response(QueryResponse { matching_query: None, original: Redis(BulkString([102, 111, 111])), result: Some(Strings("foo")), error: None })
        // ");
        //         panic!("This should be a bulk_string(byte array) and not be a string"); // I wonder if the bytes themselves need to get serialised differently....?
        match f {
            Frame::SimpleString(s) => Value::Strings(s),
            Frame::Error(e) => Value::Strings(e),
            Frame::Integer(i) => Value::Integer(i),
            Frame::BulkString(b) => Value::Bytes(Bytes::from(b)),
            Frame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            Frame::Moved(m) => Value::Strings(m),
            Frame::Ask(a) => Value::Strings(a),
            Frame::Null => Value::NULL,
        }
    }
}
impl From<&Frame> for Value {
    fn from(f: &Frame) -> Self {
        match f.clone() {
            Frame::SimpleString(s) => Value::Strings(s),
            Frame::Error(e) => Value::Strings(e),
            Frame::Integer(i) => Value::Integer(i),
            Frame::BulkString(b) => Value::Bytes(Bytes::from(b)),
            Frame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            Frame::Moved(m) => Value::Strings(m),
            Frame::Ask(a) => Value::Strings(a),
            Frame::Null => Value::NULL,
        }
    }
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
    pub fn value_byte_string(string: String) -> Value {
        Value::Bytes(Bytes::from(string))
    }

    pub fn value_byte_str(str: &'static str) -> Value {
        Value::Bytes(Bytes::from(str))
    }

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
        match self {
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
        }
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
