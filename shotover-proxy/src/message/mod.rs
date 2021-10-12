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
use redis_protocol::resp2::types::Frame;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::IpAddr;

// TODO: Clippy says this is bad due to large variation - also almost 1k in size on the stack
// Should move the message type to just be bulk..
#[derive(PartialEq, Debug, Clone)]
pub struct Messages {
    pub messages: Vec<Message>,
}

impl FromIterator<Message> for Messages {
    fn from_iter<T: IntoIterator<Item = Message>>(iter: T) -> Self {
        let mut messages = Messages::new();
        for i in iter {
            messages.messages.push(i);
        }
        messages
    }
}

impl IntoIterator for Messages {
    type Item = Message;
    type IntoIter = std::vec::IntoIter<Message>;

    fn into_iter(self) -> Self::IntoIter {
        self.messages.into_iter()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    pub details: MessageDetails,
    pub modified: bool,
    pub original: RawFrame,
}

#[derive(PartialEq, Debug, Clone)]
pub enum MessageDetails {
    Bypass(Box<MessageDetails>),
    Query(QueryMessage),
    Response(QueryResponse),
    Unknown,
}

impl Message {
    pub fn new(details: MessageDetails, modified: bool, original: RawFrame) -> Self {
        Message {
            details,
            modified,
            original,
        }
    }

    pub fn generate_message_details_response(&mut self) {
        if let MessageDetails::Unknown = self.details {
            self.details = self.original.build_message_response().unwrap()
        }
    }

    pub fn generate_message_details_query(&mut self) {
        if let MessageDetails::Unknown = self.details {
            self.details = self.original.build_message_query().unwrap() // TODO: this will panic on non utf8 data
        }
    }

    pub fn new_query(qm: QueryMessage, modified: bool, original: RawFrame) -> Self {
        Self::new(MessageDetails::Query(qm), modified, original)
    }

    pub fn new_response(qr: QueryResponse, modified: bool, original: RawFrame) -> Self {
        Self::new(MessageDetails::Response(qr), modified, original)
    }

    pub fn new_bypass(raw_frame: RawFrame) -> Self {
        Self::new(
            MessageDetails::Bypass(Box::new(MessageDetails::Unknown)),
            false,
            raw_frame,
        )
    }

    pub fn new_no_original(details: MessageDetails, modified: bool) -> Self {
        Message {
            details,
            modified,
            original: RawFrame::None,
        }
    }

    pub fn into_bypass(self) -> Self {
        if let MessageDetails::Bypass(_) = &self.details {
            self
        } else {
            Message {
                details: MessageDetails::Bypass(Box::new(self.details)),
                modified: false,
                original: self.original,
            }
        }
    }
}

impl Default for Messages {
    fn default() -> Self {
        Self::new()
    }
}

impl Messages {
    pub fn new() -> Self {
        Messages { messages: vec![] }
    }

    pub fn new_with_size_hint(capacity: usize) -> Self {
        Messages {
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn new_from_message(message: Message) -> Self {
        Messages {
            messages: vec![message],
        }
    }

    pub fn get_raw_original(self) -> Vec<RawFrame> {
        self.messages.into_iter().map(|m| m.original).collect()
    }

    pub fn new_single_query(qm: QueryMessage, modified: bool, original: RawFrame) -> Self {
        Messages {
            messages: vec![Message::new(MessageDetails::Query(qm), modified, original)],
        }
    }

    pub fn new_single_bypass(raw_frame: RawFrame) -> Self {
        Messages {
            messages: vec![Message::new(MessageDetails::Unknown, false, raw_frame)],
        }
    }

    pub fn new_single_bypass_response(raw_frame: RawFrame, modified: bool) -> Self {
        Messages {
            messages: vec![Message::new(
                MessageDetails::Response(QueryResponse::empty()),
                modified,
                raw_frame,
            )],
        }
        .into_bypass()
    }

    pub fn new_single_response(qr: QueryResponse, modified: bool, original: RawFrame) -> Self {
        Messages {
            messages: vec![Message::new(
                MessageDetails::Response(qr),
                modified,
                original,
            )],
        }
    }

    pub fn into_bypass(self) -> Self {
        Messages {
            messages: self
                .messages
                .into_iter()
                .map(Message::into_bypass)
                .collect(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
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
            ASTHolder::SQL(statement) => match statement {
                Statement::Query(_) => "SELECT",
                Statement::Insert { .. } => "INSERT",
                Statement::Update { .. } => "UPDATE",
                Statement::Delete { .. } => "DELETE",
                Statement::CreateView { .. } => "CREATE VIEW",
                Statement::CreateTable { .. } => "CREATE TABLE",
                Statement::AlterTable { .. } => "ALTER TABLE",
                Statement::Drop { .. } => "DROP",
                _ => "UNKNOWN",
            }
            .to_string(),
            ASTHolder::Commands(commands) => {
                if let Value::List(coms) = commands {
                    if let Some(Value::Bytes(b)) = coms.get(0) {
                        String::from_utf8(b.to_vec())
                            .unwrap_or_else(|_| "couldn't decode".to_string())
                    } else {
                        "UNKNOWN".to_string()
                    }
                } else {
                    "UNKNOWN".to_string()
                }
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryMessage {
    pub query_string: String,
    pub namespace: Vec<String>,
    pub primary_key: HashMap<String, Value>,
    pub query_values: Option<HashMap<String, Value>>,
    pub projection: Option<Vec<String>>,
    pub query_type: QueryType,
    pub ast: Option<ASTHolder>,
}

impl QueryMessage {
    pub fn empty() -> Self {
        QueryMessage {
            query_string: "".to_string(),
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        }
    }

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
        self.get_primary_key().map(|pk| {
            let mut buffer = String::new();
            let f = self.namespace.join(".");
            buffer.push_str(f.as_str());
            buffer.push('.');
            buffer.push_str(serde_json::to_string(&pk).unwrap().as_str());
            buffer
        })
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct QueryResponse {
    pub matching_query: Option<QueryMessage>,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub response_meta: Option<Value>,
}

//TODO this could use a Builder
impl QueryResponse {
    pub fn empty() -> Self {
        QueryResponse {
            matching_query: None,
            result: None,
            error: None,
            response_meta: None,
        }
    }

    pub fn empty_with_error(error: Option<Value>) -> Self {
        QueryResponse {
            matching_query: None,
            result: None,
            error,
            response_meta: None,
        }
    }

    pub fn just_result(result: Value) -> Self {
        QueryResponse {
            matching_query: None,
            result: Some(result),
            error: None,
            response_meta: None,
        }
    }

    pub fn result_with_matching(matching: Option<QueryMessage>, result: Value) -> Self {
        QueryResponse {
            matching_query: matching,
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
            result,
            error,
            response_meta: None,
        }
    }

    pub fn error_with_matching(matching: Option<QueryMessage>, error: Value) -> Self {
        QueryResponse {
            matching_query: matching,
            result: None,
            error: Some(error),
            response_meta: None,
        }
    }

    pub fn empty_with_matching(original: QueryMessage) -> Self {
        QueryResponse {
            matching_query: Some(original),
            result: None,
            error: None,
            response_meta: None,
        }
    }
}

#[derive(PartialEq, Debug, Clone, Deserialize)]
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
    FragmentedResponse(Vec<Value>),
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
            Frame::Null => Value::NULL,
        }
    }
}

impl From<Value> for Frame {
    fn from(value: Value) -> Frame {
        match value {
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
            Value::FragmentedResponse(l) => {
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
            match spec.col_type.id {
                ColType::Ascii => Value::Strings(decode_ascii(actual_bytes).unwrap()),
                ColType::Bigint => Value::Integer(decode_bigint(actual_bytes).unwrap()),
                ColType::Blob => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Boolean => Value::Boolean(decode_boolean(actual_bytes).unwrap()),
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
                // todo: process collection types based on ColTypeOption
                // (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L569)
                _ => Value::NULL,
            }
        } else {
            Value::NULL
        }
    }

    pub fn into_str_bytes(self) -> Bytes {
        match self {
            Value::NULL => Bytes::from("".to_string()),
            Value::None => Bytes::from("".to_string()),
            Value::Bytes(b) => b,
            Value::Strings(s) => Bytes::from(s),
            Value::Integer(i) => Bytes::from(format!("{}", i)),
            Value::Float(f) => Bytes::from(format!("{}", f)),
            Value::Boolean(b) => Bytes::from(format!("{}", b)),
            Value::Timestamp(t) => {
                Bytes::from(String::from_utf8_lossy(&t.timestamp().to_le_bytes()).to_string())
            }
            Value::Inet(i) => Bytes::from(format!("{}", i)),
            _ => unimplemented!(),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self {
            Value::NULL => Bytes::new(),
            Value::None => Bytes::new(),
            Value::Bytes(b) => b,
            Value::Strings(s) => Bytes::from(s),
            Value::Integer(i) => Bytes::from(Vec::from(i.to_le_bytes())),
            Value::Float(f) => Bytes::from(Vec::from(f.to_le_bytes())),
            Value::Boolean(b) => Bytes::from(Vec::from(if b {
                (1_u8).to_le_bytes()
            } else {
                (0_u8).to_le_bytes()
            })),
            Value::Timestamp(t) => Bytes::from(Vec::from(t.timestamp().to_le_bytes())),
            Value::Inet(i) => Bytes::from(match i {
                IpAddr::V4(four) => Vec::from(four.octets()),
                IpAddr::V6(six) => Vec::from(six.octets()),
            }),
            _ => unimplemented!(),
        }
    }
}

impl From<Value> for cassandra_proto::types::value::Bytes {
    fn from(value: Value) -> cassandra_proto::types::value::Bytes {
        match value {
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
            Value::FragmentedResponse(l) => cassandra_proto::types::value::Bytes::from(l),
        }
    }
}

mod my_bytes {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(val: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(val)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(val))
    }
}
