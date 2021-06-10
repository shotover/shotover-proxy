use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::iter::FromIterator;
use std::net::IpAddr;
use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
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
use dyn_clone::DynClone;
use metrics::{counter, timing};
use redis_protocol::types::Frame;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::codec::{Decoder, Encoder};

use ast::ASTHolder;

use crate::ast::{process_redis_frame, redis_query_type};
use crate::chain::TransformChain;

pub mod ast;
pub mod chain;
pub mod concurrency;
pub mod protocol;
pub mod util;

pub extern crate sqlparser;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub type TransformCall =
    fn(message: Messages) -> Pin<Box<dyn Future<Output = ChainResponse> + Send + Sync>>;

#[derive(Copy, Clone, Debug)]
pub struct LibDeclaration {
    pub ns_version: &'static str,
    pub rustc_version: &'static str,
    pub transform_call: TransformCall,
}

pub type ChainResponse = anyhow::Result<Messages>;

// TODO: Clippy says this is bad due to large variation - also almost 1k in size on the stack
// Should move the message type to just be bulk..
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub details: MessageDetails,
    pub modified: bool,
    pub original: RawFrame,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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

    pub fn generate_message_details(&mut self, response: bool) {
        if let MessageDetails::Unknown = self.details {
            self.details = self.original.build_message(response).unwrap()
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct RawMessage {
    pub original: RawFrame,
}

// Transforms should not try to directly serialize the AST - it's purely an in-memory representation
// query_string is also mainly there from a debugging / logging perspective as its a utf8
// encoded represntation of the query.
// Statement can be "serialized"/rendered through it's display methods
// Commands can be serialized by getting the underlying Value

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryMessage {
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
        if let Some(pk) = self.get_primary_key() {
            let mut buffer = String::new();
            let f: String = self.namespace.join(".");
            buffer.push_str(f.as_str());
            buffer.push('.');
            buffer.push_str(serde_json::to_string(&pk).unwrap().as_str());
            return Some(buffer);
        }
        None
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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

impl From<Frame> for Value {
    fn from(f: Frame) -> Self {
        // panic!("Aug 07 15:56:49.621  INFO instaproxy::transforms::tuneable_consistency_scatter: Response(QueryResponse { matching_query: None, original: Redis(BulkString([102, 111, 111])), result: Some(Strings("foo")), error: None })
        // ");
        //         panic!("This should be a bulk_string(byte array) and not be a string"); // I wonder if the bytes themselves need to get serialised differently....?
        match f {
            Frame::SimpleString(s) => Value::Strings(s),
            Frame::Error(e) => Value::Strings(e),
            Frame::Integer(i) => Value::Integer(i),
            Frame::BulkString(b) => Value::Bytes(b),
            Frame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            Frame::Moved { slot, host, port } => {
                Value::Strings(format!("MOVED {} {}:{}", slot, host, port))
            }
            Frame::Ask { slot, host, port } => {
                Value::Strings(format!("ASK {} {}:{}", slot, host, port))
            }
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
            Frame::BulkString(b) => Value::Bytes(b),
            Frame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            Frame::Moved { slot, host, port } => {
                Value::Strings(format!("MOVED {} {}:{}", slot, host, port))
            }
            Frame::Ask { slot, host, port } => {
                Value::Strings(format!("ASK {} {}:{}", slot, host, port))
            }
            Frame::Null => Value::NULL,
        }
    }
}

impl Into<Frame> for Value {
    fn into(self) -> Frame {
        match self {
            Value::NULL => Frame::Null,
            Value::None => unimplemented!(),
            Value::Bytes(b) => Frame::BulkString(b),
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

#[derive(Eq, PartialEq, Debug, Clone, Hash, Serialize, Deserialize)]
pub enum RawFrame {
    Cassandra(cassandra_proto::frame::Frame),
    Redis(redis_protocol::types::Frame),
    None,
}

impl RawFrame {
    pub fn build_message(&self, response: bool) -> Result<MessageDetails> {
        match self {
            RawFrame::Cassandra(_c) => Ok(MessageDetails::Unknown),
            RawFrame::Redis(r) => process_redis_frame(r, response),
            RawFrame::None => Ok(MessageDetails::Unknown),
        }
    }

    #[inline]
    pub fn get_query_type(&self) -> QueryType {
        match self {
            RawFrame::Cassandra(_) => QueryType::ReadWrite,
            RawFrame::Redis(r) => redis_query_type(r),
            RawFrame::None => QueryType::ReadWrite,
        }
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

#[async_trait]
pub trait Transform: DynClone + Send + Sync + Debug {
    async fn transform<'a>(&'a mut self, wrapped_messages: Wrapper<'a>) -> ChainResponse;

    fn get_name(&self) -> &'static str;

    async fn prep_transform_chain(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_transform_future<'a>(
        &'a mut self,
        wrapped_messages: Wrapper<'a>,
    ) -> Pin<Box<dyn core::future::Future<Output = ChainResponse> + Send + 'a>> {
        self.transform(wrapped_messages)
    }
}

#[typetag::serde]
#[async_trait]
pub trait TransformsFromConfig: DynClone + Sync + Send + Debug {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>>;

    fn get_source_future<'a>(
        &'a self,
        topics: &'a TopicHolder,
    ) -> Pin<
        Box<
            dyn core::future::Future<Output = Result<Box<dyn Transform + Send + Sync>>> + Send + 'a,
        >,
    > {
        self.get_source(topics)
    }
}

dyn_clone::clone_trait_object!(TransformsFromConfig);
dyn_clone::clone_trait_object!(Transform);

pub struct TopicHolder {
    pub topics_rx: HashMap<String, Receiver<ChannelMessage>>,
    pub topics_tx: HashMap<String, Sender<ChannelMessage>>,
}

impl TopicHolder {
    pub fn get_rx(&mut self, name: &str) -> Option<Receiver<ChannelMessage>> {
        let rx = self.topics_rx.remove(name)?;
        Some(rx)
    }

    pub fn get_tx(&self, name: &str) -> Option<Sender<ChannelMessage>> {
        let tx = self.topics_tx.get(name)?;
        Some(tx.clone())
    }

    pub fn new() -> Self {
        TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct ChannelMessage {
    pub messages: Messages,
    pub return_chan: Option<oneshot::Sender<ChainResponse>>,
}

impl ChannelMessage {
    pub fn new_with_no_return(m: Messages) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: None,
        }
    }

    pub fn new(m: Messages, return_chan: oneshot::Sender<ChainResponse>) -> Self {
        ChannelMessage {
            messages: m,
            return_chan: Some(return_chan),
        }
    }
}

pub struct Wrapper<'a> {
    pub message: Messages,
    pub transforms: Vec<&'a mut Box<dyn Transform + Send + Sync>>,
}

impl<'a> Debug for Wrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_list()
            .entries(self.message.messages.iter())
            .finish()
    }
}

impl<'a> Clone for Wrapper<'a> {
    fn clone(&self) -> Self {
        Wrapper {
            message: self.message.clone(),
            transforms: vec![],
        }
    }
}

impl<'a> Display for Wrapper<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("{:#?}", self.message))
    }
}

impl<'a> Wrapper<'a> {
    pub async fn call_next_transform(mut self) -> ChainResponse {
        let t = self.transforms.remove(0);

        let name = t.get_name();
        let start = Instant::now();
        let result;
        {
            result = t.transform(self).await;
        }
        let end = Instant::now();
        counter!("shotover_transform_total", 1, "transform" => name);
        if result.is_err() {
            counter!("shotover_transform_failures", 1, "transform" => name)
        }
        timing!("shotover_transform_latency", start, end, "transform" => name);
        result
    }

    pub fn swap_message(&mut self, mut m: Messages) {
        std::mem::swap(&mut self.message, &mut m);
    }

    pub fn new(m: Messages) -> Self {
        Wrapper {
            message: m,
            transforms: vec![],
        }
    }

    pub fn new_with_next_transform(m: Messages, _next_transform: usize) -> Self {
        Wrapper {
            message: m,
            transforms: vec![],
        }
    }

    pub fn reset(&mut self, transforms: Vec<&'a mut Box<dyn Transform + Send + Sync>>) {
        self.transforms = transforms;
    }
}

pub async fn build_chain_from_config(
    name: String,
    transform_configs: &[Box<dyn TransformsFromConfig + Send + Sync>],
    topics: &TopicHolder,
) -> Result<TransformChain> {
    let mut transforms: Vec<Box<dyn Transform + Send + Sync>> = Vec::new();
    let mut configs: Vec<Box<dyn TransformsFromConfig + Send + Sync>> = Vec::new();
    for tc in transform_configs {
        transforms.push(tc.get_source(topics).await?);
        configs.push(tc.clone());
    }
    Ok(TransformChain::new_with_configs(transforms, name, configs))
}

// TODO: Replace with trait_alias (RFC#1733).
pub trait CodecReadHalf: Decoder<Item = Messages, Error = anyhow::Error> + Clone + Send {}

impl<T: Decoder<Item = Messages, Error = anyhow::Error> + Clone + Send> CodecReadHalf for T {}

// TODO: Replace with trait_alias (RFC#1733).
pub trait CodecWriteHalf: Encoder<Messages, Error = anyhow::Error> + Clone + Send {}

impl<T: Encoder<Messages, Error = anyhow::Error> + Clone + Send> CodecWriteHalf for T {}

// TODO: Replace with trait_alias (RFC#1733).
pub trait Codec: CodecReadHalf + CodecWriteHalf {}

impl<T: CodecReadHalf + CodecWriteHalf> Codec for T {}
