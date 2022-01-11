use crate::protocols::RawFrame;
use bigdecimal::BigDecimal;
use bytes::Bytes;
use cassandra_protocol::{
    frame::{
        frame_error::{AdditionalErrorInfo, ErrorBody},
        frame_result::{ColSpec, ColType, ColTypeOptionValue},
        Direction, Flags, Frame as CassandraFrame, Opcode, Serialize as CassandraSerialize,
    },
    types::{
        cassandra_type::CassandraType,
        data_serialization_types::{
            decode_ascii, decode_bigint, decode_boolean, decode_date, decode_decimal,
            decode_double, decode_float, decode_inet, decode_int, decode_list, decode_map,
            decode_set, decode_smallint, decode_time, decode_timestamp, decode_tinyint,
            decode_tuple, decode_udt, decode_varchar, decode_varint,
        },
        prelude::{List, Map, Tuple, Udt},
        AsCassandraType, CBytes,
    },
};
use num::BigInt;
use ordered_float::OrderedFloat;
use redis_protocol::resp2::prelude::Frame;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::{BTreeMap, BTreeSet};
use std::{collections::HashMap, net::IpAddr};
use uuid::Uuid;

pub type Messages = Vec<Message>;

#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    pub details: MessageDetails,
    pub modified: bool,
    /// The frame in the format defined by the protocol.
    pub original: RawFrame,
}

#[derive(PartialEq, Debug, Clone)]
pub enum MessageDetails {
    Query(QueryMessage),
    Response(QueryResponse),
    // identifies a message that is to be returned to the sender.  Message is stored in the
    // `original` frame.
    ReturnToSender,
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

    pub fn new_raw(raw_frame: RawFrame) -> Self {
        Self::new(MessageDetails::Unknown, false, raw_frame)
    }

    pub fn new_no_original(details: MessageDetails, modified: bool) -> Self {
        Message {
            details,
            modified,
            original: RawFrame::None,
        }
    }

    pub fn to_filtered_reply(&self) -> Message {
        Message {
            details: MessageDetails::Unknown,
            modified: true,
            original: match &self.original {
                RawFrame::Redis(_) => RawFrame::Redis(Frame::Error(
                    "ERR Message was filtered out by shotover".into(),
                )),
                RawFrame::Cassandra(frame) => RawFrame::Cassandra(CassandraFrame {
                    version: frame.version,
                    direction: Direction::Response,
                    flags: Flags::empty(),
                    opcode: Opcode::Error,
                    stream_id: frame.stream_id,
                    body: ErrorBody {
                        error_code: 0,
                        message: "Message was filtered out by shotover".into(),
                        additional_info: AdditionalErrorInfo::Server,
                    }
                    .serialize_to_vec(),
                    tracing_id: None,
                    warnings: vec![],
                }),
                RawFrame::None => RawFrame::None,
            },
        }
    }

    /// Gets the `QueryType` for a message.
    /// First checks the `details` of the message and then falls back to the `original` message.
    pub fn get_query_type(&self) -> QueryType {
        if let MessageDetails::Query(query) = &self.details {
            query.query_type.clone()
        } else {
            self.original.get_query_type()
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
    // Statement is boxed because Statement takes up a lot more stack space than Value.
    SQL(Box<Statement>),
    Commands(Value), // A flexible representation of a structured query that will naturally convert into the required type via into/from traits
}

impl ASTHolder {
    pub fn get_command(&self) -> String {
        match self {
            ASTHolder::SQL(statement) => match **statement {
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
pub enum Value {
    NULL,
    None,
    #[serde(with = "my_bytes")]
    Bytes(Bytes),
    Ascii(String),
    Strings(String),
    Integer(i64, IntSize),
    Double(OrderedFloat<f64>),
    Float(OrderedFloat<f32>),
    Boolean(bool),
    Inet(IpAddr),
    List(Vec<Value>),
    Rows(Vec<Vec<Value>>),
    NamedRows(Vec<BTreeMap<String, Value>>),
    Document(BTreeMap<String, Value>),
    FragmentedResponse(Vec<Value>),
    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
    Varint(BigInt),
    Decimal(BigDecimal),
    Date(i32),
    Timestamp(i64),
    Timeuuid(Uuid),
    Varchar(String),
    Uuid(Uuid),
    Time(i64),
    Counter(i64),
    Tuple(Vec<Value>),
    Udt(BTreeMap<String, Value>),
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
pub enum IntSize {
    I64, // BigInt
    I32, // Int
    I16, // Smallint
    I8,  // Tinyint
}

impl From<Frame> for Value {
    fn from(f: Frame) -> Self {
        match f {
            Frame::SimpleString(s) => Value::Strings(s),
            Frame::Error(e) => Value::Strings(e),
            Frame::Integer(i) => Value::Integer(i, IntSize::I64),
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
            Frame::Integer(i) => Value::Integer(i, IntSize::I64),
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
            Value::Integer(i, _) => Frame::Integer(i),
            Value::Float(f) => Frame::SimpleString(f.to_string()),
            Value::Boolean(b) => Frame::Integer(i64::from(b)),
            Value::Inet(i) => Frame::SimpleString(i.to_string()),
            Value::List(l) => Frame::Array(l.into_iter().map(|v| v.into()).collect()),
            Value::Rows(r) => Frame::Array(r.into_iter().map(|v| Value::List(v).into()).collect()),
            Value::NamedRows(_) => unimplemented!(),
            Value::Document(_) => unimplemented!(),
            Value::FragmentedResponse(l) => Frame::Array(l.into_iter().map(|v| v.into()).collect()),
            Value::Ascii(a) => Frame::SimpleString(a),
            Value::Double(d) => Frame::SimpleString(d.to_string()),
            Value::Set(s) => Frame::Array(s.into_iter().map(|v| v.into()).collect()),
            Value::Map(_) => unimplemented!(),
            Value::Varint(v) => Frame::SimpleString(v.to_string()),
            Value::Decimal(d) => Frame::SimpleString(d.to_string()),
            Value::Date(date) => Frame::Integer(date.into()),
            Value::Timestamp(timestamp) => Frame::Integer(timestamp),
            Value::Timeuuid(timeuuid) => Frame::SimpleString(timeuuid.to_hyphenated().to_string()),
            Value::Varchar(v) => Frame::SimpleString(v),
            Value::Uuid(uuid) => Frame::SimpleString(uuid.to_hyphenated().to_string()),
            Value::Time(t) => Frame::Integer(t),
            Value::Counter(c) => Frame::Integer(c),
            Value::Tuple(_) => unimplemented!(),
            Value::Udt(_) => unimplemented!(),
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
                ColType::Bigint => {
                    Value::Integer(decode_bigint(actual_bytes).unwrap(), IntSize::I64)
                }
                ColType::Blob => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Boolean => Value::Boolean(decode_boolean(actual_bytes).unwrap()),
                ColType::Counter => Value::Counter(decode_int(actual_bytes).unwrap() as i64),
                ColType::Decimal => {
                    let decimal = decode_decimal(actual_bytes).unwrap();
                    let big_decimal = BigDecimal::new(decimal.unscaled, decimal.scale.into());
                    Value::Decimal(big_decimal)
                }
                ColType::Double => Value::Double(decode_double(actual_bytes).unwrap().into()),
                ColType::Float => Value::Float(decode_float(actual_bytes).unwrap().into()),
                ColType::Int => {
                    Value::Integer(decode_int(actual_bytes).unwrap() as i64, IntSize::I32)
                }
                ColType::Uuid => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Varchar => Value::Strings(decode_varchar(actual_bytes).unwrap()),
                ColType::Varint => Value::Varint(decode_varint(actual_bytes).unwrap()),
                ColType::Timeuuid => Value::Bytes(Bytes::copy_from_slice(actual_bytes)),
                ColType::Inet => Value::Inet(decode_inet(actual_bytes).unwrap()),
                ColType::Date => Value::Date(decode_date(actual_bytes).unwrap()),
                ColType::Timestamp => Value::Timestamp(decode_timestamp(actual_bytes).unwrap()),
                ColType::Time => Value::Time(decode_time(actual_bytes).unwrap()),
                ColType::Smallint => {
                    Value::Integer(decode_smallint(actual_bytes).unwrap() as i64, IntSize::I16)
                }
                ColType::Tinyint => {
                    Value::Integer(decode_tinyint(actual_bytes).unwrap() as i64, IntSize::I8)
                }
                ColType::List => {
                    let decoded_list = decode_list(actual_bytes).unwrap();
                    let list = List::new(spec.col_type.clone(), decoded_list)
                        .as_cassandra_type()
                        .unwrap()
                        .unwrap();

                    let typed_list = Value::create_list(list);
                    Value::List(typed_list)
                }
                ColType::Map => {
                    let decoded_map = decode_map(actual_bytes).unwrap();
                    let map = Map::new(decoded_map, spec.col_type.clone())
                        .as_cassandra_type()
                        .unwrap()
                        .unwrap();

                    #[allow(clippy::mutable_key_type)]
                    let typed_map = Value::create_map(map);

                    Value::Map(typed_map)
                }
                ColType::Set => {
                    let decoded_set = decode_set(actual_bytes).unwrap();
                    let set = List::new(spec.col_type.clone(), decoded_set)
                        .as_cassandra_type()
                        .unwrap()
                        .unwrap();

                    #[allow(clippy::mutable_key_type)]
                    let typed_set = Value::create_set(set);
                    Value::Set(typed_set)
                }
                ColType::Udt => {
                    if let Some(ColTypeOptionValue::UdtType(ref list_type_option)) =
                        spec.col_type.value
                    {
                        let len = list_type_option.descriptions.len();
                        let decoded_udt = decode_udt(actual_bytes, len).unwrap();

                        let udt = Udt::new(decoded_udt, list_type_option)
                            .as_cassandra_type()
                            .unwrap()
                            .unwrap();

                        let typed_udt = Value::create_udt(udt);

                        Value::Udt(typed_udt)
                    } else {
                        panic!("not a udt")
                    }
                }
                ColType::Tuple => {
                    if let Some(ColTypeOptionValue::TupleType(ref list_type_option)) =
                        spec.col_type.value
                    {
                        let len = list_type_option.types.len();
                        let decoded_tuple = decode_tuple(actual_bytes, len).unwrap();
                        let tuple = Tuple::new(decoded_tuple, list_type_option)
                            .as_cassandra_type()
                            .unwrap()
                            .unwrap();

                        let typed_tuple = Value::create_tuple(tuple);
                        Value::Tuple(typed_tuple)
                    } else {
                        panic!("not a tuple") // TODO
                    }
                }
                ColType::Custom => unimplemented!(),
                ColType::Null => Value::NULL,
                // TODO: process collection types based on ColTypeOption
                // (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L569)
            }
        } else {
            Value::NULL
        }
    }

    fn create_udt(collection: CassandraType) -> BTreeMap<String, Value> {
        if let CassandraType::Udt(udt) = collection {
            let mut values = BTreeMap::new();
            udt.into_iter().for_each(|(key, element)| {
                values.insert(key, Value::create_element(element));
            });

            values
        } else {
            panic!("not a udt");
        }
    }

    #[allow(clippy::mutable_key_type)]
    fn create_map(collection: CassandraType) -> BTreeMap<Value, Value> {
        if let CassandraType::Map(map) = collection {
            let mut value_list = BTreeMap::new();
            for (key, value) in map.into_iter() {
                value_list.insert(Value::create_element(key), Value::create_element(value));
            }

            value_list
        } else {
            panic!("this is not a map");
        }
    }

    fn create_element(element: CassandraType) -> Value {
        match element {
            CassandraType::Ascii(a) => Value::Ascii(a),
            CassandraType::Bigint(b) => Value::Integer(b, IntSize::I64),
            CassandraType::Blob(b) => Value::Bytes(b.into_vec().into()),
            CassandraType::Boolean(b) => Value::Boolean(b),
            CassandraType::Counter(c) => Value::Counter(c),
            CassandraType::Decimal(d) => {
                let big_decimal = BigDecimal::new(d.unscaled, d.scale.into());
                Value::Decimal(big_decimal)
            }
            CassandraType::Double(d) => Value::Double(d.into()),
            CassandraType::Float(f) => Value::Float(f.into()),
            CassandraType::Int(c) => Value::Integer(c as i64, IntSize::I64),
            CassandraType::Timestamp(t) => Value::Timestamp(t),
            CassandraType::Uuid(u) => Value::Uuid(u),
            CassandraType::Varchar(v) => Value::Varchar(v),
            CassandraType::Varint(v) => Value::Varint(v),
            CassandraType::Timeuuid(t) => Value::Timeuuid(t),
            CassandraType::Inet(i) => Value::Inet(i),
            CassandraType::Date(d) => Value::Date(d),
            CassandraType::Time(d) => Value::Time(d),
            CassandraType::Smallint(d) => Value::Integer(d.into(), IntSize::I16),
            CassandraType::Tinyint(d) => Value::Integer(d.into(), IntSize::I8),
            CassandraType::List(_) => Value::List(Value::create_list(element)),
            CassandraType::Map(_) => Value::Map(Value::create_map(element)),
            CassandraType::Set(_) => Value::Set(Value::create_set(element)),
            CassandraType::Udt(_) => Value::Udt(Value::create_udt(element)),
            CassandraType::Tuple(_) => Value::Tuple(Value::create_tuple(element)),
            CassandraType::Null => Value::NULL,
        }
    }

    fn create_list(collection: CassandraType) -> Vec<Value> {
        match collection {
            CassandraType::List(collection) => {
                let mut value_list = Vec::with_capacity(collection.len());
                for element in collection.into_iter() {
                    value_list.push(Value::create_element(element));
                }

                value_list
            }
            _ => panic!("this is not a list"), // TODO handle this better
        }
    }

    #[allow(clippy::mutable_key_type)]
    fn create_set(collection: CassandraType) -> BTreeSet<Value> {
        match collection {
            CassandraType::List(collection) | CassandraType::Set(collection) => {
                let mut value_list = BTreeSet::new();
                for element in collection.into_iter() {
                    value_list.insert(Value::create_element(element));
                }

                value_list
            }
            _ => panic!("this is not a set"), // TODO handle this better
        }
    }

    fn create_tuple(collection: CassandraType) -> Vec<Value> {
        match collection {
            CassandraType::Tuple(collection) => {
                let mut value_list = Vec::with_capacity(collection.len());
                for element in collection.into_iter() {
                    value_list.push(Value::create_element(element));
                }

                value_list
            }
            _ => panic!("this is not a tuple"),
        }
    }

    pub fn into_str_bytes(self) -> Bytes {
        match self {
            Value::NULL => Bytes::from("".to_string()),
            Value::None => Bytes::from("".to_string()),
            Value::Bytes(b) => b,
            Value::Strings(s) => Bytes::from(s),
            Value::Integer(i, _) => Bytes::from(format!("{}", i)),
            Value::Float(f) => Bytes::from(format!("{}", f)),
            Value::Boolean(b) => Bytes::from(format!("{}", b)),
            Value::Inet(i) => Bytes::from(format!("{}", i)),
            Value::FragmentedResponse(_) => unimplemented!(),
            Value::Document(_) => unimplemented!(),
            Value::NamedRows(_) => unimplemented!(),
            Value::List(_) => unimplemented!(),
            Value::Rows(_) => unimplemented!(),
            Value::Ascii(_) => unimplemented!(),
            Value::Double(_) => unimplemented!(),
            Value::Set(_) => unimplemented!(),
            Value::Map(_) => unimplemented!(),
            Value::Varint(_) => unimplemented!(),
            Value::Decimal(_) => unimplemented!(),
            Value::Date(_) => unimplemented!(),
            Value::Timestamp(_) => unimplemented!(),
            Value::Timeuuid(_) => unimplemented!(),
            Value::Varchar(_) => unimplemented!(),
            Value::Uuid(_) => unimplemented!(),
            Value::Time(_) => unimplemented!(),
            Value::Counter(_) => unimplemented!(),
            Value::Tuple(_) => unimplemented!(),
            Value::Udt(_) => unimplemented!(),
        }
    }

    pub fn into_bytes(self) -> Bytes {
        match self {
            Value::NULL => Bytes::new(),
            Value::None => Bytes::new(),
            Value::Bytes(b) => b,
            Value::Strings(s) => Bytes::from(s),
            Value::Integer(i, _) => Bytes::from(Vec::from(i.to_le_bytes())),
            Value::Float(f) => Bytes::from(Vec::from(f.to_le_bytes())),
            Value::Boolean(b) => Bytes::from(Vec::from(if b {
                (1_u8).to_le_bytes()
            } else {
                (0_u8).to_le_bytes()
            })),
            Value::Inet(i) => Bytes::from(match i {
                IpAddr::V4(four) => Vec::from(four.octets()),
                IpAddr::V6(six) => Vec::from(six.octets()),
            }),
            Value::FragmentedResponse(_) => unimplemented!(),
            Value::Document(_) => unimplemented!(),
            Value::NamedRows(_) => unimplemented!(),
            Value::List(_) => unimplemented!(),
            Value::Rows(_) => unimplemented!(),
            Value::Ascii(_) => unimplemented!(),
            Value::Double(_) => unimplemented!(),
            Value::Set(_) => unimplemented!(),
            Value::Map(_) => unimplemented!(),
            Value::Varint(_) => unimplemented!(),
            Value::Decimal(_) => unimplemented!(),
            Value::Date(_) => unimplemented!(),
            Value::Timestamp(_) => unimplemented!(),
            Value::Timeuuid(_) => unimplemented!(),
            Value::Varchar(_) => unimplemented!(),
            Value::Uuid(_) => unimplemented!(),
            Value::Time(_) => unimplemented!(),
            Value::Counter(_) => unimplemented!(),
            Value::Tuple(_) => unimplemented!(),
            Value::Udt(_) => unimplemented!(),
        }
    }
}

impl From<Value> for cassandra_protocol::types::value::Bytes {
    fn from(value: Value) -> cassandra_protocol::types::value::Bytes {
        match value {
            Value::NULL => (-1).into(),
            Value::None => cassandra_protocol::types::value::Bytes::new(vec![]),
            Value::Bytes(b) => cassandra_protocol::types::value::Bytes::new(b.to_vec()),
            Value::Strings(s) => s.into(),
            Value::Integer(i, _) => i.into(),
            Value::Float(f) => f.into_inner().into(),
            Value::Boolean(b) => b.into(),
            Value::List(l) => cassandra_protocol::types::value::Bytes::from(l),
            Value::Rows(r) => cassandra_protocol::types::value::Bytes::from(r),
            Value::NamedRows(n) => cassandra_protocol::types::value::Bytes::from(n),
            Value::Document(d) => cassandra_protocol::types::value::Bytes::from(d),
            Value::Inet(i) => i.into(),
            Value::FragmentedResponse(l) => cassandra_protocol::types::value::Bytes::from(l),
            Value::Ascii(_) => unimplemented!(),
            Value::Double(_) => unimplemented!(),
            Value::Set(_) => unimplemented!(),
            Value::Map(_) => unimplemented!(),
            Value::Varint(_) => unimplemented!(),
            Value::Decimal(_) => unimplemented!(),
            Value::Date(_) => unimplemented!(),
            Value::Timestamp(_) => unimplemented!(),
            Value::Timeuuid(_) => unimplemented!(),
            Value::Varchar(_) => unimplemented!(),
            Value::Uuid(_) => unimplemented!(),
            Value::Time(_) => unimplemented!(),
            Value::Counter(_) => unimplemented!(),
            Value::Tuple(_) => unimplemented!(),
            Value::Udt(_) => unimplemented!(),
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
