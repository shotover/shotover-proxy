use crate::protocols::CassandraFrame;
use crate::protocols::RawFrame;
use crate::protocols::RedisFrame;
use bigdecimal::BigDecimal;
use bytes::Bytes;
use cassandra_protocol::{
    frame::{
        frame_error::{AdditionalErrorInfo, ErrorBody},
        frame_result::{ColSpec, ColTypeOption},
        Direction, Flags, Opcode, Serialize as CassandraSerialize,
    },
    types::{
        cassandra_type::{wrapper_fn, CassandraType},
        CBytes,
    },
};
use num::BigInt;
use ordered_float::OrderedFloat;
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
                RawFrame::Redis(_) => RawFrame::Redis(RedisFrame::Error(
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

impl From<RedisFrame> for Value {
    fn from(f: RedisFrame) -> Self {
        match f {
            RedisFrame::SimpleString(s) => Value::Strings(s),
            RedisFrame::Error(e) => Value::Strings(e),
            RedisFrame::Integer(i) => Value::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => Value::Bytes(Bytes::from(b)),
            RedisFrame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            RedisFrame::Null => Value::NULL,
        }
    }
}
impl From<&RedisFrame> for Value {
    fn from(f: &RedisFrame) -> Self {
        match f.clone() {
            RedisFrame::SimpleString(s) => Value::Strings(s),
            RedisFrame::Error(e) => Value::Strings(e),
            RedisFrame::Integer(i) => Value::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => Value::Bytes(Bytes::from(b)),
            RedisFrame::Array(a) => Value::List(a.iter().cloned().map(Value::from).collect()),
            RedisFrame::Null => Value::NULL,
        }
    }
}

impl From<Value> for RedisFrame {
    fn from(value: Value) -> RedisFrame {
        match value {
            Value::NULL => RedisFrame::Null,
            Value::None => todo!(),
            Value::Bytes(b) => RedisFrame::BulkString(b.to_vec()),
            Value::Strings(s) => RedisFrame::SimpleString(s),
            Value::Integer(i, _) => RedisFrame::Integer(i),
            Value::Float(f) => RedisFrame::SimpleString(f.to_string()),
            Value::Boolean(b) => RedisFrame::Integer(i64::from(b)),
            Value::Inet(i) => RedisFrame::SimpleString(i.to_string()),
            Value::List(l) => RedisFrame::Array(l.into_iter().map(|v| v.into()).collect()),
            Value::Rows(r) => {
                RedisFrame::Array(r.into_iter().map(|v| Value::List(v).into()).collect())
            }
            Value::NamedRows(_) => todo!(),
            Value::Document(_) => todo!(),
            Value::FragmentedResponse(l) => {
                RedisFrame::Array(l.into_iter().map(|v| v.into()).collect())
            }
            Value::Ascii(_a) => todo!(),
            Value::Double(_d) => todo!(),
            Value::Set(_s) => todo!(),
            Value::Map(_) => todo!(),
            Value::Varint(_v) => todo!(),
            Value::Decimal(_d) => todo!(),
            Value::Date(_date) => todo!(),
            Value::Timestamp(_timestamp) => todo!(),
            Value::Timeuuid(_timeuuid) => todo!(),
            Value::Varchar(_v) => todo!(),
            Value::Uuid(_uuid) => todo!(),
            Value::Time(_t) => todo!(),
            Value::Counter(_c) => todo!(),
            Value::Tuple(_) => todo!(),
            Value::Udt(_) => todo!(),
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
        let cassandra_type = Value::into_cassandra_type(&spec.col_type, data);
        Value::create_element(cassandra_type)
    }

    fn into_cassandra_type(col_type: &ColTypeOption, data: &CBytes) -> CassandraType {
        let wrapper = wrapper_fn(&col_type.id);
        wrapper(data, col_type).unwrap()
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
            CassandraType::List(list) => {
                let value_list = list.into_iter().map(Value::create_element).collect();
                Value::List(value_list)
            }
            CassandraType::Map(map) => Value::Map(
                map.into_iter()
                    .map(|(key, value)| (Value::create_element(key), Value::create_element(value)))
                    .collect(),
            ),
            CassandraType::Set(set) => {
                Value::Set(set.into_iter().map(Value::create_element).collect())
            }
            CassandraType::Udt(udt) => {
                let values = udt
                    .into_iter()
                    .map(|(key, element)| (key, Value::create_element(element)))
                    .collect();
                Value::Udt(values)
            }
            CassandraType::Tuple(tuple) => {
                let value_list = tuple.into_iter().map(Value::create_element).collect();
                Value::Tuple(value_list)
            }
            CassandraType::Null => Value::NULL,
        }
    }

    pub fn into_str_bytes(self) -> Bytes {
        match self {
            Value::NULL => Bytes::from("".to_string()),
            Value::None => Bytes::from("".to_string()),
            Value::Bytes(b) => b,
            Value::Strings(s) => Bytes::from(s),
            Value::Integer(i, _) => Bytes::from(format!("{i}")),
            Value::Float(f) => Bytes::from(format!("{f}")),
            Value::Boolean(b) => Bytes::from(format!("{b}")),
            Value::Inet(i) => Bytes::from(format!("{i}")),
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
