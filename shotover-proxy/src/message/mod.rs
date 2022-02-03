use crate::frame::cassandra::CassandraOperation;
use crate::frame::CassandraFrame;
use crate::frame::Frame;
use crate::frame::RedisFrame;
use bigdecimal::BigDecimal;
use bytes::Bytes;
use cassandra_protocol::{
    frame::{
        frame_error::{AdditionalErrorInfo, ErrorBody},
        frame_result::{ColSpec, ColTypeOption},
    },
    types::{
        cassandra_type::{wrapper_fn, CassandraType},
        CBytes,
    },
};
use itertools::Itertools;
use num::BigInt;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use sqlparser::ast::Value as SQLValue;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::IpAddr;
use uuid::Uuid;

pub type Messages = Vec<Message>;

#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    pub details: MessageDetails,
    pub modified: bool,
    /// The frame in the format defined by the protocol.
    pub original: Frame,
    /// identifies a message that is to be returned to the sender.  Message is stored in the `original` frame.
    pub return_to_sender: bool,
}

#[derive(PartialEq, Debug, Clone)]
pub enum MessageDetails {
    Query(QueryMessage),
    Response(QueryResponse),
    Unknown,
}

impl Message {
    pub fn new(details: MessageDetails, modified: bool, original: Frame) -> Self {
        Message {
            details,
            modified,
            original,
            return_to_sender: false,
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

    pub fn new_query(qm: QueryMessage, modified: bool, original: Frame) -> Self {
        Self::new(MessageDetails::Query(qm), modified, original)
    }

    pub fn new_response(qr: QueryResponse, modified: bool, original: Frame) -> Self {
        Self::new(MessageDetails::Response(qr), modified, original)
    }

    pub fn from_frame(raw_frame: Frame) -> Self {
        Self::new(MessageDetails::Unknown, false, raw_frame)
    }

    pub fn new_no_original(details: MessageDetails, modified: bool) -> Self {
        Message {
            details,
            modified,
            original: Frame::None,
            return_to_sender: false,
        }
    }

    #[must_use]
    pub fn to_filtered_reply(&self) -> Message {
        Message {
            details: MessageDetails::Unknown,
            modified: true,
            original: match &self.original {
                Frame::Redis(_) => Frame::Redis(RedisFrame::Error(
                    "ERR Message was filtered out by shotover".into(),
                )),
                Frame::Cassandra(frame) => Frame::Cassandra(CassandraFrame {
                    version: frame.version,
                    stream_id: frame.stream_id,
                    operation: CassandraOperation::Error(ErrorBody {
                        error_code: 0,
                        message: "Message was filtered out by shotover".into(),
                        additional_info: AdditionalErrorInfo::Server,
                    }),
                    tracing_id: None,
                    warnings: vec![],
                }),
                Frame::None => Frame::None,
            },
            return_to_sender: false,
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
    pub original: Frame,
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
    Commands(MessageValue), // A flexible representation of a structured query that will naturally convert into the required type via into/from traits
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
                if let MessageValue::List(coms) = commands {
                    if let Some(MessageValue::Bytes(b)) = coms.get(0) {
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
    pub primary_key: HashMap<String, MessageValue>,
    pub query_values: Option<HashMap<String, MessageValue>>,
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
    pub result: Option<MessageValue>,
    pub error: Option<MessageValue>,
    pub response_meta: Option<MessageValue>,
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

    pub fn empty_with_error(error: Option<MessageValue>) -> Self {
        QueryResponse {
            matching_query: None,
            result: None,
            error,
            response_meta: None,
        }
    }

    pub fn just_result(result: MessageValue) -> Self {
        QueryResponse {
            matching_query: None,
            result: Some(result),
            error: None,
            response_meta: None,
        }
    }

    pub fn result_with_matching(matching: Option<QueryMessage>, result: MessageValue) -> Self {
        QueryResponse {
            matching_query: matching,
            result: Some(result),
            error: None,
            response_meta: None,
        }
    }

    pub fn result_error_with_matching(
        matching: Option<QueryMessage>,
        result: Option<MessageValue>,
        error: Option<MessageValue>,
    ) -> Self {
        QueryResponse {
            matching_query: matching,
            result,
            error,
            response_meta: None,
        }
    }

    pub fn error_with_matching(matching: Option<QueryMessage>, error: MessageValue) -> Self {
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
pub enum MessageValue {
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
    List(Vec<MessageValue>),
    Rows(Vec<Vec<MessageValue>>),
    NamedRows(Vec<BTreeMap<String, MessageValue>>),
    Document(BTreeMap<String, MessageValue>),
    FragmentedResponse(Vec<MessageValue>),
    Set(BTreeSet<MessageValue>),
    Map(BTreeMap<MessageValue, MessageValue>),
    Varint(BigInt),
    Decimal(BigDecimal),
    Date(i32),
    Timestamp(i64),
    Timeuuid(Uuid),
    Varchar(String),
    Uuid(Uuid),
    Time(i64),
    Counter(i64),
    Tuple(Vec<MessageValue>),
    Udt(BTreeMap<String, MessageValue>),
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
pub enum IntSize {
    I64, // BigInt
    I32, // Int
    I16, // Smallint
    I8,  // Tinyint
}

impl From<&MessageValue> for SQLValue {
    fn from(v: &MessageValue) -> Self {
        match v {
            MessageValue::NULL => SQLValue::Null,
            MessageValue::Bytes(b) => {
                SQLValue::SingleQuotedString(String::from_utf8(b.to_vec()).unwrap())
            } // TODO: this is definitely wrong
            MessageValue::Strings(s) => SQLValue::SingleQuotedString(s.clone()),
            MessageValue::Integer(i, _) => SQLValue::Number(i.to_string(), false),
            MessageValue::Float(f) => SQLValue::Number(f.to_string(), false),
            MessageValue::Boolean(b) => SQLValue::Boolean(*b),
            _ => SQLValue::Null,
        }
    }
}

impl From<&SQLValue> for MessageValue {
    fn from(v: &SQLValue) -> Self {
        match v {
            SQLValue::Number(v, false)
            | SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v) => MessageValue::Strings(v.clone()),
            SQLValue::HexStringLiteral(v) => MessageValue::Strings(v.to_string()),
            SQLValue::Boolean(v) => MessageValue::Boolean(*v),
            _ => MessageValue::Strings("NULL".to_string()),
        }
    }
}

impl From<RedisFrame> for MessageValue {
    fn from(f: RedisFrame) -> Self {
        match f {
            RedisFrame::SimpleString(s) => {
                MessageValue::Strings(String::from_utf8_lossy(&s).to_string())
            }
            RedisFrame::Error(e) => MessageValue::Strings(e.to_string()),
            RedisFrame::Integer(i) => MessageValue::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => MessageValue::Bytes(b),
            RedisFrame::Array(a) => {
                MessageValue::List(a.iter().cloned().map(MessageValue::from).collect())
            }
            RedisFrame::Null => MessageValue::NULL,
        }
    }
}
impl From<&RedisFrame> for MessageValue {
    fn from(f: &RedisFrame) -> Self {
        match f.clone() {
            RedisFrame::SimpleString(s) => {
                MessageValue::Strings(String::from_utf8_lossy(s.as_ref()).to_string())
            }
            RedisFrame::Error(e) => MessageValue::Strings(e.to_string()),
            RedisFrame::Integer(i) => MessageValue::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => MessageValue::Bytes(b),
            RedisFrame::Array(a) => {
                MessageValue::List(a.iter().cloned().map(MessageValue::from).collect())
            }
            RedisFrame::Null => MessageValue::NULL,
        }
    }
}

impl From<MessageValue> for RedisFrame {
    fn from(value: MessageValue) -> RedisFrame {
        match value {
            MessageValue::NULL => RedisFrame::Null,
            MessageValue::None => todo!(),
            MessageValue::Bytes(b) => RedisFrame::BulkString(b),
            MessageValue::Strings(s) => RedisFrame::SimpleString(s.into()),
            MessageValue::Integer(i, _) => RedisFrame::Integer(i),
            MessageValue::Float(f) => RedisFrame::SimpleString(f.to_string().into()),
            MessageValue::Boolean(b) => RedisFrame::Integer(i64::from(b)),
            MessageValue::Inet(i) => RedisFrame::SimpleString(i.to_string().into()),
            MessageValue::List(l) => RedisFrame::Array(l.into_iter().map(|v| v.into()).collect()),
            MessageValue::Rows(r) => RedisFrame::Array(
                r.into_iter()
                    .map(|v| MessageValue::List(v).into())
                    .collect(),
            ),
            MessageValue::NamedRows(_) => todo!(),
            MessageValue::Document(_) => todo!(),
            MessageValue::FragmentedResponse(l) => {
                RedisFrame::Array(l.into_iter().map(|v| v.into()).collect())
            }
            MessageValue::Ascii(_a) => todo!(),
            MessageValue::Double(_d) => todo!(),
            MessageValue::Set(_s) => todo!(),
            MessageValue::Map(_) => todo!(),
            MessageValue::Varint(_v) => todo!(),
            MessageValue::Decimal(_d) => todo!(),
            MessageValue::Date(_date) => todo!(),
            MessageValue::Timestamp(_timestamp) => todo!(),
            MessageValue::Timeuuid(_timeuuid) => todo!(),
            MessageValue::Varchar(_v) => todo!(),
            MessageValue::Uuid(_uuid) => todo!(),
            MessageValue::Time(_t) => todo!(),
            MessageValue::Counter(_c) => todo!(),
            MessageValue::Tuple(_) => todo!(),
            MessageValue::Udt(_) => todo!(),
        }
    }
}

impl MessageValue {
    pub fn value_byte_string(string: String) -> MessageValue {
        MessageValue::Bytes(Bytes::from(string))
    }

    pub fn value_byte_str(str: &'static str) -> MessageValue {
        MessageValue::Bytes(Bytes::from(str))
    }

    pub fn build_value_from_cstar_col_type(spec: &ColSpec, data: &CBytes) -> MessageValue {
        let cassandra_type = MessageValue::into_cassandra_type(&spec.col_type, data);
        MessageValue::create_element(cassandra_type)
    }

    fn into_cassandra_type(col_type: &ColTypeOption, data: &CBytes) -> CassandraType {
        let wrapper = wrapper_fn(&col_type.id);
        wrapper(data, col_type).unwrap()
    }

    fn create_element(element: CassandraType) -> MessageValue {
        match element {
            CassandraType::Ascii(a) => MessageValue::Ascii(a),
            CassandraType::Bigint(b) => MessageValue::Integer(b, IntSize::I64),
            CassandraType::Blob(b) => MessageValue::Bytes(b.into_vec().into()),
            CassandraType::Boolean(b) => MessageValue::Boolean(b),
            CassandraType::Counter(c) => MessageValue::Counter(c),
            CassandraType::Decimal(d) => {
                let big_decimal = BigDecimal::new(d.unscaled, d.scale.into());
                MessageValue::Decimal(big_decimal)
            }
            CassandraType::Double(d) => MessageValue::Double(d.into()),
            CassandraType::Float(f) => MessageValue::Float(f.into()),
            CassandraType::Int(c) => MessageValue::Integer(c as i64, IntSize::I64),
            CassandraType::Timestamp(t) => MessageValue::Timestamp(t),
            CassandraType::Uuid(u) => MessageValue::Uuid(u),
            CassandraType::Varchar(v) => MessageValue::Varchar(v),
            CassandraType::Varint(v) => MessageValue::Varint(v),
            CassandraType::Timeuuid(t) => MessageValue::Timeuuid(t),
            CassandraType::Inet(i) => MessageValue::Inet(i),
            CassandraType::Date(d) => MessageValue::Date(d),
            CassandraType::Time(d) => MessageValue::Time(d),
            CassandraType::Smallint(d) => MessageValue::Integer(d.into(), IntSize::I16),
            CassandraType::Tinyint(d) => MessageValue::Integer(d.into(), IntSize::I8),
            CassandraType::List(list) => {
                let value_list = list.into_iter().map(MessageValue::create_element).collect();
                MessageValue::List(value_list)
            }
            CassandraType::Map(map) => MessageValue::Map(
                map.into_iter()
                    .map(|(key, value)| {
                        (
                            MessageValue::create_element(key),
                            MessageValue::create_element(value),
                        )
                    })
                    .collect(),
            ),
            CassandraType::Set(set) => {
                MessageValue::Set(set.into_iter().map(MessageValue::create_element).collect())
            }
            CassandraType::Udt(udt) => {
                let values = udt
                    .into_iter()
                    .map(|(key, element)| (key, MessageValue::create_element(element)))
                    .collect();
                MessageValue::Udt(values)
            }
            CassandraType::Tuple(tuple) => {
                let value_list = tuple
                    .into_iter()
                    .map(MessageValue::create_element)
                    .collect();
                MessageValue::Tuple(value_list)
            }
            CassandraType::Null => MessageValue::NULL,
        }
    }

    pub fn into_str_bytes(self) -> Bytes {
        match self {
            MessageValue::NULL => Bytes::from("".to_string()),
            MessageValue::None => Bytes::from("".to_string()),
            MessageValue::Bytes(b) => b,
            MessageValue::Strings(s) => Bytes::from(s),
            MessageValue::Integer(i, _) => Bytes::from(format!("{i}")),
            MessageValue::Float(f) => Bytes::from(format!("{f}")),
            MessageValue::Boolean(b) => Bytes::from(format!("{b}")),
            MessageValue::Inet(i) => Bytes::from(format!("{i}")),
            MessageValue::FragmentedResponse(_) => unimplemented!(),
            MessageValue::Document(_) => unimplemented!(),
            MessageValue::NamedRows(_) => unimplemented!(),
            MessageValue::List(_) => unimplemented!(),
            MessageValue::Rows(_) => unimplemented!(),
            MessageValue::Ascii(_) => unimplemented!(),
            MessageValue::Double(_) => unimplemented!(),
            MessageValue::Set(_) => unimplemented!(),
            MessageValue::Map(_) => unimplemented!(),
            MessageValue::Varint(_) => unimplemented!(),
            MessageValue::Decimal(_) => unimplemented!(),
            MessageValue::Date(_) => unimplemented!(),
            MessageValue::Timestamp(_) => unimplemented!(),
            MessageValue::Timeuuid(_) => unimplemented!(),
            MessageValue::Varchar(_) => unimplemented!(),
            MessageValue::Uuid(_) => unimplemented!(),
            MessageValue::Time(_) => unimplemented!(),
            MessageValue::Counter(_) => unimplemented!(),
            MessageValue::Tuple(_) => unimplemented!(),
            MessageValue::Udt(_) => unimplemented!(),
        }
    }
}

impl From<MessageValue> for cassandra_protocol::types::value::Bytes {
    fn from(value: MessageValue) -> cassandra_protocol::types::value::Bytes {
        match value {
            MessageValue::NULL => (-1).into(),
            MessageValue::None => cassandra_protocol::types::value::Bytes::new(vec![]),
            MessageValue::Bytes(b) => cassandra_protocol::types::value::Bytes::new(b.to_vec()),
            MessageValue::Strings(s) => s.into(),
            MessageValue::Integer(i, _) => i.into(),
            MessageValue::Float(f) => f.into_inner().into(),
            MessageValue::Boolean(b) => b.into(),
            MessageValue::List(l) => cassandra_protocol::types::value::Bytes::from(l),
            MessageValue::Rows(r) => cassandra_protocol::types::value::Bytes::from(r),
            MessageValue::NamedRows(n) => cassandra_protocol::types::value::Bytes::from(n),
            MessageValue::Document(d) => cassandra_protocol::types::value::Bytes::from(d),
            MessageValue::Inet(i) => i.into(),
            MessageValue::FragmentedResponse(l) => cassandra_protocol::types::value::Bytes::from(l),
            MessageValue::Ascii(a) => a.into(),
            MessageValue::Double(d) => d.into_inner().into(),
            MessageValue::Set(s) => s.into_iter().collect_vec().into(),
            MessageValue::Map(m) => m.into(),
            MessageValue::Varint(v) => v.into(),
            MessageValue::Decimal(d) => {
                let (unscaled, scale) = d.into_bigint_and_exponent();
                cassandra_protocol::types::decimal::Decimal {
                    unscaled,
                    scale: scale as i32,
                }
                .into()
            }
            MessageValue::Date(d) => d.into(),
            MessageValue::Timestamp(t) => t.into(),
            MessageValue::Timeuuid(t) => t.into(),
            MessageValue::Varchar(v) => v.into(),
            MessageValue::Uuid(u) => u.into(),
            MessageValue::Time(t) => t.into(),
            MessageValue::Counter(c) => c.into(),
            MessageValue::Tuple(t) => t.into(),
            MessageValue::Udt(u) => u.into(),
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
