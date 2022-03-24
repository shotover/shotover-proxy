use crate::codec::redis::redis_query_type;
use crate::frame::{
    cassandra,
    cassandra::{CassandraMetadata, CassandraOperation},
};
use crate::frame::{CassandraFrame, Frame, MessageType, RedisFrame};
use anyhow::{anyhow, Result};
use bigdecimal::BigDecimal;
use byteorder::{BigEndian, WriteBytesExt};
use bytes::{Buf, Bytes};
use bytes_utils::Str;
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
use nonzero_ext::nonzero;
use num::BigInt;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Value as SQLValue;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::num::NonZeroU32;
use uuid::Uuid;

enum Metadata {
    Cassandra(CassandraMetadata),
    Redis,
    None,
}

pub type Messages = Vec<Message>;

/// The Message type is designed to effeciently abstract over the message being in various states of processing.
///
/// Usually a message is received and starts off containing just raw bytes (or possibly raw bytes + frame)
/// This can be immediately sent off to the destination without any processing cost.
///
/// However if a transform wants to query the contents of the message it must call `Message::frame()q which will cause the raw bytes to be processed into a raw bytes + Frame.
/// The first call to frame has an expensive one time cost.
///
/// The transform may also go one step further and modify the message's Frame + call `Message::invalidate_cache()`.
/// This results in an expensive cost to reassemble the message bytes when the message is sent to the destination.
#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    /// It is an invariant that this field must remain Some at all times.
    /// The only reason it is an Option is to allow temporarily taking ownership of the value from an &mut T
    inner: Option<MessageInner>,
    pub return_to_sender: bool,

    // TODO: Not a fan of this field and we could get rid of it by making TimestampTagger an implicit part of ConsistentScatter
    // This metadata field is only used for communication between transforms and should not be touched by sinks or sources
    pub meta_timestamp: Option<i64>,
}

/// `from_*` methods for `Message`
impl Message {
    /// This method should be called when you have have just the raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol where the length of the message is provided in the header. e.g. cassandra
    /// Providing just the bytes results in better performance when only the raw bytes are available.
    pub fn from_bytes(bytes: Bytes, message_type: MessageType) -> Self {
        Message {
            inner: Some(MessageInner::RawBytes {
                bytes,
                message_type,
            }),
            return_to_sender: false,
            meta_timestamp: None,
        }
    }

    /// This method should be called when you have both a Frame and matching raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol that does not include length of the message in the header. e.g. redis
    /// Providing both the raw bytes and Frame results in better performance if they are both already available.
    pub fn from_bytes_and_frame(bytes: Bytes, frame: Frame) -> Self {
        Message {
            inner: Some(MessageInner::Parsed { bytes, frame }),
            return_to_sender: false,
            meta_timestamp: None,
        }
    }

    /// This method should be called when you have just a Frame of a message.
    /// This is expected to be used by transforms that are generating custom messages.
    /// Providing just the Frame results in better performance when only the Frame is available.
    pub fn from_frame(frame: Frame) -> Self {
        Message {
            inner: Some(MessageInner::Modified { frame }),
            return_to_sender: false,
            meta_timestamp: None,
        }
    }
}

/// Methods for interacting with `Message::inner`
impl Message {
    /// Returns a `&mut Frame` which contains the processed contents of the message.
    /// A transform may choose to modify the contents of the `&mut Frame` in order to modify the message that is sent to the DB.
    /// Any future calls to `frame()` in the same or future transforms will return the same modified `&mut Frame`.
    /// If a transform chooses to modify the `&mut Frame` then they must also call `Frame::invalidate_cache()` after the modification.
    ///
    /// Returns `None` when fails to parse the message.
    /// This failure to parse the message is internally logged as an error.
    ///
    /// ## Performance implications
    /// Calling frame for the first time on a message may be an expensive operation as the raw bytes might not yet be parsed into a Frame.
    /// Calling frame again is free as the parsed message is cached.
    pub fn frame(&mut self) -> Option<&mut Frame> {
        let (inner, result) = self.inner.take().unwrap().ensure_parsed();
        self.inner = Some(inner);
        if let Err(err) = result {
            // TODO: If we could include a stacktrace in this error it would be really helpful
            tracing::error!("Failed to parse frame {err}");
            return None;
        }

        match self.inner.as_mut().unwrap() {
            MessageInner::RawBytes { .. } => {
                unreachable!("Cannot be RawBytes because ensure_parsed was called")
            }
            MessageInner::Parsed { frame, .. } => Some(frame),
            MessageInner::Modified { frame } => Some(frame),
        }
    }

    // TODO: Considering we already have the expected message type here maybe we should perform any required conversions and return a Result<Bytes> here.
    // I've left it as is to keep the PR simpler and there could be a need for codecs to control this process that I havent investigated.
    pub fn into_encodable(self, expected_message_type: MessageType) -> Result<Encodable> {
        match self.inner.unwrap() {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => {
                if message_type == expected_message_type {
                    Ok(Encodable::Bytes(bytes))
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        message_type
                    ))
                }
            }
            MessageInner::Parsed { bytes, frame } => {
                if frame.get_type() == expected_message_type {
                    Ok(Encodable::Bytes(bytes))
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        frame.name()
                    ))
                }
            }
            MessageInner::Modified { frame } => {
                if frame.get_type() == expected_message_type {
                    Ok(Encodable::Frame(frame))
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        frame.name()
                    ))
                }
            }
        }
    }

    /// Returns None when fails to parse the message
    pub fn namespace(&mut self) -> Option<Vec<String>> {
        match self.frame()? {
            Frame::Cassandra(cassandra) => Some(cassandra.namespace()),
            Frame::Redis(_) => unimplemented!(),
            Frame::None => Some(vec![]),
        }
    }

    /// Batch messages have a cell count of 1 cell per inner message.
    /// Cell count is determined as follows:
    /// * Regular message - 1 cell
    /// * Message containing submessages e.g. a batch request - 1 cell per submessage
    /// * Message containing submessages with 0 submessages - 1 cell
    pub fn cell_count(&self) -> Result<NonZeroU32> {
        Ok(match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match message_type {
                MessageType::Redis => nonzero!(1u32),
                MessageType::None => nonzero!(1u32),
                MessageType::Cassandra => cassandra::cell_count(bytes)?,
            },
            MessageInner::Modified { frame } | MessageInner::Parsed { frame, .. } => match frame {
                Frame::Cassandra(frame) => frame.cell_count()?,
                Frame::Redis(_) => nonzero!(1u32),
                Frame::None => nonzero!(1u32),
            },
        })
    }

    /// Invalidates all internal caches.
    /// This must be called after any modifications to the return value of `Message::frame()`.
    /// Otherwise values returned by getter methods and the message sent to the DB will be outdated.
    ///
    /// ## Performance implications
    /// * Clears caches used by getter methods
    /// * If `Message::frame()` has been called the message bytes must be regenerated from the `Frame` when sent to the DB
    pub fn invalidate_cache(&mut self) {
        // TODO: clear message details cache fields if we ever add any

        self.inner = self.inner.take().map(|x| x.invalidate_cache());
    }

    // TODO: this could be optimized to avoid parsing the cassandra sql
    pub fn to_filtered_reply(&mut self) -> Message {
        Message::from_frame(match self.frame().unwrap() {
            Frame::Redis(_) => Frame::Redis(RedisFrame::Error(
                "ERR Message was filtered out by shotover".into(),
            )),
            Frame::Cassandra(frame) => Frame::Cassandra(CassandraFrame {
                version: frame.version,
                stream_id: frame.stream_id,
                operation: CassandraOperation::Error(ErrorBody {
                    error_code: 0x0,
                    message: "Message was filtered out by shotover".into(),
                    additional_info: AdditionalErrorInfo::Server,
                }),
                tracing_id: frame.tracing_id,
                warnings: vec![],
            }),
            Frame::None => Frame::None,
        })
    }

    pub fn get_query_type(&mut self) -> QueryType {
        match self.frame() {
            Some(Frame::Cassandra(cassandra)) => cassandra.get_query_type(),
            Some(Frame::Redis(redis)) => redis_query_type(redis), // free-standing function as we cant define methods on RedisFrame
            Some(Frame::None) => QueryType::ReadWrite,
            None => QueryType::ReadWrite,
        }
    }

    // TODO: replace with a to_error_reply, should be easier to reason about
    pub fn set_error(&mut self, error: String) {
        *self = Message::from_frame(match self.frame().unwrap() {
            Frame::Redis(_) => {
                Frame::Redis(RedisFrame::Error(Str::from_inner(error.into()).unwrap()))
            }
            Frame::Cassandra(frame) => Frame::Cassandra(CassandraFrame {
                version: frame.version,
                stream_id: frame.stream_id,
                operation: CassandraOperation::Error(ErrorBody {
                    error_code: 0x0,
                    message: error,
                    additional_info: AdditionalErrorInfo::Server,
                }),
                tracing_id: frame.tracing_id,
                warnings: vec![],
            }),
            Frame::None => Frame::None,
        });
        self.invalidate_cache();
    }

    /// Get metadata for this `Message`
    fn metadata(&self) -> Result<Metadata> {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match message_type {
                MessageType::Cassandra => Ok(Metadata::Cassandra(cassandra::metadata(&*bytes)?)),
                MessageType::Redis => Ok(Metadata::Redis),
                MessageType::None => Ok(Metadata::None),
            },
            MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame } => match frame {
                Frame::Cassandra(frame) => Ok(Metadata::Cassandra(frame.metadata())),
                Frame::Redis(_) => Ok(Metadata::Redis),
                Frame::None => Ok(Metadata::None),
            },
        }
    }

    /// Set this `Message` to a backpressure response
    pub fn set_backpressure(&mut self) -> Result<()> {
        let metadata = self.metadata()?;

        *self = Message::from_frame(match metadata {
            Metadata::Cassandra(metadata) => {
                let body = CassandraOperation::Error(ErrorBody {
                    error_code: 0x1001,
                    message: "".into(),
                    additional_info: AdditionalErrorInfo::Overloaded,
                });

                Frame::Cassandra(CassandraFrame {
                    version: metadata.version,
                    stream_id: metadata.stream_id,
                    tracing_id: metadata.tracing_id,
                    warnings: vec![],
                    operation: body,
                })
            }
            Metadata::Redis => {
                unimplemented!()
            }
            Metadata::None => Frame::None,
        });

        Ok(())
    }

    // Retrieves the stream_id without parsing the rest of the frame.
    // Used for ordering out of order messages without parsing their contents.
    // TODO: We will have a better idea of how to make this generic once we have multiple out of order protocols
    //       For now its just written to match cassandra's stream_id field
    pub fn stream_id(&self) -> Option<i16> {
        match &self.inner {
            Some(MessageInner::RawBytes {
                bytes,
                message_type: MessageType::Cassandra,
            }) => {
                const HEADER_LEN: usize = 9;
                if bytes.len() >= HEADER_LEN {
                    Some((&bytes[2..4]).get_i16())
                } else {
                    None
                }
            }
            Some(MessageInner::RawBytes { .. }) => None,
            Some(MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame }) => {
                match frame {
                    Frame::Cassandra(cassandra) => Some(cassandra.stream_id),
                    Frame::Redis(_) => None,
                    Frame::None => None,
                }
            }
            None => None,
        }
    }
}

/// There are 3 levels of processing the message can be in.
/// RawBytes -> Parsed -> Modified
/// Where possible transforms should avoid moving to further stages to improve performance but this is an implementation detail hidden from them
#[derive(PartialEq, Debug, Clone)]
enum MessageInner {
    RawBytes {
        bytes: Bytes,
        message_type: MessageType,
    },
    Parsed {
        bytes: Bytes,
        frame: Frame,
    },
    Modified {
        frame: Frame,
    },
}

impl MessageInner {
    fn ensure_parsed(self) -> (Self, Result<()>) {
        match self {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match Frame::from_bytes(bytes.clone(), message_type) {
                Ok(frame) => (MessageInner::Parsed { bytes, frame }, Ok(())),
                Err(err) => (
                    MessageInner::RawBytes {
                        bytes,
                        message_type,
                    },
                    Err(err),
                ),
            },
            MessageInner::Parsed { .. } => (self, Ok(())),
            MessageInner::Modified { .. } => (self, Ok(())),
        }
    }

    fn invalidate_cache(self) -> Self {
        match self {
            MessageInner::RawBytes { .. } => self,
            MessageInner::Parsed { frame, .. } => MessageInner::Modified { frame },
            MessageInner::Modified { .. } => self,
        }
    }
}

#[derive(Debug)]
pub enum Encodable {
    Bytes(Bytes),
    Frame(Frame),
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
            CassandraType::Int(c) => MessageValue::Integer(c.into(), IntSize::I32),
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
            MessageValue::NULL => (-1_i32).into(),
            MessageValue::None => cassandra_protocol::types::value::Bytes::new(vec![]),
            MessageValue::Bytes(b) => cassandra_protocol::types::value::Bytes::new(b.to_vec()),
            MessageValue::Strings(s) => s.into(),
            MessageValue::Integer(x, size) => {
                let mut temp: Vec<u8> = Vec::new();

                match size {
                    IntSize::I64 => {
                        temp.write_i64::<BigEndian>(x).unwrap();
                    }
                    IntSize::I32 => {
                        temp.write_i32::<BigEndian>(x as i32).unwrap();
                    }
                    IntSize::I16 => {
                        temp.write_i16::<BigEndian>(x as i16).unwrap();
                    }
                    IntSize::I8 => {
                        temp.write_i8(x as i8).unwrap();
                    }
                }

                cassandra_protocol::types::value::Bytes::new(temp)
            }
            MessageValue::Float(f) => f.into_inner().into(),
            MessageValue::Boolean(b) => b.into(),
            MessageValue::List(l) => l.into(),
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
