use crate::codec::redis::redis_query_type;
use crate::frame::{
    cassandra,
    cassandra::{to_cassandra_type, CassandraMetadata, CassandraOperation},
};
use crate::frame::{CassandraFrame, Frame, MessageType, RedisFrame};
use anyhow::{anyhow, Result};
use bigdecimal::BigDecimal;
use bytes::{Buf, Bytes};
use bytes_utils::Str;
use cassandra_protocol::frame::Serialize as FrameSerialize;
use cassandra_protocol::types::CInt;
use cassandra_protocol::{
    frame::{
        message_error::{AdditionalErrorInfo, ErrorBody},
        message_result::{ColSpec, ColTypeOption},
        Version,
    },
    types::{
        cassandra_type::{wrapper_fn, CassandraType},
        CBytes,
    },
};
use cql3_parser::common::Operand;
use nonzero_ext::nonzero;
use num::BigInt;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
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
/// However if a transform wants to query the contents of the message it must call `Message::frame()` which will cause the raw bytes to be processed into a raw bytes + Frame.
/// The first call to frame has an expensive one time cost.
///
/// The transform may also go one step further and modify the message's Frame + call `Message::invalidate_cache()`.
/// This results in an expensive cost to reassemble the message bytes when the message is sent to the destination.
#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    /// It is an invariant that this field must remain Some at all times.
    /// The only reason it is an Option is to allow temporarily taking ownership of the value from an &mut T
    inner: Option<MessageInner>,

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
            meta_timestamp: None,
        }
    }

    /// This method should be called when you have both a Frame and matching raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol that does not include length of the message in the header. e.g. redis
    /// Providing both the raw bytes and Frame results in better performance if they are both already available.
    pub fn from_bytes_and_frame(bytes: Bytes, frame: Frame) -> Self {
        Message {
            inner: Some(MessageInner::Parsed { bytes, frame }),
            meta_timestamp: None,
        }
    }

    /// This method should be called when you have just a Frame of a message.
    /// This is expected to be used by transforms that are generating custom messages.
    /// Providing just the Frame results in better performance when only the Frame is available.
    pub fn from_frame(frame: Frame) -> Self {
        Message {
            inner: Some(MessageInner::Modified { frame }),
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

    /// Only use for messages read straight from the socket
    /// that are definitely in an unparsed state
    /// (haven't passed through any transforms where they might have been parsed or modified)
    pub(crate) fn as_raw_bytes(&self) -> Option<&Bytes> {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes { bytes, .. } => Some(bytes),
            _ => None,
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
                MessageType::Cassandra => cassandra::raw_frame::cell_count(bytes)?,
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
        *self = Message::from_frame(match self.metadata().unwrap() {
            Metadata::Redis => {
                Frame::Redis(RedisFrame::Error(Str::from_inner(error.into()).unwrap()))
            }
            Metadata::Cassandra(frame) => Frame::Cassandra(CassandraFrame {
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
            Metadata::None => Frame::None,
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
                MessageType::Cassandra => {
                    Ok(Metadata::Cassandra(cassandra::raw_frame::metadata(bytes)?))
                }
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
    /// The raw bytes the protocol should send
    Bytes(Bytes),
    /// The Frame that should be processed into bytes and then sent
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
    Null,
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
    Set(BTreeSet<MessageValue>),
    Map(BTreeMap<MessageValue, MessageValue>),
    Varint(BigInt),
    Decimal(BigDecimal),
    Date(i32),
    Timestamp(i64),
    Duration(Duration),
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

// TODO: This is tailored directly to cassandras Duration and will need to be adjusted once we add another protocol that uses it
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
pub struct Duration {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

impl From<&Operand> for MessageValue {
    fn from(operand: &Operand) -> Self {
        MessageValue::create_element(to_cassandra_type(operand))
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
            RedisFrame::Null => MessageValue::Null,
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
            RedisFrame::Null => MessageValue::Null,
        }
    }
}

impl From<MessageValue> for RedisFrame {
    fn from(value: MessageValue) -> RedisFrame {
        match value {
            MessageValue::Null => RedisFrame::Null,
            MessageValue::Bytes(b) => RedisFrame::BulkString(b),
            MessageValue::Strings(s) => RedisFrame::SimpleString(s.into()),
            MessageValue::Integer(i, _) => RedisFrame::Integer(i),
            MessageValue::Float(f) => RedisFrame::SimpleString(f.to_string().into()),
            MessageValue::Boolean(b) => RedisFrame::Integer(i64::from(b)),
            MessageValue::Inet(i) => RedisFrame::SimpleString(i.to_string().into()),
            MessageValue::List(l) => RedisFrame::Array(l.into_iter().map(|v| v.into()).collect()),
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
            MessageValue::Duration(_) => todo!(),
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

    pub fn build_value_from_cstar_col_type(
        version: Version,
        spec: &ColSpec,
        data: &CBytes,
    ) -> MessageValue {
        let cassandra_type = MessageValue::into_cassandra_type(version, &spec.col_type, data);
        MessageValue::create_element(cassandra_type)
    }

    fn into_cassandra_type(
        version: Version,
        col_type: &ColTypeOption,
        data: &CBytes,
    ) -> CassandraType {
        let wrapper = wrapper_fn(&col_type.id);
        wrapper(data, col_type, version).unwrap()
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
            CassandraType::Duration(d) => MessageValue::Duration(Duration {
                months: d.months(),
                days: d.days(),
                nanoseconds: d.nanoseconds(),
            }),
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
            CassandraType::Null => MessageValue::Null,
        }
    }

    pub fn cassandra_serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            MessageValue::Null => cursor.write_all(&[255, 255, 255, 255]).unwrap(),
            MessageValue::Bytes(b) => serialize_bytes(cursor, b),
            MessageValue::Strings(s) => serialize_bytes(cursor, s.as_bytes()),
            MessageValue::Integer(x, size) => match size {
                IntSize::I64 => serialize_bytes(cursor, &(*x as i64).to_be_bytes()),
                IntSize::I32 => serialize_bytes(cursor, &(*x as i32).to_be_bytes()),
                IntSize::I16 => serialize_bytes(cursor, &(*x as i16).to_be_bytes()),
                IntSize::I8 => serialize_bytes(cursor, &(*x as i8).to_be_bytes()),
            },
            MessageValue::Float(f) => serialize_bytes(cursor, &f.into_inner().to_be_bytes()),
            MessageValue::Boolean(b) => serialize_bytes(cursor, &[if *b { 1 } else { 0 }]),
            MessageValue::List(l) => serialize_list(cursor, l),
            MessageValue::Inet(i) => match i {
                IpAddr::V4(ip) => serialize_bytes(cursor, &ip.octets()),
                IpAddr::V6(ip) => serialize_bytes(cursor, &ip.octets()),
            },
            MessageValue::Ascii(a) => serialize_bytes(cursor, a.as_bytes()),
            MessageValue::Double(d) => serialize_bytes(cursor, &d.into_inner().to_be_bytes()),
            MessageValue::Set(s) => serialize_set(cursor, s),
            MessageValue::Map(m) => serialize_map(cursor, m),
            MessageValue::Varint(v) => serialize_bytes(cursor, &v.to_signed_bytes_be()),
            MessageValue::Decimal(d) => {
                let (unscaled, scale) = d.as_bigint_and_exponent();
                serialize_bytes(
                    cursor,
                    &cassandra_protocol::types::decimal::Decimal {
                        unscaled,
                        scale: scale as i32,
                    }
                    .serialize_to_vec(Version::V4),
                );
            }
            MessageValue::Date(d) => serialize_bytes(cursor, &d.to_be_bytes()),
            MessageValue::Timestamp(t) => serialize_bytes(cursor, &t.to_be_bytes()),
            MessageValue::Duration(d) => {
                // TODO: Either this function should be made fallible or Duration should have validated setters
                serialize_bytes(
                    cursor,
                    &cassandra_protocol::types::duration::Duration::new(
                        d.months,
                        d.days,
                        d.nanoseconds,
                    )
                    .unwrap()
                    .serialize_to_vec(Version::V4),
                );
            }
            MessageValue::Timeuuid(t) => serialize_bytes(cursor, t.as_bytes()),
            MessageValue::Varchar(v) => serialize_bytes(cursor, v.as_bytes()),
            MessageValue::Uuid(u) => serialize_bytes(cursor, u.as_bytes()),
            MessageValue::Time(t) => serialize_bytes(cursor, &t.to_be_bytes()),
            MessageValue::Counter(c) => serialize_bytes(cursor, &c.to_be_bytes()),
            MessageValue::Tuple(t) => serialize_list(cursor, t),
            MessageValue::Udt(u) => serialize_stringmap(cursor, u),
        }
    }
}

fn serialize_with_length_prefix(
    cursor: &mut Cursor<&mut Vec<u8>>,
    serializer: impl FnOnce(&mut Cursor<&mut Vec<u8>>),
) {
    // write dummy length
    let length_start = cursor.position();
    let bytes_start = length_start + 4;
    serialize_len(cursor, 0);

    // perform serialization
    serializer(cursor);

    // overwrite dummy length with actual length of serialized bytes
    let bytes_len = cursor.position() - bytes_start;
    cursor.get_mut()[length_start as usize..bytes_start as usize]
        .copy_from_slice(&(bytes_len as CInt).to_be_bytes());
}

pub fn serialize_len(cursor: &mut Cursor<&mut Vec<u8>>, len: usize) {
    let len = len as CInt;
    cursor.write_all(&len.to_be_bytes()).unwrap();
}

fn serialize_bytes(cursor: &mut Cursor<&mut Vec<u8>>, bytes: &[u8]) {
    serialize_len(cursor, bytes.len());
    cursor.write_all(bytes).unwrap();
}

fn serialize_list(cursor: &mut Cursor<&mut Vec<u8>>, values: &[MessageValue]) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for value in values {
            value.cassandra_serialize(cursor);
        }
    });
}

#[allow(clippy::mutable_key_type)]
fn serialize_set(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeSet<MessageValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for value in values {
            value.cassandra_serialize(cursor);
        }
    });
}

fn serialize_stringmap(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeMap<String, MessageValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for (key, value) in values.iter() {
            serialize_bytes(cursor, key.as_bytes());
            value.cassandra_serialize(cursor);
        }
    });
}

#[allow(clippy::mutable_key_type)]
fn serialize_map(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeMap<MessageValue, MessageValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for (key, value) in values.iter() {
            key.cassandra_serialize(cursor);
            value.cassandra_serialize(cursor);
        }
    });
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
