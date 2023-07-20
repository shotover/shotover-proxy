//! Generic representations of data types that appear in messages

use crate::frame::cassandra::to_cassandra_type;
use crate::frame::RedisFrame;
use bigdecimal::BigDecimal;
use bytes::Bytes;
use cassandra_protocol::frame::Serialize as FrameSerialize;
use cassandra_protocol::types::CInt;
use cassandra_protocol::{
    frame::{
        message_result::{ColSpec, ColTypeOption},
        Version,
    },
    types::{
        cassandra_type::{wrapper_fn, CassandraType},
        CBytes,
    },
};
use cql3_parser::common::Operand;
use num::BigInt;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::net::IpAddr;
use uuid::Uuid;

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
            _ => unreachable!(),
        }
    }

    pub fn cassandra_serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            MessageValue::Null => cursor.write_all(&[255, 255, 255, 255]).unwrap(),
            MessageValue::Bytes(b) => serialize_bytes(cursor, b),
            MessageValue::Strings(s) => serialize_bytes(cursor, s.as_bytes()),
            MessageValue::Integer(x, size) => match size {
                IntSize::I64 => serialize_bytes(cursor, &(*x).to_be_bytes()),
                IntSize::I32 => serialize_bytes(cursor, &(*x as i32).to_be_bytes()),
                IntSize::I16 => serialize_bytes(cursor, &(*x as i16).to_be_bytes()),
                IntSize::I8 => serialize_bytes(cursor, &(*x as i8).to_be_bytes()),
            },
            MessageValue::Float(f) => serialize_bytes(cursor, &f.into_inner().to_be_bytes()),
            MessageValue::Boolean(b) => serialize_bytes(cursor, &[*b as u8]),
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

pub(crate) fn serialize_with_length_prefix(
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

pub(crate) fn serialize_len(cursor: &mut Cursor<&mut Vec<u8>>, len: usize) {
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
