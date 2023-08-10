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
pub enum GenericValue {
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
    List(Vec<GenericValue>),
    Set(BTreeSet<GenericValue>),
    Map(BTreeMap<GenericValue, GenericValue>),
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
    Tuple(Vec<GenericValue>),
    Udt(BTreeMap<String, GenericValue>),
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

impl From<&Operand> for GenericValue {
    fn from(operand: &Operand) -> Self {
        GenericValue::create_element(to_cassandra_type(operand))
    }
}

impl From<RedisFrame> for GenericValue {
    fn from(f: RedisFrame) -> Self {
        match f {
            RedisFrame::SimpleString(s) => {
                GenericValue::Strings(String::from_utf8_lossy(&s).to_string())
            }
            RedisFrame::Error(e) => GenericValue::Strings(e.to_string()),
            RedisFrame::Integer(i) => GenericValue::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => GenericValue::Bytes(b),
            RedisFrame::Array(a) => {
                GenericValue::List(a.iter().cloned().map(GenericValue::from).collect())
            }
            RedisFrame::Null => GenericValue::Null,
        }
    }
}

impl From<&RedisFrame> for GenericValue {
    fn from(f: &RedisFrame) -> Self {
        match f.clone() {
            RedisFrame::SimpleString(s) => {
                GenericValue::Strings(String::from_utf8_lossy(s.as_ref()).to_string())
            }
            RedisFrame::Error(e) => GenericValue::Strings(e.to_string()),
            RedisFrame::Integer(i) => GenericValue::Integer(i, IntSize::I64),
            RedisFrame::BulkString(b) => GenericValue::Bytes(b),
            RedisFrame::Array(a) => {
                GenericValue::List(a.iter().cloned().map(GenericValue::from).collect())
            }
            RedisFrame::Null => GenericValue::Null,
        }
    }
}

impl From<GenericValue> for RedisFrame {
    fn from(value: GenericValue) -> RedisFrame {
        match value {
            GenericValue::Null => RedisFrame::Null,
            GenericValue::Bytes(b) => RedisFrame::BulkString(b),
            GenericValue::Strings(s) => RedisFrame::SimpleString(s.into()),
            GenericValue::Integer(i, _) => RedisFrame::Integer(i),
            GenericValue::Float(f) => RedisFrame::SimpleString(f.to_string().into()),
            GenericValue::Boolean(b) => RedisFrame::Integer(i64::from(b)),
            GenericValue::Inet(i) => RedisFrame::SimpleString(i.to_string().into()),
            GenericValue::List(l) => RedisFrame::Array(l.into_iter().map(|v| v.into()).collect()),
            GenericValue::Ascii(_a) => todo!(),
            GenericValue::Double(_d) => todo!(),
            GenericValue::Set(_s) => todo!(),
            GenericValue::Map(_) => todo!(),
            GenericValue::Varint(_v) => todo!(),
            GenericValue::Decimal(_d) => todo!(),
            GenericValue::Date(_date) => todo!(),
            GenericValue::Timestamp(_timestamp) => todo!(),
            GenericValue::Timeuuid(_timeuuid) => todo!(),
            GenericValue::Varchar(_v) => todo!(),
            GenericValue::Uuid(_uuid) => todo!(),
            GenericValue::Time(_t) => todo!(),
            GenericValue::Counter(_c) => todo!(),
            GenericValue::Tuple(_) => todo!(),
            GenericValue::Udt(_) => todo!(),
            GenericValue::Duration(_) => todo!(),
        }
    }
}

impl GenericValue {
    pub fn value_byte_string(string: String) -> GenericValue {
        GenericValue::Bytes(Bytes::from(string))
    }

    pub fn value_byte_str(str: &'static str) -> GenericValue {
        GenericValue::Bytes(Bytes::from(str))
    }

    pub fn build_value_from_cstar_col_type(
        version: Version,
        spec: &ColSpec,
        data: &CBytes,
    ) -> GenericValue {
        let cassandra_type = GenericValue::into_cassandra_type(version, &spec.col_type, data);
        GenericValue::create_element(cassandra_type)
    }

    fn into_cassandra_type(
        version: Version,
        col_type: &ColTypeOption,
        data: &CBytes,
    ) -> CassandraType {
        let wrapper = wrapper_fn(&col_type.id);
        wrapper(data, col_type, version).unwrap()
    }

    fn create_element(element: CassandraType) -> GenericValue {
        match element {
            CassandraType::Ascii(a) => GenericValue::Ascii(a),
            CassandraType::Bigint(b) => GenericValue::Integer(b, IntSize::I64),
            CassandraType::Blob(b) => GenericValue::Bytes(b.into_vec().into()),
            CassandraType::Boolean(b) => GenericValue::Boolean(b),
            CassandraType::Counter(c) => GenericValue::Counter(c),
            CassandraType::Decimal(d) => {
                let big_decimal = BigDecimal::new(d.unscaled, d.scale.into());
                GenericValue::Decimal(big_decimal)
            }
            CassandraType::Double(d) => GenericValue::Double(d.into()),
            CassandraType::Float(f) => GenericValue::Float(f.into()),
            CassandraType::Int(c) => GenericValue::Integer(c.into(), IntSize::I32),
            CassandraType::Timestamp(t) => GenericValue::Timestamp(t),
            CassandraType::Uuid(u) => GenericValue::Uuid(u),
            CassandraType::Varchar(v) => GenericValue::Varchar(v),
            CassandraType::Varint(v) => GenericValue::Varint(v),
            CassandraType::Timeuuid(t) => GenericValue::Timeuuid(t),
            CassandraType::Inet(i) => GenericValue::Inet(i),
            CassandraType::Date(d) => GenericValue::Date(d),
            CassandraType::Time(d) => GenericValue::Time(d),
            CassandraType::Duration(d) => GenericValue::Duration(Duration {
                months: d.months(),
                days: d.days(),
                nanoseconds: d.nanoseconds(),
            }),
            CassandraType::Smallint(d) => GenericValue::Integer(d.into(), IntSize::I16),
            CassandraType::Tinyint(d) => GenericValue::Integer(d.into(), IntSize::I8),
            CassandraType::List(list) => {
                let value_list = list.into_iter().map(GenericValue::create_element).collect();
                GenericValue::List(value_list)
            }
            CassandraType::Map(map) => GenericValue::Map(
                map.into_iter()
                    .map(|(key, value)| {
                        (
                            GenericValue::create_element(key),
                            GenericValue::create_element(value),
                        )
                    })
                    .collect(),
            ),
            CassandraType::Set(set) => {
                GenericValue::Set(set.into_iter().map(GenericValue::create_element).collect())
            }
            CassandraType::Udt(udt) => {
                let values = udt
                    .into_iter()
                    .map(|(key, element)| (key, GenericValue::create_element(element)))
                    .collect();
                GenericValue::Udt(values)
            }
            CassandraType::Tuple(tuple) => {
                let value_list = tuple
                    .into_iter()
                    .map(GenericValue::create_element)
                    .collect();
                GenericValue::Tuple(value_list)
            }
            CassandraType::Null => GenericValue::Null,
            _ => unreachable!(),
        }
    }

    pub fn cassandra_serialize(&self, cursor: &mut Cursor<&mut Vec<u8>>) {
        match self {
            GenericValue::Null => cursor.write_all(&[255, 255, 255, 255]).unwrap(),
            GenericValue::Bytes(b) => serialize_bytes(cursor, b),
            GenericValue::Strings(s) => serialize_bytes(cursor, s.as_bytes()),
            GenericValue::Integer(x, size) => match size {
                IntSize::I64 => serialize_bytes(cursor, &(*x).to_be_bytes()),
                IntSize::I32 => serialize_bytes(cursor, &(*x as i32).to_be_bytes()),
                IntSize::I16 => serialize_bytes(cursor, &(*x as i16).to_be_bytes()),
                IntSize::I8 => serialize_bytes(cursor, &(*x as i8).to_be_bytes()),
            },
            GenericValue::Float(f) => serialize_bytes(cursor, &f.into_inner().to_be_bytes()),
            GenericValue::Boolean(b) => serialize_bytes(cursor, &[*b as u8]),
            GenericValue::List(l) => serialize_list(cursor, l),
            GenericValue::Inet(i) => match i {
                IpAddr::V4(ip) => serialize_bytes(cursor, &ip.octets()),
                IpAddr::V6(ip) => serialize_bytes(cursor, &ip.octets()),
            },
            GenericValue::Ascii(a) => serialize_bytes(cursor, a.as_bytes()),
            GenericValue::Double(d) => serialize_bytes(cursor, &d.into_inner().to_be_bytes()),
            GenericValue::Set(s) => serialize_set(cursor, s),
            GenericValue::Map(m) => serialize_map(cursor, m),
            GenericValue::Varint(v) => serialize_bytes(cursor, &v.to_signed_bytes_be()),
            GenericValue::Decimal(d) => {
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
            GenericValue::Date(d) => serialize_bytes(cursor, &d.to_be_bytes()),
            GenericValue::Timestamp(t) => serialize_bytes(cursor, &t.to_be_bytes()),
            GenericValue::Duration(d) => {
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
            GenericValue::Timeuuid(t) => serialize_bytes(cursor, t.as_bytes()),
            GenericValue::Varchar(v) => serialize_bytes(cursor, v.as_bytes()),
            GenericValue::Uuid(u) => serialize_bytes(cursor, u.as_bytes()),
            GenericValue::Time(t) => serialize_bytes(cursor, &t.to_be_bytes()),
            GenericValue::Counter(c) => serialize_bytes(cursor, &c.to_be_bytes()),
            GenericValue::Tuple(t) => serialize_list(cursor, t),
            GenericValue::Udt(u) => serialize_stringmap(cursor, u),
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

fn serialize_list(cursor: &mut Cursor<&mut Vec<u8>>, values: &[GenericValue]) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for value in values {
            value.cassandra_serialize(cursor);
        }
    });
}

#[allow(clippy::mutable_key_type)]
fn serialize_set(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeSet<GenericValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for value in values {
            value.cassandra_serialize(cursor);
        }
    });
}

fn serialize_stringmap(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeMap<String, GenericValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for (key, value) in values.iter() {
            serialize_bytes(cursor, key.as_bytes());
            value.cassandra_serialize(cursor);
        }
    });
}

#[allow(clippy::mutable_key_type)]
fn serialize_map(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeMap<GenericValue, GenericValue>) {
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
