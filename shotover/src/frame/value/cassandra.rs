use super::{Duration, GenericValue, IntSize};
use crate::frame::cassandra::to_cassandra_type;
use bigdecimal::BigDecimal;
use bytes::Bytes;
use cassandra_protocol::frame::Serialize as FrameSerialize;
use cassandra_protocol::frame::message_result::ColType;
use cassandra_protocol::types::CInt;
use cassandra_protocol::{
    frame::{
        Version,
        message_result::{ColSpec, ColTypeOption},
    },
    types::{
        CBytes,
        cassandra_type::{CassandraType, wrapper_fn},
    },
};
use cql3_parser::common::Operand;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};
use std::net::IpAddr;

impl From<&Operand> for GenericValue {
    fn from(operand: &Operand) -> Self {
        GenericValue::create_element(to_cassandra_type(operand))
    }
}

/// This type is a hack and should eventually be removed.
/// To properly resolve this we really need to:
/// * remove GenericValue from the cassandra AST
///      + its a holdover from a long time ago
///          - shotover no longer attempts to provide a generic abstraction over multiple DB types
/// * Replace cassandra-protocol with something simpler and faster.
///
/// Once that is done we can just include `Custom` directly as a variant within our own CassandraType enum.
enum CustomOrStandardType {
    Custom(Option<Vec<u8>>),
    Standard(CassandraType),
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
        match cassandra_type {
            CustomOrStandardType::Custom(Some(bytes)) => GenericValue::Custom(bytes.into()),
            CustomOrStandardType::Custom(None) => GenericValue::Null,
            CustomOrStandardType::Standard(element) => Self::create_element(element),
        }
    }

    fn into_cassandra_type(
        version: Version,
        col_type: &ColTypeOption,
        data: &CBytes,
    ) -> CustomOrStandardType {
        // cassandra-protocol will error on an unknown custom type,
        // but we need to continue succesfully with custom types even if that means treating them as a magical bag of bytes.
        // so we check for custom type before running the cassandra-protocol parser.
        if col_type.id == ColType::Custom {
            CustomOrStandardType::Custom(data.clone().into_bytes())
        } else {
            let wrapper = wrapper_fn(&col_type.id);
            CustomOrStandardType::Standard(wrapper(data, col_type, version).unwrap())
        }
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
            GenericValue::Custom(b) => serialize_bytes(cursor, b),
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

fn serialize_map(cursor: &mut Cursor<&mut Vec<u8>>, values: &BTreeMap<GenericValue, GenericValue>) {
    serialize_with_length_prefix(cursor, |cursor| {
        serialize_len(cursor, values.len());

        for (key, value) in values.iter() {
            key.cassandra_serialize(cursor);
            value.cassandra_serialize(cursor);
        }
    });
}
