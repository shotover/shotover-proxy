use bytes::BufMut;
#[cfg(feature = "cassandra-cpp-driver-tests")]
use cassandra_cpp::{LendingIterator, Value, ValueType};
use cassandra_protocol::types::cassandra_type::CassandraType;
use cdrs_tokio::frame::{Serialize, Version};
use ordered_float::OrderedFloat;
use scylla::frame::response::result::CqlValue;
use std::net::IpAddr;

#[derive(Debug, Clone, PartialOrd, Eq, Ord)]
pub enum ResultValue {
    Varchar(String),
    Int(i32),
    Boolean(bool),
    Uuid(uuid::Uuid),
    Ascii(String),
    BigInt(i64),
    Blob(Vec<u8>),
    Decimal(Vec<u8>),
    Double(OrderedFloat<f64>),
    Duration(Vec<u8>), // TODO should be i32
    Float(OrderedFloat<f32>),
    Inet(IpAddr),
    SmallInt(i16),
    Time(i64),
    Timestamp(i64),
    TimeUuid(uuid::Uuid),
    Counter(i64),
    TinyInt(i8),
    VarInt(Vec<u8>),
    Date(u32),
    Set(Vec<ResultValue>),
    List(Vec<ResultValue>),
    Tuple(Vec<ResultValue>),
    Vector(Vec<ResultValue>),
    Map(Vec<(ResultValue, ResultValue)>),
    Null,
    /// Never output by the DB
    /// Can be used by the user in assertions to allow any value.
    Any,
}

impl PartialEq for ResultValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Varchar(l0), Self::Varchar(r0)) => l0 == r0,
            (Self::Int(l0), Self::Int(r0)) => l0 == r0,
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Uuid(l0), Self::Uuid(r0)) => l0 == r0,
            (Self::Ascii(l0), Self::Ascii(r0)) => l0 == r0,
            (Self::BigInt(l0), Self::BigInt(r0)) => l0 == r0,
            (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
            (Self::Decimal(l0), Self::Decimal(r0)) => l0 == r0,
            (Self::Double(l0), Self::Double(r0)) => l0 == r0,
            (Self::Duration(l0), Self::Duration(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Inet(l0), Self::Inet(r0)) => l0 == r0,
            (Self::SmallInt(l0), Self::SmallInt(r0)) => l0 == r0,
            (Self::Time(l0), Self::Time(r0)) => l0 == r0,
            (Self::Timestamp(l0), Self::Timestamp(r0)) => l0 == r0,
            (Self::TimeUuid(l0), Self::TimeUuid(r0)) => l0 == r0,
            (Self::Counter(l0), Self::Counter(r0)) => l0 == r0,
            (Self::TinyInt(l0), Self::TinyInt(r0)) => l0 == r0,
            (Self::VarInt(l0), Self::VarInt(r0)) => l0 == r0,
            (Self::Date(l0), Self::Date(r0)) => l0 == r0,
            (Self::Set(l0), Self::Set(r0)) => l0 == r0,
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (Self::Tuple(l0), Self::Tuple(r0)) => l0 == r0,
            (Self::Map(l0), Self::Map(r0)) => l0 == r0,
            (Self::Vector(l0), Self::Vector(r0)) => l0 == r0,
            (Self::Null, Self::Null) => true,
            (Self::Any, _) => true,
            (_, Self::Any) => true,
            _ => false,
        }
    }
}

impl ResultValue {
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    pub fn new_from_cpp(value: Value) -> Self {
        if value.is_null() {
            ResultValue::Null
        } else {
            match value.get_type() {
                ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
                ValueType::INT => ResultValue::Int(value.get_i32().unwrap()),
                ValueType::BOOLEAN => ResultValue::Boolean(value.get_bool().unwrap()),
                ValueType::UUID => ResultValue::Uuid(value.get_uuid().unwrap().into()),
                ValueType::ASCII => ResultValue::Ascii(value.get_string().unwrap()),
                ValueType::BIGINT => ResultValue::BigInt(value.get_i64().unwrap()),
                ValueType::BLOB => ResultValue::Blob(value.get_bytes().unwrap().to_vec()),
                ValueType::DATE => ResultValue::Date(value.get_u32().unwrap()),
                ValueType::DECIMAL => ResultValue::Decimal(value.get_bytes().unwrap().to_vec()),
                ValueType::DOUBLE => ResultValue::Double(value.get_f64().unwrap().into()),
                ValueType::DURATION => ResultValue::Duration(value.get_bytes().unwrap().to_vec()),
                ValueType::FLOAT => ResultValue::Float(value.get_f32().unwrap().into()),
                ValueType::INET => ResultValue::Inet(value.get_inet().as_ref().unwrap().into()),
                ValueType::SMALL_INT => ResultValue::SmallInt(value.get_i16().unwrap()),
                ValueType::TIME => ResultValue::Time(value.get_i64().unwrap()),
                ValueType::TIMESTAMP => ResultValue::Timestamp(value.get_i64().unwrap()),
                ValueType::TIMEUUID => ResultValue::TimeUuid(value.get_uuid().unwrap().into()),
                ValueType::COUNTER => ResultValue::Counter(value.get_i64().unwrap()),
                ValueType::VARINT => ResultValue::VarInt(value.get_bytes().unwrap().to_vec()),
                ValueType::TINY_INT => ResultValue::TinyInt(value.get_i8().unwrap()),
                ValueType::SET => ResultValue::Set(Self::new_from_cpp_set_list_or_tuple(value)),
                ValueType::LIST => ResultValue::List(Self::new_from_cpp_set_list_or_tuple(value)),
                ValueType::TUPLE => ResultValue::Tuple(Self::new_from_cpp_set_list_or_tuple(value)),
                ValueType::MAP => {
                    let mut result = vec![];
                    let mut iter = value.get_map().unwrap();
                    while let Some((k, v)) = iter.next() {
                        result.push((ResultValue::new_from_cpp(k), ResultValue::new_from_cpp(v)));
                    }
                    ResultValue::Map(result)
                }
                ValueType::UNKNOWN => todo!(),
                ValueType::CUSTOM => todo!(),
                ValueType::UDT => todo!(),
                ValueType::TEXT => unimplemented!(
                    "text is represented by the same id as varchar at the protocol level and therefore will never be instantiated by the datastax cpp driver. https://github.com/apache/cassandra/blob/703ccdee29f7e8c39aeb976e72e516415d609cf4/doc/native_protocol_v5.spec#L1184"
                ),
            }
        }
    }

    /// value must be a SET, LIST or TUPLE
    #[cfg(feature = "cassandra-cpp-driver-tests")]
    fn new_from_cpp_set_list_or_tuple(value: Value<'_>) -> Vec<Self> {
        let mut result = vec![];
        // despite the name set_to_vec is used by SET, LIST and TUPLE
        let mut iter = value.get_set().unwrap();
        while let Some(value) = iter.next() {
            result.push(ResultValue::new_from_cpp(value));
        }
        result
    }

    pub fn new_from_cdrs(value: CassandraType, version: Version) -> Self {
        match value {
            CassandraType::Ascii(ascii) => ResultValue::Ascii(ascii),
            CassandraType::Bigint(big_int) => ResultValue::BigInt(big_int),
            CassandraType::Blob(blob) => ResultValue::Blob(blob.into_vec()),
            CassandraType::Boolean(b) => ResultValue::Boolean(b),
            CassandraType::Counter(counter) => ResultValue::Counter(counter),
            CassandraType::Decimal(decimal) => {
                ResultValue::Decimal(decimal.serialize_to_vec(version))
            }
            CassandraType::Double(double) => ResultValue::Double(double.into()),
            CassandraType::Float(float) => ResultValue::Float(float.into()),
            CassandraType::Int(int) => ResultValue::Int(int),
            CassandraType::Timestamp(timestamp) => ResultValue::Timestamp(timestamp),
            CassandraType::Uuid(uuid) => ResultValue::Uuid(uuid),
            CassandraType::Varchar(varchar) => ResultValue::Varchar(varchar),
            CassandraType::Varint(var_int) => ResultValue::VarInt(var_int.to_signed_bytes_be()),
            CassandraType::Timeuuid(uuid) => ResultValue::TimeUuid(uuid),
            CassandraType::Inet(ip_addr) => ResultValue::Inet(ip_addr),
            CassandraType::Date(date) => ResultValue::Date(date as u32),
            CassandraType::Time(time) => ResultValue::Time(time),
            CassandraType::Smallint(small_int) => ResultValue::SmallInt(small_int),
            CassandraType::Tinyint(tiny_int) => ResultValue::TinyInt(tiny_int),
            CassandraType::Duration(duration) => {
                ResultValue::Duration(duration.serialize_to_vec(version))
            }
            CassandraType::List(list) => ResultValue::List(
                list.into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Map(map) => ResultValue::Map(
                map.into_iter()
                    .map(|(k, v)| {
                        (
                            ResultValue::new_from_cdrs(k, version),
                            ResultValue::new_from_cdrs(v, version),
                        )
                    })
                    .collect(),
            ),
            CassandraType::Set(set) => ResultValue::Set(
                set.into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Udt(_) => todo!(),
            CassandraType::Tuple(tuple) => ResultValue::Tuple(
                tuple
                    .into_iter()
                    .map(|element| ResultValue::new_from_cdrs(element, version))
                    .collect(),
            ),
            CassandraType::Null => ResultValue::Null,
            _ => unreachable!(),
        }
    }

    pub fn new_from_scylla(value: Option<CqlValue>) -> Self {
        match value {
            Some(value) => match value {
                CqlValue::Ascii(ascii) => Self::Ascii(ascii),
                CqlValue::BigInt(big_int) => Self::BigInt(big_int),
                CqlValue::Blob(blob) => Self::Blob(blob),
                CqlValue::Boolean(b) => Self::Boolean(b),
                CqlValue::Counter(_counter) => todo!(),
                CqlValue::Decimal(d) => {
                    let (value, scale) = d.as_signed_be_bytes_slice_and_exponent();
                    let mut buf = vec![];
                    buf.put_i32(scale);
                    buf.extend_from_slice(value);
                    Self::Decimal(buf)
                }
                CqlValue::Float(float) => Self::Float(float.into()),
                CqlValue::Int(int) => Self::Int(int),
                CqlValue::Timestamp(timestamp) => Self::Timestamp(timestamp.0),
                CqlValue::Uuid(uuid) => Self::Uuid(uuid),
                CqlValue::Varint(var_int) => Self::VarInt(var_int.into_signed_bytes_be()),
                CqlValue::Timeuuid(timeuuid) => Self::TimeUuid(timeuuid.into()),
                CqlValue::Inet(ip) => Self::Inet(ip),
                CqlValue::Date(date) => Self::Date(date.0),
                CqlValue::Time(time) => Self::Time(time.0),
                CqlValue::SmallInt(small_int) => Self::SmallInt(small_int),
                CqlValue::TinyInt(tiny_int) => Self::TinyInt(tiny_int),
                CqlValue::Duration(_duration) => todo!(),
                CqlValue::Double(double) => Self::Double(double.into()),
                CqlValue::Text(text) => Self::Varchar(text),
                CqlValue::Empty => Self::Null,
                CqlValue::List(list) => Self::List(
                    list.into_iter()
                        .map(|v| Self::new_from_scylla(Some(v)))
                        .collect(),
                ),
                CqlValue::Set(set) => Self::Set(
                    set.into_iter()
                        .map(|v| Self::new_from_scylla(Some(v)))
                        .collect(),
                ),
                CqlValue::Map(map) => Self::Map(
                    map.into_iter()
                        .map(|(k, v)| {
                            (
                                Self::new_from_scylla(Some(k)),
                                Self::new_from_scylla(Some(v)),
                            )
                        })
                        .collect(),
                ),
                CqlValue::Tuple(tuple) => {
                    Self::Tuple(tuple.into_iter().map(Self::new_from_scylla).collect())
                }
                CqlValue::UserDefinedType { .. } => todo!(),
            },
            None => Self::Null,
        }
    }
}
