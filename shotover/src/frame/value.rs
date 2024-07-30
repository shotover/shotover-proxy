//! Generic representations of data types that appear in messages
#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "redis")]
mod redis;

use bigdecimal::BigDecimal;
use bytes::Bytes;
use num_bigint::BigInt;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use uuid::Uuid;

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialOrd, Ord)]
pub enum GenericValue {
    Null,
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
    Custom(Bytes),
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
