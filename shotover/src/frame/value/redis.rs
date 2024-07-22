use super::{GenericValue, IntSize};
use crate::frame::RedisFrame;

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
            GenericValue::Custom(_) => todo!(),
        }
    }
}
