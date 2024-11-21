use super::{GenericValue, IntSize};
use crate::frame::ValkeyFrame;

impl From<ValkeyFrame> for GenericValue {
    fn from(f: ValkeyFrame) -> Self {
        match f {
            ValkeyFrame::SimpleString(s) => {
                GenericValue::Strings(String::from_utf8_lossy(&s).to_string())
            }
            ValkeyFrame::Error(e) => GenericValue::Strings(e.to_string()),
            ValkeyFrame::Integer(i) => GenericValue::Integer(i, IntSize::I64),
            ValkeyFrame::BulkString(b) => GenericValue::Bytes(b),
            ValkeyFrame::Array(a) => {
                GenericValue::List(a.iter().cloned().map(GenericValue::from).collect())
            }
            ValkeyFrame::Null => GenericValue::Null,
        }
    }
}

impl From<&ValkeyFrame> for GenericValue {
    fn from(f: &ValkeyFrame) -> Self {
        match f.clone() {
            ValkeyFrame::SimpleString(s) => {
                GenericValue::Strings(String::from_utf8_lossy(s.as_ref()).to_string())
            }
            ValkeyFrame::Error(e) => GenericValue::Strings(e.to_string()),
            ValkeyFrame::Integer(i) => GenericValue::Integer(i, IntSize::I64),
            ValkeyFrame::BulkString(b) => GenericValue::Bytes(b),
            ValkeyFrame::Array(a) => {
                GenericValue::List(a.iter().cloned().map(GenericValue::from).collect())
            }
            ValkeyFrame::Null => GenericValue::Null,
        }
    }
}

impl From<GenericValue> for ValkeyFrame {
    fn from(value: GenericValue) -> ValkeyFrame {
        match value {
            GenericValue::Null => ValkeyFrame::Null,
            GenericValue::Bytes(b) => ValkeyFrame::BulkString(b),
            GenericValue::Strings(s) => ValkeyFrame::SimpleString(s.into()),
            GenericValue::Integer(i, _) => ValkeyFrame::Integer(i),
            GenericValue::Float(f) => ValkeyFrame::SimpleString(f.to_string().into()),
            GenericValue::Boolean(b) => ValkeyFrame::Integer(i64::from(b)),
            GenericValue::Inet(i) => ValkeyFrame::SimpleString(i.to_string().into()),
            GenericValue::List(l) => ValkeyFrame::Array(l.into_iter().map(|v| v.into()).collect()),
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
