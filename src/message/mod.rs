use bytes::{Buf, Bytes};
use crate::error::{ChainResponse, RequestError};
use anyhow::{anyhow, Result};
use cassandra_proto::types::CBytes;
use chrono::serde::ts_nanoseconds::serialize as to_nano_ts;
use chrono::{DateTime, Utc, TimeZone};
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;
use pyo3::types::{
    PyBool, PyBytes, PyDateTime, PyDict, PyFloat, PyList, PyLong, PySet, PyUnicode,
};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::{Key, Nonce};
use sqlparser::ast::Statement;
use std::collections::{HashMap};
use std::error::Error;
use cassandra_proto::frame::frame_result::{ColSpec, ColType};
use cassandra_proto::types::data_serialization_types::{decode_ascii, decode_bigint, decode_boolean, decode_int, decode_decimal, decode_double, decode_float, decode_timestamp, decode_varchar, decode_varint, decode_inet, decode_smallint, decode_tinyint};
use std::net::IpAddr;
use redis_protocol::types::Frame;
use crate::protocols::RawFrame;

#[derive(PartialEq, Debug, Clone)]
pub enum Message {
    Bypass(RawMessage),
    Query(QueryMessage),
    Response(QueryResponse),
    Modified(Box<Message>) //The box is to put the nested Message on the heap so we can have a recursive Message
}

#[derive(PartialEq, Debug, Clone)]
pub struct RawMessage {
    pub original: RawFrame,
}

#[pyclass]
struct Struct {
    #[pyo3(get, set)]
    pub string: String,
    #[pyo3(get, set)]
    pub number: u32,
    #[pyo3(get, set)]
    pub vec: Vec<i32>,
}

#[pyclass]
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryMessage {
    pub original: RawFrame,
    #[pyo3(get, set)]
    pub query_string: String,
    #[pyo3(get, set)]
    pub namespace: Vec<String>,
    #[pyo3(get, set)]
    pub primary_key: HashMap<String, Value>,
    #[pyo3(get, set)]
    pub query_values: Option<HashMap<String, Value>>,
    #[pyo3(get, set)]
    pub projection: Option<Vec<String>>,
    pub query_type: QueryType,
    #[serde(skip)]
    pub ast: Option<Statement>,
}

impl QueryMessage {
    pub fn get_namespace(&self) -> Vec<String> {
        return self.namespace.clone();
    }

    pub fn set_namespace_elem(&mut self, index: usize, elem: String) -> String {
        let old = self.namespace.remove(index);
        self.namespace.insert(index, elem);
        return old;
    }

    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self
            .primary_key
            .iter()
            .map(|(_, v)| serde_json::to_string(&v).unwrap())
            .collect();
        return Some(f.join("."));
    }

    pub fn get_namespaced_primary_key(&self) -> Option<String> {
        if let Some(pk) = self.get_primary_key() {
            let mut buffer = String::new();
            let f: String = self.namespace.join(".");
            buffer.push_str(f.as_str());
            buffer.push_str(".");
            buffer.push_str(serde_json::to_string(&pk).unwrap().as_str());
            return Some(buffer);
        }
        return None;
    }
}

#[pyclass]
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub matching_query: Option<QueryMessage>,
    pub original: RawFrame,
    #[pyo3(get, set)]
    pub result: Option<Value>,
    #[pyo3(get, set)]
    pub error: Option<Value>,
}

impl QueryResponse {
    pub fn empty() -> Self {
        return QueryResponse {
            matching_query: None,
            original: RawFrame::NONE,
            result: None,
            error: None,
        };
    }

    pub fn empty_with_original(original: QueryMessage) -> Self {
        return QueryResponse {
            matching_query: Some(original),
            original: RawFrame::NONE,
            result: None,
            error: None,
        };
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum QueryType {
    Read,
    Write,
    ReadWrite,
    SchemaChange,
    PubSubMessage
}

// A protected value meets the following properties:
// https://doc.libsodium.org/secret-key_cryptography/secretbox
// This all relies on crypto_secretbox_easy which takes care of
// all padding, copying and timing issues associated with crypto
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Protected {
    Plaintext(Value),
    Ciphertext { cipher: Vec<u8>, nonce: Nonce },
}

fn encrypt(plaintext: String, sym_key: &Key) -> (Vec<u8>, Nonce) {
    let nonce = secretbox::gen_nonce();
    let ciphertext = secretbox::seal(plaintext.as_bytes(), &nonce, sym_key);
    return (ciphertext, nonce);
}

fn decrypt(ciphertext: Vec<u8>, nonce: Nonce, sym_key: &Key) -> Result<Value> {
    let decrypted_bytes = secretbox::open(&ciphertext, &nonce, sym_key).map_err(|_| anyhow!("couldn't open box"))?;
    //todo make error handing better here - failure here indicates a authenticity failure
    let decrypted_value: Value =
        serde_json::from_slice(decrypted_bytes.as_slice()).map_err(|_| anyhow!("couldn't open box"))?;
    return Ok(decrypted_value);
}

impl From<Protected> for Value {
    fn from(p: Protected) -> Self {
        match p {
            Protected::Plaintext(_) => panic!(
                "tried to move unencrypted value to plaintext without explicitly calling decrypt"
            ),
            Protected::Ciphertext { .. } => {
                Value::Bytes(Bytes::from(serde_json::to_vec(&p).unwrap()))
            }
        }
    }
}

impl Protected {
    pub fn from_encrypted_bytes_value(value: &Value) -> Result<Protected> {
        match value {
            Value::Bytes(b) => {
                return Ok(serde_json::from_slice(b.bytes())?);
            }
            _ => {
                return Err(anyhow!("Could not get bytes to decrypt - wrong value type {:?}", value));
            }
        }
    }

    pub fn protect(self, sym_key: &Key) -> Protected {
        match &self {
            Protected::Plaintext(p) => {
                let (cipher, nonce) = encrypt(serde_json::to_string(p).unwrap(), sym_key);
                Protected::Ciphertext { cipher, nonce }
            }
            Protected::Ciphertext {
                cipher: _,
                nonce: _,
            } => self,
        }
    }

    pub fn unprotect(self, sym_key: &Key) -> Value {
        return match self {
            Protected::Plaintext(p) => p,
            Protected::Ciphertext { cipher, nonce } => decrypt(cipher, nonce, sym_key).unwrap(),
        };
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    NULL,
    #[serde(with = "my_bytes")]
    Bytes(Bytes),
    Strings(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    #[serde(serialize_with = "to_nano_ts")]
    Timestamp(DateTime<Utc>),
    Inet(IpAddr),
    List(Vec<Value>),
    Rows(Vec<Vec<Value>>),
    NamedRows(Vec<HashMap<String, Value>>),
    Document(HashMap<String, Value>),
}

impl Into<Frame> for Value {
    fn into(self) -> Frame {
        match self {
            Value::NULL => {Frame::Null},
            Value::Bytes(b) => {Frame::BulkString(b.to_vec())},
            Value::Strings(s) => {Frame::SimpleString(s)},
            Value::Integer(i) => {Frame::Integer(i)},
            Value::Float(f) => {Frame::SimpleString(f.to_string())},
            Value::Boolean(b) => {Frame::Integer(i64::from(b))},
            Value::Timestamp(t) => {Frame::SimpleString(t.to_rfc2822())},
            Value::Inet(i) => {Frame::SimpleString(i.to_string())},
            Value::List(l) => {Frame::Array(l.iter().cloned().map(|v|v.into()).collect())},
            Value::Rows(r) => {Frame::Array(r.iter().cloned().map(|v|Value::List(v).into()).collect())},
            Value::NamedRows(_) => {unimplemented!()},
            Value::Document(_) => {unimplemented!()},
        }
    }
}

impl Value {
    pub fn build_value_from_cstar_col_type(spec: &ColSpec, data: &CBytes) -> Value {
        if let Some(actual_bytes) = data.as_slice() {
            return match spec.col_type.id {
                ColType::Ascii => {
                    return Value::Strings(decode_ascii(actual_bytes).unwrap())
                },
                ColType::Bigint => {
                    return Value::Integer(decode_bigint(actual_bytes).unwrap())
                },
                ColType::Blob => {
                    return Value::Bytes(Bytes::copy_from_slice(actual_bytes))
                },
                ColType::Boolean => {
                    return Value::Boolean(decode_boolean(actual_bytes).unwrap())
                },
                ColType::Counter => {
                    Value::Integer(decode_int(actual_bytes).unwrap() as i64)
                },
                ColType::Decimal => {
                    Value::Float(decode_decimal(actual_bytes).unwrap().as_plain())
                },
                ColType::Double => {
                    Value::Float(decode_double(actual_bytes).unwrap())
                },
                ColType::Float => {
                    Value::Float(decode_float(actual_bytes).unwrap() as f64)
                },
                ColType::Int => {
                    Value::Integer(decode_int(actual_bytes).unwrap() as i64)
                },
                ColType::Timestamp => {
                    Value::Timestamp(Utc.timestamp_nanos(decode_timestamp(actual_bytes).unwrap()))
                },
                ColType::Uuid => {
                    Value::Bytes(Bytes::copy_from_slice(actual_bytes))
                },
                ColType::Varchar => {
                    Value::Strings(decode_varchar(actual_bytes).unwrap())
                },
                ColType::Varint => {
                    Value::Integer(decode_varint(actual_bytes).unwrap())
                },
                ColType::Timeuuid => {
                    Value::Bytes(Bytes::copy_from_slice(actual_bytes))
                },
                ColType::Inet => {
                    Value::Inet(decode_inet(actual_bytes).unwrap())
                },
                ColType::Date => {
                    Value::NULL
                },
                ColType::Time => {
                    Value::NULL
                },
                ColType::Smallint => {
                    Value::Integer(decode_smallint(actual_bytes).unwrap() as i64)
                },
                ColType::Tinyint => {
                    Value::Integer(decode_tinyint(actual_bytes).unwrap() as i64)
                },
                _ => {
                    Value::NULL
                    // todo: process collection types based on ColTypeOption
                    // (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L569)
                },
            }
        }
        Value::NULL
    }
}

impl Into<cassandra_proto::types::value::Bytes> for Value {
    fn into(self) -> cassandra_proto::types::value::Bytes {
        return match self {
            Value::NULL => (-1).into(),
            Value::Bytes(b) => cassandra_proto::types::value::Bytes::new(b.to_vec()),
            Value::Strings(s) => s.into(),
            Value::Integer(i) => i.into(),
            Value::Float(f) => f.into(),
            Value::Boolean(b) => b.into(),
            Value::Timestamp(t) => t.timestamp().into(),
            Value::List(l) => cassandra_proto::types::value::Bytes::from(l),
            Value::Rows(r) => cassandra_proto::types::value::Bytes::from(r),
            Value::NamedRows(n) => cassandra_proto::types::value::Bytes::from(n),
            Value::Document(d) => cassandra_proto::types::value::Bytes::from(d),
            Value::Inet(i) => {i.into()}
        };
    }
}

impl<'p> ToPyObject for Value {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Value::NULL => {
                return None::<u8>.to_object(py);
            }
            Value::Bytes(b) => {
                return b.to_vec().to_object(py);
            }
            Value::Strings(s) => return s.to_object(py),
            Value::Integer(i) => return i.to_object(py),
            Value::Float(f) => return f.to_object(py),
            Value::Boolean(b) => return b.to_object(py),
            Value::Timestamp(t) => {
                return PyDateTime::from_timestamp(py, t.timestamp() as f64, None)
                    .unwrap()
                    .to_object(py)
            }
            Value::Rows(r) => return r.to_object(py),
            Value::NamedRows(n) => return n.to_object(py),
            Value::List(r) => return r.to_object(py),
            Value::Document(d) => return d.to_object(py),
            Value::Inet(i) => return i.to_string().to_object(py)
        }
    }
}

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl FromPyObject<'_> for Value {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        //TODO:: convert based on https://pyo3.rs/master/conversions.html
        if PyUnicode::is_instance(ob) {
            let f: &PyUnicode = ob.downcast()?;
            return Ok(Value::Strings(String::from(f.to_string()?)));
        } else if PyBytes::is_instance(ob) {
            let f: &PyBytes = ob.downcast()?;
            let e: Vec<u8> = f.extract()?;
            return Ok(Value::Bytes(Bytes::from(e)));
        } else if PyBool::is_instance(ob) {
            let f: &PyBool = ob.downcast()?;
            return Ok(Value::Boolean(f.extract()?));
        } else if PyLong::is_instance(ob) {
            let f: &PyLong = ob.downcast()?;
            let e: i64 = f.extract()?;
            return Ok(Value::Integer(e));
        } else if PyFloat::is_instance(ob) {
            let f: &PyLong = ob.downcast()?;
            let e: f64 = f.extract()?;
            return Ok(Value::Float(e));
        } else if PyList::is_instance(ob) {
            let i: &PyList = ob.downcast()?;
            return if i.is_empty() {
                Ok(Value::List(Vec::new()))
            } else {
                if PyList::is_instance(i.get_item(0)) {
                    Ok(Value::Rows(i.extract()?))
                } else {
                    Ok(Value::List(i.extract()?))
                }
            };
        } else if PyDict::is_instance(ob) {
            let f: &PyDict = ob.downcast()?;
            let e: HashMap<String, Value> = f.extract()?;
            return Ok(Value::Document(e));
        } else if PySet::is_instance(ob) {
            let f: &PySet = ob.downcast()?;
            let e: HashMap<String, Value> = f.extract()?; //HashSet is just a HashMap
            return Ok(Value::Document(e));
        } else if PyDict::is_instance(ob) {
            let f: &PyDict = ob.downcast()?;
            let e: HashMap<String, Value> = f.extract()?;
            return Ok(Value::Document(e));
        } else if PyDateTime::is_instance(ob) {
            // let f: &PyDateTime = ob.downcast()?;
            // let e: DateTime<Utc> = DateTime::
            // return Ok(Value::Timestamp(e));
        }
        return Err(PyErr::from_instance(ob));
    }
}

mod my_bytes {
    use bytes::{Buf, Bytes};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(val: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(val.bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(val))
    }
}

#[cfg(test)]
mod crypto_tests {
    use crate::message::{Protected, Value};
    use rdkafka::message::ToBytes;
    use sodiumoxide::crypto::secretbox;
    use std::error::Error;

    #[test]
    fn test_crypto() -> Result<(), Box<dyn Error>> {
        let key = secretbox::gen_key();

        let test_value = Value::Strings(String::from("Hello I am a string to be encrypted!!!!"));

        let mut protected = Protected::Plaintext(test_value.clone());
        protected = protected.protect(&key); //TODO look at https://crates.io/crates/replace_with to make this inplace
        let protected_value: Value = protected.into();

        if let (Value::Strings(s), Value::Bytes(b)) = (test_value.clone(), protected_value.clone())
        {
            assert_ne!(s.as_bytes(), b.to_bytes())
        }

        //Go back the other way now

        let d_protected = Protected::from_encrypted_bytes_value(&protected_value)?;
        let d_value = d_protected.unprotect(&key);

        assert_eq!(test_value, d_value);

        Ok(())
    }
}
