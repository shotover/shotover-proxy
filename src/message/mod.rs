use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc, Datelike, Timelike};
use chrono::serde::ts_nanoseconds::serialize as to_nano_ts;
use crate::protocols::cassandra_protocol2::RawFrame;
use sqlparser::ast::Statement;
use pyo3::prelude::*;
use serde::{Serialize, Deserialize};
use pyo3::types::{IntoPyDict, PyDateTime, PyUnicode, PyBytes, PyBool, PyLong, PyFloat, PyList, PyDict, PySet};
use pyo3::type_object::PyTypeInfo;
use std::borrow::Cow;
use pyo3::PyErrValue;


#[derive(PartialEq, Debug, Clone)]
pub enum Message { 
    Bypass(RawMessage),
    Query(QueryMessage),
    Response(QueryResponse)
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
    pub ast: Option<Statement>
}

impl QueryMessage {
    pub fn get_namespace(&self) -> Vec<String> {
        return self.namespace.clone()
    }

    pub fn set_namespace_elem(& mut self, index: usize, elem: String) -> String {
        let old = self.namespace.remove(index);
        self.namespace.insert(index, elem);
        return old;
    }

    pub fn get_primary_key(&self) -> Option<String> {
        let f: Vec<String> = self.primary_key.iter().map(|(_,v) | {serde_json::to_string(&v).unwrap()}).collect();
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
            error: None
        }
    }

    pub fn emptyWithOriginal(original: QueryMessage) -> Self {
        return QueryResponse {
            matching_query: Some(original),
            original: RawFrame::NONE,
            result: None,
            error: None
        }
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum QueryType {
    Read,
    Write,
    ReadWrite,
    SchemaChange
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
    List(Vec<Value>),
    Rows(Vec<Vec<Value>>),
    Document(HashMap<String, Value>),
}

impl<'p> ToPyObject for Value {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Value::NULL => {
                return None::<u8>.to_object(py);
            },
            Value::Bytes(b) => {
                return b.to_vec().to_object(py);
            },
            Value::Strings(s) => {return s.to_object(py)},
            Value::Integer(i) => {return i.to_object(py)},
            Value::Float(f) => {return f.to_object(py)},
            Value::Boolean(b) => {return b.to_object(py)},
            Value::Timestamp(t) => {return PyDateTime::from_timestamp(py, t.timestamp() as f64, None).unwrap().to_object(py)},
            Value::Rows(r) => {return r.to_object(py)},
            Value::List(r) => {return r.to_object(py)},
            Value::Document(d) => {return d.to_object(py)},
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
            }
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
    use bytes::{Bytes, Buf};
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