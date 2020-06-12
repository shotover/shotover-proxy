use pyo3::PyErr;
use std::{fmt, error};
use crate::message::Message;
use thiserror::Error;


#[derive(Error, Clone, Debug)]
pub enum RequestError {
    #[error("Invalid header (expected {expected:?}, got {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    #[error("Malform Request: {0}")]
    MalformedRequest(String),

    #[error("Could not process script: {0}")]
    ScriptProcessingError(String),

    #[error("Could not process chain: {0}")]
    ChainProcessingError(String),


}


#[derive(Error, Debug)]
pub struct ConfigError {
    pub message: String,
    pub source: Option<Box<dyn error::Error + 'static>>,
}

impl ConfigError {
    pub fn new(message: &str) -> Self {
        ConfigError {
            message: String::from(message),
            source: None,
        }
    }

    pub fn from(error: Box<dyn error::Error + 'static>) -> Self {
        ConfigError {
            message: error.to_string(),
            source: Some(error),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An error occured: {:}", self.message)
    }
}
//
// impl error::Error for ConfigError {
//     fn source(&self) -> Option<&(dyn error::Error + 'static)> {
//         self.source.as_ref().map(|e| e.as_ref())
//     }
// }


pub type ChainResponse = anyhow::Result<Message>;


impl From<pyo3::PyErr> for RequestError {
    fn from(e: PyErr) -> Self {
        return RequestError::ScriptProcessingError(format!("{:?}", e));
    }
}