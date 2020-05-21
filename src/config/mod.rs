use std::{fmt, error};
use rhai::ParseError;

pub mod topology;


#[derive(Debug)]
pub struct ConfigError {
    pub message: String,
    pub source: Option<Box<dyn error::Error + 'static>>
}

impl ConfigError {
    pub fn new(message: &str) -> Self {
        ConfigError{
            message: String::from(message),
            source: None
        }
    }

    pub fn from(error: Box<dyn error::Error + 'static>) -> Self {
        ConfigError {
            message: error.to_string(),
            source: Some(error)
        }
    }
}

impl From<rhai::ParseError> for ConfigError {
    fn from(e: ParseError) -> Self {
        return ConfigError::from(Box::new(e))
    }
}

impl From<std::boxed::Box<rhai::ParseError>> for ConfigError {
    fn from(e: Box<ParseError>) -> Self {
        return ConfigError::from(e)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "An error occured: {:}", self.message)
    }
}

impl error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.source.as_ref().map(|e| e.as_ref())
    }
}