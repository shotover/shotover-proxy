use std::io;

use crate::transforms::util::cluster_connection_pool::NoopError;
use crate::transforms::util::ConnectionError;

pub mod redis_cache;
pub mod redis_cluster;
pub mod redis_codec_destination;
pub mod timestamp_tagging;

#[derive(thiserror::Error, Clone, Debug)]
pub enum RedisError {
    #[error("authentication is required")]
    NotAuthenticated,

    #[error("user not authorized to perform action")]
    NotAuthorized,

    #[error("username or password is incorrect")]
    BadCredentials,

    #[error("unknown error: {0}")]
    Unknown(String),
}

impl RedisError {
    fn from_message(error: &str) -> RedisError {
        match error.splitn(2, ' ').next() {
            Some("NOAUTH") => RedisError::NotAuthenticated,
            Some("NOPERM") => RedisError::NotAuthorized,
            Some("WRONGPASS") => RedisError::BadCredentials,
            _ => RedisError::Unknown(error.to_string()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error(transparent)]
    Upstream(#[from] RedisError),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("io error: {0}")]
    IO(io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl TransformError {
    fn choose_upstream_or_first(errors: Vec<TransformError>) -> Option<TransformError> {
        errors
            .iter()
            .find_map(|e| match e {
                TransformError::Upstream(e) => Some(TransformError::Upstream(e.clone())),
                _ => None,
            })
            .or_else(|| errors.into_iter().next())
    }
}

impl From<ConnectionError<TransformError>> for TransformError {
    fn from(error: ConnectionError<TransformError>) -> Self {
        match error {
            ConnectionError::IO(e) => TransformError::IO(e),
            ConnectionError::Authenticator(e) => e,
        }
    }
}

impl From<ConnectionError<NoopError>> for TransformError {
    fn from(error: ConnectionError<NoopError>) -> Self {
        match error {
            ConnectionError::IO(e) => TransformError::IO(e),
            ConnectionError::Authenticator(_) => unimplemented!(),
        }
    }
}
