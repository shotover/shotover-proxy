use crate::transforms::util::ConnectionError;

#[cfg(all(feature = "valkey", feature = "cassandra"))]
pub mod cache;
pub mod cluster_ports_rewrite;
pub mod sink_cluster;
pub mod sink_single;
pub mod timestamp_tagging;

#[derive(thiserror::Error, Clone, Debug)]
pub enum ValkeyError {
    #[error("authentication is required")]
    NotAuthenticated,

    #[error("user not authorized to perform action")]
    NotAuthorized,

    #[error("username or password is incorrect")]
    BadCredentials,

    #[error("{0}")]
    Other(String),
}

impl ValkeyError {
    fn from_message(error: &str) -> ValkeyError {
        match error.split_once(' ').map(|x| x.0) {
            Some("NOAUTH") => ValkeyError::NotAuthenticated,
            Some("NOPERM") => ValkeyError::NotAuthorized,
            Some("WRONGPASS") => ValkeyError::BadCredentials,
            _ => ValkeyError::Other(error.to_string()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TransformError {
    #[error(transparent)]
    Upstream(#[from] ValkeyError),

    #[error("protocol error: {0}")]
    Protocol(String),

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
            ConnectionError::Other(e) => TransformError::Other(e),
            ConnectionError::Authenticator(e) => e,
        }
    }
}
