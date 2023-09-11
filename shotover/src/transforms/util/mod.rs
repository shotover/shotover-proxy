use anyhow::{Error, Result};
use std::fmt;

use crate::message::Message;

pub mod cluster_connection_pool;

/// Represents a `Request` to a connection within Shotover
#[derive(Debug)]
pub struct Request {
    // Message to send upstream to connection
    pub message: Message,
    // Channel to return the response to
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
}

/// Represents a `Response` to a `Request`
#[derive(Debug)]
pub struct Response {
    // Response to the original `Message`
    pub response: Result<Message>,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError<E: fmt::Debug + fmt::Display> {
    #[error("authenticator error: {0}")]
    Authenticator(E),

    #[error(transparent)]
    Other(#[from] Error),
}
