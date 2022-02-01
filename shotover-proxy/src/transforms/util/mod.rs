use anyhow::Error;
use std::fmt;
use std::io;

use crate::error::ChainResponse;
use crate::message::Message;

pub mod cluster_connection_pool;

/// Represents a `Request` to a connection within Shotover
#[derive(Debug)]
pub struct Request {
    pub message: Message, // Message to send upstream to connection
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>, // Channel to return the response to
}

/// Represents a `Response` to a `Request`
#[derive(Debug)]
pub struct Response {
    pub original: Message,       // Original `Message` that this `Response` is to
    pub response: ChainResponse, // Response to the original `Message`
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError<E: fmt::Debug + fmt::Display> {
    #[error("io error: {0}")]
    IO(io::Error),

    #[error("TLS error: {0}")]
    TLS(Error),

    #[error("authenticator error: {0}")]
    Authenticator(E),
}
