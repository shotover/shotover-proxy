use anyhow::Error;
use std::fmt;
use std::io;

use crate::error::ChainResponse;
use crate::message::Message;

pub mod cluster_connection_pool;

#[derive(Debug)]
pub struct Request {
    pub messages: Message,
    pub return_chan: Option<tokio::sync::oneshot::Sender<Response>>,
    pub message_id: Option<i16>,
}

pub type Response = (Message, ChainResponse);

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError<E: fmt::Debug + fmt::Display> {
    #[error("io error: {0}")]
    IO(io::Error),

    #[error("TLS error: {0}")]
    TLS(Error),

    #[error("authenticator error: {0}")]
    Authenticator(E),
}
