//! Codec types to use for connecting to a DB in a sink transform

use crate::message::Messages;
use cassandra_protocol::compression::Compression;
use core::fmt;
use kafka::RequestHeader;
use tokio_util::codec::{Decoder, Encoder};

pub mod cassandra;
pub mod kafka;
pub mod redis;

#[derive(Eq, PartialEq, Copy, Clone)]
pub enum Direction {
    Source,
    Sink,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sink => write!(f, "Sink"),
            Self::Source => write!(f, "Source"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum CodecState {
    Cassandra {
        compression: Compression,
    },
    Redis,
    Kafka {
        request_header: Option<RequestHeader>,
    },
}

impl CodecState {
    pub fn as_cassandra(&self) -> Compression {
        match self {
            CodecState::Cassandra { compression } => *compression,
            _ => {
                panic!("This is a {self:?}, expected CodecState::Cassandra")
            }
        }
    }

    pub fn as_kafka(&self) -> Option<RequestHeader> {
        match self {
            CodecState::Kafka { request_header } => *request_header,
            _ => {
                panic!("This is a {self:?}, expected CodecState::Kafka")
            }
        }
    }
}

#[derive(Debug)]
pub enum CodecReadError {
    /// The codec failed to parse a received message
    Parser(anyhow::Error),
    /// The tcp connection returned an error
    Io(std::io::Error),
    /// Respond to the client with the provided messages and then close the connection
    RespondAndThenCloseConnection(Messages),
}

impl From<std::io::Error> for CodecReadError {
    fn from(err: std::io::Error) -> Self {
        CodecReadError::Io(err)
    }
}

#[derive(Debug)]
pub enum CodecWriteError {
    /// The codec failed to encode a received message
    Encoder(anyhow::Error),
    /// The tcp connection returned an error
    Io(std::io::Error),
}

impl From<std::io::Error> for CodecWriteError {
    fn from(err: std::io::Error) -> Self {
        CodecWriteError::Io(err)
    }
}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait DecoderHalf: Decoder<Item = Messages, Error = CodecReadError> + Send {}
impl<T: Decoder<Item = Messages, Error = CodecReadError> + Send> DecoderHalf for T {}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait EncoderHalf: Encoder<Messages, Error = CodecWriteError> + Send {}
impl<T: Encoder<Messages, Error = CodecWriteError> + Send> EncoderHalf for T {}

pub trait CodecBuilder: Clone + Send {
    type Decoder: DecoderHalf;
    type Encoder: EncoderHalf;
    fn build(&self) -> (Self::Decoder, Self::Encoder);

    fn new(direction: Direction) -> Self;
}
