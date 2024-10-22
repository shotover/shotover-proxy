//! Codec types to use for connecting to a DB in a sink transform

use crate::{frame::MessageType, message::Messages};
#[cfg(feature = "cassandra")]
use cassandra_protocol::compression::Compression;
use core::fmt;
#[cfg(feature = "kafka")]
use kafka::RequestHeader;
#[cfg(feature = "kafka")]
use kafka::SaslType;
use metrics::{histogram, Histogram};
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(feature = "redis")]
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

pub fn message_latency(direction: Direction, destination_name: String) -> Histogram {
    match direction {
        Direction::Source => {
            histogram!("shotover_sink_to_source_latency_seconds", "source" => destination_name)
        }
        Direction::Sink => {
            histogram!("shotover_source_to_sink_latency_seconds", "sink" => destination_name)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum CodecState {
    #[cfg(feature = "cassandra")]
    Cassandra {
        compression: Compression,
    },
    #[cfg(feature = "redis")]
    Redis,
    #[cfg(feature = "kafka")]
    Kafka(KafkaCodecState),
    Dummy,
    #[cfg(feature = "opensearch")]
    OpenSearch,
}

impl CodecState {
    #[cfg(feature = "cassandra")]
    pub fn as_cassandra(&self) -> Compression {
        match self {
            CodecState::Cassandra { compression } => *compression,
            _ => {
                panic!("This is a {self:?}, expected CodecState::Cassandra")
            }
        }
    }

    #[cfg(feature = "kafka")]
    pub fn as_kafka(&self) -> KafkaCodecState {
        match self {
            CodecState::Kafka(state) => *state,
            _ => {
                panic!("This is a {self:?}, expected CodecState::Kafka")
            }
        }
    }
}

#[cfg(feature = "kafka")]
#[derive(Debug, Clone, PartialEq, Copy)]
pub struct KafkaCodecState {
    pub request_header: Option<RequestHeader>,
    pub raw_sasl: Option<SaslType>,
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

    fn new(direction: Direction, destination_name: String) -> Self;

    fn protocol(&self) -> MessageType;
}
