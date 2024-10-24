//! Codec types to use for connecting to a DB in a sink transform

use crate::{frame::MessageType, message::Messages};
#[cfg(feature = "cassandra")]
use cassandra_protocol::compression::Compression;
use core::fmt;
#[cfg(feature = "kafka")]
use kafka::RequestHeader;
#[cfg(feature = "kafka")]
use kafka::SaslMessageState;
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

/// Database protocols are often designed such that their messages can be parsed without knowledge of any state of prior messages.
/// When protocols remain stateless, Shotover's parser implementations can remain fairly simple.
/// However in the real world there is often some kind of connection level state that we need to track in order to parse messages.
///
/// Shotover solves this issue via this enum which provides any of the connection level state required to decode and then reencode messages.
/// 1. The Decoder includes this value in all messages it produces.
/// 2. If any transforms call `.frame()` this value is used to parse the frame of the message.
/// 3. The Encoder uses this value to reencode the message if it has been modified.
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
    /// When the message is:
    /// a request - this value is None
    /// a response - this value is Some and contains the header values of the corresponding request.
    pub request_header: Option<RequestHeader>,
    /// When `Some` this message is not a valid kafka protocol message and is instead a raw SASL message.
    /// KafkaFrame will parse this as a SaslHandshake to hide the legacy raw SASL message from transform implementations.
    pub raw_sasl: Option<SaslMessageState>,
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
