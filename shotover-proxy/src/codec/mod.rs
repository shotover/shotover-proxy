use crate::message::Messages;
use cassandra_protocol::compression::Compression;
use tokio_util::codec::{Decoder, Encoder};

pub mod cassandra;
pub mod kafka;
pub mod redis;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum CodecState {
    Cassandra { compression: Compression },
    Redis,
    Kafka,
}

impl CodecState {
    pub fn as_compression(&self) -> Compression {
        match self {
            CodecState::Cassandra { compression } => *compression,
            _ => {
                panic!("This is a {self:?}, expected CodecState::Cassandra")
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

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait DecoderHalf: Decoder<Item = Messages, Error = CodecReadError> + Send {}
impl<T: Decoder<Item = Messages, Error = CodecReadError> + Send> DecoderHalf for T {}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait EncoderHalf: Encoder<Messages, Error = anyhow::Error> + Send {}
impl<T: Encoder<Messages, Error = anyhow::Error> + Send> EncoderHalf for T {}

pub trait CodecBuilder: Clone + Send {
    type Decoder: DecoderHalf;
    type Encoder: EncoderHalf;
    fn build(&self) -> (Self::Decoder, Self::Encoder);
}
