use crate::message::Messages;
use tokio_util::codec::{Decoder, Encoder};

pub mod cassandra;
pub mod kafka;
pub mod redis;

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
pub trait CodecReadHalf: Decoder<Item = Messages, Error = CodecReadError> + Clone + Send {}
impl<T: Decoder<Item = Messages, Error = CodecReadError> + Clone + Send> CodecReadHalf for T {}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait CodecWriteHalf: Encoder<Messages, Error = anyhow::Error> + Clone + Send {}
impl<T: Encoder<Messages, Error = anyhow::Error> + Clone + Send> CodecWriteHalf for T {}

// TODO: Replace with trait_alias (rust-lang/rust#41517).
pub trait Codec: CodecReadHalf + CodecWriteHalf + Sized + Clone {
    fn clone_without_state(&self) -> Self {
        self.clone()
    }
}
