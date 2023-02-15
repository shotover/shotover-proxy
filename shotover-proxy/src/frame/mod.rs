use crate::{codec::CodecState, message::ProtocolType};
use anyhow::{anyhow, Result};
use bytes::Bytes;
pub use cassandra::{CassandraFrame, CassandraOperation, CassandraResult};
use cassandra_protocol::compression::Compression;
pub use redis_protocol::resp2::types::Frame as RedisFrame;
use std::fmt::{Display, Formatter, Result as FmtResult};
pub mod cassandra;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum MessageType {
    Redis,
    Cassandra,
    Kafka,
}

impl From<&ProtocolType> for MessageType {
    fn from(value: &ProtocolType) -> Self {
        match value {
            ProtocolType::Cassandra(_compression) => Self::Cassandra,
            ProtocolType::Redis => Self::Redis,
        }
    }
}

impl Frame {
    pub fn as_codec_state(&self) -> CodecState {
        match self {
            Frame::Cassandra(_) => CodecState::Cassandra {
                compression: Compression::None,
            },
            Frame::Redis(_) => CodecState::Redis,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Frame {
    Cassandra(CassandraFrame),
    Redis(RedisFrame),
}

impl Frame {
    pub fn from_bytes(
        bytes: Bytes,
        message_type: MessageType,
        codec_state: CodecState,
    ) -> Result<Self> {
        match message_type {
            MessageType::Cassandra => {
                CassandraFrame::from_bytes(bytes, codec_state.as_compression())
                    .map(Frame::Cassandra)
            }
            MessageType::Redis => redis_protocol::resp2::decode::decode(&bytes)
                .map(|x| Frame::Redis(x.unwrap().0))
                .map_err(|e| anyhow!("{e:?}")),
        }
    }

    pub fn from_bytes_with_compression(
        bytes: Bytes,
        message_type: MessageType,
        compression: Option<Compression>,
    ) -> Result<Self> {
        match message_type {
            MessageType::Cassandra => {
                CassandraFrame::from_bytes(bytes, compression.unwrap_or(Compression::None))
                    .map(Frame::Cassandra)
            }
            MessageType::Redis => redis_protocol::resp2::decode::decode(&bytes)
                .map(|x| Frame::Redis(x.unwrap().0))
                .map_err(|e| anyhow!("{e:?}")),
            MessageType::Kafka => todo!(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Frame::Redis(_) => "Redis",
            Frame::Cassandra(_) => "Cassandra",
        }
    }

    pub fn get_type(&self) -> MessageType {
        match self {
            Frame::Cassandra(_) => MessageType::Cassandra,
            Frame::Redis(_) => MessageType::Redis,
        }
    }

    pub fn redis(&mut self) -> Result<&mut RedisFrame> {
        match self {
            Frame::Redis(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected redis frame but received {} frame",
                frame.name()
            )),
        }
    }

    pub fn into_redis(self) -> Result<RedisFrame> {
        match self {
            Frame::Redis(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected redis frame but received {} frame",
                frame.name()
            )),
        }
    }

    pub fn into_cassandra(self) -> Result<CassandraFrame> {
        match self {
            Frame::Cassandra(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected cassandra frame but received {} frame",
                frame.name()
            )),
        }
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Frame::Cassandra(frame) => write!(f, "Cassandra {}", frame),
            Frame::Redis(frame) => write!(f, "Redis {:?})", frame),
        }
    }
}
