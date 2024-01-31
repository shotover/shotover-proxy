//! parsed AST-like representations of messages

use crate::{codec::CodecState, message::ProtocolType};
use anyhow::{anyhow, Result};
use bytes::Bytes;
pub use cassandra::{CassandraFrame, CassandraOperation, CassandraResult};
use cassandra_protocol::compression::Compression;
use kafka::KafkaFrame;
pub use opensearch::OpenSearchFrame;
pub use redis_protocol::resp2::types::Frame as RedisFrame;
use std::fmt::{Display, Formatter, Result as FmtResult};

pub mod cassandra;
pub mod kafka;
pub mod opensearch;
pub mod redis;
pub mod value;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum MessageType {
    Redis,
    Cassandra,
    Kafka,
    Dummy,
    OpenSearch,
}

impl From<&ProtocolType> for MessageType {
    fn from(value: &ProtocolType) -> Self {
        match value {
            ProtocolType::Cassandra { .. } => Self::Cassandra,
            ProtocolType::Redis => Self::Redis,
            ProtocolType::Kafka { .. } => Self::Kafka,
            ProtocolType::OpenSearch => Self::OpenSearch,
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
            Frame::Kafka(_) => CodecState::Kafka {
                request_header: None,
            },
            Frame::Dummy => CodecState::Dummy,
            Frame::OpenSearch(_) => CodecState::OpenSearch,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Frame {
    Cassandra(CassandraFrame),
    Redis(RedisFrame),
    Kafka(KafkaFrame),
    /// Represents a message that must exist due to shotovers requirement that every request has a corresponding response.
    /// It exists purely to keep transform invariants and codecs will completely ignore this frame when they receive it
    Dummy,
    OpenSearch(OpenSearchFrame),
}

impl Frame {
    pub fn from_bytes(
        bytes: Bytes,
        message_type: MessageType,
        codec_state: CodecState,
    ) -> Result<Self> {
        match message_type {
            MessageType::Cassandra => {
                CassandraFrame::from_bytes(bytes, codec_state.as_cassandra()).map(Frame::Cassandra)
            }
            MessageType::Redis => redis_protocol::resp2::decode::decode(&bytes)
                .map(|x| Frame::Redis(x.unwrap().0))
                .map_err(|e| anyhow!("{e:?}")),
            MessageType::Kafka => {
                KafkaFrame::from_bytes(bytes, codec_state.as_kafka()).map(Frame::Kafka)
            }
            MessageType::Dummy => Ok(Frame::Dummy),
            MessageType::OpenSearch => Ok(Frame::OpenSearch(OpenSearchFrame::from_bytes(&bytes)?)),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Frame::Redis(_) => "Redis",
            Frame::Cassandra(_) => "Cassandra",
            Frame::Kafka(_) => "Kafka",
            Frame::Dummy => "Dummy",
            Frame::OpenSearch(_) => "OpenSearch",
        }
    }

    pub fn get_type(&self) -> MessageType {
        match self {
            Frame::Cassandra(_) => MessageType::Cassandra,
            Frame::Redis(_) => MessageType::Redis,
            Frame::Kafka(_) => MessageType::Kafka,
            Frame::Dummy => MessageType::Dummy,
            Frame::OpenSearch(_) => MessageType::OpenSearch,
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

    pub fn into_kafka(self) -> Result<KafkaFrame> {
        match self {
            Frame::Kafka(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected kafka frame but received {} frame",
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

    pub fn into_opensearch(self) -> Result<OpenSearchFrame> {
        match self {
            Frame::OpenSearch(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected opensearch frame but received {} frame",
                frame.name()
            )),
        }
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Frame::Cassandra(frame) => write!(f, "Cassandra {}", frame),
            Frame::Redis(frame) => write!(f, "Redis {:?}", frame),
            Frame::Kafka(frame) => write!(f, "Kafka {}", frame),
            Frame::Dummy => write!(f, "Shotover internal dummy message"),
            Frame::OpenSearch(frame) => write!(f, "OpenSearch: {:?}", frame),
        }
    }
}
