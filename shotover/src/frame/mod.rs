//! parsed AST-like representations of messages

#[cfg(feature = "kafka")]
use crate::codec::kafka::KafkaCodecState;
use crate::codec::CodecState;
use anyhow::{anyhow, Result};
use bytes::Bytes;
#[cfg(feature = "cassandra")]
pub use cassandra::{CassandraFrame, CassandraOperation, CassandraResult};
#[cfg(feature = "cassandra")]
use cassandra_protocol::compression::Compression;
#[cfg(feature = "kafka")]
use kafka::KafkaFrame;
#[cfg(feature = "opensearch")]
pub use opensearch::OpenSearchFrame;
#[cfg(feature = "redis")]
pub use redis_protocol::resp2::types::BytesFrame as ValkeyFrame;
use std::fmt::{Display, Formatter, Result as FmtResult};

#[cfg(feature = "cassandra")]
pub mod cassandra;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "opensearch")]
pub mod opensearch;
#[cfg(feature = "redis")]
pub mod valkey;
pub mod value;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum MessageType {
    #[cfg(feature = "redis")]
    Valkey,
    #[cfg(feature = "cassandra")]
    Cassandra,
    #[cfg(feature = "kafka")]
    Kafka,
    #[cfg(feature = "opensearch")]
    OpenSearch,
    Dummy,
}

impl MessageType {
    pub fn is_inorder(&self) -> bool {
        match self {
            #[cfg(feature = "cassandra")]
            MessageType::Cassandra => false,
            #[cfg(feature = "redis")]
            MessageType::Valkey => true,
            #[cfg(feature = "kafka")]
            MessageType::Kafka => true,
            #[cfg(feature = "opensearch")]
            MessageType::OpenSearch => true,
            MessageType::Dummy => false,
        }
    }

    pub fn websocket_subprotocol(&self) -> &'static str {
        match self {
            #[cfg(feature = "cassandra")]
            MessageType::Cassandra => "cql",
            #[cfg(feature = "redis")]
            MessageType::Valkey => "redis",
            #[cfg(feature = "kafka")]
            MessageType::Kafka => "kafka",
            #[cfg(feature = "opensearch")]
            MessageType::OpenSearch => "opensearch",
            MessageType::Dummy => "dummy",
        }
    }
}

impl From<&CodecState> for MessageType {
    fn from(value: &CodecState) -> Self {
        match value {
            #[cfg(feature = "cassandra")]
            CodecState::Cassandra { .. } => Self::Cassandra,
            #[cfg(feature = "redis")]
            CodecState::Valkey => Self::Valkey,
            #[cfg(feature = "kafka")]
            CodecState::Kafka { .. } => Self::Kafka,
            #[cfg(feature = "opensearch")]
            CodecState::OpenSearch => Self::OpenSearch,
            CodecState::Dummy => Self::Dummy,
        }
    }
}

impl Frame {
    pub fn as_codec_state(&self) -> CodecState {
        match self {
            #[cfg(feature = "cassandra")]
            Frame::Cassandra(_) => CodecState::Cassandra {
                compression: Compression::None,
            },
            #[cfg(feature = "redis")]
            Frame::Valkey(_) => CodecState::Valkey,
            #[cfg(feature = "kafka")]
            Frame::Kafka(_) => CodecState::Kafka(KafkaCodecState {
                request_header: None,
                raw_sasl: false,
            }),
            Frame::Dummy => CodecState::Dummy,
            #[cfg(feature = "opensearch")]
            Frame::OpenSearch(_) => CodecState::OpenSearch,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Frame {
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraFrame),
    #[cfg(feature = "redis")]
    Valkey(ValkeyFrame),
    #[cfg(feature = "kafka")]
    Kafka(KafkaFrame),
    /// Represents a message that must exist due to shotovers requirement that every request has a corresponding response.
    /// It exists purely to keep transform invariants and codecs will completely ignore this frame when they receive it
    Dummy,
    #[cfg(feature = "opensearch")]
    OpenSearch(OpenSearchFrame),
}

impl Frame {
    pub fn from_bytes(
        bytes: Bytes,
        message_type: MessageType,
        codec_state: CodecState,
    ) -> Result<Self> {
        match message_type {
            #[cfg(feature = "cassandra")]
            MessageType::Cassandra => {
                CassandraFrame::from_bytes(bytes, codec_state.as_cassandra()).map(Frame::Cassandra)
            }
            #[cfg(feature = "redis")]
            MessageType::Valkey => redis_protocol::resp2::decode::decode_bytes(&bytes)
                .map(|x| Frame::Valkey(x.unwrap().0))
                .map_err(|e| anyhow!("{e:?}")),
            #[cfg(feature = "kafka")]
            MessageType::Kafka => {
                KafkaFrame::from_bytes(bytes, codec_state.as_kafka()).map(Frame::Kafka)
            }
            MessageType::Dummy => Ok(Frame::Dummy),
            #[cfg(feature = "opensearch")]
            MessageType::OpenSearch => Ok(Frame::OpenSearch(OpenSearchFrame::from_bytes(&bytes)?)),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            #[cfg(feature = "redis")]
            Frame::Valkey(_) => "Valkey",
            #[cfg(feature = "cassandra")]
            Frame::Cassandra(_) => "Cassandra",
            #[cfg(feature = "kafka")]
            Frame::Kafka(_) => "Kafka",
            Frame::Dummy => "Dummy",
            #[cfg(feature = "opensearch")]
            Frame::OpenSearch(_) => "OpenSearch",
        }
    }

    pub fn get_type(&self) -> MessageType {
        match self {
            #[cfg(feature = "cassandra")]
            Frame::Cassandra(_) => MessageType::Cassandra,
            #[cfg(feature = "redis")]
            Frame::Valkey(_) => MessageType::Valkey,
            #[cfg(feature = "kafka")]
            Frame::Kafka(_) => MessageType::Kafka,
            Frame::Dummy => MessageType::Dummy,
            #[cfg(feature = "opensearch")]
            Frame::OpenSearch(_) => MessageType::OpenSearch,
        }
    }

    #[cfg(feature = "redis")]
    pub fn valkey(&mut self) -> Result<&mut ValkeyFrame> {
        match self {
            Frame::Valkey(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected valkey frame but received {} frame",
                frame.name()
            )),
        }
    }

    #[cfg(feature = "kafka")]
    pub fn into_kafka(self) -> Result<KafkaFrame> {
        match self {
            Frame::Kafka(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected kafka frame but received {} frame",
                frame.name()
            )),
        }
    }

    #[cfg(feature = "redis")]
    pub fn into_valkey(self) -> Result<ValkeyFrame> {
        match self {
            Frame::Valkey(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected valkey frame but received {} frame",
                frame.name()
            )),
        }
    }

    #[cfg(feature = "cassandra")]
    pub fn into_cassandra(self) -> Result<CassandraFrame> {
        match self {
            #[cfg(feature = "cassandra")]
            Frame::Cassandra(frame) => Ok(frame),
            frame => Err(anyhow!(
                "Expected cassandra frame but received {} frame",
                frame.name()
            )),
        }
    }

    #[cfg(feature = "opensearch")]
    pub fn into_opensearch(self) -> Result<OpenSearchFrame> {
        match self {
            #[cfg(feature = "opensearch")]
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
            #[cfg(feature = "cassandra")]
            Frame::Cassandra(frame) => write!(f, "Cassandra {}", frame),
            #[cfg(feature = "redis")]
            Frame::Valkey(frame) => write!(f, "Valkey {:?}", frame),
            #[cfg(feature = "kafka")]
            Frame::Kafka(frame) => write!(f, "Kafka {}", frame),
            Frame::Dummy => write!(f, "Shotover internal dummy message"),
            #[cfg(feature = "opensearch")]
            Frame::OpenSearch(frame) => write!(f, "OpenSearch: {:?}", frame),
        }
    }
}
