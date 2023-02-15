pub mod cassandra;

pub use cassandra::{CassandraFrame, CassandraOperation, CassandraResult};
pub use redis_protocol::resp2::types::Frame as RedisFrame;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum MessageType {
    Redis,
    Cassandra,
}

#[derive(PartialEq, Debug, Clone)]
pub enum Frame {
    Cassandra(CassandraFrame),
    Redis(RedisFrame),
}

impl Frame {
    pub fn from_bytes(bytes: Bytes, message_type: MessageType) -> Result<Self> {
        match message_type {
            MessageType::Cassandra => CassandraFrame::from_bytes(bytes).map(Frame::Cassandra),
            MessageType::Redis => redis_protocol::resp2::decode::decode(&bytes)
                .map(|x| Frame::Redis(x.unwrap().0))
                .map_err(|e| anyhow!("{e:?}")),
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
