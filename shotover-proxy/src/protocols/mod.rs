pub mod cassandra_protocol2;
pub mod redis_codec;

pub use cassandra_proto::frame::Frame as CassandraFrame;
pub use redis_protocol::resp2::prelude::Frame as RedisFrame;

use anyhow::Result;

use crate::message::{MessageDetails, QueryType};

#[derive(PartialEq, Debug, Clone)]
pub enum RawFrame {
    Cassandra(CassandraFrame),
    Redis(RedisFrame),
    None,
}

impl RawFrame {
    pub fn build_message(&self, response: bool) -> Result<MessageDetails> {
        match self {
            RawFrame::Cassandra(_c) => Ok(MessageDetails::Unknown),
            RawFrame::Redis(frame) => redis_codec::process_redis_frame(frame, response),
            RawFrame::None => Ok(MessageDetails::Unknown),
        }
    }

    #[inline]
    pub fn get_query_type(&self) -> QueryType {
        match self {
            RawFrame::Cassandra(_) => QueryType::ReadWrite,
            RawFrame::Redis(frame) => redis_codec::redis_query_type(frame),
            RawFrame::None => QueryType::ReadWrite,
        }
    }
}
