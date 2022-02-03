pub mod cassandra;

pub use cassandra::CassandraFrame;
pub use redis_protocol::resp2::types::Frame as RedisFrame;

use anyhow::Result;

use crate::message::{MessageDetails, QueryType};

#[derive(PartialEq, Debug, Clone)]
pub enum Frame {
    Cassandra(CassandraFrame),
    Redis(RedisFrame),
    None,
}

impl Frame {
    pub fn build_message_response(&self) -> Result<MessageDetails> {
        match self {
            Frame::Cassandra(_c) => Ok(MessageDetails::Unknown),
            Frame::Redis(frame) => crate::codec::redis::process_redis_frame_response(frame)
                .map(MessageDetails::Response),
            Frame::None => Ok(MessageDetails::Unknown),
        }
    }

    pub fn build_message_query(&self) -> Result<MessageDetails> {
        match self {
            Frame::Cassandra(_c) => Ok(MessageDetails::Unknown),
            Frame::Redis(frame) => {
                crate::codec::redis::process_redis_frame_query(frame).map(MessageDetails::Query)
            }
            Frame::None => Ok(MessageDetails::Unknown),
        }
    }

    #[inline]
    pub fn get_query_type(&self) -> QueryType {
        match self {
            Frame::Cassandra(_) => QueryType::ReadWrite,
            Frame::Redis(frame) => crate::codec::redis::redis_query_type(frame),
            Frame::None => QueryType::ReadWrite,
        }
    }
}
