use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, Buf};
use redis_protocol::prelude::*;
use slog::Logger;
use slog::warn;
use crate::transforms::chain::RequestError;
use crate::message::{QueryResponse, QueryMessage};

#[derive(Debug)]
pub struct RedisCodec {
    logger: Logger,
}

impl RedisCodec {
    pub fn new(logger: Logger) -> RedisCodec {
        RedisCodec {
            logger
        }
    }

    pub fn build_redis_response_frame(resp: QueryResponse) -> Frame {
        if let Some(result) = resp.result {
            return result.into();
        }
        Frame::Null
    }
    pub fn build_redis_query_frame(query: QueryMessage) -> Frame {
        return Frame::SimpleString(query.query_string);
    }
}

impl Decoder for RedisCodec {
    type Item = Frame;
    type Error = RequestError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if let (Some(frame), size) = decode_bytes(&*src)
            .map_err(|e| {
            warn!(self.logger, "Error decoding redis frame {}", e);
            RequestError {}
        })? {
            src.advance(size);
            return Ok(Some(frame));
        }
        Err(RequestError {})
    }
}

impl Encoder<Frame> for RedisCodec {
    type Error = RequestError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode(dst, &item).map(|_| {()}).map_err(|e| {
            warn!(self.logger, "Uh oh {}",e);
            RequestError{}
        })
    }
}