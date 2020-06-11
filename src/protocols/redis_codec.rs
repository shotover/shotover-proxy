use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, Buf};
use redis_protocol::prelude::*;
use tracing::warn;
use crate::message::{QueryResponse, QueryMessage, Message, QueryType, RawMessage, Value};
use std::collections::HashMap;
use crate::protocols::RawFrame;
use std::error::Error;
use crate::error::{ChainResponse, RequestError};
use anyhow::{anyhow, Result};
use std::borrow::{Borrow, BorrowMut};

#[derive(Debug, Clone)]
pub struct RedisCodec {
}

impl RedisCodec {
    pub fn new() -> RedisCodec {
        RedisCodec {}
    }

    fn handle_redis_string(string: String, _frame: Frame) -> Message {
        return Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: string,
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        });
    }

    pub fn process_redis_frame(frame: Frame) -> Message {
        return match &frame {
            Frame::SimpleString(s) => { RedisCodec::handle_redis_string(s.clone(), frame) }
            Frame::BulkString(bs) => { RedisCodec::handle_redis_string(String::from_utf8(bs.clone()).unwrap_or("invalid utf-8".to_string()), frame) }
            Frame::Array(_) => {
                match frame.parse_as_pubsub(){
                    Ok((channel, message, kind)) => {
                        let mut map: HashMap<String, Value> = HashMap::new();
                        map.insert(channel.clone(), Value::Strings(message.clone()));
                        Message::Query(QueryMessage {
                            original: RawFrame::Redis(Frame::Array(vec![Frame::SimpleString(channel.clone()), Frame::SimpleString(message.clone()), Frame::SimpleString(kind.clone())])),
                            query_string: "".to_string(),
                            namespace: vec![channel],
                            primary_key: Default::default(),
                            query_values: Some(map),
                            projection: None,
                            query_type: QueryType::PubSubMessage,
                            ast: None,
                        })
                    },
                    Err(frame) => {Message::Bypass(RawMessage { original: RawFrame::Redis(frame)})},
                }
            }
            Frame::Moved(m) => { RedisCodec::handle_redis_string(m.clone(), frame) }
            Frame::Ask(a) => { RedisCodec::handle_redis_string(a.clone(), frame) }
            _ => {
                Message::Bypass(RawMessage {
                    original: RawFrame::Redis(frame)
                })
            }
        };
    }

    pub fn build_redis_response_frame(resp: &mut QueryResponse) -> Frame {
        if let Some(result) = &resp.result {
            return result.clone().into();
        }
        Frame::Null
    }
    pub fn build_redis_query_frame(query: &mut QueryMessage) -> Frame {
        return Frame::SimpleString(query.query_string.clone());
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        if let (Some(frame), size) = decode_bytes(&*src)
            .map_err(|e| {
                anyhow!("Error decoding redis frame {}", e)
            })? {
            src.advance(size);
            return Ok(Some(frame));
        } else {
            return Ok(None);
        }
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        encode_bytes(dst, &item).map(|_| {()}).map_err(|e| {
            anyhow!( "Uh - oh {} - {:#?}",e, item)
        })
    }
}

impl Decoder for RedisCodec {
    type Item = Message;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, Self::Error> {
        return Ok(self.decode_raw(src)?.map(|f| {
            RedisCodec::process_redis_frame(f)
        }));
    }
}

impl Encoder<Message> for RedisCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> std::result::Result<(), Self::Error> {
        match item {
            Message::Modified(mut modified_message) => {
                match modified_message.borrow_mut() {
                    Message::Bypass(_) => {
                        //TODO: throw error -> we should not be modifing a bypass message
                    },
                    Message::Query(q) => {
                        return self.encode_raw(RedisCodec::build_redis_query_frame(q), dst);
                    },
                    Message::Response(r) => {
                        return self.encode_raw(RedisCodec::build_redis_response_frame(r), dst);
                    },
                    Message::Modified(_) => {
                        //TODO: throw error -> we should not have a nested modified message
                    },
                }
            }

            Message::Query(qm) => {
                if let RawFrame::Redis(frame) = qm.original {
                    return self.encode_raw(frame, dst)
                } else {
                    //TODO throw error
                }
            }
            Message::Response(resp) => {
                if let RawFrame::Redis(frame) = resp.original {
                    return self.encode_raw(frame, dst)
                } else {
                    //TODO throw error
                }
            }
            Message::Bypass(resp) => {
                if let RawFrame::Redis(frame) = resp.original {
                    return self.encode_raw(frame, dst)
                } else {
                    //TODO throw error
                }
            }
        }
        Err(anyhow!("Could not process and send Cassandra Frame"))

    }
}