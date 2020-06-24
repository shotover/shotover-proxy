use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, Buf};
use redis_protocol::prelude::*;
use crate::message::{QueryResponse, QueryMessage, Message, QueryType, RawMessage, Value};
use std::collections::HashMap;
use crate::protocols::RawFrame;
use anyhow::{anyhow, Result};
use std::borrow::{BorrowMut};
use tracing::info;

#[derive(Debug, Clone)]
pub struct RedisCodec {
    // Redis doesn't have an explicit "Response" type as part of the protocol
    decode_as_response: bool
}

impl RedisCodec {
    pub fn new(decode_as_response: bool) -> RedisCodec {
        RedisCodec {
            decode_as_response
        }
    }

    fn handle_redis_string(&self, string: String, frame: Frame) -> Message {
        let message = if self.decode_as_response {
            Message::Response(QueryResponse{
                matching_query: None,
                original: RawFrame::Redis(frame),
                result: Some(Value::Strings(string)),
                error: None
            })
        } else {
            Message::Query(QueryMessage {
                original: RawFrame::Redis(frame),
                query_string: string,
                namespace: vec![],
                primary_key: Default::default(),
                query_values: None,
                projection: None,
                query_type: QueryType::Read,
                ast: None,
            })
        };

        return message
    }

    pub fn process_redis_frame(&self, frame: Frame) -> Message {
        return match &frame {
            Frame::SimpleString(s) => { self.handle_redis_string(s.clone(), frame) }
            Frame::BulkString(bs) => { self.handle_redis_string(String::from_utf8(bs.clone()).unwrap_or("invalid utf-8".to_string()), frame) }
            Frame::Array(_) => {
                match frame.parse_as_pubsub(){
                    Ok((channel, message, kind)) => {
                        let mut map: HashMap<String, Value> = HashMap::new();
                        map.insert(channel.clone(), Value::Strings(message.clone()));
                        Message::Query(QueryMessage {
                            original: RawFrame::Redis(Frame::Array(vec![Frame::SimpleString(channel.clone()), Frame::SimpleString(message), Frame::SimpleString(kind)])),
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
            Frame::Moved(m) => { self.handle_redis_string(m.clone(), frame) }
            Frame::Ask(a) => { self.handle_redis_string(a.clone(), frame) }
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
        if let Some(result) = &resp.error {
            if let Value::Strings(s) = result {
                return Frame::Error(s.clone());
            }
        }
        Frame::SimpleString("OK".to_string())
    }
    pub fn build_redis_query_frame(query: &mut QueryMessage) -> Frame {
        return Frame::SimpleString(query.query_string.clone());
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        if let (Some(frame), size) = decode_bytes(&*src)
            .map_err(|e| {
                info!("Error decoding redis frame {:?}", e);
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
            self.process_redis_frame(f)
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