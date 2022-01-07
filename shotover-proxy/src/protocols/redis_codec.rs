use crate::message::{
    ASTHolder, IntSize, Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType,
    Value,
};
use crate::protocols::RawFrame;
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use redis_protocol::resp2::prelude::*;
use std::collections::{BTreeMap, HashMap};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace, warn};

/// Redis doesn't have an explicit "Response" type as part of the protocol.
/// So it is up to the code to know whether it is processing queries or responses.
#[derive(Debug, Clone)]
pub enum DecodeType {
    Query,
    Response,
}

#[derive(Debug, Clone)]
pub struct RedisCodec {
    decode_type: DecodeType,
    current_frames: Vec<Frame>,
    enable_metadata: bool,
}

#[inline]
pub fn redis_query_type(frame: &Frame) -> QueryType {
    if let Frame::Array(frames) = frame {
        if let Some(Frame::BulkString(bytes)) = frames.get(0) {
            return match bytes.to_ascii_uppercase().as_slice() {
                b"APPEND" | b"BITCOUNT" | b"STRLEN" | b"GET" | b"GETRANGE" | b"MGET"
                | b"LRANGE" | b"LINDEX" | b"LLEN" | b"SCARD" | b"SISMEMBER" | b"SMEMBERS"
                | b"SUNION" | b"SINTER" | b"ZCARD" | b"ZCOUNT" | b"ZRANGE" | b"ZRANK"
                | b"ZSCORE" | b"ZRANGEBYSCORE" | b"HGET" | b"HGETALL" | b"HEXISTS" | b"HKEYS"
                | b"HLEN" | b"HSTRLEN" | b"HVALS" | b"PFCOUNT" => QueryType::Read,
                _ => QueryType::Write,
            };
        }
    }
    QueryType::Write
}

fn get_keys(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    frames: Vec<Frame>,
) -> Result<()> {
    let mut keys_storage = vec![];
    for frame in frames {
        if let Frame::BulkString(v) = frame {
            fields.insert(String::from_utf8(v.clone())?, Value::None);
            keys_storage.push(Frame::BulkString(v).into());
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
}

fn get_key_multi_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    mut frames: Vec<Frame>,
) -> Result<()> {
    if let Some(Frame::BulkString(v)) = frames.pop() {
        fields.insert(
            String::from_utf8(v.clone())?,
            Value::List(frames.into_iter().map(|x| x.into()).collect()),
        );
        keys.insert(
            "key".to_string(),
            Value::List(vec![Frame::BulkString(v).into()]),
        );
    }
    Ok(())
}

fn get_key_map(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    mut frames: Vec<Frame>,
) -> Result<()> {
    if let Some(Frame::BulkString(v)) = frames.pop() {
        let mut values = BTreeMap::new();
        while !frames.is_empty() {
            if let Some(Frame::BulkString(field)) = frames.pop() {
                if let Some(frame) = frames.pop() {
                    values.insert(String::from_utf8(field)?, frame.into());
                }
            }
        }
        fields.insert(String::from_utf8(v.clone())?, Value::Document(values));
        keys.insert(
            "key".to_string(),
            Value::List(vec![Frame::BulkString(v).into()]),
        );
    }
    Ok(())
}

fn get_key_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    mut frames: Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !frames.is_empty() {
        if let Some(Frame::BulkString(k)) = frames.pop() {
            keys_storage.push(Frame::BulkString(k.clone()).into());
            if let Some(frame) = frames.pop() {
                fields.insert(String::from_utf8(k)?, frame.into());
            }
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
}

fn handle_redis_array_query(commands_vec: Vec<Frame>) -> Result<QueryMessage> {
    let mut primary_key = HashMap::new();
    let mut query_values = HashMap::new();
    let mut query_type = QueryType::Write;
    let mut commands: Vec<Frame> = commands_vec.iter().cloned().rev().collect_vec();

    // This should be a command from the server
    // Behaviour cribbed from:
    // https://redis.io/commands and
    // https://gist.github.com/LeCoupa/1596b8f359ad8812c7271b5322c30946
    if let Some(Frame::BulkString(command)) = commands.pop() {
        match command.to_ascii_uppercase().as_slice() {
            b"APPEND" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // append a value to a key
            b"BITCOUNT" => {
                query_type = QueryType::Read;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // count set bits in a string
            b"SET" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // set value in key
            b"SETNX" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // set if not exist value in key
            b"SETRANGE" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // overwrite part of a string at key starting at the specified offset
            b"STRLEN" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the length of the value stored in a key
            b"MSET" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // set multiple keys to multiple query_values
            b"MSETNX" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // set multiple keys to multiple query_values, only if none of the keys exist
            b"GET" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get value in key
            b"GETRANGE" => {
                query_type = QueryType::Read;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // get a substring value of a key and return its old value
            b"MGET" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the values of all the given keys
            b"INCR" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // increment value in key
            b"INCRBY" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // increment the integer value of a key by the given amount
            b"INCRBYFLOAT" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // increment the float value of a key by the given amount
            b"DECR" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // decrement the integer value of key by one
            b"DECRBY" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // decrement the integer value of a key by the given number
            b"DEL" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // delete key
            b"EXPIRE" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // key will be deleted in 120 seconds
            b"TTL" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // returns the number of seconds until a key is deleted
            b"RPUSH" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // put the new value at the end of the list
            b"RPUSHX" => {
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // append a value to a list, only if the exists
            b"LPUSH" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // put the new value at the start of the list
            b"LRANGE" => {
                query_type = QueryType::Read;
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // give a subset of the list
            b"LINDEX" => {
                query_type = QueryType::Read;
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // get an element from a list by its index
            b"LINSERT" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // insert an element before or after another element in a list
            b"LLEN" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // return the current length of the list
            b"LPOP" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // remove the first element from the list and returns it
            b"LSET" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // set the value of an element in a list by its index
            b"LTRIM" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // trim a list to the specified range
            b"RPOP" => {
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // remove the last element from the list and returns it
            b"SADD" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // add the given value to the set
            b"SCARD" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the number of members in a set
            b"SREM" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // remove the given value from the set
            b"SISMEMBER" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // test if the given value is in the set.
            b"SMEMBERS" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // return a list of all the members of this set
            b"SUNION" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // combine two or more sets and returns the list of all elements
            b"SINTER" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // intersect multiple sets
            b"SMOVE" => {
                query_type = QueryType::Write;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // move a member from one set to another
            b"SPOP" => {
                query_type = QueryType::Write;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // remove and return one or multiple random members from a set
            b"ZADD" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // add one or more members to a sorted set, or update its score if it already exists
            b"ZCARD" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the number of members in a sorted set
            b"ZCOUNT" => {
                query_type = QueryType::Read;
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // count the members in a sorted set with scores within the given values
            b"ZINCRBY" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // increment the score of a member in a sorted set
            b"ZRANGE" => {
                query_type = QueryType::Read;
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // returns a subset of the sorted set
            b"ZRANK" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // determine the index of a member in a sorted set
            b"ZREM" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // remove one or more members from a sorted set
            b"ZREMRANGEBYRANK" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // remove all members in a sorted set within the given indexes
            b"ZREMRANGEBYSCORE" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // remove all members in a sorted set, by index, with scores ordered from high to low
            b"ZSCORE" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the score associated with the given mmeber in a sorted set
            b"ZRANGEBYSCORE" => {
                query_type = QueryType::Read;
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // return a range of members in a sorted set, by score
            b"HGET" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get the value of a hash field
            b"HGETALL" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get all the fields and values in a hash
            b"HSET" => {
                get_key_map(&mut query_values, &mut primary_key, commands)?;
            } // set the string value of a hash field
            b"HSETNX" => {
                get_key_map(&mut query_values, &mut primary_key, commands)?;
            } // set the string value of a hash field, only if the field does not exists
            b"HMSET" => {
                get_key_map(&mut query_values, &mut primary_key, commands)?;
            } // set multiple fields at once
            b"HINCRBY" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // increment value in hash by X
            b"HDEL" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // delete one or more hash fields
            b"HEXISTS" => {
                query_type = QueryType::Read;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // determine if a hash field exists
            b"HKEYS" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get all the fields in a hash
            b"HLEN" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get all the fields in a hash
            b"HSTRLEN" => {
                query_type = QueryType::Read;
                get_key_values(&mut query_values, &mut primary_key, commands)?;
            } // get the length of the value of a hash field
            b"HVALS" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // get all the values in a hash
            b"PFADD" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // add the specified elements to the specified HyperLogLog
            b"PFCOUNT" => {
                query_type = QueryType::Read;
                get_keys(&mut query_values, &mut primary_key, commands)?;
            } // return the approximated cardinality of the set(s) observed by the HyperLogLog at key's)
            b"PFMERGE" => {
                get_key_multi_values(&mut query_values, &mut primary_key, commands)?;
            } // merge N HyperLogLogs into a single one
            _ => {}
        }

        let query_string = commands_vec.iter().filter_map(|f| f.as_str()).join(" ");

        let ast = ASTHolder::Commands(Value::List(
            commands_vec.into_iter().map(|f| f.into()).collect(),
        ));

        Ok(QueryMessage {
            query_string,
            namespace: vec![],
            primary_key,
            query_values: Some(query_values),
            projection: None,
            query_type,
            ast: Some(ast),
        })
    } else {
        Ok(QueryMessage::empty())
    }
}

pub fn process_redis_frame_response(frame: &Frame) -> Result<QueryResponse> {
    match frame.clone() {
        Frame::SimpleString(string) => Ok(QueryResponse {
            matching_query: None,
            result: Some(Value::Strings(string)),
            error: None,
            response_meta: None,
        }),
        Frame::BulkString(bulkstring) => Ok(QueryResponse {
            matching_query: None,
            result: Some(Value::Bytes(Bytes::from(bulkstring))),
            error: None,
            response_meta: None,
        }),
        Frame::Array(frames) => Ok(QueryResponse {
            matching_query: None,
            result: Some(Value::List(frames.into_iter().map(|f| f.into()).collect())),
            error: None,
            response_meta: None,
        }),
        Frame::Integer(integer) => Ok(QueryResponse {
            matching_query: None,
            result: Some(Value::Integer(integer, IntSize::I32)),
            error: None,
            response_meta: None,
        }),
        Frame::Error(error) => Ok(QueryResponse {
            matching_query: None,
            result: None,
            error: Some(Value::Strings(error)),
            response_meta: None,
        }),
        Frame::Null => Ok(QueryResponse::empty()),
    }
}

pub fn process_redis_frame_query(frame: &Frame) -> Result<QueryMessage> {
    match frame.clone() {
        Frame::SimpleString(string) => Ok(QueryMessage {
            query_string: string,
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }),
        Frame::BulkString(bulkstring) => Ok(QueryMessage {
            query_string: String::from_utf8_lossy(bulkstring.as_ref()).to_string(),
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }),
        Frame::Array(frames) => handle_redis_array_query(frames),
        Frame::Integer(integer) => Ok(QueryMessage {
            query_string: format!("{}", integer),
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }),
        Frame::Error(error) => Ok(QueryMessage {
            query_string: error,
            namespace: vec![],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::ReadWrite,
            ast: None,
        }),
        Frame::Null => Ok(QueryMessage::empty()),
    }
}

#[inline]
fn get_redis_frame(rf: RawFrame) -> Result<Frame> {
    if let RawFrame::Redis(frame) = rf {
        Ok(frame)
    } else {
        warn!("Unsupported Frame detected - Dropping Frame {:?}", rf);
        Err(anyhow!("Unsupported frame found, not sending"))
    }
}

impl RedisCodec {
    fn encode_message(&mut self, item: Message) -> Result<Frame> {
        let frame = if !item.modified {
            get_redis_frame(item.original)?
        } else {
            match item.details {
                MessageDetails::Query(qm) => RedisCodec::build_redis_query_frame(qm),
                MessageDetails::Response(qr) => RedisCodec::build_redis_response_frame(qr),
                MessageDetails::Unknown => get_redis_frame(item.original)?,
                MessageDetails::ReturnToSender => get_redis_frame(item.original)?,
            }
        };
        Ok(frame)
    }

    pub fn new(decode_type: DecodeType) -> RedisCodec {
        RedisCodec {
            decode_type,
            current_frames: vec![],
            enable_metadata: false,
        }
    }

    pub fn process_redis_bulk(&self, frames: Vec<Frame>) -> Result<Messages> {
        trace!("processing bulk response {:?}", frames);
        frames
            .into_iter()
            .map(|frame| {
                if self.enable_metadata {
                    Ok(Message {
                        details: match self.decode_type {
                            DecodeType::Response => {
                                MessageDetails::Response(process_redis_frame_response(&frame)?)
                            }
                            DecodeType::Query => {
                                MessageDetails::Query(process_redis_frame_query(&frame)?)
                            }
                        },
                        modified: false,
                        original: RawFrame::Redis(frame),
                    })
                } else {
                    Ok(Message {
                        details: MessageDetails::Unknown,
                        modified: false,
                        original: RawFrame::Redis(frame),
                    })
                }
            })
            .collect()
    }

    fn build_redis_response_frame(resp: QueryResponse) -> Frame {
        if let Some(result) = resp.result {
            return result.into();
        }
        if let Some(Value::Strings(s)) = resp.error {
            return Frame::Error(s);
        }

        debug!("{:?}", resp);
        Frame::SimpleString("OK".to_string())
    }

    fn build_redis_query_frame(query: QueryMessage) -> Frame {
        match query.ast {
            Some(ASTHolder::Commands(Value::List(ast))) => {
                Frame::Array(ast.into_iter().map(|v| v.into()).collect())
            }
            _ => Frame::SimpleString(query.query_string),
        }
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Vec<Frame>>> {
        while src.remaining() != 0 {
            trace!("remaining {}", src.remaining());

            match decode(&*src).map_err(|e| {
                info!("Error decoding redis frame {:?}", e);
                anyhow!("Error decoding redis frame {}", e)
            })? {
                Some((frame, size)) => {
                    trace!("Got frame {:?} of {}", frame, size);
                    src.advance(size);
                    self.current_frames.push(frame);
                }
                None => {
                    if src.remaining() == 0 {
                        break;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
        trace!(
            "frames {:?} - remaining {}",
            self.current_frames,
            src.remaining()
        );

        if self.current_frames.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(&mut self.current_frames)))
        }
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        encode_bytes(dst, &item)
            .map(|_| ())
            .map_err(|e| anyhow!("Redis encoding error: {} - {:#?}", e, item))
    }
}

impl Decoder for RedisCodec {
    type Item = Messages;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        Ok(match self.decode_raw(src)? {
            None => None,
            Some(f) => Some(self.process_redis_bulk(f)?),
        })
    }
}

impl Encoder<Messages> for RedisCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        item.into_iter().try_for_each(|m: Message| {
            let frame = self.encode_message(m)?;
            self.encode_raw(frame, dst)
        })
    }
}

#[cfg(test)]
mod redis_tests {
    use crate::protocols::redis_codec::{DecodeType, RedisCodec};
    use bytes::BytesMut;
    use hex_literal::hex;
    use tokio_util::codec::{Decoder, Encoder};

    const SET_MESSAGE: [u8; 45] = hex!("2a330d0a24330d0a5345540d0a2431360d0a6b65793a5f5f72616e645f696e745f5f0d0a24330d0a7878780d0a");

    const OK_MESSAGE: [u8; 5] = hex!("2b4f4b0d0a");

    const GET_MESSAGE: [u8; 36] =
        hex!("2a320d0a24330d0a4745540d0a2431360d0a6b65793a5f5f72616e645f696e745f5f0d0a");

    const INC_MESSAGE: [u8; 41] =
        hex!("2a320d0a24340d0a494e43520d0a2432300d0a636f756e7465723a5f5f72616e645f696e745f5f0d0a");

    const LPUSH_MESSAGE: [u8; 36] =
        hex!("2a330d0a24350d0a4c505553480d0a24360d0a6d796c6973740d0a24330d0a7878780d0a");

    const RPUSH_MESSAGE: [u8; 36] =
        hex!("2a330d0a24350d0a52505553480d0a24360d0a6d796c6973740d0a24330d0a7878780d0a");

    const LPOP_MESSAGE: [u8; 26] = hex!("2a320d0a24340d0a4c504f500d0a24360d0a6d796c6973740d0a");

    const SADD_MESSAGE: [u8; 52] = hex!("2a330d0a24340d0a534144440d0a24350d0a6d797365740d0a2432300d0a656c656d656e743a5f5f72616e645f696e745f5f0d0a");

    const HSET_MESSAGE: [u8; 75] = hex!("2a340d0a24340d0a485345540d0a2431380d0a6d797365743a5f5f72616e645f696e745f5f0d0a2432300d0a656c656d656e743a5f5f72616e645f696e745f5f0d0a24330d0a7878780d0a");

    fn test_frame(codec: &mut RedisCodec, raw_frame: &[u8]) {
        let message = codec
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        let mut dest = BytesMut::new();
        codec.encode(message, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    #[test]
    fn test_ok_codec() {
        let mut codec = RedisCodec::new(DecodeType::Response);
        test_frame(&mut codec, &OK_MESSAGE);
    }

    #[test]
    fn test_set_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &SET_MESSAGE);
    }

    #[test]
    fn test_get_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &GET_MESSAGE);
    }

    #[test]
    fn test_inc_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &INC_MESSAGE);
    }

    #[test]
    fn test_lpush_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &LPUSH_MESSAGE);
    }

    #[test]
    fn test_rpush_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &RPUSH_MESSAGE);
    }

    #[test]
    fn test_lpop_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &LPOP_MESSAGE);
    }

    #[test]
    fn test_sadd_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &SADD_MESSAGE);
    }

    #[test]
    fn test_hset_codec() {
        let mut codec = RedisCodec::new(DecodeType::Query);
        test_frame(&mut codec, &HSET_MESSAGE);
    }
}
