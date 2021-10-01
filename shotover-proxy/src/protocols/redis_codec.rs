use crate::message::{
    ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
};
use crate::protocols::RawFrame;
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use redis_protocol::resp2::prelude::*;
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace, warn};

#[derive(Debug, Clone)]
pub struct RedisCodec {
    // Redis doesn't have an explicit "Response" type as part of the protocol
    decode_as_response: bool,
    batch_hint: usize,
    current_frames: Vec<Frame>,
    enable_metadata: bool,
}

fn get_keys(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !commands.is_empty() {
        if let Some(Frame::BulkString(v)) = commands.pop() {
            let key = String::from_utf8(v.to_vec())?;
            fields.insert(key.clone(), Value::None);
            keys_storage.push(Frame::BulkString(v).into());
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
}

fn get_key_multi_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    if let Some(Frame::BulkString(v)) = commands.pop() {
        let key = String::from_utf8(v.to_vec())?;
        keys_storage.push(Frame::BulkString(v).into());

        let mut values: Vec<Value> = vec![];
        while !commands.is_empty() {
            if let Some(frame) = commands.pop() {
                values.push(frame.into());
            }
        }
        fields.insert(key, Value::List(values));
        keys.insert("key".to_string(), Value::List(keys_storage));
    }
    Ok(())
}

fn get_key_map(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    if let Some(Frame::BulkString(v)) = commands.pop() {
        let key = String::from_utf8(v.to_vec())?;
        keys_storage.push(Frame::BulkString(v).into());

        let mut values: HashMap<String, Value> = HashMap::new();
        while !commands.is_empty() {
            if let Some(Frame::BulkString(field)) = commands.pop() {
                if let Some(frame) = commands.pop() {
                    values.insert(String::from_utf8(field.to_vec())?, frame.into());
                }
            }
        }
        fields.insert(key, Value::Document(values));
        keys.insert("key".to_string(), Value::List(keys_storage));
    }
    Ok(())
}

fn get_key_values(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !commands.is_empty() {
        if let Some(Frame::BulkString(k)) = commands.pop() {
            let key = String::from_utf8(k.to_vec())?;
            keys_storage.push(Frame::BulkString(k).into());
            if let Some(frame) = commands.pop() {
                fields.insert(key, frame.into());
            }
        }
    }
    keys.insert("key".to_string(), Value::List(keys_storage));
    Ok(())
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
                MessageDetails::Bypass(message) => self.encode_message(Message {
                    details: *message,
                    modified: item.modified,
                    original: item.original,
                })?,
                MessageDetails::Query(qm) => RedisCodec::build_redis_query_frame(qm),
                MessageDetails::Response(qr) => RedisCodec::build_redis_response_frame(qr),
                MessageDetails::Unknown => get_redis_frame(item.original)?,
            }
        };
        Ok(frame)
    }

    pub fn new(decode_as_response: bool, batch_hint: usize) -> RedisCodec {
        RedisCodec {
            decode_as_response,
            batch_hint,
            current_frames: vec![],
            enable_metadata: false,
        }
    }

    pub fn get_batch_hint(&self) -> usize {
        self.batch_hint
    }

    fn handle_redis_array(&self, commands_vec: Vec<Frame>, frame: Frame) -> Result<Message> {
        if !self.decode_as_response {
            let mut keys_map: HashMap<String, Value> = HashMap::new();
            let mut values_map: HashMap<String, Value> = HashMap::new();
            let values = &mut values_map;
            let keys = &mut keys_map;
            let mut query_type: QueryType = QueryType::Write;
            let mut commands_reversed: Vec<Frame> =
                commands_vec.iter().cloned().rev().collect_vec();
            let query_string = commands_vec
                .iter()
                .filter_map(|f| f.as_str())
                .map(|s| s.to_string())
                .collect_vec()
                .join(" ");

            let ast = ASTHolder::Commands(Value::List(
                commands_vec.iter().cloned().map(|f| f.into()).collect_vec(),
            ));

            let commands = &mut commands_reversed;

            // This should be a command from the server
            // Behaviour cribbed from:
            // https://redis.io/commands and
            // https://gist.github.com/LeCoupa/1596b8f359ad8812c7271b5322c30946
            if let Some(Frame::BulkString(v)) = commands.pop() {
                let comm = String::from_utf8(v.to_vec())
                    .unwrap_or_else(|_| "invalid utf-8".to_string())
                    .to_uppercase();
                match comm.as_str() {
                    "APPEND" => {
                        get_key_values(values, keys, commands)?;
                    } // append a value to a key
                    "BITCOUNT" => {
                        query_type = QueryType::Read;
                        get_key_values(values, keys, commands)?;
                    } // count set bits in a string
                    "SET" => {
                        get_key_values(values, keys, commands)?;
                    } // set value in key
                    "SETNX" => {
                        get_key_values(values, keys, commands)?;
                    } // set if not exist value in key
                    "SETRANGE" => {
                        get_key_values(values, keys, commands)?;
                    } // overwrite part of a string at key starting at the specified offset
                    "STRLEN" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the length of the value stored in a key
                    "MSET" => {
                        get_key_values(values, keys, commands)?;
                    } // set multiple keys to multiple values
                    "MSETNX" => {
                        get_key_values(values, keys, commands)?;
                    } // set multiple keys to multiple values, only if none of the keys exist
                    "GET" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get value in key
                    "GETRANGE" => {
                        query_type = QueryType::Read;
                        get_key_values(values, keys, commands)?;
                    } // get a substring value of a key and return its old value
                    "MGET" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the values of all the given keys
                    "INCR" => {
                        get_keys(values, keys, commands)?;
                    } // increment value in key
                    "INCRBY" => {
                        get_key_values(values, keys, commands)?;
                    } // increment the integer value of a key by the given amount
                    "INCRBYFLOAT" => {
                        get_key_values(values, keys, commands)?;
                    } // increment the float value of a key by the given amount
                    "DECR" => {
                        get_keys(values, keys, commands)?;
                    } // decrement the integer value of key by one
                    "DECRBY" => {
                        get_key_values(values, keys, commands)?;
                    } // decrement the integer value of a key by the given number
                    "DEL" => {
                        get_keys(values, keys, commands)?;
                    } // delete key
                    "EXPIRE" => {
                        get_key_values(values, keys, commands)?;
                    } // key will be deleted in 120 seconds
                    "TTL" => {
                        get_keys(values, keys, commands)?;
                    } // returns the number of seconds until a key is deleted
                    "RPUSH" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // put the new value at the end of the list
                    "RPUSHX" => {
                        get_key_values(values, keys, commands)?;
                    } // append a value to a list, only if the exists
                    "LPUSH" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // put the new value at the start of the list
                    "LRANGE" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // give a subset of the list
                    "LINDEX" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // get an element from a list by its index
                    "LINSERT" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // insert an element before or after another element in a list
                    "LLEN" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // return the current length of the list
                    "LPOP" => {
                        get_keys(values, keys, commands)?;
                    } // remove the first element from the list and returns it
                    "LSET" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // set the value of an element in a list by its index
                    "LTRIM" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // trim a list to the specified range
                    "RPOP" => {
                        get_keys(values, keys, commands)?;
                    } // remove the last element from the list and returns it
                    "SADD" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // add the given value to the set
                    "SCARD" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the number of members in a set
                    "SREM" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // remove the given value from the set
                    "SISMEMBER" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // test if the given value is in the set.
                    "SMEMBERS" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // return a list of all the members of this set
                    "SUNION" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // combine two or more sets and returns the list of all elements
                    "SINTER" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // intersect multiple sets
                    "SMOVE" => {
                        query_type = QueryType::Write;
                        get_key_values(values, keys, commands)?;
                    } // move a member from one set to another
                    "SPOP" => {
                        query_type = QueryType::Write;
                        get_key_values(values, keys, commands)?;
                    } // remove and return one or multiple random members from a set
                    "ZADD" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // add one or more members to a sorted set, or update its score if it already exists
                    "ZCARD" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the number of members in a sorted set
                    "ZCOUNT" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // count the members in a sorted set with scores within the given values
                    "ZINCRBY" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // increment the score of a member in a sorted set
                    "ZRANGE" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // returns a subset of the sorted set
                    "ZRANK" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // determine the index of a member in a sorted set
                    "ZREM" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // remove one or more members from a sorted set
                    "ZREMRANGEBYRANK" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // remove all members in a sorted set within the given indexes
                    "ZREMRANGEBYSCORE" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // remove all members in a sorted set, by index, with scores ordered from high to low
                    "ZSCORE" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the score associated with the given mmeber in a sorted set
                    "ZRANGEBYSCORE" => {
                        query_type = QueryType::Read;
                        get_key_multi_values(values, keys, commands)?;
                    } // return a range of members in a sorted set, by score
                    "HGET" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get the value of a hash field
                    "HGETALL" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get all the fields and values in a hash
                    "HSET" => {
                        get_key_map(values, keys, commands)?;
                    } // set the string value of a hash field
                    "HSETNX" => {
                        get_key_map(values, keys, commands)?;
                    } // set the string value of a hash field, only if the field does not exists
                    "HMSET" => {
                        get_key_map(values, keys, commands)?;
                    } // set multiple fields at once
                    "HINCRBY" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // increment value in hash by X
                    "HDEL" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // delete one or more hash fields
                    "HEXISTS" => {
                        query_type = QueryType::Read;
                        get_key_values(values, keys, commands)?;
                    } // determine if a hash field exists
                    "HKEYS" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get all the fields in a hash
                    "HLEN" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get all the fields in a hash
                    "HSTRLEN" => {
                        query_type = QueryType::Read;
                        get_key_values(values, keys, commands)?;
                    } // get the length of the value of a hash field
                    "HVALS" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // get all the values in a hash
                    "PFADD" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // add the specified elements to the specified HyperLogLog
                    "PFCOUNT" => {
                        query_type = QueryType::Read;
                        get_keys(values, keys, commands)?;
                    } // return the approximated cardinality of the set(s) observed by the HyperLogLog at key's)
                    "PFMERGE" => {
                        get_key_multi_values(values, keys, commands)?;
                    } // merge N HyperLogLogs into a single one
                    _ => {}
                }
                Ok(Message::new_query(
                    QueryMessage {
                        query_string,
                        namespace: vec![],
                        primary_key: keys_map,
                        query_values: Some(values_map),
                        projection: None,
                        query_type,
                        ast: Some(ast),
                    },
                    false,
                    RawFrame::Redis(frame),
                ))
            } else {
                Ok(Message::new_bypass(RawFrame::Redis(frame)))
            }
        } else {
            Ok(Message::new_response(
                QueryResponse {
                    matching_query: None,
                    result: Some(Value::List(
                        commands_vec.iter().map(|f| f.into()).collect_vec(),
                    )),
                    error: None,
                    response_meta: None,
                },
                false,
                RawFrame::Redis(frame),
            ))
        }
    }

    fn handle_redis_string(&self, string: String, frame: Frame) -> Message {
        if self.decode_as_response {
            Message::new_response(
                QueryResponse {
                    matching_query: None,
                    result: Some(Value::Strings(string)),
                    error: None,
                    response_meta: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        } else {
            Message::new_query(
                QueryMessage {
                    query_string: string,
                    namespace: vec![],
                    primary_key: Default::default(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Read,
                    ast: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        }
    }

    fn handle_redis_bulkstring(&self, bulkstring: Bytes, frame: Frame) -> Message {
        if self.decode_as_response {
            Message::new_response(
                QueryResponse {
                    matching_query: None,
                    result: Some(Value::Bytes(bulkstring)),
                    error: None,
                    response_meta: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        } else {
            Message::new_query(
                QueryMessage {
                    query_string: String::from_utf8_lossy(bulkstring.as_ref()).to_string(),
                    namespace: vec![],
                    primary_key: Default::default(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Read,
                    ast: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        }
    }

    fn handle_redis_integer(&self, integer: i64, frame: Frame) -> Message {
        if self.decode_as_response {
            Message::new_response(
                QueryResponse {
                    matching_query: None,
                    result: Some(Value::Integer(integer)),
                    error: None,
                    response_meta: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        } else {
            Message::new_query(
                QueryMessage {
                    query_string: format!("{}", integer),
                    namespace: vec![],
                    primary_key: Default::default(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Read,
                    ast: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        }
    }

    fn handle_redis_error(&self, error: String, frame: Frame) -> Message {
        if self.decode_as_response {
            Message::new_response(
                QueryResponse {
                    matching_query: None,
                    result: None,
                    error: Some(Value::Strings(error)),
                    response_meta: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        } else {
            Message::new_query(
                QueryMessage {
                    query_string: error,
                    namespace: vec![],
                    primary_key: Default::default(),
                    query_values: None,
                    projection: None,
                    query_type: QueryType::Read,
                    ast: None,
                },
                false,
                RawFrame::Redis(frame),
            )
        }
    }

    pub fn process_redis_bulk(&self, frames: Vec<Frame>) -> Result<Messages> {
        trace!("processing bulk response {:?}", frames);
        let result: Result<Messages> = frames
            .into_iter()
            .map(|f| self.process_redis_frame(f))
            .collect();
        result
    }

    pub fn process_redis_frame(&self, frame: Frame) -> Result<Message> {
        if !self.enable_metadata {
            Ok(Message {
                details: MessageDetails::Unknown,
                modified: false,
                original: RawFrame::Redis(frame),
            })
        } else {
            Ok(match frame.clone() {
                Frame::SimpleString(s) => self.handle_redis_string(s, frame),
                Frame::BulkString(bs) => self.handle_redis_bulkstring(bs, frame),
                Frame::Array(frames) => self.handle_redis_array(frames, frame)?,
                Frame::Integer(i) => self.handle_redis_integer(i, frame),
                Frame::Error(s) => self.handle_redis_error(s, frame),
                Frame::Null => {
                    if self.decode_as_response {
                        Message::new_response(QueryResponse::empty(), false, RawFrame::Redis(frame))
                    } else {
                        Message::new_query(QueryMessage::empty(), false, RawFrame::Redis(frame))
                    }
                }
            })
        }
    }

    pub fn build_redis_response_frame(resp: QueryResponse) -> Frame {
        if let Some(result) = &resp.result {
            return result.clone().into();
        }
        if let Some(Value::Strings(s)) = &resp.error {
            return Frame::Error(s.clone());
        }

        debug!("{:?}", resp);
        Frame::SimpleString("OK".to_string())
    }

    pub fn build_redis_query_frame(query: QueryMessage) -> Frame {
        if let Some(ASTHolder::Commands(Value::List(ast))) = &query.ast {
            Frame::Array(ast.iter().cloned().map(|v| v.into()).collect())
        } else {
            Frame::SimpleString(query.query_string)
        }
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Vec<Frame>>> {
        // TODO: get_batch_hint may be a premature optimisation
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
        let mut return_buf: Vec<Frame> = vec![];
        std::mem::swap(&mut self.current_frames, &mut return_buf);

        if return_buf.is_empty() {
            Ok(None)
        } else {
            trace!("Batch size {:?}", return_buf.len());
            Ok(Some(return_buf))
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
    use crate::protocols::redis_codec::RedisCodec;
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
        let mut codec = RedisCodec::new(true, 1);
        test_frame(&mut codec, &OK_MESSAGE);
    }

    #[test]
    fn test_set_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &SET_MESSAGE);
    }

    #[test]
    fn test_get_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &GET_MESSAGE);
    }

    #[test]
    fn test_inc_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &INC_MESSAGE);
    }

    #[test]
    fn test_lpush_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &LPUSH_MESSAGE);
    }

    #[test]
    fn test_rpush_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &RPUSH_MESSAGE);
    }

    #[test]
    fn test_lpop_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &LPOP_MESSAGE);
    }

    #[test]
    fn test_sadd_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &SADD_MESSAGE);
    }

    #[test]
    fn test_hset_codec() {
        let mut codec = RedisCodec::new(false, 1);
        test_frame(&mut codec, &HSET_MESSAGE);
    }
}
