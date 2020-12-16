use crate::message::{
    ASTHolder, Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
};
use crate::protocols::RawFrame;
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;
use redis_protocol::prelude::*;
use std::collections::HashMap;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, info, trace, warn};

#[derive(Debug, Clone)]
pub struct RedisCodec {
    // Redis doesn't have an explicit "Response" type as part of the protocol
    decode_as_response: bool,
    batch_hint: usize,
    current_frames: Vec<Frame>,
}

fn get_keys(
    fields: &mut HashMap<String, Value>,
    keys: &mut HashMap<String, Value>,
    commands: &mut Vec<Frame>,
) -> Result<()> {
    let mut keys_storage: Vec<Value> = vec![];
    while !commands.is_empty() {
        if let Some(Frame::BulkString(v)) = commands.pop() {
            let key = String::from_utf8(v.clone())?;
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
        let key = String::from_utf8(v.clone())?;
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
        let key = String::from_utf8(v.clone())?;
        keys_storage.push(Frame::BulkString(v).into());

        let mut values: HashMap<String, Value> = HashMap::new();
        while !commands.is_empty() {
            if let Some(Frame::BulkString(field)) = commands.pop() {
                if let Some(frame) = commands.pop() {
                    values.insert(String::from_utf8(field)?, frame.into());
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
            let key = String::from_utf8(k.clone())?;
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
        }
    }

    pub fn get_batch_hint(&self) -> usize {
        self.batch_hint
    }

    pub fn build_redis_response_frame(resp: QueryResponse) -> Frame {
        if let Some(result) = &resp.result {
            return result.clone().into();
        }
        if let Some(result) = &resp.error {
            if let Value::Strings(s) = result {
                return Frame::Error(s.clone());
            }
        }

        debug!("{:?}", resp);
        Frame::SimpleString("OK".to_string())
    }

    pub fn build_redis_query_frame(query: QueryMessage) -> Frame {
        if let Some(ASTHolder::Commands(Value::List(ast))) = &query.ast {
            let commands: Vec<Frame> = ast.iter().cloned().map(|v| v.into()).collect_vec();
            return Frame::Array(commands);
        }
        Frame::SimpleString(query.query_string)
    }

    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Vec<Frame>>> {
        // TODO: get_batch_hint may be a premature optimisation
        while src.remaining() != 0 {
            trace!("remaining {}", src.remaining());

            match decode_bytes(&*src).map_err(|e| {
                info!("Error decoding redis frame {:?}", e);
                anyhow!("Error decoding redis frame {}", e)
            })? {
                (Some(frame), size) => {
                    trace!("Got frame {:?}", frame);
                    src.advance(size);
                    self.current_frames.push(frame);
                }
                (None, _) => {
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

        if !return_buf.is_empty() {
            trace!("Batch size {:?}", return_buf.len());
            return Ok(Some(return_buf));
        }

        Ok(None)
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        encode_bytes(dst, &item)
            .map(|_| ())
            .map_err(|e| anyhow!("Uh - oh {} - {:#?}", e, item))
    }
}

pub fn process_redis_bulk(mut frames: Vec<Frame>) -> Result<Messages> {
    trace!("processing bulk response {:?}", frames);
    Ok(Messages {
        messages: frames
            .into_iter()
            .map(|f| Message {
                details: MessageDetails::Unknown,
                modified: false,
                original: RawFrame::Redis(f),
            })
            .collect(),
    })
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
            Some(f) => Some(process_redis_bulk(f)?),
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
        // .collect()
    }
}

#[cfg(test)]
mod redis_tests {
    use crate::protocols::redis_codec::RedisCodec;
    use bytes::BytesMut;
    use hex_literal::hex;
    use rdkafka::message::ToBytes;
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

    fn build_bytesmut(slice: &[u8]) -> BytesMut {
        let mut v: Vec<u8> = Vec::with_capacity(slice.len());
        v.extend_from_slice(slice);
        BytesMut::from(v.to_bytes())
    }

    fn test_frame(codec: &mut RedisCodec, raw_frame: &[u8]) {
        let mut bytes: BytesMut = build_bytesmut(raw_frame);
        if let Ok(Some(message)) = codec.decode(&mut bytes) {
            let mut dest: BytesMut = BytesMut::new();
            if let Ok(()) = codec.encode(message, &mut dest) {
                assert_eq!(build_bytesmut(raw_frame), dest)
            }
        } else {
            panic!("Could not decode frame");
        }
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
