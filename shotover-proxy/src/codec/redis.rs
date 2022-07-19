use crate::frame::RedisFrame;
use crate::frame::{Frame, MessageType};
use crate::message::{Encodable, Message, Messages, QueryType};
use anyhow::{anyhow, Error, Result};
use bytes::{Buf, BytesMut};
use redis_protocol::resp2::prelude::decode_mut;
use redis_protocol::resp2::prelude::encode_bytes;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone)]
pub enum RedisDirection {
    Source,
    Sink,
}

#[derive(Debug, Clone)]
pub struct RedisCodec {
    messages: Messages,
    direction: RedisDirection,
}

#[inline]
pub fn redis_query_type(frame: &RedisFrame) -> QueryType {
    if let RedisFrame::Array(frames) = frame {
        if let Some(RedisFrame::BulkString(bytes)) = frames.get(0) {
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

impl RedisCodec {
    pub fn new(direction: RedisDirection) -> Self {
        RedisCodec {
            messages: vec![],
            direction,
        }
    }
}

impl Decoder for RedisCodec {
    type Item = Messages;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            match decode_mut(src).map_err(|e| anyhow!(e).context("Error decoding redis frame"))? {
                Some((frame, _size, bytes)) => {
                    tracing::debug!(
                        "incoming redis message:\n{}",
                        pretty_hex::pretty_hex(&bytes)
                    );
                    let mut message = Message::from_bytes_and_frame(bytes, Frame::Redis(frame));
                    if let RedisDirection::Source = self.direction {
                        if let Some(frame) = message.frame() {
                            validate_command(frame)?;
                        } else {
                            return Err(anyhow!("redis frame could not be parsed"));
                        }
                    }
                    self.messages.push(message);
                }
                None => {
                    if self.messages.is_empty() || src.remaining() != 0 {
                        return Ok(None);
                    } else {
                        return Ok(Some(std::mem::take(&mut self.messages)));
                    }
                }
            }
        }
    }
}

/// It is critical that shotover only send valid redis commands.
/// An invalid command could result in the redis instance returning multiple responses breaking shotovers transform invariants.
fn validate_command(frame: &Frame) -> Result<()> {
    match frame {
        Frame::Redis(frame) => match frame {
            RedisFrame::Array(array) => {
                for value in array {
                    match value {
                        RedisFrame::BulkString(_) => {} // valid case
                        _ => return Err(anyhow!("Redis command must be an array of bulk strings but one of the values was not a bulk string {frame:?}")),
                    }
                }
                Ok(())
            }
            frame => Err(anyhow!("Redis command must be an array but was {frame:?}")),
        },
        _ => unreachable!("Message from a redis codec will always be a redis message"),
    }
}

impl Encoder<Messages> for RedisCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<()> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            let result = match m.into_encodable(MessageType::Redis)? {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    if let RedisDirection::Sink = self.direction {
                        validate_command(&frame)?;
                    }
                    let item = frame.into_redis().unwrap();
                    encode_bytes(dst, &item)
                        .map(|_| ())
                        .map_err(|e| anyhow!("Redis encoding error: {} - {:#?}", e, item))
                }
            };
            tracing::debug!(
                "outgoing redis message:\n{}",
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result
        })
    }
}

#[cfg(test)]
mod redis_tests {
    use crate::codec::redis::{RedisCodec, RedisDirection};
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
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &OK_MESSAGE);
    }

    #[test]
    fn test_set_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &SET_MESSAGE);
    }

    #[test]
    fn test_get_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &GET_MESSAGE);
    }

    #[test]
    fn test_inc_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &INC_MESSAGE);
    }

    #[test]
    fn test_lpush_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &LPUSH_MESSAGE);
    }

    #[test]
    fn test_rpush_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &RPUSH_MESSAGE);
    }

    #[test]
    fn test_lpop_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &LPOP_MESSAGE);
    }

    #[test]
    fn test_sadd_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &SADD_MESSAGE);
    }

    #[test]
    fn test_hset_codec() {
        let mut codec = RedisCodec::new(RedisDirection::Sink);
        test_frame(&mut codec, &HSET_MESSAGE);
    }
}
