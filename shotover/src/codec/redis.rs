use super::{CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::{Frame, MessageType};
use crate::message::{Encodable, Message, Messages};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use redis_protocol::resp2::prelude::decode_mut;
use redis_protocol::resp2::prelude::encode_bytes;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone)]
pub struct RedisCodecBuilder {
    direction: Direction,
}

impl CodecBuilder for RedisCodecBuilder {
    type Decoder = RedisDecoder;
    type Encoder = RedisEncoder;

    fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn build(&self) -> (RedisDecoder, RedisEncoder) {
        (
            RedisDecoder::new(self.direction),
            RedisEncoder::new(self.direction),
        )
    }
}

pub struct RedisEncoder {
    direction: Direction,
}

pub struct RedisDecoder {
    messages: Messages,
    direction: Direction,
}

impl RedisDecoder {
    pub fn new(direction: Direction) -> Self {
        Self {
            messages: Vec::new(),
            direction,
        }
    }
}

impl Decoder for RedisDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match decode_mut(src).map_err(|e| {
                CodecReadError::Parser(anyhow!(e).context("Error decoding redis frame"))
            })? {
                Some((frame, _size, bytes)) => {
                    tracing::debug!(
                        "{}: incoming redis message:\n{}",
                        self.direction,
                        pretty_hex::pretty_hex(&bytes)
                    );
                    self.messages
                        .push(Message::from_bytes_and_frame(bytes, Frame::Redis(frame)));
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

impl RedisEncoder {
    pub fn new(direction: Direction) -> Self {
        Self { direction }
    }
}

impl Encoder<Messages> for RedisEncoder {
    type Error = CodecWriteError;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            let result = match m
                .into_encodable(MessageType::Redis)
                .map_err(CodecWriteError::Encoder)?
            {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    let item = frame.into_redis().unwrap();
                    encode_bytes(dst, &item)
                        .map(|_| ())
                        .map_err(|e| anyhow!("Redis encoding error: {} - {:#?}", e, item))
                }
            };
            tracing::debug!(
                "{}: outgoing redis message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result.map_err(CodecWriteError::Encoder)
        })
    }
}

#[cfg(test)]
mod redis_tests {
    use crate::codec::{redis::RedisCodecBuilder, CodecBuilder, Direction};
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

    fn test_frame(raw_frame: &[u8]) {
        let (mut decoder, mut encoder) = RedisCodecBuilder::new(Direction::Sink).build();
        let message = decoder
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        let mut dest = BytesMut::new();
        encoder.encode(message, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    #[test]
    fn test_ok_codec() {
        test_frame(&OK_MESSAGE);
    }

    #[test]
    fn test_set_codec() {
        test_frame(&SET_MESSAGE);
    }

    #[test]
    fn test_get_codec() {
        test_frame(&GET_MESSAGE);
    }

    #[test]
    fn test_inc_codec() {
        test_frame(&INC_MESSAGE);
    }

    #[test]
    fn test_lpush_codec() {
        test_frame(&LPUSH_MESSAGE);
    }

    #[test]
    fn test_rpush_codec() {
        test_frame(&RPUSH_MESSAGE);
    }

    #[test]
    fn test_lpop_codec() {
        test_frame(&LPOP_MESSAGE);
    }

    #[test]
    fn test_sadd_codec() {
        test_frame(&SADD_MESSAGE);
    }

    #[test]
    fn test_hset_codec() {
        test_frame(&HSET_MESSAGE);
    }
}
