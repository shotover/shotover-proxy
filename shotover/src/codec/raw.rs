use super::{CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::MessageType;
use crate::message::{Encodable, Message, Messages, ProtocolType};
use bytes::Buf;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

#[derive(Clone)]
pub struct RawCodecBuilder {
    direction: Direction,
}

impl CodecBuilder for RawCodecBuilder {
    type Decoder = RawDecoder;
    type Encoder = RawEncoder;

    fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn build(&self) -> (Self::Decoder, Self::Encoder) {
        (
            Self::Decoder::new(self.direction),
            Self::Encoder::new(self.direction),
        )
    }

    fn websocket_subprotocol(&self) -> &'static str {
        ""
    }
}

pub struct RawDecoder {
    direction: Direction,
}

impl RawDecoder {
    fn new(direction: Direction) -> Self {
        Self { direction }
    }
}

impl Decoder for RawDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        let length = src.len();
        if length != 0 {
            let data = src[..length].to_vec();
            src.advance(length);
            tracing::debug!(
                "{}: incoming raw message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&data)
            );

            Ok(Some(vec![Message::from_bytes(
                data.into(),
                ProtocolType::Raw,
            )]))
        } else {
            Ok(None)
        }
    }
}

pub struct RawEncoder {
    direction: Direction,
}

impl RawEncoder {
    fn new(direction: Direction) -> Self {
        Self { direction }
    }
}

impl Encoder<Messages> for RawEncoder {
    type Error = CodecWriteError;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            m.ensure_message_type(MessageType::Raw)
                .map_err(CodecWriteError::Encoder)?;
            let result = match m.into_encodable() {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    let item = frame.into_raw().unwrap();
                    dst.extend_from_slice(&item);
                    Ok(())
                }
            };
            tracing::debug!(
                "{}: outgoing raw message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result.map_err(CodecWriteError::Encoder)
        })
    }
}
