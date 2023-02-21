use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::MessageType;
use crate::message::{Encodable, Message, Messages, ProtocolType};
use anyhow::Result;
use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Default)]
pub struct KafkaCodecBuilder {}

impl KafkaCodecBuilder {
    pub fn new() -> Self {
        Default::default()
    }
}

impl CodecBuilder for KafkaCodecBuilder {
    type Decoder = KafkaDecoder;
    type Encoder = KafkaEncoder;
    fn build(&self) -> (KafkaDecoder, KafkaEncoder) {
        (KafkaDecoder::new(), KafkaEncoder::new())
    }
}

#[derive(Default)]
pub struct KafkaDecoder {
    messages: Messages,
}

impl KafkaDecoder {
    pub fn new() -> Self {
        KafkaDecoder::default()
    }
}

fn get_length_of_full_message(src: &mut BytesMut) -> Option<usize> {
    if src.len() > 4 {
        let size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize + 4;
        if size >= src.len() {
            Some(size)
        } else {
            None
        }
    } else {
        None
    }
}

impl Decoder for KafkaDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        loop {
            if let Some(size) = get_length_of_full_message(src) {
                let bytes = src.split_to(size);
                tracing::debug!(
                    "incoming kafka message:\n{}",
                    pretty_hex::pretty_hex(&bytes)
                );
                self.messages
                    .push(Message::from_bytes(bytes.freeze(), ProtocolType::Kafka));
            } else if self.messages.is_empty() || src.remaining() != 0 {
                return Ok(None);
            } else {
                return Ok(Some(std::mem::take(&mut self.messages)));
            }
        }
    }
}

#[derive(Default)]
pub struct KafkaEncoder {}

impl KafkaEncoder {
    pub fn new() -> Self {
        KafkaEncoder::default()
    }
}

impl Encoder<Messages> for KafkaEncoder {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<()> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            let result = match m.into_encodable(MessageType::Kafka)? {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(_) => todo!("kafka frame is unimplemented"),
            };
            tracing::debug!(
                "outgoing kafka message:\n{}",
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result
        })
    }
}
