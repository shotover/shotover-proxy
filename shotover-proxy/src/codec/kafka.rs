use crate::codec::{Codec, CodecReadError};
use crate::frame::MessageType;
use crate::message::{Encodable, Message, Messages, ProtocolType};
use anyhow::Result;
use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone)]
pub struct KafkaCodec {
    messages: Messages,
}

impl Default for KafkaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec for KafkaCodec {}

impl KafkaCodec {
    pub fn new() -> KafkaCodec {
        KafkaCodec { messages: vec![] }
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

impl Decoder for KafkaCodec {
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

impl Encoder<Messages> for KafkaCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<()> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            // TODO: MessageType::Kafka
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
