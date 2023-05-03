use super::{CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::MessageType;
use crate::message::{Encodable, Message, Messages, ProtocolType};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use kafka_protocol::messages::ApiKey;
use std::sync::mpsc;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub version: i16,
}

#[derive(Clone)]
pub struct KafkaCodecBuilder {
    direction: Direction,
}

// Depending on if the codec is used in a sink or a source requires different processing logic:
// * Sources parse requests which do not require any special handling
// * Sinks parse responses which requires first matching up the version and api_key with its corresponding request
//     + To achieve this Sinks use an mpsc channel to send header data from the encoder to the decoder
impl CodecBuilder for KafkaCodecBuilder {
    type Decoder = KafkaDecoder;
    type Encoder = KafkaEncoder;

    fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn build(&self) -> (KafkaDecoder, KafkaEncoder) {
        let (tx, rx) = match self.direction {
            Direction::Source => (None, None),
            Direction::Sink => {
                let (tx, rx) = mpsc::channel();
                (Some(tx), Some(rx))
            }
        };
        (
            KafkaDecoder::new(rx, self.direction),
            KafkaEncoder::new(tx, self.direction),
        )
    }
}

pub struct KafkaDecoder {
    request_header_rx: Option<mpsc::Receiver<RequestHeader>>,
    messages: Messages,
    direction: Direction,
}

impl KafkaDecoder {
    pub fn new(
        request_header_rx: Option<mpsc::Receiver<RequestHeader>>,
        direction: Direction,
    ) -> Self {
        KafkaDecoder {
            request_header_rx,
            messages: vec![],
            direction,
        }
    }
}

fn get_length_of_full_message(src: &BytesMut) -> Option<usize> {
    if src.len() > 4 {
        let size = u32::from_be_bytes(src[0..4].try_into().unwrap()) as usize + 4;
        if size <= src.len() {
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

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if let Some(size) = get_length_of_full_message(src) {
                let bytes = src.split_to(size);
                tracing::debug!(
                    "{}: incoming kafka message:\n{}",
                    self.direction,
                    pretty_hex::pretty_hex(&bytes)
                );
                let request_header = if let Some(rx) = self.request_header_rx.as_ref() {
                    Some(rx.recv().map_err(|_| {
                        CodecReadError::Parser(anyhow!("kafka encoder half was lost"))
                    })?)
                } else {
                    None
                };
                self.messages.push(Message::from_bytes(
                    bytes.freeze(),
                    ProtocolType::Kafka { request_header },
                ));
            } else if self.messages.is_empty() || src.remaining() != 0 {
                return Ok(None);
            } else {
                return Ok(Some(std::mem::take(&mut self.messages)));
            }
        }
    }
}

pub struct KafkaEncoder {
    request_header_tx: Option<mpsc::Sender<RequestHeader>>,
    direction: Direction,
}

impl KafkaEncoder {
    pub fn new(
        request_header_tx: Option<mpsc::Sender<RequestHeader>>,
        direction: Direction,
    ) -> Self {
        KafkaEncoder {
            request_header_tx,
            direction,
        }
    }
}

impl Encoder<Messages> for KafkaEncoder {
    type Error = CodecWriteError;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            let result = match m
                .into_encodable(MessageType::Kafka)
                .map_err(CodecWriteError::Encoder)?
            {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => frame.into_kafka().unwrap().encode(dst),
            };

            if let Some(tx) = self.request_header_tx.as_ref() {
                let api_key = i16::from_be_bytes(dst[start + 4..start + 6].try_into().unwrap());
                let version = i16::from_be_bytes(dst[start + 6..start + 8].try_into().unwrap());
                let api_key = ApiKey::try_from(api_key)
                    .map_err(|_| CodecWriteError::Encoder(anyhow!("unknown api key {api_key}")))?;
                tx.send(RequestHeader { api_key, version })
                    .map_err(|e| CodecWriteError::Encoder(anyhow!(e)))?;
            }
            tracing::debug!(
                "{}: outgoing kafka message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result.map_err(CodecWriteError::Encoder)
        })
    }
}
