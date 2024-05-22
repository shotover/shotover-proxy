use super::{message_latency, CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError, CodecState};
use crate::frame::MessageType;
use crate::message::{Encodable, Message, MessageId, Messages};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use kafka_protocol::messages::ApiKey;
use metrics::Histogram;
use std::sync::mpsc;
use std::time::Instant;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub version: i16,
}

#[derive(Clone)]
pub struct KafkaCodecBuilder {
    direction: Direction,
    message_latency: Histogram,
}

// Depending on if the codec is used in a sink or a source requires different processing logic:
// * Sources parse requests which do not require any special handling
// * Sinks parse responses which requires first matching up the version and api_key with its corresponding request
//     + To achieve this Sinks use an mpsc channel to send header data from the encoder to the decoder
impl CodecBuilder for KafkaCodecBuilder {
    type Decoder = KafkaDecoder;
    type Encoder = KafkaEncoder;

    fn new(direction: Direction, destination_name: String) -> Self {
        let message_latency = message_latency(direction, destination_name);
        Self {
            direction,
            message_latency,
        }
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
            KafkaEncoder::new(tx, self.direction, self.message_latency.clone()),
        )
    }

    fn protocol(&self) -> MessageType {
        MessageType::Kafka
    }
}

pub struct RequestInfo {
    header: RequestHeader,
    id: MessageId,
}

pub struct KafkaDecoder {
    // Some when Sink (because it receives responses)
    request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
    direction: Direction,
}

impl KafkaDecoder {
    pub fn new(
        request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
        direction: Direction,
    ) -> Self {
        KafkaDecoder {
            request_header_rx,
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
        let received_at = Instant::now();
        if let Some(size) = get_length_of_full_message(src) {
            let bytes = src.split_to(size);
            tracing::debug!(
                "{}: incoming kafka message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&bytes)
            );
            let message = if let Some(rx) = self.request_header_rx.as_ref() {
                let RequestInfo { header, id } = rx
                    .recv()
                    .map_err(|_| CodecReadError::Parser(anyhow!("kafka encoder half was lost")))?;
                let mut message = Message::from_bytes_at_instant(
                    bytes.freeze(),
                    CodecState::Kafka {
                        request_header: Some(header),
                    },
                    Some(received_at),
                );
                message.set_request_id(id);
                message
            } else {
                Message::from_bytes_at_instant(
                    bytes.freeze(),
                    CodecState::Kafka {
                        request_header: None,
                    },
                    Some(received_at),
                )
            };
            Ok(Some(vec![message]))
        } else {
            Ok(None)
        }
    }
}

pub struct KafkaEncoder {
    message_latency: Histogram,
    // Some when Sink (because it sends requests)
    request_header_tx: Option<mpsc::Sender<RequestInfo>>,
    direction: Direction,
}

impl KafkaEncoder {
    pub fn new(
        request_header_tx: Option<mpsc::Sender<RequestInfo>>,
        direction: Direction,
        message_latency: Histogram,
    ) -> Self {
        KafkaEncoder {
            message_latency,
            request_header_tx,
            direction,
        }
    }
}

impl Encoder<Messages> for KafkaEncoder {
    type Error = CodecWriteError;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.into_iter().try_for_each(|mut m| {
            let start = dst.len();
            m.ensure_message_type(MessageType::Kafka)
                .map_err(CodecWriteError::Encoder)?;
            let response_is_dummy = m.response_is_dummy();
            let id = m.id();
            let received_at = m.received_from_source_or_sink_at;
            let result = match m.into_encodable() {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => frame.into_kafka().unwrap().encode(dst),
            };

            // Skip if the message wrote nothing to dst, possibly due to being a dummy message.
            // or if it will generate a dummy response
            if !dst[start..].is_empty() && !response_is_dummy {
                if let Some(tx) = self.request_header_tx.as_ref() {
                    let api_key = i16::from_be_bytes(dst[start + 4..start + 6].try_into().unwrap());
                    let version = i16::from_be_bytes(dst[start + 6..start + 8].try_into().unwrap());
                    let api_key = ApiKey::try_from(api_key).map_err(|_| {
                        CodecWriteError::Encoder(anyhow!("unknown api key {api_key}"))
                    })?;
                    tx.send(RequestInfo {
                        header: RequestHeader { api_key, version },
                        id,
                    })
                    .map_err(|e| CodecWriteError::Encoder(anyhow!(e)))?;
                }
            }

            if let Some(received_at) = received_at {
                self.message_latency.record(received_at.elapsed());
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
