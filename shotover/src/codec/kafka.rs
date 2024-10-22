use super::{message_latency, CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError, CodecState, KafkaCodecState};
use crate::frame::kafka::KafkaFrame;
use crate::frame::{Frame, MessageType};
use crate::message::{Encodable, Message, MessageId, Messages};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use kafka_protocol::messages::{
    ApiKey, RequestHeader as RequestHeaderProtocol, RequestKind, ResponseHeader, ResponseKind,
    SaslAuthenticateRequest, SaslAuthenticateResponse,
};
use metrics::Histogram;
use std::sync::mpsc;
use std::time::Instant;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct RequestHeader {
    // TODO: this should be i16???
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

#[derive(Debug)]
pub struct RequestInfo {
    header: RequestHeader,
    id: MessageId,
    expect_raw_sasl: Option<SaslType>,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum SaslType {
    Plain,
    ScramMessage1,
    ScramMessage2,
}

pub struct KafkaDecoder {
    // Some when Sink (because it receives responses)
    request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
    direction: Direction,
    expect_raw_sasl: Option<SaslType>,
}

impl KafkaDecoder {
    pub fn new(
        request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
        direction: Direction,
    ) -> Self {
        KafkaDecoder {
            request_header_rx,
            direction,
            expect_raw_sasl: None,
        }
    }
}

fn get_length_of_full_message(src: &BytesMut) -> Option<usize> {
    if src.len() >= 4 {
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

            struct Meta {
                request_header: RequestHeader,
                message_id: Option<u128>,
            }

            let request_info = self
                .request_header_rx
                .as_ref()
                .map(|rx| {
                    rx.recv()
                        .map_err(|_| CodecReadError::Parser(anyhow!("kafka encoder half was lost")))
                })
                .transpose()?;

            let message = if self.expect_raw_sasl.is_some() {
                // Convert the unframed raw sasl into a framed sasl
                // This allows transforms to correctly parse the message and inspect the sasl request
                let kafka_frame = match self.direction {
                    Direction::Source => KafkaFrame::Request {
                        header: RequestHeaderProtocol::default()
                            .with_request_api_key(ApiKey::SaslAuthenticateKey as i16),
                        body: RequestKind::SaslAuthenticate(
                            SaslAuthenticateRequest::default().with_auth_bytes(bytes.freeze()),
                        ),
                    },
                    Direction::Sink => KafkaFrame::Response {
                        version: 0,
                        header: ResponseHeader::default(),
                        body: ResponseKind::SaslAuthenticate(
                            SaslAuthenticateResponse::default().with_auth_bytes(bytes.freeze()),
                            // TODO: we need to set with_error_code
                        ),
                    },
                };
                let codec_state = CodecState::Kafka(KafkaCodecState {
                    request_header: None,
                    raw_sasl: self.expect_raw_sasl,
                });
                self.expect_raw_sasl = match self.expect_raw_sasl {
                    Some(SaslType::Plain) => None,
                    Some(SaslType::ScramMessage1) => Some(SaslType::ScramMessage2),
                    Some(SaslType::ScramMessage2) => None,
                    None => None,
                };
                Message::from_frame_and_codec_state_at_instant(
                    Frame::Kafka(kafka_frame),
                    codec_state,
                    Some(received_at),
                )
            } else {
                let meta = if let Some(RequestInfo {
                    header,
                    id,
                    expect_raw_sasl,
                }) = request_info
                {
                    if let Some(expect_raw_sasl) = expect_raw_sasl {
                        self.expect_raw_sasl = Some(expect_raw_sasl);
                    }
                    Meta {
                        request_header: header,
                        message_id: Some(id),
                    }
                } else {
                    Meta {
                        request_header: RequestHeader {
                            api_key: ApiKey::try_from(i16::from_be_bytes(
                                bytes[4..6].try_into().unwrap(),
                            ))
                            .unwrap(),
                            version: i16::from_be_bytes(bytes[6..8].try_into().unwrap()),
                        },
                        message_id: None,
                    }
                };
                let mut message = if let Some(id) = meta.message_id.as_ref() {
                    let mut message = Message::from_bytes_at_instant(
                        bytes.freeze(),
                        CodecState::Kafka(KafkaCodecState {
                            request_header: Some(meta.request_header),
                            raw_sasl: None,
                        }),
                        Some(received_at),
                    );
                    message.set_request_id(*id);
                    message
                } else {
                    Message::from_bytes_at_instant(
                        bytes.freeze(),
                        CodecState::Kafka(KafkaCodecState {
                            request_header: None,
                            raw_sasl: None,
                        }),
                        Some(received_at),
                    )
                };

                if meta.request_header.api_key == ApiKey::SaslHandshakeKey
                    && meta.request_header.version == 0
                {
                    // Only parse the full frame once we manually check its a v0 sasl handshake
                    if let Some(Frame::Kafka(KafkaFrame::Request {
                        body: RequestKind::SaslHandshake(sasl_handshake),
                        ..
                    })) = message.frame()
                    {
                        self.expect_raw_sasl = Some(match sasl_handshake.mechanism.as_str() {
                            "PLAIN" => SaslType::Plain,
                            "SCRAM-SHA-512" => SaslType::ScramMessage1,
                            "SCRAM-SHA-256" => SaslType::ScramMessage1,
                            mechanism => {
                                return Err(CodecReadError::Parser(anyhow!(
                                    "Unknown sasl mechanism {mechanism}"
                                )))
                            }
                        });

                        // Clear raw bytes of the message to force the encoder to encode from frame.
                        // This is needed because the encoder only has access to the frame if it does not have any raw bytes,
                        // and the encoder needs to inspect the frame to set its own sasl state.
                        message.invalidate_cache();
                    }
                }
                message
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
            let codec_state = m.codec_state.as_kafka();
            let mut expect_raw_sasl = None;
            let result = match m.into_encodable() {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    if codec_state.raw_sasl.is_some() {
                        match frame {
                            Frame::Kafka(KafkaFrame::Request {
                                body: RequestKind::SaslAuthenticate(body),
                                ..
                            }) => {
                                dst.extend_from_slice(&body.auth_bytes);
                            }
                            Frame::Kafka(KafkaFrame::Response {
                                body: ResponseKind::SaslAuthenticate(body),
                                ..
                            }) => {
                                dst.extend_from_slice(&body.auth_bytes);
                            }
                            _ => unreachable!("not expected {frame:?}"),
                        }
                        Ok(())
                    } else {
                        let frame = frame.into_kafka().unwrap();
                        // it is garanteed that all v0 SaslHandshakes will be in a parsed state since we parse it in the KafkaDecoder.
                        if let KafkaFrame::Request {
                            body: RequestKind::SaslHandshake(sasl_handshake),
                            header,
                        } = &frame
                        {
                            if header.request_api_version == 0 {
                                expect_raw_sasl = Some(match sasl_handshake.mechanism.as_str() {
                                    "PLAIN" => SaslType::Plain,
                                    "SCRAM-SHA-512" => SaslType::ScramMessage1,
                                    "SCRAM-SHA-256" => SaslType::ScramMessage1,
                                    mechanism => {
                                        return Err(CodecWriteError::Encoder(anyhow!(
                                            "Unknown sasl mechanism {mechanism}"
                                        )))
                                    }
                                });
                            }
                        }
                        frame.encode(dst)
                    }
                }
            };

            // Skip if the message wrote nothing to dst, possibly due to being a dummy message.
            // or if it will generate a dummy response
            if !dst[start..].is_empty() && !response_is_dummy {
                if let Some(tx) = self.request_header_tx.as_ref() {
                    let header = if codec_state.raw_sasl.is_some() {
                        RequestHeader {
                            api_key: ApiKey::SaslAuthenticateKey,
                            version: 0,
                        }
                    } else {
                        let api_key =
                            i16::from_be_bytes(dst[start + 4..start + 6].try_into().unwrap());
                        let version =
                            i16::from_be_bytes(dst[start + 6..start + 8].try_into().unwrap());
                        // TODO: handle unknown API key
                        let api_key = ApiKey::try_from(api_key).map_err(|_| {
                            CodecWriteError::Encoder(anyhow!("unknown api key {api_key}"))
                        })?;

                        RequestHeader { api_key, version }
                    };

                    let request_info = RequestInfo {
                        header,
                        id,
                        expect_raw_sasl,
                    };
                    tx.send(request_info)
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
