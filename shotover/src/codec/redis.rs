use std::sync::mpsc;
use std::time::Instant;

use super::{CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::{Frame, MessageType, ValkeyFrame};
use crate::message::{Encodable, Message, MessageId, Messages};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use metrics::Histogram;
use redis_protocol::resp2::decode::decode_bytes_mut;
use redis_protocol::resp2::encode::extend_encode;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone)]
pub struct ValkeyCodecBuilder {
    direction: Direction,
    message_latency: Histogram,
}

impl CodecBuilder for ValkeyCodecBuilder {
    type Decoder = ValkeyDecoder;
    type Encoder = ValkeyEncoder;

    fn new(direction: Direction, destination_name: String) -> Self {
        let message_latency = super::message_latency(direction, destination_name);
        Self {
            direction,
            message_latency,
        }
    }

    fn build(&self) -> (ValkeyDecoder, ValkeyEncoder) {
        let (tx, rx) = match self.direction {
            Direction::Source => (None, None),
            Direction::Sink => {
                let (tx, rx) = mpsc::channel();
                (Some(tx), Some(rx))
            }
        };
        (
            ValkeyDecoder::new(rx, self.direction),
            ValkeyEncoder::new(tx, self.direction, self.message_latency.clone()),
        )
    }

    fn protocol(&self) -> MessageType {
        MessageType::Valkey
    }
}

pub struct RequestInfo {
    ty: RequestType,
    id: MessageId,
}
pub enum RequestType {
    /// a pubsub subscribe
    Subscribe,
    /// a unsubscribe
    Unsubscribe,
    /// valkey reset
    Reset,
    /// Everything else
    Other,
}

pub struct ValkeyEncoder {
    // Some when Sink (because it sends requests)
    request_header_tx: Option<mpsc::Sender<RequestInfo>>,
    direction: Direction,
    message_latency: Histogram,
}

pub struct ValkeyDecoder {
    // Some when Sink (because it receives responses)
    request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
    direction: Direction,
    is_subscribed: bool,
}

impl ValkeyDecoder {
    pub fn new(
        request_header_rx: Option<mpsc::Receiver<RequestInfo>>,
        direction: Direction,
    ) -> Self {
        Self {
            direction,
            request_header_rx,
            is_subscribed: false,
        }
    }
}

impl Decoder for ValkeyDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let received_at = Instant::now();
        match decode_bytes_mut(src).map_err(|e| {
            CodecReadError::Parser(anyhow!(e).context("Error decoding valkey frame"))
        })? {
            Some((frame, _size, bytes)) => {
                tracing::debug!(
                    "{}: incoming valkey message:\n{}",
                    self.direction,
                    pretty_hex::pretty_hex(&bytes)
                );
                let mut message = Message::from_bytes_and_frame_at_instant(
                    bytes,
                    Frame::Valkey(frame),
                    Some(received_at),
                );

                // Notes on subscription responses
                //
                // There are 3 types of pubsub responses and the type is determined by the first value in the array:
                // * `subscribe` - a response to a SUBSCRIBE, PSUBSCRIBE or SSUBSCRIBE request
                // * `unsubscribe` - a response to an UNSUBSCRIBE, PUNSUBSCRIBE or SUNSUBSCRIBE request
                // * `message` - a subscription message
                //
                // Additionally valkey will:
                // * accept a few regular commands while in pubsub mode: PING, RESET and QUIT
                // * return an error response when a nonexistent or non pubsub compatible command is used
                //
                // Note: PING has a custom response when in pubsub mode.
                //       It returns an array ['pong', $pingMessage] instead of directly returning $pingMessage.
                //       But this doesnt cause any problems for us.

                // Determine if message is a `message` subscription message
                //
                // Because PING, RESET, QUIT and error responses never return a ValkeyFrame::Array starting with `message`,
                // they have no way to collide with the `message` value of a subscription message.
                // So while we are in subscription mode we can use that to determine if an
                // incoming message is a subscription message.
                let is_subscription_message = if self.is_subscribed {
                    if let Some(Frame::Valkey(ValkeyFrame::Array(array))) = message.frame() {
                        if let [ValkeyFrame::BulkString(ty), ..] = array.as_slice() {
                            ty.as_ref() == b"message"
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                // Update is_subscribed state
                //
                // In order to make sense of a response we need the main task to
                // send us the type of its corresponding request.
                //
                // In order to keep the incoming request MessageTypes in sync with their corresponding responses
                // we must only process a MessageType when the message is not a subscription message.
                // This is fine because subscription messages cannot affect the is_subscribed state.
                if !is_subscription_message {
                    if let Some(rx) = self.request_header_rx.as_ref() {
                        let request_info = rx.recv().map_err(|_| {
                            CodecReadError::Parser(anyhow!("valkey encoder half was lost"))
                        })?;
                        message.set_request_id(request_info.id);
                        match request_info.ty {
                            RequestType::Subscribe | RequestType::Unsubscribe => {
                                if let Some(Frame::Valkey(ValkeyFrame::Array(array))) =
                                    message.frame()
                                {
                                    if let Some(ValkeyFrame::Integer(
                                        number_of_subscribed_channels,
                                    )) = array.get(2)
                                    {
                                        self.is_subscribed = *number_of_subscribed_channels != 0;
                                    }
                                }
                            }
                            RequestType::Reset => {
                                self.is_subscribed = false;
                            }
                            RequestType::Other => {}
                        }
                    }
                }
                Ok(Some(vec![message]))
            }
            None => Ok(None),
        }
    }
}

impl ValkeyEncoder {
    pub fn new(
        request_header_tx: Option<mpsc::Sender<RequestInfo>>,
        direction: Direction,
        message_latency: Histogram,
    ) -> Self {
        Self {
            request_header_tx,
            direction,
            message_latency,
        }
    }
}

impl Encoder<Messages> for ValkeyEncoder {
    type Error = CodecWriteError;

    fn encode(&mut self, item: Messages, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.into_iter().try_for_each(|mut m| {
            let start = dst.len();
            m.ensure_message_type(MessageType::Valkey)
                .map_err(CodecWriteError::Encoder)?;
            let received_at = m.received_from_source_or_sink_at;
            if let Some(tx) = self.request_header_tx.as_ref() {
                let ty = if let Some(Frame::Valkey(ValkeyFrame::Array(array))) = m.frame() {
                    if let Some(ValkeyFrame::BulkString(bytes)) = array.first() {
                        match bytes.to_ascii_uppercase().as_slice() {
                            b"SUBSCRIBE" | b"PSUBSCRIBE" | b"SSUBSCRIBE" => RequestType::Subscribe,
                            b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" | b"SUNSUBSCRIBE" => {
                                RequestType::Unsubscribe
                            }
                            b"RESET" => RequestType::Reset,
                            _ => RequestType::Other,
                        }
                    } else {
                        RequestType::Other
                    }
                } else {
                    RequestType::Other
                };
                tx.send(RequestInfo { ty, id: m.id() })
                    .map_err(|e| CodecWriteError::Encoder(anyhow!(e)))?;
            }
            let result = match m.into_encodable() {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    let item = frame.into_valkey().unwrap();
                    extend_encode(dst, &item)
                        .map(|_| ())
                        .map_err(|e| anyhow!("Valkey encoding error: {} - {:#?}", e, item))
                }
            };
            if let Some(received_at) = received_at {
                self.message_latency.record(received_at.elapsed());
            }
            tracing::debug!(
                "{}: outgoing valkey message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );
            result.map_err(CodecWriteError::Encoder)
        })
    }
}

#[cfg(test)]
mod valkey_tests {

    use crate::codec::{redis::ValkeyCodecBuilder, CodecBuilder, Direction};
    use bytes::BytesMut;
    use hex_literal::hex;
    use pretty_assertions::assert_eq;
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
        let (mut decoder, mut encoder) =
            ValkeyCodecBuilder::new(Direction::Source, "valkey".to_owned()).build();
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
