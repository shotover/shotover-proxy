//! Message and supporting types - used to hold a message/query/result going between the client and database

use crate::codec::kafka::RequestHeader;
use crate::codec::CodecState;
use crate::frame::cassandra::Tracing;
use crate::frame::redis::redis_query_type;
use crate::frame::OpenSearchFrame;
use crate::frame::{
    cassandra,
    cassandra::{CassandraMetadata, CassandraOperation},
};
use crate::frame::{CassandraFrame, Frame, MessageType, RedisFrame};
use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use nonzero_ext::nonzero;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

pub enum Metadata {
    Cassandra(CassandraMetadata),
    Redis,
    Kafka,
    OpenSearch,
}

#[derive(PartialEq)]
pub enum ProtocolType {
    Cassandra {
        compression: Compression,
    },
    Redis,
    Kafka {
        request_header: Option<RequestHeader>,
    },
    OpenSearch,
}

impl From<&ProtocolType> for CodecState {
    fn from(value: &ProtocolType) -> Self {
        match value {
            ProtocolType::Cassandra { compression } => Self::Cassandra {
                compression: *compression,
            },
            ProtocolType::Redis => Self::Redis,
            ProtocolType::Kafka { request_header } => Self::Kafka {
                request_header: *request_header,
            },
            ProtocolType::OpenSearch => Self::OpenSearch,
        }
    }
}

pub type Messages = Vec<Message>;

/// Message holds a single message/query/result going between the client and database.
/// It is designed to efficiently abstract over the message being in various states of processing.
///
/// Usually a message is received and starts off containing just raw bytes (or possibly raw bytes + frame)
/// This can be immediately sent off to the destination without any processing cost.
///
/// However if a transform wants to query the contents of the message it must call [`Message::frame`] which will cause the raw bytes to be processed into a raw bytes + Frame.
/// The first call to frame has an expensive one time cost.
///
/// The transform may also go one step further and modify the message's Frame + call [`Message::invalidate_cache`].
/// This results in an expensive cost to reassemble the message bytes when the message is sent to the destination.
#[derive(PartialEq, Debug, Clone)]
pub struct Message {
    /// It is an invariant that this field must remain Some at all times.
    /// The only reason it is an Option is to allow temporarily taking ownership of the value from an &mut T
    inner: Option<MessageInner>,

    // TODO: Not a fan of this field and we could get rid of it by making TimestampTagger an implicit part of TuneableConsistencyScatter
    // This metadata field is only used for communication between transforms and should not be touched by sinks or sources
    pub(crate) meta_timestamp: Option<i64>,

    pub(crate) codec_state: CodecState,
}

// `from_*` methods for `Message`
impl Message {
    /// This method should be called when you have have just the raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol where the length of the message is provided in the header. e.g. cassandra
    /// Providing just the bytes results in better performance when only the raw bytes are available.
    pub fn from_bytes(bytes: Bytes, protocol_type: ProtocolType) -> Self {
        Message {
            inner: Some(MessageInner::RawBytes {
                bytes,
                message_type: MessageType::from(&protocol_type),
            }),
            meta_timestamp: None,
            codec_state: CodecState::from(&protocol_type),
        }
    }

    /// This method should be called when you have both a Frame and matching raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol that does not include length of the message in the header. e.g. redis
    /// Providing both the raw bytes and Frame results in better performance if they are both already available.
    pub fn from_bytes_and_frame(bytes: Bytes, frame: Frame) -> Self {
        Message {
            codec_state: frame.as_codec_state(),
            inner: Some(MessageInner::Parsed { bytes, frame }),
            meta_timestamp: None,
        }
    }

    /// This method should be called when you have just a Frame of a message.
    /// This is expected to be used by transforms that are generating custom messages.
    /// Providing just the Frame results in better performance when only the Frame is available.
    pub fn from_frame(frame: Frame) -> Self {
        Message {
            codec_state: frame.as_codec_state(),
            inner: Some(MessageInner::Modified { frame }),
            meta_timestamp: None,
        }
    }
}

// Methods for interacting with `Message::inner`
impl Message {
    /// Returns a `&mut Frame` which contains the processed contents of the message.
    /// A transform may choose to modify the contents of the `&mut Frame` in order to modify the message that is sent to the DB.
    /// Any future calls to `frame()` in the same or future transforms will return the same modified `&mut Frame`.
    /// If a transform chooses to modify the `&mut Frame` then they must also call `Frame::invalidate_cache()` after the modification.
    ///
    /// Returns `None` when fails to parse the message.
    /// This failure to parse the message is internally logged as an error.
    ///
    /// ## Performance implications
    /// Calling frame for the first time on a message may be an expensive operation as the raw bytes might not yet be parsed into a Frame.
    /// Calling frame again is free as the parsed message is cached.
    pub fn frame(&mut self) -> Option<&mut Frame> {
        let (inner, result) = self.inner.take().unwrap().ensure_parsed(self.codec_state);
        self.inner = Some(inner);
        if let Err(err) = result {
            // TODO: If we could include a stacktrace in this error it would be really helpful
            tracing::error!("{:?}", err.context("Failed to parse frame"));
            return None;
        }

        match self.inner.as_mut().unwrap() {
            MessageInner::RawBytes { .. } => {
                unreachable!("Cannot be RawBytes because ensure_parsed was called")
            }
            MessageInner::Parsed { frame, .. } => Some(frame),
            MessageInner::Modified { frame } => Some(frame),
        }
    }

    pub fn ensure_message_type(&self, expected_message_type: MessageType) -> Result<()> {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes { message_type, .. } => {
                if *message_type == expected_message_type || *message_type == MessageType::Dummy {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        message_type
                    ))
                }
            }
            MessageInner::Parsed { frame, .. } => {
                let message_type = frame.get_type();
                if message_type == expected_message_type || message_type == MessageType::Dummy {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        frame.name()
                    ))
                }
            }
            MessageInner::Modified { frame } => {
                let message_type = frame.get_type();
                if message_type == expected_message_type || message_type == MessageType::Dummy {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Expected message of type {:?} but was of type {:?}",
                        expected_message_type,
                        frame.name()
                    ))
                }
            }
        }
    }

    pub fn into_encodable(self) -> Encodable {
        match self.inner.unwrap() {
            MessageInner::RawBytes { bytes, .. } => Encodable::Bytes(bytes),
            MessageInner::Parsed { bytes, .. } => Encodable::Bytes(bytes),
            MessageInner::Modified {
                frame: Frame::Dummy,
            } => Encodable::Bytes(Bytes::new()),
            MessageInner::Modified { frame } => Encodable::Frame(frame),
        }
    }

    /// Batch messages have a cell count of 1 cell per inner message.
    /// Cell count is determined as follows:
    /// * Regular message - 1 cell
    /// * Message containing submessages e.g. a batch request - 1 cell per submessage
    /// * Message containing submessages with 0 submessages - 1 cell
    pub fn cell_count(&self) -> Result<NonZeroU32> {
        Ok(match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match message_type {
                MessageType::Redis => nonzero!(1u32),
                MessageType::Cassandra => cassandra::raw_frame::cell_count(bytes)?,
                MessageType::Kafka => todo!(),
                MessageType::Dummy => nonzero!(1u32),
                MessageType::OpenSearch => todo!(),
            },
            MessageInner::Modified { frame } | MessageInner::Parsed { frame, .. } => match frame {
                Frame::Cassandra(frame) => frame.cell_count()?,
                Frame::Redis(_) => nonzero!(1u32),
                Frame::Kafka(_) => todo!(),
                Frame::Dummy => nonzero!(1u32),
                Frame::OpenSearch(_) => todo!(),
            },
        })
    }

    /// Invalidates all internal caches.
    /// This must be called after any modifications to the return value of `Message::frame()`.
    /// Otherwise values returned by getter methods and the message sent to the DB will be outdated.
    ///
    /// ## Performance implications
    /// * Clears caches used by getter methods
    /// * If `Message::frame()` has been called the message bytes must be regenerated from the `Frame` when sent to the DB
    pub fn invalidate_cache(&mut self) {
        // TODO: clear message details cache fields if we ever add any

        self.inner = self.inner.take().map(|x| x.invalidate_cache());
    }

    pub fn get_query_type(&mut self) -> QueryType {
        match self.frame() {
            Some(Frame::Cassandra(cassandra)) => cassandra.get_query_type(),
            Some(Frame::Redis(redis)) => redis_query_type(redis), // free-standing function as we cant define methods on RedisFrame
            Some(Frame::Kafka(_)) => todo!(),
            Some(Frame::Dummy) => todo!(),
            Some(Frame::OpenSearch(os)) => os.get_query_type(),
            None => QueryType::ReadWrite,
        }
    }

    /// Returns an error response with the provided error message.
    /// If self is a request: the returned `Message` is a valid response to self
    /// If self is a response: the returned `Message` is a valid replacement of self
    pub fn to_error_response(&self, error: String) -> Result<Message> {
        Ok(Message::from_frame(match self.metadata().context("Failed to parse metadata of request or response when producing an error")? {
            Metadata::Redis => {
                // Redis errors can not contain newlines at the protocol level
                let message = format!("ERR {error}")
                    .replace("\r\n", " ")
                    .replace('\n', " ");
                Frame::Redis(RedisFrame::Error(message.into()))
            }
            Metadata::Cassandra(frame) => Frame::Cassandra(CassandraFrame {
                version: frame.version,
                stream_id: frame.stream_id,
                operation: CassandraOperation::Error(ErrorBody {
                    message: error,
                    ty: ErrorType::Server,
                }),
                tracing: Tracing::Response(None),
                warnings: vec![],
            }),
            // In theory we could actually support kafka errors in some form here but:
            // * kafka errors are defined per response type and many response types only provide an error code without a field for a custom error message.
            //     + Implementing this per response type would add a lot of (localized) complexity but might be worth it.
            // * the official C++ kafka driver we use for integration tests does not pick up errors sent just before closing a connection, so this wouldnt help the usage in server.rs where we send an error before terminating the connection for at least that driver.
            Metadata::Kafka => return Err(anyhow!(error).context(
                "A generic error cannot be formed because the kafka protocol does not support it",
            )),
            Metadata::OpenSearch => Frame::OpenSearch(OpenSearchFrame::new_server_error_response()),
        }))
    }

    /// Get metadata for this `Message`
    pub fn metadata(&self) -> Result<Metadata> {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match message_type {
                MessageType::Cassandra => {
                    Ok(Metadata::Cassandra(cassandra::raw_frame::metadata(bytes)?))
                }
                MessageType::Redis => Ok(Metadata::Redis),
                MessageType::Kafka => Ok(Metadata::Kafka),
                MessageType::Dummy => Err(anyhow!("Dummy has no metadata")),
                MessageType::OpenSearch => Ok(Metadata::OpenSearch),
            },
            MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame } => match frame {
                Frame::Cassandra(frame) => Ok(Metadata::Cassandra(frame.metadata())),
                Frame::Kafka(_) => Ok(Metadata::Kafka),
                Frame::Redis(_) => Ok(Metadata::Redis),
                Frame::Dummy => Err(anyhow!("dummy has no metadata")),
                Frame::OpenSearch(_) => Ok(Metadata::OpenSearch),
            },
        }
    }

    /// Set this `Message` to a backpressure response
    pub fn set_backpressure(&mut self) -> Result<()> {
        let metadata = self.metadata()?;

        *self = Message::from_frame(match metadata {
            Metadata::Cassandra(metadata) => {
                let body = CassandraOperation::Error(ErrorBody {
                    message: "Server overloaded".into(),
                    ty: ErrorType::Overloaded,
                });

                Frame::Cassandra(CassandraFrame {
                    version: metadata.version,
                    stream_id: metadata.stream_id,
                    tracing: Tracing::Response(None),
                    warnings: vec![],
                    operation: body,
                })
            }
            Metadata::Redis => unimplemented!(),
            Metadata::Kafka => unimplemented!(),
            Metadata::OpenSearch => unimplemented!(),
        });

        Ok(())
    }

    // Retrieves the stream_id without parsing the rest of the frame.
    // Used for ordering out of order messages without parsing their contents.
    // TODO: We will have a better idea of how to make this generic once we have multiple out of order protocols
    //       For now its just written to match cassandra's stream_id field
    pub fn stream_id(&self) -> Option<i16> {
        match &self.inner {
            Some(MessageInner::RawBytes {
                bytes,
                message_type: MessageType::Cassandra,
            }) => {
                const HEADER_LEN: usize = 9;
                if bytes.len() >= HEADER_LEN {
                    Some((&bytes[2..4]).get_i16())
                } else {
                    None
                }
            }
            Some(MessageInner::RawBytes { .. }) => None,
            Some(MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame }) => {
                match frame {
                    Frame::Cassandra(cassandra) => Some(cassandra.stream_id),
                    Frame::Redis(_) => None,
                    Frame::Kafka(_) => None,
                    Frame::Dummy => None,
                    Frame::OpenSearch(_) => None,
                }
            }
            None => None,
        }
    }

    pub fn to_high_level_string(&mut self) -> String {
        if let Some(response) = self.frame() {
            format!("{}", response)
        } else if let Some(MessageInner::RawBytes {
            bytes,
            message_type,
        }) = &self.inner
        {
            format!("Unparseable {:?} message {:?}", message_type, bytes)
        } else {
            unreachable!("self.frame() failed so MessageInner must still be RawBytes")
        }
    }
}

/// There are 3 levels of processing the message can be in.
/// RawBytes -> Parsed -> Modified
/// Where possible transforms should avoid moving to further stages to improve performance but this is an implementation detail hidden from them
#[derive(PartialEq, Debug, Clone)]
enum MessageInner {
    RawBytes {
        bytes: Bytes,
        message_type: MessageType,
    },
    Parsed {
        bytes: Bytes,
        frame: Frame,
    },
    Modified {
        frame: Frame,
    },
}

impl MessageInner {
    fn ensure_parsed(self, codec_state: CodecState) -> (Self, Result<()>) {
        match self {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match Frame::from_bytes(bytes.clone(), message_type, codec_state) {
                Ok(frame) => (MessageInner::Parsed { bytes, frame }, Ok(())),
                Err(err) => (
                    MessageInner::RawBytes {
                        bytes,
                        message_type,
                    },
                    Err(err),
                ),
            },
            MessageInner::Parsed { .. } => (self, Ok(())),
            MessageInner::Modified { .. } => (self, Ok(())),
        }
    }

    fn invalidate_cache(self) -> Self {
        match self {
            MessageInner::RawBytes { .. } => self,
            MessageInner::Parsed { frame, .. } => MessageInner::Modified { frame },
            MessageInner::Modified { .. } => self,
        }
    }
}

#[derive(Debug)]
pub enum Encodable {
    /// The raw bytes the protocol should send
    Bytes(Bytes),
    /// The Frame that should be processed into bytes and then sent
    Frame(Frame),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum QueryType {
    Read,
    Write,
    ReadWrite,
    SchemaChange,
    PubSubMessage,
}
