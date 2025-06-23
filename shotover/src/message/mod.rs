//! Message and supporting types - used to hold a message/query/result going between the client and database

use crate::codec::CodecState;
use crate::frame::{Frame, MessageType};
#[cfg(feature = "valkey")]
use crate::frame::{ValkeyFrame, valkey::valkey_query_type};
#[cfg(feature = "cassandra")]
use crate::frame::{cassandra, cassandra::CassandraMetadata};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use derivative::Derivative;
use fnv::FnvBuildHasher;
use nonzero_ext::nonzero;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::time::Instant;

pub type MessageIdMap<T> = HashMap<MessageId, T, FnvBuildHasher>;
pub type MessageIdSet = HashSet<MessageId, FnvBuildHasher>;

pub enum Metadata {
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraMetadata),
    #[cfg(feature = "valkey")]
    Valkey,
    #[cfg(feature = "kafka")]
    Kafka,
    #[cfg(feature = "opensearch")]
    OpenSearch,
}

impl Metadata {
    /// Returns an error response with the provided error message.
    /// If the metadata is from a request: the returned `Message` is a valid response to self
    /// If the metadata is from a response: the returned `Message` is a valid replacement of self
    pub fn to_error_response(&self, error: String) -> Result<Message> {
        #[allow(unreachable_code)]
        Ok(Message::from_frame(match self {
            #[cfg(feature = "valkey")]
            Metadata::Valkey => {
                // Valkey errors can not contain newlines at the protocol level
                let message = format!("ERR {error}")
                    .replace("\r\n", " ")
                    .replace('\n', " ");
                Frame::Valkey(ValkeyFrame::Error(message.into()))
            }
            #[cfg(feature = "cassandra")]
            Metadata::Cassandra(meta) => Frame::Cassandra(meta.to_error_response(error)),
            // In theory we could actually support kafka errors in some form here but:
            // * kafka errors are defined per response type and many response types only provide an error code without a field for a custom error message.
            //     + Implementing this per response type would add a lot of (localized) complexity but might be worth it.
            // * the official C++ kafka driver we use for integration tests does not pick up errors sent just before closing a connection, so this wouldnt help the usage in server.rs where we send an error before terminating the connection for at least that driver.
            #[cfg(feature = "kafka")]
            Metadata::Kafka => return Err(anyhow!(error).context(
                "A generic error cannot be formed because the kafka protocol does not support it",
            )),
            #[cfg(feature = "opensearch")]
            Metadata::OpenSearch => unimplemented!(),
        }))
    }
}

pub type Messages = Vec<Message>;

/// Unique identifier for the message assigned by shotover at creation time.
pub type MessageId = u128;

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
#[derive(Derivative, Debug, Clone)]
#[derivative(PartialEq)]
pub struct Message {
    /// It is an invariant that this field must remain Some at all times.
    /// The only reason it is an Option is to allow temporarily taking ownership of the value from an &mut T
    inner: Option<MessageInner>,

    /// The instant the bytes were read off the TCP connection at a source or sink.
    /// This field is used to measure the time it takes for a message to go from one end of the chain to the other.
    ///
    /// In order to keep this metric as accurate as possible transforms should follow the following rules:
    /// * When a transform clones a request to go down a seperate subchain this field should be duplicated into each clone.
    /// * When a transform splits a message into multiple messages the last message in the resulting sequence should retain this field and the rest should be set to `None`.
    /// * When generating a message that does not correspond to an internal message, for example to query database topology, set this field to `None`.
    /// * When a response is generated from a request, for example to return an error message to the client, set this field to `None`.
    #[derivative(PartialEq = "ignore")]
    pub(crate) received_from_source_or_sink_at: Option<Instant>,
    pub(crate) codec_state: CodecState,

    // TODO: Consider removing the "ignore" down the line, we we need it for now for compatibility with logic using the old style "in order protocol" assumption.
    #[derivative(PartialEq = "ignore")]
    pub(crate) id: MessageId,
    #[derivative(PartialEq = "ignore")]
    pub(crate) request_id: Option<MessageId>,
}

// `from_*` methods for `Message`
impl Message {
    /// This method should be called when you have have just the raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol where the length of the message is provided in the header. e.g. cassandra
    /// Providing just the bytes results in better performance when only the raw bytes are available.
    pub fn from_bytes_at_instant(
        bytes: Bytes,
        codec_state: CodecState,
        received_from_source_or_sink_at: Option<Instant>,
    ) -> Self {
        Message {
            inner: Some(MessageInner::RawBytes {
                bytes,
                message_type: MessageType::from(&codec_state),
            }),
            codec_state,
            received_from_source_or_sink_at,
            id: rand::random(),
            request_id: None,
        }
    }

    /// This method should be called when you have both a Frame and matching raw bytes of a message.
    /// This is expected to be used only by codecs that are decoding a protocol that does not include length of the message in the header. e.g. valkey
    /// Providing both the raw bytes and Frame results in better performance if they are both already available.
    pub fn from_bytes_and_frame_at_instant(
        bytes: Bytes,
        frame: Frame,
        received_from_source_or_sink_at: Option<Instant>,
    ) -> Self {
        Message {
            codec_state: frame.as_codec_state(),
            inner: Some(MessageInner::Parsed { bytes, frame: Box:: new(frame), }),
            received_from_source_or_sink_at,
            id: rand::random(),
            request_id: None,
        }
    }

    /// This method should be called when you have just a Frame of a message.
    /// This is expected to be used by transforms that are generating custom messages.
    /// Providing just the Frame results in better performance when only the Frame is available.
    pub fn from_frame_at_instant(
        frame: Frame,
        received_from_source_or_sink_at: Option<Instant>,
    ) -> Self {
        Message {
            codec_state: frame.as_codec_state(),
            inner: Some(MessageInner::Modified { frame: Box::new(frame), }),
            received_from_source_or_sink_at,
            id: rand::random(),
            request_id: None,
        }
    }

    /// This method should be called when generating a new request travelling down a seperate chain to an original request.
    /// The generated request will share the same MessageId as the message it is diverged from.
    pub fn from_frame_diverged(frame: Frame, diverged_from: &Message) -> Self {
        Message {
            codec_state: frame.as_codec_state(),
            inner: Some(MessageInner::Modified { frame: Box::new(frame), }),
            received_from_source_or_sink_at: diverged_from.received_from_source_or_sink_at,
            id: diverged_from.id(),
            request_id: None,
        }
    }

    /// Same as [`Message::from_bytes`] but `received_from_source_or_sink_at` is set to None.
    pub fn from_bytes(bytes: Bytes, codec_state: CodecState) -> Self {
        Self::from_bytes_at_instant(bytes, codec_state, None)
    }

    /// Same as [`Message::from_frame`] but `received_from_source_or_sink_at` is set to None.
    pub fn from_frame(frame: Frame) -> Self {
        Self::from_frame_at_instant(frame, None)
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

    /// Same as [`Message::frame`] but consumes the message and returns an owned [`Frame`]
    /// It is useful when the transform generates a request and consumes the response without the involvement of the client.
    pub fn into_frame(mut self) -> Option<Box<Frame>> {
        let (inner, result) = self.inner.take().unwrap().ensure_parsed(self.codec_state);
        if let Err(err) = result {
            // TODO: If we could include a stacktrace in this error it would be really helpful
            tracing::error!("{:?}", err.context("Failed to parse frame"));
            return None;
        }

        match inner {
            MessageInner::RawBytes { .. } => {
                unreachable!("Cannot be RawBytes because ensure_parsed was called")
            }
            MessageInner::Parsed { frame, .. } => Some(frame),
            MessageInner::Modified { frame } => Some(frame),
        }
    }

    /// Return the shotover assigned MessageId
    pub fn id(&self) -> MessageId {
        self.id
    }

    /// Return the MessageId of the request that resulted in this message
    /// Returns None when:
    /// * The message is a request
    /// * The message is a response but was not created in response to a request. e.g. Cassandra events and valkey pubsub
    pub fn request_id(&self) -> Option<MessageId> {
        self.request_id
    }

    pub fn set_request_id(&mut self, request_id: MessageId) {
        self.request_id = Some(request_id);
    }

    pub fn clone_with_new_id(&self) -> Self {
        Message {
            inner: self.inner.clone(),
            received_from_source_or_sink_at: None,
            codec_state: self.codec_state,
            id: rand::random(),
            request_id: self.request_id,
        }
    }

    pub fn message_type(&self) -> MessageType {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes { message_type, .. } => *message_type,
            MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame } => {
                frame.get_type()
            }
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
            MessageInner::Modified {frame} => match *frame{
                Frame::Dummy => Encodable::Bytes(Bytes::new()),
                ref inner_frame => Encodable::Frame(Box::new(inner_frame.clone())),
            // MessageInner::Modified{frame } => Encodable::Frame(Box::new(inner_frame.clone())),
            }
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
                #[cfg(feature = "cassandra")]
                bytes,
                message_type,
                ..
            } => match message_type {
                #[cfg(feature = "valkey")]
                MessageType::Valkey => nonzero!(1u32),
                #[cfg(feature = "cassandra")]
                MessageType::Cassandra => cassandra::raw_frame::cell_count(bytes)?,
                #[cfg(feature = "kafka")]
                MessageType::Kafka => todo!(),
                MessageType::Dummy => nonzero!(1u32),
                #[cfg(feature = "opensearch")]
                MessageType::OpenSearch => todo!(),
            },
            MessageInner::Modified { frame } | MessageInner::Parsed { frame, .. } => match frame.as_ref() {

                #[cfg(feature = "cassandra")]
                Frame::Cassandra(frame) => frame.cell_count()?,

                #[cfg(feature = "valkey")]
                Frame::Valkey(_) => nonzero!(1u32),

                #[cfg(feature = "kafka")]
                Frame::Kafka(_) => todo!(),
                Frame::Dummy => nonzero!(1u32),

                #[cfg(feature = "opensearch")]
                Frame::OpenSearch(_) => todo!(),
            },
        })
    }

    /// Invalidates all internal caches.
    /// This must be called after any modifications to the return value of `Message::frame()`.
    /// Otherwise values returned by getter methods and the message sent to the DB will be outdated.
    ///
    /// An error will be logged if this method is called without first making a call to `Message::frame()` that returns Some(_).
    /// This is because it is a noop in that case and likely a mistake.
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
            #[cfg(feature = "cassandra")]
            Some(Frame::Cassandra(cassandra)) => cassandra.get_query_type(),
            #[cfg(feature = "valkey")]
            Some(Frame::Valkey(valkey)) => valkey_query_type(valkey), // free-standing function as we cant define methods on ValkeyFrame
            #[cfg(feature = "kafka")]
            Some(Frame::Kafka(_)) => todo!(),
            Some(Frame::Dummy) => todo!(),
            #[cfg(feature = "opensearch")]
            Some(Frame::OpenSearch(_)) => todo!(),
            None => QueryType::ReadWrite,
        }
    }

    /// Returns an error response with the provided error message.
    pub fn from_response_to_error_response(&self, error: String) -> Result<Message> {
        let mut response = self
            .metadata()
            .context("Failed to parse metadata of request or response when producing an error")?
            .to_error_response(error)?;

        if let Some(request_id) = self.request_id() {
            response.set_request_id(request_id)
        }

        Ok(response)
    }

    /// Returns an error response with the provided error message.
    pub fn from_request_to_error_response(&self, error: String) -> Result<Message> {
        let mut request = self
            .metadata()
            .context("Failed to parse metadata of request or response when producing an error")?
            .to_error_response(error)?;

        request.set_request_id(self.id());
        Ok(request)
    }

    /// Get metadata for this `Message`
    pub fn metadata(&self) -> Result<Metadata> {
        match self.inner.as_ref().unwrap() {
            MessageInner::RawBytes {
                #[cfg(feature = "cassandra")]
                bytes,
                message_type,
                ..
            } => match message_type {
                #[cfg(feature = "cassandra")]
                MessageType::Cassandra => {
                    Ok(Metadata::Cassandra(cassandra::raw_frame::metadata(bytes)?))
                }
                #[cfg(feature = "valkey")]
                MessageType::Valkey => Ok(Metadata::Valkey),
                #[cfg(feature = "kafka")]
                MessageType::Kafka => Ok(Metadata::Kafka),
                MessageType::Dummy => Err(anyhow!("Dummy has no metadata")),
                #[cfg(feature = "opensearch")]
                MessageType::OpenSearch => Err(anyhow!("OpenSearch has no metadata")),
            },

            MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame } => match frame.as_ref() {

                #[cfg(feature = "cassandra")]
                Frame::Cassandra(frame) => Ok(Metadata::Cassandra(frame.metadata())),
                
                #[cfg(feature = "kafka")]
                Frame::Kafka(_) => Ok(Metadata::Kafka),
                
                #[cfg(feature = "valkey")]
                Frame::Valkey(_) => Ok(Metadata::Valkey),
                Frame::Dummy => Err(anyhow!("dummy has no metadata")),
                
                #[cfg(feature = "opensearch")]
                Frame::OpenSearch(_) => Err(anyhow!("OpenSearch has no metadata")),
            },
        }
    }

    /// Set this `Message` to a dummy frame so that the message will never reach the client or DB.
    /// For requests, the dummy frame will be dropped when it reaches the Sink.
    ///     Additionally a corresponding dummy response will be generated with its request_id set to the requests id.
    /// For responses, the dummy frame will be dropped when it reaches the Source.
    pub fn replace_with_dummy(&mut self) {
        self.inner = Some(MessageInner::Modified {
            frame: Box::new(Frame::Dummy),
        });
    }

    /// Returns true iff it is known that the server will not send a response to this request, instead we need to generate a dummy response
    pub(crate) fn response_is_dummy(&mut self) -> bool {
        match self.message_type() {
            #[cfg(feature = "valkey")]
            MessageType::Valkey => false,
            #[cfg(feature = "cassandra")]
            MessageType::Cassandra => false,
            #[cfg(feature = "kafka")]
            MessageType::Kafka => match self.frame() {
                Some(Frame::Kafka(crate::frame::kafka::KafkaFrame::Request {
                    body: crate::frame::kafka::RequestBody::Produce(produce),
                    ..
                })) => produce.acks == 0,
                _ => false,
            },
            #[cfg(feature = "opensearch")]
            MessageType::OpenSearch => false,
            MessageType::Dummy => true,
        }
    }

    pub fn is_dummy(&self) -> bool {
        if let Some(MessageInner::Modified{frame}) = &self.inner{
            if let Frame::Dummy = frame.as_ref(){
                return true;
            }
        }
        false
    }

    /// Set this `Message` to a backpressure response
    pub fn to_backpressure(&mut self) -> Result<Message> {
        let metadata = self.metadata()?;

        Ok(Message::from_frame_at_instant(
            match metadata {
                #[cfg(feature = "cassandra")]
                Metadata::Cassandra(metadata) => Frame::Cassandra(metadata.backpressure_response()),
                #[cfg(feature = "valkey")]
                Metadata::Valkey => unimplemented!(),
                #[cfg(feature = "kafka")]
                Metadata::Kafka => unimplemented!(),
                #[cfg(feature = "opensearch")]
                Metadata::OpenSearch => unimplemented!(),
            },
            // reachable with feature = cassandra
            #[allow(unreachable_code)]
            self.received_from_source_or_sink_at,
        ))
    }

    // Retrieves the stream_id without parsing the rest of the frame.
    // Used for ordering out of order messages without parsing their contents.
    // TODO: We will have a better idea of how to make this generic once we have multiple out of order protocols
    //       For now its just written to match cassandra's stream_id field
    // TODO: deprecated, just call `metadata()` instead
    pub(crate) fn stream_id(&self) -> Option<i16> {
        match &self.inner {
            #[cfg(feature = "cassandra")]
            Some(MessageInner::RawBytes {
                bytes,
                message_type: MessageType::Cassandra,
            }) => {
                use bytes::Buf;
                const HEADER_LEN: usize = 9;
                if bytes.len() >= HEADER_LEN {
                    Some((&bytes[2..4]).get_i16())
                } else {
                    None
                }
            }
            Some(MessageInner::RawBytes { .. }) => None,
            Some(MessageInner::Parsed { frame, .. } | MessageInner::Modified { frame }) => {
                match frame.as_ref() {
                    #[cfg(feature = "cassandra")]
                    Frame::Cassandra(cassandra) => Some(cassandra.stream_id),
                    #[cfg(feature = "valkey")]
                    Frame::Valkey(_) => None,
                    #[cfg(feature = "kafka")]
                    Frame::Kafka(_) => None,
                    Frame::Dummy => None,
                    #[cfg(feature = "opensearch")]
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
            format!("Unparseable {message_type:?} message {bytes:?}")
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
        frame: Box<Frame>,
    },
    Modified {
        frame: Box<Frame>,
    },
}

impl MessageInner {
    fn ensure_parsed(self, codec_state: CodecState) -> (Self, Result<()>) {
        match self {
            MessageInner::RawBytes {
                bytes,
                message_type,
            } => match Frame::from_bytes(bytes.clone(), message_type, codec_state) {
                Ok(frame) => (MessageInner::Parsed {bytes, frame}, Ok(())),
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
            MessageInner::RawBytes { .. } => {
                tracing::error!("Invalidated cache but the frame was not parsed");
                self
            }
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
    Frame(Box<Frame>),
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

