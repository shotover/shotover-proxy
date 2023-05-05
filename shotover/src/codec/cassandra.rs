use super::{CodecWriteError, Direction};
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::cassandra::{CassandraMetadata, CassandraOperation, Tracing};
use crate::frame::{CassandraFrame, Frame, MessageType};
use crate::message::{Encodable, Message, Messages, Metadata};
use anyhow::{anyhow, Result};
use atomic_enum::atomic_enum;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::crc::{crc24, crc32};
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::message_startup::BodyReqStartup;
use cassandra_protocol::frame::{Flags, Opcode, Version, PAYLOAD_SIZE_LIMIT};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Identifier;
use lz4_flex::{block::get_maximum_output_size, compress_into, decompress};
use metrics::increment_counter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

const ENVELOPE_HEADER_LEN: usize = 9;
const UNCOMPRESSED_FRAME_HEADER_LENGTH: usize = 6;
const COMPRESSED_FRAME_HEADER_LENGTH: usize = 8;
const FRAME_TRAILER_LENGTH: usize = 4;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CheckFrameSizeError {
    #[error("Not enough bytes!")]
    NotEnoughBytes,
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("Unsupported opcode: {0}")]
    UnsupportedOpcode(u8),
    #[error("Unsupported compression: {0}")]
    UnsupportedCompression(String),
}

#[atomic_enum]
pub enum VersionState {
    V3,
    V4,
    V5,
}

impl From<Version> for VersionState {
    fn from(val: Version) -> Self {
        match val {
            Version::V3 => VersionState::V3,
            Version::V4 => VersionState::V4,
            Version::V5 => VersionState::V5,
            _ => unimplemented!(),
        }
    }
}

impl From<VersionState> for Version {
    fn from(val: VersionState) -> Self {
        match val {
            VersionState::V3 => Version::V3,
            VersionState::V4 => Version::V4,
            VersionState::V5 => Version::V5,
        }
    }
}

#[atomic_enum]
pub enum CompressionState {
    None,
    Lz4,
    Snappy,
}

impl From<Compression> for CompressionState {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::None => CompressionState::None,
            Compression::Lz4 => CompressionState::Lz4,
            Compression::Snappy => CompressionState::Snappy,
        }
    }
}

impl From<CompressionState> for Compression {
    fn from(val: CompressionState) -> Self {
        match val {
            CompressionState::None => Compression::None,
            CompressionState::Lz4 => Compression::Lz4,
            CompressionState::Snappy => Compression::Snappy,
        }
    }
}

#[derive(Clone)]
pub struct CassandraCodecBuilder {
    direction: Direction,
}

impl CodecBuilder for CassandraCodecBuilder {
    type Decoder = CassandraDecoder;
    type Encoder = CassandraEncoder;

    fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn build(&self) -> (CassandraDecoder, CassandraEncoder) {
        let version = Arc::new(AtomicVersionState::new(VersionState::V4));
        let compression = Arc::new(AtomicCompressionState::new(CompressionState::None));

        let handshake_complete = Arc::new(AtomicBool::from(false));
        (
            CassandraDecoder::new(
                version.clone(),
                compression.clone(),
                self.direction,
                handshake_complete.clone(),
            ),
            CassandraEncoder::new(version, compression, self.direction, handshake_complete),
        )
    }
}

pub struct CassandraDecoder {
    version: Arc<AtomicVersionState>,
    compression: Arc<AtomicCompressionState>,
    handshake_complete: Arc<AtomicBool>,
    messages: Vec<Message>,
    current_use_keyspace: Option<Identifier>,
    direction: Direction,
}

impl CassandraDecoder {
    pub fn new(
        version: Arc<AtomicVersionState>,
        compression: Arc<AtomicCompressionState>,
        direction: Direction,
        handshake_complete: Arc<AtomicBool>,
    ) -> CassandraDecoder {
        CassandraDecoder {
            version,
            compression,
            handshake_complete,
            messages: vec![],
            current_use_keyspace: None,
            direction,
        }
    }
}

impl CassandraDecoder {
    fn check_compression(&mut self, bytes: &BytesMut) -> Result<bool> {
        if bytes.len() < 9 {
            return Err(anyhow!("Not enough bytes for cassandra frame"));
        }
        let opcode = Opcode::try_from(bytes[4])?;

        let compressed = Flags::from_bits_truncate(bytes[1]).contains(Flags::COMPRESSION);

        // check if startup message and set the codec's selected compression and version
        if Opcode::Startup == opcode {
            if let CassandraFrame {
                operation: CassandraOperation::Startup(startup),
                version,
                ..
            } = CassandraFrame::from_bytes(bytes.clone().freeze(), Compression::None)?
            {
                set_startup_state(
                    &mut self.compression,
                    &mut self.version,
                    version,
                    &startup,
                    self.direction,
                );
            };
        }

        if Opcode::Ready == opcode || Opcode::Authenticate == opcode {
            self.handshake_complete
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(compressed)
    }

    fn decode_frame(
        &mut self,
        src: &mut BytesMut,
        frame_len: usize,
        version: Version,
        compression: Compression,
        handshake_complete: bool,
    ) -> Result<Vec<Message>> {
        match (version, handshake_complete) {
            (Version::V5, true) => match compression {
                Compression::None => {
                    let mut frame_bytes = src.split_to(frame_len);

                    let header =
                        i64::from_le_bytes(frame_bytes[..8].try_into().unwrap()) & 0xffffffffffff; // convert to 6 byte int

                    let header_crc24 = ((header >> 24) & 0xffffff) as i32;
                    let computed_crc = crc24(&header.to_le_bytes()[..3]);

                    if header_crc24 != computed_crc {
                        return Err(header_crc_mismatch_error(computed_crc, header_crc24));
                    }

                    let payload_length = (header & 0x1ffff) as usize;
                    let payload_end = UNCOMPRESSED_FRAME_HEADER_LENGTH + payload_length;

                    let frame_end = payload_end + FRAME_TRAILER_LENGTH;

                    let payload_crc32 =
                        u32::from_le_bytes(frame_bytes[payload_end..frame_end].try_into().unwrap());

                    let computed_crc =
                        crc32(&frame_bytes[UNCOMPRESSED_FRAME_HEADER_LENGTH..payload_end]);
                    if payload_crc32 != computed_crc {
                        return Err(payload_crc_mismatch_error(computed_crc, payload_crc32));
                    }

                    let self_contained = (header & (1 << 17)) != 0;
                    if !self_contained {
                        unimplemented!("Cannot support non-self contained frames yet");
                    }

                    frame_bytes.advance(UNCOMPRESSED_FRAME_HEADER_LENGTH);
                    let payload = frame_bytes.split_to(payload_length).freeze();
                    let envelopes = self.extract_envelopes_from_payload(payload)?;

                    Ok(envelopes)
                }
                Compression::Lz4 => {
                    let mut frame_bytes = src.split_to(frame_len);

                    let header = i64::from_le_bytes(
                        frame_bytes[..COMPRESSED_FRAME_HEADER_LENGTH]
                            .try_into()
                            .unwrap(),
                    );

                    let header_crc24 = ((header >> 40) & 0xffffff) as i32;
                    let computed_crc = crc24(&header.to_le_bytes()[..5]);

                    if header_crc24 != computed_crc {
                        return Err(header_crc_mismatch_error(computed_crc, header_crc24));
                    }

                    let compressed_len = (header & 0x1ffff) as usize;
                    let compressed_payload_end = compressed_len + COMPRESSED_FRAME_HEADER_LENGTH;

                    let frame_end = compressed_payload_end + FRAME_TRAILER_LENGTH;

                    let compressed_payload_crc32 = u32::from_le_bytes(
                        frame_bytes[compressed_payload_end..frame_end]
                            .try_into()
                            .unwrap(),
                    );

                    let computed_crc =
                        crc32(&frame_bytes[COMPRESSED_FRAME_HEADER_LENGTH..compressed_payload_end]);

                    if compressed_payload_crc32 != computed_crc {
                        return Err(payload_crc_mismatch_error(
                            computed_crc,
                            compressed_payload_crc32,
                        ));
                    }

                    let self_contained = (header & (1 << 34)) != 0;
                    if !self_contained {
                        unimplemented!("Cannot support non-self contained frames yet");
                    }

                    let uncompressed_length = ((header >> 17) & 0x1ffff) as usize;

                    frame_bytes.advance(COMPRESSED_FRAME_HEADER_LENGTH);

                    let payload = if uncompressed_length == 0 {
                        // protocol spec 2.2:
                        // An uncompressed length of 0 signals that the compressed payload should be used as-is
                        // and not decompressed.
                        frame_bytes.split_to(compressed_len).freeze()
                    } else {
                        decompress(
                            &frame_bytes.split_to(compressed_len).freeze(),
                            uncompressed_length,
                        )?
                        .into()
                    };

                    let envelopes = self.extract_envelopes_from_payload(payload)?;

                    Ok(envelopes)
                }
                _ => Err(anyhow!("Only Lz4 compression is supported for v5")),
            },
            (_, _) => {
                let bytes = src.split_to(frame_len);
                tracing::debug!(
                    "{}: incoming cassandra message:\n{}",
                    self.direction,
                    pretty_hex::pretty_hex(&bytes)
                );

                let compressed = self.check_compression(&bytes).unwrap();

                let message = Message::from_bytes(
                    bytes.freeze(),
                    crate::message::ProtocolType::Cassandra {
                        compression: if compressed {
                            compression
                        } else {
                            Compression::None
                        },
                    },
                );

                Ok(vec![message])
            }
        }
    }

    fn check_size(
        &self,
        src: &BytesMut,
        version: Version,
        compression: Compression,
        handshake_complete: bool,
    ) -> Result<usize, CheckFrameSizeError> {
        match (version, handshake_complete) {
            (Version::V5, true) => match compression {
                Compression::None => {
                    let buffer_len = src.len();
                    if buffer_len < UNCOMPRESSED_FRAME_HEADER_LENGTH {
                        return Err(CheckFrameSizeError::NotEnoughBytes);
                    }

                    let payload_length =
                        (u32::from_le_bytes(src[..4].try_into().unwrap()) & 0x1ffff) as usize;

                    let payload_end = UNCOMPRESSED_FRAME_HEADER_LENGTH + payload_length;

                    let frame_len = payload_end + FRAME_TRAILER_LENGTH;
                    if buffer_len < frame_len {
                        return Err(CheckFrameSizeError::NotEnoughBytes);
                    }

                    Ok(frame_len)
                }
                Compression::Lz4 => {
                    let buffer_len = src.len();
                    if buffer_len < COMPRESSED_FRAME_HEADER_LENGTH {
                        return Err(CheckFrameSizeError::NotEnoughBytes);
                    }

                    let header = i64::from_le_bytes(
                        src[..COMPRESSED_FRAME_HEADER_LENGTH].try_into().unwrap(),
                    );

                    let compressed_length = (header & 0x1ffff) as usize;
                    let compressed_payload_end = compressed_length + COMPRESSED_FRAME_HEADER_LENGTH;

                    let frame_len = compressed_payload_end + FRAME_TRAILER_LENGTH;
                    if buffer_len < frame_len {
                        return Err(CheckFrameSizeError::NotEnoughBytes);
                    }

                    Ok(frame_len)
                }
                _ => Err(CheckFrameSizeError::UnsupportedCompression(
                    "Only Lz4 compression is supported for v5".into(),
                )),
            },
            (_, _) => {
                if src.len() < ENVELOPE_HEADER_LEN {
                    return Err(CheckFrameSizeError::NotEnoughBytes);
                }

                let body_len = i32::from_be_bytes(src[5..9].try_into().unwrap()) as usize;

                let envelope_len = ENVELOPE_HEADER_LEN + body_len;
                if src.len() < envelope_len {
                    return Err(CheckFrameSizeError::NotEnoughBytes);
                }

                let version = Version::try_from(src[0])
                    .map_err(|_| CheckFrameSizeError::UnsupportedVersion(src[0] & 0x7f))?;

                if Version::V3 == version
                    || Version::V4 == version
                    || (cfg!(feature = "alpha-transforms") && Version::V5 == version)
                {
                    // accept these versions
                } else {
                    // Reject protocols that cassandra-protocol supports but shotover does not yet support
                    return Err(CheckFrameSizeError::UnsupportedVersion(version.into()));
                };

                Ok(envelope_len)
            }
        }
    }

    fn extract_envelopes_from_payload(&self, mut payload: Bytes) -> Result<Vec<Message>> {
        let mut envelopes: Vec<Message> = vec![];

        while !payload.is_empty() {
            let body_len = i32::from_be_bytes(payload[5..9].try_into().unwrap()) as usize;

            let envelope_len = ENVELOPE_HEADER_LEN + body_len;

            if envelope_len > payload.len() {
                return Err(anyhow!(format!(
                    "envelope length {} is longer than payload length {}",
                    envelope_len,
                    payload.len()
                ),));
            }

            let envelope = payload.split_to(envelope_len);

            tracing::debug!(
                "{}: incoming cassandra message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&envelope)
            );

            envelopes.push(Message::from_bytes(
                envelope,
                crate::message::ProtocolType::Cassandra {
                    compression: Compression::None,
                },
            ));
        }

        Ok(envelopes)
    }
}

fn header_crc_mismatch_error(computed_crc: i32, header_crc24: i32) -> anyhow::Error {
    anyhow!(format!(
        "Header CRC mismatch - read {header_crc24}, computed {computed_crc}."
    ))
}

fn payload_crc_mismatch_error(computed_crc: u32, payload_crc32: u32) -> anyhow::Error {
    anyhow!(format!(
        "Payload CRC mismatch - read {payload_crc32}, computed {computed_crc}."
    ))
}

fn set_startup_state(
    compression_state: &mut Arc<AtomicCompressionState>,
    version_state: &mut Arc<AtomicVersionState>,
    version: Version,
    startup: &BodyReqStartup,
    direction: Direction,
) {
    if let Some(compression) = startup.map.get("COMPRESSION") {
        compression_state.store(
            match compression.as_str() {
                "snappy" | "SNAPPY" => Compression::Snappy,
                "lz4" | "LZ4" => Compression::Lz4,
                "" | "none" | "NONE" => Compression::None,
                _ => unimplemented!(),
            }
            .into(),
            Ordering::Relaxed,
        );
    }

    if direction == Direction::Source {
        match version {
            Version::V3 => increment_counter!("client_protocol_version", "version" => "v3"),
            Version::V4 => increment_counter!("client_protocol_version", "version" => "v4"),
            Version::V5 => increment_counter!("client_protocol_version", "version" => "v5"),
            _ => unimplemented!(),
        };
    }

    version_state.store(version.into(), Ordering::Relaxed);
}

impl Decoder for CassandraDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        let version: Version = self.version.load(Ordering::Relaxed).into();
        let compression: Compression = self.compression.load(Ordering::Relaxed).into();
        let handshake_complete = self.handshake_complete.load(Ordering::Relaxed);

        loop {
            match self.check_size(src, version, compression, handshake_complete) {
                Ok(frame_len) => {
                    let mut messages = self
                        .decode_frame(src, frame_len, version, compression, handshake_complete)
                        .map_err(CodecReadError::Parser)?;

                    for message in messages.iter_mut() {
                        if let Ok(Metadata::Cassandra(CassandraMetadata {
                            opcode: Opcode::Query | Opcode::Batch,
                            ..
                        })) = message.metadata()
                        {
                            if let Some(keyspace) = get_use_keyspace(message) {
                                self.current_use_keyspace = Some(keyspace);
                            }

                            if let Some(keyspace) = &self.current_use_keyspace {
                                set_default_keyspace(message, keyspace);
                            }
                        }
                    }

                    self.messages.append(&mut messages);
                }
                Err(CheckFrameSizeError::NotEnoughBytes) => {
                    if self.messages.is_empty() || src.remaining() != 0 {
                        return Ok(None);
                    } else {
                        return Ok(Some(std::mem::take(&mut self.messages)));
                    }
                }
                Err(CheckFrameSizeError::UnsupportedVersion(version)) => {
                    return Err(reject_protocol_version(version));
                }
                Err(CheckFrameSizeError::UnsupportedCompression(msg)) => {
                    return Err(CodecReadError::Parser(anyhow!(msg)));
                }
                err => {
                    return Err(CodecReadError::Parser(anyhow!(
                        "Failed to parse frame {:?}",
                        err
                    )))
                }
            }
        }
    }
}

fn get_use_keyspace(message: &mut Message) -> Option<Identifier> {
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        if let CassandraOperation::Query { query, .. } = &mut frame.operation {
            if let CassandraStatement::Use(keyspace) = query.as_ref() {
                return Some(keyspace.clone());
            }
        }
    }
    None
}

fn set_default_keyspace(message: &mut Message, keyspace: &Identifier) {
    // TODO: rewrite Operation::Prepared in the same way
    if let Some(Frame::Cassandra(frame)) = message.frame() {
        for query in frame.operation.queries() {
            let name = match query {
                CassandraStatement::AlterMaterializedView(x) => &mut x.name,
                CassandraStatement::AlterTable(x) => &mut x.name,
                CassandraStatement::AlterType(x) => &mut x.name,
                CassandraStatement::CreateAggregate(x) => &mut x.name,
                CassandraStatement::CreateFunction(x) => &mut x.name,
                CassandraStatement::CreateIndex(x) => &mut x.table,
                CassandraStatement::CreateMaterializedView(x) => &mut x.name,
                CassandraStatement::CreateTable(x) => &mut x.name,
                CassandraStatement::CreateTrigger(x) => &mut x.name,
                CassandraStatement::CreateType(x) => &mut x.name,
                CassandraStatement::Delete(x) => &mut x.table_name,
                CassandraStatement::DropAggregate(x) => &mut x.name,
                CassandraStatement::DropFunction(x) => &mut x.name,
                CassandraStatement::DropIndex(x) => &mut x.name,
                CassandraStatement::DropMaterializedView(x) => &mut x.name,
                CassandraStatement::DropTable(x) => &mut x.name,
                CassandraStatement::DropTrigger(x) => &mut x.name,
                CassandraStatement::DropType(x) => &mut x.name,
                CassandraStatement::Insert(x) => &mut x.table_name,
                CassandraStatement::Select(x) => &mut x.table_name,
                CassandraStatement::Truncate(name) => name,
                CassandraStatement::Update(x) => &mut x.table_name,
                CassandraStatement::AlterKeyspace(_)
                | CassandraStatement::AlterRole(_)
                | CassandraStatement::AlterUser(_)
                | CassandraStatement::ApplyBatch
                | CassandraStatement::CreateKeyspace(_)
                | CassandraStatement::CreateRole(_)
                | CassandraStatement::CreateUser(_)
                | CassandraStatement::DropRole(_)
                | CassandraStatement::DropUser(_)
                | CassandraStatement::Grant(_)
                | CassandraStatement::ListRoles(_)
                | CassandraStatement::Revoke(_)
                | CassandraStatement::DropKeyspace(_)
                | CassandraStatement::ListPermissions(_)
                | CassandraStatement::Use(_)
                | CassandraStatement::Unknown(_) => {
                    return;
                }
            };
            if name.keyspace.is_none() {
                name.keyspace = Some(keyspace.clone());
            }
        }
    }
}

/// If the client tried to use a protocol that we dont support then we need to reject it.
/// The rejection process is sending back an error and then closing the connection.
fn reject_protocol_version(version: u8) -> CodecReadError {
    info!(
        "Negotiating protocol version: rejecting version {} (configure the client to use a supported version by default to improve connection time)",
        version
    );

    CodecReadError::RespondAndThenCloseConnection(vec![Message::from_frame(Frame::Cassandra(
        CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            operation: CassandraOperation::Error(ErrorBody {
                message: "Invalid or unsupported protocol version".into(),
                ty: ErrorType::Protocol,
            }),
            tracing: Tracing::Response(None),
            warnings: vec![],
        },
    ))])
}

pub struct CassandraEncoder {
    version: Arc<AtomicVersionState>,
    compression: Arc<AtomicCompressionState>,
    direction: Direction,
    handshake_complete: Arc<AtomicBool>,
}

impl CassandraEncoder {
    pub fn new(
        version: Arc<AtomicVersionState>,
        compression: Arc<AtomicCompressionState>,
        direction: Direction,
        handshake_complete: Arc<AtomicBool>,
    ) -> CassandraEncoder {
        CassandraEncoder {
            version,
            compression,
            direction,
            handshake_complete,
        }
    }
}

impl Encoder<Messages> for CassandraEncoder {
    type Error = CodecWriteError;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let version: Version = self.version.load(Ordering::Relaxed).into();
        let compression: Compression = self.compression.load(Ordering::Relaxed).into();
        let handshake_complete = self.handshake_complete.load(Ordering::Relaxed);

        for m in item {
            let start = dst.len();
            self.encode_frame(dst, m, version, compression, handshake_complete)
                .map_err(CodecWriteError::Encoder)?;
            tracing::debug!(
                "{}: outgoing cassandra message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );
        }
        Ok(())
    }
}

impl CassandraEncoder {
    fn encode_frame(
        &mut self,
        dst: &mut BytesMut,
        m: Message,
        version: Version,
        compression: Compression,
        handshake_complete: bool,
    ) -> Result<()> {
        match (version, handshake_complete) {
            (Version::V5, true) => {
                match compression {
                    Compression::None => {
                        // write envelope header with dummy values for those we cant calculate till after we write the message
                        let header_start = dst.len();

                        dst.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
                        let payload_start = dst.len();

                        self.encode_envelope(dst, m, Compression::None)?;

                        //measure length of message and calculate crc24 and overwrite frame header values
                        let mut payload_len = (dst.len() - payload_start) as u64;

                        if true {
                            // TODO if self_contained
                            payload_len |= 1 << 17;
                        }

                        // add header length & header crc
                        let payload_len = &payload_len.to_le_bytes()[..3];
                        dst[header_start..header_start + 3].copy_from_slice(payload_len);
                        dst[header_start + 3..header_start + 6]
                            .copy_from_slice(&crc24(payload_len).to_le_bytes()[..3]);

                        // add payload crc
                        dst.extend_from_slice(&crc32(&dst[payload_start..]).to_le_bytes());
                    }
                    Compression::Lz4 => {
                        let header_start = dst.len();

                        dst.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
                        let payload_start = dst.len();

                        // TODO we should not be encoding small frames
                        let (uncompressed_len, compressed_len) =
                            self.encode_compressed_payload(dst, m, payload_start)?;

                        if compressed_len > PAYLOAD_SIZE_LIMIT {
                            todo!("non self contained frames not yet implemented.")
                        }

                        if uncompressed_len > PAYLOAD_SIZE_LIMIT {
                            todo!("non self contained frames not yet implemented.")
                        }

                        let mut header =
                            (compressed_len) as u64 | ((uncompressed_len as u64) << 17);

                        if true {
                            // TODO if self_contained
                            header |= 1 << 34;
                        }

                        let crc = crc24(&header.to_le_bytes()[..5]) as u64;

                        let header = header | (crc << 40);

                        dst[header_start..header_start + 8].copy_from_slice(&header.to_le_bytes());

                        // add payload crc
                        dst.extend_from_slice(&crc32(&dst[payload_start..]).to_le_bytes());
                    }
                    _ => unimplemented!("Only Lz4 compression is supported for v5"),
                }

                Ok(())
            }
            (_, _) => {
                let message_compression = m.codec_state.as_cassandra();
                self.encode_envelope(dst, m, message_compression)
            }
        }
    }

    fn encode_compressed_payload(
        &mut self,
        dst: &mut BytesMut,
        m: Message,
        payload_start: usize,
    ) -> Result<(usize, usize)> {
        // TODO: always check if cassandra message
        let bytes = match m.into_encodable(MessageType::Cassandra)? {
            Encodable::Bytes(bytes) => bytes,
            Encodable::Frame(frame) => frame
                .into_cassandra()
                .unwrap()
                .encode(Compression::None)
                .into(),
        };

        let mut uncompressed_len = bytes.len();
        dst.resize(payload_start + get_maximum_output_size(uncompressed_len), 0);
        let mut compressed_len = compress_into(&bytes, &mut dst[payload_start..])?;

        // fallback to uncompressed data if its more efficient
        if compressed_len > uncompressed_len {
            dst[payload_start..(payload_start + uncompressed_len)]
                .copy_from_slice(&bytes[..uncompressed_len]);

            compressed_len = uncompressed_len;
            uncompressed_len = 0;
        }

        dst.truncate(payload_start + compressed_len);
        Ok((uncompressed_len, compressed_len))
    }

    fn encode_envelope(
        &mut self,
        dst: &mut BytesMut,
        m: Message,
        envelope_compresson: Compression,
    ) -> Result<()> {
        // TODO: always check if cassandra message
        match m.into_encodable(MessageType::Cassandra)? {
            Encodable::Bytes(bytes) => {
                // check if the message is a startup message and set the codec's compression
                {
                    let opcode = Opcode::try_from(bytes[4])?;
                    if Opcode::Startup == opcode {
                        if let CassandraFrame {
                            operation: CassandraOperation::Startup(startup),
                            version,
                            ..
                        } = CassandraFrame::from_bytes(bytes.clone(), Compression::None)?
                        {
                            set_startup_state(
                                &mut self.compression,
                                &mut self.version,
                                version,
                                &startup,
                                self.direction,
                            );
                        };
                    }

                    if Opcode::Ready == opcode || Opcode::Authenticate == opcode {
                        self.handshake_complete.store(true, Ordering::Relaxed);
                    }
                }

                dst.extend_from_slice(&bytes)
            }
            Encodable::Frame(frame) => {
                // check if the message is a startup message and set the codec's compression
                if let Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Startup(startup),
                    version,
                    ..
                }) = &frame
                {
                    set_startup_state(
                        &mut self.compression,
                        &mut self.version,
                        *version,
                        startup,
                        self.direction,
                    );
                };

                if let Frame::Cassandra(CassandraFrame {
                    operation: CassandraOperation::Ready(_) | CassandraOperation::Authenticate(_),
                    ..
                }) = &frame
                {
                    self.handshake_complete.store(true, Ordering::Relaxed);
                };

                let buffer = frame.into_cassandra().unwrap().encode(envelope_compresson);

                dst.put(buffer.as_slice());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::codec::cassandra::CassandraCodecBuilder;
    use crate::codec::{CodecBuilder, Direction};
    use crate::frame::cassandra::{
        parse_statement_single, CassandraFrame, CassandraOperation, CassandraResult, Tracing,
    };
    use crate::frame::Frame;
    use crate::message::Message;
    use bytes::BytesMut;
    use cassandra_protocol::events::SimpleServerEvent;
    use cassandra_protocol::frame::message_register::BodyReqRegister;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags,
        TableSpec,
    };
    use cassandra_protocol::frame::message_startup::BodyReqStartup;
    use cassandra_protocol::frame::Version;
    use hex_literal::hex;
    use std::collections::HashMap;
    use tokio_util::codec::{Decoder, Encoder};

    fn test_frame_codec_roundtrip(
        codec: &mut CassandraCodecBuilder,
        raw_frame: &[u8],
        expected_messages: Vec<Message>,
    ) {
        let (mut decoder, mut encoder) = codec.build();
        // test decode
        let decoded_messages = decoder
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        // test messages parse correctly
        let mut parsed_messages = decoded_messages.clone();
        for message in &mut parsed_messages {
            // This has the side effect of modifying the inner message to be parsed
            message.frame().unwrap();
            message.invalidate_cache();
        }
        assert_eq!(parsed_messages, expected_messages);

        // test encode round trip - parsed messages
        {
            let mut dest = BytesMut::new();
            encoder.encode(parsed_messages, &mut dest).unwrap();
            assert_eq!(raw_frame, &dest.to_vec());
        }

        // test encode round trip - raw messages
        {
            let mut dest = BytesMut::new();
            encoder.encode(decoded_messages, &mut dest).unwrap();
            assert_eq!(raw_frame, &dest.to_vec());
        }
    }

    #[test]
    fn test_codec_startup() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let mut startup_body: HashMap<String, String> = HashMap::new();
        startup_body.insert("CQL_VERSION".into(), "3.0.0".into());
        let bytes = hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Startup(BodyReqStartup { map: startup_body }),
            stream_id: 0,
            tracing: Tracing::Request(false),
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_options() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!("040000000500000000");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Options(vec![]),
            stream_id: 0,
            tracing: Tracing::Request(false),
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_ready() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!("840000000200000000");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Ready(vec![]),
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_register() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!(
            "040000010b000000310003000f544f504f4c4f47595f4348414e4745
            000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
        );
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Register(BodyReqRegister {
                events: vec![
                    SimpleServerEvent::TopologyChange,
                    SimpleServerEvent::StatusChange,
                    SimpleServerEvent::SchemaChange,
                ],
            }),
            stream_id: 1,
            tracing: Tracing::Request(false),
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_result() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!(
            "840000020800000099000000020000000100000009000673797374656
            d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
            65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
            573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
        );
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Result(CassandraResult::Rows {
                rows: vec![],
                metadata: Box::new(RowsMetadata {
                    flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                    columns_count: 9,
                    paging_state: None,
                    new_metadata_id: None,
                    global_table_spec: Some(TableSpec {
                        ks_name: "system".into(),
                        table_name: "peers".into(),
                    }),
                    col_specs: vec![
                        ColSpec {
                            table_spec: None,
                            name: "peer".into(),
                            col_type: ColTypeOption {
                                id: ColType::Inet,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "data_center".into(),
                            col_type: ColTypeOption {
                                id: ColType::Varchar,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "host_id".into(),
                            col_type: ColTypeOption {
                                id: ColType::Uuid,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "preferred_ip".into(),
                            col_type: ColTypeOption {
                                id: ColType::Inet,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "rack".into(),
                            col_type: ColTypeOption {
                                id: ColType::Varchar,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "release_version".into(),
                            col_type: ColTypeOption {
                                id: ColType::Varchar,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "rpc_address".into(),
                            col_type: ColTypeOption {
                                id: ColType::Inet,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "schema_version".into(),
                            col_type: ColTypeOption {
                                id: ColType::Uuid,
                                value: None,
                            },
                        },
                        ColSpec {
                            table_spec: None,
                            name: "tokens".into(),
                            col_type: ColTypeOption {
                                id: ColType::Set,
                                value: Some(ColTypeOptionValue::CSet(Box::new(ColTypeOption {
                                    id: ColType::Varchar,
                                    value: None,
                                }))),
                            },
                        },
                    ],
                }),
            }),
            stream_id: 2,
            tracing: Tracing::Response(None),
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_query_select() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!(
            "0400000307000000350000002e53454c454354202a2046524f4d20737973
            74656d2e6c6f63616c205748455245206b6579203d20276c6f63616c27000100"
        );

        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 3,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT * FROM system.local WHERE key = 'local'",
                )),
                params: Box::default(),
            },
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_query_insert() {
        let mut codec = CassandraCodecBuilder::new(Direction::Sink);
        let bytes = hex!(
            "0400000307000000330000002c494e5345525420494e544f207379737465
            6d2e666f6f2028626172292056414c554553202827626172322729000100"
        );

        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 3,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "INSERT INTO system.foo (bar) VALUES ('bar2')",
                )),
                params: Box::default(),
            },
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }
}
