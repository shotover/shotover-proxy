use super::cassandra_frames::{
    CassandraFrameCodec, CheckFrameSizeError, LegacyFrameCodec, UncompressedFrameCodec,
};
use super::Direction;
use crate::codec::{CodecBuilder, CodecReadError};
use crate::frame::cassandra::{CassandraMetadata, CassandraOperation, Tracing};
use crate::frame::{CassandraFrame, Frame, MessageType};
use crate::message::{Encodable, Message, Messages, Metadata};
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::message_startup::BodyReqStartup;
use cassandra_protocol::frame::{Flags, Opcode, Version};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Identifier;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

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
        let envelope_compression = Arc::new(RwLock::new(Compression::None));
        let frame_codec = Arc::new(RwLock::new(CassandraFrameCodec::default()));

        let handshake_complete = Arc::new(AtomicBool::from(false));

        (
            CassandraDecoder::new(
                envelope_compression.clone(),
                frame_codec.clone(),
                handshake_complete.clone(),
                self.direction,
            ),
            CassandraEncoder::new(
                envelope_compression,
                frame_codec,
                handshake_complete,
                self.direction,
            ),
        )
    }
}

pub struct CassandraDecoder {
    handshake_complete: Arc<AtomicBool>,

    // compression in use for v4 protocol, this is ignored on the v5 protocol as the
    // envelope compression flag is deprecated
    envelope_compression: Arc<RwLock<Compression>>,
    messages: Vec<Message>,
    current_use_keyspace: Option<Identifier>,
    direction: Direction,

    frame_codec: Arc<RwLock<CassandraFrameCodec>>,
    // frame_buffer: Vec<u8>,
}

impl CassandraDecoder {
    pub fn new(
        envelope_compression: Arc<RwLock<Compression>>,
        frame_codec: Arc<RwLock<CassandraFrameCodec>>,
        handshake_complete: Arc<AtomicBool>,
        direction: Direction,
    ) -> Self {
        Self {
            envelope_compression,
            messages: vec![],
            current_use_keyspace: None,
            frame_codec,
            handshake_complete,
            // frame_buffer: Vec::new(),
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

        // check if startup message and set the codec's selected compression
        if Opcode::Startup == opcode {
            if let CassandraFrame {
                operation: CassandraOperation::Startup(startup),
                version,
                ..
            } = CassandraFrame::from_bytes(bytes.clone().freeze(), Compression::None)?
            {
                match version {
                    // If protocol version 3/4, set envelope compression and leave frame codec as `LegacyFrameDecoder`
                    Version::V3 | Version::V4 => {
                        set_envelope_compression(&mut self.envelope_compression, &startup);
                    }
                    // If protocol version 5, leave envelope compression as `Compression::None` and set frame codec
                    Version::V5 => {
                        set_frame_codec(&mut self.frame_codec, &startup);
                    }
                    _ => return Err(anyhow!("Invalid version: {version}")),
                }
            };
        }

        if Opcode::Ready == opcode || Opcode::Authenticate == opcode {
            self.handshake_complete
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(compressed)
    }
}

fn set_envelope_compression(
    envelope_compression_state: &mut Arc<RwLock<Compression>>,
    startup: &BodyReqStartup,
) {
    if let Some(envelope_compression) = startup.map.get("COMPRESSION") {
        let mut write = envelope_compression_state.write().unwrap();

        *write = match envelope_compression.as_str() {
            "snappy" | "SNAPPY" => Compression::Snappy,
            "lz4" | "LZ4" => Compression::Lz4,
            "" | "none" | "NONE" => Compression::None,
            _ => panic!(), // TODO FIX THIS PANIC
        };
    }
}

fn set_frame_codec(
    frame_codec_state: &mut Arc<RwLock<CassandraFrameCodec>>,
    startup: &BodyReqStartup,
) {
    let mut write = frame_codec_state.write().unwrap();

    if let Some(frame_compresssion) = startup.map.get("COMPRESSION") {
        *write = match frame_compresssion.as_str() {
            "snappy" | "SNAPPY" => unimplemented!(),
            "lz4" | "LZ4" => unimplemented!(),
            "" | "none" | "NONE" => CassandraFrameCodec::Uncompressed(UncompressedFrameCodec {}),
            _ => panic!(), // TODO FIX THIS PANIC
        }
    } else {
        *write = CassandraFrameCodec::Uncompressed(UncompressedFrameCodec {})
    }
}

impl Decoder for CassandraDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        let frame_codec = if self
            .handshake_complete
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            *self.frame_codec.read().unwrap()
        } else {
            CassandraFrameCodec::Legacy(LegacyFrameCodec {})
        };

        loop {
            match frame_codec.check_size(src) {
                Ok(frame_len) => {
                    let bytes: BytesMut = src.split_to(frame_len);

                    tracing::debug!(
                        "{}: incoming cassandra message:\n{}",
                        self.direction,
                        pretty_hex::pretty_hex(&bytes)
                    );

                    let envelopes = frame_codec
                        .decode_envelopes(bytes, Compression::None)
                        .unwrap();

                    for envelope_bytes in envelopes {
                        let compressed = self.check_compression(&envelope_bytes).unwrap();

                        let mut message = Message::from_bytes(
                            envelope_bytes.freeze(),
                            crate::message::ProtocolType::Cassandra {
                                compression: if compressed {
                                    *self.envelope_compression.read().unwrap()
                                } else {
                                    Compression::None
                                },
                            },
                        );

                        if let Ok(Metadata::Cassandra(CassandraMetadata {
                            opcode: Opcode::Query | Opcode::Batch,
                            ..
                        })) = message.metadata()
                        {
                            if let Some(keyspace) = get_use_keyspace(&mut message) {
                                self.current_use_keyspace = Some(keyspace);
                            }

                            if let Some(keyspace) = &self.current_use_keyspace {
                                set_default_keyspace(&mut message, keyspace);
                            }
                        }

                        self.messages.push(message);
                    }
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
                err => {
                    return Err(CodecReadError::Parser(anyhow!(
                        "Failed to decode frame {:?}",
                        err
                    )));
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
    envelope_compression: Arc<RwLock<Compression>>,
    frame_codec: Arc<RwLock<CassandraFrameCodec>>,
    handshake_complete: Arc<AtomicBool>,
    direction: Direction,
}

impl CassandraEncoder {
    pub fn new(
        envelope_compression: Arc<RwLock<Compression>>,
        frame_codec: Arc<RwLock<CassandraFrameCodec>>,
        handshake_complete: Arc<AtomicBool>,
        direction: Direction,
    ) -> Self {
        Self {
            envelope_compression,
            frame_codec,
            handshake_complete,
            direction,
        }
    }
}

impl Encoder<Messages> for CassandraEncoder {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let mut envelopes = Vec::<Bytes>::new();
        let mut completed_handshake = false;
        for m in item {
            // let start = dst.len();
            let compression = m.codec_state.as_cassandra();

            // TODO: always check if cassandra message
            let envelope_bytes = match m.into_encodable(MessageType::Cassandra)? {
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
                                match version {
                                    // If protocol version 3/4, set envelope compression and leave frame codec as `LegacyFrameDecoder`
                                    Version::V3 | Version::V4 => {
                                        set_envelope_compression(
                                            &mut self.envelope_compression,
                                            &startup,
                                        );
                                    }
                                    // If protocol version 5, leave envelope compression as `Compression::None` and set frame codec
                                    Version::V5 => {
                                        set_frame_codec(&mut self.frame_codec, &startup);
                                    }
                                    _ => return Err(anyhow!("Invalid version: {version}")),
                                }
                            };
                        }

                        if Opcode::Ready == opcode || Opcode::Authenticate == opcode {
                            completed_handshake = true;
                        }
                    }

                    bytes
                }
                Encodable::Frame(frame) => {
                    {
                        // check if the message is a startup message and set the codec's compression
                        if let Frame::Cassandra(CassandraFrame {
                            operation: CassandraOperation::Startup(startup),
                            version,
                            ..
                        }) = &frame
                        {
                            match version {
                                // If protocol version 3/4, set envelope compression and leave frame codec as `LegacyFrameDecoder`
                                Version::V3 | Version::V4 => {
                                    set_envelope_compression(
                                        &mut self.envelope_compression,
                                        startup,
                                    );
                                }
                                // If protocol version 5, leave envelope compression as `Compression::None` and set frame codec
                                Version::V5 => {
                                    set_frame_codec(&mut self.frame_codec, startup);
                                }
                                _ => return Err(anyhow!("Invalid version: {version}")),
                            }
                        };

                        if let Frame::Cassandra(CassandraFrame {
                            operation:
                                CassandraOperation::Ready(_) | CassandraOperation::Authenticate(_),
                            ..
                        }) = &frame
                        {
                            completed_handshake = true;
                        };
                    }

                    let buffer = frame.into_cassandra().unwrap().encode(compression);

                    Bytes::copy_from_slice(buffer.as_slice())
                }
            };
            envelopes.push(envelope_bytes);
        }

        let bytes = if self
            .handshake_complete
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.frame_codec.read().unwrap().encode_envelopes(envelopes)
        } else {
            LegacyFrameCodec {}.encode_envelopes(envelopes)
        };

        tracing::debug!(
            "{}: outgoing cassandra message:\n{}",
            self.direction,
            pretty_hex::pretty_hex(&&bytes),
        );

        dst.extend_from_slice(&bytes);

        if completed_handshake {
            self.handshake_complete
                .store(true, std::sync::atomic::Ordering::Relaxed);
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
