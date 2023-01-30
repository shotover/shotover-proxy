use crate::frame::cassandra::{CassandraMetadata, CassandraOperation, Tracing};
use crate::frame::{CassandraFrame, Frame, MessageType};
use crate::message::{Encodable, Message, Messages, Metadata};
use crate::server::CodecReadError;
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_error::{ErrorBody, ErrorType};
use cassandra_protocol::frame::{
    CheckEnvelopeSizeError, Envelope as RawCassandraFrame, Opcode, Version,
};
use cql3_parser::cassandra_statement::CassandraStatement;
use cql3_parser::common::Identifier;
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

#[derive(Debug, Clone)]
pub struct CassandraCodec {
    compression: Compression,
    messages: Vec<Message>,
    current_use_keyspace: Option<Identifier>,
}

impl Default for CassandraCodec {
    fn default() -> Self {
        CassandraCodec::new()
    }
}

impl CassandraCodec {
    pub fn new() -> CassandraCodec {
        CassandraCodec {
            compression: Compression::None,
            messages: vec![],
            current_use_keyspace: None,
        }
    }
}

impl CassandraCodec {
    fn encode_raw(&mut self, item: CassandraFrame, dst: &mut BytesMut) {
        let buffer = item.encode(self.compression);
        if buffer.is_empty() {
            info!("trying to send 0 length frame");
        }
        dst.put(buffer.as_slice());
    }
}

impl Decoder for CassandraCodec {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        loop {
            match RawCassandraFrame::check_envelope_size(src) {
                Ok(frame_len) => {
                    // Clear the read bytes from the FramedReader
                    let bytes = src.split_to(frame_len);
                    tracing::debug!(
                        "incoming cassandra message:\n{}",
                        pretty_hex::pretty_hex(&bytes)
                    );

                    let version = Version::try_from(bytes[0])
                        .expect("Gauranteed because check_envelope_size only returns Ok if the Version will parse");
                    if let Version::V3 | Version::V4 = version {
                        // Accept these protocols
                    } else {
                        // Reject protocols that cassandra-protocol supports but shotover does not yet support
                        return Err(reject_protocol_version(version.into()));
                    }

                    let mut message = Message::from_bytes(bytes.freeze(), MessageType::Cassandra);

                    // if is startup message, reject compression because shotover does not support
                    if let Some(Frame::Cassandra(frame)) = message.frame() {
                        if let CassandraOperation::Startup(startup) = &mut frame.operation {
                            if let Some(compression) = startup.map.get("COMPRESSION") {
                                return Err(reject_compression(frame.stream_id, compression));
                            }
                        }
                    }

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
                Err(CheckEnvelopeSizeError::NotEnoughBytes) => {
                    if self.messages.is_empty() || src.remaining() != 0 {
                        return Ok(None);
                    } else {
                        return Ok(Some(std::mem::take(&mut self.messages)));
                    }
                }
                Err(CheckEnvelopeSizeError::UnsupportedVersion(version)) => {
                    return Err(reject_protocol_version(version));
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

fn reject_compression(stream_id: i16, compression: &String) -> CodecReadError {
    info!(
        "Rejecting compression option {} (configure the client to use no compression)",
        compression
    );

    CodecReadError::RespondAndThenCloseConnection(vec![Message::from_frame(Frame::Cassandra(
        CassandraFrame {
            version: Version::V4,
            stream_id,
            operation: CassandraOperation::Error(ErrorBody {
                message: format!("Unsupported compression type {}", compression),
                ty: ErrorType::Protocol,
            }),
            tracing: Tracing::Response(None),
            warnings: vec![],
        },
    ))])
}

impl Encoder<Messages> for CassandraCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        for m in item {
            let start = dst.len();
            // TODO: always check if cassandra message
            match m.into_encodable(MessageType::Cassandra)? {
                Encodable::Bytes(bytes) => dst.extend_from_slice(&bytes),
                Encodable::Frame(frame) => self.encode_raw(frame.into_cassandra().unwrap(), dst),
            }
            tracing::debug!(
                "outgoing cassandra message:\n{}",
                pretty_hex::pretty_hex(&&dst[start..])
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::codec::cassandra::CassandraCodec;
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
        codec: &mut CassandraCodec,
        raw_frame: &[u8],
        expected_messages: Vec<Message>,
    ) {
        // test decode
        let decoded_messages = codec
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
            codec.encode(parsed_messages, &mut dest).unwrap();
            assert_eq!(raw_frame, &dest.to_vec());
        }

        // test encode round trip - raw messages
        {
            let mut dest = BytesMut::new();
            codec.encode(decoded_messages, &mut dest).unwrap();
            assert_eq!(raw_frame, &dest.to_vec());
        }
    }

    #[test]
    fn test_codec_startup() {
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
        let mut codec = CassandraCodec::new();
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
