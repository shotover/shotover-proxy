use crate::frame::cassandra::CassandraOperation;
use crate::frame::{CassandraFrame, Frame, MessageType};
use crate::message::{Encodable, Message, Messages};
use crate::server::CodecReadError;
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_error::{AdditionalErrorInfo, ErrorBody};
use cassandra_protocol::frame::{CheckEnvelopeSizeError, Envelope as RawCassandraFrame, Version};
use tokio_util::codec::{Decoder, Encoder};
use tracing::info;

#[derive(Debug, Clone)]
pub struct CassandraCodec {
    compressor: Compression,
    messages: Vec<Message>,
}

impl Default for CassandraCodec {
    fn default() -> Self {
        CassandraCodec::new()
    }
}

impl CassandraCodec {
    pub fn new() -> CassandraCodec {
        CassandraCodec {
            compressor: Compression::None,
            messages: vec![],
        }
    }
}

impl CassandraCodec {
    fn encode_raw(&mut self, item: CassandraFrame, dst: &mut BytesMut) {
        let buffer = item.encode().encode_with(self.compressor).unwrap();
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

                    self.messages
                        .push(Message::from_bytes(bytes.freeze(), MessageType::Cassandra));
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
                error_code: 0xA, // https://github.com/apache/cassandra/blob/adf2f4c83a2766ef8ebd20b35b49df50957bdf5e/doc/native_protocol_v4.spec#L1053
                message: "Invalid or unsupported protocol version".into(),
                additional_info: AdditionalErrorInfo::Server,
            }),
            tracing_id: None,
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
        parse_statement_single, CassandraFrame, CassandraOperation, CassandraResult,
    };
    use crate::frame::Frame;
    use crate::message::{Message, MessageValue};
    use bytes::BytesMut;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags,
        TableSpec,
    };
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::QueryParams;
    use hex_literal::hex;
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
        let bytes = hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Startup(vec![
                0, 1, 0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78, 0, 5, 51, 46, 48, 46, 48,
            ]),
            stream_id: 0,
            tracing_id: None,
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
            tracing_id: None,
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
            tracing_id: None,
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
            operation: CassandraOperation::Register(vec![
                0, 3, 0, 15, 84, 79, 80, 79, 76, 79, 71, 89, 95, 67, 72, 65, 78, 71, 69, 0, 13, 83,
                84, 65, 84, 85, 83, 95, 67, 72, 65, 78, 71, 69, 0, 13, 83, 67, 72, 69, 77, 65, 95,
                67, 72, 65, 78, 71, 69,
            ]),
            stream_id: 1,
            tracing_id: None,
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_result() {
        let mut codec = CassandraCodec::new();
        let bytes = hex!(
            "040000020800000099000000020000000100000009000673797374656
            d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
            65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
            573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
        );
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Result(CassandraResult::Rows {
                value: MessageValue::Rows(vec![]),
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
            tracing_id: None,
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
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "SELECT * FROM system.local WHERE key = 'local'",
                )),
                params: Box::new(QueryParams::default()),
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
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single(
                    "INSERT INTO system.foo (bar) VALUES ('bar2')",
                )),
                params: Box::new(QueryParams::default()),
            },
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }
}
