use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use cassandra_proto::compressors::no_compression::NoCompression;
use cassandra_proto::consistency::Consistency;
use cassandra_proto::frame::frame_response::ResponseBody;
use cassandra_proto::frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ResResultBody, RowsMetadata,
};
use cassandra_proto::frame::parser::FrameHeader;
use cassandra_proto::frame::{parser, Flag, Frame, IntoBytes, Opcode, Version};
use cassandra_proto::query::QueryValues;
use cassandra_proto::types::value::Value as CValue;
use cassandra_proto::types::{CBytes, CInt, CString};

use byteorder::{BigEndian, WriteBytesExt};
use shotover_transforms::ast::ASTHolder;
use shotover_transforms::RawFrame;
use shotover_transforms::{
    Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
};

use shotover_transforms::protocol::cassandra::{
    build_cassandra_query_frame, build_cassandra_response_frame, build_response_message,
};
use tokio_util::codec::{Decoder, Encoder};
use tracing::trace;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct CassandraCodec2 {
    compressor: NoCompression,
    current_head: Option<FrameHeader>,
    pk_col_map: HashMap<String, Vec<String>>,
    bypass: bool,
}

impl CassandraCodec2 {
    pub fn new(pk_col_map: HashMap<String, Vec<String>>, bypass: bool) -> CassandraCodec2 {
        CassandraCodec2 {
            compressor: NoCompression::new(),
            current_head: None,
            pk_col_map,
            bypass,
        }
    }
}

impl CassandraCodec2 {
    fn decode_raw(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        trace!("Parsing C* frame");
        let v = parser::parse_frame(src, &self.compressor, &self.current_head);
        match v {
            Ok((r, h)) => {
                self.current_head = h;
                Ok(r)
            }
            // Note these should be parse errors, not actual protocol errors
            Err(e) => Err(anyhow!(e)),
        }
    }

    fn encode_raw(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        let buffer = item.into_cbytes();
        dst.put(buffer.as_slice());
        Ok(())
    }

    pub fn process_cassandra_frame(&self, frame: Frame) -> Messages {
        if self.bypass {
            return Messages::new_single_bypass(RawFrame::CASSANDRA(frame));
        }

        match frame.opcode {
            Opcode::Query => {
                if let Ok(body) = frame.get_body() {
                    if let ResponseBody::Query(brq) = body {
                        let parsed_string =
                            shotover_transforms::protocol::cassandra::parse_query_string(
                                brq.query.clone().into_plain(),
                                &self.pk_col_map,
                            );
                        if parsed_string.ast.is_none() {
                            // TODO: Currently this will probably catch schema changes that don't match
                            // what the SQL parser expects
                            return Messages::new_single_bypass(RawFrame::CASSANDRA(frame));
                        }
                        return Messages::new_single_query(
                            QueryMessage {
                                query_string: brq.query.into_plain(),
                                namespace: parsed_string.namespace.unwrap(),
                                primary_key: parsed_string.primary_key,
                                query_values: parsed_string.colmap,
                                projection: parsed_string.projection,
                                query_type: QueryType::Read,
                                ast: parsed_string.ast.map(ASTHolder::SQL),
                            },
                            false,
                            RawFrame::CASSANDRA(frame),
                        );
                    }
                }
                Messages::new_single_bypass(RawFrame::CASSANDRA(frame))
            }
            Opcode::Result => build_response_message(frame, None),
            Opcode::Error => {
                if let Ok(body) = frame.get_body() {
                    if let ResponseBody::Error(e) = body {
                        return Messages::new_single_response(
                            QueryResponse {
                                matching_query: None,
                                result: None,
                                error: Some(Value::Strings(e.message.as_plain())),
                                response_meta: None,
                            },
                            false,
                            RawFrame::CASSANDRA(frame),
                        );
                    }
                }
                Messages::new_single_bypass(RawFrame::CASSANDRA(frame))
            }
            _ => Messages::new_single_bypass(RawFrame::CASSANDRA(frame)),
        }
    }
}

impl Decoder for CassandraCodec2 {
    type Item = Messages;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        Ok(self
            .decode_raw(src)?
            .map(|f| self.process_cassandra_frame(f)))
    }
}

fn get_cassandra_frame(rf: RawFrame) -> Result<Frame> {
    if let RawFrame::CASSANDRA(frame) = rf {
        Ok(frame)
    } else {
        warn!("Unsupported Frame detected - Dropping Frame {:?}", rf);
        Err(anyhow!("Unsupported frame found, not sending"))
    }
}

impl CassandraCodec2 {
    fn encode_message(&mut self, item: Message) -> Result<Frame> {
        let frame = if !item.modified {
            get_cassandra_frame(item.original)?
        } else {
            match item.details {
                MessageDetails::Bypass(message) => self.encode_message(Message {
                    details: *message,
                    modified: item.modified,
                    original: item.original,
                })?,
                MessageDetails::Query(qm) => {
                    build_cassandra_query_frame(qm, Consistency::LocalQuorum)
                }
                MessageDetails::Response(qr) => {
                    build_cassandra_response_frame(qr, get_cassandra_frame(item.original)?)
                }
                MessageDetails::Unknown => get_cassandra_frame(item.original)?,
            }
        };
        Ok(frame)
    }
}

impl Encoder<Messages> for CassandraCodec2 {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        item.into_iter().try_for_each(|m: Message| {
            let frame = self.encode_message(m)?;
            self.encode_raw(frame, dst)
        })
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use std::collections::HashMap;

    use anyhow::{anyhow, Result};
    use bytes::BytesMut;

    use crate::cassandra_protocol2::CassandraCodec2;
    use hex_literal::hex;
    use shotover_transforms::ast::ASTHolder;
    use shotover_transforms::{Message, MessageDetails, QueryMessage};
    use tokio_util::codec::{Decoder, Encoder};

    const STARTUP_BYTES: [u8; 31] =
        hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");

    const READY_BYTES: [u8; 9] = hex!("840000000200000000");

    const REGISTER_BYTES: [u8; 58] = hex!(
        "040000010b000000310003000f544f504f4c4f47595f4348414e4745
    000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
    );

    const QUERY_BYTES: [u8; 60] = hex!(
        "0400000307000000330000002c53454c454354202a2046524f4d20737973
    74656d2e6c6f63616c205748455245206b65793d276c6f63616c27000100"
    );

    const RESULT_BYTES: [u8; 162] = hex!(
        "840000020800000099000000020000000100000009000673797374656
    d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
    65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
    573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
    );

    fn build_bytesmut(slice: &[u8]) -> BytesMut {
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(slice);
        BytesMut::from(v.as_slice())
    }

    fn test_frame(codec: &mut CassandraCodec2, raw_frame: &[u8]) {
        let mut bytes: BytesMut = build_bytesmut(raw_frame);
        if let Ok(Some(message)) = codec.decode(&mut bytes) {
            let mut dest: BytesMut = BytesMut::new();
            if let Ok(()) = codec.encode(message, &mut dest) {
                assert_eq!(build_bytesmut(raw_frame), dest)
            }
        } else {
            panic!("Could not decode frame");
        }
    }

    #[test]
    fn test_startup_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &STARTUP_BYTES);
    }

    #[test]
    fn test_ready_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &READY_BYTES);
    }

    #[test]
    fn test_register_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &REGISTER_BYTES);
    }

    #[test]
    fn test_result_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &RESULT_BYTES);
    }

    #[test]
    fn test_query_codec() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );
        let mut codec = CassandraCodec2::new(pk_map, false);
        test_frame(&mut codec, &QUERY_BYTES);
    }

    fn remove_whitespace(s: &mut String) {
        s.retain(|c| !c.is_whitespace());
    }

    #[test]
    fn test_query_codec_ast_builder() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let mut codec = CassandraCodec2::new(pk_map, false);
        let mut bytes: BytesMut = build_bytesmut(&QUERY_BYTES);
        if let Ok(Some(messages)) = codec.decode(&mut bytes) {
            let answer: Result<()> = messages
                .into_iter()
                .map(|m: Message| {
                    if let MessageDetails::Query(QueryMessage {
                        query_string,
                        namespace: _,
                        primary_key: _,
                        query_values: _,
                        projection: _,
                        query_type: _,
                        ast: Some(ASTHolder::SQL(ast)),
                    }) = m.details
                    {
                        let mut query_s = query_string.clone();
                        let mut ast_string = format!("{}", ast);

                        remove_whitespace(&mut query_s);
                        remove_whitespace(&mut ast_string);

                        println!("{}", query_string);
                        println!("{}", ast);
                        assert_eq!(query_s, ast_string);
                        Ok(())
                    } else {
                        Err(anyhow!("uh oh"))
                    }
                })
                .collect();
            assert_eq!(true, answer.is_ok());
        } else {
            panic!("Could not decode frame");
        }
    }
}
