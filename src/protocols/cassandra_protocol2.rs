use crate::message::{QueryMessage, QueryResponse, Value};
use crate::protocols::cassandra_helper::{rebuild_ast_in_message, rebuild_query_string_from_ast};
use byteorder::{BigEndian, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use cassandra_proto::compressors::no_compression::NoCompression;
use cassandra_proto::consistency::Consistency;
use cassandra_proto::error::Error;
use cassandra_proto::frame::frame_response::ResponseBody;
use cassandra_proto::frame::frame_result::{
    BodyResResultRows, ColSpec, ColType, ColTypeOption, ResResultBody, RowsMetadata,
};
use cassandra_proto::frame::parser::FrameHeader;
use cassandra_proto::frame::{parser, Flag, Frame, IntoBytes, Opcode, Version};
use cassandra_proto::query::{QueryParams, QueryValues};
use cassandra_proto::types::value::Value as CValue;
use cassandra_proto::types::{CBytes, CInt, CString};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub struct CassandraCodec2 {
    compressor: NoCompression,
    current_head: Option<FrameHeader>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash, Serialize, Deserialize)]
pub enum RawFrame {
    CASSANDRA(Frame),
    NONE,
}

impl CassandraCodec2 {
    pub fn new() -> CassandraCodec2 {
        return CassandraCodec2 {
            compressor: NoCompression::new(),
            current_head: None,
        };
    }

    pub fn build_cassandra_query_frame(
        mut query: QueryMessage,
        default_consistency: Consistency,
    ) -> Frame {
        rebuild_ast_in_message(&mut query);
        rebuild_query_string_from_ast(&mut query);
        let QueryMessage {
            original,
            query_string,
            namespace,
            primary_key,
            query_values,
            projection,
            query_type,
            ast,
        } = query;

        let values: Option<QueryValues> = Some(QueryValues::SimpleValues(
            query_values
                .unwrap()
                .values()
                .map(|x| CValue::new_normal(x.clone()))
                .collect(),
        ));
        let with_names: Option<bool> = Some(false);
        let page_size: Option<i32> = None;
        let paging_state: Option<CBytes> = None;
        let serial_consistency: Option<Consistency> = None;
        let timestamp: Option<i64> = None;
        let flags: Vec<Flag> = vec![];
        return Frame::new_req_query(
            query_string,
            default_consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp,
            flags,
        );
    }

    pub fn build_cassandra_response_frame(resp: QueryResponse) -> Frame {
        if let Some(Value::Rows(rows)) = resp.result {
            if let Some(ref query) = resp.matching_query {
                if let RawFrame::CASSANDRA(ref query_frame) = query.original {
                    if let Some(ref proj) = query.projection {
                        let col_spec = proj
                            .iter()
                            .map(|x| {
                                ColSpec {
                                    ksname: Some(CString::new(
                                        query.namespace.get(0).unwrap().clone(),
                                    )),
                                    tablename: Some(CString::new(
                                        query.namespace.get(1).unwrap().clone(),
                                    )),
                                    name: CString::new(x.clone()),
                                    col_type: ColTypeOption {
                                        id: ColType::Ascii, // todo: get types working
                                        value: None,
                                    },
                                }
                            })
                            .collect();
                        let count = rows.get(0).unwrap().len() as i32;
                        let metadata = RowsMetadata {
                            flags: 0,
                            columns_count: count,
                            paging_state: None,
                            // global_table_space: Some(query.namespace.iter()
                            //     .map(|x| CString::new(x.clone())).collect()),
                            global_table_space: None,
                            col_specs: col_spec,
                        };

                        let result_bytes = rows
                            .iter()
                            .map(|i| {
                                let rr: Vec<CBytes> = i
                                    .iter()
                                    .map(|j| {
                                        let rb: CBytes = CBytes::new(match j {
                                            Value::NULL => (-1 as CInt).into_cbytes(),
                                            Value::Bytes(x) => x.to_vec(),
                                            Value::Strings(x) => {
                                                Vec::from(x.clone().as_bytes())
                                                // CString::new(x.clone()).into_cbytes()
                                            }
                                            Value::Integer(x) => {
                                                let mut temp: Vec<u8> = Vec::new();
                                                let _ = temp.write_i64::<BigEndian>(*x).unwrap();
                                                temp
                                                // Decimal::new(*x, 0).into_cbytes()
                                            }
                                            Value::Float(x) => {
                                                let mut temp: Vec<u8> = Vec::new();
                                                let _ = temp.write_f64::<BigEndian>(*x).unwrap();
                                                temp
                                            }
                                            Value::Boolean(x) => {
                                                let mut temp: Vec<u8> = Vec::new();
                                                let _ =
                                                    temp.write_i32::<BigEndian>(*x as i32).unwrap();
                                                temp
                                                // (x.clone() as CInt).into_cbytes()
                                            }
                                            Value::Timestamp(x) => {
                                                Vec::from(x.to_rfc2822().clone().as_bytes())
                                            }
                                            Value::Rows(x) => unreachable!(),
                                            Value::Document(x) => unreachable!(),
                                            Value::List(_) => unreachable!(),
                                            Value::Inet(i) => {unreachable!()}
                                            Value::NamedRows(_) => {unreachable!()}
                                        });
                                        return rb;
                                    })
                                    .collect();
                                return rr;
                            })
                            .collect();

                        let response = ResResultBody::Rows(BodyResResultRows {
                            metadata,
                            rows_count: rows.len() as CInt,
                            rows_content: result_bytes,
                        });

                        return Frame {
                            version: Version::Response,
                            flags: query_frame.flags.clone(),
                            opcode: Opcode::Result,
                            stream: query_frame.stream,
                            body: response.into_cbytes(),
                            tracing_id: query_frame.tracing_id,
                            warnings: Vec::new(),
                        };
                    }
                }
            }
        }
        unreachable!()
    }
}

impl Decoder for CassandraCodec2 {
    type Item = Frame;
    type Error = Error;

    fn decode<'a>(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let v = parser::parse_frame(src, &self.compressor, &self.current_head);
        match v {
            Ok((r, h)) => {
                self.current_head = h;
                return Ok(r);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

impl Encoder<Frame> for CassandraCodec2 {
    type Error = Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let buffer = item.into_cbytes();
        dst.put(buffer.as_slice());
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::message::{Message, QueryMessage};
    use crate::protocols::cassandra_helper::process_cassandra_frame;
    use crate::protocols::cassandra_protocol2::CassandraCodec2;
    use bytes::BytesMut;
    use hex_literal::hex;
    use rdkafka::message::ToBytes;
    use std::collections::HashMap;
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
        return BytesMut::from(v.to_bytes());
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
        let mut codec = CassandraCodec2::new();
        test_frame(&mut codec, &STARTUP_BYTES);
    }

    #[test]
    fn test_ready_codec() {
        let mut codec = CassandraCodec2::new();
        test_frame(&mut codec, &READY_BYTES);
    }

    #[test]
    fn test_register_codec() {
        let mut codec = CassandraCodec2::new();
        test_frame(&mut codec, &REGISTER_BYTES);
    }

    #[test]
    fn test_result_codec() {
        let mut codec = CassandraCodec2::new();
        test_frame(&mut codec, &RESULT_BYTES);
    }

    #[test]
    fn test_query_codec() {
        let mut codec = CassandraCodec2::new();
        test_frame(&mut codec, &QUERY_BYTES);
    }

    #[test]
    fn test_query_codec_ast_builder() {
        let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
        pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
        pk_map.insert(
            "test.clustering".to_string(),
            vec!["pk".to_string(), "clustering".to_string()],
        );

        let mut codec = CassandraCodec2::new();
        let mut bytes: BytesMut = build_bytesmut(&QUERY_BYTES);
        if let Ok(Some(frame)) = codec.decode(&mut bytes) {
            let message = process_cassandra_frame(frame, &pk_map);
            if let Message::Query(QueryMessage {
                original,
                query_string,
                namespace,
                primary_key,
                query_values,
                projection,
                query_type,
                ast: Some(ast),
            }) = message
            {
                println!("{}", query_string);
                println!("{}", ast);
                assert_eq!(format!("{}", query_string), format!("{}", ast));
            }
        } else {
            panic!("Could not decode frame");
        }
    }
}
