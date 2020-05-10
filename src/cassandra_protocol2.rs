use cassandra_proto::frame::{parser, Frame, IntoBytes, Version, Opcode};
use std::io::{Cursor, Read};
use bytes::{Buf, BufMut, BytesMut, Bytes};
use tokio_util::codec::{Decoder, Encoder};
use std::cell::RefCell;
use cassandra_proto::compression::Compressor;
use std::borrow::{Borrow, BorrowMut};
use cassandra_proto::compressors::no_compression::NoCompression;
use cassandra_proto::error::Error;
use cassandra_proto::frame::parser::FrameHeader;
use crate::message::{Message, Value, QueryResponse};
use crate::cassandra_protocol::RawFrame;
use cassandra_proto::frame::frame_result::{ResResultBody, BodyResResultRows, RowsMetadata, ColSpec, ColTypeOption, ColType};
use cassandra_proto::frame::frame_response::ResponseBody;
use cassandra_proto::types::{CString, CBytes, CInt};
use futures::StreamExt;
use cassandra_proto::types::prelude::Decimal;
use cassandra_proto::types::list::List;

#[derive(Debug)]
pub struct CassandraCodec2 {
    compressor: NoCompression,
    current_head: Option<FrameHeader>
}

impl CassandraCodec2 {
    pub fn new() -> CassandraCodec2 {
        return CassandraCodec2 {
            compressor: NoCompression::new(),
            current_head: None
        }
    }

    pub fn build_cassandra_response_frame(resp: QueryResponse) -> Frame {
            if let Some(Value::Rows(rows)) = resp.result {
                if let Some(ref query) = resp.matching_query {
                    if let RawFrame::CASSANDRA(ref query_frame) = query.original {
                        if let Some(ref proj) = query.projection {
                            let col_spec = proj.iter().map(|x| {
                                ColSpec {
                                    ksname: Some(CString::new(query.namespace.get(0).unwrap().clone())),
                                    tablename: Some(CString::new(query.namespace.get(1).unwrap().clone())),
                                    name: CString::new(x.clone()),
                                    col_type: ColTypeOption {
                                        id: ColType::Ascii, // todo: get types working
                                        value: None
                                    }
                                }
                            }).collect();
                            let count = rows.get(0).unwrap().len() as i32;
                            let metadata = RowsMetadata {
                                flags: 0,
                                columns_count: count,
                                paging_state: None,
                                // global_table_space: Some(query.namespace.iter()
                                //     .map(|x| CString::new(x.clone())).collect()),
                                global_table_space: None,
                                col_specs: col_spec
                            };

                            let result_bytes = rows.iter().map(|i| {
                                let rr: Vec<CBytes> = i.iter().map(|j| {
                                    let rb: CBytes = CBytes::new(match j {
                                        Value::NULL => {
                                            (-1 as CInt).into_cbytes()
                                        },
                                        Value::Bytes(x) => {
                                            CBytes::new(x.to_vec()).into_cbytes()
                                        },
                                        Value::Strings(x) => {
                                            CString::new(x.clone()).into_cbytes()
                                        },
                                        Value::Integer(x) => {
                                            Decimal::new(*x, 0).into_cbytes()
                                        },
                                        Value::Float(x) => {
                                            (x.clone() as CInt).into_cbytes()
                                        },
                                        Value::Boolean(x) => {
                                            (x.clone() as CInt).into_cbytes()
                                        },
                                        Value::Timestamp(x) => {
                                            CString::new(x.to_rfc2822()).into_cbytes()
                                        },
                                        Value::Rows(x) => {
                                            unreachable!()
                                        },
                                        Value::Document(x) => {
                                            unreachable!()
                                        },
                                    });
                                    return rb;
                                }).collect();
                                return rr;
                            }).collect();

                            let response = ResResultBody::Rows(
                                BodyResResultRows {
                                    metadata,
                                    rows_count: rows.len() as CInt,
                                    rows_content: result_bytes,
                                }
                            );

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

    fn decode<'a>(&mut self, src: & mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let v = parser::parse_frame(src, &self.compressor, &self.current_head);
        match v {
            Ok((r, h)) => {
                self.current_head = h;
                return Ok(r);
            },
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