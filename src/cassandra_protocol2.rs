use cassandra_proto::frame::{parser, Frame, IntoBytes};
use std::io::{Cursor, Read};
use bytes::{Buf, BufMut, BytesMut, Bytes};
use tokio_util::codec::{Decoder, Encoder};
use std::cell::RefCell;
use cassandra_proto::compression::Compressor;
use std::borrow::{Borrow, BorrowMut};
use cassandra_proto::compressors::no_compression::NoCompression;
use cassandra_proto::error::Error;
use cassandra_proto::frame::parser::FrameHeader;

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