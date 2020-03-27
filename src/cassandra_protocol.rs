use tokio_util::codec::{Decoder, Encoder};
use std::io::Cursor;

use num_enum::TryFromPrimitive;
use std::convert::{TryFrom, From};
use bytes::{Buf, BufMut, BytesMut, Bytes};
use std::{fmt, io, str, usize};
use num_enum::IntoPrimitive;
use std::collections::HashSet;
use cdrs::query::QueryFlags;
use cdrs::query::QueryParams;
use cdrs::frame::frame_result::*;
use cdrs::frame::traits::FromCursor;
use cdrs::types::rows::Row;
use serde::export::fmt::Debug;

// use rust_praxctice::generic_protocol::{MessageContainer};

const PROTOCOL_VERSION_MASK: u8 = 0x7f;

#[derive(PartialEq, Debug, Clone, Hash, Eq)]
pub struct CassandraFrame {
    pub header: CassandraMessageHeader,
    pub body: BytesMut,
}

#[derive(IntoPrimitive, TryFromPrimitive, Eq, PartialEq, Debug, Copy, Clone, Hash)]
#[repr(u8)]
pub enum Flag {
    COMPRESSED,
    TRACING,
    CUSTOM_PAYLOAD,
    WARNING,
    USE_BETA,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum RawFrame {
    CASSANDRA(CassandraFrame),
    NONE
}


#[derive(Debug)]
pub struct Query {
    pub query_string: Bytes,
    pub query_flags: Vec<QueryFlags>,
}

#[derive(Debug)]
pub struct Results {
    pub metadata: Option<RowsMetadata>,
    pub rows: Option<Vec<Row>>,
}


impl CassandraFrame {
    fn generate_header_bytes(&mut self) -> BytesMut {
        let mut dst = BytesMut::with_capacity(9);
        dst.put_u8(self.header.version_num); //TODO change direction to the binary
        dst.put_u8(self.header.flags);
        dst.put_u16(self.header.stream_id);
        dst.put_u8(self.header.message_type as u8);
        dst.put_u32(self.body.len() as u32);
        return dst;
    }

    pub fn decode_flags(&mut self) -> HashSet<Flag> {
        let mut flags = HashSet::new();
        for x in 0..5 {
            if self.header.flags & (1 << x) != 0 {
                if let Ok(flag) = Flag::try_from(x) {
                    flags.insert(flag);
                }
            }
        }
        flags
    }

    pub fn get_type(&mut self) -> Option<MessageType> {
        if let Ok(m_type) = MessageType::try_from(self.header.message_type) {
            return Some(m_type);
        }
        None
    }


    pub fn get_results(&mut self) -> Option<Results> {
        let body_copy = self.body.clone().freeze();
        if let Some(message_type) = self.get_type() {
            match message_type {
                MessageType::Result => {
                    let mut cursor = Cursor::new(body_copy.as_ref());
                    // let kind = ResultKind::from_u32(body_copy.get_u32());
                    let res_body = ResResultBody::from_cursor(&mut cursor);

                    match res_body {
                        Ok(result_body_ok) => {
                            return Some(Results{
                                metadata: result_body_ok.as_rows_metadata(),
                                rows: result_body_ok.into_rows()
                            })
                        },
                        Err(e) => {}
                    }
                },
                _ => {return None}
            }
        }
        None
    }

    pub fn get_query(&mut self) -> Option<Query> {
        let mut body_copy = self.body.clone().freeze();
        let decoded_flags = self.decode_flags();
        //TODO: Handle compression
        if decoded_flags.contains(&Flag::TRACING) {
            let trace_id = body_copy.get_u16();
            //Handle trace UUID
        }

        if decoded_flags.contains(&Flag::WARNING) {
            let length = body_copy.get_u16();
            let warnings = body_copy.split_to(length as usize);
            //Handle warning flag
        }

        if decoded_flags.contains(&Flag::CUSTOM_PAYLOAD) {
            let length = body_copy.get_u16();
            let payload_bytesmap = body_copy.split_to(length as usize);
        }

        if let Some(message_type) = self.get_type() {
            match message_type {
                MessageType::Query => {
                    let length = body_copy.get_u32();
                    let query_string = body_copy.split_to(length as usize);
                    let flags = QueryParams::parse_query_flags(body_copy.get_u8());
                    return Some(Query{
                        query_string: query_string,
                        query_flags: flags
                    });
                },
                _ => {
                    return None;
                }
            }
        }
        None
    }
}

pub struct CassandraCodec {
   state: DecodeState,
   current_header: Option<CassandraMessageHeader>,
}

#[derive(Debug, Clone, Copy)]
enum DecodeState {
    Head,
    Data(usize),
}

#[derive(PartialEq, Debug, Copy, Clone, Hash, Eq)]
pub struct CassandraMessageHeader {
    pub message_type: u8,
    pub version_num: u8,
    pub flags: u8,
    pub body_length: u32,
    pub stream_id: u16,
    pub direction: Direction,
}

#[derive(TryFromPrimitive, Eq, PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
pub enum MessageType {
    Error          ,
    Startup        ,
    Ready          ,
    Authenticate   ,
    Credentials    ,
    Options        ,
    Supported      ,
    Query          ,
    Result         ,
    Prepare        ,
    Execute        ,
    Register       ,
    Event          ,
    Batch          ,
    AuthChallenge ,
    AuthResponse  ,
    AuthSuccess
}


impl CassandraCodec {
   pub fn new() -> CassandraCodec {
        CassandraCodec{
           state: DecodeState::Head,
           current_header: None,
        }
   }

   fn decode_data(&self, n: usize, src: &mut BytesMut) -> io::Result<Option<BytesMut>> {
      // At this point, the buffer has already had the required capacity
      // reserved. All there is to do is read.
      if src.len() < n {
          return Ok(None);
      }

      Ok(Some(src.split_to(n)))
  }

   fn decode_head(&mut self, src: &mut BytesMut) -> io::Result<Option<usize>> {
      let head_len = 9;
      let max_frame_len = 1024 * 1024 * 15; //15MB

      if src.len() < head_len {
          // Not enough data
          return Ok(None);
      }

      let n = {
        //  let version_num = first_byte & PROTOCOL_VERSION_MASK;
         let version_num = src.get_u8();
        //  if version_num != 5 || version_num {
        //      return Err(io::Error::new(
        //        io::ErrorKind::InvalidInput,
        //        "wrong protocol version",
        //    ));
        //  }
         let direction = if (version_num & PROTOCOL_VERSION_MASK) & 0x80 == 0 { Direction::Request } else { Direction::Response};
         let flags = src.get_u8();
         let stream_id = src.get_u16();
         let opcode = src.get_u8(); 
         let n = src.get_u32() as usize; //TODO: defensive check
         if (n as usize) > max_frame_len {
             //TODO throw error
             return Err(io::Error::new(
               io::ErrorKind::InvalidInput,
               "Max frame length exceeded",
           ));
         }
         self.current_header = Some(CassandraMessageHeader {
            message_type: opcode,
            version_num: version_num,
            flags: flags,
            body_length: n as u32,
            stream_id: stream_id,
            direction: direction,
         });
         n
         // return Ok(Some(CassandraMessage {
         //    message_type: MessageType::try_from(opcode).unwrap(),
         //    version_num: version_num,
         //    stream_id: stream_id,
         //    flags: flags,
         //    body_length: length,
         //    direction: direction,
         //    body: body
         // }))

          // Error handling
      };
      src.reserve(n);
      Ok(Some(n))
  }
}

impl Decoder for CassandraCodec {
   type Item = CassandraFrame;
   type Error = CassandraCodecError;

   fn decode(&mut self, src: &mut BytesMut) -> Result<Option<CassandraFrame>, Self::Error> {
      let n = match self.state {
         DecodeState::Head => match self.decode_head(src)? {
             Some(n) => {
                 self.state = DecodeState::Data(n);
                 n
             }
             None => return Ok(None),
         },
         DecodeState::Data(n) => n,
     };

     match self.decode_data(n, src)? {
         Some(data) => {
             // Update the decode state
             self.state = DecodeState::Head;

             // Make sure the buffer has enough space to read the next head
             src.reserve(9);

             match self.current_header {
                Some(x) => Ok(Some(CassandraFrame{
                    header: x,
                    body: data
                })),
                None => Ok(None)
             }
            
         }
         None => Ok(None),
     }
   }
}

impl Encoder for CassandraCodec {
    type Item = CassandraFrame;
    type Error = CassandraCodecError;

    fn encode(&mut self, message: CassandraFrame, dst: &mut BytesMut) -> Result<(), CassandraCodecError> {
       dst.put_u8(message.header.version_num); //TODO change direction to the binary
       dst.put_u8(message.header.flags);
       dst.put_u16(message.header.stream_id);
       dst.put_u8(message.header.message_type as u8);
       dst.put_u32(message.body.len() as u32);
       dst.put(message.body);
       Ok(())
    }
}

#[derive(Debug)]
pub enum CassandraCodecError {  
    ProtocolError,
    Io(io::Error),
}

impl fmt::Display for CassandraCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CassandraCodecError::ProtocolError => write!(f,"Protocol error"),
            CassandraCodecError::Io(e) => write!(f, "{}", e)
        }
    }
}

impl From<io::Error> for CassandraCodecError {
    fn from(e: io::Error) -> CassandraCodecError {
        CassandraCodecError::Io(e)
    }
}

impl std::error::Error for CassandraCodecError {}

#[derive(PartialEq, Debug, Copy, Clone, Hash, Eq)]
pub enum Direction {
    Request = 0x7F,
    Response = 0x80
}