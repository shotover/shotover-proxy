use super::{CodecBuilder, CodecReadError, CodecWriteError, Direction};
use crate::frame::{
    opensearch::{HttpHead, RequestParts, ResponseParts},
    Frame, MessageType, OpenSearchFrame,
};
use crate::message::{Encodable, Message, Messages};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use http::{header, HeaderName, HeaderValue, Method, Request, Response};
use std::sync::{Arc, Mutex};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone)]
pub struct OpenSearchCodecBuilder {
    direction: Direction,
}

impl CodecBuilder for OpenSearchCodecBuilder {
    type Decoder = OpenSearchDecoder;
    type Encoder = OpenSearchEncoder;

    fn new(direction: Direction) -> Self {
        Self { direction }
    }

    fn build(&self) -> (OpenSearchDecoder, OpenSearchEncoder) {
        let last_outgoing_method = Arc::new(Mutex::new(None));

        (
            OpenSearchDecoder::new(self.direction, last_outgoing_method.clone()),
            OpenSearchEncoder::new(self.direction, last_outgoing_method),
        )
    }

    fn websocket_subprotocol(&self) -> &'static str {
        "opensearch"
    }
}

pub struct OpenSearchDecoder {
    direction: Direction,
    state: State,
    last_outgoing_method: Arc<Mutex<Option<Method>>>,
}

struct DecodeResult {
    body_start: usize,
    http_headers: HttpHead,
    content_length: usize,
}

impl OpenSearchDecoder {
    pub fn new(direction: Direction, last_outgoing_method: Arc<Mutex<Option<Method>>>) -> Self {
        Self {
            direction,
            state: State::ParsingResponse,
            last_outgoing_method,
        }
    }

    fn decode_request(&self, src: &mut BytesMut) -> Result<Option<DecodeResult>> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut request = httparse::Request::new(&mut headers);

        let body_start = match request.parse(src)? {
            httparse::Status::Complete(body_start) => body_start,
            httparse::Status::Partial => return Ok(None),
        };
        match request.version.unwrap() {
            1 => (),
            version => {
                return Err(anyhow!(
                    "HTTP version: {} unsupported. Requires HTTP/1",
                    version
                ))
            }
        }

        let mut builder = Request::builder()
            .method(request.method.unwrap())
            .uri(request.path.unwrap());

        let builder_headers = builder.headers_mut().unwrap();

        for header in request.headers {
            if header.name.is_empty() && header.value.is_empty() {
                break;
            }
            builder_headers.insert(
                HeaderName::from_bytes(header.name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(header.value).unwrap(),
            );
        }

        let r = builder.body(()).unwrap();
        let content_length = match r.headers().get(header::CONTENT_LENGTH) {
            Some(content_length) => match atoi::atoi(content_length.as_bytes()) {
                Some(content_length) => content_length,
                None => return Err(anyhow!("content-length header invalid")),
            },
            None => 0,
        };
        let (parts, _) = r.into_parts();
        Ok(Some(DecodeResult {
            body_start,
            http_headers: HttpHead::Request(RequestParts {
                method: parts.method,
                uri: parts.uri,
                version: parts.version,
                headers: parts.headers,
            }),
            content_length,
        }))
    }

    fn decode_response(&self, src: &mut BytesMut) -> Result<Option<DecodeResult>> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut response = httparse::Response::new(&mut headers);

        let body_start = match response.parse(src) {
            Ok(httparse::Status::Complete(body_start)) => body_start,
            Ok(httparse::Status::Partial) => return Ok(None),
            Err(err) => return Err(anyhow!("error parsing response: {}", err)),
        };
        match response.version.unwrap() {
            1 => (),
            version => {
                return Err(anyhow!(
                    "HTTP version: {} unsupported. Requires HTTP/1",
                    version
                ))
            }
        }

        let mut builder = Response::builder().status(response.code.unwrap());

        let builder_headers = builder.headers_mut().unwrap();

        for header in response.headers {
            if header.name.is_empty() && header.value.is_empty() {
                break;
            }
            builder_headers.insert(
                HeaderName::from_bytes(header.name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(header.value).unwrap(),
            );
        }

        let r = builder.body(()).unwrap();
        let content_length = match r.headers().get(header::CONTENT_LENGTH) {
            Some(cl) => match atoi::atoi(cl.as_bytes()) {
                Some(cl) => cl,
                None => return Err(anyhow!("content-length header invalid")),
            },
            None => 0,
        };
        let (parts, _) = r.into_parts();
        Ok(Some(DecodeResult {
            body_start,
            http_headers: HttpHead::Response(ResponseParts {
                version: parts.version,
                status: parts.status,
                headers: parts.headers,
            }),
            content_length,
        }))
    }
}

#[derive(Debug)]
enum State {
    ParsingResponse,
    ReadingBody(HttpHead, usize),
}

impl Decoder for OpenSearchDecoder {
    type Item = Messages;
    type Error = CodecReadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, CodecReadError> {
        if src.is_empty() {
            return Ok(None);
        }

        loop {
            match std::mem::replace(&mut self.state, State::ParsingResponse) {
                State::ParsingResponse => {
                    let decode_result = if self.direction == Direction::Source {
                        self.decode_request(src).map_err(CodecReadError::Parser)?
                    } else {
                        self.decode_response(src).map_err(CodecReadError::Parser)?
                    };

                    if let Some(DecodeResult {
                        body_start,
                        http_headers,
                        content_length,
                    }) = decode_result
                    {
                        self.state = State::ReadingBody(http_headers, content_length);
                        src.advance(body_start);
                    } else {
                        return Ok(None);
                    };
                }
                State::ReadingBody(http_headers, content_length) => {
                    if let Some(Method::HEAD) = *self.last_outgoing_method.lock().unwrap() {
                        return Ok(Some(vec![Message::from_frame(Frame::OpenSearch(
                            OpenSearchFrame::new(http_headers, bytes::Bytes::new()),
                        ))]));
                    }

                    if src.len() < content_length {
                        self.state = State::ReadingBody(http_headers, content_length);
                        return Ok(None);
                    }

                    let body = src.split_to(content_length).freeze();
                    return Ok(Some(vec![Message::from_frame(Frame::OpenSearch(
                        OpenSearchFrame::new(http_headers, body),
                    ))]));
                }
            }
        }
    }
}

pub struct OpenSearchEncoder {
    direction: Direction,
    last_outgoing_method: Arc<Mutex<Option<Method>>>,
}

impl OpenSearchEncoder {
    pub fn new(direction: Direction, last_outgoing_method: Arc<Mutex<Option<Method>>>) -> Self {
        Self {
            direction,
            last_outgoing_method,
        }
    }
}

impl Encoder<Messages> for OpenSearchEncoder {
    type Error = CodecWriteError;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        item.into_iter().try_for_each(|m| {
            let start = dst.len();
            m.ensure_message_type(MessageType::OpenSearch)
                .map_err(CodecWriteError::Encoder)?;
            let result = match m.into_encodable() {
                Encodable::Bytes(bytes) => {
                    dst.extend_from_slice(&bytes);
                    Ok(())
                }
                Encodable::Frame(frame) => {
                    let opensearch_frame = frame.into_opensearch().unwrap();

                    match &opensearch_frame.headers {
                        HttpHead::Request(request_parts) => {
                            self.last_outgoing_method
                                .lock()
                                .unwrap()
                                .replace(request_parts.method.clone());

                            dst.extend_from_slice(
                                format!("{} ", request_parts.method.as_str()).as_bytes(),
                            );
                            dst.extend_from_slice(format!("{} ", request_parts.uri).as_bytes());
                            dst.extend_from_slice(b"HTTP/1.1");
                            dst.extend_from_slice(b"\r\n");

                            for (k, v) in &request_parts.headers {
                                dst.extend_from_slice(k.as_str().as_bytes());
                                dst.extend_from_slice(b": ");
                                dst.extend_from_slice(v.as_bytes());
                                dst.extend_from_slice(b"\r\n");
                            }
                        }
                        HttpHead::Response(response_parts) => {
                            *self.last_outgoing_method.lock().unwrap() = None;

                            dst.extend_from_slice(b"HTTP/1.1 ");
                            dst.extend_from_slice(format!("{}", response_parts.status).as_bytes());
                            dst.extend_from_slice(b"\r\n");

                            for (k, v) in &response_parts.headers {
                                dst.extend_from_slice(k.as_str().as_bytes());
                                dst.extend_from_slice(b": ");
                                dst.extend_from_slice(v.as_bytes());
                                dst.extend_from_slice(b"\r\n");
                            }
                        }
                    }

                    dst.extend_from_slice(b"\r\n");
                    dst.extend_from_slice(&opensearch_frame.body);

                    Ok(())
                }
            };

            tracing::debug!(
                "{}: outgoing OpenSearch message:\n{}",
                self.direction,
                pretty_hex::pretty_hex(&&dst[start..])
            );

            result.map_err(CodecWriteError::Encoder)
        })
    }
}

// #[cfg(test)]
// mod opensearch_tests {
//     use crate::codec::{opensearch::OpenSearchCodecBuilder, CodecBuilder, Direction};
//     use bytes::BytesMut;
//     use hex_literal::hex;
//     use serde_json::Value;
//     use tokio_util::codec::{Decoder, Encoder};
//
//     fn test_frame(raw_frame: &[u8], direction: Direction) {
//         let (mut decoder, mut encoder) = OpenSearchCodecBuilder::new(direction).build();
//         let message = decoder
//             .decode(&mut BytesMut::from(raw_frame))
//             .unwrap()
//             .unwrap();
//
//         println!("message: {:?}", message);
//
//         let mut dest = BytesMut::new();
//         encoder.encode(message, &mut dest).unwrap();
//
//         println!("dest: {:x?}", dest);
//         println!("raw_frame: {:?}", raw_frame);
//
//         assert_eq!(raw_frame, &dest);
//     }
//
//     const RESPONSE: [u8; 186] = *b"\
//              HTTP/1.1 200 OK\r\n\
//              date: Mon, 27 Jul 2009 12:28:53 GMT\r\n\
//              server: Apache/2.2.14 (Win32)\r\n\
//              last-modified: Wed, 22 Jul 2009 19:15:56 GMT\r\n\
//              content-length: 9\r\n\
//              content-type: text/html\r\n\r\n\
//              something";
//
//     const REQUEST: [u8; 90] = *b"\
//     POST /cgi-bin/process.cgi HTTP/1.1\r\n\
//                         connection: Keep-Alive\r\n\
//                         content-length: 9\r\n\r\n\
//                         something";
//
//     const GZIP_RESPONSE: [u8; 594] = hex!("48 54 54 50  2f 31 2e 31  20 32 30 30  20 4f 4b 0d  0a 63 6f 6e  74 65 6e 74  2d 74 79 70  65 3a 20 61  70 70 6c 69  63 61 74 69  6f 6e 2f 6a  73 6f 6e 3b  20 63 68 61  72 73 65 74  3d 55 54 46  2d 38 0d 0a  63 6f 6e 74  65 6e 74 2d  65 6e 63 6f  64 69 6e 67  3a 20 67 7a  69 70 0d 0a  63 6f 6e 74  65 6e 74 2d  6c 65 6e 67  74 68 3a 20  34 38 33 0d  0a 0d 0a 1f  8b 08 00 00  00 00 00 00  00 cc 94 c9  6e db 30 10  86 5f 45 98  b3 52 78 5f  74 33 d2 16  c8 d6 c0 4e  da 06 0d 0c  82 16 a9 98  30 25 2a 5c  0c 0b 82 df  3d 23 1a 41  63 a0 25 7b  ec 45 20 31  ff 7c 9c e1  fc 54 0b 56  a9 1d 64 a3  71 0a 56 94  9c 11 e5 2c  64 05 95 86  a7 40 cc 96  6a 66 20 6b  51 66 a9 84  6c 98 82 71  79 ce 8d 29  1c 6e fb b8  dd 89 ba e6  0c b2 5e 0a  05 15 b2 5b  0e 4e 4b a7  39 e6 3e b7  e0 31 5e 21  2a c6 0f 90  c1 27 55 f3  ca 70 aa f3  ed 85 da 18  ae f7 74 23  a4 b0 0d a4  50 29 c6 51  b2 b9 5d 15  8b d5 ed f4  a1 bc bc d9  7d bd 53 e6  d1 bc 5c 2d  31 ae 39 35  aa f2 45 35  75 a7 7c 75  5c 37 a7 5a  09 3f e4 bc  b6 02 e3 bf  85 f0 4d 25  25 ad 6b 51  bd 24 85 72  15 c3 af 4e  9e c5 3a 11  55 a2 34 e3  3a b1 2a 31  4a db c4 e7  fd 4b 91 5e  43 9c 13 d8  17 b0 5f 0b  db 3c 95 bd  c7 dc fd dc  7c a7 07 42  d8 e7 e5 02  8e c7 f4 4f  bd 5b 6e ec  c5 e9 8c ff  a8 d9 b3 aa  ce ba a3 d7  fb d5 6a 71  65 96 4d 79  7d f9 63 76  77 73 ff 85  b0 25 76 b7  3e a6 b0 15  f6 a3 3f 5a  d8 53 e9 70  28 7d 1c b6  e6 92 fa 51  64 c0 5f 01  c5 25 5e 8d  c9 95 c6 78  e5 a4 7c cf  46 87 90 f7  3b af 95 41  20 5a cf 5f  6c af 5b 9d  65 10 a3 9c  ce 11 d0 82  f0 8e b2 c2  ca ce 04 f7  68 a8 07 6f  a8 ee a4 6e  98 68 bd 1e  96 f8 57 78  3f 02 47 73  07 e1 fd 10  7c 10 81 e3  13 09 c2 07  21 f8 30 02  c7 57 1a 84  0f 43 f0 51  04 3e 8a c0  47 21 f8 38  02 f7 bf a1  c0 40 c7 21  f8 24 02 9f  44 e0 93 10  7c 1a 81 4f  23 f0 69 08  3e 8b c0 67  11 f8 2c 04  9f 47 e0 f3  08 7c be c6  97 7e 7c 03  00 00 ff ff  03 00 c2 4e  64 8e 2a 06  00 00");
//
//     // #[test]
//     // fn test_request() {
//     //     test_frame(&REQUEST, Direction::Source);
//     // }
//     //
//     // #[test]
//     // fn test_response() {
//     //     test_frame(&RESPONSE, Direction::Sink);
//     // }
//
//     #[test]
//     fn test_encoding() {
//         test_frame(&GZIP_RESPONSE, Direction::Sink);
//     }
//
//     fn decode_gzip(bytes: Vec<u8>) -> Value {
//         use libflate::gzip::Decoder;
//         use libflate::gzip::Encoder;
//         use std::io::{Read, Write};
//
//         let mut decoder = Decoder::new(&bytes[..]).unwrap();
//
//         let mut decoded_data = Vec::new();
//         decoder.read_to_end(&mut decoded_data).unwrap();
//
//         let json = serde_json::from_slice::<serde_json::Value>(&decoded_data).unwrap();
//
//         println!("decoded json: {:?}", json);
//
//         json
//     }
//
//     fn encode_gzip(value: Value) -> Vec<u8> {
//         use libflate::gzip::Decoder;
//         use libflate::gzip::Encoder;
//         use std::io::{Read, Write};
//
//         let json_bytes = serde_json::to_vec(&value).unwrap();
//
//         let mut encoder = Encoder::new(Vec::new()).unwrap();
//         encoder.write_all(&json_bytes).unwrap();
//         let encoded_data = encoder.finish().into_result().unwrap();
//
//         encoded_data
//     }
//
//     #[test]
//     fn test_gzip_codec() {
//         use libflate::gzip::Decoder;
//         use libflate::gzip::Encoder;
//         use std::io::{Read, Write};
//
//         /*         let bytes = hex!("1f  8b 08 00 00  00 00 00 00  00 cc 94 c9  6e db 30 10  86 5f 45 98  b3 52 78 5f  74 33 d2 16  c8 d6 c0 4e  da 06 0d 0c  82 16 a9 98  30 25 2a 5c  0c 0b 82 df  3d 23 1a 41  63 a0 25 7b  ec 45 20 31  ff 7c 9c e1  fc 54 0b 56  a9 1d 64 a3  71 0a 56 94  9c 11 e5 2c  64 05 95 86  a7 40 cc 96  6a 66 20 6b  51 66 a9 84  6c 98 82 71  79 ce 8d 29  1c 6e fb b8  dd 89 ba e6  0c b2 5e 0a  05 15 b2 5b  0e 4e 4b a7  39 e6 3e b7  e0 31 5e 21  2a c6 0f 90  c1 27 55 f3  ca 70 aa f3  ed 85 da 18  ae f7 74 23  a4 b0 0d a4  50 29 c6 51  b2 b9 5d 15  8b d5 ed f4  a1 bc bc d9  7d bd 53 e6  d1 bc 5c 2d  31 ae 39 35  aa f2 45 35  75 a7 7c 75  5c 37 a7 5a  09 3f e4 bc  b6 02 e3 bf  85 f0 4d 25  25 ad 6b 51  bd 24 85 72  15 c3 af 4e  9e c5 3a 11  55 a2 34 e3  3a b1 2a 31  4a db c4 e7  fd 4b 91 5e  43 9c 13 d8  17 b0 5f 0b  db 3c 95 bd  c7 dc fd dc  7c a7 07 42  d8 e7 e5 02  8e c7 f4 4f  bd 5b 6e ec  c5 e9 8c ff  a8 d9 b3 aa  ce ba a3 d7  fb d5 6a 71  65 96 4d 79  7d f9 63 76  77 73 ff 85  b0 25 76 b7  3e a6 b0 15  f6 a3 3f 5a  d8 53 e9 70  28 7d 1c b6  e6 92 fa 51  64 c0 5f 01  c5 25 5e 8d  c9 95 c6 78  e5 a4 7c cf  46 87 90 f7  3b af 95 41  20 5a cf 5f  6c af 5b 9d  65 10 a3 9c  ce 11 d0 82  f0 8e b2 c2  ca ce 04 f7  68 a8 07 6f  a8 ee a4 6e  98 68 bd 1e  96 f8 57 78  3f 02 47 73  07 e1 fd 10  7c 10 81 e3  13 09 c2 07  21 f8 30 02  c7 57 1a 84  0f 43 f0 51  04 3e 8a c0  47 21 f8 38  02 f7 bf a1  c0 40 c7 21  f8 24 02 9f  44 e0 93 10  7c 1a 81 4f  23 f0 69 08  3e 8b c0 67  11 f8 2c 04  9f 47 e0 f3  08 7c be c6  97 7e 7c 03  00 00 ff ff  03 00 c2 4e  64 8e 2a 06  00 00"); */
//
//         let bytes =
//             hex!("1f8b0800000000000003ab56ca48cdc9c957b252502acf2fca4951aa050022aea38612000000");
//
//         let json = decode_gzip(bytes.to_vec());
//         let encoded_data = encode_gzip(json.clone());
//
//         // assert_eq!(bytes.to_vec(), encoded_data);
//         //
//         let json = decode_gzip(encoded_data.clone());
//         let encoded_data2 = encode_gzip(json.clone());
//         assert_eq!(encoded_data, encoded_data2);
//
//         //
//         // let json_bytes = serde_json::to_vec(&json).unwrap();
//         // let new_json = serde_json::from_slice::<serde_json::Value>(&json_bytes).unwrap();
//         //
//         // assert_eq!(json,new_json);
//         // assert_eq!(decoded_data, json_bytes);
//         //
//         /*        let mut encoder = Encoder::new(Vec::new()).unwrap();
//         encoder.write_all(&decoded_data).unwrap();
//         let encoded_data = encoder.finish().into_result().unwrap();
//
//         assert_eq!(encoded_data, bytes); */
//     }
// }
