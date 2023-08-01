use super::{CodecBuilder, CodecReadError, CodecWriteError, Direction};
use crate::frame::{opensearch::HttpHead, Frame, MessageType, OpenSearchFrame};
use crate::message::{Encodable, Message, Messages};
use anyhow::Result;
use bytes::{Buf, BytesMut};
use http::{header, HeaderName, HeaderValue, Request};
use std::sync::Arc;
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
        (
            OpenSearchDecoder::new(self.direction),
            OpenSearchEncoder::new(self.direction),
        )
    }

    fn websocket_subprotocol(&self) -> &'static str {
        "opensearch" //TODO
    }
}

pub struct OpenSearchDecoder {
    direction: Direction,
    state: State,
}

impl OpenSearchDecoder {
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            state: State::ParsingResponse,
        }
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
        if src.len() == 0 {
            return Ok(None);
        }

        loop {
            match std::mem::replace(&mut self.state, State::ParsingResponse) {
                State::ParsingResponse => {
                    let amt = {
                        let mut headers = [httparse::EMPTY_HEADER; 16];
                        let mut request = httparse::Request::new(&mut headers);
                        tracing::info!("{:x?}", &src[0..src.len()]);

                        let s = String::from_utf8_lossy(&src[0..src.len()]);
                        tracing::info!("result: {}", s);

                        let amt = match request.parse(&src[0..src.len()]).unwrap() {
                            httparse::Status::Complete(amt) => amt,
                            httparse::Status::Partial => return Ok(None),
                        };
                        match request.version.unwrap() {
                            1 => (),
                            _version => panic!("error!"), // TODO version error
                        }

                        let mut builder = Request::builder();

                        builder = builder
                            .method(request.method.unwrap().to_owned().as_str())
                            .uri(request.path.unwrap().to_owned().as_str());

                        let builder_headers = builder.headers_mut().unwrap();

                        let request_headers: Vec<(String, Vec<u8>)> = request
                            .headers
                            .iter()
                            .take_while(|h| !h.name.is_empty() && !h.value.is_empty())
                            .map(|h| (h.name.to_owned(), h.value.to_owned()))
                            .collect();

                        for (name, value) in request_headers {
                            builder_headers.insert(
                                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                                HeaderValue::from_bytes(&value).unwrap(),
                            );
                        }

                        let r = builder.body(()).unwrap();
                        let cl = match r.headers().get(header::CONTENT_LENGTH) {
                            Some(cl) => match atoi::atoi(cl.as_bytes()) {
                                Some(cl) => cl,
                                None => panic!(), // TODO content length error
                            },
                            None => 0, // TODO content length error
                        };
                        let (parts, _) = r.into_parts();
                        self.state = State::ReadingBody(HttpHead::Request(parts), cl);
                        amt
                    };
                    src.advance(amt);
                }
                State::ReadingBody(parts, cl) => {
                    if src.len() < cl {
                        self.state = State::ReadingBody(parts, cl);
                        return Ok(None);
                    }
                    let body = src.split_to(cl).freeze();
                    return Ok(Some(vec![Message::from_frame(Frame::OpenSearch(
                        OpenSearchFrame {
                            headers: Arc::new(parts),
                            body,
                        },
                    ))]));
                }
            }
        }
    }
}

pub struct OpenSearchEncoder {
    direction: Direction,
}

impl OpenSearchEncoder {
    pub fn new(direction: Direction) -> Self {
        Self { direction }
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
                    match opensearch_frame.headers.as_ref() {
                        HttpHead::Request(request_parts) => {
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
                        HttpHead::Response(_response_parts) => {
                            todo!();
                        }
                    }

                    dst.extend_from_slice(b"\r\n");
                    dst.extend_from_slice(&opensearch_frame.body);

                    tracing::debug!(
                        "{}: outgoing redis message:\n{}",
                        self.direction,
                        pretty_hex::pretty_hex(&&dst[start..])
                    );

                    Ok(())
                }
            };

            result.map_err(CodecWriteError::Encoder)
        })
    }
}

#[cfg(test)]
mod opensearch_tests {
    use crate::codec::{opensearch::OpenSearchCodecBuilder, CodecBuilder, Direction};
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    fn test_frame(raw_frame: &[u8]) {
        let (mut decoder, mut encoder) = OpenSearchCodecBuilder::new(Direction::Sink).build();
        let message = decoder
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        println!("{:?}", message);

        let mut dest = BytesMut::new();
        encoder.encode(message, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    const RESPONSE: [u8; 186] = *b"\
             HTTP/1.1 200 OK\r\n\
             Date: Mon, 27 Jul 2009 12:28:53 GMT\r\n\
             Server: Apache/2.2.14 (Win32)\r\n\
             Last-Modified: Wed, 22 Jul 2009 19:15:56 GMT\r\n\
             Content-Length: 9\r\n\
             Content-Type: text/html\r\n\r\n\
             something";

    const REQUEST: [u8; 90] = *b"\
    POST /cgi-bin/process.cgi HTTP/1.1\r\n\
                        connection: Keep-Alive\r\n\
                        content-length: 9\r\n\r\n\
                        something";

    #[test]
    fn test_request() {
        test_frame(&REQUEST);
    }

    // #[test]
    // fn test_response() {
    //     test_frame(&RESPONSE);
    // }
}
