use super::{CodecBuilder, CodecReadError, CodecWriteError, Direction};
use crate::frame::{
    opensearch::{HttpHead, RequestParts, ResponseParts},
    Frame, MessageType, OpenSearchFrame,
};
use crate::message::{Encodable, Message, Messages};
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use http::{header, HeaderName, HeaderValue, Request, Response};
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
        "opensearch"
    }
}

pub struct OpenSearchDecoder {
    direction: Direction,
    state: State,
}

struct DecodeResult {
    body_start: usize,
    http_headers: HttpHead,
    content_length: usize,
}

impl OpenSearchDecoder {
    pub fn new(direction: Direction) -> Self {
        Self {
            direction,
            state: State::ParsingResponse,
        }
    }

    fn decode_request(&self, src: &mut BytesMut) -> Result<Option<DecodeResult>> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut request = httparse::Request::new(&mut headers);

        let body_start = match request.parse(src).unwrap() {
            httparse::Status::Complete(body_start) => body_start,
            httparse::Status::Partial => return Ok(None),
        };
        match request.version.unwrap() {
            1 => (),
            _version => panic!("error!"), // TODO version error
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
                None => panic!(), // TODO content length error
            },
            None => 0, // TODO content length error
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

        let body_start = match response.parse(src).unwrap() {
            httparse::Status::Complete(body_start) => body_start,
            httparse::Status::Partial => return Ok(None),
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
                None => panic!(), // TODO content length error
            },
            None => 0, // TODO content length error
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
                        self.decode_request(src).unwrap() // TODO
                    } else {
                        self.decode_response(src).unwrap() // TODO
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
                    if src.is_empty() {
                        // sometimes messages will have a content-length but no
                        // body ?
                        return Ok(Some(vec![Message::from_frame(Frame::OpenSearch(
                            OpenSearchFrame::new(http_headers, bytes::Bytes::new()),
                        ))]));
                    }

                    if src.len() < content_length {
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
                    match opensearch_frame.headers {
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
                        HttpHead::Response(response_parts) => {
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

#[cfg(test)]
mod opensearch_tests {
    use crate::codec::{opensearch::OpenSearchCodecBuilder, CodecBuilder, Direction};
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    fn test_frame(raw_frame: &[u8], direction: Direction) {
        let (mut decoder, mut encoder) = OpenSearchCodecBuilder::new(direction).build();
        let message = decoder
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();

        let mut dest = BytesMut::new();
        encoder.encode(message, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    const RESPONSE: [u8; 186] = *b"\
             HTTP/1.1 200 OK\r\n\
             date: Mon, 27 Jul 2009 12:28:53 GMT\r\n\
             server: Apache/2.2.14 (Win32)\r\n\
             last-modified: Wed, 22 Jul 2009 19:15:56 GMT\r\n\
             content-length: 9\r\n\
             content-type: text/html\r\n\r\n\
             something";

    const REQUEST: [u8; 90] = *b"\
    POST /cgi-bin/process.cgi HTTP/1.1\r\n\
                        connection: Keep-Alive\r\n\
                        content-length: 9\r\n\r\n\
                        something";

    #[test]
    fn test_request() {
        test_frame(&REQUEST, Direction::Source);
    }

    #[test]
    fn test_response() {
        test_frame(&RESPONSE, Direction::Sink);
    }
}
