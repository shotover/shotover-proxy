use anyhow::Result;
use bytes::Bytes;
use derivative::Derivative;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, Derivative)]
#[derivative(PartialEq)]
pub struct ResponseParts {
    pub status: StatusCode,
    pub version: Version,
    #[derivative(PartialEq = "ignore")]
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, Derivative)]
#[derivative(PartialEq)]
pub struct RequestParts {
    pub method: Method,
    pub uri: Uri,
    pub version: Version,
    #[derivative(PartialEq = "ignore")]
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HttpHead {
    Response(ResponseParts),
    Request(RequestParts),
}

impl HttpHead {
    pub fn headers(&self) -> &HeaderMap {
        match &self {
            HttpHead::Response(response) => &response.headers,
            HttpHead::Request(request) => &request.headers,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(PartialEq)]
pub struct OpenSearchFrame {
    pub headers: HttpHead,

    #[derivative(PartialEq = "ignore")]
    pub body: Bytes,
}

impl fmt::Debug for OpenSearchFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenSearchFrame")
            .field("headers", &self.headers)
            .field("body", &self.json_str())
            .finish()
    }
}

impl OpenSearchFrame {
    pub fn new(headers: HttpHead, body: Bytes) -> Self {
        Self { headers, body }
    }

    pub fn from_bytes(_bytes: &Bytes) -> Result<Self> {
        todo!();
    }

    pub fn new_server_error_response() -> Self {
        let headers = HttpHead::Response(ResponseParts {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            version: Version::HTTP_11,
            headers: HeaderMap::new(),
        });
        let body = Bytes::new();
        Self::new(headers, body)
    }

    pub fn json_str(&self) -> String {
        use http::header;

        if self.body.is_empty() {
            return "".to_string();
        };

        let body = if self
            .headers
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_some()
        {
            use libflate::gzip::Decoder;
            use std::io::Read;

            let mut decoder = Decoder::new(&self.body[..]).unwrap();

            let mut decoded_data = Vec::new();
            decoder.read_to_end(&mut decoded_data).unwrap();

            decoded_data
        } else {
            self.body.to_vec()
        };

        match serde_json::from_slice::<Value>(&body) {
            Ok(json) => serde_json::to_string_pretty(&json).unwrap(),
            Err(_) => format!("failed to parse json: {:?}", pretty_hex::pretty_hex(&body)),
        }
    }
}
