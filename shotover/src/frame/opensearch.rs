use anyhow::Result;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri, Version};

#[derive(Debug, Clone, PartialEq)]
pub struct ResponseParts {
    pub status: StatusCode,
    pub version: Version,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestParts {
    pub method: Method,
    pub uri: Uri,
    pub version: Version,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HttpHead {
    Response(ResponseParts),
    Request(RequestParts),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenSearchFrame {
    pub headers: HttpHead,
    pub body: Bytes,
}

impl OpenSearchFrame {
    pub fn new(headers: HttpHead, body: Bytes) -> Self {
        Self { headers, body }
    }

    pub fn from_bytes(_bytes: &Bytes) -> Result<Self> {
        todo!();
    }
}
