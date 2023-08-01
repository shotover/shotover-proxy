use anyhow::Result;
use bytes::Bytes;
use derivative::Derivative;
use http::request::Parts as RequestParts;
use http::response::Parts as ResponseParts;
use std::sync::Arc;

#[derive(Debug)]
pub enum HttpHead {
    Response(ResponseParts),
    Request(RequestParts),
}

#[derive(Debug, Clone, Derivative)]
#[derivative(PartialEq)]
pub struct OpenSearchFrame {
    #[derivative(PartialEq = "ignore")]
    pub headers: Arc<HttpHead>,
    pub body: Bytes,
}

impl OpenSearchFrame {
    pub fn new(headers: Arc<HttpHead>, body: Bytes) -> Self {
        Self { headers, body }
    }

    pub fn from_bytes(_bytes: &Bytes) -> Result<Self> {
        todo!();
    }
}
