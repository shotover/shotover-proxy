use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Null {}

#[async_trait]
impl Transform for Null {
    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(vec![Message::new(
            MessageDetails::Response(QueryResponse::empty()),
            true,
            RawFrame::None,
        )])
    }

    fn get_name(&self) -> &'static str {
        "Null"
    }
}
