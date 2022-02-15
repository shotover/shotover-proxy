use crate::error::ChainResponse;
use crate::frame::Frame;
use crate::message::Message;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Null {}

#[async_trait]
impl Transform for Null {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(vec![Message::from_frame(Frame::None)])
    }
}
