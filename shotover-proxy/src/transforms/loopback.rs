use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Loopback {}

#[async_trait]
impl Transform for Loopback {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(message_wrapper.messages)
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
