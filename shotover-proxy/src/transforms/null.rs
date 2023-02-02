use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct NullSink {}

#[async_trait]
impl Transform for NullSink {
    fn is_terminating(&self) -> bool {
        true
    }

    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for message in &mut message_wrapper.messages {
            message.set_error("Handled by shotover null transform".to_string());
        }
        Ok(message_wrapper.messages)
    }
}
