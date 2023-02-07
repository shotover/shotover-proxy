use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

use super::{TransformBuilder, Transforms};

#[derive(Debug, Clone, Default)]
pub struct NullSink {}

impl TransformBuilder for NullSink {
    fn build(&self) -> super::Transforms {
        Transforms::NullSink(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "NullSink"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[async_trait]
impl Transform for NullSink {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for message in &mut message_wrapper.messages {
            *message = message.to_error_response("Handled by shotover null transform".to_string());
        }
        Ok(message_wrapper.messages)
    }
}
