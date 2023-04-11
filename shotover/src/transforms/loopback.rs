use crate::transforms::ChainResponse;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Loopback {}

impl TransformBuilder for Loopback {
    fn build(&self) -> Transforms {
        Transforms::Loopback(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "Loopback"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[async_trait]
impl Transform for Loopback {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(message_wrapper.messages)
    }
}
