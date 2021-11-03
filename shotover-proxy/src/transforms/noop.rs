use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct NoOp {}

impl NoOp {
    pub fn new() -> NoOp {
        NoOp {}
    }
}

impl Default for NoOp {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transform for NoOp {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        message_wrapper.call_next_transform().await
    }
}
