use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct NoOp {
    name: &'static str,
}

impl NoOp {
    pub fn new() -> NoOp {
        NoOp { name: "NoOp" }
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

    fn get_name(&self) -> &'static str {
        self.name
    }
}
