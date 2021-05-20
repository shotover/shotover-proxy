use async_trait::async_trait;

use shotover_transforms::Wrapper;

use shotover_transforms::{ChainResponse, Messages, Transform};

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
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        wrapped_messages.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
