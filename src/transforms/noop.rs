use crate::transforms::chain::{Transform, TransformChain, Wrapper};

use crate::error::ChainResponse;
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
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
