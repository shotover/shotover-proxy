use crate::transforms::chain::{ Transform, TransformChain, Wrapper};

use async_trait::async_trait;
use crate::error::ChainResponse;

#[derive(Debug, Clone)]
pub struct NoOp {
    name: &'static str,
}

impl NoOp {
    pub fn new() -> NoOp {
        NoOp { name: "NoOp" }
    }
}

#[async_trait]
impl Transform for NoOp {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        return self.call_next_transform(qd, t).await;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
