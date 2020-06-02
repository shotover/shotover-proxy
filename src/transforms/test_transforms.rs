use crate::message::Message;
use crate::transforms::chain::{Transform, Wrapper, TransformChain, ChainResponse};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ReturnerTransform {
    pub message: Message
}

#[async_trait]
impl Transform for ReturnerTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        return Ok(self.message.clone())
    }

    fn get_name(&self) -> &'static str {
        return "returner"
    }
}