use crate::message::Messages;
use crate::transforms::{Transform, Wrapper};
use anyhow::Result;
use async_trait::async_trait;

const NAME: &str = "NoOp";

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
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        requests_wrapper.call_next_transform().await
    }
}
