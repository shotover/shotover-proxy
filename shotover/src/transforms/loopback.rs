use crate::message::Messages;
use crate::transforms::{BodyTransformBuilder, Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Loopback {}

impl BodyTransformBuilder for Loopback {
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
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        Ok(requests_wrapper.requests)
    }
}
