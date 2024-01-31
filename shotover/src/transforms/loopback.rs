use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, Wrapper};
use anyhow::Result;
use async_trait::async_trait;

const NAME: &str = "Loopback";

#[derive(Debug, Clone, Default)]
pub struct Loopback {}

impl TransformBuilder for Loopback {
    fn build(&self) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[async_trait]
impl Transform for Loopback {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        Ok(requests_wrapper.requests)
    }
}
