use super::TransformContextBuilder;
use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, Wrapper};
use anyhow::Result;
use async_trait::async_trait;

const NAME: &str = "Loopback";

#[derive(Clone, Default)]
pub struct Loopback {}

impl TransformBuilder for Loopback {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
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

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        requests_wrapper: &'shorter mut Wrapper<'longer>,
    ) -> Result<Messages> {
        // This transform ultimately doesnt make a lot of sense semantically
        // but make a vague attempt to follow transform invariants anyway.
        for request in &mut requests_wrapper.requests {
            request.set_request_id(request.id());
        }
        Ok(std::mem::take(&mut requests_wrapper.requests))
    }
}
