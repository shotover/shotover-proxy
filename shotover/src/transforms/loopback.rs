use super::{Responses, TransformContextBuilder};
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

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Responses> {
        // This transform ultimately doesnt make a lot of sense semantically
        // but make a vague attempt to follow transform invariants anyway.
        for request in &mut requests_wrapper.requests {
            request.set_request_id(request.id());
        }
        Ok(Responses::return_to_client(requests_wrapper.requests))
    }
}
