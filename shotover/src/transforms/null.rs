use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct NullSinkConfig;

const NAME: &str = "NullSink";
#[typetag::serde(name = "NullSink")]
#[async_trait(?Send)]
impl TransformConfig for NullSinkConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(NullSink {}))
    }
}

#[derive(Debug, Clone, Default)]
pub struct NullSink {}

impl TransformBuilder for NullSink {
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
impl Transform for NullSink {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for message in &mut requests_wrapper.requests {
            let request_id = message.id();

            // reuse the requests to hold the responses to avoid an allocation
            *message =
                message.to_error_response("Handled by shotover null transform".to_string())?;

            // set the response to point to its corresponding request
            message.set_request_id(request_id)
        }
        Ok(requests_wrapper.requests)
    }
}
