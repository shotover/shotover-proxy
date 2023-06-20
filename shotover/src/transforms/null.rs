use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct NullSinkConfig;

#[typetag::deserialize(name = "NullSink")]
#[async_trait(?Send)]
impl TransformConfig for NullSinkConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(NullSink {}))
    }
}

#[derive(Debug, Clone, Default)]
pub struct NullSink {}

impl TransformBuilder for NullSink {
    fn build(&self) -> super::Transforms {
        Transforms::NullSink(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "NullSink"
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[async_trait]
impl Transform for NullSink {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for message in &mut requests_wrapper.requests {
            *message =
                message.to_error_response("Handled by shotover null transform".to_string())?;
        }
        Ok(requests_wrapper.requests)
    }
}
