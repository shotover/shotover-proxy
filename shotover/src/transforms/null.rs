use super::{
    DownChainProtocol, DownChainTransforms, TransformContextBuilder, TransformContextConfig,
    UpChainProtocol,
};
use crate::message::Messages;
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
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
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(NullSink {}))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::Terminating
    }
}

#[derive(Default)]
pub struct NullSink {}

impl TransformBuilder for NullSink {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(NullSink {})
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

    async fn transform(
        &mut self,
        chain_state: &mut ChainState,
        _down_chain: DownChainTransforms<'_>,
    ) -> Result<Messages> {
        for request in &mut chain_state.requests {
            // reuse the requests to hold the responses to avoid an allocation
            *request = request
                .from_request_to_error_response("Handled by shotover null transform".to_string())?;
        }
        Ok(std::mem::take(&mut chain_state.requests))
    }
}
