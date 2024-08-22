use crate::message::Messages;
use crate::transforms::{
    ChainState, DownChainProtocol, Transform, TransformBuilder, TransformConfig,
    TransformContextBuilder, TransformContextConfig, UpChainProtocol,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DebugPrinterConfig;

const NAME: &str = "DebugPrinter";
#[typetag::serde(name = "DebugPrinter")]
#[async_trait(?Send)]
impl TransformConfig for DebugPrinterConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugPrinter::new()))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[derive(Clone)]
pub(crate) struct DebugPrinter {
    counter: i32,
}

impl Default for DebugPrinter {
    fn default() -> Self {
        Self::new()
    }
}

impl DebugPrinter {
    pub(crate) fn new() -> DebugPrinter {
        DebugPrinter { counter: 0 }
    }
}

impl TransformBuilder for DebugPrinter {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for DebugPrinter {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for request in &mut chain_state.requests {
            info!("Request: {}", request.to_high_level_string());
        }

        self.counter += 1;
        let mut responses = chain_state.call_next_transform().await?;

        for response in &mut responses {
            info!("Response: {}", response.to_high_level_string());
        }
        Ok(responses)
    }
}
