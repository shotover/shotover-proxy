use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DebugPrinterConfig;

#[typetag::serde(name = "DebugPrinter")]
#[async_trait(?Send)]
impl TransformConfig for DebugPrinterConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugPrinter::new()))
    }
}

#[derive(Debug, Clone)]
pub struct DebugPrinter {
    counter: i32,
}

impl Default for DebugPrinter {
    fn default() -> Self {
        Self::new()
    }
}

impl DebugPrinter {
    pub fn new() -> DebugPrinter {
        DebugPrinter { counter: 0 }
    }
}

impl TransformBuilder for DebugPrinter {
    fn build(&self) -> Transforms {
        Transforms::DebugPrinter(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "DebugPrinter"
    }
}

#[async_trait]
impl Transform for DebugPrinter {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for request in &mut requests_wrapper.requests {
            info!("Request: {}", request.to_high_level_string());
        }

        self.counter += 1;
        let mut responses = requests_wrapper.call_next_transform().await?;

        for response in &mut responses {
            info!("Response: {}", response.to_high_level_string());
        }
        Ok(responses)
    }
}
