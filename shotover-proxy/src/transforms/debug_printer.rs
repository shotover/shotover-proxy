use tracing::info;

use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

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

#[async_trait]
impl Transform for DebugPrinter {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        info!("Request content: {:?}", message_wrapper.messages);
        self.counter += 1;
        let response = message_wrapper.call_next_transform().await;
        info!("Response content: {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        "DebugPrinter"
    }
}
