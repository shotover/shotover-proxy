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
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for request in &mut message_wrapper.messages {
            info!("Request: {}", request.to_high_level_string());
        }

        self.counter += 1;
        let mut responses = message_wrapper.call_next_transform().await?;

        for response in &mut responses {
            info!("Response: {}", response.to_high_level_string());
        }
        Ok(responses)
    }
}
