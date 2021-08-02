use tracing::info;

use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Printer {
    name: &'static str,
    counter: i32,
}

impl Default for Printer {
    fn default() -> Self {
        Self::new()
    }
}

impl Printer {
    pub fn new() -> Printer {
        Printer {
            name: "Printer",
            counter: 0,
        }
    }
}

#[async_trait]
impl Transform for Printer {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        info!("Request content: {:?}", message_wrapper.message);
        self.counter += 1;
        let response = message_wrapper.call_next_transform().await;
        info!("Response content: {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
