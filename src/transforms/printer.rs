use async_trait::async_trait;
use tracing::info;

use shotover_transforms::Wrapper;

use crate::transforms::InternalTransform;
use shotover_transforms::{ChainResponse, Messages, Transform};

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
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        info!("Request content: {:?}", qd.message);
        self.counter += 1;
        let response = qd.call_next_transform().await;
        info!("Response content: {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
