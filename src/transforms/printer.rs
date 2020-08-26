use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use tracing::info;

use crate::error::ChainResponse;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Printer {
    name: &'static str,
}

impl Printer {
    pub fn new() -> Printer {
        Printer { name: "Printer" }
    }
}

#[async_trait]
impl Transform for Printer {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        info!("Request content: {:?}", qd.message);
        let response = self.call_next_transform(qd, t).await;
        info!("Response content: {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
