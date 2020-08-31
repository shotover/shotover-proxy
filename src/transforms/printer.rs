use crate::transforms::chain::TransformChain;
use tracing::info;

use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
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
        let response = t.call_next_transform(qd).await;
        info!("Response content: {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
