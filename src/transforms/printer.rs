use crate::transforms::chain::{ Transform, TransformChain, Wrapper};

use async_trait::async_trait;
use crate::error::ChainResponse;

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
        println!("Message content: {:#?}", qd.message);
        return self.call_next_transform(qd, t).await;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
