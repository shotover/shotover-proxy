use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Forward {
    name: &'static str,
}

impl Forward {
    pub fn new() -> Forward {
        Forward{
            name: "Forward",
        }
    }
}


#[async_trait]
impl Transform for Forward {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
        return ChainResponse::Ok(qd.message.clone());
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
