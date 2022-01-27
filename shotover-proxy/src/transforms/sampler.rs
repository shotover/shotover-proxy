use crate::error::ChainResponse;
use crate::transforms::chain::TransformChain;
use crate::transforms::{Transform, Wrapper};

use async_trait::async_trait;
use tokio::macros::support::thread_rng_n;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct Sampler {
    numerator: u32,
    denominator: u32,
    sample_chain: TransformChain,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new()
    }
}

impl Sampler {
    pub fn new() -> Sampler {
        Sampler {
            numerator: 1,
            denominator: 100,
            sample_chain: TransformChain::new(vec![], "dummy".to_string()),
        }
    }

    fn get_name(&self) -> &'static str {
        "Sampler"
    }
}

#[async_trait]
impl Transform for Sampler {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let chance = thread_rng_n(self.denominator);
        if chance < self.numerator {
            let sample = message_wrapper.clone();
            let (sample, downstream) = tokio::join!(
                self.sample_chain
                    .process_request(sample, self.get_name().to_string()),
                message_wrapper.call_next_transform()
            );
            if sample.is_err() {
                warn!("Could not sample request {:?}", sample);
            }
            downstream
        } else {
            message_wrapper.call_next_transform().await
        }
    }
}
