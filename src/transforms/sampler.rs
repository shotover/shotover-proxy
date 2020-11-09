use crate::error::ChainResponse;
use crate::transforms::chain::TransformChain;
use crate::transforms::{Transform, Wrapper};

use async_trait::async_trait;
use tokio::macros::support::thread_rng_n;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct Sampler {
    name: &'static str,
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
            name: "Sampler",
            numerator: 1,
            denominator: 100,
            sample_chain: TransformChain::new_no_shared_state(vec![], "dummy".to_string()),
        }
    }
}

#[async_trait]
impl Transform for Sampler {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let chance = thread_rng_n(self.denominator);
        return if chance < self.numerator {
            let sample = qd.clone();
            let (sample, downstream) = tokio::join!(
                self.sample_chain.process_request(sample),
                qd.call_next_transform()
            );
            if sample.is_err() {
                warn!("Could not sample request {:?}", sample);
            }
            downstream
        } else {
            qd.call_next_transform().await
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
