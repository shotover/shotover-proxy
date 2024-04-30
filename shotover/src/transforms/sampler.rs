use crate::message::Messages;
use crate::transforms::chain::{TransformChain, TransformChainBuilder};
use crate::transforms::{Transform, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use tokio::macros::support::thread_rng_n;
use tracing::warn;

const NAME: &str = "Sampler";

pub struct SamplerBuilder {
    pub numerator: u32,
    pub denominator: u32,
    pub sample_chain: TransformChainBuilder,
}

impl SamplerBuilder {
    pub fn new() -> SamplerBuilder {
        SamplerBuilder {
            numerator: 1,
            denominator: 100,
            sample_chain: TransformChainBuilder::new(vec![], "dummy"),
        }
    }
}

impl Default for SamplerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Sampler {
    numerator: u32,
    denominator: u32,
    sample_chain: TransformChain,
}

#[async_trait]
impl Transform for Sampler {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let chance = thread_rng_n(self.denominator);
        if chance < self.numerator {
            let sample = requests_wrapper.clone();
            let (sample, downstream) = tokio::join!(
                self.sample_chain.process_request(sample),
                requests_wrapper.call_next_transform()
            );
            if sample.is_err() {
                warn!("Could not sample request {:?}", sample);
            }
            downstream
        } else {
            requests_wrapper.call_next_transform().await
        }
    }
}
