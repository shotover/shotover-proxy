use crate::message::{Message, Messages};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use nonzero_ext::nonzero;
use serde::Deserialize;
use std::num::NonZeroU32;
use std::sync::Arc;

use super::Transforms;

#[derive(Deserialize, Debug)]
pub struct RequestThrottlingConfig {
    pub max_requests_per_second: NonZeroU32,
}

#[typetag::deserialize(name = "RequestThrottling")]
#[async_trait(?Send)]
impl TransformConfig for RequestThrottlingConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(RequestThrottling {
            limiter: Arc::new(RateLimiter::direct(Quota::per_second(
                self.max_requests_per_second,
            ))),
            max_requests_per_second: self.max_requests_per_second,
        }))
    }
}

#[derive(Clone)]
pub struct RequestThrottling {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    max_requests_per_second: NonZeroU32,
}

impl TransformBuilder for RequestThrottling {
    fn build(&self) -> Transforms {
        Transforms::RequestThrottling(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "RequestThrottlingConfig"
    }

    fn validate(&self) -> Vec<String> {
        if self.max_requests_per_second < nonzero!(50u32) {
            vec![
                "RequestThrottling:".into(),
                "  max_requests_per_second has a minimum allowed value of 50".into(),
            ]
        } else {
            vec![]
        }
    }
}

#[async_trait]
impl Transform for RequestThrottling {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        // extract throttled messages from the requests_wrapper
        let throttled_messages: Vec<(Message, usize)> = (0..requests_wrapper.requests.len())
            .rev()
            .filter_map(|i| {
                match self
                    .limiter
                    .check_n(requests_wrapper.requests[i].cell_count().ok()?)
                {
                    // occurs if all cells can be accommodated and 
                    Ok(Ok(())) => None,
                    // occurs if not all cells can be accommodated.
                    Ok(Err(_)) => {
                        let message = requests_wrapper.requests.remove(i);
                        Some((message, i))
                    }
                    // occurs when the batch can never go through, meaning the rate limiter's quota's burst size is too low for the given number of cells to be ever allowed through
                    Err(_) => {
                        tracing::warn!("A message was received that could never have been successfully delivered since it contains more sub messages than can ever be allowed through via the `RequestThrottling` transforms `max_requests_per_second` configuration.");
                        let message = requests_wrapper.requests.remove(i);
                        Some((message, i))
                    }
                }
            })
            .collect();

        // if every message got backpressured we can skip this
        let mut responses = if !requests_wrapper.requests.is_empty() {
            // send allowed messages to Cassandra
            requests_wrapper.call_next_transform().await?
        } else {
            vec![]
        };

        // reinsert backpressure error responses back into responses
        for (mut message, i) in throttled_messages.into_iter().rev() {
            message.set_backpressure()?;
            responses.insert(i, message);
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::null::NullSink;

    #[test]
    fn test_validate() {
        {
            let chain = TransformChainBuilder::new(
                vec![
                    Box::new(RequestThrottling {
                        limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(20u32)))),
                        max_requests_per_second: nonzero!(20u32),
                    }),
                    Box::<NullSink>::default(),
                ],
                "test-chain".to_string(),
            );

            assert_eq!(
                chain.validate(),
                vec![
                    "test-chain:",
                    "  RequestThrottling:",
                    "    max_requests_per_second has a minimum allowed value of 50"
                ]
            );
        }

        {
            let chain = TransformChainBuilder::new(
                vec![
                    Box::new(RequestThrottling {
                        limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(100u32)))),
                        max_requests_per_second: nonzero!(100u32),
                    }),
                    Box::<NullSink>::default(),
                ],
                "test-chain".to_string(),
            );

            assert_eq!(chain.validate(), Vec::<String>::new());
        }
    }
}
