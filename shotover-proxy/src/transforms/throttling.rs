use crate::{
    error::ChainResponse,
    message::Message,
    transforms::{Transform, TransformBuilder, Wrapper},
};
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

impl RequestThrottlingConfig {
    pub fn get_builder(&self) -> Result<Box<dyn TransformBuilder>> {
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
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // extract throttled messages from the message_wrapper
        #[allow(clippy::needless_collect)]
        let throttled_messages: Vec<(Message, usize)> = (0..message_wrapper.messages.len())
            .into_iter()
            .rev()
            .filter_map(|i| {
                if self
                    .limiter
                    .check_n(message_wrapper.messages[i].cell_count().ok()?)
                    .is_err()
                {
                    let message = message_wrapper.messages.remove(i);
                    Some((message, i))
                } else {
                    None
                }
            })
            .collect();

        // if every message got backpressured we can skip this
        let mut responses = if !message_wrapper.messages.is_empty() {
            // send allowed messages to Cassandra
            message_wrapper.call_next_transform().await?
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
