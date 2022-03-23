use crate::{
    error::ChainResponse,
    message::Message,
    transforms::{Transform, Transforms, Wrapper},
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

#[derive(Deserialize, Debug, Clone)]
pub struct RequestThrottlingConfig {
    pub max_requests_per_second: NonZeroU32,
}

impl RequestThrottlingConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::RequestThrottling(RequestThrottling {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::null::Null;
    use crate::transforms::Transforms;

    #[test]
    fn test_validate() {
        {
            let chain = TransformChain::new(
                vec![
                    Transforms::RequestThrottling(RequestThrottling {
                        limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(20u32)))),
                        max_requests_per_second: nonzero!(20u32),
                    }),
                    Transforms::Null(Null::default()),
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
            let chain = TransformChain::new(
                vec![
                    Transforms::RequestThrottling(RequestThrottling {
                        limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(100u32)))),
                        max_requests_per_second: nonzero!(100u32),
                    }),
                    Transforms::Null(Null::default()),
                ],
                "test-chain".to_string(),
            );

            assert_eq!(chain.validate(), Vec::<String>::new());
        }
    }
}
