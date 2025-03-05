use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::frame::MessageType;
use crate::message::{Message, MessageIdMap, Messages};
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
use anyhow::Result;
use async_trait::async_trait;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use nonzero_ext::nonzero;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RequestThrottlingConfig {
    pub max_requests_per_second: NonZeroU32,
}

const NAME: &str = "RequestThrottling";
#[typetag::serde(name = "RequestThrottling")]
#[async_trait(?Send)]
impl TransformConfig for RequestThrottlingConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(RequestThrottling {
            limiter: Arc::new(RateLimiter::direct(Quota::per_second(
                self.max_requests_per_second,
            ))),
            max_requests_per_second: self.max_requests_per_second,
            throttled_requests: MessageIdMap::default(),
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Cassandra])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[derive(Clone)]
struct RequestThrottling {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    max_requests_per_second: NonZeroU32,
    throttled_requests: MessageIdMap<Message>,
}

impl TransformBuilder for RequestThrottling {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
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
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for request in &mut chain_state.requests {
            if let Ok(cell_count) = request.cell_count() {
                let throttle = match self.limiter.check_n(cell_count) {
                    // occurs if all cells can be accommodated
                    Ok(Ok(())) => false,
                    // occurs if not all cells can be accommodated.
                    Ok(Err(_)) => true,
                    // occurs when the batch can never go through, meaning the rate limiter's quota's burst size is too low for the given number of cells to be ever allowed through
                    Err(_) => {
                        tracing::warn!(
                            "A message was received that could never have been successfully delivered since it contains more sub messages than can ever be allowed through via the `RequestThrottling` transforms `max_requests_per_second` configuration."
                        );
                        true
                    }
                };
                if throttle {
                    self.throttled_requests
                        .insert(request.id(), request.to_backpressure()?);
                    request.replace_with_dummy();
                }
            }
        }

        // send allowed messages to Cassandra
        let mut responses = chain_state.call_next_transform().await?;

        // replace dummy responses with throttle messages
        for response in responses.iter_mut() {
            if let Some(request_id) = response.request_id() {
                if let Some(error_response) = self.throttled_requests.remove(&request_id) {
                    *response = error_response;
                }
            }
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transforms::chain::TransformChainBuilder;
    use crate::transforms::null::NullSink;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_validate() {
        {
            let chain = TransformChainBuilder::new(
                vec![
                    Box::new(RequestThrottling {
                        limiter: Arc::new(RateLimiter::direct(Quota::per_second(nonzero!(20u32)))),
                        max_requests_per_second: nonzero!(20u32),
                        throttled_requests: MessageIdMap::default(),
                    }),
                    Box::<NullSink>::default(),
                ],
                "test-chain",
            );

            assert_eq!(
                chain.validate(),
                vec![
                    "test-chain chain:",
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
                        throttled_requests: MessageIdMap::default(),
                    }),
                    Box::<NullSink>::default(),
                ],
                "test-chain",
            );

            assert_eq!(chain.validate(), Vec::<String>::new());
        }
    }
}
