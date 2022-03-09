use crate::{
    error::ChainResponse,
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
use serde::Deserialize;
use std::num::NonZeroU32;
use std::sync::Arc;

#[derive(Deserialize, Debug, Clone)]
pub struct RequestThrottlingConfig {
    pub max_requests_per_second: NonZeroU32,
}

impl RequestThrottlingConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        tracing::info!("here");
        Ok(Transforms::RequestThrottling(RequestThrottling {
            limiter: Arc::new(RateLimiter::direct(Quota::per_second(
                self.max_requests_per_second,
            ))),
        }))
    }
}

#[derive(Clone)]
pub struct RequestThrottling {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
}

#[async_trait]
impl Transform for RequestThrottling {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut backpressured_messages: Vec<crate::message::Message> = vec![];

        // find indexes of messages that need to be backpressured
        let remove_indexes = message_wrapper
            .messages
            .iter_mut()
            .enumerate()
            .filter_map(|(i, _)| {
                if self.limiter.check().is_err() {
                    Some(i)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // remove messsages and set to backpressure
        for i in &remove_indexes {
            let mut message = message_wrapper.messages.remove(*i);
            message.set_backpressure()?;
            backpressured_messages.push(message);
        }

        let mut responses = message_wrapper.call_next_transform().await?;

        // reinsert into responses
        for i in remove_indexes.into_iter() {
            responses.insert(i, backpressured_messages.pop().unwrap());
        }

        Ok(responses)
    }
}
