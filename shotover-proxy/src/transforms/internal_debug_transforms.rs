use crate::error::ChainResponse;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;
use rand_distr::Distribution;
use rand_distr::Normal;
use tokio::time::Duration;

#[cfg(test)]
use {
    crate::message::{Message, MessageDetails, Messages},
    crate::protocols::RawFrame,
    crate::transforms::Transforms,
    anyhow::{anyhow, Result},
    redis_protocol::resp2::prelude::Frame,
    serde::Deserialize,
};

#[cfg(test)]
#[derive(Deserialize, Debug, Clone)]
pub struct DebugReturnerTransformConfig {
    ok: bool,
    response: Response,
}

#[cfg(test)]
impl DebugReturnerTransformConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::DebugReturnerTransform(
            DebugReturnerTransform::new(self.response.clone(), self.ok),
        ))
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Deserialize)]
pub enum Response {
    #[serde(skip)]
    Message(Messages),
    Redis(String),
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct DebugReturnerTransform {
    response: Response,
    ok: bool,
}

#[cfg(test)]
impl DebugReturnerTransform {
    pub fn new(response: Response, ok: bool) -> Self {
        DebugReturnerTransform { response, ok }
    }
}

#[cfg(test)]
#[async_trait]
impl Transform for DebugReturnerTransform {
    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.ok {
            match &self.response {
                Response::Message(message) => Ok(message.clone()),
                Response::Redis(string) => {
                    let res = vec![Message {
                        details: MessageDetails::Unknown,
                        modified: false,
                        original: RawFrame::Redis(Frame::BulkString(string.clone().into_bytes())),
                    }];
                    return Ok(res);
                }
            }
        } else {
            Err(anyhow!("Intentional Fail"))
        }
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct DebugRandomDelayTransform {
    pub delay: u64,
    pub distribution: Option<Normal<f64>>,
}

#[async_trait]
impl Transform for DebugRandomDelayTransform {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let delay;
        if let Some(dist) = self.distribution {
            delay = Duration::from_millis(dist.sample(&mut rand::thread_rng()) as u64 + self.delay);
        } else {
            delay = Duration::from_millis(self.delay);
        }
        tokio::time::sleep(delay).await;
        message_wrapper.call_next_transform().await
    }
}
