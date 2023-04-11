use crate::message::Messages;
use crate::transforms::{Transform, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use rand_distr::Distribution;
use rand_distr::Normal;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct DebugRandomDelay {
    pub delay: u64,
    pub distribution: Option<Normal<f64>>,
}

#[async_trait]
impl Transform for DebugRandomDelay {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> Result<Messages> {
        let delay = if let Some(dist) = self.distribution {
            Duration::from_millis(dist.sample(&mut rand::thread_rng()) as u64 + self.delay)
        } else {
            Duration::from_millis(self.delay)
        };
        tokio::time::sleep(delay).await;
        message_wrapper.call_next_transform().await
    }
}
