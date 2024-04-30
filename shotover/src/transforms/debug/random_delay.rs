use crate::message::Messages;
use crate::transforms::{Transform, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use rand_distr::Distribution;
use rand_distr::Normal;
use tokio::time::Duration;

const NAME: &str = "DebugRandomDelay";

pub struct DebugRandomDelay {
    pub delay: u64,
    pub distribution: Option<Normal<f64>>,
}

#[async_trait]
impl Transform for DebugRandomDelay {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let delay = if let Some(dist) = self.distribution {
            Duration::from_millis(dist.sample(&mut rand::thread_rng()) as u64 + self.delay)
        } else {
            Duration::from_millis(self.delay)
        };
        tokio::time::sleep(delay).await;
        requests_wrapper.call_next_transform().await
    }
}
