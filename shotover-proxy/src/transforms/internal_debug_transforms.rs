use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::{Transform, Wrapper};
use anyhow::anyhow;
use async_trait::async_trait;
use rand_distr::Distribution;
use rand_distr::Normal;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct DebugReturnerTransform {
    pub message: Messages,
    pub ok: bool,
}

#[async_trait]
impl Transform for DebugReturnerTransform {
    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.ok {
            Ok(self.message.clone())
        } else {
            Err(anyhow!("Intentional Fail"))
        }
    }

    fn get_name(&self) -> &'static str {
        "returner"
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

    fn get_name(&self) -> &'static str {
        "RandomDelay"
    }
}
