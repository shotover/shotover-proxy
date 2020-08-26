use crate::message::Messages;
use crate::transforms::chain::{Transform, Wrapper, TransformChain};
use async_trait::async_trait;
use crate::error::ChainResponse;
use tokio::time::Duration;
use rand::prelude::*;
use rand_distr::{Normal};
use anyhow::anyhow;

#[derive(Debug, Clone)]
pub struct ReturnerTransform {
    pub message: Messages,
    pub ok: bool
}

#[async_trait]
impl Transform for ReturnerTransform {
    async fn transform(&self, _qd: Wrapper, _t: &TransformChain) -> ChainResponse {
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
pub struct RandomDelayTransform {
    pub delay: u64,
    pub distribution: Option<Normal<f64>>
}

#[async_trait]
impl Transform for RandomDelayTransform {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let delay;
        if let Some(dist) = self.distribution {
            delay = Duration::from_millis(dist.sample(&mut rand::thread_rng()) as u64 + self.delay);
        } else {
            delay = Duration::from_millis(self.delay);
        }
        tokio::time::delay_for(delay).await;
        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        "RandomDelay"
    }
}