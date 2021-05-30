use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use rand_distr::Distribution;
use rand_distr::Normal;
use tokio::time::Duration;

use serde::{Deserialize, Serialize};
use shotover_transforms::{TopicHolder, TransformsFromConfig, Wrapper};

use shotover_transforms::Messages;
use shotover_transforms::{ChainResponse, Transform};

#[derive(Debug, Clone)]
pub struct ReturnerTransform {
    pub message: Messages,
    pub ok: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ReturnerTransformConfig {
    pub message: Messages,
    pub ok: bool,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for ReturnerTransformConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(ReturnerTransform {
            message: self.message.clone(),
            ok: self.ok.clone(),
        }))
    }
}

#[async_trait]
impl Transform for ReturnerTransform {
    async fn transform<'a>(&'a mut self, _wrapped_messages: Wrapper<'a>) -> ChainResponse {
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
    pub distribution: Option<Normal<f64>>,
}

#[async_trait]
impl Transform for RandomDelayTransform {
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        let delay;
        if let Some(dist) = self.distribution {
            delay = Duration::from_millis(dist.sample(&mut rand::thread_rng()) as u64 + self.delay);
        } else {
            delay = Duration::from_millis(self.delay);
        }
        tokio::time::sleep(delay).await;
        wrapped_messages.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        "RandomDelay"
    }
}
