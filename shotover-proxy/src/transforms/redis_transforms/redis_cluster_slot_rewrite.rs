use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterSlotRewriteConfig {
    pub new_port: u16,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterSlotRewriteConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisClusterSlotRewrite(
            RedisClusterSlotRewrite {
                name: "RedisClusterSlotRewrite",
                new_port: self.new_port,
            },
        ))
    }
}

#[derive(Clone)]
pub struct RedisClusterSlotRewrite {
    name: &'static str,
    new_port: u16,
}

#[async_trait]
impl Transform for RedisClusterSlotRewrite {
    async fn transform<'a>(&'a mut self, mut _message_wrapper: Wrapper<'a>) -> ChainResponse {
        todo!();
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
