use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SequentialMap {
    name: &'static str,
    chain: TransformChain,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SequentialMapConfig {
    pub name: String,
    pub chain: Vec<TransformsConfig>,
}

#[async_trait]
impl TransformsFromConfig for SequentialMapConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::SequentialMap(SequentialMap {
            name: "SequentialMap",
            chain: build_chain_from_config(self.name.clone(), &self.chain, &topics).await?,
        }))
    }
}

#[async_trait]
impl Transform for SequentialMap {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        let mut results: Vec<Messages> = Vec::with_capacity(qd.message.messages.len());
        while !qd.message.messages.is_empty() {
            results.push(
                self.chain
                    .process_request(Wrapper::new(
                        Messages {
                            messages: vec![qd.message.messages.remove(0)],
                        },
                        "SequentialMap".to_string(),
                        None,
                    ))
                    .await?,
            )
        }

        Ok(Messages {
            messages: results.into_iter().flat_map(|ms| ms.messages).collect(),
        })
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
