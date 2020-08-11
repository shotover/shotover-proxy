use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct ResponseUnifier {}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ResponseUnifierConfig {}

#[async_trait]
impl TransformsFromConfig for ResponseUnifierConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        return Ok(Transforms::ResponseUnifier(ResponseUnifier {}));
    }
}

#[async_trait]
impl Transform for ResponseUnifier {
    async fn transform(&self, _qd: Wrapper, _t: &TransformChain) -> ChainResponse {
        unimplemented!()
    }

    fn get_name(&self) -> &'static str {
        "Response unifier"
    }
}
