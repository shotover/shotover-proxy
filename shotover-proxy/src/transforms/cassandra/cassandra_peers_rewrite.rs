use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CassandraPeersRewriteConfig {}

#[async_trait]
impl TransformsFromConfig for CassandraPeersRewriteConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        todo!();
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn prep_transform_chain(
        &mut self,
        _t: &mut crate::transforms::chain::TransformChain,
    ) -> Result<()> {
        Ok(())
    }

    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        todo!()
    }

    fn get_name(&self) -> &'static str {
        "CassandraPeersRewrite"
    }
}
