use crate::frame::{CassandraOperation, Frame};
use crate::{
    error::ChainResponse,
    transforms::{Transform, TransformBuilder, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::frame::message_supported::BodyResSupported;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraOptionsRewriteConfig {
    pub map: HashMap<String, Vec<String>>,
}

impl CassandraOptionsRewriteConfig {
    pub async fn get_transform(&self) -> Result<TransformBuilder> {
        Ok(TransformBuilder::CassandraOptionsRewrite(
            CassandraOptionsRewrite::new(self.map.clone()),
        ))
    }
}

#[derive(Clone)]
pub struct CassandraOptionsRewrite {
    map: HashMap<String, Vec<String>>,
}

impl CassandraOptionsRewrite {
    pub fn new(map: HashMap<String, Vec<String>>) -> Self {
        CassandraOptionsRewrite { map }
    }
}

#[async_trait]
impl Transform for CassandraOptionsRewrite {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut response = message_wrapper.call_next_transform().await?;
        for message in &mut response {
            let mut invalidate = false;
            if let Some(Frame::Cassandra(frame)) = message.frame() {
                if let CassandraOperation::Supported(BodyResSupported { data }) =
                    &mut frame.operation
                {
                    for key in self.map.keys() {
                        data.insert(key.clone(), self.map.get(key).unwrap().clone());
                    }
                    invalidate = true;
                }
            }
            if invalidate {
                message.invalidate_cache();
            }
        }

        Ok(response)
    }

    async fn transform_pushed<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let response = message_wrapper.call_next_transform_pushed().await?;
        Ok(response)
    }
}
