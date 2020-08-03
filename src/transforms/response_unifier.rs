
use serde::{Deserialize, Serialize};
use crate::transforms::{TransformsFromConfig, Transforms};
use crate::config::topology::TopicHolder;
use anyhow::{Result};
use async_trait::async_trait;
use crate::transforms::chain::{Transform, Wrapper, TransformChain};
use crate::error::ChainResponse;
use crate::message::{Message, Value, QueryResponse};
use itertools::Itertools;


#[derive(Clone)]
pub struct ResponseUnifier {
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ResponseUnifierConfig {
}

#[async_trait]
impl TransformsFromConfig for ResponseUnifierConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms>  {
        return Ok(Transforms::ResponseUnifier(ResponseUnifier{}));
    }
}

impl ResponseUnifier {
    fn resolve_fragments(&self, qr: &mut QueryResponse) {
        let mut ptr: Option<Value> = None;
        if let Some(Value::FragmentedResponese(list)) = &mut qr.result {
            if list.len() == 0 {
                ptr = None; // We shouldn't hit this point
            } else if list.iter().all_equal() {
                ptr = Some(list.remove(0));
            } else {
                //TODO: Call resolver - logic to resolve inconsistencies between responses
                ptr = Some(list.remove(0));
            }
        }
        std::mem::swap( &mut qr.result, &mut ptr);
    }
}

#[async_trait]
impl Transform for ResponseUnifier {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let mut chain_response = self.call_next_transform(qd, t).await?;
        match &mut chain_response {
            Message::Modified(box Message::Response(qr)) => self.resolve_fragments(qr),
            Message::Response(qr) => self.resolve_fragments(qr),
            _ => {}
        }
        return Ok(chain_response);
    }

    fn get_name(&self) -> &'static str {
        "Response unifier"
    }
}