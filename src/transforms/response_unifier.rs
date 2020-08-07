use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, QueryResponse, Value};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::info;

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

impl ResponseUnifier {
    fn resolve_fragments(&self, qr: &mut QueryResponse) {
        let mut ptr: Option<Value> = None;
        info!("{:?}", qr.result);
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
        std::mem::swap(&mut qr.result, &mut ptr);

        if let Some(Value::FragmentedResponese(list)) = &mut qr.error {
            if list.len() == 0 {
                ptr = None; // We shouldn't hit this point
            } else if list.iter().all_equal() {
                ptr = Some(list.remove(0));
            } else {
                //TODO: Call resolver - logic to resolve inconsistencies between responses
                ptr = Some(list.remove(0));
            }
        }
        std::mem::swap(&mut qr.error, &mut ptr);
    }
}

#[async_trait]
impl Transform for ResponseUnifier {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let mut chain_response = self.call_next_transform(qd, t).await?;
        match &mut chain_response {
            Message::Modified(box Message::Response(qr)) => self.resolve_fragments(qr),
            Message::Response(qr) => self.resolve_fragments(qr),
            _ => {
                info!("uh oh");
            }
        }
        return Ok(chain_response);
    }

    fn get_name(&self) -> &'static str {
        "Response unifier"
    }
}
