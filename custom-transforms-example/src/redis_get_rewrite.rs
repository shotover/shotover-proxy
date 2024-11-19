use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use shotover::frame::{Frame, MessageType, ValkeyFrame};
use shotover::message::{MessageIdSet, Messages};
use shotover::transforms::{
    ChainState, Transform, TransformBuilder, TransformConfig, TransformContextConfig,
};
use shotover::transforms::{DownChainProtocol, TransformContextBuilder, UpChainProtocol};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedisGetRewriteConfig {
    pub result: String,
}

const NAME: &str = "RedisGetRewrite";
#[typetag::serde(name = "RedisGetRewrite")]
#[async_trait(?Send)]
impl TransformConfig for RedisGetRewriteConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(RedisGetRewriteBuilder {
            result: self.result.clone(),
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Valkey])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

pub struct RedisGetRewriteBuilder {
    result: String,
}

impl TransformBuilder for RedisGetRewriteBuilder {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(RedisGetRewrite {
            get_requests: MessageIdSet::default(),
            result: self.result.clone(),
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

pub struct RedisGetRewrite {
    get_requests: MessageIdSet,
    result: String,
}

#[async_trait]
impl Transform for RedisGetRewrite {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for message in chain_state.requests.iter_mut() {
            if let Some(frame) = message.frame() {
                if is_get(frame) {
                    self.get_requests.insert(message.id());
                }
            }
        }
        let mut responses = chain_state.call_next_transform().await?;

        for response in responses.iter_mut() {
            if response
                .request_id()
                .map(|id| self.get_requests.remove(&id))
                .unwrap_or(false)
            {
                if let Some(frame) = response.frame() {
                    rewrite_get(frame, &self.result);
                    response.invalidate_cache();
                }
            }
        }

        Ok(responses)
    }
}

fn is_get(frame: &Frame) -> bool {
    if let Frame::Valkey(ValkeyFrame::Array(array)) = frame {
        if let Some(ValkeyFrame::BulkString(first)) = array.first() {
            first.eq_ignore_ascii_case(b"GET")
        } else {
            false
        }
    } else {
        false
    }
}

fn rewrite_get(frame: &mut Frame, result: &str) {
    tracing::info!("Replaced {frame:?} with BulkString(\"{result}\")");
    *frame = Frame::Valkey(ValkeyFrame::BulkString(result.to_owned().into()));
}
