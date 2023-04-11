use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use shotover::frame::{Frame, RedisFrame};
use shotover::message::Messages;
use shotover::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};

#[derive(Deserialize, Debug, Clone)]
pub struct RedisGetRewriteConfig {
    pub result: String,
}

#[typetag::deserialize(name = "RedisGetRewrite")]
#[async_trait(?Send)]
impl TransformConfig for RedisGetRewriteConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(RedisGetRewriteBuilder {
            result: self.result.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct RedisGetRewriteBuilder {
    result: String,
}

impl TransformBuilder for RedisGetRewriteBuilder {
    fn build(&self) -> Transforms {
        Transforms::Custom(Box::new(RedisGetRewrite {
            result: self.result.clone(),
        }))
    }

    fn get_name(&self) -> &'static str {
        "RedisGetRewrite"
    }
}

pub struct RedisGetRewrite {
    result: String,
}

#[async_trait]
impl Transform for RedisGetRewrite {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> Result<Messages> {
        let mut get_indices = vec![];
        for (i, message) in message_wrapper.messages.iter_mut().enumerate() {
            if let Some(frame) = message.frame() {
                if is_get(frame) {
                    get_indices.push(i);
                }
            }
        }
        let mut responses = message_wrapper.call_next_transform().await?;

        for i in get_indices {
            if let Some(frame) = responses[i].frame() {
                rewrite_get(frame, &self.result);
                responses[i].invalidate_cache();
            }
        }

        Ok(responses)
    }
}

fn is_get(frame: &Frame) -> bool {
    if let Frame::Redis(RedisFrame::Array(array)) = frame {
        if let Some(RedisFrame::BulkString(first)) = array.first() {
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
    *frame = Frame::Redis(RedisFrame::BulkString(result.to_owned().into()));
}
