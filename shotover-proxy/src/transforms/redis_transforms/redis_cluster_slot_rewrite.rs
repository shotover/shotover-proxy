use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use hyper::body::Bytes;
pub use redis_protocol::prelude::Frame;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::protocols::RawFrame;
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
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let cluster_slots_indices = message_wrapper
            .message
            .messages
            .iter()
            .enumerate()
            .filter(|(_, m)| is_cluster_slots(&m.original))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        let mut response = message_wrapper.call_next_transform().await?;

        for i in cluster_slots_indices {
            response.messages[i].original =
                rewrite_port(&response.messages[i].original, self.new_port)?;
        }

        Ok(response)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }

    async fn prep_transform_chain(
        &mut self,
        _t: &mut crate::transforms::chain::TransformChain,
    ) -> Result<()> {
        Ok(())
    }
}

fn rewrite_port(frame: &RawFrame, new_port: u16) -> Result<RawFrame> {
    let mut new_frame = frame.clone();
    if let RawFrame::Redis(Frame::Array(ref mut array)) = new_frame {
        for elem in array.iter_mut() {
            if let Frame::Array(slot) = elem {
                slot.iter_mut()
                    .enumerate()
                    .map(|(index, frame): (usize, &mut Frame)| match (index, frame) {
                        (0..=1, _frame) => Ok(()),
                        (_, Frame::Array(master)) => {
                            if let Frame::BulkString(ip) = &master[0] {
                                if ip == &Bytes::from("") {
                                    return Ok(()); // IP is unknown
                                }
                            } else {
                                bail!("unexpected type for ip")
                            };

                            if let Frame::Integer(_) = master[1] {
                                master[1] = Frame::Integer(new_port.into());
                                Ok(())
                            } else {
                                bail!("unexpected type for port")
                            }
                        }
                        _ => bail!("unexpected value in slot map"),
                    })
                    .collect::<Result<Vec<_>>>()?;
            };
        }
    };

    Ok(new_frame)
}

fn is_cluster_slots(frame: &RawFrame) -> bool {
    let args = if let RawFrame::Redis(Frame::Array(array)) = frame {
        array
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => String::from_utf8(b.to_vec())
                    .map(|s| s.to_uppercase())
                    .context("expected utf-8"),
                _ => bail!("expected bulk string"),
            })
            .collect::<Result<Vec<_>>>()
    } else {
        return false;
    };

    let args = if let Ok(args) = args {
        args
    } else {
        return false;
    };

    args == vec!["CLUSTER", "SLOTS"]
}
