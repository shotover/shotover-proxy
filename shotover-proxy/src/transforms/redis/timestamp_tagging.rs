use crate::codec::redis::redis_query_type;
use crate::error::ChainResponse;
use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, QueryType};
use crate::transforms::{Transform, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

#[derive(Clone, Default)]
pub struct RedisTimestampTagger {}

impl RedisTimestampTagger {
    pub fn new() -> Self {
        RedisTimestampTagger {}
    }
}

// The way we try and get a "liveness" timestamp from redis, is to use
// OBJECT IDLETIME. This requires maxmemory-policy is set to an LRU policy
// or noeviction and maxmemory is set.
// Unfortunately REDIS only provides a 10 second resolution on this, so high
// update keys where update freq < 20 seconds, will not be terribly consistent
fn wrap_command(frame: &mut RedisFrame) -> Result<RedisFrame> {
    let query_type = redis_query_type(frame);
    if let RedisFrame::Array(com) = frame {
        if let Some(RedisFrame::BulkString(first)) = com.first() {
            if let QueryType::Read | QueryType::ReadWrite = query_type {
                // Continue onto the wrapping logic.
            } else {
                return Err(anyhow!(
                    "cannot wrap command {:?}, command is not read or read/write",
                    first
                ));
            }
            if com.len() < 2 {
                return Err(anyhow!(
                    "cannot wrap command {:?}, command does not contain key",
                    first
                ));
            }
        }

        let original_command = com
            .iter()
            .map(|v| {
                if let RedisFrame::BulkString(b) = v {
                    let mut literal = String::with_capacity(2 + b.len() * 4);
                    literal.push('\'');
                    for value in b {
                        // Here we encode an arbitrary sequence of bytes into a lua string.
                        // lua, unlike rust, is quite happy to store whatever in its strings as long as you give it the relevant escape sequence https://www.lua.org/pil/2.4.html
                        //
                        // We could just write every value as a \ddd escape sequence but its probably faster and definitely more readable to someone inspecting traffic to just use the actual value when we can
                        if *value == b'\'' {
                            // despite being printable we cant include this without escaping it
                            literal.push_str("\\\'");
                        } else if *value == b'\\' {
                            // despite being printable we cant include this without escaping it
                            literal.push_str("\\\\");
                        } else if value.is_ascii_graphic() {
                            literal.push(*value as char);
                        } else {
                            write!(literal, "\\{}", value).unwrap();
                        }
                    }
                    literal.push('\'');
                    literal
                } else {
                    todo!("this might not be right... but we should only be dealing with bytes")
                }
            })
            .join(",");

        let original_pcall = format!(r###"redis.call({original_command})"###);
        let last_used_pcall = r###"redis.call('OBJECT', 'IDLETIME', KEYS[1])"###;

        let script = format!("return {{{original_pcall},{last_used_pcall}}}");
        debug!("\n\nGenerated eval script for timestamp: {}\n\n", script);
        let commands = vec![
            RedisFrame::BulkString(Bytes::from_static(b"EVAL")),
            RedisFrame::BulkString(Bytes::from(script)),
            RedisFrame::BulkString(Bytes::from_static(b"1")),
            // Key is always the 2nd element of a redis command
            com.swap_remove(1),
        ];
        Ok(RedisFrame::Array(commands))
    } else {
        Err(anyhow!("redis frame is not an array"))
    }
}

fn unwrap_response(message: &mut Message) -> Result<()> {
    struct ToWrite {
        meta_timestamp: i64,
        frame: RedisFrame,
    }

    if let Some(redis_frame) = message.frame().map(|frame| frame.redis()).transpose()? {
        let to_write = if let RedisFrame::Array(values) = redis_frame {
            let all_arrays = values.iter().all(|v| matches!(v, RedisFrame::Array(_)));
            if all_arrays && values.len() > 1 {
                // This means the result is likely from a transaction or something that returns
                // lots of things

                let mut timestamps: Vec<RedisFrame> = vec![];
                let mut results: Vec<RedisFrame> = vec![];
                for v_u in values {
                    if let RedisFrame::Array(v) = v_u {
                        if v.len() == 2 {
                            let timestamp = v.pop().unwrap();
                            let actual = v.pop().unwrap();
                            timestamps.push(timestamp);
                            results.push(actual);
                        }
                    }
                }

                todo!("ConsistentScatter isnt built to handle multiple timestamps yet: {timestamps:?} {results:?}",)
            } else if values.len() == 2 {
                let timestamp = values.pop().unwrap();
                let frame = values.pop().unwrap();

                let timestamp = match timestamp {
                    RedisFrame::Integer(i) => i,
                    _ => 0,
                };

                let since_the_epoch = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards");
                let seconds_since_the_epoch = since_the_epoch.as_secs();
                if seconds_since_the_epoch.leading_ones() > 1 {
                    panic!("Cannot convert u64 to i64 without overflow");
                }
                let meta_timestamp = seconds_since_the_epoch as i64 - timestamp;
                Some(ToWrite {
                    meta_timestamp,
                    frame,
                })
            } else {
                None
            }
        } else {
            None
        };

        if let Some(to_write) = to_write {
            *redis_frame = to_write.frame;
            message.meta_timestamp = Some(to_write.meta_timestamp);
            message.invalidate_cache();
        }
    }
    Ok(())
}

#[async_trait]
impl Transform for RedisTimestampTagger {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // TODO: This is wrong. We need to keep track of tagged_success per message
        let mut tagged_success = true;
        // TODO: This is wrong. We need more robust handling of transactions
        let mut exec_block = false;

        for message in message_wrapper.messages.iter_mut() {
            if let Some(Frame::Redis(frame)) = message.frame() {
                if let RedisFrame::Array(array) = frame {
                    if array
                        .get(0)
                        .map(|x| x == &RedisFrame::BulkString("EXEC".into()))
                        .unwrap_or(false)
                    {
                        exec_block = true;
                    }
                }
                match wrap_command(frame) {
                    Ok(result) => {
                        *frame = result;
                        message.invalidate_cache();
                    }
                    Err(err) => {
                        trace!("Couldn't wrap command with timestamp tagger: {}", err);
                        tagged_success = false;
                    }
                }
            }
        }

        let mut response = message_wrapper.call_next_transform().await;
        debug!("tagging transform got {:?}", response);
        if let Ok(messages) = &mut response {
            if tagged_success || exec_block {
                for message in messages.iter_mut() {
                    unwrap_response(message)?;
                }
            }
        }

        debug!("response after trying to unwrap -> {:?}", response);
        response
    }
}
