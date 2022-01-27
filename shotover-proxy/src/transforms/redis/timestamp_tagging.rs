use crate::error::ChainResponse;
use crate::message::{
    ASTHolder, IntSize, MessageDetails, MessageValue, QueryMessage, QueryResponse,
};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

#[derive(Clone, Default)]
pub struct RedisTimestampTagger {}

#[derive(Deserialize, Debug, Clone)]
pub struct RedisTimestampTaggerConfig {}

impl RedisTimestampTagger {
    pub fn new() -> Self {
        RedisTimestampTagger {}
    }
}

impl RedisTimestampTaggerConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::RedisTimestampTagger(RedisTimestampTagger {}))
    }
}

// The way we try and get a "liveness" timestamp from redis, is to use
// OBJECT IDLETIME. This requires maxmemory-policy is set to an LRU policy
// or noeviction and maxmemory is set.
// Unfortunately REDIS only provides a 10 second resolution on this, so high
// update keys where update freq < 20 seconds, will not be terribly consistent

fn wrap_command(qm: &mut QueryMessage) -> Result<()> {
    if let Some(MessageValue::List(keys)) = qm.primary_key.get_mut("key") {
        if keys.is_empty() {
            bail!("primary key `key` contained no keys");
        }
        let first = keys.swap_remove(0);
        if let Some(ASTHolder::Commands(MessageValue::List(com))) = &qm.ast {
            let original_command = com
                .iter()
                .map(|v| {
                    if let MessageValue::Bytes(b) = v {
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
                MessageValue::Bytes(Bytes::from_static(b"EVAL")),
                MessageValue::Bytes(Bytes::from(script)),
                MessageValue::Bytes(Bytes::from_static(b"1")),
                first,
            ];
            qm.ast = Some(ASTHolder::Commands(MessageValue::List(commands)));
        } else {
            bail!("Ast is not a command with a list");
        }
    } else {
        bail!("Primary key `key` does not exist or is not a list");
    }

    Ok(())
}

fn unwrap_response(qr: &mut QueryResponse) {
    if let Some(MessageValue::List(mut values)) = qr.result.take() {
        let all_lists = values.iter().all(|v| matches!(v, MessageValue::List(_)));
        qr.result = if all_lists && values.len() > 1 {
            // This means the result is likely from a transaction or something that returns
            // lots of things

            let mut timestamps: Vec<MessageValue> = vec![];
            let mut results: Vec<MessageValue> = vec![];
            for v_u in values {
                if let MessageValue::List(mut v) = v_u {
                    if v.len() == 2 {
                        let timestamp = v.pop().unwrap();
                        let actual = v.pop().unwrap();
                        timestamps.push(timestamp);
                        results.push(actual);
                    }
                }
            }

            qr.response_meta = Some(MessageValue::Document(BTreeMap::from([(
                "timestamp".to_string(),
                MessageValue::List(timestamps),
            )])));
            Some(MessageValue::List(results))
        } else if values.len() == 2 {
            qr.response_meta = values.pop().map(|v| {
                let processed_value = match v {
                    MessageValue::Integer(i, _) => {
                        let start = SystemTime::now();
                        let since_the_epoch = start
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards");
                        let seconds = since_the_epoch.as_secs();
                        if seconds.leading_ones() > 1 {
                            panic!("Cannot convert u64 to i64 without overflow");
                        }
                        MessageValue::Integer(seconds as i64 - i, IntSize::I64)
                    }
                    v => v,
                };
                MessageValue::Document(BTreeMap::from([("timestamp".to_string(), processed_value)]))
            });
            values.pop()
        } else {
            Some(MessageValue::List(values))
        };
    }
}

#[async_trait]
impl Transform for RedisTimestampTagger {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // TODO: This is wrong. We need to keep track of tagged_success per message
        let mut tagged_success = true;
        // TODO: This is wrong. We need more robust handling of transactions
        let mut exec_block = false;

        for message in message_wrapper.messages.iter_mut() {
            message.generate_message_details_query();
            if let MessageDetails::Query(ref mut qm) = message.details {
                if let Some(a) = &qm.ast {
                    if a.get_command() == *"EXEC" {
                        exec_block = true;
                    }
                }

                if let Err(err) = wrap_command(qm) {
                    trace!("Couldn't wrap command with timestamp tagger: {}", err);
                    tagged_success = false;
                }
                message.modified = true;
            }
        }

        let mut response = message_wrapper.call_next_transform().await;
        debug!("tagging transform got {:?}", response);
        if let Ok(messages) = &mut response {
            if tagged_success || exec_block {
                for message in messages.iter_mut() {
                    message.generate_message_details_response();
                    if let MessageDetails::Response(qr) = &mut message.details {
                        unwrap_response(qr);
                        message.modified = true;
                    }
                }
            }
        }

        debug!("response after trying to unwrap -> {:?}", response);
        response
    }
}
