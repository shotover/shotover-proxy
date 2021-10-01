use crate::config::topology::TopicHolder;

use crate::error::ChainResponse;
use crate::message::{ASTHolder, MessageDetails, QueryMessage, QueryResponse, Value};
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::Deserialize;
use std::collections::HashMap;
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

#[async_trait]
impl TransformsFromConfig for RedisTimestampTaggerConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisTimeStampTagger(RedisTimestampTagger {}))
    }
}

// The way we try and get a "liveness" timestamp from redis, is to use
// OBJECT IDLETIME. This requires maxmemory-policy is set to an LRU policy
// or noeviction and maxmemory is set.
// Unfortuantly REDIS only provides a 10second resolution on this, so high
// update keys where update freq < 20 seconds, will not be terribly consistent

fn value_byte_string(string: String) -> Value {
    Value::Bytes(Bytes::from(string))
}

fn wrap_command(qm: &QueryMessage) -> Result<Value> {
    if let Some(Value::List(keys)) = qm.primary_key.get("key") {
        //TODO: this could be a take instead
        if let Some(first) = keys.get(0) {
            if let Some(ASTHolder::Commands(Value::List(com))) = &qm.ast {
                let original_command = com
                    .iter()
                    .map(|v| {
                        if let Value::Bytes(b) = v {
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

                let original_pcall = format!(r###"redis.call({})"###, original_command);
                let last_used_pcall = r###"redis.call('OBJECT', 'IDLETIME', KEYS[1])"###;

                let script = format!("return {{{},{}}}", original_pcall, last_used_pcall);
                debug!("\n\nGenerated eval script for timestamp: {}\n\n", script);
                let commands: Vec<Value> = vec![
                    value_byte_string("EVAL".to_string()),
                    value_byte_string(script),
                    value_byte_string("1".to_string()),
                    first.clone(),
                ];
                return Ok(Value::List(commands));
            }
        }
    }
    Err(anyhow!("Could not build command from AST"))
}

fn try_tag_query_message(qm: &mut QueryMessage) -> bool {
    if let Ok(wrapped) = wrap_command(qm) {
        std::mem::swap(&mut qm.ast, &mut Some(ASTHolder::Commands(wrapped)));
        true
    } else {
        trace!("couldn't wrap commands");
        false
    }
}

fn unwrap_response(qr: &mut QueryResponse) {
    if let Some(Value::List(mut values)) = qr.result.clone() {
        let all_lists = values.iter().all(|v| matches!(v, Value::List(_)));
        // This means the result is likely from a transaction or something that returns
        // lots of things
        // panic!("this doesn't seem to work on the test_pass_through_one test")
        if all_lists && values.len() > 1 {
            let mut timestamps: Vec<Value> = vec![];
            let mut results: Vec<Value> = vec![];
            for v_u in values {
                if let Value::List(mut v) = v_u {
                    if v.len() == 2 {
                        let timestamp = v.pop().unwrap();
                        let actual = v.pop().unwrap();
                        timestamps.push(timestamp);
                        results.push(actual);
                    }
                }
            }
            let mut hm: HashMap<String, Value> = HashMap::new();
            hm.insert("timestamp".to_string(), Value::List(timestamps));

            let mut timestamps_holder = Some(Value::Document(hm));
            let mut results_holder = Some(Value::List(results));
            std::mem::swap(&mut qr.response_meta, &mut timestamps_holder);
            std::mem::swap(&mut qr.result, &mut results_holder);
        } else if values.len() == 2 {
            let mut timestamp = values.pop().map(|v| {
                let mut hm: HashMap<String, Value> = HashMap::new();
                if let Value::Integer(i) = v {
                    let start = SystemTime::now();
                    let since_the_epoch = start
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    hm.insert(
                        "timestamp".to_string(),
                        Value::Integer(since_the_epoch.as_secs() as i64 - i),
                    );
                } else {
                    hm.insert("timestamp".to_string(), v);
                }
                Value::Document(hm)
            });
            let mut actual = values.pop();
            std::mem::swap(&mut qr.response_meta, &mut timestamp);
            std::mem::swap(&mut qr.result, &mut actual);
        }
    }
}

#[async_trait]
impl Transform for RedisTimestampTagger {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let mut tagged_success: bool = true;
        let mut exec_block: bool = false;

        for message in message_wrapper.message.messages.iter_mut() {
            message.generate_message_details(false);
            if let MessageDetails::Query(ref mut qm) = message.details {
                if let Some(a) = &qm.ast {
                    if a.get_command() == *"EXEC" {
                        exec_block = true;
                    }
                }

                let tagged = try_tag_query_message(qm);
                tagged_success = tagged_success && tagged;
                message.modified = true;
            }
        }

        let response = message_wrapper.call_next_transform().await;
        debug!("tagging transform got {:?}", response);
        if let Ok(mut messages) = response {
            if tagged_success || exec_block {
                for mut message in messages.messages.iter_mut() {
                    message.generate_message_details(true);
                    if let MessageDetails::Response(ref mut qr) = message.details {
                        unwrap_response(qr);
                        message.modified = true;
                    }
                }
            }
            return Ok(messages);
        }

        debug!("response after trying to unwrap -> {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        "RedisTimeStampTagger"
    }
}
