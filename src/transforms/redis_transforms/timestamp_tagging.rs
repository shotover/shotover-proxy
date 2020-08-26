use crate::config::topology::TopicHolder;

use crate::error::ChainResponse;
use crate::message::{ASTHolder, Messages, QueryMessage, QueryResponse, Value};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

#[derive(Clone)]
pub struct RedisTimestampTagger {
    name: &'static str,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisTimestampTaggerConfig {}

impl RedisTimestampTagger {
    pub fn new() -> Self {
        RedisTimestampTagger {
            name: "RedisTimestampTagger",
        }
    }
}

#[async_trait]
impl TransformsFromConfig for RedisTimestampTaggerConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisTimeStampTagger(RedisTimestampTagger {
            name: "RedisTimeStampTagger",
        }))
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
                            unsafe { format!("'{}'", String::from_utf8_unchecked(b.to_vec())) }
                        } else {
                            // TODO this might not be right... but we should only be dealing with bytes
                            format!("{:?}", v)
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

    // redis.call('set','foo','bar')"
}

fn try_tag_query_message(qm: &QueryMessage) -> (bool, Messages) {
    let mut m = qm.clone();
    if let Ok(wrapped) = wrap_command(qm) {
        std::mem::swap(&mut m.ast, &mut Some(ASTHolder::Commands(wrapped)));
        return (true, Messages::Modified(Box::new(Messages::Query(m))));
    }
    (false, Messages::Query(m))
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
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let mut tagged_success: bool = false;
        let mut exec_block: bool = false;
        match &qd.message {
            Messages::Query(qm) => {
                if let Some(a) = &qm.ast {
                    if a.get_command() == *"EXEC" {
                        exec_block = true;
                    }
                }

                let (tagged, message) = try_tag_query_message(qm);
                qd.swap_message(message);
                tagged_success = tagged;
            }
            Messages::Modified(m) => {
                if let Messages::Query(ref qm) = **m {
                    if let Some(a) = &qm.ast {
                        if a.get_command() == *"EXEC" {
                            exec_block = true;
                        }
                    }

                    let (tagged, message) = try_tag_query_message(qm);
                    qd.swap_message(message);
                    tagged_success = tagged;
                }
            }
            Messages::Bulk(bulk_messages) => {
                let new_messages: Result<Vec<Messages>> = bulk_messages
                    .iter()
                    .map(|message| {
                        if let Messages::Query(ref qm) = message {
                            if let Some(a) = &qm.ast {
                                if a.get_command() == *"EXEC" {
                                    exec_block = true;
                                }
                            }

                            let (tagged, message) = try_tag_query_message(qm);
                            tagged_success = tagged;
                            return Ok(message);
                        }
                        Err(anyhow!("not a query"))
                    })
                    .collect();

                qd.swap_message(Messages::Bulk(new_messages?));
            }
            _ => {}
        }
        let mut response = self.call_next_transform(qd, t).await;
        debug!("tagging transform got {:?}", response);
        if tagged_success || exec_block {
            match &mut response {
                Ok(Messages::Response(qr)) => {
                    unwrap_response(qr);
                    response = response.map(|m| Messages::Modified(Box::new(m)));
                }
                Ok(Messages::Bulk(messages)) => {
                    for message in messages {
                        if let Messages::Response(qr) = message {
                            unwrap_response(qr);
                        }
                    }
                    response = response.map(|m| Messages::Modified(Box::new(m)));
                }
                _ => {}
            }
        }
        debug!("response after trying to unwrap -> {:?}", response);
        response
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
