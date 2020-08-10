use crate::config::topology::TopicHolder;

use crate::error::ChainResponse;
use crate::message::{ASTHolder, Message, QueryMessage, QueryResponse, Value};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

#[derive(Clone)]
pub struct RedisTimestampTagger {
    name: &'static str,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisTimestampTaggerConfig {}

#[async_trait]
impl TransformsFromConfig for RedisTimestampTaggerConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
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
                            unsafe { String::from_utf8_unchecked(b.to_vec()) }
                        } else {
                            // TODO this might not be right... but we should only be dealing with bytes
                            format!("{:?}", v)
                        }
                    })
                    .join(",");

                let original_pcall = format!(r###"redis.call({})""###, original_command);
                let last_used_pcall = r###"redis.call('OBJECT', 'IDLETIME', KEYS[1])""###;

                let script = format!("return {{{},{}}}", original_pcall, last_used_pcall);
                debug!("Generated eval script for timestamp: {}", script);
                let key_count = keys.len();
                let commands: Vec<Value> = vec![
                    value_byte_string("EVAL".to_string()),
                    value_byte_string(script),
                    value_byte_string(format!("{}", key_count)),
                    first.clone(),
                ];
                return Ok(Value::List(commands));
            }
        }
    }
    Err(anyhow!("Could not build command from AST"))

    // redis.call('set','foo','bar')"
}

fn try_tag_query_message(qm: &QueryMessage) -> Message {
    let mut m = qm.clone();
    if let Ok(wrapped) = wrap_command(qm) {
        std::mem::swap(&mut m.ast, &mut Some(ASTHolder::Commands(wrapped)));
        return Message::Modified(Box::new(Message::Query(m)));
    }
    return Message::Query(m);
}

#[async_trait]
impl Transform for RedisTimestampTagger {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        match &qd.message {
            Message::Query(qm) => {
                let tagged = try_tag_query_message(qm);
                qd.swap_message(tagged);
            }
            Message::Modified(m) => {
                if let Message::Query(ref qm) = **m {
                    let tagged = try_tag_query_message(qm);
                    qd.swap_message(tagged);
                }
            }
            _ => {}
        }
        let response = self.call_next_transform(qd, t).await;
        debug!("tagging transform got {:?}", response);

        return response;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
