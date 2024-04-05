use crate::config::chain::TransformChainConfig;
use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, Messages, QueryType};
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{
    Transform, TransformBuilder, TransformConfig, TransformContextBuilder, TransformContextConfig,
    Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, warn};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TuneableConsistencyScatterConfig {
    pub route_map: HashMap<String, TransformChainConfig>,
    pub write_consistency: i32,
    pub read_consistency: i32,
}

const NAME: &str = "TuneableConsistencyScatter";
#[typetag::serde(name = "TuneableConsistencyScatter")]
#[async_trait(?Send)]
impl TransformConfig for TuneableConsistencyScatterConfig {
    async fn get_builder(
        &self,
        transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        let mut route_map = Vec::with_capacity(self.route_map.len());
        warn!("Using this transform is considered unstable - Does not work with REDIS pipelines");

        for (key, value) in &self.route_map {
            let chain_config = TransformContextConfig {
                chain_name: key.clone(),
                protocol: transform_context.protocol,
            };
            route_map.push(value.get_builder(chain_config).await?);
        }
        route_map.sort_by_key(|x| x.name);

        Ok(Box::new(TuneableConsistencyScatterBuilder {
            route_map,
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
        }))
    }
}

pub struct TuneableConsistencyScatterBuilder {
    route_map: Vec<TransformChainBuilder>,
    write_consistency: i32,
    read_consistency: i32,
}

impl TransformBuilder for TuneableConsistencyScatterBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(TuneableConsistentencyScatter {
            route_map: self
                .route_map
                .iter()
                .map(|x| x.build_buffered(10, transform_context.clone()))
                .collect(),
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
        })
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }

    fn validate(&self) -> Vec<String> {
        let mut errors = self
            .route_map
            .iter()
            .flat_map(|buffer_chain| {
                buffer_chain
                    .validate()
                    .into_iter()
                    .map(|x| format!("  {x}"))
            })
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.get_name()));
        }
        errors
    }
}

#[derive(Clone)]
pub struct TuneableConsistentencyScatter {
    route_map: Vec<BufferedChain>,
    write_consistency: i32,
    read_consistency: i32,
}

fn get_size(_message: &Message) -> usize {
    4 // TODO: Implement. Old impl was just removed because it was broken anyway
}

fn resolve_fragments(fragments: &mut Vec<Message>) -> Message {
    let mut newest_fragment: Option<Message> = None;
    let mut biggest_fragment: Option<Message> = None;

    // Check the age of the response, store most recent
    // If we don't have an age, store the biggest one.
    // Return newest, otherwise biggest. Returns newest, even
    // if we have a bigger response.
    while !fragments.is_empty() {
        if let Some(fragment) = fragments.pop() {
            let candidate = fragment.meta_timestamp.unwrap_or(0);
            if candidate > 0 {
                match &newest_fragment {
                    None => newest_fragment = Some(fragment),
                    Some(frag) => {
                        let current = frag.meta_timestamp.unwrap_or(0);
                        if candidate > current {
                            newest_fragment = Some(fragment);
                        }
                    }
                }
            } else {
                let candidate = get_size(&fragment);
                match &newest_fragment {
                    None => newest_fragment = Some(fragment),
                    Some(frag) => {
                        let current = get_size(frag);
                        if candidate > current {
                            biggest_fragment = Some(fragment);
                        }
                    }
                }
            }
        }
    }

    newest_fragment.or(biggest_fragment).unwrap()
}

fn resolve_fragments_max_integer(fragments: Vec<Message>) -> Message {
    fragments
        .into_iter()
        .map(|mut m| {
            let integer = if let Some(Frame::Redis(RedisFrame::Integer(i))) = m.frame() {
                *i
            } else {
                // Not an expected value so prefer all other values over it
                -1
            };
            (integer, m)
        })
        .max_by_key(|(key, _)| *key)
        .map(|(_, m)| m)
        .unwrap()
}

fn get_upper_command_name(message: &mut Message) -> Vec<u8> {
    if let Some(Frame::Redis(RedisFrame::Array(frames))) = message.frame() {
        if let Some(RedisFrame::BulkString(bytes)) = frames.first() {
            return bytes.to_ascii_uppercase();
        }
    }
    vec![]
}

fn is_error(message: &mut Message) -> bool {
    match message.frame() {
        Some(Frame::Redis(RedisFrame::Error(_))) => true,
        // Also consider a failure to parse the message as an error message
        None => true,
        _ => false,
    }
}

struct Consistency {
    pub consistency: i32,
    pub resolver: Resolver,
}

enum Resolver {
    MaxInteger,
    Standard,
}

#[async_trait]
impl Transform for TuneableConsistentencyScatter {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        // TODO: This is wrong. We need to keep track of tagged_success per message
        let mut tagged_success = true;
        // TODO: This is wrong. We need more robust handling of transactions
        let mut exec_block = false;

        for message in requests_wrapper.requests.iter_mut() {
            if let Some(Frame::Redis(frame)) = message.frame() {
                if let RedisFrame::Array(array) = frame {
                    if array
                        .first()
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

        let mut response = requests_wrapper.call_next_transform().await;
        debug!("tagging transform got {:?}", response);
        if let Ok(messages) = &mut response {
            if tagged_success || exec_block {
                for message in messages.iter_mut() {
                    unwrap_response(message)?;
                }
            }
        }
        debug!("response after trying to unwrap -> {:?}", response);

        let consistency: Vec<_> = requests_wrapper
            .requests
            .iter_mut()
            .map(|m| match get_upper_command_name(m).as_slice() {
                b"DBSIZE" => Consistency {
                    consistency: self.route_map.len() as i32,
                    resolver: Resolver::MaxInteger,
                },
                b"FLUSHDB" => Consistency {
                    consistency: self.route_map.len() as i32,
                    resolver: Resolver::Standard,
                },
                _ => {
                    let consistency = if m.get_query_type() == QueryType::Read {
                        self.read_consistency
                    } else {
                        self.write_consistency
                    };
                    Consistency {
                        consistency,
                        resolver: Resolver::Standard,
                    }
                }
            })
            .collect();
        let max_required_successes = consistency
            .iter()
            .map(|x| x.consistency)
            .max()
            .unwrap_or(self.write_consistency);

        let mut rec_fu = FuturesUnordered::new();

        //TODO: FuturesUnordered does bias to polling the first submitted task - this will bias all requests
        for chain in self.route_map.iter_mut() {
            rec_fu.push(chain.process_request(requests_wrapper.clone(), None));
        }

        let mut results = Vec::new();
        while let Some(res) = rec_fu.next().await {
            match res {
                Ok(messages) => results.push(messages),
                Err(e) => error!("failed response {}", e),
            }
            if results.len() >= max_required_successes as usize {
                break;
            }
        }

        drop(rec_fu);

        Ok(if results.len() < max_required_successes as usize {
            let mut messages = requests_wrapper.requests;
            for message in &mut messages {
                *message = message.to_error_response("Not enough responses".into())?
            }
            messages
        } else {
            consistency
                .into_iter()
                .map(|consistency| {
                    // filter out errors, storing a single error in case we need it later
                    let mut collated_results = vec![];
                    let mut an_error = None;
                    for res in &mut results {
                        if let Some(mut m) = res.pop() {
                            if is_error(&mut m) {
                                if an_error.is_none() {
                                    an_error = Some(m);
                                }
                            } else {
                                collated_results.push(m);
                            }
                        }
                    }

                    // If every message got filtered out then return an error, otherwise apply a resolver to figure out which message to keep
                    if collated_results.is_empty() {
                        an_error.unwrap()
                    } else {
                        match consistency.resolver {
                            Resolver::MaxInteger => resolve_fragments_max_integer(collated_results),
                            Resolver::Standard => resolve_fragments(&mut collated_results),
                        }
                    }
                })
                // We do this as we are pop'ing from the end of the results in the filter_map above
                .rev()
                .collect()
        })
    }
}

mod redis {
    use crate::frame::redis::redis_query_type;
    use crate::frame::{Frame, RedisFrame};
    use crate::message::{Message, Messages, QueryType};
    use crate::transforms::{
        Transform, TransformBuilder, TransformConfig, TransformContextBuilder,
        TransformContextConfig, Wrapper,
    };
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use bytes::Bytes;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::fmt::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tracing::{debug, trace};

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

                    todo!("TuneableConsistencyScatter isnt built to handle multiple timestamps yet: {timestamps:?} {results:?}",)
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
}

#[cfg(test)]
mod scatter_transform_tests {
    use crate::frame::{Frame, RedisFrame};
    use crate::message::{Message, Messages};
    use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::debug::returner::{DebugReturner, Response};
    use crate::transforms::distributed::tuneable_consistency_scatter::{
        TuneableConsistencyScatterBuilder, TuneableConsistentencyScatter,
    };
    use crate::transforms::null::NullSink;
    use crate::transforms::{Transform, TransformBuilder, TransformContextBuilder, Wrapper};
    use bytes::Bytes;
    use std::collections::HashMap;

    fn check_ok_responses(mut messages: Messages) {
        let mut message = messages.pop().unwrap();
        let expected = Frame::Redis(RedisFrame::BulkString("OK".into()));
        assert_eq!(message.frame().unwrap(), &expected);
    }

    fn check_err_responses(mut messages: Messages, expected_err: &str) {
        let mut message = messages.pop().unwrap();
        let expected = Frame::Redis(RedisFrame::Error(format!("ERR {expected_err}").into()));
        assert_eq!(message.frame().unwrap(), &expected);
    }

    fn build_chains(route_map: HashMap<String, TransformChainBuilder>) -> Vec<BufferedChain> {
        let context = TransformContextBuilder::new_test();
        route_map
            .into_values()
            .map(|x| x.build_buffered(10, context.clone()))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scatter_success() {
        let response = Message::from_frame(Frame::Redis(RedisFrame::BulkString("OK".into())));

        let wrapper = Wrapper::new_test(vec![Message::from_frame(Frame::Redis(
            RedisFrame::BulkString(Bytes::from_static(b"foo")),
        ))]);

        let ok_repeat = Box::new(DebugReturner::new(Response::Message(response.clone())));
        let err_repeat = Box::new(DebugReturner::new(Response::Fail));

        let mut two_of_three = HashMap::new();
        two_of_three.insert(
            "one".to_string(),
            TransformChainBuilder::new(vec![ok_repeat.clone()], "one"),
        );
        two_of_three.insert(
            "two".to_string(),
            TransformChainBuilder::new(vec![ok_repeat.clone()], "two"),
        );
        two_of_three.insert(
            "three".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "three"),
        );

        let mut tuneable_success_consistency = Box::new(TuneableConsistentencyScatter {
            route_map: build_chains(two_of_three),
            write_consistency: 2,
            read_consistency: 2,
        });

        let test = tuneable_success_consistency
            .transform(wrapper.clone())
            .await
            .unwrap();

        check_ok_responses(test);

        let mut one_of_three = HashMap::new();
        one_of_three.insert(
            "one".to_string(),
            TransformChainBuilder::new(vec![ok_repeat.clone()], "one"),
        );
        one_of_three.insert(
            "two".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "two"),
        );
        one_of_three.insert(
            "three".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "three"),
        );

        let mut tuneable_fail_consistency = Box::new(TuneableConsistentencyScatter {
            route_map: build_chains(one_of_three),
            write_consistency: 2,
            read_consistency: 2,
        });

        let response_fail = tuneable_fail_consistency
            .transform(wrapper.clone())
            .await
            .unwrap();

        check_err_responses(response_fail, "Not enough responses");
    }

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let chain_1 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-1",
        );
        let chain_2 = TransformChainBuilder::new(vec![], "test-chain-2");

        let transform = TuneableConsistencyScatterBuilder {
            route_map: vec![chain_1, chain_2],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(
            transform.validate(),
            vec![
                "TuneableConsistencyScatter:",
                "  test-chain-2 chain:",
                "    Chain cannot be empty"
            ]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let chain_1 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-1",
        );
        let chain_2 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-2",
        );

        let transform = TuneableConsistencyScatterBuilder {
            route_map: vec![chain_1, chain_2],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
