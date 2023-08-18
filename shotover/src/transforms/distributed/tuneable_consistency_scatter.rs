use crate::config::chain::TransformChainConfig;
use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, Messages, QueryType};
use crate::transforms::chain::{BufferedChain, TransformChainBuilder};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use tracing::{error, warn};

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TuneableConsistencyScatterConfig {
    pub route_map: HashMap<String, TransformChainConfig>,
    pub write_consistency: i32,
    pub read_consistency: i32,
}

#[typetag::deserialize(name = "TuneableConsistencyScatter")]
#[async_trait(?Send)]
impl TransformConfig for TuneableConsistencyScatterConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        let mut route_map = Vec::with_capacity(self.route_map.len());
        warn!("Using this transform is considered unstable - Does not work with REDIS pipelines");

        for (key, value) in &self.route_map {
            route_map.push(value.get_builder(key.clone()).await?);
        }
        route_map.sort_by_key(|x| x.name.clone());

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
    fn build(&self) -> Transforms {
        Transforms::TuneableConsistencyScatter(TuneableConsistentencyScatter {
            route_map: self
                .route_map
                .iter()
                .map(|x| x.build_buffered(10))
                .collect(),
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
        })
    }

    fn get_name(&self) -> &'static str {
        "TuneableConsistencyScatter"
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
        if let Some(RedisFrame::BulkString(bytes)) = frames.get(0) {
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
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
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
    use crate::transforms::{TransformBuilder, Transforms, Wrapper};
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

    async fn build_chains(route_map: HashMap<String, TransformChainBuilder>) -> Vec<BufferedChain> {
        route_map
            .into_values()
            .map(|x| x.build_buffered(10))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scatter_success() {
        let response = vec![Message::from_frame(Frame::Redis(RedisFrame::BulkString(
            "OK".into(),
        )))];

        let wrapper = Wrapper::new(vec![Message::from_frame(Frame::Redis(
            RedisFrame::BulkString(Bytes::from_static(b"foo")),
        ))]);

        let ok_repeat = Box::new(DebugReturner::new(Response::Message(response.clone())));
        let err_repeat = Box::new(DebugReturner::new(Response::Fail));

        let mut two_of_three = HashMap::new();
        two_of_three.insert(
            "one".to_string(),
            TransformChainBuilder::new(vec![ok_repeat.clone()], "one".to_string()),
        );
        two_of_three.insert(
            "two".to_string(),
            TransformChainBuilder::new(vec![ok_repeat.clone()], "two".to_string()),
        );
        two_of_three.insert(
            "three".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "three".to_string()),
        );

        let mut tuneable_success_consistency =
            Transforms::TuneableConsistencyScatter(TuneableConsistentencyScatter {
                route_map: build_chains(two_of_three).await,
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
            TransformChainBuilder::new(vec![ok_repeat.clone()], "one".to_string()),
        );
        one_of_three.insert(
            "two".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "two".to_string()),
        );
        one_of_three.insert(
            "three".to_string(),
            TransformChainBuilder::new(vec![err_repeat.clone()], "three".to_string()),
        );

        let mut tuneable_fail_consistency =
            Transforms::TuneableConsistencyScatter(TuneableConsistentencyScatter {
                route_map: build_chains(one_of_three).await,
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
            "test-chain-1".to_string(),
        );
        let chain_2 = TransformChainBuilder::new(vec![], "test-chain-2".to_string());

        let transform = TuneableConsistencyScatterBuilder {
            route_map: vec![chain_1, chain_2],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(
            transform.validate(),
            vec![
                "TuneableConsistencyScatter:",
                "  test-chain-2:",
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
            "test-chain-1".to_string(),
        );
        let chain_2 = TransformChainBuilder::new(
            vec![
                Box::<DebugPrinter>::default(),
                Box::<DebugPrinter>::default(),
                Box::<NullSink>::default(),
            ],
            "test-chain-2".to_string(),
        );

        let transform = TuneableConsistencyScatterBuilder {
            route_map: vec![chain_1, chain_2],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
