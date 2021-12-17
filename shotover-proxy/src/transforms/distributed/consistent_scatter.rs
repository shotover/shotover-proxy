use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, QueryResponse, QueryType, Value};
use crate::protocols::RawFrame;
use crate::transforms::chain::BufferedChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, Wrapper,
};

#[derive(Clone)]
pub struct ConsistentScatter {
    route_map: Vec<BufferedChain>,
    write_consistency: i32,
    read_consistency: i32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConsistentScatterConfig {
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub write_consistency: i32,
    pub read_consistency: i32,
}

impl ConsistentScatterConfig {
    pub async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let mut route_map = Vec::with_capacity(self.route_map.len());
        warn!("Using this transform is considered unstable - Does not work with REDIS pipelines");

        for (key, value) in &self.route_map {
            route_map.push(
                build_chain_from_config(key.clone(), value, topics)
                    .await?
                    .into_buffered_chain(10),
            );
        }
        route_map.sort_by_key(|x| x.original_chain.name.clone());

        Ok(Transforms::ConsistentScatter(ConsistentScatter {
            route_map,
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
        }))
    }
}

fn get_timestamp(frag: &QueryResponse) -> i64 {
    debug!("\n\n {:#?} \n\n", frag.response_meta);
    if let Some(Value::Document(meta)) = frag.response_meta.as_ref() {
        if let Some(t) = meta.get("timestamp") {
            if let Value::Integer(i, _) = t {
                return *i;
            }
            return 0;
        }
    }
    0
}

fn get_size(frag: &QueryResponse) -> usize {
    frag.result.as_ref().map_or(0, std::mem::size_of_val)
}

fn resolve_fragments(fragments: &mut Vec<QueryResponse>) -> Option<QueryResponse> {
    let mut newest_fragment: Option<QueryResponse> = None;
    let mut biggest_fragment: Option<QueryResponse> = None;

    // Check the age of the response, store most recent
    // If we don't have an age, store the biggest one.
    // Return newest, otherwise biggest. Returns newest, even
    // if we have a bigger response.
    while !fragments.is_empty() {
        if let Some(fragment) = fragments.pop() {
            let candidate = get_timestamp(&fragment);
            if candidate > 0 {
                match newest_fragment {
                    None => newest_fragment = Some(fragment),
                    Some(ref frag) => {
                        let current = get_timestamp(frag);
                        if candidate > current {
                            newest_fragment = Some(fragment);
                        }
                    }
                }
            } else {
                let candidate = get_size(&fragment);
                match newest_fragment {
                    None => newest_fragment = Some(fragment),
                    Some(ref frag) => {
                        let current = get_size(frag);
                        if candidate > current {
                            biggest_fragment = Some(fragment);
                        }
                    }
                }
            }
        }
    }
    trace!("fragments {:?}-{:?}", newest_fragment, biggest_fragment);
    if newest_fragment.is_some() {
        newest_fragment
    } else {
        // panic!("shouldn't happen");
        biggest_fragment
    }
}

impl ConsistentScatter {
    fn get_name(&self) -> &'static str {
        "ConsistentScatter"
    }
}

#[async_trait]
impl Transform for ConsistentScatter {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let required_successes: Vec<_> = message_wrapper
            .messages
            .iter_mut()
            .map(|m| {
                m.generate_message_details_query();

                if m.get_query_type() == QueryType::Read {
                    self.read_consistency
                } else {
                    self.write_consistency
                }
            })
            .collect();
        let max_required_successes = *required_successes
            .iter()
            .max()
            .unwrap_or(&self.write_consistency);

        // Bias towards the write_consistency value for everything else
        let mut rec_fu = FuturesUnordered::new();

        //TODO: FuturesUnordered does bias to polling the first submitted task - this will bias all requests
        for chain in self.route_map.iter_mut() {
            rec_fu.push(chain.process_request(message_wrapper.clone(), None));
        }

        let mut results = Vec::new();
        while let Some(res) = rec_fu.next().await {
            match res {
                Ok(mut messages) => {
                    debug!("{:#?}", messages);
                    for message in &mut messages {
                        message.generate_message_details_response();
                    }
                    results.push(messages);
                }
                Err(e) => {
                    error!("failed response {}", e);
                }
            }
            if results.len() >= max_required_successes as usize {
                break;
            }
        }

        drop(rec_fu);

        Ok(if results.len() < max_required_successes as usize {
            required_successes
                .iter()
                .map(|_| {
                    Message::new_response(
                        QueryResponse::empty_with_error(Some(Value::Strings(
                            "Not enough responses".to_string(),
                        ))),
                        true,
                        RawFrame::None,
                    )
                })
                .collect()
        } else {
            required_successes
                .into_iter()
                .filter_map(|_required_successes| {
                    let mut collated_results = vec![];
                    for res in &mut results {
                        if let Some(m) = res.pop() {
                            if let MessageDetails::Response(qm) = m.details {
                                collated_results.push(qm);
                            }
                        }
                    }
                    resolve_fragments(&mut collated_results)
                        .map(|qr| Message::new_response(qr, true, RawFrame::None))
                })
                // We do this as we are pop'ing from the end of the results in the filter_map above
                .rev()
                .collect()
        })
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
                    .original_chain
                    .validate()
                    .into_iter()
                    .map(|x| format!("  {}", x))
            })
            .collect::<Vec<String>>();

        if !errors.is_empty() {
            errors.insert(0, format!("{}:", self.get_name()));
        }

        errors
    }
}

#[cfg(test)]
mod scatter_transform_tests {
    use crate::transforms::chain::{BufferedChain, TransformChain};
    use crate::transforms::debug::printer::DebugPrinter;
    use crate::transforms::debug::returner::{DebugReturner, Response};
    use crate::transforms::distributed::consistent_scatter::ConsistentScatter;

    use crate::message::{
        Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
    };
    use crate::protocols::RawFrame;
    use crate::transforms::null::Null;
    use crate::transforms::{Transform, Transforms, Wrapper};
    use std::collections::HashMap;

    fn check_ok_responses(mut messages: Messages, expected_ok: &Value) {
        let test_message_details = messages.pop().unwrap().details;
        if let MessageDetails::Response(QueryResponse {
            result: Some(r), ..
        }) = test_message_details
        {
            assert_eq!(expected_ok, &r);
        } else {
            panic!("Couldn't destructure message");
        }
    }

    fn check_err_responses(mut messages: Messages, expected_err: &Value) {
        if let MessageDetails::Response(QueryResponse {
            error: Some(err), ..
        }) = messages.pop().unwrap().details
        {
            assert_eq!(expected_err, &err);
        } else {
            panic!("Couldn't destructure message");
        }
    }

    async fn build_chains(route_map: HashMap<String, TransformChain>) -> Vec<BufferedChain> {
        route_map
            .into_values()
            .map(|x| x.into_buffered_chain(10))
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scatter_success() {
        let response = vec![Message::new(
            MessageDetails::Response(QueryResponse::just_result(Value::Strings("OK".to_string()))),
            true,
            RawFrame::None,
        )];

        let wrapper = Wrapper::new(vec![Message::new(
            MessageDetails::Query(QueryMessage {
                query_string: "".to_string(),
                namespace: vec![String::from("keyspace"), String::from("old")],
                primary_key: Default::default(),
                query_values: None,
                projection: None,
                query_type: QueryType::Read,
                ast: None,
            }),
            true,
            RawFrame::None,
        )]);

        let ok_repeat =
            Transforms::DebugReturner(DebugReturner::new(Response::Message(response.clone())));
        let err_repeat = Transforms::DebugReturner(DebugReturner::new(Response::Fail));

        let mut two_of_three = HashMap::new();
        two_of_three.insert(
            "one".to_string(),
            TransformChain::new(vec![ok_repeat.clone()], "one".to_string()),
        );
        two_of_three.insert(
            "two".to_string(),
            TransformChain::new(vec![ok_repeat.clone()], "two".to_string()),
        );
        two_of_three.insert(
            "three".to_string(),
            TransformChain::new(vec![err_repeat.clone()], "three".to_string()),
        );

        let mut tuneable_success_consistency = Transforms::ConsistentScatter(ConsistentScatter {
            route_map: build_chains(two_of_three).await,
            write_consistency: 2,
            read_consistency: 2,
        });

        let expected_ok = Value::Strings("OK".to_string());

        let test = tuneable_success_consistency
            .transform(wrapper.clone())
            .await
            .unwrap();

        check_ok_responses(test, &expected_ok);

        let mut one_of_three = HashMap::new();
        one_of_three.insert(
            "one".to_string(),
            TransformChain::new(vec![ok_repeat.clone()], "one".to_string()),
        );
        one_of_three.insert(
            "two".to_string(),
            TransformChain::new(vec![err_repeat.clone()], "two".to_string()),
        );
        one_of_three.insert(
            "three".to_string(),
            TransformChain::new(vec![err_repeat.clone()], "three".to_string()),
        );

        let mut tuneable_fail_consistency = Transforms::ConsistentScatter(ConsistentScatter {
            route_map: build_chains(one_of_three).await,
            write_consistency: 2,
            read_consistency: 2,
        });

        let response_fail = tuneable_fail_consistency
            .transform(wrapper.clone())
            .await
            .unwrap();

        let expected_err = Value::Strings("Not enough responses".to_string());

        check_err_responses(response_fail, &expected_err);
    }

    #[tokio::test]
    async fn test_validate_invalid_chain() {
        let chain_1 = TransformChain::new_no_shared_state(
            vec![
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::Null(Null::default()),
            ],
            "test-chain-1".to_string(),
        );
        let chain_2 = TransformChain::new_no_shared_state(vec![], "test-chain-2".to_string());

        let transform = ConsistentScatter {
            route_map: vec![
                chain_1.into_buffered_chain(10),
                chain_2.into_buffered_chain(10),
            ],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(
            transform.validate(),
            vec![
                "ConsistentScatter:",
                "  test-chain-2:",
                "    Chain cannot be empty"
            ]
        );
    }

    #[tokio::test]
    async fn test_validate_valid_chain() {
        let chain_1 = TransformChain::new_no_shared_state(
            vec![
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::Null(Null::default()),
            ],
            "test-chain-1".to_string(),
        );
        let chain_2 = TransformChain::new_no_shared_state(
            vec![
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::DebugPrinter(DebugPrinter::new()),
                Transforms::Null(Null::default()),
            ],
            "test-chain-2".to_string(),
        );

        let transform = ConsistentScatter {
            route_map: vec![
                chain_1.into_buffered_chain(10),
                chain_2.into_buffered_chain(10),
            ],
            write_consistency: 1,
            read_consistency: 1,
        };

        assert_eq!(transform.validate(), Vec::<String>::new());
    }
}
