use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::stream::StreamExt;
use tokio::time::timeout;
use tracing::debug;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};

#[derive(Clone)]
pub struct TuneableConsistency {
    name: &'static str,
    route_map: Vec<TransformChain>,
    write_consistency: i32,
    read_consistency: i32,
    timeout: u64,
    count: u32,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TuneableConsistencyConfig {
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub write_consistency: i32,
    pub read_consistency: i32,
}

#[async_trait]
impl TransformsFromConfig for TuneableConsistencyConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let mut temp: Vec<TransformChain> = Vec::with_capacity(self.route_map.len());
        for (key, value) in self.route_map.clone() {
            temp.push(build_chain_from_config(key, &value, topics).await?);
        }
        Ok(Transforms::TuneableConsistency(TuneableConsistency {
            name: "TuneableConsistency",
            route_map: temp,
            write_consistency: self.write_consistency,
            read_consistency: self.read_consistency,
            timeout: 500, //todo this timeout needs to be longer for the initial connection...
            count: 0,
        }))
    }
}

fn get_timestamp(frag: &QueryResponse) -> i64 {
    debug!("\n\n {:#?} \n\n", frag.response_meta);
    if let Some(Value::Document(meta)) = frag.response_meta.as_ref() {
        if let Some(t) = meta.get("timestamp") {
            if let Value::Integer(i) = t {
                return i.clone();
            }
            return 0;
        }
    }
    return 0;
}

fn get_size(frag: &QueryResponse) -> usize {
    return frag.result.as_ref().map_or(0, |v| std::mem::size_of_val(v));
}

fn resolve_fragments<'a>(fragments: &mut Vec<QueryResponse>) -> Option<QueryResponse> {
    let mut newest_fragment: Option<QueryResponse> = None;
    let mut biggest_fragment: Option<QueryResponse> = None;

    // Check the age of the response, store most recent
    // If we don't have an age, store the biggest one.
    // Return newest, otherwise biggest. Returns newest, even
    // if we have a bigger response.
    while fragments.len() != 0 {
        if let Some(fragment) = fragments.pop() {
            let candidate = get_timestamp(&fragment);
            if candidate > 0 {
                match newest_fragment {
                    None => newest_fragment = Some(fragment),
                    Some(ref frag) => {
                        let current = get_timestamp(frag);
                        if candidate > current {
                            newest_fragment.replace(fragment);
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
                            biggest_fragment.replace(fragment);
                        }
                    }
                }
            }
        }
    }
    return if newest_fragment.is_some() {
        newest_fragment
    } else {
        // panic!("shouldn't happen");
        biggest_fragment
    };
}

impl TuneableConsistency {}

#[async_trait]
impl Transform for TuneableConsistency {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let sref = self;
        let required_successes = if let Message::Query(QueryMessage {
            original: _,
            query_string: _,
            namespace: _,
            primary_key: _,
            query_values: _,
            projection: _,
            query_type,
            ast: _,
        }) = &qd.message
        {
            match query_type {
                QueryType::Read => self.read_consistency,
                _ => self.write_consistency,
            }
        } else {
            self.write_consistency
        };
        // Bias towards the write_consistency value for everything else
        let mut successes: i32 = 0;

        let fu: FuturesUnordered<_> = FuturesUnordered::new();

        for i in 0..sref.route_map.len() {
            let u = ((qd.clock.0 + (i as u32)) % (sref.route_map.len() as u32)) as usize;
            if let Some(c) = sref.route_map.get(u) {
                let mut wrapper = qd.clone();
                wrapper.reset();
                fu.push(timeout(
                    Duration::from_millis(sref.timeout),
                    c.process_request(wrapper, self.get_name().to_string()),
                ))
            }
        }

        /*
        Tuneable consistent scatter follows these rules:
        - When a downstream chain returns a Result::Ok for the ChainResponse, that counts as a success
        - This transform doesn't try to resolve data consistency issues, it just waits for successes
         */

        let mut r = fu.take_while(|x| {
            let resp = successes < required_successes;
            if let Ok(Ok(x)) = x {
                debug!("{:?}", x);
                successes += 1;
            }
            resp
        });
        let mut message_holder = vec![];

        while let Some(Ok(Ok(m))) = r.next().await {
            debug!("{:#?}", m);
            message_holder.push(m);
        }

        let collated_results = message_holder
            .iter()
            .cloned()
            .filter_map(move |m| match m {
                Message::Response(qr) => Some(vec![qr]),
                Message::Modified(m) => match *m {
                    Message::Response(qr) => Some(vec![qr]),
                    Message::Bulk(messages) => Some(
                        messages
                            .iter()
                            .filter_map(move |m| {
                                if let Message::Response(qr) = m {
                                    Some(qr.clone())
                                } else {
                                    None
                                }
                            })
                            .collect_vec(),
                    ),
                    _ => None,
                },
                Message::Bulk(messages) => Some(
                    messages
                        .iter()
                        .filter_map(move |m| {
                            if let Message::Response(qr) = m {
                                Some(qr.clone())
                            } else {
                                None
                            }
                        })
                        .collect_vec(),
                ),
                _ => None,
            })
            .collect_vec();

        return if successes >= required_successes {
            if collated_results.len() > 0 {
                let mut responses: Vec<Message> = Vec::new();
                if let Some(first_replica_response) = collated_results.get(0) {
                    for i in 0..first_replica_response.len() {
                        let mut fragments = Vec::new();
                        for j in 0..collated_results.len() {
                            unsafe { fragments.push(collated_results.get_unchecked(j).get(i)) }
                        }
                        let mut fragments = fragments
                            .iter()
                            .filter_map(|v| v.as_deref())
                            .cloned()
                            .collect_vec();
                        if let Some(collated_response) = resolve_fragments(&mut fragments) {
                            responses.push(Message::new_mod(Message::Response(collated_response)))
                        }
                    }
                }
                if responses.len() == 1 {
                    if let Some(m) = responses.pop() {
                        let res = ChainResponse::Ok(m);
                        return res;
                    }
                } else {
                    return ChainResponse::Ok(Message::Bulk(responses));
                }
            }
            ChainResponse::Ok(Message::Modified(Box::new(Message::Response(
                QueryResponse::empty(),
            ))))
        } else {
            debug!("Got {}, needed {}", successes, required_successes);
            ChainResponse::Ok(Message::Modified(Box::new(Message::Response(
                QueryResponse::empty_with_error(Some(Value::Strings(
                    "Not enough responses".to_string(),
                ))),
            ))))
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod scatter_transform_tests {
    use anyhow::anyhow;
    use anyhow::Result;

    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::RawFrame;
    use crate::transforms::chain::{Transform, TransformChain, Wrapper};
    use crate::transforms::distributed::tuneable_consistency_scatter::TuneableConsistency;
    use crate::transforms::test_transforms::ReturnerTransform;
    use crate::transforms::Transforms;

    fn check_ok_responses(
        message: Message,
        expected_ok: &Value,
        expected_count: usize,
    ) -> Result<()> {
        if let Message::Response(QueryResponse {
            matching_query: _,
            original: _,
            result: Some(r),
            error: _,
            response_meta: _,
        }) = message
        {
            if let Value::FragmentedResponese(v) = r {
                let ok_responses = v.iter().filter(|&x| x == expected_ok).count();
                if ok_responses != expected_count {
                    return Err(anyhow!("not enough ok responses {}", ok_responses));
                }
            } else {
                return Err(anyhow!("Expected Fragmented response"));
            }
        }
        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_scatter_success() -> Result<()> {
        let t_holder = TopicHolder::get_test_holder();

        let response =
            Message::Response(QueryResponse::just_result(Value::Strings("OK".to_string())));
        let dummy_chain = TransformChain::new(
            vec![],
            "dummy".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        );

        let wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        }));

        let ok_repeat = Transforms::RepeatMessage(Box::new(ReturnerTransform {
            message: response.clone(),
            ok: true,
        }));
        let err_repeat = Transforms::RepeatMessage(Box::new(ReturnerTransform {
            message: response.clone(),
            ok: false,
        }));

        let mut two_of_three = Vec::new();
        two_of_three.push(TransformChain::new(
            vec![ok_repeat.clone()],
            "one".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));
        two_of_three.push(TransformChain::new(
            vec![ok_repeat.clone()],
            "two".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));
        two_of_three.push(TransformChain::new(
            vec![err_repeat.clone()],
            "three".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));

        let tuneable_success_consistency = Transforms::TuneableConsistency(TuneableConsistency {
            name: "TuneableConsistency",
            route_map: two_of_three,
            write_consistency: 2,
            read_consistency: 2,
            timeout: 500, //todo this timeout needs to be longer for the initial connection...
            count: 0,
        });

        let expected_ok = Value::Strings("OK".to_string());

        check_ok_responses(
            tuneable_success_consistency
                .transform(wrapper.clone(), &dummy_chain)
                .await?,
            &expected_ok,
            2,
        )?;

        let mut one_of_three = Vec::new();
        one_of_three.push(TransformChain::new(
            vec![ok_repeat.clone()],
            "one".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));
        one_of_three.push(TransformChain::new(
            vec![err_repeat.clone()],
            "two".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));
        one_of_three.push(TransformChain::new(
            vec![err_repeat.clone()],
            "three".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        ));

        let tuneable_fail_consistency = Transforms::TuneableConsistency(TuneableConsistency {
            name: "TuneableConsistency",
            route_map: one_of_three,
            write_consistency: 2,
            read_consistency: 2,
            timeout: 500, //todo this timeout needs to be longer for the initial connection...
            count: 0,
        });

        check_ok_responses(
            tuneable_fail_consistency
                .transform(wrapper.clone(), &dummy_chain)
                .await?,
            &expected_ok,
            1,
        )?;

        Ok(())
    }
}
