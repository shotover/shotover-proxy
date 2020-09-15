use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Error, Result};
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, Next};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::stream::StreamExt;
use tokio::time::{timeout, Elapsed, Timeout};
use tracing::{debug, trace};

use crate::config::topology::{ChannelMessage, TopicHolder};
use crate::error::ChainResponse;
use crate::message::{
    Message, MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value,
};
use crate::protocols::RawFrame;
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use std::iter::FromIterator;
use tokio::sync::broadcast::SendError;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct TuneableConsistency {
    name: &'static str,
    route_map: Vec<Sender<ChannelMessage>>,
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
        let mut temp: Vec<Sender<ChannelMessage>> = Vec::with_capacity(self.route_map.len());

        for (key, value) in self.route_map.clone() {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(10);
            let mut chain = build_chain_from_config(key, &value, topics).await?;
            let _jh = tokio::spawn(async move {
                while let Some(ChannelMessage {
                    return_chan,
                    messages,
                }) = rx.recv().await
                {
                    let response = chain
                        .process_request(Wrapper::new(messages), "TuneableConsistency".to_string())
                        .await;
                    if let Some(response_sender) = return_chan {
                        match response_sender.send(response) {
                            Ok(_) => {}
                            Err(e) => trace!(
                                "Dropping response message {:?} as not needed by TuneableConsistency",
                                e
                            ),
                        }
                    }
                }
            });
            temp.push(tx);
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
                return *i;
            }
            return 0;
        }
    }
    0
}

fn get_size(frag: &QueryResponse) -> usize {
    frag.result.as_ref().map_or(0, |v| std::mem::size_of_val(v))
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
    if newest_fragment.is_some() {
        newest_fragment
    } else {
        // panic!("shouldn't happen");
        biggest_fragment
    }
}

impl TuneableConsistency {}

#[async_trait]
impl Transform for TuneableConsistency {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        let required_successes = qd
            .message
            .messages
            .iter()
            .map(|m| {
                if let MessageDetails::Query(QueryMessage {
                    query_string: _,
                    namespace: _,
                    primary_key: _,
                    query_values: _,
                    projection: _,
                    query_type,
                    ast: _,
                }) = &m.details
                {
                    match query_type {
                        QueryType::Read => self.read_consistency,
                        _ => self.write_consistency,
                    }
                } else {
                    self.write_consistency
                }
            })
            .collect_vec();
        let max_required_successes = *required_successes
            .iter()
            .max()
            .unwrap_or_else(|| &self.write_consistency);

        // Bias towards the write_consistency value for everything else
        let mut successes: i32 = 0;
        let name = self.get_name().to_string();
        let timeout_count = self.timeout;
        let mut mut_iter = self.route_map.iter_mut();
        let rec_fu: FuturesUnordered<_> = FuturesUnordered::new();

        //TODO: FuturesUnordered does bias to polling the first submitted task - this will bias all requests
        let send_fu: FuturesUnordered<_> = FuturesUnordered::from_iter(mut_iter.map(|c| {
            let (one_tx, one_rx) = tokio::sync::oneshot::channel::<ChainResponse>();
            rec_fu.push(timeout(Duration::from_millis(timeout_count), one_rx));
            let chan_message = ChannelMessage::new(qd.message.clone(), one_tx);
            c.send(chan_message)
        }));

        let _ = send_fu.collect::<Vec<_>>().await;

        // for i in 0..self.route_map.len() {
        //     let u = ((qd.clock.0 + (i as u32)) % (self.route_map.len() as u32)) as usize;
        //     if let Some(c) = self.route_map.get_mut(u) {
        //         let mut wrapper = qd.clone();
        //         fu.push(timeout(
        //             Duration::from_millis(self.timeout),
        //             c.process_request(wrapper, self.get_name().clone().to_string()),
        //         ))
        //     }
        // }

        /*
        Tuneable consistent scatter follows these rules:
        - When a downstream chain returns a Result::Ok for the ChainResponse, that counts as a success
        - This transform doesn't try to resolve data consistency issues, it just waits for successes
         */

        let mut r = rec_fu.take_while(|x| {
            let resp = successes < max_required_successes;
            if let Ok(Ok(x)) = x {
                debug!("{:?}", x);
                successes += 1;
            }
            resp
        });

        let mut results: Vec<Messages> = Vec::new();
        while let Some(Ok(Ok(Ok(messages)))) = r.next().await {
            debug!("{:#?}", messages);
            results.push(messages);
        }

        drop(r);

        if results.len()
            < *required_successes
                .iter()
                .max()
                .unwrap_or_else(|| &self.write_consistency) as usize
        {
            let collated_response: Vec<Message> = required_successes
                .iter()
                .map(|_| {
                    Message::new_response(
                        QueryResponse::empty_with_error(Some(Value::Strings(
                            "Not enough responses".to_string(),
                        ))),
                        true,
                        RawFrame::NONE,
                    )
                })
                .collect_vec();

            Ok(Messages {
                messages: collated_response,
            })
        } else {
            let mut collated_response: Vec<Message> = required_successes
                .into_iter()
                .filter_map(|_required_successes| {
                    let mut collated_results = vec![];
                    for res in &mut results {
                        if let Some(m) = res.messages.pop() {
                            if let MessageDetails::Response(qm) = &m.details {
                                collated_results.push(qm.clone());
                            }
                        }
                    }
                    resolve_fragments(&mut collated_results)
                        .map(|qr| Message::new_response(qr, true, RawFrame::NONE))
                })
                .collect_vec();

            // We do this as we are pop'ing from the end of the results in the filter_map above
            collated_response.reverse();

            Ok(Messages {
                messages: collated_response,
            })
        }
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
    use crate::message::{MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::RawFrame;
    use crate::transforms::chain::TransformChain;
    use crate::transforms::distributed::tuneable_consistency_scatter::TuneableConsistency;
    use crate::transforms::test_transforms::ReturnerTransform;
    use crate::transforms::{Transform, Transforms, Wrapper};

    fn check_ok_responses(
        mut message: Messages,
        expected_ok: &Value,
        _expected_count: usize,
    ) -> Result<()> {
        let test_message_details = message.messages.pop().unwrap().details;
        println!("{:?}", test_message_details);
        if let MessageDetails::Response(QueryResponse {
            matching_query: _,
            result: Some(r),
            error: _,
            response_meta: _,
        }) = test_message_details
        {
            assert_eq!(expected_ok, &r);
            Ok(())
        } else {
            Err(anyhow!("Couldn't destructure message"))
        }
    }

    fn check_err_responses(
        mut message: Messages,
        expected_err: &Value,
        _expected_count: usize,
    ) -> Result<()> {
        if let MessageDetails::Response(QueryResponse {
            matching_query: _,
            result: _,
            error: Some(err),
            response_meta: _,
        }) = message.messages.pop().unwrap().details
        {
            assert_eq!(expected_err, &err);
            Ok(())
        } else {
            Err(anyhow!("Couldn't destructure message"))
        }
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_scatter_success() -> Result<()> {
        let t_holder = TopicHolder::get_test_holder();

        let response = Messages::new_single_response(
            QueryResponse::just_result(Value::Strings("OK".to_string())),
            true,
            RawFrame::NONE,
        );
        let _dummy_chain = TransformChain::new(
            vec![],
            "dummy".to_string(),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        );

        let wrapper = Wrapper::new(Messages::new_single_query(
            QueryMessage {
                query_string: "".to_string(),
                namespace: vec![String::from("keyspace"), String::from("old")],
                primary_key: Default::default(),
                query_values: None,
                projection: None,
                query_type: QueryType::Read,
                ast: None,
            },
            true,
            RawFrame::NONE,
        ));

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

        let mut tuneable_success_consistency =
            Transforms::TuneableConsistency(TuneableConsistency {
                name: "TuneableConsistency",
                route_map: two_of_three,
                write_consistency: 2,
                read_consistency: 2,
                timeout: 5000, //todo this timeout needs to be longer for the initial connection...
                count: 0,
            });

        let expected_ok = Value::Strings("OK".to_string());

        let test = tuneable_success_consistency
            .transform(wrapper.clone())
            .await;

        println!("{:?}", test);

        check_ok_responses(test?, &expected_ok, 2)?;

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

        let mut tuneable_fail_consistency = Transforms::TuneableConsistency(TuneableConsistency {
            name: "TuneableConsistency",
            route_map: one_of_three,
            write_consistency: 2,
            read_consistency: 2,
            timeout: 500, //todo this timeout needs to be longer for the initial connection...
            count: 0,
        });

        let response_fail = tuneable_fail_consistency.transform(wrapper.clone()).await?;

        let expected_err = Value::Strings("Not enough responses".to_string());

        check_err_responses(response_fail, &expected_err, 1)?;

        Ok(())
    }
}
