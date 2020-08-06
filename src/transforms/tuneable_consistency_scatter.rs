use crate::config::topology::TopicHolder;

use crate::error::ChainResponse;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::stream::StreamExt;
use tokio::time::timeout;

use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
use itertools::Itertools;
use rand::prelude::*;
use std::time::Duration;
use tracing::{debug, info};

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
                    c.process_request(wrapper),
                ))
            }
        }

        let mut r = fu.take_while(|x| {
            if let Ok(Ok(x)) = x {
                info!("{:?}", x);
                successes += 1;
            }
            successes < required_successes
        });
        let mut collated_results = vec![];
        let mut collated_errors = vec![];

        while let Some(Ok(Ok(m))) = r.next().await {
            if let Message::Response(QueryResponse {
                matching_query: _,
                original: _,
                result,
                error,
            }) = &m
            {
                if let Some(res) = result {
                    collated_results.push(res.clone());
                }
                if let Some(res) = error {
                    collated_errors.push(res.clone());
                }
            }
        }

        // tokio::spawn(async {
        //     // TODO check that we are actually asynchronously draining the late response
        //
        // });

        return if successes >= required_successes {
            let matching = if let Message::Query(qm) = qd.message {
                Some(qm)
            } else {
                None
            };

            if collated_results.len() > 0 || collated_errors.len() > 0 {
                return ChainResponse::Ok(Message::Modified(Box::new(Message::Response(
                    QueryResponse::result_error_with_matching(
                        matching,
                        if collated_results.len() > 0 {
                            Some(Value::FragmentedResponese(collated_results))
                        } else {
                            None
                        },
                        if collated_errors.len() > 0 {
                            Some(Value::FragmentedResponese(collated_errors))
                        } else {
                            None
                        },
                    ),
                ))));
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
    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::RawFrame;
    use crate::transforms::chain::{Transform, TransformChain, Wrapper};
    use crate::transforms::test_transforms::ReturnerTransform;
    use crate::transforms::tuneable_consistency_scatter::TuneableConsistency;
    use crate::transforms::{Transforms, TransformsFromConfig};
    use anyhow::anyhow;
    use anyhow::Result;
    use std::collections::HashMap;

    fn check_ok_responses(
        message: Message,
        expected_ok: &Value,
        expected_count: usize,
    ) -> Result<()> {
        if let Message::Response(QueryResponse {
            matching_query,
            original,
            result: Some(r),
            error,
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
