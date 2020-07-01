// #![cfg(tokio_unstable)]

use crate::config::topology::TopicHolder;

use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::error::ChainResponse;
use anyhow::{Result};
use futures::stream::FuturesUnordered;
use tokio::stream::StreamExt;
use tokio::time::timeout;

use crate::message::{Message, QueryResponse, Value};
use crate::concurrency::*;
use crate::scope;
use std::borrow::Borrow;
use crate::concurrency::scope_impl;
use std::time::Duration;
#[derive(Clone)]
pub struct TuneableConsistency {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    number_of_successes: i32,
    timeout: u64
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TuneableConsistencyConfig {
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub number_of_successes: i32,
}

#[async_trait]
impl TransformsFromConfig for TuneableConsistencyConfig {
    async fn get_source(
        &self,
        topics: &TopicHolder,
    ) -> Result<Transforms> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.route_map.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, topics).await?,
            );
        }
        Ok(Transforms::TuneableConsistency(TuneableConsistency {
            name: "TuneableConsistency",
            route_map: temp,
            number_of_successes: self.number_of_successes,
            timeout: 50
        }))
    }
}

#[async_trait]
impl Transform for TuneableConsistency {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let handle = tokio::runtime::Handle::current();
        let sref = self;
        let func = |scope| {
            async move {
                let mut successes: i32 = 0;
                {
                    let fu: FuturesUnordered<_> = sref.route_map.iter()
                        .map(|(_, c)| {
                            let mut wrapper = qd.clone();
                            wrapper.reset();
                            scope.spawn(timout(Duration::from_millis(sref.timeout) , c.process_request(wrapper)))
                        })
                        .collect();


                    let mut r = fu.take_while(|x| {
                        if let Ok(Ok(_)) = x {
                            successes += 1;
                        }
                        successes < self.number_of_successes
                    });

                    while let Ok(Some(_))= r.try_next().await {}
                }
                return successes >= self.number_of_successes;
            }
        };
        let result = unsafe {scope_impl(handle, func).await};

        return if result {
            drop(result);
            ChainResponse::Ok(Message::Modified(Box::new(Message::Response(QueryResponse::empty()))))
        } else {
            drop(result);
            ChainResponse::Ok(Message::Modified(Box::new(Message::Response(QueryResponse::empty_with_error(Some(Value::Strings("Not enough responses".to_string())))))))
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
