use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::stream::StreamExt;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{ASTHolder, Message, QueryMessage, QueryResponse, QueryType, Value};
use crate::protocols::RawFrame;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};

#[derive(Clone)]
pub struct RedisCluster {
    name: &'static str,
    route_map: Vec<TransformChain>,
    timeout: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RedisClusterConfig {
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
}

#[async_trait]
impl TransformsFromConfig for RedisClusterConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let mut temp: Vec<TransformChain> = Vec::with_capacity(self.route_map.len());
        for (key, value) in self.route_map.clone() {
            temp.push(build_chain_from_config(key, &value, topics).await?);
        }

        Ok(Transforms::RedisCluster(RedisCluster {
            name: "RedisCluster",
            route_map: temp,
            timeout: 50,
        }))
    }
}

#[async_trait]
impl Transform for RedisCluster {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        let sref = self;

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

        let mut collated_results = message_holder
            .iter()
            .cloned()
            .filter_map(move |m| match m {
                Message::Response(qr) => Some(qr),
                Message::Modified(m) => {
                    if let Message::Response(qr) = *m {
                        Some(qr)
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect_vec();

        unimplemented!()
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod scatter_transform_tests {}
