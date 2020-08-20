use crate::config::topology::TopicHolder;

use crate::error::ChainResponse;
use crate::message::{Message, QueryMessage, QueryResponse, Value};
use crate::runtimes::{ScriptConfigurator, ScriptDefinition, ScriptHolder};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Scatter {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    route_script: ScriptHolder<(QueryMessage, Vec<String>), Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ScatterConfig {
    #[serde(rename = "config_values")]
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub route_script: ScriptConfigurator,
}

#[async_trait]
impl TransformsFromConfig for ScatterConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.route_map.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, topics).await?,
            );
        }
        Ok(Transforms::Scatter(Scatter {
            name: "scatter",
            route_map: temp,
            route_script: self.route_script.get_script_func()?,
        }))
    }
}

#[async_trait]
impl Transform for Scatter {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = &qd.message {
            let routes: Vec<String> = self.route_map.keys().cloned().collect();
            let chosen_route = self
                .route_script
                .call(&t.lua_runtime, (qm.clone(), routes))?;
            if chosen_route.len() == 1 {
                return self
                    .route_map
                    .get(chosen_route.get(0).unwrap().as_str())
                    .unwrap()
                    .process_request(qd, self.get_name().to_string())
                    .await;
            } else if chosen_route.len() == 0 {
                return ChainResponse::Err(anyhow!("no routes found"));
            } else {
                let mut fu = FuturesUnordered::new();
                for ref route in &chosen_route {
                    let chain = self.route_map.get(route.as_str()).unwrap();
                    let mut wrapper = qd.clone();
                    wrapper.reset();
                    fu.push(chain.process_request(wrapper, self.get_name().to_string()));
                }
                // TODO I feel like there should be some streamext function that does this for me

                let mut collated_results = vec![];

                while let Some(Ok(m)) = fu.next().await {
                    if let Message::Response(QueryResponse {
                        matching_query: _,
                        original: _,
                        result,
                        error: _,
                        response_meta: _,
                    }) = &m
                    {
                        if let Some(res) = result {
                            collated_results.push(res.clone());
                        }
                    }
                }
                ChainResponse::Ok(Message::Response(QueryResponse::just_result(
                    Value::FragmentedResponese(collated_results),
                )))
            }
        } else {
            Err(anyhow!("expected a query for the scatter"))
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }

    async fn prep_transform_chain(&mut self, t: &mut TransformChain) -> Result<()> {
        self.route_script.prep_lua_runtime(&t.lua_runtime)?;
        Ok(())
    }
}
