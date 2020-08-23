use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, QueryMessage};
use crate::runtimes::{ScriptConfigurator, ScriptDefinition, ScriptHolder};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    route_script: ScriptHolder<(QueryMessage, Vec<String>), String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RouteConfig {
    #[serde(rename = "config_values")]
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub route_script: ScriptConfigurator,
}

#[async_trait]
impl TransformsFromConfig for RouteConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Transforms> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.route_map.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, &topics).await?,
            );
        }
        Ok(Transforms::Route(Route {
            name: "scatter",
            route_map: temp,
            route_script: self.route_script.get_script_func()?,
        }))
    }
}

#[async_trait]
impl Transform for Route {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Message::Query(qm) = &qd.message {
            let routes: Vec<String> = self.route_map.keys().map(|x| x).cloned().collect();
            let chosen_route = self
                .route_script
                .call(&t.lua_runtime, (qm.clone(), routes))?;
            qd.reset();
            return self
                .route_map
                .get(chosen_route.as_str())
                .unwrap()
                .process_request(qd, self.get_name().to_string())
                .await;
        }

        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }

    async fn prep_transform_chain(&mut self, t: &mut TransformChain) -> Result<()> {
        self.route_script.prep_lua_runtime(&t.lua_runtime)
    }
}
