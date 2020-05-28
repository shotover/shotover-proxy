use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};
use crate::transforms::{
    build_chain_from_config, Transforms, TransformsConfig, TransformsFromConfig,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use slog::Logger;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    python_script: String,
    logger: Logger,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RouteConfig {
    #[serde(rename = "config_values")]
    pub route_map: HashMap<String, Vec<TransformsConfig>>,
    pub python_script: String,
}

#[async_trait]
impl TransformsFromConfig for RouteConfig {
    async fn get_source(
        &self,
        topics: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.route_map.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, &value, &topics, logger).await?,
            );
        }
        Ok(Transforms::Route(Route {
            name: "scatter",
            route_map: temp,
            python_script: self.python_script.clone(),
            logger: logger.clone(),
        }))
    }
}

#[async_trait]
impl Transform for Route {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        // let routes: Vec<String> = self.route_map.keys().map(|x| x).cloned().collect();
        // let mut chosen_route = self.function_env.call_routing_func(qd.clone(), routes)?;
        // qd.reset();
        // let result = self.route_map.get(chosen_route.as_str()).unwrap().process_request(qd).await;
        // return self.function_env.call_route_handle_func(result, chosen_route)
        self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
