use std::collections::HashMap;
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::transforms::{TransformsFromConfig, Transforms};
use crate::config::ConfigError;
use crate::runtimes::rhai::RhaiEnvironment;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    function_env: RhaiEnvironment,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RouteConfig {
    #[serde(rename = "config_values")]
    pub route_map: HashMap<String, String>,
    pub script: String
}

#[async_trait]
impl TransformsFromConfig for RouteConfig {
    async fn get_source(&self, transforms: &HashMap<String, TransformChain>) -> Result<Transforms, ConfigError> {
        let mut temp : HashMap<String, TransformChain> = HashMap::new();
        for (k, v) in &self.route_map {
            if let Some(t) = transforms.get(v.as_str()) {
                temp.insert(k.clone(), t.clone());

            } else {
                return Err(ConfigError{})
            }
        }

        let script_env = RhaiEnvironment::new(&self.script)?;

        Ok(Transforms::Route(Route{
            name: "Route",
            route_map: temp,
            function_env: script_env
        }))
    }
}


#[async_trait]
impl Transform for Route {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let routes: Vec<String> = self.route_map.keys().map(|x| x).cloned().collect();
        let mut chosen_route = self.function_env.call_routing_func(qd.clone(), routes)?;
        qd.reset();
        let result = self.route_map.get(chosen_route.as_str()).unwrap().process_request(qd).await;
        return self.function_env.call_route_handle_func(result, chosen_route)
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

