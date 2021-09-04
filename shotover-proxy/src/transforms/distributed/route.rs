use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::Messages;
use crate::runtimes::{ScriptConfigurator, ScriptDefinition, ScriptHolder};
use crate::transforms::chain::TransformChain;
use crate::transforms::{
    build_chain_from_config, Transform, Transforms, TransformsConfig, TransformsFromConfig, Wrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use mlua::Lua;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    route_script: ScriptHolder<(Messages, Vec<String>), String>,
    lua_runtime: Arc<Mutex<mlua::Lua>>,
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
                build_chain_from_config(key, &value, topics).await?,
            );
        }
        Ok(Transforms::Route(Route {
            name: "scatter",
            route_map: temp,
            route_script: self.route_script.get_script_func()?,
            lua_runtime: Arc::new(Mutex::new(Lua::new())),
        }))
    }
}

#[async_trait]
impl Transform for Route {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        let name = self.get_name().to_string();
        let routes: Vec<String> = self.route_map.keys().cloned().collect();
        let rt = self.lua_runtime.lock().await;
        let chosen_route = self
            .route_script
            .call(rt.borrow(), (message_wrapper.message.clone(), routes))?;
        self.route_map
            .get_mut(chosen_route.as_str())
            .unwrap()
            .process_request(message_wrapper, name)
            .await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }

    async fn prep_transform_chain(&mut self, _t: &mut TransformChain) -> Result<()> {
        let rt = self.lua_runtime.lock().await;
        self.route_script.prep_lua_runtime(rt.borrow())
    }
}
