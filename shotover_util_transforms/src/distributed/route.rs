// use std::collections::HashMap;
// use std::sync::Arc;
//
// use anyhow::Result;
// use async_trait::async_trait;
// use serde::{Deserialize, Serialize};
// use tokio::sync::Mutex;
//
// use shotover_scripts::{ScriptConfigurator, ScriptDefinition, ScriptHolder};
// use shotover_transforms::TopicHolder;
// use shotover_transforms::{ChainResponse, TransformsFromConfig, Wrapper};
// use shotover_transforms::{Messages, Transform};
//
// use crate::transforms::build_chain_from_config;
// use crate::transforms::chain::{BufferedChain, TransformChain};
// use std::fmt::Debug;
// use std::rc::Rc;
//
// #[derive(Clone, Debug)]
// pub struct Route {
//     name: &'static str,
//     route_map: HashMap<String, BufferedChain>,
//     route_script: ScriptHolder<(Messages, Vec<String>), String>,
//     // lua_runtime: Arc<Mutex<mlua::Lua>>,
// }
//
// #[derive(Deserialize, Serialize, Debug, Clone)]
// pub struct RouteConfig {
//     #[serde(rename = "config_values")]
//     pub route_map: HashMap<String, Vec<Box<dyn TransformsFromConfig + Send + Sync>>>,
//     pub route_script: ScriptConfigurator,
// }
//
// #[typetag::serde]
// #[async_trait]
// impl TransformsFromConfig for RouteConfig {
//     async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
//         let mut temp: HashMap<String, BufferedChain> = HashMap::new();
//         for (key, value) in self.route_map.clone() {
//             temp.insert(
//                 key.clone(),
//                 build_chain_from_config(key, value.as_slice(), &topics)
//                     .await?
//                     .build_buffered_chain(5),
//             );
//         }
//         Ok(Box::new(Route {
//             name: "scatter",
//             route_map: temp,
//             route_script: self.route_script.get_script_func()?,
//             // lua_runtime: Arc::new(Mutex::new(Lua::new())),
//         }))
//     }
// }
//
// #[async_trait]
// impl Transform for Route {
//     async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
//         // let name = self.get_name().to_string();
//         // let routes: Vec<String> = self.route_map.keys().cloned().collect();
//         // let rt = self.lua_runtime.lock().await;
//         // let chosen_route = self
//         //     .route_script
//         //     .call(rt.borrow(), (wrapped_messages.message.clone(), routes))?;
//         // self.route_map
//         //     .get_mut(chosen_route.as_str())
//         //     .unwrap()
//         //     .process_request(wrapped_messages, name)
//         //     .await
//         unimplemented!()
//     }
//
//     fn get_name(&self) -> &'static str {
//         self.name
//     }
// }
