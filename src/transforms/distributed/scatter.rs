use std::borrow::Borrow;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
// use mlua::Lua;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use shotover_scripts::{ScriptConfigurator, ScriptDefinition, ScriptHolder};
use shotover_transforms::TopicHolder;
use shotover_transforms::{
    ChainResponse, Message, MessageDetails, Messages, QueryResponse, Transform, Value, Wrapper,
};
use shotover_transforms::{RawFrame, TransformsFromConfig};

use crate::transforms::build_chain_from_config;
use crate::transforms::chain::TransformChain;
use crate::transforms::InternalTransform;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct Scatter {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    route_script: ScriptHolder<(Messages, Vec<String>), Vec<String>>,
    // lua_runtime: Arc<Mutex<mlua::Lua>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ScatterConfig {
    #[serde(rename = "config_values")]
    pub route_map: HashMap<String, Vec<Box<dyn TransformsFromConfig + Send + Sync>>>,
    pub route_script: ScriptConfigurator,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for ScatterConfig {
    async fn get_source(&self, topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        let mut temp: HashMap<String, TransformChain> = HashMap::new();
        for (key, value) in self.route_map.clone() {
            temp.insert(
                key.clone(),
                build_chain_from_config(key, value.as_slice(), topics).await?,
            );
        }
        Ok(Box::new(Scatter {
            name: "scatter",
            route_map: temp,
            route_script: self.route_script.get_script_func()?,
            // lua_runtime: Arc::new(Mutex::new(Lua::new())),
        }))
    }
}

#[async_trait]
impl Transform for Scatter {
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        // let name = self.get_name().to_string();
        //
        // let routes: Vec<String> = self.route_map.keys().cloned().collect();
        // let rt = self.lua_runtime.lock().await;
        //
        // let chosen_route = self
        //     .route_script
        //     .call(rt.borrow(), (qd.message.clone(), routes))?
        //     .clone();
        // if chosen_route.len() == 1 {
        //     self.route_map
        //         .get_mut(chosen_route.get(0).unwrap().as_str())
        //         .unwrap()
        //         .process_request(qd, name)
        //         .await
        // } else if chosen_route.is_empty() {
        //     ChainResponse::Err(anyhow!("no routes found"))
        // } else {
        //     let mut fu = FuturesUnordered::from_iter(self.route_map.iter_mut().filter_map(
        //         |(name, chain)| {
        //             if let Some(_f) = chosen_route.iter().find(|p| *p == name) {
        //                 let wrapper = qd.clone();
        //                 Some(chain.process_request(wrapper, name.clone()))
        //             } else {
        //                 None
        //             }
        //         },
        //     ));
        //
        //     let mut results: Vec<Messages> = Vec::new();
        //     while let Some(Ok(messages)) = fu.next().await {
        //         results.push(messages);
        //     }
        //
        //     let collated_response: Vec<Message> = (0..qd.message.messages.len())
        //         .into_iter()
        //         .map(|_i| {
        //             let mut collated_results = vec![];
        //             for res in &mut results {
        //                 if let Some(m) = res.messages.pop() {
        //                     if let MessageDetails::Response(QueryResponse {
        //                         matching_query: _,
        //                         result,
        //                         error: _,
        //                         response_meta: _,
        //                     }) = &m.details
        //                     {
        //                         if let Some(res) = result {
        //                             collated_results.push(res.clone());
        //                         }
        //                     }
        //                 }
        //             }
        //             Message::new_response(
        //                 QueryResponse::just_result(Value::FragmentedResponese(collated_results)),
        //                 true,
        //                 RawFrame::NONE,
        //             )
        //         })
        //         .rev()
        //         .collect_vec();
        //
        //     ChainResponse::Ok(Messages {
        //         messages: collated_response,
        //     })
        unimplemented!()
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
