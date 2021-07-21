use anyhow::Result;
use async_trait::async_trait;
use mlua::Lua;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

pub struct LuaFilterTransform {
    name: &'static str,
    pub function_def: String,
    pub function_name: String,
    pub slua: &'static Lua,
}

impl Clone for LuaFilterTransform {
    fn clone(&self) -> Self {
        //TODO: we may need to reload the preloaded scripts
        LuaFilterTransform {
            name: self.name,
            function_def: self.function_def.clone(),
            function_name: self.function_name.clone(),
            slua: Lua::new().into_static(),
        }
    }
}

// TODO: Verify we can actually do this (mlua seems to think so)
unsafe impl Send for LuaFilterTransform {}

unsafe impl Sync for LuaFilterTransform {}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct LuaConfig {
    pub function_def: String,
    pub function_name: String,
}

#[async_trait]
impl TransformsFromConfig for LuaConfig {
    async fn get_source(&self, _: &TopicHolder) -> Result<Transforms> {
        let lua_t = LuaFilterTransform {
            name: "lua",
            function_def: self.function_def.clone(),
            function_name: self.function_name.clone(),
            slua: Lua::new().into_static(),
        };
        // lua_t.build_lua();
        Ok(Transforms::Lua(lua_t))
    }
}

#[async_trait]
impl Transform for LuaFilterTransform {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        message_wrapper.call_next_transform().await
        // let globals = self.slua.globals();
        // let qm_v = mlua_serde::to_value(&self.slua, message_wrapper.message.clone()).unwrap();
        //
        // //Warning, message_wrapper's transform chain is empty after the creation of this scoped function
        //
        // let result = self.slua.scope(|scope| {
        //     let spawn_func = scope.create_function_mut(
        //         |_lua: &Lua, _messages: Messages| -> mlua::Result<Messages> {
        //             //Warning, message_wrapper's transform chain is empty after the creation of this scoped function
        //             let mut future = Wrapper::new(Messages::new());
        //             std::mem::swap(&mut future.transforms, &mut message_wrapper.transforms);
        //             std::mem::swap(&mut future.message, &mut message_wrapper.message);
        //             //hacky but I can't figure out how to do async_scope stuff safely in the current transformChain mess
        //             tokio::runtime::Handle::current().spawn(async {
        //                 let result = future.call_next_transform().await;
        //                 Ok(result.map_err(|_e| {
        //                     mlua::Error::RuntimeError(
        //                         "help!!@! - - TODO implement From anyhow to mlua errors"
        //                             .to_string(),
        //                     )
        //                 })?)
        //             }).await
        //         },
        //     )?;
        //     globals.set("call_next_transform", spawn_func)?;
        //     let func = scope.create_function(
        //         |lua: &Lua, _qm: QueryMessage| -> mlua::Result<Messages> {
        //             let value = lua
        //                 .load(self.function_name.clone().as_str())
        //                 .set_name("fnc")?
        //                 .eval()?;
        //             let result: Vec<Message> = mlua_serde::from_value(value)?;
        //             let a = Messages { messages: result };
        //             Ok(a)
        //         },
        //     )?;
        //     func.call(qm_v)
        // });
        // result.map_err(|e| anyhow!("uh oh lua broke {}", e))
    }

    fn get_name(&self) -> &'static str {
        "lua"
    }
}

#[cfg(test)]
mod lua_transform_tests {
    // use std::error::Error;

    // use crate::config::topology::TopicHolder;
    // use crate::message::{MessageDetails, Messages, QueryMessage, QueryResponse, QueryType, Value};
    // use crate::protocols::RawFrame;
    // use crate::transforms::chain::TransformChain;
    // use crate::transforms::lua::LuaConfig;
    // use crate::transforms::null::Null;
    // use crate::transforms::printer::Printer;
    // use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};

//    const REQUEST_STRING: &str = r###"
//qm.namespace = {"aaaaaaaaaa", "bbbbb"}
//return call_next_transform(qm)
//"###;

    // #[tokio::test(flavor = "multi_thread")]
    // async fn test_lua_script() -> Result<(), Box<dyn Error>> {
    //     // let (mut global_map_r, mut global_map_w) = evmap::new();
    //     // let (global_tx, mut global_rx) = channel(1);
    //
    //     let lua_t = LuaConfig {
    //         function_def: REQUEST_STRING.to_string(),
    //         // query_filter: Some(String::from(REQUEST_STRING)),
    //         // response_filter: Some(String::from(RESPONSE_STRING)),
    //         function_name: "".to_string(),
    //     };
    //
    //     let wrapper = Wrapper::new(Messages::new_single_query(
    //         QueryMessage {
    //             query_string: "".to_string(),
    //             namespace: vec![String::from("keyspace"), String::from("old")],
    //             primary_key: Default::default(),
    //             query_values: None,
    //             projection: None,
    //             query_type: QueryType::Read,
    //             ast: None,
    //         },
    //         true,
    //         RawFrame::None,
    //     ));
    //
    //     let transforms: Vec<Transforms> = vec![
    //         Transforms::Printer(Printer::new()),
    //         Transforms::Null(Null::new()),
    //     ];
    //
    //     let _chain = TransformChain::new(transforms, String::from("test_chain"));
    //
    //     if let Transforms::Lua(mut lua) = lua_t.get_source(&t_holder).await? {
    //         let result = lua.transform(wrapper).await;
    //         if let Ok(m) = result {
    //             if let MessageDetails::Response(QueryResponse {
    //                 matching_query: Some(oq),
    //                 result: _,
    //                 error: _,
    //                 response_meta: _,
    //             }) = &m.messages.get(0).unwrap().details
    //             {
    //                 assert_eq!(oq.namespace.get(0).unwrap(), "aaaaaaaaaa");
    //             } else {
    //                 panic!()
    //             }
    //             if let MessageDetails::Response(QueryResponse {
    //                 matching_query: _,
    //                 result: Some(x),
    //                 error: _,
    //                 response_meta: _,
    //             }) = &m.messages.get(0).unwrap().details
    //             {
    //                 assert_eq!(*x, Value::Integer(42));
    //             } else {
    //                 panic!()
    //             }
    //             return Ok(());
    //         }
    //     } else {
    //         panic!()
    //     }
    //     Ok(())
    // }
}
