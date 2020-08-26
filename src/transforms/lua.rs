use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mlua::Lua;
use serde::{Deserialize, Serialize};

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Message, QueryMessage, QueryResponse};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};

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

async fn temp_transform(mut qd: Wrapper, transforms: &TransformChain) -> ChainResponse {
    let next = qd.next_transform;
    qd.next_transform += 1;
    match transforms.chain.get(next) {
        Some(t) => t.instrument_transform(qd, transforms).await,
        None => Err(anyhow!("No more transforms left in the chain".to_string())),
    }
}

#[async_trait]
impl Transform for LuaFilterTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let chain_count = qd.next_transform;
        let globals = self.slua.globals();
        if let Message::Query(qm) = &mut qd.message {
            let qm_v = mlua_serde::to_value(&self.slua, qm.clone()).unwrap();
            let result = self.slua.scope(|scope| {
                let spawn_func = scope.create_function(
                    |_lua: &Lua, qm: QueryMessage| -> mlua::Result<Message> {
                        //hacky but I can't figure out how to do async_scope stuff safely in the current transformChain mess
                        let result = tokio::runtime::Handle::current().block_on(async move {
                            let w =
                                Wrapper::new_with_next_transform(Message::Query(qm), chain_count);
                            return Ok(temp_transform(w, t).await.map_err(|_e| {
                                mlua::Error::RuntimeError(
                                    "help!!@! - - TODO implement From anyhow to mlua errors"
                                        .to_string(),
                                )
                            })?);
                        });
                        result
                    },
                )?;
                globals.set("call_next_transform", spawn_func)?;
                let func = scope.create_function(
                    |lua: &Lua, _qm: QueryMessage| -> mlua::Result<Message> {
                        let value = lua
                            .load(self.function_name.clone().as_str())
                            .set_name("fnc")?
                            .eval()?;
                        let result: QueryResponse = mlua_serde::from_value(value)?;
                        Ok(Message::Response(result))
                    },
                )?;
                func.call(qm_v)
            });
            result
                
                .map_err(|e| anyhow!("uh oh lua broke {}", e))
                .map(Message::Response)
        } else {
            Err(anyhow!("expected a request"))
        }
    }

    fn get_name(&self) -> &'static str {
        "lua"
    }
}

#[cfg(test)]
mod lua_transform_tests {
    use std::error::Error;

    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::RawFrame;
    use crate::transforms::chain::{Transform, TransformChain, Wrapper};
    use crate::transforms::lua::LuaConfig;
    use crate::transforms::null::Null;
    use crate::transforms::printer::Printer;
    use crate::transforms::{Transforms, TransformsFromConfig};

    const REQUEST_STRING: &str = r###"
qm.namespace = {"aaaaaaaaaa", "bbbbb"}
return call_next_transform(qm)
"###;

    #[tokio::test(threaded_scheduler)]
    async fn test_lua_script() -> Result<(), Box<dyn Error>> {
        // let (mut global_map_r, mut global_map_w) = evmap::new();
        // let (global_tx, mut global_rx) = channel(1);

        let t_holder = TopicHolder::get_test_holder();

        let lua_t = LuaConfig {
            function_def: REQUEST_STRING.to_string(),
            // query_filter: Some(String::from(REQUEST_STRING)),
            // response_filter: Some(String::from(RESPONSE_STRING)),
            function_name: "".to_string(),
        };

        let wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        }));

        let transforms: Vec<Transforms> = vec![
            Transforms::Printer(Printer::new()),
            Transforms::Null(Null::new()),
        ];

        let chain = TransformChain::new(
            transforms,
            String::from("test_chain"),
            t_holder.get_global_map_handle(),
            t_holder.get_global_tx(),
        );

        if let Transforms::Lua(lua) = lua_t.get_source(&t_holder).await? {
            let result = lua.transform(wrapper, &chain).await;
            if let Ok(m) = result {
                if let Message::Response(QueryResponse {
                                             matching_query: Some(oq),
                                             original: _,
                                             result: _,
                                             error: _, 
                                             response_meta: _,
                                         }) = &m
                {
                    assert_eq!(oq.namespace.get(0).unwrap(), "aaaaaaaaaa");
                } else {
                    panic!()
                }
                if let Message::Response(QueryResponse {
                                             matching_query: _,
                                             original: _,
                                             result: Some(x),
                                             error: _, 
                                             response_meta: _,
                                         }) = m
                {
                    assert_eq!(x, Value::Integer(42));
                } else {
                    panic!()
                }
                return Ok(());
            }
        } else {
            panic!()
        }
        Ok(())
    }
}
