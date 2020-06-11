use crate::config::topology::TopicHolder;
use crate::message::{Message, QueryMessage, QueryResponse};
use crate::runtimes::lua::LuaRuntime;
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use core::mem;
use rlua_serde;
use serde::{Deserialize, Serialize};

use crate::error::{ChainResponse, RequestError};
use anyhow::{anyhow, Result};

pub struct LuaFilterTransform {
    name: &'static str,
    pub query_filter: Option<String>,
    pub response_filter: Option<String>,
    pub lua: LuaRuntime,
}

impl Clone for LuaFilterTransform {
    fn clone(&self) -> Self {
        //TODO: we may need to reload the preloaded scripts
        return LuaFilterTransform {
            name: self.name.clone(),
            query_filter: self.query_filter.clone(),
            response_filter: self.response_filter.clone(),
            lua: LuaRuntime::new(),
        };
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct LuaConfig {
    pub query_filter: Option<String>,
    pub response_filter: Option<String>,
}

#[async_trait]
impl TransformsFromConfig for LuaConfig {
    async fn get_source(
        &self,
        _: &TopicHolder,
    ) -> Result<Transforms> {
        Ok(Transforms::Lua(LuaFilterTransform {
            name: "lua",
            query_filter: self.query_filter.clone(),
            response_filter: self.response_filter.clone(),
            lua: LuaRuntime::new(),
        }))
    }
}

#[async_trait]
impl Transform for LuaFilterTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Ok(lua) = self.lua.vm.try_lock() {
            if let Some(query_script) = &self.query_filter {
                if let Message::Query(qm) = &mut qd.message {
                    let qm_clone = qm.clone();
                    lua.context(|lua_ctx| {
                        let globals = lua_ctx.globals();
                        let lval = rlua_serde::to_value(lua_ctx, qm_clone).unwrap();
                        globals.set("qm", lval).unwrap();
                        let chunk = lua_ctx
                            .load(query_script.as_str())
                            .set_name("test")
                            .unwrap();
                        let result: QueryMessage =
                            rlua_serde::from_value(chunk.eval().unwrap()).unwrap();
                        // This is safe as the message lasts for more than the lifetime of the chain (and thus the lua VM).
                        // Todo: this may result in memory leaks?? - We do override it above in the globals table for the next new messgage
                        let _ = mem::replace(&mut qd.message, Message::Query(result));
                    });
                }
            }
        }
        let mut result = self.call_next_transform(qd, t).await?;
        if let Ok(lua) = self.lua.vm.try_lock() {
            if let Some(response_script) = &self.response_filter {
                if let Message::Response(rm) = &mut result {
                    let rm_clone = rm.clone();
                    lua.context(|lua_ctx| {
                        let globals = lua_ctx.globals();
                        let lval = rlua_serde::to_value(lua_ctx, rm_clone).unwrap();
                        globals.set("qr", lval).unwrap();
                        let chunk = lua_ctx
                            .load(response_script.as_str())
                            .set_name("test")
                            .unwrap();
                        let result: QueryResponse =
                            rlua_serde::from_value(chunk.eval().unwrap()).unwrap();
                        let _ = mem::replace(&mut rm.error, result.error);
                        let _ = mem::replace(&mut rm.result, result.result);
                    });
                }
            }
        }
        return Ok(result);
    }

    fn get_name(&self) -> &'static str {
        "lua"
    }
}

#[cfg(test)]
mod lua_transform_tests {
    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::transforms::chain::{Transform, TransformChain, Wrapper};
    use crate::transforms::lua::LuaConfig;
    use crate::transforms::null::Null;
    use crate::transforms::printer::Printer;
    use crate::transforms::{Transforms, TransformsFromConfig};
    use std::error::Error;
    use crate::protocols::RawFrame;

    const REQUEST_STRING: &str = r###"
qm.namespace = {"aaaaaaaaaa", "bbbbb"}
return qm
"###;

    const RESPONSE_STRING: &str = r###"
qr.result = {Integer=42}
return qr
"###;

    #[tokio::test(threaded_scheduler)]
    async fn test_lua_script() -> Result<(), Box<dyn Error>> {
        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
        };
        let lua_t = LuaConfig {
            query_filter: Some(String::from(REQUEST_STRING)),
            response_filter: Some(String::from(RESPONSE_STRING)),
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

        let chain = TransformChain::new(transforms, String::from("test_chain"));

        if let Transforms::Lua(lua) = lua_t.get_source(&t_holder).await? {
            let result = lua.transform(wrapper, &chain).await;
            if let Ok(m) = result {
                if let Message::Response(QueryResponse {
                    matching_query: Some(oq),
                    original: _,
                    result: _,
                    error: _,
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
