use crate::config::topology::TopicHolder;
use crate::message::{Message, QueryMessage, QueryResponse};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{ChainResponse};
use anyhow::{anyhow, Result};
use mlua::{Lua, MultiValue};
use std::sync::Arc;
use tokio::sync::Mutex;


pub struct LuaFilterTransform {
    name: &'static str,
    pub function_def: String,
    pub function_name: String,
    pub lua: Arc<Mutex<Lua>>,
}

impl Clone for LuaFilterTransform {
    fn clone(&self) -> Self {
        //TODO: we may need to reload the preloaded scripts
        return LuaFilterTransform {
            name: self.name,
            function_def: self.function_def.clone(),
            function_name: self.function_name.clone(),
            lua: Arc::new(Mutex::new(Lua::new())),
        };
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct LuaConfig {
    pub function_def: String,
    pub function_name: String,
}

#[async_trait]
impl TransformsFromConfig for LuaConfig {
    async fn get_source(
        &self,
        _: &TopicHolder,
    ) -> Result<Transforms> {
        let foo = Lua::new();
        let mut lua_t = LuaFilterTransform {
            name: "lua",
            function_def: self.function_def.clone(),
            function_name: self.function_name.clone(),
            lua: Arc::new(Mutex::new(Lua::new())),
        };
        lua_t.build_lua();
        Ok(Transforms::Lua(lua_t))
    }
}

async fn wrapped_next_transform(lua: &'static mlua::Lua, (mut qd, transforms): (Wrapper, &TransformChain)) -> mlua::Result<QueryResponse> {
    let next = qd.next_transform;
    qd.next_transform += 1;
    return match transforms.chain.get(next) {
        Some(t) => {
            if let Message::Response(qr) = t.instrument_transform(qd, transforms).await.map_err(|e|{mlua::Error::RuntimeError("help!!@! - - TODO implement From anyhow to mlua errors".to_string())})? {
                mlua::Result::Ok(qr)
            } else {
                mlua::Result::Err(mlua::Error::RuntimeError("No more chains - TODO implement From anyhow to mlua errors".to_string()))
            }
        },
        None => {
            mlua::Result::Err(mlua::Error::RuntimeError("No more chains - TODO implement From anyhow to mlua errors".to_string()))
        },
    };
}

impl LuaFilterTransform {
    fn build_lua(&mut self) -> Result<()> {
        // let lua = self.lua.try_lock().map_err(|e| anyhow!("couldn't get lock for lua runtime"))?;
        // let next_lua_func = lua.create_async_function(wrapped_next_transform).unwrap();
        // lua.globals().set("call_next_transform", next_lua_func).map_err(|e| anyhow!("couldn't set next_transform function in globals"))?;
        // let user_func = lua.load(self.function_def.as_str()).into_function().map_err(|e| anyhow!("couldn't build user function for wrapped transform"))?;
        // lua.globals().set(self.function_name.clone(), user_func).map_err(|e| anyhow!("couldn't set user function in globals"))?;
        Ok(())
    }
}


async fn temp_transform(mut qd: Wrapper,
                        transforms: &TransformChain) -> ChainResponse {
    let next = qd.next_transform;
    qd.next_transform += 1;
    return match transforms.chain.get(next) {
        Some(t) => t.instrument_transform(qd, transforms).await,
        None => {
            Err(anyhow!("No more transforms left in the chain".to_string()))
        },
    };
}


#[async_trait]
impl Transform for LuaFilterTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let chain_count = qd.next_transform.clone();
        let lua = self.lua.lock().await;
        if let Message::Query(qm) = &mut qd.message {
            let globals = lua.globals();
            globals.set("qm", mlua_serde::to_value(&lua, qm.clone()).unwrap()).unwrap();
            let result = lua.scope(|scope|{
                let spawn_func = scope.create_function(|lua: &Lua, qm: QueryMessage| -> mlua::Result<Message> {
                    let result = tokio::runtime::Handle::current().block_on(async move {
                        let w = Wrapper::new_with_next_transform(Message::Query(qm), chain_count);
                        return Ok(temp_transform( w, t).await.map_err(|e|{mlua::Error::RuntimeError("help!!@! - - TODO implement From anyhow to mlua errors".to_string())})?);
                    });
                    return result;
                })?;
                globals.set("call_next_transform", spawn_func);

                let func = scope.create_function( |lua: &Lua, qm: QueryMessage| -> mlua::Result<Message> {
                    let value = lua.load(self.function_name.clone().as_str()).set_name("fnc")?.eval()?;
                    let result: QueryResponse =
                        mlua_serde::from_value(value)?;
                    return Ok(Message::Response(result));
                })?;
                // This is safe as the message lasts for more than the life
                return func.call(MultiValue::new());
            });
            return result.clone().map_err(|e| anyhow!("uh oh lua broke")).map(|x| Message::Response(x));
        }
        Err(anyhow!("Couldn't get lua vm lock"))
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
    use tokio::sync::mpsc::channel;

    const REQUEST_STRING: &str = r###"
qm.namespace = {"aaaaaaaaaa", "bbbbb"}
return call_next_transform(qm)
"###;

    const RESPONSE_STRING: &str = r###"
qr.result = {Integer=42}
return qr
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
            function_name: "".to_string()
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

        let chain = TransformChain::new(transforms, String::from("test_chain"), t_holder.get_global_map_handle(), t_holder.get_global_tx());

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
