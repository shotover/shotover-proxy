use anyhow::anyhow;
use anyhow::Result;
use mlua::{Function, Lua};
use serde::{Deserialize, Serialize};
use std::fs;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;
use wasmer_runtime::{imports, instantiate, Instance};

/*
TODO:
A safer way to run Lua would be to have a LUA VM per Transform chain (this ends up then being on a per
connection basis). Each message would then get it's own scope (e.g. lua.async_scope) and a transform would use the scope).
Or get a ref to the Lua VM, maybe?
*/

pub enum Script {
    Lua {
        function_name: String,
        function_def: String,
    },
    Wasm {
        function_name: String,
        function_def: Arc<Mutex<Instance>>,
    },
}

impl Clone for Script {
    fn clone(&self) -> Self {
        match self {
            Script::Lua {
                function_name,
                function_def,
            } => Script::new_lua(function_name.clone(), function_def.clone()).unwrap(),
            // Script::Wasm => {}
            Script::Wasm {
                function_name: _,
                function_def: _,
            } => unimplemented!(),
        }
    }
}

impl Script {
    fn new_lua(function_name: String, script_definition: String) -> Result<Self> {
        return Ok(Script::Lua {
            function_name,
            function_def: script_definition,
        });
    }

    fn new_wasm(function_name: String, script_definition: String) -> Result<Self> {
        let wasm_bytes = fs::read(script_definition)?;
        let import_object = imports! {};
        let instance = instantiate(wasm_bytes.as_slice(), &import_object)
            .map_err(|e| anyhow!("Couldn't load wasm module {}", e))?;
        return Ok(Script::Wasm {
            function_name,
            function_def: Arc::new(Mutex::new(instance)),
        });
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScriptConfigurator {
    pub script_type: String,
    pub function_name: String,
    pub script_definition: String,
}

impl ScriptConfigurator {
    pub fn get_script_func<A, R>(&self) -> Result<ScriptHolder<A, R>> {
        match self.script_type.as_str() {
            "lua" => {
                return Ok(ScriptHolder::new(Script::new_lua(
                    self.function_name.clone(),
                    self.script_definition.clone(),
                )?));
            }
            "wasm" => {
                return Ok(ScriptHolder::new(Script::new_wasm(
                    self.function_name.clone(),
                    self.script_definition.clone(),
                )?));
            }
            // "wasm" => {
            //     return Ok(ScriptHolder::new(Script))
            // },
            _ => panic!("unsupported script type, tru 'lua' or 'python'"),
        }
    }
}

#[derive(Clone)]
pub struct ScriptHolder<A, R> {
    pub env: Script,
    pub _phantom: PhantomData<(A, R)>,
}

pub trait ScriptDefinition<A, R> {
    type Args;
    type Return;

    fn call<'de>(&self, lua: &'de Lua, args: Self::Args) -> Result<Self::Return>
    where
        A: serde::Serialize + Clone,
        R: serde::de::DeserializeOwned + Clone;
}

impl<A, R> ScriptHolder<A, R> {
    pub fn new(script: Script) -> Self {
        return ScriptHolder {
            env: script,
            _phantom: PhantomData,
        };
    }

    pub fn prep_lua_runtime(&self, lua: &Lua) -> Result<()> {
        match &self.env {
            Script::Lua {
                function_name: _,
                function_def,
            } => {
                lua.load(function_def.as_str()).exec()?;
            }
            Script::Wasm {
                function_name: _,
                function_def: _,
            } => {
                // function_instance.exports.get()
            }
        }
        Ok(())
    }
}

impl<A, R> ScriptDefinition<A, R> for ScriptHolder<A, R> {
    type Args = A;
    type Return = R;

    fn call<'de>(&self, lua: &'de Lua, args: Self::Args) -> Result<Self::Return>
    where
        A: serde::Serialize + Clone,
        R: serde::de::DeserializeOwned + Clone,
    {
        match &self.env {
            Script::Lua {
                function_name,
                function_def,
            } => {
                // TODO: fix lifetimes going on here
                let lval = mlua_serde::to_value(lua, args.clone()).unwrap();
                let foo = lua
                    .globals()
                    .get::<_, Function>(function_name.as_str())?
                    .call(lval)?;
                let result: Self::Return = mlua_serde::from_value(foo).unwrap();
                return Ok(result);
            }
            Script::Wasm {
                function_name,
                function_def,
            } => {
                if let Ok(wasm_inst) = function_def.try_lock() {
                    let context = wasm_inst.context();
                    let mmemory = context.memory(0);
                    let view = mmemory.view();
                    // first 4 bytes to be used to indicate size
                    for cell in view[1..5].iter() {
                        cell.set(0);
                    }
                    let lval = bincode::serialize(&args.clone())?;
                    let len = lval.len();

                    for (cell, byte) in view[5..len + 5].iter().zip(lval.iter()) {
                        cell.set(*byte);
                    }
                    let func = wasm_inst.func::<(i32, u32), i32>(function_name.as_str())?;
                    let start = func
                        .call(5 as i32, len as u32)
                        .map_err(|e| anyhow!("wasm error: {}", e))?;
                    let new_view = mmemory.view::<u8>();
                    let mut new_len_bytes = [0u8; 4];

                    for i in 0..4 {
                        // attempt to get i+1 from the memory view (1,2,3,4)
                        // If we can, return the value it contains, otherwise
                        // default back to 0
                        new_len_bytes[i] = new_view.get(i + 1).map(|c| c.get()).unwrap_or(0);
                    }

                    let new_len = u32::from_ne_bytes(new_len_bytes) as usize;
                    let end = start as usize + new_len;

                    let mut updated_bytes: Vec<u8> = new_view[start as usize..end]
                        .iter()
                        .map(|c| c.get())
                        .collect();

                    let updated: Self::Return;
                    updated = bincode::deserialize::<Self::Return>(&updated_bytes)?.to_owned();
                    return Ok(updated);
                }
                unimplemented!()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::runtimes::{Script, ScriptConfigurator, ScriptDefinition, ScriptHolder};
    use anyhow::Result;
    use mlua::Lua;

    #[test]
    fn script() -> Result<()> {
        let lua = Lua::new();
        let script_definition = "function identity(arg)
            return arg
        end"
        .to_string();
        let sh: ScriptHolder<i32, i32> =
            ScriptHolder::new(Script::new_lua("identity".to_string(), script_definition)?);
        sh.prep_lua_runtime(&lua);
        assert_eq!(400, sh.call(&lua, 400)?);
        Ok(())
    }

    #[test]
    fn config() -> Result<()> {
        let lua = Lua::new();
        let config = ScriptConfigurator {
            script_type: "lua".to_string(),
            function_name: "identity".to_string(),
            script_definition: "function identity(arg)
            return arg
        end"
            .to_string(),
        };
        let sh: ScriptHolder<i32, i32> = config.get_script_func()?;
        sh.prep_lua_runtime(&lua);
        assert_eq!(400, sh.call(&lua, 400)?);
        Ok(())
    }
}
