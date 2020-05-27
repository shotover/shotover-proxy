use rlua::{Lua, Context, Function, UserData, Value};
use rlua::prelude::*;

use serde::{Serialize, Deserialize};
use std::mem;
use tokio::sync::Mutex;
use std::sync::Arc;

pub struct LuaRuntime {
    pub vm: Arc<Mutex<Lua>>,
}


impl LuaRuntime {
    pub fn new() -> LuaRuntime {
        return LuaRuntime {
            vm : Arc::new(Mutex::new(Lua::new()))
        }
    }
    
    pub fn new_with_script(function_name: &str, function_definition: &str) -> Result<LuaRuntime, LuaError> {
        let vm = Lua::new();
        vm.context(|ctx | -> LuaResult<()> {
            ctx.load(function_definition).set_name(function_name)?.exec()?;
            Ok(())
        })?;
        return Ok(LuaRuntime {
            vm: Arc::new(Mutex::new(vm)),
        });
    }

    pub fn build_function(&self, function_name: &str, function_definition: &str) -> LuaResult<()> {
        if let Ok(vm) = self.vm.try_lock() {
            vm.context(|ctx | -> LuaResult<()> {
                ctx.load(function_definition).set_name(function_name)?.exec()?;
                Ok(())
            })?;
        }
        Ok(())
    }
}