use mlua::{Lua, TableExt, Function, UserData};
use std::marker::PhantomData;
use serde::{Deserialize, Serialize};
use anyhow::Result;


pub mod lua;
pub mod python;

/*
TODO:
A safer way to run Lua would be to have a LUA VM per Transform chain (this ends up then being on a per
connection basis). Each message would then get it's own scope (e.g. lua.async_scope) and a transform would use the scope).
Or get a ref to the Lua VM, maybe?
*/

pub enum Script {
    Python,
    Lua {function_name: String, function_def: String },
}

impl Clone for Script {
    fn clone(&self) -> Self {
        match self {
            Script::Python => {Script::Python},
            Script::Lua {function_name , function_def} => {
                Script::new_lua(function_name.clone(), function_def.clone()).unwrap()
            },
        }
    }
}

impl Script {
    fn new_lua(function_name: String, script_definition: String) -> Result<Self> {
        return Ok(Script::Lua {
            function_name,
            function_def: script_definition
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
                return Ok(ScriptHolder::new(Script::new_lua(self.function_name.clone(), self.script_definition.clone())?));
            },
            "python" => {
                unimplemented!()
            },
            _ => {panic!("unsupported script type, tru 'lua' or 'python'")}
        }
    }
}

#[derive(Clone)]
pub struct ScriptHolder<A, R> {
    pub env: Script,
    // pub fn_def: fn(A) -> R,
    pub _phantom: PhantomData<(A, R)>
}

pub trait ScriptDefinition<A, R>{
    type Args;
    type Return;

    fn call<'de>(&self, lua: &'de Lua, args: Self::Args) -> Result<Self::Return>
        where
            A: serde::Serialize + Clone,
            R: serde::Deserialize<'de> + Clone;
}

impl <A, R> ScriptHolder<A, R> {
    pub fn new(script: Script) -> Self {
        return ScriptHolder{
            env: script,
            _phantom: PhantomData
        }
    }

    pub fn prep_lua_runtime(&self, lua: &Lua) -> Result<()> {
        match &self.env {
            Script::Python => {unimplemented!()},
            Script::Lua { function_name, function_def } => {
                lua.load(function_def.as_str()).exec()?;
            },
        }
        Ok(())
    }
}

impl <A, R> ScriptDefinition<A,R> for ScriptHolder<A,R> {
    type Args = A;
    type Return = R;

    fn call<'de>(&self, lua: &'de Lua, args: Self::Args) -> Result<Self::Return>
    where
        A: serde::Serialize + Clone,
        R: serde::Deserialize<'de> + Clone,
    {
        match &self.env {
            Script::Python => {unimplemented!()},
            Script::Lua{function_name, function_def } => {
                // TODO: fix lifetimes going on here
                let lval = mlua_serde::to_value(lua, args.clone()).unwrap();
                let foo = lua.globals().get::<_, Function>(function_name.as_str())?.call(lval)?;
                let result: Self::Return = mlua_serde::from_value(foo).unwrap();
                return Ok(result);
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::runtimes::{ScriptHolder, Script, ScriptDefinition, ScriptConfigurator};
    use mlua::{Lua};
    use anyhow::Result;

    #[test]
    fn script() -> Result<()> {
        let lua = Lua::new();
        let script_definition = "function identity(arg)
            return arg
        end".to_string();
        let sh: ScriptHolder<i32, i32> = ScriptHolder::new(Script::new_lua("identity".to_string(), script_definition)?);
        sh.prep_lua_runtime(&lua);
        assert_eq!(400, sh.call(&lua,400)?);
        Ok(())
    }

    #[test]
    fn config() -> Result<()> {
        let lua = Lua::new();
        let config = ScriptConfigurator{ script_type: "lua".to_string(), function_name: "identity".to_string(), script_definition: "function identity(arg)
            return arg
        end".to_string() };
        let sh: ScriptHolder<i32, i32> = config.get_script_func()?;
        sh.prep_lua_runtime(&lua);
        assert_eq!(400, sh.call(&lua,400)?);
        Ok(())
    }
}