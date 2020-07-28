// use crate::runtimes::lua::LuaRuntime;
use std::sync::Arc;
use tokio::sync::Mutex;
// use rlua::Lua;

pub mod lua;
pub mod python;
//
// pub enum Script {
//     Python(python::PythonEnvironment),
//     Lua(lua::LuaRuntime),
// }
//
// pub struct ScriptHolder<A, I, R> {
//     pub env: Script,
//     pub script_definition: String,
//     pub fn_def: fn(A, I) -> R
// }
//
// pub trait ScriptDefinition<A, I, R>{
//     type Args;
//     type Intermediate;
//     type Return;
//
//     fn call<'de>(self, args: Self::Args) -> Self::Return
//         where
//             A: serde::Serialize + Clone,
//             R: serde::Deserialize<'de> + Clone + Default,
//             I: serde::Deserialize<'de> + Clone + Default;
// }
//
// impl <A, I, R> ScriptDefinition<A,I,R> for ScriptHolder<A,I, R> {
//     type Args = A;
//     type Intermediate = I;
//     type Return = R;
//
//     fn call<'de>(self, args: Self::Args) -> Self::Return
//     where
//         A: serde::Serialize + Clone,
//         R: serde::Deserialize<'de> + Clone + Default,
//         I: serde::Deserialize<'de> + Clone + Default,
//     {
//         match &self.env {
//             Script::Python(p) => {},
//             Script::Lua(l) => {
//                 // TODO: fix lifetimes going on here
//                 // will need to figure out transmute
//                 let x;
//                 if let Ok(lua) = l.vm.try_lock() {
//                     lua.context(|lua_ctx| {
//                         let globals = lua_ctx.globals();
//                         let lval = rlua_serde::to_value(lua_ctx, args.clone()).unwrap();
//                         globals.set("arg", lval).unwrap();
//                         let chunk = lua_ctx
//                             .load(self.script_definition.as_str())
//                             .set_name("test")
//                             .unwrap();
//                         let result: Self::Intermediate =
//                             rlua_serde::from_value(chunk.eval().unwrap()).unwrap();
//                         unsafe {
//                             x = std::mem::transmute::<I, R>(result.clone());
//                         }
//
//                     });
//                     return x;
//                 }
//             },
//         }
//         unimplemented!()
//     }
// }
//
//
//
//
// fn dummy() {
//     let sh = ScriptHolder{
//         env: Script::Lua(LuaRuntime{ vm: Arc::new(Mutex::new(Lua::new())) }),
//         script_definition: "".to_string(),
//         fn_def: |a: i32| -> f32 {unimplemented!()}
//     };
// }