use rhai::{Engine, Scope, AST, RegisterFn};
use crate::transforms::chain::{Wrapper, ChainResponse, RequestError};
use crate::config::ConfigError;
use crate::message::{Message, QueryMessage};


pub struct RhaiEnvironment {
    engine: Engine,
    plain_script: String,
    ast: AST
}

impl Clone for RhaiEnvironment {
    fn clone(&self) -> Self {
        let engine = Engine::new();
        RhaiEnvironment {
            engine,
            plain_script: self.plain_script.clone(),
            ast: self.ast.clone()
        }
    }
}

impl RhaiEnvironment {
    pub fn new(plain_script: &String) -> Result<Self, ConfigError>  {
        let mut engine = Engine::new();
        engine.register_type::<Wrapper>();
        engine.register_type::<Message>();
        engine.register_type::<QueryMessage>();

        engine.register_fn("get_namespace", QueryMessage::get_namespace);
        engine.register_fn("set_namespace_elem", QueryMessage::set_namespace_elem);
        let ast = engine.compile(plain_script.as_str())?;
        return Ok(RhaiEnvironment {
            engine,
            plain_script: plain_script.clone(),
            ast
        });
    }

    pub fn call_routing_func(&self, w: Wrapper, available_routes: Vec<String>) -> Result<String, RequestError> {
        let mut scope = Scope::new();
        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "route", (w, available_routes)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

    pub fn call_scatter_route(&self, w: Wrapper, available_routes: Vec<String>) -> Result<Vec<String>, RequestError> {
        let mut scope = Scope::new();
        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "route", (w, available_routes)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

    pub fn call_route_handle_func(&self, c: ChainResponse, chosen_route: String) -> ChainResponse {
        let mut scope = Scope::new();

        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "process_result", (c, chosen_route)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

    pub fn call_scatter_handle_func(&self, c: Vec<ChainResponse>, chosen_route: Vec<String>) -> ChainResponse {
        let mut scope = Scope::new();
        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "process_result", (c, chosen_route)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

    pub async fn call_rhai_transform_func(&self, qd: Wrapper) -> Result<Wrapper, RequestError> {
        let mut scope = Scope::new();
        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "rhai_transform_func", (qd,)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

    pub async fn call_rhai_transform_response_func(&self, c: ChainResponse) -> Result<ChainResponse, RequestError> {
        let mut scope = Scope::new();
        return if let Ok(result) = self.engine.call_fn(&mut scope, &self.ast, "rhai_transform_response_func", (c,)) {
            Ok(result)
        } else {
            Err(RequestError {})
        }
    }

}
