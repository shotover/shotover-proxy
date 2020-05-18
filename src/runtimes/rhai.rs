use rhai::{Engine, Func, EvalAltResult, ParseError, Scope, AST};
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use crate::config::ConfigError;


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
        let engine = Engine::new();
        if let Ok(ast) = engine.compile(plain_script.as_str()) {
            return Ok(RhaiEnvironment {
                engine,
                plain_script: plain_script.clone(),
                ast
            });
        }
        Err(ConfigError{})
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

    pub fn build_rhai_routing_func(&self, script: String) {
        let engine = Engine::new();
        let script = "";

        unimplemented!()
    }
}
