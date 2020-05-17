use std::collections::HashMap;
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use async_trait::async_trait;

pub type RoutingFunc = fn (w: &Wrapper, available_routes: &Vec<&String>) -> String;
pub type HandleResultFunc = fn (c: ChainResponse, chosen_route: &String) -> ChainResponse;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    routing_func: RoutingFunc,
    result_func: Option<HandleResultFunc>
}


impl Route {
    fn new(route_map: HashMap<String, TransformChain>, routing_func: RoutingFunc, result_func: Option<HandleResultFunc>) -> Self {
        Route {
            name: "route",
            route_map,
            routing_func,
            result_func
        }
    }
}


#[async_trait]
impl Transform for Route {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let routes: Vec<&String> = self.route_map.keys().map(|x| x).collect();
        let mut chosen_route = (self.routing_func)(&qd, &routes);
        qd.reset();
        let result = self.route_map.get(chosen_route.as_str()).unwrap().process_request(qd).await;
        return if let Some(f) = self.result_func {
            (f)(result, &chosen_route)
        } else {
            result
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

