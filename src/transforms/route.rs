use std::collections::HashMap;
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use async_trait::async_trait;

pub type RoutingFunc = fn (w: &Wrapper, available_routes: &Vec<&String>) -> String;

#[derive(Clone)]
pub struct Route {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    routing_func: RoutingFunc
}


impl Route {
    fn new(route_map: HashMap<String, TransformChain>, routing_func: RoutingFunc) -> Self {
        Route {
            name: "route",
            route_map,
            routing_func
        }
    }
}


#[async_trait]
impl Transform for Route {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let routes: Vec<&String> = self.route_map.keys().map(|x| x).collect();
        let mut chosen_route = (self.routing_func)(&qd, &routes);
        qd.reset();
        return self.route_map.get(chosen_route.as_str()).unwrap().process_request(qd).await;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

