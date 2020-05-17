use std::collections::HashMap;
use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse, RequestError};
use crate::transforms::Transforms;

use async_trait::async_trait;
use futures::future::{try_join_all};
use futures_core::{Future, TryFuture};
use crate::message::{QueryResponse, Message};
use std::pin::Pin;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub type ScatterFunc = fn (w: &Wrapper, available_routes: &Vec<&String>) -> Vec<String>;

#[derive(Clone)]
pub struct Scatter {
    name: &'static str,
    route_map: HashMap<String, TransformChain>,
    scatter_func: ScatterFunc
}

impl Scatter {
    fn new(route_map: HashMap<String, TransformChain>, scatter_func: ScatterFunc) -> Self {
        Scatter {
            name: "route",
            route_map,
            scatter_func
        }
    }
}

#[async_trait]
impl Transform for Scatter {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let routes: Vec<&String> = self.route_map.keys().map(|x| x).collect();
        let chosen_route = (self.scatter_func)(&qd, &routes);
        if chosen_route.len() == 1 {
            return self.route_map.get(chosen_route.get(0).unwrap().as_str()).unwrap().process_request(qd).await;
        } else if chosen_route.len() == 0 {
            return ChainResponse::Err(RequestError{})
        } else {
            let mut fu = FuturesUnordered::new();
            for route in chosen_route {
                let chain = self.route_map.get(route.as_str()).unwrap();
                let mut wrapper = qd.clone();
                wrapper.reset();
                fu.push(chain.process_request(wrapper));
            }
            // TODO I feel like there should be some streamext function that does this for me
            while let Some(r) = fu.next().await {
                if let Err(e) = r {
                    return ChainResponse::Err(RequestError{})
                }
            }
            return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}