use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};

use crate::message::{Message, QueryResponse};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Null {
    name: &'static str,
    with_request: bool
}

impl Null {
    pub fn new() -> Null {
        Null { name: "Null" , with_request: true}
    }

    pub fn new_without_request() -> Null {
        Null { name: "Null" , with_request: false}
    }
}

#[async_trait]
impl Transform for Null {
    async fn transform(&self, qd: Wrapper, _: &TransformChain) -> ChainResponse {
        if self.with_request {
            if let Message::Query(qm) = qd.message {
                return ChainResponse::Ok(Message::Response(QueryResponse::empty_with_original(qm)));
            }
        }
        return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
