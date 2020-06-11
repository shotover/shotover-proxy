use crate::message::QueryType;
use crate::message::{Message, QueryResponse};
use crate::transforms::chain::{Transform, TransformChain, Wrapper};
use std::borrow::Borrow;

use async_trait::async_trait;
use crate::error::ChainResponse;

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    name: &'static str,
    filters: Vec<QueryType>,
}

impl QueryTypeFilter {
    pub fn new(nfilters: Vec<QueryType>) -> QueryTypeFilter {
        QueryTypeFilter {
            name: "QueryFilter",
            filters: nfilters,
        }
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        // TODO this is likely the wrong way to get around the borrow from the match statement
        let message = qd.message.borrow();

        return match message {
            Message::Query(q) => {
                if self.filters.iter().any(|x| *x == q.query_type) {
                    return ChainResponse::Ok(Message::Response(QueryResponse::empty()));
                }
                self.call_next_transform(qd, t).await
            }
            _ => self.call_next_transform(qd, t).await,
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
