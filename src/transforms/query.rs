use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};
use crate::message::QueryType;
use std::borrow::{Borrow, BorrowMut};
use crate::message::{Message, QueryResponse};

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    name: &'static str,
    filters: Vec<QueryType>,
}

impl QueryTypeFilter {
    pub fn new(nfilters: Vec<QueryType>) -> QueryTypeFilter {
        QueryTypeFilter{
            name: "QueryFilter",
            filters: nfilters,
        }
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain) -> ChainResponse {
        // TODO this is likely the wrong way to get around the borrow from the match statement
        let message  = qd.message.borrow();

        return match message {
            Message::Query(q) => {
                if self.filters.iter().any(|x| *x == q.query_type) {
                    return ChainResponse::Ok(Message::Response(QueryResponse::empty()))
                }
                self.call_next_transform(qd, t).await
            }
            _ => self.call_next_transform(qd, t).await
        }
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}