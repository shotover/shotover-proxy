use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct Null {
    name: &'static str,
    with_request: bool,
}

impl Null {
    pub fn new() -> Null {
        Null {
            name: "Null",
            with_request: true,
        }
    }

    pub fn new_without_request() -> Null {
        Null {
            name: "Null",
            with_request: false,
        }
    }
}

#[async_trait]
impl Transform for Null {
    async fn transform<'a>(&'a mut self, qd: Wrapper<'a>) -> ChainResponse {
        if self.with_request {
            return ChainResponse::Ok(Messages {
                messages: qd
                    .message
                    .messages
                    .into_iter()
                    .filter_map(|m| {
                        if let MessageDetails::Query(qm) = m.details {
                            Some(Message::new_response(
                                QueryResponse::empty_with_matching(qm),
                                true,
                                RawFrame::NONE,
                            ))
                        } else {
                            None
                        }
                    })
                    .collect_vec(),
            });
        }
        ChainResponse::Ok(Messages::new_single_response(
            QueryResponse::empty(),
            true,
            RawFrame::NONE,
        ))
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
