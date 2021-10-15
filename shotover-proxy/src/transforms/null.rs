use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct Null {
    with_request: bool,
}

impl Default for Null {
    fn default() -> Self {
        Self::new()
    }
}

impl Null {
    pub fn new() -> Null {
        Null { with_request: true }
    }

    pub fn new_without_request() -> Null {
        Null {
            with_request: false,
        }
    }
}

#[async_trait]
impl Transform for Null {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.with_request {
            return ChainResponse::Ok(Messages {
                messages: message_wrapper
                    .messages
                    .messages
                    .into_iter()
                    .filter_map(|m| {
                        if let MessageDetails::Query(qm) = m.details {
                            Some(Message::new_response(
                                QueryResponse::empty_with_matching(qm),
                                true,
                                RawFrame::None,
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
            RawFrame::None,
        ))
    }

    fn get_name(&self) -> &'static str {
        "Null"
    }
}
