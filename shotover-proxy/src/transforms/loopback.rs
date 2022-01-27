use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, QueryResponse};
use crate::protocols::Frame;
use crate::transforms::{Transform, Wrapper};
use async_trait::async_trait;

#[derive(Debug, Clone, Default)]
pub struct Loopback {}

#[async_trait]
impl Transform for Loopback {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        Ok(message_wrapper
            .messages
            .into_iter()
            .filter_map(|m| {
                if let MessageDetails::Query(qm) = m.details {
                    Some(Message::new_response(
                        QueryResponse::empty_with_matching(qm),
                        true,
                        Frame::None,
                    ))
                } else {
                    None
                }
            })
            .collect())
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
