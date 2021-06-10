use async_trait::async_trait;
use anyhow::Result;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use shotover_transforms::{
    ChainResponse, Message, MessageDetails, Messages, QueryResponse, TopicHolder, Transform,
    TransformsFromConfig,
};

use shotover_transforms::RawFrame;
use shotover_transforms::Wrapper;

#[derive(Debug, Clone)]
pub struct Null {
    name: &'static str,
    with_request: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub struct NullConfig {}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for NullConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(Null::default()))
    }
}

impl Default for Null {
    fn default() -> Self {
        Self::new()
    }
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
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        if self.with_request {
            return ChainResponse::Ok(Messages {
                messages: wrapped_messages
                    .message
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
        self.name
    }
}
