use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages};
use crate::protocols::RawFrame;
use crate::transforms::Transforms;
use crate::transforms::{Transform, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use redis_protocol::resp2::prelude::Frame;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct DebugReturnerConfig {
    response: Response,
}

impl DebugReturnerConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::DebugReturner(DebugReturner::new(
            self.response.clone(),
        )))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum Response {
    #[serde(skip)]
    Message(Messages),
    Redis(String),
    Fail,
}

#[derive(Debug, Clone)]
pub struct DebugReturner {
    response: Response,
}

impl DebugReturner {
    pub fn new(response: Response) -> Self {
        DebugReturner { response }
    }
}

#[async_trait]
impl Transform for DebugReturner {
    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        match &self.response {
            Response::Message(message) => Ok(message.clone()),
            Response::Redis(string) => Ok(vec![Message {
                protocol_error: false,
                details: MessageDetails::Unknown,
                modified: false,
                original: RawFrame::Redis(Frame::BulkString(string.clone().into_bytes())),
            }]),
            Response::Fail => Err(anyhow!("Intentional Fail")),
        }
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
