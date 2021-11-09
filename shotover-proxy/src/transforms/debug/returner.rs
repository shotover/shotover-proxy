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
    ok: bool,
    response: Response,
}

impl DebugReturnerConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::DebugReturner(DebugReturner::new(
            self.response.clone(),
            self.ok,
        )))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum Response {
    #[serde(skip)]
    Message(Messages),
    Redis(String),
}

#[derive(Debug, Clone)]
pub struct DebugReturner {
    response: Response,
    ok: bool,
}

impl DebugReturner {
    pub fn new(response: Response, ok: bool) -> Self {
        DebugReturner { response, ok }
    }
}

#[async_trait]
impl Transform for DebugReturner {
    async fn transform<'a>(&'a mut self, _message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.ok {
            match &self.response {
                Response::Message(message) => Ok(message.clone()),
                Response::Redis(string) => {
                    let res = vec![Message {
                        details: MessageDetails::Unknown,
                        modified: false,
                        original: RawFrame::Redis(Frame::BulkString(string.clone().into_bytes())),
                    }];
                    return Ok(res);
                }
            }
        } else {
            Err(anyhow!("Intentional Fail"))
        }
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
