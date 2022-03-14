use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, Messages};
use crate::transforms::{ChainResponse, Transform, Transforms, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct DebugReturnerConfig {
    #[serde(flatten)]
    response: Response,
}

impl DebugReturnerConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
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
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        match &self.response {
            Response::Message(message) => Ok(message.clone()),
            Response::Redis(string) => Ok(message_wrapper
                .messages
                .iter()
                .map(|_| {
                    Message::from_frame(Frame::Redis(RedisFrame::BulkString(
                        string.to_string().into(),
                    )))
                })
                .collect()),
            Response::Fail => Err(anyhow!("Intentional Fail")),
        }
    }

    fn is_terminating(&self) -> bool {
        true
    }
}
