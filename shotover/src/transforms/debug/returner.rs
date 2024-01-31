use crate::frame::{Frame, RedisFrame};
use crate::message::{Message, Messages};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DebugReturnerConfig {
    #[serde(flatten)]
    response: Response,
}

const NAME: &str = "DebugReturner";
#[typetag::serde(name = "DebugReturner")]
#[async_trait(?Send)]
impl TransformConfig for DebugReturnerConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugReturner::new(self.response.clone())))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
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

impl TransformBuilder for DebugReturner {
    fn build(&self) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }

    fn is_terminating(&self) -> bool {
        true
    }
}

#[async_trait]
impl Transform for DebugReturner {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        match &self.response {
            Response::Message(message) => Ok(message.clone()),
            Response::Redis(string) => Ok(requests_wrapper
                .requests
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
}
