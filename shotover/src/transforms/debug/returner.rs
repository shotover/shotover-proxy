use crate::message::{Message, Messages};
use crate::transforms::{
    ChainState, DownChainProtocol, Transform, TransformBuilder, TransformConfig,
    TransformContextBuilder, TransformContextConfig, UpChainProtocol,
};
use anyhow::{Result, anyhow};
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
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugReturner::new(self.response.clone())))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum Response {
    #[serde(skip)]
    Message(Message),
    #[cfg(feature = "valkey")]
    Valkey(String),
    Fail,
}

#[derive(Clone)]
pub struct DebugReturner {
    response: Response,
}

impl DebugReturner {
    pub fn new(response: Response) -> Self {
        DebugReturner { response }
    }
}

impl TransformBuilder for DebugReturner {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
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

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        chain_state
            .requests
            .iter_mut()
            .map(|request| match &self.response {
                Response::Message(message) => {
                    let mut message = message.clone();
                    message.set_request_id(request.id());
                    Ok(message)
                }
                #[cfg(feature = "valkey")]
                Response::Valkey(string) => {
                    use crate::frame::{Frame, ValkeyFrame};
                    use crate::message::Message;
                    let mut message = Message::from_frame(Frame::Valkey(ValkeyFrame::BulkString(
                        string.to_string().into(),
                    )));
                    message.set_request_id(request.id());
                    Ok(message)
                }
                Response::Fail => Err(anyhow!("Intentional Fail!")),
            })
            .collect()
    }
}
