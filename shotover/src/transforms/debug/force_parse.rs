use crate::message::Messages;
/// This transform will by default parse requests and responses that pass through it.
/// request and response parsing can be individually disabled if desired.
///
/// The use of this transform is to allow benchmarking the performance impact of parsing messages
/// without worrying about the performance impact of other transform logic.
/// It could also be used to ensure that messages round trip correctly when parsed.
use crate::transforms::TransformConfig;
use crate::transforms::TransformContextBuilder;
use crate::transforms::TransformContextConfig;
use crate::transforms::{ChainState, Transform, TransformBuilder};
use crate::transforms::{DownChainProtocol, UpChainProtocol};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Messages that pass through this transform will be parsed.
/// Must be individually enabled at the request or response level.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DebugForceParseConfig {
    pub parse_requests: bool,
    pub parse_responses: bool,
}

#[typetag::serde(name = "DebugForceParse")]
#[async_trait(?Send)]
impl TransformConfig for DebugForceParseConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugForceParse {
            parse_requests: self.parse_requests,
            parse_responses: self.parse_responses,
            encode_requests: false,
            encode_responses: false,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

/// Messages that pass through this transform will be parsed and then reencoded.
/// Must be individually enabled at the request or response level.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct DebugForceEncodeConfig {
    pub encode_requests: bool,
    pub encode_responses: bool,
}

const NAME: &str = "DebugForceEncode";
#[typetag::serde(name = "DebugForceEncode")]
#[async_trait(?Send)]
impl TransformConfig for DebugForceEncodeConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugForceParse {
            parse_requests: self.encode_requests,
            parse_responses: self.encode_responses,
            encode_requests: self.encode_requests,
            encode_responses: self.encode_responses,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

#[derive(Clone)]
struct DebugForceParse {
    parse_requests: bool,
    parse_responses: bool,
    encode_requests: bool,
    encode_responses: bool,
}

impl TransformBuilder for DebugForceParse {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for DebugForceParse {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for request in &mut chain_state.requests {
            if self.parse_requests {
                request.frame();
            }
            if self.encode_requests {
                request.frame();
                request.invalidate_cache();
            }
        }

        let mut response = chain_state.call_next_transform().await;

        if let Ok(responses) = response.as_mut() {
            for response in responses {
                if self.parse_responses {
                    response.frame();
                }
                if self.encode_responses {
                    response.frame();
                    response.invalidate_cache();
                }
            }
        }

        response
    }
}
