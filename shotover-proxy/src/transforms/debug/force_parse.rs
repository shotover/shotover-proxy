/// This transform will by default parse requests and responses that pass through it.
/// request and response parsing can be individually disabled if desired.
///
/// The use of this transform is to allow benchmarking the performance impact of parsing messages
/// without worrying about the performance impact of other transform logic.
/// It could also be used to ensure that messages round trip correctly when parsed.
use crate::error::ChainResponse;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

/// Messages that pass through this transform will be parsed.
/// Must be individually enabled at the request or response level.
#[derive(Deserialize, Debug, Clone)]
pub struct DebugForceParseConfig {
    parse_requests: bool,
    parse_responses: bool,
}

impl DebugForceParseConfig {
    pub async fn get_builder(&self) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugForceParse {
            parse_requests: self.parse_requests,
            parse_responses: self.parse_responses,
            encode_requests: false,
            encode_responses: false,
        }))
    }
}

/// Messages that pass through this transform will be parsed and then reencoded.
/// Must be individually enabled at the request or response level.
#[derive(Deserialize, Debug, Clone)]
pub struct DebugForceEncodeConfig {
    encode_requests: bool,
    encode_responses: bool,
}

impl DebugForceEncodeConfig {
    pub async fn get_builder(&self) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(DebugForceParse {
            parse_requests: self.encode_requests,
            parse_responses: self.encode_responses,
            encode_requests: self.encode_requests,
            encode_responses: self.encode_responses,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct DebugForceParse {
    parse_requests: bool,
    parse_responses: bool,
    encode_requests: bool,
    encode_responses: bool,
}

impl TransformBuilder for DebugForceParse {
    fn build(&self) -> Transforms {
        Transforms::DebugForceParse(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "DebugForceParse"
    }
}

#[async_trait]
impl Transform for DebugForceParse {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        for message in &mut message_wrapper.messages {
            if self.parse_requests {
                message.frame();
            }
            if self.encode_requests {
                message.invalidate_cache();
            }
        }

        let mut response = message_wrapper.call_next_transform().await;

        if let Ok(response) = response.as_mut() {
            for message in response {
                if self.parse_responses {
                    message.frame();
                }
                if self.encode_responses {
                    message.invalidate_cache();
                }
            }
        }

        response
    }
}
