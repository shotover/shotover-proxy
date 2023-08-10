use crate::message::Messages;
/// This transform will by default parse requests and responses that pass through it.
/// request and response parsing can be individually disabled if desired.
///
/// The use of this transform is to allow benchmarking the performance impact of parsing messages
/// without worrying about the performance impact of other transform logic.
/// It could also be used to ensure that messages round trip correctly when parsed.
#[cfg(feature = "alpha-transforms")]
use crate::transforms::TransformConfig;
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

/// Messages that pass through this transform will be parsed.
/// Must be individually enabled at the request or response level.
#[derive(Deserialize, Debug)]
pub struct DebugForceParseConfig {
    pub parse_requests: bool,
    pub parse_responses: bool,
}

#[cfg(feature = "alpha-transforms")]
#[typetag::deserialize(name = "DebugForceParse")]
#[async_trait(?Send)]
impl TransformConfig for DebugForceParseConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
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
#[derive(Deserialize, Debug)]
pub struct DebugForceEncodeConfig {
    pub encode_requests: bool,
    pub encode_responses: bool,
}

#[cfg(feature = "alpha-transforms")]
#[typetag::deserialize(name = "DebugForceEncode")]
#[async_trait(?Send)]
impl TransformConfig for DebugForceEncodeConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
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
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        for message in &mut requests_wrapper.requests {
            if self.parse_requests {
                message.frame();
            }
            if self.encode_requests {
                message.invalidate_cache();
            }
        }

        let mut response = requests_wrapper.call_next_transform().await;

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
