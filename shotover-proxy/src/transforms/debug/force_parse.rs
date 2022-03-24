/// This transform will by default parse requests and responses that pass through it.
/// request and response parsing can be individually disabled if desired.
///
/// The use of this transform is to allow benchmarking the performance impact of parsing messages
/// without worrying about the performance impact of other transform logic.
/// It could also be used to ensure that messages round trip correctly when parsed.
use crate::error::ChainResponse;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct DebugForceParseConfig {
    parse_requests: Option<bool>,
    parse_responses: Option<bool>,
}

impl DebugForceParseConfig {
    pub async fn get_transform(&self) -> Result<Transforms> {
        Ok(Transforms::DebugForceParse(DebugForceParse {
            parse_requests: self.parse_requests.unwrap_or(true),
            parse_responses: self.parse_responses.unwrap_or(true),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct DebugForceParse {
    parse_requests: bool,
    parse_responses: bool,
}

#[async_trait]
impl Transform for DebugForceParse {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        if self.parse_requests {
            for message in &mut message_wrapper.messages {
                message.frame();
            }
        }

        let mut response = message_wrapper.call_next_transform().await;

        if self.parse_responses {
            if let Ok(response) = response.as_mut() {
                for message in response {
                    message.frame();
                }
            }
        }

        response
    }
}
