use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::message::{Message, MessageIdMap, Messages, QueryType};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum Filter {
    AllowList(Vec<QueryType>),
    DenyList(Vec<QueryType>),
}

#[derive(Clone)]
pub struct QueryTypeFilter {
    pub filter: Filter,
    pub filtered_requests: MessageIdMap<Message>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryTypeFilterConfig {
    #[serde(flatten)]
    pub filter: Filter,
}

const NAME: &str = "QueryTypeFilter";
#[typetag::serde(name = "QueryTypeFilter")]
#[async_trait(?Send)]
impl TransformConfig for QueryTypeFilterConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(QueryTypeFilter {
            filter: self.filter.clone(),
            filtered_requests: MessageIdMap::default(),
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

impl TransformBuilder for QueryTypeFilter {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(
        &'a mut self,
        requests_wrapper: &'a mut Wrapper<'a>,
    ) -> Result<Messages> {
        for request in requests_wrapper.requests.iter_mut() {
            let filter_out = match &self.filter {
                Filter::AllowList(allow_list) => !allow_list.contains(&request.get_query_type()),
                Filter::DenyList(deny_list) => deny_list.contains(&request.get_query_type()),
            };

            if filter_out {
                self.filtered_requests.insert(
                    request.id(),
                    request
                        .from_request_to_error_response(
                            "Message was filtered out by shotover".to_owned(),
                        )
                        .map_err(|e| e.context("Failed to filter message"))?,
                );
                request.replace_with_dummy();
            }
        }

        let mut responses = requests_wrapper.call_next_transform().await?;
        for response in responses.iter_mut() {
            if let Some(request_id) = response.request_id() {
                if let Some(error_response) = self.filtered_requests.remove(&request_id) {
                    *response = error_response;
                }
            }
        }

        Ok(responses)
    }
}

#[cfg(all(test, feature = "redis"))]
mod test {
    use super::Filter;
    use crate::frame::Frame;
    use crate::frame::RedisFrame;
    use crate::message::MessageIdMap;
    use crate::message::{Message, QueryType};
    use crate::transforms::chain::TransformAndMetrics;
    use crate::transforms::filter::QueryTypeFilter;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Wrapper};
    use pretty_assertions::assert_eq;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter_denylist() {
        let mut filter_transform = QueryTypeFilter {
            filter: Filter::DenyList(vec![QueryType::Read]),
            filtered_requests: MessageIdMap::default(),
        };

        let mut chain = vec![TransformAndMetrics::new(Box::new(Loopback::default()))];

        let messages: Vec<_> = (0..26)
            .map(|i| {
                if i % 2 == 0 {
                    Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString("GET".into()),
                        RedisFrame::BulkString("key".into()),
                    ])))
                } else {
                    Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString("SET".into()),
                        RedisFrame::BulkString("key".into()),
                        RedisFrame::BulkString("value".into()),
                    ])))
                }
            })
            .collect();

        let mut requests_wrapper = Wrapper::new_test(messages);
        requests_wrapper.reset(&mut chain);
        let result = filter_transform
            .transform(&mut requests_wrapper)
            .await
            .unwrap();

        assert_eq!(result.len(), 26);

        for (i, mut message) in result.into_iter().enumerate() {
            if let Some(frame) = message.frame() {
                if i % 2 == 0 {
                    assert_eq!(
                        frame,
                        &Frame::Redis(RedisFrame::Error(
                            "ERR Message was filtered out by shotover".into()
                        )),
                    )
                } else {
                    assert_eq!(
                        frame,
                        &Frame::Redis(RedisFrame::Array(vec![
                            RedisFrame::BulkString("SET".into()),
                            RedisFrame::BulkString("key".into()),
                            RedisFrame::BulkString("value".into()),
                        ]))
                    )
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter_allowlist() {
        let mut filter_transform = QueryTypeFilter {
            filter: Filter::AllowList(vec![QueryType::Write]),
            filtered_requests: MessageIdMap::default(),
        };

        let mut chain = vec![TransformAndMetrics::new(Box::new(Loopback::default()))];

        let messages: Vec<_> = (0..26)
            .map(|i| {
                if i % 2 == 0 {
                    Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString("GET".into()),
                        RedisFrame::BulkString("key".into()),
                    ])))
                } else {
                    Message::from_frame(Frame::Redis(RedisFrame::Array(vec![
                        RedisFrame::BulkString("SET".into()),
                        RedisFrame::BulkString("key".into()),
                        RedisFrame::BulkString("value".into()),
                    ])))
                }
            })
            .collect();

        let mut requests_wrapper = Wrapper::new_test(messages);
        requests_wrapper.reset(&mut chain);
        let result = filter_transform
            .transform(&mut requests_wrapper)
            .await
            .unwrap();

        assert_eq!(result.len(), 26);

        for (i, mut message) in result.into_iter().enumerate() {
            if let Some(frame) = message.frame() {
                if i % 2 == 0 {
                    assert_eq!(
                        frame,
                        &Frame::Redis(RedisFrame::Error(
                            "ERR Message was filtered out by shotover".into()
                        )),
                    )
                } else {
                    assert_eq!(
                        frame,
                        &Frame::Redis(RedisFrame::Array(vec![
                            RedisFrame::BulkString("SET".into()),
                            RedisFrame::BulkString("key".into()),
                            RedisFrame::BulkString("value".into()),
                        ]))
                    )
                }
            }
        }
    }
}
