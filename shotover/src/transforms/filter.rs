use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::message::{Message, MessageIdMap, Messages, QueryType};
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
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

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for request in chain_state.requests.iter_mut() {
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

        let mut responses = chain_state.call_next_transform().await?;
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

#[cfg(all(test, feature = "valkey"))]
mod test {
    use super::Filter;
    use crate::frame::Frame;
    use crate::frame::ValkeyFrame;
    use crate::message::MessageIdMap;
    use crate::message::{Message, QueryType};
    use crate::transforms::chain::TransformAndMetrics;
    use crate::transforms::filter::QueryTypeFilter;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{ChainState, Transform};
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
                    Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                        ValkeyFrame::BulkString("GET".into()),
                        ValkeyFrame::BulkString("key".into()),
                    ])))
                } else {
                    Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                        ValkeyFrame::BulkString("SET".into()),
                        ValkeyFrame::BulkString("key".into()),
                        ValkeyFrame::BulkString("value".into()),
                    ])))
                }
            })
            .collect();

        let mut chain_state = ChainState::new_test(messages);
        chain_state.reset(&mut chain);
        let result = filter_transform.transform(&mut chain_state).await.unwrap();

        assert_eq!(result.len(), 26);

        for (i, mut message) in result.into_iter().enumerate() {
            if let Some(frame) = message.frame() {
                if i % 2 == 0 {
                    assert_eq!(
                        frame,
                        &Frame::Valkey(ValkeyFrame::Error(
                            "ERR Message was filtered out by shotover".into()
                        )),
                    )
                } else {
                    assert_eq!(
                        frame,
                        &Frame::Valkey(ValkeyFrame::Array(vec![
                            ValkeyFrame::BulkString("SET".into()),
                            ValkeyFrame::BulkString("key".into()),
                            ValkeyFrame::BulkString("value".into()),
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
                    Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                        ValkeyFrame::BulkString("GET".into()),
                        ValkeyFrame::BulkString("key".into()),
                    ])))
                } else {
                    Message::from_frame(Frame::Valkey(ValkeyFrame::Array(vec![
                        ValkeyFrame::BulkString("SET".into()),
                        ValkeyFrame::BulkString("key".into()),
                        ValkeyFrame::BulkString("value".into()),
                    ])))
                }
            })
            .collect();

        let mut chain_state = ChainState::new_test(messages);
        chain_state.reset(&mut chain);
        let result = filter_transform.transform(&mut chain_state).await.unwrap();

        assert_eq!(result.len(), 26);

        for (i, mut message) in result.into_iter().enumerate() {
            if let Some(frame) = message.frame() {
                if i % 2 == 0 {
                    assert_eq!(
                        frame,
                        &Frame::Valkey(ValkeyFrame::Error(
                            "ERR Message was filtered out by shotover".into()
                        )),
                    )
                } else {
                    assert_eq!(
                        frame,
                        &Frame::Valkey(ValkeyFrame::Array(vec![
                            ValkeyFrame::BulkString("SET".into()),
                            ValkeyFrame::BulkString("key".into()),
                            ValkeyFrame::BulkString("value".into()),
                        ]))
                    )
                }
            }
        }
    }
}
