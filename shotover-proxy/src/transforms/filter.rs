use crate::error::ChainResponse;
use crate::message::{Message, QueryType};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    filter: QueryType,
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryTypeFilterConfig {
    pub filter: QueryType,
}

impl QueryTypeFilterConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::QueryTypeFilter(QueryTypeFilter {
            filter: self.filter.clone(),
        }))
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        let removed_indexes: Vec<(usize, Message)> = message_wrapper
            .messages
            .iter()
            .enumerate()
            .filter(|(_, m)| m.get_query_type() == self.filter)
            .map(|(i, m)| (i, m.to_filtered_reply()))
            .collect();

        for (i, _) in removed_indexes.iter().rev() {
            message_wrapper.messages.remove(*i);
        }

        message_wrapper
            .call_next_transform()
            .await
            .map(|mut messages| {
                for (i, message) in removed_indexes.into_iter() {
                    messages.insert(i, message);
                }
                messages
            })
    }
}

#[cfg(test)]
mod test {
    use crate::frame::Frame;
    use crate::frame::RedisFrame;
    use crate::message::{Message, QueryMessage, QueryType};
    use crate::transforms::filter::QueryTypeFilter;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Transforms, Wrapper};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter() {
        let mut filter_transform = QueryTypeFilter {
            filter: QueryType::Read,
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..26)
            .map(|i| {
                let query_type = if i % 2 == 0 {
                    QueryType::Read
                } else {
                    QueryType::Write
                };

                Message::new_query(
                    QueryMessage {
                        query_string: "".to_string(),
                        namespace: vec![],
                        primary_key: Default::default(),
                        query_values: None,
                        projection: None,
                        query_type,
                        ast: None,
                    },
                    true,
                    Frame::Redis(RedisFrame::BulkString("FOO".into())),
                )
            })
            .collect();

        let mut message_wrapper = Wrapper::new(messages);
        message_wrapper.transforms = vec![&mut loopback];
        let result = filter_transform.transform(message_wrapper).await.unwrap();

        assert_eq!(result.len(), 26);
        for (i, message) in result.iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(
                    message.original,
                    Frame::Redis(RedisFrame::Error(
                        "ERR Message was filtered out by shotover".into()
                    )),
                )
            } else {
                assert_eq!(
                    message.original,
                    Frame::Redis(RedisFrame::BulkString("FOO".into()))
                )
            }
        }
    }
}
