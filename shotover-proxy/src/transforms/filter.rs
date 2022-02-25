use crate::error::ChainResponse;
use crate::message::{Message, QueryType};
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    pub filter: QueryType,
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
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| {
                if m.get_query_type() == self.filter {
                    Some((i, m))
                } else {
                    None
                }
            })
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
    use crate::message::{Message, QueryType};
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

        let mut message_wrapper = Wrapper::new(messages);
        message_wrapper.transforms = vec![&mut loopback];
        let result = filter_transform.transform(message_wrapper).await.unwrap();

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
