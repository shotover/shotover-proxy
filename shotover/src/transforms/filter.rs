use crate::message::{Message, Messages, QueryType};
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

use super::Transforms;

static SHOWN_ERROR: AtomicBool = AtomicBool::new(false);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub enum Filter {
    AllowList(Vec<QueryType>),
    DenyList(Vec<QueryType>),
}

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    pub filter: Filter,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct QueryTypeFilterConfig {
    #[serde(flatten)]
    pub filter: Filter,
}

#[typetag::serde(name = "QueryTypeFilter")]
#[async_trait(?Send)]
impl TransformConfig for QueryTypeFilterConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(QueryTypeFilter {
            filter: self.filter.clone(),
        }))
    }
}

impl TransformBuilder for QueryTypeFilter {
    fn build(&self) -> Transforms {
        Transforms::QueryTypeFilter(self.clone())
    }

    fn get_name(&self) -> &'static str {
        "QueryTypeFilter"
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        let removed_indexes: Result<Vec<(usize, Message)>> = requests_wrapper
            .requests
            .iter_mut()
            .enumerate()
            .filter_map(|(i, m)| match self.filter {
                Filter::AllowList(ref allow_list) => {
                    if allow_list.contains(&m.get_query_type()) {
                        return None;
                    } else {
                        return Some((i, m));
                    }
                }
                Filter::DenyList(ref deny_list) => {
                    if deny_list.contains(&m.get_query_type()) {
                        return Some((i, m));
                    } else {
                        return None;
                    }
                }
            })
            .map(|(i, m)| {
                Ok((
                    i,
                    m.to_error_response("Message was filtered out by shotover".to_owned())
                        .map_err(|e| e.context("Failed to filter message {e:?}"))?,
                ))
            })
            .collect();

        let removed_indexes = removed_indexes?;

        for (i, _) in removed_indexes.iter().rev() {
            requests_wrapper.requests.remove(*i);
        }

        let mut shown_error = SHOWN_ERROR.load(Ordering::Relaxed);

        requests_wrapper
            .call_next_transform()
            .await
            .map(|mut messages| {

                for (i, message) in removed_indexes.into_iter() {
                    if i <= messages.len() {
                        messages.insert(i, message);
                    }
                    else if !shown_error{
                        tracing::error!("The current filter transform implementation does not obey the current transform invariants. see https://github.com/shotover/shotover-proxy/issues/499");
                        shown_error = true;
                        SHOWN_ERROR.store(true , Ordering::Relaxed);
                    }
                }
                messages
            })
    }
}

#[cfg(test)]
mod test {
    use super::Filter;
    use crate::frame::Frame;
    use crate::frame::RedisFrame;
    use crate::message::{Message, QueryType};
    use crate::transforms::filter::QueryTypeFilter;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Transforms, Wrapper};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter_denylist() {
        let mut filter_transform = QueryTypeFilter {
            filter: Filter::DenyList(vec![QueryType::Read]),
        };

        let mut chain = vec![Transforms::Loopback(Loopback::default())];

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

        let mut requests_wrapper = Wrapper::new(messages);
        requests_wrapper.reset(&mut chain);
        let result = filter_transform.transform(requests_wrapper).await.unwrap();

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
        };

        let mut chain = vec![Transforms::Loopback(Loopback::default())];

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

        let mut requests_wrapper = Wrapper::new(messages);
        requests_wrapper.reset(&mut chain);
        let result = filter_transform.transform(requests_wrapper).await.unwrap();

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
