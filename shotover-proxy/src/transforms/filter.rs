use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{MessageDetails, QueryType};
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
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

#[async_trait]
impl TransformsFromConfig for QueryTypeFilterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::QueryTypeFilter(QueryTypeFilter {
            filter: self.filter.clone(),
        }))
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        message_wrapper.messages.retain(|m| {
            if let MessageDetails::Query(qm) = &m.details {
                qm.query_type != self.filter
            } else {
                m.original.get_query_type() != self.filter
            }
        });
        message_wrapper.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        "QueryType Filter"
    }
}

#[cfg(test)]
mod test {
    use crate::message::{Message, MessageDetails, QueryMessage, QueryType};
    use crate::protocols::RawFrame;
    use crate::transforms::filter::QueryTypeFilter;
    use crate::transforms::null::Null;
    use crate::transforms::{Transform, Transforms, Wrapper};
    use anyhow::Result;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter() -> Result<()> {
        let mut coalesce = QueryTypeFilter {
            filter: QueryType::Read,
        };

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..26)
            .map(|i| {
                let qt = if i % 2 == 0 {
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
                        query_type: qt,
                        ast: None,
                    },
                    true,
                    RawFrame::None,
                )
            })
            .collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut null];
        let result = coalesce.transform(message_wrapper).await?;
        assert_eq!(result.len(), 13);
        let any = result.iter().find(|m| {
            if let MessageDetails::Response(qr) = &m.details {
                if let Some(qm) = &qr.matching_query {
                    return qm.query_type == QueryType::Read;
                }
            }

            false
        });

        assert_eq!(any, None);

        Ok(())
    }
}
