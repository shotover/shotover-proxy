use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use shotover_transforms::TopicHolder;
use shotover_transforms::{
    ChainResponse, MessageDetails, QueryType, Transform, TransformsFromConfig, Wrapper,
};

#[derive(Debug, Clone)]
pub struct QueryTypeFilter {
    name: &'static str,
    filter: QueryType,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct QueryTypeFilterConfig {
    pub filter: QueryType,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for QueryTypeFilterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(QueryTypeFilter {
            name: "QueryType Filter",
            filter: self.filter.clone(),
        }))
    }
}

#[async_trait]
impl Transform for QueryTypeFilter {
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        wrapped_messages.message.messages.retain(|m| {
            if let MessageDetails::Query(qm) = &m.details {
                qm.query_type != self.filter
            } else {
                m.original.get_query_type() != self.filter
            }
        });
        wrapped_messages.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use shotover_transforms::Wrapper;
    use shotover_transforms::{Message, MessageDetails, Messages, QueryMessage, QueryType};
    use shotover_transforms::{RawFrame, Transform};

    use crate::filter::QueryTypeFilter;
    use crate::null::Null;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_filter() -> Result<()> {
        let mut coalesce = QueryTypeFilter {
            name: "QueryTypeFilter",
            filter: QueryType::Read,
        };

        let mut null: Box<(dyn Transform + Send + Sync)> = Box::new(Null::new());

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

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        let result = coalesce.transform(wrapped_messages).await?;
        assert_eq!(result.messages.len(), 13);
        let any = result.messages.iter().find(|m| {
            if let MessageDetails::Response(qr) = &m.details {
                if let Some(qm) = &qr.matching_query {
                    return qm.query_type == QueryType::Read;
                }
            }
            return false;
        });

        assert_eq!(any, None);

        Ok(())
    }
}
