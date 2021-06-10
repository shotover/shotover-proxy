use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use shotover_transforms::util::CoalesceBehavior;
use shotover_transforms::TopicHolder;
use shotover_transforms::{ChainResponse, Messages, QueryResponse, Transform, Wrapper};
use shotover_transforms::{RawFrame, TransformsFromConfig};

#[derive(Debug, Clone)]
pub struct Coalesce {
    name: &'static str,
    max_behavior: CoalesceBehavior,
    buffer: Messages,
    last_write: Instant,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Default)]
pub struct CoalesceConfig {
    pub max_behavior: CoalesceBehavior,
}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for CoalesceConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        let hint = match self.max_behavior {
            CoalesceBehavior::COUNT(c) => Some(c),
            CoalesceBehavior::COUNT_OR_WAIT(c, _) => Some(c),
            _ => None,
        };
        Ok(Box::new(Coalesce {
            name: "Coalesce",
            max_behavior: self.max_behavior.clone(),
            buffer: Messages {
                messages: if let Some(c) = hint {
                    Vec::with_capacity(c)
                } else {
                    Vec::new()
                },
            },
            last_write: Instant::now(),
        }))
    }
}

#[async_trait]
impl Transform for Coalesce {
    async fn transform<'a>(&'a mut self, mut wrapped_messages: Wrapper<'a>) -> ChainResponse {
        self.buffer
            .messages
            .append(&mut wrapped_messages.message.messages);

        return if match self.max_behavior {
            CoalesceBehavior::COUNT(c) => self.buffer.messages.len() >= c,
            CoalesceBehavior::WAIT_MS(w) => self.last_write.elapsed().as_millis() >= w,
            CoalesceBehavior::COUNT_OR_WAIT(c, w) => {
                self.last_write.elapsed().as_millis() >= w || self.buffer.messages.len() >= c
            }
        } {
            //this could be done in the if statement above, but for the moment lets keep the
            //evaluation logic separate from the update
            match self.max_behavior {
                CoalesceBehavior::WAIT_MS(_) | CoalesceBehavior::COUNT_OR_WAIT(_, _) => {
                    self.last_write = Instant::now()
                }
                _ => {}
            }
            std::mem::swap(
                &mut self.buffer.messages,
                &mut wrapped_messages.message.messages,
            );
            wrapped_messages.call_next_transform().await
        } else {
            ChainResponse::Ok(Messages::new_single_response(
                QueryResponse::empty(),
                true,
                RawFrame::None,
            ))
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, Instant};

    use anyhow::Result;

    use shotover_transforms::util::CoalesceBehavior;
    use shotover_transforms::Wrapper;
    use shotover_transforms::{Message, Messages, QueryMessage};
    use shotover_transforms::{RawFrame, Transform};

    use crate::coalesce::Coalesce;
    use crate::null::Null;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count() -> Result<()> {
        let mut coalesce = Coalesce {
            name: "test",
            max_behavior: CoalesceBehavior::COUNT(100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null: Box<(dyn Transform + Send + Sync)> = Box::new(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            100
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait() -> Result<()> {
        let mut coalesce = Coalesce {
            name: "wait",
            max_behavior: CoalesceBehavior::WAIT_MS(100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let t = Null::new();

        let mut null: Box<(dyn Transform + Send + Sync)> = Box::new(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            75
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait_or_count() -> Result<()> {
        let mut coalesce = Coalesce {
            name: "wait",
            max_behavior: CoalesceBehavior::COUNT_OR_WAIT(100, 100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null: Box<(dyn Transform + Send + Sync)> = Box::new(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            75
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            100
        );

        let mut wrapped_messages = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        wrapped_messages.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(wrapped_messages).await?.messages.len(),
            1
        );

        Ok(())
    }
}
