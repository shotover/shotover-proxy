use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Coalesce {
    max_behavior: CoalesceBehavior,
    buffer: Messages,
    last_write: Instant,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Clone)]
pub enum CoalesceBehavior {
    COUNT(usize),
    WAIT_MS(u128),
    COUNT_OR_WAIT(usize, u128),
}

#[derive(Deserialize, Debug, Clone)]
pub struct CoalesceConfig {
    pub max_behavior: CoalesceBehavior,
}

#[async_trait]
impl TransformsFromConfig for CoalesceConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        let hint = match self.max_behavior {
            CoalesceBehavior::COUNT(c) => Some(c),
            CoalesceBehavior::COUNT_OR_WAIT(c, _) => Some(c),
            _ => None,
        };
        Ok(Transforms::Coalesce(Coalesce {
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
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.buffer
            .messages
            .append(&mut message_wrapper.messages.messages);

        if match self.max_behavior {
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
                &mut message_wrapper.messages.messages,
            );
            message_wrapper.call_next_transform().await
        } else {
            ChainResponse::Ok(Messages::new_single_response(
                QueryResponse::empty(),
                true,
                RawFrame::None,
            ))
        }
    }

    fn get_name(&self) -> &'static str {
        "Coalesce"
    }
}

#[cfg(test)]
mod test {
    use crate::message::{Message, Messages, QueryMessage};
    use crate::protocols::RawFrame;
    use crate::transforms::coalesce::{Coalesce, CoalesceBehavior};
    use crate::transforms::null::Null;
    use crate::transforms::{Transform, Transforms, Wrapper};
    use anyhow::Result;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::COUNT(100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(message_wrapper).await?.messages.len(),
            100
        );

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::WAIT_MS(100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(message_wrapper).await?.messages.len(),
            75
        );

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait_or_count() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::COUNT_OR_WAIT(100, 100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(message_wrapper).await?.messages.len(),
            75
        );

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(
            coalesce.transform(message_wrapper).await?.messages.len(),
            100
        );

        let mut message_wrapper = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        message_wrapper.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(message_wrapper).await?.messages.len(), 1);

        Ok(())
    }
}
