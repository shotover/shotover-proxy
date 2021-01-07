use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::message::{Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Coalesce {
    name: &'static str,
    max_behavior: CoalesceBehavior,
    buffer: Messages,
    last_write: Instant,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum CoalesceBehavior {
    COUNT(usize),
    WAIT_MS(u128),
    COUNT_OR_WAIT(usize, u128),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
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
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        self.buffer.messages.append(&mut qd.message.messages);

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
            std::mem::swap(&mut self.buffer.messages, &mut qd.message.messages);
            qd.call_next_transform().await
        } else {
            ChainResponse::Ok(Messages::new_single_response(
                QueryResponse::empty(),
                true,
                RawFrame::NONE,
            ))
        };
    }

    fn get_name(&self) -> &'static str {
        self.name
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
            name: "test",
            max_behavior: CoalesceBehavior::COUNT(100),
            buffer: Messages::new_with_size_hint(100),
            last_write: Instant::now(),
        };

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::NONE))
            .collect();

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 100);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

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

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::NONE))
            .collect();

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 75);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

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

        let mut null = Transforms::Null(Null::new());

        let messages: Vec<Message> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::NONE))
            .collect();

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 75);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 100);

        let mut qd = Wrapper::new(Messages {
            messages: messages.clone(),
        });
        qd.transforms = vec![&mut null];
        assert_eq!(coalesce.transform(qd).await?.messages.len(), 1);

        Ok(())
    }
}
