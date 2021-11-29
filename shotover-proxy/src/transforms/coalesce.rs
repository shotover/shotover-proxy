use crate::error::ChainResponse;
use crate::message::{Message, MessageDetails, Messages, QueryResponse};
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, Wrapper};
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

#[derive(Deserialize, Debug, Clone)]
pub enum CoalesceBehavior {
    Count(usize),
    WaitMs(u128),
    CountOrWait(usize, u128),
}

#[derive(Deserialize, Debug, Clone)]
pub struct CoalesceConfig {
    pub max_behavior: CoalesceBehavior,
}

impl CoalesceConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        let hint = match self.max_behavior {
            CoalesceBehavior::Count(c) => Some(c),
            CoalesceBehavior::CountOrWait(c, _) => Some(c),
            _ => None,
        };
        Ok(Transforms::Coalesce(Coalesce {
            max_behavior: self.max_behavior.clone(),
            buffer: if let Some(c) = hint {
                Vec::with_capacity(c)
            } else {
                Vec::new()
            },
            last_write: Instant::now(),
        }))
    }
}

#[async_trait]
impl Transform for Coalesce {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.buffer.append(&mut message_wrapper.messages);

        if match self.max_behavior {
            CoalesceBehavior::Count(c) => self.buffer.len() >= c,
            CoalesceBehavior::WaitMs(w) => self.last_write.elapsed().as_millis() >= w,
            CoalesceBehavior::CountOrWait(c, w) => {
                self.last_write.elapsed().as_millis() >= w || self.buffer.len() >= c
            }
        } {
            //this could be done in the if statement above, but for the moment lets keep the
            //evaluation logic separate from the update
            match self.max_behavior {
                CoalesceBehavior::WaitMs(_) | CoalesceBehavior::CountOrWait(_, _) => {
                    self.last_write = Instant::now()
                }
                _ => {}
            }
            std::mem::swap(&mut self.buffer, &mut message_wrapper.messages);
            message_wrapper.call_next_transform().await
        } else {
            Ok(vec![Message::new(
                MessageDetails::Response(QueryResponse::empty()),
                true,
                RawFrame::None,
            )])
        }
    }

    async fn shutdown<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> Result<()> {
        message_wrapper.messages.append(&mut self.buffer);
        message_wrapper.call_next_shutdown().await
    }
}

#[cfg(test)]
mod test {
    use crate::message::{Message, QueryMessage};
    use crate::protocols::RawFrame;
    use crate::transforms::coalesce::{Coalesce, CoalesceBehavior};
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Transforms, Wrapper};
    use anyhow::Result;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::Count(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 100);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::WaitMs(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 75);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait_or_count() -> Result<()> {
        let mut coalesce = Coalesce {
            max_behavior: CoalesceBehavior::CountOrWait(100, 100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25)
            .map(|_| Message::new_query(QueryMessage::empty(), true, RawFrame::None))
            .collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 75);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 100);

        let mut message_wrapper = Wrapper::new(messages);
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 1);

        Ok(())
    }
}
