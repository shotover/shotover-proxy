use crate::error::ChainResponse;
use crate::message::Messages;
use crate::transforms::{Transform, Transforms, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Coalesce {
    flush_when_buffered_message_count: Option<usize>,
    flush_when_millis_since_last_flush: Option<u128>,
    buffer: Messages,
    last_write: Instant,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CoalesceConfig {
    pub flush_when_buffered_message_count: Option<usize>,
    pub flush_when_millis_since_last_flush: Option<u128>,
}

impl CoalesceConfig {
    pub async fn get_source(&self) -> Result<Transforms> {
        Ok(Transforms::Coalesce(Coalesce {
            buffer: Vec::with_capacity(self.flush_when_buffered_message_count.unwrap_or(0)),
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            flush_when_millis_since_last_flush: self.flush_when_millis_since_last_flush,
            last_write: Instant::now(),
        }))
    }
}

#[async_trait]
impl Transform for Coalesce {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        self.buffer.append(&mut message_wrapper.messages);

        let flush_buffer = message_wrapper.flush
            || self
                .flush_when_buffered_message_count
                .map(|n| self.buffer.len() >= n)
                .unwrap_or(false)
            || self
                .flush_when_millis_since_last_flush
                .map(|ms| self.last_write.elapsed().as_millis() >= ms)
                .unwrap_or(false);

        if flush_buffer {
            if self.flush_when_millis_since_last_flush.is_some() {
                self.last_write = Instant::now()
            }
            std::mem::swap(&mut self.buffer, &mut message_wrapper.messages);
            message_wrapper.call_next_transform().await
        } else {
            Ok(vec![])
        }
    }

    fn validate(&self) -> Vec<String> {
        if self.flush_when_buffered_message_count.is_none()
            && self.flush_when_millis_since_last_flush.is_none()
        {
            vec![
                "Coalesce:".into(),
                "  Need to provide at least one of these fields:".into(),
                "  * flush_when_buffered_message_count".into(),
                "  * flush_when_millis_since_last_flush".into(),
                "".into(),
                "  But none of them were provided.".into(),
                "  Check https://docs.shotover.io/transforms.html#coalesce for more information."
                    .into(),
            ]
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod test {
    use crate::frame::Frame;
    use crate::message::Message;
    use crate::transforms::coalesce::Coalesce;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Transforms, Wrapper};
    use anyhow::Result;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count() -> Result<()> {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: Some(100),
            flush_when_millis_since_last_flush: None,
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25).map(|_| Message::from_frame(Frame::None)).collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 100);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait() -> Result<()> {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: None,
            flush_when_millis_since_last_flush: Some(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25).map(|_| Message::from_frame(Frame::None)).collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 75);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait_or_count() -> Result<()> {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: Some(100),
            flush_when_millis_since_last_flush: Some(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut loopback = Transforms::Loopback(Loopback::default());

        let messages: Vec<_> = (0..25).map(|_| Message::from_frame(Frame::None)).collect();

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 75);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        let mut message_wrapper = Wrapper::new(messages.clone());
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 100);

        let mut message_wrapper = Wrapper::new(messages);
        message_wrapper.transforms = vec![&mut loopback];
        assert_eq!(coalesce.transform(message_wrapper).await?.len(), 0);

        Ok(())
    }
}
