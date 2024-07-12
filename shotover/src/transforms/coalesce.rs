use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::message::Messages;
use crate::transforms::{Transform, TransformBuilder, TransformConfig, Wrapper};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Clone)]
struct Coalesce {
    flush_when_buffered_message_count: Option<usize>,
    flush_when_millis_since_last_flush: Option<u128>,
    buffer: Messages,
    last_write: Instant,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CoalesceConfig {
    pub flush_when_buffered_message_count: Option<usize>,
    pub flush_when_millis_since_last_flush: Option<u128>,
}

const NAME: &str = "Coalesce";
#[typetag::serde(name = "Coalesce")]
#[async_trait(?Send)]
impl TransformConfig for CoalesceConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(Coalesce {
            buffer: Vec::with_capacity(self.flush_when_buffered_message_count.unwrap_or(0)),
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            flush_when_millis_since_last_flush: self.flush_when_millis_since_last_flush,
            last_write: Instant::now(),
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

impl TransformBuilder for Coalesce {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
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

#[async_trait]
impl Transform for Coalesce {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'a>(&'a mut self, mut requests_wrapper: Wrapper<'a>) -> Result<Messages> {
        self.buffer.append(&mut requests_wrapper.requests);

        let flush_buffer = requests_wrapper.flush
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
            std::mem::swap(&mut self.buffer, &mut requests_wrapper.requests);
            requests_wrapper.call_next_transform().await
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(all(test, feature = "redis"))]
mod test {
    use crate::frame::{Frame, RedisFrame};
    use crate::message::Message;
    use crate::transforms::chain::TransformAndMetrics;
    use crate::transforms::coalesce::Coalesce;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{Transform, Wrapper};
    use pretty_assertions::assert_eq;
    use std::time::{Duration, Instant};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_count() {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: Some(100),
            flush_when_millis_since_last_flush: None,
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut chain = vec![TransformAndMetrics::new(Box::new(Loopback::default()))];

        let messages: Vec<_> = (0..25)
            .map(|_| Message::from_frame(Frame::Redis(RedisFrame::Null)))
            .collect();

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(
            coalesce.transform(requests_wrapper).await.unwrap().len(),
            100
        );

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait() {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: None,
            flush_when_millis_since_last_flush: Some(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut chain = vec![TransformAndMetrics::new(Box::new(Loopback::default()))];

        let messages: Vec<_> = (0..25)
            .map(|_| Message::from_frame(Frame::Redis(RedisFrame::Null)))
            .collect();

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(
            coalesce.transform(requests_wrapper).await.unwrap().len(),
            75
        );

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_wait_or_count() {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: Some(100),
            flush_when_millis_since_last_flush: Some(100),
            buffer: Vec::with_capacity(100),
            last_write: Instant::now(),
        };

        let mut chain = vec![TransformAndMetrics::new(Box::new(Loopback::default()))];

        let messages: Vec<_> = (0..25)
            .map(|_| Message::from_frame(Frame::Redis(RedisFrame::Null)))
            .collect();

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        tokio::time::sleep(Duration::from_millis(10_u64)).await;

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        tokio::time::sleep(Duration::from_millis(100_u64)).await;

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(
            coalesce.transform(requests_wrapper).await.unwrap().len(),
            75
        );

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);

        let mut requests_wrapper = Wrapper::new_test(messages.clone());
        requests_wrapper.reset(&mut chain);
        assert_eq!(
            coalesce.transform(requests_wrapper).await.unwrap().len(),
            100
        );

        let mut requests_wrapper = Wrapper::new_test(messages);
        requests_wrapper.reset(&mut chain);
        assert_eq!(coalesce.transform(requests_wrapper).await.unwrap().len(), 0);
    }
}
