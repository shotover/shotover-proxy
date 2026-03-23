use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::message::Messages;
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::time::{Duration, Instant};

struct Coalesce {
    flush_when_buffered_message_count: Option<usize>,
    buffer: Messages,
    /// When set, a time-based flush is due once `last_flush` is at least this old (see docs).
    millis_interval: Option<Duration>,
    /// Shared with the timer task (`tokio::time::Instant` matches `tokio::time::sleep`).
    /// Timer uses `read()`; transform uses `write()` when a run restarts the millis clock (`tokio::sync::RwLock` is write-preferring).
    last_flush: Option<Arc<RwLock<Instant>>>,
    /// Wake the timer when `last_flush` changes so it recomputes remaining sleep (see docs).
    restart_sleep_after_flush: Option<Arc<Notify>>,
    /// Aborted in [`Drop`] when the connection chain is torn down.
    timer_task: Option<tokio::task::JoinHandle<()>>,
}

struct CoalesceBuilder {
    name: String,
    flush_when_buffered_message_count: Option<usize>,
    flush_when_millis_since_last_flush: Option<u64>,
}

impl Drop for Coalesce {
    fn drop(&mut self) {
        if let Some(h) = self.timer_task.take() {
            h.abort();
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CoalesceConfig {
    pub name: String,
    pub flush_when_buffered_message_count: Option<usize>,
    pub flush_when_millis_since_last_flush: Option<u64>,
}

const NAME: &str = "Coalesce";
#[typetag::serde(name = "Coalesce")]
#[async_trait(?Send)]
impl TransformConfig for CoalesceConfig {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(CoalesceBuilder {
            name: self.name.clone(),
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            flush_when_millis_since_last_flush: self.flush_when_millis_since_last_flush,
        }))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::Any
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }

    fn get_sub_chain_configs(&self) -> Vec<(&crate::config::chain::TransformChainConfig, String)> {
        vec![]
    }
}

impl TransformBuilder for CoalesceBuilder {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        let mut coalesce = Coalesce {
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            buffer: Vec::with_capacity(self.flush_when_buffered_message_count.unwrap_or(0)),
            millis_interval: None,
            last_flush: None,
            restart_sleep_after_flush: None,
            timer_task: None,
        };

        if let Some(ms) = self.flush_when_millis_since_last_flush.filter(|&m| m > 0) {
            let interval = Duration::from_millis(ms);
            let restart = Arc::new(Notify::new());
            let last_flush = Arc::new(RwLock::new(Instant::now()));

            coalesce.millis_interval = Some(interval);
            coalesce.last_flush = Some(last_flush.clone());
            coalesce.restart_sleep_after_flush = Some(restart.clone());

            let force_run_chain = transform_context.force_run_chain.clone();
            let restart_task = restart.clone();
            let last_flush_task = last_flush;
            let handle = tokio::spawn(async move {
                loop {
                    // Wait until the transform has set `last_flush` for this cycle (startup or post-flush).
                    restart_task.notified().await;

                    loop {
                        let remaining = {
                            let last = *last_flush_task.read().await;
                            interval.saturating_sub(last.elapsed())
                        };
                        tokio::select! {
                            _ = restart_task.notified() => {}
                            _ = tokio::time::sleep(remaining) => break,
                        }
                    }

                    force_run_chain.notify_one();
                }
            });
            restart.notify_one();
            coalesce.timer_task = Some(handle);
        }

        Box::new(coalesce)
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_type_name(&self) -> &'static str {
        NAME
    }

    fn validate(&self) -> Vec<String> {
        if self
            .flush_when_millis_since_last_flush
            .is_some_and(|m| m == 0)
        {
            return vec![
                "Coalesce:".into(),
                "  flush_when_millis_since_last_flush must be greater than 0 when set.".into(),
                "  Check https://shotover.io/docs/latest/transforms.html#coalesce for more information."
                    .into(),
            ];
        }

        let has_count = self.flush_when_buffered_message_count.is_some();
        let has_timer_millis = self
            .flush_when_millis_since_last_flush
            .is_some_and(|m| m > 0);
        if !has_count && !has_timer_millis {
            return vec![
                "Coalesce:".into(),
                "  Provide at least one of:".into(),
                "  * flush_when_buffered_message_count".into(),
                "  * flush_when_millis_since_last_flush (must be greater than 0)".into(),
                "".into(),
                "  But none of them were provided.".into(),
                "  Check https://shotover.io/docs/latest/transforms.html#coalesce for more information."
                    .into(),
            ];
        }

        vec![]
    }
}

#[async_trait]
impl Transform for Coalesce {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        self.buffer.append(&mut chain_state.requests);

        let millis_due = match (self.millis_interval, &self.last_flush) {
            (Some(interval), Some(last_arc)) => {
                let last = *last_arc.read().await;
                last.elapsed() >= interval
            }
            _ => false,
        };

        if self.buffer.is_empty() && !chain_state.flush {
            if let (Some(last_arc), Some(n)) = (&self.last_flush, &self.restart_sleep_after_flush) {
                *last_arc.write().await = Instant::now();
                n.notify_one();
            }
            return Ok(vec![]);
        }

        let millis_wants_flush = millis_due && !self.buffer.is_empty();

        let flush_buffer = chain_state.flush
            || self
                .flush_when_buffered_message_count
                .map(|n| self.buffer.len() >= n)
                .unwrap_or(false)
            || millis_wants_flush;

        if flush_buffer {
            std::mem::swap(&mut self.buffer, &mut chain_state.requests);
            let out = chain_state.call_next_transform().await?;
            if let Some(last_arc) = &self.last_flush {
                *last_arc.write().await = Instant::now();
            }
            if let Some(n) = &self.restart_sleep_after_flush {
                n.notify_one();
            }
            Ok(out)
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(all(test, feature = "valkey"))]
mod test {
    use crate::frame::{Frame, MessageType, ValkeyFrame};
    use crate::message::Message;
    use crate::transforms::chain::TransformAndMetrics;
    use crate::transforms::coalesce::CoalesceConfig;
    use crate::transforms::loopback::Loopback;
    use crate::transforms::{
        ChainState, Transform, TransformConfig, TransformContextBuilder, TransformContextConfig,
    };
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio::time::Duration;

    /// Counts every `force_run_chain.notify_one()` the millis timer (or anything else) performs.
    /// Tokio `Notify` delivers each `notify_one` to one waiter; this task is the only waiter in tests.
    struct ForceRunNotifyCounter {
        count: Arc<AtomicUsize>,
        _recorder: JoinHandle<()>,
    }

    impl ForceRunNotifyCounter {
        fn spawn(force_run_chain: Arc<Notify>) -> Self {
            let count = Arc::new(AtomicUsize::new(0));
            let count_task = count.clone();
            let force = force_run_chain.clone();
            let recorder = tokio::spawn(async move {
                loop {
                    force.notified().await;
                    count_task.fetch_add(1, Ordering::Relaxed);
                }
            });
            Self {
                count,
                _recorder: recorder,
            }
        }

        fn get(&self) -> usize {
            self.count.load(Ordering::Relaxed)
        }
    }

    impl Drop for ForceRunNotifyCounter {
        fn drop(&mut self) {
            self._recorder.abort();
        }
    }

    /// Production `build` with a shared `force_run_chain` for assertions.
    async fn coalesce_from_config_with_force_notify(
        flush_when_buffered_message_count: Option<usize>,
        flush_when_millis_since_last_flush: Option<u64>,
    ) -> (Box<dyn Transform>, Arc<Notify>) {
        let force_run_chain = Arc::new(Notify::new());
        let config = CoalesceConfig {
            name: "coalesce".to_string(),
            flush_when_buffered_message_count,
            flush_when_millis_since_last_flush,
        };
        let transform = config
            .get_builder(TransformContextConfig {
                chain_name: "test".to_string(),
                up_chain_protocol: MessageType::Valkey,
            })
            .await
            .unwrap()
            .build(TransformContextBuilder {
                force_run_chain: force_run_chain.clone(),
                client_details: String::new(),
            });
        (transform, force_run_chain)
    }

    fn test_chain() -> Vec<TransformAndMetrics> {
        vec![TransformAndMetrics::new(
            Box::new(Loopback::new("loopback".to_string())),
            "loopback",
            "Loopback",
        )]
    }

    fn batch_of_25() -> Vec<Message> {
        (0..25)
            .map(|_| Message::from_frame(Frame::Valkey(ValkeyFrame::Null)))
            .collect()
    }

    async fn assert_responses_len(
        chain: &mut [TransformAndMetrics],
        coalesce: &mut dyn Transform,
        requests: &[Message],
        expected_len: usize,
    ) {
        let mut wrapper = ChainState::new_test(requests.to_vec());
        wrapper.reset(chain, "test");
        assert_eq!(
            coalesce.transform(&mut wrapper).await.unwrap().len(),
            expected_len
        );
    }

    /// Flush when the buffer reaches `flush_when_buffered_message_count` (no millis timer).
    ///
    /// **Notify count:** `force_run_chain` is never used — expect **0** throughout (no background task).
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_on_buffer_limit() {
        let (mut coalesce, force) = coalesce_from_config_with_force_notify(Some(100), None).await;
        let counter = ForceRunNotifyCounter::spawn(force);
        // notify count: 0
        assert_eq!(counter.get(), 0);

        let mut chain = test_chain();
        let requests = batch_of_25();

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_eq!(counter.get(), 0); // still 0
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_eq!(counter.get(), 0);
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_eq!(counter.get(), 0);
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 100).await;
        assert_eq!(counter.get(), 0); // count flush does not call notify_one
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_eq!(counter.get(), 0);
    }

    /// Flush when the millis timer fires; simulates the source running the chain with an empty client batch.
    ///
    /// **Notify count:** goes **up by at least 1** each time the sleep interval elapses; each `notify_one`
    /// is what wakes the source so the chain can run and Coalesce can flush.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_on_timer() {
        const INTERVAL_MS: u64 = 100;
        let (mut coalesce, force) =
            coalesce_from_config_with_force_notify(None, Some(INTERVAL_MS)).await;
        let counter = ForceRunNotifyCounter::spawn(force);
        assert_eq!(counter.get(), 0);

        let mut chain = test_chain();
        let requests = batch_of_25();

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        tokio::time::sleep(Duration::from_millis(10_u64)).await;
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;

        tokio::time::sleep(Duration::from_millis(INTERVAL_MS + 15)).await;
        // First interval: expect >= 1 notify
        assert!(
            counter.get() >= 1,
            "timer should call force_run_chain::notify_one after interval"
        );

        assert_responses_len(&mut chain, &mut *coalesce, &[], 50).await;

        let after_first_flush = counter.get();
        assert!(
            after_first_flush >= 1,
            "at least one notify before empty-batch flush"
        );

        tokio::time::sleep(Duration::from_millis(INTERVAL_MS + 15)).await;
        // Second interval: strictly more notifies than after first flush
        assert!(
            counter.get() > after_first_flush,
            "timer should notify again for the next interval"
        );

        assert_responses_len(&mut chain, &mut *coalesce, &[], 0).await;
    }

    /// Timer fires while there is nothing buffered — `force_run_chain` still signals, but Coalesce does not flush.
    ///
    /// **Notify count:** **≥ 1** after waiting past the interval (timer still calls `notify_one`).
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_on_timer_empty_buffer_only_notifies() {
        const INTERVAL_MS: u64 = 80;
        let (mut coalesce, force) =
            coalesce_from_config_with_force_notify(None, Some(INTERVAL_MS)).await;
        let counter = ForceRunNotifyCounter::spawn(force);

        let mut chain = test_chain();

        tokio::time::sleep(Duration::from_millis(INTERVAL_MS + 20)).await;
        assert!(counter.get() >= 1); // notify fired; no messages to flush
        assert_responses_len(&mut chain, &mut *coalesce, &[], 0).await;
    }

    /// Buffer limit and millis timer both enabled: timer-driven flush, then count-driven flush.
    ///
    /// **Notify count:** **≥ 1** before the timer-assisted flush (75 msgs). Further timer ticks may occur
    /// while adding batches for the count flush; count-based flush does **not** call `notify_one`.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_on_both_timer_and_buffer_limit() {
        const INTERVAL_MS: u64 = 100;
        let (mut coalesce, force) =
            coalesce_from_config_with_force_notify(Some(100), Some(INTERVAL_MS)).await;
        let counter = ForceRunNotifyCounter::spawn(force);
        assert_eq!(counter.get(), 0);

        let mut chain = test_chain();
        let requests = batch_of_25();

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        tokio::time::sleep(Duration::from_millis(10_u64)).await;
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;

        tokio::time::sleep(Duration::from_millis(INTERVAL_MS + 15)).await;
        let notifies_before_timer_driven_flush = counter.get();
        assert!(
            notifies_before_timer_driven_flush >= 1,
            "timer path must signal force_run_chain"
        );

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 75).await;

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 100).await;
    }

    /// Both limits configured, but the buffer hits its threshold before the (long) timer fires.
    ///
    /// **Notify count:** **0** — only the count path runs; the millis task has not completed an interval yet.
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_on_buffer_limit_before_timer() {
        let (mut coalesce, force) =
            coalesce_from_config_with_force_notify(Some(50), Some(10_000)).await;
        let counter = ForceRunNotifyCounter::spawn(force);
        let mut chain = test_chain();
        let requests = batch_of_25();

        assert_responses_len(&mut chain, &mut *coalesce, &requests, 0).await;
        assert_eq!(counter.get(), 0);
        assert_responses_len(&mut chain, &mut *coalesce, &requests, 50).await;
        assert_eq!(
            counter.get(),
            0,
            "force_run_chain must not run until the 10s timer fires"
        );
    }
}
