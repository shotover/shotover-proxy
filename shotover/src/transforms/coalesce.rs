use super::{DownChainProtocol, TransformContextBuilder, TransformContextConfig, UpChainProtocol};
use crate::message::Messages;
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

struct Coalesce {
    name: String,
    flush_when_buffered_message_count: Option<usize>,
    flush_when_millis_since_last_flush: Option<u64>,
    buffer: Messages,
    /// Background timer sets this before `force_run_chain`; transform clears it to detect a timer-driven run.
    timer_flush_pending: Option<Arc<AtomicBool>>,
    /// Notify to restart the sleep so the next wake is `duration` after the last flush.
    restart_sleep_after_flush: Option<Arc<Notify>>,
    /// Aborted in [`Drop`] when the connection chain is torn down.
    timer_task: Option<tokio::task::JoinHandle<()>>,
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
        Ok(Box::new(Coalesce {
            name: self.name.clone(),
            buffer: Vec::with_capacity(self.flush_when_buffered_message_count.unwrap_or(0)),
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            flush_when_millis_since_last_flush: self.flush_when_millis_since_last_flush,
            timer_flush_pending: None,
            restart_sleep_after_flush: None,
            timer_task: None,
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

impl TransformBuilder for Coalesce {
    fn build(&self, transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        let mut coalesce = Coalesce {
            name: self.name.clone(),
            flush_when_buffered_message_count: self.flush_when_buffered_message_count,
            flush_when_millis_since_last_flush: self.flush_when_millis_since_last_flush,
            buffer: Vec::with_capacity(self.flush_when_buffered_message_count.unwrap_or(0)),
            timer_flush_pending: None,
            restart_sleep_after_flush: None,
            timer_task: None,
        };

        if let Some(ms) = self.flush_when_millis_since_last_flush.filter(|&m| m > 0) {
            let duration = Duration::from_millis(ms);
            let pending = Arc::new(AtomicBool::new(false));
            let restart = Arc::new(Notify::new());

            coalesce.timer_flush_pending = Some(pending.clone());
            coalesce.restart_sleep_after_flush = Some(restart.clone());

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = restart.notified() => {}
                        _ = tokio::time::sleep(duration) => {
                            pending.store(true, Ordering::Relaxed);
                            transform_context.force_run_chain.notify_one();
                        }
                    }
                }
            });
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
        // No millis timer → no pending flag; timer task is not running.
        let timer_wake = match &self.timer_flush_pending {
            Some(pending) => pending.swap(false, Ordering::Relaxed),
            None => false,
        };

        self.buffer.append(&mut chain_state.requests);

        if self.buffer.is_empty() && !chain_state.flush {
            return Ok(vec![]);
        }

        let millis_wants_flush = timer_wake && !self.buffer.is_empty();

        let flush_buffer = chain_state.flush
            || self
                .flush_when_buffered_message_count
                .map(|n| self.buffer.len() >= n)
                .unwrap_or(false)
            || millis_wants_flush;

        if flush_buffer {
            std::mem::swap(&mut self.buffer, &mut chain_state.requests);
            let out = chain_state.call_next_transform().await?;
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
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;

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
