//! Range-0 read barrier backed by an on-demand broker end sample.

use std::{sync::Arc, time::Duration};

use crabka_pgexec::{ExecError, Linearizer};
use tokio::sync::{Mutex, Notify, watch};

use crate::range0_tail::{Range0Tail, Range0TailError};

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Samples a conservative committed range-0 end after a read-barrier call begins.
#[async_trait::async_trait]
pub trait Range0EndSampler: Send + Sync {
    /// Return the offset the local range-0 tail must apply before a read may serve.
    async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError>;
}

/// `Linearizer` implementation for catalog/global reads guarded by range-0.
#[derive(Clone)]
pub struct Range0Barrier {
    tail: Range0Tail,
    sampler: Arc<dyn Range0EndSampler>,
    timeout: Duration,
    samples: Arc<Mutex<SampleQueue>>,
    refresh_poke: Option<Arc<Notify>>,
}

/// The in-flight end sample, tagged so late arrivals can refuse to adopt it.
struct InflightSample {
    generation: u64,
    receiver: watch::Receiver<SampleState>,
}

struct SampleQueue {
    next_generation: u64,
    current: Option<InflightSample>,
}

impl Range0Barrier {
    /// Build a barrier over a local range-0 tail and broker end sampler.
    #[must_use]
    pub fn new(tail: Range0Tail, sampler: Arc<dyn Range0EndSampler>) -> Self {
        Self::with_timeout(tail, sampler, DEFAULT_WAIT_TIMEOUT)
    }

    /// Build a barrier with an explicit catch-up timeout.
    #[must_use]
    pub fn with_timeout(
        tail: Range0Tail,
        sampler: Arc<dyn Range0EndSampler>,
        timeout: Duration,
    ) -> Self {
        Self {
            tail,
            sampler,
            timeout,
            samples: Arc::new(Mutex::new(SampleQueue {
                next_generation: 0,
                current: None,
            })),
            refresh_poke: None,
        }
    }

    /// Wake the local follower's poll loop before sampling.
    #[must_use]
    pub fn with_refresh_poke(mut self, poke: Arc<Notify>) -> Self {
        self.refresh_poke = Some(poke);
        self
    }

    /// Wait using a committed-end sample initiated by this call.
    ///
    /// Never joins an inflight sample: a caller whose write committed before
    /// this call began must not be satisfied by a sample that started earlier.
    /// (Read barriers enforce the same rule per generation and additionally
    /// coalesce concurrent callers; this path also wakes the follower.) If a
    /// refresh poke is configured, the follower's poll loop is woken before
    /// sampling so the tail can catch up without waiting out its poll timer.
    ///
    /// # Errors
    ///
    /// Returns [`BarrierError::CatchUpTimeout`] when the local tail does not
    /// apply the sampled end within the barrier timeout, and propagates
    /// sampling and tail failures.
    pub async fn wait_for_fresh_end(&self) -> Result<(), BarrierError> {
        if let Some(poke) = &self.refresh_poke {
            poke.notify_one();
        }
        let end = self.sampler.sample_end_after_call_begins().await?;
        tokio::time::timeout(self.timeout, self.tail.wait_until_applied(end))
            .await
            .map_err(|_elapsed| BarrierError::CatchUpTimeout(self.timeout))??;
        Ok(())
    }

    /// Return an end offset from a sample whose broker fetch started after
    /// this call began — the [`Range0EndSampler`] contract.
    ///
    /// A sample already in flight at arrival may have read the log end before
    /// a write this caller must observe (a commit decision acknowledged just
    /// before the caller's release RPC, say), so adopting it would wait to a
    /// too-low offset and serve a stale read. Such a sample is only waited
    /// OUT: arrivals during a fetch form the next generation's batch and share
    /// one fresh fetch — the same conveyor coalescing as a single in-flight
    /// slot, one generation later.
    async fn sample_target_offset(&self) -> Result<i64, BarrierError> {
        let mut stale_generation: Option<u64> = None;
        loop {
            let (mut receiver, adopted) = {
                let mut guard = self.samples.lock().await;
                // A completed sample whose fetch task has not yet cleared the
                // slot would otherwise be re-observed as in flight; clear it
                // here so the next generation can start without waiting on
                // (or spinning against) that task's own deferred clear.
                if guard
                    .current
                    .as_ref()
                    .is_some_and(|sample| *sample.receiver.borrow() != SampleState::Pending)
                {
                    guard.current = None;
                }
                match &guard.current {
                    Some(sample)
                        if stale_generation.is_none()
                            || stale_generation == Some(sample.generation) =>
                    {
                        stale_generation = Some(sample.generation);
                        (sample.receiver.clone(), false)
                    }
                    Some(sample) => (sample.receiver.clone(), true),
                    None => (self.start_sample(&mut guard), true),
                }
            };
            loop {
                let state = receiver.borrow_and_update().clone();
                match state {
                    SampleState::Pending => {
                        receiver.changed().await.map_err(|_| BarrierError::Closed)?;
                    }
                    SampleState::Ready(result) if adopted => {
                        return result.map_err(BarrierError::Sample);
                    }
                    SampleState::Ready(_) => break,
                }
            }
        }
    }

    /// Start a new sample generation under the queue lock and return its
    /// receiver. The spawned fetch clears the slot on completion so the next
    /// arrival starts the following generation.
    fn start_sample(&self, guard: &mut SampleQueue) -> watch::Receiver<SampleState> {
        let (sender, receiver) = watch::channel(SampleState::Pending);
        let generation = guard.next_generation;
        guard.next_generation += 1;
        guard.current = Some(InflightSample {
            generation,
            receiver: receiver.clone(),
        });

        let sampler = self.sampler.clone();
        let queue = Arc::clone(&self.samples);
        tokio::spawn(async move {
            let result = sampler
                .sample_end_after_call_begins()
                .await
                .map_err(|error| error.to_string());
            let _send_result = sender.send(SampleState::Ready(result));
            let mut guard = queue.lock().await;
            if guard
                .current
                .as_ref()
                .is_some_and(|sample| sample.generation == generation)
            {
                guard.current = None;
            }
        });

        receiver
    }
}

#[async_trait::async_trait]
impl Linearizer for Range0Barrier {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        let target_offset = self.sample_target_offset().await?;
        tokio::time::timeout(self.timeout, self.tail.wait_until_applied(target_offset))
            .await
            .map_err(|_| ExecError::Unavailable)??;
        Ok(())
    }
}

/// Errors from range-0 barrier sampling or catch-up.
#[derive(Debug, thiserror::Error)]
pub enum BarrierError {
    /// Broker-side end sampling failed.
    #[error("range-0 end sample failed: {0}")]
    Sample(String),
    /// The tail observable closed while waiting.
    #[error("range-0 barrier observable closed")]
    Closed,
    /// Local tail application failed.
    #[error(transparent)]
    Tail(#[from] Range0TailError),
    /// The local tail did not apply the sampled end within the timeout.
    #[error("range-0 tail did not catch up within {0:?}")]
    CatchUpTimeout(Duration),
}

impl From<BarrierError> for ExecError {
    fn from(error: BarrierError) -> Self {
        match error {
            BarrierError::Sample(_)
            | BarrierError::Closed
            | BarrierError::Tail(_)
            | BarrierError::CatchUpTimeout(_) => Self::Unavailable,
        }
    }
}

impl From<Range0TailError> for ExecError {
    fn from(_error: Range0TailError) -> Self {
        Self::Unavailable
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SampleState {
    Pending,
    Ready(Result<i64, String>),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv, WriteOp};
    use tokio::sync::{Mutex as TokioMutex, Notify};

    use super::*;
    use crate::range0_tail::Range0Frame;

    #[derive(Default)]
    struct ControlledSampler {
        calls: AtomicUsize,
        offset: TokioMutex<Option<i64>>,
        notify: Notify,
    }

    #[async_trait::async_trait]
    impl Range0EndSampler for ControlledSampler {
        async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            loop {
                if let Some(offset) = *self.offset.lock().await {
                    return Ok(offset);
                }
                self.notify.notified().await;
            }
        }
    }

    impl ControlledSampler {
        async fn release(&self, offset: i64) {
            *self.offset.lock().await = Some(offset);
            self.notify.notify_waiters();
        }
    }

    /// Sampler whose calls block individually until released by call index.
    #[derive(Default)]
    struct IndexedSampler {
        calls: AtomicUsize,
        released: TokioMutex<HashMap<usize, i64>>,
        notify: Notify,
    }

    #[async_trait::async_trait]
    impl Range0EndSampler for IndexedSampler {
        async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            loop {
                let notified = self.notify.notified();
                if let Some(offset) = self.released.lock().await.get(&index).copied() {
                    return Ok(offset);
                }
                notified.await;
            }
        }
    }

    impl IndexedSampler {
        async fn release_call(&self, index: usize, offset: i64) {
            self.released.lock().await.insert(index, offset);
            self.notify.notify_waiters();
        }
    }

    // Bounded so a wait that can never be satisfied (for example under a
    // mutant that skips sampling) fails as an assertion gap, not a test hang.
    async fn wait_for_calls(calls: &AtomicUsize, at_least: usize) {
        let spin = async {
            while calls.load(Ordering::SeqCst) < at_least {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), spin)
            .await
            .unwrap_or_else(|_| panic!("sampler never reached {at_least} calls"));
    }

    #[tokio::test]
    async fn barrier_conservative_with_open_uncommitted_producer_transaction() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store.clone());
        let sampler = Arc::new(ControlledSampler::default());
        let barrier =
            Range0Barrier::with_timeout(tail.clone(), sampler.clone(), Duration::from_millis(500));

        let waiter = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        tokio::task::yield_now().await;
        sampler.release(3).await;
        tokio::task::yield_now().await;

        assert!(!waiter.is_finished());

        tail.apply_committed(&Range0Frame::new(
            3,
            vec![WriteOp::Put {
                key: b"txn-visible".to_vec(),
                value: b"yes".to_vec(),
            }],
        ))
        .expect("apply committed marker");

        assert!(waiter.await.expect("join").is_ok());
        assert!(store.get(b"txn-visible").expect("get") == Some(b"yes".to_vec()));
    }

    #[tokio::test]
    async fn barrier_freshness_commit_acked_before_call_is_visible_after_return() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store.clone());
        tail.apply_committed(&Range0Frame::new(
            7,
            vec![WriteOp::Put {
                key: b"catalog-row".to_vec(),
                value: b"acked".to_vec(),
            }],
        ))
        .expect("apply");
        let sampler = Arc::new(ControlledSampler::default());
        sampler.release(7).await;
        let barrier = Range0Barrier::new(tail, sampler);

        barrier.ensure_readable().await.expect("readable");

        assert!(store.get(b"catalog-row").expect("get") == Some(b"acked".to_vec()));
    }

    #[tokio::test]
    async fn arrivals_during_a_fetch_coalesce_into_the_next_generation() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        tail.apply_committed(&Range0Frame::new(5, Vec::new()))
            .expect("apply");
        let sampler = Arc::new(ControlledSampler::default());
        let barrier = Range0Barrier::new(tail, sampler.clone());

        let first = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        while sampler.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        // Both arrive while the first fetch is in flight: neither may adopt
        // it, and both share ONE next-generation fetch.
        let second = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        let third = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        tokio::task::yield_now().await;
        sampler.release(5).await;

        assert!(first.await.expect("join").is_ok());
        assert!(second.await.expect("join").is_ok());
        assert!(third.await.expect("join").is_ok());
        assert!(sampler.calls.load(Ordering::SeqCst) == 2);
    }

    /// Per-call scripted sampler: each fetch pops the next queued offset,
    /// blocking until the test releases it.
    struct ScriptedSampler {
        calls: AtomicUsize,
        offsets: TokioMutex<Vec<i64>>,
        released: AtomicUsize,
        notify: Notify,
    }

    impl ScriptedSampler {
        fn new(offsets: Vec<i64>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                offsets: TokioMutex::new(offsets),
                released: AtomicUsize::new(0),
                notify: Notify::new(),
            }
        }

        fn release_next(&self) {
            self.released.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    #[async_trait::async_trait]
    impl Range0EndSampler for ScriptedSampler {
        async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst);
            loop {
                if self.released.load(Ordering::SeqCst) > index {
                    return Ok(self.offsets.lock().await[index]);
                }
                self.notify.notified().await;
            }
        }
    }

    /// The linearizability floor: a caller must never be satisfied by a
    /// sample whose fetch started before the caller arrived. Here the stale
    /// in-flight fetch returns 3 — enough for the tail's applied offset — but
    /// the late caller must wait for a FRESH fetch (returning 9, covering the
    /// write it must observe) and only complete once the tail applies 9.
    /// Under the old join-any-in-flight behavior the late caller adopted the
    /// stale 3 and returned early, serving a stale read (the 55000
    /// "decision is `InProgress`" release failures under concurrent load).
    #[tokio::test]
    async fn caller_never_adopts_a_sample_started_before_its_arrival() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        tail.apply_committed(&Range0Frame::new(3, Vec::new()))
            .expect("apply through 3");
        let sampler = Arc::new(ScriptedSampler::new(vec![3, 9]));
        let barrier =
            Range0Barrier::with_timeout(tail.clone(), sampler.clone(), Duration::from_secs(5));

        let early = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        while sampler.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        // The late caller arrives while the first fetch is in flight.
        let late = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        tokio::task::yield_now().await;
        sampler.release_next();

        // The early caller's own fetch (3) satisfies it immediately.
        assert!(early.await.expect("join").is_ok());

        // The late caller must be waiting on the second fetch, not done on
        // the stale first one.
        tokio::task::yield_now().await;
        assert!(!late.is_finished());
        sampler.release_next();
        tokio::task::yield_now().await;
        assert!(
            !late.is_finished(),
            "the fresh sample's offset 9 is beyond the applied tail"
        );

        tail.apply_committed(&Range0Frame::new(9, Vec::new()))
            .expect("apply through 9");
        assert!(late.await.expect("join").is_ok());
        assert!(sampler.calls.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn fresh_end_wait_ignores_inflight_samples() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        let sampler = Arc::new(IndexedSampler::default());
        let barrier =
            Range0Barrier::with_timeout(tail.clone(), sampler.clone(), Duration::from_secs(5));

        let stale = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        wait_for_calls(&sampler.calls, 1).await;

        let fresh = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.wait_for_fresh_end().await }
        });
        wait_for_calls(&sampler.calls, 2).await;

        // Resolve the inflight sample and let the tail reach its stale end.
        sampler.release_call(0, 3).await;
        tail.apply_committed(&Range0Frame::new(3, Vec::new()))
            .expect("apply stale end");
        assert!(stale.await.expect("join stale").is_ok());
        tokio::task::yield_now().await;
        assert!(!fresh.is_finished());

        // Resolve the fresh sample; the wait completes only once its end applies.
        sampler.release_call(1, 5).await;
        tokio::task::yield_now().await;
        assert!(!fresh.is_finished());
        tail.apply_committed(&Range0Frame::new(5, Vec::new()))
            .expect("apply fresh end");
        assert!(fresh.await.expect("join fresh").is_ok());
        assert!(sampler.calls.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn fresh_end_wait_pokes_refresh_before_sampling() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        let sampler = Arc::new(ControlledSampler::default());
        let poke = Arc::new(Notify::new());
        let barrier =
            Range0Barrier::with_timeout(tail.clone(), sampler.clone(), Duration::from_secs(5))
                .with_refresh_poke(poke.clone());

        // Poll-loop stand-in: parked on the poke; only it releases the sampler
        // and advances the tail, so the wait below completes only if the poke
        // lands before sampling.
        let poll_loop = tokio::spawn({
            let tail = tail.clone();
            let sampler = sampler.clone();
            let poke = poke.clone();
            async move {
                poke.notified().await;
                tail.apply_committed(&Range0Frame::new(2, Vec::new()))
                    .expect("apply on poke");
                sampler.release(2).await;
            }
        });
        tokio::task::yield_now().await;

        assert!(barrier.wait_for_fresh_end().await.is_ok());

        // Assert the side effects before joining: a wait that skipped the poke
        // or the sample must fail here, not hang on a never-poked poll loop.
        assert!(tail.applied_offset() == 2);
        assert!(sampler.calls.load(Ordering::SeqCst) == 1);
        tokio::time::timeout(Duration::from_secs(5), poll_loop)
            .await
            .expect("poll loop must have been poked")
            .expect("join poll loop");
    }

    #[tokio::test]
    async fn fresh_end_wait_times_out_when_tail_never_catches_up() {
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store);
        let sampler = Arc::new(ControlledSampler::default());
        sampler.release(9).await;
        let barrier = Range0Barrier::with_timeout(tail, sampler, Duration::from_millis(50));

        let error = barrier
            .wait_for_fresh_end()
            .await
            .expect_err("tail never catches up");

        assert!(matches!(
            error,
            BarrierError::CatchUpTimeout(timeout) if timeout == Duration::from_millis(50)
        ));
    }
}
