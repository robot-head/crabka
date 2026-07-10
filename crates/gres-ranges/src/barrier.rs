//! Range-0 read barrier backed by an on-demand broker end sample.

use std::{sync::Arc, time::Duration};

use crabka_pgexec::{ExecError, Linearizer};
use tokio::sync::{Mutex, watch};

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
    inflight: Arc<Mutex<Option<watch::Receiver<SampleState>>>>,
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
            inflight: Arc::new(Mutex::new(None)),
        }
    }

    async fn sample_target_offset(&self) -> Result<i64, BarrierError> {
        let mut receiver = self.join_or_start_sample().await;
        loop {
            let state = receiver.borrow_and_update().clone();
            match state {
                SampleState::Pending => {
                    receiver.changed().await.map_err(|_| BarrierError::Closed)?;
                }
                SampleState::Ready(Ok(offset)) => return Ok(offset),
                SampleState::Ready(Err(message)) => return Err(BarrierError::Sample(message)),
            }
        }
    }

    async fn join_or_start_sample(&self) -> watch::Receiver<SampleState> {
        let mut guard = self.inflight.lock().await;
        if let Some(receiver) = guard.as_ref() {
            return receiver.clone();
        }

        let (sender, receiver) = watch::channel(SampleState::Pending);
        *guard = Some(receiver.clone());

        let sampler = self.sampler.clone();
        let inflight = self.inflight.clone();
        tokio::spawn(async move {
            let result = sampler
                .sample_end_after_call_begins()
                .await
                .map_err(|error| error.to_string());
            let _send_result = sender.send(SampleState::Ready(result));
            *inflight.lock().await = None;
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
}

impl From<BarrierError> for ExecError {
    fn from(error: BarrierError) -> Self {
        match error {
            BarrierError::Sample(_) | BarrierError::Closed | BarrierError::Tail(_) => {
                Self::Unavailable
            }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    async fn concurrent_callers_piggyback_on_one_sample() {
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
        let second = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        tokio::task::yield_now().await;
        sampler.release(5).await;

        assert!(first.await.expect("join").is_ok());
        assert!(second.await.expect("join").is_ok());
        assert!(sampler.calls.load(Ordering::SeqCst) == 1);
    }
}
