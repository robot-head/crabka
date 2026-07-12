//! Batched timestamp-oracle client seam.

use std::{num::NonZeroU64, sync::Arc};

use tokio::sync::{Mutex, oneshot};

use crate::tso::oracle::{GrantLease, TsoError, TsoTimestamp, parse_count};

/// RPC seam implemented by local test doubles and range transport adapters.
#[async_trait::async_trait]
pub trait TsoRpc: Send + Sync + 'static {
    /// Request one contiguous timestamp grant from range 0.
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError>;
}

/// Timestamp client that coalesces concurrent requests into one range-0 grant.
pub struct BatchedTsoClient<R> {
    rpc: Arc<R>,
    pending: Arc<Mutex<Vec<PendingGrant>>>,
}

impl<R> Clone for BatchedTsoClient<R> {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            pending: Arc::clone(&self.pending),
        }
    }
}

impl<R> BatchedTsoClient<R>
where
    R: TsoRpc,
{
    /// Build a batched client over a timestamp RPC seam.
    #[must_use]
    pub fn new(rpc: Arc<R>) -> Self {
        Self {
            rpc,
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Grant `count` contiguous timestamps, batched with concurrent callers.
    pub async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        let (sender, receiver) = oneshot::channel();
        let should_spawn = {
            let mut pending = self.pending.lock().await;
            let should_spawn = pending.is_empty();
            pending.push(PendingGrant { count, sender });
            should_spawn
        };

        if should_spawn {
            self.spawn_flush();
        }

        receiver
            .await
            .map_err(|_| TsoError::Rpc("batched timestamp request was canceled".to_owned()))?
    }

    fn spawn_flush(&self) {
        let rpc = Arc::clone(&self.rpc);
        let pending = Arc::clone(&self.pending);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let batch = {
                let mut guard = pending.lock().await;
                std::mem::take(&mut *guard)
            };
            flush_batch(rpc.as_ref(), batch).await;
        });
    }
}

#[async_trait::async_trait]
impl<R> TsoRpc for BatchedTsoClient<R>
where
    R: TsoRpc,
{
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        BatchedTsoClient::grant(self, count).await
    }
}

struct PendingGrant {
    count: NonZeroU64,
    sender: oneshot::Sender<Result<GrantLease, TsoError>>,
}

async fn flush_batch(rpc: &dyn TsoRpc, batch: Vec<PendingGrant>) {
    let Some(total) = sum_counts(&batch) else {
        return;
    };

    let grant = rpc.grant(total).await;
    match grant {
        Ok(lease) => split_lease(lease, batch),
        Err(error) => fail_batch(&error, batch),
    }
}

fn sum_counts(batch: &[PendingGrant]) -> Option<NonZeroU64> {
    let mut total = 0_u64;
    for pending in batch {
        total = total.checked_add(pending.count.get())?;
    }
    NonZeroU64::new(total)
}

fn split_lease(lease: GrantLease, batch: Vec<PendingGrant>) {
    let mut next_ts = lease.first_ts.get();
    for pending in batch {
        let response = parse_count(pending.count.get()).and_then(|count| {
            let first_ts =
                TsoTimestamp::new(NonZeroU64::new(next_ts).ok_or(TsoError::TimestampOverflow)?);
            next_ts = next_ts
                .checked_add(count.get())
                .ok_or(TsoError::TimestampOverflow)?;
            Ok(GrantLease::new(first_ts, count))
        });
        let _send_result = pending.sender.send(response);
    }
}

fn fail_batch(error: &TsoError, batch: Vec<PendingGrant>) {
    let message = error.to_string();
    for pending in batch {
        let _send_result = pending.sender.send(Err(TsoError::Rpc(message.clone())));
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };

    use assert2::assert;

    use super::*;

    struct CountingRpc {
        next_ts: AtomicU64,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TsoRpc for CountingRpc {
        async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let first = self.next_ts.fetch_add(count.get(), Ordering::SeqCst);
            Ok(GrantLease::new(
                TsoTimestamp::new(NonZeroU64::new(first).expect("non-zero first timestamp")),
                count,
            ))
        }
    }

    fn count(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test count is non-zero")
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_rpc_grant() {
        let rpc = Arc::new(CountingRpc {
            next_ts: AtomicU64::new(1),
            calls: AtomicUsize::new(0),
        });
        let client = BatchedTsoClient::new(rpc.clone());

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(2)).await }
        });
        let second = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(3)).await }
        });

        let first = first.await.expect("join").expect("grant");
        let second = second.await.expect("join").expect("grant");

        assert!(first == GrantLease::new(TsoTimestamp::new(count(1)), count(2)));
        assert!(second == GrantLease::new(TsoTimestamp::new(count(3)), count(3)));
        assert!(rpc.calls.load(Ordering::SeqCst) == 1);
    }
}
