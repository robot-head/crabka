//! Batched timestamp-oracle client seam.

use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::oneshot;

use crate::tso::{
    oracle::{GrantLease, TsoError, TsoTimestamp, parse_count},
    stats::TsoClientStats,
};

/// RPC seam implemented by local test doubles and range transport adapters.
#[async_trait::async_trait]
pub trait TsoRpc: Send + Sync + 'static {
    /// Request one contiguous timestamp grant from range 0.
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError>;
}

/// Timestamp client that coalesces concurrent requests into one range-0 grant.
///
/// Grants ride a conveyor: at most one upstream RPC is in flight at a time,
/// and requests arriving while it runs accumulate into the next single RPC,
/// so batch size adapts to upstream latency without artificial delay.
pub struct BatchedTsoClient<R> {
    rpc: Arc<R>,
    queue: Arc<Mutex<GrantQueue>>,
    stats: Arc<TsoClientStats>,
}

impl<R> Clone for BatchedTsoClient<R> {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            queue: Arc::clone(&self.queue),
            stats: Arc::clone(&self.stats),
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
            queue: Arc::new(Mutex::new(GrantQueue {
                pending: Vec::new(),
                flusher_running: false,
            })),
            stats: Arc::default(),
        }
    }

    /// Record batch-fill activity into `stats` so an external poller can
    /// observe this client.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<TsoClientStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Return the stats handle recording this client's batch fill.
    #[must_use]
    pub fn stats(&self) -> Arc<TsoClientStats> {
        Arc::clone(&self.stats)
    }

    /// Grant `count` contiguous timestamps, batched with concurrent callers.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        let (sender, receiver) = oneshot::channel();
        let should_spawn = {
            let mut queue = lock_queue(&self.queue);
            queue.pending.push(PendingGrant { count, sender });
            // Test-and-set the flag under the same lock as the push (see the
            // `GrantQueue` invariant): only the caller that flips it from
            // clear to set starts a flusher.
            !std::mem::replace(&mut queue.flusher_running, true)
        };

        if should_spawn {
            self.spawn_flush();
        }

        receiver
            .await
            .map_err(|_| TsoError::Rpc("batched timestamp request was canceled".to_owned()))?
    }

    // Conveyor flusher: each iteration drains everything queued so far and
    // issues one upstream RPC for the summed count, so at most one RPC is in
    // flight at a time and batch size self-tunes to upstream latency.
    fn spawn_flush(&self) {
        let rpc = Arc::clone(&self.rpc);
        let queue = Arc::clone(&self.queue);
        let stats = Arc::clone(&self.stats);
        tokio::spawn(async move {
            let mut reset = FlusherResetGuard {
                queue: Arc::clone(&queue),
                armed: true,
            };
            // Let same-tick concurrent callers enqueue before the first batch
            // is taken so they coalesce into a single upstream grant. Requests
            // arriving during a later flush are already queued when it ends,
            // so subsequent iterations never need to yield — and must not:
            // with no await between the final reply send and task exit, the
            // task (and the upstream handles it holds) drops before any
            // awakened caller runs, so a caller may tear down the client and
            // its upstream immediately after receiving a grant.
            tokio::task::yield_now().await;
            loop {
                let batch = {
                    let mut guard = lock_queue(&queue);
                    if guard.pending.is_empty() {
                        // Clean exit: clear the flag under the same lock as
                        // the emptiness check (see the `GrantQueue`
                        // invariant) and disarm the panic-recovery guard.
                        guard.flusher_running = false;
                        reset.disarm();
                        return;
                    }
                    std::mem::take(&mut guard.pending)
                };
                flush_batch(rpc.as_ref(), &stats, batch).await;
            }
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

// Pending requests plus the single-flusher flag, guarded by one mutex.
//
// Invariant: the flusher's "queue empty -> clear flag -> exit" step and a
// caller's "push -> flag clear -> set flag -> spawn" step each run under this
// mutex, so a request enqueued concurrently with flusher exit either lands
// before the emptiness check (and is drained by the exiting flusher's final
// iteration) or observes the cleared flag and starts a new flusher — it is
// never stranded.
struct GrantQueue {
    pending: Vec<PendingGrant>,
    flusher_running: bool,
}

// Lock the queue, recovering from poisoning: the mutex only guards queue and
// flag bookkeeping, and `FlusherResetGuard::drop` must never itself panic.
fn lock_queue(queue: &Mutex<GrantQueue>) -> MutexGuard<'_, GrantQueue> {
    queue.lock().unwrap_or_else(PoisonError::into_inner)
}

// Re-arms the client if the flush task exits without its clean-exit path.
//
// The flusher normally clears `flusher_running` under the queue lock and then
// disarms this guard. If the upstream RPC panics (or the task is canceled),
// `Drop` runs instead: it clears the flag so the next `grant` call can start
// a fresh flusher, and fails any requests that accumulated behind the doomed
// RPC by dropping their senders, which surfaces the canceled-request error to
// those callers. Either way the client never wedges.
struct FlusherResetGuard {
    queue: Arc<Mutex<GrantQueue>>,
    armed: bool,
}

impl FlusherResetGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FlusherResetGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let stranded = {
            let mut queue = lock_queue(&self.queue);
            queue.flusher_running = false;
            std::mem::take(&mut queue.pending)
        };
        // Dropping the senders outside the lock wakes the stranded callers
        // with the canceled-request error.
        drop(stranded);
    }
}

struct PendingGrant {
    count: NonZeroU64,
    sender: oneshot::Sender<Result<GrantLease, TsoError>>,
}

async fn flush_batch(rpc: &dyn TsoRpc, stats: &TsoClientStats, batch: Vec<PendingGrant>) {
    let Some(total) = sum_counts(&batch) else {
        // The summed batch overflows `u64`; no single upstream grant can
        // satisfy it, so fail every caller instead of dropping the batch.
        for pending in batch {
            let _send_result = pending.sender.send(Err(TsoError::TimestampOverflow));
        }
        return;
    };

    stats.record_flush(u64::try_from(batch.len()).unwrap_or(u64::MAX));
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
    for pending in batch {
        let _send_result = pending.sender.send(Err(fan_out_error(error)));
    }
}

/// Reproduce the upstream failure for one coalesced caller.
///
/// A fenced epoch must survive batching as [`TsoError::FencedEpoch`]: the
/// range service maps it to a re-resolvable wire error so gateways find the
/// successor oracle. Flattening it into [`TsoError::Rpc`] would make a
/// failover read as a non-retryable failure.
fn fan_out_error(error: &TsoError) -> TsoError {
    match error {
        TsoError::FencedEpoch { epoch } => TsoError::FencedEpoch { epoch: *epoch },
        other => TsoError::Rpc(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
    };

    use assert2::assert;
    use tokio::sync::Semaphore;

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

    // Records each call's count and blocks completion until the test adds a
    // permit; asserts upstream calls never overlap.
    struct GatedRpc {
        next_ts: AtomicU64,
        calls: Mutex<Vec<u64>>,
        gate: Semaphore,
        in_flight: AtomicBool,
    }

    impl GatedRpc {
        fn new(first_ts: u64) -> Self {
            Self {
                next_ts: AtomicU64::new(first_ts),
                calls: Mutex::new(Vec::new()),
                gate: Semaphore::new(0),
                in_flight: AtomicBool::new(false),
            }
        }

        fn recorded_calls(&self) -> Vec<u64> {
            self.calls.lock().expect("calls mutex").clone()
        }
    }

    #[async_trait::async_trait]
    impl TsoRpc for GatedRpc {
        async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
            assert!(!self.in_flight.swap(true, Ordering::SeqCst));
            self.calls.lock().expect("calls mutex").push(count.get());
            self.gate
                .acquire()
                .await
                .expect("gate semaphore closed")
                .forget();
            let first = self.next_ts.fetch_add(count.get(), Ordering::SeqCst);
            assert!(self.in_flight.swap(false, Ordering::SeqCst));
            Ok(GrantLease::new(
                TsoTimestamp::new(NonZeroU64::new(first).expect("non-zero first timestamp")),
                count,
            ))
        }
    }

    // Panics on the first call and behaves like `CountingRpc` afterwards.
    struct PanicOnceRpc {
        next_ts: AtomicU64,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TsoRpc for PanicOnceRpc {
        async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
            // The first call panics to simulate a buggy upstream implementation.
            assert!(
                self.calls.fetch_add(1, Ordering::SeqCst) > 0,
                "injected timestamp rpc panic"
            );
            let first = self.next_ts.fetch_add(count.get(), Ordering::SeqCst);
            Ok(GrantLease::new(
                TsoTimestamp::new(NonZeroU64::new(first).expect("non-zero first timestamp")),
                count,
            ))
        }
    }

    // Asserts upstream calls never overlap, with await points inside each
    // call to widen any would-be race window.
    struct SerializingRpc {
        next_ts: AtomicU64,
        in_flight: AtomicBool,
    }

    #[async_trait::async_trait]
    impl TsoRpc for SerializingRpc {
        async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
            assert!(!self.in_flight.swap(true, Ordering::SeqCst));
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            let first = self.next_ts.fetch_add(count.get(), Ordering::SeqCst);
            assert!(self.in_flight.swap(false, Ordering::SeqCst));
            Ok(GrantLease::new(
                TsoTimestamp::new(NonZeroU64::new(first).expect("non-zero first timestamp")),
                count,
            ))
        }
    }

    fn count(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test count is non-zero")
    }

    // Assert `leases` partition the contiguous timestamp range starting at
    // `first_ts` and covering `total` timestamps, with no gaps or overlaps.
    fn assert_leases_tile(leases: &[GrantLease], first_ts: u64, total: u64) {
        let mut intervals: Vec<(u64, u64)> = leases
            .iter()
            .map(|lease| (lease.first_ts.get(), lease.count.get()))
            .collect();
        intervals.sort_unstable();
        let mut next = first_ts;
        for (first, granted) in intervals {
            assert!(first == next);
            next += granted;
        }
        assert!(next == first_ts + total);
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

    #[tokio::test]
    async fn in_flight_rpc_accumulates_queued_grants_into_one_batch() {
        let rpc = Arc::new(GatedRpc::new(1));
        let client = BatchedTsoClient::new(rpc.clone());

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(2)).await }
        });
        while !rpc.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        let queued_counts = [1_u64, 2, 3, 4, 5];
        let mut queued = Vec::new();
        for &requested in &queued_counts {
            queued.push(tokio::spawn({
                let client = client.clone();
                async move { client.grant(count(requested)).await }
            }));
        }
        while lock_queue(&client.queue).pending.len() < queued_counts.len() {
            tokio::task::yield_now().await;
        }

        // One permit per expected upstream call: the in-flight RPC plus the
        // single conveyor batch for everything queued behind it.
        rpc.gate.add_permits(2);

        let first = first.await.expect("join").expect("first grant");
        let mut leases = vec![first];
        for (handle, &requested) in queued.into_iter().zip(&queued_counts) {
            let lease = handle.await.expect("join").expect("queued grant");
            assert!(lease.count.get() == requested);
            leases.push(lease);
        }

        assert!(first == GrantLease::new(TsoTimestamp::new(count(1)), count(2)));
        assert!(rpc.recorded_calls() == vec![2, 15]);
        assert_leases_tile(&leases, 1, 17);
    }

    #[tokio::test]
    async fn batch_fill_stats_count_rpcs_and_coalesced_requests() {
        use crate::tso::stats::TsoClientStatsSnapshot;

        let rpc = Arc::new(GatedRpc::new(1));
        let stats = Arc::new(TsoClientStats::default());
        let client = BatchedTsoClient::new(rpc.clone()).with_stats(Arc::clone(&stats));

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(2)).await }
        });
        while !rpc.in_flight.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        let queued_counts = [1_u64, 2, 3, 4, 5];
        let mut queued = Vec::new();
        for &requested in &queued_counts {
            queued.push(tokio::spawn({
                let client = client.clone();
                async move { client.grant(count(requested)).await }
            }));
        }
        while lock_queue(&client.queue).pending.len() < queued_counts.len() {
            tokio::task::yield_now().await;
        }
        rpc.gate.add_permits(2);

        first.await.expect("join").expect("first grant");
        for handle in queued {
            handle.await.expect("join").expect("queued grant");
        }

        // One RPC carried the lone opener, one carried the 5 queued callers.
        assert!(
            stats.snapshot()
                == TsoClientStatsSnapshot {
                    rpcs_issued: 2,
                    requests_coalesced: 6,
                }
        );
        assert!(client.stats().snapshot() == stats.snapshot());
    }

    #[tokio::test]
    async fn fenced_upstream_error_survives_batching_for_every_caller() {
        struct FencedRpc;

        #[async_trait::async_trait]
        impl TsoRpc for FencedRpc {
            async fn grant(&self, _count: NonZeroU64) -> Result<GrantLease, TsoError> {
                Err(TsoError::FencedEpoch { epoch: 7 })
            }
        }

        let client = BatchedTsoClient::new(Arc::new(FencedRpc));
        let first = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(1)).await }
        });
        let second = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(2)).await }
        });

        let first = first.await.expect("join").expect_err("fenced grant");
        let second = second.await.expect("join").expect_err("fenced grant");

        // Both coalesced callers must see the fenced epoch itself, not a
        // flattened generic RPC failure, so the wire mapping stays
        // re-resolvable.
        assert!(matches!(first, TsoError::FencedEpoch { epoch: 7 }));
        assert!(matches!(second, TsoError::FencedEpoch { epoch: 7 }));
    }

    #[tokio::test]
    async fn flusher_restarts_after_queue_drains_idle() {
        let rpc = Arc::new(CountingRpc {
            next_ts: AtomicU64::new(1),
            calls: AtomicUsize::new(0),
        });
        let client = BatchedTsoClient::new(rpc.clone());

        let first = client.grant(count(2)).await.expect("first grant");
        while lock_queue(&client.queue).flusher_running {
            tokio::task::yield_now().await;
        }
        let calls_after_first = rpc.calls.load(Ordering::SeqCst);

        let second = client.grant(count(3)).await.expect("second grant");

        assert!(first == GrantLease::new(TsoTimestamp::new(count(1)), count(2)));
        assert!(second == GrantLease::new(TsoTimestamp::new(count(3)), count(3)));
        assert!(rpc.calls.load(Ordering::SeqCst) == calls_after_first + 1);
    }

    #[tokio::test]
    async fn grant_recovers_after_upstream_rpc_panic() {
        let rpc = Arc::new(PanicOnceRpc {
            next_ts: AtomicU64::new(1),
            calls: AtomicUsize::new(0),
        });
        let client = BatchedTsoClient::new(rpc.clone());

        let error = client.grant(count(1)).await.expect_err("panicking rpc");
        assert!(matches!(error, TsoError::Rpc(_)));

        // Bound the recovery grant: a client wedged by a broken reset guard
        // must fail this test promptly instead of awaiting a flusher that
        // will never be re-armed.
        let recovered =
            tokio::time::timeout(std::time::Duration::from_secs(5), client.grant(count(2)))
                .await
                .expect("client must not wedge after an upstream panic")
                .expect("post-panic grant");

        assert!(recovered == GrantLease::new(TsoTimestamp::new(count(1)), count(2)));
        assert!(rpc.calls.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn overflowing_batch_fails_all_callers_with_timestamp_overflow() {
        let rpc = Arc::new(CountingRpc {
            next_ts: AtomicU64::new(1),
            calls: AtomicUsize::new(0),
        });
        let client = BatchedTsoClient::new(rpc.clone());

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(u64::MAX)).await }
        });
        let second = tokio::spawn({
            let client = client.clone();
            async move { client.grant(count(1)).await }
        });

        let first = first.await.expect("join").expect_err("overflowed batch");
        let second = second.await.expect("join").expect_err("overflowed batch");

        assert!(matches!(first, TsoError::TimestampOverflow));
        assert!(matches!(second, TsoError::TimestampOverflow));
        assert!(rpc.calls.load(Ordering::SeqCst) == 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn upstream_rpcs_never_overlap_under_concurrent_load() {
        let rpc = Arc::new(SerializingRpc {
            next_ts: AtomicU64::new(1),
            in_flight: AtomicBool::new(false),
        });
        let client = BatchedTsoClient::new(rpc.clone());

        let mut tasks = Vec::new();
        for task_index in 0..8_u64 {
            tasks.push(tokio::spawn({
                let client = client.clone();
                async move {
                    let mut leases = Vec::new();
                    for round in 0..4_u64 {
                        let requested = task_index + round + 1;
                        let lease = client.grant(count(requested)).await.expect("hammer grant");
                        assert!(lease.count.get() == requested);
                        leases.push(lease);
                    }
                    leases
                }
            }));
        }

        let mut leases = Vec::new();
        for task in tasks {
            leases.extend(task.await.expect("join"));
        }

        let total: u64 = leases.iter().map(|lease| lease.count.get()).sum();
        assert_leases_tile(&leases, 1, total);
    }
}
