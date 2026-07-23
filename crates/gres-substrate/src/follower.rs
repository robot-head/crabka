//! Read-only range-0 recovery and committed-WAL application.

use std::sync::Arc;

use crabka_gres_ranges::{Range0EndSampler, Range0Frame, Range0Tail};
use crabka_pgkv::{Kv, RestoreKv};
use tokio::sync::Mutex;

use crate::{
    ReplayItem, WalFrame,
    checkpoint::{CheckpointStore, restore_latest},
    error::SubstrateError,
    recovery::{CommittedWalReader, FetchedWalPartition, LiveEndDialer, LiveRecoveryConfig},
};

/// Samples the committed end of a WAL topic after a barrier call begins.
#[async_trait::async_trait]
pub trait CommittedEndSampler: Send + Sync {
    /// Return the current committed end offset, or `-1` for an empty topic.
    async fn committed_end_after_call_begins(&self) -> Result<i64, SubstrateError>;
}

/// Adapter that makes a committed-end sampler usable by range-0 read barriers.
pub struct BrokerRange0EndSampler(pub Arc<dyn CommittedEndSampler>);

/// One live broker attachment used for committed-end sampling.
///
/// A page fetched at an offset below the retained log start must not fail:
/// it reports the retained start through [`FetchedWalPartition::next_offset`]
/// with no records, so a scan can jump over pruned history.
#[async_trait::async_trait]
pub(crate) trait CommittedEndConnection: Send + Sync {
    /// Fetch one committed-isolation page starting at `fetch_offset`.
    async fn fetch_page(&self, fetch_offset: i64) -> Result<FetchedWalPartition, SubstrateError>;
}

/// Dials broker attachments (connection + resolved topic identity) for
/// [`LiveCommittedEndSampler`].
#[async_trait::async_trait]
pub(crate) trait CommittedEndDialer: Send + Sync {
    /// Establish a fresh attachment to the WAL topic.
    async fn dial(&self) -> Result<Box<dyn CommittedEndConnection>, SubstrateError>;
}

/// Broker-backed committed-end sampler for a live range-zero follower.
///
/// The sampler keeps one broker attachment (connection plus resolved topic
/// UUID) and a monotone scan cursor across calls, so consecutive samples cost
/// one positioned fetch on the live connection instead of a fresh TLS
/// handshake, admin metadata round-trip, and full-topic record scan per call.
/// Every call still issues at least one broker fetch that starts after the
/// call begins, so the linearizable-sample contract of
/// [`CommittedEndSampler`] is unchanged. When the cached attachment fails,
/// the call falls back to a fresh dial — the availability of the previous
/// dial-per-call implementation — and only surfaces an error when the fresh
/// attempt also fails.
pub struct LiveCommittedEndSampler {
    dialer: Box<dyn CommittedEndDialer>,
    state: Mutex<Option<SamplerState>>,
}

struct SamplerState {
    connection: Box<dyn CommittedEndConnection>,
    cursor: EndScanCursor,
}

/// Monotone facts about the committed WAL learned by earlier scans.
///
/// Offsets below `next_fetch` have been scanned; `last_visible` is the
/// highest committed visible record offset observed so far (`-1` before any).
/// Both only grow, and both stay valid across reconnects: they describe
/// broker-side log state, not connection state.
#[derive(Debug, Clone, Copy)]
struct EndScanCursor {
    next_fetch: i64,
    last_visible: i64,
}

impl Default for EndScanCursor {
    fn default() -> Self {
        Self {
            next_fetch: 0,
            last_visible: -1,
        }
    }
}

impl LiveCommittedEndSampler {
    /// Build a sampler that dials the live broker in `config` on first use.
    #[must_use]
    pub fn new(config: LiveRecoveryConfig) -> Self {
        Self::with_dialer(Box::new(LiveEndDialer::new(config)))
    }

    pub(crate) fn with_dialer(dialer: Box<dyn CommittedEndDialer>) -> Self {
        Self {
            dialer,
            state: Mutex::new(None),
        }
    }

    /// Sample the committed end with a broker fetch issued after this call
    /// begins, reusing the cached attachment and scan cursor when possible.
    ///
    /// # Errors
    ///
    /// Returns an error when no attachment can serve the sample: a cached
    /// attachment that fails is dropped and replaced by one fresh dial, and
    /// only that fresh attempt's failure surfaces.
    pub async fn committed_end(&self) -> Result<i64, SubstrateError> {
        let mut guard = self.state.lock().await;
        if let Some(state) = guard.as_mut() {
            match scan_to_stable_end(state.connection.as_ref(), &mut state.cursor).await {
                Ok(end) => return Ok(end),
                Err(error) => {
                    tracing::debug!(%error, "cached committed-end attachment failed; redialing");
                }
            }
        }
        let mut cursor = guard
            .take()
            .map_or_else(EndScanCursor::default, |state| state.cursor);
        let connection = self.dialer.dial().await?;
        let end = scan_to_stable_end(connection.as_ref(), &mut cursor).await?;
        *guard = Some(SamplerState { connection, cursor });
        Ok(end)
    }
}

#[async_trait::async_trait]
impl CommittedEndSampler for LiveCommittedEndSampler {
    async fn committed_end_after_call_begins(&self) -> Result<i64, SubstrateError> {
        self.committed_end().await
    }
}

/// Advance `cursor` to the stable end observed by a fetch issued now and
/// return the highest committed visible record offset.
///
/// The first page's `last_stable_offset` is the linearization point: it is
/// read by a fetch that started after the caller began, so every record
/// whose commit was acknowledged before the call sits below it. The scan
/// then walks only `[cursor.next_fetch, stable_end)` — offsets below the
/// cursor are immutable history already folded into `cursor.last_visible`
/// by earlier scans. Records at or above the stable end may still be
/// undecided and are never counted, and the cursor never crosses the stable
/// end, so a later abort cannot poison the cached value.
async fn scan_to_stable_end(
    connection: &dyn CommittedEndConnection,
    cursor: &mut EndScanCursor,
) -> Result<i64, SubstrateError> {
    let mut page = connection.fetch_page(cursor.next_fetch).await?;
    let stable_end = page.last_stable_offset;
    // Each continuing iteration advances `next_fetch` strictly toward
    // `stable_end`, so the scan cannot legitimately take more iterations than
    // the offset span. Bound it explicitly: a broker whose pages never let the
    // scan reach the stable end must surface an error rather than spin the
    // barrier — and thus every read waiting on it — forever.
    let mut remaining_iterations = u64::try_from(stable_end.saturating_sub(cursor.next_fetch))
        .unwrap_or(u64::MAX)
        .saturating_add(2);
    loop {
        if let Some(record) = page
            .records
            .iter()
            .rev()
            .find(|record| record.offset < stable_end)
        {
            cursor.last_visible = cursor.last_visible.max(record.offset);
        }
        let progress = page.next_offset.min(stable_end);
        if progress <= cursor.next_fetch || progress >= stable_end {
            cursor.next_fetch = cursor.next_fetch.max(progress);
            break;
        }
        remaining_iterations = remaining_iterations.checked_sub(1).ok_or_else(|| {
            SubstrateError::Unavailable(
                "range-0 committed-end scan did not reach the stable end".to_owned(),
            )
        })?;
        cursor.next_fetch = progress;
        page = connection.fetch_page(cursor.next_fetch).await?;
    }
    Ok(cursor.last_visible)
}

#[async_trait::async_trait]
impl Range0EndSampler for BrokerRange0EndSampler {
    async fn sample_end_after_call_begins(&self) -> Result<i64, crabka_gres_ranges::BarrierError> {
        self.0
            .committed_end_after_call_begins()
            .await
            .map_err(|error| crabka_gres_ranges::BarrierError::Sample(error.to_string()))
    }
}

/// A range-0 replica that can only restore and apply committed WAL records.
#[derive(Clone)]
pub struct ReadOnlyRange0Follower {
    tail: Range0Tail,
}

impl ReadOnlyRange0Follower {
    /// Restore a checkpoint when supplied, then replay all committed records after it.
    ///
    /// This API deliberately accepts a reader rather than a producer: follower construction
    /// cannot initialize transactions, fence a writer, or append a recovery barrier.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn bootstrap(
        config: &LiveRecoveryConfig,
        store: Arc<dyn RestoreKv>,
        reader: &dyn CommittedWalReader,
        checkpoints: Option<&dyn CheckpointStore>,
    ) -> Result<Self, SubstrateError> {
        let log_start = reader.log_start_offset().await?;
        let restored = match checkpoints {
            Some(checkpoints) => {
                restore_latest(
                    checkpoints,
                    &config.checkpoint_namespace(),
                    store.as_ref(),
                    0,
                    log_start,
                )
                .await?
            }
            None => None,
        };
        if restored.is_none() && log_start.is_some_and(|offset| offset > 0) {
            return Err(SubstrateError::Checkpoint(format!(
                "no valid checkpoint covers retained WAL starting at {}",
                log_start.expect("checked above")
            )));
        }
        let applied_offset = restored.map_or(-1, |checkpoint| checkpoint.covered_offset);
        let kv: Arc<dyn Kv> = store;
        let follower = Self {
            tail: Range0Tail::from_checkpoint(kv, applied_offset),
        };
        let start = applied_offset.checked_add(1).ok_or_else(|| {
            SubstrateError::Unavailable("range-0 follower replay offset overflowed".into())
        })?;
        for item in &reader.committed_from(start).await? {
            follower.apply_committed(item)?;
        }
        Ok(follower)
    }

    /// Apply one record obtained from a `READ_COMMITTED` continuous tail.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn apply_committed(&self, item: &ReplayItem) -> Result<(), SubstrateError> {
        let frame = WalFrame::decode(&item.bytes)?;
        self.tail
            .apply_committed(&Range0Frame::new(item.offset, frame.ops))
            .map_err(|error| {
                SubstrateError::Unavailable(format!("range-0 follower apply: {error}"))
            })
    }

    /// Return the local tail used to build a range-0 barrier.
    #[must_use]
    pub fn tail(&self) -> Range0Tail {
        self.tail.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;
    use crabka_gres_ranges::{Range0Barrier, RangeId, TenantName};
    use crabka_pgexec::Linearizer as _;
    use crabka_pgkv::{Kv, MemKv, WriteOp};

    use super::*;
    use crate::{
        InMemoryWalLog, TransactionalWalWriter, WriterGeneration,
        checkpoint::{
            CheckpointSnapshot, DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore, write_checkpoint,
        },
    };

    #[tokio::test]
    async fn bootstrap_restores_checkpoint_and_applies_only_committed_post_checkpoint_records() {
        let log = InMemoryWalLog::shared();
        let checkpoint_frame = WalFrame {
            journal_seq: 0,
            ops: vec![WriteOp::Put {
                key: b"catalog/checkpoint".to_vec(),
                value: b"covered".to_vec(),
            }],
        };
        log.commit_group(crate::GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![checkpoint_frame],
        })
        .await
        .expect("commit checkpoint-covered frame");

        let checkpoints = InMemoryCheckpointStore::shared();
        let checkpoint_store = MemKv::default();
        checkpoint_store
            .put(b"catalog/checkpoint".to_vec(), b"covered".to_vec())
            .expect("seed checkpoint state");
        write_checkpoint(
            checkpoints.as_ref(),
            "replica/r0",
            &checkpoint_store,
            CheckpointSnapshot {
                covered_offset: 0,
                journal_seq: 1,
                producer_epoch: 0,
                wal_generation: 0,
                garbage_horizon_xid: 0,
            },
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("write checkpoint");

        let committed = WalFrame {
            journal_seq: 1,
            ops: vec![WriteOp::Put {
                key: b"catalog/committed-after-checkpoint".to_vec(),
                value: b"visible".to_vec(),
            }],
        };
        log.commit_group(crate::GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![committed],
        })
        .await
        .expect("commit post-checkpoint frame");
        let uncommitted = WalFrame {
            journal_seq: 2,
            ops: vec![WriteOp::Put {
                key: b"catalog/aborted".to_vec(),
                value: b"hidden".to_vec(),
            }],
        };
        log.append_unacked(WriterGeneration(0), &[uncommitted])
            .await
            .expect("append aborted frame");
        let store: Arc<dyn RestoreKv> = Arc::new(MemKv::default());
        let config = LiveRecoveryConfig::new(
            "unused",
            TenantName::parse("replica").expect("tenant"),
            RangeId::COORDINATOR,
            None,
        );

        let follower = ReadOnlyRange0Follower::bootstrap(
            &config,
            store.clone(),
            log.as_ref(),
            Some(checkpoints.as_ref()),
        )
        .await
        .expect("bootstrap follower");

        let kv: Arc<dyn Kv> = store;
        assert!(kv.get(b"catalog/checkpoint").expect("checkpoint") == Some(b"covered".to_vec()));
        assert!(
            kv.get(b"catalog/committed-after-checkpoint")
                .expect("committed")
                == Some(b"visible".to_vec())
        );
        assert!(kv.get(b"catalog/aborted").expect("aborted").is_none());
        assert!(follower.tail().applied_offset() == 1);
        assert!(log.current_generation().await == WriterGeneration(0));
    }

    // ── persistent committed-end sampler ─────────────────────────────────────

    /// Broker-side WAL truth: committed visible record offsets, the retained
    /// log start, and the last stable offset.
    #[derive(Default)]
    struct FakeWal {
        log_start: i64,
        stable_end: i64,
        records: Vec<i64>,
    }

    /// Counting broker fixture: every dial and every fetch is recorded, and
    /// failures can be injected per dial or per fetch.
    #[derive(Default)]
    struct FakeBroker {
        wal: StdMutex<FakeWal>,
        dials: AtomicUsize,
        dial_failures: AtomicUsize,
        fetch_failures: AtomicUsize,
        fetch_offsets: StdMutex<Vec<i64>>,
    }

    impl FakeBroker {
        fn shared(log_start: i64, stable_end: i64, records: Vec<i64>) -> Arc<Self> {
            let broker = Self::default();
            *broker.wal.lock().expect("wal lock") = FakeWal {
                log_start,
                stable_end,
                records,
            };
            Arc::new(broker)
        }

        fn commit(&self, offset: i64, stable_end: i64) {
            let mut wal = self.wal.lock().expect("wal lock");
            wal.records.push(offset);
            wal.stable_end = stable_end;
        }

        fn set_stable_end(&self, stable_end: i64) {
            self.wal.lock().expect("wal lock").stable_end = stable_end;
        }

        fn fetch_offsets(&self) -> Vec<i64> {
            self.fetch_offsets.lock().expect("fetch offsets").clone()
        }

        fn sampler(self: &Arc<Self>) -> LiveCommittedEndSampler {
            LiveCommittedEndSampler::with_dialer(Box::new(FakeDialer(Arc::clone(self))))
        }
    }

    struct FakeDialer(Arc<FakeBroker>);

    #[async_trait::async_trait]
    impl CommittedEndDialer for FakeDialer {
        async fn dial(&self) -> Result<Box<dyn CommittedEndConnection>, SubstrateError> {
            if take_failure(&self.0.dial_failures) {
                return Err(SubstrateError::Unavailable("injected dial failure".into()));
            }
            self.0.dials.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeConnection(Arc::clone(&self.0))))
        }
    }

    struct FakeConnection(Arc<FakeBroker>);

    #[async_trait::async_trait]
    impl CommittedEndConnection for FakeConnection {
        async fn fetch_page(
            &self,
            fetch_offset: i64,
        ) -> Result<FetchedWalPartition, SubstrateError> {
            self.0
                .fetch_offsets
                .lock()
                .expect("fetch offsets")
                .push(fetch_offset);
            if take_failure(&self.0.fetch_failures) {
                return Err(SubstrateError::Unavailable("injected fetch failure".into()));
            }
            let wal = self.0.wal.lock().expect("wal lock");
            if fetch_offset < wal.log_start {
                // The real decode path maps OFFSET_OUT_OF_RANGE to an empty
                // page whose next offset restarts at the retained log start.
                return Ok(FetchedWalPartition {
                    log_start_offset: wal.log_start,
                    high_watermark: wal.stable_end,
                    last_stable_offset: wal.stable_end,
                    decoded_batches: 0,
                    next_offset: wal.log_start,
                    records: Vec::new(),
                });
            }
            let records: Vec<ReplayItem> = wal
                .records
                .iter()
                .filter(|offset| **offset >= fetch_offset)
                .map(|offset| ReplayItem {
                    offset: *offset,
                    bytes: Vec::new(),
                })
                .collect();
            let next_offset = records
                .last()
                .map_or(wal.log_start, |record| record.offset + 1)
                .max(wal.log_start);
            Ok(FetchedWalPartition {
                log_start_offset: wal.log_start,
                high_watermark: wal.stable_end,
                last_stable_offset: wal.stable_end,
                decoded_batches: usize::from(!records.is_empty()),
                next_offset,
                records,
            })
        }
    }

    fn take_failure(budget: &AtomicUsize) -> bool {
        budget
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    #[tokio::test]
    async fn consecutive_samples_reuse_one_attachment_and_scan_incrementally() {
        let broker = FakeBroker::shared(0, 3, vec![0, 1, 2]);
        let sampler = broker.sampler();

        assert!(sampler.committed_end().await.expect("first sample") == 2);
        assert!(sampler.committed_end().await.expect("idle sample") == 2);
        broker.commit(3, 4);
        assert!(sampler.committed_end().await.expect("advanced sample") == 3);

        // Three samples, ONE dial: the attachment persists across calls.
        assert!(broker.dials.load(Ordering::SeqCst) == 1);
        // Every call still issues a fresh fetch (its linearization point),
        // and each resumes from the cursor rather than re-scanning history.
        assert!(broker.fetch_offsets() == vec![0, 3, 3]);
    }

    #[tokio::test]
    async fn cached_attachment_failure_falls_back_to_a_fresh_dial_within_the_call() {
        let broker = FakeBroker::shared(0, 2, vec![0, 1]);
        let sampler = broker.sampler();
        assert!(sampler.committed_end().await.expect("prime the cache") == 1);

        broker.fetch_failures.store(1, Ordering::SeqCst);
        broker.commit(2, 3);

        // The failed cached fetch is replaced by one fresh dial in the same
        // call, preserving the scan cursor (both attempts fetch offset 2).
        assert!(sampler.committed_end().await.expect("redialed sample") == 2);
        assert!(broker.dials.load(Ordering::SeqCst) == 2);
        assert!(broker.fetch_offsets() == vec![0, 2, 2]);
    }

    #[tokio::test]
    async fn fresh_dial_failure_surfaces_and_does_not_poison_later_samples() {
        let broker = FakeBroker::shared(0, 1, vec![0]);
        broker.dial_failures.store(1, Ordering::SeqCst);
        let sampler = broker.sampler();

        assert!(sampler.committed_end().await.is_err());
        assert!(sampler.committed_end().await.expect("recovered sample") == 0);
        assert!(broker.dials.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn scan_jumps_over_pruned_history() {
        let broker = FakeBroker::shared(5, 7, vec![5, 6]);
        let sampler = broker.sampler();

        assert!(sampler.committed_end().await.expect("pruned sample") == 6);
        assert!(broker.fetch_offsets() == vec![0, 5]);
    }

    #[tokio::test]
    async fn records_at_or_beyond_the_stable_end_are_not_sampled() {
        // Offset 2 exists but sits at the stable end: its transaction is
        // still undecided, so the sample must stop below it — otherwise the
        // barrier could wait on (and a cached cursor could adopt) a record
        // that later aborts.
        let broker = FakeBroker::shared(0, 2, vec![0, 1, 2]);
        let sampler = broker.sampler();

        assert!(sampler.committed_end().await.expect("undecided excluded") == 1);

        broker.set_stable_end(3);
        assert!(sampler.committed_end().await.expect("decided included") == 2);
    }

    #[tokio::test]
    async fn empty_topic_samples_negative_one() {
        let broker = FakeBroker::shared(0, 0, Vec::new());
        let sampler = broker.sampler();

        assert!(sampler.committed_end().await.expect("empty sample") == -1);
        assert!(sampler.committed_end().await.expect("still empty") == -1);
        assert!(broker.dials.load(Ordering::SeqCst) == 1);
    }

    /// End-to-end over the barrier seam: the range-0 read barrier built on
    /// the persistent sampler still blocks reads until the local tail applies
    /// the sampled committed end, and N barrier calls share one dial.
    #[tokio::test]
    async fn barrier_blocks_until_tail_catches_up_and_reuses_the_attachment() {
        let broker = FakeBroker::shared(0, 3, vec![0, 1, 2]);
        let store = Arc::new(MemKv::default());
        let tail = Range0Tail::new(store.clone());
        let barrier = Range0Barrier::with_timeout(
            tail.clone(),
            Arc::new(BrokerRange0EndSampler(Arc::new(broker.sampler()))),
            std::time::Duration::from_secs(5),
        );

        let waiter = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.ensure_readable().await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !waiter.is_finished(),
            "tail has not applied the sampled end"
        );

        for offset in 0..=2 {
            tail.apply_committed(&Range0Frame::new(
                offset,
                vec![WriteOp::Put {
                    key: b"catalog-row".to_vec(),
                    value: offset.to_string().into_bytes(),
                }],
            ))
            .expect("apply committed frame");
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), waiter)
            .await
            .expect("barrier completes once caught up")
            .expect("join")
            .expect("readable");
        assert!(store.get(b"catalog-row").expect("get") == Some(b"2".to_vec()));

        for _ in 0..8 {
            barrier.ensure_readable().await.expect("caught-up barrier");
        }
        assert!(broker.dials.load(Ordering::SeqCst) == 1);
    }
}
