//! Transactional WAL writer primitives and pgexec adapters.

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use crabka_client_producer::{OwnedTransaction, Producer, ProducerError, ProducerRecord};
use crabka_gres_ranges::tso::{
    EpochHeartbeat, HeartbeatVerdict, MAX_TS_KEY, TsoError, TsoHorizonCommitter, TsoTimestamp,
};
use crabka_pgexec::{Committer, ExecError, Linearizer};
use crabka_pgkv::{Kv, KvSnapshot, SnapshotKv, WriteOp};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    apply::apply_frame,
    checkpoint::{CheckpointSnapshot, CheckpointStats},
    error::SubstrateError,
    frame::WalFrame,
};

/// Default upper bound for an encoded `GRW1` frame.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1_048_576;

/// A compute-writer generation fenced by the WAL substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WriterGeneration(pub u64);

/// Durable append acknowledgement for one committed frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalAppendAck {
    /// Offset assigned to the committed WAL record.
    pub offset: i64,
    /// Journal sequence carried by the frame.
    pub journal_seq: u64,
}

/// Shared source of exact snapshot metadata for checkpoint requests.
pub struct CheckpointSnapshotSource {
    covered_offset: std::sync::atomic::AtomicI64,
    journal_seq: std::sync::atomic::AtomicU64,
    wal_generation: std::sync::atomic::AtomicU64,
    producer_epoch: std::sync::atomic::AtomicI16,
    group_gate: Arc<Semaphore>,
    garbage_horizon: Mutex<Option<GarbageHorizonProvider>>,
    fence_lease: Mutex<Option<(Arc<dyn FenceLease>, WriterGeneration)>>,
}

/// Runtime callback that computes the current safe checkpoint garbage horizon.
pub type GarbageHorizonProvider = Arc<dyn Fn() -> Result<u64, SubstrateError> + Send + Sync>;

impl std::fmt::Debug for CheckpointSnapshotSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointSnapshotSource")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl CheckpointSnapshotSource {
    /// Create a source from recovery state before new writes are accepted.
    #[must_use]
    pub fn new(covered_offset: i64, journal_seq: u64, generation: WriterGeneration) -> Self {
        Self {
            covered_offset: std::sync::atomic::AtomicI64::new(covered_offset),
            journal_seq: std::sync::atomic::AtomicU64::new(journal_seq),
            wal_generation: std::sync::atomic::AtomicU64::new(generation.0),
            producer_epoch: std::sync::atomic::AtomicI16::new(producer_epoch(generation)),
            group_gate: Arc::new(Semaphore::new(1)),
            garbage_horizon: Mutex::new(None),
            fence_lease: Mutex::new(None),
        }
    }

    /// Atomically capture WAL metadata and the matching KV snapshot between commit groups.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer gate is closed or the KV snapshot cannot be opened.
    pub async fn capture(
        &self,
        kv: &dyn SnapshotKv,
    ) -> Result<(CheckpointSnapshot, Box<dyn KvSnapshot>), SubstrateError> {
        let _permit = Arc::clone(&self.group_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("checkpoint group gate closed".into()))?;
        let mut metadata = self.snapshot();
        metadata.garbage_horizon_xid = self.garbage_horizon_xid()?;
        let snapshot = kv.snapshot()?;
        Ok((metadata, snapshot))
    }

    /// Capture a checkpoint snapshot from the latest committed WAL acknowledgement.
    #[must_use]
    pub fn snapshot(&self) -> CheckpointSnapshot {
        CheckpointSnapshot {
            covered_offset: self.covered_offset.load(Ordering::SeqCst),
            journal_seq: self.journal_seq.load(Ordering::SeqCst),
            producer_epoch: self.producer_epoch.load(Ordering::SeqCst),
            wal_generation: self.wal_generation.load(Ordering::SeqCst),
            garbage_horizon_xid: self.garbage_horizon_xid().unwrap_or(0),
        }
    }

    /// Install the engine's active-snapshot/recovery-watermark horizon callback.
    pub fn set_garbage_horizon_provider(&self, provider: GarbageHorizonProvider) {
        *self
            .garbage_horizon
            .lock()
            .expect("garbage horizon provider") = Some(provider);
    }

    /// Install the writer lease checked again after upload and before WAL truncation.
    pub fn set_fence_lease(&self, lease: Arc<dyn FenceLease>, generation: WriterGeneration) {
        *self.fence_lease.lock().expect("checkpoint fence lease") = Some((lease, generation));
    }

    pub(crate) async fn assert_current(&self) -> Result<(), SubstrateError> {
        let guard = self
            .fence_lease
            .lock()
            .expect("checkpoint fence lease")
            .clone();
        match guard {
            Some((lease, generation)) => lease.assert_current(generation).await,
            None => Ok(()),
        }
    }

    fn garbage_horizon_xid(&self) -> Result<u64, SubstrateError> {
        self.garbage_horizon
            .lock()
            .expect("garbage horizon provider")
            .as_ref()
            .map_or(Ok(0), |provider| provider())
    }

    pub(crate) fn record_commit(&self, ack: WalAppendAck) {
        self.covered_offset.store(ack.offset, Ordering::SeqCst);
        self.journal_seq
            .store(ack.journal_seq.saturating_add(1), Ordering::SeqCst);
    }

    pub(crate) fn record_recovery(
        &self,
        generation: WriterGeneration,
        barrier_offset: i64,
        next_journal_seq: u64,
    ) {
        self.covered_offset.store(barrier_offset, Ordering::SeqCst);
        self.journal_seq.store(next_journal_seq, Ordering::SeqCst);
        self.wal_generation.store(generation.0, Ordering::SeqCst);
        self.producer_epoch
            .store(producer_epoch(generation), Ordering::SeqCst);
    }
}

/// A group-commit request: one pgexec batch split into one or more frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitRequest {
    /// Writer generation expected by the broker-side fence.
    pub generation: WriterGeneration,
    /// Frames to append atomically in one transaction.
    pub frames: Vec<WalFrame>,
}

/// The durable acknowledgement for a group-commit request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCommitAck {
    /// One ack per frame, in request order.
    pub frames: Vec<WalAppendAck>,
}

/// Transactional append seam used by the substrate committer.
#[async_trait::async_trait]
pub trait TransactionalWalWriter: Send + Sync {
    /// Append every frame in `request` atomically and return only after commit.
    async fn commit_group(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError>;
}

/// An exact committed WAL boundary established while its range writer is paused.
///
/// Dropping this value resumes the writer. Call [`Self::resume`] when the
/// resume point should be explicit instead.
pub struct PausedWalWriter {
    pause: PauseReservation,
    permit: Option<OwnedSemaphorePermit>,
    /// Inclusive offset of the durable barrier frame.
    pub barrier_offset: i64,
}

impl PausedWalWriter {
    /// Resume commits accepted by this writer.
    pub fn resume(mut self) {
        self.pause.release();
        let _ = self.permit.take();
    }
}

impl Drop for PausedWalWriter {
    fn drop(&mut self) {
        self.pause.release();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterPauseState {
    Idle,
    Pausing,
    Paused,
}

struct PauseReservation {
    state: Arc<Mutex<WriterPauseState>>,
    active: bool,
}

impl PauseReservation {
    fn reserve(state: Arc<Mutex<WriterPauseState>>) -> Result<Self, SubstrateError> {
        let mut current = state.lock().expect("writer pause state lock poisoned");
        if *current != WriterPauseState::Idle {
            return Err(SubstrateError::AlreadyPaused);
        }
        *current = WriterPauseState::Pausing;
        drop(current);
        Ok(Self {
            state,
            active: true,
        })
    }

    fn mark_paused(&self) {
        let mut current = self.state.lock().expect("writer pause state lock poisoned");
        debug_assert_eq!(*current, WriterPauseState::Pausing);
        *current = WriterPauseState::Paused;
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        *self.state.lock().expect("writer pause state lock poisoned") = WriterPauseState::Idle;
        self.active = false;
    }
}

impl Drop for PauseReservation {
    fn drop(&mut self) {
        self.release();
    }
}

/// Adapter from the substrate WAL seam to the transactional producer client.
pub struct ProducerWalWriter {
    producer: Arc<Producer>,
    topic: String,
    fenced: AtomicBool,
    commit_gate: Arc<Semaphore>,
    pause_state: Arc<Mutex<WriterPauseState>>,
    indeterminate_handler: Arc<dyn Fn(&ProducerError) + Send + Sync>,
    fault_injector: Option<Arc<dyn WalWriterFaultInjector>>,
}

/// Deterministic fault points in the production producer-backed state machine.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalWriterFaultStage {
    /// Before the first record is sent, while a broker transaction is open.
    BeforeFirstSend,
    /// After every produce acknowledgement and before `EndTxn(commit)`.
    AfterSendAcks,
    /// In the delivery-result branch, before awaiting the first send result.
    PendingSendResult,
    /// Immediately before aborting a transaction after a send failure.
    BeforeAbort,
    /// After a successful broker commit but before acknowledging its caller.
    AfterCommit,
}

/// Test-only-style seam used by deterministic live fault gates.
#[doc(hidden)]
pub trait WalWriterFaultInjector: Send + Sync {
    /// Return an injected producer failure for this exact stage.
    fn inject(&self, stage: WalWriterFaultStage) -> Option<ProducerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitFailure {
    Rejected,
    RejectedNeedsAbort,
    Indeterminate,
}

fn classify_commit_failure(error: &ProducerError) -> CommitFailure {
    match error {
        ProducerError::ConcurrentTransactions => CommitFailure::RejectedNeedsAbort,
        ProducerError::FencedProducer | ProducerError::TransactionAborted => {
            CommitFailure::Rejected
        }
        _ => CommitFailure::Indeterminate,
    }
}

impl ProducerWalWriter {
    /// Build a transactional producer-backed WAL writer.
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: String) -> Self {
        Self {
            producer,
            topic,
            fenced: AtomicBool::new(false),
            commit_gate: Arc::new(Semaphore::new(1)),
            pause_state: Arc::new(Mutex::new(WriterPauseState::Idle)),
            indeterminate_handler: Arc::new(|error| {
                tracing::error!(%error, "indeterminate WAL EndTxn outcome; terminating compute");
                std::process::abort();
            }),
            fault_injector: None,
        }
    }

    /// Override the fatal indeterminate-outcome action.
    ///
    /// Production uses process abort so no SQL client can receive a false
    /// failure acknowledgement. Tests install a notifier and assert that the
    /// commit future never resolves.
    #[doc(hidden)]
    #[must_use]
    pub fn with_indeterminate_handler(
        mut self,
        handler: Arc<dyn Fn(&ProducerError) + Send + Sync>,
    ) -> Self {
        self.indeterminate_handler = handler;
        self
    }

    /// Install deterministic faults for live acceptance tests.
    #[doc(hidden)]
    #[must_use]
    pub fn with_fault_injector(mut self, injector: Arc<dyn WalWriterFaultInjector>) -> Self {
        self.fault_injector = Some(injector);
        self
    }

    fn mark_fenced(&self) {
        self.fenced.store(true, Ordering::SeqCst);
    }

    /// Pause new commits, wait for every already accepted commit, then append
    /// and commit an exact empty WAL barrier.
    ///
    /// The returned guard owns the sole commit permit. Therefore every commit
    /// acknowledged before this method returns precedes `barrier_offset`, and
    /// no later commit can be acknowledged until the guard is resumed or
    /// dropped. The permit is a semaphore permit, not a mutex guard; no mutex
    /// is held across broker awaits.
    pub async fn pause_and_barrier(
        &self,
        generation: WriterGeneration,
    ) -> Result<PausedWalWriter, SubstrateError> {
        pause_and_commit_barrier(
            Arc::clone(&self.pause_state),
            Arc::clone(&self.commit_gate),
            || async {
                let ack = self
                    .commit_group_while_permitted(GroupCommitRequest {
                        generation,
                        frames: vec![WalFrame {
                            journal_seq: crate::frame::BARRIER_SEQ,
                            ops: Vec::new(),
                        }],
                    })
                    .await?;
                ack.frames.first().map(|ack| ack.offset).ok_or_else(|| {
                    SubstrateError::Unavailable("pause barrier did not produce an ack".into())
                })
            },
        )
        .await
    }

    async fn commit_group_while_permitted(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        if self.fenced.load(Ordering::SeqCst) {
            return Err(SubstrateError::Fenced);
        }

        let transaction = self
            .producer
            .clone()
            .begin_transaction_owned()
            .await
            .map_err(|error| self.map_producer_error(&error))?;
        if let Some(error) = self.inject_fault(WalWriterFaultStage::BeforeFirstSend) {
            return self.abort_after_send_error(transaction, error).await;
        }
        let mut sent = Vec::with_capacity(request.frames.len());
        for frame in &request.frames {
            let pending = self
                .producer
                .send(ProducerRecord {
                    topic: self.topic.clone(),
                    partition: Some(0),
                    key: Some(Bytes::copy_from_slice(&request.generation.0.to_be_bytes())),
                    value: Some(Bytes::from(frame.encode())),
                    ..ProducerRecord::default()
                })
                .await;
            sent.push((frame.journal_seq, pending));
        }

        let mut frames = Vec::with_capacity(sent.len());
        for (journal_seq, pending) in sent {
            if let Some(error) = self.inject_fault(WalWriterFaultStage::PendingSendResult) {
                return self.abort_after_send_error(transaction, error).await;
            }
            let metadata = match pending.await {
                Ok(Ok(metadata)) => metadata,
                Ok(Err(error)) => return self.abort_after_send_error(transaction, error).await,
                Err(error) => {
                    return self
                        .abort_with_error(
                            transaction,
                            SubstrateError::Unavailable(format!("producer dropped ack: {error}")),
                        )
                        .await;
                }
            };
            frames.push(WalAppendAck {
                offset: metadata.offset,
                journal_seq,
            });
        }
        if let Some(error) = self.inject_fault(WalWriterFaultStage::AfterSendAcks) {
            return self.abort_after_send_error(transaction, error).await;
        }
        if let Err(error) = transaction.commit().await {
            let substrate_error = self.map_producer_error(&error.source);
            match classify_commit_failure(&error.source) {
                CommitFailure::RejectedNeedsAbort => {
                    return self
                        .abort_with_error(error.transaction, substrate_error)
                        .await;
                }
                CommitFailure::Rejected => return Err(substrate_error),
                CommitFailure::Indeterminate => {
                    (self.indeterminate_handler)(&error.source);
                    std::future::pending::<()>().await;
                    unreachable!("indeterminate WAL handler must terminate the compute");
                }
            }
        }
        if let Some(error) = self.inject_fault(WalWriterFaultStage::AfterCommit) {
            (self.indeterminate_handler)(&error);
            std::future::pending::<()>().await;
            unreachable!("indeterminate WAL handler must terminate the compute");
        }
        Ok(GroupCommitAck { frames })
    }
}

async fn pause_and_commit_barrier<F, Barrier>(
    pause_state: Arc<Mutex<WriterPauseState>>,
    commit_gate: Arc<Semaphore>,
    commit_barrier: F,
) -> Result<PausedWalWriter, SubstrateError>
where
    F: FnOnce() -> Barrier,
    Barrier: Future<Output = Result<i64, SubstrateError>>,
{
    pause_and_commit_barrier_after_reservation(pause_state, commit_gate, || {}, commit_barrier)
        .await
}

async fn pause_and_commit_barrier_after_reservation<F, Barrier, OnReserved>(
    pause_state: Arc<Mutex<WriterPauseState>>,
    commit_gate: Arc<Semaphore>,
    on_reserved: OnReserved,
    commit_barrier: F,
) -> Result<PausedWalWriter, SubstrateError>
where
    F: FnOnce() -> Barrier,
    Barrier: Future<Output = Result<i64, SubstrateError>>,
    OnReserved: FnOnce(),
{
    let pause = PauseReservation::reserve(pause_state)?;
    on_reserved();
    let permit = commit_gate
        .acquire_owned()
        .await
        .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
    let barrier_offset = commit_barrier().await?;
    pause.mark_paused();
    Ok(PausedWalWriter {
        pause,
        permit: Some(permit),
        barrier_offset,
    })
}

#[async_trait::async_trait]
impl TransactionalWalWriter for ProducerWalWriter {
    async fn commit_group(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        if self.is_pause_reserved() {
            return Err(SubstrateError::Unavailable("WAL writer is paused".into()));
        }
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.is_pause_reserved() {
            return Err(SubstrateError::Unavailable("WAL writer is paused".into()));
        }
        self.commit_group_while_permitted(request).await
    }
}

impl ProducerWalWriter {
    fn inject_fault(&self, stage: WalWriterFaultStage) -> Option<ProducerError> {
        self.fault_injector
            .as_ref()
            .and_then(|injector| injector.inject(stage))
    }

    fn is_pause_reserved(&self) -> bool {
        *self
            .pause_state
            .lock()
            .expect("writer pause state lock poisoned")
            != WriterPauseState::Idle
    }

    async fn abort_after_send_error(
        &self,
        transaction: OwnedTransaction,
        error: ProducerError,
    ) -> Result<GroupCommitAck, SubstrateError> {
        let substrate_error = self.map_producer_error(&error);
        if matches!(
            error,
            ProducerError::FencedProducer | ProducerError::TransactionAborted
        ) {
            // The broker has proved this epoch cannot commit. The returned
            // guard cannot be cleanly aborted once the producer is fenced,
            // but this is not ambiguous: no record from it can become durable.
            drop(transaction);
            return Err(substrate_error);
        }
        self.abort_with_error(transaction, substrate_error).await
    }

    async fn abort_with_error(
        &self,
        transaction: OwnedTransaction,
        substrate_error: SubstrateError,
    ) -> Result<GroupCommitAck, SubstrateError> {
        if let Some(error) = self.inject_fault(WalWriterFaultStage::BeforeAbort) {
            (self.indeterminate_handler)(&error);
            std::future::pending::<()>().await;
            unreachable!("indeterminate WAL handler must terminate the compute");
        }
        match transaction.abort().await {
            Ok(()) => Err(substrate_error),
            Err(error) => {
                (self.indeterminate_handler)(&error.source);
                std::future::pending::<()>().await;
                unreachable!("indeterminate WAL handler must terminate the compute");
            }
        }
    }

    fn map_producer_error(&self, error: &ProducerError) -> SubstrateError {
        if matches!(error, &ProducerError::FencedProducer) {
            self.mark_fenced();
            return SubstrateError::Fenced;
        }
        SubstrateError::Unavailable(error.to_string())
    }
}

#[async_trait::async_trait]
impl FenceLease for ProducerWalWriter {
    async fn assert_current(&self, generation: WriterGeneration) -> Result<(), SubstrateError> {
        if self.is_pause_reserved() {
            return Err(SubstrateError::Unavailable("WAL writer is paused".into()));
        }
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.is_pause_reserved() {
            return Err(SubstrateError::Unavailable("WAL writer is paused".into()));
        }
        if self.fenced.load(Ordering::SeqCst) {
            return Err(SubstrateError::Fenced);
        }
        self.commit_group_while_permitted(GroupCommitRequest {
            generation,
            frames: vec![WalFrame {
                journal_seq: crate::frame::BARRIER_SEQ,
                ops: Vec::new(),
            }],
        })
        .await
        .map(|_| ())
    }
}

/// Chunk a logical operation batch into monotone `GRW1` frames.
pub fn chunk_wal_batch(
    ops: Vec<WriteOp>,
    first_journal_seq: u64,
    max_frame_bytes: usize,
) -> Result<Vec<WalFrame>, SubstrateError> {
    if ops.is_empty() {
        return Ok(vec![WalFrame {
            journal_seq: first_journal_seq,
            ops,
        }]);
    }

    let mut frames = Vec::new();
    let mut current_ops = Vec::new();
    let mut current_seq = first_journal_seq;

    for op in ops {
        let single_len = WalFrame {
            journal_seq: current_seq,
            ops: vec![op.clone()],
        }
        .encoded_len();
        if single_len > max_frame_bytes {
            if !current_ops.is_empty() {
                frames.push(WalFrame {
                    journal_seq: current_seq,
                    ops: current_ops,
                });
                current_seq = current_seq.checked_add(1).ok_or_else(|| {
                    SubstrateError::Frame("journal sequence exhausted while chunking".into())
                })?;
                current_ops = Vec::new();
            }
            frames.push(WalFrame {
                journal_seq: current_seq,
                ops: vec![op],
            });
            current_seq = current_seq.checked_add(1).ok_or_else(|| {
                SubstrateError::Frame("journal sequence exhausted while chunking".into())
            })?;
            continue;
        }

        let mut candidate_ops = current_ops.clone();
        candidate_ops.push(op.clone());
        let candidate_len = WalFrame {
            journal_seq: current_seq,
            ops: candidate_ops,
        }
        .encoded_len();
        if !current_ops.is_empty() && candidate_len > max_frame_bytes {
            frames.push(WalFrame {
                journal_seq: current_seq,
                ops: current_ops,
            });
            current_seq = current_seq.checked_add(1).ok_or_else(|| {
                SubstrateError::Frame("journal sequence exhausted while chunking".into())
            })?;
            current_ops = vec![op];
            continue;
        }
        current_ops.push(op);
    }

    if !current_ops.is_empty() {
        frames.push(WalFrame {
            journal_seq: current_seq,
            ops: current_ops,
        });
    }
    Ok(frames)
}

/// pgexec committer adapter that journals, waits for commit, then applies locally.
pub struct SubstrateCommitter<W> {
    kv: Arc<dyn Kv>,
    writer: Arc<W>,
    generation: WriterGeneration,
    max_frame_bytes: usize,
    next_journal_seq: std::sync::atomic::AtomicU64,
    commit_gate: Arc<Semaphore>,
    checkpoint_stats: Option<Arc<CheckpointStats>>,
    checkpoint_snapshot_source: Option<Arc<CheckpointSnapshotSource>>,
}

/// Range-0 substrate-backed timestamp horizon.
#[derive(Clone)]
pub struct SubstrateTsoHorizon {
    store: Arc<dyn Kv>,
    committer: Arc<dyn Committer>,
    lease: Arc<dyn FenceLease>,
    generation: WriterGeneration,
    epoch: i16,
}

impl SubstrateTsoHorizon {
    /// Build a TSO horizon over the recovered range-0 store and live WAL writer seams.
    #[must_use]
    pub fn new(
        store: Arc<dyn Kv>,
        committer: Arc<dyn Committer>,
        lease: Arc<dyn FenceLease>,
        generation: WriterGeneration,
    ) -> Self {
        Self {
            store,
            committer,
            lease,
            generation,
            epoch: producer_epoch(generation),
        }
    }

    /// Load the recovered inclusive durable timestamp horizon.
    pub fn load_max_ts(&self) -> Result<u64, TsoError> {
        self.store
            .get(MAX_TS_KEY)?
            .as_deref()
            .map_or(Ok(0), decode_u64)
    }

    /// Return the writer epoch used to fence oracle instances.
    #[must_use]
    pub const fn epoch(&self) -> i16 {
        self.epoch
    }
}

#[async_trait::async_trait]
impl TsoHorizonCommitter for SubstrateTsoHorizon {
    async fn persist_max_ts_for_epoch(
        &self,
        epoch: i16,
        max_ts: TsoTimestamp,
    ) -> Result<(), TsoError> {
        if epoch != self.epoch {
            return Err(TsoError::FencedEpoch { epoch });
        }
        self.committer
            .commit(vec![WriteOp::Put {
                key: MAX_TS_KEY.to_vec(),
                value: max_ts.get().to_be_bytes().to_vec(),
            }])
            .await
            .map_err(|error| map_exec_error_to_tso(error, epoch))
    }
}

#[async_trait::async_trait]
impl EpochHeartbeat for SubstrateTsoHorizon {
    async fn heartbeat(&self, epoch: i16) -> Result<HeartbeatVerdict, TsoError> {
        if epoch != self.epoch {
            return Ok(HeartbeatVerdict::Fenced);
        }
        self.lease
            .assert_current(self.generation)
            .await
            .map(|()| HeartbeatVerdict::Live)
            .map_err(|error| map_substrate_error_to_tso(&error, epoch))
    }
}

fn decode_u64(bytes: &[u8]) -> Result<u64, TsoError> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        TsoError::CorruptHorizon(format!("expected 8 bytes, found {}", bytes.len()))
    })?;
    Ok(u64::from_be_bytes(array))
}

fn map_exec_error_to_tso(error: ExecError, epoch: i16) -> TsoError {
    if matches!(error, ExecError::NotLeader) {
        return TsoError::FencedEpoch { epoch };
    }
    TsoError::Rpc(error.into_pg().message)
}

fn map_substrate_error_to_tso(error: &SubstrateError, epoch: i16) -> TsoError {
    if matches!(error, SubstrateError::Fenced) {
        return TsoError::FencedEpoch { epoch };
    }
    TsoError::Rpc(error.to_string())
}

impl<W> SubstrateCommitter<W> {
    /// Build a substrate-backed pgexec committer.
    #[must_use]
    pub fn new(kv: Arc<dyn Kv>, writer: Arc<W>, generation: WriterGeneration, next: u64) -> Self {
        Self {
            kv,
            writer,
            generation,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            next_journal_seq: std::sync::atomic::AtomicU64::new(next),
            commit_gate: Arc::new(Semaphore::new(1)),
            checkpoint_stats: None,
            checkpoint_snapshot_source: None,
        }
    }

    /// Override the maximum encoded frame size.
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Record committed frame and byte counts into checkpointer stats.
    #[must_use]
    pub fn with_checkpoint_stats(mut self, checkpoint_stats: Arc<CheckpointStats>) -> Self {
        self.checkpoint_stats = Some(checkpoint_stats);
        self
    }

    /// Publish exact committed offsets to checkpoint snapshot callers.
    #[must_use]
    pub fn with_checkpoint_snapshot_source(
        mut self,
        source: Arc<CheckpointSnapshotSource>,
    ) -> Self {
        self.commit_gate = Arc::clone(&source.group_gate);
        self.checkpoint_snapshot_source = Some(source);
        self
    }
}

#[async_trait::async_trait]
impl<W> Committer for SubstrateCommitter<W>
where
    W: TransactionalWalWriter + 'static,
{
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| ExecError::Unavailable)?;
        let next = self.next_journal_seq.load(Ordering::SeqCst);
        let frames = chunk_wal_batch(ops, next, self.max_frame_bytes)?;
        let ack = self
            .writer
            .commit_group(GroupCommitRequest {
                generation: self.generation,
                frames: frames.clone(),
            })
            .await?;
        for frame in &frames {
            apply_frame(self.kv.as_ref(), &frame.ops)?;
        }
        if let Some(stats) = &self.checkpoint_stats {
            let frame_count = u64::try_from(frames.len()).map_err(|_| ExecError::Unavailable)?;
            let byte_count = frames.iter().try_fold(0_u64, |total, frame| {
                let len = u64::try_from(frame.encoded_len()).map_err(|_| ExecError::Unavailable)?;
                total.checked_add(len).ok_or(ExecError::Unavailable)
            })?;
            stats.record_committed(frame_count, byte_count);
        }
        if let Some(source) = &self.checkpoint_snapshot_source
            && let Some(last_ack) = ack.frames.last().copied()
        {
            source.record_commit(last_ack);
        }
        let next = next
            .checked_add(u64::try_from(frames.len()).map_err(|_| ExecError::Unavailable)?)
            .ok_or(ExecError::Unavailable)?;
        self.next_journal_seq.store(next, Ordering::SeqCst);
        Ok(())
    }
}

fn producer_epoch(generation: WriterGeneration) -> i16 {
    i16::try_from(generation.0).unwrap_or(i16::MAX)
}

/// Fence-check seam for linearizable reads.
#[async_trait::async_trait]
pub trait FenceLease: Send + Sync {
    /// Fail when `generation` no longer owns the WAL writer lease.
    async fn assert_current(&self, generation: WriterGeneration) -> Result<(), SubstrateError>;
}

/// pgexec linearizer adapter backed by a substrate writer fence.
pub struct SubstrateLinearizer<L> {
    lease: Arc<L>,
    generation: WriterGeneration,
}

impl<L> SubstrateLinearizer<L> {
    /// Build a substrate-backed pgexec linearizer.
    #[must_use]
    pub fn new(lease: Arc<L>, generation: WriterGeneration) -> Self {
        Self { lease, generation }
    }
}

#[async_trait::async_trait]
impl<L> Linearizer for SubstrateLinearizer<L>
where
    L: FenceLease + 'static,
{
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        self.lease.assert_current(self.generation).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use assert2::assert;
    use crabka_gres_ranges::tso::{GrantLease, TsoOracle};
    use crabka_pgkv::{Kv, MemKv};
    use tokio::sync::Notify;

    use super::*;
    use crate::recovery::RecoveryFencer;

    #[test]
    fn end_txn_classification_only_calls_proven_broker_rejections_definite() {
        assert!(matches!(
            classify_commit_failure(&ProducerError::FencedProducer),
            CommitFailure::Rejected
        ));
        assert!(matches!(
            classify_commit_failure(&ProducerError::ConcurrentTransactions),
            CommitFailure::RejectedNeedsAbort
        ));
        assert!(matches!(
            classify_commit_failure(&ProducerError::Server(42)),
            CommitFailure::Indeterminate
        ));
        assert!(matches!(
            classify_commit_failure(&ProducerError::FlushTimeout),
            CommitFailure::Indeterminate
        ));
        assert!(matches!(
            classify_commit_failure(&ProducerError::RecoveryRequired),
            CommitFailure::Indeterminate
        ));
    }

    #[derive(Default)]
    struct FakeWalWriter {
        current_generation: AtomicU64,
        next_offset: AtomicU64,
    }

    #[async_trait::async_trait]
    impl TransactionalWalWriter for FakeWalWriter {
        async fn commit_group(
            &self,
            request: GroupCommitRequest,
        ) -> Result<GroupCommitAck, SubstrateError> {
            if request.generation.0 != self.current_generation.load(Ordering::SeqCst) {
                return Err(SubstrateError::Fenced);
            }
            let frames = request
                .frames
                .iter()
                .map(|frame| WalAppendAck {
                    offset: i64::try_from(self.next_offset.fetch_add(1, Ordering::SeqCst))
                        .expect("offset fits i64"),
                    journal_seq: frame.journal_seq,
                })
                .collect();
            Ok(GroupCommitAck { frames })
        }
    }

    #[async_trait::async_trait]
    impl FenceLease for FakeWalWriter {
        async fn assert_current(&self, generation: WriterGeneration) -> Result<(), SubstrateError> {
            if generation.0 != self.current_generation.load(Ordering::SeqCst) {
                return Err(SubstrateError::Fenced);
            }
            Ok(())
        }
    }

    #[test]
    fn dropping_pause_reservation_reopens_the_writer_from_every_reserved_state() {
        let pause_state = Arc::new(Mutex::new(WriterPauseState::Idle));

        {
            let pause = PauseReservation::reserve(Arc::clone(&pause_state)).expect("reserve");
            assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Pausing);
            let Err(error) = PauseReservation::reserve(Arc::clone(&pause_state)) else {
                panic!("a second pause reservation must be rejected");
            };
            assert!(matches!(error, SubstrateError::AlreadyPaused));
            drop(pause);
        }

        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);

        {
            let pause = PauseReservation::reserve(Arc::clone(&pause_state)).expect("reserve");
            pause.mark_paused();
            assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Paused);
        }

        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);
    }

    #[tokio::test]
    async fn cancelling_while_waiting_for_the_commit_permit_reopens_the_writer() {
        let pause_state = Arc::new(Mutex::new(WriterPauseState::Idle));
        let commit_gate = Arc::new(Semaphore::new(1));
        let held_permit = Arc::clone(&commit_gate)
            .acquire_owned()
            .await
            .expect("hold commit permit");
        let reservation_ready = Arc::new(Notify::new());
        let wait_for_reservation = reservation_ready.notified();
        let task_pause_state = Arc::clone(&pause_state);
        let task_commit_gate = Arc::clone(&commit_gate);
        let task_reservation_ready = Arc::clone(&reservation_ready);
        let task = tokio::spawn(async move {
            pause_and_commit_barrier_after_reservation(
                task_pause_state,
                task_commit_gate,
                move || task_reservation_ready.notify_one(),
                || async { Ok(7) },
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), wait_for_reservation)
            .await
            .expect("pause reservation should be acquired before cancellation");
        task.abort();
        let Err(cancellation) = task.await else {
            panic!("task must be cancelled");
        };
        assert!(cancellation.is_cancelled());
        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);

        drop(held_permit);
        let paused = pause_and_commit_barrier(pause_state.clone(), commit_gate, || async { Ok(8) })
            .await
            .expect("retry pause");
        assert!(paused.barrier_offset == 8);
    }

    #[tokio::test]
    async fn barrier_failure_reopens_the_writer_for_retry() {
        let pause_state = Arc::new(Mutex::new(WriterPauseState::Idle));
        let commit_gate = Arc::new(Semaphore::new(1));

        let result = pause_and_commit_barrier(
            Arc::clone(&pause_state),
            Arc::clone(&commit_gate),
            || async {
                Err(SubstrateError::Unavailable(
                    "injected barrier failure".into(),
                ))
            },
        )
        .await;
        let Err(error) = result else {
            panic!("barrier must fail");
        };

        assert!(matches!(error, SubstrateError::Unavailable(_)));
        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);
        let paused = pause_and_commit_barrier(pause_state.clone(), commit_gate, || async { Ok(9) })
            .await
            .expect("retry pause");
        assert!(paused.barrier_offset == 9);
    }

    #[tokio::test]
    async fn second_pause_while_first_is_pausing_is_rejected() {
        let pause_state = Arc::new(Mutex::new(WriterPauseState::Idle));
        let commit_gate = Arc::new(Semaphore::new(1));
        let barrier_started = Arc::new(Notify::new());
        let release_barrier = Arc::new(Notify::new());
        let first_pause_state = Arc::clone(&pause_state);
        let first_commit_gate = Arc::clone(&commit_gate);
        let first_barrier_started = Arc::clone(&barrier_started);
        let first_release_barrier = Arc::clone(&release_barrier);
        let first = tokio::spawn(async move {
            pause_and_commit_barrier(first_pause_state, first_commit_gate, move || async move {
                first_barrier_started.notify_one();
                first_release_barrier.notified().await;
                Ok(10)
            })
            .await
        });

        barrier_started.notified().await;
        let result = pause_and_commit_barrier(
            Arc::clone(&pause_state),
            Arc::clone(&commit_gate),
            || async { Ok(11) },
        )
        .await;
        let Err(error) = result else {
            panic!("second pause must fail");
        };

        assert!(matches!(error, SubstrateError::AlreadyPaused));
        release_barrier.notify_one();
        let paused = first.await.expect("first pause task").expect("first pause");
        assert!(paused.barrier_offset == 10);
    }

    #[test]
    fn chunking_splits_oversized_batches_without_reordering() {
        let ops = vec![
            WriteOp::Put {
                key: b"a".to_vec(),
                value: vec![1; 10],
            },
            WriteOp::Put {
                key: b"b".to_vec(),
                value: vec![2; 10],
            },
        ];

        let frames = chunk_wal_batch(ops, 7, 36).expect("chunk");

        assert!(frames.len() == 2);
        assert!(frames[0].journal_seq == 7);
        assert!(frames[1].journal_seq == 8);
        assert!(
            frames[0].ops[0]
                == WriteOp::Put {
                    key: b"a".to_vec(),
                    value: vec![1; 10]
                }
        );
        assert!(
            frames[1].ops[0]
                == WriteOp::Put {
                    key: b"b".to_vec(),
                    value: vec![2; 10]
                }
        );
    }

    #[test]
    fn chunking_keeps_single_oversized_operation_as_one_frame() {
        let ops = vec![WriteOp::Put {
            key: b"huge".to_vec(),
            value: vec![7; 64],
        }];

        let frames = chunk_wal_batch(ops, 3, 16).expect("chunk");

        assert!(frames.len() == 1);
        assert!(frames[0].journal_seq == 3);
        assert!(frames[0].encoded_len() > 16);
    }

    #[tokio::test]
    async fn committer_applies_locally_only_after_wal_ack() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let writer = Arc::new(FakeWalWriter::default());
        let committer = SubstrateCommitter::new(kv.clone(), writer, WriterGeneration(0), 0);

        committer
            .commit(vec![WriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }])
            .await
            .expect("commit");

        assert!(kv.get(b"k").expect("get") == Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn committer_updates_checkpoint_snapshot_from_current_wal_ack() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let writer = Arc::new(FakeWalWriter::default());
        let snapshot_source = Arc::new(CheckpointSnapshotSource::new(0, 0, WriterGeneration(0)));
        let committer = SubstrateCommitter::new(kv, writer, WriterGeneration(0), 0)
            .with_checkpoint_snapshot_source(Arc::clone(&snapshot_source));

        committer
            .commit(vec![WriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }])
            .await
            .expect("commit");

        assert!(
            snapshot_source.snapshot()
                == CheckpointSnapshot {
                    covered_offset: 0,
                    journal_seq: 1,
                    producer_epoch: 0,
                    wal_generation: 0,
                    garbage_horizon_xid: 0,
                }
        );
    }

    #[tokio::test]
    async fn checkpoint_capture_is_between_acknowledged_commit_groups() {
        let kv = Arc::new(MemKv::default());
        let writer = Arc::new(FakeWalWriter::default());
        let snapshot_source = Arc::new(CheckpointSnapshotSource::new(-1, 0, WriterGeneration(0)));
        let committer = Arc::new(
            SubstrateCommitter::new(kv.clone() as Arc<dyn Kv>, writer, WriterGeneration(0), 0)
                .with_checkpoint_snapshot_source(Arc::clone(&snapshot_source)),
        );
        committer
            .commit(vec![WriteOp::Put {
                key: b"before".to_vec(),
                value: b"included".to_vec(),
            }])
            .await
            .expect("pre-request group");

        let gate = Arc::clone(&snapshot_source.group_gate)
            .acquire_owned()
            .await
            .expect("hold group boundary");
        let capture_source = Arc::clone(&snapshot_source);
        let capture_kv = Arc::clone(&kv);
        let capture =
            tokio::spawn(async move { capture_source.capture(capture_kv.as_ref()).await });
        tokio::task::yield_now().await;
        let post_committer = Arc::clone(&committer);
        let post = tokio::spawn(async move {
            post_committer
                .commit(vec![WriteOp::Put {
                    key: b"after".to_vec(),
                    value: b"excluded".to_vec(),
                }])
                .await
        });
        drop(gate);

        let (metadata, mut snapshot) = capture.await.expect("capture task").expect("capture");
        post.await.expect("post task").expect("post-request group");
        let mut pairs = Vec::new();
        while let Some(pair) = snapshot.next().expect("snapshot pair") {
            pairs.push(pair);
        }
        assert!(metadata.covered_offset == 0);
        assert!(metadata.journal_seq == 1);
        assert!(pairs == vec![(b"before".to_vec(), b"included".to_vec())]);
        assert!(kv.get(b"after").expect("live after") == Some(b"excluded".to_vec()));
    }

    #[tokio::test]
    async fn stale_writer_is_rejected_before_local_apply() {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
        let writer = Arc::new(FakeWalWriter::default());
        writer.current_generation.store(2, Ordering::SeqCst);
        let committer = SubstrateCommitter::new(kv.clone(), writer, WriterGeneration(1), 0);

        let error = committer
            .commit(vec![WriteOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }])
            .await
            .expect_err("fenced");

        assert!(matches!(error, ExecError::NotLeader));
        assert!(kv.get(b"k").expect("get").is_none());
    }

    #[tokio::test]
    async fn linearizer_rejects_fenced_generation() {
        let writer = Arc::new(FakeWalWriter::default());
        writer.current_generation.store(9, Ordering::SeqCst);
        let linearizer = SubstrateLinearizer::new(writer, WriterGeneration(8));

        let error = linearizer.ensure_readable().await.expect_err("fenced");

        assert!(matches!(error, ExecError::NotLeader));
    }

    #[tokio::test]
    async fn substrate_tso_horizon_persists_grants_and_recovers() {
        let store: Arc<dyn Kv> = Arc::new(MemKv::default());
        let log = crate::InMemoryWalLog::shared();
        let committer = Arc::new(SubstrateCommitter::new(
            Arc::clone(&store),
            Arc::clone(&log),
            WriterGeneration(0),
            0,
        ));
        let committer_trait: Arc<dyn Committer> = committer;
        let lease_trait: Arc<dyn FenceLease> = log.clone();
        let horizon = SubstrateTsoHorizon::new(
            Arc::clone(&store),
            committer_trait,
            lease_trait,
            WriterGeneration(0),
        );
        let oracle = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            0,
        )
        .expect("recover");

        let before = oracle
            .grant(NonZeroU64::new(2).expect("count"))
            .await
            .expect("grant");

        assert!(before == GrantLease::new(TsoTimestamp::FIRST, NonZeroU64::new(2).expect("count")));
        assert!(horizon.load_max_ts().expect("horizon") == 4);

        let recovered_store: Arc<dyn Kv> = Arc::new(MemKv::default());
        let (_barrier, outcome) =
            crate::recover_after_barrier(recovered_store.as_ref(), log.as_ref(), log.as_ref())
                .await
                .expect("recover wal");
        let recovered_committer = Arc::new(SubstrateCommitter::new(
            Arc::clone(&recovered_store),
            Arc::clone(&log),
            WriterGeneration(1),
            outcome.next_journal_seq,
        ));
        let recovered_horizon = SubstrateTsoHorizon::new(
            recovered_store,
            recovered_committer,
            log,
            WriterGeneration(1),
        );
        let recovered_oracle = TsoOracle::recover(
            recovered_horizon.clone(),
            recovered_horizon.clone(),
            recovered_horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            recovered_horizon.load_max_ts().expect("horizon"),
        )
        .expect("recover oracle");
        let after = recovered_oracle
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect("grant after recovery");

        assert!(before.last_ts().expect("last") < after.first_ts);
        assert!(after.first_ts.get() == 5);
    }

    #[tokio::test]
    async fn substrate_tso_horizon_rejects_stale_epoch_grants() {
        let store: Arc<dyn Kv> = Arc::new(MemKv::default());
        let log = crate::InMemoryWalLog::shared();
        let committer = Arc::new(SubstrateCommitter::new(
            Arc::clone(&store),
            Arc::clone(&log),
            WriterGeneration(0),
            0,
        ));
        let committer_trait: Arc<dyn Committer> = committer;
        let lease_trait: Arc<dyn FenceLease> = log.clone();
        let horizon = SubstrateTsoHorizon::new(
            Arc::clone(&store),
            committer_trait,
            lease_trait,
            WriterGeneration(0),
        );
        let oracle = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            0,
        )
        .expect("recover");
        oracle
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect("initial grant");

        log.fence_with_barrier().await.expect("fence");
        let heartbeat_error = oracle
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect_err("heartbeat fenced");
        let persist_error = horizon
            .persist_max_ts_for_epoch(
                horizon.epoch() + 1,
                TsoTimestamp::new(NonZeroU64::new(9).expect("ts")),
            )
            .await
            .expect_err("stale epoch");

        assert!(matches!(
            heartbeat_error,
            TsoError::FencedEpoch { epoch: 0 }
        ));
        assert!(matches!(persist_error, TsoError::FencedEpoch { epoch: 1 }));
        assert!(horizon.load_max_ts().expect("horizon") == 4);
    }

    #[tokio::test]
    async fn substrate_tso_horizon_heartbeat_checks_writer_for_cached_grants() {
        let store: Arc<dyn Kv> = Arc::new(MemKv::default());
        let writer = Arc::new(FakeWalWriter::default());
        let committer = Arc::new(SubstrateCommitter::new(
            Arc::clone(&store),
            Arc::clone(&writer),
            WriterGeneration(0),
            0,
        ));
        let committer_trait: Arc<dyn Committer> = committer;
        let lease_trait: Arc<dyn FenceLease> = writer.clone();
        let horizon = SubstrateTsoHorizon::new(
            Arc::clone(&store),
            committer_trait,
            lease_trait,
            WriterGeneration(0),
        );
        let oracle = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            0,
        )
        .expect("recover");
        oracle
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect("initial grant");
        writer.current_generation.store(1, Ordering::SeqCst);

        let error = oracle
            .grant(NonZeroU64::new(1).expect("count"))
            .await
            .expect_err("cached grant heartbeat must observe fence");

        assert!(matches!(error, TsoError::FencedEpoch { epoch: 0 }));
        assert!(horizon.load_max_ts().expect("horizon") == 4);
    }
}
