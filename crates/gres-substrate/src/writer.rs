//! Transactional WAL writer primitives and pgexec adapters.

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use crabka_client_producer::{Header, OwnedTransaction, Producer, ProducerError, ProducerRecord};
use crabka_gres_ranges::tso::{
    EpochHeartbeat, HeartbeatVerdict, MAX_TS_KEY, TsoError, TsoHorizonCommitter, TsoTimestamp,
};
use crabka_pgexec::{Committer, ExecError, Linearizer};
use crabka_pgkv::{Kv, KvSnapshot, SnapshotKv, WriteOp};
use crabka_trace_context::TraceCarrier;
use crabka_units::{ByteSize, convert::ByteSizeExt as _, mebibytes};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument as _;

use crate::{
    apply::apply_frame,
    checkpoint::{CheckpointSnapshot, CheckpointStats},
    error::SubstrateError,
    frame::WalFrame,
    telemetry,
};

/// Default upper bound for an encoded `GRW1` frame.
pub const DEFAULT_MAX_FRAME_SIZE: ByteSize = mebibytes(1);

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
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn set_garbage_horizon_provider(&self, provider: GarbageHorizonProvider) {
        *self
            .garbage_horizon
            .lock()
            .expect("garbage horizon provider") = Some(provider);
    }

    /// Install the writer lease checked again after upload and before WAL truncation.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
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

/// Write-once activation handle that assembles recovered engines without the
/// canonical producer lease.
///
/// Every engine-facing operation fails closed until [`Self::activate`] binds
/// the already-prepared live writer. Activation is an infallible pointer swap
/// after the caller has constructed ownership of `writer`.
pub struct DeferredWalWriter<W> {
    live: std::sync::RwLock<Option<Arc<W>>>,
}

impl<W> DeferredWalWriter<W> {
    /// Create an unbound writer for staged recovery.
    #[must_use]
    pub const fn staged() -> Self {
        Self {
            live: std::sync::RwLock::new(None),
        }
    }

    /// Bind the canonical writer exactly once.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error if the lock is poisoned or the handle was
    /// already activated.
    pub fn activate(&self, writer: Arc<W>) -> Result<(), SubstrateError> {
        let mut live = self
            .live
            .write()
            .map_err(|_| SubstrateError::Unavailable("deferred writer lock poisoned".into()))?;
        if live.is_some() {
            return Err(SubstrateError::Unavailable(
                "deferred writer is already activated".into(),
            ));
        }
        *live = Some(writer);
        Ok(())
    }

    /// Return whether the irreversible binding has completed.
    #[must_use]
    pub fn is_activated(&self) -> bool {
        self.live.read().is_ok_and(|live| live.is_some())
    }

    fn current(&self) -> Result<Arc<W>, SubstrateError> {
        self.live
            .read()
            .map_err(|_| SubstrateError::Unavailable("deferred writer lock poisoned".into()))?
            .clone()
            .ok_or_else(|| SubstrateError::Unavailable("deferred writer is not activated".into()))
    }
}

impl DeferredWalWriter<ProducerWalWriter> {
    /// Pause the activated producer and reject staged handles fail-closed.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn pause_and_barrier(
        &self,
        generation: WriterGeneration,
    ) -> Result<PausedWalWriter, SubstrateError> {
        self.current()?.pause_and_barrier(generation).await
    }
}

#[async_trait::async_trait]
impl<W> TransactionalWalWriter for DeferredWalWriter<W>
where
    W: TransactionalWalWriter + 'static,
{
    async fn commit_group(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        self.current()?.commit_group(request).await
    }
}

#[async_trait::async_trait]
impl<W> FenceLease for DeferredWalWriter<W>
where
    W: FenceLease + 'static,
{
    async fn assert_current(&self, generation: WriterGeneration) -> Result<(), SubstrateError> {
        self.current()?.assert_current(generation).await
    }
}

/// An exact committed WAL boundary established while its range writer is paused.
///
/// When the caller drops this value, the writer resumes. Call [`Self::resume`]
/// when the resume point should be explicit instead.
pub struct PausedWalWriter {
    pause: PauseReservation,
    permit: Option<OwnedSemaphorePermit>,
    /// Inclusive offset of the durable barrier frame.
    pub barrier_offset: i64,
}

/// Unforgeable authorization tied to one writer's currently held pause barrier.
#[derive(Clone)]
pub struct PausedWalAuthorization {
    state: Arc<Mutex<WriterPauseState>>,
    nonce: u64,
    barrier_offset: i64,
}

impl PausedWalAuthorization {
    fn matches_writer(
        &self,
        writer_state: &Arc<Mutex<WriterPauseState>>,
        expected_barrier_offset: i64,
    ) -> bool {
        self.barrier_offset == expected_barrier_offset
            && Arc::ptr_eq(&self.state, writer_state)
            && self.state.lock().is_ok_and(|state| {
                matches!(
                    *state,
                    WriterPauseState::Paused { nonce, barrier_offset }
                        if nonce == self.nonce && barrier_offset == self.barrier_offset
                )
            })
    }
}

impl PausedWalWriter {
    /// Borrow authority for the narrow activation-receipt append while this guard stays held.
    #[must_use]
    pub fn activation_authorization(&self) -> PausedWalAuthorization {
        PausedWalAuthorization {
            state: Arc::clone(&self.pause.state),
            nonce: self.pause.nonce,
            barrier_offset: self.barrier_offset,
        }
    }
    /// Resume commits accepted by this writer.
    pub fn resume(mut self) {
        self.pause.release();
        let _ = self.permit.take();
    }

    /// Permanently retire this writer without reopening its commit gate.
    ///
    /// The pause nonce remains fenced, so stale handles cannot resume commits after ownership
    /// moved to a successor generation.
    pub fn retire(mut self) {
        self.pause.active = false;
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
    Pausing { nonce: u64 },
    Paused { nonce: u64, barrier_offset: i64 },
}

static NEXT_PAUSE_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct PauseReservation {
    state: Arc<Mutex<WriterPauseState>>,
    nonce: u64,
    active: bool,
}

impl PauseReservation {
    fn reserve(state: Arc<Mutex<WriterPauseState>>) -> Result<Self, SubstrateError> {
        let mut current = state.lock().expect("writer pause state lock poisoned");
        if *current != WriterPauseState::Idle {
            return Err(SubstrateError::AlreadyPaused);
        }
        let nonce = NEXT_PAUSE_NONCE.fetch_add(1, Ordering::Relaxed);
        *current = WriterPauseState::Pausing { nonce };
        drop(current);
        Ok(Self {
            state,
            nonce,
            active: true,
        })
    }

    fn mark_paused(&self, barrier_offset: i64) {
        let mut current = self.state.lock().expect("writer pause state lock poisoned");
        debug_assert_eq!(*current, WriterPauseState::Pausing { nonce: self.nonce });
        *current = WriterPauseState::Paused {
            nonce: self.nonce,
            barrier_offset,
        };
    }

    fn release(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.state.lock().expect("writer pause state lock poisoned");
        if matches!(
            *state,
            WriterPauseState::Pausing { nonce }
                | WriterPauseState::Paused { nonce, .. }
                if nonce == self.nonce
        ) {
            *state = WriterPauseState::Idle;
        }
        self.active = false;
    }
}

impl Drop for PauseReservation {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod pause_retirement_tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::Semaphore;

    use super::{PauseReservation, PausedWalWriter, WriterPauseState};

    #[tokio::test]
    async fn retiring_paused_writer_never_reopens_commit_gate() {
        let state = Arc::new(Mutex::new(WriterPauseState::Paused {
            nonce: 7,
            barrier_offset: 11,
        }));
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        PausedWalWriter {
            pause: PauseReservation {
                state: Arc::clone(&state),
                nonce: 7,
                active: true,
            },
            permit: Some(permit),
            barrier_offset: 11,
        }
        .retire();
        assert_eq!(
            *state.lock().expect("pause state"),
            WriterPauseState::Paused {
                nonce: 7,
                barrier_offset: 11,
            }
        );
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
    async fn commit_group_with_pause_authorization(
        &self,
        authorization: &PausedWalAuthorization,
        expected_barrier_offset: i64,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        if !authorization.matches_writer(&self.pause_state, expected_barrier_offset) {
            return Err(SubstrateError::Unavailable(
                "activation receipt append lacks the matching pause barrier".into(),
            ));
        }
        self.commit_group_while_permitted(request).await
    }

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
    /// The returned guard owns the sole commit permit. So every commit
    /// acknowledged before this method returns precedes `barrier_offset`, and
    /// no later commit can be acknowledged until the caller resumes or drops
    /// the guard. The permit is a semaphore permit, not a mutex guard. The
    /// writer holds no mutex across broker awaits.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
        let span =
            telemetry::wal_append_span(&self.topic, request.generation, request.frames.len());
        // `let` first, not a `match` scrutinee: a scrutinee's temporaries live
        // until the end of the `match`, which would keep the `Instrumented`
        // future's borrow of `span` alive and block the `drop(span)` below.
        let outcome = self
            .append_group(request, &span)
            .instrument(span.clone())
            .await;
        match outcome {
            Ok(ack) => {
                record_append_offsets(&span, &ack);
                Ok(ack)
            }
            Err(AppendFailure::Rejected(error)) => {
                if matches!(error, SubstrateError::Fenced) {
                    span.record("pg.wal.fenced", true);
                }
                telemetry::record_error(&span, append_error_type(&error), &error);
                Err(error)
            }
            Err(AppendFailure::Indeterminate(error)) => {
                // The commit outcome is unknown and the handler is about to
                // terminate this compute, so the `pending()` below never
                // resolves. Record the status and drop the span *first* — a
                // span still open when the process dies is never exported, and
                // this is precisely the span an operator needs.
                telemetry::record_error(&span, "indeterminate", &error);
                drop(span);
                (self.indeterminate_handler)(&error);
                std::future::pending::<()>().await;
                unreachable!("indeterminate WAL handler must terminate the compute");
            }
        }
    }

    /// Append and commit one group inside the open Kafka transaction.
    ///
    /// This method is separate from [`Self::commit_group_while_permitted`] so
    /// that every "broker outcome unknown" exit is an
    /// [`AppendFailure::Indeterminate`] value and not an inline `pending()`
    /// await. The caller then owns the single site that closes the span before
    /// the compute stops.
    async fn append_group(
        &self,
        request: GroupCommitRequest,
        span: &tracing::Span,
    ) -> Result<GroupCommitAck, AppendFailure> {
        if self.fenced.load(Ordering::SeqCst) {
            return Err(AppendFailure::Rejected(SubstrateError::Fenced));
        }

        let transaction = self
            .producer
            .clone()
            .begin_transaction_owned()
            .await
            .map_err(|error| AppendFailure::Rejected(self.map_producer_error(&error)))?;
        if let Some(error) = self.inject_fault(WalWriterFaultStage::BeforeFirstSend) {
            return self.abort_after_send_error(transaction, error).await;
        }
        // Hoisted out of the per-frame loop: capturing the current context runs
        // the W3C propagator, and one group commit can carry many frames.
        let trace = TraceCarrier::capture_current();
        let headers: Vec<Header> = trace
            .headers()
            .map(|(key, value)| Header {
                key: key.to_owned(),
                value: Some(Bytes::copy_from_slice(value)),
            })
            .collect();
        let mut sent = Vec::with_capacity(request.frames.len());
        let mut encoded_bytes = 0_u64;
        for frame in &request.frames {
            let encoded = Bytes::from(frame.encode());
            encoded_bytes =
                encoded_bytes.saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
            let pending = self
                .producer
                .send(ProducerRecord {
                    topic: self.topic.clone(),
                    partition: Some(0),
                    key: Some(Bytes::copy_from_slice(&request.generation.0.to_be_bytes())),
                    value: Some(encoded),
                    headers: headers.clone(),
                    ..ProducerRecord::default()
                })
                .await;
            sent.push((frame.journal_seq, pending));
        }
        span.record("pg.wal.bytes", telemetry::integer(encoded_bytes));

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
                CommitFailure::Rejected => return Err(AppendFailure::Rejected(substrate_error)),
                CommitFailure::Indeterminate => {
                    return Err(AppendFailure::Indeterminate(error.source));
                }
            }
        }
        if let Some(error) = self.inject_fault(WalWriterFaultStage::AfterCommit) {
            return Err(AppendFailure::Indeterminate(error));
        }
        Ok(GroupCommitAck { frames })
    }
}

/// How one group-commit attempt ended.
///
/// This type separates broker rejections from an unknown outcome. The caller
/// reports a broker rejection as an error. An unknown outcome stops the
/// compute, because no SQL client may be told that a durable write failed.
#[derive(Debug)]
enum AppendFailure {
    /// The broker proved the transaction did not commit.
    Rejected(SubstrateError),
    /// The commit outcome is unknown; the compute must not continue.
    Indeterminate(ProducerError),
}

/// The low-cardinality `error.type` for a refused WAL append.
///
/// The append path only ever yields these two shapes:
/// [`ProducerWalWriter::map_producer_error`] maps a fenced producer to
/// [`SubstrateError::Fenced`] and everything else to
/// [`SubstrateError::Unavailable`].
fn append_error_type(error: &SubstrateError) -> &'static str {
    if matches!(error, SubstrateError::Fenced) {
        "fenced"
    } else {
        "unavailable"
    }
}

fn record_append_offsets(span: &tracing::Span, ack: &GroupCommitAck) {
    if span.is_disabled() {
        return;
    }
    if let Some(first) = ack.frames.first() {
        span.record("pg.wal.first_offset", first.offset);
    }
    if let Some(last) = ack.frames.last() {
        span.record("pg.wal.last_offset", last.offset);
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
    pause.mark_paused(barrier_offset);
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
            return Err(self.reject_paused(&request));
        }
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.is_pause_reserved() {
            return Err(self.reject_paused(&request));
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

    /// Refuse an append because a pause barrier owns the writer, and emit the
    /// `gres.wal_append` span that reports the refusal.
    ///
    /// The attempt is a real, observable rejection, so it gets its own very
    /// short producer span instead of no span at all.
    fn reject_paused(&self, request: &GroupCommitRequest) -> SubstrateError {
        let error = SubstrateError::Unavailable("WAL writer is paused".into());
        let span =
            telemetry::wal_append_span(&self.topic, request.generation, request.frames.len());
        span.record("pg.wal.paused", true);
        telemetry::record_error(&span, "paused", &error);
        error
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
    ) -> Result<GroupCommitAck, AppendFailure> {
        let substrate_error = self.map_producer_error(&error);
        if matches!(
            error,
            ProducerError::FencedProducer | ProducerError::TransactionAborted
        ) {
            // The broker has proved this epoch cannot commit. The returned
            // guard cannot be cleanly aborted once the producer is fenced,
            // but this is not ambiguous: no record from it can become durable.
            drop(transaction);
            return Err(AppendFailure::Rejected(substrate_error));
        }
        self.abort_with_error(transaction, substrate_error).await
    }

    async fn abort_with_error(
        &self,
        transaction: OwnedTransaction,
        substrate_error: SubstrateError,
    ) -> Result<GroupCommitAck, AppendFailure> {
        if let Some(error) = self.inject_fault(WalWriterFaultStage::BeforeAbort) {
            return Err(AppendFailure::Indeterminate(error));
        }
        match transaction.abort().await {
            Ok(()) => Err(AppendFailure::Rejected(substrate_error)),
            Err(error) => Err(AppendFailure::Indeterminate(error.source)),
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
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn chunk_wal_batch(
    ops: Vec<WriteOp>,
    first_journal_seq: u64,
    max_frame_size: ByteSize,
) -> Result<Vec<WalFrame>, SubstrateError> {
    let span = telemetry::chunk_span(ops.len(), first_journal_seq);
    let _entered = span.enter();
    let frames = chunk_frames(ops, first_journal_seq, max_frame_size)?;
    span.record("wal.chunk.frames", telemetry::integer(frames.len()));
    Ok(frames)
}

fn chunk_frames(
    ops: Vec<WriteOp>,
    first_journal_seq: u64,
    max_frame_size: ByteSize,
) -> Result<Vec<WalFrame>, SubstrateError> {
    if ops.is_empty() {
        return Ok(vec![WalFrame {
            journal_seq: first_journal_seq,
            ops,
        }]);
    }

    // Encoded lengths are `usize`; comparing there keeps the budget exact.
    let max_frame_bytes = max_frame_size.bytes_usize();
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
    max_frame_size: ByteSize,
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
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            next_journal_seq: std::sync::atomic::AtomicU64::new(next),
            commit_gate: Arc::new(Semaphore::new(1)),
            checkpoint_stats: None,
            checkpoint_snapshot_source: None,
        }
    }

    /// Override the maximum encoded frame size.
    #[must_use]
    pub fn with_max_frame_size(mut self, max_frame_size: ByteSize) -> Self {
        self.max_frame_size = max_frame_size;
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
        let span = telemetry::commit_span(ops.len());
        let result = self
            .commit_in_span(ops, &span)
            .instrument(span.clone())
            .await;
        if let Err(error) = &result {
            // `into_pg` consumes, and the SQLSTATE is exactly the
            // low-cardinality discriminator the OTel convention wants.
            let rendered = error.clone().into_pg();
            telemetry::record_error(&span, &rendered.code, &rendered.message);
        }
        result
    }
}

impl<W> SubstrateCommitter<W>
where
    W: TransactionalWalWriter + 'static,
{
    /// The commit body, run inside the `pg.commit` span so the WAL append and
    /// the local applies attach to it as children.
    async fn commit_in_span(
        &self,
        ops: Vec<WriteOp>,
        span: &tracing::Span,
    ) -> Result<(), ExecError> {
        // `Instant::now` is not free, so only pay for it when someone is
        // listening; `pg.gate_wait_ms` must cover the permit wait alone.
        let gate_started = (!span.is_disabled()).then(std::time::Instant::now);
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| ExecError::Unavailable)?;
        if let Some(started) = gate_started {
            span.record("pg.gate_wait_ms", started.elapsed().as_secs_f64() * 1_000.0);
        }
        let next = self.next_journal_seq.load(Ordering::SeqCst);
        let frames = chunk_wal_batch(ops, next, self.max_frame_size)?;
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
        // Summing encoded lengths walks every frame, so compute it only when
        // the checkpointer or the span will actually consume it.
        let byte_count = if self.checkpoint_stats.is_some() || !span.is_disabled() {
            Some(frames.iter().try_fold(0_u64, |total, frame| {
                let len = u64::try_from(frame.encoded_len()).map_err(|_| ExecError::Unavailable)?;
                total.checked_add(len).ok_or(ExecError::Unavailable)
            })?)
        } else {
            None
        };
        let frame_count = u64::try_from(frames.len()).map_err(|_| ExecError::Unavailable)?;
        if let Some(stats) = &self.checkpoint_stats
            && let Some(byte_count) = byte_count
        {
            stats.record_committed(frame_count, byte_count);
        }
        if let Some(source) = &self.checkpoint_snapshot_source
            && let Some(last_ack) = ack.frames.last().copied()
        {
            source.record_commit(last_ack);
        }
        let next_after = next
            .checked_add(frame_count)
            .ok_or(ExecError::Unavailable)?;
        self.next_journal_seq.store(next_after, Ordering::SeqCst);
        if !span.is_disabled() {
            span.record("pg.commit.frames", telemetry::integer(frame_count));
            span.record("pg.commit.bytes", byte_count.map(telemetry::integer));
            span.record("pg.journal_seq.first", telemetry::integer(next));
            span.record("pg.journal_seq.next", telemetry::integer(next_after));
        }
        Ok(())
    }
}

impl SubstrateCommitter<DeferredWalWriter<ProducerWalWriter>> {
    /// Copy the predecessor's exact `MustActivate` anchor into a newly initialized canonical
    /// producer before the deferred engine handle is bound.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn commit_activation_anchor_before_bind(
        &self,
        canonical: Arc<ProducerWalWriter>,
        tenant: &str,
        operation_id: &str,
        expected: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<bool, SubstrateError> {
        if self.writer.is_activated() {
            return Err(SubstrateError::Unavailable(
                "activation anchor must precede deferred writer binding".into(),
            ));
        }
        validate_must_activate_transition(tenant, operation_id, None, &expected, &value)?;
        let key = crabka_pgkv::key::topology_activation_receipt_key(tenant, operation_id);
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.kv.get(&key).map_err(SubstrateError::from)? != Some(expected.clone()) {
            return Ok(false);
        }
        let next = self.next_journal_seq.load(Ordering::SeqCst);
        let operation = WriteOp::ConditionalPut {
            key: key.clone(),
            expected: Some(expected),
            value: value.clone(),
        };
        let frame = WalFrame {
            journal_seq: next,
            ops: vec![operation],
        };
        let ack = canonical
            .commit_group(GroupCommitRequest {
                generation: self.generation,
                frames: vec![frame.clone()],
            })
            .await?;
        apply_frame(self.kv.as_ref(), &frame.ops)?;
        if let Some(source) = &self.checkpoint_snapshot_source
            && let Some(last_ack) = ack.frames.last().copied()
        {
            source.record_commit(last_ack);
        }
        self.next_journal_seq.store(
            next.checked_add(1)
                .ok_or_else(|| SubstrateError::Frame("journal sequence overflow".into()))?,
            Ordering::SeqCst,
        );
        Ok(self.kv.get(&key).map_err(SubstrateError::from)? == Some(value))
    }

    /// Commit exactly one validated `MustActivate` receipt while the predecessor writer is paused.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn commit_activation_receipt_cas(
        &self,
        authorization: &PausedWalAuthorization,
        barrier_offset: i64,
        tenant: &str,
        operation_id: &str,
        expected: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<bool, SubstrateError> {
        validate_must_activate_transition(
            tenant,
            operation_id,
            Some(barrier_offset),
            &expected,
            &value,
        )?;
        let key = crabka_pgkv::key::topology_activation_receipt_key(tenant, operation_id);
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.kv.get(&key).map_err(SubstrateError::from)? != Some(expected.clone()) {
            return Ok(false);
        }
        let next_journal_seq = self.next_journal_seq.load(Ordering::SeqCst);
        let frame = WalFrame {
            journal_seq: next_journal_seq,
            ops: vec![WriteOp::ConditionalPut {
                key,
                expected: Some(expected),
                value,
            }],
        };
        let writer = self.writer.current()?;
        let ack = writer
            .commit_group_with_pause_authorization(
                authorization,
                barrier_offset,
                GroupCommitRequest {
                    generation: self.generation,
                    frames: vec![frame.clone()],
                },
            )
            .await?;
        apply_frame(self.kv.as_ref(), &frame.ops)?;
        if let Some(source) = &self.checkpoint_snapshot_source
            && let Some(last_ack) = ack.frames.last().copied()
        {
            source.record_commit(last_ack);
        }
        self.next_journal_seq.store(
            next_journal_seq
                .checked_add(1)
                .ok_or_else(|| SubstrateError::Frame("journal sequence overflow".into()))?,
            Ordering::SeqCst,
        );
        Ok(self
            .kv
            .get(&crabka_pgkv::key::topology_activation_receipt_key(
                tenant,
                operation_id,
            ))
            .map_err(SubstrateError::from)?
            == Some(
                frame
                    .ops
                    .into_iter()
                    .find_map(|op| match op {
                        WriteOp::ConditionalPut { value, .. } => Some(value),
                        _ => None,
                    })
                    .expect("activation frame contains its value"),
            ))
    }

    /// Commit one range-control receipt while the matching predecessor pause is held.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn commit_range_control_receipt_cas(
        &self,
        authorization: &PausedWalAuthorization,
        barrier_offset: i64,
        tenant: &str,
        receipt: &str,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool, SubstrateError> {
        validate_paused_control_receipt_transition(tenant, receipt, expected.as_deref(), &value)?;
        let key = crabka_pgkv::key::range_control_receipt_key(tenant, receipt);
        let _permit = Arc::clone(&self.commit_gate)
            .acquire_owned()
            .await
            .map_err(|_| SubstrateError::Unavailable("WAL commit gate closed".into()))?;
        if self.kv.get(&key).map_err(SubstrateError::from)? != expected {
            return Ok(false);
        }
        let next_journal_seq = self.next_journal_seq.load(Ordering::SeqCst);
        let frame = WalFrame {
            journal_seq: next_journal_seq,
            ops: vec![WriteOp::ConditionalPut {
                key: key.clone(),
                expected,
                value: value.clone(),
            }],
        };
        let writer = self.writer.current()?;
        let ack = writer
            .commit_group_with_pause_authorization(
                authorization,
                barrier_offset,
                GroupCommitRequest {
                    generation: self.generation,
                    frames: vec![frame.clone()],
                },
            )
            .await?;
        apply_frame(self.kv.as_ref(), &frame.ops)?;
        if let Some(source) = &self.checkpoint_snapshot_source
            && let Some(last_ack) = ack.frames.last().copied()
        {
            source.record_commit(last_ack);
        }
        self.next_journal_seq.store(
            next_journal_seq
                .checked_add(1)
                .ok_or_else(|| SubstrateError::Frame("journal sequence overflow".into()))?,
            Ordering::SeqCst,
        );
        Ok(self.kv.get(&key).map_err(SubstrateError::from)? == Some(value))
    }
}

fn validate_paused_control_receipt_transition(
    tenant: &str,
    receipt_key: &str,
    expected: Option<&[u8]>,
    value: &[u8],
) -> Result<(), SubstrateError> {
    use crabka_gres_ranges::{
        control::RangeControlReceipt,
        transport::{RangeControlOperation as Operation, RangeControlResp},
    };
    let next: RangeControlReceipt = serde_json::from_slice(value)
        .map_err(|error| SubstrateError::Frame(format!("decode control receipt: {error}")))?;
    let step = match next.request.operation {
        Operation::ForceCheckpoint => "checkpoint",
        Operation::PauseAtCoveredOffset { .. } => "pause",
        Operation::Status => "status",
        Operation::StageFilteredRestore { .. } => "stage",
        Operation::SuccessorFencePrologue { .. } => "prologue",
        Operation::InheritMarkers { .. } => "markers",
        Operation::RetirePredecessor => "retire-predecessor",
        Operation::Resume => "resume",
    };
    let exact_key = format!(
        "r{}.g{}:{}:{step}",
        next.request.range_id.as_u32(),
        next.request.generation,
        next.request.operation_id
    );
    if next.request.tenant != tenant || exact_key != receipt_key {
        return Err(SubstrateError::Frame(
            "paused control receipt tenant or exact step key mismatch".into(),
        ));
    }
    let Some(expected) = expected else {
        if next.revision == 1 && next.result.is_none() {
            return Ok(());
        }
        return Err(SubstrateError::Frame(
            "paused control receipt creation must be an in-progress revision one".into(),
        ));
    };
    let prior: RangeControlReceipt = serde_json::from_slice(expected)
        .map_err(|error| SubstrateError::Frame(format!("decode prior control receipt: {error}")))?;
    let immutable = prior.request == next.request
        && prior.request_digest == next.request_digest
        && prior.generation == next.generation
        && next.revision == prior.revision.saturating_add(1);
    if !immutable {
        return Err(SubstrateError::Frame(
            "paused control receipt changed immutable request evidence or revision".into(),
        ));
    }
    match (&prior.result, &next.result) {
        (None, Some(_))
        | (Some(RangeControlResp::Staged { .. }), Some(RangeControlResp::Staged { .. })) => Ok(()),
        (
            Some(RangeControlResp::Paused {
                barrier_offset: old,
            }),
            Some(RangeControlResp::Paused {
                barrier_offset: new,
            }),
        ) if new >= old => Ok(()),
        _ => Err(SubstrateError::Frame(
            "paused control receipt transition is not an allowed completion or reconciliation"
                .into(),
        )),
    }
}

#[cfg(test)]
mod paused_control_receipt_validation_tests {
    use crabka_gres_ranges::{
        RangeId,
        control::RangeControlReceipt,
        transport::{RangeControlOperation, RangeControlReq, RangeControlResp},
    };

    use super::validate_paused_control_receipt_transition;

    fn receipt(revision: u64, result: Option<RangeControlResp>) -> RangeControlReceipt {
        RangeControlReceipt {
            request: RangeControlReq {
                tenant: "tenant-a".into(),
                range_id: RangeId::COORDINATOR,
                generation: 7,
                operation_id: "split-9".into(),
                operation: RangeControlOperation::RetirePredecessor,
            },
            request_digest: "fixed-digest".into(),
            generation: 7,
            revision,
            result,
        }
    }

    #[test]
    fn paused_receipt_accepts_exact_intent_and_completion_only() {
        let intent = receipt(1, None);
        let intent_bytes = serde_json::to_vec(&intent).unwrap();
        assert!(
            validate_paused_control_receipt_transition(
                "tenant-a",
                "r0.g7:split-9:retire-predecessor",
                None,
                &intent_bytes,
            )
            .is_ok()
        );
        let completion = receipt(2, Some(RangeControlResp::Applied));
        assert!(
            validate_paused_control_receipt_transition(
                "tenant-a",
                "r0.g7:split-9:retire-predecessor",
                Some(&intent_bytes),
                &serde_json::to_vec(&completion).unwrap(),
            )
            .is_ok()
        );
    }

    #[test]
    fn paused_receipt_rejects_wrong_key_digest_revision_and_result_rewrite() {
        let prior = receipt(2, Some(RangeControlResp::Applied));
        let prior_bytes = serde_json::to_vec(&prior).unwrap();
        let mut candidate = prior.clone();
        candidate.revision = 3;
        candidate.result = Some(RangeControlResp::Rejected {
            code: "forged".into(),
            message: "forged".into(),
        });
        assert!(
            validate_paused_control_receipt_transition(
                "tenant-a",
                "r0.g7:split-9:retire-predecessor",
                Some(&prior_bytes),
                &serde_json::to_vec(&candidate).unwrap(),
            )
            .is_err()
        );
        candidate.result = Some(RangeControlResp::Applied);
        candidate.request_digest = "changed".into();
        assert!(
            validate_paused_control_receipt_transition(
                "tenant-a",
                "r0.g7:split-9:retire-predecessor",
                Some(&prior_bytes),
                &serde_json::to_vec(&candidate).unwrap(),
            )
            .is_err()
        );
        assert!(
            validate_paused_control_receipt_transition(
                "tenant-a",
                "r0.g8:split-9:retire-predecessor",
                None,
                &serde_json::to_vec(&receipt(1, None)).unwrap(),
            )
            .is_err()
        );
    }
}

fn validate_must_activate_transition(
    tenant: &str,
    operation_id: &str,
    barrier_offset: Option<i64>,
    expected: &[u8],
    value: &[u8],
) -> Result<(), SubstrateError> {
    use crabka_gres_ranges::control::{TopologyActivationPhase, TopologyActivationReceipt};
    let prior: TopologyActivationReceipt = serde_json::from_slice(expected).map_err(|error| {
        SubstrateError::Frame(format!("decode prior activation receipt: {error}"))
    })?;
    let next: TopologyActivationReceipt = serde_json::from_slice(value).map_err(|error| {
        SubstrateError::Frame(format!("decode next activation receipt: {error}"))
    })?;
    let valid = prior.tenant == tenant
        && next.tenant == tenant
        && prior.operation_id == operation_id
        && next.operation_id == operation_id
        && prior.split == next.split
        && prior.targets.len() == next.targets.len()
        && prior.targets.iter().all(|(range_id, prior_target)| {
            prior_target.range_id == *range_id
                && prior_target.replay_journal_seq.is_none()
                && !prior_target.writer_activated
                && prior_target.bootstrap_checkpoint.is_none()
                && next.targets.get(range_id).is_some_and(|next_target| {
                    next_target.range_id == prior_target.range_id
                        && next_target.wal_generation == prior_target.wal_generation
                        && next_target.endpoint == prior_target.endpoint
                        && next_target.interval == prior_target.interval
                })
        })
        && prior.phase == TopologyActivationPhase::SourceCheckpoint
        && next.phase == TopologyActivationPhase::MustActivate
        && next.revision
            == prior.revision.checked_add(1).ok_or_else(|| {
                SubstrateError::Frame("activation receipt revision overflow".into())
            })?
        && next.source_checkpoint == prior.source_checkpoint
        && prior.source_checkpoint.is_some()
        && prior.barrier_offset.is_none()
        && prior.tail_sha256.is_none()
        && barrier_offset.is_none_or(|offset| next.barrier_offset == Some(offset))
        && next.barrier_offset.is_some()
        && next.tail_sha256.is_some()
        && next.targets.values().all(|target| {
            target.replay_journal_seq.is_some()
                && !target.writer_activated
                && target.bootstrap_checkpoint.is_none()
        });
    if valid {
        Ok(())
    } else {
        Err(SubstrateError::Frame(
            "invalid MustActivate receipt transition".into(),
        ))
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

    #[tokio::test]
    async fn deferred_writer_rejects_until_atomically_activated() {
        let deferred = DeferredWalWriter::staged();
        let request = GroupCommitRequest {
            generation: WriterGeneration(7),
            frames: vec![WalFrame {
                journal_seq: 1,
                ops: Vec::new(),
            }],
        };

        let staged = deferred
            .commit_group(request.clone())
            .await
            .expect_err("staged writer must fail closed");
        assert!(matches!(staged, SubstrateError::Unavailable(_)));

        let live = Arc::new(FakeWalWriter {
            current_generation: AtomicU64::new(7),
            next_offset: AtomicU64::new(0),
        });
        deferred
            .activate(live.clone())
            .expect("first activation succeeds");
        deferred
            .commit_group(request)
            .await
            .expect("activated writer delegates");
        deferred
            .assert_current(WriterGeneration(7))
            .await
            .expect("activated fence lease delegates");
        assert_eq!(live.next_offset.load(Ordering::SeqCst), 1);

        let second = deferred
            .activate(Arc::new(FakeWalWriter::default()))
            .expect_err("activation is write-once");
        assert!(matches!(second, SubstrateError::Unavailable(_)));
    }

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
            assert!(matches!(
                *pause_state.lock().expect("pause state"),
                WriterPauseState::Pausing { .. }
            ));
            let Err(error) = PauseReservation::reserve(Arc::clone(&pause_state)) else {
                panic!("a second pause reservation must be rejected");
            };
            assert!(matches!(error, SubstrateError::AlreadyPaused));
            drop(pause);
        }

        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);

        {
            let pause = PauseReservation::reserve(Arc::clone(&pause_state)).expect("reserve");
            pause.mark_paused(7);
            assert!(matches!(
                *pause_state.lock().expect("pause state"),
                WriterPauseState::Paused {
                    barrier_offset: 7,
                    ..
                }
            ));
        }

        assert!(*pause_state.lock().expect("pause state") == WriterPauseState::Idle);
    }

    #[test]
    fn pause_authorization_is_invalid_after_release_and_during_a_later_pause() {
        let state = Arc::new(Mutex::new(WriterPauseState::Idle));
        let mut first = PauseReservation::reserve(Arc::clone(&state)).expect("first pause");
        first.mark_paused(7);
        let authorization = PausedWalAuthorization {
            state: Arc::clone(&state),
            nonce: first.nonce,
            barrier_offset: 7,
        };
        assert!(authorization.matches_writer(&state, 7));
        first.release();
        assert!(!authorization.matches_writer(&state, 7));

        let mut second = PauseReservation::reserve(Arc::clone(&state)).expect("second pause");
        second.mark_paused(7);
        assert_ne!(first.nonce, second.nonce);
        assert!(!authorization.matches_writer(&state, 7));
        second.release();
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

        let frames = chunk_wal_batch(ops, 7, crabka_units::bytes(36)).expect("chunk");

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

        let frames = chunk_wal_batch(ops, 3, crabka_units::bytes(16)).expect("chunk");

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
        let oracle = TsoOracle::recover_with_heartbeat_interval(
            horizon.clone(),
            horizon.clone(),
            horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            0,
            <crabka_units::Time as crabka_units::convert::TimeExt>::ZERO,
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
        let oracle = TsoOracle::recover_with_heartbeat_interval(
            horizon.clone(),
            horizon.clone(),
            horizon.epoch(),
            NonZeroU64::new(4).expect("stride"),
            0,
            <crabka_units::Time as crabka_units::convert::TimeExt>::ZERO,
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
