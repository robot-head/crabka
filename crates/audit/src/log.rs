//! The `AuditLog` handle and the background `AuditWriter`.
//!
//! `AuditLog::emit` is synchronous and does not block. The `AuditWriter` drains
//! events into a sink.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use crabka_units::prelude::{Time, TimeExt as _};
use qubit_clock::sleep::AsyncSleeper;
use tokio::sync::mpsc;

use crate::{
    chain::ChainState,
    checkpoint::Checkpoint,
    event::AuditEvent,
    ids::{EpochMs, Seq},
    ocsf::ProductInfo,
    signing::SigningKeyProvider,
    sink::{AuditRecord, AuditSink},
    spool::Spool,
    stats::AuditStats,
};

/// Cloneable, cheap handle that broker code calls to record events.
///
/// `emit` is synchronous and never blocks. It is safe to call from the
/// synchronous `Authorizer::authorize` trait and from async request handlers.
#[derive(Debug)]
pub struct AuditLog {
    tx: ArcSwapOption<mpsc::Sender<AuditEvent>>,
    dropped: AtomicU64,
}

impl AuditLog {
    /// Create an enabled log and the receiver for an [`AuditWriter`].
    #[must_use]
    pub fn new(capacity: usize) -> (Arc<Self>, mpsc::Receiver<AuditEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Arc::new(Self {
                tx: ArcSwapOption::new(Some(Arc::new(tx))),
                dropped: AtomicU64::new(0),
            }),
            rx,
        )
    }

    /// A no-op log for a disabled audit subsystem.
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            tx: ArcSwapOption::new(None),
            dropped: AtomicU64::new(0),
        })
    }

    /// Record an event.
    ///
    /// This method does not block. If the queue is full, it drops the event and
    /// counts the drop. Durable spooling is Slice 3 / AU-5.
    pub fn emit(&self, event: AuditEvent) {
        let Some(tx) = self.tx.load_full() else {
            return;
        };
        if tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("audit event dropped (queue full or writer stopped)");
        }
    }

    /// Close the event stream for every clone of this handle.
    ///
    /// Events already in the queue remain available to the writer. Once they
    /// are drained, the writer exits cleanly.
    pub fn close(&self) {
        self.tx.store(None);
    }

    /// Count of events dropped because of backpressure.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Construction parameters for [`AuditWriter`].
pub struct AuditWriterParams {
    pub sink: Arc<dyn AuditSink>,
    pub product: ProductInfo,
    pub signer: Option<Arc<dyn SigningKeyProvider>>,
    /// Emit a checkpoint after the writer chains this many records since the
    /// last checkpoint. This field is a count, not an extent. `0` disables the
    /// count trigger.
    pub checkpoint_every_n: u64,
    pub checkpoint_every: Time,
    /// Chain state, possibly resumed from a recovered position.
    pub chain: ChainState,
    /// Durable spool for the AU-5 degraded path. `None` disables spooling.
    pub spool: Option<Spool>,
    pub stats: Arc<AuditStats>,
    /// How often the writer tries to drain the spool in spool mode.
    pub replay_every: Time,
    /// Relative sleeper that drives the checkpoint and replay cadence.
    /// Production uses [`qubit_clock::sleep::SystemSleeper`]. Tests inject a
    /// [`qubit_clock::sleep::MockSleeper`], so the two tickers fire on a
    /// controlled mock timeline and not on real wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

/// Background task that chains and writes audit events.
///
/// The writer spools records when the sink fails, and it replays them when the
/// sink recovers. It also emits signed checkpoints on a cadence.
pub struct AuditWriter {
    rx: mpsc::Receiver<AuditEvent>,
    sink: Arc<dyn AuditSink>,
    product: ProductInfo,
    chain: ChainState,
    signer: Option<Arc<dyn SigningKeyProvider>>,
    checkpoint_every_n: u64,
    checkpoint_every: Time,
    since_checkpoint: u64,
    spool: Option<Spool>,
    spooling: bool,
    stats: Arc<AuditStats>,
    replay_every: Time,
    sleeper: Arc<dyn AsyncSleeper>,
}

impl AuditWriter {
    #[must_use]
    pub fn new(rx: mpsc::Receiver<AuditEvent>, params: AuditWriterParams) -> Self {
        let spooling = params.spool.as_ref().is_some_and(|s| !s.is_empty());
        if let Some(spool) = &params.spool {
            params.stats.set_depth(spool.count(), spool.size());
        }
        Self {
            rx,
            sink: params.sink,
            product: params.product,
            chain: params.chain,
            signer: params.signer,
            checkpoint_every_n: params.checkpoint_every_n,
            checkpoint_every: params.checkpoint_every,
            since_checkpoint: 0,
            spool: params.spool,
            spooling,
            stats: params.stats,
            replay_every: params.replay_every,
            sleeper: params.sleeper,
        }
    }

    /// Drain the channel until all senders drop.
    ///
    /// The writer then emits a final checkpoint for any pending tail.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(checkpoint_every_n = self.checkpoint_every_n, spooling = self.spooling)
    )]
    pub async fn run(mut self) {
        // Drive the checkpoint and replay cadence through the injected
        // `AsyncSleeper` (production: real time; tests: a mock timeline). Each
        // ticker is a single sleep future re-armed only after it fires, which
        // matches `tokio::time::interval` with `MissedTickBehavior::Delay`: a
        // steady stream of events never resets or starves either tick. The
        // sleeper is cloned into a local so the futures borrow it rather than
        // `self`, leaving `self` free for the `&mut self` handlers below.
        let sleeper = self.sleeper.clone();
        let mut ckpt = sleeper.sleep_for_async(self.checkpoint_every.to_std());
        let mut replay = sleeper.sleep_for_async(self.replay_every.to_std());

        loop {
            tokio::select! {
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(event) => {
                            self.write_chained(&event).await;
                            if self.checkpoint_every_n > 0
                                && self.since_checkpoint >= self.checkpoint_every_n
                            {
                                self.emit_checkpoint().await;
                            }
                        }
                        None => break,
                    }
                }
                () = &mut ckpt => {
                    if self.since_checkpoint > 0 {
                        self.emit_checkpoint().await;
                    }
                    ckpt = sleeper.sleep_for_async(self.checkpoint_every.to_std());
                }
                () = &mut replay => {
                    if self.spooling {
                        self.try_replay().await;
                    }
                    replay = sleeper.sleep_for_async(self.replay_every.to_std());
                }
            }
        }
        if self.since_checkpoint > 0 {
            self.emit_checkpoint().await;
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?event.class(), seq = tracing::field::Empty)
    )]
    async fn write_chained(&mut self, event: &AuditEvent) {
        let mut record = AuditRecord::from_event(event, &self.product);
        let (seq, prev) = self.chain.extend(&record.value);
        tracing::Span::current().record("seq", seq);
        record.push_chain_headers(seq, &prev);
        self.write_or_spool(record).await;
        self.since_checkpoint += 1;
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(since_checkpoint = self.since_checkpoint, seq_high = tracing::field::Empty)
    )]
    async fn emit_checkpoint(&mut self) {
        let Some(signer) = self.signer.clone() else {
            self.since_checkpoint = 0;
            return;
        };
        let seq_high = Seq(self.chain.next_seq().saturating_sub(1));
        tracing::Span::current().record("seq_high", seq_high.0);
        let head = self.chain.head();
        let cp = Checkpoint::signed(signer.as_ref(), seq_high, &head, EpochMs(now_ms()));
        self.write_or_spool(cp.to_record()).await;
        self.since_checkpoint = 0;
    }

    /// Write to the sink, or to the spool.
    ///
    /// This method writes to the spool when the writer is in spool mode, which
    /// is sticky, or when the sink write fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, spooling = self.spooling)
    )]
    async fn write_or_spool(&mut self, record: AuditRecord) {
        if self.spooling {
            self.spool_record(&record);
            return;
        }
        if let Err(e) = self.sink.write(record.clone()).await {
            tracing::warn!(error = %e, "audit sink write failed; entering spool mode");
            self.spooling = true;
            self.spool_record(&record);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(class = ?record.class))]
    fn spool_record(&mut self, record: &AuditRecord) {
        let Some(spool) = &mut self.spool else {
            self.stats.inc_dropped();
            return;
        };
        match spool.append(record) {
            Ok(true) => {
                self.stats.inc_spooled();
                self.stats.set_depth(spool.count(), spool.size());
            }
            Ok(false) => {
                self.stats.inc_dropped();
                tracing::warn!("audit spool full; record dropped");
            }
            Err(e) => {
                self.stats.inc_dropped();
                tracing::error!(error = %e, "audit spool write failed; record dropped");
            }
        }
    }

    /// Drain the spool to the sink in order.
    ///
    /// The writer exits spool mode when the spool is fully drained.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(replayed = tracing::field::Empty, total = tracing::field::Empty)
    )]
    async fn try_replay(&mut self) {
        let Some(spool) = &mut self.spool else {
            self.spooling = false;
            return;
        };
        if spool.is_empty() {
            self.spooling = false;
            return;
        }
        let records = match spool.read_all() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "audit spool read failed during replay");
                return;
            }
        };
        let mut replayed = 0usize;
        for rec in &records {
            if self.sink.write(rec.clone()).await.is_err() {
                break; // topic still unhealthy
            }
            replayed += 1;
        }
        let span = tracing::Span::current();
        span.record("replayed", replayed);
        span.record("total", records.len());
        if replayed == records.len() {
            if let Err(e) = spool.truncate() {
                tracing::error!(error = %e, "audit spool truncate failed");
                return;
            }
            self.spooling = false;
            tracing::info!(replayed, "audit spool drained; resumed direct topic writes");
        } else if let Err(e) = spool.rewrite(&records[replayed..]) {
            tracing::error!(error = %e, "audit spool rewrite failed during replay");
            return;
        }
        self.stats
            .inc_replayed_by(u64::try_from(replayed).unwrap_or(u64::MAX));
        self.stats.set_depth(spool.count(), spool.size());
    }
}

/// Epoch-millisecond clock for the checkpoint timestamps.
// cargo-mutants: wall-clock read; no deterministic assertion.
#[cfg_attr(test, mutants::skip)]
fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering::SeqCst},
    };

    use assert2::check;
    use crabka_units::prelude::{ByteSize, Time, TimeExt as _, bytes, hours, mebibytes, millis};
    use qubit_clock::{MockTimeline, sleep::MockSleeper};

    use super::*;
    use crate::{
        checkpoint::Checkpoint,
        event::*,
        ocsf::ProductInfo,
        signing::FileEd25519Signer,
        sink::{AuditRecord, AuditSink, MemorySink},
        spool::Spool,
        stats::AuditStats,
    };

    fn product() -> ProductInfo {
        ProductInfo {
            vendor_name: "Crabka".into(),
            name: "crabka-broker".into(),
            version: "0".into(),
        }
    }

    fn life(n: i64) -> AuditEvent {
        AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: n,
            time_ms: n,
        }
    }

    fn header(rec: &AuditRecord, key: &str) -> Option<String> {
        rec.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
    }

    fn test_signer() -> (std::sync::Arc<FileEd25519Signer>, Vec<u8>) {
        use ring::signature::{Ed25519KeyPair, KeyPair};
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let pubkey = kp.public_key().as_ref().to_vec();
        let s = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap();
        (std::sync::Arc::new(s), pubkey)
    }

    #[derive(Debug)]
    struct FailableSink {
        fail: AtomicBool,
        /// -1 = unlimited; >= 0 = writes remaining before budget error.
        allow: AtomicI64,
        inner: MemorySink,
    }

    impl Default for FailableSink {
        fn default() -> Self {
            Self {
                fail: AtomicBool::new(false),
                allow: AtomicI64::new(-1),
                inner: MemorySink::default(),
            }
        }
    }

    impl FailableSink {
        fn set_fail(&self, v: bool) {
            self.fail.store(v, SeqCst);
        }

        fn allow_n(&self, n: i64) {
            self.fail.store(false, SeqCst);
            self.allow.store(n, SeqCst);
        }

        fn allow_unlimited(&self) {
            self.allow.store(-1, SeqCst);
            self.fail.store(false, SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl AuditSink for FailableSink {
        async fn write(&self, record: AuditRecord) -> Result<(), crate::sink::AuditError> {
            if self.fail.load(SeqCst) {
                return Err(crate::sink::AuditError::Sink("forced".into()));
            }
            let allow = self.allow.load(SeqCst);
            if allow >= 0 {
                if allow == 0 {
                    return Err(crate::sink::AuditError::Sink("budget exhausted".into()));
                }
                self.allow.fetch_sub(1, SeqCst);
            }
            self.inner.write(record).await
        }
    }

    /// Replay ticker cadence for the test params. Tests advance the mock
    /// timeline by this amount to fire the replay ticker exactly once.
    const REPLAY_EVERY: Time = millis(20);

    /// A cadence that no test reaches. No test advances the mock timeline that
    /// far, so the ticker this cadence drives stays dormant.
    const DORMANT: Time = hours(1);

    /// A spool cap that is large enough that no test reaches it by accident.
    const ROOMY_CAP: ByteSize = mebibytes(1);

    fn params(sink: Arc<dyn AuditSink>, spool: Spool, stats: Arc<AuditStats>) -> AuditWriterParams {
        AuditWriterParams {
            sink,
            product: product(),
            signer: None,
            checkpoint_every_n: 0,
            checkpoint_every: DORMANT,
            chain: crate::chain::ChainState::new(),
            spool: Some(spool),
            stats,
            replay_every: REPLAY_EVERY,
            // A dormant mock sleeper: its checkpoint/replay tickers only fire
            // when a test advances the shared timeline, so tests that don't
            // exercise the tickers stay quiet and deterministic.
            sleeper: Arc::new(MockSleeper::new()),
        }
    }

    /// Like [`params`], but also returns the mock [`MockTimeline`].
    ///
    /// The timeline backs the checkpoint and replay tickers. A test can fire
    /// them deterministically with `timeline.advance(replay_every)` instead of
    /// a sleep in real time.
    fn params_with_timeline(
        sink: Arc<dyn AuditSink>,
        spool: Spool,
        stats: Arc<AuditStats>,
    ) -> (AuditWriterParams, MockTimeline) {
        let sleeper = MockSleeper::new();
        let timeline = sleeper.timeline();
        let mut p = params(sink, spool, stats);
        p.sleeper = Arc::new(sleeper);
        (p, timeline)
    }

    /// Polls `cond` on every executor turn until it holds.
    ///
    /// The function yields, so the spawned writer task can make progress. It
    /// replaces the fixed `sleep` calls that waited for the writer to drain the
    /// channel. It returns at the instant the observable condition is true,
    /// which is deterministic. The large iteration cap is only a hang guard.
    async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..1_000_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never held: {what}");
    }

    #[tokio::test]
    async fn emitted_events_reach_the_sink_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let sink = Arc::new(MemorySink::default());
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(16);
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: None,
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats,
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let handle = tokio::spawn(writer.run());

        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));

        // Dropping the only sender ends the writer loop cleanly.
        drop(log);
        handle.await.unwrap();

        let recs = sink.records();
        check!((recs.len(), recs[0].class) == (3, AuditEventClass::ApplicationLifecycle));
        // node_id 1,2,3 preserved in order via the OCSF "device.uid" field.
        let v0: serde_json::Value = serde_json::from_slice(&recs[0].value).unwrap();
        check!(v0["device"]["uid"] == "1");
    }

    #[tokio::test]
    async fn close_ends_stream_for_every_log_clone_after_queued_events() {
        let (log, mut rx) = AuditLog::new(16);
        let clone = Arc::clone(&log);
        log.emit(life(1));

        clone.close();
        log.emit(life(2));

        check!(rx.recv().await == Some(life(1)));
        check!(rx.recv().await.is_none());
        check!(log.dropped() == 0);
    }

    #[test]
    fn disabled_log_drops_without_panicking() {
        let log = AuditLog::disabled();
        log.emit(life(1)); // no receiver, no panic
        check!(log.dropped() == 0); // disabled path is a silent no-op, not a "drop"
    }

    #[tokio::test]
    async fn full_queue_increments_dropped() {
        let (log, _rx) = AuditLog::new(1); // tiny queue, receiver never drains
        // First may enqueue; subsequent ones overflow.
        for i in 0..10 {
            log.emit(life(i));
        }
        check!(log.dropped() == 9);
    }

    #[tokio::test]
    async fn chained_records_carry_seq_and_prev_hash() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // no signer, huge interval => no checkpoints, just chaining
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: None,
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        check!(recs.len() == 2); // no checkpoints (no signer)
        // seq headers present and monotonic from 0
        let seq0 = header(&recs[0], "seq");
        let seq1 = header(&recs[1], "seq");
        check!(
            (seq0, seq1, header(&recs[0], "prev_hash"))
                == (
                    Some("0".to_string()),
                    Some("1".to_string()),
                    Some("0".repeat(64)),
                )
        );
        // record 1 prev_hash == chain_hash(genesis, 0, value0)
        let expect = crate::chain::to_hex(&crate::chain::chain_hash(
            &crate::chain::GENESIS_HEAD,
            0,
            &recs[0].value,
        ));
        check!(header(&recs[1], "prev_hash") == Some(expect));
    }

    #[tokio::test]
    async fn checkpoints_emitted_by_count_and_verify_against_recomputed_head() {
        let (signer, pubkey) = test_signer();
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(64);
        let sink = Arc::new(MemorySink::default());
        // checkpoint every 2 records; long interval so only count triggers
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: Some(signer),
                checkpoint_every_n: 2,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        for i in 0..4 {
            log.emit(life(i));
        }
        drop(log); // closes channel -> final checkpoint (none pending here: 4 % 2 == 0)
        h.await.unwrap();

        let recs = sink.records();
        // 4 chained + 2 checkpoints (after record 2 and record 4)
        let checkpoints: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(checkpoints.len() == 2);

        // recompute the chain over the non-checkpoint records and verify each checkpoint
        let mut head = crate::chain::GENESIS_HEAD;
        let mut seq = 0u64;
        for r in &recs {
            if r.class == AuditEventClass::Checkpoint {
                let v: serde_json::Value = serde_json::from_slice(&r.value).unwrap();
                let cp = Checkpoint::from_value(&v).expect("cp");
                check!(
                    (cp.verify(&pubkey), cp.chain_head, cp.seq_high) == (true, head, Seq(seq - 1))
                );
            } else {
                head = crate::chain::chain_hash(&head, seq, &r.value);
                seq += 1;
            }
        }
    }

    #[tokio::test]
    async fn shutdown_emits_final_checkpoint_for_pending_tail() {
        let (signer, pubkey) = test_signer();
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, rx) = AuditLog::new(16);
        let sink = Arc::new(MemorySink::default());
        // every_n large so only the shutdown path emits
        let writer = AuditWriter::new(
            rx,
            AuditWriterParams {
                sink: sink.clone(),
                product: product(),
                signer: Some(signer),
                checkpoint_every_n: 1_000_000,
                checkpoint_every: DORMANT,
                chain: ChainState::new(),
                spool: Some(spool),
                stats: Arc::new(AuditStats::new()),
                replay_every: DORMANT,
                sleeper: Arc::new(MockSleeper::new()),
            },
        );
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));
        drop(log);
        h.await.unwrap();

        let recs = sink.records();
        let cps: Vec<_> = recs
            .iter()
            .filter(|r| r.class == AuditEventClass::Checkpoint)
            .collect();
        check!(cps.len() == 1); // single final checkpoint at shutdown
        let v: serde_json::Value = serde_json::from_slice(&cps[0].value).unwrap();
        let cp = Checkpoint::from_value(&v).unwrap();
        check!((cp.verify(&pubkey), cp.seq_high) == (true, Seq(2)));
    }

    #[tokio::test]
    async fn records_spool_on_sink_failure_then_replay_to_sink() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // topic "down"
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());

        log.emit(life(1));
        log.emit(life(2));
        log.emit(life(3));
        // wait until the writer has drained all three into the spool
        await_until("3 records spooled", || stats.spooled() >= 3).await;
        check!(stats.depth() >= 3);
        check!(sink.inner.records().is_empty()); // nothing reached the topic yet

        // topic recovers; fire the replay ticker by advancing the mock timeline
        sink.set_fail(false);
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("spool drained after replay", || stats.depth() == 0).await;

        drop(log);
        h.await.unwrap();

        // all three chained records reached the sink, in order, with monotonic seq
        let recs = sink.inner.records();
        let seqs: Vec<String> = recs
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .map(|r| header(r, "seq").unwrap())
            .collect();
        check!(
            (seqs, stats.replayed() >= 3, stats.depth())
                == (
                    vec!["0".to_string(), "1".to_string(), "2".to_string()],
                    true,
                    0
                )
        );
    }

    #[tokio::test]
    async fn direct_writes_when_sink_healthy_do_not_spool() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default()); // healthy
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(16);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let writer = AuditWriter::new(rx, params(sink.clone(), spool, stats.clone()));
        let h = tokio::spawn(writer.run());
        log.emit(life(1));
        log.emit(life(2));
        drop(log);
        h.await.unwrap();
        check!((sink.inner.records().len(), stats.spooled(), stats.depth()) == (2, 0, 0));
    }

    #[tokio::test]
    async fn checkpoint_is_spooled_in_spool_mode_and_replayed_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (signer, _pubkey) = test_signer();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // topic down → everything spools
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (mut p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        p.signer = Some(signer);
        p.checkpoint_every_n = 2; // emit a checkpoint after every 2 records
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());
        log.emit(life(0));
        log.emit(life(1)); // 2 records → triggers a checkpoint, all spooled
        // 2 chained records + 1 count-triggered checkpoint all land in the spool
        await_until("2 records + checkpoint spooled", || stats.spooled() >= 3).await;
        check!(sink.inner.records().is_empty()); // nothing on topic yet
        sink.set_fail(false); // recover → replay drains spool in order
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("spool drained after replay", || stats.depth() == 0).await;
        drop(log);
        h.await.unwrap();
        let recs = sink.inner.records();
        // exactly 2 chained records, and at least one checkpoint, and the checkpoint
        // appears AFTER both chained records (it was spooled + replayed in order).
        check!(
            recs.iter()
                .filter(|r| r.class != AuditEventClass::Checkpoint)
                .count()
                == 2
        );
        let cp_idx = recs
            .iter()
            .position(|r| r.class == AuditEventClass::Checkpoint)
            .expect("checkpoint present");
        let chained_before = recs[..cp_idx]
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .count();
        check!(chained_before == 2); // checkpoint comes after the 2 records it covers
    }

    #[tokio::test]
    async fn spool_overflow_drops_and_updates_stats() {
        let dir = tempfile::tempdir().unwrap();
        // size of one chained record, to cap the spool at ~1 record
        let one = {
            let d2 = tempfile::tempdir().unwrap();
            let mut s = Spool::open(d2.path(), ROOMY_CAP).unwrap();
            let mut rec = AuditRecord::from_event(&life(0), &product());
            rec.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
            s.append(&rec).unwrap();
            s.size()
        };
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true); // stay in spool mode (no replay), so drops accumulate
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), one).unwrap();
        let writer = AuditWriter::new(rx, params(sink.clone(), spool, stats.clone()));
        let h = tokio::spawn(writer.run());
        for i in 0..6 {
            log.emit(life(i));
        }
        // wait until all six events are accounted for (each is spooled or dropped)
        await_until("6 events processed", || {
            stats.spooled() + stats.dropped() >= 6
        })
        .await;
        drop(log);
        h.await.unwrap();
        // Strict bounds chosen to also kill the "return constant 1" mutants.
        assert2::check!(stats.dropped() >= 2); // many overflowed (kills inc_dropped/() , dropped->0/1)
        assert2::check!(stats.spool_bytes() > bytes(1)); // ~one record is buffered (kills spool_bytes->0/1)
    }

    #[tokio::test]
    async fn partial_replay_keeps_remainder_then_drains() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true);
        let stats = Arc::new(AuditStats::new());
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (p, timeline) = params_with_timeline(sink.clone(), spool, stats.clone());
        let writer = AuditWriter::new(rx, p);
        let h = tokio::spawn(writer.run());
        log.emit(life(0));
        log.emit(life(1));
        log.emit(life(2));
        await_until("3 records spooled", || stats.depth() == 3).await;

        // allow exactly 2 replay writes, then fail → partial replay
        sink.allow_n(2);
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("2 of 3 replayed", || {
            stats.replayed() == 2 && stats.depth() == 1
        })
        .await;
        check!(stats.depth() == 1); // remainder retained, still spooling

        // allow the rest; fire the replay ticker again to drain the remainder
        sink.allow_unlimited();
        timeline.advance(REPLAY_EVERY.to_std());
        await_until("remainder drained", || stats.depth() == 0).await;

        drop(log);
        h.await.unwrap();
        // all 3 chained records reached the sink exactly once, in seq order
        let seqs: Vec<String> = sink
            .inner
            .records()
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .map(|r| header(r, "seq").unwrap())
            .collect();
        check!(seqs == vec!["0".to_string(), "1".to_string(), "2".to_string()]);
    }
}
