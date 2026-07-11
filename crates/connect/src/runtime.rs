//! The [`ConnectorRuntime`]: an async driver that owns one [`Source`] and one
//! [`Sink`] and pipes records between them in a single process — no Connect
//! worker protocol, no REST, no broker required between the two ends. This is
//! the embeddable, single-binary shape (a ground-station / aircraft edge box):
//! build it programmatically, [`run`](ConnectorRuntime::run) it, and drive its
//! lifecycle through the returned [`ConnectorHandle`].
//!
//! ## The driver loop
//!
//! The runtime drives one sequential
//! `poll → put → commit → checkpoint → acknowledge` loop on the source. It is
//! deliberately *not* a two-task pipeline with a channel
//! between poll and put: [`Source::checkpoint`] reads the source's *live*
//! position, so the poll side must never run ahead of what the sink has made
//! durable — otherwise a persisted checkpoint would name records that were
//! never delivered. Backpressure is therefore intrinsic (the loop never polls
//! the next batch until the current one is committed) and bounded by
//! [`max_batch`](ConnectorRuntime::max_batch): at most that many records are buffered in
//! memory before they are pushed to the sink.
//!
//! Each interval:
//!
//! 1. Poll the source into a batch — up to `max_batch` records, or until the
//!    source reports caught-up ([`poll`](Source::poll) returns `None`), or the
//!    [`commit_interval`](ConnectorRuntime::commit_interval) deadline elapses.
//! 2. If the batch is non-empty: lazily [`begin`](Sink::begin) a transaction
//!    (only when the sink supports one — so an idle interval opens none),
//!    [`put`](Sink::put) the batch, [`commit`](Sink::commit) it (which for an
//!    at-least-once sink delegates to [`flush`](Sink::flush)).
//! 3. After the commit is durable, [`checkpoint`](Source::checkpoint) the source
//!    and persist it through the [`CheckpointStore`]. Only after that save
//!    succeeds does the runtime call [`acknowledge`](Source::acknowledge), so an
//!    upstream cursor advances only after the durable checkpoint names it. On
//!    restart the runtime [`seek`](Source::seek)s to this offset, so delivery
//!    resumes from the last fully-committed record.
//!
//! A put/commit failure on a transactional sink triggers a best-effort
//! [`abort`](Sink::abort) before the error propagates, so a half-written
//! interval is rolled back rather than leaked.
//!
//! ## Lifecycle
//!
//! [`run`](ConnectorRuntime::run) spawns the loop and hands back a
//! [`ConnectorHandle`]. The handle [`pause`](ConnectorHandle::pause)s and
//! [`resume`](ConnectorHandle::resume)s between intervals (an in-flight batch
//! always commits before the loop parks), and
//! [`shutdown`](ConnectorHandle::shutdown) performs a graceful drain: it commits
//! one final bounded batch of whatever is immediately available, checkpoints it,
//! then [`close`](Source::close)s both ends. These pause/resume + drain hooks are
//! the seam a contact-window scheduler plugs into.

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use qubit_clock::sleep::{AsyncSleeper, SystemSleeper};
use tokio::{sync::watch, task::JoinHandle};

use crate::{error::ConnectError, record::SourceOffset, sink::Sink, source::Source};

/// Where the runtime persists source [`checkpoint`](Source::checkpoint)s so a
/// restart can [`seek`](Source::seek) back to the last committed position.
///
/// The runtime owns a single source, so a store holds a single
/// [`SourceOffset`]. The default [`InMemoryCheckpointStore`] keeps it in memory
/// (durable only for the process lifetime); a real edge deployment supplies a
/// file- or NVRAM-backed implementation.
#[async_trait]
pub trait CheckpointStore: Send + Sync + 'static {
    /// Persist `offset` as the latest committed position. Called only after the
    /// records it covers are durable in the sink.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the offset cannot be persisted.
    async fn save(&self, offset: &SourceOffset) -> Result<(), ConnectError>;

    /// Load the last persisted position, or `None` if none was ever saved (a
    /// fresh start). Called once before the first [`poll`](Source::poll).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the stored offset cannot be read.
    async fn load(&self) -> Result<Option<SourceOffset>, ConnectError>;
}

/// In-memory [`CheckpointStore`]: holds the latest offset in a mutex. Survives
/// pause/resume and restart-within-process, but not a process restart — use it
/// for tests and for sources that re-derive their position from the backend.
#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    offset: Mutex<Option<SourceOffset>>,
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save(&self, offset: &SourceOffset) -> Result<(), ConnectError> {
        *self.offset.lock().expect("checkpoint mutex poisoned") = Some(offset.clone());
        Ok(())
    }

    async fn load(&self) -> Result<Option<SourceOffset>, ConnectError> {
        Ok(self
            .offset
            .lock()
            .expect("checkpoint mutex poisoned")
            .clone())
    }
}

/// Observable lifecycle state of a running connector, read via
/// [`ConnectorHandle::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    /// Spawned but not yet inside the loop (seeking to its checkpoint).
    Starting,
    /// Actively polling and writing.
    Running,
    /// Parked by [`pause`](ConnectorHandle::pause); committing nothing until
    /// [`resume`](ConnectorHandle::resume)d.
    Paused,
    /// Draining the remaining available records on the way to a clean stop.
    Draining,
    /// Stopped cleanly — the source drained and both ends were closed.
    Stopped,
    /// Stopped because the loop returned an error.
    Failed,
}

/// Internal control signal flowing from the handle to the driver loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    Run,
    Pause,
    Shutdown,
}

/// Tuning knobs for the driver loop.
#[derive(Debug, Clone, Copy)]
struct Config {
    commit_interval: Duration,
    max_batch: usize,
    poll_backoff: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            commit_interval: Duration::from_secs(5),
            max_batch: 500,
            poll_backoff: Duration::from_millis(100),
        }
    }
}

/// A programmatically-assembled connector: one source, one sink, and the policy
/// that brackets them. Build it with [`ConnectorRuntime::new`], wire the ends
/// with [`add_source`](ConnectorRuntime::add_source) /
/// [`add_sink`](ConnectorRuntime::add_sink), then
/// [`run`](ConnectorRuntime::run) it.
///
/// ```no_run
/// # use crabka_connect::runtime::ConnectorRuntime;
/// # async fn demo<S, K, K2>(source: S, sink: K2)
/// # where
/// #     S: crabka_connect::Source<K, K>,
/// #     K2: crabka_connect::Sink<K, K>,
/// #     K: Send + 'static,
/// # {
/// let handle = ConnectorRuntime::new()
///     .add_source(source)
///     .add_sink(sink)
///     .run();
/// // ... later ...
/// handle.shutdown().await.expect("clean drain");
/// # }
/// ```
pub struct ConnectorRuntime<K, V, S = NoSource, T = NoSink> {
    source: S,
    sink: T,
    checkpoints: Arc<dyn CheckpointStore>,
    config: Config,
    /// Drives the poll-backoff sleep. Kept out of `Config` (which is `Copy`) so
    /// production uses real time via [`SystemSleeper`] while tests inject a mock
    /// timeline.
    sleeper: Arc<dyn AsyncSleeper>,
    _marker: PhantomData<(K, V)>,
}

/// Typestate marker: no source has been added to the runtime yet.
/// [`run`](ConnectorRuntime::run) does not exist in this state.
pub struct NoSource;

/// Typestate marker: a source has been added and is ready to run.
pub struct HasSource<K, V>(Box<dyn Source<K, V>>);

/// Typestate marker: no sink has been added to the runtime yet.
/// [`run`](ConnectorRuntime::run) does not exist in this state.
pub struct NoSink;

/// Typestate marker: a sink has been added and is ready to run.
pub struct HasSink<K, V>(Box<dyn Sink<K, V>>);

impl<K, V> Default for ConnectorRuntime<K, V, NoSource, NoSink> {
    fn default() -> Self {
        Self {
            source: NoSource,
            sink: NoSink,
            checkpoints: Arc::new(InMemoryCheckpointStore::default()),
            config: Config::default(),
            sleeper: Arc::new(SystemSleeper::new()),
            _marker: PhantomData,
        }
    }
}

impl<K, V> ConnectorRuntime<K, V, NoSource, NoSink>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// A new, empty runtime with default policy and an in-memory checkpoint
    /// store. Add a source and a sink before [`run`](Self::run).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V, S, T> ConnectorRuntime<K, V, S, T>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Set the source the runtime polls. Replaces any previously-added source.
    #[must_use]
    pub fn add_source<Src: Source<K, V>>(
        self,
        source: Src,
    ) -> ConnectorRuntime<K, V, HasSource<K, V>, T> {
        ConnectorRuntime {
            source: HasSource(Box::new(source)),
            sink: self.sink,
            checkpoints: self.checkpoints,
            config: self.config,
            sleeper: self.sleeper,
            _marker: PhantomData,
        }
    }

    /// Set the sink the runtime writes to. Replaces any previously-added sink.
    #[must_use]
    pub fn add_sink<Snk: Sink<K, V>>(self, sink: Snk) -> ConnectorRuntime<K, V, S, HasSink<K, V>> {
        ConnectorRuntime {
            source: self.source,
            sink: HasSink(Box::new(sink)),
            checkpoints: self.checkpoints,
            config: self.config,
            sleeper: self.sleeper,
            _marker: PhantomData,
        }
    }

    /// Persist source checkpoints through `store` instead of the default
    /// in-memory one. Supply a durable store to survive process restarts.
    #[must_use]
    pub fn checkpoint_store(mut self, store: Arc<dyn CheckpointStore>) -> Self {
        self.checkpoints = store;
        self
    }

    /// How often to commit + checkpoint. The loop also commits early once a
    /// batch fills [`max_batch`](Self::max_batch) or the source catches up.
    /// Default: 5s.
    #[must_use]
    pub fn commit_interval(mut self, interval: Duration) -> Self {
        self.config.commit_interval = interval;
        self
    }

    /// The bounded-backpressure cap: the most records buffered in memory before
    /// the loop must push them to the sink. Default: 500. Clamped to at least 1.
    #[must_use]
    pub fn max_batch(mut self, max_batch: usize) -> Self {
        self.config.max_batch = max_batch.max(1);
        self
    }

    /// How long to back off after the source reports caught-up before polling
    /// again. Default: 100ms.
    #[must_use]
    pub fn poll_backoff(mut self, backoff: Duration) -> Self {
        self.config.poll_backoff = backoff;
        self
    }

    /// Drive the poll-backoff sleep through `sleeper` instead of the default
    /// [`SystemSleeper`]. Production leaves this as real time; tests inject a
    /// mock so the backoff cadence advances on a mock timeline deterministically
    /// instead of on the wall clock.
    #[must_use]
    pub fn sleeper(mut self, sleeper: Arc<dyn AsyncSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }
}

impl<K, V> ConnectorRuntime<K, V, HasSource<K, V>, HasSink<K, V>>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Spawn the driver loop and return a handle to control it.
    ///
    /// Must be called from within a Tokio runtime. The loop runs until
    /// [`shutdown`](ConnectorHandle::shutdown) (graceful drain) or a fatal
    /// error; an infinite source otherwise runs forever, backing off whenever it
    /// is momentarily caught up.
    #[must_use]
    pub fn run(self) -> ConnectorHandle {
        let source = self.source.0;
        let sink = self.sink.0;

        let (control_tx, control_rx) = watch::channel(Control::Run);
        let (state_tx, state_rx) = watch::channel(RuntimeState::Starting);

        let driver = Driver {
            source,
            sink,
            checkpoints: self.checkpoints,
            config: self.config,
            sleeper: self.sleeper,
            control: control_rx,
            state: state_tx,
        };
        let join = tokio::spawn(driver.run());

        ConnectorHandle {
            control: control_tx,
            state: state_rx,
            join: Some(join),
        }
    }
}

/// A control handle for a running [`ConnectorRuntime`]. Pause, resume, observe
/// state, and shut down gracefully. Dropping the handle without calling
/// [`shutdown`](Self::shutdown) signals a graceful drain to the orphaned loop so
/// it does not run forever.
pub struct ConnectorHandle {
    control: watch::Sender<Control>,
    state: watch::Receiver<RuntimeState>,
    join: Option<JoinHandle<Result<(), ConnectError>>>,
}

impl ConnectorHandle {
    /// Park the loop after the current interval commits. Idempotent; a no-op if
    /// the loop has already stopped.
    pub fn pause(&self) {
        let _ = self.control.send(Control::Pause);
    }

    /// Resume a [`pause`](Self::pause)d loop. Idempotent.
    pub fn resume(&self) {
        let _ = self.control.send(Control::Run);
    }

    /// The connector's current [`RuntimeState`].
    #[must_use]
    pub fn state(&self) -> RuntimeState {
        *self.state.borrow()
    }

    /// Gracefully drain and stop: commit one final bounded batch of whatever is
    /// immediately available, checkpoint it, close both ends, and await the
    /// loop's result.
    ///
    /// # Errors
    ///
    /// Returns the error the loop failed with, or [`ConnectError::Backend`] if
    /// the loop task panicked.
    #[tracing::instrument(level = "info", skip_all, err)]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn shutdown(mut self) -> Result<(), ConnectError> {
        let _ = self.control.send(Control::Shutdown);
        let join = self.join.take().expect("join handle taken once");
        match join.await {
            Ok(result) => result,
            Err(e) => Err(ConnectError::Backend(format!(
                "connector task panicked: {e}"
            ))),
        }
    }
}

impl Drop for ConnectorHandle {
    fn drop(&mut self) {
        // A handle dropped without `shutdown` would otherwise orphan a loop that
        // runs forever. Signal a graceful drain so the loop observes it at the
        // top of its next interval and stops on its own.
        let _ = self.control.send(Control::Shutdown);
    }
}

/// Whether an interval polled records or found the source caught up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Progress {
    Wrote,
    CaughtUp,
}

/// The owned state the spawned loop drives. Separated from the builder so the
/// builder's generic surface stays clean.
struct Driver<K, V> {
    source: Box<dyn Source<K, V>>,
    sink: Box<dyn Sink<K, V>>,
    checkpoints: Arc<dyn CheckpointStore>,
    config: Config,
    sleeper: Arc<dyn AsyncSleeper>,
    control: watch::Receiver<Control>,
    state: watch::Sender<RuntimeState>,
}

impl<K, V> Driver<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// The full lifecycle: seek to the stored checkpoint, run the loop, then
    /// close both ends regardless of how the loop ended.
    #[tracing::instrument(level = "info", skip_all, err)]
    async fn run(mut self) -> Result<(), ConnectError> {
        let result = self.seek_and_loop().await;

        // Close is best-effort cleanup; surface a close error only if the loop
        // itself succeeded (a loop error is the more interesting failure).
        let close = self.close_ends().await;
        let result = result.and(close);

        let _ = self.state.send(if result.is_ok() {
            RuntimeState::Stopped
        } else {
            RuntimeState::Failed
        });
        result
    }

    #[tracing::instrument(level = "info", skip_all, err)]
    async fn seek_and_loop(&mut self) -> Result<(), ConnectError> {
        if let Some(offset) = self.checkpoints.load().await? {
            tracing::debug!(?offset, "seeking source to restored checkpoint");
            self.source.seek(offset).await?;
        }
        self.main_loop().await
    }

    async fn main_loop(&mut self) -> Result<(), ConnectError> {
        loop {
            let control = *self.control.borrow_and_update();
            match control {
                Control::Run => {}
                Control::Pause => {
                    let _ = self.state.send(RuntimeState::Paused);
                    // Park until the control signal changes (resume / shutdown)
                    // or the handle is dropped (Err → treat as shutdown).
                    if self.control.changed().await.is_err() {
                        break;
                    }
                    continue;
                }
                Control::Shutdown => break,
            }

            let _ = self.state.send(RuntimeState::Running);
            if self.run_once().await? == Progress::CaughtUp {
                // Caught up: back off, but wake immediately on a control change.
                // The backoff sleep goes through the injected `AsyncSleeper`
                // (production: real time; tests: a mock timeline). The sleeper is
                // cloned into a local so its future borrows the local rather than
                // `self`, leaving `self.control` free for the `&mut self` wait.
                let sleeper = self.sleeper.clone();
                tokio::select! {
                    () = sleeper.sleep_for_async(self.config.poll_backoff) => {}
                    _ = self.control.changed() => {}
                }
            }
        }

        // Graceful drain: capture and commit one final bounded batch of
        // whatever is immediately available, advancing the checkpoint, then
        // stop. Each interval already commits atomically, so at most one batch
        // can be pending; a single pass suffices and guarantees termination
        // even for an unbounded source that never reports caught-up.
        let _ = self.state.send(RuntimeState::Draining);
        self.run_once().await?;
        Ok(())
    }

    /// Poll one bounded batch and, if non-empty, write + commit + checkpoint it.
    /// Returns whether the batch wrote anything or the source was caught up.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(batch = tracing::field::Empty, caught_up = tracing::field::Empty),
        err,
    )]
    async fn run_once(&mut self) -> Result<Progress, ConnectError> {
        let deadline = Instant::now() + self.config.commit_interval;
        let mut batch = Vec::new();
        let mut caught_up = false;

        while batch.len() < self.config.max_batch {
            if let Some(record) = self.source.poll().await? {
                batch.push(record);
            } else {
                caught_up = true;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
        }

        let span = tracing::Span::current();
        span.record("batch", batch.len());
        span.record("caught_up", caught_up);

        if !batch.is_empty() {
            self.write_committed(batch).await?;
        }

        Ok(if caught_up {
            Progress::CaughtUp
        } else {
            Progress::Wrote
        })
    }

    /// Write a non-empty batch inside the transactional gate (lazy `begin`),
    /// commit it, persist the source checkpoint, then acknowledge that offset.
    /// Aborts on failure when the sink is transactional.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(records = batch.len(), transactional = tracing::field::Empty),
        err,
    )]
    async fn write_committed(
        &mut self,
        batch: Vec<crate::record::ConnectRecord<K, V>>,
    ) -> Result<(), ConnectError> {
        let count = batch.len();
        let transactional = self.sink.supports_transactions();
        tracing::Span::current().record("transactional", transactional);

        // The gate is lazy: only a non-empty batch opens a transaction, so an
        // idle interval never churns an empty txn.
        if transactional {
            self.sink.begin().await?;
        }

        if let Err(e) = self.deliver(batch).await {
            if transactional {
                // Best-effort rollback of the half-written interval; surface the
                // original delivery error, not a secondary abort failure.
                if let Err(abort_err) = self.sink.abort().await {
                    tracing::warn!(error = %abort_err, "sink abort failed after delivery error");
                }
            }
            return Err(e);
        }

        // Only once the records are durable may the checkpoint advance — so a
        // restart resumes from the last fully-committed record, never past it.
        if let Some(offset) = self.source.checkpoint() {
            self.checkpoints.save(&offset).await?;
            self.source.acknowledge(&offset).await?;
        }
        tracing::debug!(records = count, transactional, "committed connector batch");
        Ok(())
    }

    /// `put` then `commit` the batch (commit delegates to `flush` for an
    /// at-least-once sink), as one fallible unit so the caller can roll back.
    async fn deliver(
        &mut self,
        batch: Vec<crate::record::ConnectRecord<K, V>>,
    ) -> Result<(), ConnectError> {
        self.sink.put(batch).await?;
        self.sink.commit().await
    }

    #[tracing::instrument(level = "info", skip_all, err)]
    async fn close_ends(&mut self) -> Result<(), ConnectError> {
        let source_close = self.source.close().await;
        let sink_close = self.sink.close().await;
        source_close.and(sink_close)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::check;
    use async_trait::async_trait;
    use bytes::Bytes;
    use qubit_clock::{MockTimeline, sleep::MockSleeper};
    use tokio::sync::mpsc;

    use super::*;
    use crate::record::{ConnectRecord, OffsetMap, OffsetValue};

    /// Yield until `cond` holds, letting a spawned task make progress between
    /// checks without sleeping on real time. The bound makes a stuck condition
    /// fail the test fast instead of hanging it.
    async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..100_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition never held: {what}");
    }

    /// A source that yields a fixed list of values once, tracking its position
    /// as the index, then reports caught-up forever.
    struct VecSource {
        records: Vec<Bytes>,
        pos: usize,
    }

    impl VecSource {
        fn new(values: &[&'static [u8]]) -> Self {
            Self {
                records: values.iter().map(|v| Bytes::from_static(v)).collect(),
                pos: 0,
            }
        }
    }

    #[async_trait]
    impl Source<Bytes, Bytes> for VecSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            let Some(v) = self.records.get(self.pos).cloned() else {
                return Ok(None);
            };
            self.pos += 1;
            Ok(Some(ConnectRecord::new(None, Some(v))))
        }

        fn checkpoint(&self) -> Option<SourceOffset> {
            if self.pos == 0 {
                return None;
            }
            let mut position = OffsetMap::new();
            let index = i64::try_from(self.pos).expect("test source position fits in i64");
            position.insert("index".into(), OffsetValue::Long(index));
            Some(SourceOffset::new(OffsetMap::new().into(), position.into()))
        }

        async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError> {
            match offset.position.get("index") {
                Some(OffsetValue::Long(i)) => {
                    self.pos = usize::try_from(*i).unwrap();
                    Ok(())
                }
                _ => Err(ConnectError::Offset("missing index".into())),
            }
        }
    }

    /// A source that never produces but counts how often it was polled — used to
    /// prove pause stops polling.
    struct CountingIdleSource {
        polls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Source<Bytes, Bytes> for CountingIdleSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
        fn checkpoint(&self) -> Option<SourceOffset> {
            None
        }
        async fn seek(&mut self, _offset: SourceOffset) -> Result<(), ConnectError> {
            Ok(())
        }
    }

    /// A sink that forwards every delivered value over a channel and records the
    /// transactional bracket calls + the size of each `put`, so a test can
    /// assert on both the gate and the batch boundaries the loop chose.
    struct ChannelSink {
        tx: mpsc::UnboundedSender<Bytes>,
        transactional: bool,
        begins: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
        puts: Arc<Mutex<Vec<usize>>>,
        staged: Vec<Bytes>,
    }

    #[async_trait]
    impl Sink<Bytes, Bytes> for ChannelSink {
        async fn put(
            &mut self,
            records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            self.puts.lock().unwrap().push(records.len());
            self.staged
                .extend(records.into_iter().filter_map(|r| r.value));
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ConnectError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            for v in self.staged.drain(..) {
                let _ = self.tx.send(v);
            }
            Ok(())
        }

        fn supports_transactions(&self) -> bool {
            self.transactional
        }

        async fn begin(&mut self) -> Result<(), ConnectError> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn abort(&mut self) -> Result<(), ConnectError> {
            self.staged.clear();
            Ok(())
        }
    }

    fn channel_sink(transactional: bool) -> (ChannelSink, mpsc::UnboundedReceiver<Bytes>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            ChannelSink {
                tx,
                transactional,
                begins: Arc::new(AtomicUsize::new(0)),
                commits: Arc::new(AtomicUsize::new(0)),
                puts: Arc::new(Mutex::new(Vec::new())),
                staged: Vec::new(),
            },
            rx,
        )
    }

    /// Receive `n` records, failing fast if any does not arrive promptly. The
    /// bound is what makes a delivery-suppressing regression fail the test in
    /// seconds rather than hang it — without it a no-op `put`/`commit` would
    /// block `recv` forever.
    async fn collect(rx: &mut mpsc::UnboundedReceiver<Bytes>, n: usize) -> Vec<Bytes> {
        let mut out = Vec::new();
        for _ in 0..n {
            let rec = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("record delivered within 5s")
                .expect("sink channel stayed open");
            out.push(rec);
        }
        out
    }

    /// Shut down with a bound, so a regression that makes the drain loop never
    /// terminate fails the test fast instead of hanging it.
    async fn shutdown(handle: ConnectorHandle) -> Result<(), ConnectError> {
        tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
            .await
            .expect("runtime shut down within 10s")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipes_source_records_to_sink_in_order() {
        let (sink, mut rx) = channel_sink(false);
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a", b"b", b"c"]))
            .add_sink(sink)
            .poll_backoff(Duration::from_millis(5))
            .run();

        let got = collect(&mut rx, 3).await;
        check!(
            got == vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]
        );
        shutdown(handle).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transactional_sink_brackets_each_nonempty_commit() {
        let (sink, mut rx) = channel_sink(true);
        let begins = sink.begins.clone();
        let commits = sink.commits.clone();
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"x"]))
            .add_sink(sink)
            .poll_backoff(Duration::from_millis(5))
            .run();

        let got = collect(&mut rx, 1).await;
        check!(got == vec![Bytes::from_static(b"x")]);
        shutdown(handle).await.unwrap();

        // The one non-empty interval opened exactly one transaction and
        // committed it; idle backoff intervals opened none (begins == commits).
        check!(
            (
                begins.load(Ordering::SeqCst),
                commits.load(Ordering::SeqCst)
            ) == (1, 1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_persists_and_restart_resumes_after_it() {
        let store = Arc::new(InMemoryCheckpointStore::default());

        let (sink, mut rx) = channel_sink(false);
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a", b"b"]))
            .add_sink(sink)
            .checkpoint_store(store.clone())
            .poll_backoff(Duration::from_millis(5))
            .run();
        let _ = collect(&mut rx, 2).await;
        shutdown(handle).await.unwrap();

        // The persisted checkpoint names the drained position.
        let saved = store.load().await.unwrap().expect("checkpoint saved");
        check!(saved.position.get("index") == Some(&OffsetValue::Long(2)));

        // A fresh runtime over the same store + a full source seeks past the
        // already-delivered records and produces nothing new.
        let (sink2, mut rx2) = channel_sink(false);
        let handle2 = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a", b"b"]))
            .add_sink(sink2)
            .checkpoint_store(store.clone())
            .poll_backoff(Duration::from_millis(5))
            .run();
        // Give the loop time to seek + poll a couple of backoff cycles.
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown(handle2).await.unwrap();
        check!(rx2.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pause_stops_polling_and_resume_restarts_it() {
        let polls = Arc::new(AtomicUsize::new(0));
        let (sink, _rx) = channel_sink(false);
        let handle = ConnectorRuntime::new()
            .add_source(CountingIdleSource {
                polls: polls.clone(),
            })
            .add_sink(sink)
            .poll_backoff(Duration::from_millis(5))
            .run();

        // Let it poll a few times, then pause and let the loop park.
        tokio::time::sleep(Duration::from_millis(40)).await;
        handle.pause();
        tokio::time::sleep(Duration::from_millis(20)).await;
        check!(handle.state() == RuntimeState::Paused);

        let paused_count = polls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(40)).await;
        // No further polls while paused.
        check!(polls.load(Ordering::SeqCst) == paused_count);

        handle.resume();
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Polling resumed.
        check!(polls.load(Ordering::SeqCst) > paused_count);
        shutdown(handle).await.unwrap();
    }

    /// A sink whose `put` always fails, to prove a transactional failure aborts.
    struct FailingSink {
        aborts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Sink<Bytes, Bytes> for FailingSink {
        async fn put(
            &mut self,
            _records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            Err(ConnectError::Backend("write rejected".into()))
        }
        async fn flush(&mut self) -> Result<(), ConnectError> {
            Ok(())
        }
        fn supports_transactions(&self) -> bool {
            true
        }
        async fn abort(&mut self) -> Result<(), ConnectError> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delivery_failure_aborts_transaction_and_fails_runtime() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a"]))
            .add_sink(FailingSink {
                aborts: aborts.clone(),
            })
            .poll_backoff(Duration::from_millis(5))
            .run();

        // The loop fails on the first delivery; shutdown surfaces that error.
        let result = shutdown(handle).await;
        check!((result.is_err(), aborts.load(Ordering::SeqCst)) == (true, 1));
    }

    /// A source whose `seek` always fails — to prove a restored checkpoint the
    /// source cannot resume from fails the runtime at startup.
    struct SeekFailSource;

    #[async_trait]
    impl Source<Bytes, Bytes> for SeekFailSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            Ok(None)
        }
        fn checkpoint(&self) -> Option<SourceOffset> {
            None
        }
        async fn seek(&mut self, _offset: SourceOffset) -> Result<(), ConnectError> {
            Err(ConnectError::Offset(
                "upstream truncated past offset".into(),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restored_checkpoint_seek_failure_fails_runtime() {
        // A store with a saved offset triggers `seek` before the first poll.
        let store = Arc::new(InMemoryCheckpointStore::default());
        store.save(&SourceOffset::default()).await.unwrap();

        let handle = ConnectorRuntime::new()
            .add_source(SeekFailSource)
            .add_sink(channel_sink(false).0)
            .checkpoint_store(store)
            .run();
        check!(shutdown(handle).await.is_err());
    }

    /// A checkpoint store that fails the requested direction, to exercise the
    /// runtime's load (startup) and save (post-commit) error paths.
    struct FailingCheckpointStore {
        fail_load: bool,
    }

    #[async_trait]
    impl CheckpointStore for FailingCheckpointStore {
        async fn save(&self, _offset: &SourceOffset) -> Result<(), ConnectError> {
            if self.fail_load {
                Ok(())
            } else {
                Err(ConnectError::Backend("save rejected".into()))
            }
        }
        async fn load(&self) -> Result<Option<SourceOffset>, ConnectError> {
            if self.fail_load {
                Err(ConnectError::Backend("load rejected".into()))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_load_error_fails_runtime_at_startup() {
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a"]))
            .add_sink(channel_sink(false).0)
            .checkpoint_store(Arc::new(FailingCheckpointStore { fail_load: true }))
            .run();
        check!(shutdown(handle).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_save_error_after_commit_fails_runtime() {
        // The batch is delivered, but persisting its checkpoint fails — the
        // runtime must surface that rather than silently advancing.
        let (sink, mut rx) = channel_sink(false);
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a"]))
            .add_sink(sink)
            .checkpoint_store(Arc::new(FailingCheckpointStore { fail_load: false }))
            .poll_backoff(Duration::from_millis(5))
            .run();
        check!(collect(&mut rx, 1).await == vec![Bytes::from_static(b"a")]);
        check!(shutdown(handle).await.is_err());
    }

    struct OrderingSource {
        emitted: bool,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl Source<Bytes, Bytes> for OrderingSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            if self.emitted {
                return Ok(None);
            }
            self.emitted = true;
            Ok(Some(ConnectRecord::new(
                None,
                Some(Bytes::from_static(b"a")),
            )))
        }

        fn checkpoint(&self) -> Option<SourceOffset> {
            self.emitted.then(|| {
                let mut position = OffsetMap::new();
                position.insert("index".into(), OffsetValue::Long(1));
                SourceOffset::new(OffsetMap::new().into(), position.into())
            })
        }

        async fn seek(&mut self, _offset: SourceOffset) -> Result<(), ConnectError> {
            Ok(())
        }

        async fn acknowledge(&mut self, _offset: &SourceOffset) -> Result<(), ConnectError> {
            self.events.lock().unwrap().push("acknowledge");
            Ok(())
        }
    }

    struct OrderingSink {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl Sink<Bytes, Bytes> for OrderingSink {
        async fn put(
            &mut self,
            _records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            self.events.lock().unwrap().push("put");
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ConnectError> {
            self.events.lock().unwrap().push("commit");
            Ok(())
        }
    }

    struct OrderingCheckpointStore {
        events: Arc<Mutex<Vec<&'static str>>>,
        saved: Mutex<Option<SourceOffset>>,
    }

    #[async_trait]
    impl CheckpointStore for OrderingCheckpointStore {
        async fn save(&self, offset: &SourceOffset) -> Result<(), ConnectError> {
            self.events.lock().unwrap().push("checkpoint_save");
            *self.saved.lock().unwrap() = Some(offset.clone());
            Ok(())
        }

        async fn load(&self) -> Result<Option<SourceOffset>, ConnectError> {
            Ok(self.saved.lock().unwrap().clone())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_acknowledge_runs_after_sink_commit_and_checkpoint_save() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(OrderingCheckpointStore {
            events: events.clone(),
            saved: Mutex::new(None),
        });
        let handle = ConnectorRuntime::new()
            .add_source(OrderingSource {
                emitted: false,
                events: events.clone(),
            })
            .add_sink(OrderingSink {
                events: events.clone(),
            })
            .checkpoint_store(store)
            .poll_backoff(Duration::from_millis(5))
            .run();

        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown(handle).await.unwrap();

        check!(
            events.lock().unwrap().as_slice()
                == ["put", "commit", "checkpoint_save", "acknowledge"]
        );
    }

    /// A sink whose `close` fails, to prove a close error surfaces from an
    /// otherwise-clean run.
    struct CloseFailSink;

    #[async_trait]
    impl Sink<Bytes, Bytes> for CloseFailSink {
        async fn put(
            &mut self,
            _records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), ConnectError> {
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ConnectError> {
            Err(ConnectError::Backend("close rejected".into()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_failure_surfaces_after_clean_run() {
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a"]))
            .add_sink(CloseFailSink)
            .poll_backoff(Duration::from_millis(5))
            .run();
        // The run drains cleanly; closing the sink fails, so shutdown reports it.
        check!(shutdown(handle).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_caught_up_interval_batches_all_available_records() {
        // With a commit interval far larger than the run, a source that catches
        // up mid-interval delivers everything it had in a SINGLE put — proving
        // the loop polls until caught-up (not one record per interval) and that
        // the deadline is in the future (`now + interval`), not the past.
        let (sink, mut rx) = channel_sink(false);
        let puts = sink.puts.clone();
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a", b"b", b"c"]))
            .add_sink(sink)
            .max_batch(100)
            .commit_interval(Duration::from_secs(30))
            .poll_backoff(Duration::from_millis(5))
            .run();
        let _ = collect(&mut rx, 3).await;
        shutdown(handle).await.unwrap();
        check!(*puts.lock().unwrap() == vec![3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn max_batch_caps_each_put() {
        // A cap of 2 over four available records forces the loop to break the
        // batch at the bound, so no put exceeds 2 (and every record still flows).
        let (sink, mut rx) = channel_sink(false);
        let puts = sink.puts.clone();
        let handle = ConnectorRuntime::new()
            .add_source(VecSource::new(&[b"a", b"b", b"c", b"d"]))
            .add_sink(sink)
            .max_batch(2)
            .commit_interval(Duration::from_secs(30))
            .poll_backoff(Duration::from_millis(5))
            .run();
        let _ = collect(&mut rx, 4).await;
        shutdown(handle).await.unwrap();
        let sizes = puts.lock().unwrap().clone();
        check!((sizes.iter().all(|&n| n <= 2), sizes.iter().sum::<usize>()) == (true, 4));
    }

    #[tokio::test]
    async fn idle_source_backs_off_between_polls() {
        // A caught-up source must be polled on the backoff cadence, not spun on.
        // Injecting a mock timeline makes this exact rather than fuzzy: the loop
        // polls once, finds the source caught up, and parks on the backoff sleep;
        // thereafter each advance of one `poll_backoff` period releases exactly
        // one further poll. No real time elapses.
        const ADVANCES: usize = 5;
        let polls = Arc::new(AtomicUsize::new(0));
        let (sink, _rx) = channel_sink(false);
        let backoff = Duration::from_millis(50);
        let sleeper = MockSleeper::new();
        let timeline: MockTimeline = sleeper.timeline();
        let handle = ConnectorRuntime::new()
            .add_source(CountingIdleSource {
                polls: polls.clone(),
            })
            .add_sink(sink)
            .poll_backoff(backoff)
            .sleeper(Arc::new(sleeper))
            .run();

        // First poll: the loop reaches the select and parks on the backoff sleep
        // (the mock waiter registers as the future is created).
        await_until("first idle poll", || polls.load(Ordering::SeqCst) >= 1).await;

        // Each advance of one backoff period wakes the parked sleep, letting the
        // loop poll exactly once more before parking again.
        for i in 0..ADVANCES {
            timeline.advance(backoff);
            let expected = i + 2;
            await_until("poll after backoff advance", || {
                polls.load(Ordering::SeqCst) >= expected
            })
            .await;
        }

        // Deterministic: exactly the initial poll plus one per advance — the loop
        // never spins, because it stays parked on a sleep the timeline has not
        // yet passed.
        check!(polls.load(Ordering::SeqCst) == ADVANCES + 1);
        shutdown(handle).await.unwrap();
    }

    /// A finite source that signals over a channel when it is closed, so a test
    /// can observe that the loop reached its clean-shutdown path.
    struct ClosingSource {
        records: Vec<Bytes>,
        pos: usize,
        closed: mpsc::UnboundedSender<()>,
    }

    #[async_trait]
    impl Source<Bytes, Bytes> for ClosingSource {
        async fn poll(&mut self) -> Result<Option<ConnectRecord<Bytes, Bytes>>, ConnectError> {
            let Some(v) = self.records.get(self.pos).cloned() else {
                return Ok(None);
            };
            self.pos += 1;
            Ok(Some(ConnectRecord::new(None, Some(v))))
        }
        fn checkpoint(&self) -> Option<SourceOffset> {
            None
        }
        async fn seek(&mut self, _offset: SourceOffset) -> Result<(), ConnectError> {
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ConnectError> {
            let _ = self.closed.send(());
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_handle_drains_and_closes_the_source() {
        // Dropping the handle without `shutdown` must still signal a graceful
        // stop, so the loop drains, closes both ends, and does not run forever.
        let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
        let (sink, _rx) = channel_sink(false);
        let handle = ConnectorRuntime::new()
            .add_source(ClosingSource {
                records: vec![Bytes::from_static(b"a")],
                pos: 0,
                closed: closed_tx,
            })
            .add_sink(sink)
            .poll_backoff(Duration::from_millis(5))
            .run();

        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), closed_rx.recv())
            .await
            .expect("source closed within 5s of dropping the handle")
            .expect("close signal received");
    }
}
