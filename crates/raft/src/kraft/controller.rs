//! The async `KraftController` consensus engine: a single owning tokio task
//! holds all consensus state (the 3a [`QuorumStateMachine`] core, the 3b
//! [`KraftLog`], and the published [`MetadataImage`]) and turns inbound
//! commands/RPCs into core [`Event`]s whose [`Action`]s it executes.
//!
//! Ownership model: one task owns the [`Engine`]; everything else talks to it
//! over an mpsc of [`Command`]. The public [`KraftController`] handle is a
//! cheap clone holding the command sender plus the `watch` receivers. This
//! a single-owner actor pattern; the engine is entirely ours.
//!
//! ## Concurrency / no-inline-await invariant
//!
//! The loop is single-threaded over all consensus state, so it never blocks on
//! a peer RPC. Each `Send*` [`Action`] is dispatched **fire-and-forget**: a
//! [`tokio::spawn`]ed task calls [`PeerSender::send`], decodes the response
//! body into the matching `Receive*Response` [`Event`], and posts it back to
//! the loop via a clone of the command sender. This is critical for the
//! in-process multi-node sim, where engines RPC each other reciprocally — a
//! loop that awaited a send inline would deadlock.
//!
//! ## Timers & liveness (Task 3)
//!
//! The loop drives a real monotonic clock and `select!`s over the mpsc plus an
//! election timer, a fetch timer, and a leader heartbeat interval:
//! - on a role transition the now-irrelevant timer is cancelled (a follower has
//!   no election timer; a leader has no fetch timer and runs the heartbeat);
//! - a fetch-timer expiry while the leader is still reachable RE-POLLS
//!   (`SendFetch`), it does not elect; only [`FETCH_MISS_LIMIT`] consecutive
//!   misses feed `Event::FetchTimeout` to start an election;
//! - the leader re-broadcasts `BeginQuorumEpoch` to voters each heartbeat tick.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::BufMut;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

use crabka_metadata::{MetadataImage, from_kafka_record};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::kraft::action::{Action, TimerKind};
use crate::kraft::core::QuorumStateMachine;
use crate::kraft::event::{Event, LogEnd};
use crate::kraft::log::KraftLog;
use crate::kraft::role::Role;
use crate::kraft::transport::{
    Command, Inbound, MetadataFetchSlice, PeerSender, QuorumStateSnapshot, TimerTick, api_key, wire,
};
use crate::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};

/// Consecutive fetch-timer misses a follower tolerates before electing. A
/// single miss re-polls (the leader may just be slow); a sustained loss of
/// contact (this many in a row) feeds `Event::FetchTimeout` to elect.
const FETCH_MISS_LIMIT: u32 = 3;

/// Leader heartbeat interval as a fraction of the election timeout. The leader
/// re-broadcasts `BeginQuorumEpoch` this often so followers that lost the
/// initial announcement (or a rejoining old leader) re-attach without waiting
/// for an election.
const HEARTBEAT_DIVISOR: u64 = 3;

/// Filename of the node-local durable quorum-state file.
const QUORUM_STATE_FILE: &str = "quorum-state";

/// Subdirectory under the data dir holding KIP-630 `.checkpoint` artifacts for
/// the single metadata partition. Matches the on-disk layout the broker's
/// `FetchSnapshot` handler and broker-only observers expect.
const METADATA_SUBDIR: &str = "@metadata-0";

/// The checkpoint directory for a controller rooted at `data_dir`.
#[must_use]
pub fn checkpoint_dir(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join(METADATA_SUBDIR)
}

/// All consensus state owned by the single engine task.
struct Engine {
    me: NodeId,
    core: QuorumStateMachine,
    log: KraftLog,
    image: MetadataImage,
    peers: Arc<dyn PeerSender>,
    /// Publishes the latest applied [`MetadataImage`] to readers.
    image_tx: watch::Sender<Arc<MetadataImage>>,
    /// Publishes the current leader id (None while unknown / election running).
    leader_tx: watch::Sender<Option<NodeId>>,
    /// Publishes a structured consensus snapshot for the handle's synchronous
    /// `quorum_state()` (the broker's `DescribeQuorum` reads it without an mpsc
    /// round-trip).
    quorum_tx: watch::Sender<QuorumStateSnapshot>,
    /// Clone of the command sender, handed to fire-and-forget send tasks so
    /// they can post the decoded `Receive*Response` event back to the loop.
    cmd_tx: mpsc::Sender<Command>,
    /// Directory holding the metadata log + checkpoints + quorum-state file.
    data_dir: PathBuf,
    /// Monotonic clock base: `SimInstant(ms)` is `(now - base).as_millis()`.
    clock_base: Instant,
    /// Base election timeout in ms (varied per node by the caller for liveness).
    election_timeout_ms: u64,
    /// Pending timer deadlines as `tokio::time::Instant`s. `None` = disarmed.
    election_at: Option<Instant>,
    fetch_at: Option<Instant>,
    /// Consecutive fetch misses while still believing in a leader.
    fetch_misses: u32,
    /// Outstanding `submit_change` waiters keyed by the end offset they need
    /// committed+applied. Resolved (Ok or per-record rejection) on apply.
    commit_waiters: Vec<CommitWaiter>,
    /// Whether we held leadership as of the last reconcile, and at what epoch.
    /// Used to detect a leadership-loss edge (Leader → non-Leader, or a
    /// leader-epoch bump while still nominally leading) so we can fail parked
    /// `submit_change` waiters instead of leaving them hung (FIX 1).
    was_leader: bool,
    held_epoch: LeaderEpoch,
}

/// A parked `submit_change`: it completes once the HWM reaches `need_offset`
/// AND the records have been run through `validate`/`apply`.
struct CommitWaiter {
    /// Base (append) offset of this waiter's batch. Its appended range is
    /// `[base_offset, need_offset)`; a committed-record rejection only attaches
    /// to a waiter whose range actually contains the failing offset (FIX 2).
    base_offset: i64,
    need_offset: i64,
    /// First per-record rejection observed at apply time, if any.
    rejection: Option<RaftError>,
    reply: oneshot::Sender<Result<(), RaftError>>,
}

/// Cheap, cloneable handle to the running engine: holds the command sender and
/// the `watch` receivers the broker/handle read.
#[derive(Clone)]
pub struct KraftController {
    cmd_tx: mpsc::Sender<Command>,
    image_rx: watch::Receiver<Arc<MetadataImage>>,
    leader_rx: watch::Receiver<Option<NodeId>>,
    quorum_rx: watch::Receiver<QuorumStateSnapshot>,
    me: NodeId,
}

/// Configuration to build a [`KraftController`].
pub struct KraftConfig {
    pub me: NodeId,
    pub cluster_id: Uuid,
    pub initial_state: QuorumState,
    pub election_timeout_ms: u64,
    pub peers: Arc<dyn PeerSender>,
}

impl KraftController {
    /// Build the engine over an already-opened [`KraftLog`] and spawn its loop
    /// task. Recovery (snapshot + replay + quorum-state file) is wired by
    /// [`Self::open`]; this lower-level entrypoint takes the seed state directly
    /// and is used by tests/drivers that supply their own [`KraftLog`].
    ///
    /// The returned handle's loop runs until [`Self::shutdown`] (or all handles
    /// drop). `data_dir` is where the engine writes the quorum-state file and
    /// checkpoints.
    #[must_use]
    pub fn spawn(config: KraftConfig, log: KraftLog, data_dir: PathBuf) -> Self {
        let cluster_id = config.cluster_id;
        let image = MetadataImage::new(cluster_id);
        Self::spawn_with_image(config, log, data_dir, image)
    }

    /// Spawn the engine starting from an already-recovered [`MetadataImage`]
    /// (the restart-recovery path through [`Self::open`] threads the rebuilt
    /// image in here so the published `current_image` reflects it immediately).
    fn spawn_with_image(
        config: KraftConfig,
        log: KraftLog,
        data_dir: PathBuf,
        image: MetadataImage,
    ) -> Self {
        let KraftConfig {
            me,
            cluster_id: _,
            initial_state,
            election_timeout_ms,
            peers,
        } = config;

        let core = QuorumStateMachine::new(me, initial_state, election_timeout_ms);
        let initial_leader = core.quorum_state().leader_id;
        let initial_was_leader = core.role().is_leader();
        let initial_epoch = core.quorum_state().leader_epoch;

        let (image_tx, image_rx) = watch::channel(Arc::new(image.clone()));
        let (leader_tx, leader_rx) = watch::channel(initial_leader);
        let initial_snapshot = QuorumStateSnapshot {
            leader_id: initial_leader,
            leader_epoch: initial_epoch,
            high_watermark: log.hwm(),
            log_end_offset: log.log_end_offset(),
            voters: initial_state_voters(&core),
            per_voter_fetch_offset: std::collections::BTreeMap::new(),
        };
        let (quorum_tx, quorum_rx) = watch::channel(initial_snapshot);
        let (cmd_tx, cmd_rx) = mpsc::channel(256);

        let clock_base = Instant::now();
        // A fresh voter arms its election timer so a bootstrap cluster elects
        // without an injected event. Observers/followers leave it disarmed.
        let election_at = if core.is_voter() && initial_leader.is_none() {
            Some(clock_base + Duration::from_millis(election_timeout_ms))
        } else {
            None
        };

        let engine = Engine {
            me,
            core,
            log,
            image,
            peers,
            image_tx,
            leader_tx,
            quorum_tx,
            cmd_tx: cmd_tx.clone(),
            data_dir,
            clock_base,
            election_timeout_ms,
            election_at,
            fetch_at: None,
            fetch_misses: 0,
            commit_waiters: Vec::new(),
            was_leader: initial_was_leader,
            held_epoch: initial_epoch,
        };

        tokio::spawn(engine.run(cmd_rx));

        Self {
            cmd_tx,
            image_rx,
            leader_rx,
            quorum_rx,
            me,
        }
    }

    /// Open the engine over `data_dir`: recover the [`MetadataImage`] from the
    /// latest checkpoint + replay committed log batches, and seed the durable
    /// [`QuorumState`] from the node-local quorum-state file (Task 5). The
    /// `bootstrap` voter set/cluster id is used only when no quorum-state file
    /// exists yet.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log/checkpoint cannot be opened or read.
    pub fn open(
        data_dir: PathBuf,
        me: NodeId,
        cluster_id: Uuid,
        bootstrap_voters: crabka_metadata::voters::VoterSet,
        election_timeout_ms: u64,
        peers: Arc<dyn PeerSender>,
    ) -> Result<Self, RaftError> {
        std::fs::create_dir_all(&data_dir).map_err(crabka_log::LogError::Io)?;
        let mut log = KraftLog::open(&data_dir)?;

        // Recover the image: latest checkpoint, then replay committed batches
        // past it. The committed prefix is the whole log on a clean restart
        // (the HWM is not persisted separately; the log only holds committed
        // metadata in this slice, so we apply the full log end).
        let mut image = MetadataImage::new(cluster_id);
        if let Some(bytes) = load_latest_checkpoint(&checkpoint_dir(&data_dir))? {
            let records = crate::snapshot::SnapshotReader::read_records(&bytes)?;
            image = MetadataImage::from_records(cluster_id, &records);
            // Checkpoints in this slice cover the in-memory image, not a log
            // prefix offset, so replay the full log on top (idempotent:
            // duplicate records fail validate and are skipped). A precise
            // checkpoint-offset cursor lands with FetchSnapshot (Slice 3d).
        }
        replay_committed(&log, &mut image, 0);
        log.advance_hwm(log.log_end_offset());

        // Seed the durable quorum state from the file, falling back to a fresh
        // bootstrap when absent.
        let initial_state = load_quorum_state(&data_dir, cluster_id, &bootstrap_voters)?
            .unwrap_or_else(|| QuorumState::bootstrap(cluster_id, bootstrap_voters));

        Ok(Self::spawn_with_image(
            KraftConfig {
                me,
                cluster_id,
                initial_state,
                election_timeout_ms,
                peers,
            },
            log,
            data_dir,
            image,
        ))
    }

    /// The node id this controller runs as.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.me
    }

    /// A snapshot of the latest applied [`MetadataImage`].
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.image_rx.borrow().clone()
    }

    /// Watch the published [`MetadataImage`].
    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_rx.clone()
    }

    /// Watch the current leader id.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_rx.clone()
    }

    /// A synchronous snapshot of consensus state (the handle's `quorum_state()`
    /// reads this without an mpsc round-trip — the engine republishes it on
    /// every event). Cheap `watch` borrow + clone.
    #[must_use]
    pub fn quorum_snapshot(&self) -> QuorumStateSnapshot {
        self.quorum_rx.borrow().clone()
    }

    /// Submit a metadata change. On the leader, appends the batch at the current
    /// leader epoch and returns once it is committed (HWM ≥ the appended end
    /// offset) AND applied, surfacing the first per-record rejection. On a
    /// follower, returns [`RaftError::NotLeader`] with the leader hint (the
    /// handle layer forwards via `forward_submit_to` — Task 8).
    ///
    /// # Errors
    /// - [`RaftError::Metadata`] if a record fails `validate`.
    /// - [`RaftError::NotLeader`] if this node is not the leader.
    /// - [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubmitChange { records, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// A structured snapshot of consensus state for the broker's
    /// `DescribeQuorum` admin view.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn quorum_state(&self) -> Result<QuorumStateSnapshot, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::QuorumStateSnapshot { reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    /// Read a committed `__cluster_metadata` slice for an observer's
    /// `API_KEY_METADATA_FETCH` (1004).
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn metadata_fetch(
        &self,
        fetch_offset: i64,
        max_bytes: usize,
    ) -> Result<MetadataFetchSlice, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MetadataFetch {
                fetch_offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }

    /// Serialize the current image to a KIP-630 checkpoint under the data dir.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::TriggerSnapshot { reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)?
    }

    /// Inject a raw core [`Event`] into the loop (test/driver entrypoint and the
    /// internal feedback path for peer-RPC responses).
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn inject_event(&self, event: Event) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Event(event))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Deliver an inbound peer RPC to the engine.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn deliver(&self, inbound: Inbound) -> Result<(), RaftError> {
        self.cmd_tx
            .send(Command::Inbound(inbound))
            .await
            .map_err(|_| RaftError::Shutdown)
    }

    /// Stop the engine task.
    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(Command::Shutdown).await;
    }

    /// Test-only: append `records` as a committed batch and apply them through
    /// the real pipeline; returns the appended base offset.
    #[cfg(test)]
    async fn test_append_and_commit(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<i64, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::TestAppendAndCommit { records, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}

impl Engine {
    /// The event loop. `select!`s the command mpsc against the election/fetch
    /// timers and the leader heartbeat interval, turning each into core input
    /// and executing the resulting [`Action`]s. Single-threaded over all
    /// consensus state, so no locking is needed inside; peer sends are
    /// fire-and-forget (see the module docs).
    async fn run(mut self, mut cmd_rx: mpsc::Receiver<Command>) {
        // Heartbeat ticks the whole time; the loop only acts on it while leader.
        let hb_period =
            Duration::from_millis((self.election_timeout_ms / HEARTBEAT_DIVISOR).max(1));
        let mut heartbeat = tokio::time::interval(hb_period);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Build the timer futures fresh each turn from the current deadlines.
            let election_sleep = sleep_until_opt(self.election_at);
            let fetch_sleep = sleep_until_opt(self.fetch_at);
            tokio::pin!(election_sleep);
            tokio::pin!(fetch_sleep);

            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None | Some(Command::Shutdown) => break,
                        Some(c) => self.on_command(c),
                    }
                }
                () = &mut election_sleep => {
                    self.election_at = None;
                    self.on_timer(TimerTick::Election);
                }
                () = &mut fetch_sleep => {
                    self.fetch_at = None;
                    self.on_timer(TimerTick::Fetch);
                }
                _ = heartbeat.tick() => {
                    self.on_timer(TimerTick::Heartbeat);
                }
            }
        }
        // Fail any parked submitters so callers don't hang on shutdown.
        for w in self.commit_waiters.drain(..) {
            let _ = w.reply.send(Err(RaftError::Shutdown));
        }
    }

    /// Logical "now" for the core, derived from the monotonic clock base.
    fn now(&self) -> SimInstant {
        let ms = Instant::now()
            .saturating_duration_since(self.clock_base)
            .as_millis();
        SimInstant(u64::try_from(ms).unwrap_or(u64::MAX))
    }

    fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Shutdown => {}
            Command::Event(event) => self.on_event(event),
            Command::FetchResponse { from, body } => self.on_fetch_response(from, &body),
            Command::Inbound(inbound) => self.on_inbound(inbound),
            Command::Timer(tick) => self.on_timer(tick),
            Command::SubmitChange { records, reply } => self.on_submit_change(records, reply),
            Command::TriggerSnapshot { reply } => {
                let _ = reply.send(self.do_trigger_snapshot());
            }
            Command::QuorumStateSnapshot { reply } => {
                let _ = reply.send(self.quorum_state_snapshot());
            }
            Command::MetadataFetch {
                fetch_offset,
                max_bytes,
                reply,
            } => {
                let _ = reply.send(self.metadata_fetch_slice(fetch_offset, max_bytes));
            }
            #[cfg(test)]
            Command::TestAppendAndCommit { records, reply } => {
                let off = self.test_append_and_commit(records);
                let _ = reply.send(off);
            }
        }
    }

    fn on_event(&mut self, event: Event) {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
    }

    /// Map a timer tick to liveness behavior (Task 3).
    fn on_timer(&mut self, tick: TimerTick) {
        match tick {
            TimerTick::Election => {
                // The election timer is only armed for voters not currently
                // leading. Firing it starts an election.
                if self.core.is_voter() && !self.core.role().is_leader() {
                    self.on_event(Event::ElectionTimeout);
                }
            }
            TimerTick::Fetch => {
                // A fetch-timer expiry while we still believe in a reachable
                // leader RE-POLLS rather than electing; only a sustained loss
                // (FETCH_MISS_LIMIT consecutive misses) feeds FetchTimeout.
                let leader = self.following_leader();
                if let Some(leader_id) = leader {
                    self.fetch_misses += 1;
                    if self.fetch_misses >= FETCH_MISS_LIMIT {
                        self.fetch_misses = 0;
                        self.on_event(Event::FetchTimeout);
                    } else {
                        // Re-poll the leader and re-arm the fetch timer.
                        self.send_fetch(leader_id);
                        self.arm_fetch_timer();
                    }
                } else if self.core.is_voter() {
                    // No leader to poll but the fetch watchdog fired: elect.
                    self.on_event(Event::FetchTimeout);
                }
            }
            TimerTick::Heartbeat => {
                if self.core.role().is_leader() {
                    let epoch = self.core.quorum_state().leader_epoch;
                    self.broadcast_begin_quorum_epoch(epoch);
                }
            }
        }
    }

    /// The leader id we are actively following (Follower / attached Observer),
    /// if any.
    fn following_leader(&self) -> Option<NodeId> {
        match self.core.role() {
            Role::Follower { leader_id, .. } => Some(*leader_id),
            Role::Observer { leader_id, .. } => *leader_id,
            _ => None,
        }
    }

    fn on_inbound(&mut self, inbound: Inbound) {
        // Decode the node-local request body, run it through the core, and
        // encode the produced reply back onto the oneshot. Task 7 swaps the
        // codec for the generated KIP-595 types; the loop logic is unchanged.
        match inbound {
            Inbound::Vote { req, reply } => {
                if let Some(wire::PeerRequest::Vote {
                    candidate_epoch,
                    candidate,
                    last_epoch,
                    last_offset,
                    pre_vote,
                }) = wire::decode_vote(&req)
                {
                    let event = Event::ReceiveVoteRequest {
                        from: candidate,
                        candidate_epoch,
                        candidate,
                        candidate_log_end: LogEnd {
                            last_epoch,
                            last_offset,
                        },
                        pre_vote,
                    };
                    let resp = self.run_inbound_reply(event);
                    let _ = reply.send(resp);
                }
            }
            Inbound::BeginQuorumEpoch { req, reply } => {
                if let Some(wire::PeerRequest::BeginQuorumEpoch {
                    leader_id,
                    leader_epoch,
                }) = wire::decode_begin(&req)
                {
                    self.on_event(Event::ReceiveBeginQuorumEpoch {
                        leader_id,
                        leader_epoch,
                    });
                    let ack = wire::PeerResponse::Ack {
                        epoch: self.core.quorum_state().leader_epoch,
                    };
                    let _ = reply.send(ack.encode());
                }
            }
            Inbound::EndQuorumEpoch { req, reply } => {
                if let Some(wire::PeerRequest::EndQuorumEpoch {
                    leader_id,
                    leader_epoch,
                }) = wire::decode_end(&req)
                {
                    self.on_event(Event::ReceiveEndQuorumEpoch {
                        leader_id,
                        leader_epoch,
                    });
                    let ack = wire::PeerResponse::Ack {
                        epoch: self.core.quorum_state().leader_epoch,
                    };
                    let _ = reply.send(ack.encode());
                }
            }
            Inbound::Fetch { req, reply } => {
                if let Some(wire::PeerRequest::Fetch {
                    from,
                    fetch_epoch,
                    fetch_offset,
                }) = wire::decode_fetch(&req)
                {
                    let now = self.now();
                    let prev_role = self.core.role().name();
                    let actions = self.core.on_event(
                        Event::ReceiveFetch {
                            from,
                            fetch_epoch,
                            fetch_offset,
                        },
                        &self.log,
                        now,
                    );
                    // A Fetch may yield a TruncateTo (divergence hint) for the
                    // follower, or AdvanceHighWatermark for the leader. Encode
                    // the divergence into the response; apply HWM locally.
                    let mut diverging = None;
                    for action in &actions {
                        if let Action::TruncateTo(point) = action {
                            diverging = Some(*point);
                        }
                    }
                    self.execute(actions);
                    self.reconcile_timers(prev_role);
                    self.publish_leader();
                    // Serve the follower the batch bytes it is missing: every
                    // batch at/after its `fetch_offset` up to our log end (KRaft
                    // replicates up to the leader's log end, not just the HWM —
                    // the HWM rides separately in the response). Only the leader
                    // serves records; a divergent fetch sends none (the follower
                    // truncates first, then re-fetches).
                    let records = if diverging.is_some() || !self.core.role().is_leader() {
                        bytes::Bytes::new()
                    } else {
                        self.serve_fetch_records(fetch_offset)
                    };
                    let resp = wire::PeerResponse::Fetch {
                        leader_id: self.me,
                        leader_epoch: self.core.quorum_state().leader_epoch,
                        diverging,
                        hwm: self.log.hwm(),
                        records,
                    };
                    let _ = reply.send(resp.encode());
                }
            }
        }
    }

    /// Run an inbound event whose actions include a `ReplyVote`, returning the
    /// encoded response body (the loop side-effects from non-reply actions are
    /// applied too).
    fn run_inbound_reply(&mut self, event: Event) -> bytes::Bytes {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        let mut resp = wire::PeerResponse::Vote {
            epoch: self.core.quorum_state().leader_epoch,
            granted: false,
            pre_vote: false,
        };
        let mut local = Vec::new();
        for action in actions {
            if let Action::ReplyVote {
                epoch,
                granted,
                pre_vote,
                ..
            } = action
            {
                resp = wire::PeerResponse::Vote {
                    epoch,
                    granted,
                    pre_vote,
                };
            } else {
                local.push(action);
            }
        }
        self.execute_local_only(local);
        self.reconcile_timers(prev_role);
        self.publish_leader();
        resp.encode()
    }

    /// Execute a batch of [`Action`]s, dispatching peer sends fire-and-forget.
    fn execute(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => {
                    self.broadcast_vote(epoch, pre_vote);
                }
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch { epoch } => {
                    self.broadcast_end_quorum_epoch(epoch);
                }
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute only the local (non-network, non-reply) actions in `actions`.
    fn execute_local_only(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => self.broadcast_vote(epoch, pre_vote),
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch { epoch } => self.broadcast_end_quorum_epoch(epoch),
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                Action::ReplyVote { .. } => {}
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute a single non-network [`Action`] synchronously.
    fn execute_one_local(&mut self, action: Action) {
        match action {
            Action::AppendLeaderChange { epoch } => {
                if let Err(e) = self.append_leader_change(epoch) {
                    tracing::error!(?e, "kraft: append leader-change failed");
                }
            }
            Action::AdvanceHighWatermark(n) => {
                self.advance_and_apply(n);
            }
            Action::TruncateTo(point) => {
                if let Err(e) = self.log.truncate_to(point.offset) {
                    tracing::error!(?e, "kraft: truncate failed");
                }
            }
            Action::PersistQuorumState => {
                if let Err(e) = self.persist_quorum_state() {
                    tracing::error!(?e, "kraft: persist quorum-state failed");
                }
            }
            Action::ResetTimer { kind, deadline } => match kind {
                TimerKind::Election => self.election_at = Some(self.deadline_instant(deadline)),
                TimerKind::Fetch => self.fetch_at = Some(self.deadline_instant(deadline)),
            },
            Action::TransitionedTo(_name) => {}
            Action::SendVoteRequest { .. }
            | Action::SendBeginQuorumEpoch { .. }
            | Action::SendEndQuorumEpoch { .. }
            | Action::SendFetch { .. }
            | Action::ReplyVote { .. } => {
                debug_assert!(false, "network/reply action routed to local executor");
            }
        }
    }

    /// After processing an event, cancel timers irrelevant to the new role and
    /// arm the ones it needs that the core did not explicitly reset.
    fn reconcile_timers(&mut self, _prev_role: &'static str) {
        match self.core.role() {
            Role::Leader { .. } => {
                // A leader never elects on a timer and never fetch-watchdogs.
                self.election_at = None;
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                // A follower has no election timer; the fetch watchdog (armed by
                // the core's ResetTimer/Fetch) covers liveness.
                self.election_at = None;
            }
            Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Unattached { .. }
            | Role::Voted { .. } => {
                // Mid-election: no leader to fetch from, election timer governs.
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Resigned => {}
        }
        self.fail_waiters_on_leadership_loss();
    }

    /// Detect a transition away from leadership — Leader → non-Leader, or a
    /// leader-epoch bump while we still nominally lead — and fail every parked
    /// `submit_change` waiter with `NotLeader` so the caller's future resolves
    /// promptly instead of hanging until shutdown (FIX 1). Records appended at
    /// our old epoch can no longer commit once we step down (a new leader may
    /// truncate them), so the parked waiters are unresolvable and must error.
    fn fail_waiters_on_leadership_loss(&mut self) {
        let is_leader = self.core.role().is_leader();
        let epoch = self.core.quorum_state().leader_epoch;
        let lost_leadership = self.was_leader && (!is_leader || epoch != self.held_epoch);
        if lost_leadership && !self.commit_waiters.is_empty() {
            let current_leader = self.core.quorum_state().leader_id;
            for w in self.commit_waiters.drain(..) {
                let _ = w.reply.send(Err(RaftError::NotLeader { current_leader }));
            }
        }
        self.was_leader = is_leader;
        self.held_epoch = epoch;
    }

    /// Arm the fetch timer one election-timeout out from now (re-poll cadence).
    fn arm_fetch_timer(&mut self) {
        self.fetch_at = Some(Instant::now() + Duration::from_millis(self.election_timeout_ms));
    }

    /// Convert a core [`SimInstant`] deadline into a `tokio::time::Instant`.
    fn deadline_instant(&self, deadline: SimInstant) -> Instant {
        self.clock_base + Duration::from_millis(deadline.0)
    }

    /// Append the leader's `LeaderChange` control marker for `epoch`.
    fn append_leader_change(&mut self, epoch: LeaderEpoch) -> Result<i64, RaftError> {
        let mut batch = leader_change_batch(epoch);
        self.log.append(&mut batch)
    }

    /// Handle a `submit_change`: leader appends + parks a waiter; non-leader
    /// rejects immediately with the leader hint. Takes `records` by value: it
    /// owns the batch moved out of the [`Command`].
    #[allow(clippy::needless_pass_by_value)]
    fn on_submit_change(
        &mut self,
        records: Vec<crabka_metadata::MetadataRecord>,
        reply: oneshot::Sender<Result<(), RaftError>>,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }

        // Pre-validate against the current image (fail fast, before appending a
        // batch that would be rejected on apply). Validate against a scratch
        // clone so a batch mixing topic+partition is validated as a sequence.
        let mut scratch = self.image.clone();
        for r in &records {
            if let Err(e) = scratch.validate(r) {
                let _ = reply.send(Err(RaftError::Metadata(e)));
                return;
            }
            scratch.apply(r);
        }

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let kafka_records: Result<Vec<_>, _> = records
            .iter()
            .map(crabka_metadata::to_kafka_record)
            .collect();
        let kafka_records = match kafka_records {
            Ok(r) => r,
            Err(e) => {
                let _ = reply.send(Err(RaftError::ChangeRejected(format!("encode: {e}"))));
                return;
            }
        };
        let mut batch = RecordBatch {
            partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
            last_offset_delta: i32::try_from(kafka_records.len().saturating_sub(1)).unwrap_or(0),
            records: kafka_records,
            ..Default::default()
        };
        let base = match self.log.append(&mut batch) {
            Ok(off) => off,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let need_offset = base + i64::try_from(records.len().max(1)).unwrap_or(1);

        // Park the waiter, then try to advance the HWM immediately: a single
        // voter commits its own append with no peer fetch.
        self.commit_waiters.push(CommitWaiter {
            base_offset: base,
            need_offset,
            rejection: None,
            reply,
        });
        // Drive a self-fetch so the core recomputes the HWM (single voter
        // commits immediately; multi-voter commits when followers fetch).
        if self.core.quorum_state().majority() == 1 {
            self.advance_and_apply(self.log.log_end_offset());
        }
        self.try_resolve_waiters();
    }

    /// Test-only: append a metadata batch and commit it through the real apply
    /// pipeline. Returns the appended base offset (or -1 on failure).
    #[cfg(test)]
    fn test_append_and_commit(&mut self, records: Vec<crabka_metadata::MetadataRecord>) -> i64 {
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let kafka_records: Vec<_> = records
            .into_iter()
            .filter_map(|r| crabka_metadata::to_kafka_record(&r).ok())
            .collect();
        let mut batch = RecordBatch {
            partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
            last_offset_delta: i32::try_from(kafka_records.len().saturating_sub(1)).unwrap_or(0),
            records: kafka_records,
            ..Default::default()
        };
        let base = match self.log.append(&mut batch) {
            Ok(off) => off,
            Err(e) => {
                tracing::error!(?e, "kraft: test append failed");
                return -1;
            }
        };
        self.advance_and_apply(self.log.log_end_offset());
        base
    }

    /// Advance the HWM and apply the records newly committed by it to the
    /// [`MetadataImage`], then publish and resolve any satisfied waiters.
    fn advance_and_apply(&mut self, new_hwm: i64) {
        let prev_hwm = self.log.hwm();
        self.log.advance_hwm(new_hwm);
        let applied_hwm = self.log.hwm();
        if applied_hwm <= prev_hwm {
            self.try_resolve_waiters();
            return;
        }
        match self.log.read_decoded(prev_hwm, MAX_APPLY_BYTES) {
            Ok(batches) => {
                let mut changed = false;
                for batch in &batches {
                    if batch.base_offset < prev_hwm || batch.base_offset >= applied_hwm {
                        continue;
                    }
                    for rec in &batch.records {
                        if let Ok(meta) = from_kafka_record(rec) {
                            match self.image.validate(&meta) {
                                Ok(()) => {
                                    self.image.apply(&meta);
                                    changed = true;
                                }
                                Err(e) => {
                                    // Record the first rejection against any
                                    // waiter that covers this offset so the
                                    // submitter learns the canonical error.
                                    self.note_rejection(batch.base_offset, &e);
                                    tracing::debug!(
                                        ?e,
                                        "kraft: rejected committed record on apply"
                                    );
                                }
                            }
                        }
                    }
                }
                if changed {
                    let _ = self.image_tx.send(Arc::new(self.image.clone()));
                }
            }
            Err(e) => tracing::error!(?e, "kraft: read for apply failed"),
        }
        self.try_resolve_waiters();
    }

    /// Attach a rejection to the waiter whose appended range
    /// `[base_offset, need_offset)` actually contains `record_offset`. Gating on
    /// both bounds (not just `need_offset > record_offset`) prevents a failing
    /// record from bleeding its rejection onto later, unrelated waiters whose
    /// own records committed fine (FIX 2).
    fn note_rejection(&mut self, record_offset: i64, err: &crabka_metadata::MetadataError) {
        for w in &mut self.commit_waiters {
            if w.base_offset <= record_offset
                && record_offset < w.need_offset
                && w.rejection.is_none()
            {
                w.rejection = Some(RaftError::Metadata(err.clone()));
            }
        }
    }

    /// Resolve every waiter whose target offset is now committed.
    fn try_resolve_waiters(&mut self) {
        let hwm = self.log.hwm();
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm >= w.need_offset {
                let result = w.rejection.map_or(Ok(()), Err);
                let _ = w.reply.send(result);
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }

    /// Serialize the current image into a KIP-630 checkpoint under the data dir.
    fn do_trigger_snapshot(&self) -> Result<(), RaftError> {
        let bytes = crate::snapshot::SnapshotWriter::serialize(&self.image, 0)?;
        let end_offset = self.log.hwm();
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset, epoch, &bytes)
    }

    /// Persist the durable quorum state atomically (Task 5).
    fn persist_quorum_state(&self) -> Result<(), RaftError> {
        save_quorum_state(&self.data_dir, self.core.quorum_state())
    }

    /// Snapshot the consensus state for `DescribeQuorum`.
    fn quorum_state_snapshot(&self) -> QuorumStateSnapshot {
        let qs = self.core.quorum_state();
        let mut per_voter_fetch_offset = std::collections::BTreeMap::new();
        if let Role::Leader { replicas, .. } = self.core.role() {
            for (id, progress) in replicas {
                per_voter_fetch_offset.insert(*id, progress.fetch_offset);
            }
        }
        QuorumStateSnapshot {
            leader_id: qs.leader_id,
            leader_epoch: qs.leader_epoch,
            high_watermark: self.log.hwm(),
            log_end_offset: self.log.log_end_offset(),
            voters: qs.voters.ids().into_iter().collect(),
            per_voter_fetch_offset,
        }
    }

    /// Serve a committed `__cluster_metadata` slice for an observer's metadata
    /// fetch (1004): read committed batches at/after `fetch_offset` up to the
    /// HWM and concatenate their verbatim `RecordBatch` bytes (the engine's
    /// records are already Kafka record batches). At least the first batch is
    /// always emitted so the observer makes progress.
    fn metadata_fetch_slice(&self, fetch_offset: i64, max_bytes: usize) -> MetadataFetchSlice {
        let high_watermark = self.log.hwm();
        let log_start_offset = self.log.log_start_offset();
        let records = if fetch_offset < 0 || fetch_offset >= high_watermark {
            bytes::Bytes::new()
        } else {
            match self.log.read_decoded(fetch_offset, max_bytes.max(1)) {
                Ok(batches) => {
                    let committed: Vec<RecordBatch> = batches
                        .into_iter()
                        .filter(|b| b.base_offset < high_watermark)
                        .collect();
                    encode_batches(&committed)
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: metadata fetch read failed");
                    bytes::Bytes::new()
                }
            }
        };
        MetadataFetchSlice {
            records,
            log_start_offset,
            high_watermark,
        }
    }

    /// Voter ids other than self.
    fn other_voters(&self) -> Vec<NodeId> {
        self.core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|&id| id != self.me)
            .collect()
    }

    fn broadcast_vote(&self, epoch: LeaderEpoch, pre_vote: bool) {
        let last_epoch = self.log.last_epoch();
        let last_offset = self.log.end_offset();
        let body = wire::PeerRequest::Vote {
            candidate_epoch: epoch,
            candidate: self.me,
            last_epoch,
            last_offset,
            pre_vote,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::VOTE, body.clone());
        }
    }

    fn broadcast_begin_quorum_epoch(&self, epoch: LeaderEpoch) {
        let body = wire::PeerRequest::BeginQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::BEGIN_QUORUM_EPOCH, body.clone());
        }
    }

    fn broadcast_end_quorum_epoch(&self, epoch: LeaderEpoch) {
        let body = wire::PeerRequest::EndQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::END_QUORUM_EPOCH, body.clone());
        }
    }

    fn send_fetch(&self, leader_id: NodeId) {
        if leader_id == self.me {
            return;
        }
        let fetch_offset = self.log.end_offset();
        let fetch_epoch = self.log.last_epoch();
        let body = wire::PeerRequest::Fetch {
            from: self.me,
            fetch_epoch,
            fetch_offset,
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH, body);
    }

    /// (Leader side) serialize every log batch at/after `fetch_offset` up to our
    /// log end into a length-prefixed run of `RecordBatch::encode` blobs for the
    /// fetching follower. `KRaft` replicates up to the leader's log end (not just
    /// the HWM — the HWM is carried separately in the response and gates apply on
    /// the follower); this is what moves real record bytes so multi-voter
    /// `submit_change` waiters can commit once a majority has fetched.
    fn serve_fetch_records(&self, fetch_offset: i64) -> bytes::Bytes {
        let log_end = self.log.log_end_offset();
        if fetch_offset < 0 || fetch_offset >= log_end {
            return bytes::Bytes::new();
        }
        let batches = match self.log.read_decoded(fetch_offset, MAX_APPLY_BYTES) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(?e, "kraft: serve_fetch read failed");
                return bytes::Bytes::new();
            }
        };
        encode_batches(&batches)
    }

    /// (Follower side) apply the leader's Fetch response: truncate on a
    /// divergence hint, append the carried batches at their leader-assigned
    /// offsets, advance our HWM to `min(leader_hwm, own log_end)`, apply the
    /// newly-committed records to the image, then feed the core
    /// `ReceiveFetchResponse` (which re-arms the fetch timer / re-fetches).
    fn on_fetch_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::Fetch {
            leader_id,
            leader_epoch,
            diverging,
            hwm,
            records,
        }) = wire::PeerResponse::decode_fetch(body)
        else {
            return;
        };
        let _ = from;

        if let Some(point) = diverging {
            // Diverged: truncate to the leader's hint. The follower will
            // re-fetch from the truncation point on the next cycle. We still
            // feed the core event below so it processes the divergence too.
            if let Err(e) = self.log.truncate_to(point.offset) {
                tracing::error!(?e, "kraft: follower truncate failed");
            }
        } else if !records.is_empty() {
            // Append the carried batches at their leader-assigned offsets. A
            // batch already present (base_offset < our log end) is skipped:
            // `append_at` requires the offset to equal our current log end.
            match decode_batches(&records) {
                Ok(batches) => {
                    for mut batch in batches {
                        let at = batch.base_offset;
                        let log_end = self.log.log_end_offset();
                        if at < log_end {
                            continue; // already have it
                        }
                        if at > log_end {
                            // Gap: we are missing earlier records. Stop; the next
                            // fetch (from our true log end) will refill in order.
                            break;
                        }
                        if let Err(e) = self.log.append_at(&mut batch, at) {
                            tracing::error!(?e, at, "kraft: follower append_at failed");
                            break;
                        }
                    }
                }
                Err(e) => tracing::error!(?e, "kraft: follower decode batches failed"),
            }
            // Advance the HWM to the leader's, clamped to our log end, and apply
            // newly-committed records to the image.
            let target = hwm.min(self.log.log_end_offset());
            self.advance_and_apply(target);
        } else {
            // No records but the leader's HWM may have moved past what we already
            // have (e.g. the leader committed entries we already replicated).
            let target = hwm.min(self.log.log_end_offset());
            self.advance_and_apply(target);
        }

        // Feed the core so it re-arms its fetch timer / issues the next fetch.
        self.on_event(Event::ReceiveFetchResponse {
            leader_id,
            leader_epoch,
            diverging,
        });
    }

    /// Fire-and-forget a peer send: spawn a task that performs the RPC, decodes
    /// the response into the matching `Receive*Response` core event, and posts
    /// it back to the loop. The loop NEVER awaits a peer RPC inline.
    fn spawn_send(&self, peer: NodeId, api_key: i16, body: bytes::Bytes) {
        let peers = Arc::clone(&self.peers);
        let cmd_tx = self.cmd_tx.clone();
        tokio::spawn(async move {
            match peers.send(peer, api_key, body).await {
                Ok(resp_body) => {
                    // A Fetch response carries log records the follower must
                    // truncate/append/apply before the core sees it, so it goes
                    // through the dedicated `FetchResponse` command. Every other
                    // response decodes to a pure `Receive*Response` event.
                    if api_key == self::api_key::FETCH {
                        let _ = cmd_tx
                            .send(Command::FetchResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if let Some(event) = response_to_event(peer, api_key, &resp_body) {
                        let _ = cmd_tx.send(Command::Event(event)).await;
                    }
                }
                Err(e) => tracing::debug!(peer, ?e, "kraft: peer send failed"),
            }
        });
    }

    fn publish_leader(&self) {
        let leader = self.core.quorum_state().leader_id;
        if *self.leader_tx.borrow() != leader {
            let _ = self.leader_tx.send(leader);
        }
        // Republish the structured consensus snapshot for the handle's
        // synchronous `quorum_state()` (DescribeQuorum). `send_replace` keeps
        // the watch's stored value current even with no active receiver.
        let snapshot = self.quorum_state_snapshot();
        self.quorum_tx.send_replace(snapshot);
    }
}

/// Decode a non-Fetch peer response body into the matching `Receive*Response`
/// event. `peer` is the responder, used to fill `from`. Returns `None` for
/// `Ack` (Begin/End acks produce no core event), `Fetch` (handled by the
/// dedicated [`Engine::on_fetch_response`] path, which must touch the log before
/// the core sees the event), and undecodable bodies.
fn response_to_event(peer: NodeId, api_key: i16, body: &[u8]) -> Option<Event> {
    match api_key {
        self::api_key::VOTE => match wire::PeerResponse::decode_vote(body)? {
            wire::PeerResponse::Vote {
                epoch,
                granted,
                pre_vote,
            } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
                pre_vote,
            }),
            _ => None,
        },
        // Begin/End acks produce no core event; Fetch is handled by the
        // dedicated `FetchResponse` command path before reaching here.
        _ => None,
    }
}

/// `Some` sleep future for an armed deadline; a never-ready future otherwise so
/// `select!` ignores the disarmed timer.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}

const MAX_APPLY_BYTES: usize = 8 * 1024 * 1024;

/// Encode a run of `RecordBatch`es into one contiguous `Bytes` blob (each batch
/// is self-describing via its `batch_length` header, so they concatenate and
/// decode back in order — see [`decode_batches`]). Used by the leader's Fetch
/// serve path to ship replicated record bytes to a follower.
fn encode_batches(batches: &[RecordBatch]) -> bytes::Bytes {
    let mut out = bytes::BytesMut::new();
    for batch in batches {
        if let Err(e) = batch.encode(&mut out) {
            tracing::error!(?e, "kraft: encode batch for fetch serve failed");
        }
    }
    out.freeze()
}

/// Decode the contiguous `Bytes` blob produced by [`encode_batches`] back into a
/// `Vec<RecordBatch>` (each batch's `base_offset` is preserved). Used by the
/// follower's Fetch-response apply path.
fn decode_batches(mut buf: &[u8]) -> Result<Vec<RecordBatch>, RaftError> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        match RecordBatch::decode(&mut buf) {
            Ok(batch) => out.push(batch),
            Err(e) => {
                return Err(RaftError::ChangeRejected(format!(
                    "decode replicated batch: {e}"
                )));
            }
        }
    }
    Ok(out)
}

/// Voter ids from the core's current quorum state (for the initial published
/// snapshot, before the loop runs).
fn initial_state_voters(core: &QuorumStateMachine) -> Vec<NodeId> {
    core.quorum_state().voters.ids().into_iter().collect()
}

/// Build the leader's `LeaderChange` marker batch for `epoch`.
fn leader_change_batch(epoch: LeaderEpoch) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
        last_offset_delta: 0,
        records: Vec::new(),
        ..Default::default()
    }
}

/// Replay committed log batches starting at `from` into `image` (idempotent:
/// records that fail `validate` are skipped). Used by restart recovery.
fn replay_committed(log: &KraftLog, image: &mut MetadataImage, from: i64) {
    match log.read_decoded(from, MAX_APPLY_BYTES) {
        Ok(batches) => {
            for batch in &batches {
                for rec in &batch.records {
                    if let Ok(meta) = from_kafka_record(rec)
                        && image.validate(&meta).is_ok()
                    {
                        image.apply(&meta);
                    }
                }
            }
        }
        Err(e) => tracing::error!(?e, "kraft: replay for recovery failed"),
    }
}

// ---- quorum-state file (Task 5) ----------------------------------------------

/// Write `state` to the node-local `quorum-state` file atomically (temp +
/// rename). The format is node-local (not wire), so a compact deterministic
/// little-endian layout of: cluster id (16 bytes), leader epoch (u32), leader id
/// (tag u8 then u64), voted key (tag u8 then u64 then a 16-byte directory id).
/// The voter set is NOT persisted here — it is reconstructed from the bootstrap
/// config / metadata image (static voters this slice).
fn save_quorum_state(dir: &std::path::Path, state: &QuorumState) -> Result<(), RaftError> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(state.cluster_id.as_bytes());
    buf.put_u32(state.leader_epoch);
    if let Some(id) = state.leader_id {
        buf.put_u8(1);
        buf.put_u64(id);
    } else {
        buf.put_u8(0);
        buf.put_u64(0);
    }
    if let Some(k) = state.voted_key {
        buf.put_u8(1);
        buf.put_u64(k.id);
        buf.extend_from_slice(k.directory_id.as_bytes());
    } else {
        buf.put_u8(0);
        buf.put_u64(0);
        buf.extend_from_slice(&[0u8; 16]);
    }
    let path = dir.join(QUORUM_STATE_FILE);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &buf).map_err(crabka_log::LogError::Io)?;
    std::fs::rename(&tmp, &path).map_err(crabka_log::LogError::Io)?;
    Ok(())
}

/// Load `<dir>/quorum-state` back into a [`QuorumState`], using `voters` for the
/// (static) voter set. Returns `None` when the file is absent.
fn load_quorum_state(
    dir: &std::path::Path,
    cluster_id: Uuid,
    voters: &crabka_metadata::voters::VoterSet,
) -> Result<Option<QuorumState>, RaftError> {
    use bytes::Buf;
    let path = dir.join(QUORUM_STATE_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(RaftError::Storage(crabka_log::LogError::Io(e))),
    };
    // 16 + 4 + (1+8) + (1+8+16) = 54 bytes minimum.
    if bytes.len() < 54 {
        return Ok(None);
    }
    let mut cur: &[u8] = &bytes;
    let mut cid = [0u8; 16];
    cur.copy_to_slice(&mut cid);
    let _ = cluster_id; // file is authoritative for cluster id
    let leader_epoch = cur.get_u32();
    let leader_present = cur.get_u8() != 0;
    let leader_raw = cur.get_u64();
    let leader_id = leader_present.then_some(leader_raw);
    let voted_present = cur.get_u8() != 0;
    let voted_id = cur.get_u64();
    let mut dir_bytes = [0u8; 16];
    cur.copy_to_slice(&mut dir_bytes);
    let voted_key = voted_present.then(|| ReplicaKey {
        id: voted_id,
        directory_id: Uuid::from_bytes(dir_bytes),
    });
    Ok(Some(QuorumState {
        cluster_id: Uuid::from_bytes(cid),
        leader_epoch,
        leader_id,
        voted_key,
        voters: voters.clone(),
    }))
}

/// Write a KIP-630 `.checkpoint` artifact (bytes only — the `.meta`
/// sidecar that `snapshot::persist` writes is being removed in Task 9, so the
/// engine writes the checkpoint directly with the same temp+rename atomicity).
fn write_checkpoint(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
    bytes: &[u8],
) -> Result<(), RaftError> {
    std::fs::create_dir_all(dir).map_err(crabka_log::LogError::Io)?;
    let name = format!("{end_offset:020}-{epoch:010}.checkpoint");
    let path = dir.join(name);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(crabka_log::LogError::Io)?;
    std::fs::rename(&tmp, &path).map_err(crabka_log::LogError::Io)?;
    Ok(())
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts, pick the highest
/// `(end_offset, epoch)`, and return its raw bytes. Returns `None` when the
/// directory is absent or holds no checkpoint. Unlike `snapshot::load_latest`,
/// this reads only the `.checkpoint` (the `.meta` sidecar is gone in
/// this engine — the durable epoch lives in the quorum-state file).
fn load_latest_checkpoint(dir: &std::path::Path) -> Result<Option<Vec<u8>>, RaftError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(RaftError::Storage(crabka_log::LogError::Io(e))),
    };
    let mut best: Option<((i64, i32), std::path::PathBuf)> = None;
    for entry in entries {
        let entry = entry.map_err(crabka_log::LogError::Io)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".checkpoint") else {
            continue;
        };
        let Some((off, ep)) = stem.split_once('-') else {
            continue;
        };
        let (Ok(off), Ok(ep)) = (off.parse::<i64>(), ep.parse::<i32>()) else {
            continue;
        };
        if best.as_ref().is_none_or(|(cur, _)| (off, ep) > *cur) {
            best = Some(((off, ep), entry.path()));
        }
    }
    let Some((_, path)) = best else {
        return Ok(None);
    };
    let bytes = std::fs::read(&path).map_err(crabka_log::LogError::Io)?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::time::Duration as StdDuration;

    use crate::kraft::transport::NullPeerSender;

    fn voter_set(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
            crabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: Vec::new(),
                kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }

    fn build(me: NodeId, ids: &[NodeId]) -> (KraftController, tempfile::TempDir) {
        build_with_timeout(me, ids, 1000)
    }

    fn build_with_timeout(
        me: NodeId,
        ids: &[NodeId],
        timeout_ms: u64,
    ) -> (KraftController, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open log");
        let state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids));
        let ctrl = KraftController::spawn(
            KraftConfig {
                me,
                cluster_id: uuid::Uuid::nil(),
                initial_state: state,
                election_timeout_ms: timeout_ms,
                peers: Arc::new(NullPeerSender),
            },
            log,
            dir.path().to_path_buf(),
        );
        (ctrl, dir)
    }

    fn topic_record(name: &str) -> crabka_metadata::MetadataRecord {
        crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
            name: name.to_string(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        })
    }

    /// Drive a voter to leadership in a multi-voter cluster under `NullPeerSender`
    /// by injecting the vote responses it would have received: `ElectionTimeout`
    /// starts a pre-vote round (epoch unchanged), a granted pre-vote from `helper`
    /// promotes to `Candidate` (epoch +1) and broadcasts a real vote, and a
    /// granted real vote from `helper` reaches majority and promotes to `Leader`.
    async fn elect_leader_with_helper(ctrl: &KraftController, me: NodeId, helper: NodeId) {
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        // Pre-vote round runs at the current (pre-bump) epoch 0.
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: helper,
            epoch: 0,
            vote_granted: true,
            pre_vote: true,
        })
        .await
        .unwrap();
        // Candidate round runs at the bumped epoch 1.
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: helper,
            epoch: 1,
            vote_granted: true,
            pre_vote: false,
        })
        .await
        .unwrap();
        await_leader(ctrl, Some(me)).await;
    }

    async fn await_leader(ctrl: &KraftController, want: Option<NodeId>) {
        let mut rx = ctrl.watch_leader();
        for _ in 0..200 {
            if *rx.borrow() == want {
                return;
            }
            let _ = tokio::time::timeout(StdDuration::from_secs(5), rx.changed()).await;
        }
        assert!(*rx.borrow() == want, "leader did not reach {want:?}");
    }

    #[tokio::test]
    async fn single_voter_engine_starts_with_no_initial_leader() {
        let (ctrl, _dir) = build(1, &[1]);
        let initial = *ctrl.watch_leader().borrow();
        assert!(initial.is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn injected_election_makes_single_voter_leader() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn committed_batch_applies_to_image() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        assert!(ctrl.current_image().topic("t").is_none());

        let off = ctrl
            .test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(off >= 0);

        let mut img_rx = ctrl.watch_image();
        assert!(img_rx.borrow_and_update().topic("t").is_some());
        assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_committed_record_rejected_on_apply() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        ctrl.test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(ctrl.current_image().topic("t").is_some());

        ctrl.test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    // ---- Task 3: timers + liveness ----

    /// A single-voter engine started with the REAL clock auto-elects after the
    /// election timeout — no injected event.
    #[tokio::test]
    async fn single_voter_auto_elects_on_election_timeout() {
        let (ctrl, _dir) = build_with_timeout(1, &[1], 80);
        // The election timer is armed at construction; wait for it to fire.
        tokio::time::timeout(StdDuration::from_secs(5), await_leader(&ctrl, Some(1)))
            .await
            .expect("auto-elected within timeout");
        ctrl.shutdown().await;
    }

    /// A follower with a live leader (heartbeats keep arriving) does not
    /// spuriously elect: the leader stays node 2 across several fetch cycles.
    #[tokio::test]
    async fn follower_with_live_leader_does_not_elect() {
        // Node 1 is a follower in a 3-voter cluster; the NullPeerSender means
        // its fetches fail, but a steady stream of BeginQuorumEpoch heartbeats
        // (which we inject) must keep it attached without electing.
        let (ctrl, _dir) = build_with_timeout(1, &[1, 2, 3], 120);
        // Attach to leader 2.
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: 2,
            leader_epoch: 1,
        })
        .await
        .unwrap();
        await_leader(&ctrl, Some(2)).await;

        // Keep re-announcing leader 2 faster than the fetch watchdog would
        // accumulate FETCH_MISS_LIMIT misses; the leader must remain 2.
        for _ in 0..6 {
            tokio::time::sleep(StdDuration::from_millis(40)).await;
            ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
                leader_id: 2,
                leader_epoch: 1,
            })
            .await
            .unwrap();
        }
        let leader = *ctrl.watch_leader().borrow();
        assert!(
            leader == Some(2),
            "follower spuriously left leader 2: {leader:?}"
        );
        ctrl.shutdown().await;
    }

    // ---- Task 4: handle ops ----

    #[tokio::test]
    async fn submit_change_commits_on_single_voter_leader() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        tokio::time::timeout(
            StdDuration::from_secs(5),
            ctrl.submit_change(vec![topic_record("orders")]),
        )
        .await
        .expect("submit did not hang")
        .expect("submit ok");
        assert!(ctrl.current_image().topic("orders").is_some());

        let qs = ctrl.quorum_state().await.unwrap();
        assert!(qs.leader_id == Some(1));
        assert!(qs.high_watermark > 0);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_change_duplicate_rejected() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        ctrl.submit_change(vec![topic_record("t")]).await.unwrap();
        let dup = ctrl.submit_change(vec![topic_record("t")]).await;
        assert!(matches!(dup, Err(RaftError::Metadata(_))), "got {dup:?}");
        ctrl.shutdown().await;
    }

    /// FIX 1: a leader that parks a `submit_change` waiter and then steps down
    /// (higher-epoch `BeginQuorumEpoch` forces Leader → Follower) must fail the
    /// parked waiter promptly with `NotLeader` rather than leaving it hung until
    /// engine shutdown. In a 3-voter cluster with a `NullPeerSender`, no follower
    /// ever fetches, so the appended record never commits — the only way the
    /// waiter resolves is the leadership-loss drain.
    #[tokio::test]
    async fn submit_waiter_fails_on_leadership_loss() {
        let (ctrl, _dir) = build(1, &[1, 2, 3]);
        elect_leader_with_helper(&ctrl, 1, 2).await;

        // Park a submit on a separate task: it appends but cannot commit (no
        // peer fetches under NullPeerSender), so it stays parked.
        let ctrl2 = ctrl.clone();
        let submit =
            tokio::spawn(async move { ctrl2.submit_change(vec![topic_record("orders")]).await });

        // Give the submit a moment to reach the engine and park its waiter.
        tokio::time::sleep(StdDuration::from_millis(50)).await;

        // A strictly-higher-epoch BeginQuorumEpoch from node 2 forces node 1 to
        // step down from Leader to Follower.
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: 2,
            leader_epoch: 9,
        })
        .await
        .unwrap();

        // The parked submit must resolve promptly (bounded) with NotLeader.
        let result = tokio::time::timeout(StdDuration::from_secs(5), submit)
            .await
            .expect("submit did not hang on leadership loss")
            .expect("join");
        assert!(
            matches!(
                result,
                Err(RaftError::NotLeader {
                    current_leader: Some(2)
                })
            ),
            "got {result:?}"
        );
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_change_on_non_leader_rejects() {
        let (ctrl, _dir) = build(1, &[1, 2, 3]);
        // Never elected; node 1 is Unattached → not leader.
        let r = ctrl.submit_change(vec![topic_record("t")]).await;
        assert!(matches!(r, Err(RaftError::NotLeader { .. })), "got {r:?}");
        ctrl.shutdown().await;
    }

    fn topic_record_named(name: &str, id: u128) -> crabka_metadata::MetadataRecord {
        crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
            name: name.to_string(),
            topic_id: uuid::Uuid::from_u128(id),
            partitions: 1,
            replication_factor: 1,
        })
    }

    /// FIX 2: a committed record that fails apply-`validate` must only fail the
    /// waiter whose appended range actually contains it, not every later waiter.
    /// Park three submits in a 3-voter leader (no peer fetches → nothing commits
    /// on its own): A creates "first" (valid), B re-creates "first" (duplicate →
    /// rejected at apply), C creates "third" (valid). Then drive a single HWM
    /// advance past all three via a follower fetch. B must get `Err`; C must get
    /// `Ok` (not bled the rejection from B's earlier offset).
    #[tokio::test]
    async fn rejection_scoped_to_owning_waiter_range() {
        let (ctrl, _dir) = build(1, &[1, 2, 3]);
        elect_leader_with_helper(&ctrl, 1, 2).await;

        let ca = ctrl.clone();
        let cb = ctrl.clone();
        let cc = ctrl.clone();
        // A and B both create topic "first"; B is the duplicate that fails apply.
        // C creates a distinct "third" and must commit cleanly.
        let a =
            tokio::spawn(
                async move { ca.submit_change(vec![topic_record_named("first", 1)]).await },
            );
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        let b =
            tokio::spawn(
                async move { cb.submit_change(vec![topic_record_named("first", 1)]).await },
            );
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        let c =
            tokio::spawn(
                async move { cc.submit_change(vec![topic_record_named("third", 3)]).await },
            );
        tokio::time::sleep(StdDuration::from_millis(40)).await;

        // Drive the HWM past all appended batches by simulating a follower (node
        // 2) that has fetched the whole log. With a 3-voter majority of 2, the
        // leader's own log end plus node 2's fetch offset commits everything.
        let qs = ctrl.quorum_state().await.unwrap();
        ctrl.inject_event(Event::ReceiveFetch {
            from: 2,
            fetch_epoch: qs.leader_epoch,
            fetch_offset: qs.log_end_offset,
        })
        .await
        .unwrap();

        let ra = tokio::time::timeout(StdDuration::from_secs(5), a)
            .await
            .expect("A did not hang")
            .expect("join");
        let rb = tokio::time::timeout(StdDuration::from_secs(5), b)
            .await
            .expect("B did not hang")
            .expect("join");
        let rc = tokio::time::timeout(StdDuration::from_secs(5), c)
            .await
            .expect("C did not hang")
            .expect("join");

        assert!(ra.is_ok(), "A (first valid) should commit: {ra:?}");
        assert!(
            matches!(rb, Err(RaftError::Metadata(_))),
            "B (duplicate) should be rejected: {rb:?}"
        );
        assert!(
            rc.is_ok(),
            "C (distinct valid) must NOT bleed B's rejection: {rc:?}"
        );
        ctrl.shutdown().await;
    }

    // ---- Task 5: recovery + quorum-state file ----

    #[tokio::test]
    async fn snapshot_then_restart_recovers_image() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();
        let cluster_id = uuid::Uuid::from_u128(7);
        let voters = voter_set(&[1]);

        {
            let log = KraftLog::open(&data_dir).expect("open log");
            let ctrl = KraftController::spawn(
                KraftConfig {
                    me: 1,
                    cluster_id,
                    initial_state: QuorumState::bootstrap(cluster_id, voters.clone()),
                    election_timeout_ms: 1000,
                    peers: Arc::new(NullPeerSender),
                },
                log,
                data_dir.clone(),
            );
            ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
            await_leader(&ctrl, Some(1)).await;
            ctrl.submit_change(vec![topic_record("recovered")])
                .await
                .unwrap();
            assert!(ctrl.current_image().topic("recovered").is_some());
            ctrl.trigger_snapshot().await.unwrap();
            ctrl.shutdown().await;
            // Give the loop a moment to fully drain.
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }

        // Reopen over the same dir: the image is rebuilt from checkpoint+log.
        let ctrl2 = KraftController::open(
            data_dir.clone(),
            1,
            cluster_id,
            voters,
            1000,
            Arc::new(NullPeerSender),
        )
        .expect("reopen");
        assert!(ctrl2.current_image().topic("recovered").is_some());
        ctrl2.shutdown().await;
    }

    #[tokio::test]
    async fn quorum_state_file_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cid = uuid::Uuid::from_u128(9);
        let mut state = QuorumState::bootstrap(cid, voter_set(&[1, 2, 3]));
        state.leader_epoch = 5;
        state.leader_id = Some(2);
        state.voted_key = Some(ReplicaKey {
            id: 3,
            directory_id: uuid::Uuid::from_u128(3),
        });
        save_quorum_state(dir.path(), &state).unwrap();

        let loaded = load_quorum_state(dir.path(), cid, &voter_set(&[1, 2, 3]))
            .unwrap()
            .expect("present");
        assert!(loaded.leader_epoch == 5);
        assert!(loaded.leader_id == Some(2));
        assert!(loaded.voted_key.map(|k| k.id) == Some(3));
        assert!(loaded.cluster_id == cid);
    }
}
