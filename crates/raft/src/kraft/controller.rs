//! The async `KraftController` consensus engine: a single owning tokio task
//! holds all consensus state (the [`QuorumStateMachine`] core, the
//! [`KraftLog`], and the published [`MetadataImage`]) and turns inbound
//! commands/RPCs into core [`Event`]s whose [`Action`]s it executes.
//!
//! Ownership model: one task owns the `Engine`; everything else talks to it
//! over an mpsc of [`Command`]. The public [`KraftController`] handle is a
//! cheap clone holding the command sender plus the `watch` receivers. This is
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
//! ## Timers & liveness
//!
//! The loop drives a real monotonic clock and `select!`s over the mpsc plus an
//! election timer, a fetch timer, and a leader heartbeat interval:
//! - on a role transition the now-irrelevant timer is cancelled (a follower has
//!   no election timer; a leader has no fetch timer and runs the heartbeat);
//! - a fetch-timer expiry while the leader is still reachable RE-POLLS
//!   (`SendFetch`), it does not elect; only `FETCH_MISS_LIMIT` consecutive
//!   misses feed `Event::FetchTimeout` to start an election;
//! - the leader re-broadcasts `BeginQuorumEpoch` to voters each heartbeat tick.

use std::{path::PathBuf, sync::Arc};

use bytes::BufMut;
use crabka_ids::Offset;
use crabka_metadata::{
    MetadataImage, MetadataRecord, VotersRecord, from_kraft_value, to_kraft_values,
};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, Time, TimeExt as _},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::{
    OffsetReservation, SubmitChangeResult,
    config::{
        ControllerFetchMissLimit, DEFAULT_METADATA_RAFT_FETCH_MAX,
        MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax,
    },
    error::RaftError,
    kraft::{
        action::{Action, TimerKind},
        core::QuorumStateMachine,
        event::{Event, LogEnd},
        log::KraftLog,
        role::Role,
        snapshot_fetch::{MetadataSnapshotFetchMax, SnapshotFetchState, SnapshotFetchStep},
        transport::{
            Command, Inbound, MetadataFetchSlice, PeerSender, QuorumStateSnapshot, TimerTick,
            api_key, wire,
        },
        types::{Epoch, LogView, NodeId, QuorumState, ReplicaKey, SimInstant},
    },
};

/// Leader heartbeat interval as a fraction of the election timeout. The leader
/// re-broadcasts `BeginQuorumEpoch` this often so followers that lost the
/// initial announcement (or a rejoining old leader) re-attach without waiting
/// for an election.
const HEARTBEAT_DIVISOR: u64 = 3;

/// Floor on an observer's metadata-fetch budget: at least the first committed
/// batch is always emitted so a zero-budget fetch still makes progress.
const MIN_FETCH_BUDGET: ByteSize = crabka_units::bytes(1);

/// Filename of the node-local durable quorum-state file.
const QUORUM_STATE_FILE: &str = "quorum-state";

/// Crabka-internal "snapshot not available" signal in a `FetchSnapshot`
/// response (voter↔voter).
const SNAPSHOT_NOT_FOUND: i16 = 98;

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
    /// Base election timeout (varied per node by the caller for liveness).
    election_timeout: Time,
    heartbeat_interval: Option<Time>,
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
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
    held_epoch: Epoch,
    /// Snapshot every this many committed records past the last snapshot, then
    /// prune the log below that point. `0` disables snapshotting (KIP-630).
    snapshot_interval_records: u64,
    metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
    /// HWM at which the last checkpoint was written (and the log pruned to).
    /// Seeded from the recovered checkpoint on `open`.
    last_snapshot_end_offset: Offset,
    /// In-flight follower snapshot reassembly, if any.
    snapshot_fetch: Option<SnapshotFetchState>,
    /// Set when a snapshot was just installed; the next follower Fetch carries
    /// this epoch (the log is empty at the snapshot boundary so it has no epoch
    /// of its own). Cleared once a normal fetch advances the log.
    installed_snapshot_epoch: Option<Epoch>,
}

/// A parked `submit_change`: it completes once the HWM reaches `need_offset`
/// AND the records have been run through `validate`/`apply`.
struct CommitWaiter {
    /// Base (append) offset of this waiter's batch. Its appended range is
    /// `[base_offset, need_offset)`; a committed-record rejection only attaches
    /// to a waiter whose range actually contains the failing offset (FIX 2).
    base_offset: Offset,
    need_offset: Offset,
    /// First per-record rejection observed at apply time, if any.
    rejection: Option<RaftError>,
    result: SubmitChangeResult,
    reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
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
    pub election_timeout: Time,
    pub heartbeat_interval: Option<Time>,
    pub controller_fetch_miss_limit: ControllerFetchMissLimit,
    pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    pub metadata_raft_fetch_max: MetadataRaftFetchMax,
    pub peers: Arc<dyn PeerSender>,
    /// Snapshot once committed offset advances this many records past the
    /// last snapshot, then prune the log below it. `0` disables snapshotting.
    pub snapshot_interval_records: u64,
    /// Validated maximum metadata snapshot size this follower will fetch.
    pub metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
}

/// The configured election timeout as whole milliseconds.
///
/// Every deadline derived from the timeout crosses into integers here. The
/// core's per-(node, epoch) jitter is defined over integer milliseconds
/// (`election_jitter_ms`), so keeping the base in the same domain leaves every
/// election deadline bit-identical to the raw-integer arithmetic it replaces.
fn election_timeout_ms(election_timeout: Time) -> u64 {
    u64::try_from(election_timeout.millis_i64()).unwrap_or(0)
}

fn initial_election_at(
    core: &QuorumStateMachine,
    initial_leader: Option<NodeId>,
    clock_base: Instant,
    me: NodeId,
    initial_epoch: Epoch,
    election_timeout: Time,
) -> Option<Instant> {
    match (
        core.is_voter(),
        initial_leader,
        core.quorum_state().voters.len(),
    ) {
        (true, None, 1) => {
            // Sole voter: there is no peer to race, so the election timeout
            // jitter stagger is pure startup latency. Fire on the first tick;
            // the lone-voter fast path already holds the only vote.
            Some(clock_base)
        }
        (true, None, _) => {
            // Same deterministic per-(node, epoch) jitter the core applies to
            // re-election timers, so the first election round is staggered
            // across closely-synchronized voters.
            let base_ms = election_timeout_ms(election_timeout);
            let jitter = crate::kraft::core::election_jitter_ms(me, initial_epoch, base_ms);
            let delay_ms = base_ms.saturating_add(jitter);
            Some(
                clock_base
                    .checked_add(Duration::from_millis(delay_ms))
                    .unwrap_or(clock_base),
            )
        }
        _ => None,
    }
}

fn heartbeat_period(election_timeout: Time, configured: Option<Time>) -> Time {
    if let Some(configured) = configured {
        return configured;
    }
    let period_ms = election_timeout_ms(election_timeout)
        .div_euclid(HEARTBEAT_DIVISOR)
        .max(1);
    Time::from_millis(i64::try_from(period_ms).unwrap_or(i64::MAX))
}

fn election_timer_starts_election(is_voter: bool, is_leader: bool) -> bool {
    matches!((is_voter, is_leader), (true, false))
}

fn following_leader_for_role(role: &Role) -> Option<NodeId> {
    match role {
        Role::Follower { leader_id, .. } => Some(*leader_id),
        Role::Observer { leader_id, .. } => *leader_id,
        _ => None,
    }
}

fn should_serve_fetch_records(has_snapshot: bool, has_divergence: bool, is_leader: bool) -> bool {
    matches!(
        (has_snapshot, has_divergence, is_leader),
        (false, false, true)
    )
}

fn should_fail_waiters_on_leadership_change(
    was_leader: bool,
    is_leader: bool,
    held_epoch: Epoch,
    current_epoch: Epoch,
) -> bool {
    matches!(
        (was_leader, is_leader, held_epoch == current_epoch),
        (true, false, _) | (true, true, false)
    )
}

fn instant_from_clock_base(clock_base: Instant, deadline: SimInstant) -> Instant {
    clock_base
        .checked_add(Duration::from_millis(deadline.0))
        .unwrap_or(clock_base)
}

fn assigned_record_offset(assign_base: Offset, delta: i64) -> i64 {
    assign_base.0.saturating_add(delta)
}

fn metadata_record_batch(leader_epoch: Epoch, blobs: &[bytes::Bytes]) -> RecordBatch {
    let records: Vec<Record> = blobs
        .iter()
        .map(|blob| Record {
            value: Some(blob.clone()),
            ..Default::default()
        })
        .collect();

    RecordBatch {
        partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
        last_offset_delta: i32::try_from(blobs.len().saturating_sub(1)).unwrap_or(0),
        records,
        ..Default::default()
    }
}

fn append_result_is_consistent(
    expected_base: Offset,
    returned_base: Offset,
    log_end_after: Offset,
) -> bool {
    returned_base.cmp(&expected_base).is_eq() && log_end_after.cmp(&expected_base).is_gt()
}

fn validate_append_result(
    context: &str,
    expected_base: Offset,
    returned_base: Offset,
    log_end_after: Offset,
) -> Result<(), RaftError> {
    if append_result_is_consistent(expected_base, returned_base, log_end_after) {
        Ok(())
    } else {
        Err(RaftError::ChangeRejected(format!(
            "{context} append invariant failed: expected base {expected_base}, got {returned_base}, log end {log_end_after}"
        )))
    }
}

fn submit_waiter_need_offset(base: Offset, blob_count: usize) -> Offset {
    base + i64::try_from(blob_count).unwrap_or(1)
}

fn is_single_voter_majority(majority: usize) -> bool {
    matches!(majority, 1)
}

fn batch_base_in_apply_window(base_offset: i64, prev_hwm: Offset, applied_hwm: Offset) -> bool {
    match base_offset.checked_sub(prev_hwm.0) {
        Some(distance_from_prev) if distance_from_prev >= 0 => {
            matches!(applied_hwm.0.checked_sub(base_offset), Some(distance_to_hwm) if distance_to_hwm > 0)
        }
        _ => false,
    }
}

fn committed_records_since_snapshot(hwm: Offset, last_snapshot_end_offset: Offset) -> u64 {
    u64::try_from(hwm.0.saturating_sub(last_snapshot_end_offset.0)).unwrap_or(0)
}

fn snapshot_interval_reached(advanced: u64, snapshot_interval_records: u64) -> bool {
    matches!(
        advanced.cmp(&snapshot_interval_records),
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
    )
}

fn expected_hwm_after_advance(prev_hwm: Offset, new_hwm: Offset, log_end: Offset) -> Offset {
    prev_hwm.max(new_hwm.min(log_end))
}

fn hwm_advanced_as_expected(applied_hwm: Offset, expected_hwm: Offset) -> bool {
    !applied_hwm.cmp(&expected_hwm).is_lt()
}

fn hwm_reaches_waiter(hwm: Offset, need_offset: Offset) -> bool {
    matches!(
        hwm.cmp(&need_offset),
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
    )
}

fn metadata_fetch_offset_in_committed_window(fetch_offset: Offset, high_watermark: Offset) -> bool {
    (0..high_watermark.0).contains(&fetch_offset.0)
}

fn fetch_batch_committed_before_hwm(base_offset: i64, high_watermark: Offset) -> bool {
    (i64::MIN..high_watermark.0).contains(&base_offset)
}

fn fetch_offset_has_records(fetch_offset: Offset, log_end: Offset) -> bool {
    (0..log_end.0).contains(&fetch_offset.0)
}

fn fetch_epoch_for_request(
    installed_snapshot_epoch: Option<Epoch>,
    log_start: Offset,
    log_end: Offset,
    last_epoch: Epoch,
) -> Epoch {
    match installed_snapshot_epoch {
        Some(epoch) if log_end.cmp(&log_start).is_eq() => epoch,
        _ => last_epoch,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchBatchDisposition {
    AlreadyPresent,
    Append,
    Gap,
}

fn classify_fetch_batch(at: Offset, log_end: Offset) -> FetchBatchDisposition {
    match at.cmp(&log_end) {
        std::cmp::Ordering::Less => FetchBatchDisposition::AlreadyPresent,
        std::cmp::Ordering::Equal => FetchBatchDisposition::Append,
        std::cmp::Ordering::Greater => FetchBatchDisposition::Gap,
    }
}

fn should_start_snapshot_fetch(
    snapshot_id: (i64, i32),
    log_end: Offset,
    active_snapshot_id: Option<(i64, i32)>,
) -> bool {
    snapshot_id.0.cmp(&log_end.0).is_gt()
        && !matches!(active_snapshot_id, Some(id) if id == snapshot_id)
}

fn snapshot_fetch_response_invalid(error_code: i16, from: NodeId, leader_id: NodeId) -> bool {
    !matches!(
        (error_code.cmp(&0), from.cmp(&leader_id)),
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal)
    )
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
        Self::spawn_with_image(config, log, data_dir, image, Offset(0))
    }

    /// Spawn the engine starting from an already-recovered [`MetadataImage`]
    /// (the restart-recovery path through [`Self::open`] threads the rebuilt
    /// image in here so the published `current_image` reflects it immediately).
    fn spawn_with_image(
        config: KraftConfig,
        log: KraftLog,
        data_dir: PathBuf,
        image: MetadataImage,
        last_snapshot_end_offset: Offset,
    ) -> Self {
        let KraftConfig {
            me,
            cluster_id: _,
            initial_state,
            election_timeout,
            heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max,
            peers,
            snapshot_interval_records,
            metadata_snapshot_fetch_max,
        } = config;

        let core = QuorumStateMachine::new(me, initial_state, election_timeout);
        let initial_leader = core.quorum_state().leader_id;
        let initial_was_leader = core.role().is_leader();
        let initial_epoch = core.quorum_state().leader_epoch;

        // The controller voter set lives in the raft `QuorumState` (seeded from
        // config under KIP-595 static voters, recovered from the quorum-state
        // file on restart), NOT on the KIP-631-framed metadata log — `V1Voters`
        // is a raft-control record with no KIP-631 counterpart. Mirror it into
        // the published `MetadataImage` so image readers (e.g. the broker's
        // voter-set views, auto-join) observe the live quorum membership.
        let mut image = image;
        image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: core.quorum_state().voters.clone(),
        }));

        let (image_tx, image_rx) = watch::channel(Arc::new(image.clone()));
        let (leader_tx, leader_rx) = watch::channel(initial_leader);
        let initial_snapshot = QuorumStateSnapshot {
            leader_id: initial_leader,
            leader_epoch: initial_epoch,
            high_watermark: log.hwm().0,
            log_end_offset: log.log_end_offset().0,
            log_start_offset: log.log_start_offset().0,
            voters: initial_state_voters(&core),
            per_voter_fetch_offset: std::collections::BTreeMap::new(),
        };
        let (quorum_tx, quorum_rx) = watch::channel(initial_snapshot);
        let (cmd_tx, cmd_rx) = mpsc::channel(metadata_raft_command_queue_capacity.get());

        let clock_base = Instant::now();
        // A fresh voter arms its election timer so a bootstrap cluster elects
        // without an injected event. Observers/followers leave it disarmed.
        let election_at = initial_election_at(
            &core,
            initial_leader,
            clock_base,
            me,
            initial_epoch,
            election_timeout,
        );

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
            election_timeout,
            heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_fetch_max,
            election_at,
            fetch_at: None,
            fetch_misses: 0,
            commit_waiters: Vec::new(),
            was_leader: initial_was_leader,
            held_epoch: initial_epoch,
            snapshot_interval_records,
            metadata_snapshot_fetch_max,
            last_snapshot_end_offset,
            snapshot_fetch: None,
            installed_snapshot_epoch: None,
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
    /// [`QuorumState`] from the node-local quorum-state file. The
    /// `bootstrap` voter set/cluster id is used only when no quorum-state file
    /// exists yet.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log/checkpoint cannot be opened or read.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = me.0, %cluster_id, election_timeout = %election_timeout.human()),
        err
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "recovery inputs are independent and explicit at this low-level boundary"
    )]
    pub fn open(
        data_dir: PathBuf,
        me: NodeId,
        cluster_id: Uuid,
        bootstrap_voters: crabka_metadata::voters::VoterSet,
        election_timeout: Time,
        heartbeat_interval: Option<Time>,
        controller_fetch_miss_limit: ControllerFetchMissLimit,
        metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
        metadata_raft_fetch_max: MetadataRaftFetchMax,
        peers: Arc<dyn PeerSender>,
        snapshot_interval_records: u64,
        metadata_snapshot_fetch_max: MetadataSnapshotFetchMax,
    ) -> Result<Self, RaftError> {
        std::fs::create_dir_all(&data_dir).map_err(crabka_log::LogError::Io)?;
        let mut log = KraftLog::open(&data_dir)?;

        // Recover the image: latest checkpoint, then replay committed batches
        // past it. The committed prefix is the whole log on a clean restart
        // (the HWM is not persisted separately; the log only holds committed
        // metadata here, so we apply the full log end).
        let mut image = MetadataImage::new(cluster_id);
        let mut last_snapshot_end_offset = Offset(0);
        if let Some(bytes) = load_latest_checkpoint(&checkpoint_dir(&data_dir))? {
            let records = crate::snapshot::SnapshotReader::read_records(&bytes)?;
            image = MetadataImage::from_records(cluster_id, &records);
            if let Some((off, _ep)) = latest_checkpoint_id(&checkpoint_dir(&data_dir)) {
                // Checkpoint filenames encode the raw offset (on-disk boundary).
                last_snapshot_end_offset = Offset(off);
            }
            // Checkpoints cover the in-memory image, not a log
            // prefix offset, so replay the full log on top (idempotent:
            // duplicate records fail validate and are skipped). A precise
            // checkpoint-offset cursor.
        }
        replay_committed(&log, &mut image, Offset(0), metadata_raft_fetch_max);
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
                election_timeout,
                heartbeat_interval,
                controller_fetch_miss_limit,
                metadata_raft_command_queue_capacity,
                metadata_raft_fetch_max,
                peers,
                snapshot_interval_records,
                metadata_snapshot_fetch_max,
            },
            log,
            data_dir,
            image,
            last_snapshot_end_offset,
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
    /// follower, returns [`RaftError::NotLeader`] with the leader hint; the
    /// handle layer forwards via `forward_submit_to`.
    ///
    /// # Errors
    /// - [`RaftError::Metadata`] if a record fails `validate`.
    /// - [`RaftError::NotLeader`] if this node is not the leader.
    /// - [`RaftError::Shutdown`] if the engine task is gone.
    pub async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
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
        max_size: ByteSize,
    ) -> Result<MetadataFetchSlice, RaftError> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::MetadataFetch {
                fetch_offset,
                max_size,
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
        let hb_period = heartbeat_period(self.election_timeout, self.heartbeat_interval);
        let mut heartbeat = tokio::time::interval(hb_period.to_std());
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
            Command::FetchSnapshotResponse { from, body } => {
                self.on_fetch_snapshot_response(from, &body);
            }
            Command::Inbound(inbound) => self.on_inbound(inbound),
            Command::Timer(tick) => self.on_timer(tick),
            Command::SubmitChange { records, reply } => self.on_submit_change(&records, reply),
            Command::TriggerSnapshot { reply } => {
                let _ = reply.send(self.do_trigger_snapshot());
            }
            Command::QuorumStateSnapshot { reply } => {
                let _ = reply.send(self.quorum_state_snapshot());
            }
            Command::MetadataFetch {
                fetch_offset,
                max_size,
                reply,
            } => {
                let _ = reply.send(self.metadata_fetch_slice(fetch_offset, max_size));
            }
            #[cfg(test)]
            Command::TestAppendAndCommit { records, reply } => {
                let off = self.test_append_and_commit(&records);
                let _ = reply.send(off);
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, role = self.core.role().name())
    )]
    fn on_event(&mut self, event: Event) {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
    }

    /// Map a timer tick to liveness behavior.
    fn on_timer(&mut self, tick: TimerTick) {
        match tick {
            TimerTick::Election => {
                // The election timer is only armed for voters not currently
                // leading. Firing it starts an election.
                if election_timer_starts_election(
                    self.core.is_voter(),
                    self.core.role().is_leader(),
                ) {
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
                    if self.fetch_misses >= self.controller_fetch_miss_limit.get() {
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
        following_leader_for_role(self.core.role())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, role = self.core.role().name())
    )]
    fn on_inbound(&mut self, inbound: Inbound) {
        // Decode the request body, run it through the core, and encode the
        // produced reply back onto the oneshot.
        match inbound {
            Inbound::Vote { req, reply } => {
                if let Some(wire::PeerRequest::Vote {
                    voter_id,
                    candidate_epoch,
                    candidate,
                    last_epoch,
                    last_offset,
                    pre_vote,
                }) = wire::decode_vote(&req)
                {
                    let event = Event::ReceiveVoteRequest {
                        from: candidate,
                        voter_id,
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
                    // If the follower's fetch offset is below our pruned
                    // log-start, it cannot replicate from the log — point it at
                    // the latest snapshot instead (KIP-630).
                    // `fetch_offset` arrives raw on the KIP-595 wire; wrap it into
                    // the `KraftLog` offset domain to compare against log bounds.
                    let fetch_offset = Offset(fetch_offset);
                    let log_start = self.log.log_start_offset();
                    let snapshot_id = if fetch_offset >= 0 && fetch_offset < log_start {
                        self.latest_snapshot_id()
                    } else {
                        None
                    };
                    let records = if should_serve_fetch_records(
                        snapshot_id.is_some(),
                        diverging.is_some(),
                        self.core.role().is_leader(),
                    ) {
                        self.serve_fetch_records(fetch_offset)
                    } else {
                        bytes::Bytes::new()
                    };
                    // Advertise the ACTUAL current leader, not `self.me`: a
                    // follower serving a Fetch must redirect the fetcher to the
                    // real leader via `current_leader`. Returning `self.me` made a
                    // follower claim leadership of the current epoch — a strict
                    // KRaft follower (the JVM) caches that, then fatal-faults when
                    // the true leader's BeginQuorumEpoch arrives ("inconsistent
                    // leader at the same epoch"). Fall back to `self.me` only when
                    // no leader is known (mid-election).
                    let advertised_leader = self.core.quorum_state().leader_id.unwrap_or(self.me);
                    let resp = wire::PeerResponse::Fetch {
                        leader_id: advertised_leader,
                        leader_epoch: self.core.quorum_state().leader_epoch,
                        diverging,
                        snapshot_id,
                        hwm: self.log.hwm().0,
                        records,
                    };
                    let _ = reply.send(resp.encode());
                }
            }
            Inbound::FetchSnapshot { req, reply } => {
                if let Some(wire::PeerRequest::FetchSnapshot {
                    snapshot_id,
                    position,
                    max_bytes,
                    ..
                }) = wire::decode_fetch_snapshot(&req)
                {
                    let (end_offset, epoch) = snapshot_id;
                    let resp = match load_checkpoint_by_id(
                        &checkpoint_dir(&self.data_dir),
                        end_offset,
                        epoch,
                    ) {
                        Some(bytes) => {
                            // KIP-595 `FetchSnapshot` addresses a byte window of
                            // the on-disk checkpoint. Both fields are slice
                            // indices straight off the wire, so they clamp to
                            // `usize` here rather than becoming quantities.
                            let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
                            let pos = usize::try_from(position.max(0)).unwrap_or(0);
                            let chunk =
                                crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
                            wire::PeerResponse::FetchSnapshot {
                                snapshot_id,
                                size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                                position,
                                bytes: bytes::Bytes::copy_from_slice(chunk),
                                error_code: 0,
                            }
                        }
                        None => wire::PeerResponse::FetchSnapshot {
                            snapshot_id,
                            size: 0,
                            position,
                            bytes: bytes::Bytes::new(),
                            error_code: SNAPSHOT_NOT_FOUND,
                        },
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
        };
        let mut local = Vec::new();
        for action in actions {
            if let Action::ReplyVote { epoch, granted, .. } = action {
                resp = wire::PeerResponse::Vote { epoch, granted };
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
                // `n` is the core's raw i64 HWM target; wrap into the log domain.
                self.advance_and_apply(Offset(n));
            }
            Action::TruncateTo(point) => {
                // `point.offset` is the core's raw i64 divergence point.
                if let Err(e) = self.log.truncate_to(Offset(point.offset)) {
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
        let lost_leadership = should_fail_waiters_on_leadership_change(
            self.was_leader,
            is_leader,
            self.held_epoch,
            epoch,
        );
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
        self.fetch_at = Some(
            Instant::now() + Duration::from_millis(election_timeout_ms(self.election_timeout)),
        );
    }

    /// Convert a core [`SimInstant`] deadline into a `tokio::time::Instant`.
    fn deadline_instant(&self, deadline: SimInstant) -> Instant {
        instant_from_clock_base(self.clock_base, deadline)
    }

    /// Append the leader's `LeaderChange` control marker for `epoch`.
    #[tracing::instrument(level = "info", skip_all, fields(node = self.me.0, epoch), err)]
    fn append_leader_change(&mut self, epoch: Epoch) -> Result<Offset, RaftError> {
        let voter_ids: Vec<NodeId> = self.core.quorum_state().voters.ids().into_iter().collect();
        let mut batch = leader_change_batch(epoch, self.me, &voter_ids);
        let expected_base = self.log.log_end_offset();
        let base = self.log.append(&mut batch)?;
        validate_append_result(
            "leader-change",
            expected_base,
            base,
            self.log.log_end_offset(),
        )?;
        Ok(base)
    }

    /// Handle a `submit_change`: leader appends + parks a waiter; non-leader
    /// rejects immediately with the leader hint.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            node = self.me.0,
            epoch = self.core.quorum_state().leader_epoch,
            is_leader = self.core.role().is_leader(),
            records = records.len()
        )
    )]
    fn on_submit_change(
        &mut self,
        records: &[crabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }

        // Pre-validate and translate to KIP-631 value blobs in ONE pass against
        // an evolving scratch image, so config-diff / ACL-resolution in
        // `to_kraft_values` see in-batch prior records (a batch mixing
        // topic+partition is validated and encoded as a sequence).

        // KIP-903: broker epoch = the offset this batch commits at. The i-th
        // value blob lands at `assign_base + i`; a V1BrokerRegistration fans
        // out to exactly one blob, so its offset delta equals the number of
        // blobs already allocated. Single-writer leader: the current log end
        // offset is the base `append` will return.
        let assign_base = self.log.log_end_offset();

        let mut scratch = self.image.clone();
        let mut result = SubmitChangeResult::default();
        let mut value_blobs: Vec<bytes::Bytes> = Vec::new();
        for r in records {
            // Stamp the registration epoch = its committed offset.
            let stamped;
            let r: &MetadataRecord = match r {
                MetadataRecord::V1BrokerRegistration(b) => {
                    let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
                    let mut b = b.clone();
                    b.broker_epoch = assigned_record_offset(assign_base, delta);
                    stamped = MetadataRecord::V1BrokerRegistration(b);
                    &stamped
                }
                other => other,
            };
            if let Err(e) = scratch.validate(r) {
                let _ = reply.send(Err(RaftError::Metadata(e)));
                return;
            }
            if let MetadataRecord::V1PartitionOffsetAdvance(r) = r {
                let (base_offset, _next_offset) = crabka_verified::reserve_offsets(
                    scratch
                        .partition_next_offset(&r.topic, r.partition)
                        .unwrap_or(0),
                    r.count,
                );
                result.offset_reservations.push(OffsetReservation {
                    topic: r.topic.clone(),
                    partition: r.partition,
                    base_offset,
                    count: r.count,
                });
            }
            match to_kraft_values(r, &scratch) {
                Ok(mut blobs) => value_blobs.append(&mut blobs),
                Err(e) => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!("encode: {e}"))));
                    return;
                }
            }
            scratch.apply(r);
        }

        // Every record fanned out to nothing (e.g. an empty config clear): the
        // submit is a committed no-op. Reply success without appending a batch.
        if value_blobs.is_empty() {
            let _ = reply.send(Ok(result));
            return;
        }

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = metadata_record_batch(leader_epoch, &value_blobs);
        let base = match self.log.append(&mut batch) {
            Ok(off) => off,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Err(e) = validate_append_result(
            "submit-change",
            assign_base,
            base,
            self.log.log_end_offset(),
        ) {
            let _ = reply.send(Err(e));
            return;
        }
        let need_offset = submit_waiter_need_offset(base, value_blobs.len());

        // Park the waiter, then try to advance the HWM immediately: a single
        // voter commits its own append with no peer fetch.
        self.commit_waiters.push(CommitWaiter {
            base_offset: base,
            need_offset,
            rejection: None,
            result,
            reply,
        });
        // Drive a self-fetch so the core recomputes the HWM (single voter
        // commits immediately; multi-voter commits when followers fetch).
        if is_single_voter_majority(self.core.quorum_state().majority()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
        self.try_resolve_waiters();
    }

    /// Test-only: append a metadata batch and commit it through the real apply
    /// pipeline. Returns the appended base offset (or -1 on failure).
    #[cfg(test)]
    fn test_append_and_commit(&mut self, records: &[crabka_metadata::MetadataRecord]) -> i64 {
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut scratch = self.image.clone();
        let mut blobs: Vec<bytes::Bytes> = Vec::new();
        for r in records {
            if let Ok(mut bs) = to_kraft_values(r, &scratch) {
                blobs.append(&mut bs);
            }
            scratch.apply(r);
        }
        let mut batch = metadata_record_batch(leader_epoch, &blobs);
        let expected_base = self.log.log_end_offset();
        let base = match self.log.append(&mut batch) {
            Ok(off) => off,
            Err(e) => {
                tracing::error!(?e, "kraft: test append failed");
                return -1;
            }
        };
        if let Err(e) = validate_append_result(
            "test append",
            expected_base,
            base,
            self.log.log_end_offset(),
        ) {
            tracing::error!(?e, "kraft: test append invariant failed");
            return -1;
        }
        self.advance_and_apply(self.log.log_end_offset());
        // Test helper returns the raw base offset (compared against `-1` sentinel).
        base.0
    }

    /// Advance the HWM and apply the records newly committed by it to the
    /// [`MetadataImage`], then publish and resolve any satisfied waiters.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, new_hwm = new_hwm.0, prev_hwm = tracing::field::Empty)
    )]
    fn advance_and_apply(&mut self, new_hwm: Offset) {
        let prev_hwm = self.log.hwm();
        tracing::Span::current().record("prev_hwm", prev_hwm.0);
        let expected_hwm = expected_hwm_after_advance(prev_hwm, new_hwm, self.log.log_end_offset());
        self.log.advance_hwm(new_hwm);
        let applied_hwm = self.log.hwm();
        if !hwm_advanced_as_expected(applied_hwm, expected_hwm) {
            tracing::error!(
                prev_hwm = prev_hwm.0,
                new_hwm = new_hwm.0,
                expected_hwm = expected_hwm.0,
                applied_hwm = applied_hwm.0,
                "kraft: high watermark failed to advance"
            );
            self.fail_waiters_reached_by(
                expected_hwm,
                "high watermark failed to advance to committed offset",
            );
            self.maybe_snapshot_and_prune();
            return;
        }
        if applied_hwm <= prev_hwm {
            self.try_resolve_waiters();
            self.maybe_snapshot_and_prune();
            return;
        }
        let mut cursor = prev_hwm;
        let mut changed = false;
        while cursor < applied_hwm {
            match self
                .log
                .read_decoded(cursor, self.metadata_raft_fetch_max.size())
            {
                Ok(batches) => {
                    let next = next_batch_offset(&batches);
                    if batches.is_empty() {
                        break;
                    }
                    for batch in &batches {
                        if !batch_base_in_apply_window(batch.base_offset, prev_hwm, applied_hwm) {
                            continue;
                        }
                        // The LeaderChange control batch carries no metadata records;
                        // never feed it to the metadata decoder.
                        if batch.attributes.is_control_batch() {
                            continue;
                        }
                        for rec in &batch.records {
                            let Some(value) = rec.value.as_ref() else {
                                continue;
                            };
                            match from_kraft_value(value, &self.image) {
                                Ok(meta) => match self.image.validate(&meta) {
                                    Ok(()) => {
                                        self.image.apply(&meta);
                                        changed = true;
                                    }
                                    Err(e) => {
                                        // Record the first rejection against any
                                        // waiter that covers this offset so the
                                        // submitter learns the canonical error.
                                        self.note_rejection(Offset(batch.base_offset), &e);
                                        tracing::debug!(
                                            ?e,
                                            "kraft: rejected committed record on apply"
                                        );
                                    }
                                },
                                Err(e) => {
                                    tracing::debug!(?e, "kraft: failed to decode committed record");
                                }
                            }
                        }
                    }
                    let Some(next) = next.filter(|next| *next > cursor) else {
                        break;
                    };
                    cursor = next;
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: read for apply failed");
                    break;
                }
            }
        }
        if changed {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
        self.publish_leader();
        self.try_resolve_waiters();
        self.maybe_snapshot_and_prune();
    }

    /// (Leader, KIP-630) once the committed offset has advanced
    /// `snapshot_interval_records` past the last snapshot, serialize the current
    /// image to a checkpoint and prune the log below the snapshot boundary.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, hwm = tracing::field::Empty)
    )]
    fn maybe_snapshot_and_prune(&mut self) {
        if self.snapshot_interval_records == 0 || !self.core.role().is_leader() {
            return;
        }
        let hwm = self.log.hwm();
        let advanced = committed_records_since_snapshot(hwm, self.last_snapshot_end_offset);
        if !snapshot_interval_reached(advanced, self.snapshot_interval_records) {
            return;
        }
        tracing::Span::current().record("hwm", hwm.0);
        let bytes = match crate::snapshot::SnapshotWriter::serialize(&self.image, 0) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(?e, "kraft: snapshot serialize failed");
                return;
            }
        };
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        // Checkpoint filenames encode the raw offset (on-disk boundary).
        if let Err(e) = write_checkpoint(&checkpoint_dir(&self.data_dir), hwm.0, epoch, &bytes) {
            tracing::error!(?e, "kraft: checkpoint write failed; skipping prune");
            return;
        }
        self.last_snapshot_end_offset = hwm;
        if let Err(e) = self.log.prune_to(hwm) {
            tracing::error!(?e, "kraft: prune_to failed");
        }
        retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
    }

    /// The latest local snapshot id `(end_offset, epoch)`, if any (leader's
    /// `FetchSnapshot` hint).
    fn latest_snapshot_id(&self) -> Option<(i64, i32)> {
        latest_checkpoint_id(&checkpoint_dir(&self.data_dir))
    }

    /// Attach a rejection to the waiter whose appended range
    /// `[base_offset, need_offset)` actually contains `record_offset`. Gating on
    /// both bounds (not just `need_offset > record_offset`) prevents a failing
    /// record from bleeding its rejection onto later, unrelated waiters whose
    /// own records committed fine (FIX 2).
    fn note_rejection(&mut self, record_offset: Offset, err: &crabka_metadata::MetadataError) {
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
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let result = w.rejection.map_or(Ok(w.result), Err);
                let _ = w.reply.send(result);
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }

    fn fail_waiters_reached_by(&mut self, hwm: Offset, reason: &str) {
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let _ = w
                    .reply
                    .send(Err(RaftError::ChangeRejected(reason.to_string())));
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }

    /// Serialize the current image into a KIP-630 checkpoint under the data dir.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, end_offset = self.log.hwm().0),
        err
    )]
    fn do_trigger_snapshot(&self) -> Result<(), RaftError> {
        let bytes = crate::snapshot::SnapshotWriter::serialize(&self.image, 0)?;
        let end_offset = self.log.hwm();
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        // Checkpoint filenames encode the raw offset (on-disk boundary).
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset.0, epoch, &bytes)
    }

    /// Persist the durable quorum state atomically.
    fn persist_quorum_state(&self) -> Result<(), RaftError> {
        save_quorum_state(&self.data_dir, self.core.quorum_state())
    }

    /// Snapshot the consensus state for `DescribeQuorum`.
    fn quorum_state_snapshot(&self) -> QuorumStateSnapshot {
        let qs = self.core.quorum_state();
        let mut per_voter_fetch_offset = std::collections::BTreeMap::new();
        if let Role::Leader { replicas, .. } = self.core.role() {
            // The leader's own matched index is its log end offset — its local
            // log is, by definition, fully matched against itself. The
            // `replicas` progress map tracks only *peers*, so the leader must
            // insert its own entry explicitly (otherwise a single-voter quorum
            // reports an empty matched-index map and `DescribeQuorum` returns
            // the JVM "unknown" sentinel -1 for the leader).
            // `per_voter_fetch_offset` is a wire-facing DescribeQuorum DTO of raw
            // `i64`s; the peer entries already come from the core as `i64`.
            per_voter_fetch_offset.insert(self.core.me(), self.log.log_end_offset().0);
            for (id, progress) in replicas {
                per_voter_fetch_offset.insert(*id, progress.fetch_offset);
            }
        }
        QuorumStateSnapshot {
            leader_id: qs.leader_id,
            leader_epoch: qs.leader_epoch,
            high_watermark: self.log.hwm().0,
            log_end_offset: self.log.log_end_offset().0,
            log_start_offset: self.log.log_start_offset().0,
            voters: qs.voters.ids().into_iter().collect(),
            per_voter_fetch_offset,
        }
    }

    /// Serve a committed `__cluster_metadata` slice for an observer's metadata
    /// fetch (1004): read committed batches at/after `fetch_offset` up to the
    /// HWM and concatenate their verbatim `RecordBatch` bytes (the engine's
    /// records are already Kafka record batches). At least the first batch is
    /// always emitted so the observer makes progress.
    fn metadata_fetch_slice(&self, fetch_offset: i64, max_size: ByteSize) -> MetadataFetchSlice {
        // `fetch_offset` arrives raw on the observer metadata-fetch wire; wrap it
        // into the `KraftLog` offset domain for the log-bound comparisons/read.
        let fetch_offset = Offset(fetch_offset);
        let high_watermark = self.log.hwm();
        let log_start_offset = self.log.log_start_offset();
        let records = if metadata_fetch_offset_in_committed_window(fetch_offset, high_watermark) {
            match self
                .log
                .read_decoded(fetch_offset, max_size.max(MIN_FETCH_BUDGET))
            {
                Ok(batches) => {
                    let committed: Vec<RecordBatch> = batches
                        .into_iter()
                        .filter(|b| fetch_batch_committed_before_hwm(b.base_offset, high_watermark))
                        .collect();
                    encode_batches(&committed)
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: metadata fetch read failed");
                    bytes::Bytes::new()
                }
            }
        } else {
            bytes::Bytes::new()
        };
        MetadataFetchSlice {
            records,
            // `MetadataFetchSlice` is a wire-facing DTO of raw `i64` offsets.
            log_start_offset: log_start_offset.0,
            high_watermark: high_watermark.0,
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

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch, pre_vote))]
    fn broadcast_vote(&self, epoch: Epoch, pre_vote: bool) {
        let last_epoch = self.log.last_epoch();
        let last_offset = self.log.end_offset();
        // The wire top-level `voterId` must name the recipient voter; the JVM
        // rejects a Vote addressed to anyone else (or to the sentinel `-1`). So
        // build a per-recipient body inside the loop rather than broadcasting a
        // single shared body.
        for peer in self.other_voters() {
            let body = wire::PeerRequest::Vote {
                voter_id: peer,
                candidate_epoch: epoch,
                candidate: self.me,
                last_epoch,
                last_offset,
                pre_vote,
            }
            .encode();
            self.spawn_send(peer, api_key::VOTE, body);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    fn broadcast_begin_quorum_epoch(&self, epoch: Epoch) {
        let body = wire::PeerRequest::BeginQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::BEGIN_QUORUM_EPOCH, body.clone());
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, epoch))]
    fn broadcast_end_quorum_epoch(&self, epoch: Epoch) {
        let body = wire::PeerRequest::EndQuorumEpoch {
            leader_id: self.me,
            leader_epoch: epoch,
        }
        .encode();
        for peer in self.other_voters() {
            self.spawn_send(peer, api_key::END_QUORUM_EPOCH, body.clone());
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, leader_id = leader_id.0, fetch_offset = self.log.end_offset()))]
    fn send_fetch(&self, leader_id: NodeId) {
        if leader_id == self.me {
            return;
        }
        let fetch_offset = self.log.end_offset();
        // Post-install epoch hazard: right after installing a snapshot the log is
        // empty at the snapshot boundary, so it carries no epoch of its own and
        // `last_epoch()` would report 0. Sending `fetch_epoch = 0` from a
        // non-zero boundary makes the leader's divergence check emit a spurious
        // truncate hint → a re-fetch loop. While we hold a freshly-installed
        // epoch AND the log is still empty at the boundary, fetch with that
        // epoch instead. Cleared once a normal fetch appends past the boundary.
        let fetch_epoch = fetch_epoch_for_request(
            self.installed_snapshot_epoch,
            self.log.log_start_offset(),
            self.log.log_end_offset(),
            self.log.last_epoch(),
        );
        let body = wire::PeerRequest::Fetch {
            from: self.me,
            fetch_epoch,
            fetch_offset,
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH, body);
    }

    /// (Follower side) request a byte range of `snapshot_id` from `leader_id`.
    fn send_fetch_snapshot(&self, leader_id: NodeId, snapshot_id: (i64, i32), position: i64) {
        if leader_id == self.me {
            return;
        }
        let body = wire::PeerRequest::FetchSnapshot {
            from: self.me,
            snapshot_id,
            position,
            // KIP-595 `FetchSnapshot.MaxBytes` is an `int32`; the quantity
            // converts here, at the wire boundary.
            max_bytes: self.metadata_raft_fetch_max.bytes(),
        }
        .encode();
        self.spawn_send(leader_id, api_key::FETCH_SNAPSHOT, body);
    }

    /// (Leader side) serialize every log batch at/after `fetch_offset` up to our
    /// log end into a length-prefixed run of `RecordBatch::encode` blobs for the
    /// fetching follower. `KRaft` replicates up to the leader's log end (not just
    /// the HWM — the HWM is carried separately in the response and gates apply on
    /// the follower); this is what moves real record bytes so multi-voter
    /// `submit_change` waiters can commit once a majority has fetched.
    fn serve_fetch_records(&self, fetch_offset: Offset) -> bytes::Bytes {
        let log_end = self.log.log_end_offset();
        if !fetch_offset_has_records(fetch_offset, log_end) {
            return bytes::Bytes::new();
        }
        let batches = match self
            .log
            .read_decoded(fetch_offset, self.metadata_raft_fetch_max.size())
        {
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, from = from.0, log_end = self.log.log_end_offset().0)
    )]
    fn on_fetch_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::Fetch {
            leader_id,
            leader_epoch,
            diverging,
            snapshot_id,
            hwm,
            records,
        }) = wire::PeerResponse::decode_fetch(body)
        else {
            return;
        };
        let _ = from;

        // The leader signalled our fetch offset is below its pruned log-start:
        // we must fetch the snapshot instead of replicating from the log. Start
        // (or continue) a snapshot transfer, feed the core for liveness/epoch
        // bookkeeping, then stop — no append/apply on this response.
        if let Some(id) = snapshot_id {
            let active_id = self.snapshot_fetch.as_ref().map(|s| s.snapshot_id);
            if should_start_snapshot_fetch(id, self.log.log_end_offset(), active_id) {
                self.snapshot_fetch = Some(SnapshotFetchState::with_max(
                    id,
                    leader_id,
                    self.metadata_snapshot_fetch_max,
                ));
                self.send_fetch_snapshot(leader_id, id, 0);
            }
            self.on_event(Event::ReceiveFetchResponse {
                leader_id,
                leader_epoch,
                diverging,
            });
            return;
        }

        // `hwm` arrives raw on the KIP-595 Fetch response wire; wrap into the
        // log offset domain.
        let hwm = Offset(hwm);
        if let Some(point) = diverging {
            // Diverged: truncate to the leader's hint. The follower will
            // re-fetch from the truncation point on the next cycle. We still
            // feed the core event below so it processes the divergence too.
            // `point.offset` is the core's raw i64 divergence point.
            if let Err(e) = self.log.truncate_to(Offset(point.offset)) {
                tracing::error!(?e, "kraft: follower truncate failed");
            }
        } else if !records.is_empty() {
            // Append the carried batches at their leader-assigned offsets. A
            // batch already present (base_offset < our log end) is skipped:
            // `append_at` requires the offset to equal our current log end.
            match decode_batches(&records) {
                Ok(batches) => {
                    for mut batch in batches {
                        // `base_offset` is a raw record-format field; wrap into
                        // the log offset domain for the append.
                        let at = Offset(batch.base_offset);
                        let log_end = self.log.log_end_offset();
                        match classify_fetch_batch(at, log_end) {
                            FetchBatchDisposition::AlreadyPresent => {
                                continue; // already have it
                            }
                            FetchBatchDisposition::Append => {}
                            FetchBatchDisposition::Gap => {
                                // Gap: we are missing earlier records. Stop; the next
                                // fetch (from our true log end) will refill in order.
                                break;
                            }
                        }
                        if let Err(e) = self.log.append_at(&mut batch, at) {
                            tracing::error!(?e, at = at.0, "kraft: follower append_at failed");
                            break;
                        }
                        // Appended past the snapshot boundary: the log now has a
                        // real epoch of its own, so drop the post-install epoch
                        // override (see `send_fetch`).
                        self.installed_snapshot_epoch = None;
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

    /// (Follower side) handle a `FetchSnapshot` response chunk: reassemble via
    /// the [`SnapshotFetchState`], requesting the next range until complete, then
    /// install the assembled snapshot and resume normal fetching. Any error /
    /// abort falls back to a plain Fetch against the same peer.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, from = from.0))]
    fn on_fetch_snapshot_response(&mut self, from: NodeId, body: &[u8]) {
        let Some(wire::PeerResponse::FetchSnapshot {
            snapshot_id,
            size,
            position,
            bytes,
            error_code,
        }) = wire::PeerResponse::decode_fetch_snapshot(body)
        else {
            return;
        };
        let Some(state) = self.snapshot_fetch.as_mut() else {
            return;
        };
        if snapshot_fetch_response_invalid(error_code, from, state.leader_id) {
            self.snapshot_fetch = None;
            self.send_fetch(from);
            return;
        }
        match state.on_chunk(snapshot_id, size, position, &bytes) {
            SnapshotFetchStep::Continue { next_position } => {
                self.send_fetch_snapshot(from, snapshot_id, next_position);
            }
            SnapshotFetchStep::Restart => {
                self.snapshot_fetch = None;
                self.send_fetch(from);
            }
            SnapshotFetchStep::Complete(assembled) => {
                let id = state.snapshot_id;
                self.snapshot_fetch = None;
                if let Err(e) = self.install_fetched_snapshot(id, &assembled) {
                    tracing::error!(?e, "kraft: snapshot install failed; will re-fetch");
                }
                self.send_fetch(from);
            }
        }
    }

    /// Validate, persist, and install a fetched snapshot: rebuild the image from
    /// its records, write the checkpoint, install it into the log (resetting the
    /// log-start/end to `end_offset`), publish the new image, and arm the
    /// post-install fetch epoch (see `send_fetch`).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, end_offset = id.0, snapshot_epoch = id.1, bytes = bytes.len()),
        err
    )]
    fn install_fetched_snapshot(&mut self, id: (i64, i32), bytes: &[u8]) -> Result<(), RaftError> {
        // `end_offset` is the snapshot id's raw offset (wire / checkpoint-filename
        // boundary); wrap into the log offset domain where it addresses the log.
        let (end_offset, epoch) = id;
        let end_offset_pos = Offset(end_offset);
        // Validate the bytes decode before mutating any durable state.
        let records = crate::snapshot::SnapshotReader::read_records(bytes)?;
        if end_offset_pos <= self.log.log_end_offset() {
            return Ok(()); // stale; we already advanced past this snapshot
        }
        let cluster_id = self.image.cluster_id();
        let mut new_image = MetadataImage::from_records(cluster_id, &records);
        // KIP-630 snapshots cover the KIP-631-framed metadata records only;
        // `to_records` does NOT emit the raft-control `V1Voters` (the controller
        // voter set lives in the raft `QuorumState`, not the metadata log — see
        // `spawn_with_image`). Rebuilding the image straight from snapshot
        // records would therefore drop the live quorum membership, leaving image
        // readers (DescribeQuorum / auto-join) blind on a follower that caught up
        // via snapshot. Mirror the live voter set back in, exactly as spawn does.
        new_image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: self.core.quorum_state().voters.clone(),
        }));
        write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset, epoch, bytes)?;
        self.image = new_image;
        self.log.install_snapshot(end_offset_pos)?;
        self.last_snapshot_end_offset = end_offset_pos;
        self.installed_snapshot_epoch = Some(u32::try_from(epoch).unwrap_or(0));
        let _ = self.image_tx.send(Arc::new(self.image.clone()));
        retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
        Ok(())
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
                    } else if api_key == self::api_key::FETCH_SNAPSHOT {
                        // A FetchSnapshot response carries snapshot bytes the
                        // follower reassembles + installs before resuming, so it
                        // takes its own command path (mirrors FetchResponse).
                        let _ = cmd_tx
                            .send(Command::FetchSnapshotResponse {
                                from: peer,
                                body: resp_body,
                            })
                            .await;
                    } else if let Some(event) = response_to_event(peer, api_key, &resp_body) {
                        let _ = cmd_tx.send(Command::Event(event)).await;
                    }
                }
                Err(e) => tracing::debug!(peer = peer.0, ?e, "kraft: peer send failed"),
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
            wire::PeerResponse::Vote { epoch, granted } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
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

/// Build the leader's `LeaderChange` control batch for `epoch`: a single
/// KIP-595 `LeaderChange` control record (control-batch attribute set), naming
/// the new leader and the current voter set. A real `KRaft` batch MUST contain at
/// least one record — an empty batch crashes a JVM follower
/// (`Batch must contain at least one record`) — so this carries the proper
/// `LeaderChangeMessage` rather than zero records. Crabka readers skip it via
/// `is_control_batch()`; it occupies exactly one log offset
/// (`last_offset_delta = 0`), unchanged from the prior empty batch.
// The `version: 0` field equals `LeaderChangeMessage`'s `Default` (i16 -> 0), so
// deleting it yields byte-identical encoding; it is not the wire schema version
// (that is the `0` passed to `msg.encode`). Equivalent mutant.
#[cfg_attr(test, mutants::skip)]
fn leader_change_batch(epoch: Epoch, leader_id: NodeId, voter_ids: &[NodeId]) -> RecordBatch {
    use crabka_protocol::{
        Encode,
        owned::{
            common::leader_change_message::voter::Voter, leader_change_message::LeaderChangeMessage,
        },
        records::{
            header::Attributes,
            metadata::control::{ControlRecordType, control_record_key},
        },
    };

    let voters: Vec<Voter> = voter_ids
        .iter()
        .map(|&id| Voter {
            voter_id: i32::try_from(id.0).unwrap_or(i32::MAX),
            ..Default::default()
        })
        .collect();
    let msg = LeaderChangeMessage {
        version: 0,
        leader_id: i32::try_from(leader_id.0).unwrap_or(i32::MAX),
        voters: voters.clone(),
        granting_voters: voters,
        ..Default::default()
    };
    let mut value = bytes::BytesMut::new();
    // LeaderChangeMessage v0; encode is infallible for a well-formed message.
    let _ = msg.encode(&mut value, 0);
    let key = control_record_key(ControlRecordType::LeaderChange);
    RecordBatch {
        partition_leader_epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
        attributes: Attributes::default().with_control(true),
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            key: Some(key),
            value: Some(value.freeze()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Replay committed log batches starting at `from` into `image` (idempotent:
/// records that fail `validate` are skipped). Used by restart recovery.
fn replay_committed(
    log: &KraftLog,
    image: &mut MetadataImage,
    from: Offset,
    max: MetadataRaftFetchMax,
) {
    let mut cursor = from;
    let target = log.log_end_offset();
    while cursor < target {
        match log.read_decoded(cursor, max.size()) {
            Ok(batches) => {
                let next = next_batch_offset(&batches);
                if batches.is_empty() {
                    break;
                }
                for batch in &batches {
                    if batch.attributes.is_control_batch() {
                        continue;
                    }
                    for rec in &batch.records {
                        let Some(value) = rec.value.as_ref() else {
                            continue;
                        };
                        if let Ok(meta) = from_kraft_value(value, image)
                            && image.validate(&meta).is_ok()
                        {
                            image.apply(&meta);
                        }
                    }
                }
                let Some(next) = next.filter(|next| *next > cursor) else {
                    break;
                };
                cursor = next;
            }
            Err(e) => {
                tracing::error!(?e, "kraft: replay for recovery failed");
                break;
            }
        }
    }
}

fn next_batch_offset(batches: &[RecordBatch]) -> Option<Offset> {
    batches.last().map(|batch| {
        Offset(
            batch
                .base_offset
                .saturating_add(i64::from(batch.last_offset_delta))
                .saturating_add(1),
        )
    })
}

// ---- quorum-state file --------------------------------------------------------

/// Write `state` to the node-local `quorum-state` file atomically (temp +
/// rename). The format is node-local (not wire), so a compact deterministic
/// little-endian layout of: cluster id (16 bytes), leader epoch (u32), leader id
/// (tag u8 then u64), voted key (tag u8 then u64 then a 16-byte directory id).
/// The voter set is NOT persisted here — it is reconstructed from the bootstrap
/// config / metadata image (static voters).
fn save_quorum_state(dir: &std::path::Path, state: &QuorumState) -> Result<(), RaftError> {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(state.cluster_id.as_bytes());
    buf.put_u32(state.leader_epoch);
    if let Some(id) = state.leader_id {
        buf.put_u8(1);
        buf.put_u64(id.0);
    } else {
        buf.put_u8(0);
        buf.put_u64(0);
    }
    if let Some(k) = state.voted_key {
        buf.put_u8(1);
        buf.put_u64(k.id.0);
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
    let _leader_present_tag = cur.get_u8();
    let _leader_raw = cur.get_u64();
    let voted_present = cur.get_u8() != 0;
    let voted_id = cur.get_u64();
    let mut dir_bytes = [0u8; 16];
    cur.copy_to_slice(&mut dir_bytes);
    let voted_key = voted_present.then(|| ReplicaKey {
        id: NodeId(voted_id),
        directory_id: Uuid::from_bytes(dir_bytes),
    });
    // Leadership is VOLATILE, not durable: Raft persists only currentTerm
    // (`leader_epoch`) and votedFor (`voted_key`), never the current leader. A
    // restarted node must NOT trust a persisted `leader_id` — especially an
    // ex-leader, which would otherwise come back believing it is still the
    // leader (stale `leader_id == self`), publish itself via `watch_leader`,
    // and never re-discover the real leader elected while it was down. Start
    // with no known leader; the node re-attaches via the current leader's
    // `BeginQuorumEpoch` heartbeat (higher epoch → Follower) or a re-election.
    Ok(Some(QuorumState {
        cluster_id: Uuid::from_bytes(cid),
        leader_epoch,
        leader_id: None,
        voted_key,
        voters: voters.clone(),
    }))
}

/// Write a KIP-630 `.checkpoint` artifact (bytes only) directly with
/// temp+rename atomicity.
fn write_checkpoint(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
    bytes: &[u8],
) -> Result<(), RaftError> {
    std::fs::create_dir_all(dir).map_err(crabka_log::LogError::Io)?;
    let name = checkpoint_name(end_offset, epoch);
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
    let Some((end_offset, epoch)) = latest_checkpoint_id(dir) else {
        return Ok(None);
    };
    let bytes = std::fs::read(dir.join(checkpoint_name(end_offset, epoch)))
        .map_err(crabka_log::LogError::Io)?;
    Ok(Some(bytes))
}

fn checkpoint_name(end_offset: i64, epoch: i32) -> String {
    format!("{end_offset:020}-{epoch:010}.checkpoint")
}

pub(crate) fn parse_checkpoint_name(name: &str) -> Option<(i64, i32)> {
    let stem = name.strip_suffix(".checkpoint")?;
    let (off, ep) = stem.split_once('-')?;
    Some((off.parse().ok()?, ep.parse().ok()?))
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` id, or `None` when the directory is absent or
/// holds no checkpoint.
fn latest_checkpoint_id(dir: &std::path::Path) -> Option<(i64, i32)> {
    let entries = std::fs::read_dir(dir).ok()?;
    let mut best: Option<(i64, i32)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((off, ep)) = parse_checkpoint_name(name) else {
            continue;
        };
        if best.is_none_or(|cur| checkpoint_id_is_newer((off, ep), cur)) {
            best = Some((off, ep));
        }
    }
    best
}

fn checkpoint_id_is_newer(candidate: (i64, i32), current: (i64, i32)) -> bool {
    matches!(candidate.cmp(&current), std::cmp::Ordering::Greater)
}

/// Delete every `.checkpoint` in `dir` except the latest `(end_offset, epoch)`,
/// keeping the checkpoint directory single-snapshot after a snapshot+prune or
/// install. Best-effort: read/remove errors are ignored.
fn retain_latest_checkpoint(dir: &std::path::Path) {
    let Some(latest) = latest_checkpoint_id(dir) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((off, ep)) = parse_checkpoint_name(name) else {
            continue;
        };
        if (off, ep) != latest {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Read a specific checkpoint `<end_offset>-<epoch>.checkpoint` by id, or `None`
/// if it is absent (the leader's `FetchSnapshot` serve path).
fn load_checkpoint_by_id(dir: &std::path::Path, end_offset: i64, epoch: i32) -> Option<Vec<u8>> {
    std::fs::read(dir.join(checkpoint_name(end_offset, epoch))).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use assert2::{assert, check};
    use crabka_units::prelude::{millis, secs};

    use super::*;
    use crate::kraft::transport::NullPeerSender;

    /// Deadline every test-side channel receive is bounded by.
    const TEST_RECV_TIMEOUT: Time = secs(1);

    /// Default election timeout for engines built by [`build`].
    const TEST_ELECTION_TIMEOUT: Time = secs(1);

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
        build_with_timeout(me, ids, TEST_ELECTION_TIMEOUT)
    }

    fn build_with_timeout(
        me: NodeId,
        ids: &[NodeId],
        election_timeout: Time,
    ) -> (KraftController, tempfile::TempDir) {
        build_full(me, ids, election_timeout, 0)
    }

    fn build_with_snapshot_interval(
        me: NodeId,
        ids: &[NodeId],
        snapshot_interval_records: u64,
    ) -> (KraftController, tempfile::TempDir) {
        build_full(me, ids, TEST_ELECTION_TIMEOUT, snapshot_interval_records)
    }

    fn build_full(
        me: NodeId,
        ids: &[NodeId],
        election_timeout: Time,
        snapshot_interval_records: u64,
    ) -> (KraftController, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open log");
        let state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids));
        let ctrl = KraftController::spawn(
            KraftConfig {
                me,
                cluster_id: uuid::Uuid::nil(),
                initial_state: state,
                election_timeout,
                heartbeat_interval: None,
                controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
                metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
                metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
                peers: Arc::new(NullPeerSender),
                snapshot_interval_records,
                metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
            },
            log,
            dir.path().to_path_buf(),
        );
        (ctrl, dir)
    }

    fn build_engine_only(me: NodeId, ids: &[NodeId]) -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open log");
        let core = QuorumStateMachine::new(
            me,
            QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids)),
            TEST_ELECTION_TIMEOUT,
        );
        let image = MetadataImage::new(uuid::Uuid::nil());
        let (image_tx, _image_rx) = watch::channel(Arc::new(image.clone()));
        let (leader_tx, _leader_rx) = watch::channel(core.quorum_state().leader_id);
        let initial_snapshot = QuorumStateSnapshot {
            leader_id: core.quorum_state().leader_id,
            leader_epoch: core.quorum_state().leader_epoch,
            high_watermark: log.hwm().0,
            log_end_offset: log.log_end_offset().0,
            log_start_offset: log.log_start_offset().0,
            voters: initial_state_voters(&core),
            per_voter_fetch_offset: std::collections::BTreeMap::new(),
        };
        let (quorum_tx, _quorum_rx) = watch::channel(initial_snapshot);
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let held_epoch = core.quorum_state().leader_epoch;
        let was_leader = core.role().is_leader();
        let clock_base = Instant::now();
        (
            Engine {
                me,
                core,
                log,
                image,
                peers: Arc::new(NullPeerSender),
                image_tx,
                leader_tx,
                quorum_tx,
                cmd_tx,
                data_dir: dir.path().to_path_buf(),
                clock_base,
                election_timeout: TEST_ELECTION_TIMEOUT,
                heartbeat_interval: None,
                controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
                metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
                election_at: None,
                fetch_at: None,
                fetch_misses: 0,
                commit_waiters: Vec::new(),
                was_leader,
                held_epoch,
                snapshot_interval_records: 0,
                metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
                last_snapshot_end_offset: Offset(0),
                snapshot_fetch: None,
                installed_snapshot_epoch: None,
            },
            dir,
        )
    }

    #[derive(Debug)]
    struct CapturedPeerSend {
        peer: NodeId,
        api_key: i16,
        body: bytes::Bytes,
    }

    struct RecordingPeerSender {
        sends: mpsc::UnboundedSender<CapturedPeerSend>,
        response: bytes::Bytes,
    }

    #[async_trait::async_trait]
    impl PeerSender for RecordingPeerSender {
        async fn send(
            &self,
            peer: NodeId,
            api_key: i16,
            body: bytes::Bytes,
        ) -> Result<bytes::Bytes, RaftError> {
            self.sends
                .send(CapturedPeerSend {
                    peer,
                    api_key,
                    body,
                })
                .expect("record peer send");
            Ok(self.response.clone())
        }
    }

    fn record_peer_sends(
        engine: &mut Engine,
        response: bytes::Bytes,
    ) -> mpsc::UnboundedReceiver<CapturedPeerSend> {
        let (sends, rx) = mpsc::unbounded_channel();
        engine.peers = Arc::new(RecordingPeerSender { sends, response });
        rx
    }

    async fn recv_peer_send(
        rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
    ) -> CapturedPeerSend {
        tokio::time::timeout(TEST_RECV_TIMEOUT.to_std(), rx.recv())
            .await
            .expect("peer send timed out")
            .expect("peer send channel closed")
    }

    async fn recv_peer_send_with_api(
        rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
        api_key: i16,
    ) -> CapturedPeerSend {
        let deadline = tokio::time::Instant::now() + TEST_RECV_TIMEOUT.to_std();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let send = tokio::time::timeout(remaining, rx.recv())
                .await
                .expect("peer send with api timed out")
                .expect("peer send channel closed");
            if send.api_key == api_key {
                return send;
            }
        }
    }

    fn one_offset_batch(base_offset: i64, epoch: i32, value: &[u8]) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: epoch,
            last_offset_delta: 0,
            records: vec![Record {
                value: Some(bytes::Bytes::copy_from_slice(value)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn initial_election_deadline_matches_startup_role() {
        /// Base election timeout the staggered startup deadline is derived from.
        const TIMEOUT: Time = millis(400);
        /// The same extent in the integer milliseconds the core's jitter uses.
        const TIMEOUT_MS: u64 = 400;

        let base = Instant::now();
        let single = QuorumStateMachine::new(
            NodeId(1),
            QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(&[NodeId(1)])),
            TIMEOUT,
        );
        assert2::assert!(
            initial_election_at(&single, None, base, NodeId(1), 0, TIMEOUT) == Some(base)
        );

        let known_leader = QuorumStateMachine::new(
            NodeId(1),
            QuorumState::bootstrap(
                uuid::Uuid::nil(),
                voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
            ),
            TIMEOUT,
        );
        assert2::assert!(
            initial_election_at(&known_leader, Some(NodeId(2)), base, NodeId(1), 0, TIMEOUT)
                .is_none()
        );

        let non_voter = QuorumStateMachine::new(
            NodeId(4),
            QuorumState::bootstrap(
                uuid::Uuid::nil(),
                voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
            ),
            TIMEOUT,
        );
        assert2::assert!(
            initial_election_at(&non_voter, None, base, NodeId(4), 0, TIMEOUT).is_none()
        );

        let multi = QuorumStateMachine::new(
            NodeId(1),
            QuorumState::bootstrap(
                uuid::Uuid::nil(),
                voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
            ),
            TIMEOUT,
        );
        // The jitter is integer milliseconds and the deadline is the integer
        // sum, so the quantity must not shift the deadline by even a nanosecond.
        let jitter = crate::kraft::core::election_jitter_ms(NodeId(1), 0, TIMEOUT_MS);
        let at = initial_election_at(&multi, None, base, NodeId(1), 0, TIMEOUT)
            .expect("multi voter timer");
        assert2::assert!(at.duration_since(base) == Duration::from_millis(TIMEOUT_MS + jitter));
    }

    #[test]
    fn election_timeout_converts_to_whole_milliseconds() {
        for (_case, timeout, want_ms) in [
            ("whole second", secs(1), 1_000u64),
            ("sub-second", millis(250), 250),
            ("zero", secs(0), 0),
            ("negative clamps to zero", Time::from_millis(-4), 0),
        ] {
            check!(election_timeout_ms(timeout) == want_ms);
        }
    }

    #[test]
    fn initial_state_voters_preserves_configured_quorum_ids() {
        let (engine, _dir) = build_engine_only(NodeId(2), &[NodeId(1), NodeId(2), NodeId(3)]);
        assert2::assert!(
            initial_state_voters(&engine.core) == vec![NodeId(1), NodeId(2), NodeId(3)]
        );
        assert2::assert!(
            engine.quorum_tx.borrow().voters.clone() == vec![NodeId(1), NodeId(2), NodeId(3)]
        );
    }

    #[test]
    fn heartbeat_period_is_one_third_of_election_timeout_with_floor() {
        for (_case, timeout_ms, want_ms) in [
            ("ordinary timeout", 1000, 333),
            ("short timeout", 120, 40),
            ("floor below three milliseconds", 2, 1),
            ("zero timeout floor", 0, 1),
        ] {
            assert2::assert!(heartbeat_period(millis(timeout_ms), None) == millis(want_ms));
        }
    }

    #[test]
    fn configured_heartbeat_overrides_derived_period() {
        assert2::assert!(heartbeat_period(secs(5), Some(millis(500))) == millis(500));
    }

    #[test]
    fn election_timer_only_starts_non_leader_voters() {
        for (_case, is_voter, is_leader, want) in [
            ("non-leader voter", true, false, true),
            ("leader voter", true, true, false),
            ("non-voter follower", false, false, false),
            ("non-voter leader", false, true, false),
        ] {
            assert2::assert!(election_timer_starts_election(is_voter, is_leader) == want);
        }
    }

    #[test]
    fn following_leader_for_role_reports_followed_leader_only() {
        for (role, want) in [
            (
                Role::Follower {
                    leader_id: NodeId(7),
                    fetch_deadline: SimInstant(10),
                },
                Some(NodeId(7)),
            ),
            (
                Role::Observer {
                    leader_id: Some(NodeId(9)),
                    fetch_deadline: SimInstant(10),
                },
                Some(NodeId(9)),
            ),
            (
                Role::Observer {
                    leader_id: None,
                    fetch_deadline: SimInstant(10),
                },
                None,
            ),
            (
                Role::Leader {
                    replicas: std::collections::BTreeMap::new(),
                    high_watermark: 0,
                    epoch_start_offset: 0,
                },
                None,
            ),
        ] {
            assert2::assert!(following_leader_for_role(&role) == want);
        }
    }

    #[test]
    fn fetch_records_are_served_only_by_clean_leader_fetches() {
        for (_case, has_snapshot, has_divergence, is_leader, want) in [
            ("clean leader", false, false, true, true),
            ("snapshot response", true, false, true, false),
            ("divergence response", false, true, true, false),
            ("clean follower", false, false, false, false),
        ] {
            assert2::assert!(
                should_serve_fetch_records(has_snapshot, has_divergence, is_leader) == want
            );
        }
    }

    #[test]
    fn leadership_loss_detection_handles_stepdown_and_epoch_bump() {
        for (_case, was_leader, is_leader, held_epoch, current_epoch, want) in [
            ("leader stepped down", true, false, 3, 3, true),
            ("leader epoch advanced", true, true, 3, 4, true),
            ("leadership unchanged", true, true, 3, 3, false),
            ("follower epoch advanced", false, false, 3, 4, false),
        ] {
            assert2::assert!(
                should_fail_waiters_on_leadership_change(
                    was_leader,
                    is_leader,
                    held_epoch,
                    current_epoch
                ) == want
            );
        }
    }

    #[test]
    fn deadline_instant_offsets_from_engine_clock_base() {
        let base = Instant::now();
        let at = instant_from_clock_base(base, SimInstant(250));
        assert2::assert!(at.checked_duration_since(base) == Some(Duration::from_millis(250)));
    }

    #[test]
    fn submit_offset_helpers_use_base_plus_blob_count() {
        for (_case, base, count, want) in [
            ("empty submission", 9, 0, 9),
            ("three-record submission", 9, 3, 12),
        ] {
            assert2::assert!(assigned_record_offset(Offset(base), count) == want);
            assert2::assert!(
                submit_waiter_need_offset(Offset(base), usize::try_from(count).unwrap()) == want
            );
        }
    }

    #[test]
    fn append_result_must_match_previous_log_end_and_advance_log() {
        for (_case, expected_base, returned_base, log_end_after, want) in [
            ("matching advancing append", 4, 4, 5, true),
            ("negative returned base", 4, -1, 5, false),
            ("mismatched returned base", 4, 5, 5, false),
            ("log did not advance", 4, 4, 4, false),
        ] {
            assert2::assert!(
                append_result_is_consistent(
                    Offset(expected_base),
                    Offset(returned_base),
                    Offset(log_end_after)
                ) == want
            );
        }
        assert2::assert!(validate_append_result("test", Offset(4), Offset(4), Offset(5)).is_ok());
        assert2::assert!(validate_append_result("test", Offset(4), Offset(-1), Offset(4)).is_err());
    }

    #[test]
    fn single_voter_majority_detection_is_exact() {
        for (_case, majority, want) in [
            ("single vote", 1, true),
            ("no votes", 0, false),
            ("multiple votes", 2, false),
        ] {
            assert2::assert!(is_single_voter_majority(majority) == want);
        }
    }

    #[test]
    fn apply_window_includes_only_newly_committed_batch_bases() {
        for (_case, base_offset, prev_hwm, applied_hwm, want) in [
            ("first newly committed batch", 5, 5, 6, true),
            ("interior newly committed batch", 6, 5, 8, true),
            ("already applied batch", 4, 5, 8, false),
            ("exclusive applied boundary", 8, 5, 8, false),
        ] {
            assert2::assert!(
                batch_base_in_apply_window(base_offset, Offset(prev_hwm), Offset(applied_hwm))
                    == want
            );
        }
    }

    #[test]
    fn snapshot_threshold_uses_positive_hwm_delta_from_last_snapshot() {
        for (_case, hwm, last_snapshot_end, want) in [
            ("positive committed delta", 10, 4, 6),
            ("snapshot ahead of HWM", 4, 10, 0),
        ] {
            assert2::assert!(
                committed_records_since_snapshot(Offset(hwm), Offset(last_snapshot_end)) == want
            );
        }
        for (_case, advanced, interval, want) in [
            ("exact threshold", 3, 3, true),
            ("above threshold", 4, 3, true),
            ("below threshold", 2, 3, false),
        ] {
            assert2::assert!(snapshot_interval_reached(advanced, interval) == want);
        }
    }

    #[test]
    fn expected_hwm_after_advance_is_monotonic_and_clamped_to_log_end() {
        for (_case, prev_hwm, new_hwm, log_end, want) in [
            ("clamp above log end", 2, 5, 4, 4),
            ("prevent regression", 2, 1, 4, 2),
            ("ordinary advance", 2, 3, 4, 3),
        ] {
            assert2::assert!(
                expected_hwm_after_advance(Offset(prev_hwm), Offset(new_hwm), Offset(log_end))
                    == want
            );
        }
        for (_case, applied_hwm, expected_hwm, want) in [
            ("exact expected HWM", 4, 4, true),
            ("beyond expected HWM", 5, 4, true),
            ("below expected HWM", 3, 4, false),
        ] {
            assert2::assert!(
                hwm_advanced_as_expected(Offset(applied_hwm), Offset(expected_hwm)) == want
            );
        }
    }

    #[test]
    fn waiter_resolution_requires_hwm_to_reach_need_offset() {
        for (_case, hwm, need_offset, want) in [
            ("HWM reaches waiter", 5, 5, true),
            ("HWM passes waiter", 6, 5, true),
            ("HWM below waiter", 4, 5, false),
        ] {
            assert2::assert!(hwm_reaches_waiter(Offset(hwm), Offset(need_offset)) == want);
        }
    }

    #[test]
    fn metadata_fetch_window_is_committed_half_open_range() {
        for (_case, fetch_offset, hwm, want) in [
            ("first committed offset", 0, 1, true),
            ("last committed offset", 4, 5, true),
            ("negative offset", -1, 5, false),
            ("exclusive HWM boundary", 5, 5, false),
        ] {
            assert2::assert!(
                metadata_fetch_offset_in_committed_window(Offset(fetch_offset), Offset(hwm))
                    == want
            );
        }
        assert2::assert!(fetch_batch_committed_before_hwm(4, Offset(5)));
        assert2::assert!(!fetch_batch_committed_before_hwm(5, Offset(5)));
    }

    #[test]
    fn fetch_record_offsets_are_inside_log_window_only() {
        for (_case, fetch_offset, log_end, want) in [
            ("first available record", 0, 1, true),
            ("last available record", 4, 5, true),
            ("negative offset", -1, 5, false),
            ("exclusive log-end boundary", 5, 5, false),
        ] {
            assert2::assert!(
                fetch_offset_has_records(Offset(fetch_offset), Offset(log_end)) == want
            );
        }
    }

    #[test]
    fn fetch_epoch_uses_installed_snapshot_epoch_only_at_empty_boundary() {
        for (_case, installed, log_start, log_end, last_epoch, want) in [
            ("empty log with installed snapshot", Some(7), 10, 10, 3, 7),
            ("non-empty log with snapshot", Some(7), 10, 11, 3, 3),
            ("empty log without snapshot", None, 10, 10, 3, 3),
        ] {
            assert2::assert!(
                fetch_epoch_for_request(installed, Offset(log_start), Offset(log_end), last_epoch)
                    == want
            );
        }
    }

    #[test]
    fn fetch_batch_classifier_separates_duplicate_append_and_gap() {
        for (_case, base_offset, log_end, want) in [
            (
                "duplicate batch",
                4,
                5,
                FetchBatchDisposition::AlreadyPresent,
            ),
            ("contiguous append", 5, 5, FetchBatchDisposition::Append),
            ("offset gap", 6, 5, FetchBatchDisposition::Gap),
        ] {
            assert2::assert!(classify_fetch_batch(Offset(base_offset), Offset(log_end)) == want);
        }
    }

    #[test]
    fn snapshot_fetch_hint_starts_only_for_future_non_duplicate_snapshots() {
        for (_case, snapshot_id, log_end, in_flight, want) in [
            ("future snapshot", (11, 2), 10, None, true),
            ("snapshot at log end", (10, 2), 10, None, false),
            (
                "duplicate in-flight snapshot",
                (11, 2),
                10,
                Some((11, 2)),
                false,
            ),
            ("newer in-flight snapshot", (12, 2), 10, Some((11, 2)), true),
        ] {
            assert2::assert!(
                should_start_snapshot_fetch(snapshot_id, Offset(log_end), in_flight) == want
            );
        }
    }

    #[test]
    fn snapshot_fetch_response_is_invalid_unless_success_from_active_leader() {
        for (_case, error_code, response_epoch, current_epoch, want) in [
            ("successful active leader", 0, 2, 2, false),
            ("error from active leader", 1, 2, 2, true),
            ("success from wrong epoch", 0, 3, 2, true),
            ("error from wrong epoch", 1, 3, 2, true),
        ] {
            assert2::assert!(
                snapshot_fetch_response_invalid(
                    error_code,
                    NodeId(response_epoch),
                    NodeId(current_epoch)
                ) == want
            );
        }
    }

    #[test]
    fn checkpoint_id_ordering_prefers_higher_offset_then_epoch_without_equal_replacement() {
        for (_case, candidate, current, want) in [
            ("higher offset", (11, 1), (10, 9), true),
            ("same offset higher epoch", (10, 9), (10, 2), true),
            ("same offset lower epoch", (10, 2), (10, 9), false),
            ("equal checkpoint", (10, 9), (10, 9), false),
        ] {
            assert2::assert!(checkpoint_id_is_newer(candidate, current) == want);
        }
    }

    #[test]
    fn execute_local_only_appends_leader_change_batch_to_log() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let start = engine.log.log_end_offset();

        engine.execute_local_only(vec![Action::AppendLeaderChange { epoch: 4 }]);

        assert2::assert!(engine.log.log_end_offset() == start + 1);
        let batches = engine
            .log
            .read_decoded(start, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .expect("read appended leader-change");
        assert2::assert!(batches.len() == 1);
        let batch = &batches[0];
        check!(
            (
                batch.base_offset,
                batch.partition_leader_epoch,
                batch.attributes.is_control_batch(),
                batch.records.len(),
            ) == (start.0, 4, true, 1)
        );
    }

    #[test]
    fn leader_change_batch_encodes_control_record_payload() {
        use crabka_protocol::{
            Decode,
            owned::leader_change_message::LeaderChangeMessage,
            records::metadata::control::{ControlRecordType, control_record_key},
        };

        let batch = leader_change_batch(7, NodeId(2), &[NodeId(1), NodeId(2), NodeId(3)]);

        check!(
            (
                batch.partition_leader_epoch,
                batch.attributes.is_control_batch(),
                batch.last_offset_delta,
                batch.records.len(),
            ) == (7, true, 0, 1)
        );
        let record = &batch.records[0];
        check!(record.offset_delta == 0);
        check!(record.key.as_ref() == Some(&control_record_key(ControlRecordType::LeaderChange)));
        let value = record.value.as_ref().expect("leader change value");
        let mut cur: &[u8] = value;
        let decoded = LeaderChangeMessage::decode(&mut cur, 0).expect("decode leader change");
        check!(cur.is_empty());
        check!((decoded.version, decoded.leader_id) == (0, 2));
        let voters: Vec<i32> = decoded.voters.iter().map(|v| v.voter_id).collect();
        let granting_voters: Vec<i32> =
            decoded.granting_voters.iter().map(|v| v.voter_id).collect();
        assert2::assert!(voters == vec![1, 2, 3]);
        assert2::assert!(granting_voters == vec![1, 2, 3]);
    }

    fn elect_single_voter_engine(engine: &mut Engine) {
        engine.on_event(Event::ElectionTimeout);
        assert2::assert!(engine.core.role().is_leader());
    }

    #[tokio::test]
    async fn engine_following_leader_reflects_current_role() {
        let (mut follower, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        assert2::assert!(follower.following_leader().is_none());

        follower.on_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 1,
        });
        assert2::assert!(follower.following_leader() == Some(NodeId(2)));

        let (mut leader, _leader_dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut leader);
        assert2::assert!(leader.following_leader().is_none());
    }

    #[test]
    fn direct_single_voter_submit_applies_image_and_resolves_waiter() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut engine);
        assert2::assert!(engine.image.topic("direct").is_none());

        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&topic_record("direct"), reply);

        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        check!(engine.image.topic("direct").is_some());
        check!(engine.log.hwm() == engine.log.log_end_offset());
        check!(engine.commit_waiters.is_empty());
    }

    #[test]
    fn offset_advance_submit_returns_actor_ordered_base() {
        use crabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut engine);
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&topic_record("topic"), reply);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));

        let advance = |count| {
            vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: "topic".to_string(),
                    partition: 0,
                    count,
                },
            )]
        };

        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&advance(3), reply);
        let first = rx.try_recv().expect("first reply").expect("first ok");
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&advance(5), reply);
        let second = rx.try_recv().expect("second reply").expect("second ok");

        assert_eq!(first.offset_reservations[0].base_offset, 0);
        assert_eq!(first.offset_reservations[0].count, 3);
        assert_eq!(second.offset_reservations[0].base_offset, 3);
        assert_eq!(second.offset_reservations[0].count, 5);
        assert_eq!(engine.image.partition_next_offset("topic", 0), Some(8));
    }

    #[test]
    fn broker_registration_epoch_is_assigned_from_appended_offset() {
        use crabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut engine);

        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&topic_record("anchor"), reply);
        assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

        let base = engine.log.log_end_offset();
        let reg = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(7),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::from_u128(7),
            host: "broker-7".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        });
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&[reg], reply);

        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        assert!(engine.image.broker_epoch(NodeId(7)) == Some(base.0));
    }

    #[test]
    fn replay_committed_rebuilds_image_from_log_records() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        elect_single_voter_engine(&mut engine);
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(&topic_record("replayed"), reply);
        assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

        let mut recovered = MetadataImage::new(uuid::Uuid::nil());
        replay_committed(
            &engine.log,
            &mut recovered,
            Offset(0),
            MetadataRaftFetchMax::default(),
        );

        assert2::assert!(recovered.topic("replayed").is_some());
    }

    #[test]
    fn try_resolve_waiters_resolves_at_exact_hwm_and_keeps_future_waiter() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        for offset in 0..5 {
            let mut batch = one_offset_batch(offset, 1, b"x");
            engine.log.append(&mut batch).expect("append");
        }
        engine.log.advance_hwm(Offset(5));

        let (ready_tx, mut ready_rx) = oneshot::channel();
        let (future_tx, mut future_rx) = oneshot::channel();
        engine.commit_waiters.push(CommitWaiter {
            base_offset: Offset(4),
            need_offset: Offset(5),
            rejection: None,
            result: SubmitChangeResult::default(),
            reply: ready_tx,
        });
        engine.commit_waiters.push(CommitWaiter {
            base_offset: Offset(5),
            need_offset: Offset(6),
            rejection: None,
            result: SubmitChangeResult::default(),
            reply: future_tx,
        });

        engine.try_resolve_waiters();

        assert!(matches!(ready_rx.try_recv(), Ok(Ok(_))));
        assert!(matches!(
            future_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert2::assert!(
            engine
                .commit_waiters
                .iter()
                .map(|waiter| waiter.need_offset)
                .collect::<Vec<_>>()
                == vec![Offset(6)]
        );
    }

    #[test]
    fn fail_waiters_reached_by_fails_only_waiters_at_or_below_target_hwm() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let (future_tx, mut future_rx) = oneshot::channel();
        engine.commit_waiters.push(CommitWaiter {
            base_offset: Offset(4),
            need_offset: Offset(5),
            rejection: None,
            result: SubmitChangeResult::default(),
            reply: ready_tx,
        });
        engine.commit_waiters.push(CommitWaiter {
            base_offset: Offset(5),
            need_offset: Offset(6),
            rejection: None,
            result: SubmitChangeResult::default(),
            reply: future_tx,
        });

        engine.fail_waiters_reached_by(Offset(5), "test hwm stall");

        assert2::assert!(matches!(
            ready_rx.try_recv(),
            Ok(Err(RaftError::ChangeRejected(_)))
        ));
        assert2::assert!(matches!(
            future_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert2::assert!(
            engine
                .commit_waiters
                .iter()
                .map(|waiter| waiter.need_offset)
                .collect::<Vec<_>>()
                == vec![Offset(6)]
        );
    }

    #[test]
    fn publish_leader_updates_leader_and_quorum_watchers() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        let mut leader_rx = engine.leader_tx.subscribe();
        let quorum_rx = engine.quorum_tx.subscribe();

        engine.on_event(Event::ElectionTimeout);

        check!(
            (
                *leader_rx.borrow_and_update(),
                quorum_rx.borrow().leader_id,
                quorum_rx.borrow().log_end_offset,
            ) == (
                Some(NodeId(1)),
                Some(NodeId(1)),
                engine.log.log_end_offset().0,
            )
        );
    }

    #[tokio::test]
    async fn broadcast_end_quorum_epoch_sends_to_every_other_voter() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        let mut sends =
            record_peer_sends(&mut engine, wire::PeerResponse::Ack { epoch: 4 }.encode());

        engine.broadcast_end_quorum_epoch(4);

        let mut peers = Vec::new();
        for _ in 0..2 {
            let send = recv_peer_send(&mut sends).await;
            assert2::assert!(send.api_key == api_key::END_QUORUM_EPOCH);
            match wire::decode_end(&send.body) {
                Some(wire::PeerRequest::EndQuorumEpoch {
                    leader_id,
                    leader_epoch,
                }) => {
                    assert2::assert!(leader_id == NodeId(1));
                    assert2::assert!(leader_epoch == 4);
                }
                other => panic!("unexpected end quorum request: {other:?}"),
            }
            peers.push(send.peer);
        }
        peers.sort_unstable();
        assert2::assert!(peers == vec![NodeId(2), NodeId(3)]);
    }

    #[test]
    fn metadata_fetch_slice_excludes_negative_hwm_and_uncommitted_batches() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        let mut first = one_offset_batch(0, 1, b"a");
        let mut second = one_offset_batch(1, 1, b"b");
        engine.log.append(&mut first).expect("append first");
        engine.log.append(&mut second).expect("append second");
        engine.log.advance_hwm(Offset(1));

        assert2::assert!(
            engine
                .metadata_fetch_slice(-1, DEFAULT_METADATA_RAFT_FETCH_MAX)
                .records
                .is_empty()
        );
        assert2::assert!(
            engine
                .metadata_fetch_slice(1, DEFAULT_METADATA_RAFT_FETCH_MAX)
                .records
                .is_empty()
        );

        let slice = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
        let decoded = decode_batches(&slice.records).expect("decode fetch slice");
        check!(
            (
                decoded
                    .iter()
                    .map(|batch| batch.base_offset)
                    .collect::<Vec<_>>(),
                slice.high_watermark,
            ) == (vec![0], 1)
        );
    }

    #[tokio::test]
    async fn send_fetch_uses_snapshot_epoch_only_until_log_extends_past_boundary() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
        engine
            .log
            .install_snapshot(Offset(10))
            .expect("install snapshot");
        engine.installed_snapshot_epoch = Some(7);
        let fetch_response = wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 7,
            diverging: None,
            snapshot_id: None,
            hwm: 10,
            records: bytes::Bytes::new(),
        }
        .encode();
        let mut sends = record_peer_sends(&mut engine, fetch_response.clone());

        engine.send_fetch(NodeId(2));
        let send = recv_peer_send(&mut sends).await;
        match wire::decode_fetch(&send.body) {
            Some(wire::PeerRequest::Fetch {
                fetch_epoch,
                fetch_offset,
                ..
            }) => {
                assert2::assert!(fetch_epoch == 7);
                assert2::assert!(fetch_offset == 10);
            }
            other => panic!("unexpected fetch request: {other:?}"),
        }

        let mut batch = one_offset_batch(10, 9, b"after-snapshot");
        engine
            .log
            .append_at(&mut batch, Offset(10))
            .expect("append after snapshot");
        engine.send_fetch(NodeId(2));
        let send = recv_peer_send(&mut sends).await;
        match wire::decode_fetch(&send.body) {
            Some(wire::PeerRequest::Fetch {
                fetch_epoch,
                fetch_offset,
                ..
            }) => {
                assert2::assert!(fetch_epoch == 9);
                assert2::assert!(fetch_offset == 11);
            }
            other => panic!("unexpected fetch request: {other:?}"),
        }
    }

    #[test]
    fn serve_fetch_records_returns_batches_only_for_offsets_inside_log() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
        let mut batch = one_offset_batch(0, 1, b"a");
        engine.log.append(&mut batch).expect("append");

        assert2::assert!(engine.serve_fetch_records(Offset(-1)).is_empty());
        assert2::assert!(engine.serve_fetch_records(Offset(1)).is_empty());
        let records = engine.serve_fetch_records(Offset(0));
        let decoded = decode_batches(&records).expect("decode served records");
        assert2::assert!(
            decoded
                .iter()
                .map(|batch| batch.base_offset)
                .collect::<Vec<_>>()
                == vec![0]
        );
    }

    #[tokio::test]
    async fn fetch_response_snapshot_hint_starts_once_and_ignores_stale_hint() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
        let fetch_snapshot_response = wire::PeerResponse::FetchSnapshot {
            snapshot_id: (11, 3),
            size: 0,
            position: 0,
            bytes: bytes::Bytes::new(),
            error_code: 0,
        }
        .encode();
        let mut sends = record_peer_sends(&mut engine, fetch_snapshot_response);

        let body = wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 3,
            diverging: None,
            snapshot_id: Some((11, 3)),
            hwm: 11,
            records: bytes::Bytes::new(),
        }
        .encode();
        engine.on_fetch_response(NodeId(2), &body);
        let send = recv_peer_send_with_api(&mut sends, api_key::FETCH_SNAPSHOT).await;
        match wire::decode_fetch_snapshot(&send.body) {
            Some(wire::PeerRequest::FetchSnapshot {
                snapshot_id,
                position,
                ..
            }) => {
                assert2::assert!(snapshot_id == (11, 3));
                assert2::assert!(position == 0);
            }
            other => panic!("unexpected fetch snapshot request: {other:?}"),
        }
        assert2::assert!(
            engine
                .snapshot_fetch
                .as_ref()
                .is_some_and(|s| s.snapshot_id == (11, 3))
        );

        engine.on_fetch_response(NodeId(2), &body);
        assert2::assert!(
            tokio::time::timeout(StdDuration::from_millis(20), async {
                loop {
                    let send = recv_peer_send(&mut sends).await;
                    if send.api_key == api_key::FETCH_SNAPSHOT {
                        return send;
                    }
                }
            })
            .await
            .is_err()
        );

        engine
            .log
            .install_snapshot(Offset(11))
            .expect("install snapshot");
        engine.snapshot_fetch = None;
        engine.on_fetch_response(NodeId(2), &body);
        assert2::assert!(engine.snapshot_fetch.is_none());
    }

    #[tokio::test]
    async fn fetch_snapshot_response_error_or_wrong_leader_aborts_transfer() {
        let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
        let fetch_response = wire::PeerResponse::Fetch {
            leader_id: NodeId(2),
            leader_epoch: 3,
            diverging: None,
            snapshot_id: None,
            hwm: 0,
            records: bytes::Bytes::new(),
        }
        .encode();
        let mut sends = record_peer_sends(&mut engine, fetch_response);

        engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
        let error_body = wire::PeerResponse::FetchSnapshot {
            snapshot_id: (12, 3),
            size: 0,
            position: 0,
            bytes: bytes::Bytes::new(),
            error_code: 99,
        }
        .encode();
        engine.on_fetch_snapshot_response(NodeId(2), &error_body);
        assert2::assert!(engine.snapshot_fetch.is_none());
        let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
        assert2::assert!(send.peer == 2);

        engine.snapshot_fetch = Some(SnapshotFetchState::new((12, 3), NodeId(2)));
        let ok_body = wire::PeerResponse::FetchSnapshot {
            snapshot_id: (12, 3),
            size: 0,
            position: 0,
            bytes: bytes::Bytes::new(),
            error_code: 0,
        }
        .encode();
        engine.on_fetch_snapshot_response(NodeId(3), &ok_body);
        assert2::assert!(engine.snapshot_fetch.is_none());
        let send = recv_peer_send_with_api(&mut sends, api_key::FETCH).await;
        assert2::assert!(send.peer == 3);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_until_opt_waits_for_some_and_never_completes_for_none() {
        assert2::assert!(
            tokio::time::timeout(StdDuration::from_millis(1), sleep_until_opt(None))
                .await
                .is_err()
        );

        let deadline = Instant::now() + Duration::from_millis(50);
        let mut sleep = Box::pin(sleep_until_opt(Some(deadline)));
        assert2::assert!(
            tokio::time::timeout(StdDuration::from_millis(1), &mut sleep)
                .await
                .is_err()
        );
        tokio::time::advance(Duration::from_millis(50)).await;
        assert2::assert!(
            tokio::time::timeout(StdDuration::from_millis(1), sleep)
                .await
                .is_ok()
        );
    }

    /// A realistic single-partition create batch: a `V1Topic` plus its one
    /// `V1Partition`. KIP-631 framing derives the topic's partition count from
    /// the partition records (the `TopicRecord` wire shape carries no count), so
    /// a bare `V1Topic` would round-trip back to zero partitions and fail
    /// validation on apply.
    fn topic_record(name: &str) -> Vec<crabka_metadata::MetadataRecord> {
        topic_record_named(name, 1)
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
        })
        .await
        .unwrap();
        // Candidate round runs at the bumped epoch 1.
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: helper,
            epoch: 1,
            vote_granted: true,
        })
        .await
        .unwrap();
        await_leader(ctrl, Some(me)).await;
    }

    async fn await_leader(ctrl: &KraftController, want: Option<NodeId>) {
        let result = tokio::time::timeout(StdDuration::from_secs(2), async {
            let mut rx = ctrl.watch_leader();
            loop {
                if *rx.borrow() == want {
                    return;
                }
                rx.changed().await.expect("leader watch closed");
            }
        })
        .await;
        assert2::assert!(result.is_ok());
    }

    async fn submit_change_with_timeout(
        ctrl: &KraftController,
        records: Vec<crabka_metadata::MetadataRecord>,
        context: &str,
    ) -> Result<(), RaftError> {
        tokio::time::timeout(StdDuration::from_secs(2), ctrl.submit_change(records))
            .await
            .unwrap_or_else(|_| panic!("{context} submit_change timed out"))
            .map(|_| ())
    }

    #[tokio::test]
    async fn single_voter_engine_starts_with_no_initial_leader() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        let initial = *ctrl.watch_leader().borrow();
        assert2::assert!(initial.is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn node_id_reports_configured_node() {
        let (ctrl, _dir) = build(NodeId(7), &[NodeId(7)]);
        assert2::assert!(ctrl.node_id() == 7);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn injected_election_makes_single_voter_leader() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn injected_vote_sequence_makes_multi_voter_leader_before_timer() {
        let (ctrl, _dir) =
            build_with_timeout(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)], secs(60));
        elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn injected_election_timer_makes_single_voter_leader() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.cmd_tx
            .send(Command::Timer(TimerTick::Election))
            .await
            .unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn committed_batch_applies_to_image() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        assert2::assert!(ctrl.current_image().topic("t").is_none());

        let off = ctrl
            .test_append_and_commit(topic_record("t"))
            .await
            .unwrap();
        assert2::assert!(off >= 0);

        let mut img_rx = ctrl.watch_image();
        assert2::assert!(img_rx.borrow_and_update().topic("t").is_some());
        assert2::assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_committed_record_rejected_on_apply() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        ctrl.test_append_and_commit(topic_record("t"))
            .await
            .unwrap();
        assert2::assert!(ctrl.current_image().topic("t").is_some());

        ctrl.test_append_and_commit(topic_record("t"))
            .await
            .unwrap();
        assert2::assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    // ---- timers + liveness ----

    /// A single-voter engine started with the REAL clock auto-elects after the
    /// election timeout — no injected event.
    #[tokio::test]
    async fn single_voter_auto_elects_on_election_timeout() {
        let (ctrl, _dir) = build_with_timeout(NodeId(1), &[NodeId(1)], millis(80));
        // The election timer is armed at construction; wait for it to fire.
        tokio::time::timeout(
            StdDuration::from_secs(5),
            await_leader(&ctrl, Some(NodeId(1))),
        )
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
        let (ctrl, _dir) =
            build_with_timeout(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)], millis(120));
        // Attach to leader 2.
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 1,
        })
        .await
        .unwrap();
        await_leader(&ctrl, Some(NodeId(2))).await;

        // Keep re-announcing leader 2 faster than the fetch watchdog would
        // accumulate FETCH_MISS_LIMIT misses; the leader must remain 2.
        for _ in 0..6 {
            tokio::time::sleep(StdDuration::from_millis(40)).await;
            ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 1,
            })
            .await
            .unwrap();
        }
        let leader = *ctrl.watch_leader().borrow();
        assert2::assert!(leader == Some(NodeId(2)));
        ctrl.shutdown().await;
    }

    // ---- handle ops ----

    #[tokio::test]
    async fn submit_change_commits_on_single_voter_leader() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        tokio::time::timeout(
            StdDuration::from_secs(5),
            ctrl.submit_change(topic_record("orders")),
        )
        .await
        .expect("submit did not hang")
        .expect("submit ok");
        assert2::assert!(ctrl.current_image().topic("orders").is_some());

        let qs = ctrl.quorum_state().await.unwrap();
        assert2::assert!(qs.leader_id == Some(NodeId(1)));
        assert2::assert!(qs.high_watermark > 0);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_change_duplicate_rejected() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        submit_change_with_timeout(&ctrl, topic_record("t"), "first duplicate-test submit")
            .await
            .unwrap();
        let dup =
            submit_change_with_timeout(&ctrl, topic_record("t"), "duplicate-test submit").await;
        assert2::assert!(matches!(dup, Err(RaftError::Metadata(_))));
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
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

        // Park a submit on a separate task: it appends but cannot commit (no
        // peer fetches under NullPeerSender), so it stays parked.
        let ctrl2 = ctrl.clone();
        let submit = tokio::spawn(async move { ctrl2.submit_change(topic_record("orders")).await });

        // Give the submit a moment to reach the engine and park its waiter.
        tokio::time::sleep(StdDuration::from_millis(50)).await;

        // A strictly-higher-epoch BeginQuorumEpoch from node 2 forces node 1 to
        // step down from Leader to Follower.
        ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 9,
        })
        .await
        .unwrap();

        // The parked submit must resolve promptly (bounded) with NotLeader.
        let result = tokio::time::timeout(StdDuration::from_secs(5), submit)
            .await
            .expect("submit did not hang on leadership loss")
            .expect("join");
        assert2::assert!(matches!(
            result,
            Err(RaftError::NotLeader {
                current_leader: Some(NodeId(2))
            })
        ));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_change_on_non_leader_rejects() {
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        // Never elected; node 1 is Unattached → not leader.
        let r = ctrl.submit_change(topic_record("t")).await;
        assert2::assert!(matches!(r, Err(RaftError::NotLeader { .. })));
        ctrl.shutdown().await;
    }

    fn topic_record_named(name: &str, id: u128) -> Vec<crabka_metadata::MetadataRecord> {
        vec![
            crabka_metadata::MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
                name: name.to_string(),
                topic_id: uuid::Uuid::from_u128(id),
                partitions: 1,
                replication_factor: 1,
            }),
            crabka_metadata::MetadataRecord::V1Partition(crabka_metadata::PartitionRecord {
                topic: name.to_string(),
                partition: 0,
                leader: NodeId(1),
                replicas: vec![NodeId(1)],
                isr: vec![NodeId(1)],
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ]
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
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
        elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2)).await;

        let ca = ctrl.clone();
        let cb = ctrl.clone();
        let cc = ctrl.clone();
        // A and B both create topic "first"; B is the duplicate that fails apply.
        // C creates a distinct "third" and must commit cleanly.
        let a = tokio::spawn(async move { ca.submit_change(topic_record_named("first", 1)).await });
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        let b = tokio::spawn(async move { cb.submit_change(topic_record_named("first", 1)).await });
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        let c = tokio::spawn(async move { cc.submit_change(topic_record_named("third", 3)).await });
        tokio::time::sleep(StdDuration::from_millis(40)).await;

        // Drive the HWM past all appended batches by simulating a follower (node
        // 2) that has fetched the whole log. With a 3-voter majority of 2, the
        // leader's own log end plus node 2's fetch offset commits everything.
        let qs = ctrl.quorum_state().await.unwrap();
        ctrl.inject_event(Event::ReceiveFetch {
            from: NodeId(2),
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

        check!(ra.is_ok(), "A (first valid) should commit: {ra:?}");
        assert2::assert!(matches!(rb, Err(RaftError::Metadata(_))));
        check!(
            rc.is_ok(),
            "C (distinct valid) must NOT bleed B's rejection: {rc:?}"
        );
        ctrl.shutdown().await;
    }

    // ---- recovery + quorum-state file ----

    #[tokio::test]
    async fn snapshot_then_restart_recovers_image() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_dir = dir.path().to_path_buf();
        let cluster_id = uuid::Uuid::from_u128(7);
        let voters = voter_set(&[NodeId(1)]);

        {
            let log = KraftLog::open(&data_dir).expect("open log");
            let ctrl = KraftController::spawn(
                KraftConfig {
                    me: NodeId(1),
                    cluster_id,
                    initial_state: QuorumState::bootstrap(cluster_id, voters.clone()),
                    election_timeout: TEST_ELECTION_TIMEOUT,
                    heartbeat_interval: None,
                    controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
                    metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(
                    ),
                    metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
                    peers: Arc::new(NullPeerSender),
                    snapshot_interval_records: 0,
                    metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
                },
                log,
                data_dir.clone(),
            );
            ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
            await_leader(&ctrl, Some(NodeId(1))).await;
            submit_change_with_timeout(&ctrl, topic_record("recovered"), "recovery seed")
                .await
                .unwrap();
            assert2::assert!(ctrl.current_image().topic("recovered").is_some());
            ctrl.trigger_snapshot().await.unwrap();
            ctrl.shutdown().await;
            // Give the loop a moment to fully drain.
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }

        // Reopen over the same dir: the image is rebuilt from checkpoint+log.
        let ctrl2 = KraftController::open(
            data_dir.clone(),
            NodeId(1),
            cluster_id,
            voters,
            TEST_ELECTION_TIMEOUT,
            None,
            ControllerFetchMissLimit::default(),
            MetadataRaftCommandQueueCapacity::default(),
            MetadataRaftFetchMax::default(),
            Arc::new(NullPeerSender),
            0,
            MetadataSnapshotFetchMax::default(),
        )
        .expect("reopen");
        assert2::assert!(ctrl2.current_image().topic("recovered").is_some());
        ctrl2.shutdown().await;
    }

    #[tokio::test]
    async fn quorum_state_file_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cid = uuid::Uuid::from_u128(9);
        let mut state = QuorumState::bootstrap(cid, voter_set(&[NodeId(1), NodeId(2), NodeId(3)]));
        state.leader_epoch = 5;
        state.leader_id = Some(NodeId(2));
        state.voted_key = Some(ReplicaKey {
            id: NodeId(3),
            directory_id: uuid::Uuid::from_u128(3),
        });
        save_quorum_state(dir.path(), &state).unwrap();

        let loaded = load_quorum_state(
            dir.path(),
            cid,
            &voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
        )
        .unwrap()
        .expect("present");
        // Leadership is volatile (Raft persists only currentTerm + votedFor):
        // `leader_id` is deliberately cleared on load so a restarted ex-leader
        // re-discovers the current leader instead of trusting stale state.
        check!(
            (
                loaded.leader_epoch,
                loaded.leader_id,
                loaded.voted_key.map(|k| k.id),
                loaded.cluster_id,
            ) == (5, None, Some(NodeId(3)), cid)
        );
    }

    #[test]
    fn load_quorum_state_reports_unreadable_non_missing_file_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join(QUORUM_STATE_FILE)).expect("mkdir quorum-state path");

        let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]));

        assert2::assert!(matches!(loaded, Err(RaftError::Storage(_))));
    }

    #[test]
    fn load_quorum_state_ignores_truncated_file_without_panicking() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(QUORUM_STATE_FILE), [0u8; 53]).expect("write short state");

        let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]))
            .expect("short file is ignored");

        assert2::assert!(loaded.is_none());
    }

    // ---- snapshot trigger + prune ----

    /// A single-voter leader with `snapshot_interval_records = 3` snapshots and
    /// prunes once the committed offset has advanced past the threshold. After
    /// committing four distinct topics, a checkpoint exists on disk and the log
    /// has been pruned (its log-start offset rose above 0).
    #[tokio::test]
    async fn leader_snapshots_and_prunes_at_threshold() {
        let (ctrl, dir) = build_with_snapshot_interval(NodeId(1), &[NodeId(1)], 3);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        // Four distinct topics, each committed immediately (single voter). Each
        // commit advances the HWM well past the 3-record interval, so a
        // snapshot+prune fires.
        for name in ["a", "b", "c", "d"] {
            submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
                .await
                .unwrap();
        }

        // A checkpoint was written.
        let cp = load_latest_checkpoint(&checkpoint_dir(dir.path()))
            .expect("scan checkpoints")
            .expect("a checkpoint exists");
        assert2::assert!(!cp.is_empty());

        // The log was pruned: log-start advanced past 0.
        let qs = ctrl.quorum_state().await.unwrap();
        assert2::assert!(qs.log_start_offset > 0);
        ctrl.shutdown().await;
    }

    #[test]
    fn latest_checkpoint_id_picks_highest_offset_then_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cp_dir = checkpoint_dir(dir.path());
        write_checkpoint(&cp_dir, 10, 2, b"ten-two").expect("write checkpoint 10/2");
        write_checkpoint(&cp_dir, 10, 9, b"ten-nine").expect("write checkpoint 10/9");
        write_checkpoint(&cp_dir, 11, 1, b"eleven-one").expect("write checkpoint 11/1");

        assert2::assert!(latest_checkpoint_id(&cp_dir) == Some((11, 1)));
        let latest = load_latest_checkpoint(&cp_dir)
            .expect("load latest")
            .expect("latest exists");
        assert2::assert!(latest == b"eleven-one");
    }

    #[test]
    fn retain_latest_checkpoint_deletes_older_checkpoints_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cp_dir = checkpoint_dir(dir.path());
        write_checkpoint(&cp_dir, 5, 1, b"old").expect("write old");
        write_checkpoint(&cp_dir, 6, 1, b"new").expect("write new");
        write_checkpoint(&cp_dir, 6, 0, b"older-same-offset").expect("write older same offset");

        retain_latest_checkpoint(&cp_dir);

        for (_case, end_offset, epoch, want_present) in [
            ("matching checkpoint", 6, 1, true),
            ("wrong end offset", 5, 1, false),
            ("wrong epoch", 6, 0, false),
        ] {
            assert2::assert!(
                load_checkpoint_by_id(&cp_dir, end_offset, epoch).is_some() == want_present
            );
        }
        let entries: Vec<_> = std::fs::read_dir(&cp_dir)
            .expect("read checkpoint dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("read entries");
        assert2::assert!(entries.len() == 1);
    }

    #[tokio::test]
    async fn broker_registration_epoch_equals_commit_offset() {
        use crabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
        let (ctrl, _dir) = build(NodeId(1), &[NodeId(1)]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;

        let reg = |id: u64| {
            vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(id),
                    broker_epoch: 0, // overwritten by the leader at append
                    incarnation_id: uuid::Uuid::from_u128(u128::from(id)),
                    host: "h".into(),
                    port: 9092,
                    rack: None,
                    endpoints: vec![],
                },
            )]
        };

        let base1 = ctrl.quorum_state().await.unwrap().log_end_offset;
        submit_change_with_timeout(&ctrl, reg(7), "first broker registration")
            .await
            .expect("first registration");
        let e1 = ctrl.current_image().broker_epoch(NodeId(7));
        assert2::assert!(e1 == Some(base1));

        let base2 = ctrl.quorum_state().await.unwrap().log_end_offset;
        submit_change_with_timeout(&ctrl, reg(7), "broker re-registration")
            .await
            .expect("re-registration");
        let e2 = ctrl.current_image().broker_epoch(NodeId(7));
        assert2::assert!(e2 == Some(base2));
        assert2::assert!(base2 > base1 && e2 > e1);

        ctrl.shutdown().await;
    }
}
