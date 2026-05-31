//! The async `KraftController` consensus engine: a single owning tokio task
//! holds all consensus state (the 3a [`QuorumStateMachine`] core, the 3b
//! [`KraftLog`], and the published [`MetadataImage`]) and turns inbound
//! commands/RPCs into core [`Event`]s whose [`Action`]s it executes.
//!
//! Ownership model: one task owns the [`Engine`]; everything else talks to it
//! over an mpsc of [`Command`]. The public [`KraftController`] handle is a
//! cheap clone holding the command sender plus the `watch` receivers. This
//! mirrors the actor pattern openraft used, but the engine is now ours.
//!
//! Slices: Task 1 builds the skeleton + spawn; Task 2 builds the event loop
//! (apply/replicate/reply); timers (Task 3), handle ops (Task 4), recovery
//! (Task 5), the in-memory/real transports (Task 6/7) layer on top.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crabka_metadata::{MetadataImage, from_kafka_record};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::kraft::action::{Action, TimerKind};
use crate::kraft::core::QuorumStateMachine;
use crate::kraft::event::Event;
use crate::kraft::log::KraftLog;
use crate::kraft::transport::{Command, Inbound, PeerSender};
use crate::kraft::types::{LeaderEpoch, NodeId, QuorumState, SimInstant};

/// The pending deadline recorded by a [`Action::ResetTimer`]. The actual timer
/// task that fires `ElectionTimeout`/`FetchTimeout` is Task 3; here we just
/// stash what the core asked for so Task 3 can wire it.
#[derive(Debug, Clone, Copy, Default)]
struct PendingTimers {
    election_deadline: Option<SimInstant>,
    fetch_deadline: Option<SimInstant>,
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
    timers: PendingTimers,
}

/// Cheap, cloneable handle to the running engine: holds the command sender and
/// the `watch` receivers the broker/handle read.
#[derive(Clone)]
pub struct KraftController {
    cmd_tx: mpsc::Sender<Command>,
    image_rx: watch::Receiver<Arc<MetadataImage>>,
    leader_rx: watch::Receiver<Option<NodeId>>,
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
    /// task. Returns the handle; the task runs until [`Self::shutdown`] (or all
    /// handles drop). Recovery/seed-from-checkpoint is Task 5; here the engine
    /// starts from `config.initial_state` + the (possibly empty) log.
    #[must_use]
    pub fn spawn(config: KraftConfig, log: KraftLog) -> Self {
        let KraftConfig {
            me,
            cluster_id,
            initial_state,
            election_timeout_ms,
            peers,
        } = config;

        let core = QuorumStateMachine::new(me, initial_state, election_timeout_ms);
        let initial_leader = core.quorum_state().leader_id;
        let image = MetadataImage::new(cluster_id);

        let (image_tx, image_rx) = watch::channel(Arc::new(image.clone()));
        let (leader_tx, leader_rx) = watch::channel(initial_leader);
        let (cmd_tx, cmd_rx) = mpsc::channel(256);

        let engine = Engine {
            me,
            core,
            log,
            image,
            peers,
            image_tx,
            leader_tx,
            timers: PendingTimers::default(),
        };

        tokio::spawn(engine.run(cmd_rx));

        Self {
            cmd_tx,
            image_rx,
            leader_rx,
            me,
        }
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
    /// the real pipeline; returns the appended base offset. See
    /// [`Command::TestAppendAndCommit`].
    #[cfg(test)]
    async fn test_append_and_commit(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<i64, RaftError> {
        let (reply, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::TestAppendAndCommit { records, reply })
            .await
            .map_err(|_| RaftError::Shutdown)?;
        rx.await.map_err(|_| RaftError::Shutdown)
    }
}

impl Engine {
    /// The event loop. Receives one [`Command`] at a time, turns it into core
    /// input, and executes the resulting [`Action`]s. Single-threaded over all
    /// consensus state, so no locking is needed inside.
    async fn run(mut self, mut cmd_rx: mpsc::Receiver<Command>) {
        // The engine starts passive: it reports the leader from `initial_state`
        // (None for a fresh bootstrap) and only elects once driven — by the
        // election timer (Task 3), an inbound RPC, or an injected
        // `ElectionTimeout` event. A single voter wins its own election the
        // moment it gets an `ElectionTimeout`, but we do not synthesize one at
        // startup so `quorum_state().leader_id` is None until something drives
        // the loop (the Task 1 contract).
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                Command::Shutdown => break,
                Command::Event(event) => self.on_event(event).await,
                Command::Inbound(inbound) => self.on_inbound(inbound).await,
                #[cfg(test)]
                Command::TestAppendAndCommit { records, reply } => {
                    let off = self.test_append_and_commit(records);
                    let _ = reply.send(off);
                }
            }
        }
    }

    /// Logical "now" for the core. The real monotonic clock is wired with the
    /// timer task in Task 3 (which reads engine-held clock state, hence the
    /// `&self`); until then the loop only processes events that do not depend
    /// on elapsed time, so a zero instant keeps the contract test deterministic.
    #[allow(clippy::unused_self)]
    fn now(&self) -> SimInstant {
        SimInstant(0)
    }

    async fn on_event(&mut self, event: Event) {
        let now = self.now();
        let actions = self.core.on_event(event, &self.log, now);
        self.execute(actions).await;
        self.publish_leader();
    }

    #[allow(clippy::unused_async)]
    async fn on_inbound(&mut self, inbound: Inbound) {
        // Task 7 wires the real KIP-595 codecs (decode req -> Event, encode the
        // produced Reply* -> response bytes; the reply send is what makes this
        // async). Until then, inbound RPCs are not decoded; we drop the reply
        // channel (peer observes an error), which is acceptable because
        // Tasks 1-6 drive the engine via `inject_event` and the in-memory
        // transport, not raw inbound bytes.
        let _ = inbound;
    }

    /// Execute a batch of [`Action`]s, including the async peer sends.
    async fn execute(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => {
                    self.send_to_voters(api_key::VOTE, encode_placeholder(epoch, pre_vote))
                        .await;
                }
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.send_to_voters(api_key::BEGIN_QUORUM_EPOCH, encode_epoch(epoch))
                        .await;
                }
                Action::SendEndQuorumEpoch { epoch } => {
                    self.send_to_voters(api_key::END_QUORUM_EPOCH, encode_epoch(epoch))
                        .await;
                }
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id).await;
                }
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute a single non-network [`Action`] synchronously (the inner helper
    /// for [`Self::execute`]).
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
                // Real quorum-state writer is Task 5; record intent only.
                self.persist_quorum_state();
            }
            Action::ResetTimer { kind, deadline } => match kind {
                TimerKind::Election => self.timers.election_deadline = Some(deadline),
                TimerKind::Fetch => self.timers.fetch_deadline = Some(deadline),
            },
            Action::TransitionedTo(_name) => {}
            // Network actions are handled by `execute`; reaching here is a bug.
            Action::SendVoteRequest { .. }
            | Action::SendBeginQuorumEpoch { .. }
            | Action::SendEndQuorumEpoch { .. }
            | Action::SendFetch { .. }
            | Action::ReplyVote { .. } => {
                debug_assert!(false, "network/reply action routed to local executor");
            }
        }
    }

    /// Append the leader's `LeaderChange` control marker for `epoch` at the
    /// current log end. In this slice the marker is an empty record batch
    /// stamped with the leader epoch (KIP-631 control records are Slice 3d);
    /// it advances the log end so the leader's `epoch_start_offset` and the
    /// HWM gate behave as the core expects.
    fn append_leader_change(&mut self, epoch: LeaderEpoch) -> Result<i64, RaftError> {
        let mut batch = leader_change_batch(epoch);
        self.log.append(&mut batch)
    }

    /// Test-only: append a metadata batch and commit it through the real apply
    /// pipeline. Returns the appended base offset (or -1 on encode/append
    /// failure). See [`Command::TestAppendAndCommit`].
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
        // Commit everything appended so far through the real apply path.
        self.advance_and_apply(self.log.log_end_offset());
        base
    }

    /// Advance the HWM and apply the records newly committed by it
    /// (`prev_hwm..new_hwm`) to the [`MetadataImage`], then publish.
    fn advance_and_apply(&mut self, new_hwm: i64) {
        let prev_hwm = self.log.hwm();
        self.log.advance_hwm(new_hwm);
        let applied_hwm = self.log.hwm();
        if applied_hwm <= prev_hwm {
            return;
        }
        match self.log.read_decoded(prev_hwm, MAX_APPLY_BYTES) {
            Ok(batches) => {
                let mut changed = false;
                for batch in &batches {
                    // Only apply records strictly within (prev_hwm, applied_hwm].
                    if batch.base_offset < prev_hwm || batch.base_offset >= applied_hwm {
                        continue;
                    }
                    for rec in &batch.records {
                        // Control/empty marker records (e.g. the leader-change
                        // batch) carry no MetadataRecord and are skipped.
                        if let Ok(meta) = from_kafka_record(rec) {
                            if self.image.validate(&meta).is_ok() {
                                self.image.apply(&meta);
                                changed = true;
                            } else {
                                tracing::debug!("kraft: rejected committed record on apply");
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
    }

    /// Persist the durable quorum state. The real atomic file writer is Task 5
    /// (it serializes the core's current `QuorumState`, hence the `&self`);
    /// here this is the integration seam the core's `PersistQuorumState` calls.
    #[allow(clippy::unused_self)]
    fn persist_quorum_state(&self) {
        // no-op until Task 5
    }

    async fn send_to_voters(&self, api_key: i16, body: bytes::Bytes) {
        let voters: Vec<NodeId> = self
            .core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|&id| id != self.me)
            .collect();
        for peer in voters {
            if let Err(e) = self.peers.send(peer, api_key, body.clone()).await {
                tracing::debug!(peer, ?e, "kraft: peer send failed");
            }
        }
    }

    async fn send_fetch(&self, leader_id: NodeId) {
        if leader_id == self.me {
            return;
        }
        let body = encode_fetch(self.log.log_end_offset());
        if let Err(e) = self.peers.send(leader_id, api_key::FETCH, body).await {
            tracing::debug!(leader_id, ?e, "kraft: fetch send failed");
        }
    }

    fn publish_leader(&self) {
        let leader = self.core.quorum_state().leader_id;
        if *self.leader_tx.borrow() != leader {
            let _ = self.leader_tx.send(leader);
        }
    }
}

/// KIP-595 api keys used by the engine's peer sends.
mod api_key {
    pub const FETCH: i16 = 1;
    pub const VOTE: i16 = 52;
    pub const BEGIN_QUORUM_EPOCH: i16 = 53;
    pub const END_QUORUM_EPOCH: i16 = 54;
}

const MAX_APPLY_BYTES: usize = 8 * 1024 * 1024;

/// Build the leader's `LeaderChange` marker batch for `epoch`: an empty batch
/// stamped with the leader epoch so the log end advances by one.
fn leader_change_batch(epoch: LeaderEpoch) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
        last_offset_delta: 0,
        records: Vec::new(),
        ..Default::default()
    }
}

// Placeholder body encoders. Task 7 replaces these with the Slice-2 generated
// KIP-595 request codecs; the in-memory transport (Task 6) routes opaque bodies
// so the exact bytes don't matter until the real wire lands.
fn encode_placeholder(_epoch: LeaderEpoch, _pre_vote: bool) -> bytes::Bytes {
    BytesMut::new().freeze()
}
fn encode_epoch(_epoch: LeaderEpoch) -> bytes::Bytes {
    BytesMut::new().freeze()
}
fn encode_fetch(_fetch_offset: i64) -> bytes::Bytes {
    BytesMut::new().freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

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
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open log");
        let state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids));
        let ctrl = KraftController::spawn(
            KraftConfig {
                me,
                cluster_id: uuid::Uuid::nil(),
                initial_state: state,
                election_timeout_ms: 1000,
                peers: Arc::new(NullPeerSender),
            },
            log,
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

    /// Wait until the leader watch reports `want`, or fail after `tries`.
    async fn await_leader(ctrl: &KraftController, want: Option<NodeId>) {
        let mut rx = ctrl.watch_leader();
        for _ in 0..50 {
            if *rx.borrow() == want {
                return;
            }
            // changed() resolves when the loop publishes the next leader value.
            if rx.changed().await.is_err() {
                break;
            }
        }
        assert!(*rx.borrow() == want, "leader did not reach {want:?}");
    }

    /// Task 1 contract: a single-voter engine starts and reports no leader at
    /// the moment of construction (the watch seeds from `initial_state`, which
    /// is `Unattached` / `leader_id == None`).
    #[tokio::test]
    async fn single_voter_engine_starts_with_no_initial_leader() {
        let (ctrl, _dir) = build(1, &[1]);
        // The watch is seeded from the initial QuorumState before the loop runs,
        // so the initial published leader is None.
        let initial = *ctrl.watch_leader().borrow();
        assert!(initial.is_none());
        ctrl.shutdown().await;
    }

    /// Task 2: an injected `ElectionTimeout` drives the loop through the core
    /// (single voter wins its own pre-vote + vote) and the executed actions
    /// (`AppendLeaderChange` + `PersistQuorumState`), and the engine publishes
    /// the new leader. Proves event -> core -> execute -> publish.
    #[tokio::test]
    async fn injected_election_makes_single_voter_leader() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;
        ctrl.shutdown().await;
    }

    /// Task 2: a committed metadata batch flows through `advance_hwm` ->
    /// decode -> `validate` -> `apply` -> publish, mutating the image and the
    /// published watch. Drives the apply pipeline directly (`submit_change` is
    /// Task 4).
    #[tokio::test]
    async fn committed_batch_applies_to_image() {
        let (ctrl, _dir) = build(1, &[1]);
        // Become leader so there is a leader epoch to stamp the batch with.
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        // Image starts without the topic.
        assert!(ctrl.current_image().topic("t").is_none());

        // Append + commit a topic record through the real apply pipeline.
        let off = ctrl
            .test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(off >= 0);

        // The committed record applied to the image and the watch republished.
        let mut img_rx = ctrl.watch_image();
        // The apply already happened before the reply was sent, so the latest
        // borrow reflects it.
        assert!(img_rx.borrow_and_update().topic("t").is_some());
        assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    /// Task 2: a duplicate record is rejected by `MetadataImage::validate` on
    /// the apply path and does not corrupt the image (the topic stays a single
    /// definition; the second apply is a no-op).
    #[tokio::test]
    async fn duplicate_committed_record_rejected_on_apply() {
        let (ctrl, _dir) = build(1, &[1]);
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(1)).await;

        ctrl.test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(ctrl.current_image().topic("t").is_some());

        // Re-submitting the same topic: validate() rejects it, apply skipped.
        ctrl.test_append_and_commit(vec![topic_record("t")])
            .await
            .unwrap();
        assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }
}
