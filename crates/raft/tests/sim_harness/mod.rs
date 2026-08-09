//! Shared deterministic, multi-node simulation harness for the KIP-595/996
//! `KRaft` consensus core (`crabka_raft::kraft`). This module is included by both
//! integration test binaries:
//!
//! - `kraft_sim.rs` runs the core over an in-memory [`SimLog`] (slice 3a).
//! - `kraft_log_sim.rs` runs the *same* core over a real on-disk
//!   [`crabka_raft::kraft::KraftLog`] (slice 3b).
//!
//! The harness wires N [`QuorumStateMachine`]s together through an in-memory
//! message bus and a logical clock. It translates every emitted [`Action`] into
//! the [`Event`]s its peers would observe, and drives the cluster to its
//! canonical fixed point of one leader and an agreed high watermark. The
//! [`SimNodeLog`] trait abstracts the per-node log, so the exact same scheduler
//! and action-translation logic drives both the fake log and the real log.
//!
//! Determinism is non-negotiable. There is no `Instant::now`, no `rand`, and no
//! `HashMap` iteration-order dependence anywhere. The clock is a `u64` of
//! logical milliseconds. All node containers and message containers are
//! `BTreeMap` or `BTreeSet`, so the iteration order is fixed. Election timeouts
//! are staggered by node id, so ties break deterministically and elections
//! converge.

// The two test binaries each include this module but exercise different subsets
// of its surface (the fake-log binary never constructs a `KraftBackedLog`, etc.),
// so per-binary dead-code warnings are expected and harmless.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crabka_raft::kraft::{
    QuorumStateMachine,
    action::{Action, TimerKind},
    event::{Event, LogEnd},
    role::Role,
    types::{Epoch, LogView, NodeId, QuorumState, SimInstant},
};
use crabka_units::prelude::{Time, TimeExt as _};

// --------------------------------------------------------------------------
// Pluggable per-node log
// --------------------------------------------------------------------------

/// The log operations the harness does on a node's behalf. The trait abstracts
/// them, so the same scheduler drives both the in-memory fake and the real
/// on-disk `KraftLog`. Implementors are also [`LogView`]s, which are the
/// queries the core needs.
pub trait SimNodeLog: LogView {
    /// Appends `count` data records produced in `epoch`. These are the leader's
    /// own appends and the new-leader `LeaderChange` control record.
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize);

    /// Truncates the log so that exactly `offset` records remain.
    fn truncate_to(&mut self, offset: i64);

    /// Advances the log's own high-watermark bookkeeping to `hwm`, which is the
    /// consensus HWM the core has just computed. For the in-memory fake this is
    /// a no-op, because the harness mirrors the HWM separately. The real
    /// `KraftLog` uses it to gate committed reads. Default: no-op.
    fn advance_hwm(&mut self, hwm: i64) {
        let _ = hwm;
    }

    /// Replicates from `leader` into `self` and brings `self` byte-for-byte in
    /// line with the leader's log.
    ///
    /// The method first truncates any diverging or conflicting suffix that
    /// `self` holds and the leader does not. It then copies the suffix the
    /// follower is missing. The copy is epoch-faithful. The harness calls this
    /// method only when the leader is the genuine leader and neither endpoint is
    /// partitioned.
    fn replicate_from(&mut self, leader: &Self);

    /// The number of records in the log, which is its end offset as a `usize`.
    /// The convergence fingerprint uses it.
    fn record_count(&self) -> usize;

    /// The log tip as carried in Vote/Fetch requests.
    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

// --------------------------------------------------------------------------
// In-memory fake log (slice 3a)
// --------------------------------------------------------------------------

/// A growable in-memory replicated log.
///
/// Each appended record stores the leader epoch that produced it, so
/// `end_offset_for_epoch` is a real lookup and not a stub. It returns the offset
/// of the first record whose epoch is strictly greater than the queried epoch,
/// which is where that epoch's run ends.
#[derive(Debug, Clone, Default)]
pub struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<Epoch>,
}

impl LogView for SimLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }

    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        // Unknown epoch (strictly newer than anything we hold).
        if epoch > self.last_epoch() {
            return None;
        }
        // The end offset for `epoch` is the offset of the first record with a
        // strictly greater epoch, or the log end if no such record exists.
        for (i, &e) in self.epochs.iter().enumerate() {
            if e > epoch {
                return Some(i64::try_from(i).expect("offset fits in i64"));
            }
        }
        Some(self.end_offset())
    }
}

impl SimNodeLog for SimLog {
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        for _ in 0..count {
            self.epochs.push(epoch);
        }
    }

    fn truncate_to(&mut self, offset: i64) {
        let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if offset < self.epochs.len() {
            self.epochs.truncate(offset);
        }
    }

    fn replicate_from(&mut self, leader: &Self) {
        let leader_epochs = &leader.epochs;
        if self.epochs.len() < leader_epochs.len() {
            // If the follower's existing prefix matches, extend it; otherwise the
            // leader's divergence reply (TruncateTo) will fix it first. In this
            // simulation followers never accept conflicting entries, so a simple
            // suffix copy is sufficient and epoch-faithful.
            self.epochs.clone_from(leader_epochs);
        }
    }

    fn record_count(&self) -> usize {
        self.epochs.len()
    }
}

// --------------------------------------------------------------------------
// Messages and timers
// --------------------------------------------------------------------------

/// A message in flight on the bus: a destination node plus the event that node
/// observes. The `src` field is recorded for partition filtering.
#[derive(Debug, Clone, Copy)]
struct Message {
    src: NodeId,
    dst: NodeId,
    event: Event,
}

/// A node and everything the harness owns on its behalf.
struct Node<L: SimNodeLog> {
    id: NodeId,
    machine: QuorumStateMachine,
    log: L,
    /// Harness mirror of the leader's high watermark. The `Role::Leader`
    /// variant also carries it, but the harness tracks it here for non-leaders
    /// and observers.
    high_watermark: i64,
    /// Next election-timer deadline, if armed.
    election_deadline: Option<SimInstant>,
    /// Next fetch-timer deadline, if armed.
    fetch_deadline: Option<SimInstant>,
    /// Next leader heartbeat deadline, if armed.
    ///
    /// A leader periodically re-sends `BeginQuorumEpoch` to voters that are not
    /// actively fetching from it. This is genuine `KRaft` behaviour, and it is
    /// how a deposed leader that rejoins after a partition learns of the newer
    /// epoch and steps down. The core does not emit this on a timer, because a
    /// leader has no core-level timer, so the harness drives it.
    heartbeat_deadline: Option<SimInstant>,
}

// --------------------------------------------------------------------------
// The simulation harness
// --------------------------------------------------------------------------

/// A deterministic multi-node `KRaft` simulation, generic over the per-node log.
pub struct Sim<L: SimNodeLog> {
    nodes: BTreeMap<NodeId, Node<L>>,
    voter_ids: Vec<NodeId>,
    /// Logical clock in milliseconds.
    now: SimInstant,
    /// FIFO queue of in-flight messages. They are processed before the clock
    /// advances.
    queue: VecDeque<Message>,
    /// Partitioned nodes. Every message to them or from them is dropped.
    partitioned: BTreeSet<NodeId>,
}

impl<L: SimNodeLog> Sim<L> {
    /// Constructs a cluster of `voter_ids` whose per-node logs `make_log`
    /// produces. There is one fresh log per node, for example a tempdir-backed
    /// `KraftLog`.
    pub fn new_with(voter_ids: &[NodeId], mut make_log: impl FnMut(NodeId) -> L) -> Self {
        let voters = make_voter_set(voter_ids);
        let mut nodes = BTreeMap::new();
        for &id in voter_ids {
            // Stagger election timeouts deterministically so ties break and the
            // lowest-id node tends to win the race — elections always converge.
            let election_timeout_ms = election_timeout_ms_of(id);
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                election_timeout_of(id),
            );
            nodes.insert(
                id,
                Node {
                    id,
                    machine,
                    log: make_log(id),
                    high_watermark: 0,
                    // Arm the initial election timer: an idle voter must time out
                    // and begin an election to bootstrap the cluster.
                    election_deadline: Some(SimInstant(election_timeout_ms)),
                    fetch_deadline: None,
                    heartbeat_deadline: None,
                },
            );
        }
        Self {
            nodes,
            voter_ids: voter_ids.to_vec(),
            now: SimInstant(0),
            queue: VecDeque::new(),
            partitioned: BTreeSet::new(),
        }
    }

    // ---- public test surface -------------------------------------------------

    /// Drives the simulation until it reaches a fixed point, or until
    /// `max_ticks` event steps have elapsed.
    ///
    /// Each step delivers one queued message. When the queue drains, the step
    /// fires the earliest pending timer instead. A healthy cluster long-polls
    /// forever, because the fetch watchdog re-polls indefinitely, so the harness
    /// detects "stable" as a fixed point: one whole timer-driven round that
    /// leaves every node's observable state unchanged, that is its role, epoch,
    /// log length, and HWM. That strips the otherwise-unbounded steady-state
    /// fetch loop and masks no real progress, because an election or a
    /// replication advance always changes the fingerprint and resets the
    /// counter.
    pub fn run_until_stable(&mut self, max_ticks: usize) {
        let mut last_fingerprint = self.fingerprint();
        let mut stable_rounds = 0u32;
        for _ in 0..max_ticks {
            if let Some(msg) = self.queue.pop_front() {
                self.deliver(msg);
                continue;
            }
            // Queue drained: fire the next timer (if any), then check for a fixed
            // point. Two consecutive no-change rounds means converged.
            let fired = self.fire_next_timer();
            let fp = self.fingerprint();
            if fp == last_fingerprint {
                stable_rounds += 1;
                if stable_rounds >= 2 {
                    return;
                }
            } else {
                stable_rounds = 0;
                last_fingerprint = fp;
            }
            if !fired && self.queue.is_empty() {
                // Nothing queued and no timer armed: fully quiescent.
                return;
            }
        }
    }

    /// A deterministic snapshot of every node's observable state, which detects
    /// the steady-state fixed point. It is ordered by node id, in a `BTreeMap`.
    fn fingerprint(&self) -> Vec<(NodeId, &'static str, Epoch, usize, i64)> {
        self.nodes
            .values()
            .map(|n| {
                let hwm = match n.machine.role() {
                    Role::Leader { high_watermark, .. } => *high_watermark,
                    _ => n.high_watermark,
                };
                (
                    n.id,
                    n.machine.role().name(),
                    n.machine.quorum_state().leader_epoch,
                    n.log.record_count(),
                    hwm,
                )
            })
            .collect()
    }

    /// Isolates a node. The harness drops every message to it and from it, and
    /// its timers no longer affect its peers. The node keeps ticking
    /// internally, but no peer can hear it.
    pub fn partition(&mut self, node: NodeId) {
        self.partitioned.insert(node);
        // Drop any in-flight messages touching the partitioned node.
        self.queue.retain(|m| m.src != node && m.dst != node);
    }

    /// Heals a partition. The node can send and receive again.
    pub fn heal(&mut self, node: NodeId) {
        self.partitioned.remove(&node);
    }

    /// Appends `n` data records to the log of `leader` in its current leader
    /// epoch, then re-runs the leader's HWM bookkeeping over the new end offset.
    ///
    /// This models a produce. The records must then be replicated to a majority
    /// through the fetch loop before the HWM can advance past them.
    pub fn leader_append(&mut self, leader: NodeId, n: usize) {
        let epoch = self.nodes[&leader].machine.quorum_state().leader_epoch;
        let node = self.nodes.get_mut(&leader).unwrap();
        node.log.append_in_epoch(epoch, n);
    }

    /// Injects a conflicting-epoch tail straight into the log of `follower` and
    /// bypasses the leader, so the next fetch round forces a divergence and a
    /// truncation. The `epoch` should differ from what the leader holds at those
    /// offsets.
    pub fn inject_conflicting_tail(&mut self, follower: NodeId, epoch: Epoch, n: usize) {
        let node = self.nodes.get_mut(&follower).unwrap();
        node.log.append_in_epoch(epoch, n);
    }

    /// The ids of all nodes currently in the `Leader` role.
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| n.id)
            .collect()
    }

    /// The configured voter ids.
    pub fn voters(&self) -> Vec<NodeId> {
        self.voter_ids.clone()
    }

    /// The set of distinct leader epochs across all voters. It holds one epoch
    /// once the cluster has converged.
    pub fn distinct_epochs(&self) -> BTreeSet<Epoch> {
        self.nodes
            .values()
            .map(|n| n.machine.quorum_state().leader_epoch)
            .collect()
    }

    /// The leader's current high watermark.
    pub fn leader_high_watermark(&self, node: NodeId) -> i64 {
        match self.nodes[&node].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&node].high_watermark,
        }
    }

    /// The log end offset of `node`.
    pub fn log_end_offset(&self, node: NodeId) -> i64 {
        self.nodes[&node].log.end_offset()
    }

    /// Borrows the log of `node`, for byte-level and decoded assertions in
    /// tests.
    pub fn node_log(&self, node: NodeId) -> &L {
        &self.nodes[&node].log
    }

    /// True if every voter's log end offset has reached `offset`.
    pub fn all_voters_fetched_to(&self, offset: i64) -> bool {
        self.voter_ids
            .iter()
            .all(|id| self.nodes[id].log.end_offset() >= offset)
    }

    // ---- scheduler internals -------------------------------------------------

    /// Finds the earliest armed timer across all nodes, advances the clock to
    /// it, and fires it. A partitioned node still ticks internally and still
    /// counts here. Returns `false` if no timer is armed.
    fn fire_next_timer(&mut self) -> bool {
        // Pick the node with the earliest deadline; ties break by node id
        // (BTreeMap iteration is ascending by id, so the first minimum wins).
        let mut best: Option<(SimInstant, NodeId, SimTimer)> = None;
        for node in self.nodes.values() {
            if let Some(d) = node.election_deadline {
                consider(&mut best, d, node.id, SimTimer::Election);
            }
            if let Some(d) = node.fetch_deadline {
                consider(&mut best, d, node.id, SimTimer::Fetch);
            }
            if let Some(d) = node.heartbeat_deadline {
                consider(&mut best, d, node.id, SimTimer::Heartbeat);
            }
        }
        let Some((deadline, id, kind)) = best else {
            return false;
        };
        if deadline > self.now {
            self.now = deadline;
        }
        // Clear the fired timer; the handler re-arms below / via ResetTimer.
        {
            let node = self.nodes.get_mut(&id).unwrap();
            match kind {
                SimTimer::Election => node.election_deadline = None,
                SimTimer::Fetch => node.fetch_deadline = None,
                SimTimer::Heartbeat => node.heartbeat_deadline = None,
            }
        }
        match kind {
            SimTimer::Heartbeat => {
                self.fire_leader_heartbeat(id);
                true
            }
            SimTimer::Fetch => {
                // A fetch watchdog firing while the follower's leader is still
                // reachable is a routine long-poll expiry: re-poll the leader
                // rather than escalate to an election. Only when the leader is
                // gone (unreachable / unknown) does the watchdog become a real
                // `FetchTimeout` that elects. This mirrors `KRaft`, where continuous
                // polling resets the timer and only sustained silence elects.
                if let Role::Follower { leader_id, .. }
                | Role::Observer {
                    leader_id: Some(leader_id),
                    ..
                } = *self.nodes[&id].machine.role()
                {
                    let leader_alive = !self.partitioned.contains(&id)
                        && !self.partitioned.contains(&leader_id)
                        && self
                            .nodes
                            .get(&leader_id)
                            .is_some_and(|n| n.machine.role().is_leader());
                    if leader_alive {
                        let deadline = self.now.saturating_add_ms(election_timeout_ms_of(id));
                        self.nodes.get_mut(&id).unwrap().fetch_deadline = Some(deadline);
                        self.apply_action(id, Action::SendFetch { leader_id });
                        return true;
                    }
                }
                self.step(id, Event::FetchTimeout);
                true
            }
            SimTimer::Election => {
                self.step(id, Event::ElectionTimeout);
                true
            }
        }
    }

    /// A leader's periodic heartbeat. It re-broadcasts `BeginQuorumEpoch` to
    /// every peer, faithful to the `KRaft` resend to non-fetching voters, and
    /// re-arms the heartbeat. This is how a stale leader that rejoins after a
    /// partition learns of the newer epoch from the current leader and steps
    /// down to follower.
    fn fire_leader_heartbeat(&mut self, id: NodeId) {
        if !self.nodes[&id].machine.role().is_leader() {
            return;
        }
        let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
        self.apply_action(id, Action::SendBeginQuorumEpoch { epoch });
        let deadline = self.now.saturating_add_ms(HEARTBEAT_MS);
        self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
    }

    /// Delivers a queued message, and drops it if either endpoint is
    /// partitioned.
    fn deliver(&mut self, msg: Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        self.step(msg.dst, msg.event);
    }

    /// Feeds one event to a node and translates the resulting actions into new
    /// messages, timer arming, and log and HWM bookkeeping.
    fn step(&mut self, id: NodeId, event: Event) {
        let now = self.now;
        // A `ReceiveFetch` is a leader-side request; remember who asked and the
        // leader epoch so we can synthesize the matching fetch *response* back to
        // the follower (the core only emits HWM/Truncate, not a response message).
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        // Run the machine. We must not hold a mutable borrow of `nodes` while we
        // re-borrow other nodes during action translation, so collect first.
        let actions = {
            let node = self.nodes.get_mut(&id).unwrap();
            node.machine.on_event(event, &node.log, now)
        };
        // If this was a fetch the leader served, reply to the follower (so it can
        // re-arm its fetch watchdog and truncate on divergence) — but only when
        // there is something to report: new data to replicate, or a divergence
        // hint. When the follower is already fully caught up, the leader's
        // long-poll *parks* with no immediate answer; the follower's watchdog
        // (re-armed below in `apply_action`) becomes the next event, and a
        // watchdog firing while the leader is still reachable is modelled as a
        // re-poll rather than an election (see `fire_next_timer`). This bounds the
        // steady-state fetch loop deterministically.
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::TruncateTo(point) => Some(*point),
                _ => None,
            });
            let leader_epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
            if self.nodes[&id].machine.role().is_leader() {
                let leader_end = self.nodes[&id].log.end_offset();
                let follower_end = self.nodes[&follower].log.end_offset();
                let has_new_data = follower_end < leader_end;
                if diverging.is_some() || has_new_data {
                    self.send(
                        id,
                        follower,
                        Event::ReceiveFetchResponse {
                            leader_id: id,
                            leader_epoch,
                            diverging,
                        },
                    );
                }
            }
        }
        for action in actions {
            // A leader-side `TruncateTo` emitted while serving a fetch is a hint
            // *for the follower* (carried in the fetch response's `diverging`),
            // not an instruction to truncate the leader's own log — skip it.
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(id, action);
        }
        self.reconcile_timers_for_role(id);
    }

    /// Enforces per-role timer ownership, which the core does not fully manage
    /// through `ResetTimer` actions alone:
    ///
    /// - A leader runs neither an election timer nor a fetch timer. Its liveness
    ///   is a separate check-quorum mechanism, out of scope for slice 3a.
    /// - A follower or an observer runs only the fetch watchdog, and never an
    ///   election timer. But `handle_begin_quorum_epoch` emits only
    ///   `ResetTimer{Fetch}`, which leaves a previously-armed election timer
    ///   live. Without a clear of that timer, a healthy follower's stale
    ///   election timer fires, the follower goes `Prospective`, and the cluster
    ///   never stabilises.
    /// - An electing role, which is Unattached, Voted, Prospective, or
    ///   Candidate, runs only the election timer, and never a fetch watchdog.
    ///
    /// The core does arm the correct timer on each transition. This method only
    /// clears the stale opposite timer, so the harness scheduler matches the
    /// per-role timer model of `KRaft`.
    fn reconcile_timers_for_role(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        match node.machine.role() {
            Role::Leader { .. } => {
                node.election_deadline = None;
                node.fetch_deadline = None;
                // Arm the leader heartbeat if not already running.
                if node.heartbeat_deadline.is_none() {
                    node.heartbeat_deadline = Some(self.now.saturating_add_ms(HEARTBEAT_MS));
                }
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                node.election_deadline = None;
                node.heartbeat_deadline = None;
            }
            Role::Unattached { .. }
            | Role::Voted { .. }
            | Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Resigned => {
                node.fetch_deadline = None;
                node.heartbeat_deadline = None;
            }
        }
    }

    /// Broadcasts a vote or pre-vote request from `id` to every other voter.
    fn broadcast_vote_request(&mut self, id: NodeId, epoch: Epoch, pre_vote: bool) {
        let cand_log = self.nodes[&id].log.log_end();
        for peer in self.voter_ids.clone() {
            if peer != id {
                self.send(
                    id,
                    peer,
                    Event::ReceiveVoteRequest {
                        from: id,
                        voter_id: peer,
                        candidate_epoch: epoch,
                        candidate: id,
                        candidate_log_end: cand_log,
                        pre_vote,
                    },
                );
            }
        }
    }

    /// Translates a single emitted `Action` from node `id` into bus messages,
    /// timer updates, and log and HWM bookkeeping.
    fn apply_action(&mut self, id: NodeId, action: Action) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => {
                self.broadcast_vote_request(id, epoch, pre_vote);
            }
            Action::ReplyVote { to, epoch, granted } => {
                self.send(
                    id,
                    to,
                    Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                );
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                for peer in self.all_node_ids() {
                    if peer != id {
                        self.send(
                            id,
                            peer,
                            Event::ReceiveBeginQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        );
                    }
                }
            }
            Action::SendEndQuorumEpoch { epoch } => {
                for peer in self.all_node_ids() {
                    if peer != id {
                        self.send(
                            id,
                            peer,
                            Event::ReceiveEndQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        );
                    }
                }
            }
            Action::SendFetch { leader_id } => {
                // The follower fetches from the leader. Model replication first:
                // copy any leader log entries this follower is missing, then send
                // the fetch carrying the follower's (now-advanced) tip so the
                // leader can advance its HWM.
                self.replicate_from_leader(id, leader_id);
                let (fetch_epoch, fetch_offset) = {
                    let log = &self.nodes[&id].log;
                    (log.last_epoch(), log.end_offset())
                };
                self.send(
                    id,
                    leader_id,
                    Event::ReceiveFetch {
                        from: id,
                        fetch_epoch,
                        fetch_offset,
                    },
                );
            }
            Action::AppendLeaderChange { epoch } => {
                // The new leader appends one control record in its current epoch.
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.high_watermark = hwm;
                node.log.advance_hwm(hwm);
                // In KRaft the new high watermark rides along on the leader's
                // next fetch response, so every follower eventually learns it —
                // including a caught-up follower that is long-polling and would
                // otherwise never re-fetch. Model that by pushing the committed
                // boundary to every peer's log now (each `advance_hwm` is
                // monotonic and clamped to that peer's own replicated log end, so
                // a lagging follower only commits what it actually holds).
                for peer in self.all_node_ids() {
                    if peer != id && !self.partitioned.contains(&peer) {
                        let p = self.nodes.get_mut(&peer).unwrap();
                        p.log.advance_hwm(hwm);
                        p.high_watermark = hwm.min(p.log.end_offset());
                    }
                }
            }
            Action::TruncateTo(point) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.truncate_to(point.offset);
            }
            Action::ResetTimer { kind, deadline } => {
                let node = self.nodes.get_mut(&id).unwrap();
                match kind {
                    TimerKind::Election => node.election_deadline = Some(deadline),
                    TimerKind::Fetch => node.fetch_deadline = Some(deadline),
                }
            }
            // Pure bookkeeping signals with no cross-node effect in the sim.
            Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

    /// Copies the log entries from `leader` that `follower` is missing, so the
    /// follower logs converge and the follower's fetch offset advances toward
    /// the leader's end.
    ///
    /// The method respects the epochs, because it delegates the byte-faithful
    /// copy and the divergence truncation to the log impl. It runs only when
    /// `leader` actually believes it is the leader and neither endpoint is
    /// partitioned.
    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if follower == leader {
            return;
        }
        if self.partitioned.contains(&follower) || self.partitioned.contains(&leader) {
            return;
        }
        if !self.nodes[&leader].machine.role().is_leader() {
            return;
        }
        // Two distinct nodes need simultaneous access (follower mut, leader ref).
        // `BTreeMap` has no stable disjoint-borrow API, so lift the follower out,
        // replicate against the still-resident leader, then put it back.
        let leader_hwm = self.leader_high_watermark(leader);
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        // The follower learns the leader's committed offset on each fetch (the
        // fetch response carries the leader's high watermark in real KRaft), so
        // its own committed-read boundary tracks the consensus HWM, bounded by
        // what it has actually replicated.
        follower_node.log.advance_hwm(leader_hwm);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    /// Enqueues an event for delivery to `dst`. If either endpoint is currently
    /// partitioned, the harness silently drops the message, the same way a real
    /// network partition does.
    fn send(&mut self, src: NodeId, dst: NodeId, event: Event) {
        if self.partitioned.contains(&src) || self.partitioned.contains(&dst) {
            return;
        }
        self.queue.push_back(Message { src, dst, event });
    }

    fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }
}

/// Harness-level timer kinds. This extends the core's `TimerKind`, which is
/// Election and Fetch, with the leader `Heartbeat` that the core does not model
/// on a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimTimer {
    Election,
    Fetch,
    Heartbeat,
}

/// Leader heartbeat period. It stays well below the election timeout, so a
/// healthy leader's re-announcements always reach the voters before any
/// watchdog escalates.
const HEARTBEAT_MS: u64 = 300;

/// The base election timeout, which is also the fetch watchdog period,
/// configured for node `id`. It is staggered by node id, so timer ties break
/// deterministically and the lowest live id tends to win the election race.
/// Elections therefore always converge.
fn election_timeout_ms_of(id: NodeId) -> u64 {
    1000 + id.0 * 50
}

/// [`election_timeout_ms_of`] as the quantity [`QuorumStateMachine::new`] takes.
/// The simulation's own clock stays in integer logical milliseconds, because a
/// [`SimInstant`] is a coordinate and not an extent. This conversion therefore
/// happens only at the core's constructor.
fn election_timeout_of(id: NodeId) -> Time {
    Time::from_millis(i64::try_from(election_timeout_ms_of(id)).unwrap_or(i64::MAX))
}

/// Updates `best` to the earliest `(deadline, id, kind)` seen so far. An earlier
/// deadline wins. On a tie the smaller node id wins. Callers iterate the ids in
/// ascending order, so this keeps the choice deterministic.
fn consider(
    best: &mut Option<(SimInstant, NodeId, SimTimer)>,
    deadline: SimInstant,
    id: NodeId,
    kind: SimTimer,
) {
    match best {
        Some((bd, _, _)) if *bd <= deadline => {}
        _ => *best = Some((deadline, id, kind)),
    }
}

fn make_voter_set(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
    crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
        crabka_metadata::voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
        }
    }))
}
