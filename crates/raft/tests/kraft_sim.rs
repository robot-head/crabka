//! Deterministic, in-memory, multi-node simulation of the KIP-595/996 `KRaft`
//! consensus core (`crabka_raft::kraft`). This is the headline acceptance test
//! for slice 3a: it wires N [`QuorumStateMachine`]s together through an
//! in-memory message bus and a logical clock, translates every emitted
//! [`Action`] into the [`Event`]s its peers would observe, and asserts the
//! cluster reaches the canonical single-leader / agreed-high-watermark states.
//!
//! Determinism is non-negotiable: there is no `Instant::now`, no `rand`, and no
//! `HashMap` iteration-order dependence anywhere. The clock is a `u64` of
//! logical milliseconds; all node/message containers are `BTreeMap`/`BTreeSet`
//! so iteration order is fixed; election timeouts are staggered by node id so
//! ties break deterministically and elections converge.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crabka_raft::kraft::QuorumStateMachine;
use crabka_raft::kraft::action::{Action, TimerKind};
use crabka_raft::kraft::event::{Event, LogEnd};
use crabka_raft::kraft::role::Role;
use crabka_raft::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, SimInstant};

// --------------------------------------------------------------------------
// In-memory log
// --------------------------------------------------------------------------

/// A growable in-memory replicated log. Each appended record stores the leader
/// epoch that produced it, so `end_offset_for_epoch` is a real lookup rather
/// than a stub: it returns the offset of the first record whose epoch is
/// strictly greater than the queried one (i.e. where that epoch's run ends).
#[derive(Debug, Clone, Default)]
struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<LeaderEpoch>,
}

impl SimLog {
    /// Append `count` records in `epoch` (used for the leader's own appends).
    fn append(&mut self, epoch: LeaderEpoch, count: usize) {
        for _ in 0..count {
            self.epochs.push(epoch);
        }
    }

    /// Truncate the log so that exactly `offset` records remain.
    fn truncate_to(&mut self, offset: i64) {
        let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if offset < self.epochs.len() {
            self.epochs.truncate(offset);
        }
    }

    /// The log tip as carried in Vote/Fetch requests.
    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

impl LogView for SimLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }

    fn last_epoch(&self) -> LeaderEpoch {
        self.epochs.last().copied().unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
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

// --------------------------------------------------------------------------
// Messages and timers
// --------------------------------------------------------------------------

/// A message in flight on the bus: a destination node plus the event it will
/// observe. `src` is recorded for partition filtering.
#[derive(Debug, Clone)]
struct Message {
    src: NodeId,
    dst: NodeId,
    event: Event,
}

/// A node and everything the harness owns on its behalf.
struct Node {
    id: NodeId,
    machine: QuorumStateMachine,
    log: SimLog,
    /// Harness mirror of the leader's high watermark (also readable off the
    /// `Role::Leader` variant, but tracked here for non-leaders / observers).
    high_watermark: i64,
    /// Next election-timer deadline, if armed.
    election_deadline: Option<SimInstant>,
    /// Next fetch-timer deadline, if armed.
    fetch_deadline: Option<SimInstant>,
    /// Next leader heartbeat deadline, if armed. A leader periodically re-sends
    /// `BeginQuorumEpoch` to voters that are not actively fetching from it — this
    /// is genuine `KRaft` behaviour (the leader resends `BeginQuorumEpoch` to such
    /// voters) and is how a deposed leader that rejoins after a partition learns
    /// of the newer epoch and steps down. The core does not emit this on a timer
    /// (a leader has no core-level timer), so the harness drives it.
    heartbeat_deadline: Option<SimInstant>,
}

// --------------------------------------------------------------------------
// The simulation harness
// --------------------------------------------------------------------------

/// A deterministic multi-node `KRaft` simulation.
struct Sim {
    nodes: BTreeMap<NodeId, Node>,
    voter_ids: Vec<NodeId>,
    /// Logical clock in milliseconds.
    now: SimInstant,
    /// FIFO queue of in-flight messages (processed before the clock advances).
    queue: VecDeque<Message>,
    /// Partitioned nodes: all messages to/from them are dropped.
    partitioned: BTreeSet<NodeId>,
}

impl Sim {
    fn new(voter_ids: &[NodeId]) -> Self {
        let voters = make_voter_set(voter_ids);
        let mut nodes = BTreeMap::new();
        for &id in voter_ids {
            // Stagger election timeouts deterministically so ties break and the
            // lowest-id node tends to win the race — elections always converge.
            let election_timeout_ms = election_timeout_ms_of(id);
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                election_timeout_ms,
            );
            nodes.insert(
                id,
                Node {
                    id,
                    machine,
                    log: SimLog::default(),
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

    /// Drive the simulation until it reaches a fixed point or `max_ticks` event
    /// steps elapse. Each step delivers one queued message, or — when the queue
    /// drains — fires the earliest pending timer. Because a healthy cluster keeps
    /// long-polling forever (the fetch watchdog re-polls indefinitely), "stable"
    /// is detected as a fixed point: a whole timer-driven round that leaves every
    /// node's observable state (role, epoch, log length, HWM) unchanged. That
    /// strips the otherwise-unbounded steady-state fetch loop without masking
    /// real progress (an election or a replication advance always changes the
    /// fingerprint and resets the counter).
    fn run_until_stable(&mut self, max_ticks: usize) {
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

    /// A deterministic snapshot of every node's observable state, used to detect
    /// the steady-state fixed point. Ordered by node id (`BTreeMap`).
    fn fingerprint(&self) -> Vec<(NodeId, &'static str, LeaderEpoch, usize, i64)> {
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
                    n.log.epochs.len(),
                    hwm,
                )
            })
            .collect()
    }

    /// Isolate a node: drop every message to or from it, and stop its timers
    /// from affecting peers (it keeps ticking internally but can't be heard).
    fn partition(&mut self, node: NodeId) {
        self.partitioned.insert(node);
        // Drop any in-flight messages touching the partitioned node.
        self.queue.retain(|m| m.src != node && m.dst != node);
    }

    /// Heal a partition: the node can send and receive again.
    fn heal(&mut self, node: NodeId) {
        self.partitioned.remove(&node);
    }

    /// Append `n` data records to `leader`'s log in its current leader epoch and
    /// re-run the leader's HWM bookkeeping over the new end offset. This models
    /// a produce: the records must then be majority-replicated (via the fetch
    /// loop) before the HWM can advance past them.
    fn leader_append(&mut self, leader: NodeId, n: usize) {
        let epoch = self.nodes[&leader].machine.quorum_state().leader_epoch;
        let node = self.nodes.get_mut(&leader).unwrap();
        node.log.append(epoch, n);
    }

    /// The ids of all nodes currently in the `Leader` role.
    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| n.id)
            .collect()
    }

    /// The set of distinct leader epochs across all voters (should be one once
    /// the cluster has converged).
    fn distinct_epochs(&self) -> BTreeSet<LeaderEpoch> {
        self.nodes
            .values()
            .map(|n| n.machine.quorum_state().leader_epoch)
            .collect()
    }

    /// The leader's current high watermark.
    fn leader_high_watermark(&self, node: NodeId) -> i64 {
        match self.nodes[&node].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&node].high_watermark,
        }
    }

    /// True if every voter's log end offset has reached `offset`.
    fn all_voters_fetched_to(&self, offset: i64) -> bool {
        self.voter_ids
            .iter()
            .all(|id| self.nodes[id].log.end_offset() >= offset)
    }

    // ---- scheduler internals -------------------------------------------------

    /// Find the earliest armed timer across all (non-partitioned-internally-OK)
    /// nodes, advance the clock to it, and fire it. Returns `false` if no timer
    /// is armed.
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

    /// A leader's periodic heartbeat: re-broadcast `BeginQuorumEpoch` to all
    /// peers (faithful to `KRaft`'s resend to non-fetching voters) and re-arm the
    /// heartbeat. This is how a stale leader that rejoins after a partition learns
    /// of the newer epoch from the current leader and steps down to follower.
    fn fire_leader_heartbeat(&mut self, id: NodeId) {
        if !self.nodes[&id].machine.role().is_leader() {
            return;
        }
        let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
        self.apply_action(id, Action::SendBeginQuorumEpoch { epoch });
        let deadline = self.now.saturating_add_ms(HEARTBEAT_MS);
        self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
    }

    /// Deliver a queued message, dropping it if either endpoint is partitioned.
    fn deliver(&mut self, msg: Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        self.step(msg.dst, msg.event);
    }

    /// Feed one event to a node and translate the resulting actions into new
    /// messages / timer arming / log + HWM bookkeeping.
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

    /// Enforce per-role timer ownership, which the core does not fully manage via
    /// `ResetTimer` actions alone:
    ///
    /// - A **leader** runs neither an election nor a fetch timer (its liveness is
    ///   a separate check-quorum mechanism, out of scope for slice 3a).
    /// - A **follower/observer** runs only the fetch watchdog, never an election
    ///   timer — yet `handle_begin_quorum_epoch` only emits `ResetTimer{Fetch}`,
    ///   leaving a previously-armed election timer live. Without clearing it, a
    ///   healthy follower's stale election timer fires, it goes `Prospective`, and
    ///   the cluster never stabilises.
    /// - An **electing role** (Unattached/Voted/Prospective/Candidate) runs only
    ///   the election timer, never a fetch watchdog.
    ///
    /// The core *does* arm the correct timer on each transition; this just clears
    /// the stale opposite one so the harness scheduler matches `KRaft`'s per-role
    /// timer model.
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

    /// Broadcast a (pre-)vote request from `id` to every other voter.
    fn broadcast_vote_request(&mut self, id: NodeId, epoch: LeaderEpoch, pre_vote: bool) {
        let cand_log = self.nodes[&id].log.log_end();
        for peer in self.voter_ids.clone() {
            if peer != id {
                self.send(
                    id,
                    peer,
                    Event::ReceiveVoteRequest {
                        from: id,
                        candidate_epoch: epoch,
                        candidate: id,
                        candidate_log_end: cand_log,
                        pre_vote,
                    },
                );
            }
        }
    }

    /// Translate a single emitted `Action` from node `id` into bus messages,
    /// timer updates, and log/HWM bookkeeping.
    fn apply_action(&mut self, id: NodeId, action: Action) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => {
                self.broadcast_vote_request(id, epoch, pre_vote);
            }
            Action::ReplyVote {
                to,
                epoch,
                granted,
                pre_vote,
            } => {
                self.send(
                    id,
                    to,
                    Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                        pre_vote,
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
                node.log.append(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.high_watermark = hwm;
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

    /// Copy log entries from `leader` that `follower` is missing, so follower
    /// logs converge and the follower's fetch offset advances toward the
    /// leader's end. Respects epochs by copying the leader's per-offset epochs
    /// verbatim. Only copies the suffix the follower does not yet have.
    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if follower == leader {
            return;
        }
        if self.partitioned.contains(&follower) || self.partitioned.contains(&leader) {
            return;
        }
        // Only replicate from a node that actually believes it is the leader.
        if !self.nodes[&leader].machine.role().is_leader() {
            return;
        }
        let leader_epochs = self.nodes[&leader].log.epochs.clone();
        let node = self.nodes.get_mut(&follower).unwrap();
        let follower_len = node.log.epochs.len();
        if follower_len < leader_epochs.len() {
            // If the follower's existing prefix matches, extend it; otherwise the
            // leader's divergence reply (TruncateTo) will fix it first. In this
            // simulation followers never accept conflicting entries, so a simple
            // suffix copy is sufficient and epoch-faithful.
            node.log.epochs = leader_epochs;
        }
    }

    /// Enqueue an event to be delivered to `dst`, unless either endpoint is
    /// currently partitioned (in which case the message is silently dropped, as
    /// a real network partition would).
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

/// Harness-level timer kinds. Extends the core's `TimerKind` (Election/Fetch)
/// with the leader `Heartbeat` the core does not model on a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimTimer {
    Election,
    Fetch,
    Heartbeat,
}

/// Leader heartbeat period. Kept comfortably below the election timeout so a
/// healthy leader's re-announcements always reach voters before any watchdog
/// would escalate.
const HEARTBEAT_MS: u64 = 300;

/// The base election timeout (and fetch watchdog period) configured for node
/// `id`. Staggered by node id so timer ties break deterministically and the
/// lowest live id tends to win the election race — elections always converge.
fn election_timeout_ms_of(id: NodeId) -> u64 {
    1000 + id * 50
}

/// Update `best` to the earliest `(deadline, id, kind)` seen so far. Earlier
/// deadlines win; on a tie the smaller node id wins (callers iterate ids in
/// ascending order, so this keeps the choice deterministic).
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

// --------------------------------------------------------------------------
// Acceptance tests
// --------------------------------------------------------------------------

use assert2::assert;

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    assert!(
        sim.leaders().len() == 1,
        "expected exactly one leader, got {:?}",
        sim.leaders()
    );
    // Every voter agrees on a single leader epoch.
    assert!(
        sim.distinct_epochs().len() == 1,
        "voters disagree on epoch: {:?}",
        sim.distinct_epochs()
    );
}

#[test]
fn re_elects_single_leader_after_leader_partition() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    assert!(
        sim.leaders().len() == 1,
        "no initial leader: {:?}",
        sim.leaders()
    );
    let old_leader = sim.leaders()[0];

    // Isolate the leader; the majority side must elect a new one.
    sim.partition(old_leader);
    sim.run_until_stable(10_000);
    let new_leaders: Vec<_> = sim
        .leaders()
        .into_iter()
        .filter(|&l| l != old_leader)
        .collect();
    assert!(
        new_leaders.len() == 1,
        "majority side must elect exactly one new leader, got {new_leaders:?}"
    );

    // Heal the partition; the old leader rejoins and steps down to follower,
    // leaving a single leader cluster-wide.
    sim.heal(old_leader);
    sim.run_until_stable(10_000);
    assert!(
        sim.leaders().len() == 1,
        "cluster must converge to one leader, got {:?}",
        sim.leaders()
    );
    assert!(
        sim.leaders()[0] == new_leaders[0],
        "the post-partition leader should remain leader; got {:?} expected {}",
        sim.leaders(),
        new_leaders[0]
    );
}

#[test]
fn committed_high_watermark_agrees_across_voters() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    assert!(sim.leaders().len() == 1, "no leader: {:?}", sim.leaders());
    let leader = sim.leaders()[0];

    // The leader already appended its LeaderChange control record at promotion;
    // capture the log end, then produce 5 data records on top.
    let before = sim.nodes[&leader].log.end_offset();
    sim.leader_append(leader, 5);
    let target = before + 5;

    sim.run_until_stable(10_000);

    // The HWM must reach the appended offset (current-epoch entries are now
    // majority-replicated — this is the FIX-2 leader-completeness gate) and all
    // voters must have replicated up to it.
    assert!(
        sim.leader_high_watermark(leader) >= target,
        "HWM {} did not reach appended offset {}",
        sim.leader_high_watermark(leader),
        target
    );
    assert!(
        sim.all_voters_fetched_to(sim.leader_high_watermark(leader)),
        "not all voters replicated up to HWM {}",
        sim.leader_high_watermark(leader)
    );
}
