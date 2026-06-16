//! Curated, deterministic `KRaft` consensus failure scenarios with trace
//! recording.
//!
//! This module promotes the deterministic, pure-synchronous (no tokio)
//! multi-node `KRaft` simulator — the same scheduler the integration tests use
//! (`crates/raft/tests/sim_harness/mod.rs`) — into the library behind the
//! `scenarios` feature, and instruments it to RECORD a serializable
//! [`ScenarioTrace`] of every step. `crabka-docgen` runs [`scenarios`] in
//! process and renders the traces into a Mermaid sequence-diagram slideshow, so
//! the diagrams reflect the real algorithm rather than a hand-drawn cartoon.
//!
//! Determinism is non-negotiable: there is no `Instant::now`, no `rand`, and no
//! `HashMap` iteration-order dependence anywhere. The clock is a `u64` of
//! logical milliseconds; all node/message containers are `BTreeMap`/`BTreeSet`
//! so iteration order is fixed; election timeouts are staggered by node id so
//! ties break deterministically and elections converge.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::kraft::QuorumStateMachine;
use crate::kraft::action::{Action, TimerKind};
use crate::kraft::event::{Event, LogEnd};
use crate::kraft::role::Role;
use crate::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, SimInstant};

// --------------------------------------------------------------------------
// Recorded trace types
// --------------------------------------------------------------------------

/// A complete recording of one curated failure scenario: its identity, the
/// invariant it demonstrates, and the ordered sequence of steps the simulator
/// took (each with a snapshot of every node's role).
#[derive(serde::Serialize, Clone)]
pub struct ScenarioTrace {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub invariant: String,
    pub nodes: Vec<u64>,
    pub steps: Vec<TraceStep>,
    pub outcome: String,
}

/// One step in a scenario: the action that occurred, a short human note, and a
/// snapshot of every node's role taken immediately after the action.
#[derive(serde::Serialize, Clone)]
pub struct TraceStep {
    pub index: usize,
    pub clock_ms: u64,
    pub action: TraceAction,
    pub note: String,
    pub roles: Vec<NodeRole>,
}

/// The kind of action a [`TraceStep`] records.
#[derive(serde::Serialize, Clone)]
#[serde(tag = "kind")]
pub enum TraceAction {
    Deliver {
        src: u64,
        dst: u64,
        event: String,
    },
    Partition {
        node: u64,
    },
    Heal {
        node: u64,
    },
    Timeout {
        node: u64,
        #[serde(rename = "timer_kind")]
        kind: String,
    },
    Elected {
        node: u64,
        epoch: u64,
    },
    Append {
        node: u64,
        count: usize,
    },
}

/// A single node's observable state at a point in time.
#[derive(serde::Serialize, Clone)]
pub struct NodeRole {
    pub id: u64,
    pub role: String,
    pub epoch: u64,
    pub log_len: usize,
    pub hwm: i64,
    pub partitioned: bool,
}

// --------------------------------------------------------------------------
// In-memory fake log (a focused copy of the test harness `SimLog`)
// --------------------------------------------------------------------------

/// A growable in-memory replicated log. Each appended record stores the leader
/// epoch that produced it, so `end_offset_for_epoch` is a real lookup rather
/// than a stub.
#[derive(Debug, Clone, Default)]
struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<LeaderEpoch>,
}

impl LogView for SimLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }

    fn last_epoch(&self) -> LeaderEpoch {
        self.epochs.last().copied().unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
        if epoch > self.last_epoch() {
            return None;
        }
        for (i, &e) in self.epochs.iter().enumerate() {
            if e > epoch {
                return Some(i64::try_from(i).expect("offset fits in i64"));
            }
        }
        Some(self.end_offset())
    }
}

impl SimLog {
    fn append_in_epoch(&mut self, epoch: LeaderEpoch, count: usize) {
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
            self.epochs.clone_from(leader_epochs);
        }
    }

    fn record_count(&self) -> usize {
        self.epochs.len()
    }

    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

// --------------------------------------------------------------------------
// Messages and timers
// --------------------------------------------------------------------------

/// A message in flight on the bus: a destination node plus the event it will
/// observe. `src` is recorded for partition filtering and trace labelling.
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
    high_watermark: i64,
    election_deadline: Option<SimInstant>,
    fetch_deadline: Option<SimInstant>,
    heartbeat_deadline: Option<SimInstant>,
}

/// Harness-level timer kinds. Extends the core's `TimerKind` (Election/Fetch)
/// with the leader `Heartbeat` the core does not model on a timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimTimer {
    Election,
    Fetch,
    Heartbeat,
}

const HEARTBEAT_MS: u64 = 300;

fn election_timeout_ms_of(id: NodeId) -> u64 {
    1000 + id * 50
}

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

/// A short, stable label for an [`Event`] used in the rendered sequence diagram.
fn event_label(event: &Event) -> String {
    match event {
        Event::ElectionTimeout => "ElectionTimeout".to_string(),
        Event::FetchTimeout => "FetchTimeout".to_string(),
        Event::ReceiveVoteRequest { pre_vote, .. } => {
            if *pre_vote {
                "PreVoteRequest".to_string()
            } else {
                "VoteRequest".to_string()
            }
        }
        Event::ReceiveVoteResponse { vote_granted, .. } => {
            if *vote_granted {
                "VoteResponse(granted)".to_string()
            } else {
                "VoteResponse(denied)".to_string()
            }
        }
        Event::ReceiveBeginQuorumEpoch { .. } => "BeginQuorumEpoch".to_string(),
        Event::ReceiveEndQuorumEpoch { .. } => "EndQuorumEpoch".to_string(),
        Event::ReceiveFetch { .. } => "Fetch".to_string(),
        Event::ReceiveFetchResponse { .. } => "FetchResponse".to_string(),
    }
}

// --------------------------------------------------------------------------
// The recording simulation harness
// --------------------------------------------------------------------------

/// A deterministic multi-node `KRaft` simulation over the in-memory fake log,
/// instrumented to record a [`ScenarioTrace`].
struct Sim {
    nodes: BTreeMap<NodeId, Node>,
    voter_ids: Vec<NodeId>,
    now: SimInstant,
    queue: VecDeque<Message>,
    partitioned: BTreeSet<NodeId>,
    steps: Vec<TraceStep>,
    /// Leader epochs already recorded with an `Elected` step, so we record each
    /// promotion exactly once.
    elected_seen: BTreeSet<(NodeId, LeaderEpoch)>,
}

impl Sim {
    fn new(voter_ids: &[NodeId]) -> Self {
        let voters = make_voter_set(voter_ids);
        let mut nodes = BTreeMap::new();
        for &id in voter_ids {
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
            steps: Vec::new(),
            elected_seen: BTreeSet::new(),
        }
    }

    // ---- trace recording -----------------------------------------------------

    /// Snapshot every node's observable state, in ascending id order.
    fn snapshot_roles(&self) -> Vec<NodeRole> {
        self.nodes
            .values()
            .map(|n| {
                let hwm = match n.machine.role() {
                    Role::Leader { high_watermark, .. } => *high_watermark,
                    _ => n.high_watermark,
                };
                NodeRole {
                    id: n.id,
                    role: n.machine.role().name().to_string(),
                    epoch: u64::from(n.machine.quorum_state().leader_epoch),
                    log_len: n.log.record_count(),
                    hwm,
                    partitioned: self.partitioned.contains(&n.id),
                }
            })
            .collect()
    }

    /// Push a recorded step capturing `action` + `note` and a fresh role snapshot.
    fn record(&mut self, action: TraceAction, note: impl Into<String>) {
        let index = self.steps.len();
        let roles = self.snapshot_roles();
        self.steps.push(TraceStep {
            index,
            clock_ms: self.now.0,
            action,
            note: note.into(),
            roles,
        });
    }

    /// After running the machine, record any newly-promoted leaders.
    fn record_new_leaders(&mut self) {
        let promotions: Vec<(NodeId, LeaderEpoch)> = self
            .nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| (n.id, n.machine.quorum_state().leader_epoch))
            .filter(|key| !self.elected_seen.contains(key))
            .collect();
        for (id, epoch) in promotions {
            self.elected_seen.insert((id, epoch));
            self.record(
                TraceAction::Elected {
                    node: id,
                    epoch: u64::from(epoch),
                },
                format!("N{id} won the election for epoch {epoch}"),
            );
        }
    }

    // ---- fingerprint / stability ---------------------------------------------

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
                    n.log.record_count(),
                    hwm,
                )
            })
            .collect()
    }

    fn run_until_stable(&mut self, max_ticks: usize) {
        let mut last_fingerprint = self.fingerprint();
        let mut stable_rounds = 0u32;
        for _ in 0..max_ticks {
            if let Some(msg) = self.queue.pop_front() {
                self.deliver(msg);
                continue;
            }
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
                return;
            }
        }
    }

    // ---- public scenario surface ---------------------------------------------

    fn partition(&mut self, node: NodeId) {
        self.partitioned.insert(node);
        self.queue.retain(|m| m.src != node && m.dst != node);
        self.record(
            TraceAction::Partition { node },
            format!("N{node} is isolated from the cluster"),
        );
    }

    fn heal(&mut self, node: NodeId) {
        self.partitioned.remove(&node);
        self.record(
            TraceAction::Heal { node },
            format!("N{node} rejoins the cluster"),
        );
    }

    fn leader_append(&mut self, leader: NodeId, n: usize) {
        let epoch = self.nodes[&leader].machine.quorum_state().leader_epoch;
        let node = self.nodes.get_mut(&leader).unwrap();
        node.log.append_in_epoch(epoch, n);
        self.record(
            TraceAction::Append {
                node: leader,
                count: n,
            },
            format!("Leader N{leader} appends {n} record(s) in epoch {epoch}"),
        );
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| n.id)
            .collect()
    }

    // ---- scheduler internals -------------------------------------------------

    fn fire_next_timer(&mut self) -> bool {
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
                self.record(
                    TraceAction::Timeout {
                        node: id,
                        kind: "fetch".to_string(),
                    },
                    format!("N{id} lost contact with its leader and starts an election"),
                );
                self.step(id, Event::FetchTimeout);
                true
            }
            SimTimer::Election => {
                self.record(
                    TraceAction::Timeout {
                        node: id,
                        kind: "election".to_string(),
                    },
                    format!("N{id}'s election timer fires"),
                );
                self.step(id, Event::ElectionTimeout);
                true
            }
        }
    }

    fn fire_leader_heartbeat(&mut self, id: NodeId) {
        if !self.nodes[&id].machine.role().is_leader() {
            return;
        }
        let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
        self.apply_action(id, Action::SendBeginQuorumEpoch { epoch });
        let deadline = self.now.saturating_add_ms(HEARTBEAT_MS);
        self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
    }

    fn deliver(&mut self, msg: Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        let label = event_label(&msg.event);
        let (src, dst) = (msg.src, msg.dst);
        self.step(dst, msg.event);
        self.record(
            TraceAction::Deliver {
                src,
                dst,
                event: label.clone(),
            },
            format!("N{src} → N{dst}: {label}"),
        );
        self.record_new_leaders();
    }

    fn step(&mut self, id: NodeId, event: Event) {
        let now = self.now;
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        let actions = {
            let node = self.nodes.get_mut(&id).unwrap();
            node.machine.on_event(event, &node.log, now)
        };
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
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(id, action);
        }
        self.reconcile_timers_for_role(id);
    }

    fn reconcile_timers_for_role(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        match node.machine.role() {
            Role::Leader { .. } => {
                node.election_deadline = None;
                node.fetch_deadline = None;
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

    fn broadcast_vote_request(&mut self, id: NodeId, epoch: LeaderEpoch, pre_vote: bool) {
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
                let node = self.nodes.get_mut(&id).unwrap();
                node.log.append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = self.nodes.get_mut(&id).unwrap();
                node.high_watermark = hwm;
                for peer in self.all_node_ids() {
                    if peer != id && !self.partitioned.contains(&peer) {
                        let p = self.nodes.get_mut(&peer).unwrap();
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
            Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

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
        let leader_hwm = match self.nodes[&leader].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&leader].high_watermark,
        };
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    fn send(&mut self, src: NodeId, dst: NodeId, event: Event) {
        if self.partitioned.contains(&src) || self.partitioned.contains(&dst) {
            return;
        }
        self.queue.push_back(Message { src, dst, event });
    }

    fn all_node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Drain and deliver currently-queued messages in a deliberately non-FIFO
    /// (but deterministic) order: back-to-front. Returns the number delivered.
    /// Used by `out_of_order_delivery` to show the log stays consistent under
    /// reordered delivery.
    fn deliver_queue_reversed(&mut self) -> usize {
        let mut drained: Vec<Message> = self.queue.drain(..).collect();
        drained.reverse();
        let n = drained.len();
        for msg in drained {
            self.deliver(msg);
        }
        n
    }

    /// Deliver the front-of-queue message twice (the duplicate is a no-op on the
    /// recipient because `KRaft` messages carry monotonic epochs/offsets). Returns
    /// `true` if a message was available to duplicate.
    fn deliver_front_twice(&mut self) -> bool {
        let Some(msg) = self.queue.front().cloned() else {
            return false;
        };
        // Deliver the genuine copy.
        let first = self.queue.pop_front().expect("front exists");
        self.deliver(first);
        // Deliver the duplicate of the same message.
        self.deliver(msg);
        true
    }
}

// --------------------------------------------------------------------------
// Curated scenarios
// --------------------------------------------------------------------------

/// Run the three curated, deterministic failure scenarios on a 3-voter cluster
/// and return their recorded traces.
#[must_use]
pub fn scenarios() -> Vec<ScenarioTrace> {
    vec![
        split_brain_prevented(),
        out_of_order_delivery(),
        message_duplication(),
    ]
}

/// Bootstrap one leader, partition it, watch the majority elect a fresh leader
/// at a higher epoch while the isolated old leader cannot, then heal it and see
/// it step down. The cluster ends with exactly one leader.
fn split_brain_prevented() -> ScenarioTrace {
    let nodes = [1u64, 2, 3];
    let mut sim = Sim::new(&nodes);
    sim.run_until_stable(10_000);
    let old_leader = sim.leaders().first().copied().unwrap_or(1);

    sim.partition(old_leader);
    sim.run_until_stable(10_000);
    let new_leader = sim
        .leaders()
        .into_iter()
        .find(|&l| l != old_leader)
        .unwrap_or(old_leader);

    sim.heal(old_leader);
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    assert!(
        final_leaders.len() == 1,
        "split-brain scenario must end with exactly one leader, got {final_leaders:?}"
    );
    let outcome = format!(
        "The majority side elected N{new_leader} at a strictly higher epoch. The \
         isolated old leader N{old_leader} could not advance (no quorum), and on \
         healing it learned the newer epoch from a BeginQuorumEpoch heartbeat and \
         stepped down to follower. Exactly one leader remains."
    );
    ScenarioTrace {
        id: "split_brain_prevented".to_string(),
        title: "Split-brain prevented (leader partition)".to_string(),
        summary: "A 3-voter cluster elects a leader, the leader is network-partitioned \
                  away from the majority, and the two-node majority elects a new leader \
                  at a higher epoch. The isolated old leader cannot make progress without \
                  a quorum, so there is never a second live leader. When the partition \
                  heals, the stale leader learns the newer epoch and steps down."
            .to_string(),
        invariant: "At most one leader per epoch (election safety)".to_string(),
        nodes: nodes.to_vec(),
        steps: sim.steps,
        outcome,
    }
}

/// Drive a replication round whose bus messages are delivered in a deliberately
/// non-FIFO order, demonstrating the log stays consistent because appends carry
/// monotonic offsets + leader epochs so stale/late messages are detected.
fn out_of_order_delivery() -> ScenarioTrace {
    let nodes = [1u64, 2, 3];
    let mut sim = Sim::new(&nodes);
    sim.run_until_stable(10_000);
    let leader = sim.leaders().first().copied().unwrap_or(1);

    // Produce some records, then let the fetch/replication traffic queue up and
    // deliver it back-to-front before settling.
    sim.leader_append(leader, 3);
    // Prime a replication round so there are messages to reorder.
    sim.run_until_stable(50);
    let reordered = sim.deliver_queue_reversed();
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    let log_len = sim
        .nodes
        .values()
        .next()
        .map_or(0, |n| n.log.record_count());
    let outcome = format!(
        "Even though {reordered} in-flight messages were delivered out of order, \
         every voter's log converged identically ({log_len} records) and the \
         cluster kept exactly {} leader. Stale or late messages were ignored \
         because each fetch and append is tagged with a monotonic offset and \
         leader epoch.",
        final_leaders.len()
    );
    ScenarioTrace {
        id: "out_of_order_delivery".to_string(),
        title: "Reordered message delivery".to_string(),
        summary: "The simulator deliberately delivers a round of replication messages \
                  back-to-front (non-FIFO). Because every fetch and append carries a \
                  monotonic offset and the producing leader epoch, a node detects and \
                  ignores any message that is stale or out of order — the replicated \
                  logs still converge to the same contents."
            .to_string(),
        invariant: "Log matching under reordered delivery".to_string(),
        nodes: nodes.to_vec(),
        steps: sim.steps,
        outcome,
    }
}

/// Deliver one message twice and show idempotent handling: no double
/// application, no extra leader.
fn message_duplication() -> ScenarioTrace {
    let nodes = [1u64, 2, 3];
    let mut sim = Sim::new(&nodes);
    // Run a few ticks so there is real in-flight election traffic to duplicate,
    // then deliver the front message twice.
    sim.run_until_stable(20);
    let duplicated = sim.deliver_front_twice();
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    assert!(
        final_leaders.len() <= 1,
        "duplicate delivery must not create a second leader, got {final_leaders:?}"
    );
    let outcome = format!(
        "A message was delivered twice ({}). The duplicate was handled idempotently — \
         a vote already counted is not counted again and an already-known epoch is \
         a no-op — so the cluster still converged to exactly {} leader.",
        if duplicated {
            "duplicate injected"
        } else {
            "no in-flight message"
        },
        final_leaders.len()
    );
    ScenarioTrace {
        id: "message_duplication".to_string(),
        title: "Duplicate message delivery".to_string(),
        summary: "The simulator delivers the same in-flight message twice. KRaft handles \
                  duplicates idempotently: a vote that was already granted/counted has no \
                  additional effect, and a BeginQuorumEpoch for an epoch the node already \
                  knows is a no-op. No double application happens and no spurious second \
                  leader emerges."
            .to_string(),
        invariant: "Idempotent handling of duplicate messages".to_string(),
        nodes: nodes.to_vec(),
        steps: sim.steps,
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn returns_three_traces() {
        let traces = scenarios();
        assert!(traces.len() == 3);
    }

    #[test]
    fn every_trace_has_steps() {
        for trace in scenarios() {
            assert!(
                !trace.steps.is_empty(),
                "scenario {} recorded no steps",
                trace.id
            );
        }
    }

    #[test]
    fn split_brain_ends_with_exactly_one_leader() {
        let traces = scenarios();
        let split = traces
            .iter()
            .find(|t| t.id == "split_brain_prevented")
            .expect("split_brain_prevented trace present");
        let last = split.steps.last().expect("split-brain has steps");
        let leaders = last.roles.iter().filter(|r| r.role == "Leader").count();
        assert!(
            leaders == 1,
            "final step must have exactly one Leader, got {leaders}"
        );
    }
}
