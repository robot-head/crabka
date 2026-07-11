//! Stateright model of the KIP-595/996 `KRaft` consensus core. The model state
//! holds the REAL `QuorumStateMachine` per node plus an in-memory log and an
//! unordered message network; `next_state` runs the production `on_event` and
//! the checker explores every interleaving. The committed-log linearizability
//! tester lives here too, and message-loss/duplication and node crashes are
//! modeled as explicit `ModelAction`s.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use crabka_raft::kraft::{
    QuorumStateMachine,
    action::{Action, TimerKind},
    event::{Event, LogEnd},
    role::Role,
    types::{Epoch, LogView, NodeId, QuorumState, SimInstant},
};
use stateright::{
    Model, Property,
    semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec},
};

/// Constant logical time. Timeouts are modeled as nondeterministic actions, so
/// the core never needs a varying clock; constant `now` keeps role deadlines
/// constant and the state space finite.
const NOW: SimInstant = SimInstant(0);

/// Identifies a client request thread for the linearizability tester. Each
/// `ClientAppend` uses a fresh id, so every "thread" has exactly one in-flight
/// operation (invoke once, return once).
pub type ClientId = u64;

/// Sequential reference model of the committed log: appends return the assigned
/// offset; a read returns the committed value sequence. A committed Kafka log is
/// an append-only sequence (not a single-value register), so we define our own
/// `SequentialSpec` rather than reuse the built-in register. The linearization
/// point of an append is when the value enters the committed prefix (the
/// leader's high-watermark passes its offset).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct KraftLogSpec {
    committed: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogOp {
    Append(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogRet {
    /// The committed client-value prefix (in commit order) up to and including
    /// this append, observed at the moment it committed. Using the value prefix
    /// rather than a physical offset avoids the leader-change control records
    /// that occupy raft offsets, and makes a lost/reordered committed entry
    /// produce an unserializable history.
    Committed(Vec<u64>),
}

impl SequentialSpec for KraftLogSpec {
    type Op = LogOp;
    type Ret = LogRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        let LogOp::Append(v) = op;
        self.committed.push(*v);
        LogRet::Committed(self.committed.clone())
    }
}

/// In-memory replicated log: `epochs[i]` is the leader epoch of offset `i`.
/// (A self-contained copy of the sim-harness `SimLog`, made `Eq + Hash` so it
/// can live in fingerprinted model state.)
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ModelLog {
    epochs: Vec<Epoch>,
}

impl ModelLog {
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
    fn replicate_from(&mut self, leader: &ModelLog) {
        if self.epochs.len() < leader.epochs.len() {
            self.epochs.clone_from(&leader.epochs);
        }
    }
    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

impl LogView for ModelLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }
    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
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

/// One node: the real consensus machine + its log + its committed high-watermark.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeModel {
    pub machine: QuorumStateMachine,
    pub log: ModelLog,
    pub high_watermark: i64,
}

/// An in-flight message. The network is an unordered multiset of these.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Envelope {
    pub src: NodeId,
    pub dst: NodeId,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelState {
    pub nodes: BTreeMap<NodeId, NodeModel>,
    /// Unordered in-flight messages. `BTreeSet` => deterministic Hash/Eq and
    /// identical duplicate envelopes collapse to one; explicit `DuplicateDeliver`
    /// actions model network duplication without accumulating copies in the set.
    pub network: BTreeSet<Envelope>,
    /// Linearizability auxiliary state, recomputed + fingerprinted per state.
    pub linz: LinearizabilityTester<ClientId, KraftLogSpec>,
    /// Client appends not yet observed committed, keyed by the leader-assigned
    /// offset they were written at. When some node's high-watermark passes an
    /// offset, that append's `on_return` is recorded.
    pub pending: BTreeMap<i64, (ClientId, u64)>,
    /// Authoritative committed client values, in commit order. Grown as appends
    /// commit; the linearizability return values are checked against it.
    pub committed: Vec<u64>,
    /// Total client appends issued so far (bounded by `ConsensusModel::max_appends`).
    pub appends_issued: u32,
    /// Crashed (unreachable) nodes. Omission model: a crashed node sends/receives
    /// nothing and is offered no actions until `Recover`; on `Crash` we also drop
    /// its in-flight messages (conservative — a real crash-stop could still have
    /// already-sent messages arrive, so a violation needing that delivery would be
    /// missed; sound because it only removes interleavings). Its
    /// `QuorumStateMachine` retains its (durable) state — modelling volatile-state
    /// loss on restart is out of scope for this phase (no public reset API).
    pub crashed: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ModelAction {
    Deliver(Envelope),
    Timeout(NodeId, TimerKind),
    /// A client appends `value` (as `client`) to the single current leader.
    ClientAppend(ClientId, u64),
    /// Drop an in-flight message without delivering it (network loss).
    DropMsg(Envelope),
    /// Deliver a copy of an in-flight message but leave the original queued
    /// (network duplication).
    DuplicateDeliver(Envelope),
    /// A node crashes (becomes unreachable). Omission model.
    Crash(NodeId),
    /// A crashed node recovers (becomes reachable again).
    Recover(NodeId),
}

pub struct ConsensusModel {
    pub voter_ids: Vec<NodeId>,
    /// Max client appends issued across a path. `0` disables the client-append /
    /// linearizability machinery entirely (election/log-matching focus).
    pub max_appends: u32,
    /// Cap on in-flight messages (state-space bound).
    pub max_inflight: usize,
    /// Cap on leader epoch (state-space bound).
    pub max_epoch: Epoch,
    /// Enable message loss + duplication faults.
    pub enable_loss_dup: bool,
    /// Max concurrently-crashed nodes (`0` = no crashes).
    pub max_crashes: usize,
}

impl ConsensusModel {
    /// Election + log-matching focus: NO client appends, so the space stays the
    /// small/fast one from the scaffolding task. Exercises leader election and
    /// log replication safety across N voters.
    pub fn elections(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends: 0,
            max_inflight: 3,
            max_epoch: 2,
            enable_loss_dup: false,
            max_crashes: 0,
        }
    }

    /// Linearizability focus: client appends ENABLED but with tight bounds, since
    /// the linearizability tester (history in the fingerprinted state) makes the
    /// space far larger. Kept small enough to exhaust exactly.
    pub fn linearizable(voter_ids: &[NodeId], max_appends: u32) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends,
            max_inflight: 3,
            max_epoch: 2,
            enable_loss_dup: false,
            max_crashes: 0,
        }
    }

    /// Fault-injection focus: message loss + duplication + a single crash/recover,
    /// over very tight bounds (faults multiply the action space). No client
    /// appends — this exercises election + log-matching safety under an adversarial
    /// network, which is where the bounded space stays exhaustible.
    pub fn faults(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
            max_appends: 0,
            max_inflight: 2,
            max_epoch: 2,
            enable_loss_dup: true,
            max_crashes: 1,
        }
    }

    fn election_timeout_ms_of(id: NodeId) -> u64 {
        1000 + id.0 * 50
    }

    fn voter_set(&self) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(self.voter_ids.iter().map(|&id| {
            crabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: Vec::new(),
                kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }

    /// Translate one `Action` emitted by `id` into mutations of `state`
    /// (network envelopes / log / HWM). Ported from `sim_harness` `apply_action`,
    /// minus the timer arming (timeouts are model actions here).
    // A single match over every `Action` variant: long by nature, and `action`
    // is logically consumed (translated) here, so take it by value.
    fn apply_action(&self, state: &mut ModelState, id: NodeId, action: &Action) {
        match action.clone() {
            Action::SendVoteRequest { epoch, pre_vote } => {
                let cand_log = state.nodes[&id].log.log_end();
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveVoteRequest {
                                from: id,
                                voter_id: peer,
                                candidate_epoch: epoch,
                                candidate: id,
                                candidate_log_end: cand_log,
                                pre_vote,
                            },
                        });
                    }
                }
            }
            Action::ReplyVote { to, epoch, granted } => {
                state.network.insert(Envelope {
                    src: id,
                    dst: to,
                    event: Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                });
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveBeginQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        });
                    }
                }
            }
            Action::SendEndQuorumEpoch { epoch } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveEndQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        });
                    }
                }
            }
            Action::SendFetch { leader_id } => Self::apply_fetch_action(state, id, leader_id),
            Action::AppendLeaderChange { epoch } => {
                state
                    .nodes
                    .get_mut(&id)
                    .expect("appender exists")
                    .log
                    .append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = state.nodes.get_mut(&id).expect("leader exists");
                node.high_watermark = hwm;
                // The HWM rides the leader's fetch responses to followers; mirror
                // that by pushing the committed boundary to peers, clamped to
                // what each has actually replicated.
                let peers: Vec<NodeId> = self
                    .voter_ids
                    .iter()
                    .copied()
                    .filter(|&p| p != id)
                    .collect();
                for peer in peers {
                    if let Some(p) = state.nodes.get_mut(&peer) {
                        p.high_watermark = hwm.min(p.log.end_offset());
                    }
                }
            }
            Action::TruncateTo(point) => {
                let node = state.nodes.get_mut(&id).expect("truncator exists");
                node.log.truncate_to(point.offset);
                // A node cannot retain a committed-prefix marker past what it
                // now physically holds: clamp its high-watermark to the new log
                // end (the eager HWM propagation in `AdvanceHighWatermark` may
                // have set it higher before this divergence-driven truncation).
                node.high_watermark = node.high_watermark.min(node.log.end_offset());
            }
            // Timer arming is modeled by the `Timeout` action set; durable-state
            // + role-transition signals have no cross-node effect in the model.
            Action::ResetTimer { .. } | Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

    fn apply_fetch_action(state: &mut ModelState, id: NodeId, leader_id: NodeId) {
        // Replicate any missing entries from the leader, then fetch at
        // the follower's (now-advanced) tip so the leader can advance HWM.
        if leader_id != id
            && state
                .nodes
                .get(&leader_id)
                .is_some_and(|n| n.machine.role().is_leader())
        {
            let leader_log = state.nodes[&leader_id].log.clone();
            let leader_hwm = node_high_watermark(&state.nodes[&leader_id]);
            let f = state.nodes.get_mut(&id).expect("fetcher exists");
            f.log.replicate_from(&leader_log);
            f.high_watermark = leader_hwm.min(f.log.end_offset());
        }
        let (fetch_epoch, fetch_offset) = {
            let log = &state.nodes[&id].log;
            (log.last_epoch(), log.end_offset())
        };
        // Single outstanding fetch per follower (one Kafka fetch
        // connection): a new fetch supersedes any in-flight one from this
        // node. Without this, the unordered network could deliver a stale
        // lower-offset fetch after a newer one, regressing the leader's
        // recorded follower progress — which the production core forbids
        // (`handle_fetch` overwrites `progress.fetch_offset`
        // unconditionally, relying on per-follower fetch offsets arriving
        // monotonically, as they do over a single TCP connection).
        state
            .network
            .retain(|e| !(e.src == id && matches!(e.event, Event::ReceiveFetch { .. })));
        state.network.insert(Envelope {
            src: id,
            dst: leader_id,
            event: Event::ReceiveFetch {
                from: id,
                fetch_epoch,
                fetch_offset,
            },
        });
    }

    /// Deliver `event` to `dst`: run the real machine and translate emitted
    /// actions. Also synthesizes the leader's fetch RESPONSE (the core emits
    /// HWM/Truncate, not a response message) — ported from `sim_harness` `step`.
    fn step(&self, state: &mut ModelState, dst: NodeId, event: Event) {
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        let actions = {
            let node = state.nodes.get_mut(&dst).expect("dst exists");
            node.machine.on_event(event, &node.log, NOW)
        };
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::TruncateTo(point) => Some(*point),
                _ => None,
            });
            if state.nodes[&dst].machine.role().is_leader() && state.nodes.contains_key(&follower) {
                let leader_epoch = state.nodes[&dst].machine.quorum_state().leader_epoch;
                let leader_end = state.nodes[&dst].log.end_offset();
                let follower_end = state.nodes[&follower].log.end_offset();
                if diverging.is_some() || follower_end < leader_end {
                    state.network.insert(Envelope {
                        src: dst,
                        dst: follower,
                        event: Event::ReceiveFetchResponse {
                            leader_id: dst,
                            leader_epoch,
                            diverging,
                        },
                    });
                }
            }
        }
        for action in actions {
            // A leader-side TruncateTo while serving a fetch is a hint for the
            // FOLLOWER (carried in the response's `diverging`), not the leader.
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(state, dst, &action);
        }
    }
}

/// High watermark of a node regardless of role.
fn node_high_watermark(n: &NodeModel) -> i64 {
    match n.machine.role() {
        Role::Leader { high_watermark, .. } => *high_watermark,
        _ => n.high_watermark,
    }
}

/// True iff `n` currently believes itself leader.
fn is_leader(n: &NodeModel) -> bool {
    n.machine.role().is_leader()
}

/// Record `on_return` for any pending append whose offset is now committed (the
/// max high-watermark across nodes has passed it). Returns are recorded in
/// ascending offset order — the order in which the committed prefix grows.
fn settle_committed(state: &mut ModelState) {
    let max_hwm = state
        .nodes
        .values()
        .map(node_high_watermark)
        .max()
        .unwrap_or(0);
    // Offsets strictly below the high-watermark are committed (HWM is one past
    // the last committed offset).
    let ready: Vec<i64> = state
        .pending
        .range(..max_hwm)
        .map(|(&off, _)| off)
        .collect();
    for off in ready {
        let (client, value) = state.pending.remove(&off).expect("pending entry exists");
        state.committed.push(value);
        let _ = state
            .linz
            .on_return(client, LogRet::Committed(state.committed.clone()))
            .expect("matching invoke recorded");
    }
}

impl Model for ConsensusModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let voters = self.voter_set();
        let mut nodes = BTreeMap::new();
        for &id in &self.voter_ids {
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                Self::election_timeout_ms_of(id),
            );
            nodes.insert(
                id,
                NodeModel {
                    machine,
                    log: ModelLog::default(),
                    high_watermark: 0,
                },
            );
        }
        vec![ModelState {
            nodes,
            network: BTreeSet::new(),
            linz: LinearizabilityTester::new(KraftLogSpec::default()),
            pending: BTreeMap::new(),
            committed: Vec::new(),
            appends_issued: 0,
            crashed: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every in-flight message is independently deliverable (unordered net).
        // Loss/duplication, when enabled, offer a drop and a duplicate-deliver
        // for each in-flight message.
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
            if self.enable_loss_dup {
                actions.push(ModelAction::DropMsg(env.clone()));
                actions.push(ModelAction::DuplicateDeliver(env.clone()));
            }
        }
        // Any voter that is not currently leader may suffer an election timeout;
        // any follower/observer may suffer a fetch timeout. The core ignores
        // inapplicable ones, so over-offering is sound (it only adds interleavings).
        // Crashed nodes are unreachable: offered no timeouts.
        for (&id, node) in &state.nodes {
            if state.crashed.contains(&id) {
                continue;
            }
            match node.machine.role() {
                Role::Leader { .. } => {}
                Role::Follower { .. } | Role::Observer { .. } => {
                    actions.push(ModelAction::Timeout(id, TimerKind::Fetch));
                    actions.push(ModelAction::Timeout(id, TimerKind::Election));
                }
                _ => actions.push(ModelAction::Timeout(id, TimerKind::Election)),
            }
        }
        // Crash/recover, capped at `max_crashes` concurrently crashed.
        if state.crashed.len() < self.max_crashes {
            for &id in &self.voter_ids {
                if !state.crashed.contains(&id) {
                    actions.push(ModelAction::Crash(id));
                }
            }
        }
        for &id in &state.crashed {
            actions.push(ModelAction::Recover(id));
        }
        // A client appends to the single current (live) leader (only when the
        // target is unambiguous and the append budget remains). A fresh client id
        // per append keeps every linearizability "thread" single-op.
        let leaders: Vec<NodeId> = state
            .nodes
            .iter()
            .filter(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
            .map(|(&id, _)| id)
            .collect();
        if leaders.len() == 1 && state.appends_issued < self.max_appends {
            let client = ClientId::from(state.appends_issued) + 1;
            let value = u64::from(state.appends_issued) + 1;
            actions.push(ModelAction::ClientAppend(client, value));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                // A crashed destination is unreachable: the message is consumed
                // (removed) but produces no transition.
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::DropMsg(env) => {
                // Network loss: remove without delivering. No-op if already gone.
                if !state.network.remove(&env) {
                    return None;
                }
            }
            ModelAction::DuplicateDeliver(env) => {
                // Network duplication: deliver a copy, leave the original queued.
                if !state.network.contains(&env) {
                    return None;
                }
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
            ModelAction::Crash(id) => {
                if !state.crashed.insert(id) {
                    return None;
                }
                // Omission model: drop all messages to/from the crashed node.
                state.network.retain(|e| e.src != id && e.dst != id);
            }
            ModelAction::Recover(id) => {
                if !state.crashed.remove(&id) {
                    return None;
                }
            }
            ModelAction::Timeout(id, kind) => {
                let event = match kind {
                    TimerKind::Election => Event::ElectionTimeout,
                    TimerKind::Fetch => Event::FetchTimeout,
                };
                self.step(&mut state, id, event);
            }
            ModelAction::ClientAppend(client, value) => {
                let leader = state
                    .nodes
                    .iter()
                    .find(|(id, n)| is_leader(n) && !state.crashed.contains(*id))
                    .map(|(&id, _)| id)?;
                let epoch = state.nodes[&leader].machine.quorum_state().leader_epoch;
                let offset = state.nodes[&leader].log.end_offset();
                // Record the invocation, append at the leader, track until committed.
                let _ = state
                    .linz
                    .on_invoke(client, LogOp::Append(value))
                    .expect("fresh client id has no in-flight op");
                state
                    .nodes
                    .get_mut(&leader)
                    .expect("leader exists")
                    .log
                    .append_in_epoch(epoch, 1);
                state.pending.insert(offset, (client, value));
                state.appends_issued += 1;
            }
        }
        // After ANY transition, record `on_return` for appends whose offset is
        // now committed (some node's high-watermark passed it). HWM advances on
        // fetch deliveries, so commits land on later steps than the append.
        settle_committed(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Anti-vacuity witness: a leader is actually elected in some state.
            Property::sometimes("leader_elected", |_, s: &ModelState| {
                s.nodes.values().any(is_leader)
            }),
            // Safety: at most one leader per leader-epoch.
            Property::always("election_safety", |_, s: &ModelState| {
                let mut by_epoch: BTreeMap<Epoch, NodeId> = BTreeMap::new();
                for (&id, n) in &s.nodes {
                    if is_leader(n) {
                        let epoch = n.machine.quorum_state().leader_epoch;
                        if let Some(&other) = by_epoch.get(&epoch)
                            && other != id
                        {
                            return false;
                        }
                        by_epoch.insert(epoch, id);
                    }
                }
                true
            }),
            // Safety: the committed log is linearizable — there exists a single
            // total order of client appends consistent with every observed
            // invoke/return. A lost or reordered committed entry has no such
            // serialization.
            Property::always("linearizable", |_, s: &ModelState| {
                s.linz.serialized_history().is_some()
            }),
            // Anti-vacuity witness: a CLIENT append is actually committed.
            // Without this, `linearizable` could hold vacuously because no
            // client value ever committed (a control-record-only HWM advance
            // would not count).
            Property::sometimes("entry_committed", |m: &ConsensusModel, s: &ModelState| {
                // Only required when client appends are enabled; a no-append
                // config (election focus) satisfies this trivially.
                m.max_appends == 0 || !s.committed.is_empty()
            }),
            // Safety (Raft log matching): two logs may diverge only as an
            // uncommitted suffix — if they disagree on the epoch at some offset
            // `k`, they must not agree again at any later offset (equal entries
            // imply equal prefixes). Re-agreement after disagreement is a true
            // matching violation.
            Property::always("log_matching", |_, s: &ModelState| {
                let logs: Vec<&Vec<Epoch>> = s.nodes.values().map(|n| &n.log.epochs).collect();
                for i in 0..logs.len() {
                    for j in (i + 1)..logs.len() {
                        let (a, b) = (logs[i], logs[j]);
                        let common = a.len().min(b.len());
                        for k in 0..common {
                            if a[k] != b[k] && (k + 1..common).any(|m| a[m] == b[m]) {
                                return false;
                            }
                        }
                    }
                }
                true
            }),
            // Safety: no node's committed high-watermark exceeds its own log end
            // (a node cannot have committed past what it physically holds).
            Property::always("hwm_within_log", |_, s: &ModelState| {
                s.nodes
                    .values()
                    .all(|n| node_high_watermark(n) <= n.log.end_offset())
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound the space HARD: stateright BFS/DFS keeps every visited unique
        // state in memory, so loose bounds OOM the machine. Cap in-flight
        // messages and the maximum leader epoch per the model's config.
        state.network.len() <= self.max_inflight
            && state
                .nodes
                .values()
                .all(|n| n.machine.quorum_state().leader_epoch <= self.max_epoch)
    }
}
