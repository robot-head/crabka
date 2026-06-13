//! Stateright model of the KIP-595/996 `KRaft` consensus core. The model state
//! holds the REAL `QuorumStateMachine` per node plus an in-memory log and an
//! unordered message network; `next_state` runs the production `on_event` and
//! the checker explores every interleaving. Faults (loss/dup/crash) and the
//! linearizability tester are layered in by later tasks.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use crabka_raft::kraft::QuorumStateMachine;
use crabka_raft::kraft::action::{Action, TimerKind};
use crabka_raft::kraft::event::{Event, LogEnd};
use crabka_raft::kraft::role::Role;
use crabka_raft::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, SimInstant};
use stateright::{Model, Property};

/// Constant logical time. Timeouts are modeled as nondeterministic actions, so
/// the core never needs a varying clock; constant `now` keeps role deadlines
/// constant and the state space finite.
const NOW: SimInstant = SimInstant(0);

/// In-memory replicated log: `epochs[i]` is the leader epoch of offset `i`.
/// (A self-contained copy of the sim-harness `SimLog`, made `Eq + Hash` so it
/// can live in fingerprinted model state.)
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ModelLog {
    epochs: Vec<LeaderEpoch>,
}

impl ModelLog {
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
    /// identical duplicate envelopes collapse to one (we model duplication via
    /// an explicit `DuplicateDeliver` action instead).
    pub network: BTreeSet<Envelope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ModelAction {
    Deliver(Envelope),
    Timeout(NodeId, TimerKind),
}

pub struct ConsensusModel {
    pub voter_ids: Vec<NodeId>,
}

impl ConsensusModel {
    pub fn new(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
        }
    }

    fn election_timeout_ms_of(id: NodeId) -> u64 {
        1000 + id * 50
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
    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    fn apply_action(&self, state: &mut ModelState, id: NodeId, action: Action) {
        match action {
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
            Action::SendFetch { leader_id } => {
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
                state
                    .nodes
                    .get_mut(&id)
                    .expect("truncator exists")
                    .log
                    .truncate_to(point.offset);
            }
            // Timer arming is modeled by the `Timeout` action set; durable-state
            // + role-transition signals have no cross-node effect in the model.
            Action::ResetTimer { .. } | Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
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
            self.apply_action(state, dst, action);
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
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every in-flight message is independently deliverable (unordered net).
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
        }
        // Any voter that is not currently leader may suffer an election timeout;
        // any follower/observer may suffer a fetch timeout. The core ignores
        // inapplicable ones, so over-offering is sound (it only adds interleavings).
        for (&id, node) in &state.nodes {
            match node.machine.role() {
                Role::Leader { .. } => {}
                Role::Follower { .. } | Role::Observer { .. } => {
                    actions.push(ModelAction::Timeout(id, TimerKind::Fetch));
                    actions.push(ModelAction::Timeout(id, TimerKind::Election));
                }
                _ => actions.push(ModelAction::Timeout(id, TimerKind::Election)),
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                self.step(&mut state, env.dst, env.event);
            }
            ModelAction::Timeout(id, kind) => {
                let event = match kind {
                    TimerKind::Election => Event::ElectionTimeout,
                    TimerKind::Fetch => Event::FetchTimeout,
                };
                self.step(&mut state, id, event);
            }
        }
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
                let mut by_epoch: BTreeMap<LeaderEpoch, NodeId> = BTreeMap::new();
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
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound the space HARD: stateright BFS/DFS keeps every visited unique
        // state in memory, so loose bounds OOM the machine. Cap in-flight
        // messages and the maximum leader epoch tightly; these suffice to
        // exercise election safety while keeping the reachable set small.
        const MAX_INFLIGHT: usize = 4;
        const MAX_EPOCH: LeaderEpoch = 3;
        state.network.len() <= MAX_INFLIGHT
            && state
                .nodes
                .values()
                .all(|n| n.machine.quorum_state().leader_epoch <= MAX_EPOCH)
    }
}
