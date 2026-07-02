//! Exhaustive stateright model of the KIP-848 reconciliation core.
//!
//! The model drives the real `step_heartbeat` (membership + heartbeat actions)
//! and a faithful-client environment (revoke-before-add, trust-the-coordinator)
//! to check the headline KIP-848 safety property: no two members ever
//! simultaneously own the same partition. Design:
//! `docs/superpowers/specs/2026-06-14-crabka-kip848-reconciliation-model-design.md`.
//!
//! The faithful client adds/revokes partitions strictly according to the
//! **advertised** assignment the coordinator returned in that member's last
//! heartbeat response (`ReconState::advertised`) — never the raw target — with
//! no cross-member check. That is exactly how a real consumer behaves: it
//! trusts the coordinator's assignment. The safety guarantee must therefore come
//! entirely from the coordinator's withholding (`GroupState::reconcile_member`).
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant},
};

use crabka_protocol::{
    owned::consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
    primitives::uuid::Uuid,
};
use stateright::{Checker, Model, Property};

use super::{
    super::{
        config::NextGenConfig,
        consumer_state::{GroupState, MemberState},
        persistence_next_gen::MemberAssignmentState,
        reconciler::ReconcileInput,
    },
    HeartbeatStep, MetadataProvider, step_heartbeat,
};

const TOPIC: Uuid = Uuid([7; 16]);
const TOPIC_NAME: &str = "t";

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Bounded config (held here, not in the state).
struct ReconModel {
    /// Member-id pool. A member may join, leave, and rejoin.
    pool: Vec<&'static str>,
    partitions: i32,
    max_epoch: i32,
}

/// Per-member coordinator-side projection (single topic → `Vec<i32>`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct MemberProj {
    id: String,
    member_epoch: i32,
    assignment_state: MemberAssignmentState,
    assigned: Vec<i32>,           // coordinator's authoritative current assignment
    pending_revocation: Vec<i32>, // sorted
    target: Vec<i32>,             // group.target.per_member (sorted)
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReconState {
    group_epoch: i32,
    dirty: bool,
    target_epoch: i32,
    members: Vec<MemberProj>, // sorted by id
    /// Ground-truth ledger: what each member actually consumes. Sorted by id;
    /// partitions sorted. This is the observable the headline invariant checks.
    client_owned: Vec<(String, Vec<i32>)>,
    /// The assignment the coordinator last advertised to each member (its last
    /// heartbeat response). The faithful client adds/revokes against THIS, not
    /// the raw target — a member only learns its new assignment when it
    /// heartbeats. Sorted by id; partitions sorted.
    advertised: Vec<(String, Vec<i32>)>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ReconAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
}

/// Static metadata image: one topic with `partitions` partitions.
#[derive(Debug)]
struct ModelMetadata {
    input: ReconcileInput,
}
impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

impl ReconModel {
    fn basic() -> Self {
        Self {
            pool: vec!["a", "b"],
            partitions: 2,
            max_epoch: 8,
        }
    }

    fn wide() -> Self {
        Self {
            pool: vec!["a", "b", "c"],
            partitions: 2,
            max_epoch: 6,
        }
    }

    fn metadata(&self) -> ModelMetadata {
        ModelMetadata {
            input: ReconcileInput {
                topic_id_by_name: [(TOPIC_NAME.to_string(), TOPIC)].into(),
                partitions_per_topic: [(TOPIC, self.partitions)].into(),
                ..Default::default()
            },
        }
    }
}

fn config() -> NextGenConfig {
    NextGenConfig::default() // seeds UniformAssignor (and RangeAssignor)
}

// ── projection <-> real GroupState ──────────────────────────────

fn parts_of(map: Option<&HashMap<Uuid, Vec<i32>>>) -> Vec<i32> {
    let mut v: Vec<i32> = map.and_then(|m| m.get(&TOPIC)).cloned().unwrap_or_default();
    v.sort_unstable();
    v
}

fn to_map(parts: &[i32]) -> HashMap<Uuid, Vec<i32>> {
    if parts.is_empty() {
        HashMap::new()
    } else {
        [(TOPIC, parts.to_vec())].into()
    }
}

/// Reconstruct a real `GroupState` from the projection so the next real call
/// behaves identically to a live run. Fields not in the projection (subscription
/// fixed to the one topic, `last_seen` constant, `previous_member_epoch`
/// irrelevant to any decision) are set to faithful constants.
fn rebuild_group(s: &ReconState) -> GroupState {
    let mut g = GroupState::new("g");
    g.group_epoch = s.group_epoch;
    g.dirty = s.dirty;
    g.target.epoch = s.target_epoch;
    let now = Instant::now();
    for m in &s.members {
        let mut subs = HashSet::new();
        subs.insert(TOPIC_NAME.to_string());
        let ms = MemberState {
            member_id: m.id.clone(),
            instance_id: None,
            rack_id: None,
            client_id: String::new(),
            client_host: String::new(),
            subscribed_topic_names: subs,
            subscribed_topic_regex: None,
            compiled_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: m.member_epoch,
            previous_member_epoch: 0,
            assignment_state: m.assignment_state,
            assigned_partitions: to_map(&m.assigned),
            partitions_pending_revocation: to_map(&m.pending_revocation),
            last_seen: now,
            classic: None,
        };
        g.members.insert(m.id.clone(), ms);
        if !m.target.is_empty() {
            g.target.per_member.insert(m.id.clone(), to_map(&m.target));
        }
    }
    g
}

/// Project a real `GroupState` + the client ledger + advertised map back into
/// the hashable state.
fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
) -> ReconState {
    let mut members: Vec<MemberProj> = g
        .members
        .values()
        .map(|m| MemberProj {
            id: m.member_id.clone(),
            member_epoch: m.member_epoch,
            assignment_state: m.assignment_state,
            assigned: parts_of(Some(&m.assigned_partitions)),
            pending_revocation: parts_of(Some(&m.partitions_pending_revocation)),
            target: parts_of(g.target.per_member.get(&m.member_id)),
        })
        .collect();
    members.sort_by(|a, b| a.id.cmp(&b.id));
    let client_owned: Vec<(String, Vec<i32>)> = owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect();
    let advertised: Vec<(String, Vec<i32>)> = advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ReconState {
        group_epoch: g.group_epoch,
        dirty: g.dirty,
        target_epoch: g.target.epoch,
        members,
        client_owned,
        advertised,
    }
}

fn owned_map(s: &ReconState) -> BTreeMap<String, BTreeSet<i32>> {
    s.client_owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}

fn advertised_map(s: &ReconState) -> BTreeMap<String, Vec<i32>> {
    s.advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}

fn member<'a>(s: &'a ReconState, id: &str) -> Option<&'a MemberProj> {
    s.members.iter().find(|m| m.id == id)
}

fn advertised_for(s: &ReconState, id: &str) -> Vec<i32> {
    s.advertised
        .iter()
        .find(|(k, _)| k == id)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

fn hb_request(
    member_id: &str,
    member_epoch: i32,
    owned: &BTreeSet<i32>,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec![TOPIC_NAME.into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: Some(vec![TopicPartitions {
            topic_id: TOPIC,
            partitions: owned.iter().copied().collect(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

/// The partitions the coordinator advertised to a member in `step`'s response.
fn advertised_of(step: &HeartbeatStep) -> Vec<i32> {
    let mut v: Vec<i32> = step
        .response
        .assignment
        .as_ref()
        .map(|a| {
            a.topic_partitions
                .iter()
                .filter(|tp| tp.topic_id == TOPIC)
                .flat_map(|tp| tp.partitions.iter().copied())
                .collect()
        })
        .unwrap_or_default();
    v.sort_unstable();
    v
}

/// Per-member epoch must never regress across a real step.
fn assert_epoch_monotonic(pre: &ReconState, post: &GroupState) {
    for pm in &pre.members {
        if let Some(m) = post.members.get(&pm.id) {
            assert!(
                m.member_epoch >= pm.member_epoch,
                "member_epoch regressed for {}: {} -> {}",
                pm.id,
                pm.member_epoch,
                m.member_epoch
            );
        }
    }
}

impl Model for ReconModel {
    type State = ReconState;
    type Action = ReconAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReconState {
            group_epoch: 0,
            dirty: false,
            target_epoch: 0,
            members: vec![],
            client_owned: vec![],
            advertised: vec![],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = state.group_epoch < self.max_epoch;
        // Join: any pool id not currently a member (epoch-advancing → gated).
        if under_cap {
            for &id in &self.pool {
                if member(state, id).is_none() {
                    actions.push(ReconAction::Join(id.to_string()));
                }
            }
        }
        for m in &state.members {
            // Leave + Heartbeat are epoch-advancing → gated by the cap.
            if under_cap {
                actions.push(ReconAction::Leave(m.id.clone()));
                actions.push(ReconAction::Heartbeat(m.id.clone()));
            }
            // Faithful-client moves gate on the ADVERTISED assignment (what the
            // member was last told), not the raw target. No cross-member check.
            let advertised = advertised_for(state, &m.id);
            let owned: BTreeSet<i32> = state
                .client_owned
                .iter()
                .find(|(k, _)| k == &m.id)
                .map(|(_, v)| v.iter().copied().collect())
                .unwrap_or_default();
            for &tp in &advertised {
                if !owned.contains(&tp) {
                    actions.push(ReconAction::ClientAdd(m.id.clone(), tp));
                }
            }
            for &tp in &owned {
                if !advertised.contains(&tp) {
                    actions.push(ReconAction::ClientRevoke(m.id.clone(), tp));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(last);
        let mut adv = advertised_map(last);
        match action {
            ReconAction::ClientAdd(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if !advertised_has || entry.contains(&tp) {
                    return None;
                }
                entry.insert(tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            ReconAction::ClientRevoke(id, tp) => {
                let advertised_has = advertised_for(last, &id).contains(&tp);
                let entry = owned.entry(id).or_default();
                if advertised_has || !entry.contains(&tp) {
                    return None;
                }
                entry.remove(&tp);
                let mut next = last.clone();
                next.client_owned = owned_to_vec(&owned);
                Some(next)
            }
            ReconAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                let mut g = rebuild_group(last);
                let req = hb_request(&id, 0, &BTreeSet::new());
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    "",
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.entry(id.clone()).or_default(); // new member owns nothing yet
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv))
            }
            ReconAction::Leave(id) => {
                member(last, &id)?;
                let mut g = rebuild_group(last);
                let req = hb_request(&id, -1, &BTreeSet::new());
                let _ = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    "",
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                owned.remove(&id);
                adv.remove(&id);
                Some(project(&g, &owned, &adv))
            }
            ReconAction::Heartbeat(id) => {
                let epoch = member(last, &id)?.member_epoch;
                let cur_owned: BTreeSet<i32> = owned.get(&id).cloned().unwrap_or_default();
                let mut g = rebuild_group(last);
                let req = hb_request(&id, epoch, &cur_owned);
                let step = step_heartbeat(
                    &mut g,
                    &config(),
                    &self.metadata(),
                    &req,
                    "",
                    Instant::now(),
                );
                assert_epoch_monotonic(last, &g);
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv))
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition.
            Property::always("no_double_ownership", |_, s: &ReconState| {
                let mut seen: HashSet<i32> = HashSet::new();
                for (_, parts) in &s.client_owned {
                    for &p in parts {
                        if !seen.insert(p) {
                            return false;
                        }
                    }
                }
                true
            }),
            // A member is never advertised a partition another member currently
            // owns — the coordinator-side withholding invariant.
            Property::always(
                "advertised_disjoint_from_others_owned",
                |_, s: &ReconState| {
                    for (mid, adv) in &s.advertised {
                        for &p in adv {
                            let owned_by_other = s
                                .client_owned
                                .iter()
                                .any(|(k, v)| k != mid && v.contains(&p));
                            if owned_by_other {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Non-vacuity: a handoff state is reachable (a partition is in one
            // member's target while another member currently owns it).
            Property::sometimes("handoff_witness", |_, s: &ReconState| {
                for m in &s.members {
                    for &tp in &m.target {
                        let owned_by_other = s
                            .client_owned
                            .iter()
                            .any(|(k, v)| k != &m.id && v.contains(&tp));
                        if owned_by_other {
                            return true;
                        }
                    }
                }
                false
            }),
            // Non-vacuity: a fully-converged state is reachable (every member
            // owns exactly its target and is Stable).
            Property::sometimes("converged_witness", |_, s: &ReconState| {
                !s.members.is_empty()
                    && s.members.iter().all(|m| {
                        let owned: Vec<i32> = s
                            .client_owned
                            .iter()
                            .find(|(k, _)| k == &m.id)
                            .map(|(_, v)| v.clone())
                            .unwrap_or_default();
                        m.assignment_state == MemberAssignmentState::Stable && owned == m.target
                    })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch
    }
}

fn run(model: ReconModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn recon_basic() {
    // 2 members, 1 topic, 2 partitions: the minimal handoff scenario. Proves the
    // coordinator's `reconcile_member` withholding keeps ownership disjoint
    // across every interleaving of join / leave / heartbeat / client revoke+add.
    run(ReconModel::basic(), "recon_basic");
}

#[test]
fn recon_wide() {
    // 3 members contending for 2 partitions: more handoff interleavings as
    // members join/leave and partitions migrate between live members.
    run(ReconModel::wide(), "recon_wide");
}
