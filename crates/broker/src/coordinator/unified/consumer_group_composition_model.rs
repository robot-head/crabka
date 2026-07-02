//! COMPOSITIONAL model of consumer-group offset-commit FENCING through rebalances
//! — the third compositional model (after data-path #539, txn/EOS #541). It
//! reuses the proven driving machinery of `reconciler_model.rs` (#521) —
//! rebuilding a real `GroupState` and driving the REAL `step_heartbeat` /
//! `reconcile_member` over join / leave / heartbeat / faithful-client moves — and
//! composes it with the REAL `OffsetCommit` epoch-fence.
//!
//! Verifies the genuine consumer-group offset-integrity guarantee: through any
//! rebalance interleaving, (a) no two members ever own the same partition
//! (exclusivity — the real reconciliation's withholding, re-verified in the
//! composed context), and (b) a member may commit ONLY with its CURRENT member
//! epoch — a zombie from before a rebalance (whose epoch the reconciliation has
//! since bumped) is fenced — so a stale/forward commit can never corrupt the
//! committed offset.
//!
//! Scope — DRIVEN vs MODELED (stated up front, per the txn/EOS review which found
//! the first draft over-claimed):
//!   - DRIVEN (real code): the KIP-848 reconciliation engine via `step_heartbeat`,
//!     AND the real `OffsetCommit` fence `GroupState::validate_commit_decision`
//!     (extracted from the actor's `ValidateCommit`). The model drives the real
//!     fence and cross-checks it against an INDEPENDENT oracle (the expected
//!     epoch comparison + error codes) — a divergence is a real fence/regression
//!     bug.
//!   - NOT verified here (deliberately): NO-DUPLICATE / NO-GAP delivery — plain
//!     consumer groups are AT-LEAST-ONCE (a zombie reprocesses until fenced);
//!     exactly-once is the txn/EOS path (#541). The committed offset is a small
//!     bounded counter, present only to witness accepted commits. The
//!     `__consumer_offsets` log persistence (#539) and the classic protocol
//!     (`classic_state_model` #534) are out of scope.
//!
//! Memory safety: run under the host memory watchdog while bounds are tuned.

use std::{
    cmp::Ordering,
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
const MAX_OFFSET: i64 = 2; // bound the committed offset so the state space stays finite

const MAX_STATES: usize = 2_000_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct CgcModel {
    pool: Vec<&'static str>,
    partitions: i32,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct MemberProj {
    id: String,
    member_epoch: i32,
    assignment_state: MemberAssignmentState,
    assigned: Vec<i32>,
    pending_revocation: Vec<i32>,
    target: Vec<i32>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CgcState {
    group_epoch: i32,
    dirty: bool,
    target_epoch: i32,
    members: Vec<MemberProj>,              // sorted by id
    client_owned: Vec<(String, Vec<i32>)>, // ground-truth ownership ledger
    advertised: Vec<(String, Vec<i32>)>,   // last advertised to each member
    committed: Vec<(i32, i64)>,            // MODELED per-partition committed offset, sorted
}

/// Which epoch a member presents on an `OffsetCommit`: its current epoch (the
/// legitimate owner), one behind (a zombie from before the last rebalance), or
/// one ahead (an impossible/forward epoch). The real fence must accept only
/// `Current`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum EpochKind {
    Current,
    Stale,
    Forward,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum CgcAction {
    Join(String),
    Leave(String),
    Heartbeat(String),
    ClientAdd(String, i32),
    ClientRevoke(String, i32),
    Commit(String, i32, EpochKind), // (member, partition, presented-epoch) — fenced commit
}

#[derive(Debug)]
struct ModelMetadata {
    input: ReconcileInput,
}
impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

impl CgcModel {
    fn basic() -> Self {
        Self {
            pool: vec!["a", "b"],
            partitions: 2,
            max_epoch: 6,
        }
    }
    fn wide() -> Self {
        Self {
            pool: vec!["a", "b", "c"],
            partitions: 2,
            max_epoch: 5,
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
    NextGenConfig::default()
}

// ── projection <-> real GroupState (mirrors reconciler_model) ───────

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

fn rebuild_group(s: &CgcState) -> GroupState {
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

fn project(
    g: &GroupState,
    owned: &BTreeMap<String, BTreeSet<i32>>,
    advertised: &BTreeMap<String, Vec<i32>>,
    committed: &BTreeMap<i32, i64>,
) -> CgcState {
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
    CgcState {
        group_epoch: g.group_epoch,
        dirty: g.dirty,
        target_epoch: g.target.epoch,
        members,
        client_owned: owned
            .iter()
            .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
            .collect(),
        advertised: advertised
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        committed: committed.iter().map(|(&k, &v)| (k, v)).collect(),
    }
}

fn owned_map(s: &CgcState) -> BTreeMap<String, BTreeSet<i32>> {
    s.client_owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}
fn advertised_map(s: &CgcState) -> BTreeMap<String, Vec<i32>> {
    s.advertised
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
fn committed_map(s: &CgcState) -> BTreeMap<i32, i64> {
    s.committed.iter().copied().collect()
}
fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}
fn member<'a>(s: &'a CgcState, id: &str) -> Option<&'a MemberProj> {
    s.members.iter().find(|m| m.id == id)
}
fn advertised_for(s: &CgcState, id: &str) -> Vec<i32> {
    s.advertised
        .iter()
        .find(|(k, _)| k == id)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
fn committed_of(s: &CgcState, part: i32) -> i64 {
    s.committed
        .iter()
        .find(|(p, _)| *p == part)
        .map_or(0, |(_, o)| *o)
}
/// INDEPENDENT oracle for the `OffsetCommit` fence: the expected decision for a
/// member presenting `epoch`. Deliberately a different structure (`Ordering`)
/// from the real `validate_commit_decision`'s if-guards, so driving the real fn
/// and asserting equality is a genuine cross-check (a fence regression diverges).
fn oracle_commit(g: &GroupState, id: &str, epoch: i32) -> Result<(), i16> {
    match g.members.get(id) {
        None => Err(crate::codes::UNKNOWN_MEMBER_ID),
        Some(m) => match epoch.cmp(&m.member_epoch) {
            Ordering::Less => Err(crate::codes::STALE_MEMBER_EPOCH),
            Ordering::Greater => Err(crate::codes::FENCED_MEMBER_EPOCH),
            Ordering::Equal => Ok(()),
        },
    }
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

fn assert_epoch_monotonic(pre: &CgcState, post: &GroupState) {
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

/// Drive the REAL `OffsetCommit` epoch fence (`validate_commit_decision`) for the
/// epoch `kind` the member presents, cross-check it against the independent
/// oracle, and — only on accept (a current-epoch member) — advance the bounded
/// committed offset. The member's CURRENT epoch is whatever the real
/// reconciliation last set, so a `Stale` commit after a rebalance is a zombie and
/// is fenced. Kafka does NOT check partition ownership here (at-least-once).
fn do_commit(last: &CgcState, id: &str, part: i32, kind: EpochKind) -> Option<CgcState> {
    let g = rebuild_group(last);
    let cur = member(last, id).map(|m| m.member_epoch);
    let epoch = match (cur, kind) {
        (Some(e), EpochKind::Current) => e,
        (Some(e), EpochKind::Stale) => e - 1,
        (Some(e), EpochKind::Forward) => e + 1,
        (None, _) => 0,
    };
    let real = g.validate_commit_decision(id, epoch);
    let oracle = oracle_commit(&g, id, epoch);
    assert_eq!(
        real, oracle,
        "OffsetCommit fence diverges from oracle: member={id} epoch={epoch}"
    );
    if real.is_err() {
        return None; // fenced (stale/forward/unknown) — cannot touch the offset
    }
    let mut committed = committed_map(last);
    let off = committed.entry(part).or_insert(0);
    if *off >= MAX_OFFSET {
        return None;
    }
    *off += 1;
    let mut next = last.clone();
    next.committed = committed.iter().map(|(&k, &v)| (k, v)).collect();
    Some(next)
}

impl Model for CgcModel {
    type State = CgcState;
    type Action = CgcAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CgcState {
            group_epoch: 0,
            dirty: false,
            target_epoch: 0,
            members: vec![],
            client_owned: vec![],
            advertised: vec![],
            committed: vec![],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = state.group_epoch < self.max_epoch;
        if under_cap {
            for &id in &self.pool {
                if member(state, id).is_none() {
                    actions.push(CgcAction::Join(id.to_string()));
                }
            }
        }
        for m in &state.members {
            if under_cap {
                actions.push(CgcAction::Leave(m.id.clone()));
                actions.push(CgcAction::Heartbeat(m.id.clone()));
            }
            let advertised = advertised_for(state, &m.id);
            let owned: BTreeSet<i32> = state
                .client_owned
                .iter()
                .find(|(k, _)| k == &m.id)
                .map(|(_, v)| v.iter().copied().collect())
                .unwrap_or_default();
            for &tp in &advertised {
                if !owned.contains(&tp) {
                    actions.push(CgcAction::ClientAdd(m.id.clone(), tp));
                }
            }
            for &tp in &owned {
                if !advertised.contains(&tp) {
                    actions.push(CgcAction::ClientRevoke(m.id.clone(), tp));
                }
            }
            // Offset commit: offered with EACH epoch kind (current / stale /
            // forward) so the real epoch fence — not a precondition — is what's
            // exercised; the committed counter is bounded by MAX_OFFSET.
            for part in 0..self.partitions {
                if committed_of(state, part) < MAX_OFFSET {
                    for kind in [EpochKind::Current, EpochKind::Stale, EpochKind::Forward] {
                        actions.push(CgcAction::Commit(m.id.clone(), part, kind));
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(last);
        let mut adv = advertised_map(last);
        let committed = committed_map(last);
        match action {
            CgcAction::ClientAdd(id, tp) => {
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
            CgcAction::ClientRevoke(id, tp) => {
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
            CgcAction::Join(id) => {
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
                owned.entry(id.clone()).or_default();
                adv.insert(id, advertised_of(&step));
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Leave(id) => {
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
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Heartbeat(id) => {
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
                Some(project(&g, &owned, &adv, &committed))
            }
            CgcAction::Commit(id, part, kind) => do_commit(last, &id, part, kind),
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: no two members ever simultaneously own the same partition
            // (the real reconciliation's withholding — re-verified in the composed
            // context, with offset traffic interleaved).
            Property::always("exclusive_ownership", |_, s: &CgcState| {
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
                |_, s: &CgcState| {
                    for (mid, adv) in &s.advertised {
                        for &p in adv {
                            if s.client_owned
                                .iter()
                                .any(|(k, v)| k != mid && v.contains(&p))
                            {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // The real OffsetCommit epoch fence agrees with the independent oracle
            // is enforced as a per-transition `assert_eq!` in the Commit arm (a
            // divergence is a real `validate_commit_decision` regression). The
            // value here is the COMPOSITION: the epochs that fence drives are set
            // by the real reconciliation, so a zombie from before a rebalance is
            // rejected — see the `member_epoch_advanced` witness.

            // ----- non-vacuity witnesses -----
            // A current-epoch commit was accepted (the fence's accept path fires).
            Property::sometimes("offset_advanced", |_, s: &CgcState| {
                s.committed.iter().any(|&(_, o)| o > 0)
            }),
            // The reconciliation actually advanced a member's epoch past its first
            // generation — so `Stale`/`Forward` commits are genuinely distinct from
            // `Current` and the fence is exercised over non-trivial epochs (a real
            // zombie scenario: a stale commit after a rebalance bumped the epoch).
            Property::sometimes("member_epoch_advanced", |_, s: &CgcState| {
                s.members.iter().any(|m| m.member_epoch >= 2)
            }),
            // A handoff state: a partition is in one member's target while another
            // member currently owns it (the baton is mid-pass).
            Property::sometimes("handoff_witness", |_, s: &CgcState| {
                for m in &s.members {
                    for &tp in &m.target {
                        if s.client_owned
                            .iter()
                            .any(|(k, v)| k != &m.id && v.contains(&tp))
                        {
                            return true;
                        }
                    }
                }
                false
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.group_epoch <= self.max_epoch && state.committed.iter().all(|&(_, o)| o <= MAX_OFFSET)
    }
}

fn run(model: CgcModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(checker.state_count() < MAX_STATES, "[{label}] truncated");
    checker.assert_properties();
}

#[test]
fn cg_basic() {
    run(CgcModel::basic(), "cg_basic");
}

#[test]
fn cg_wide() {
    run(CgcModel::wide(), "cg_wide");
}
