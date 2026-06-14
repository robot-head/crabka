# KIP-848 Reconciliation Safety Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an exhaustive `stateright` model of the KIP-848 consumer-group reconciliation core that proves (or refutes with a concrete trace) the headline safety property — no two members ever simultaneously own the same partition during a rebalance — by driving the real coordinator decision logic.

**Architecture:** Extract the pure synchronous decision core of `handle_heartbeat` into `step_heartbeat` (the same wrap-real seam used for `failover_one`/`reassign_one`). A `#[cfg(test)]` descendant module of `actor.rs` reconstructs a real `GroupState` from a hashable projection each step, drives `step_heartbeat`, and supplies a faithful-client environment (revoke-before-add, trust-the-coordinator) as nondeterministic model actions. The BFS checker explores every interleaving and asserts disjoint ownership.

**Tech Stack:** Rust, `stateright` 0.31 (dev-dep, already on main), the real `crabka-broker` next-gen coordinator (`crates/broker/src/coordinator/unified/`).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-kip848-reconciliation-model-design.md`

**Verification discipline (MANDATORY):** stateright BFS keeps every visited unique state resident. Every checker run is fenced with `within_boundary` + `target_state_count` + `timeout`, and MUST be executed under the host memory watchdog (kill on >3 GB or >150 s) while bounds are tuned. See `[[feedback_bound_model_checkers]]`. The CI fmt gate is **nightly**; the clippy gate is `-D warnings`. Timeouts use `Duration::from_mins`, not `from_secs(120)` (clippy `duration_suboptimal_units`).

---

## File Structure

- `crates/broker/src/coordinator/unified/actor.rs` — **modify**: extract `step_heartbeat` + `HeartbeatStep` + `leave_step` from `handle_heartbeat`/`handle_leave`; rewrite `handle_heartbeat` to call `step_heartbeat` then flush. Append the model's `#[cfg(test)]` descendant module declaration. (Descendant wiring chosen so the model reaches the private `step_heartbeat` directly, matching the share-group `state_model` precedent.)
- `crates/broker/src/coordinator/unified/persistence_next_gen.rs` — **modify**: add `Hash` to the `MemberAssignmentState` derive (no behavior change; needed only so the model's projection can derive `Hash`).
- `crates/broker/src/coordinator/unified/reconciler_model.rs` — **create**: the model (state projection, faithful-client actions, properties, configs, watchdog-guarded `run` harness).

---

## Task KRM-T1: Extract `step_heartbeat` (pure core) + wire the model module

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs:1015-1092` (`handle_heartbeat`), `:1150-1176` (`handle_leave`)
- Modify: `crates/broker/src/coordinator/unified/persistence_next_gen.rs:319-324` (`MemberAssignmentState` derive)
- Create (stub): `crates/broker/src/coordinator/unified/reconciler_model.rs`

This is a **behavior-preserving** extraction. The existing `actor.rs` heartbeat test suite is the regression gate; additionally we add one focused unit test that calls `step_heartbeat` directly to prove the pure seam works in isolation.

- [ ] **Step 1: Add `Hash` to `MemberAssignmentState`**

In `persistence_next_gen.rs`, change the derive at line 319 from:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberAssignmentState {
```

to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberAssignmentState {
```

- [ ] **Step 2: Add `HeartbeatStep` + `step_heartbeat` + `leave_step` to `actor.rs`**

Insert these three items immediately **above** `async fn handle_heartbeat` (currently line 1015). `step_heartbeat` is the verbatim pure logic of the current `handle_heartbeat` with every `.await` flush removed and replaced by returning the `PendingRecords` to the caller; `leave_step` is the pure form of `handle_leave`.

```rust
/// Outcome of the pure heartbeat decision phase: the response to return to the
/// client and the records the async caller must append to the offsets log.
pub(crate) struct HeartbeatStep {
    pub response: ConsumerGroupHeartbeatResponse,
    pub pending: PendingRecords,
}

/// The pure, synchronous heartbeat decision core: assignor-selection and epoch
/// validation, member upsert / leave, `update_member_state`, `run_reconcile`,
/// `advance_member_epoch`, and response build. Contains no `.await` and performs
/// no I/O — `handle_heartbeat` calls this, then flushes `pending` to the log.
/// Extracted so the reconciliation policy is independently model-checkable.
pub(crate) fn step_heartbeat(
    state: &mut super::consumer_state::GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
    now: Instant,
) -> HeartbeatStep {
    // ─── Leave path ──────────────────────────────────────────────
    if req.member_epoch == -1 {
        return leave_step(state, config, req);
    }

    // ─── Validate assignor selection ─────────────────────────────
    if req
        .server_assignor
        .as_deref()
        .is_some_and(|name| !config.assignor_enabled(name))
    {
        return HeartbeatStep {
            response: error_resp(codes::UNSUPPORTED_ASSIGNOR, config),
            pending: PendingRecords::default(),
        };
    }

    // ─── First-join path ─────────────────────────────────────────
    if req.member_epoch == 0 && !state.members.contains_key(&req.member_id) {
        let new_member_id = if req.member_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.member_id.clone()
        };
        if let Some(iid) = req.instance_id.as_deref()
            && state
                .current_member_for_instance(iid)
                .and_then(|existing| state.members.get(existing))
                .is_some_and(|m| m.member_epoch != 0)
        {
            return HeartbeatStep {
                response: error_resp(codes::UNRELEASED_INSTANCE_ID, config),
                pending: PendingRecords::default(),
            };
        }
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, std::slice::from_ref(&new_member_id));
        let response = build_assignment_resp(state, &new_member_id, config);
        return HeartbeatStep { response, pending };
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = state
        .members
        .get(&req.member_id)
        .map_or(-2, |m| m.member_epoch);
    if cur_epoch == -2 {
        return HeartbeatStep {
            response: error_resp(codes::UNKNOWN_MEMBER_ID, config),
            pending: PendingRecords::default(),
        };
    }
    if req.member_epoch < cur_epoch {
        return HeartbeatStep {
            response: error_resp(codes::STALE_MEMBER_EPOCH, config),
            pending: PendingRecords::default(),
        };
    }
    if req.member_epoch > cur_epoch {
        return HeartbeatStep {
            response: error_resp(codes::FENCED_MEMBER_EPOCH, config),
            pending: PendingRecords::default(),
        };
    }

    // ─── Steady-state ────────────────────────────────────────────
    let any_change = update_member_state(state, config, metadata, req, now, cur_epoch);
    let pending = if any_change {
        snapshot_pending_after_change(state, std::slice::from_ref(&req.member_id))
    } else {
        PendingRecords::default()
    };
    let response = build_assignment_resp(state, &req.member_id, config);
    HeartbeatStep { response, pending }
}

/// Pure form of the leave path (`member_epoch == -1`): remove the member, bump
/// the group epoch, and build the tombstone + group-epoch records. The async
/// caller flushes the returned `pending`.
fn leave_step(
    state: &mut super::consumer_state::GroupState,
    config: &NextGenConfig,
    req: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    let mut pending = PendingRecords::default();
    if state.members.contains_key(&req.member_id) {
        pending.member_metadata.push((req.member_id.clone(), None));
        pending
            .target_per_member
            .push((req.member_id.clone(), None));
        pending
            .current_per_member
            .push((req.member_id.clone(), None));
    }
    state.remove_member(&req.member_id);
    state.bump_epoch();
    pending.group_metadata = Some(GroupMetadataValue {
        epoch: state.group_epoch,
    });
    HeartbeatStep {
        response: base_resp(0, req.member_epoch, config),
        pending,
    }
}
```

- [ ] **Step 3: Rewrite `handle_heartbeat` to call `step_heartbeat` then flush**

Replace the entire body of `async fn handle_heartbeat` (lines 1024-1092, everything after the signature) with:

```rust
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    let step = step_heartbeat(state, config, metadata, req, client_host, now);
    flush_pending(state, step.pending, offsets_log, coordinator, now_ms).await?;
    Ok(step.response)
```

This is behavior-preserving:
- Error / steady-state-no-change paths return `PendingRecords::default()`, and `flush_pending` early-returns when `pending.is_empty()` (verified: `PendingRecords::default()` satisfies `is_empty()` — all fields `None`/empty/`false`), so no I/O occurs — exactly as before.
- Leave and first-join always produce non-empty pending, so the flush (and `coordinator.update_cache` inside it) runs as before.

- [ ] **Step 4: Delete the now-unused `async fn handle_leave`**

`handle_leave` (lines 1150-1176) is fully replaced by `leave_step` (called from `step_heartbeat`). Remove it. If any other caller exists, grep first: `git grep -n "handle_leave(" crates/broker/src` — if the only reference was the deleted line 1029, delete the function.

- [ ] **Step 5: Build + run the existing heartbeat test suite (regression gate)**

Run: `cargo test -p crabka-broker --lib coordinator::unified::actor`
Expected: all existing actor tests PASS (e.g. `first_join_emits_one_batch`, the epoch-fencing and leave tests). This proves the extraction preserved behavior.

- [ ] **Step 6: Add a direct unit test for `step_heartbeat`**

In `actor.rs`'s existing `#[cfg(test)] mod tests`, add a test that drives the pure function directly (no actor, no async):

```rust
#[test]
fn step_heartbeat_first_join_targets_all_partitions() {
    use crate::coordinator::unified::consumer_state::GroupState;
    use crate::coordinator::unified::reconciler::ReconcileInput;
    let topic_id = Uuid([7; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_string(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let config = NextGenConfig::default();
    let mut group = GroupState::new("g");
    let req = ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 0,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    let step = step_heartbeat(&mut group, &config, &metadata, &req, "", Instant::now());
    assert!(step.response.error_code == 0);
    assert!(step.response.member_epoch == 1, "first join advances to group epoch 1");
    // Sole member gets both partitions in its target.
    assert!(group.target.per_member["m1"][&topic_id] == vec![0, 1]);
    assert!(!step.pending.is_empty(), "first join must persist records");
}
```

- [ ] **Step 7: Create the model module stub + wire it**

Create `crates/broker/src/coordinator/unified/reconciler_model.rs` with a placeholder so the wiring compiles:

```rust
//! Exhaustive stateright model of the KIP-848 reconciliation core — built in
//! KRM-T2. See `docs/superpowers/specs/2026-06-14-crabka-kip848-reconciliation-model-design.md`.
```

Append to the **end** of `actor.rs` (outside the existing `mod tests`):

```rust
#[cfg(test)]
#[path = "reconciler_model.rs"]
mod reconciler_model;
```

- [ ] **Step 8: Build the broker test target**

Run: `cargo test -p crabka-broker --lib coordinator::unified -- --list 2>&1 | tail -3`
Expected: compiles; the actor + reconciler tests are listed; no errors.

- [ ] **Step 9: Format + clippy + commit**

```bash
cargo +nightly fmt -p crabka-broker
cargo +nightly fmt -p crabka-broker -- --check        # expect exit 0
cargo clippy -p crabka-broker --all-targets -- -D warnings   # expect exit 0
git add crates/broker/src/coordinator/unified/actor.rs \
        crates/broker/src/coordinator/unified/persistence_next_gen.rs \
        crates/broker/src/coordinator/unified/reconciler_model.rs
git commit -m "refactor(broker): extract pure step_heartbeat + wire KIP-848 reconciliation model module"
```

---

## Task KRM-T2: The model + faithful-client environment + headline property

**Files:**
- Modify: `crates/broker/src/coordinator/unified/reconciler_model.rs` (replace the stub with the full model)

The model reconstructs a real `GroupState` from a hashable projection each step, drives the real `step_heartbeat` for membership/heartbeat actions, and mutates a ground-truth `client_owned` ledger for the faithful-client actions. Single topic, so each member's partition map collapses to a sorted `Vec<i32>`.

- [ ] **Step 1: Write the full model**

Replace the contents of `reconciler_model.rs` with:

```rust
//! Exhaustive stateright model of the KIP-848 reconciliation core.
//!
//! The model drives the real `step_heartbeat` (membership + heartbeat actions)
//! and a faithful-client environment (revoke-before-add, trust-the-coordinator)
//! to check the headline KIP-848 safety property: no two members ever
//! simultaneously own the same partition. Design:
//! `docs/superpowers/specs/2026-06-14-crabka-kip848-reconciliation-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use crabka_protocol::owned::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequest, TopicPartitions,
};
use crabka_protocol::primitives::uuid::Uuid;
use stateright::{Checker, Model, Property};

use super::super::config::NextGenConfig;
use super::super::consumer_state::GroupState;
use super::super::persistence_next_gen::MemberAssignmentState;
use super::super::reconciler::ReconcileInput;
use super::{step_heartbeat, MetadataProvider};

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
    assigned: Vec<i32>,           // coordinator view (sorted)
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
    let mut v: Vec<i32> = map
        .and_then(|m| m.get(&TOPIC))
        .cloned()
        .unwrap_or_default();
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
        let ms = super::super::consumer_state::MemberState {
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

/// Project a real `GroupState` + the client ledger back into the hashable state.
fn project(g: &GroupState, owned: &BTreeMap<String, BTreeSet<i32>>) -> ReconState {
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
    ReconState {
        group_epoch: g.group_epoch,
        dirty: g.dirty,
        target_epoch: g.target.epoch,
        members,
        client_owned,
    }
}

fn owned_map(s: &ReconState) -> BTreeMap<String, BTreeSet<i32>> {
    s.client_owned
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
        .collect()
}

fn member<'a>(s: &'a ReconState, id: &str) -> Option<&'a MemberProj> {
    s.members.iter().find(|m| m.id == id)
}

fn hb_request(member_id: &str, member_epoch: i32, owned: &BTreeSet<i32>) -> ConsumerGroupHeartbeatRequest {
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
            // Faithful-client moves don't bump the group epoch → always allowed.
            let owned: BTreeSet<i32> = state
                .client_owned
                .iter()
                .find(|(k, _)| k == &m.id)
                .map(|(_, v)| v.iter().copied().collect())
                .unwrap_or_default();
            for &tp in &m.target {
                if !owned.contains(&tp) {
                    actions.push(ReconAction::ClientAdd(m.id.clone(), tp));
                }
            }
            for &tp in &owned {
                if !m.target.contains(&tp) {
                    actions.push(ReconAction::ClientRevoke(m.id.clone(), tp));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut owned = owned_map(last);
        match action {
            ReconAction::ClientAdd(id, tp) => {
                let target_has = member(last, &id).is_some_and(|m| m.target.contains(&tp));
                let entry = owned.entry(id).or_default();
                if !target_has || entry.contains(&tp) {
                    return None;
                }
                entry.insert(tp);
                // No coordinator call: member projection unchanged, ledger updated.
                let mut next = last.clone();
                next.client_owned = owned
                    .iter()
                    .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
                    .collect();
                Some(next)
            }
            ReconAction::ClientRevoke(id, tp) => {
                let target_has = member(last, &id).is_some_and(|m| m.target.contains(&tp));
                let entry = owned.entry(id).or_default();
                if target_has || !entry.contains(&tp) {
                    return None;
                }
                entry.remove(&tp);
                let mut next = last.clone();
                next.client_owned = owned
                    .iter()
                    .map(|(k, v)| (k.clone(), v.iter().copied().collect()))
                    .collect();
                Some(next)
            }
            ReconAction::Join(id) => {
                if member(last, &id).is_some() {
                    return None;
                }
                let mut g = rebuild_group(last);
                let req = hb_request(&id, 0, &BTreeSet::new());
                let _ = step_heartbeat(&mut g, &config(), &self.metadata(), &req, "", Instant::now());
                assert_epoch_monotonic(last, &g);
                owned.entry(id).or_default(); // new member owns nothing yet
                Some(project(&g, &owned))
            }
            ReconAction::Leave(id) => {
                let Some(m) = member(last, &id) else { return None };
                let mut g = rebuild_group(last);
                let req = hb_request(&id, m.member_epoch.max(-1), &BTreeSet::new());
                // Force the leave sentinel regardless of stored epoch.
                let req = ConsumerGroupHeartbeatRequest { member_epoch: -1, ..req };
                let _ = step_heartbeat(&mut g, &config(), &self.metadata(), &req, "", Instant::now());
                assert_epoch_monotonic(last, &g);
                owned.remove(&id);
                Some(project(&g, &owned))
            }
            ReconAction::Heartbeat(id) => {
                let Some(m) = member(last, &id) else { return None };
                let cur_owned: BTreeSet<i32> = owned.get(&id).cloned().unwrap_or_default();
                let mut g = rebuild_group(last);
                let req = hb_request(&id, m.member_epoch, &cur_owned);
                let _ = step_heartbeat(&mut g, &config(), &self.metadata(), &req, "", Instant::now());
                assert_epoch_monotonic(last, &g);
                Some(project(&g, &owned))
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
            // Non-vacuity: a handoff state is reachable (some partition's target
            // owner differs from its current owner).
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
    // 2 members, 1 topic, 2 partitions: the minimal handoff scenario.
    run(ReconModel::basic(), "recon_basic");
}
```

- [ ] **Step 2: Format + clippy**

```bash
cargo +nightly fmt -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings   # expect exit 0 (fix any lint, e.g. needless clones)
```

- [ ] **Step 3: Run the model UNDER THE WATCHDOG**

Build the test binary, then run the single test through the host memory watchdog (do NOT run unguarded — see `[[feedback_bound_model_checkers]]`):

```powershell
# Build the broker lib-test exe without running it, capture its path:
$exe = cargo test -p crabka-broker --lib --no-run --message-format=json 2>$null |
  ConvertFrom-Json | Where-Object { $_.profile.test -eq $true -and $_.target.name -eq 'crabka_broker' } |
  Select-Object -Last 1 -ExpandProperty executable
# Run just recon_basic under the watchdog (Invoke-GuardedExe: poll WorkingSet64,
# kill on >3GB or >150s). Use the established helper.
Invoke-GuardedExe -Exe $exe -ArgList @('reconciler_model::recon_basic','--exact','--nocapture') -MaxRamGB 3 -MaxSeconds 150
```

Expected output line: `[recon_basic] unique_states=… generated=… max_depth=…`.

- [ ] **Step 4: DECISION GATE — record GREEN or RED**

- **GREEN** (`test result: ok`): the model proved `no_double_ownership` exhaustively. Proceed to **KRM-T3 (GREEN path)**.
- **RED** (`assertion failed` / a property counterexample / an `assert_epoch_monotonic` panic): stateright prints a minimal action trace. Capture it verbatim. Proceed to **KRM-T3 (RED path)**.

Per the spec's analysis (UniformAssignor is non-sticky, so adding member `b` immediately moves a partition off `a`; `build_assignment_resp` hands `b` its full target with no withholding), RED is the expected outcome and indicates a real KIP-848 conformance gap. Do not treat RED as a model bug without first checking the client-model fidelity (Step 5).

- [ ] **Step 5: If RED — sanity-check client-model fidelity before declaring a bug**

Confirm the counterexample is a real gap, not an over-permissive client:
- The violating `ClientAdd(b, p)` must have been enabled only because `p ∈ target[b]` — i.e. the **coordinator** put `p` in `b`'s target/response while `a` still owned it. Verify in the trace that `a` had not yet revoked `p` (no `ClientRevoke(a, p)` precedes the violation) and that `a` still lists `p` in `client_owned`.
- Cross-check intent: read `build_assignment_resp` (`actor.rs:1263`) — it returns `state.target.per_member[member]` with no "withhold partitions still owned elsewhere" gate. Canonical KIP-848 returns the member's *current* (reconciled) assignment, withholding a partition until its previous owner revokes.

If both hold, it is a genuine conformance gap. Record the minimal trace and proceed to KRM-T3 (RED path).

- [ ] **Step 6: Commit the model (regardless of green/red)**

If GREEN, commit normally. If RED, the model test currently fails — commit it as `#[ignore]` with a doc comment pointing at the counterexample, so the committed tree builds green and the failing model is preserved for KRM-T3:

```bash
# GREEN:
git add crates/broker/src/coordinator/unified/reconciler_model.rs
git commit -m "test(broker): stateright model of KIP-848 reconciliation (no-double-ownership)"
# RED (mark #[ignore] with a FIXME referencing the trace first):
git add crates/broker/src/coordinator/unified/reconciler_model.rs
git commit -m "test(broker): KIP-848 reconciliation model surfaces a cross-member handoff gap"
```

---

## Task KRM-T3: Resolve the outcome

### GREEN path — scale up + finalize

**Files:** Modify `reconciler_model.rs`.

- [ ] **Step G1: Add a `wide` config**

Add to `impl ReconModel`:

```rust
fn wide() -> Self {
    Self {
        pool: vec!["a", "b", "c"],
        partitions: 3,
        max_epoch: 8,
    }
}
```

And a test:

```rust
#[test]
fn recon_wide() {
    // 3 members, 3 partitions: more handoff interleavings.
    run(ReconModel::wide(), "recon_wide");
}
```

- [ ] **Step G2: Run `recon_wide` under the watchdog** (same `Invoke-GuardedExe` recipe, test name `reconciler_model::recon_wide`). If it exceeds ~100k states or the watchdog kills it, reduce to `partitions: 2` or `pool: vec!["a","b","c"]` with `partitions: 2`, and record the bound in a comment. Keep the config only if exhaustive (`state_count < MAX_STATES`, `max_depth < MAX_DEPTH`).

- [ ] **Step G3: Confirm both configs green + the broader suite**

```
cargo test -p crabka-broker --lib coordinator::unified::actor   # actor refactor still green
```
(Run the two model tests via the watchdog, not bare cargo.)

- [ ] **Step G4: fmt + clippy + commit + update memory**

```bash
cargo +nightly fmt -p crabka-broker && cargo +nightly fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/coordinator/unified/reconciler_model.rs
git commit -m "test(broker): add wide KIP-848 reconciliation config + final verification"
```
Then update `project_stateright_testing_program.md` to record the reconciliation model as implemented and GREEN (Workstream A now also covers KIP-848 rebalance), and document *why* the simpler full-target response preserves disjointness.

### RED path — confirm, present the fix scope, then (with user OK) fix + re-verify

A genuine counterexample means Crabka's coordinator omits KIP-848's cross-member withholding. The fix (return the member's reconciled *current* assignment, withholding a partition until its previous owner revokes) is a **real protocol change** that will alter `build_assignment_resp` and likely several existing actor tests that assert the response equals the target. Because that is larger than a mechanical fix, **pause and report to the user before implementing**, presenting the minimal trace and the fix options. This honors the spec's "extends to a coordinator fix + re-verify" while not bulldozing a protocol change unreviewed.

- [ ] **Step R1: Minimize + document the counterexample**

From the stateright trace, write the shortest action sequence reproducing the double-ownership (expected: `Join(a) → ClientAdd(a,0) → ClientAdd(a,1) → Heartbeat(a) → Join(b) → ClientAdd(b,1)` with `a` still owning `1`). Add it as an `#[ignore]`d regression test `recon_basic_counterexample` with a doc comment, and capture the trace in the commit message / a short note.

- [ ] **Step R2: Present the finding + fix scope to the user (STOP)**

Report to the user: the minimal trace, the confirmation that `build_assignment_resp` returns the raw target with no withholding, the conformance implication (a stock consumer double-consumes during rebalance), and the fix options:
1. **Implement KIP-848 withholding** (`CurrentAssignmentBuilder`-equivalent): in the response path, compute each member's *current* assignment = `target ∩ (partitions not still owned/pending by any other member)`; only grow a member's returned assignment as other members revoke. Extract it as a pure `current_assignment_for(member, group)` function the model also drives, then re-run the model to GREEN. Update the existing actor tests that assert response==target.
2. **File as a known gap** (model committed `#[ignore]`d with the trace) and defer the fix to its own slice.

Wait for the user's decision. Do not implement option 1 without explicit approval — it is a protocol change touching the hot heartbeat path.

- [ ] **Step R3: (Only if user approves the fix)** Implement `current_assignment_for`, wire it into `build_assignment_resp`, un-`#[ignore]` the model, re-run both configs under the watchdog to GREEN, fix affected actor tests, then fmt + clippy + commit and update the memory note. (Detailed code for this step is deferred until the trace confirms the exact withholding condition needed — writing it now would be speculative; R2 surfaces the real shape.)

---

## Self-Review

**Spec coverage:**
- Headline `no_double_ownership` → KRM-T2 Step 1 (`Property::always`). ✓
- `member_epoch_monotonic` → `assert_epoch_monotonic` in `next_state`. ✓
- `handoff_witness` / `converged_witness` non-vacuity → KRM-T2 properties. ✓
- Faithful client (revoke-before-add, no cross-member check) → `ClientAdd`/`ClientRevoke` actions. ✓
- Wrap-real via `step_heartbeat` extraction → KRM-T1. ✓
- Two-outcome plan (green finalize / red fix) → KRM-T3 both paths. ✓
- Bounding + watchdog + caps → `within_boundary`, `target_state_count`, `timeout`, `Invoke-GuardedExe`. ✓
- `ownership_entitled` (spec supporting property) → **intentionally dropped**: the coordinator's `pending_revocation` lags client `ClientAdd`/`Revoke` (it is recomputed only on reconcile against the last-heartbeated `assigned_partitions`), so `client_owned ⊆ target ∪ pending` would false-positive during a legitimate handoff window. The headline `no_double_ownership` is the property that matters; this divergence from the spec is a deliberate fidelity call, noted here.

**Placeholder scan:** The only deferred code is KRM-T3 RED Step R3, which is *correctly* deferred — its exact form depends on the counterexample the model produces, and writing it speculatively would violate "no placeholders that are actually guesses." R1/R2 are concrete.

**Type consistency:** `ReconState`/`MemberProj`/`ReconAction` field and variant names are consistent across `actions`/`next_state`/`properties`. `step_heartbeat` / `HeartbeatStep` / `leave_step` signatures match between T1 (definition) and T2 (call sites). `TopicPartitions` request type path (`crabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions`) matches the generated `to_owned` target. `ModelMetadata` implements the real `MetadataProvider` trait. ✓
