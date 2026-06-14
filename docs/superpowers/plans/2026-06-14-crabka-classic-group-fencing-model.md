# Classic Group-Coordinator Membership/Fencing Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap-real stateright model of the classic consumer-group membership state machine (KIP-345/62) proving static-index coherence + single-owner-per-instance + static-never-expired across all interleavings of join/leave/heartbeat/complete/sync/expire, plus a proptest at large N.

**Architecture:** The model holds a real `Group` (`#[derive(Clone)]` added) and drives its real `&mut self` transitions + the handler's KIP-345 fence pre-check. The model `State` newtypes the real `Group` + a logical `clock`, with a manual `Hash`/`Eq` over a canonical projection (the `HashMap`s aren't `Hash`). Time is a deterministic `epoch + clock*unit` (`Instant` is `Hash`) so expiry is exercised.

**Tech Stack:** Rust, `stateright` 0.31 + `proptest` (both `crabka-broker` dev-deps).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-classic-group-fencing-model-design.md`

**Verification discipline:** stateright runs watchdog-guarded (3 GB / 150 s — `[[feedback_bound_model_checkers]]`); proptest bounded. `cargo +nightly fmt` per-crate (`[[reference_windows_fmt_path_length]]`); clippy `-D warnings`; backtick doc-comment code identifiers.

---

## File Structure

- `crates/broker/src/coordinator/unified/classic_state.rs` — **modify**: add `#[derive(Clone)]` to `Group`; wire the model module.
- `crates/broker/src/coordinator/unified/classic_state_model.rs` — **create**: stateright model + proptest (`#[cfg(test)]` descendant of `classic_state`).

Batches: **B1** {Task CGM-A} · **B2** {Task CGM-B} (sequential; both in the new file).

---

## Task CGM-A: `#[derive(Clone)]` on `Group` + the stateright model

**Files:** modify `classic_state.rs`; create `classic_state_model.rs`.

- [ ] **Step 1: Add `Clone` to `Group`**

In `classic_state.rs`, change `#[derive(Debug)]` on `struct Group` to `#[derive(Debug, Clone)]`. (All fields are `Clone`: `HashMap`/`HashSet`, `Instant`, `Bytes`, `String`, the `Copy` enums.) Run `cargo test -p crabka-broker --lib classic_state` — existing tests pass (additive derive).

- [ ] **Step 2: Wire the model module** — append to `classic_state.rs`:

```rust
#[cfg(test)]
#[path = "classic_state_model.rs"]
mod classic_state_model;
```

- [ ] **Step 3: Write the model** — create `classic_state_model.rs`:

```rust
//! Exhaustive stateright enumeration of the classic consumer-group membership
//! state machine (KIP-345/62), wrapping the real `super::Group`. Drives the real
//! `add_member` / `remove_member` / `complete_rebalance` / `install_assignments`
//! / `expire_dead_members` + the handler's KIP-345 fence pre-check
//! (`current_member_id_for_instance`) under every interleaving of join (dynamic /
//! static / fenced), heartbeat, leave, rebalance completion, sync, and
//! session-timeout expiry. Asserts the static-index coherence + single-owner +
//! static-never-expired invariants (see the design spec).

use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use stateright::{Checker, Model, Property};

use super::{AddMemberOutcome, GroupState, Member};
use super::Group;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}
const UNIT: Duration = Duration::from_secs(1);
const SESSION: Duration = Duration::from_secs(2); // 2 * UNIT
fn at(clock: i64) -> Instant {
    epoch() + UNIT * u32::try_from(clock.max(0)).unwrap_or(0)
}

struct ClassicModel {
    members: Vec<&'static str>,   // member-id alphabet
    instances: Vec<&'static str>, // group.instance.id alphabet
    max_clock: i64,
}

/// Model state: a real `Group` plus the logical clock. Hash/Eq are manual over a
/// canonical projection because `Group` holds `HashMap`s.
#[derive(Clone, Debug)]
struct GrpState {
    g: Group,
    clock: i64,
}

type Proj = (
    GroupState,
    i32,                                          // generation_id
    Option<String>,                               // leader_id
    Option<String>,                               // protocol_name
    bool,                                         // rebalance_from_empty
    i64,                                          // clock
    Vec<(String, Option<String>, bool, Instant)>, // members: (id, iid, has_assignment, last_hb)
    Vec<(String, String)>,                        // static_members
    Vec<String>,                                  // joined_this_round
);

impl GrpState {
    fn proj(&self) -> Proj {
        let mut members: Vec<(String, Option<String>, bool, Instant)> = self
            .g
            .members
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    m.group_instance_id.clone(),
                    m.assignment.is_some(),
                    m.last_heartbeat,
                )
            })
            .collect();
        members.sort();
        let mut idx: Vec<(String, String)> = self
            .g
            .static_members
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        idx.sort();
        let mut joined: Vec<String> = self.g.joined_this_round.iter().cloned().collect();
        joined.sort();
        (
            self.g.state,
            self.g.generation_id,
            self.g.leader_id.clone(),
            self.g.protocol_name.clone(),
            self.g.rebalance_from_empty,
            self.clock,
            members,
            idx,
            joined,
        )
    }
}

impl PartialEq for GrpState {
    fn eq(&self, other: &Self) -> bool {
        self.proj() == other.proj()
    }
}
impl Eq for GrpState {}
impl Hash for GrpState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Act {
    JoinDynamic(&'static str),
    JoinStatic(&'static str, &'static str), // (instance_id, member_id)
    Heartbeat(&'static str),
    Leave(&'static str),
    CompleteRebalance,
    Sync,
    ExpireTick,
}

fn mk_member(mid: &str, iid: Option<&str>, clock: i64) -> Member {
    let mut m = Member::new(
        mid,
        "c",
        "h",
        SESSION,
        Duration::from_secs(10),
        vec![("range".to_string(), Bytes::new())],
    )
    .with_instance_id(iid.map(str::to_string));
    m.last_heartbeat = at(clock);
    m
}

impl Model for ClassicModel {
    type State = GrpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![GrpState {
            g: Group::new("g"),
            clock: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for &mid in &self.members {
            actions.push(Act::JoinDynamic(mid));
            actions.push(Act::Heartbeat(mid));
            actions.push(Act::Leave(mid));
            for &iid in &self.instances {
                actions.push(Act::JoinStatic(iid, mid));
            }
        }
        if matches!(s.g.state, GroupState::PreparingRebalance) && !s.g.members.is_empty() {
            actions.push(Act::CompleteRebalance);
        }
        if matches!(s.g.state, GroupState::CompletingRebalance) {
            actions.push(Act::Sync);
        }
        if s.clock < self.max_clock {
            actions.push(Act::ExpireTick);
        }
    }

    #[allow(clippy::too_many_lines)]
    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::JoinDynamic(mid) => {
                s.g.add_member(mk_member(mid, None, s.clock));
            }
            Act::JoinStatic(iid, mid) => {
                // Handler KIP-345 fence pre-check: a different live member pinned
                // to this instance id ⇒ FENCED, no state change.
                if let Some(pinned) = s.g.current_member_id_for_instance(iid)
                    && pinned != mid
                {
                    return None;
                }
                s.g.add_member(mk_member(mid, Some(iid), s.clock));
            }
            Act::Heartbeat(mid) => {
                if let Some(m) = s.g.members.get_mut(mid) {
                    m.last_heartbeat = at(s.clock);
                } else {
                    return None;
                }
            }
            Act::Leave(mid) => {
                if !s.g.members.contains_key(mid) {
                    return None;
                }
                s.g.remove_member(mid);
            }
            Act::CompleteRebalance => {
                if !matches!(s.g.state, GroupState::PreparingRebalance) || s.g.members.is_empty() {
                    return None;
                }
                s.g.complete_rebalance("range");
            }
            Act::Sync => {
                if !matches!(s.g.state, GroupState::CompletingRebalance) {
                    return None;
                }
                let assignments = s
                    .g
                    .members
                    .keys()
                    .map(|id| (id.clone(), Bytes::from_static(b"a")))
                    .collect();
                s.g.install_assignments(assignments);
            }
            Act::ExpireTick => {
                s.clock += 1;
                let dropped = s.g.expire_dead_members(at(s.clock));
                // static-never-expired: no dropped member was static.
                for id in &dropped {
                    assert!(
                        !last.g.members.get(id).is_some_and(Member::is_static),
                        "static member {id} was expired"
                    );
                }
            }
        }
        assert_invariants(&s);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("index_coherence", |_, s: &GrpState| index_coherent(&s.g)),
            Property::always("single_owner_per_instance", |_, s: &GrpState| {
                single_owner(&s.g)
            }),
            Property::always("joined_subset", |_, s: &GrpState| {
                s.g.joined_this_round
                    .iter()
                    .all(|id| s.g.members.contains_key(id))
            }),
            Property::always("empty_iff_empty_state", |_, s: &GrpState| {
                s.g.members.is_empty() == matches!(s.g.state, GroupState::Empty)
            }),
            Property::always("leader_in_members", |_, s: &GrpState| {
                s.g.leader_id
                    .as_ref()
                    .is_none_or(|l| s.g.members.contains_key(l))
            }),
            // Non-vacuity witnesses.
            Property::sometimes("reached_stable", |_, s: &GrpState| {
                matches!(s.g.state, GroupState::Stable)
            }),
            Property::sometimes("instance_pinned", |_, s: &GrpState| {
                !s.g.static_members.is_empty()
            }),
            Property::sometimes("two_members", |_, s: &GrpState| s.g.members.len() >= 2),
            Property::sometimes("generation_bumped", |_, s: &GrpState| s.g.generation_id >= 1),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.clock <= self.max_clock && s.g.members.len() <= self.members.len()
    }
}

fn index_coherent(g: &Group) -> bool {
    // Every index entry points at a live member with the matching instance id.
    for (iid, mid) in &g.static_members {
        match g.members.get(mid) {
            Some(m) if m.group_instance_id.as_deref() == Some(iid.as_str()) => {}
            _ => return false,
        }
    }
    // Every static member has a matching index entry.
    for (mid, m) in &g.members {
        if let Some(iid) = &m.group_instance_id
            && g.static_members.get(iid).map(String::as_str) != Some(mid.as_str())
        {
            return false;
        }
    }
    true
}

fn single_owner(g: &Group) -> bool {
    let mut seen = std::collections::HashSet::new();
    for m in g.members.values() {
        if let Some(iid) = &m.group_instance_id
            && !seen.insert(iid.clone())
        {
            return false; // two live members share an instance id
        }
    }
    true
}

fn assert_invariants(s: &GrpState) {
    assert!(index_coherent(&s.g), "index coherence violated: {:?}", s.g);
    assert!(single_owner(&s.g), "single-owner violated: {:?}", s.g);
}

fn run(model: ClassicModel, label: &str) {
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
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(checker.state_count() < MAX_STATES, "[{label}] state cap hit");
    checker.assert_properties();
}

#[test]
fn classic_basic() {
    run(
        ClassicModel {
            members: vec!["a", "b"],
            instances: vec!["x"],
            max_clock: 4,
        },
        "classic_basic",
    );
}

#[test]
fn classic_wide() {
    run(
        ClassicModel {
            members: vec!["a", "b", "c"],
            instances: vec!["x", "y"],
            max_clock: 5,
        },
        "classic_wide",
    );
}
```

- [ ] **Step 4: fmt + clippy + run under watchdog**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings` (fix `doc_markdown` / `is_none_or` MSRV / borrow lints as needed); build `cargo test -p crabka-broker --lib classic_state_model --no-run`. The CONTROLLER runs `classic_basic` + `classic_wide` under the host memory watchdog (poll `WorkingSet64`, kill > 3 GB / > 150 s).

**Handling a RED result** (e.g. `index_coherence` fires on the `JoinDynamic`-over-a-static-`member_id` case): investigate reachability exactly as prior slices did — (a) if a real RPC can drive it (a client can supply an arbitrary `member_id`), it's a genuine bug: fix the production guard (e.g. `add_member`/handler must reject or cleanly re-key a dynamic join onto a static member's id) and re-verify GREEN, recording the counterexample; (b) if it's an unrealistic action the handler already prevents, constrain the model's action generator (partition the alphabet so a `member_id` is consistently dynamic-or-static) and document why. Scale `classic_wide` while exhaustive; tune the clock/alphabet (and apply the unique-state-bound technique) if a config truncates.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/coordinator/unified/classic_state.rs crates/broker/src/coordinator/unified/classic_state_model.rs
git commit -m "test(broker): stateright model of classic group-coordinator membership/fencing (KIP-345)"
```
(If a real bug was found+fixed, split into a `fix(broker):` commit + the `test(broker):` model commit, RED→GREEN, as in #521/#528/#531.)

---

## Task CGM-B: proptest fuzz at large N

**Files:** modify `classic_state_model.rs`.

- [ ] **Step 1: Add the proptest** — append a `#[cfg(test)] mod fuzz` to `classic_state_model.rs`:

```rust
#[cfg(test)]
mod fuzz {
    use proptest::prelude::*;

    use super::{at, index_coherent, mk_member, single_owner};
    use crate::coordinator::unified::{GroupState, Group, Member};

    #[derive(Clone, Debug)]
    enum Op {
        JoinDynamic(u8),
        JoinStatic(u8, u8),
        Heartbeat(u8),
        Leave(u8),
        Complete,
        Sync,
        Expire,
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u8..3).prop_map(Op::JoinDynamic),
            (0u8..2, 0u8..3).prop_map(|(i, m)| Op::JoinStatic(i, m)),
            (0u8..3).prop_map(Op::Heartbeat),
            (0u8..3).prop_map(Op::Leave),
            Just(Op::Complete),
            Just(Op::Sync),
            Just(Op::Expire),
        ]
    }

    proptest! {
        /// Large-N random op sequences over a real `Group`: the membership
        /// invariants hold after every step.
        #[test]
        fn classic_invariants_hold(ops in proptest::collection::vec(op_strategy(), 0..300)) {
            let mids = ["a", "b", "c"];
            let iids = ["x", "y"];
            let mut g = Group::new("g");
            let mut clock: i64 = 0;
            for op in ops {
                match op {
                    Op::JoinDynamic(m) => { g.add_member(mk_member(mids[m as usize], None, clock)); }
                    Op::JoinStatic(i, m) => {
                        let iid = iids[i as usize];
                        let mid = mids[m as usize];
                        let fenced = g.current_member_id_for_instance(iid).is_some_and(|p| p != mid);
                        if !fenced { g.add_member(mk_member(mid, Some(iid), clock)); }
                    }
                    Op::Heartbeat(m) => { if let Some(mm) = g.members.get_mut(mids[m as usize]) { mm.last_heartbeat = at(clock); } }
                    Op::Leave(m) => { g.remove_member(mids[m as usize]); }
                    Op::Complete => { if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty() { g.complete_rebalance("range"); } }
                    Op::Sync => { if matches!(g.state, GroupState::CompletingRebalance) { let a = g.members.keys().map(|k| (k.clone(), bytes::Bytes::from_static(b"a"))).collect(); g.install_assignments(a); } }
                    Op::Expire => { clock += 1; let dropped = g.expire_dead_members(at(clock)); for id in &dropped { prop_assert!(!g.members.get(id).is_some_and(Member::is_static)); } }
                }
                prop_assert!(index_coherent(&g), "index coherence");
                prop_assert!(single_owner(&g), "single owner");
                prop_assert!(g.joined_this_round.iter().all(|id| g.members.contains_key(id)), "joined subset");
                prop_assert_eq!(g.members.is_empty(), matches!(g.state, GroupState::Empty), "empty iff Empty");
            }
        }
    }
}
```

(Adjust the `super::`/`crate::` import paths to whatever resolves — `mk_member`/`index_coherent`/`single_owner` are in the parent model module; `Group`/`Member`/`GroupState` re-exported from `unified`. If the dropped-static check duplicates the model, keep it — it's cheap.)

- [ ] **Step 2: Run + fmt + clippy + commit**

`cargo test -p crabka-broker --lib classic_state_model::fuzz`; `cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`. Then:
```bash
git add crates/broker/src/coordinator/unified/classic_state_model.rs
git commit -m "test(broker): proptest fuzz of classic group membership invariants at large N"
```

---

## Self-Review

**Spec coverage:** `#[derive(Clone)]` on `Group` (CGM-A.1) ✓; wrap-real model driving real transitions + fence pre-check (CGM-A.3) ✓; manual `Hash`/`Eq` projection (CGM-A.3 `GrpState::proj`) ✓; deterministic `epoch+clock*unit` time + expiry (`at`, `ExpireTick`) ✓; index-coherence / single-owner / joined-subset / static-never-expired / empty⟺Empty / leader-in-members + monotone-generation-via-`complete_rebalance` ✓; non-vacuity witnesses ✓; proptest large-N (CGM-B) ✓; watchdog discipline + RED-handling (CGM-A.4) ✓.

**Placeholder scan:** the model + proptest are complete code; bounds + import-path nits + a possible RED are resolved at the run step (CGM-A.4) as in every prior model slice. No hidden TODOs.

**Type consistency:** `Group` real methods (`add_member`/`remove_member`/`complete_rebalance`/`install_assignments`/`expire_dead_members`/`current_member_id_for_instance`) called with their real signatures; `AddMemberOutcome`/`GroupState`/`Member` from `super`; `mk_member`/`index_coherent`/`single_owner`/`at` shared between the model and the proptest module. `GrpState::proj` projects exactly the fingerprinted fields (excludes the always-`None` `rebalance_deadline`/`protocol_type` and the constant `session_timeout`).
