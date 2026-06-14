# KIP-227 Fetch-Session Forget+Merge Composition Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extract the forget+merge partition-map mutation from `FetchSessionCache::classify` into a pure `apply_incremental`, and verify the `forget`(OR-match) + `merge`(find-by-either-half) **composition** under topic-rename / identity-churn sequences — wrap-real stateright model (the cache is stateful) + proptest. Headline: **no-shadow** (no two cached keys for one logical partition) + subscription fidelity. Capstone slice.

**Architecture:** `apply_incremental(&mut partitions, forgotten, topics)` holds the forget `retain` + merge find-or-insert (current lines 271-318); `classify` calls it. A stateright model drives it over sequences of incremental fetches with varying identity halves, starting from a fully-resolved session. Likely GREEN-by-construction (immutable keys + merge insert-guard) — a RED would be a real cache-corruption bug.

**Tech Stack:** Rust, `stateright` 0.31 + `proptest` (`crabka-broker` dev-deps).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-fetch-session-forget-merge-design.md`

**Verification discipline:** stateright watchdog-guarded (3 GB / 150 s; `[[feedback_bound_model_checkers]]`); proptest bounded. `cargo +nightly fmt -p crabka-broker`; clippy `-D warnings`; backtick doc identifiers.

---

## File Structure

- `crates/broker/src/fetch_session.rs` — **modify**: extract `apply_incremental`; `classify` calls it; wire the model module; add a `proptest` module.
- `crates/broker/src/fetch_session_model.rs` — **create**: stateright model (`#[cfg(test)]` descendant).

Batches: **B1** {FSM-A: extract + model} · **B2** {FSM-B: proptest} (sequential; both in the new file / fetch_session.rs).

---

## Task FSM-A: extract `apply_incremental` + stateright model

**Files:** modify `fetch_session.rs`; create `fetch_session_model.rs`.

- [ ] **Step 1: Extract `apply_incremental`** (behavior-preserving)

In `fetch_session.rs`, add a module-level `pub(crate)` fn holding the forget+merge block (current lines 271-318):

```rust
use crabka_protocol::owned::fetch_request::{FetchTopic, ForgottenTopic};

/// Pure core of the incremental-fetch session update: drop forgotten partitions
/// (a `ForgottenTopic` matches a cached key by name-OR-id + partition) then merge
/// the requested topics (find a cached key by either-half identity + partition and
/// update its desired state in place, else insert a new default-state key). The
/// asymmetry between the OR-match forget and the either-half-match merge is what
/// this slice's model exercises (see `fetch_session_model.rs`).
pub(crate) fn apply_incremental(
    partitions: &mut HashMap<FetchSessionKey, CachedPartitionState>,
    forgotten: &[ForgottenTopic],
    topics: &[FetchTopic],
) {
    for ft in forgotten {
        partitions.retain(|k, _| {
            let topic_match = (!ft.topic.is_empty() && k.topic_name == ft.topic)
                || (ft.topic_id != WireUuid::ZERO && k.topic_id == ft.topic_id);
            if !topic_match {
                return true;
            }
            !ft.partitions.contains(&k.partition)
        });
    }
    for t in topics {
        for fp in &t.partitions {
            let existing_key = partitions
                .keys()
                .find(|k| {
                    k.partition == fp.partition
                        && ((!t.topic.is_empty() && k.topic_name == t.topic)
                            || (t.topic_id != WireUuid::ZERO && k.topic_id == t.topic_id))
                })
                .cloned();
            let key = existing_key.unwrap_or_else(|| FetchSessionKey {
                topic_name: t.topic.clone(),
                topic_id: t.topic_id,
                partition: fp.partition,
            });
            let entry = partitions.entry(key).or_default();
            entry.fetch_offset = fp.fetch_offset;
            entry.max_bytes = fp.partition_max_bytes;
            entry.current_leader_epoch = fp.current_leader_epoch;
            entry.last_fetched_epoch = fp.last_fetched_epoch;
            entry.log_start_offset = fp.log_start_offset;
        }
    }
}
```

Replace the inlined block in `classify` (between the `partitions_before` and `partitions_after` snapshots) with a single call:
```rust
        apply_incremental(&mut session.partitions, &req.forgotten_topics_data, &req.topics);
```
Keep the `num_partitions` gauge bookkeeping in `classify`.

- [ ] **Step 2: Verify behavior-preserving**

`cargo test -p crabka-broker --lib fetch_session` and `cargo test -p crabka-broker --test fetch_session` (the unit + integration tests, incl. `incremental_merge_matches_cached_key_by_topic_id_only` / `_by_topic_name_only`). Expected: all pass unchanged.

- [ ] **Step 3: Wire the model module** — append to `fetch_session.rs`:
```rust
#[cfg(test)]
#[path = "fetch_session_model.rs"]
mod fetch_session_model;
```

- [ ] **Step 4: Write the model** — create `fetch_session_model.rs`:

```rust
//! Exhaustive stateright enumeration of the KIP-227 forget+merge composition
//! (`super::apply_incremental`). The session-cache partition map is stateful, so
//! the model drives the REAL forget+merge over sequences of incremental fetches
//! with varying identity halves (name-only / id-only / both, incl. the
//! topic-rename cycle), starting from a fully-resolved session, and asserts no
//! shadow entry (two cached keys for one logical partition) + subscription
//! fidelity. See the design spec.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use stateright::{Checker, Model, Property};

use super::{apply_incremental, CachedPartitionState, FetchSessionKey};

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 500_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

// Tiny symbolic universe. Names {A,B}, ids {U,V} (a rename = same id, new name),
// one partition. WireUuid is [u8;16]; use two distinct non-zero ids.
const NAME_A: &str = "A";
const NAME_B: &str = "B";
fn uid_u() -> WireUuid { WireUuid([1u8; 16]) }
fn uid_v() -> WireUuid { WireUuid([2u8; 16]) }

/// A client topic reference: which identity halves it carries.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Ref {
    Both(&'static str, u8),  // (name, id-tag 1=U/2=V)
    NameOnly(&'static str),
    IdOnly(u8),
}
fn id_of(tag: u8) -> WireUuid { if tag == 1 { uid_u() } else { uid_v() } }
fn ref_name(r: Ref) -> String {
    match r { Ref::Both(n, _) | Ref::NameOnly(n) => n.to_string(), Ref::IdOnly(_) => String::new() }
}
fn ref_id(r: Ref) -> WireUuid {
    match r { Ref::Both(_, t) | Ref::IdOnly(t) => id_of(t), Ref::NameOnly(_) => WireUuid::ZERO }
}

#[derive(Clone, Debug)]
struct CacheState {
    partitions: HashMap<FetchSessionKey, CachedPartitionState>,
}
impl CacheState {
    fn proj(&self) -> Vec<(String, [u8; 16], i32, i32)> {
        let mut v: Vec<_> = self
            .partitions
            .iter()
            .map(|(k, s)| (k.topic_name.clone(), k.topic_id.0, k.partition, s.max_bytes))
            .collect();
        v.sort();
        v
    }
}
impl PartialEq for CacheState { fn eq(&self, o: &Self) -> bool { self.proj() == o.proj() } }
impl Eq for CacheState {}
impl Hash for CacheState { fn hash<H: Hasher>(&self, h: &mut H) { self.proj().hash(h); } }

/// One incremental fetch: optionally forget one ref, optionally subscribe one ref.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Fetch { forget: Option<Ref>, sub: Option<(Ref, i32)> }

struct FsModel {
    refs: Vec<Ref>,
    partition: i32,
}

fn forgotten_topic(r: Ref, p: i32) -> ForgottenTopic {
    ForgottenTopic { topic: ref_name(r), topic_id: ref_id(r), partitions: vec![p], ..Default::default() }
}
fn fetch_topic(r: Ref, p: i32, mb: i32) -> FetchTopic {
    FetchTopic {
        topic: ref_name(r),
        topic_id: ref_id(r),
        partitions: vec![FetchPartition { partition: p, partition_max_bytes: mb, ..Default::default() }],
        ..Default::default()
    }
}

impl Model for FsModel {
    type State = CacheState;
    type Action = Fetch;

    fn init_states(&self) -> Vec<Self::State> {
        // Seed: a fully-resolved session — one key with BOTH halves (name A, id U).
        let mut partitions = HashMap::new();
        partitions.insert(
            FetchSessionKey { topic_name: NAME_A.to_string(), topic_id: uid_u(), partition: self.partition },
            CachedPartitionState { max_bytes: 9, ..Default::default() },
        );
        vec![CacheState { partitions }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        let forgets = std::iter::once(None).chain(self.refs.iter().map(|r| Some(*r)));
        for f in forgets {
            actions.push(Fetch { forget: f, sub: None });
            for &r in &self.refs {
                for mb in [1, 2] {
                    actions.push(Fetch { forget: f, sub: Some((r, mb)) });
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        let forgotten: Vec<ForgottenTopic> =
            a.forget.into_iter().map(|r| forgotten_topic(r, self.partition)).collect();
        let topics: Vec<FetchTopic> =
            a.sub.into_iter().map(|(r, mb)| fetch_topic(r, self.partition, mb)).collect();
        apply_incremental(&mut s.partitions, &forgotten, &topics);
        assert!(no_shadow(&s.partitions), "shadow entry: {:?}", s.proj());
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_shadow", |_, s: &CacheState| no_shadow(&s.partitions)),
            // No key carries default state (max_bytes==0) — merge always sets it.
            Property::always("no_orphan_default", |_, s: &CacheState| {
                s.partitions.values().all(|v| v.max_bytes != 0)
            }),
            Property::sometimes("renamed", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name == NAME_B)
            }),
            Property::sometimes("empty", |_, s: &CacheState| s.partitions.is_empty()),
            Property::sometimes("id_only_key", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name.is_empty())
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.partitions.len() <= 4
    }
}

/// No two distinct keys refer to the same logical partition: same partition AND
/// (same non-empty name OR same non-zero id).
fn no_shadow(partitions: &HashMap<FetchSessionKey, CachedPartitionState>) -> bool {
    let keys: Vec<&FetchSessionKey> = partitions.keys().collect();
    for (i, a) in keys.iter().enumerate() {
        for b in &keys[i + 1..] {
            if a.partition == b.partition
                && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                    || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id))
            {
                return false;
            }
        }
    }
    true
}

fn run(model: FsModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(), checker.state_count(), checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(checker.state_count() < TARGET_STATE_COUNT, "[{label}] truncated — not exhaustive");
    assert!(checker.unique_state_count() < MAX_UNIQUE_STATES, "[{label}] unique bound exceeded ({})", checker.unique_state_count());
    checker.assert_properties();
}

#[test]
fn fetch_session_basic() {
    run(
        FsModel {
            refs: vec![Ref::Both(NAME_A, 1), Ref::NameOnly(NAME_A), Ref::IdOnly(1), Ref::Both(NAME_B, 1)],
            partition: 0,
        },
        "fetch_session_basic",
    );
}

#[test]
fn fetch_session_wide() {
    run(
        FsModel {
            refs: vec![
                Ref::Both(NAME_A, 1), Ref::NameOnly(NAME_A), Ref::IdOnly(1),
                Ref::Both(NAME_B, 1), Ref::NameOnly(NAME_B), Ref::Both(NAME_A, 2), Ref::IdOnly(2),
            ],
            partition: 0,
        },
        "fetch_session_wide",
    );
}
```

(`WireUuid` is a tuple-struct over `[u8; 16]`; confirm the constructor form — `WireUuid([..])` vs a `from_bytes` — at implementation time, and adjust `uid_*`/`proj` accordingly. `ForgottenTopic`/`FetchTopic`/`FetchPartition` have `Default` (FetchPartition's is at FetchRequest.owned.rs:553).)

- [ ] **Step 5: fmt + clippy + run under watchdog**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`; build `cargo test -p crabka-broker --lib fetch_session_model --no-run`. Controller runs `fetch_session_basic` + `fetch_session_wide` under the host watchdog. **RED handling**: if `no_shadow` / `no_orphan_default` fires, capture the forget+merge sequence and assess reachability via real Fetch RPCs — if real, fix `apply_incremental` (RED→GREEN, recording the counterexample); if unrealistic, constrain the model. If a config truncates, tune the ref universe / depth (unique-state-bound technique).

- [ ] **Step 6: Commit**
```bash
git add crates/broker/src/fetch_session.rs crates/broker/src/fetch_session_model.rs
git commit -m "test(broker): stateright model of KIP-227 fetch-session forget+merge composition"
```
(If a real bug is found+fixed, split `fix(broker):` + `test(broker):` RED→GREEN.)

---

## Task FSM-B: proptest fuzz at large N

**Files:** modify `fetch_session.rs`.

- [ ] **Step 1: Add the proptest** — append a `#[cfg(test)] mod fuzz` to `fetch_session.rs`:

```rust
#[cfg(test)]
mod fuzz {
    use std::collections::HashMap;
    use proptest::prelude::*;
    use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic};
    use crabka_protocol::primitives::uuid::Uuid as WireUuid;
    use super::{apply_incremental, CachedPartitionState, FetchSessionKey};

    // Op: (forget?: Option<(name_idx, id_idx, p)>, sub?: Option<(name_idx, id_idx, p, mb)>)
    // name_idx 0=empty,1=A,2=B ; id_idx 0=zero,1=U,2=V (never both empty/zero).
    fn name_of(i: u8) -> String { ["", "A", "B"][i as usize].to_string() }
    fn id_of(i: u8) -> WireUuid { [WireUuid::ZERO, WireUuid([1;16]), WireUuid([2;16])][i as usize] }

    proptest! {
        #[test]
        fn forget_merge_invariants(
            ops in proptest::collection::vec(
                (0u8..3, 0u8..3, 0u8..2, any::<bool>(), 0u8..3, 0u8..3, 0u8..2, 1i32..3),
                0..200,
            )
        ) {
            let mut partitions: HashMap<FetchSessionKey, CachedPartitionState> = HashMap::new();
            for (fn_, fi, fp, do_sub, sn, si, sp, mb) in ops {
                // forget (skip the all-empty identity)
                let forgotten = if fn_ == 0 && fi == 0 { vec![] } else {
                    vec![ForgottenTopic { topic: name_of(fn_), topic_id: id_of(fi), partitions: vec![fp as i32], ..Default::default() }]
                };
                let topics = if do_sub && !(sn == 0 && si == 0) {
                    vec![FetchTopic { topic: name_of(sn), topic_id: id_of(si),
                        partitions: vec![FetchPartition { partition: sp as i32, partition_max_bytes: mb, ..Default::default() }], ..Default::default() }]
                } else { vec![] };
                apply_incremental(&mut partitions, &forgotten, &topics);
                // no shadow: no two keys share a logical partition.
                let keys: Vec<_> = partitions.keys().cloned().collect();
                for (i, a) in keys.iter().enumerate() {
                    for b in &keys[i+1..] {
                        let shadow = a.partition == b.partition
                            && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                                || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id));
                        prop_assert!(!shadow, "shadow: {a:?} vs {b:?}");
                    }
                }
                // a merge-created/updated entry always carries the request's max_bytes (>0 here)
                prop_assert!(partitions.values().all(|v| v.max_bytes != 0 || partitions.is_empty()));
            }
        }
    }
}
```

(Adjust `WireUuid` construction to the real constructor; the `name_of`/`id_of` index helpers keep the alphabet tiny. If the `no_orphan_default` assertion is awkward with a never-subscribed seed, drop it from the proptest — the model covers it.)

- [ ] **Step 2: Run + fmt + clippy + commit**
`cargo test -p crabka-broker --lib fetch_session::fuzz`; fmt; clippy. Then:
```bash
git add crates/broker/src/fetch_session.rs
git commit -m "test(broker): proptest fuzz of fetch-session forget+merge invariants at large N"
```

---

## Self-Review

**Spec coverage:** extract `apply_incremental` (FSM-A.1) ✓; behavior-preserving gate (A.2) ✓; wrap-real stateful model driving the real fn over fetch sequences (A.4) ✓; no-shadow + no-orphan-default + witnesses ✓; manual Hash/Eq projection (the map isn't Hash) ✓; proptest large-N (FSM-B) ✓; watchdog + RED-handling (A.5) ✓.

**Placeholder scan:** `apply_incremental` is the verbatim current logic (behavior-preserving). The model/proptest are complete; the `WireUuid` constructor form + bounds are confirmed/tuned at implementation (flagged). No hidden TODOs.

**Type consistency:** `apply_incremental(&mut HashMap<FetchSessionKey, CachedPartitionState>, &[ForgottenTopic], &[FetchTopic])` used in the extraction, `classify`, the model, and the proptest. `FetchSessionKey { topic_name, topic_id, partition }` / `CachedPartitionState { max_bytes, .. }` / `FetchPartition { partition, partition_max_bytes, .. }` field names match the real types.
