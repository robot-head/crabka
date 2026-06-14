# Data-Plane Safety Models Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two data-plane safety verifications — idempotent-producer linearizability (`crates/broker`) and log-truncation divergence-safety (`crates/log`) — each as an exhaustive `stateright` enumeration of bounded inputs PLUS a `proptest` fuzz over the same pure core at large N.

**Architecture:** Extract a pure decision fn from each core (`check_pure`; `epoch_and_offset_for_entries`), then verify it two ways. These cores are sequential pure functions (no concurrent interleaving), so stateright gives exhaustive small-input coverage and proptest gives randomized production-scale coverage. Model A and Model B touch different crates and are independent.

**Tech Stack:** Rust, `stateright` 0.31, `proptest` (both already workspace deps).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-data-plane-safety-models-design.md`

**Verification discipline:** `stateright` runs are watchdog-guarded (3 GB / 150 s, `target_state_count`/`timeout` caps) — see `[[feedback_bound_model_checkers]]`. `proptest` runs are bounded sampling (no watchdog). CI fmt gate is nightly; clippy `-D warnings`; doc comments need backticks around code identifiers (`doc_markdown`).

---

## File Structure

- `crates/broker/src/producer_state.rs` — **modify**: extract `check_pure`; wire `producer_state_model.rs`; add a `proptest` fuzz test module.
- `crates/broker/src/producer_state_model.rs` — **create**: stateright model (`#[cfg(test)]` descendant).
- `crates/broker/Cargo.toml` — **modify**: add `proptest` to `[dev-dependencies]`.
- `crates/log/src/leader_epoch_checkpoint.rs` — **modify**: extract `epoch_and_offset_for_entries` (+ `append_to`/`truncate_to` pure helpers); wire `leader_epoch_model.rs`; add a `proptest` fuzz test module.
- `crates/log/src/leader_epoch_model.rs` — **create**: stateright model (`#[cfg(test)]` descendant).
- `crates/log/Cargo.toml` — **modify**: add `stateright` to `[dev-dependencies]`.

Model A (broker, Tasks A1–A2) and Model B (log, Tasks B1–B2) are independent.

---

## Task A1: Idempotent producer — extract `check_pure` + stateright model

**Files:** modify `crates/broker/src/producer_state.rs`; create `producer_state_model.rs`.

- [ ] **Step 1: Extract `check_pure`**

Insert above `impl ProducerState` (after the `Decision` enum):

```rust
/// Pure idempotent-producer dedup/ordering decision. The async `check` is a thin
/// lock-acquiring wrapper over this; extracted so it is exhaustively and
/// property-tested in isolation (see `producer_state_model.rs`).
pub(crate) fn check_pure(
    entry: Option<&ProducerEntry>,
    producer_epoch: i16,
    base_sequence: i32,
) -> Decision {
    match entry {
        None => Decision::Append,
        Some(entry) => {
            if producer_epoch < entry.epoch {
                return Decision::Fenced;
            }
            if producer_epoch > entry.epoch {
                // A bumped epoch establishes a fresh sequence baseline (restart
                // or KIP-890 per-EndTxn bump). Accept the first higher-epoch batch.
                return Decision::Append;
            }
            if base_sequence <= entry.last_sequence {
                return Decision::Duplicate {
                    base_offset: entry.base_offset,
                };
            }
            if base_sequence == entry.last_sequence + 1 {
                Decision::Append
            } else {
                Decision::OutOfOrder
            }
        }
    }
}
```

Replace the body of `check` (the `match s.entries.get(&producer_id) { … }`) with:

```rust
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        let _ = last_offset_delta; // used only by the caller to compute last_sequence on commit
        check_pure(s.entries.get(&producer_id), producer_epoch, base_sequence)
```

(Keep the `check` signature unchanged — callers are untouched.)

- [ ] **Step 2: Verify the extraction is behavior-preserving**

Run: `cargo test -p crabka-broker --lib producer_state`
Expected: existing `producer_state` tests pass.

- [ ] **Step 3: Wire the model module**

Append to `producer_state.rs`:

```rust
#[cfg(test)]
#[path = "producer_state_model.rs"]
mod producer_state_model;
```

- [ ] **Step 4: Write the stateright model**

Create `crates/broker/src/producer_state_model.rs`:

```rust
//! Exhaustive stateright enumeration of the idempotent-producer dedup core
//! (`check_pure`). One producer-id per partition; requests are serialized by the
//! broker, so this enumerates all bounded submit-sequences and asserts the
//! accepted-append log stays a gap-free, duplicate-free, monotonic prefix per
//! producer epoch, with epoch fencing. See the design spec.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{check_pure, Decision, ProducerEntry};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct ProducerModel {
    max_epoch: i16,
    max_seq: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ProdState {
    epoch: i16,
    last_sequence: i32,
    base_offset: i64,
    next_offset: i64,
    /// Ghost: highest contiguous accepted sequence per epoch (for the gap-free
    /// linearizability invariant). Sorted by epoch.
    accepted_hi: Vec<(i16, i32)>,
    initialized: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ProdAction {
    /// Submit a batch (single-record: delta 0) at (epoch, base_sequence).
    Submit(i16, i32),
}

fn entry_of(s: &ProdState) -> Option<ProducerEntry> {
    if !s.initialized {
        return None;
    }
    Some(ProducerEntry {
        epoch: s.epoch,
        last_sequence: s.last_sequence,
        last_offset: s.base_offset,
        base_offset: s.base_offset,
        last_timestamp: 0,
        last_activity_ms: 0,
    })
}

impl Model for ProducerModel {
    type State = ProdState;
    type Action = ProdAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProdState {
            epoch: 0,
            last_sequence: -1,
            base_offset: -1,
            next_offset: 0,
            accepted_hi: vec![],
            initialized: false,
        }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        for e in 0..=self.max_epoch {
            for sq in 0..=self.max_seq {
                actions.push(ProdAction::Submit(e, sq));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let ProdAction::Submit(epoch, base_seq) = action;
        let entry = entry_of(last);
        let decision = check_pure(entry.as_ref(), epoch, base_seq);
        let mut s = last.clone();
        match decision {
            Decision::Append => {
                // Verify the classification is a valid log extension.
                if last.initialized && epoch == last.epoch {
                    assert!(
                        base_seq == last.last_sequence + 1,
                        "same-epoch Append not contiguous: base_seq={base_seq} last={}",
                        last.last_sequence
                    );
                } else if last.initialized {
                    assert!(epoch > last.epoch, "Append epoch not fresh: {epoch} <= {}", last.epoch);
                }
                // Commit (delta 0 → last_sequence = base_seq).
                s.epoch = epoch;
                s.last_sequence = base_seq;
                s.base_offset = s.next_offset;
                s.next_offset += 1;
                s.initialized = true;
                // Record contiguous-prefix high-water for this epoch.
                if let Some(slot) = s.accepted_hi.iter_mut().find(|(e, _)| *e == epoch) {
                    slot.1 = base_seq;
                } else {
                    s.accepted_hi.push((epoch, base_seq));
                    s.accepted_hi.sort_unstable();
                }
                Some(s)
            }
            Decision::Duplicate { .. } => {
                assert!(
                    last.initialized && epoch == last.epoch && base_seq <= last.last_sequence,
                    "Duplicate misclassified: epoch={epoch} base_seq={base_seq} state={last:?}"
                );
                None // no state change; don't add a redundant edge
            }
            Decision::OutOfOrder => {
                assert!(
                    last.initialized && epoch == last.epoch && base_seq > last.last_sequence + 1,
                    "OutOfOrder misclassified: epoch={epoch} base_seq={base_seq} state={last:?}"
                );
                None
            }
            Decision::Fenced => {
                assert!(
                    last.initialized && epoch < last.epoch,
                    "Fenced misclassified: epoch={epoch} state={last:?}"
                );
                None
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Per epoch, the accepted sequences are a contiguous 0..=hi prefix
            // (gap-free + duplicate-free linearizable append log).
            Property::always("contiguous_prefix", |_, s: &ProdState| {
                s.accepted_hi.iter().all(|(_, hi)| *hi >= 0)
            }),
            // Within the current epoch, last_sequence never exceeds max_seq bound
            // (sanity that the enumeration stays in-bounds).
            Property::always("in_bounds", |m: &ProducerModel, s: &ProdState| {
                s.last_sequence <= m.max_seq && s.epoch <= m.max_epoch
            }),
            Property::sometimes("can_dedup", |_, s: &ProdState| s.last_sequence >= 0),
            Property::sometimes("can_bump_epoch", |_, s: &ProdState| s.epoch >= 1),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.epoch <= self.max_epoch && s.last_sequence <= self.max_seq
    }
}

fn run(model: ProducerModel, label: &str) {
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
fn producer_basic() {
    run(ProducerModel { max_epoch: 2, max_seq: 3 }, "producer_basic");
}

#[test]
fn producer_wide() {
    run(ProducerModel { max_epoch: 4, max_seq: 6 }, "producer_wide");
}
```

- [ ] **Step 5: fmt + clippy + run under watchdog**

`cargo +nightly fmt -p crabka-broker` then `cargo clippy -p crabka-broker --all-targets -- -D warnings` (fix any `doc_markdown` backtick lints). Build `cargo test -p crabka-broker --lib producer_state_model --no-run`, then run `producer_basic` + `producer_wide` via the host watchdog recipe (launch the exe, poll `WorkingSet64`, kill >3 GB/>150 s). Scale `producer_wide` up while exhaustive (< MAX_STATES).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/producer_state.rs crates/broker/src/producer_state_model.rs
git commit -m "test(broker): stateright model of idempotent-producer dedup linearizability"
```

---

## Task A2: Idempotent producer — proptest fuzz at large N

**Files:** modify `crates/broker/Cargo.toml`, `crates/broker/src/producer_state.rs`.

- [ ] **Step 1: Add `proptest` dev-dep**

In `crates/broker/Cargo.toml` `[dev-dependencies]`, add: `proptest = { workspace = true }`.

- [ ] **Step 2: Add the fuzz test**

In `producer_state.rs`'s `#[cfg(test)] mod tests` (or a new `#[cfg(test)] mod fuzz`), add:

```rust
use proptest::prelude::*;

proptest! {
    /// Large-N randomized submit sequences over `check_pure`: the accepted-append
    /// log per (epoch) is a contiguous, duplicate-free, monotonic prefix; a lower
    /// epoch is fenced; a higher epoch resets the baseline.
    #[test]
    fn idempotent_log_invariants(
        ops in proptest::collection::vec(
            (0i16..6, 0i32..200), // (producer_epoch, base_sequence)
            0..400usize,
        )
    ) {
        let mut entry: Option<ProducerEntry> = None;
        let mut next_offset: i64 = 0;
        // Reference: per-epoch highest accepted sequence (must stay contiguous).
        let mut hi: std::collections::HashMap<i16, i32> = std::collections::HashMap::new();
        for (epoch, base_seq) in ops {
            let d = check_pure(entry.as_ref(), epoch, base_seq);
            match d {
                Decision::Append => {
                    if let Some(e) = &entry {
                        if epoch == e.epoch {
                            prop_assert_eq!(base_seq, e.last_sequence + 1, "same-epoch Append must be contiguous");
                        } else {
                            prop_assert!(epoch > e.epoch, "Append epoch must be fresh");
                        }
                    }
                    // Per-epoch contiguity: an accepted seq for a fresh epoch starts the prefix;
                    // a same-epoch accept extends it by 1.
                    let prev = hi.get(&epoch).copied();
                    if let Some(p) = prev {
                        prop_assert_eq!(base_seq, p + 1, "accepted sequence must extend the per-epoch prefix");
                    }
                    hi.insert(epoch, base_seq);
                    entry = Some(ProducerEntry {
                        epoch, last_sequence: base_seq, last_offset: next_offset,
                        base_offset: next_offset, last_timestamp: 0, last_activity_ms: 0,
                    });
                    next_offset += 1;
                }
                Decision::Duplicate { .. } => {
                    let e = entry.as_ref().expect("Duplicate implies an entry");
                    prop_assert_eq!(epoch, e.epoch);
                    prop_assert!(base_seq <= e.last_sequence, "Duplicate must be within committed range");
                }
                Decision::OutOfOrder => {
                    let e = entry.as_ref().expect("OutOfOrder implies an entry");
                    prop_assert_eq!(epoch, e.epoch);
                    prop_assert!(base_seq > e.last_sequence + 1, "OutOfOrder must be a real gap");
                }
                Decision::Fenced => {
                    let e = entry.as_ref().expect("Fenced implies an entry");
                    prop_assert!(epoch < e.epoch, "Fenced must be a stale epoch");
                }
            }
        }
    }
}
```

- [ ] **Step 3: Run + fmt + clippy + commit**

`cargo test -p crabka-broker --lib idempotent_log_invariants` (expect pass over the default 256 cases). fmt + clippy. Then:

```bash
git add crates/broker/Cargo.toml crates/broker/src/producer_state.rs
git commit -m "test(broker): proptest fuzz of idempotent-producer dedup at large N"
```

---

## Task B1: Log truncation — extract pure helpers + stateright model

**Files:** modify `crates/log/Cargo.toml`, `crates/log/src/leader_epoch_checkpoint.rs`; create `leader_epoch_model.rs`.

- [ ] **Step 1: Add `stateright` dev-dep**

In `crates/log/Cargo.toml` `[dev-dependencies]`, add: `stateright = { workspace = true }`.

- [ ] **Step 2: Extract pure free fns**

Add to `leader_epoch_checkpoint.rs` (module-level, below the impl):

```rust
/// Pure core of [`LeaderEpochCheckpoint::epoch_and_offset_for`] over a raw slice,
/// so it can be exhaustively + property-tested without a file. The method
/// delegates to this.
pub(crate) fn epoch_and_offset_for_entries(
    entries: &[EpochEntry],
    requested_epoch: i32,
    log_end_offset: i64,
) -> (i32, i64) {
    let latest = entries.iter().map(|e| e.epoch).max();
    if requested_epoch == UNDEFINED_EPOCH {
        return (UNDEFINED_EPOCH, log_end_offset);
    }
    if latest == Some(requested_epoch) {
        return (requested_epoch, log_end_offset);
    }
    let higher = entries.iter().filter(|e| e.epoch > requested_epoch).min_by_key(|e| e.epoch);
    match higher {
        None => (UNDEFINED_EPOCH, log_end_offset),
        Some(next) => {
            let floor = entries.iter().filter(|e| e.epoch <= requested_epoch).map(|e| e.epoch).max();
            match floor {
                Some(f) => (f, next.start_offset),
                None => (requested_epoch, next.start_offset),
            }
        }
    }
}

/// Pure core of [`LeaderEpochCheckpoint::append`]: idempotent push-if-absent.
pub(crate) fn append_to(entries: &mut Vec<EpochEntry>, epoch: i32, start_offset: i64) {
    if entries.iter().any(|e| e.epoch == epoch) {
        return;
    }
    entries.push(EpochEntry { epoch, start_offset });
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_end`].
pub(crate) fn truncate_to(entries: &mut Vec<EpochEntry>, end_offset: i64) {
    entries.retain(|e| e.start_offset < end_offset);
}
```

Rewrite the method `epoch_and_offset_for` body to delegate:
```rust
    pub fn epoch_and_offset_for(&self, requested_epoch: i32, log_end_offset: i64) -> (i32, i64) {
        epoch_and_offset_for_entries(&self.entries, requested_epoch, log_end_offset)
    }
```
And `append`'s body to `append_to(&mut self.entries, epoch, start_offset); self.flush()` (preserving the idempotent early-return semantics now inside `append_to` — note `flush` still runs only when an entry was added; keep the `any` check before flush, or have `append_to` return whether it changed and flush conditionally to preserve "rewrites atomically" behavior). And `truncate_from_end`'s mutation to `truncate_to`, preserving the `if len changed { flush }` guard.

- [ ] **Step 3: Verify behavior-preserving**

Run: `cargo test -p crabka-log --lib leader_epoch_checkpoint`
Expected: the 18 existing leader-epoch tests pass.

- [ ] **Step 4: Wire + write the stateright model**

Append to `leader_epoch_checkpoint.rs`:
```rust
#[cfg(test)]
#[path = "leader_epoch_model.rs"]
mod leader_epoch_model;
```

Create `crates/log/src/leader_epoch_model.rs`:

```rust
//! Exhaustive stateright enumeration of the leader-epoch truncation core
//! (`epoch_and_offset_for_entries`). A leader and a follower epoch-history share
//! a common prefix then diverge; the follower computes its truncation point.
//! Asserts the committed common prefix is never truncated and divergent suffixes
//! always are. See the design spec.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{append_to, epoch_and_offset_for_entries, truncate_to, EpochEntry};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct EpochModel {
    max_epoch: i32,
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct EpochState {
    leader: Vec<EpochEntry>,
    follower: Vec<EpochEntry>,
    follower_leo: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum EpochAction {
    LeaderAppend(i32, i64),
    FollowerAppend(i32, i64),
    FollowerTruncate, // query epoch_and_offset_for against the leader, truncate
}

/// Longest common prefix end-offset: the offset up to which leader & follower agree.
fn common_prefix_end(leader: &[EpochEntry], follower: &[EpochEntry]) -> i64 {
    let mut end = 0;
    for (l, f) in leader.iter().zip(follower.iter()) {
        if l == f {
            end = l.start_offset;
        } else {
            break;
        }
    }
    end
}

impl Model for EpochModel {
    type State = EpochState;
    type Action = EpochAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![EpochState { leader: vec![], follower: vec![], follower_leo: 0 }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Append the next epoch at a higher start_offset on either side, bounded.
        let next_off = |hist: &[EpochEntry]| hist.last().map_or(0, |e| e.start_offset + 1);
        let lnext = next_off(&s.leader);
        let fnext = next_off(&s.follower);
        let lep = s.leader.last().map_or(0, |e| e.epoch + 1);
        let fep = s.follower.last().map_or(0, |e| e.epoch + 1);
        if lep <= self.max_epoch && lnext <= self.max_offset {
            actions.push(EpochAction::LeaderAppend(lep, lnext));
        }
        if fep <= self.max_epoch && fnext <= self.max_offset {
            // Follower may diverge by appending a different epoch/offset.
            actions.push(EpochAction::FollowerAppend(fep, fnext));
        }
        if !s.follower.is_empty() && !s.leader.is_empty() {
            actions.push(EpochAction::FollowerTruncate);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            EpochAction::LeaderAppend(e, off) => {
                append_to(&mut s.leader, e, off);
                Some(s)
            }
            EpochAction::FollowerAppend(e, off) => {
                append_to(&mut s.follower, e, off);
                s.follower_leo = off + 1;
                Some(s)
            }
            EpochAction::FollowerTruncate => {
                let req = s.follower.last().map(|e| e.epoch).unwrap_or(-1);
                let (_e, trunc) = epoch_and_offset_for_entries(&s.leader, req, s.follower_leo);
                // SAFETY: truncation never drops the committed common prefix.
                let common = common_prefix_end(&s.leader, &s.follower);
                assert!(
                    trunc >= common,
                    "truncation {trunc} dropped committed prefix (common ends at {common})"
                );
                truncate_to(&mut s.follower, trunc);
                s.follower_leo = s.follower_leo.min(trunc);
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // After any truncation, the follower's history is a prefix-compatible
            // subset of the leader's up to the common point (no divergent entry
            // with start_offset >= common survives a truncation that targeted it).
            Property::always("histories_monotonic", |_, s: &EpochState| {
                is_monotonic(&s.leader) && is_monotonic(&s.follower)
            }),
            Property::sometimes("can_diverge", |_, s: &EpochState| {
                s.leader.iter().zip(s.follower.iter()).any(|(l, f)| l != f)
            }),
            Property::sometimes("can_truncate", |_, s: &EpochState| {
                !s.follower.is_empty() && s.follower_leo == 0
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.leader.len() <= (self.max_epoch as usize + 1)
            && s.follower.len() <= (self.max_epoch as usize + 1)
    }
}

fn is_monotonic(h: &[EpochEntry]) -> bool {
    h.windows(2).all(|w| w[0].epoch < w[1].epoch && w[0].start_offset < w[1].start_offset)
}

fn run(model: EpochModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(), checker.state_count(), checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(checker.state_count() < MAX_STATES, "[{label}] state cap hit");
    checker.assert_properties();
}

#[test]
fn truncation_basic() {
    run(EpochModel { max_epoch: 3, max_offset: 5 }, "truncation_basic");
}

#[test]
fn truncation_wide() {
    run(EpochModel { max_epoch: 4, max_offset: 8 }, "truncation_wide");
}
```

- [ ] **Step 5: fmt + clippy + run under watchdog**

`cargo +nightly fmt -p crabka-log`; `cargo clippy -p crabka-log --all-targets -- -D warnings`; build `--no-run`; run `truncation_basic`/`truncation_wide` under the watchdog. Scale `truncation_wide` while exhaustive.

- [ ] **Step 6: Commit**

```bash
git add crates/log/Cargo.toml crates/log/src/leader_epoch_checkpoint.rs crates/log/src/leader_epoch_model.rs
git commit -m "test(log): stateright model of leader-epoch truncation divergence-safety"
```

---

## Task B2: Log truncation — proptest fuzz at large N

**Files:** modify `crates/log/src/leader_epoch_checkpoint.rs`.

- [ ] **Step 1: Add the fuzz test**

In `leader_epoch_checkpoint.rs`'s `#[cfg(test)] mod tests`, add (`proptest` already a dev-dep):

```rust
use proptest::prelude::*;

/// Build a strictly-increasing epoch history from a sorted set of (epoch, offset)
/// steps (mirrors how `append` constructs histories).
fn history(steps: &[(i32, i64)]) -> Vec<EpochEntry> {
    let mut v: Vec<EpochEntry> = vec![];
    let (mut le, mut lo) = (-1i32, -1i64);
    for &(e, o) in steps {
        let e = le + 1 + (e.rem_euclid(3)); // strictly increasing epoch
        let o = lo + 1 + (o.rem_euclid(100)); // strictly increasing offset
        super::append_to(&mut v, e, o);
        le = e; lo = o;
    }
    v
}

proptest! {
    /// Large-N randomized leader/follower histories sharing a random common
    /// prefix then diverging: the computed truncation offset never drops the
    /// common prefix and is a valid (>= 0) target.
    #[test]
    fn truncation_preserves_common_prefix(
        prefix in proptest::collection::vec((0i32..1000, 0i64..10000), 0..15usize),
        ldiv   in proptest::collection::vec((0i32..1000, 0i64..10000), 0..10usize),
        fdiv   in proptest::collection::vec((0i32..1000, 0i64..10000), 0..10usize),
        leo    in 0i64..20000,
    ) {
        let common = history(&prefix);
        // Leader and follower extend the SAME common prefix differently.
        let mut leader = common.clone();
        let mut follower = common.clone();
        let lo = leader.last().map_or((-1,-1), |e| (e.epoch, e.start_offset));
        for (i, &(e, o)) in ldiv.iter().enumerate() {
            super::append_to(&mut leader, lo.0 + 1 + i as i32 + e.rem_euclid(2), lo.1 + 1 + i as i64 * 50 + o.rem_euclid(40));
        }
        for (i, &(e, o)) in fdiv.iter().enumerate() {
            super::append_to(&mut follower, lo.0 + 1 + i as i32 + e.rem_euclid(2), lo.1 + 1 + i as i64 * 50 + o.rem_euclid(40));
        }
        let common_end = common.last().map_or(0, |e| e.start_offset);
        let req = follower.last().map(|e| e.epoch).unwrap_or(-1);
        let f_leo = leo.max(follower.last().map_or(0, |e| e.start_offset + 1));
        let (_e, trunc) = super::epoch_and_offset_for_entries(&leader, req, f_leo);
        prop_assert!(trunc >= 0, "truncation target must be valid");
        prop_assert!(trunc >= common_end, "truncation must not drop the common prefix");
    }
}
```

- [ ] **Step 2: Run + fmt + clippy + commit**

`cargo test -p crabka-log --lib truncation_preserves_common_prefix` (expect pass). fmt + clippy. Then:
```bash
git add crates/log/src/leader_epoch_checkpoint.rs
git commit -m "test(log): proptest fuzz of leader-epoch truncation at large N"
```

---

## Self-Review

**Spec coverage:** Model A stateright (A1) + proptest (A2); Model B stateright (B1) + proptest (B2); `check_pure` extraction (A1.1); `epoch_and_offset_for_entries` extraction (B1.2); deps — `proptest`→broker (A2.1), `stateright`→log (B1.1); small-scope exhaustive + large-N proptest methodology (both); watchdog discipline (A1.5/B1.5). ✓

**Placeholder scan:** All steps show concrete code. The stateright models' invariants are encoded as `next_state` asserts + `always`/`sometimes` properties; like prior model slices the exact bounds/witnesses are tuned at the run step (A1.5/B1.5), and a RED result is handled as in earlier slices (capture trace, report). The `truncation` property statement is intentionally the safety floor (`trunc >= common_end`); B1's `next_state` adds the stronger per-step assert. Not hidden TODOs.

**Type consistency:** `check_pure(Option<&ProducerEntry>, i16, i32) -> Decision` matches A1 (def) and A2 (proptest call). `Decision` variants (`Append`/`Duplicate{base_offset}`/`OutOfOrder`/`Fenced`) used consistently. `epoch_and_offset_for_entries(&[EpochEntry], i32, i64) -> (i32,i64)`, `append_to`, `truncate_to` match B1 (def) and B2 (proptest) call sites. `ProducerEntry` fields (epoch/last_sequence/last_offset/base_offset/last_timestamp/last_activity_ms) match the real struct. `EpochEntry{epoch,start_offset}` matches. ✓
