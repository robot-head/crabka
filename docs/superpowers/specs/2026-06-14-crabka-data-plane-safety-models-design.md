# Data-Plane Safety Models — Idempotent Producer + Log Truncation — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright models of pure cores), extended with property-based fuzzing
**Predecessors:** raft consensus, share-group (#514), ISR (#515), failover (#516), reassignment
(#520), KIP-848 reconciliation (#521), KIP-98/EOS txn coordinator (#523).

## Goal

Add two independent data-plane safety models — **idempotent-producer linearizability** and
**log-truncation divergence-safety** — each verified two complementary ways:

1. **Exhaustive `stateright` BFS** at bounds scaled to the largest config that stays exhaustive
   under the host memory watchdog (the *small-scope hypothesis*: concurrency/protocol bugs live
   in the interleaving structure, so exhaustive-at-small catches the bug classes — the
   reconciliation model found a real bug at 2 members / 2 partitions).
2. **`proptest` fuzz** at large N over the *same pure cores* — thousands of randomized,
   production-scale inputs the BFS can't reach exhaustively, asserting the same invariants. This
   is the "realistic scale" complement: exhaustive-small for interleaving coverage, randomized-large
   for scale-dependent surprises.

These round out the data-plane correctness story (idempotence + log-divergence) the way the
consensus models rounded out coordination.

## Methodology — why two levers

`stateright` keeps every visited state resident, so exhaustive BFS is bounded by the 3 GB watchdog;
production scale (millions of sequence numbers, long epoch histories) is unreachable exhaustively.
Both cores here are *relative*-logic (compare two values), so small exhaustive bounds already
cover every decision branch and interleaving. The `proptest` layer then samples large instances
over the identical pure function, catching anything scale-dependent without pretending an
exhaustive check runs at production scale.

---

## Model A — Idempotent-producer linearizability (`crates/broker`)

### Background

`ProducerState::check` (`crates/broker/src/producer_state.rs:76-132`) is the idempotent-producer
dedup/ordering decision; it's pure logic behind a `tokio::Mutex`. It returns:

```rust
pub enum Decision { Append, Duplicate { base_offset: i64 }, OutOfOrder, Fenced }
```

and `commit` (`:136-162`) records `ProducerEntry { epoch, last_sequence, last_offset, base_offset, … }`.

### Refactor (small, behavior-preserving)

Extract the pure decision from the async lock:

```rust
pub(crate) fn check_pure(entry: Option<&ProducerEntry>, producer_epoch: i16, base_sequence: i32)
    -> Decision;
```

`ProducerState::check` becomes `let s = handle.lock().await; check_pure(s.entries.get(&producer_id), producer_epoch, base_sequence)`.
Behavior-preserving; gated by the existing `producer_state` unit tests.

### Stateright model (`producer_state_model.rs`, `#[cfg(test)]` descendant)

- **State:** per producer-id `{ epoch, last_sequence, last_offset }` (one partition — partitions are
  independent), plus a ghost reference `accepted: map (epoch, base_sequence) → assigned base_offset`.
- **Actions:** a producer instance submits a batch `(producer_epoch, base_sequence, last_offset_delta)`
  → drive `check_pure`; on `Append`, apply the commit (advance `last_sequence`/`last_offset`, assign a
  fresh `base_offset`); interleave ≥2 producer instances (epoch bumps = restarts).
- **Properties (`always`):**
  - **no_duplicate_offset** (HEADLINE linearizability): a given `(epoch, base_sequence)` never maps to
    two distinct `base_offset`s; a re-submit returns `Duplicate { base_offset }` with the *original* offset.
  - **contiguous**: an accepted `Append` has `base_sequence == last_sequence + 1` (within an epoch) — no gaps.
  - **monotonic**: `last_sequence` is non-decreasing within an epoch.
  - **fencing**: `producer_epoch < entry.epoch` ⟹ `Fenced`.
  - **epoch_reset**: a higher `producer_epoch`'s first batch is accepted regardless of sequence (the
    documented EOS-restart anti-data-loss rule).
  - `sometimes` witnesses: a `Duplicate` is detected; a `Fenced` occurs; an epoch bump resets the baseline.
- **Bounds (scaled to watchdog):** start 2 producer instances / epoch 0-2 / sequence window ~3, then
  scale up (more instances, epoch 0-4, wider window) keeping the largest config exhaustive (< ~150k states).

### proptest fuzz (`proptest` added to `crates/broker` dev-deps)

Generate long randomized op-sequences (epoch 0..~10, base_sequence 0..~1000, varied
`last_offset_delta`, ≥2 producers); drive `check_pure` + apply commits against a reference
idempotent log; assert the same invariants (no-duplicate-offset, contiguity, monotonicity, fencing)
over thousands of cases at a scale the BFS can't reach.

---

## Model B — Log-truncation divergence-safety (`crates/log`)

### Background

`LeaderEpochCheckpoint::epoch_and_offset_for(requested_epoch, log_end_offset) -> (i32, i64)`
(`crates/log/src/leader_epoch_checkpoint.rs:181-212`) is **already a pure sync fn** (KIP-101/320):
given the leader's epoch history and a follower's last epoch, it returns the epoch + offset the
follower should truncate to. Supporting pure fns: `append(epoch, start_offset)`,
`truncate_from_end(end_offset)`, `EpochEntry`.

### Stateright model (`leader_epoch_model.rs`, `#[cfg(test)]` descendant; `stateright` added to `crates/log` dev-deps)

- **State:** a leader epoch-history and a follower epoch-history (each a bounded `Vec<EpochEntry>`,
  monotonic in epoch + start_offset) that share a common prefix then diverge, plus the follower's LEO.
- **Actions:** leader appends an epoch; follower appends an epoch (possibly diverging); follower
  queries `epoch_and_offset_for` and truncates to the result.
- **Properties (`always`):**
  - **committed_prefix_preserved** (HEADLINE): the truncation offset is ≥ the offset of the last
    entry the leader and follower agree on — a committed record (in the common prefix) is never truncated.
  - **divergent_suffix_removed**: the truncation offset is ≤ the follower's first divergent offset —
    every divergent record is truncated.
  - **epoch_history_monotonic**: epochs and start-offsets are strictly increasing.
  - `sometimes` witnesses: a real divergence (follower has an epoch the leader doesn't) triggers a
    truncation strictly below the follower's LEO.
- **Bounds (scaled to watchdog):** start epochs 0-3 / offsets 0-5, scale up (longer histories,
  offsets 0-10) keeping the largest exhaustive config.

### proptest fuzz (`proptest` already in `crates/log`)

Generate randomized leader + follower epoch histories (length up to ~20, offsets up to ~10000,
random divergence points); compute the truncation via `epoch_and_offset_for`; assert
committed-prefix-preserved + divergent-suffix-removed over thousands of large cases.

---

## Dependencies

- `crates/broker/Cargo.toml`: add `proptest = { workspace = true }` to `[dev-dependencies]`
  (`stateright` already present).
- `crates/log/Cargo.toml`: add `stateright = { workspace = true }` to `[dev-dependencies]`
  (`proptest` already present, line 37).

## Out of scope (YAGNI)

- Model A: the produce I/O path, cross-partition behavior (partitions are independent in the dedup),
  the 5-batch retained-cache eviction mechanics (the model checks the dedup *decision*, not the LRU).
- Model B: the `OffsetForLeaderEpoch` RPC transport and the on-disk truncation I/O (the model checks
  the offset *computation*); multi-follower fan-out (each follower truncates independently).
- Both: time-based behavior (no clocks in the models).

## Verification discipline

- Every `stateright` checker run is fenced with `within_boundary` + `target_state_count` + `timeout`
  (`Duration::from_mins(2)`) and run under the host memory watchdog (kill >3 GB / >150 s) while bounds
  are tuned — see `[[feedback_bound_model_checkers]]`. `proptest` runs are bounded sampling (fast,
  bounded memory) — no watchdog needed.
- `cargo +nightly fmt` clean; `cargo clippy --all-targets -- -D warnings` clean.

## Success criteria

1. `check_pure` extracted; all existing `producer_state` tests pass unchanged.
2. Both stateright models prove their safety properties exhaustively across configs (or produce a
   concrete counterexample — handled like prior slices); non-vacuity witnesses satisfied.
3. Both proptest fuzz suites pass at large N (assert the same invariants over thousands of cases).
4. All exhaustive configs run clean under the watchdog (no cap truncation); fmt + clippy clean; the
   broader broker + log suites unaffected.
