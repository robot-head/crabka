# Fetch HWM Visibility-Window Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright model of a pure core) + proptest.
**Predecessors:** raft, share-group, ISR, failover, reassignment, KIP-848 reconciliation
(found a bug), KIP-98 txn, data-plane idempotent-producer + log-truncation (#524), KIP-534
log-compaction retention (found a bug, #528).

## Goal

Extract the fetch read-path **visibility-window** decision into a pure function, **de-duplicate**
the two independent copies of the response-field computation, and prove the clamp contract +
KIP-227 monotonicity two ways — exhaustive `stateright` + `proptest`.

The visibility window is the single most safety-critical decision in the read path: it determines
which offsets a fetch may expose. A wrong clamp is a **dirty read** (records beyond the
high-watermark become visible) or **effective data loss / stuck consumer** (committed records hidden
from a `read_committed` consumer), or a wrong replication bound for a follower. The survey ranked
this #1 by blast radius; it is mostly *confirmation* (the logic is believed correct), with one
concrete latent hazard this slice eliminates.

## Background — the decision + the latent hazard

`do_read` (`crates/broker/src/handlers/fetch.rs:974-1135`) computes, under a brief metadata-only
hold of the log mutex, from `(is_follower_fetch, read_committed, log_start, hw, lso, log_end,
fetch_offset)`:

- `upper_bound = if is_follower { log_end } else { hw }`
- `effective_lso = if read_committed && !is_follower { lso.min(hw) } else { lso }`
- `fetch_offset < log_start` ⟹ `OFFSET_OUT_OF_RANGE`, and sets `out.high_watermark` +
  `out.last_stable_offset` (block at `:1017-1024`).
- else `limit_offset = follower ? log_end : read_committed ? effective_lso : hw`; if
  `fetch_offset >= upper_bound` ⟹ `Empty`; else `Read { limit_offset, effective_lso,
  read_committed_aborts }`.

**The latent hazard:** the response fields `out.high_watermark` / `out.last_stable_offset` are
computed in **two** places — the `OFFSET_OUT_OF_RANGE` block (`:1017-1024`) and the success/`NONE`
block (`:1115-1123`). Today both reduce to the same formula (`follower ? log_end : hw` and
`read_committed&&!follower ? lso.min(hw) : follower ? log_end : hw`), so they agree — but they are
duplicated logic that can silently drift. This slice collapses them into one source of truth.

Kafka partition invariants the model relies on: `0 <= log_start <= hw <= log_end` (LEO), and
`lso <= hw` (last-stable-offset never exceeds the high-watermark). `read_committed` is only set for
consumer fetches (`fetch.rs:111`: `!is_follower_fetch && isolation_level == 1`), so
`read_committed ⟹ !is_follower`.

## Refactor (small, behavior-preserving)

Extract a pure, total decision:

```rust
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct VisibilityWindow {
    /// `fetch_offset < log_start` — caller returns OFFSET_OUT_OF_RANGE.
    pub out_of_range: bool,
    /// `fetch_offset >= upper_bound` — nothing to read (no bytes).
    pub empty: bool,
    /// Exclusive upper offset the raw read may expose: `[fetch_offset, limit_offset)`.
    pub limit_offset: i64,
    /// read_committed aborted-txn scan ceiling (`lso.min(hw)` for a read_committed consumer).
    pub effective_lso: i64,
    /// `out.high_watermark` to report.
    pub response_hw: i64,
    /// `out.last_stable_offset` to report.
    pub response_lso: i64,
}

pub(crate) fn compute_visibility_window(
    is_follower: bool,
    read_committed: bool,
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
    fetch_offset: i64,
) -> VisibilityWindow;
```

`do_read` calls it once after taking `hw`/`log_start`/`log_end`/`lso` under the lock, then:
- on `out_of_range`: set `out.error_code = OFFSET_OUT_OF_RANGE`, `out.log_start_offset = log_start`,
  `out.high_watermark = w.response_hw`, `out.last_stable_offset = w.response_lso`; return.
- else build the `ReadPlan` from `w.empty` / `w.limit_offset` / `w.effective_lso`; after the read,
  set `out.high_watermark = w.response_hw`, `out.last_stable_offset = w.response_lso`,
  `out.log_start_offset = log_start`. **Both response-field sites now read from the single `w`.**

Behavior-preserving — gated by the existing fetch handler tests (unit + integration).

## Stateright model (`fetch_visibility_model.rs`, `#[cfg(test)]` descendant of `fetch`)

- **State:** the advancing partition watermarks `{ log_start, hw, lso, log_end }`, constrained to the
  Kafka invariant `0 <= log_start <= hw <= log_end` and `lso` in `[log_start, hw]`. No fetch params
  in the state (they are probe-action parameters).
- **Actions:**
  - `AdvanceLogEnd` / `AdvanceHw` / `AdvanceLso` / `AdvanceLogStart` — advance one watermark by 1
    while preserving the invariant and staying `<= max_offset`. These model the log progressing
    (appends raise LEO, ISR catch-up raises HW, txn commits raise LSO, retention raises log_start).
    **Monotonicity is asserted here**: for both `is_follower ∈ {false,true}` and `read_committed ∈
    {…}`, the recomputed `response_hw` and `response_lso` of the new state are `>=` those of the old
    state (KIP-227: a consumer's reported HW/LSO never regress across incremental fetches).
  - `Fetch(is_follower, read_committed, fetch_offset)` — `read_committed ⟹ !is_follower`;
    `fetch_offset ∈ [0, max_offset]`. Drives the real `compute_visibility_window` and asserts the
    per-fetch contract (below) in `next_state`; returns `None` (no state change).
- **Per-fetch safety asserts (in `next_state` on `Fetch`):**
  - **no-dirty-read** (HEADLINE): for a consumer (`!is_follower`), `limit_offset <= hw` — never
    exposes an offset beyond the high-watermark.
  - **read_committed clamp**: `read_committed ⟹ effective_lso == lso.min(hw)` and
    `limit_offset <= lso.min(hw) <= hw`.
  - **follower bound**: `is_follower ⟹ limit_offset == log_end (>= hw)`.
  - **valid targets**: `limit_offset >= 0`, `response_hw >= 0`, `response_lso >= 0`.
  - **response consistency / `lso <= hw`**: `response_lso <= response_hw` for consumers;
    `response_hw == (is_follower ? log_end : hw)`; `response_lso == (read_committed ? lso.min(hw) :
    is_follower ? log_end : hw)` — the single-source-of-truth contract (the de-dup'd hazard: the OOR
    and success paths share this exact computation).
  - **out_of_range / empty correctness**: `out_of_range == (fetch_offset < log_start)`;
    when not out-of-range, `empty == (fetch_offset >= (is_follower ? log_end : hw))`.
- **Properties:** `Property::always` for a structural state invariant (the watermark ordering holds
  in every reachable state); `Property::sometimes` non-vacuity witnesses — a real read_committed
  clamp occurs (`lso < hw` with a read_committed fetch returning `limit_offset == lso`); an
  `out_of_range`; an `empty`; a follower fetch exposing `> hw`.
- **Bounds (watchdog-guarded):** `max_offset` small (the decision is relative-comparison logic, so
  small bounds are exhaustive). Two configs: `visibility_basic` (`max_offset` ~4) and
  `visibility_wide` (`max_offset` ~7), scaled up while exhaustive under the host memory watchdog.

## proptest fuzz (`proptest` already a `crabka-broker` dev-dep)

Generate large-N random *valid* watermark tuples (`log_start <= hw <= log_end`, `lso ∈
[log_start, hw]`, offsets up to ~1e6) + random `fetch_offset` + the two bools (respecting
`read_committed ⟹ !is_follower`), drive `compute_visibility_window`, and assert the same per-fetch
contract. Plus a **relational monotonicity** property: for two states with `hw <= hw'`,
`log_end <= log_end'`, `lso <= lso'` (others equal), `response_hw(s') >= response_hw(s)` and
`response_lso(s') >= response_lso(s)` for each `(is_follower, read_committed)`.

## Out of scope (YAGNI)

- The actual byte read (`read_raw`), the `spawn_blocking` offload, the aborted-txn `.txnindex` scan,
  and the KIP-405 remote-tier fallback — the model checks the *offset decision*, not the I/O.
- Incremental-fetch session caching mechanics (KIP-227 session state) — only the HW/LSO
  monotonicity property is modeled, not the session cache.
- Multi-partition fetch aggregation, throttling, max_bytes accounting.

## Verification discipline

- Every `stateright` run is fenced (`within_boundary` + `target_state_count`/`timeout`
  `Duration::from_mins(2)`) and run under the host memory watchdog (kill > 3 GB / > 150 s) while
  bounds are tuned — `[[feedback_bound_model_checkers]]`. `proptest` is bounded sampling.
- `cargo +nightly fmt` per-crate (Windows deep-path workaround — `[[reference_windows_fmt_path_length]]`);
  `cargo clippy --all-targets -- -D warnings` clean.

## Success criteria

1. `compute_visibility_window` extracted; both response-field sites in `do_read` call it (the
   duplicated logic is gone); all existing fetch tests pass unchanged.
2. The stateright model proves the per-fetch contract + KIP-227 monotonicity exhaustively across
   both configs; non-vacuity witnesses satisfied; runs clean under the watchdog.
3. The proptest passes at large N (same contract + relational monotonicity).
4. fmt + clippy clean; the broader broker suite unaffected.
