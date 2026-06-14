# Token-Bucket Concurrency Model (KIP-73) — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (stateright concurrency model) + proptest — and a **real concurrency bug fix**.
**Predecessors:** raft, share-group, ISR, failover, reassignment, KIP-848 reconciliation (found a
bug), KIP-98 txn, data-plane (#524), KIP-534 log-compaction (found a bug, #528), fetch-HWM
visibility (#529).

## Goal

Fix a **confirmed lock-free over-grant / underflow race** in the KIP-73 token-bucket rate limiter,
and prove the fix with an exhaustive `stateright` **shared-memory interleaving** model (the truest
concurrency slice since raft) + a `proptest` over the pure arithmetic. The bug is demonstrated first
as a committed RED witness, then fixed (CAS loop) → GREEN.

## Background — the race, confirmed in source

`TokenBucket::try_consume` (`crates/broker/src/throttle/bucket.rs:49-67`) does a **non-atomic
read-modify-write** of `available`:

```rust
let mut cur = self.available.load(Relaxed);            // load
let new_avail = (cur.saturating_add(refill)).min(rate);
self.available.store(new_avail, Relaxed);              // store  (NOT atomic with the load)
cur = new_avail;
let grant = requested.min(cur);
self.available.fetch_sub(grant, Relaxed);              // can underflow-wrap u64
```

`ThrottleState` holds broker-wide `Arc<TokenBucket>`s (`leader_out` / `follower_in`) that every
connection's fetch/produce handler calls concurrently — so `try_consume` runs from many tasks/threads
on one bucket. The `last_refill_nanos` **swap** is atomic (only one caller claims each elapsed gap, so
refill is not double-counted), but the `available` load→store→`fetch_sub` has no CAS. Concrete
interleaving at `rate = available = 1000`:

- T1 `load`→1000, T2 `load`→1000, both `store`→1000, T1 `fetch_sub(1000)`→`available=0`,
  T2 `fetch_sub(1000)`→**`available` underflows to ~`u64::MAX`**.
- Total granted = 2000 against a 1000-token bucket: **over-grant past the burst cap**, plus a
  transient **throttle-disable** (the wrapped `available` is only re-capped on the next call's
  `.min(rate)`).

`set_rate` (`:35-39`) additionally does **three** non-atomic `Relaxed` stores (`rate`, `available`,
`last_refill`); interleaved with a concurrent `try_consume` it can reset `available` mid-RMW. Severity:
fairness / availability (a client briefly exceeds its quota; the throttle is briefly disabled) — not
data-loss. This is the survey's rank-5 "likely a real bug."

## The fix (KIP-73-faithful)

Make refill+consume atomic with a `compare_exchange_weak` loop on `available`; extract the pure
arithmetic so production and the model/proptest share it:

```rust
/// Pure: given the current available, the refill claimed for this call, the rate cap, and the
/// requested bytes, return (grant, new_available). `new_available = min(cur+refill, rate) - grant`,
/// `grant = min(requested, min(cur+refill, rate))`, so `new_available >= 0` always.
pub(crate) fn plan_consume(cur: u64, refill: u64, rate: u64, requested: u64) -> (u64, u64);

pub fn try_consume(&self, requested: u64) -> u64 {
    let rate = self.rate_bytes_per_sec.load(Relaxed);
    if rate == 0 { return requested; }
    let now = now_nanos();
    let last = self.last_refill_nanos.swap(now, Relaxed);       // unchanged: claims the elapsed gap
    let elapsed = now.saturating_sub(last);
    let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;
    loop {
        let cur = self.available.load(Relaxed);
        let (grant, new_avail) = plan_consume(cur, refill, rate, requested);
        match self.available.compare_exchange_weak(cur, new_avail, Relaxed, Relaxed) {
            Ok(_) => return grant,
            Err(_) => continue,  // contended (consume or set_rate changed it) — retry on the fresh value
        }
    }
}
```

`fetch_sub` is gone, so `available` can never underflow. A concurrent `set_rate` store of `available`
makes the CAS fail → the consume retries against the reset value (self-correcting, no underflow,
bounded by the cap). `set_rate` stays as-is — the CAS in `try_consume` is the complete fix.

## Stateright model (`bucket_model.rs`, `#[cfg(test)]` descendant of `bucket`)

A **shared-memory interleaving** model. The nanosecond arithmetic is abstracted (covered by the
proptest); the model focuses on the *concurrency* over small abstract token counts.

- **State:** shared `{ rate: i64, available: i64, pending_refill: i64 }` (`available` is `i64` so the
  buggy underflow shows up as a **negative** value the asserts catch) + a fixed-size array of
  per-thread `ThreadPc` (the in-flight `try_consume`/`set_rate` program counter + locals: claimed
  refill, observed `cur`, requested, computed grant/new). `pending_refill` models the
  not-yet-claimed elapsed refill (a `Tick` raises it; a consume's claim-step swaps it to 0 — the
  atomic-swap semantics, abstractly).
- **Actions** (one atomic step each, interleaved across threads):
  - `Tick` — `pending_refill = (pending_refill + 1).min(max_refill)`.
  - `StartConsume(t, requested)` / `StartSetRate(t, new_rate)` — an idle thread begins.
  - `ClaimRefill(t)` — `refill[t] = pending_refill; pending_refill = 0` (atomic swap).
  - `Load(t)` — `cur[t] = available`.
  - **Commit(t)** — branches on the model's `cas` flag:
    - `cas = false` (BUGGY): two separate interleavable steps — `Store(t)` sets `available =
      min(cur[t]+refill[t], rate)`, then `Sub(t)` sets `available -= grant[t]` (the `fetch_sub`,
      which can drive `available` negative).
    - `cas = true` (FIXED): one step — if `available == cur[t]` then `available = new` (from the real
      `plan_consume`), else `t` retries from `Load`.
  - `SetRate` steps — `StoreRate(t)`, `StoreAvail(t)` (`available = new_rate`), `ResetRefill(t)`
    (`pending_refill = 0`) — three interleavable stores (the `set_rate` race).
- **Safety asserts (per transition / `Property::always`):**
  - **no-underflow / no-over-grant** (HEADLINE): `available >= 0` in every state. The buggy
    store+`fetch_sub` interleaving drives it negative; the CAS version keeps `new = min(cur+refill,
    rate) - grant >= 0`.
  - **burst-cap**: `available <= max_rate_ever_set` in every state.
  - **grant-bounded**: each completed grant `<=` the `available` it observed (`grant[t] <=
    min(cur[t]+refill[t], rate)`).
- **Non-vacuity (`sometimes`):** a refill is claimed; a real grant occurs; a `set_rate` interleaves an
  in-flight consume; the buggy path actually reaches a negative `available` (in the RED config) /
  a CAS retry occurs (in the fixed config).
- **RED → GREEN (committed witness):** a `cas = false` config drives the model and a
  `#[should_panic]` test asserts the no-underflow invariant **fires** (the over-grant/underflow
  interleaving — record the concrete trace, e.g. two consumes both `Load` then both `Sub`). The
  `cas = true` configs (`bucket_basic`, `bucket_wide`) run GREEN. Production is fixed to the CAS
  version, so the GREEN model matches the real code; the buggy path is the test-only legacy shim.
- **Bounds (watchdog-guarded):** 2 threads, `rate`/`requested` ~0-3, `max_refill` ~2; scale a `wide`
  config (3 threads or wider counts) while exhaustive under the host memory watchdog.

## proptest fuzz (`proptest` already a `crabka-broker` dev-dep)

Large-N over the pure `plan_consume`: random `cur`, `refill`, `rate`, `requested` (including the
`u128→u64` cast edges, `saturating_add` overflow, `rate = 0`, `requested = 0`, huge values). Assert:
`grant <= requested`; `grant <= min(cur+refill, rate)`; `new_available == min(cur+refill, rate) -
grant` and `new_available >= 0` (no underflow) and `new_available <= rate` (cap). Plus a **sequential
conservation** property: a chain of `plan_consume` calls (sequential, no interleaving) never grants
more than `initial + Σ refills` capped by the running cap.

## Out of scope (YAGNI)

- The real nanosecond clock / `now_nanos` epoch (the model abstracts time as `pending_refill`; the
  arithmetic precision is the proptest's job).
- `> 3` concurrent threads (2 suffices to exhibit the race; a `wide` config adds a third).
- Replacing the `Relaxed` orderings with stronger ones (the CAS loop is correct under `Relaxed` for
  this single-location invariant; memory-ordering across the separate `rate` atomic is not a safety
  concern here — a stale rate read only changes the cap, not correctness).

## Verification discipline

- Every `stateright` run is fenced (`within_boundary` + `target_state_count`/`timeout`
  `Duration::from_mins(2)`) and run under the host memory watchdog (kill > 3 GB / > 150 s) while
  bounds are tuned — `[[feedback_bound_model_checkers]]`. `proptest` is bounded sampling.
- `cargo +nightly fmt` per-crate (`[[reference_windows_fmt_path_length]]`); `cargo clippy
  --all-targets -- -D warnings` clean.

## Success criteria

1. `plan_consume` extracted; `try_consume` rewritten as a CAS loop; existing bucket unit tests pass
   unchanged (behavior-preserving for the single-threaded paths they cover).
2. The model reproduces the over-grant/underflow race as a committed RED witness (`#[should_panic]`
   on the `cas = false` shim), then proves no-underflow + burst-cap + grant-bounded exhaustively for
   the fixed (`cas = true`) configs, with concurrent `set_rate`; non-vacuity witnesses satisfied.
3. The proptest passes at large N over `plan_consume` (grant/cap/no-underflow + conservation).
4. fmt + clippy clean; the broader broker suite unaffected.
