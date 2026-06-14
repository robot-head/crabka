# Integration-Tests Sleep De-flake — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** B (replace flaky `sleep`/`timeout` waits with deterministic event-synchronization)

## Goal

Make the `crates/integration-tests/tests/` e2e suite deterministic: replace the 27
reducible `sleep(...)` calls with condition-awaits and bounded timeouts, so the tests
cannot flake under load and fail *fast with a legible message* instead of hanging or
racing. The 2 deliberately time-based share-lock tests stay as sleeps (documented).

## Background

All five affected test files spin up an **in-process Crabka broker**
(`Broker::start(BrokerConfig::for_tests(...))`) and drive it with **Crabka's own Rust
clients** (`crabka-client-consumer`, `crabka-client-admin`, `crabka-client-core`) — no
`rdkafka`, no testcontainers, no JVM. Every wait is therefore observable from code we
control, so almost all sleeps are reducible.

A reconnaissance pass classified all 29 sleeps:

| File | sleeps | broker | poll | keep |
|---|---|---|---|---|
| `consumer_cooperative_rebalance.rs` | 11 | 2 | 9 | 0 |
| `consumer_integration.rs` | 5 | 0 | 5 | 0 |
| `consumer_share_consumer.rs` | 9 | 4 | 3 | 2 |
| `admin_round_trip.rs` | 2 | 0 | 2 | 0 |
| `admin_log_dirs_round_trip.rs` | 2 | 1 | 1 | 0 |
| **total** | **29** | **7** | **20** | **2** |

The existing program already established the de-flake patterns and the discipline (a
bounded inner-retry sleep *inside* a `timeout`-bounded poll loop is acceptable; a fixed
sleep followed by an assert is not). `BrokerHandle` already exposes the awaiters these
tests need (`wait_until_partition_present`, `wait_for_share_state_summary`, …, behind the
existing test-helper `cfg`).

## The three de-flake patterns

### Pattern A — fixed-sleep-then-act → await the real condition

The genuinely-flaky shape: a fixed sleep *guesses* that an async transition (a
cooperative rebalance) has settled, then proceeds.

```rust
// crates/integration-tests/tests/consumer_cooperative_rebalance.rs:51 (before)
tokio::time::sleep(Duration::from_millis(500)).await;
let m2 = build_cooperative_consumer(&bootstrap, ...).await;
```

becomes an await of the actual settled condition (the helper already exists and is
called on the very next line):

```rust
// after
wait_for_total_assignment(&[&m1], EXPECTED, Duration::from_secs(30)).await;
let m2 = build_cooperative_consumer(&bootstrap, ...).await;
```

Applies to: cooperative-rebalance lines 51, 56.

### Pattern B — unbounded poll loop → `tokio::time::timeout(30s, …)`

The ~20 condition-poll loops already break on the correct observable condition
(`assignment().len() == N`, `values_seen.len() == 4`, member evicted from
`describe_group`, metadata shows N partitions, log-dir move complete). The flake risk is
that they are **unbounded** — under CI load they can spin indefinitely (manifesting as a
whole-test hang) or were preceded by a fixed warm-up sleep. The fix wraps each loop in a
bounded outer timeout that fails with a clear message; the bounded inner retry sleep
(50–250 ms) stays — that is the program's already-accepted pattern.

```rust
// pattern
tokio::time::timeout(Duration::from_secs(30), async {
    loop {
        if <condition observable from the client/admin> { break; }
        tokio::time::sleep(Duration::from_millis(100)).await; // bounded inner retry — OK
    }
})
.await
.expect("<condition> within 30s");
```

The duplicated per-file helpers (`wait_for_assignment_count`, `wait_for_total_assignment`,
`poll_until`) are made bounded once at their definition.

### Pattern C — ad-hoc "poll until partition materializes" → existing `BrokerHandle` awaiter

```rust
// before (e.g. admin_log_dirs_round_trip.rs:50)
for _ in 0..200 {
    if handle.has_partition("t", 0).await && handle.has_partition("t", 1).await { break; }
    tokio::time::sleep(Duration::from_millis(50)).await;
}
// after
handle.wait_until_partition_present("t", 0).await;
handle.wait_until_partition_present("t", 1).await;
```

Applies to the 7 `REDUCIBLE-broker` sleeps (cooperative 51/56 overlap with A where the
broker hook is cleaner; share-consumer 99/144/169/243; log-dirs 50). Exact awaiter signatures
are confirmed against `crates/broker/src/broker.rs` during planning; if a needed awaiter
is genuinely absent, it is added behind the existing test-helper `cfg` (no production
surface).

## Keep list (documented, not changed)

- `consumer_share_consumer.rs:791` — sleeps until ~400 ms after a record is acquired, then
  renews the share lock *before* its 1 s expiry (renew-before-expiry scenario).
- `consumer_share_consumer.rs:803` — sleeps until ~1150 ms after acquire (past the original
  1 s lock, before the renewed ~1400 ms deadline) to verify redelivery is *not* triggered.

These exercise time-based share-lock semantics; replacing the sleep with state-polling
would destroy what they test. Each gets a one-line comment marking the sleep intentional.

**Discovered during execution (added to the keep list):** the two membership-pacing
sleeps in `consumer_cooperative_rebalance.rs` (before m2 and m3 join). A 15× stress run
showed that introducing the next member while the prior cooperative-sticky rebalance is
still in flight causes cascading rebalances that never converge to a clean snapshot.
There is no client- or broker-observable "group fully stable" signal to await (only
member-count, which fires at JoinGroup before SyncGroup completes), so these joins must
be paced — a real protocol-timing property, not a flaky guess. They are kept with an
explanatory comment. (So 4 sleeps are kept; 25 reduced.)

## DRY

Several files duplicate near-identical bounded-await helpers. Where it removes duplication
cleanly, the bounded helpers are lifted into the integration-tests crate lib
(`crates/integration-tests/src/lib.rs`) so all test binaries share one implementation;
otherwise they stay per-file (decided in the plan from what `src/lib.rs` already exposes).
No behavior change — only consolidation + bounding.

## Scope and constraints

- **Test-only.** No production code changes, except adding a `BrokerHandle` awaiter behind
  the existing test-helper `cfg` if (and only if) a needed one is missing.
- The metadata-race produce-retry loops (`err == 3`/`6` → short sleep → retry) are bounded
  retries already; under the full pass they are converted to await-partition-present
  (Pattern C) then produce, removing the blind retry where clean, or left as bounded
  retries where awaiting is awkward — the test still cannot hang because the surrounding
  produce has a bounded attempt count.

## Out of scope

- The 2 keep-list timing tests (above).
- Non-integration-tests crates (the broker/raft/share de-flakes are already done; the
  remaining `jvm_*` / testcontainers tests are inherently sleep-based and excluded).
- Any change to test *assertions* or coverage — this slice only changes *how the test
  waits*, never *what it verifies*.

## Verification

- Each touched test is stress-run ≥10× locally (the de-flaking discipline) to confirm zero
  flakes.
- `cargo +nightly fmt` + `cargo clippy --all-targets -- -D warnings` clean.
- The bounded timeouts mean any real regression surfaces as a fast, named failure rather
  than a hang.

## Success criteria

1. No fixed-sleep-then-assert remains in the 5 files; every poll loop is bounded by a
   `timeout`.
2. The 7 broker-side waits use `BrokerHandle` awaiters.
3. The 2 timing tests remain (with explanatory comments); all touched tests pass and
   stress-pass ≥10×.
4. Test-only diff (modulo a possible test-helper-`cfg` awaiter); fmt + clippy clean.
