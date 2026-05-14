# Bulletproof EOS sub-slice 10b follow-up — Design Spec

## Goal

Close the slice-10b deferrals: re-enable the 4 rust integration tests
and 3 JVM acceptance tests that we marked `#[ignore]` / `--skip` in
PR #73 because they consistently failed with
`NOT_ENOUGH_REPLICAS_AFTER_APPEND` or "no leader elected within 2 min"
on Linux/macOS CI.

## Background

The deferred tests all exercise a 3-broker cluster where one broker
dies mid-test and the surviving brokers must continue making progress
under `acks=-1`. The original slice-10b investigation chased an "ISR
shrink" hypothesis through multiple fixes:

- Eager `install_isr` in `CreateTopics` (no longer races the supervisor)
- Supervisor calls `install_isr(&part_record.isr, …)` instead of
  `&part_record.replicas` (so AlterPartition-driven shrinks aren't
  immediately re-expanded)
- Idempotent `install_leader_change` (preserves `per_follower` when the
  leader/epoch didn't actually change)
- `install_isr` recomputes HW + fires `hw_advance_notify` (so shrink
  unblocks waiters without depending on a follower fetch)
- `CreateTopics` uses the shared `materialize_partition` helper so the
  handler and supervisor can't spawn two competing `Partition` instances

These fixes were correct but addressed downstream symptoms. After
running the test in WSL with `RUST_LOG=openraft=info`, the actual
root cause surfaced:

```
warn isr_maintenance: AlterPartition propose failed
  error=send: connect to 127.0.0.1:11292: Connection refused
```

`isr_maintenance` was sending `AlterPartition` to broker 3 — which the
test had just killed — because broker 3 was the openraft controller
leader. `Broker::start` (broker.rs:234) hardcodes
`election_timeout: Duration::from_secs(5)`, which makes openraft's
`election_timeout_max = 10000ms` and therefore
`leader_lease = 10s` (openraft `engine_config.rs` line 56:
`leader_lease: Duration::from_millis(config.election_timeout_max)`).
Openraft's docs are explicit:

> The node WILL NOT handle a `VoteRequest` before the `leader_lease`
> expires; The node WILL NOT elect itself until
> `leader_lease + election_timeout` has passed.

So when broker 3 dies, brokers 1 and 2 refuse to elect a new
controller for ~15 seconds. ISR shrink can't propose because there's
no controller leader to commit to. HW stays pinned at 0. The
producer's 10s timeout fires first.

This is an upstream cause: nothing in the slice-10b ISR bookkeeping
could have fixed it.

## Architecture

Three independent changes; each can land in its own commit.

### 1. Make raft timings configurable on `BrokerConfig`

Add two fields to `BrokerConfig`:

```rust
/// Openraft election timeout (`election_timeout_min`; max is 2×).
/// Indirectly sets `leader_lease = election_timeout × 2`. Default 5s
/// (production-safe; conservative to avoid split-vote on slow runners).
/// Tests targeting failover scenarios override to ~500ms so a dead
/// leader is replaced within the test window.
pub controller_election_timeout: Duration,

/// Openraft heartbeat interval. Default 500ms; aggressive tests use
/// ~100ms. Should be ≤ election_timeout / 3 by raft consensus norms.
pub controller_heartbeat_interval: Duration,
```

Update `Broker::start` to pass these through to `crabka_raft::ControllerConfig`
instead of hardcoding 5s/500ms.

Defaults: `Duration::from_secs(5)` and `Duration::from_millis(500)`
(unchanged for production). `for_tests` defaults aren't relevant for
the multi-broker test path — those construct `BrokerConfig` directly.

### 2. Apply boot retry to slice-10b multi-broker tests

`tests/quorum.rs` and `tests/replication.rs` already have
`start_n_node_with_retry` (3 attempts with port juggling) that solves
"split-vote on cold start" with short timings. Slice-10b's
`tests/durability.rs::boot_three_node`,
`tests/leader_election.rs::boot_three_node`, and the JVM acceptance
helpers do single-shot boot.

Hoist `start_n_node_with_retry` into a shared module
(`tests/support/cluster.rs`) and have slice-10b helpers use it.
This keeps the short timings stable across the test suite.

### 3. Use ephemeral ports for repeated multi-broker tests

Linux's TIME_WAIT keeps openraft sockets bound for ~60s after teardown.
`leader_election::isr_expand_on_catchup` failed with "no leader elected
within 2 min" because the second `boot_three_node` in the same test
file (cluster_lock serializes them) hit ports still in TIME_WAIT.

The existing `start_n_node_with_retry` pattern uses
`bind_and_drop_addrs` to capture stable loopback ports — that's fine
for *one* run but doesn't help when the same fixed port range is
re-used across tests.

Fix: have `bind_and_drop_addrs` return a fresh port range for *each*
test, not hardcoded constants like `12_092..=12_293`. The Rust broker
configs propagate the bound port into both `listen_addr` and the
voter map.

This is the same approach `tests/quorum.rs::start_n_node` already
uses. We just need to apply it to the slice-10b tests.

## Components

```
crates/broker/src/
├── config.rs                # MODIFIED — 2 new fields, defaults
└── broker.rs                # MODIFIED — pass new fields to ControllerConfig

crates/broker/tests/
├── support/
│   └── mod.rs               # NEW — shared start_n_node_with_retry, port helper
├── durability.rs            # MODIFIED — un-#[ignore], use shared boot, short timings
├── leader_election.rs       # MODIFIED — un-#[ignore] 3 tests, use shared boot
├── replication.rs           # MODIFIED — un-#[ignore] 2 tests
└── jvm_acceptance.rs        # MODIFIED — use new BrokerConfig fields where applicable

.github/workflows/ci.yml     # MODIFIED — remove --skip flags
```

The `tests/support/mod.rs` module is included via
`mod support;` declarations in each test binary (Rust's standard
multi-file integration test pattern).

## Test Plan

Acceptance:

1. **Local Linux (WSL):** all 4 previously-#[ignored] rust tests pass
   in a single `cargo test --workspace -- --include-ignored` run.
   Specifically validate `acks_all_completes_via_isr_shrink_when_follower_dead`
   completes in < 5s (test assertion) by observing the
   `start.elapsed()` log.

2. **CI ubuntu/macos/windows:** `cargo test --workspace` is green
   without `#[ignore]` annotations on the 4 rust tests; windows path
   stays gated by `#![cfg(not(target_os = "windows"))]` as before.

3. **JVM acceptance:** all 9 tests run (no `--skip`); job completes
   under the 30-minute cap.

4. **Defensive:** the `controller_election_timeout` default stays at
   `Duration::from_secs(5)` so production behavior is unchanged. A
   new unit test asserts the default value to catch accidental
   regression.

## Error Handling

- **Split-vote on cold start (short timings):** mitigated by boot
  retry in `start_n_node_with_retry`. Each retry rotates ports.
- **Port already in use:** the helper attempts up to 3 boot rounds;
  fails the test with a clear "cluster start failed after 3 attempts"
  message if all attempts collide.
- **No raft leader within 2 min during a test:** the existing
  `Broker::start` timeout fires with `Startup("no leader elected
  within 2 min")`. With short timings + retry, this should be rare,
  but the message is sufficient to diagnose.

## Out of Scope

- Production-time raft tuning beyond exposing the knob. We're not
  changing defaults.
- A `Raft::trigger_election` integration when the broker liveness
  ticker detects a dead controller leader. That's a cleaner fix
  architecturally but bigger; the timing-knob approach unblocks the
  tests faster and remains compatible with a future `trigger_election`
  layered on top.
- Re-running the deferred tests under JVM-differential. Those tests
  use docker-driven JVM clients, which we already re-enable as part
  of the `--skip` removal.

## YAGNI Decisions

- No builder pattern for `BrokerConfig`. Adding two fields keeps the
  struct-literal pattern that tests already use.
- No env-var override (`RUST_LOG`-style). Tests construct
  `BrokerConfig` directly; no need for a runtime knob.
- No exposing of openraft's `election_timeout_max` separately. The
  2× factor is fine; only one knob needed.
