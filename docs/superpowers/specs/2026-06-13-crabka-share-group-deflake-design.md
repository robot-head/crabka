# Crabka — de-flake broker share + group-coordination tests (Phase 2)

- **Date:** 2026-06-13
- **Status:** Approved design — ready for implementation planning
- **Scope:** Phase 2 of the deterministic-test program. Continues Workstream B
  (de-flaking sleep-based tests) from Phase 1
  (`2026-06-13-crabka-stateright-consensus-deflake-design.md`), targeting the
  broker's share-group (KIP-932) and consumer/streams group-coordination tests —
  the known-flaky-on-Windows subset.

## Problem & framing

Phase 1 de-flaked the broker's *consensus-correctness* integration tests by
adding metadata-image `wait_*` awaiters to `BrokerHandle` and converting fixed
`sleep`-poll loops to event-driven condition-waits. Those awaiters only observe
the **metadata image** (partition presence, leader, ISR, broker registration,
local log-end offset).

The share-group and group-coordination tests wait on state the metadata image
does **not** carry: share-partition acquisition state (SPSO, delivery-complete
count, acquired records) and consumer/streams group-coordinator state (group
epoch, member epoch, member count, Stable/Empty, assignment). They currently
fix-sleep-poll on these, which flakes — these are the tests the project memory
flags as timing out / flaking on Windows.

This phase adds two new **test-only** awaiter families to `BrokerHandle` —
share-state and group-state — and converts 9 test files onto them. Both families
follow the Phase 1 convention exactly: `#[cfg(any(test, feature = "test-helpers"))]`,
a 30 s overall `tokio::time::timeout` safety-net, and **poll-with-bounded-recheck**
(neither subsystem exposes a Notify/watch, so the awaiter re-checks real state at
a ~25–50 ms cadence under the timeout — the same shape as Phase 1's
`wait_until_local_log_end_offset_eq`). They are condition-waits on real state, so
they cannot flake on timing; they only fail if the condition never holds in 30 s.

### Scope: 9 files, two batches

- **Batch A — share-state:** add share-state awaiters; convert
  `crates/broker/tests/{share_state, share_groups, share_consume, share_admin_offsets}.rs`.
- **Batch B — group-state:** add group-state awaiters; convert
  `crates/broker/tests/{streams_groups, streams_classic_downgrade, streams_classic_upgrade,
  consumer_group_next_gen_persistence, consumer_proactive_validation}.rs`.

The batches are independent (different hook families, different files) and are
dispatched as two batches within one plan.

## Grounding facts (verified)

- `BrokerHandle` holds `_broker: Arc<Broker>`. `Broker` exposes
  `group_coordinator: Arc<GroupCoordinator>` (`crates/broker/src/broker.rs:48`)
  and `share_partition_leaders: Arc<SharePartitionLeaderManager>`
  (`crates/broker/src/broker.rs:53`).
- An existing test accessor `BrokerHandle::share_state_summary_for_test(group, topic_id, partition)`
  (`crates/broker/src/broker.rs:433`, async) already returns the persisted
  share-state summary `(state_epoch, leader_epoch, start_offset, dcc)`; it is
  already used by `share_consume.rs`. Reuse it; do not duplicate.
- Share state: `SharePartitionLeaderManager.leaders` is a
  `DashMap<(group, topic_id, partition), Arc<Mutex<AcquisitionState>>>`
  (`crates/broker/src/share_partition/manager.rs`). `AcquisitionState`
  (`crates/broker/src/share_partition/state.rs:86`) holds `delivery_complete_count`
  (`state.rs:99`) and per-batch `RecordState` (`state.rs:29`, with `Acquired` at
  `state.rs:31`); `start_offset()` is the SPSO. No Notify/watch on mutations —
  only the lock-timeout sweeper (`manager.rs` `spawn_lock_sweeper`, a
  `tokio::time::interval`).
- Group state: `GroupCoordinator.groups` is a
  `DashMap<group_id, Arc<GroupActorHandle>>` (`crates/broker/src/coordinator/unified/mod.rs`).
  The per-group actor answers `GroupActorMessage::Describe { reply }`
  (`crates/broker/src/coordinator/unified/actor.rs:69`) with a `DescribeView`
  (`actor.rs:254`) carrying `group_epoch`, `assignment_epoch`, member count, and
  per-member `member_epoch` + `assignment_state` (`Stable` |
  `UnreleasedPartitions` | `UnrevokedPartitions`). No Notify/watch — only the
  actor's ~1 s tick + per-RPC dispatch.
- The survey classified ~64 sleep/poll sites across the 9 files: ~26 need a
  share-state hook, ~10 need a group-state hook, ~8 reuse the Phase 1 image
  awaiters, ~4 are legitimate `consumer.poll()` loops (keep), ~6 are
  pre-shutdown flush windows (some convertible), and 6 are share lock-timeout
  sites in `share_consume.rs`.

## Workstream A — share-state awaiters (`crates/broker/src/broker.rs`)

Add a minimal test accessor on `AcquisitionState` (or the manager) for the
in-flight **Acquired** batch count (the only datum the existing summary doesn't
expose), then these `#[cfg(any(test, feature = "test-helpers"))]` awaiters on
`BrokerHandle`, each a poll-with-bounded-recheck under a 30 s timeout:

- `wait_for_share_state_summary(group, topic_id, partition)` — await until
  `share_state_summary_for_test(...)` is `Some` (share-state initialized or
  recovered). Replaces the "share-state initialization / recovered summary"
  poll loops.
- `wait_until_share_spso(group, topic_id, partition, min_spso)` — await SPSO
  (summary `start_offset`) ≥ `min_spso`. Replaces "SPSO advanced past accepted /
  reset offsets" loops.
- `wait_until_share_delivery_complete(group, topic_id, partition, min_dcc)` —
  await `delivery_complete_count` ≥ `min_dcc`. Replaces "dcc advanced to accept
  count" loops (and the lock-timeout *archive* outcome, where dcc advances).
- `wait_until_share_acquired_count(group, topic_id, partition, n)` — await the
  number of `Acquired` batches == `n`. Replaces "records acquired" /
  "fragmented-window acquired" loops, and the lock-timeout *redelivery* outcome
  (await re-acquirable: acquired count returns to the expected value).

These cover the 26 share-state sites and the lock-timeout **outcome** sites: the
sweeper fires on its own (small configured `record_lock_duration`); the test
awaits the resulting observable state (re-acquirable / dcc-incremented /
archived) rather than sleeping a fixed duration.

**Calibrated exceptions (kept as sleeps):** the 2–3 *precise renew-timing*
sub-tests in `share_consume.rs` (`renew_extends_lock`, `no_renew_redelivers`)
prove a lock is **not** released before its deadline — proving a non-event
inherently requires waiting through the deadline. Their sleeps are functions of
the explicitly-configured `record_lock_duration` (not "guess how long async
took"), so they stay. `tokio::time::pause()` is **not** used: these are full
broker+client integration tests, and pausing the runtime clock would freeze the
real network/heartbeats.

## Workstream B — group-state awaiters (`crates/broker/src/broker.rs`)

Add a `BrokerHandle` method to describe a group via the existing actor message:

- `group_describe_for_test(group_id) -> Option<DescribeView>` — look up the group
  in `_broker.group_coordinator.groups`, send `GroupActorMessage::Describe`, await
  the oneshot reply. `None` if the group is absent.

Then these awaiters (poll-with-bounded-recheck on `group_describe_for_test`,
30 s timeout):

- `wait_for_group_stable(group_id)` — members non-empty AND every member's
  `assignment_state == Stable`.
- `wait_until_group_epoch(group_id, min_epoch)` — `group_epoch` ≥ `min_epoch`.
- `wait_until_group_member_count(group_id, expected)` — member count == `expected`.
- `wait_until_group_empty(group_id)` — member count == 0 (drained after leave).

**DescribeView extension (if needed):** the streams tests assert on active-task
partition counts and classic-member `generation_id`, which `DescribeView` may not
expose. If so, extend `DescribeView` with the missing fields (populated from the
group state in the `Describe` handler). The added fields are read-only view data;
prefer test-relevant fields that the actor already has. (Resolve during
implementation by reading `DescribeView` + the streams tests' assertions.)

## Workstream applied — test conversions (both batches)

For each of the 9 files, replace each sleep/poll site per its classification:
- state-propagation share sleeps → the Workstream A awaiters;
- group-state sleeps → the Workstream B awaiters;
- partition-present / leader-ready / produce-retry sleeps → the **Phase 1**
  awaiters (`wait_until_partition_present`, etc.);
- pre-shutdown flush sleeps → await the persisted condition (dcc/SPSO) where one
  is awaitable (e.g. `share_admin_offsets.rs` already awaits dcc before restart);
  keep the genuinely-opaque log-flush windows;
- `consumer.poll()` loops and the calibrated renew-timing sleeps → keep.

All existing assertions are preserved; only the waiting changes.

## Verification plan

- `cargo build -p crabka-broker --test <name>` per converted file (single test
  binary — the Windows OS-1455 paging-file linker limit can fail an all-tests
  build; build/run binaries individually).
- `cargo test -p crabka-broker --test <name> -- --test-threads=1` per file —
  PASS, then **stress ≈10×** (these are the known Windows flakes per project
  memory — stress is the primary acceptance signal; zero flakes required).
- `grep` each file to confirm no state-guessing poll-`sleep` remains (only
  `consumer.poll()` and the calibrated `record_lock_duration`-derived sleeps).
- `cargo fmt -p crabka-broker` (per-crate; Windows OS-206 path-length) and
  `cargo clippy -p crabka-broker --features test-helpers --lib -- -D warnings`
  plus clippy on each converted test binary — clean.

## Risks & open questions (resolved during implementation)

1. **Poll contention.** The group actor mpsc (capacity 64) and the share-state
   `Mutex` are on the hot path; the awaiters re-check at ~25–50 ms, well clear of
   starving them (vs. a tight loop). Confirm the cadence is comfortable under the
   stress runs.
2. **DescribeView gaps.** The streams tests may need `active_tasks` /
   `generation_id` that `DescribeView` doesn't expose — extend it (test-relevant,
   read-only) if so.
3. **Acquired-count accessor placement.** Adding a test-only reader to
   `AcquisitionState`/manager touches the share-partition module; keep it
   `#[cfg(test)]`/test-helper-gated and side-effect-free (a read under the mutex).
4. **Residual flakiness is a signal.** If a converted test still flakes under
   stress, that's a real bug/race to investigate — not a sleep to re-add. The
   pre-existing Windows share-test flakiness is exactly what this phase fixes.

## Out of scope (later slices)

- The remaining ~80 sleep-using test files in other crates (schema-registry,
  grpc-gateway, client-streams, rebalancer, operator, client-core, protocol,
  integration-tests) — each needs its own crate's awaiter infrastructure.
- Category-B unit-with-timers (paused-clock) conversions across crates.
- JVM/testcontainers tests (`describe_groups_jvm`, `jvm_*`) stay sleep-based
  (real Docker JVM Kafka; cannot be made deterministic).
- Further stateright models (share-groups, ISR, dynamic voters, etc.) — separate
  spec → plan cycles per `project_stateright_testing_program`.
