# KIP-112 — Handle disk failure for JBOD (completion via a KIP-858 directory-assignment slice)

Date: 2026-06-03
Status: approved (design)
KIP: [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112) (disk-failure handling); rides on a focused slice of [KIP-858](https://cwiki.apache.org/confluence/display/KAFKA/KIP-858) (JBOD in KRaft) for the controller-side mapping.

## Problem

Most of KIP-112 is already implemented in Crabka:

- Startup writability probe marks unwritable dirs offline and the broker boots on
  the survivors (`crates/broker/src/log_dir_status.rs`).
- **Runtime** write/fsync failures flip a dir offline mid-life:
  `crates/broker/src/partition_writer.rs:32-41` (`flag_storage_failure`) calls
  `LogDirRegistry::mark_offline` on any `LogError::Io(_)` from append / replicate /
  truncate / reset / trim / compact. (The `log_dir_status.rs` module doc claiming
  this is "deferred" is **stale** and must be fixed.)
- Produce / Fetch / DescribeLogDirs return `KAFKA_STORAGE_ERROR` for offline dirs
  (`handlers/produce.rs:408`, `handlers/fetch.rs:363`, `handlers/describe_log_dirs.rs:77`),
  and JBOD placement skips offline dirs (`LogDirRegistry::online_subset`).

Two gaps remain, and they are the heart of KIP-112's availability promise:

1. **Controller-side failover on disk failure.** Leader failover today
   (`crates/broker/src/leader_election.rs::compute_failover_changes`) fires only
   when the liveness ticker observes a broker transition `alive → dead`. A broker
   whose disk fails stays *alive* (it keeps heartbeating), so leadership never
   moves off the dead disk — clients receive `KAFKA_STORAGE_ERROR` indefinitely
   instead of failing over to a healthy replica.
2. **All-log-dirs-offline → broker shutdown.** KIP-112 specifies that a broker
   shuts down when *every* log directory fails. No monitor exists.

The architectural tension: the failing broker knows the **dir → partition**
mapping (it owns the local partitions and their dirs) but not cluster liveness;
the controller knows **liveness** but not which partitions live on which dir,
because `PartitionRecord` carries no per-replica directory field. We resolve this
the JVM-faithful way: implement the KIP-858 directory-assignment metadata so the
controller can map a failed-dir UUID back to exactly the affected partitions.

## Goals

- A runtime disk failure precisely fails over exactly the partitions whose replica
  on the failing broker lived on the dead dir: the controller elects a new leader
  from the surviving alive ISR (honoring the existing clean / KIP-841 / KIP-966
  recovery policy) and drops the offline replica from ISR.
- A broker that loses **all** data log dirs shuts itself down.
- Faithful KIP-858 wire throughout (`offline_log_dirs` on `BrokerHeartbeatRequest`,
  `AssignReplicasToDirs` api key 73, per-replica `directories` on the KRaft
  `PartitionRecord`).
- Flip README KIP-112 ⚠️ → ✅.

## Non-goals (out of scope)

- Full reassignment / data movement of offline replicas to other brokers
  (KIP-858's reassignment story). We move *leadership and ISR*, not data.
- The controller responding `should_shut_down=true` based on comparing a broker's
  registered `log_dirs` against its reported `offline_log_dirs`. Shutdown stays a
  **local** decision (more robust: a broker with no disks is useless regardless of
  controller reachability).
- Any backwards-compat shim (greenfield — see CLAUDE.md). The internal
  `PartitionRecord` simply gains a field; no `#[serde(default)]`, no migration.

## Design

### Topology note

In Crabka's dominant (and only fully Mac-testable) topology, the single broker is
also the controller leader and heartbeats the controller leader (itself). The
failover path is therefore driven uniformly by `offline_log_dirs` on the heartbeat
for **both** co-located and remote brokers — there is no separate self-failover
monitor task. The controller leader, wherever it runs, receives the heartbeat,
maps the offline dir UUIDs to affected partitions via `PartitionRecord.directories`,
and submits the failover changes. (In single-broker RF=1 there is no failover
target, so the partition correctly stays unavailable with `KAFKA_STORAGE_ERROR`
until restart, and if all dirs are gone the local all-dirs check shuts the broker
down.)

### Component 1 — Per-log-dir UUIDs

Each configured `log.dir` gets a stable UUID persisted in that dir's
`meta.properties.json` (today only the primary/metadata dir has one — written by
`crates/cli/src/format.rs::write_meta_properties`, read by
`crates/broker/src/bootstrap.rs::read_directory_id`). `extra_log_dirs` currently
have no identity file.

- On startup the broker reads each dir's `directory_id`; for any dir lacking the
  file (e.g. a freshly added JBOD dir) it generates a v4 UUID and persists it.
  The primary dir keeps its existing id (which doubles as the KRaft voter
  `directory_id` — unchanged).
- Expose a `path ↔ uuid` mapping. Cleanest home: extend `LogDirRegistry` (or a
  sibling struct constructed alongside it in `Broker::start`) so every consumer —
  heartbeat client, assignment reporter, placement — shares one table. The
  registry is built where the probe runs today (`broker.rs:1433`).
- Populate `BrokerRegistrationRequest.log_dirs` (the generated field already
  exists) with the online dir UUIDs at registration time.

### Component 2 — `PartitionRecord.directories`

Add `directories: Vec<Uuid>` to the internal `PartitionRecord`
(`crates/metadata/src/records.rs:19`), parallel to `replicas` (one UUID per
replica, same order). `Uuid::nil()` is `DirectoryId.UNASSIGNED` (not yet reported).

Threading:

- **Raft log:** the internal `MetadataRecord` is `serde_wincode`-serialized, so the
  new field round-trips automatically. Add a round-trip assertion to the existing
  `partition_record_round_trip` test.
- **Snapshot (KRaft byte-exact):** `crates/metadata/src/kraft_translate.rs::partition_to_kraft`
  currently emits a KRaft `PartitionRecord` at apiVersion 0 with
  `directories: ..Default::default()` (empty). Change it to populate `directories`
  and emit **apiVersion 1**; update the inverse `from_kraft` decode to read it.
  This is required so directory assignments survive a controller snapshot +
  recovery (the documented "to_records completeness" hazard) — otherwise post-
  snapshot the controller loses the dir → partition map and failover silently
  breaks.
- **Reconstruction sites (correctness-critical):** every place that builds a new
  `PartitionRecord` from an existing one must carry `directories` forward
  (`pr.directories.clone()`), or — for a leader change where the replica set is
  unchanged — preserve it verbatim. Known sites: `leader_election.rs`
  (`compute_failover_changes`, `select_new_leader_for_partition`,
  `select_replacement_leader_for_shutdown`, the unclean branches),
  `handlers/alter_partition.rs`, `handlers/create_topics.rs` (new partitions start
  all-`nil`), `reassignment.rs`, `leader_rebalance.rs`, `unclean_recovery.rs`, plus
  test fixtures. Adding `#[derive(Default)]` to `PartitionRecord` lets construction
  sites that genuinely have no directory info use `..Default::default()`, but any
  site copying an existing record MUST clone the field.

### Component 3 — `AssignReplicasToDirs` (api key 73)

The generated wire types already exist
(`crates/protocol/generated/AssignReplicasToDirsRequest.owned.rs`,
`...Response.owned.rs`). Mirror the established broker→controller "propose a
partition change" pattern used by ISR maintenance
(`isr_maintenance.rs::send_alter_partition` → `handlers/alter_partition.rs`):

- **Broker send:** after `replicator_supervisor.rs::materialize_partition` picks a
  dir via `place_partition_dir`, collect `(topic_id, partition, this_broker_dir_uuid)`
  and send a batched `AssignReplicasToDirs` to the controller leader. Triggers:
  startup (recovered partitions), new materialization, and after a KIP-113 log-dir
  swap (`partition_writer::swap_future_log` changes the owning dir). Resolve
  `topic_id` from the metadata image (KIP-516 topic IDs are implemented).
- **Controller handler:** new `handlers/assign_replicas_to_dirs.rs`. Leader-only
  (return `NOT_CONTROLLER` otherwise, like the other controller handlers). For each
  reported `(topic, partition, dir_uuid)`, set the reporting broker's slot in
  `PartitionRecord.directories` and submit the updated record via
  `controller.submit_change`. Register the handler + api version in the api catalog.

### Component 4 — Heartbeat reports offline dirs

The heartbeat client reads `LogDirRegistry::offline()`, maps the offline paths to
their UUIDs, and sets `BrokerHeartbeatRequest.offline_log_dirs` (the real KIP-858
field, presently decoded and ignored at `handlers/broker_heartbeat.rs:41`). Thread
the dir-UUID map and `log_dir_status` into the heartbeat client task (spawned at
`broker.rs:1617`).

### Component 5 — Controller-side offline-dir failover (the payoff)

New pure function in `leader_election.rs`:

```
compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &BTreeSet<Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &BrokerMetrics,
) -> FailoverPlan
```

For each partition where `broker` is a replica and its `directories[broker_slot]`
is in `offline_uuids`:

- If `broker` is the leader: elect a new leader from the alive ISR minus `broker`,
  reusing the exact election + recovery-strategy logic of
  `compute_failover_changes` (clean pick; KIP-966 URM deferral; KIP-841 unclean
  opt-in; otherwise leave unavailable). Drop `broker` from ISR and bump
  `leader_epoch`.
- If `broker` is a non-leader replica in ISR: drop it from ISR without bumping
  `leader_epoch` (mirrors the non-leader branch of `compute_failover_changes`).

Wire it into `handlers/broker_heartbeat.rs`: when `req.offline_log_dirs` is
non-empty and this node is the controller leader, run the scan for `req.broker_id`
and submit the resulting changes (and enqueue URM recoveries, as
`on_broker_dead` does).

Idempotency / no thrash: after re-election the broker is no longer leader, and
after the ISR drop it is no longer in ISR, so subsequent identical heartbeats
produce empty plans. The offline broker cannot replicate (its dir is dead), so
`isr_maintenance` will not re-admit it until the dir recovers (broker restart),
at which point it re-materializes, re-sends `AssignReplicasToDirs`, catches up, and
is re-admitted — self-healing.

### Component 6 — All-dirs-offline → self-shutdown

A lightweight local check: when every **data** log dir (`config.all_log_dirs()`
minus the metadata-only primary, per the existing semantics) is offline, latch
`want_shutdown` (`broker.rs:110`) and drive the existing controlled-shutdown /
drain sequence. Natural evaluation points: the heartbeat loop tick and/or
immediately after a `mark_offline` flip. KIP-112's "shut down when all dirs fail."

### Component 7 — Cleanup + verification

- Fix the stale module doc in `log_dir_status.rs` (runtime detection is wired, not
  deferred).
- Unit tests for `compute_offline_dir_failover_changes` (multi-broker, in-process,
  pure over a `MetadataImage` + liveness — same style as the existing
  `compute_failover_changes` tests): leader-on-dead-dir elects alive ISR member;
  non-leader-on-dead-dir ISR shrink; empty-ISR honors recovery strategy; partition
  on a *healthy* dir of the same broker is untouched; idempotent re-run.
- Unit tests for the `AssignReplicasToDirs` handler (sets the right slot;
  leader-only).
- Integration test (in-process, single broker): flip a dir offline at runtime via
  a test seam, assert produce/fetch return `KAFKA_STORAGE_ERROR` and the heartbeat
  carries the dir UUID in `offline_log_dirs`. A test seam is needed since reliable
  cross-platform EIO injection is hard — add a `#[cfg(any(test, feature = "test-helpers"))]`
  `BrokerHandle::test_mark_log_dir_offline(path)` (and/or a per-dir directory.id /
  registry round-trip test).
- All-dirs-offline self-shutdown integration test (single broker, all dirs flipped
  → broker shuts down).
- Update affected KRaft snapshot / mixed-quorum byte-exact tests for the
  PartitionRecord v0 → v1 emission change.
- README KIP-112 ⚠️ → ✅; STATUS.md slice entry.

## Risks

- **PartitionRecord KRaft v0 → v1 emission** changes snapshot/metadata-log bytes.
  This is JVM-faithful (Kafka emits PartitionRecord v1 since KIP-858), but any test
  asserting exact v0 partition-record bytes must be updated, and the change must be
  verified against the JVM mixed-quorum interop tests on Linux CI.
- **Broad mechanical churn** from the new `directories` field across every
  `PartitionRecord` construction site; the correctness risk is a site that drops
  the field on a copy. Mitigate with a grep-driven checklist and the round-trip /
  failover unit tests.

## Testability summary

- Components 1–4, 6, 7: fully testable on macOS (single broker / in-process).
- Component 5: election logic unit-tested in-process; full live multi-broker E2E
  deferred to Linux CI (inter-broker data replication does not work on the Mac dev
  host).

## Rough implementation batches (for the plan)

Non-overlapping file sets run in parallel per CLAUDE.md:

- **Batch A (metadata foundation):** `records.rs` (+ field, round-trip test),
  `kraft_translate.rs` (v1 emit/decode), `image.rs` apply if needed. Fix all
  `PartitionRecord` reconstruction sites.
- **Batch B (dir identity + registry):** per-dir UUID persistence
  (`bootstrap.rs` / `format.rs`), `log_dir_status.rs` path↔uuid map + stale-doc
  fix, broker-registration `log_dirs` population.
- **Batch C (wire + reporting):** `AssignReplicasToDirs` handler + api-catalog
  registration + broker send path in the supervisor; heartbeat `offline_log_dirs`
  send.
- **Batch D (failover + shutdown):** `compute_offline_dir_failover_changes` +
  heartbeat-handler wiring; all-dirs self-shutdown.
- **Batch E (tests + docs):** integration tests, test seam, README, STATUS.

Batches A and B are independent and run together; C depends on A+B; D depends on
A+C; E last. The plan will refine exact task/file boundaries.
