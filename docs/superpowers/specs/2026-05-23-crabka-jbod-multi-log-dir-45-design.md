# Slice 45: JBOD / multi-log-dir + DescribeLogDirs (KIP-113) — Design

**Status:** Approved 2026-05-23.

**Goal:** Make `crabka-broker` a real JBOD broker — store partition data
across multiple log directories on one broker, place new partitions by
least-loaded balancing, and report per-directory contents over the wire via
`DescribeLogDirs` (api key 35). This is the read/placement half of KIP-113;
the intra-broker replica *move* (`AlterReplicaLogDirs`, api key 34) is
deferred to slice 45b. First slice of Phase 8 (storage gaps).

---

## 1. Scope

### In

- `BrokerConfig.extra_log_dirs: Vec<PathBuf>` — additional JBOD data
  directories alongside the existing primary `log_dir`. New
  `BrokerConfig::all_log_dirs()` returns `[log_dir] + extra_log_dirs`,
  de-duplicated, primary first.
- CLI flag `--log-dirs` (env `CRABKA_EXTRA_LOG_DIRS`, comma-separated) and
  TOML `extra_log_dirs` in `FileConfig`.
- Least-loaded placement helper in `crates/broker/src/log_dir.rs`:
  - `count_partitions(dir)` — number of `topic-partition` subdirs.
  - `place_partition_dir(log_dirs, topic, partition)` — existing location
    wins; otherwise the dir with the fewest partitions (ties broken by
    order). Stateless: reads the filesystem on each call. Runs under the
    `DashMap` entry lock in `materialize_partition`, so concurrent
    materialization of one partition can never split across dirs.
  - `scan_all(log_dirs)` — discover every `(topic, partition, owning_dir)`
    across all dirs, first dir wins on duplicates (logged).
- Every partition materialization site routed through `all_log_dirs()` +
  `place_partition_dir`: startup recovery, the replicator supervisor's
  `materialize_partition`, the follower replicator's `ensure_local_partition`,
  `__consumer_offsets` bootstrap, and the `CreateTopics` / `CreatePartitions`
  / `InitProducerId` handlers. `DeleteTopics` resolves the actual dir before
  `remove_dir_all`. The disk-usage scanner (slice 43e) walks all dirs.
- `DescribeLogDirs` handler (api key 35, v1–5): one `Results` entry per
  configured dir, each listing the topics/partitions physically present with
  `partition_size` (sum of file bytes) and `offset_lag` (`LEO − HW`,
  clamped ≥ 0, for the loaded current log). `is_future_key = false`
  (no future logs this slice). Registered in the handler table + advertised
  in `ApiVersions` + added to the dispatch flexibility table.

### Out (deferred)

| Concern | Slice |
|---|---|
| `AlterReplicaLogDirs` move + future-log catch-up (KIP-113 write side) | 45b |
| `DescribeLogDirsResult.total_bytes` / `usable_bytes` (volume statvfs) | 45b |
| `kafka-reassign-partitions` `log_dirs` per-replica field | 45b |
| Offline-log-dir handling / `KAFKA_STORAGE_ERROR` on a dead disk | 45b |
| `Kafka.spec.storage` JBOD operator surface | 46 |
| `IsCordoned` (v5) semantics | future |

### Semantics for slice 45

- **Metadata stays put.** `__cluster_metadata` always lives on the primary
  `log_dir`; bootstrap-mode detection and the raft log are unchanged.
  JBOD spreads regular partition data only.
- **Placement is least-loaded by partition count**, matching Kafka's
  default `LogManager` round-robin-by-count. Existing on-disk location
  always wins, so restarts and re-materializations are idempotent.
- **Log-dir assignment is broker-local**, not in cluster metadata — exactly
  as in Kafka. There is no per-replica log-dir field in `PartitionRecord`.
- **`DescribeLogDirs` reflects the filesystem.** It scans each dir on each
  request rather than maintaining a separate registry, so it can never
  drift from what's on disk.

---

## 2. Config

`BrokerConfig` gains `extra_log_dirs: Vec<PathBuf>` (default empty) and:

```rust
pub fn all_log_dirs(&self) -> Vec<PathBuf> {
    let mut out = vec![self.log_dir.clone()];
    for d in &self.extra_log_dirs {
        if !out.contains(d) { out.push(d.clone()); }
    }
    out
}
```

`log_dir` keeps its meaning (primary + metadata + default data dir), so the
~100 existing references and tests that build configs via `..default()` /
`for_tests` are untouched. Only the two `BrokerConfig` constructors gain the
new field.

CLI: `--log-dirs a,b,c` → `extra_log_dirs`. TOML: `extra_log_dirs = ["..."]`.

---

## 3. Placement

`place_partition_dir` is the single chokepoint. All materialization paths
funnel through it, so JBOD behaviour is uniform:

```
existing(topic-partition) under any dir? -> reuse it
else                                     -> dir with min count_partitions(), ties by order
```

`materialize_partition` (the race-free `DashMap::entry` helper) calls it
inside the vacant arm, so placement and writer-spawn are atomic per key.

---

## 4. DescribeLogDirs handler

Per request (`topics: None` → all; `Some` → filter, empty partition list =
all of that topic):

1. For each dir in `all_log_dirs()`: `scan(dir)` → `(topic, partition)`.
2. `partition_size = sum_partition_dir(dir/topic-partition)`.
3. `offset_lag = max(0, LEO − HW)` when the partition is loaded, else 0.
4. Group by topic; emit one `DescribeLogDirsResult { error_code: 0,
   log_dir: <canonical abs path>, topics, .. }`. `total_bytes` /
   `usable_bytes` keep the generated `-1` ("unknown") default.

Backs `kafka-log-dirs --describe`.

---

## 5. Testing

### Pure-function unit tests (`crates/broker/src/log_dir.rs`)

- `count_partitions_ignores_non_partition_entries`, `_missing_dir_is_zero`
- `place_reuses_existing_location`
- `place_picks_least_loaded_then_order`
- `scan_all_merges_dirs_and_sorts`, `scan_all_first_dir_wins_on_duplicate`

### Handler unit tests (`describe_log_dirs.rs`)

- Filter `All` / `Topics` / empty-partition-list semantics.

### Broker integration test (`crates/broker/tests/jbod.rs`, new)

`partitions_spread_across_dirs_and_describe_log_dirs_reports_them`: boot a
single broker with two dirs, create a 6-partition topic, assert both dirs
hold partitions on disk, and that `DescribeLogDirs` (wire v4) returns one
result per dir whose union covers all 6 partitions.

### JVM acceptance test (`jvm_acceptance.rs`, `#[ignore]`)

`jvm_kafka_log_dirs_describe_reports_jbod_spread`: two-dir host broker,
`kafka-topics --create` (6 partitions), `kafka-log-dirs --describe`; assert
both absolute dir paths and the topic appear in the JVM tool's JSON.

---

## 6. File structure

```
crates/broker/src/
├── config.rs                 # MODIFIED — extra_log_dirs + all_log_dirs()
├── file_config.rs            # MODIFIED — extra_log_dirs TOML key
├── log_dir.rs                # MODIFIED — count_partitions / place_partition_dir / scan_all
├── bin/broker.rs             # MODIFIED — --log-dirs CLI
├── broker.rs                 # MODIFIED — scan_all startup + all_log_dirs threading
├── replicator_supervisor.rs  # MODIFIED — materialize_partition takes &[PathBuf]
├── replicator.rs             # MODIFIED — Config.log_dirs + place_partition_dir
├── disk_scanner/mod.rs       # MODIFIED — scan all dirs
├── coordinator/bootstrap.rs  # MODIFIED — offsets topic placement
├── handlers/mod.rs           # MODIFIED — register api 35
├── handlers/api_versions.rs  # MODIFIED — advertise api 35
├── handlers/describe_log_dirs.rs   # NEW — handler + filter unit tests
├── handlers/{create_topics,create_partitions,init_producer_id,delete_topics}.rs  # MODIFIED — all_log_dirs()
└── network/dispatch.rs       # MODIFIED — api 35 in flexibility table
crates/broker/tests/
├── jbod.rs                   # NEW — placement spread + DescribeLogDirs wire test
└── jvm_acceptance.rs         # MODIFIED — kafka-log-dirs --describe
```

No protocol regeneration: `DescribeLogDirs` / `AlterReplicaLogDirs` types
are already generated from the Kafka 4.3.0 schemas.
