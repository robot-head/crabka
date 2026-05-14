# Slice 11: Admin handlers — Design Spec

## Goal

Add the operator-facing admin handlers to `crabka-broker` so the JVM
`kafka-*.sh` tools work against a Rust broker without skipping or
falling back to JVM brokers. No new crate. No Rust CLI (deferred to a
future slice). No ACLs or quotas (deferred).

Eight new request handlers, one new metadata record type, one small
plumbing change to the log layer so config edits take effect live.

## Background

Slices 1–10b shipped a broker that JVM clients can produce, consume,
and replicate against. Topic-create and config-describe work; topic-
config edits, partition expansion, group inspection, record trimming,
and cluster description do not. Operators reaching for any of
`kafka-configs.sh --alter`, `kafka-topics.sh --alter --partitions`,
`kafka-consumer-groups.sh --list/--describe/--delete`,
`kafka-delete-records.sh`, or `kafka-cluster.sh --describe` hit an
unimplemented `api_key` and the AdminClient errors.

The roadmap's Slice 11 ("all admin APIs + a `kafka-*.sh`-parity CLI in
Rust") is too wide for one spec. This slice is the operator-facing
broker side only; the Rust CLI is a separate future slice, and ACLs,
quotas, partition reassignments, and leader-election RPCs are explicit
non-goals.

## Architecture

Eight new handlers, registered in `handlers/mod.rs::build_table`:

```
ApiKey  Handler                  Routes through
─────   ──────────────────────   ───────────────────────────
33      AlterConfigs             controller.submit_change(V1TopicConfig)
44      IncrementalAlterConfigs  controller.submit_change(V1TopicConfig)
37      CreatePartitions         controller.submit_change(extra V1Partition)
21      DeleteRecords            partitions[(t,p)].writer_tx <- TrimToOffset
60      DescribeCluster          controller.current_image()
16      ListGroups               group_manager.list_groups()
15      DescribeGroups            group_manager.get(group_id)
42      DeleteGroups             group_manager.delete(group_id)
```

All read paths use existing in-memory state. All mutation paths route
through existing subsystems (`controller.submit_change` for raft-backed
changes; the partition writer actor for log trims).

### Mutable topic config record

New variant in `crabka_metadata::MetadataRecord`:

```rust
MetadataRecord::V1TopicConfig {
    topic: String,
    overrides: BTreeMap<String, String>,
}
```

`MetadataImage::apply` validates (key must be in the whitelist; topic
must exist) and stores into a new field
`topic_configs: HashMap<String, BTreeMap<String, String>>`. Last-write-
wins per topic: each `V1TopicConfig` record replaces the prior map
entirely. Merging happens at the handler — the record is authoritative
target state.

New accessor `MetadataImage::topic_config(name) -> Option<&BTreeMap>`.

`MetadataRecord` is openraft-serialized via `serde_wincode`. Adding an
enum variant is backwards-compatible: existing on-disk records decode
unchanged because variant discrimination is by-tag.

### Config whitelist

Six keys, one source of truth in `crates/broker/src/config_keys.rs`:

| Key | Status |
|---|---|
| `retention.ms` | honored — propagates live to `Log.config.retention_ms` |
| `retention.bytes` | honored — propagates live to `Log.config.retention_bytes` |
| `segment.bytes` | honored — propagates live to `Log.config.segment_bytes` |
| `cleanup.policy=delete` | accepted as no-op (default) |
| `cleanup.policy=compact` | rejected — log compaction unimplemented |
| `compression.type=producer` | accepted as no-op (default — broker pass-through) |
| `compression.type=<other>` | rejected — broker-side recompression unimplemented |
| `min.insync.replicas=N` | accepted as no-op — see below |

`min.insync.replicas` is accepted but not yet enforced: today's
`acks=-1` produce blocks on full-ISR HW (which is strictly stronger
than `min.insync.replicas` for any N ≤ |ISR|), so the operator's
intent is satisfied. Documented in the spec; honoring it as a separate
threshold is deferred to a future hardening pass.

Every other key — `INVALID_CONFIG` with the offending key in
`error_message`. No silent no-ops.

### Live propagation

`crabka_log::Log` currently owns `LogConfig` by value. Wrap in
`Arc<RwLock<LogConfig>>` so the broker can swap fields while
retention/roll loops keep running. Retention and segment-roll checks
already snapshot the config at the top of each iteration; the lock is
held for trivially short windows.

```rust
// crabka_log
pub struct Log {
    config: Arc<RwLock<LogConfig>>,
    // ...
}

impl Log {
    pub fn set_config(&self, new: LogConfig) {
        *self.config.write().unwrap() = new;
    }

    fn config(&self) -> LogConfig {
        self.config.read().unwrap().clone()
    }
}
```

`LogConfig` is `Clone` and small; cloning per iteration is free.

`ReplicatorSupervisor::reconcile` gains one extra loop. After the
existing partition reconcile, for each locally-hosted partition, merge
the topic's overrides with `LogConfig::default()` and push to the
writer:

```rust
for (topic, partition) in desired_local_set(self.node_id, image) {
    let overrides = image.topic_config(&topic).cloned().unwrap_or_default();
    if let Some(part) = self.partitions.get(&(topic.clone(), partition)) {
        let _ = part.apply_log_config_overrides(&overrides).await;
    }
}
```

`Partition::apply_log_config_overrides` sends one new writer message:

```rust
enum WriterMessage {
    // ... existing variants
    SetLogConfig { config: LogConfig, ack: oneshot::Sender<()> },
}
```

The writer task calls `log.set_config(...)`. Goes through the actor
so it serializes with appends — no in-flight `RecordBatch` sees a
half-applied config.

Idempotent: pushing the same `LogConfig` twice is a noop write.

### `DeleteRecords` plumbing

New writer message:

```rust
WriterMessage::TrimToOffset {
    new_start: i64,
    ack: oneshot::Sender<Result<i64, BrokerError>>,
}
```

Delegates to `Log::trim_to(offset)` which already exists for retention.
Returns the actual new `log_start_offset` (clamped: caller asks for an
offset > LEO, we trim to LEO, return LEO).

`DeleteRecords` is leader-only and **not** replicated through raft —
it's a local segment trim. Followers learn about the new
`log_start_offset` on the next Fetch (the leader reports it in the
fetch response), and their replicator's existing `OFFSET_OUT_OF_RANGE`
recovery path picks it up. Same pattern Apache Kafka uses.

### Group manager accessors

`GroupManager` already tracks the state needed for ListGroups/
DescribeGroups; we just expose it:

```rust
impl GroupManager {
    pub fn list_groups(&self) -> Vec<GroupSnapshot>;
    pub fn get(&self, group_id: &str) -> Option<GroupSnapshot>;
    pub fn delete(&self, group_id: &str) -> Result<(), BrokerError>;
}

pub struct GroupSnapshot {
    pub group_id: String,
    pub state: GroupState,
    pub protocol_type: String,
    pub members: Vec<MemberSnapshot>,
}
```

`delete` writes a tombstone to `__consumer_offsets` via the existing
group-commit path, then drops the in-memory entry.

## Components

```
crates/metadata/src/
├── record.rs                       # MODIFIED — V1TopicConfig variant
└── image.rs                        # MODIFIED — topic_configs field + apply

crates/log/src/
├── config.rs                       # MODIFIED — (no shape change)
└── log.rs                          # MODIFIED — Arc<RwLock<LogConfig>>, set_config()

crates/broker/src/
├── config_keys.rs                  # NEW — validate(), apply_to_log_config()
├── partition.rs                    # MODIFIED — apply_log_config_overrides()
├── partition_writer.rs             # MODIFIED — SetLogConfig, TrimToOffset arms
├── replicator_supervisor.rs        # MODIFIED — push overrides per reconcile
├── coordinator.rs                  # MODIFIED — GroupManager::{list_groups,get,delete}
└── handlers/
    ├── mod.rs                      # MODIFIED — register 8 new handlers
    ├── alter_configs.rs            # NEW — api_key 33
    ├── incremental_alter_configs.rs# NEW — api_key 44
    ├── create_partitions.rs        # NEW — api_key 37
    ├── delete_records.rs           # NEW — api_key 21
    ├── describe_cluster.rs         # NEW — api_key 60
    ├── list_groups.rs              # NEW — api_key 16
    ├── describe_group.rs           # NEW — api_key 15
    └── delete_groups.rs            # NEW — api_key 42

crates/broker/tests/
├── admin_handlers.rs               # NEW — broker-side integration tests
└── jvm_acceptance.rs               # MODIFIED — 5 new JVM CLI tests
```

## Data flow

### AlterConfigs / IncrementalAlterConfigs

```
JVM client                                Broker (controller leader)
─────────                                 ────────────────────────────
AlterConfigsRequest                  ──>  handlers::alter_configs::handle
  resources: [Topic("t", configs)]        │
                                          ├─ for each resource:
                                          │   ├─ type == TOPIC?  else INVALID_RESOURCE_TYPE
                                          │   ├─ image.topic("t").exist?  else UNKNOWN_TOPIC_OR_PARTITION
                                          │   ├─ for (k, v): config_keys::validate(k, v)
                                          │   │     err → INVALID_CONFIG with key+reason
                                          │   └─ build BTreeMap
                                          │       full = AlterConfigs (overwrite)
                                          │       merge = IncrementalAlterConfigs (read current first)
                                          ├─ controller.submit_change(vec![V1TopicConfig{...}])
                                          └─ build response (per-resource error_code)

(meanwhile, all brokers reconcile the new image)
ReplicatorSupervisor::reconcile  ──>  for each local (t,p):
                                        partitions[(t,p)].apply_log_config_overrides(...)
                                          └─ WriterMessage::SetLogConfig{ ... }
                                                └─ log.set_config(new)   // RwLock swap

next retention/roll tick on that partition reads the new config.
```

`IncrementalAlterConfigs` differs from `AlterConfigs` only at the merge
step: it carries per-key `op` (SET/DELETE/APPEND/SUBTRACT) and merges
with the current `topic_configs.get(name)`. APPEND/SUBTRACT are list-
valued operations that none of our whitelisted keys use; we reject
them with `INVALID_CONFIG` for non-list keys (which is all of ours).

### CreatePartitions

```
JVM client                                Broker (controller leader)
─────────                                 ────────────────────────────
CreatePartitionsRequest               ─>  handlers::create_partitions::handle
  topics: [{"t", count: 5}]               │
                                          ├─ for each topic:
                                          │   ├─ existing = image.partition_count("t")
                                          │   ├─ count > existing?  else INVALID_PARTITIONS
                                          │   ├─ broker_count >= rf?  else INVALID_REPLICATION_FACTOR
                                          │   └─ build (existing..count).map(|p|
                                          │         V1Partition{round-robin replicas, isr=replicas})
                                          ├─ controller.submit_change(records)
                                          ├─ materialize new partitions on disk (where self in replicas)
                                          └─ build response
```

Reuses the existing `round_robin_replicas` helper from
`handlers/create_topics.rs`. New partitions seed with `isr == replicas`
(matching CreateTopics).

### DeleteRecords

```
DeleteRecordsRequest                      Broker (partition leader)
─────────                                 ───────────────────────────
{topic, partition, offset: 50}            handlers::delete_records::handle
                                          │
                                          ├─ partitions[(t,p)]?      else UNKNOWN_TOPIC_OR_PARTITION
                                          ├─ part.current_leader == self?  else NOT_LEADER_OR_FOLLOWER
                                          ├─ 0 ≤ offset ≤ leo?       else OFFSET_OUT_OF_RANGE
                                          ├─ part.trim_to(offset) → new_log_start
                                          │   └─ WriterMessage::TrimToOffset
                                          │       └─ log.trim_to(offset)  // drops segments < offset
                                          └─ response{low_watermark: new_log_start}
```

Special offset `-1`: Kafka semantics is "delete up to high watermark".
Translate to `offset = high_watermark` before validation.

### Read-only handlers

`DescribeCluster`, `ListGroups`, `DescribeGroups` are pure projections
of in-memory state. No raft round-trip, no actor message.

### DeleteGroups

```
DeleteGroupsRequest    ──>  handlers::delete_groups::handle
  groups: ["g1"]            │
                            ├─ for g in groups:
                            │   ├─ group_manager.get(g)?  else GROUP_ID_NOT_FOUND
                            │   ├─ state in {Empty, Dead}?  else NON_EMPTY_GROUP
                            │   └─ group_manager.delete(g)
                            └─ build response
```

Group state lives in the replicated `__consumer_offsets` topic;
deletion writes a tombstone via the existing OffsetCommit-style path.

## Error handling

Standard guardrails:
- Non-controller broker on a controller-leader op → `NOT_CONTROLLER`
  (41). Java AdminClient retries against the new controller.
- All errors are per-resource / per-topic / per-partition / per-group.
  Partial-success responses are honest about which items succeeded.

| Condition | Code | Note |
|---|---|---|
| Non-topic resource (e.g., BROKER) | 35 `INVALID_RESOURCE_TYPE` | |
| Topic doesn't exist | 3 `UNKNOWN_TOPIC_OR_PARTITION` | |
| Config key not whitelisted | 40 `INVALID_CONFIG` | key in `error_message` |
| Config value rejected | 40 `INVALID_CONFIG` | reason in `error_message` |
| `CreatePartitions`: new ≤ existing | 37 `INVALID_PARTITIONS` | |
| `CreatePartitions`: rf > brokers | 38 `INVALID_REPLICATION_FACTOR` | |
| `DeleteRecords`: offset > LEO or < -1 | 1 `OFFSET_OUT_OF_RANGE` | |
| `DeleteRecords`: not leader | 6 `NOT_LEADER_OR_FOLLOWER` | |
| `DeleteGroups`: group not found | 69 `GROUP_ID_NOT_FOUND` | |
| `DeleteGroups`: group not Empty/Dead | 68 `NON_EMPTY_GROUP` | |
| `controller.submit_change` not leader | 41 `NOT_CONTROLLER` | |
| Anything else | -1 `UNKNOWN_SERVER_ERROR` | |

All codes already exist in `crabka_broker::codes`. No new variants.

## Testing

Three layers, all required for the acceptance gate.

### Unit tests (in-file `mod tests`)

- `config_keys`: each whitelisted key accepts valid values, rejects
  invalid (`cleanup.policy=compact` rejected, `=delete` accepted).
  Unknown keys rejected.
- Each handler file: happy path + each error path against an in-memory
  fixture. Pattern matches the existing `handlers::create_topics::tests`.
- `Log::set_config`: swap is atomic; retention reads new value on next
  iteration.
- `GroupManager::{list,get,delete}`: state transitions on synthetic
  group lifecycle.

### Broker integration (`crates/broker/tests/admin_handlers.rs`, new)

- AlterConfigs round-trip: submit `retention.ms=60000` override, wait
  for metadata-image change, verify the leader's open partition's
  `log.config.retention_ms` is `Some(60s)`.
- IncrementalAlterConfigs DELETE: alter then delete one key, verify
  the partition's effective config reverts to default for that key
  while keeping other overrides.
- CreatePartitions: extend 1-partition topic to 3 partitions, verify
  three new partition directories materialize on disk, verify replica
  placement is round-robin.
- DeleteRecords: produce 100 records, trim to offset 50, verify
  `log_start_offset == 50`, verify subsequent fetch from offset 0
  returns `OFFSET_OUT_OF_RANGE`.
- DeleteRecords offset=-1: produce, trim with -1, verify new
  `log_start_offset == high_watermark`.
- DescribeCluster: response includes all registered brokers and the
  current controller_id.
- ListGroups / DescribeGroups: spin up a 2-member consumer group, list,
  describe (both members shown with assignment), verify state machine.
- DeleteGroups: empty group deletes; live group → `NON_EMPTY_GROUP`.

### JVM acceptance (`crates/broker/tests/jvm_acceptance.rs`, batch 5)

Five new tests, alongside the existing 9, under the same
`broker-jvm-acceptance` CI job:

1. `kafka-configs --alter --add-config retention.ms=60000 --topic t`
   then `--describe` shows the override.
2. `kafka-topics --alter --topic t --partitions 5` then `--describe`
   shows 5 partitions.
3. `kafka-delete-records --offset-json-file <(...)` then
   `kafka-console-consumer --from-beginning` starts at the trim point.
4. `kafka-consumer-groups --list` shows a live group;
   `--describe` shows members + offsets.
5. `kafka-cluster --describe` lists all brokers and the controller.

All five run against a single-broker cluster (existing pattern for the
non-failover JVM tests). DeleteRecords with replication is exercised
in the broker-integration layer to keep JVM test runtime bounded.

## Matches Kafka how

Every handler follows the wire protocol exactly — request/response
shape, error codes, partial-success semantics. The AdminClient on the
JVM side can't tell whether it's talking to a Rust broker or a JVM
broker for any of these calls.

| Apache Kafka | Crabka (this slice) |
|---|---|
| `kafka-configs --alter` → AlterConfigs/IncrementalAlterConfigs | Same handlers, topic-only whitelist |
| Topic config stored in `ZK /config/topics/<t>` (ZK mode) or `__cluster_metadata` (KRaft) | Stored as `V1TopicConfig` in the same raft-backed metadata log |
| Config takes effect at next retention/roll tick | Same — `Log.config` is swappable, retention/roll re-reads each iteration |
| `kafka-topics --alter --partitions` → CreatePartitions | Same handler, same round-robin placement |
| `kafka-delete-records` → DeleteRecords | Same handler, same leader-only + Fetch-driven follower convergence |
| `kafka-consumer-groups --list/--describe/--delete` | Same handlers, same state machine |

## Acceptance gate

1. `cargo test --workspace` green on ubuntu-latest, macos-latest,
   windows-latest at toolchain 1.95.0.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo fmt --all -- --check` clean.
4. All 14 JVM acceptance tests pass (9 existing + 5 new) under
   `broker-jvm-acceptance`.
5. Live propagation verified: change `retention.ms` on a topic with
   open partitions, retention loop honors it on next tick without
   broker restart. (Broker-integration test.)

## Out of scope

- Rust `crabka-cli` (kafka-*.sh-parity command-line tool). Separate
  future slice.
- ACLs (CreateAcls/DescribeAcls/DeleteAcls, api_keys 30/29/31).
  Separate slice; needs an authorizer interface first.
- Quotas (AlterClientQuotas/DescribeClientQuotas, api_keys 49/48).
  Separate slice.
- Partition reassignments (AlterPartitionReassignments/
  ListPartitionReassignments, api_keys 45/46). Significant scope;
  separate slice.
- ElectLeaders (api_key 43). Defer until needed.
- Broker-level dynamic configs. Topic-level only for this slice.
- `cleanup.policy=compact` runtime support (log compaction). Big
  feature; separate slice.
- Broker-side compression recompression for
  `compression.type=<non-producer>`. Separate slice.
- Per-broker overrides for `min.insync.replicas`. Today's full-ISR HW
  gate is strictly stronger; honoring the lower threshold requires a
  separate decision.
- Auth / TLS. Separate slice (Slice 12 per master roadmap).
