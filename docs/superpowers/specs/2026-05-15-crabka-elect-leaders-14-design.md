# Slice 14: ElectLeaders + auto-rebalance — Design Spec

## Goal

Two pieces of operator-facing leader-election control on top of slice
10b's automatic-on-broker-death election:

1. **Manual `ElectLeaders` RPC** (api_key 43, KIP-460). PREFERRED type
   moves a partition's leader back to `replicas[0]` after operator
   intervention; UNCLEAN type allows election outside the ISR when
   every ISR member is dead. JVM `kafka-leader-election.sh` works.
2. **Auto preferred-replica rebalance**. A controller-side background
   task periodically scans every partition; when the current leader
   isn't the preferred replica AND the preferred is alive + in ISR,
   schedules a PREFERRED election. Driven by Kafka's standard
   `auto.leader.rebalance.enable` / `leader.imbalance.*` knobs.

Out of scope for slice 14: manual partition reassignment (separate
follow-up slice), quotas, log compaction, KIP-841 partition-split
force-elect, operator-supplied preferred-replica overrides,
delegation-token auth for the operator client.

## Background

Slice 10b shipped automatic leader re-election when a broker dies
(`crates/broker/src/leader_election.rs::on_broker_dead`). After
recovery, the partition stays with its emergency leader forever —
there's no path to return leadership to the originally-preferred
replica. Operators today must restart the new leader to force an
election, which is heavy-handed.

Slice 14 fixes that by adding (a) an explicit operator-triggered RPC
matching Kafka's standard `ElectLeaders` (api_key 43) and (b) a
background ticker that calls the same algorithm on a schedule. Both
paths converge on a single pure function (`select_new_leader_for_
partition`) that produces a new `PartitionRecord` to submit through
the existing raft pipeline. Existing replicator + leader-epoch machinery
from slice 8/10b handles the cluster-wide cutover.

## Architecture

### Election algorithm

A pure function in `crates/broker/src/leader_election.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionType { Preferred, Unclean }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectError {
    UnknownTopicOrPartition,
    PreferredAlreadyLeader,
    PreferredNotInIsr,
    PreferredNotAlive,
    NoEligibleReplica,
    NotControllerLeader,
}

pub(crate) fn select_new_leader_for_partition(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
    topic: &str,
    partition: i32,
    election: ElectionType,
) -> Result<PartitionRecord, ElectError>;
```

PREFERRED:
1. Look up the partition. Missing → `UnknownTopicOrPartition`.
2. `preferred = partition.replicas.first()`. Missing → `UnknownTopicOrPartition`.
3. `partition.leader == preferred` → `PreferredAlreadyLeader`.
4. `partition.isr.contains(preferred)` → false → `PreferredNotInIsr`.
5. `liveness.is_alive(preferred)` → false → `PreferredNotAlive`.
6. Build `PartitionRecord { leader: preferred, leader_epoch: pr.leader_epoch + 1, ..pr.clone() }`.

UNCLEAN:
1. Look up the partition. Missing → `UnknownTopicOrPartition`.
2. Any ISR member alive → `PreferredAlreadyLeader` (Kafka calls this
   `ELECTION_NOT_NEEDED` on the wire; the algorithm uses one variant for
   both "not needed" cases).
3. Find first alive replica from `partition.replicas`. None → `NoEligibleReplica`.
4. Build `PartitionRecord { leader: new_leader, isr: vec![new_leader],
   leader_epoch: pr.leader_epoch + 1, ..pr.clone() }`. ISR shrinks to
   just the new leader — old ISR members rejoin via the existing
   replicator catch-up flow once they're back online.

### Wire handler

`crates/broker/src/handlers/elect_leaders.rs` (new). Pattern matches
slice 13's ACL handlers:

1. Authorize `Alter` on `Cluster("kafka-cluster")` →
   `CLUSTER_AUTHORIZATION_FAILED (31)` on every per-partition row on
   Deny.
2. Decode `election_type: i8` (0 = PREFERRED, 1 = UNCLEAN).
   Anything else → whole-request `INVALID_REQUEST (42)`.
3. Resolve target set:
   - `topic_partitions = None` → every partition in the image.
   - `Some([{ topic, partitions: [] }])` → every partition of that
     topic.
   - explicit list → exact set.
4. For each target, call `select_new_leader_for_partition`. On Ok,
   queue the new `PartitionRecord`. On Err, record the wire error
   code from the mapping table below.
5. Submit queued records via `controller.submit_change(records)`.
   Submit failure → mark every queued row with `COORDINATOR_NOT_
   AVAILABLE (15)`.
6. Build per-partition response rows.

Inline-intercept dispatch (same slice-13 pattern — the handler needs
`&Principal` + `&SocketAddr` for the ACL check, which the static
`HandlerTable` can't carry).

### Auto-rebalance ticker

`crates/broker/src/leader_rebalance.rs` (new, ~120 lines). Spawned from
`Broker::start` when `auto_leader_rebalance_enable` is true.

```rust
pub(crate) async fn run(
    controller: Arc<ControllerHandle>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
);
```

Per tick:
1. If not controller leader → skip silently.
2. `image = controller.current_image()`.
3. For every `(topic, partition)`, call
   `select_new_leader_for_partition(image, liveness, ..., Preferred)`.
4. Collect successes; compute `pct = imbalanced * 100 / total`.
5. If `pct < cfg.imbalance_threshold_pct` → defer.
6. Else → batched `controller.submit_change(to_submit)`. Failures
   logged at `warn`.

### Wire error-code map

| Algorithm result | Wire code |
|---|---|
| `Ok(new_pr)` (submitted, committed) | `0` |
| `Err(UnknownTopicOrPartition)` | `UNKNOWN_TOPIC_OR_PARTITION (3)` |
| `Err(PreferredAlreadyLeader)` | `ELECTION_NOT_NEEDED (84)` |
| `Err(PreferredNotInIsr)` | `PREFERRED_LEADER_NOT_AVAILABLE (80)` |
| `Err(PreferredNotAlive)` | `PREFERRED_LEADER_NOT_AVAILABLE (80)` |
| `Err(NoEligibleReplica)` | `ELIGIBLE_LEADERS_NOT_AVAILABLE (81)` |
| `Err(NotControllerLeader)` | `COORDINATOR_NOT_AVAILABLE (15)` |
| Authorization denied | `CLUSTER_AUTHORIZATION_FAILED (31)` |
| Unknown election_type discriminant | `INVALID_REQUEST (42)` |

## Components

### `crabka-broker/src/handlers/elect_leaders.rs` (new, ~150 lines)

Decode `ElectLeadersRequest`, authorize Cluster Alter, drive
`select_new_leader_for_partition` per target, submit, build response.
Mirrors the shape of slice 13's `create_acls`/`delete_acls` handlers.

### `crabka-broker/src/leader_election.rs` (extended)

Slice 10b's `on_broker_dead` stays. Add:
- `pub(crate) enum ElectionType { Preferred, Unclean }`
- `pub(crate) enum ElectError { … }`
- `pub(crate) fn select_new_leader_for_partition(…) -> Result<PartitionRecord, ElectError>`

8 unit tests covering the algorithm (matrix on PREFERRED + UNCLEAN
paths, ISR + liveness + unknown-topic cases).

### `crabka-broker/src/leader_rebalance.rs` (new, ~120 lines)

`run` (the spawned task) + `rebalance_tick` (the pure-ish per-tick
logic, takes a `&dyn ControllerLike` trait object so tests can mock).
2 unit tests on `rebalance_tick`: below-threshold no-op, above-
threshold submits exact set.

### `crabka-broker/src/config.rs` (extended)

Three new fields on `BrokerConfig`:

```rust
pub auto_leader_rebalance_enable: bool,           // default true (matches Kafka)
pub leader_imbalance_check_interval_secs: u64,    // default 300 (matches Kafka)
pub leader_imbalance_per_broker_percentage: u32,  // default 10 (matches Kafka)
```

`BrokerConfig::for_tests`: defaults
`auto_leader_rebalance_enable = false` so slice-10b multi-broker tests
don't see surprise re-elections triggered by the background ticker.
Production `Default` keeps `true`.

`validate()`:
- `leader_imbalance_check_interval_secs == 0` →
  `BrokerError::InvalidLeaderRebalanceInterval { value: 0 }`.
- `leader_imbalance_per_broker_percentage > 100` →
  `BrokerError::InvalidLeaderRebalanceThreshold { value }`.

### `crabka-broker/src/broker.rs` (extended)

`Broker::start` spawns the rebalance task when
`config.auto_leader_rebalance_enable` AND the broker is configured to
participate in the controller quorum. Cancellation via the existing
`shutdown` token.

### `crabka-broker/src/codes.rs` (extended)

```rust
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 81;
pub const ELECTION_NOT_NEEDED: i16 = 84;
```

`UNKNOWN_TOPIC_OR_PARTITION (3)`, `COORDINATOR_NOT_AVAILABLE (15)`,
`CLUSTER_AUTHORIZATION_FAILED (31)`, `INVALID_REQUEST (42)` are
already defined.

### `crabka-broker/src/error.rs` (extended)

Two new `BrokerError` variants for config validation. If a sibling
`from_broker_error` does exhaustive matching, add map arms.

### Dispatch + api_versions

- `network/dispatch.rs::handler_body_flexible`: `(43, v) => v >= 2`.
- `handlers/api_versions.rs::supported_apis`: api_key 43 with the
  version range from the generated `ElectLeadersRequest` constants.

### Wire types

Already in `crates/protocol/generated/ElectLeaders{Request,Response}.owned.rs`.

## Data flow

### Operator-triggered PREFERRED election

1. Operator: `kafka-leader-election --election-type preferred --topic foo --partition 0 --bootstrap-server $BS --command-config admin.properties`.
2. JVM admin client sends ApiVersions, SaslHandshake/Authenticate
   (admin/PLAIN, super-user), then ElectLeaders v2:
   `election_type = 0`, `topic_partitions = Some([{ "foo", [0] }])`.
3. `handle_elect_leaders_frame` runs Cluster Alter authorize → admin
   is super-user → ALLOW.
4. Decode election_type = Preferred. Resolve target = `[("foo", 0)]`.
5. `select_new_leader_for_partition` for `("foo", 0)`:
   - `partition.leader = 2`, `replicas = [1, 2, 3]`, `isr = [1, 2, 3]`.
   - Preferred = 1, ≠ leader, in ISR, alive → build new `PartitionRecord
     { leader: 1, leader_epoch += 1, … }`.
6. Handler submits the new record via `controller.submit_change`.
7. Response: `error_code = 0` for the partition row.
8. Existing slice-10b replicator + leader-epoch flow takes care of the
   cluster-wide cutover.

### UNCLEAN election (all ISR dead)

1. Operator: `kafka-leader-election --election-type unclean ...`.
   `election_type = 1`.
2. Authorize passes.
3. `select_new_leader_for_partition`:
   - `isr = [1]`, broker 1 dead. No ISR member alive → proceed.
   - Brokers 2, 3 alive (out of ISR). First alive replica → 2.
   - Build `PartitionRecord { leader: 2, isr: [2], leader_epoch += 1, … }`.
4. Submit → response `error_code = 0`. Operator accepted data-loss risk.

### Auto-rebalance tick

1. Wakes every `leader_imbalance_check_interval_secs` (300s default).
2. If not controller leader → skip.
3. Iterate every partition in the image; collect those where
   `select_new_leader_for_partition(Preferred)` returns Ok.
4. Compute `pct`. Below threshold → defer.
5. Above → batched submit. Failures logged `warn`, retried next tick.

### Authorization-denied request

1. alice (not super-user, no Cluster Alter ACL) sends ElectLeaders.
2. Authorize preamble denies → response carries per-partition
   `error_code = 31` on every requested row. Connection stays open.

### Fetch-all sentinel

`topic_partitions = None` → enumerate every partition in the image.
Partitions where election isn't needed get `ELECTION_NOT_NEEDED (84)`;
failures get the appropriate code; successes get `0`.

### Submit-then-apply lag

`controller.submit_change` returns after the raft commit but before
followers' replicators have re-targeted. Slice 10b's leader-epoch +
replication-restart flow handles the cluster-wide cutover
asynchronously. The wire response is `0` on commit, not on full
convergence — same as every other slice-7+ admin RPC.

## Error handling

### Wire-level error codes (per-partition)

Mapping table reproduced from Architecture. The handler's response-
building loop maps `Result<PartitionRecord, ElectError>` → wire code
via a small match.

### Whole-request errors

| Scenario | Wire response |
|---|---|
| Caller not authorized | `CLUSTER_AUTHORIZATION_FAILED (31)` on every per-partition row |
| Unknown election_type discriminant | `INVALID_REQUEST (42)` per-row (or whole-response if the wire schema has a top-level error_code) |
| Raft submit failure | `COORDINATOR_NOT_AVAILABLE (15)` on rows queued but not committed |

### Algorithm errors

Pure logic, no panic paths. `replicas.first()` on an empty vec returns
`None` → `UnknownTopicOrPartition` (degenerate metadata state, should
not arise in practice; the handler is defensive).

### Auto-rebalance failure handling

| Scenario | Behavior |
|---|---|
| Not controller leader at tick | Skip tick silently |
| Transient `submit_change` error | `warn!` log; next tick reassesses |
| Stale liveness (broker dies mid-tick) | New `PartitionRecord` may name a now-dead leader; slice-10b's `on_broker_dead` re-elects. Self-healing |
| `auto_leader_rebalance_enable = false` | Task never spawns |
| Zero check interval | Rejected at startup |

### Config validation (startup, fatal)

```rust
pub enum BrokerError {
    // ...
    InvalidLeaderRebalanceInterval { value: u64 },
    InvalidLeaderRebalanceThreshold { value: u32 },
}
```

Both produce broker-startup failures with a clear message.

### Race: manual + auto rebalance for the same partition

Both submit through `controller.submit_change`. openraft serializes;
the second observer either confirms a no-op (same leader chosen) or
supersedes (leader_epoch bumped twice). Replicators see the change
via the existing slice-10b flow. No special locking.

### Logging

| Event | Level |
|---|---|
| Successful election | `info!(topic, partition, new_leader, "elected leader")` |
| `ELECTION_NOT_NEEDED` | `debug!` (operator polling routinely) |
| `PREFERRED_LEADER_NOT_AVAILABLE` / `ELIGIBLE_LEADERS_NOT_AVAILABLE` | `info!` |
| UNCLEAN election succeeded | `warn!(topic, partition, new_leader, isr_dropped, "UNCLEAN election — potential data loss")` |
| Auto-rebalance tick committed N records | `info!(count, "auto-rebalance")` |
| Auto-rebalance tick below threshold | `debug!` |

UNCLEAN-success at `warn` is deliberate — operators need visibility
into data-loss-bearing decisions.

## Testing

### Unit tests — `crabka-broker::leader_election`

8 tests on `select_new_leader_for_partition`:

- `preferred_happy_path`
- `preferred_already_leader` → `Err(PreferredAlreadyLeader)`
- `preferred_not_in_isr`
- `preferred_not_alive`
- `unclean_happy_path` (no live ISR, alive out-of-ISR replica picked, ISR shrinks to `[new_leader]`)
- `unclean_no_alive_replicas`
- `unclean_isr_member_alive_returns_election_not_needed`
- `unknown_topic_returns_error`

### Unit tests — `crabka-broker::leader_rebalance`

2 tests on `rebalance_tick` via a small `ControllerLike` trait mock
that captures submitted records in a `Mutex<Vec<MetadataRecord>>`:

- `below_threshold_skips_submit` (100 partitions, 5 imbalanced, threshold 10% → no submit)
- `above_threshold_submits_imbalanced_set` (100 partitions, 20 imbalanced, threshold 10% → exactly 20 records)

### Unit tests — `crabka-broker::config`

- `auto_leader_rebalance_defaults_to_true_in_default`
- `auto_leader_rebalance_defaults_to_false_in_for_tests`
- `rebalance_zero_interval_rejected_by_validate`
- `rebalance_threshold_over_100_rejected_by_validate`

### Integration tests — `crates/broker/tests/elect_leaders.rs` (new, no Docker)

Gated `#![cfg(not(target_os = "windows"))]`:

- `preferred_election_via_wire_returns_success` — 2-broker cluster, rf=2 topic. Kill broker 1 → broker 2 leads. Revive 1. Send ElectLeaders Preferred via wire (admin/PLAIN super-user). Assert `error_code=0`, partition.leader == 1 on both brokers.
- `unclean_election_via_wire_picks_alive_replica` — 2-broker cluster, rf=2. Kill broker 1, wait for ISR shrink, kill broker 2, revive broker 1 only. Send ElectLeaders Unclean. Assert `error_code=0`, partition.leader == 1, partition.isr == [1].
- `non_super_user_without_acl_denied` — single-broker SASL_PLAINTEXT, super-user admin, alice has PLAIN creds but no ACLs (one unrelated ACL exists to disable compat shim). Auth as alice, send ElectLeaders. Assert per-partition `error_code=31`.
- `auto_rebalance_restores_preferred_leader` — 2-broker cluster with `auto_leader_rebalance_enable=true`, `leader_imbalance_check_interval_secs=1`, threshold=0. Create rf=2 topic. Kill broker 1 → broker 2 takes over. Revive broker 1. Within ~3s, partition.leader == 1 again. Poll up to 10s.

### JVM acceptance — `crates/broker/tests/jvm_acceptance.rs`

- `jvm_kafka_leader_election_preferred` — 2-broker SASL_PLAINTEXT cluster, super-user admin. rf=2 topic. Kill broker 1 → broker 2 leads. Revive broker 1. Run via Docker:
  ```
  kafka-leader-election --election-type preferred --topic foo --partition 0 \
                        --bootstrap-server $BOOTSTRAP --command-config /admin.properties
  ```
  Assert command exits 0 and broker 1 is leader again. Cp-kafka 7.5 image.

UNCLEAN election via JVM is more invasive (double-kill orchestration);
the Rust-driven `unclean_election_via_wire_picks_alive_replica` covers
the wire path adequately.

### Regression guards

- Slice 10b automatic on-broker-dead election tests pass unchanged.
  `auto_leader_rebalance_enable=false` in `BrokerConfig::for_tests`
  prevents surprise re-elections from the background ticker.
- Slice 13 ACL tests pass. ElectLeaders is gated on Cluster Alter;
  slice-13 test setups configure super-users, so the bypass keeps them
  working.
- Slice 11 `kafka-cluster --describe` JVM test still passes (no
  Metadata response shape change).

### Out of scope for tests

- Concurrent manual + auto-rebalance race (openraft serialization
  covered elsewhere; not slice-specific).
- 3-broker UNCLEAN election (2-broker test sufficient; algorithm isn't
  scale-dependent).
- Rebalance under network partition (slice 10b's partition tests cover
  that flow).
- Performance benchmarks for the rebalance ticker.

## Wire-protocol additions

| api_key | Name | Versions |
|---------|------|----------|
| 43 | ElectLeaders | v0–v2 (verify against generated constants) |

`ElectLeadersRequest`/`Response` schemas already generated in
`crates/protocol/generated/`. Flexible-body table + `supported_apis`
register the api_key.

## Out of scope

- Manual partition reassignment (`AlterPartitionReassignments` / `ListPartitionReassignments`) — separate slice.
- Quotas.
- Log compaction.
- KIP-841 force-elect on partition split.
- Operator-supplied preferred-replica override.
- `auto.leader.rebalance.enable=false` config-string parsing (the field
  exists; persisted-config loading is its own slice).
- Per-controller-affinity rebalance scheduling.
- Audit log destinations beyond `tracing`.
