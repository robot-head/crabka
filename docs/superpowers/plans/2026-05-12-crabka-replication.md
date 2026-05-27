# `crabka-replication` (slice 8, basic) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Multi-broker partition replication for Crabka. When `CreateTopics` runs with `replication_factor=N`, the controller assigns N replicas per partition via deterministic round-robin over registered brokers; each follower broker runs per-partition replication tasks that issue standard Kafka `Fetch` requests to the leader (with `replica_id` set) and appends received batches to its local `crabka-log`. After this slice, a 3-broker cluster with `replication_factor=3` has each partition's records on every replica's local disk, byte-compatible with what `kafka-dump-log` produces.

**Architecture:** Two new modules inside `crabka-broker`: `replicator.rs` (per-(topic, partition) fetch loop) and `replicator_supervisor.rs` (subscribes to `controller.watch_image()` and diffs the running task set on each metadata apply). `CreateTopics` handler grows a round-robin replica-placement step that reads `MetadataImage::brokers()` before submitting the records to the controller. The leader-side `Fetch` handler gets one branch on `replica_id` (follower vs consumer); in slice 8 both branches return the same bytes because HW tracking is deferred.

**Tech Stack:** Rust 1.95.0, tokio (sync, time, net, rt-multi-thread), `crabka-client-core::Client` for outbound Fetch requests, `crabka-log` for the local log, `crabka-protocol::owned::fetch_request::FetchRequest`, `tokio_util::sync::CancellationToken` for per-task lifecycle, `dashmap` for shared maps. No new workspace deps.

**Reference spec:** [`docs/superpowers/specs/2026-05-12-crabka-replication-design.md`](../specs/2026-05-12-crabka-replication-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/replication-plan` (this file). Implementation runs on `feature/replication` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/broker/
└── src/
    ├── handlers/
    │   ├── create_topics.rs               # MODIFIED — round-robin replica assignment from MetadataImage::brokers()
    │   └── fetch.rs                       # MODIFIED — branch on replica_id (follower vs consumer)
    ├── replicator.rs                      # NEW — per-(topic, partition) replication task
    ├── replicator_supervisor.rs           # NEW — subscribes to controller.watch_image(); diffs task set
    ├── broker.rs                          # MODIFIED — spawn supervisor in Broker::start; cancel in shutdown
    ├── error.rs                           # MODIFIED — add BrokerError::Replication
    └── lib.rs                             # MODIFIED — mod replicator + mod replicator_supervisor

crates/broker/tests/
├── replication.rs                         # NEW — multi-node in-process replication tests
└── jvm_acceptance.rs                      # MODIFIED — append three_node_replication_byte_compare
```

---

## Phase A — `CreateTopics` round-robin replica assignment

### Task 1: Round-robin assignment helper

**Files:**
- Modify: `crates/broker/src/handlers/create_topics.rs`

- [ ] **Step 1: Write the helper**

Append to `crates/broker/src/handlers/create_topics.rs` (above the `handle` fn):

```rust
/// Round-robin replica placement.
///
/// Given a sorted broker set `bs = [b0, b1, …, bk-1]` and a partition
/// count `P`, returns a `Vec<Vec<NodeId>>` of length `P`, where each
/// inner vec is `R = replication_factor` long. Partition `p`'s leader
/// is `bs[(p) % k]`; the remaining replicas are `bs[(p + i) % k]` for
/// `i in 1..R`. Caller must guarantee `R <= k` (else returns an empty
/// outer vec and the caller surfaces `INVALID_REPLICATION_FACTOR`).
fn round_robin_replicas(
    sorted_brokers: &[crabka_raft::NodeId],
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<Vec<crabka_raft::NodeId>> {
    let k = sorted_brokers.len();
    let r = usize::try_from(replication_factor).unwrap_or(0);
    if r == 0 || r > k {
        return Vec::new();
    }
    let p_count = usize::try_from(num_partitions).unwrap_or(0);
    (0..p_count)
        .map(|p| {
            (0..r)
                .map(|i| sorted_brokers[(p + i) % k])
                .collect::<Vec<_>>()
        })
        .collect()
}
```

- [ ] **Step 2: Test scaffolding**

In the same file's existing `#[cfg(test)] mod tests` (add one if missing):

```rust
#[cfg(test)]
mod replica_assignment_tests {
    use super::round_robin_replicas;

    #[test]
    fn three_brokers_three_partitions_rf_three() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 3, 3);
        // Every broker should lead exactly one partition.
        let leaders: Vec<_> = out.iter().map(|r| r[0]).collect();
        let mut sorted = leaders.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2, 3]);
        // Each partition has all three brokers as replicas.
        for replicas in &out {
            let mut s = replicas.clone();
            s.sort_unstable();
            assert_eq!(s, vec![1, 2, 3]);
        }
    }

    #[test]
    fn offset_per_partition_means_distinct_leaders() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 3, 1);
        assert_eq!(out[0], vec![1]);
        assert_eq!(out[1], vec![2]);
        assert_eq!(out[2], vec![3]);
    }

    #[test]
    fn rf_too_high_returns_empty() {
        let bs = vec![1u64, 2, 3];
        let out = round_robin_replicas(&bs, 1, 5);
        assert!(out.is_empty());
    }

    #[test]
    fn rf_one_single_broker_preserves_slice7_shape() {
        let bs = vec![1u64];
        let out = round_robin_replicas(&bs, 2, 1);
        assert_eq!(out, vec![vec![1u64], vec![1u64]]);
    }
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-broker --lib replica_assignment_tests
git add crates/broker/src/handlers/create_topics.rs
git commit -m "$(cat <<'EOF'
feat(broker): round-robin replica placement helper

Pure helper that the CreateTopics handler will call before submitting
partition records to the controller. Deterministic on the sorted
broker set. Returns empty when replication_factor > broker count so
the caller can surface INVALID_REPLICATION_FACTOR (38).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Wire round-robin into the CreateTopics handler

**Files:**
- Modify: `crates/broker/src/handlers/create_topics.rs`

- [ ] **Step 1: Recon current shape**

```bash
grep -n "fn handle\|replicas\|leader: node_id\|num_partitions\|replication_factor\|INVALID_REPLICATION_FACTOR" crates/broker/src/handlers/create_topics.rs
```

Today the handler hard-codes `replicas: vec![node_id], leader: node_id` and ignores the request's `replication_factor`. After this task, it computes `replicas` via the helper above using the current broker set.

- [ ] **Step 2: Replace the placement logic**

Find the loop that builds `MetadataRecord::V1Partition` records (per Task 17 of the slice-7 plan, this loop runs after the `V1Topic` record is appended). Replace its inner body — specifically the `replicas`/`leader` construction — with:

```rust
// Read the current broker set from the controller's image; sort by
// node_id for determinism.
let image = controller.current_image();
let mut sorted_brokers: Vec<crabka_raft::NodeId> =
    image.brokers().map(|b| b.node_id).collect();
sorted_brokers.sort_unstable();

let assignments = round_robin_replicas(
    &sorted_brokers,
    topic.num_partitions,
    topic.replication_factor,
);

if assignments.is_empty() {
    // RF > broker count. Surface INVALID_REPLICATION_FACTOR per Apache
    // Kafka semantics.
    topic_results.push(CreatableTopicResult {
        name: topic.name.clone(),
        topic_id: ProtoUuid([0u8; 16]),
        error_code: codes::INVALID_REPLICATION_FACTOR,
        error_message: None,
        ..Default::default()
    });
    continue;
}

let topic_id = Uuid::new_v4();
let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
    name: topic.name.clone(),
    topic_id,
    partitions: topic.num_partitions,
    replication_factor: topic.replication_factor,
})];
for (p, replicas) in assignments.iter().enumerate() {
    let p_i32 = i32::try_from(p).unwrap_or(0);
    records.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.name.clone(),
        partition: p_i32,
        leader: replicas[0],
        replicas: replicas.clone(),
        isr: replicas.clone(),
    }));
}
let result = controller.submit_change(records).await;
```

Then keep the existing post-submit on-disk materialization branch, BUT update it so the local broker only materializes partitions for which IT is the leader (the broker that handled CreateTopics may or may not be in the replica set):

```rust
let error_code = match result {
    Ok(()) => {
        for (p, replicas) in assignments.iter().enumerate() {
            let p_i32 = i32::try_from(p).unwrap_or(0);
            // Only the partition's leader materializes the on-disk
            // partition synchronously. Followers will materialize it
            // lazily inside their replicator task.
            if replicas[0] == node_id {
                let dir = log_dir.join(format!("{}-{}", topic.name, p_i32));
                std::fs::create_dir_all(&dir).ok();
                if let Ok(log) = crabka_log::Log::open(&dir, log_config.clone()) {
                    let part = spawn_partition(topic.name.clone(), p_i32, log);
                    partitions_map.insert((topic.name.clone(), p_i32), part);
                }
            }
        }
        codes::NONE
    }
    Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {
        codes::TOPIC_ALREADY_EXISTS
    }
    Err(RaftError::NotLeader { .. }) | Err(RaftError::LeaderUnknown) => {
        codes::NOT_CONTROLLER
    }
    Err(e) => {
        tracing::error!(topic = %topic.name, error = %e, "CreateTopics submit_change failed");
        codes::UNKNOWN_SERVER_ERROR
    }
};
```

Add the `topic_id` field to the topic_results push that follows:

```rust
topic_results.push(CreatableTopicResult {
    name: topic.name,
    topic_id: ProtoUuid(topic_id.into_bytes()),
    error_code,
    error_message: None,
    ..Default::default()
});
```

ADAPT to whatever variable names are already in the existing handler (`topic_results` vs `results`, `node_id` vs `self.node_id`, etc.). The grep recon in Step 1 told you the local names.

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib --tests
git add crates/broker/src/handlers/create_topics.rs
git commit -m "$(cat <<'EOF'
feat(broker): CreateTopics honors replication_factor via round-robin

Reads the current MetadataImage::brokers() set, sorts by node_id, and
asks the round-robin helper for an assignment per partition. RF > k
returns INVALID_REPLICATION_FACTOR (38). Only the partition's leader
materializes its on-disk partition synchronously; followers materialize
lazily inside their replicator task (added in a later phase).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

All slice-7 single-broker tests must still pass (with rf=1 the per-partition replicas vec is `[self.node_id]`, identical to the prior hard-coding).

---

### Task 3: End-to-end smoke test of placement

**Files:**
- Modify: `crates/broker/tests/unit.rs` (or wherever slice-7 added existing CreateTopics tests; recon first)

- [ ] **Step 1: Recon**

```bash
grep -n "create_topic\|replication_factor" crates/broker/tests/unit.rs crates/broker/tests/integration.rs 2>&1 | head -20
```

Find the slice-7 single-node create-topic test to copy its shape.

- [ ] **Step 2: Add the RF>k integration test**

In `crates/broker/tests/unit.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_rf_too_high_returns_invalid_replication_factor() {
    let p = support::start().await; // single-voter broker
    let resp = p.client.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "boom".into(),
            num_partitions: 1,
            replication_factor: 5, // single broker → RF=5 is invalid
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(resp.topics[0].error_code, 38 /* INVALID_REPLICATION_FACTOR */);
    p.broker.shutdown().await;
}
```

The 3-node placement tests come in Phase F (Task 14 sets up the cluster harness; reusing it here would require importing the same setup from the multi-node test file).

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-broker --test unit create_topics_rf
git add crates/broker/tests
git commit -m "$(cat <<'EOF'
test(broker): RF > broker count returns INVALID_REPLICATION_FACTOR

Single-voter integration test that exercises the rejection path
introduced by Task 2.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Phase B — Leader-side `Fetch` follower fork

### Task 4: Branch `Fetch` handler on `replica_id`

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Recon**

```bash
grep -n "fn handle\|partition.log\|max_bytes\|filtered\|hw\|partitions\b" crates/broker/src/handlers/fetch.rs | head -20
```

Find the spot inside the handler where the response is being built per `FetchPartition`. The existing slice-4 logic reads from `partition.log`, builds a `FetchableTopicResponse`, etc.

- [ ] **Step 2: Add the branch**

At the top of the per-partition loop (or wherever `max_bytes` / log read currently happens):

```rust
let is_follower_fetch = req.replica_id >= 0;
let _ = is_follower_fetch; // wired up here for slice 8; HW filtering
                            // lands in a slice-8 follow-up (spec §"Non-goals").
```

Add a comment explaining that the branch is a no-op in slice 8 (consumer and follower fetches return the same bytes because we don't track HW yet):

```rust
// `replica_id >= 0` means follower fetch (Apache Kafka convention).
// Slice 8 does NOT filter consumer fetches to HW because HW tracking
// is deferred (see `docs/superpowers/specs/2026-05-12-crabka-replication-design.md`
// §"Non-goals"). The branch is wired here so that slice-8-followup can
// add HW filtering on the consumer arm without re-shaping the handler.
```

- [ ] **Step 3: Verify + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker
git add crates/broker/src/handlers/fetch.rs
git commit -m "$(cat <<'EOF'
feat(broker): branch Fetch handler on replica_id (no-op fork in slice 8)

The follower-vs-consumer distinction is the standard Kafka wire
contract: replica_id >= 0 = follower fetch (no HW filtering),
replica_id < 0 = consumer fetch (filter to HW). Slice 8 doesn't track
HW yet, so both arms return the same bytes. The branch is in place
so slice-8-followup can add HW filtering on the consumer arm without
re-shaping the handler.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Phase C — Replicator (per-partition fetch loop)

### Task 5: `BrokerError::Replication` + replicator module skeleton

**Files:**
- Modify: `crates/broker/src/error.rs`
- Create: `crates/broker/src/replicator.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Add the error variant**

Append to the `BrokerError` enum in `crates/broker/src/error.rs`:

```rust
    #[error("replication: {0}")]
    Replication(String),
```

And add a `from_broker_error` arm mapping it to whatever code makes sense — `UNKNOWN_SERVER_ERROR` is fine since this variant only ever logs:

```rust
        BrokerError::Replication(_) => codes::UNKNOWN_SERVER_ERROR,
```

- [ ] **Step 2: Create the replicator skeleton**

`crates/broker/src/replicator.rs`:

```rust
//! Per-(topic, partition) replication task. Issues standard Kafka `Fetch`
//! requests against the partition's leader (with `replica_id` set to the
//! local broker's `node_id`), appending each returned batch to the local
//! `crabka-log`. Handles `OFFSET_OUT_OF_RANGE` by truncating local log to
//! 0 and restarting; `NOT_LEADER_FOR_PARTITION` by returning so the
//! supervisor's next reconcile re-evaluates.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_client_core::{Client, ClientError};
use crabka_log::{Log, LogConfig};
use crabka_protocol::owned::fetch_request::{
    FetchPartition, FetchRequest, FetchTopic,
};
use crabka_raft::NodeId;

use crate::broker::spawn_partition;
use crate::codes;
use crate::partition::Partition;

const FETCH_MAX_BYTES: i32 = 1 << 20;
const FETCH_MAX_WAIT_MS: i32 = 500;
const FETCH_MIN_BYTES: i32 = 1;

/// Configuration handed to a single replicator task.
pub(crate) struct Config {
    pub node_id: NodeId,
    pub topic: String,
    pub partition: i32,
    pub leader_node_id: NodeId,
    pub leader_addr: String,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub log_dir: PathBuf,
    pub log_config: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
}

/// Entry point: drive a single (topic, partition) replication loop until
/// cancelled.
pub(crate) async fn run(cfg: Config) {
    info!(
        topic = %cfg.topic,
        partition = cfg.partition,
        leader_node_id = cfg.leader_node_id,
        "replicator.started"
    );

    // First-run materialization of the local on-disk partition.
    if let Err(e) = ensure_local_partition(&cfg).await {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
            "replicator failed to open local partition; aborting");
        return;
    }

    if let Err(e) = run_inner(&cfg).await {
        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
            "replicator stopped on unrecoverable error");
    }

    info!(topic = %cfg.topic, partition = cfg.partition, "replicator.stopped");
}

/// Build (or recover) the on-disk `Partition` for this follower, inserting
/// it into the broker's shared `partitions` map. Idempotent.
async fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
    if cfg.partitions.contains_key(&(cfg.topic.clone(), cfg.partition)) {
        return Ok(());
    }
    let dir = cfg.log_dir.join(format!("{}-{}", cfg.topic, cfg.partition));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let log = Log::open(&dir, cfg.log_config.clone())
        .map_err(|e| format!("Log::open: {e}"))?;
    let part = spawn_partition(cfg.topic.clone(), cfg.partition, log);
    cfg.partitions.insert((cfg.topic.clone(), cfg.partition), part);
    Ok(())
}

async fn run_inner(_cfg: &Config) -> Result<(), String> {
    // Stub — Tasks 6-8 fill this in.
    Ok(())
}
```

- [ ] **Step 3: Hook into `lib.rs`**

In `crates/broker/src/lib.rs`, add (alphabetically near `mod replicator_supervisor;` which Task 9 will add):

```rust
mod replicator;
```

Keep visibility internal — no `pub use`.

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src
git commit -m "$(cat <<'EOF'
feat(broker): replicator skeleton + BrokerError::Replication

Per-(topic, partition) replication-task scaffolding. The `run` entry
point creates the local on-disk partition on first run (idempotent
through `broker.partitions`), then delegates to `run_inner` for the
fetch loop — currently a stub, filled in by the next tasks.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Fetch loop happy path

**Files:**
- Modify: `crates/broker/src/replicator.rs`

- [ ] **Step 1: Implement `run_inner` happy path**

Replace the stub `run_inner` with a real fetch loop. Drop in below the existing `ensure_local_partition`:

```rust
async fn run_inner(cfg: &Config) -> Result<(), String> {
    let mut client = connect_with_backoff(cfg).await?;
    loop {
        if cfg.shutdown.is_cancelled() {
            return Ok(());
        }

        let fetch_offset = {
            let entry = cfg
                .partitions
                .get(&(cfg.topic.clone(), cfg.partition))
                .ok_or_else(|| "local partition missing".to_string())?;
            entry.value().log_end_offset().await.map_err(|e| e.to_string())?
        };

        let req = FetchRequest {
            replica_id: i32::try_from(cfg.node_id).unwrap_or(-1),
            max_wait_ms: FETCH_MAX_WAIT_MS,
            min_bytes: FETCH_MIN_BYTES,
            max_bytes: FETCH_MAX_BYTES,
            topics: vec![FetchTopic {
                topic: cfg.topic.clone(),
                partitions: vec![FetchPartition {
                    partition: cfg.partition,
                    fetch_offset,
                    partition_max_bytes: FETCH_MAX_BYTES,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let send = tokio::select! {
            () = cfg.shutdown.cancelled() => return Ok(()),
            r = client.send(req) => r,
        };

        let resp = match send {
            Ok(r) => r,
            Err(ClientError::Disconnected | ClientError::Io(_)) => {
                client = connect_with_backoff(cfg).await?;
                continue;
            }
            Err(e) => {
                warn!(error = %e, "replicator: client.send unexpected error; retrying after backoff");
                tokio::time::sleep(Duration::from_secs(1)).await;
                client = connect_with_backoff(cfg).await?;
                continue;
            }
        };

        if let Some(action) = handle_response(&resp, cfg).await {
            match action {
                LoopAction::Continue => continue,
                LoopAction::StopNotLeader => {
                    info!(topic = %cfg.topic, partition = cfg.partition,
                        "replicator.not_leader; supervisor will re-evaluate");
                    return Ok(());
                }
            }
        }
    }
}

/// Outcome of one fetch round.
enum LoopAction {
    Continue,
    StopNotLeader,
}

async fn handle_response(
    resp: &crabka_protocol::owned::fetch_response::FetchResponse,
    cfg: &Config,
) -> Option<LoopAction> {
    // Find this partition's response. Slice 8 only ever requests one
    // (topic, partition) per Fetch.
    let part_resp = resp
        .responses
        .iter()
        .find(|t| t.topic == cfg.topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == cfg.partition));
    let Some(p) = part_resp else {
        return Some(LoopAction::Continue);
    };

    match p.error_code {
        codes::NONE => {
            // Append every returned batch to local log.
            if let Some(records) = &p.records {
                let entry = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition))?;
                for batch in records.batches.iter() {
                    let mut b = batch.clone();
                    if let Err(e) = entry.value().append_already_assigned(&mut b).await {
                        warn!(error = %e, "replicator: append failed");
                        return Some(LoopAction::Continue);
                    }
                }
            }
            Some(LoopAction::Continue)
        }
        codes::OFFSET_OUT_OF_RANGE => {
            warn!(topic = %cfg.topic, partition = cfg.partition,
                "replicator.out_of_range; truncating local log to 0");
            let entry = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition))?;
            if let Err(e) = entry.value().truncate_to(0).await {
                warn!(error = %e, "replicator: truncate_to(0) failed");
            }
            Some(LoopAction::Continue)
        }
        codes::UNKNOWN_TOPIC_OR_PARTITION => {
            // Leader hasn't materialized its side yet (CreateTopics-vs-replicator race).
            tokio::time::sleep(Duration::from_millis(100)).await;
            Some(LoopAction::Continue)
        }
        codes::NOT_LEADER_OR_FOLLOWER => Some(LoopAction::StopNotLeader),
        other => {
            warn!(error_code = other, "replicator: unexpected fetch error_code");
            tokio::time::sleep(Duration::from_millis(500)).await;
            Some(LoopAction::Continue)
        }
    }
}

async fn connect_with_backoff(cfg: &Config) -> Result<Client, String> {
    let mut delay = Duration::from_millis(100);
    let cap = Duration::from_secs(5);
    loop {
        let attempt = Client::builder()
            .bootstrap(cfg.leader_addr.clone())
            .client_id(cfg.client_id.clone())
            .build();
        let result = tokio::select! {
            () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt => r,
        };
        match result {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(addr = %cfg.leader_addr, error = %e,
                    "replicator: connect failed; retrying after {:?}", delay);
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
                    () = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(cap);
            }
        }
    }
}
```

NOTE: `Partition::log_end_offset()`, `Partition::append_already_assigned`, and `Partition::truncate_to` are method names I've assumed. Recon the actual `Partition` API:

```bash
grep -n "pub.* fn\|pub async fn" crates/broker/src/partition.rs
```

If the actual method names differ, adapt. The slice-4 Partition wraps an mpsc-fed writer task — append-without-reassignment may need a new helper. If `append_already_assigned` doesn't exist, ADD it: it takes a `&mut RecordBatch`, sends it through the writer task without modifying its `base_offset` (the leader already assigned it). This is a small new mpsc message kind — alternatively, get the underlying `Log` reference and append directly.

Simplest pragmatic shape: add a method on `Partition` like:

```rust
pub async fn replicate_batch(&self, batch: RecordBatch) -> Result<(), BrokerError> {
    // sends through the writer task; the writer appends without
    // reassigning base_offset
}
```

The writer task's existing message enum gains a new variant. Tasks 7 and 8 below assume `replicate_batch` exists.

- [ ] **Step 2: Add `Partition::replicate_batch` (if not already present)**

In `crates/broker/src/partition.rs`, find the existing writer-task enum (it'll be a `ProduceJob` or similar — the slice-7 plan called it `ProduceJob`). Add a new variant:

```rust
enum WriterMessage {
    Produce(ProduceJob),
    Replicate {
        batch: RecordBatch,
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    // ... existing variants
}
```

In the writer-task loop, handle the new variant by calling `log.append(&mut batch)` BUT using the already-assigned `batch.base_offset` rather than letting `log.append` overwrite it. (`crabka-log::Log::append` overwrites `base_offset` by design — for replication we need to override that.)

Recon `Log::append`:

```bash
grep -n "fn append" crates/log/src/log.rs
```

If `Log::append` strictly assigns offsets, add a `Log::append_at(batch, offset)` helper to `crabka-log` that writes the batch at a caller-specified offset. Slice 8 is the right time for this — replication fundamentally needs caller-specified offsets.

Add the corresponding `Partition::replicate_batch(&self, batch: RecordBatch) -> Result<(), BrokerError>` async helper that constructs the `WriterMessage::Replicate` and awaits the oneshot.

This is a sub-task within Task 6 because the replicator can't work without it. If extracting it grows beyond ~30 lines of changes outside `partition.rs`, split into Task 6a (Partition + Log) and Task 6b (replicator wiring).

- [ ] **Step 3: Update replicator.rs to call the real method**

Replace `entry.value().append_already_assigned(&mut b)` with `entry.value().replicate_batch(b.clone()).await`.

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src crates/log/src
git commit -m "$(cat <<'EOF'
feat(broker,log): replicator happy-path fetch loop + replicate_batch

The replicator's `run_inner` opens a Client against the partition's
leader, loops Fetch with replica_id=self.node_id, and appends every
returned batch through `Partition::replicate_batch`. That method goes
through the existing writer-task mpsc with a new Replicate variant; the
writer then calls `Log::append_at(batch, batch.base_offset)` to honor
the leader-assigned offset instead of overwriting it.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Out-of-range truncate + transport-retry hardening

**Files:**
- Modify: `crates/broker/src/replicator.rs`

- [ ] **Step 1: Verify the error paths**

The Task 6 listing already includes the OFFSET_OUT_OF_RANGE truncate-to-0 branch and a transport-retry path via `connect_with_backoff`. This task is to:

1. Audit `handle_response`'s `OFFSET_OUT_OF_RANGE` branch: confirm `Partition::truncate_to(0)` exists. If not, add it as a passthrough to `Log::truncate_to(0)`.

2. Audit `connect_with_backoff`: confirm `ClientError::Disconnected` and `ClientError::Io(_)` are variants on the real `ClientError` enum:

```bash
grep -n "^    [A-Z]" crates/client-core/src/error.rs
```

If the variant names differ (e.g., `ConnectionClosed` instead of `Disconnected`), adapt the `match` in `run_inner`. The fallback `_ => connect_with_backoff` already covers any error variant generically; the explicit match arms are for documentation/future precision.

3. Add a unit test for the out-of-range path using a mock Client (or skip — the Layer-3 integration test in Task 15 covers it end-to-end).

- [ ] **Step 2: Build + commit**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/replicator.rs crates/broker/src/partition.rs
git commit -m "$(cat <<'EOF'
feat(broker): replicator out-of-range truncate + transport retry hardening

Wired `Partition::truncate_to(0)` for the OFFSET_OUT_OF_RANGE recovery
path; confirmed `ClientError` variant names; tightened clippy.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Phase D — `replicator_supervisor`

### Task 8: Supervisor scaffolding + diff helper

**Files:**
- Create: `crates/broker/src/replicator_supervisor.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Create the file**

`crates/broker/src/replicator_supervisor.rs`:

```rust
//! Subscribes to the controller's metadata-image watch channel and
//! diffs the desired follower-replication assignments on each apply.
//! Spawns a `replicator::run` task per new (topic, partition); cancels
//! tasks for partitions removed from the image.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crabka_log::LogConfig;
use crabka_metadata::MetadataImage;
use crabka_raft::{ControllerHandle, NodeId};

use crate::partition::Partition;
use crate::replicator;

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    controller: Arc<ControllerHandle>,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    log_dir: PathBuf,
    log_config: LogConfig,
    client_id: String,
    /// Per-partition cancellation tokens for active replication tasks.
    tasks: DashMap<(String, i32), CancellationToken>,
    shutdown: CancellationToken,
}

impl ReplicatorSupervisor {
    pub(crate) fn new(
        node_id: NodeId,
        controller: Arc<ControllerHandle>,
        partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
        log_dir: PathBuf,
        log_config: LogConfig,
        client_id: String,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            node_id,
            controller,
            partitions,
            log_dir,
            log_config,
            client_id,
            tasks: DashMap::new(),
            shutdown,
        }
    }

    /// Compute the set of (topic, partition) we should be following: every
    /// partition where `self.node_id` is in `replicas` AND `leader != self.node_id`.
    pub(crate) fn desired_follower_set(&self, image: &MetadataImage) -> HashSet<(String, i32)> {
        let mut out = HashSet::new();
        for t in image.topics() {
            for p in image.partitions_of(&t.name) {
                if p.replicas.contains(&self.node_id) && p.leader != self.node_id {
                    out.insert((p.topic.clone(), p.partition));
                }
            }
        }
        out
    }

    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
        let desired = self.desired_follower_set(image);

        // 1. Cancel removed.
        let current: Vec<(String, i32)> =
            self.tasks.iter().map(|e| e.key().clone()).collect();
        for k in current {
            if !desired.contains(&k) {
                if let Some((_, token)) = self.tasks.remove(&k) {
                    token.cancel();
                }
            }
        }

        // 2. Spawn new.
        for k in desired {
            if self.tasks.contains_key(&k) {
                continue;
            }
            let part = image.partition(&k.0, k.1).cloned();
            let Some(part) = part else { continue };
            let leader = part.leader;
            let Some(broker) = image.broker(leader).cloned() else {
                warn!(topic = %k.0, partition = k.1, leader,
                    "leader broker not yet registered in MetadataImage; deferring");
                continue;
            };
            let token = CancellationToken::new();
            self.tasks.insert(k.clone(), token.clone());
            tokio::spawn(replicator::run(replicator::Config {
                node_id: self.node_id,
                topic: k.0,
                partition: k.1,
                leader_node_id: leader,
                leader_addr: format!("{}:{}", broker.host, broker.port),
                partitions: self.partitions.clone(),
                log_dir: self.log_dir.clone(),
                log_config: self.log_config.clone(),
                client_id: self.client_id.clone(),
                shutdown: token,
            }));
        }
    }

    pub(crate) async fn run(self) {
        let mut rx = self.controller.watch_image();
        loop {
            let image = rx.borrow().clone();
            self.reconcile(&image).await;
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                res = rx.changed() => {
                    if res.is_err() {
                        break;
                    }
                }
            }
        }
        // Final cancel of everything still running.
        for entry in self.tasks.iter() {
            entry.value().cancel();
        }
    }

    /// Spawn the supervisor on the current tokio runtime, returning a join
    /// handle the broker keeps for shutdown sequencing.
    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

In `crates/broker/src/lib.rs`, add:

```rust
mod replicator_supervisor;
```

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src
git commit -m "$(cat <<'EOF'
feat(broker): replicator supervisor scaffolding

Subscribes to controller.watch_image() and reconciles the running set
of per-partition replication tasks against the desired follower set
on every metadata apply. Cancels removed via per-task
CancellationTokens; spawns new via replicator::run.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Supervisor reconcile unit tests

**Files:**
- Modify: `crates/broker/src/replicator_supervisor.rs`

- [ ] **Step 1: Unit tests on `desired_follower_set`**

Append to `replicator_supervisor.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use uuid::Uuid;

    fn image_with(records: &[MetadataRecord]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for r in records {
            img.apply(r);
        }
        img
    }

    fn supervisor(node_id: NodeId) -> ReplicatorSupervisor {
        ReplicatorSupervisor {
            node_id,
            controller: Arc::new(unsafe { std::mem::zeroed() }), // unused in pure desired_follower_set
            partitions: Arc::new(DashMap::new()),
            log_dir: PathBuf::new(),
            log_config: LogConfig::default(),
            client_id: "test".into(),
            tasks: DashMap::new(),
            shutdown: CancellationToken::new(),
        }
    }

    #[test]
    fn includes_partition_where_self_is_follower() {
        let s = supervisor(2);
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(), topic_id: Uuid::new_v4(),
                partitions: 1, replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(), partition: 0,
                leader: 1, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
        ]);
        let d = s.desired_follower_set(&img);
        assert!(d.contains(&("t".into(), 0)));
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn excludes_partition_where_self_is_leader() {
        let s = supervisor(1);
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(), topic_id: Uuid::new_v4(),
                partitions: 1, replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(), partition: 0,
                leader: 1, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
        ]);
        assert!(s.desired_follower_set(&img).is_empty());
    }

    #[test]
    fn excludes_partition_where_self_is_not_a_replica() {
        let s = supervisor(99);
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(), topic_id: Uuid::new_v4(),
                partitions: 1, replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(), partition: 0,
                leader: 1, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
        ]);
        assert!(s.desired_follower_set(&img).is_empty());
    }

    #[test]
    fn multiple_topics_aggregated() {
        let s = supervisor(2);
        let img = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "a".into(), topic_id: Uuid::new_v4(),
                partitions: 1, replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "a".into(), partition: 0,
                leader: 1, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "b".into(), topic_id: Uuid::new_v4(),
                partitions: 2, replication_factor: 3,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "b".into(), partition: 0,
                leader: 3, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "b".into(), partition: 1,
                leader: 2, replicas: vec![1, 2, 3], isr: vec![1, 2, 3],
            }),
        ]);
        let d = s.desired_follower_set(&img);
        assert!(d.contains(&("a".into(), 0)));
        assert!(d.contains(&("b".into(), 0)));
        assert!(!d.contains(&("b".into(), 1))); // self is leader for b/1
        assert_eq!(d.len(), 2);
    }
}
```

The `unsafe { std::mem::zeroed() }` for the unused `controller` field is ugly but necessary in a pure-data unit test. If the field is unused in the test, an alternative is to refactor `desired_follower_set` to be a free fn taking only `(node_id, &MetadataImage)` and avoid the supervisor mock entirely. Either is fine.

Actually — refactor is cleaner. Pull `desired_follower_set` out into a free fn at the module level:

```rust
fn desired_follower_set(node_id: NodeId, image: &MetadataImage) -> HashSet<(String, i32)> {
    let mut out = HashSet::new();
    for t in image.topics() {
        for p in image.partitions_of(&t.name) {
            if p.replicas.contains(&node_id) && p.leader != node_id {
                out.insert((p.topic.clone(), p.partition));
            }
        }
    }
    out
}
```

Then the supervisor method calls `desired_follower_set(self.node_id, image)`. Tests call it directly without needing a supervisor. Drop the `unsafe`-y test helper.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-broker --lib replicator_supervisor
git add crates/broker/src/replicator_supervisor.rs
git commit -m "$(cat <<'EOF'
test(broker): replicator_supervisor unit tests on desired_follower_set

Refactored the desired-set computation into a free fn so tests don't
need to construct a fake ControllerHandle. Four scenarios covered:
self-as-follower, self-as-leader (excluded), self-not-a-replica
(excluded), multi-topic aggregation.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Phase E — Broker integration

### Task 10: Wire supervisor into `Broker::start`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Recon**

```bash
grep -nE "pub.* shutdown|JoinHandle|tokio::spawn|controller.shutdown" crates/broker/src/broker.rs | head -15
```

Find the shutdown sequence — the supervisor needs to be cancelled before the controller, before the listeners.

- [ ] **Step 2: Spawn the supervisor**

In `Broker::start`, after the controller is up (and after the broker has self-registered via `submit_change`), add:

```rust
let supervisor_shutdown = tokio_util::sync::CancellationToken::new();
let supervisor = crate::replicator_supervisor::ReplicatorSupervisor::new(
    config.node_id,
    controller.clone(),
    partitions.clone(),
    config.log_dir.clone(),
    config.log_config.clone(),
    format!("crabka-broker-{}-replicator", config.broker_id),
    supervisor_shutdown.clone(),
);
let supervisor_handle = supervisor.spawn();
```

Store `supervisor_shutdown` and `supervisor_handle` on the `Broker` struct (add two fields):

```rust
pub(crate) supervisor_shutdown: tokio_util::sync::CancellationToken,
pub(crate) supervisor_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
```

The `Mutex<Option<JoinHandle>>` shape mirrors how `ControllerHandle` keeps its `listener_task` joinable from a `&Broker` shutdown method.

In `BrokerHandle::shutdown` (or wherever the existing shutdown sequence lives), call BEFORE the controller's shutdown:

```rust
self.supervisor_shutdown.cancel();
if let Some(h) = self.supervisor_handle.lock().await.take() {
    let _ = h.await;
}
```

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib --tests
git add crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
feat(broker): wire replicator supervisor into Broker::start

Spawns the supervisor after controller startup + self-registration so
the initial reconcile already sees this broker in the brokers() set.
Shutdown cancels the supervisor BEFORE the controller so in-flight
replication tasks see a clean cancellation rather than a torn-down
controller.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

After this commit, all slice-1..7 tests must still pass. With `replication_factor=1` (the slice-7 default), the supervisor's `desired_follower_set` is always empty, so no replication tasks spawn — single-broker behavior is unchanged.

---

## Phase F — Integration + acceptance

### Task 11: In-process 3-node replication test

**Files:**
- Create: `crates/broker/tests/replication.rs`

- [ ] **Step 1: Test scaffolding**

Reuse the slice-7 `start_n_node_with_retry` harness pattern from `crates/broker/tests/quorum.rs`. If it's not already exported into a shared support module, copy the helper. Future cleanup can dedupe.

```rust
//! Multi-node in-process tests for slice-8 basic replication. Gated
//! `#[cfg(not(target_os = "windows"))]` to mirror quorum.rs (openraft
//! `debug_assert!` race on the hosted Windows runner).

#![cfg(not(target_os = "windows"))]

use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;
use tokio::sync::Mutex;

static CLUSTER_LOCK: tokio::sync::OnceCell<Mutex<()>> = tokio::sync::OnceCell::const_new();
async fn cluster_lock() -> &'static Mutex<()> {
    CLUSTER_LOCK.get_or_init(|| async { Mutex::new(()) }).await
}

// `start_n_node_with_retry`: copy verbatim from
// `crates/broker/tests/quorum.rs`. (Or, if you extract a shared support
// module first, import from there.) ~80 lines.

async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    // ... see quorum.rs for the canonical body ...
    todo!("copy from quorum.rs")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers() {
    let _g = cluster_lock().await.lock().await;
    let cluster = start_n_node_with_retry(3).await;

    // Wait for ALL 3 brokers to register in each other's MetadataImage.
    let deadline = Instant::now() + Duration::from_mins(2);
    'wait_brokers: loop {
        for (h, _, _) in &cluster {
            let n = h.broker_count().await; // ADD this accessor; see below
            if n >= 3 {
                continue 'wait_brokers;
            }
            break;
        }
        // All 3 brokers see 3 brokers.
        if cluster.iter().all(|(h, _, _)| futures_lite::future::block_on(h.broker_count()) >= 3) {
            // ↑ awkward; just do it serially with tokio
            break;
        }
        if Instant::now() > deadline {
            panic!("brokers didn't converge on 3-broker view within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Identify the cluster's broker-1 client port (= node_id 1's broker).
    // The start_n_node_with_retry harness binds them in order, so cluster[0]
    // is node 1.
    let leader_addr = cluster[0].1.listen_addr.to_string();

    // CreateTopics(name="repl", num_partitions=1, replication_factor=3).
    let admin = Client::builder().bootstrap(leader_addr.clone()).build().await.unwrap();
    let resp = admin.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "repl".into(),
            num_partitions: 1,
            replication_factor: 3,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(resp.topics[0].error_code, 0);

    // Wait for all 3 brokers to see the topic in their image.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let all = cluster.iter().all(|(h, _, _)| {
            // accessor: returns Option<&PartitionRecord> via image.partition()
            futures_lite::future::block_on(h.has_partition("repl", 0))
        });
        if all { break; }
        if Instant::now() > deadline {
            panic!("topic 'repl' didn't propagate to all 3 brokers within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Identify the partition leader (always node 1 with rf=3, partition_index=0).
    // Produce 20 records to that broker.
    let producer = Client::builder().bootstrap(leader_addr).build().await.unwrap();
    let mut batch = RecordBatch::default();
    batch.base_offset = 0;
    batch.last_offset_delta = 19;
    batch.records = (0..20)
        .map(|i| Record {
            offset_delta: i,
            value: Some(bytes::Bytes::from(format!("v{i}"))),
            ..Default::default()
        })
        .collect();
    let prod = producer.send(ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "repl".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(prod.responses[0].partition_responses[0].error_code, 0);

    // Wait until every broker's local log shows log_end_offset >= 20.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let all = futures::future::join_all(cluster.iter().map(|(h, _, _)| async {
            h.local_log_end_offset("repl", 0).await.unwrap_or(0)
        })).await;
        if all.iter().all(|&n| n >= 20) { break; }
        if Instant::now() > deadline {
            panic!("not all 3 brokers caught up to 20 records within 2 min; saw: {all:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
```

The accessors `broker_count`, `has_partition`, `local_log_end_offset` need to be added on `BrokerHandle`:

- `broker_count(&self) -> usize` — `self.broker.controller.current_image().brokers().count()`.
- `has_partition(&self, topic: &str, idx: i32) -> bool` — `self.broker.controller.current_image().partition(topic, idx).is_some()`.
- `local_log_end_offset(&self, topic: &str, idx: i32) -> Option<i64>` — look up `(topic, idx)` in `self.broker.partitions` and return the `Partition::log_end_offset()` if present.

Add these in the same commit.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-broker --test replication replication_factor_three_propagates_to_all_followers
git add crates/broker
git commit -m "$(cat <<'EOF'
test(broker): replication_factor=3 propagation in-process

3-broker cluster, rf=3, produce 20 records to the leader, assert all
3 brokers' local logs reach log_end_offset=20 within 2 min. Gated
non-Windows to match quorum.rs. Adds three small `BrokerHandle`
accessors used only by tests.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

Use 2-minute deadlines throughout for the same reason as slice-7's `quorum.rs` (slow CI runners).

---

### Task 12: Out-of-range truncate test

**Files:**
- Modify: `crates/broker/tests/replication.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_truncates_and_recovers() {
    let _g = cluster_lock().await.lock().await;
    let cluster = start_n_node_with_retry(3).await;

    // Same setup as the propagation test.
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = Client::builder().bootstrap(leader_addr.clone()).build().await.unwrap();
    let _ = admin.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "oor".into(), num_partitions: 1, replication_factor: 3,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();

    // Wait for propagation.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let all = cluster.iter().all(|(h, _, _)| {
            futures_lite::future::block_on(h.has_partition("oor", 0))
        });
        if all { break; }
        if Instant::now() > deadline { panic!("topic propagation timed out"); }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Produce 50 records.
    let producer = Client::builder().bootstrap(leader_addr).build().await.unwrap();
    let mut batch = RecordBatch::default();
    batch.last_offset_delta = 49;
    batch.records = (0..50).map(|i| Record {
        offset_delta: i, value: Some(bytes::Bytes::from(format!("v{i}"))),
        ..Default::default()
    }).collect();
    let _ = producer.send(ProduceRequest {
        acks: -1, timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "oor".into(),
            partition_data: vec![PartitionProduceData {
                index: 0, records: Some(batch), ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();

    // Wait for all 3 to catch up to >=50.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let all = futures::future::join_all(cluster.iter().map(|(h, _, _)| async {
            h.local_log_end_offset("oor", 0).await.unwrap_or(0)
        })).await;
        if all.iter().all(|&n| n >= 50) { break; }
        if Instant::now() > deadline {
            panic!("initial replication didn't reach 50 in 2 min: {all:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Simulate broker 3 "falling behind past retention" by truncating its
    // local log + advancing the leader's log_start past it. Requires test
    // accessors that reach into the broker's partition map directly.
    cluster[2].0.test_truncate_local_log("oor", 0, 0).await.expect("truncate broker 3");
    cluster[0].0.test_advance_log_start("oor", 0, 25).await.expect("advance leader log_start");

    // Wait for broker 3 to converge again.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let lag = cluster[2].0.local_log_end_offset("oor", 0).await.unwrap_or(0);
        if lag >= 50 { break; }
        if Instant::now() > deadline {
            panic!("broker 3 didn't recover from OFFSET_OUT_OF_RANGE in 2 min");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
```

Add the test-only accessors on `BrokerHandle`:
- `test_truncate_local_log(&self, topic, idx, offset) -> Result<...>` — calls `partition.truncate_to(offset)`.
- `test_advance_log_start(&self, topic, idx, new_start) -> Result<...>` — calls `Log::set_log_start_offset(new_start)` or equivalent. If `crabka-log` doesn't expose log_start mutation, add a `Log::test_set_log_start(offset)` helper guarded by `#[cfg(feature = "test-helpers")]` or similar. Slice 8 owns this introduction.

Gate the test accessors behind `#[cfg(any(test, feature = "test-helpers"))]` so they're not shipped to production crates.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-broker --test replication out_of_range
git add crates/broker
git commit -m "$(cat <<'EOF'
test(broker): out-of-range truncate-and-recover replication

3-broker cluster, rf=3. Produce 50 records; wait for all to catch up;
truncate broker 3's local log + advance leader's log_start past it;
assert broker 3 converges again via the OFFSET_OUT_OF_RANGE recovery
path. Test-only `Log::test_set_log_start` introduced.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: JVM acceptance — byte-compare via `kafka-dump-log`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Recon existing JVM test pattern**

The slice-7 `three_node_jvm_round_trip` already shows the parallel-broker-startup pattern + `host.docker.internal` config + JVM-tool docker-run pattern.

- [ ] **Step 2: Append the new test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_replication_byte_compare() {
    const TOPIC: &str = "crabka-replication-itest";
    let client_ports = [9192u16, 9292, 9392];
    let controller_ports = [9193u16, 9293, 9393];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i]).parse().unwrap(),
            )
        })
        .collect();

    // Parallel spawn — sequential blocks on each broker's leader-wait.
    let mut tempdirs = Vec::with_capacity(3);
    let mut spawns = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().expect("static"),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i])
                .parse()
                .expect("static"),
            controller_quorum_voters: voters.clone(),
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (sp, dir) in spawns.into_iter().zip(tempdirs) {
        cluster.push((sp.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);

    // CreateTopics(repl=3, partitions=1).
    docker_run_kafka_tool(&[
        "kafka-topics", "--create", "--if-not-exists", "--topic", TOPIC,
        "--partitions", "1", "--replication-factor", "3",
        "--bootstrap-server", &bootstrap_1,
    ]);

    // Wait for ISR to include all 3 brokers (slice 8: ISR = replicas always,
    // so this is just "did the metadata propagate"). Poll `kafka-topics --describe`
    // until output contains "Isr: 1,2,3" or equivalent.
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
    loop {
        let desc = docker_run_kafka_tool(&[
            "kafka-topics", "--describe", "--topic", TOPIC,
            "--bootstrap-server", &bootstrap_1,
        ]);
        let s = String::from_utf8_lossy(&desc.stdout);
        let has_isr_3 = s.contains("Isr: 1,2,3")
            || s.contains("Isr: 1,3,2")
            || s.contains("Isr: 2,1,3")
            || s.contains("Isr: 2,3,1")
            || s.contains("Isr: 3,1,2")
            || s.contains("Isr: 3,2,1");
        if has_isr_3 { break; }
        assert!(
            std::time::Instant::now() <= deadline,
            "topic metadata not fully propagated within 2 min: {s}",
        );
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Produce 100 records via kafka-console-producer.
    let mut producer_child = Command::new("docker")
        .args([
            "run", "--rm", "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server", &bootstrap_1,
            "--topic", TOPIC,
        ])
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().expect("spawn JVM producer");
    {
        let stdin = producer_child.stdin.as_mut().expect("stdin");
        for i in 0..100 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(producer_child.stdin.take());
    let prod_out = producer_child.wait_with_output().expect("wait producer");
    assert!(prod_out.status.success(), "producer failed");

    // Wait for replication lag to drain. `kafka-topics --describe` doesn't
    // directly report lag; we rely on a brief sleep then dump-log compare.
    std::thread::sleep(std::time::Duration::from_secs(3));

    // For each broker, dump-log against the on-disk partition file.
    let mut dumps = Vec::with_capacity(3);
    for (i, (_, dir)) in cluster.iter().enumerate() {
        let partition_dir = dir.path().join(format!("{TOPIC}-0"));
        // The first segment is `00000000000000000000.log`.
        let log_file = partition_dir.join("00000000000000000000.log");
        assert!(log_file.exists(), "broker {} missing log file: {log_file:?}", i + 1);

        // Mount the host directory into the docker tool container and dump.
        let mount = format!("{}:/data:ro", partition_dir.display());
        let out = Command::new("docker")
            .args([
                "run", "--rm", "-v", &mount,
                KAFKA_IMAGE,
                "kafka-dump-log",
                "--files", "/data/00000000000000000000.log",
                "--print-data-log",
            ])
            .output().expect("spawn dump-log");
        assert!(out.status.success(),
            "dump-log failed for broker {}: {}",
            i + 1, String::from_utf8_lossy(&out.stderr));
        dumps.push(String::from_utf8_lossy(&out.stdout).to_string());
    }

    // All 3 dumps should be identical.
    assert_eq!(dumps[0], dumps[1], "broker 1 vs broker 2 dump differ");
    assert_eq!(dumps[1], dumps[2], "broker 2 vs broker 3 dump differ");

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
```

The `-v <host>:/data:ro` mount makes the broker's local partition dir visible inside the `kafka-dump-log` container. The `host.docker.internal` bridge entry is already set up by the slice-6 CI workflow step (`/etc/hosts`).

- [ ] **Step 3: Build + commit**

```bash
cargo check -p crabka-broker --tests
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(broker): JVM acceptance — 3-node replication byte-compare

Creates a 3-broker Crabka cluster on fixed ports, asks
kafka-topics for a replication_factor=3 topic, waits for ISR to
include all 3 brokers, produces 100 records via kafka-console-producer,
then runs kafka-dump-log against each broker's local partition file
and asserts all three dumps are byte-identical.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

The test is `#[ignore]`-gated; CI runs it via `--include-ignored`.

---

## Phase G — Rustdoc + acceptance gate + PR

### Task 14: Rustdoc + acceptance gate + PR

- [ ] **Step 1: Crate-level rustdoc updates**

Append a `## Replication` section to `crates/broker/src/lib.rs`'s crate-level rustdoc:

```rust
//! ## Replication (slice 8)
//!
//! `CreateTopics` with `replication_factor > 1` assigns N replicas per
//! partition via round-robin over `MetadataImage::brokers()`. The
//! [`replicator_supervisor`] subscribes to controller metadata changes
//! and spawns a [`replicator`] task per partition where this broker is
//! a non-leader replica. Each replicator opens a [`crabka_client_core::Client`]
//! to its partition's leader and loops on `Fetch` with `replica_id` set,
//! appending every returned `RecordBatch` to the local log.
//!
//! ISR shrink/expand, high-watermark tracking, `acks=all` blocking,
//! AlterPartition RPC, leader-election-on-failure, and cross-broker
//! producer routing are deferred — see the slice 8 design spec.
```

- [ ] **Step 2: Full local acceptance gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
# Docker-gated JVM acceptance:
cargo test --workspace -- --include-ignored
```

All MUST be clean.

- [ ] **Step 3: Commit any final cleanups**

```bash
git add -A
git commit -m "$(cat <<'EOF'
docs(broker): crate-level rustdoc on replication (slice 8)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 4: Push + open PR**

```bash
git push -u origin feature/replication
gh pr create --base main --head feature/replication \
    --title "Slice 8: basic partition replication" \
    --body "$(cat <<'PRBODY'
## Summary

Multi-broker partition replication for Crabka. `CreateTopics` with `replication_factor=N` assigns N replicas per partition via deterministic round-robin from `MetadataImage::brokers()`; each follower runs a per-(topic, partition) replication task that issues standard Kafka `Fetch` requests against the leader (with `replica_id` set) and appends every returned batch to its local `crabka-log`. After this slice, a 3-broker cluster with `replication_factor=3` has each partition's records on every replica's local disk, byte-compatible with `kafka-dump-log`.

## What landed

- `crates/broker/src/handlers/create_topics.rs`: round-robin replica assignment over `MetadataImage::brokers()`; rejects RF > broker count with `INVALID_REPLICATION_FACTOR (38)`.
- `crates/broker/src/handlers/fetch.rs`: branch on `replica_id` (follower vs consumer; no-op fork in slice 8 because HW tracking is deferred).
- `crates/broker/src/replicator.rs` (new): per-partition fetch loop with `OFFSET_OUT_OF_RANGE` truncate-to-0 recovery and transport-retry backoff.
- `crates/broker/src/replicator_supervisor.rs` (new): subscribes to `controller.watch_image()`, diffs the running task set on each metadata apply.
- `crates/broker/src/partition.rs`: `Partition::replicate_batch` for caller-assigned-offset appends.
- `crates/log/src/log.rs`: `Log::append_at` for caller-assigned-offset writes.
- Tests: 4 round-robin unit tests, 4 supervisor reconcile unit tests, 2 in-process 3-node integration tests, 1 JVM `kafka-dump-log` acceptance test.

## Out of scope (deferred to future slice-8 follow-ups)

- ISR shrink/expand on lag.
- High-watermark tracking + `acks=all` blocking.
- AlterPartition RPC.
- Controller-driven leader election on broker failure.
- Cross-broker Rust producer routing (slice-6 follow-up).
- KIP-101 leader-epoch + KIP-279 truncation safety.
- Per-source-broker batched ReplicaFetcherThread (optimization).

## Reference

Spec: `docs/superpowers/specs/2026-05-12-crabka-replication-design.md`
Plan: `docs/superpowers/plans/2026-05-12-crabka-replication.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

Report the PR URL.

---

## Self-review against the spec

| # | Spec section / requirement                                                | Plan task |
|---|---------------------------------------------------------------------------|-----------|
| 1 | Round-robin replica placement over `MetadataImage::brokers()`             | Tasks 1, 2 |
| 2 | `INVALID_REPLICATION_FACTOR (38)` when RF > broker count                   | Tasks 1, 2, 3 |
| 3 | `CreateTopics` materializes on-disk partition only for the local leader   | Task 2 |
| 4 | `Fetch` handler branches on `replica_id`                                  | Task 4 |
| 5 | Per-partition `replicator::run` fetch loop                                | Tasks 5, 6, 7 |
| 6 | `OFFSET_OUT_OF_RANGE` truncate-to-0 + re-fetch                            | Task 6 |
| 7 | `NOT_LEADER_FOR_PARTITION` stops the task                                  | Task 6 |
| 8 | `UNKNOWN_TOPIC_OR_PARTITION` retries with backoff (CreateTopics race)      | Task 6 |
| 9 | Transport-error reconnect with exponential backoff                        | Tasks 6, 7 |
| 10 | `replicator_supervisor` subscribes to `controller.watch_image()`         | Task 8 |
| 11 | Diff-and-spawn / diff-and-cancel reconcile per metadata apply             | Task 8 |
| 12 | Reconcile unit tests (self-follower, self-leader, not-replica, multi)    | Task 9 |
| 13 | Wire supervisor into `Broker::start`; cancel on shutdown                  | Task 10 |
| 14 | Layer-3 `replication_factor_three_propagates_to_all_followers`            | Task 11 |
| 15 | Layer-3 `out_of_range_truncates_and_recovers`                             | Task 12 |
| 16 | Layer-4 `three_node_replication_byte_compare` (JVM `kafka-dump-log`)      | Task 13 |
| 17 | Rustdoc + acceptance gate + PR                                            | Task 14 |
| 18 | `BrokerError::Replication` variant                                        | Task 5 |
| 19 | `Partition::replicate_batch` for caller-assigned offsets                  | Task 6 |
| 20 | `Log::append_at` for caller-assigned offsets                              | Task 6 |
| 21 | Test-only `Log::test_set_log_start` for out-of-range simulation           | Task 12 |
| 22 | `BrokerHandle::broker_count` / `has_partition` / `local_log_end_offset`   | Task 11 |
| 23 | Test-only `BrokerHandle::test_truncate_local_log` / `test_advance_log_start` | Task 12 |
| 24 | `#[cfg(not(target_os = "windows"))]` gate on `replication.rs`             | Task 11 |

**Placeholder scan:** No `TBD`/`TODO` markers. One ADAPTATION note in Task 11 about copying `start_n_node_with_retry` from `quorum.rs` (calls for the implementer to either copy or extract into a shared support module — the choice is between two clear options, not unspecified). The `todo!()` in the example body of Task 11 is illustrative of "see quorum.rs"; the implementer should paste the actual helper.

**Type consistency:** `replicator::Config`, `ReplicatorSupervisor`, `desired_follower_set`, `Partition::replicate_batch`, `Log::append_at` all named consistently across tasks. `NodeId = u64` from `crabka-raft` used everywhere. `MetadataImage::brokers()` / `partitions_of(topic)` / `partition(name, idx)` / `broker(node_id)` are the slice-7 accessors confirmed to exist.

The plan is ready for execution.
