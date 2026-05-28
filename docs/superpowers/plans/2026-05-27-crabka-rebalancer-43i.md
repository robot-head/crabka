# Rebalancer 43i — State migration to internal Crabka topic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `{data_dir}/in_flight.json` writes/reads/deletes in the rebalancer's executor with produces/consumes against a compacted internal topic `__crabka_rebalancer_state`. Gate state-dependent API endpoints + `/readyz` until the topic is loaded.

**Architecture:** New `state_topic/` module containing a `StateTopic` handle (`write` / `delete` / `loaded` / `is_loaded`), a `StateTopicLoader` background task that consume-from-beginning + end-of-log-detect via a 5-poll quiet period, and a `StateProducer` wrapper. All three use `crabka_client_core::Client` directly (matches the existing `ingest::admin_client` pattern). Topic auto-created via the existing AdminClient surface on startup with `cleanup.policy=compact`, single partition, configurable replication. Executor swaps `InFlightFile::write/load/delete` calls for `StateTopic::write/loaded/delete`.

**Tech Stack:** Rust 1.95, `crabka_client_core::Client`, `crabka_client_admin::CreateTopicSpec`, `crabka_protocol::owned::{produce_request, fetch_request, fetch_response}`, `arc_swap::ArcSwap`, `tokio`, existing rebalancer workspace member.

**Spec:** `docs/superpowers/specs/2026-05-27-crabka-rebalancer-43i-design.md`

**Branch:** Create a new branch off `main` named `rebalancer-43i`.

---

## Pre-flight: branch + baseline

- [ ] **Step 1: Create the branch on main**

```bash
git checkout main && git pull --ff-only
git checkout -b rebalancer-43i
```

- [ ] **Step 2: Verify the rebalancer crate baseline**

```bash
cargo test -p crabka-rebalancer --lib 2>&1 | tail -10
cargo build -p crabka-rebalancer --bins 2>&1 | tail -3
```

Expected: all existing tests pass; clean build.

---

## File structure

| File | Responsibility |
|---|---|
| `crates/rebalancer/src/state_topic/mod.rs` (new) | `StateTopic` handle: write / delete / loaded / is_loaded. Re-exports child types. |
| `crates/rebalancer/src/state_topic/error.rs` (new) | `StateTopicError` (thiserror). |
| `crates/rebalancer/src/state_topic/producer.rs` (new) | One function: `produce_state(client, topic, key, value: Option<Bytes>) -> Result<(), StateTopicError>`. Builds + sends a `ProduceRequest` directly via `Client`. |
| `crates/rebalancer/src/state_topic/loader.rs` (new) | `StateTopicLoader::run(client, topic, store)` background task. Consumes the topic via raw `FetchRequest`s from offset 0, follows compaction by keeping the last record per key in memory, calls `store.set_loaded(value)` once 5 consecutive polls yielded no new records (~500ms quiet period). |
| `crates/rebalancer/src/state_topic/topic_admin.rs` (new) | `ensure_topic(client, name, replication)`: idempotent topic create with the right compaction configs. |
| `crates/rebalancer/src/state_topic/serde_format.rs` (new) | `encode(&InFlightFile) -> Bytes` + `decode(&[u8]) -> Result<InFlightFile, StateTopicError>`. Wraps the existing `serde_json` round-trip; isolated so the encoding is swappable later. |
| `crates/rebalancer/src/state_topic/test_double.rs` (new, `#[cfg(test)]`) | `InMemoryStateTopic`: shared-state test double the executor tests use. |
| `crates/rebalancer/src/executor/mod.rs` (modify) | Swap the three `InFlightFile::write/load/delete` call sites for `state_topic.write/loaded/delete`. Inject `StateTopic` via `ExecutorState`. |
| `crates/rebalancer/src/api/...` (modify) | The `/readyz` handler + the `execute` endpoint check `state_topic.is_loaded()`. |
| `crates/rebalancer/src/bin/rebalancer.rs` (modify) | Add CLI flags; ensure topic; construct `StateTopic`; spawn `StateTopicLoader`; pass handle to executor + API. |
| `crates/rebalancer/tests/state_topic.rs` (new) | testcontainers-backed integration test: round-trip + tombstone + auto-create. |

---

## Task 1: `state_topic` module skeleton + error type

**Files:**
- Create: `crates/rebalancer/src/state_topic/mod.rs`
- Create: `crates/rebalancer/src/state_topic/error.rs`
- Create: `crates/rebalancer/src/state_topic/serde_format.rs`
- Modify: `crates/rebalancer/src/lib.rs` (add `pub mod state_topic;`)

### Step 1: Add `state_topic` module declaration

In `crates/rebalancer/src/lib.rs`, find the existing `pub mod ingest;` etc. and add:

```rust
pub mod state_topic;
```

Keep it alphabetical with neighboring `pub mod` lines.

### Step 2: Write `error.rs`

```rust
//! Error types for the state-topic subsystem.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateTopicError {
    #[error("client error: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("admin error: {0}")]
    Admin(#[from] crabka_client_admin::AdminError),

    #[error("produce returned error code {code}")]
    ProduceErrorCode { code: i16 },

    #[error("fetch returned error code {code}")]
    FetchErrorCode { code: i16 },

    #[error("malformed json: {0}")]
    MalformedJson(#[from] serde_json::Error),

    #[error("state load did not converge within timeout")]
    LoadTimeout,
}
```

### Step 3: Write `serde_format.rs`

```rust
//! Wire-format isolation for state-topic records. Today this is
//! `serde_json::to_vec` over `InFlightFile`; swapping to bincode
//! or protobuf is a one-function change behind these helpers.

use bytes::Bytes;

use crate::executor::state::InFlightFile;
use crate::state_topic::error::StateTopicError;

pub(crate) fn encode(f: &InFlightFile) -> Result<Bytes, StateTopicError> {
    let v = serde_json::to_vec(f)?;
    Ok(Bytes::from(v))
}

pub(crate) fn decode(bytes: &[u8]) -> Result<InFlightFile, StateTopicError> {
    let f: InFlightFile = serde_json::from_slice(bytes)?;
    Ok(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::state::Phase;

    #[test]
    fn round_trip_preserves_all_fields() {
        let f = InFlightFile::new("p-abc".into(), Phase::Wait, 1234, 50_000_000);
        let bytes = encode(&f).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.proposal_id, f.proposal_id);
        assert_eq!(back.phase, f.phase);
        assert_eq!(back.started_at_ms, f.started_at_ms);
        assert_eq!(back.throttle_bytes_per_sec, f.throttle_bytes_per_sec);
        assert_eq!(back.version, f.version);
    }

    #[test]
    fn decode_rejects_malformed_json() {
        let err = decode(b"{not json").unwrap_err();
        assert!(matches!(err, StateTopicError::MalformedJson(_)));
    }
}
```

### Step 4: Write `mod.rs` (skeleton)

```rust
//! Slice 43i: rebalancer state persistence via an internal compacted
//! topic on the Crabka cluster being managed. Replaces the slice-43b
//! `{data_dir}/in_flight.json` file. Survives pod restart; prerequisite
//! for multi-replica HA (slice 43j).

mod error;
pub(crate) mod serde_format;

pub use error::StateTopicError;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;

use crate::executor::state::InFlightFile;

/// In-memory mirror of the latest record under the `STATE_KEY` on the
/// state topic. Populated by `StateTopicLoader` at startup and by
/// `StateTopic::write` / `delete` thereafter.
#[derive(Debug, Default)]
pub struct LoadedState {
    pub value: ArcSwap<Option<InFlightFile>>,
    pub is_loaded: AtomicBool,
}

impl LoadedState {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            value: ArcSwap::from_pointee(None),
            is_loaded: AtomicBool::new(false),
        })
    }

    pub fn current(&self) -> Option<InFlightFile> {
        let guard = self.value.load();
        let opt: &Option<InFlightFile> = &guard;
        opt.clone()
    }

    pub fn is_loaded(&self) -> bool {
        self.is_loaded.load(Ordering::Acquire)
    }

    pub(crate) fn store(&self, value: Option<InFlightFile>) {
        self.value.store(Arc::new(value));
    }

    pub(crate) fn mark_loaded(&self) {
        self.is_loaded.store(true, Ordering::Release);
    }
}

/// The fixed key under which the executor's state is published. Single
/// in-flight record per topic; tombstone (null value) clears it.
pub const STATE_KEY: &str = "in_flight";
```

### Step 5: Build + run the serde-format tests

```bash
cargo build -p crabka-rebalancer --lib 2>&1 | tail -3
cargo test -p crabka-rebalancer --lib state_topic::serde_format 2>&1 | tail -10
```

Expected: clean build; 2 tests pass.

### Step 6: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 7: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/state_topic/ crates/rebalancer/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(state-topic): module skeleton — error + serde-format + LoadedState handle"
```

---

## Task 2: Topic-create helper

**Files:**
- Create: `crates/rebalancer/src/state_topic/topic_admin.rs`
- Modify: `crates/rebalancer/src/state_topic/mod.rs` (add `pub(crate) mod topic_admin;`)

### Step 1: Inspect the existing AdminClient surface to anchor

```bash
sed -n '1,80p' crates/client-admin/src/topics.rs
```

You should see `CreateTopicSpec { name, partitions, replication_factor, configs: Vec<(String, String)> }` and an admin-client method that takes a slice of these and returns outcomes. Use whichever type names actually exist; the example below uses the names visible at the time of writing.

### Step 2: Write `topic_admin.rs`

```rust
//! Idempotent topic-create for the rebalancer's state topic. Run once
//! at startup; existing topic is left alone.

use crabka_client_admin::{CreateTopicSpec, CreatableTopicConfig, AdminClient};

use crate::state_topic::error::StateTopicError;

/// Create the state topic if missing, with the compaction configs the
/// loader expects. `replication_factor` is the requested value; the
/// broker may downgrade it (or reject) based on the live broker count.
///
/// Idempotent: if the topic already exists with any config, this is a
/// no-op (the existing topic's configs are NOT updated; that's a
/// separate operator slice).
pub async fn ensure_topic(
    admin: &AdminClient,
    name: &str,
    replication_factor: i16,
) -> Result<(), StateTopicError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: 1,
        replication_factor,
        configs: vec![
            CreatableTopicConfig { name: "cleanup.policy".into(),               value: Some("compact".into()) },
            CreatableTopicConfig { name: "min.cleanable.dirty.ratio".into(),    value: Some("0.01".into()) },
            CreatableTopicConfig { name: "segment.ms".into(),                   value: Some("60000".into()) },
        ],
    };
    let outcomes = admin
        .create_topics(&[spec], /* timeout_ms */ 10_000)
        .await?;
    for o in outcomes {
        // The exact "topic already exists" error code is 36 (TOPIC_ALREADY_EXISTS);
        // treat it as success. Anything else is a hard error.
        match o.error_code {
            0 | 36 => {}
            code => return Err(StateTopicError::ProduceErrorCode { code }),
        }
    }
    Ok(())
}
```

If the actual `CreateTopicSpec` / `AdminClient::create_topics` field names differ from the snippet above (e.g. `replication_factor` is named `replication`, or the config wrapper has a different shape), check `crates/client-admin/src/topics.rs` lines 17–80 and the call shape at line 75, and adapt. The semantic intent (one partition, compact policy, configurable replication, idempotent on already-exists) is what matters.

### Step 3: Add `pub(crate) mod topic_admin;` to `state_topic/mod.rs`

Edit `crates/rebalancer/src/state_topic/mod.rs` and add the new module declaration:

```rust
mod error;
pub(crate) mod serde_format;
pub(crate) mod topic_admin;
```

### Step 4: Verify compilation

```bash
cargo build -p crabka-rebalancer --lib 2>&1 | tail -3
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -3
```

Expected: clean. No new unit tests at this stage (testcontainers covers `ensure_topic` end-to-end in Task 6).

### Step 5: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/state_topic/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(state-topic): topic_admin::ensure_topic helper"
```

---

## Task 3: Producer helper

**Files:**
- Create: `crates/rebalancer/src/state_topic/producer.rs`
- Modify: `crates/rebalancer/src/state_topic/mod.rs` (add `pub(crate) mod producer;`)

### Step 1: Inspect existing produce-path call shape

```bash
grep -rn "ProduceRequest\b" crates/rebalancer/src/ crates/client-admin/src/ | head
```

If no existing produce-via-Client example is available in the rebalancer, anchor on the protocol struct at `crates/protocol/generated/ProduceRequest.owned.rs` — `pub struct ProduceRequest { transactional_id: Option<String>, acks: i16, timeout_ms: i32, topic_data: Vec<TopicProduceData>, … }`.

### Step 2: Write `producer.rs`

```rust
//! Single-key produce path for the state topic. Built directly on
//! `crabka_client_core::Client` to match the rebalancer's
//! `ingest::admin_client` pattern; we don't pull in the high-level
//! `crabka-client-producer` for a one-key-per-write workload.

use bytes::Bytes;

use crabka_client_core::Client;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::owned::{Record, RecordBatch};
use crabka_protocol::records::RecordsPayload;

use crate::state_topic::error::StateTopicError;

/// Produce a single record to `(topic, partition=0)`. `value=None` is
/// a tombstone (null value), matching Kafka compaction semantics.
/// `acks=all`, `timeout_ms=10_000`.
pub(crate) async fn produce_state(
    client: &Client,
    topic: &str,
    key: &str,
    value: Option<Bytes>,
) -> Result<(), StateTopicError> {
    let record = Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value,
        ..Default::default()
    };
    let batch = RecordBatch {
        records: vec![record],
        ..Default::default()
    };
    let req = ProduceRequest {
        transactional_id: None,
        acks: -1, // all
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(batch).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = client.send(req).await?;
    for t in &resp.responses {
        for p in &t.partition_responses {
            if p.error_code != 0 {
                return Err(StateTopicError::ProduceErrorCode { code: p.error_code });
            }
        }
    }
    Ok(())
}
```

If `RecordsPayload::V2(batch).into()` doesn't compile (the records-field type), grep for an existing produce call site in the rebalancer or broker: `grep -rn "ProduceRequest {" crates/broker/src/ crates/client-producer/src/` — adapt to whatever wrap the `records` field expects today. The struct shape is auto-generated; field names should match exactly. The semantic intent is: single record, key=`key`, value=`value`, partition 0, acks=all.

### Step 3: Add to `state_topic/mod.rs`

```rust
mod error;
pub(crate) mod producer;
pub(crate) mod serde_format;
pub(crate) mod topic_admin;
```

### Step 4: Verify compilation + clippy

```bash
cargo build -p crabka-rebalancer --lib 2>&1 | tail -3
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -3
```

Expected: clean. (Integration test in Task 6 exercises the produce path against a real broker.)

### Step 5: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/state_topic/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(state-topic): producer::produce_state for single-key writes + tombstones"
```

---

## Task 4: Loader (background task)

**Files:**
- Create: `crates/rebalancer/src/state_topic/loader.rs`
- Modify: `crates/rebalancer/src/state_topic/mod.rs` (add `pub mod loader;`)

### Step 1: Write `loader.rs`

```rust
//! Background task: consume the state topic from offset 0, track the
//! latest non-tombstone value, and flip `LoadedState::is_loaded` once
//! the consumer has seen no new records for 5 consecutive 100ms polls
//! (the "quiet period" end-of-log heuristic).

use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::Client;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::records::RecordsPayload;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::state_topic::error::StateTopicError;
use crate::state_topic::{LoadedState, STATE_KEY, serde_format};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const QUIET_POLLS_TO_DECLARE_LOADED: u32 = 5;
const MAX_BYTES_PER_FETCH: i32 = 1 << 20; // 1 MiB

pub struct StateTopicLoader {
    pub client: Arc<Client>,
    pub topic: String,
    pub state: Arc<LoadedState>,
    pub shutdown: CancellationToken,
}

impl StateTopicLoader {
    pub async fn run(self) {
        info!(topic = %self.topic, "state-topic loader started");
        let mut next_offset: i64 = 0;
        let mut quiet_polls: u32 = 0;
        loop {
            tokio::select! {
                () = tokio::time::sleep(POLL_INTERVAL) => {}
                () = self.shutdown.cancelled() => {
                    info!("state-topic loader shutting down");
                    return;
                }
            }
            match self.poll_once(next_offset).await {
                Ok(records) => {
                    let saw_new = !records.is_empty();
                    for (offset, key, value) in records {
                        if key.as_deref() != Some(STATE_KEY.as_bytes()) {
                            continue; // ignore unknown keys
                        }
                        match value {
                            None => self.state.store(None),
                            Some(bytes) => match serde_format::decode(&bytes) {
                                Ok(f) => self.state.store(Some(f)),
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        offset,
                                        "state-topic record had malformed JSON; skipping"
                                    );
                                }
                            },
                        }
                        next_offset = offset + 1;
                    }
                    if saw_new {
                        quiet_polls = 0;
                    } else {
                        quiet_polls += 1;
                        if quiet_polls >= QUIET_POLLS_TO_DECLARE_LOADED
                            && !self.state.is_loaded()
                        {
                            info!("state-topic load reached steady state; marking loaded");
                            self.state.mark_loaded();
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "state-topic poll failed; will retry");
                    // Do NOT advance offset; do NOT count as quiet.
                }
            }
        }
    }

    async fn poll_once(
        &self,
        fetch_offset: i64,
    ) -> Result<Vec<(i64, Option<Vec<u8>>, Option<Vec<u8>>)>, StateTopicError> {
        let req = FetchRequest {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: MAX_BYTES_PER_FETCH,
            topics: vec![FetchTopic {
                topic: self.topic.clone(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset,
                    partition_max_bytes: MAX_BYTES_PER_FETCH,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = self.client.send(req).await?;
        let mut out: Vec<(i64, Option<Vec<u8>>, Option<Vec<u8>>)> = Vec::new();
        for t in &resp.responses {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(StateTopicError::FetchErrorCode { code: p.error_code });
                }
                let Some(payload) = &p.records else { continue };
                let RecordsPayload::V2(batch) = payload else { continue };
                for (i, r) in batch.records.iter().enumerate() {
                    let off = batch.base_offset + i as i64;
                    out.push((
                        off,
                        r.key.as_ref().map(|b| b.to_vec()),
                        r.value.as_ref().map(|b| b.to_vec()),
                    ));
                }
            }
        }
        Ok(out)
    }
}
```

The exact field names on `FetchRequest`/`FetchTopic`/`FetchPartition` are auto-generated; if any field name differs, grep `crates/protocol/generated/FetchRequest.owned.rs` and adapt. Pay attention to:
- `topic` vs `name` on `FetchTopic`
- `partition` vs `partition_index` on `FetchPartition`
- `partitions` vs `partition_data` field name on the response

### Step 2: Add to `state_topic/mod.rs`

```rust
mod error;
pub mod loader;
pub(crate) mod producer;
pub(crate) mod serde_format;
pub(crate) mod topic_admin;

pub use loader::StateTopicLoader;
```

### Step 3: Build + clippy

```bash
cargo build -p crabka-rebalancer --lib 2>&1 | tail -3
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -5
```

Expected: clean. Loader correctness is verified end-to-end in Task 6.

### Step 4: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/state_topic/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(state-topic): StateTopicLoader background consume-from-beginning task"
```

---

## Task 5: `StateTopic` handle + executor swap

**Files:**
- Modify: `crates/rebalancer/src/state_topic/mod.rs` (add `StateTopic` struct + write/delete impls)
- Modify: `crates/rebalancer/src/executor/mod.rs` (swap call sites; inject `StateTopic`)
- Modify: `crates/rebalancer/src/executor/state.rs` (keep `InFlightFile` struct; tests will need updating)

### Step 1: Add the `StateTopic` handle

Append to `crates/rebalancer/src/state_topic/mod.rs`:

```rust
use bytes::Bytes;
use crabka_client_core::Client;

use crate::executor::state::InFlightFile;

/// Public handle the executor + API consume. Cheap to clone (all Arc).
#[derive(Clone)]
pub struct StateTopic {
    client: Arc<Client>,
    topic: String,
    pub(crate) state: Arc<LoadedState>,
}

impl StateTopic {
    #[must_use]
    pub fn new(client: Arc<Client>, topic: String, state: Arc<LoadedState>) -> Self {
        Self { client, topic, state }
    }

    /// Snapshot the latest known in-flight record. Returns `None` if
    /// the topic is empty / has been tombstoned, or if the load
    /// hasn't completed yet (caller must check `is_loaded` first).
    pub fn loaded(&self) -> Option<InFlightFile> {
        self.state.current()
    }

    pub fn is_loaded(&self) -> bool {
        self.state.is_loaded()
    }

    /// Produce a record (no tombstone) and locally mirror it into
    /// `loaded`. The executor reads `loaded()` immediately after,
    /// so we don't need to wait for the loader to round-trip the
    /// write back through the topic.
    pub async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
        let value = serde_format::encode(f)?;
        producer::produce_state(&self.client, &self.topic, STATE_KEY, Some(value)).await?;
        self.state.store(Some(f.clone()));
        Ok(())
    }

    /// Tombstone the state key; locally mirrors `None`.
    pub async fn delete(&self) -> Result<(), StateTopicError> {
        producer::produce_state(&self.client, &self.topic, STATE_KEY, None).await?;
        self.state.store(None);
        Ok(())
    }
}
```

Also re-export `StateTopic`:

```rust
pub use loader::StateTopicLoader;
// add:
pub use error::StateTopicError;
```

(`StateTopicError` may already be re-exported via `pub use error::StateTopicError;` from earlier; verify and don't double.)

### Step 2: Inspect the existing executor call sites

```bash
grep -n "InFlightFile::write\|InFlightFile::load\|InFlightFile::delete\|InFlightFile::new\|data_dir" crates/rebalancer/src/executor/mod.rs | head -15
```

You should see roughly three call sites: a `.write()` after constructing an `InFlightFile`, a `.load()` on entry to the `ClearThrottle` phase, and a `.delete()` after a phase completes. Match the actual line numbers from the grep output.

### Step 3: Inject the state-backend trait into `ExecutorState`

(The `StateBackend` trait is defined in Step 5 below. Step 3 declares the field shape and Step 4 swaps the call sites; both reference the trait that Step 5 introduces. Read Step 5 first if needed.)

In `crates/rebalancer/src/executor/mod.rs`, find the `ExecutorState` struct (search for `pub data_dir: PathBuf`). Add a new field:

```rust
pub state_topic: std::sync::Arc<dyn crate::state_topic::StateBackend + Send + Sync>,
```

The `data_dir: PathBuf` field stays (anomaly store still uses it). Add a doc note above it: `// Anomaly-store only post-43i; executor state lives on the cluster.`

### Step 4: Swap the three call sites

Find each `InFlightFile::write(...)` / `::load(...)` / `::delete(...)` call in `executor/mod.rs` and replace:

```rust
// Was:
let mut f = InFlightFile::new(proposal_id.clone(), phase, started_at_ms, throttle);
f.write(&self.state.config.data_dir)?;

// Now:
let f = InFlightFile::new(proposal_id.clone(), phase, started_at_ms, throttle);
self.state.state_topic.write(&f).await
    .map_err(|e| StateError::Io(std::io::Error::other(format!("state topic write: {e}"))))?;
```

```rust
// Was:
Phase::ClearThrottle => InFlightFile::load(&self.state.config.data_dir)
    .map_err(...)?,

// Now:
Phase::ClearThrottle => self.state.state_topic.loaded(),
```

```rust
// Was:
InFlightFile::delete(&self.state.config.data_dir)?;

// Now:
self.state.state_topic.delete().await
    .map_err(|e| StateError::Io(std::io::Error::other(format!("state topic delete: {e}"))))?;
```

The exact error-conversion shim depends on the function's return type. If `StateError` has a more natural variant for "topic write failed," add one and use it; the `Io(...)` wrap above is a fallback that doesn't require touching `StateError`.

### Step 5: Introduce a `StateBackend` trait so executor tests can use an in-memory fake

The executor needs to be testable without touching a real broker. Introduce a `StateBackend` trait, make `StateTopic` implement it, and have `ExecutorState.state_topic` hold a `Arc<dyn StateBackend>`.

Update `crates/rebalancer/src/state_topic/mod.rs`:

```rust
#[async_trait::async_trait]
pub trait StateBackend: Send + Sync {
    fn loaded(&self) -> Option<InFlightFile>;
    fn is_loaded(&self) -> bool;
    async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError>;
    async fn delete(&self) -> Result<(), StateTopicError>;
}

#[async_trait::async_trait]
impl StateBackend for StateTopic {
    fn loaded(&self) -> Option<InFlightFile> { self.state.current() }
    fn is_loaded(&self) -> bool { self.state.is_loaded() }
    async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
        let value = serde_format::encode(f)?;
        producer::produce_state(&self.client, &self.topic, STATE_KEY, Some(value)).await?;
        self.state.store(Some(f.clone()));
        Ok(())
    }
    async fn delete(&self) -> Result<(), StateTopicError> {
        producer::produce_state(&self.client, &self.topic, STATE_KEY, None).await?;
        self.state.store(None);
        Ok(())
    }
}
```

Add `async-trait = "0.1"` to `crates/rebalancer/Cargo.toml` if not already present. `ExecutorState.state_topic: Arc<dyn StateBackend>` then takes the trait object.

In tests, hand-roll a tiny `InMemoryBackend` (~30 lines):

```rust
#[cfg(test)]
pub mod fake {
    use std::sync::Mutex;
    use async_trait::async_trait;
    use super::*;

    #[derive(Default)]
    pub struct InMemoryBackend {
        pub state: Mutex<Option<InFlightFile>>,
        pub loaded_flag: std::sync::atomic::AtomicBool,
    }

    impl InMemoryBackend {
        pub fn new_loaded() -> Self {
            Self {
                state: Mutex::new(None),
                loaded_flag: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl StateBackend for InMemoryBackend {
        fn loaded(&self) -> Option<InFlightFile> {
            self.state.lock().unwrap().clone()
        }
        fn is_loaded(&self) -> bool {
            self.loaded_flag.load(std::sync::atomic::Ordering::Acquire)
        }
        async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> {
            *self.state.lock().unwrap() = Some(f.clone());
            Ok(())
        }
        async fn delete(&self) -> Result<(), StateTopicError> {
            *self.state.lock().unwrap() = None;
            Ok(())
        }
    }
}
```

Executor tests use `Arc::new(InMemoryBackend::new_loaded())` for the `state_topic` field.

### Step 6: Run the executor tests

```bash
cargo test -p crabka-rebalancer --lib executor 2>&1 | tail -15
```

Expected: all executor tests pass with the new backend.

### Step 7: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --lib --tests -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 8: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/state_topic/ crates/rebalancer/src/executor/ \
        crates/rebalancer/Cargo.toml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer: executor uses StateBackend trait; default impl is the topic-backed StateTopic"
```

---

## Task 6: Binary wiring + `/readyz` + `execute` gating + integration test

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs` (CLI flags + topic-create + StateTopic construction + loader spawn + injection)
- Modify: `crates/rebalancer/src/api/...` (find the `/readyz` + `execute` handlers; gate on `state_topic.is_loaded()`)
- Create: `crates/rebalancer/tests/state_topic.rs` (testcontainers integration test)

### Step 1: Add CLI flags

In `crates/rebalancer/src/bin/rebalancer.rs`, add to the `Args` struct (with the rest of the rebalancer's flags):

```rust
/// Name of the internal compacted topic the rebalancer uses to
/// persist executor state. Survives pod restart. Created on first
/// startup with `cleanup.policy=compact`, single partition.
#[arg(
    long,
    env = "CRABKA_REBALANCER_STATE_TOPIC",
    default_value = "__crabka_rebalancer_state"
)]
state_topic_name: String,

/// Replication factor for the state topic at create time. Capped at
/// broker count by the broker if the cluster has fewer brokers.
#[arg(
    long,
    env = "CRABKA_REBALANCER_STATE_TOPIC_REPLICATION",
    default_value_t = 3
)]
state_topic_replication: i16,

/// Soft deadline for state-topic load at startup; the loader emits
/// a WARN and keeps retrying past this. `/readyz` stays 503 until
/// the load completes successfully.
#[arg(
    long,
    env = "CRABKA_REBALANCER_STATE_LOAD_TIMEOUT_SECS",
    default_value_t = 60
)]
state_load_timeout_secs: u64,
```

### Step 2: Wire topic-create + StateTopic + loader

In the binary's `main()` after the admin client + producer client are constructed and before the executor + API are wired, add:

```rust
// Slice 43i: ensure the state topic exists; spawn the background loader.
let admin = crabka_client_admin::AdminClient::new(client.clone());
crabka_rebalancer::state_topic::topic_admin::ensure_topic(
    &admin,
    &args.state_topic_name,
    args.state_topic_replication,
)
.await
.map_err(|e| anyhow::anyhow!("ensure state topic: {e}"))?;

let loaded_state = crabka_rebalancer::state_topic::LoadedState::new();
let state_topic = crabka_rebalancer::state_topic::StateTopic::new(
    client.clone(),
    args.state_topic_name.clone(),
    loaded_state.clone(),
);
let loader = crabka_rebalancer::state_topic::StateTopicLoader {
    client: client.clone(),
    topic: args.state_topic_name.clone(),
    state: loaded_state.clone(),
    shutdown: shutdown.clone(),
};
tokio::spawn(loader.run());

info!(
    topic = %args.state_topic_name,
    "state topic ready; loader spawned"
);
```

Then wherever `ExecutorState` is constructed, pass `Arc::new(state_topic.clone())` (boxed-as-`dyn StateBackend`) as the `state_topic` field.

Where the API router is built, also pass `state_topic.clone()` so the handlers can read `is_loaded()`.

The exact construction sites depend on the binary's existing layout — locate them via:

```bash
grep -n "ExecutorState\b\|app::router\|api::router\|Router::new" crates/rebalancer/src/bin/rebalancer.rs | head
```

### Step 3: Gate `/readyz` and `/execute`

Find the API router setup:

```bash
grep -rn "readyz\|/api/v1/proposals.*execute" crates/rebalancer/src/api/ 2>&1 | head
```

For `/readyz`, wherever the handler returns `200`, change to:

```rust
async fn readyz_handler(
    State(state): State<ApiState>,
) -> impl IntoResponse {
    if state.state_topic.is_loaded() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "state topic loading")
    }
}
```

For `POST /api/v1/proposals/{id}/execute`, at the top of the handler:

```rust
if !state.state_topic.is_loaded() {
    return (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "status": "loading",
            "message": "state topic not yet loaded; retry shortly"
        })),
    ).into_response();
}
```

(Adjust to whatever the existing handler-return shape is.)

### Step 4: Write the integration test

Create `crates/rebalancer/tests/state_topic.rs`:

```rust
//! Slice 43i: end-to-end round-trip against a real broker.
//!
//! Requires Docker; gated `#[ignore]` and CI runs with `--include-ignored`.

#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::Duration;

use crabka_client_core::{Client, ClientConfig};
use crabka_client_admin::AdminClient;
use crabka_rebalancer::executor::state::{InFlightFile, Phase};
use crabka_rebalancer::state_topic::{LoadedState, StateTopic, StateTopicLoader, topic_admin};
use tokio_util::sync::CancellationToken;

async fn connect(bootstrap: &str) -> Arc<Client> {
    let cfg = ClientConfig::new(bootstrap.parse().unwrap());
    Arc::new(Client::connect(cfg).await.expect("connect"))
}

async fn drive_loader_until_loaded(state: Arc<LoadedState>, timeout: Duration) {
    let start = std::time::Instant::now();
    while !state.is_loaded() {
        if start.elapsed() > timeout {
            panic!("loader did not converge within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn write_load_round_trip_via_real_broker() {
    // Use whatever testcontainers helper the rebalancer's other
    // integration tests already use (e.g. crabka-broker testcontainer).
    // Replace this stub with the real bootstrap address resolver.
    let bootstrap = "127.0.0.1:9092";
    let client = connect(bootstrap).await;
    let admin = AdminClient::new(client.clone());

    let topic = format!("__test_state_topic_{}", uuid::Uuid::new_v4());
    topic_admin::ensure_topic(&admin, &topic, 1).await.expect("create topic");

    // Round 1: write a record, then start the loader, expect to see it.
    let state = LoadedState::new();
    let st = StateTopic::new(client.clone(), topic.clone(), state.clone());
    let f = InFlightFile::new("p-1".into(), Phase::Wait, 1_111, 50_000_000);
    st.write(&f).await.expect("write");

    let shutdown = CancellationToken::new();
    let loader = StateTopicLoader {
        client: client.clone(),
        topic: topic.clone(),
        state: state.clone(),
        shutdown: shutdown.clone(),
    };
    let handle = tokio::spawn(loader.run());

    drive_loader_until_loaded(state.clone(), Duration::from_secs(10)).await;
    let loaded = state.current().expect("non-tombstone");
    assert_eq!(loaded.proposal_id, "p-1");
    assert_eq!(loaded.phase, Phase::Wait);
    shutdown.cancel();
    handle.await.unwrap();

    // Round 2: tombstone, restart loader, expect None.
    let state2 = LoadedState::new();
    let st2 = StateTopic::new(client.clone(), topic.clone(), state2.clone());
    st2.delete().await.expect("delete");

    let shutdown2 = CancellationToken::new();
    let loader2 = StateTopicLoader {
        client: client.clone(),
        topic: topic.clone(),
        state: state2.clone(),
        shutdown: shutdown2.clone(),
    };
    let handle2 = tokio::spawn(loader2.run());

    drive_loader_until_loaded(state2.clone(), Duration::from_secs(10)).await;
    assert!(state2.current().is_none(), "tombstone should clear state");
    shutdown2.cancel();
    handle2.await.unwrap();
}
```

If the rebalancer's existing test infrastructure already has a testcontainers helper (search `crates/rebalancer/tests/` for `broker_image` / `start_broker`), reuse it instead of the hand-rolled `connect()` above. The plan keeps it stub-shaped because Docker is not running locally and the implementer should adapt to whatever's there.

### Step 5: Build + run lib tests

```bash
cargo build -p crabka-rebalancer --bins --tests 2>&1 | tail -3
cargo test -p crabka-rebalancer --lib 2>&1 | tail -10
```

Expected: clean build; lib tests pass. The integration test is `#[ignore]` so `cargo test` doesn't run it; it'll execute under `cargo test -- --include-ignored` in CI.

### Step 6: Clippy + fmt

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

### Step 7: Commit

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    add crates/rebalancer/src/bin/rebalancer.rs crates/rebalancer/src/api/ \
        crates/rebalancer/tests/state_topic.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
    commit -m "rebalancer(bin): ensure state topic + spawn loader + gate /readyz and /execute on is_loaded"
```

---

## Execution batches (for parallel subagent dispatch)

All 6 tasks touch `crates/rebalancer/src/state_topic/` or `crates/rebalancer/src/executor/`; they have linear dependencies (Task N+1 builds on Task N). Sequential:

- Batch A: Task 1 (skeleton + error + serde-format)
- Batch B: Task 2 (topic_admin)
- Batch C: Task 3 (producer)
- Batch D: Task 4 (loader)
- Batch E: Task 5 (StateTopic handle + executor swap + trait + tests)
- Batch F: Task 6 (binary wiring + API gating + integration test)

---

## Final verification

- [ ] **Step 1: Full workspace build + tests**

```bash
cargo build --workspace 2>&1 | tail -3
cargo test --workspace --lib 2>&1 | grep -E "test result|FAILED" | tail -25
```

Expected: clean build; no test regressions; the new state-topic + executor tests pass.

- [ ] **Step 2: Workspace clippy + fmt**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo fmt --check 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 3: Open PR**

```bash
git push -u origin rebalancer-43i
gh pr create --title "Slice 43i: rebalancer state migration to internal Crabka topic" --body "$(cat <<'EOF'
## Summary

Replaces `{data_dir}/in_flight.json` with a compacted internal topic `__crabka_rebalancer_state`. State survives pod restarts; this unblocks the next slice (43j, multi-replica HA via `Lease`).

- New `crates/rebalancer/src/state_topic/` module: error, serde-format, topic-create helper, single-key produce helper, background loader, and the `StateTopic` handle exposed via a `StateBackend` trait.
- Executor's `InFlightFile::write/load/delete` call sites swap to `StateTopic::write/loaded/delete`.
- `/readyz` returns 503 until the state topic is fully loaded (Kafka coordinator-load semantics).
- `POST /api/v1/proposals/{id}/execute` returns 503 with a `loading` status while the topic is still loading.
- New CLI flags: `--state-topic-name`, `--state-topic-replication`, `--state-load-timeout-secs`.

Spec: `docs/superpowers/specs/2026-05-27-crabka-rebalancer-43i-design.md`
Plan: `docs/superpowers/plans/2026-05-27-crabka-rebalancer-43i.md`

## Test plan

- [x] `cargo build --workspace`
- [x] `cargo test --workspace --lib` (unit tests for state_topic + executor)
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo fmt --check`
- [ ] CI integration test (`tests/state_topic.rs`, `#[ignore]`-gated, runs against a testcontainers broker)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed.
