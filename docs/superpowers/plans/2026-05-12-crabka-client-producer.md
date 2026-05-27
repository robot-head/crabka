# `crabka-client-producer` (slice 6) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a full-idempotent Rust Kafka producer + the broker-side support that backs it. A JVM `kafka-console-consumer --partition 0 --from-beginning` reads records produced by the Rust client. The retrofit task converts slice 2's `ClientBuilder` and slice 5's `ConsumerBuilder` to `bon`-generated builders alongside the producer's new one.

**Architecture:** A `ProducerIdManager` + per-(topic, partition) `ProducerState` inside `crabka-broker` runs the standard dedup / out-of-order / epoch-fence checks before every Produce append. A new `crabka-client-producer` crate exposes a `bon`-built `Producer` whose `send()` returns a oneshot-backed future; a single background sender task drains per-partition accumulators, compresses + frames `ProduceRequest`s through `crabka-client-core`, and resolves the futures from the response.

**Tech Stack:** Rust 1.95.0 edition 2024; `tokio` (sync, time, macros, rt-multi-thread); `bon = "3"` for builders; `crabka-protocol`, `crabka-log`, `crabka-broker`, `crabka-client-core`, `crabka-client-consumer`, `crabka-compression` (all shipped); `bytes`, `dashmap`, `tracing`, `thiserror`.

**Reference spec:** [`docs/superpowers/specs/2026-05-12-crabka-client-producer-design.md`](../specs/2026-05-12-crabka-client-producer-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/client-producer-plan` (this file). Implementation runs on `feature/client-producer` branched off `main` once this plan's PR merges.

---

## File structure

```
Cargo.toml                                              # MODIFIED — add bon = "3" to [workspace.dependencies]

crates/broker/                                          # additions to slice-4/5 crate
└── src/
    ├── codes.rs                                        # MODIFIED — add 5 codes
    ├── error.rs                                        # MODIFIED — add ProducerEpochFenced
    ├── producer_id_manager.rs                          # NEW
    ├── producer_state.rs                               # NEW
    ├── broker.rs                                       # MODIFIED — wire ProducerIdManager + ProducerState
    ├── handlers/
    │   ├── mod.rs                                      # MODIFIED — register init_producer_id
    │   ├── api_versions.rs                             # MODIFIED — include InitProducerId
    │   ├── init_producer_id.rs                         # NEW
    │   └── produce.rs                                  # MODIFIED — dedup checks before append
    └── tests/
        ├── unit.rs                                     # MODIFIED — add per-handler tests
        └── jvm_acceptance.rs                           # MODIFIED — add rust_producer_to_console_consumer

crates/client-core/src/
└── client.rs                                           # MODIFIED — Client::builder via #[bon::builder]

crates/client-consumer/src/
└── builder.rs                                          # MODIFIED — Consumer::builder via #[bon::builder]

crates/client-producer/                                  # NEW crate
├── Cargo.toml
└── src/
    ├── lib.rs                                          # public re-exports
    ├── error.rs                                        # ProducerError
    ├── record.rs                                       # ProducerRecord, RecordMetadata, Header
    ├── compression.rs                                  # Compression enum + codec mapping
    ├── partitioner.rs                                  # UniformStickyPartitioner
    ├── accumulator.rs                                  # per-partition InProgressBatch queue
    ├── sender.rs                                       # spawned sender task
    ├── builder.rs                                      # #[bon::builder] on Producer::start
    └── producer.rs                                     # Producer + send/flush/close

crates/client-producer/tests/
├── unit.rs                                             # MockBroker-driven flows + partitioner + accumulator
└── integration.rs                                      # in-process broker + producer + consumer round-trips
```

---

## Phase A — Workspace dep + codes + error + bon retrofits

### Task 1: Add `bon` workspace dep + 5 wire codes + `ProducerEpochFenced`

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/broker/src/codes.rs`
- Modify: `crates/broker/src/error.rs`

- [ ] **Step 1: Add `bon` to workspace deps**

In root `Cargo.toml`, under `[workspace.dependencies]`, append:

```toml
bon = "3"
```

- [ ] **Step 2: Add wire-level codes**

Append to `crates/broker/src/codes.rs`:

```rust
// Phase 6 additions — idempotent-producer codes.
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 47;
pub const INVALID_PRODUCER_EPOCH: i16 = 53;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 67;
```

- [ ] **Step 3: Add `ProducerEpochFenced` variant**

In `crates/broker/src/error.rs`, append to the `BrokerError` enum:

```rust
    #[error("producer epoch fenced: pid={producer_id} got {requested}, current {current}")]
    ProducerEpochFenced {
        producer_id: i64,
        current: i16,
        requested: i16,
    },
```

- [ ] **Step 4: Map the new variant in `from_broker_error`**

In `crates/broker/src/codes.rs`'s `from_broker_error`, add an arm above the catch-all:

```rust
        BrokerError::ProducerEpochFenced { .. } => INVALID_PRODUCER_EPOCH,
```

- [ ] **Step 5: Test + commit**

Add a unit test inside `codes.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn maps_producer_epoch_fenced_to_53() {
        let e = BrokerError::ProducerEpochFenced {
            producer_id: 1000,
            current: 2,
            requested: 1,
        };
        assert_eq!(from_broker_error(&e), INVALID_PRODUCER_EPOCH);
    }
```

```bash
cargo test -p crabka-broker codes
git add Cargo.toml crates/broker/src/codes.rs crates/broker/src/error.rs
git commit -m "feat(broker): idempotent-producer wire codes + ProducerEpochFenced + bon workspace dep"
```

---

### Task 2: Retrofit `Client::builder` to `bon`

**Files:**
- Modify: `crates/client-core/Cargo.toml`
- Modify: `crates/client-core/src/client.rs`

- [ ] **Step 1: Add `bon` to `crates/client-core/Cargo.toml`**

Append under `[dependencies]`:

```toml
bon = { workspace = true }
```

- [ ] **Step 2: Replace `ClientBuilder` with a `bon::builder` constructor**

Replace the existing `pub fn builder(bootstrap: impl Into<String>) -> ClientBuilder { ... }` and `pub struct ClientBuilder { ... }` and `impl ClientBuilder { ... }` with:

```rust
impl Client {
    /// Build a [`Client`] pointed at the given bootstrap address.
    ///
    /// All builder methods map 1:1 to `ConnectionOptions` fields except
    /// `bootstrap`, which becomes a required positional argument.
    #[bon::builder(finish_fn = build)]
    pub async fn start(
        bootstrap: impl Into<String>,
        #[builder(default = "crabka".to_string())] client_id: String,
        #[builder(default = std::time::Duration::from_secs(30))] connect_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_secs(30))] request_timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        let options = ConnectionOptions {
            client_id,
            connect_timeout,
            request_timeout,
        };
        Self::start_with_options(bootstrap.into(), options).await
    }
}
```

The plan retains the existing `start_with_options` private constructor (or extracts it from the previous `ClientBuilder::build()` body) so the new `start` method's body is a single call.

Add `Client::start_with_options(bootstrap, options) -> Result<Self, ClientError>` near the old `ClientBuilder::build` impl — copy its body verbatim, replacing references to `self.bootstrap` / `self.options` with the `bootstrap` / `options` parameters.

- [ ] **Step 3: Update the only external builder caller pattern**

Existing tests and other crates use `Client::builder(bootstrap).client_id(...).build().await`. With `bon`'s `finish_fn = build`, the call site becomes:

```rust
Client::builder().bootstrap(addr).client_id(...).build().await?
```

Note the move from positional `builder(bootstrap)` to setter-based `.bootstrap(...)`. Update every call site:

```bash
grep -rn "Client::builder(" crates/ | grep -v generated
```

Replace each with the new form. Expected call sites: `crates/broker/tests/support/mod.rs`, `crates/broker/tests/integration.rs`, `crates/client-consumer/src/builder.rs`, `crates/client-consumer/tests/integration.rs`, plus anything in `tests/unit.rs`. The change is mechanical.

- [ ] **Step 4: Build + test + commit**

```bash
cargo build --workspace
cargo test -p crabka-client-core
cargo test -p crabka-broker
cargo test -p crabka-client-consumer
git add Cargo.toml crates/client-core/Cargo.toml crates/client-core/src/client.rs crates
git commit -m "refactor(client-core): retrofit ClientBuilder to bon"
```

Expected: all existing tests still pass.

---

### Task 3: Retrofit `Consumer::builder` to `bon`

**Files:**
- Modify: `crates/client-consumer/Cargo.toml`
- Modify: `crates/client-consumer/src/builder.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: Add `bon` to consumer crate's manifest**

```toml
bon = { workspace = true }
```

- [ ] **Step 2: Replace `ConsumerBuilder` struct with a `bon::builder` constructor on `Consumer`**

Move the existing `ConsumerBuilder::build()` body into a new `Consumer::start` async impl method annotated with `#[bon::builder]`. Setter-style mapping:

```rust
impl Consumer {
    #[bon::builder(finish_fn = build)]
    pub async fn start(
        bootstrap: impl Into<String>,
        #[builder(default = "crabka-consumer".to_string())] client_id: String,
        group_id: String,
        #[builder(default = std::time::Duration::from_secs(45))] session_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_secs(60))] rebalance_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_secs(3))] heartbeat_interval: std::time::Duration,
        #[builder(into)] subscribe: Vec<String>,
        #[builder(default = AutoOffsetReset::Latest)] auto_offset_reset: AutoOffsetReset,
    ) -> Result<Self, ConsumerError> {
        // body: paste old ConsumerBuilder::build() here, replacing self.* with the parameter names
    }
}
```

- [ ] **Step 3: Drop `pub use builder::{AutoOffsetReset, ConsumerBuilder}` and re-export only `AutoOffsetReset`**

In `crates/client-consumer/src/lib.rs`:

```rust
pub use builder::AutoOffsetReset;
pub use consumer::{Consumer, ConsumerRecord};
pub use error::ConsumerError;
```

- [ ] **Step 4: Update call sites**

The new call site shape:

```rust
Consumer::builder()
    .bootstrap(&addr)
    .group_id("my-group")
    .subscribe(["my-topic"])           // bon's `into` handles &str→String conversion
    .build()
    .await?
```

Update the integration tests in `crates/client-consumer/tests/integration.rs` accordingly. The `.subscribe(&["x"])` slice-style is no longer needed — `bon`'s `into` accepts `Vec<String>` or `Vec<&str>` directly.

- [ ] **Step 5: Build + test + commit**

```bash
cargo test -p crabka-client-consumer
cargo test -p crabka-client-consumer --test integration
git add crates/client-consumer
git commit -m "refactor(consumer): retrofit ConsumerBuilder to bon"
```

---

## Phase B — Broker: ProducerIdManager + ProducerState + handlers

### Task 4: `ProducerIdManager`

**Files:**
- Create: `crates/broker/src/producer_id_manager.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/broker/src/producer_id_manager.rs`:

```rust
//! Allocates `(producer_id, producer_epoch)` pairs. Single-broker MVP:
//! the id space is a single monotonic counter. Slice 9 (transactions)
//! will revisit when transactional ids enter the picture.

use std::sync::atomic::{AtomicI16, AtomicI64, Ordering};

use dashmap::DashMap;

/// Lowest pid handed out. Mirrors Apache Kafka's `0` initial range
/// (we start above the legacy non-idempotent sentinel of `-1`).
const PID_BASE: i64 = 1000;

#[derive(Debug)]
pub struct ProducerIdManager {
    next_pid: AtomicI64,
    epochs: DashMap<i64, AtomicI16>,
}

impl Default for ProducerIdManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerIdManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_pid: AtomicI64::new(PID_BASE),
            epochs: DashMap::new(),
        }
    }

    /// Allocate a fresh `(producer_id, producer_epoch=0)`.
    pub fn allocate(&self) -> (i64, i16) {
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        self.epochs.insert(pid, AtomicI16::new(0));
        (pid, 0)
    }

    /// Bump the epoch for an existing pid. Used by transactional producers
    /// re-initialising under the same `transactional_id`. Returns the new
    /// epoch.
    pub fn bump_epoch(&self, pid: i64) -> Option<i16> {
        self.epochs
            .get(&pid)
            .map(|e| e.value().fetch_add(1, Ordering::Relaxed) + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_returns_monotonic_pids_starting_at_base() {
        let m = ProducerIdManager::new();
        assert_eq!(m.allocate(), (PID_BASE, 0));
        assert_eq!(m.allocate(), (PID_BASE + 1, 0));
        assert_eq!(m.allocate(), (PID_BASE + 2, 0));
    }

    #[test]
    fn bump_epoch_increments() {
        let m = ProducerIdManager::new();
        let (pid, _) = m.allocate();
        assert_eq!(m.bump_epoch(pid), Some(1));
        assert_eq!(m.bump_epoch(pid), Some(2));
        assert_eq!(m.bump_epoch(9999), None);
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod producer_id_manager;` (internal).

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker producer_id_manager
git add crates/broker/src/producer_id_manager.rs crates/broker/src/lib.rs
git commit -m "feat(broker): ProducerIdManager (monotonic pid + epoch tracking)"
```

---

### Task 5: `ProducerState` (per-partition dedup tracker)

**Files:**
- Create: `crates/broker/src/producer_state.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/broker/src/producer_state.rs`:

```rust
//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    pub last_offset: i64,
    pub last_timestamp: i64,
}

#[derive(Debug, Default)]
pub struct PartitionProducerState {
    pub entries: HashMap<i64, ProducerEntry>,
}

/// Outcome of a dedup check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Producer is fresh or the sequence is one past the last commit. Caller
    /// should append, then call `commit` with the assigned base offset.
    Append,
    /// Previously-committed sequence range. Caller should respond with
    /// `error_code = NONE` and `base_offset = last_offset`.
    Duplicate { last_offset: i64 },
    /// `base_sequence != last_sequence + 1`. Caller responds with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER (45)`.
    OutOfOrder,
    /// `epoch < entry.epoch`. Caller responds with
    /// `INVALID_PRODUCER_EPOCH (53)`.
    Fenced,
}

#[derive(Debug, Default)]
pub struct ProducerState {
    by_partition: Arc<DashMap<(String, i32), Arc<Mutex<PartitionProducerState>>>>,
}

impl ProducerState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_partition: Arc::new(DashMap::new()),
        }
    }

    /// Decide whether to append the incoming batch.
    ///
    /// `base_sequence` is the wire `base_sequence`; `last_offset_delta` is
    /// the batch's `last_offset_delta` field. Together they imply the
    /// batch's `last_sequence = base_sequence + last_offset_delta`.
    pub async fn check(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Decision {
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        match s.entries.get(&producer_id) {
            None => Decision::Append,
            Some(entry) => {
                if producer_epoch < entry.epoch {
                    return Decision::Fenced;
                }
                let batch_last_seq = base_sequence + last_offset_delta;
                if base_sequence <= entry.last_sequence {
                    // Anywhere within (or before) the committed range counts
                    // as duplicate. We echo the previously-committed offset.
                    return Decision::Duplicate { last_offset: entry.last_offset };
                }
                if base_sequence == entry.last_sequence + 1 {
                    Decision::Append
                } else {
                    Decision::OutOfOrder
                }
            }
        }
    }

    /// Commit a successful append into the tracker.
    pub async fn commit(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: i64,
        last_timestamp: i64,
    ) {
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let last_sequence = base_sequence + last_offset_delta;
        let last_offset = base_offset + i64::from(last_offset_delta);
        s.entries.insert(
            producer_id,
            ProducerEntry {
                epoch: producer_epoch,
                last_sequence,
                last_offset,
                last_timestamp,
            },
        );
    }

    fn handle(&self, topic: &str, partition: i32) -> Arc<Mutex<PartitionProducerState>> {
        self.by_partition
            .entry((topic.to_string(), partition))
            .or_insert_with(|| Arc::new(Mutex::new(PartitionProducerState::default())))
            .value()
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_batch_appends() {
        let s = ProducerState::new();
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert_eq!(d, Decision::Append);
    }

    #[tokio::test]
    async fn next_sequence_appends() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, /* base_offset */ 0, /* ts */ 1).await;
        let d = s.check("t", 0, 1000, 0, 5, 2).await;
        assert_eq!(d, Decision::Append);
    }

    #[tokio::test]
    async fn duplicate_returns_cached_offset() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert_eq!(d, Decision::Duplicate { last_offset: 4 });
    }

    #[tokio::test]
    async fn out_of_order_when_gap() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
        let d = s.check("t", 0, 1000, 0, 10, 2).await;
        assert_eq!(d, Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn lower_epoch_is_fenced() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 5, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 4, 5, 2).await;
        assert_eq!(d, Decision::Fenced);
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod producer_state;` (internal).

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker producer_state
git add crates/broker/src/producer_state.rs crates/broker/src/lib.rs
git commit -m "feat(broker): ProducerState dedup tracker + Decision enum"
```

---

### Task 6: Wire `ProducerIdManager` + `ProducerState` into `Broker`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Add the two fields**

In `crates/broker/src/broker.rs`'s `Broker` struct, add fields between `group_manager` and `handlers`:

```rust
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    pub(crate) producer_state: Arc<crate::producer_state::ProducerState>,
```

- [ ] **Step 2: Construct them in `Broker::start`**

In `Broker::start`, near where `group_manager` is constructed:

```rust
        let producer_ids = Arc::new(crate::producer_id_manager::ProducerIdManager::new());
        let producer_state = Arc::new(crate::producer_state::ProducerState::new());
```

Pass them into the `Arc::new(Self { … })` construction with `producer_ids: producer_ids.clone()` and `producer_state: producer_state.clone()`.

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker
git add crates/broker/src/broker.rs
git commit -m "feat(broker): wire ProducerIdManager + ProducerState into Broker"
```

Expected: builds clean; existing tests pass (we haven't used the new fields yet).

---

### Task 7: Real `InitProducerId` handler

**Files:**
- Create: `crates/broker/src/handlers/init_producer_id.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/init_producer_id.rs`:

```rust
//! `InitProducerId` (api_key=22). Hands out `(producer_id, producer_epoch)`
//! to a producer. Transactional ids are rejected — slice 9 will add
//! transaction-id-to-pid binding.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;
use crabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let producer_ids = broker.producer_ids.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = InitProducerIdRequest::decode(&mut cur, version)?;

        // Reject transactional ids — slice 9 will add the txn id manager.
        let is_transactional = req
            .transactional_id
            .as_ref()
            .is_some_and(|t| !t.is_empty());
        let resp = if is_transactional {
            InitProducerIdResponse {
                error_code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                throttle_time_ms: 0,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            }
        } else {
            let (pid, epoch) = producer_ids.allocate();
            InitProducerIdResponse {
                error_code: codes::NONE,
                throttle_time_ms: 0,
                producer_id: pid,
                producer_epoch: epoch,
                ..Default::default()
            }
        };

        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 2: Register the handler**

In `crates/broker/src/handlers/mod.rs`, add `pub(crate) mod init_producer_id;` and register in `build_table()`:

```rust
    t.register(22, init_producer_id::handle);
```

- [ ] **Step 3: Add to ApiVersions advertisement + dispatch flexibility table**

In `crates/broker/src/handlers/api_versions.rs::supported_apis()`, add an entry:

```rust
        v!(init_producer_id_request),
```

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`, add an arm:

```rust
        22 => version >= owned::init_producer_id_request::FLEXIBLE_MIN,
```

- [ ] **Step 4: Unit test**

Append to `crates/broker/tests/unit.rs`:

```rust
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;

#[tokio::test]
async fn init_producer_id_returns_fresh_pid() {
    let p = support::start().await;
    let r = p.client.send(InitProducerIdRequest::default()).await.expect("InitProducerId");
    assert_eq!(r.error_code, 0);
    assert!(r.producer_id >= 1000);
    assert_eq!(r.producer_epoch, 0);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_rejects_transactional() {
    let p = support::start().await;
    let r = p.client.send(InitProducerIdRequest {
        transactional_id: Some("tx-1".into()),
        ..Default::default()
    }).await.expect("InitProducerId");
    assert_eq!(r.error_code, 67); // TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    p.broker.shutdown().await;
}
```

- [ ] **Step 5: Test + commit**

```bash
cargo test -p crabka-broker --test unit init_producer_id
git add crates/broker
git commit -m "feat(broker): real InitProducerId handler (replaces slice-4 stub)"
```

---

### Task 8: Wire idempotent dedup into the Produce handler

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Extend the handler with the dedup branch**

The slice-4 `produce::handle` accepts a `RecordBatch` per (topic, partition) and forwards it to the partition writer. Extend it: before the existing `writer_tx.send(ProduceJob { ... })`, check the producer-id metadata on the batch and consult `ProducerState`.

Replace the inner per-partition body inside `produce::handle` with (the surrounding loop / response building stays the same):

```rust
                let Some(part_handle) = partitions
                    .get(&(topic_name.clone(), partition_index))
                    .map(|e| e.value().clone())
                else {
                    presult.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    part_results.push(presult);
                    continue;
                };

                // ── idempotent-producer dedup gate ───────────────────────
                let pid = batch.producer_id;
                let epoch = batch.producer_epoch;
                let base_seq = batch.base_sequence;
                let last_offset_delta = batch.last_offset_delta;

                let dedup_outcome = if pid >= 0 {
                    Some(
                        producer_state
                            .check(&topic_name, partition_index, pid, epoch, base_seq, last_offset_delta)
                            .await,
                    )
                } else {
                    None
                };

                match dedup_outcome {
                    Some(crate::producer_state::Decision::Duplicate { last_offset }) => {
                        presult.error_code = codes::NONE;
                        presult.base_offset = last_offset - i64::from(last_offset_delta);
                        part_results.push(presult);
                        continue;
                    }
                    Some(crate::producer_state::Decision::OutOfOrder) => {
                        presult.error_code = codes::OUT_OF_ORDER_SEQUENCE_NUMBER;
                        part_results.push(presult);
                        continue;
                    }
                    Some(crate::producer_state::Decision::Fenced) => {
                        presult.error_code = codes::INVALID_PRODUCER_EPOCH;
                        part_results.push(presult);
                        continue;
                    }
                    Some(crate::producer_state::Decision::Append) | None => {
                        // fall through to writer dispatch
                    }
                }

                // ── existing slice-4 writer dispatch ─────────────────────
                let (ack_tx, ack_rx) = oneshot::channel();
                if part_handle
                    .writer_tx
                    .send(ProduceJob { batch: batch.clone(), ack: ack_tx })
                    .await
                    .is_err()
                {
                    presult.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    part_results.push(presult);
                    continue;
                }

                match tokio::time::timeout(timeout, ack_rx).await {
                    Ok(Ok(Ok(base))) => {
                        presult.error_code = codes::NONE;
                        presult.base_offset = base;

                        if pid >= 0 {
                            producer_state
                                .commit(
                                    &topic_name,
                                    partition_index,
                                    pid,
                                    epoch,
                                    base_seq,
                                    last_offset_delta,
                                    base,
                                    batch.max_timestamp,
                                )
                                .await;
                        }
                    }
                    Ok(Ok(Err(e))) => {
                        tracing::error!(topic = %topic_name, partition = partition_index, error = %e, "writer ack error");
                        presult.error_code = crate::codes::from_broker_error(&e);
                    }
                    Ok(Err(_recv_dropped)) => {
                        presult.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    }
                    Err(_elapsed) => {
                        presult.error_code = codes::REQUEST_TIMED_OUT;
                    }
                }
                part_results.push(presult);
```

Capture `producer_state` at the top of the boxed future (same pattern as `partitions`):

```rust
        let partitions = broker.partitions.clone();
        let producer_state = broker.producer_state.clone();
```

- [ ] **Step 2: Test idempotent path**

Append to `crates/broker/tests/unit.rs`:

```rust
use crabka_protocol::records::{Record, RecordBatch};

fn one_batch_with_producer(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.producer_id = pid;
    b.producer_epoch = epoch;
    b.base_sequence = base_seq;
    b.last_offset_delta = (values.len() as i32) - 1;
    b.max_timestamp = values.len() as i64;
    for (i, v) in values.iter().enumerate() {
        b.records.push(Record {
            offset_delta: i as i32,
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    b
}

#[tokio::test]
async fn idempotent_produce_dedups_duplicate_batch() {
    let p = support::start().await;

    // Create topic.
    let _ = p.client.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "idem".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();

    // Allocate a pid.
    let init = p.client.send(InitProducerIdRequest::default()).await.unwrap();
    let pid = init.producer_id;

    // First send: succeed at offset 0.
    let req = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "idem".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, 0, &["a", "b", "c"])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let r1 = p.client.send(req.clone()).await.unwrap();
    assert_eq!(r1.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(r1.responses[0].partition_responses[0].base_offset, 0);

    // Same request again: duplicate → success with the original offset.
    let r2 = p.client.send(req).await.unwrap();
    assert_eq!(r2.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(r2.responses[0].partition_responses[0].base_offset, 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_order_returns_45() {
    let p = support::start().await;
    let _ = p.client.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "ooo".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    let init = p.client.send(InitProducerIdRequest::default()).await.unwrap();
    let pid = init.producer_id;

    let mk = |base_seq: i32| ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "ooo".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, base_seq, &["x", "y"])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let r1 = p.client.send(mk(0)).await.unwrap();
    assert_eq!(r1.responses[0].partition_responses[0].error_code, 0);
    // Skip ahead — next valid base_seq is 2; sending 10 is out-of-order.
    let r2 = p.client.send(mk(10)).await.unwrap();
    assert_eq!(r2.responses[0].partition_responses[0].error_code, 45);

    p.broker.shutdown().await;
}
```

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker --test unit idempotent
cargo test -p crabka-broker --test unit out_of_order
git add crates/broker
git commit -m "feat(broker): idempotent-producer dedup + fence checks in Produce handler"
```

---

## Phase C — Producer client: scaffolding

### Task 9: `crabka-client-producer` crate skeleton + `ProducerError`

**Files:**
- Create: `crates/client-producer/Cargo.toml`
- Create: `crates/client-producer/src/lib.rs`
- Create: `crates/client-producer/src/error.rs`

- [ ] **Step 1: Manifest**

`crates/client-producer/Cargo.toml`:

```toml
[package]
name = "crabka-client-producer"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Idempotent producer client for Apache Kafka in Rust"

[lints]
workspace = true

[features]
default = []

[dependencies]
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
crabka-client-core = { version = "0.1", path = "../client-core" }
crabka-compression = { version = "0.1", path = "../compression" }
bon = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "sync", "time", "macros"] }
tokio-util = { workspace = true, features = ["rt"] }
tracing = { workspace = true }
dashmap = { workspace = true }
futures-util = { workspace = true }

[dev-dependencies]
crabka-broker = { version = "0.1", path = "../broker" }
crabka-log = { version = "0.1", path = "../log" }
crabka-client-consumer = { version = "0.1", path = "../client-consumer" }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
proptest = { workspace = true }
```

- [ ] **Step 2: Stub `lib.rs`**

`crates/client-producer/src/lib.rs`:

```rust
//! Idempotent producer client for Apache Kafka in Rust.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-12-crabka-client-producer-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod error;

pub use error::ProducerError;
```

- [ ] **Step 3: Write `ProducerError`**

`crates/client-producer/src/error.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProducerError {
    #[error("client: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("broker error_code {0}")]
    Server(i16),

    #[error("fenced by newer producer instance")]
    FencedProducer,

    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),

    #[error("batch too large: {batch_size} > max")]
    BatchTooLarge { batch_size: usize },

    #[error("record too large: {record_size} > max_request_size")]
    RecordTooLarge { record_size: usize },

    #[error("send buffer full (max_block exceeded)")]
    BufferFull,

    #[error("producer closed")]
    Closed,

    #[error("compression: {0}")]
    Compression(#[from] crabka_compression::CompressionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_fenced_producer() {
        assert!(ProducerError::FencedProducer.to_string().contains("fenced"));
    }

    #[test]
    fn display_invalid_config() {
        let e = ProducerError::InvalidConfig("idempotence requires acks=all");
        assert!(e.to_string().contains("idempotence"));
    }
}
```

- [ ] **Step 4: Build + test + commit**

```bash
cargo build -p crabka-client-producer
cargo test -p crabka-client-producer
git add crates/client-producer
git commit -m "feat(producer): crate skeleton + ProducerError"
```

---

### Task 10: `ProducerRecord`, `RecordMetadata`, `Header`

**Files:**
- Create: `crates/client-producer/src/record.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-producer/src/record.rs`:

```rust
//! Public record types: `ProducerRecord` (what you send), `RecordMetadata`
//! (what you get back), and `Header` (per-record key/value pairs).

use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ProducerRecord {
    pub topic: String,
    /// If `Some(p)`, the partitioner is bypassed and partition `p` is used.
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    /// If `None`, the producer fills in the current wall-clock time at
    /// accumulator append time.
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordMetadata {
    pub topic_index: usize,    // index into the original topic list — useful for batching callers
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_record_default_is_empty() {
        let r = ProducerRecord::default();
        assert!(r.topic.is_empty());
        assert!(r.key.is_none());
        assert!(r.headers.is_empty());
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Replace `lib.rs` with:

```rust
//! Idempotent producer client for Apache Kafka in Rust.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod error;
mod record;

pub use error::ProducerError;
pub use record::{Header, ProducerRecord, RecordMetadata};
```

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-client-producer record
git add crates/client-producer
git commit -m "feat(producer): ProducerRecord, RecordMetadata, Header"
```

---

### Task 11: `Compression` enum + codec mapping

**Files:**
- Create: `crates/client-producer/src/compression.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-producer/src/compression.rs`:

```rust
//! `Compression` enum + mapping from the producer's choice to a
//! `RecordBatch` v2 `attributes` value + a `crabka-compression::CompressionType`.

use bytes::{Bytes, BytesMut};

use crate::error::ProducerError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl Compression {
    /// The 3-bit `compression_type` field that goes into the `RecordBatch`
    /// v2 `attributes` (bits 0..3).
    #[must_use]
    pub fn attribute_bits(self) -> i16 {
        match self {
            Compression::None => 0,
            Compression::Gzip => 1,
            Compression::Snappy => 2,
            Compression::Lz4 => 3,
            Compression::Zstd => 4,
        }
    }

    /// Compress the encoded record body. Returns the byte payload that
    /// goes into the `RecordBatch.records_body` slot.
    pub fn compress(self, raw: &[u8]) -> Result<Bytes, ProducerError> {
        use crabka_compression::CompressionType;
        let codec = match self {
            Compression::None => {
                return Ok(Bytes::copy_from_slice(raw));
            }
            Compression::Gzip => CompressionType::Gzip,
            Compression::Snappy => CompressionType::Snappy,
            Compression::Lz4 => CompressionType::Lz4,
            Compression::Zstd => CompressionType::Zstd,
        };
        let mut out = BytesMut::new();
        codec.compress(raw, &mut out)?;
        Ok(out.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_round_trip_is_identity() {
        let raw = b"hello producer";
        let out = Compression::None.compress(raw).unwrap();
        assert_eq!(out.as_ref(), raw);
    }

    #[test]
    fn attribute_bits_match_kafka_table() {
        assert_eq!(Compression::None.attribute_bits(), 0);
        assert_eq!(Compression::Gzip.attribute_bits(), 1);
        assert_eq!(Compression::Snappy.attribute_bits(), 2);
        assert_eq!(Compression::Lz4.attribute_bits(), 3);
        assert_eq!(Compression::Zstd.attribute_bits(), 4);
    }

    #[test]
    fn gzip_round_trip_via_decoder() {
        use crabka_compression::CompressionType;
        let raw = b"the quick brown fox";
        let compressed = Compression::Gzip.compress(raw).unwrap();
        let mut decoded = BytesMut::new();
        CompressionType::Gzip.decompress(&compressed, &mut decoded).unwrap();
        assert_eq!(decoded.as_ref(), raw);
    }
}
```

`CompressionType::compress` / `::decompress` may have slightly different signatures depending on slice 1's API — grep `crates/compression/src/lib.rs` and adapt if needed.

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod compression;` + `pub use compression::Compression;`.

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-client-producer compression
git add crates/client-producer
git commit -m "feat(producer): Compression enum + codec mapping (all four codecs)"
```

---

### Task 12: `UniformStickyPartitioner`

**Files:**
- Create: `crates/client-producer/src/partitioner.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-producer/src/partitioner.rs`:

```rust
//! `UniformStickyPartitioner` — Java 3.0+ default. Hash-on-key for keyed
//! records; sticky-per-topic for null-key records, rotating only when the
//! current accumulator drains.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct UniformStickyPartitioner {
    sticky: Mutex<HashMap<String, i32>>,
}

impl UniformStickyPartitioner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pick the partition for a record.
    ///
    /// `num_partitions` must be > 0.
    pub fn pick(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        assert!(num_partitions > 0, "num_partitions must be > 0");
        match key {
            Some(k) => {
                let h = murmur2(k);
                (h.unsigned_abs() % (num_partitions as u32)) as i32
            }
            None => {
                let mut s = self.sticky.lock().expect("sticky mutex poisoned");
                *s.entry(topic.to_string()).or_insert(0) % num_partitions
            }
        }
    }

    /// Rotate the sticky partition for `topic` to a new one (called by the
    /// sender after a batch flushes).
    pub fn rotate(&self, topic: &str, num_partitions: i32) {
        if num_partitions <= 0 {
            return;
        }
        let mut s = self.sticky.lock().expect("sticky mutex poisoned");
        let entry = s.entry(topic.to_string()).or_insert(0);
        *entry = (*entry + 1) % num_partitions;
    }
}

/// MurmurHash2 — Kafka's `DefaultPartitioner` key hash.
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    let mut h: u32 = SEED ^ (length as u32);

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();
    for chunk in chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    match remainder.len() {
        3 => {
            h ^= u32::from(remainder[2]) << 16;
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remainder[1]) << 8;
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remainder[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hash_is_stable_across_calls() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", Some(b"my-key"), 12);
        let b = p.pick("t", Some(b"my-key"), 12);
        assert_eq!(a, b);
        assert!(a >= 0 && a < 12);
    }

    #[test]
    fn null_key_uses_sticky_partition() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", None, 4);
        let b = p.pick("t", None, 4);
        let c = p.pick("t", None, 4);
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn rotate_moves_sticky_to_next() {
        let p = UniformStickyPartitioner::new();
        let a = p.pick("t", None, 4);
        p.rotate("t", 4);
        let b = p.pick("t", None, 4);
        assert_ne!(a, b);
        assert_eq!(b, (a + 1) % 4);
    }

    #[test]
    fn distinct_topics_have_distinct_sticky_state() {
        let p = UniformStickyPartitioner::new();
        let _ = p.pick("a", None, 4);
        p.rotate("a", 4);
        // Topic "b"'s sticky is still 0.
        assert_eq!(p.pick("b", None, 4), 0);
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod partitioner;` (internal — sender uses it; not in public API).

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-client-producer partitioner
git add crates/client-producer
git commit -m "feat(producer): UniformStickyPartitioner + MurmurHash2"
```

---

## Phase D — Producer client: core path

### Task 13: `Accumulator` (per-partition batch queue)

**Files:**
- Create: `crates/client-producer/src/accumulator.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-producer/src/accumulator.rs`:

```rust
//! Per-(topic, partition) accumulator. Each `try_append` enqueues a
//! record + a oneshot tx; the sender drains in-flight batches and
//! resolves the oneshots from the `ProduceResponse`.

use std::collections::VecDeque;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::error::ProducerError;
use crate::record::{Header, RecordMetadata};

/// A record waiting inside an in-progress batch.
#[derive(Debug)]
pub(crate) struct PendingRecord {
    pub offset_delta: i32,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    pub ack: oneshot::Sender<Result<RecordMetadata, ProducerError>>,
}

/// One in-progress `RecordBatch`. The sender wraps this into a Kafka
/// `RecordBatch` at flush time and assigns `base_sequence`.
#[derive(Debug)]
pub(crate) struct InProgressBatch {
    /// Wall-clock time when this batch's first record was appended.
    /// Used by the sender to decide `linger.ms` expiry.
    pub first_append_at: Instant,
    /// Approximate uncompressed body size.
    pub size_bytes: usize,
    pub records: Vec<PendingRecord>,
}

impl InProgressBatch {
    fn new() -> Self {
        Self {
            first_append_at: Instant::now(),
            size_bytes: 0,
            records: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// One accumulator per (topic, partition).
#[derive(Debug)]
pub(crate) struct Accumulator {
    /// `None` until the first append. Sender pops the in-progress batch
    /// into `ready` when it flushes (rotating the partitioner sticky if
    /// applicable).
    pub current: Option<InProgressBatch>,
    /// FIFO of batches the sender hasn't flushed yet.
    pub ready: VecDeque<InProgressBatch>,
    /// `batch.size` cap. If a single record would push us past this, we
    /// seal `current` first and start fresh.
    pub batch_size: usize,
}

/// Result of [`Accumulator::try_append`].
pub(crate) enum AppendResult {
    Appended(oneshot::Receiver<Result<RecordMetadata, ProducerError>>),
    /// The accumulator's `batch.size` is full but a new batch could be
    /// started. The caller (sender wakeup) needs to seal and rotate.
    BatchFull,
}

impl Accumulator {
    pub fn new(batch_size: usize) -> Self {
        Self {
            current: None,
            ready: VecDeque::new(),
            batch_size,
        }
    }

    pub fn try_append(
        &mut self,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<Header>,
        timestamp_ms: i64,
    ) -> AppendResult {
        // Approximate the per-record size: 8 bytes overhead + key + value + headers.
        let record_size = approx_record_size(key.as_deref(), value.as_deref(), &headers);

        let need_new_batch = match &self.current {
            None => true,
            Some(b) => b.size_bytes + record_size > self.batch_size && !b.is_empty(),
        };

        if need_new_batch {
            if let Some(prev) = self.current.take() {
                self.ready.push_back(prev);
            }
            self.current = Some(InProgressBatch::new());
        }

        let batch = self
            .current
            .as_mut()
            .expect("current set above when need_new_batch was true");

        let (tx, rx) = oneshot::channel();
        let offset_delta = batch.records.len() as i32;
        batch.records.push(PendingRecord {
            offset_delta,
            timestamp_ms,
            key,
            value,
            headers,
            ack: tx,
        });
        batch.size_bytes += record_size;
        AppendResult::Appended(rx)
    }

    /// Move the current in-progress batch into `ready`. Called by the
    /// sender at flush time (linger expiry, explicit flush, batch full).
    pub fn seal_current(&mut self) {
        if let Some(b) = self.current.take() {
            if !b.is_empty() {
                self.ready.push_back(b);
            }
        }
    }
}

fn approx_record_size(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[Header]) -> usize {
    let mut n = 8usize; // varint overhead estimate
    n += key.map_or(0, <[u8]>::len) + 4;
    n += value.map_or(0, <[u8]>::len) + 4;
    for h in headers {
        n += h.key.len() + h.value.as_ref().map_or(0, bytes::Bytes::len) + 8;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_append_creates_batch() {
        let mut a = Accumulator::new(1024);
        let _ = a.try_append(None, Some(Bytes::from_static(b"hi")), vec![], 0);
        assert!(a.current.is_some());
        assert_eq!(a.current.as_ref().unwrap().records.len(), 1);
    }

    #[test]
    fn record_past_batch_size_rolls_over() {
        let mut a = Accumulator::new(40);
        // Each record is ~20+ bytes; two records exceed 40.
        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);
        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);
        assert_eq!(a.ready.len(), 1);
        assert!(a.current.is_some());
        assert_eq!(a.current.as_ref().unwrap().records.len(), 1);
    }

    #[test]
    fn seal_moves_current_to_ready() {
        let mut a = Accumulator::new(1024);
        let _ = a.try_append(None, Some(Bytes::from_static(b"x")), vec![], 0);
        a.seal_current();
        assert!(a.current.is_none());
        assert_eq!(a.ready.len(), 1);
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod accumulator;` (internal).

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-client-producer accumulator
git add crates/client-producer
git commit -m "feat(producer): per-partition Accumulator with batch-size rollover"
```

---

### Task 14: `Producer` struct + state machine (skeleton)

**Files:**
- Create: `crates/client-producer/src/producer.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-producer/src/producer.rs`:

```rust
//! `Producer` — public type. Builder lives in `builder.rs`. Sender task
//! lives in `sender.rs`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;

use crate::accumulator::Accumulator;
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    Zero,
    One,
    All,
}

impl Acks {
    #[must_use]
    pub fn wire(self) -> i16 {
        match self {
            Acks::Zero => 0,
            Acks::One => 1,
            Acks::All => -1,
        }
    }
}

/// Tri-state lifecycle.
const STATE_ACTIVE: u8 = 0;
const STATE_FENCED: u8 = 1;
const STATE_CLOSED: u8 = 2;

#[derive(Debug)]
pub(crate) struct TopicMetadata {
    pub num_partitions: i32,
}

pub struct Producer {
    pub(crate) client: Client,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) acks: Acks,
    pub(crate) compression: Compression,
    pub(crate) batch_size: usize,
    pub(crate) linger: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) retries: i32,
    pub(crate) retry_backoff: Duration,
    pub(crate) max_in_flight: usize,
    pub(crate) metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    pub(crate) accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    pub(crate) next_seq: Arc<DashMap<(String, i32), i32>>,
    pub(crate) partitioner: Arc<UniformStickyPartitioner>,
    pub(crate) state: Arc<AtomicU8>,
    pub(crate) wake_tx: tokio::sync::mpsc::Sender<()>,
    pub(crate) flush_notify: Arc<Notify>,
    pub(crate) sender_shutdown: CancellationToken,
    pub(crate) sender_handle: Option<JoinHandle<()>>,
}

impl Producer {
    #[must_use]
    pub fn producer_id(&self) -> i64 {
        self.producer_id
    }

    #[must_use]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }

    pub(crate) fn is_active(&self) -> Result<(), ProducerError> {
        match self.state.load(Ordering::Acquire) {
            STATE_ACTIVE => Ok(()),
            STATE_FENCED => Err(ProducerError::FencedProducer),
            _ => Err(ProducerError::Closed),
        }
    }

    pub(crate) fn fence(&self) {
        self.state
            .compare_exchange(STATE_ACTIVE, STATE_FENCED, Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    pub async fn close(mut self) -> Result<(), ProducerError> {
        self.flush().await?;
        self.state.store(STATE_CLOSED, Ordering::Release);
        self.sender_shutdown.cancel();
        if let Some(h) = self.sender_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), ProducerError> {
        // The flush implementation lands in Task 15 once the sender is wired.
        Ok(())
    }
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

pub(crate) const STATE_ACTIVE_PUB: u8 = STATE_ACTIVE;
pub(crate) const STATE_FENCED_PUB: u8 = STATE_FENCED;
pub(crate) const STATE_CLOSED_PUB: u8 = STATE_CLOSED;
```

(Per-file dead-code warnings for the `STATE_*_PUB` are OK at this stage; downstream tasks reference them.)

- [ ] **Step 2: Hook into `lib.rs`**

```rust
mod accumulator;
mod compression;
mod error;
mod partitioner;
mod producer;
mod record;

pub use compression::Compression;
pub use error::ProducerError;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
```

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-client-producer
git add crates/client-producer
git commit -m "feat(producer): Producer struct + Acks + state machine skeleton"
```

---

### Task 15: `sender::run` (batch → ProduceRequest → resolve oneshots)

**Files:**
- Create: `crates/client-producer/src/sender.rs`
- Modify: `crates/client-producer/src/lib.rs`
- Modify: `crates/client-producer/src/producer.rs` (implement `flush`)

- [ ] **Step 1: Write the sender**

`crates/client-producer/src/sender.rs`:

```rust
//! Background sender task. Drains ready batches from every accumulator
//! and ships them as `ProduceRequest`s through `crabka-client-core`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

use crate::accumulator::{Accumulator, InProgressBatch};
use crate::compression::Compression;
use crate::error::ProducerError;
use crate::producer::{Acks, TopicMetadata};
use crate::record::RecordMetadata;

pub(crate) struct SenderConfig {
    pub client: Client,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub acks: Acks,
    pub compression: Compression,
    pub linger: Duration,
    pub request_timeout: Duration,
    pub retries: i32,
    pub retry_backoff: Duration,
    pub metadata_cache: Arc<Mutex<HashMap<String, TopicMetadata>>>,
    pub accumulators: Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>,
    pub next_seq: Arc<DashMap<(String, i32), i32>>,
    pub state: Arc<AtomicU8>,
    pub wake_rx: tokio::sync::mpsc::Receiver<()>,
    pub flush_notify: Arc<Notify>,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(mut cfg: SenderConfig) {
    let mut ticker = tokio::time::interval(cfg.linger.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cfg.shutdown.cancelled() => break,
            _ = ticker.tick() => {
                drain_once(&mut cfg).await;
            }
            _ = cfg.wake_rx.recv() => {
                drain_once(&mut cfg).await;
            }
        }
    }

    // Drain anything left when we shut down so close() doesn't lose records.
    drain_once(&mut cfg).await;
}

async fn drain_once(cfg: &mut SenderConfig) {
    // Snapshot the partitions we need to look at.
    let keys: Vec<(String, i32)> = cfg.accumulators.iter().map(|e| e.key().clone()).collect();
    let mut any_work = false;
    for key in keys {
        let acc = match cfg.accumulators.get(&key) {
            Some(a) => a.value().clone(),
            None => continue,
        };
        let batch = {
            let mut a = acc.lock().await;
            a.seal_current();
            a.ready.pop_front()
        };
        let Some(batch) = batch else { continue };
        any_work = true;
        send_one(cfg, &key.0, key.1, batch).await;
    }
    if !any_work {
        // No work this round; signal flush waiters in case they're polling.
        cfg.flush_notify.notify_waiters();
    }
}

async fn send_one(cfg: &SenderConfig, topic: &str, partition: i32, batch: InProgressBatch) {
    // 1. Allocate base_sequence.
    let base_sequence = {
        let mut entry = cfg
            .next_seq
            .entry((topic.to_string(), partition))
            .or_insert(0);
        let cur = *entry;
        let count = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
        *entry = cur.wrapping_add(count);
        cur
    };

    // 2. Build the RecordBatch (uncompressed). Records' offset_delta /
    //    timestamp_delta are filled per the accumulator's order.
    let max_ts = batch
        .records
        .iter()
        .map(|r| r.timestamp_ms)
        .max()
        .unwrap_or(0);
    let mut record_batch = RecordBatch::default();
    record_batch.producer_id = cfg.producer_id;
    record_batch.producer_epoch = cfg.producer_epoch;
    record_batch.base_sequence = base_sequence;
    record_batch.last_offset_delta = i32::try_from(batch.records.len()).unwrap_or(i32::MAX) - 1;
    record_batch.max_timestamp = max_ts;
    record_batch.base_timestamp = batch.records.first().map_or(0, |r| r.timestamp_ms);
    record_batch.attributes = cfg.compression.attribute_bits();
    // Record body: each record has offset_delta, timestamp_delta, key, value, headers.
    for r in &batch.records {
        record_batch.records.push(Record {
            offset_delta: r.offset_delta,
            timestamp_delta: r.timestamp_ms - record_batch.base_timestamp,
            key: r.key.clone(),
            value: r.value.clone(),
            ..Default::default()
        });
    }

    // 3. Frame ProduceRequest.
    let req = ProduceRequest {
        acks: cfg.acks.wire(),
        timeout_ms: i32::try_from(cfg.request_timeout.as_millis()).unwrap_or(i32::MAX),
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(record_batch.clone()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // 4. Send.
    let mut attempts: i32 = 0;
    let final_resp = loop {
        attempts += 1;
        let send_result = cfg.client.send(req.clone()).await;
        match send_result {
            Ok(r) => break Some(r),
            Err(e) => {
                if attempts > cfg.retries {
                    tracing::error!(topic, partition, error = %e, "producer giving up after {attempts} attempts");
                    fail_batch(batch.records, ProducerError::Client(e));
                    return;
                }
                tokio::time::sleep(cfg.retry_backoff).await;
            }
        }
    };

    let Some(resp) = final_resp else { return };

    // 5. Resolve oneshots from the per-(topic, partition) entry.
    let part_resp = resp
        .responses
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partition_responses.iter().find(|p| p.index == partition));
    let Some(part_resp) = part_resp else {
        fail_batch(batch.records, ProducerError::Closed);
        return;
    };

    match part_resp.error_code {
        0 => {
            // Resolve every record as success.
            for r in batch.records {
                let _ = r.ack.send(Ok(RecordMetadata {
                    topic_index: 0,
                    partition,
                    offset: part_resp.base_offset + i64::from(r.offset_delta),
                    timestamp_ms: r.timestamp_ms,
                }));
            }
        }
        46 /* DUPLICATE_SEQUENCE_NUMBER */ => {
            for r in batch.records {
                let _ = r.ack.send(Ok(RecordMetadata {
                    topic_index: 0,
                    partition,
                    offset: part_resp.base_offset + i64::from(r.offset_delta),
                    timestamp_ms: r.timestamp_ms,
                }));
            }
        }
        45 /* OUT_OF_ORDER_SEQUENCE_NUMBER */ | 53 /* INVALID_PRODUCER_EPOCH */ => {
            cfg.state.compare_exchange(
                crate::producer::STATE_ACTIVE_PUB,
                crate::producer::STATE_FENCED_PUB,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).ok();
            fail_batch(batch.records, ProducerError::FencedProducer);
        }
        code => {
            fail_batch(batch.records, ProducerError::Server(code));
        }
    }
}

fn fail_batch(records: Vec<crate::accumulator::PendingRecord>, err: ProducerError) {
    for r in records {
        let _ = r.ack.send(Err(clone_err(&err)));
    }
}

// Helper: ProducerError doesn't derive Clone (Box<dyn Error>); manually clone
// the variants we use here.
fn clone_err(e: &ProducerError) -> ProducerError {
    match e {
        ProducerError::FencedProducer => ProducerError::FencedProducer,
        ProducerError::Closed => ProducerError::Closed,
        ProducerError::Server(c) => ProducerError::Server(*c),
        ProducerError::Client(_) => ProducerError::Closed, // can't clone underlying ClientError
        _ => ProducerError::Closed,
    }
}
```

Note: `clone_err` is a workaround for `ClientError` not being `Clone`. If you'd rather, change the signature of `fail_batch` to consume `err: ProducerError` and resolve only the FIRST record's oneshot with the real error and the rest with `ProducerError::Closed`. Either is fine for MVP.

- [ ] **Step 2: Implement `Producer::flush` properly**

In `crates/client-producer/src/producer.rs`, replace the stub `flush`:

```rust
    pub async fn flush(&self) -> Result<(), ProducerError> {
        self.is_active()?;
        // Wake the sender to drain everything.
        let _ = self.wake_tx.send(()).await;
        // Wait until at least one drain pass found nothing — i.e. all
        // accumulators are empty.
        for _ in 0..1000 {
            if self.all_empty().await {
                return Ok(());
            }
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                self.flush_notify.notified(),
            )
            .await;
        }
        Err(ProducerError::Closed)
    }

    async fn all_empty(&self) -> bool {
        for entry in self.accumulators.iter() {
            let a = entry.value().lock().await;
            if a.current.as_ref().is_some_and(|b| !b.is_empty()) {
                return false;
            }
            if !a.ready.is_empty() {
                return false;
            }
        }
        true
    }
```

- [ ] **Step 3: Hook into `lib.rs`**

Add `mod sender;` (internal).

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-client-producer
git add crates/client-producer
git commit -m "feat(producer): sender task + Producer::flush"
```

---

### Task 16: `Producer::builder` (bon) + `send`

**Files:**
- Create: `crates/client-producer/src/builder.rs`
- Modify: `crates/client-producer/src/producer.rs`
- Modify: `crates/client-producer/src/lib.rs`

- [ ] **Step 1: `Producer::send` and the partitioner glue**

In `crates/client-producer/src/producer.rs`, add:

```rust
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::accumulator::{Accumulator, AppendResult};
use crate::record::ProducerRecord;

impl Producer {
    /// Enqueue a record and return a future that resolves when the broker
    /// acks (or the producer fences / closes).
    pub async fn send(
        &self,
        record: ProducerRecord,
    ) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>> {
        if let Err(e) = self.is_active() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(e));
            return rx;
        }

        // 1. Resolve partition.
        let partition = match record.partition {
            Some(p) => p,
            None => self.partition_for(&record.topic, record.key.as_deref()).await,
        };

        // 2. Find/create accumulator.
        let key = (record.topic.clone(), partition);
        let acc = self
            .accumulators
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Accumulator::new(self.batch_size))))
            .value()
            .clone();

        // 3. Append.
        let timestamp = record
            .timestamp_ms
            .unwrap_or_else(|| current_millis());
        let mut a = acc.lock().await;
        let AppendResult::Appended(rx) = a.try_append(record.key, record.value, record.headers, timestamp);
        let _ = self.wake_tx.try_send(());
        rx
    }

    async fn partition_for(&self, topic: &str, key: Option<&[u8]>) -> i32 {
        let num_partitions = self.partitions_for(topic).await;
        self.partitioner.pick(topic, key, num_partitions)
    }

    async fn partitions_for(&self, topic: &str) -> i32 {
        // Cached metadata first.
        {
            let m = self.metadata_cache.lock().await;
            if let Some(meta) = m.get(topic) {
                return meta.num_partitions;
            }
        }
        // Cache miss: fetch metadata.
        let req = crabka_protocol::owned::metadata_request::MetadataRequest {
            topics: Some(vec![
                crabka_protocol::owned::metadata_request::MetadataRequestTopic {
                    name: Some(topic.to_string()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        match self.client.send(req).await {
            Ok(resp) => {
                let count = resp
                    .topics
                    .iter()
                    .find(|t| t.name.as_deref() == Some(topic))
                    .map(|t| t.partitions.len() as i32)
                    .unwrap_or(1);
                let mut m = self.metadata_cache.lock().await;
                m.insert(topic.to_string(), TopicMetadata { num_partitions: count });
                count
            }
            Err(_) => 1,
        }
    }
}

fn current_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(0)
}
```

- [ ] **Step 2: Builder via `#[bon::builder]`**

`crates/client-producer/src/builder.rs`:

```rust
//! `Producer::builder()` — `bon`-generated builder for `Producer::start`.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;

use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;
use crate::producer::{Acks, Producer};
use crate::sender;

impl Producer {
    #[bon::builder(finish_fn = build)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(default = "crabka-producer".to_string())] client_id: String,
        #[builder(default = Compression::None)] compression: Compression,
        #[builder(default = true)] enable_idempotence: bool,
        #[builder(default = Acks::One)] acks: Acks,
        #[builder(default = Duration::from_millis(0))] linger: Duration,
        #[builder(default = 16 * 1024)] batch_size: usize,
        #[builder(default = Duration::from_secs(30))] request_timeout: Duration,
        #[builder(default = i32::MAX)] retries: i32,
        #[builder(default = Duration::from_millis(100))] retry_backoff: Duration,
        #[builder(default = 5)] max_in_flight_per_connection: usize,
    ) -> Result<Self, ProducerError> {
        // Validate config.
        let acks = if enable_idempotence { Acks::All } else { acks };
        if enable_idempotence && acks == Acks::Zero {
            return Err(ProducerError::InvalidConfig(
                "enable_idempotence=true requires acks=all (not Zero)",
            ));
        }

        // 1. Build inner client.
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .build()
            .await?;

        // 2. InitProducerId if idempotence on.
        let (producer_id, producer_epoch) = if enable_idempotence {
            let init = client
                .send(InitProducerIdRequest {
                    transactional_id: None,
                    transaction_timeout_ms: 0,
                    ..Default::default()
                })
                .await?;
            if init.error_code != 0 {
                return Err(ProducerError::Server(init.error_code));
            }
            (init.producer_id, init.producer_epoch)
        } else {
            (-1, -1)
        };

        // 3. Spawn the sender.
        let (wake_tx, wake_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(0));
        let metadata_cache = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let accumulators = Arc::new(DashMap::new());
        let next_seq = Arc::new(DashMap::new());
        let partitioner = Arc::new(UniformStickyPartitioner::new());
        let flush_notify = Arc::new(Notify::new());

        let sender_handle = tokio::spawn(sender::run(sender::SenderConfig {
            client: client.clone(),
            producer_id,
            producer_epoch,
            acks,
            compression,
            linger,
            request_timeout,
            retries,
            retry_backoff,
            metadata_cache: metadata_cache.clone(),
            accumulators: accumulators.clone(),
            next_seq: next_seq.clone(),
            state: state.clone(),
            wake_rx,
            flush_notify: flush_notify.clone(),
            shutdown: shutdown.clone(),
        }));

        Ok(Producer {
            client,
            producer_id,
            producer_epoch,
            acks,
            compression,
            batch_size,
            linger,
            request_timeout,
            retries,
            retry_backoff,
            max_in_flight: max_in_flight_per_connection,
            metadata_cache,
            accumulators,
            next_seq,
            partitioner,
            state,
            wake_tx,
            flush_notify,
            sender_shutdown: shutdown,
            sender_handle: Some(sender_handle),
        })
    }
}
```

- [ ] **Step 3: Hook into `lib.rs`**

Add `mod builder;` (the `bon::builder` macro doesn't need to be re-exported).

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-client-producer
git add crates/client-producer
git commit -m "feat(producer): Producer::builder via bon + send() + metadata cache"
```

---

## Phase E — Integration tests

### Task 17: Cross-crate integration tests

**Files:**
- Create: `crates/client-producer/tests/integration.rs`

- [ ] **Step 1: End-to-end Rust producer + Rust consumer**

`crates/client-producer/tests/integration.rs`:

```rust
//! Spawn an in-process broker; produce records via crabka-client-producer;
//! consume them via crabka-client-consumer.

use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tempfile::TempDir;

async fn boot() -> (Broker::BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let _ = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_produce_then_consume() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "rp1", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .unwrap();

    let mut futs = Vec::new();
    for i in 0..100 {
        futs.push(
            producer
                .send(ProducerRecord {
                    topic: "rp1".into(),
                    value: Some(Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    for f in futs {
        let m = f.await.unwrap().unwrap();
        assert_eq!(m.partition, 0);
    }

    // Consume back.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("rp1-grp".to_string())
        .subscribe(["rp1"])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    let mut seen = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen < 100 && std::time::Instant::now() < deadline {
        seen += consumer.poll(Duration::from_millis(200)).await.unwrap().len();
    }
    assert_eq!(seen, 100);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_idempotent_acks_zero_fire_and_forget() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "rp2", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(false)
        .acks(Acks::Zero)
        .build()
        .await
        .unwrap();
    let f = producer
        .send(ProducerRecord {
            topic: "rp2".into(),
            value: Some(Bytes::from_static(b"x")),
            ..Default::default()
        })
        .await;
    producer.flush().await.unwrap();
    // acks=0: the oneshot resolves as soon as the request is sent. We don't
    // assert offset (the broker doesn't ack offsets with acks=0).
    let _ = f.await; // either Ok or Err is fine; we just want flush to succeed.

    producer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotence_plus_acks_zero_rejects() {
    let (broker, bootstrap, _dir) = boot().await;
    let res = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(true)
        .acks(Acks::Zero)
        .build()
        .await;
    assert!(matches!(
        res,
        Err(crabka_client_producer::ProducerError::InvalidConfig(_))
    ));
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-client-producer --test integration
git add crates/client-producer/tests
git commit -m "test(producer): end-to-end Rust producer → Rust consumer + non-idempotent path"
```

---

## Phase F — JVM acceptance + CI + final PR

### Task 18: JVM acceptance test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add the new scenario**

Append to `crates/broker/tests/jvm_acceptance.rs`:

```rust
use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rust_producer_to_console_consumer() {
    const TOPIC: &str = "crabka-rust-producer-itest";

    let (broker, _dir) = start_host_broker().await;

    // 1. Create the topic.
    docker_run_kafka_tool(&[
        "kafka-topics", "--create", "--if-not-exists", "--topic", TOPIC,
        "--partitions", "1", "--replication-factor", "1",
        "--bootstrap-server", BOOTSTRAP,
    ]);

    // 2. Build a Rust producer pointed at the host broker and produce 3 records.
    let producer = Producer::builder()
        .bootstrap(BOOTSTRAP.to_string())
        .enable_idempotence(true)
        .acks(Acks::All)
        .compression(Compression::Lz4)
        .build()
        .await
        .expect("producer");
    for v in ["x", "y", "z"] {
        let fut = producer
            .send(ProducerRecord {
                topic: TOPIC.into(),
                value: Some(bytes::Bytes::from(v)),
                ..Default::default()
            })
            .await;
        let m = fut.await.expect("oneshot").expect("ack");
        assert_eq!(m.partition, 0);
    }
    producer.flush().await.expect("flush");
    producer.close().await.expect("close");

    // 3. Consume via kafka-console-consumer --partition 0.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server", BOOTSTRAP,
        "--topic", TOPIC,
        "--partition", "0",
        "--from-beginning",
        "--max-messages", "3",
        "--timeout-ms", "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Add `crabka-client-producer` as a broker dev-dep**

In `crates/broker/Cargo.toml` `[dev-dependencies]`:

```toml
crabka-client-producer = { version = "0.1", path = "../client-producer" }
```

- [ ] **Step 3: Commit**

```bash
cargo check -p crabka-broker --tests
git add crates/broker/tests crates/broker/Cargo.toml
git commit -m "test(broker): JVM acceptance — Rust producer → console consumer"
```

CI will exercise the test on Linux.

---

### Task 19: Acceptance gate + rustdoc + PR

- [ ] **Step 1: Crate-level rustdoc**

Update `crates/client-producer/src/lib.rs`:

```rust
//! Idempotent producer client for Apache Kafka in Rust.
//!
//! Builds on [`crabka_client_core`] for transport. Adds full
//! idempotent-producer semantics: `InitProducerId` on connect, per-batch
//! `(producer_id, producer_epoch, base_sequence)`, retries that re-frame
//! the same `RecordBatch` so the broker's dedup catches them.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use bytes::Bytes;
//! use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let producer = Producer::builder()
//!     .bootstrap("localhost:9092")
//!     .compression(Compression::Lz4)
//!     .acks(Acks::All)
//!     .linger(Duration::from_millis(5))
//!     .build()
//!     .await?;
//!
//! let metadata = producer
//!     .send(ProducerRecord {
//!         topic: "my-topic".into(),
//!         value: Some(Bytes::from("hello")),
//!         ..Default::default()
//!     })
//!     .await
//!     .await??;
//!
//! producer.flush().await?;
//! producer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - Transactions (slice 9).
//! - Persisted producer-state snapshots (broker restart resets sequences).
//! - Custom partitioner trait — sticky+hash only; `ProducerRecord::partition`
//!   bypasses the partitioner per record.
//! - Schema registry / serde glue — `key` and `value` are `Bytes`.

#![doc(html_root_url = "https://docs.rs/crabka-client-producer/0.0.0")]

mod accumulator;
mod builder;
mod compression;
mod error;
mod partitioner;
mod producer;
mod record;
mod sender;

pub use compression::Compression;
pub use error::ProducerError;
pub use producer::{Acks, Producer};
pub use record::{Header, ProducerRecord, RecordMetadata};
```

- [ ] **Step 2: Verify**

```bash
RUSTDOCFLAGS="-D warnings" cargo doc -p crabka-broker --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p crabka-client-producer --no-deps
```

- [ ] **Step 3: Full local gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-broker
cargo test -p crabka-client-producer
cargo test --workspace -- --include-ignored   # Docker-only tests will be skipped if Docker missing
```

- [ ] **Step 4: Push + PR**

```bash
git push -u origin feature/client-producer
gh pr create --base main --head feature/client-producer \
    --title "Slice 6: crabka-client-producer (idempotent producer + bon retrofit)" \
    --body "$(cat <<'PRBODY'
## Summary

Full-idempotent Rust Kafka producer + broker support. After this slice, a Rust app can write records that round-trip through a JVM `kafka-console-consumer`. The new producer ships with all four compression codecs, uniform-sticky partitioner, idempotence-by-default, and a `bon`-generated builder.

## What landed

- `crates/broker/`: `ProducerIdManager`, `ProducerState`, real `InitProducerId` handler, Produce-handler dedup / out-of-order / epoch-fence checks.
- `crates/client-producer/` (new): `Producer`, `ProducerBuilder` (bon), `Compression`, `Acks`, `ProducerRecord`, `RecordMetadata`, `Header`, `UniformStickyPartitioner`, per-partition accumulator + sender task.
- `crates/client-core/`: `Client::builder` retrofitted to `bon`.
- `crates/client-consumer/`: `Consumer::builder` retrofitted to `bon`.
- Tests: per-component unit tests, in-process Rust producer → Rust consumer integration tests, JVM acceptance `rust_producer_to_console_consumer`.

## Out of scope

Transactions (slice 9), persisted producer-state snapshots, custom partitioner trait, sender metrics, `crabka-producer` binary CLI (slice 10), schema registry. Each maps to a later slice.

## Reference

Spec: `docs/superpowers/specs/2026-05-12-crabka-client-producer-design.md`
Plan: `docs/superpowers/plans/2026-05-12-crabka-client-producer.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

---

## Self-review against the spec

| # | Spec criterion                                          | Plan task           |
|---|---------------------------------------------------------|---------------------|
| 1 | 5 wire codes + `ProducerEpochFenced`                    | Task 1              |
| 2 | `bon` retrofit for `ClientBuilder`                      | Task 2              |
| 3 | `bon` retrofit for `ConsumerBuilder`                    | Task 3              |
| 4 | `ProducerIdManager`                                     | Task 4              |
| 5 | `ProducerState` (dedup / out-of-order / fence)          | Task 5              |
| 6 | `ProducerIdManager` + `ProducerState` on `Broker`       | Task 6              |
| 7 | Real `InitProducerId` handler                           | Task 7              |
| 8 | Produce handler dedup integration                       | Task 8              |
| 9 | `crabka-client-producer` crate skeleton + `ProducerError` | Task 9            |
| 10 | `ProducerRecord` / `RecordMetadata` / `Header`         | Task 10             |
| 11 | All four compression codecs                            | Task 11             |
| 12 | `UniformStickyPartitioner`                              | Task 12             |
| 13 | Per-partition `Accumulator`                            | Task 13             |
| 14 | `Producer` struct + state machine                      | Task 14             |
| 15 | Sender task + retry + dedup-aware response handling    | Task 15             |
| 16 | `Producer::builder` via `bon` + `send` + metadata cache | Task 16            |
| 17 | Integration tests (idempotent + non-idempotent)        | Task 17             |
| 18 | JVM acceptance `rust_producer_to_console_consumer`     | Task 18             |
| 19 | Rustdoc + acceptance gate + PR                         | Task 19             |

**Placeholder scan:** No "TBD" / "TODO" markers. Two intentional defer-to-later notes in code blocks (the `STATE_*_PUB` constants and the `clone_err` workaround for non-`Clone` errors) are flagged inline with rationale; the implementer doesn't need to invent anything.

**Type consistency:** `Producer`, `ProducerBuilder` (generated by `bon`), `ProducerRecord`, `RecordMetadata`, `Header`, `Compression`, `Acks`, `ProducerError`, `Accumulator`, `InProgressBatch`, `PendingRecord`, `TopicMetadata`, `UniformStickyPartitioner`, `ProducerIdManager`, `ProducerState`, `Decision`, `ProducerEntry` — used consistently across tasks. `bon`-generated builder uses `bootstrap(...)` setter (string) and `subscribe(...)` setter (with `#[builder(into)]` accepting `Vec<&str>` or `Vec<String>`).

The plan is ready for execution.
