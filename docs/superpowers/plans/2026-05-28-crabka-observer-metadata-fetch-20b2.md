# Component B — True Observer Metadata Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let broker-only KRaft nodes keep their `MetadataImage` current by *fetching* the `__cluster_metadata` log from controllers over a new controller-listener RPC, instead of joining the openraft voter quorum.

**Architecture:** Controllers expose committed raft-log entries as Kafka-record-encoded batches over a new `API_KEY_METADATA_FETCH` RPC on the existing controller listener (port 9093, same `OutboundDialer` TLS/SASL path as `API_KEY_SUBMIT_CHANGE`). A `MetadataObserver` on broker-only nodes polls that RPC, decodes each record through a stable `MetadataRecord ↔ Kafka Record` bridge, and feeds them through `MetadataImage::validate` + `apply`, publishing each new image on its own `watch` channel. Handlers reach metadata through a `MetadataSource` trait satisfied by either `ControllerHandle` (combined/controller nodes) or the observer-backed source (broker-only nodes), so no handler code changes. Broker-only nodes therefore do **not** start a `Controller` at all.

**Tech Stack:** Rust, openraft 0.9, tokio, `crabka_protocol` `RecordBatch`/`Record` codec, `serde-wincode`, existing crabka controller-listener wire framing.

**Greenfield constraint (from CLAUDE.md):** No backwards-compat shims. When a schema/wire/enum changes, just change it. Kafka wire-protocol byte exactness and KIP semantics are the binding constraints — but `__cluster_metadata` fetch is internal crabka↔crabka transport (clients never fetch it; `kafka-metadata-quorum --describe` uses `DescribeQuorum`), so the bridge's wire shape is crabka-private and only needs to be *stable + round-trippable*, not Kafka-canonical.

---

## File Structure

**Phase 1 — Serialization bridge (`crates/metadata`)**
- Create: `crates/metadata/src/kafka_record.rs` — `to_kafka_record` / `from_kafka_record` + `KafkaRecordError`.
- Modify: `crates/metadata/Cargo.toml` — add `crabka-protocol` dependency.
- Modify: `crates/metadata/src/lib.rs` — `mod kafka_record;` + re-exports.

**Phase 2 — Controller read API (`crates/raft`)**
- Modify: `crates/raft/src/log_store.rs` — make `read_range` `pub`, add `pub async fn log_start_index`.
- Create: `crates/raft/src/metadata_fetch.rs` — `MetadataFetchSlice` type + `encode_committed_records` free fn.
- Modify: `crates/raft/src/controller.rs` — stash `Arc<RaftLogStore>` in `ControllerHandle`, add `metadata_records` method.
- Modify: `crates/raft/src/lib.rs` — `mod metadata_fetch;` + re-export `MetadataFetchSlice`.

**Phase 3 — Controller-listener RPC (`crates/raft`)**
- Modify: `crates/raft/src/wire.rs` — `API_KEY_METADATA_FETCH` + request/response types.
- Modify: `crates/raft/src/server.rs` — thread `Arc<RaftLogStore>` into `run`/`dispatch`, add `dispatch_metadata_fetch`.
- Modify: `crates/raft/src/controller.rs` — pass `log_store` to `server::run`; add `fetch_metadata_from` client method.

**Phase 4 — Observer (`crates/broker`)**
- Create: `crates/broker/src/metadata_observer.rs` — `MetadataObserver` + `ObserverConfig` + fetch loop.
- Modify: `crates/broker/src/lib.rs` — `mod metadata_observer;`.

**Phase 5 — `MetadataSource` abstraction + broker wiring (`crates/broker`)**
- Create: `crates/broker/src/metadata_source.rs` — `MetadataSource` trait, `impl` for `ControllerHandle`, `ObserverSource`.
- Modify: `crates/broker/src/broker.rs` — retype `Broker.controller`, branch startup on `is_controller()`.
- Modify: `crates/broker/src/lib.rs` — `mod metadata_source;`.

**Phase 6 — Integration test (`crates/broker/tests`)**
- Create: `crates/broker/tests/role_separation_observer.rs`.

---

## Phase 1 — Serialization Bridge

### Task 1: Add `crabka-protocol` dependency to `crates/metadata`

**Files:**
- Modify: `crates/metadata/Cargo.toml`

- [ ] **Step 1: Add the dependency**

In `crates/metadata/Cargo.toml`, under `[dependencies]`, add the `crabka-protocol` line (matches how `crates/raft/Cargo.toml` references it, `default-features = false`):

```toml
[dependencies]
wincode = { workspace = true }
serde-wincode = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true, features = ["derive"] }
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
```

- [ ] **Step 2: Verify it builds (no cycle)**

Run: `cargo build -p crabka-metadata`
Expected: compiles clean. (`crabka-protocol` does not depend on `crabka-metadata`, so there is no dependency cycle.)

- [ ] **Step 3: Commit**

```bash
git add crates/metadata/Cargo.toml
git commit -m "build(metadata): depend on crabka-protocol for the kafka-record bridge"
```

---

### Task 2: `to_kafka_record` / `from_kafka_record` bridge

**Files:**
- Create: `crates/metadata/src/kafka_record.rs`
- Modify: `crates/metadata/src/lib.rs`

The bridge encodes one `MetadataRecord` into one `crabka_protocol` `Record`: `key = None`, `value = Some(wincode(MetadataRecord))`. The `MetadataRecord` enum variant already encodes the record type + version (e.g. `V1Topic`), so no extra type tag is needed. This is the crabka-private `__cluster_metadata` wire schema.

- [ ] **Step 1: Write the failing tests**

Create `crates/metadata/src/kafka_record.rs`:

```rust
//! Bridge between [`MetadataRecord`] and the Kafka `Record` wire type.
//!
//! `__cluster_metadata` is fetched by broker-only observers as Kafka
//! record batches (Component B). Each [`MetadataRecord`] maps to exactly
//! one [`Record`]: `key = None`, `value = wincode(MetadataRecord)`. The
//! enum variant itself is the record type + version, so no separate type
//! tag is carried. This wire surface is crabka-private (clients never
//! fetch `__cluster_metadata`), so it only needs to be stable and
//! round-trippable — not byte-identical to Apache Kafka's `ApiMessage`
//! framing.

use bytes::Bytes;
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as _, Serialize as _};

use crabka_protocol::records::owned::Record;

use crate::records::MetadataRecord;

/// Error decoding a `Record` back into a [`MetadataRecord`].
#[derive(Debug, thiserror::Error)]
pub enum KafkaRecordError {
    #[error("metadata record has no value payload")]
    MissingValue,
    #[error("wincode decode failed: {0}")]
    Decode(String),
    #[error("wincode encode failed: {0}")]
    Encode(String),
}

/// Encode one [`MetadataRecord`] as a Kafka [`Record`].
///
/// # Errors
/// Returns [`KafkaRecordError::Encode`] if wincode serialization fails
/// (in practice this cannot happen for `MetadataRecord`).
pub fn to_kafka_record(rec: &MetadataRecord) -> Result<Record, KafkaRecordError> {
    let payload = <SerdeCompat<MetadataRecord>>::serialize(rec)
        .map_err(|e| KafkaRecordError::Encode(e.to_string()))?;
    Ok(Record {
        key: None,
        value: Some(Bytes::from(payload)),
        ..Default::default()
    })
}

/// Decode a Kafka [`Record`] back into a [`MetadataRecord`].
///
/// # Errors
/// - [`KafkaRecordError::MissingValue`] if the record carries no value.
/// - [`KafkaRecordError::Decode`] if the value is not a valid
///   wincode-encoded `MetadataRecord`.
pub fn from_kafka_record(rec: &Record) -> Result<MetadataRecord, KafkaRecordError> {
    let value = rec.value.as_ref().ok_or(KafkaRecordError::MissingValue)?;
    <SerdeCompat<MetadataRecord>>::deserialize(value)
        .map_err(|e| KafkaRecordError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{MetadataRecord, TopicRecord};
    use uuid::Uuid;

    fn sample_topic() -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::from_u128(0x1234_5678_9abc_def0),
            partitions: 6,
            replication_factor: 3,
        })
    }

    #[test]
    fn round_trips_through_kafka_record() {
        let rec = sample_topic();
        let kafka = to_kafka_record(&rec).expect("encode");
        let back = from_kafka_record(&kafka).expect("decode");
        assert_eq!(rec, back);
    }

    #[test]
    fn key_is_none_and_value_is_present() {
        let kafka = to_kafka_record(&sample_topic()).expect("encode");
        assert!(kafka.key.is_none());
        assert!(kafka.value.is_some());
    }

    #[test]
    fn missing_value_is_an_error() {
        let empty = Record::default();
        assert!(matches!(
            from_kafka_record(&empty),
            Err(KafkaRecordError::MissingValue)
        ));
    }

    #[test]
    fn encoding_is_stable_across_calls() {
        // Stability guard: the same record must always produce the same
        // value bytes, so an observer fetching twice sees identical frames.
        let rec = sample_topic();
        let a = to_kafka_record(&rec).expect("encode");
        let b = to_kafka_record(&rec).expect("encode");
        assert_eq!(a.value, b.value);
    }
}
```

- [ ] **Step 2: Wire the module in**

In `crates/metadata/src/lib.rs`, add the module declaration alongside the other `mod` lines and re-export the public surface (place near the existing `records` / `image` re-exports):

```rust
pub mod kafka_record;
pub use kafka_record::{from_kafka_record, to_kafka_record, KafkaRecordError};
```

(If `thiserror` is not yet a dependency of `crates/metadata`, add `thiserror = { workspace = true }` to `[dependencies]` in `crates/metadata/Cargo.toml`. Check first: `grep thiserror crates/metadata/Cargo.toml`.)

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p crabka-metadata kafka_record`
Expected: 4 tests pass (`round_trips_through_kafka_record`, `key_is_none_and_value_is_present`, `missing_value_is_an_error`, `encoding_is_stable_across_calls`).

- [ ] **Step 4: Commit**

```bash
git add crates/metadata/src/kafka_record.rs crates/metadata/src/lib.rs crates/metadata/Cargo.toml
git commit -m "feat(metadata): MetadataRecord <-> Kafka Record bridge for __cluster_metadata fetch"
```

---

## Phase 2 — Controller Read API

### Task 3: Expose `read_range` + add `log_start_index` on `RaftLogStore`

**Files:**
- Modify: `crates/raft/src/log_store.rs`

- [ ] **Step 1: Make `read_range` public and add `log_start_index`**

In `crates/raft/src/log_store.rs`, change `read_range`'s visibility from `pub(crate)` to `pub`:

```rust
    pub async fn read_range<R: RangeBounds<u64>>(&self, range: R) -> Vec<Entry<TypeConfig>> {
        self.cache
            .lock()
            .await
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect()
    }
```

Immediately after `read_range`, add:

```rust
    /// Lowest log index currently retained in the store, or `0` if the
    /// log is empty. Tracks raft log truncation/snapshotting; an observer
    /// that has fallen behind this offset must rebuild from a snapshot.
    pub async fn log_start_index(&self) -> u64 {
        self.cache
            .lock()
            .await
            .entries
            .keys()
            .next()
            .copied()
            .unwrap_or(0)
    }
```

- [ ] **Step 2: Add a unit test**

In the `#[cfg(test)]` module of `crates/raft/src/log_store.rs` (append a new test; if no test module exists, create `#[cfg(test)] mod tests { use super::*; ... }`):

```rust
    #[tokio::test]
    async fn read_range_and_log_start_index() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let store = RaftLogStore::open(dir.path().to_path_buf()).await.unwrap();
        assert_eq!(store.log_start_index().await, 0);

        let entries: Vec<Entry<TypeConfig>> = (1..=3)
            .map(|i| Entry {
                log_id: LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: i,
                },
                payload: openraft::EntryPayload::Blank,
            })
            .collect();
        store.append(entries).await.unwrap();

        assert_eq!(store.log_start_index().await, 1);
        let got = store.read_range(2..=3).await;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 2);
        assert_eq!(got[1].log_id.index, 3);
    }
```

(Ensure `use openraft::LogId;` and the `Entry`/`TypeConfig` imports are in scope for the test module; add `use super::*;` and `use crate::types::TypeConfig;` if missing.)

- [ ] **Step 3: Run the test**

Run: `cargo test -p crabka-raft read_range_and_log_start_index`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/log_store.rs
git commit -m "feat(raft): expose RaftLogStore::read_range + log_start_index for observer fetch"
```

---

### Task 4: `MetadataFetchSlice` + `encode_committed_records`

**Files:**
- Create: `crates/raft/src/metadata_fetch.rs`
- Modify: `crates/raft/src/lib.rs`

This converts a slice of committed openraft `Entry<TypeConfig>` into a `Bytes` payload that is a concatenation of `RecordBatch`es — one batch per log entry, `base_offset = entry.log_id.index`, `last_offset_delta = 0`, containing one `Record` per `MetadataRecord` in that entry's `AppData` (Blank/Membership entries produce an empty batch so the observer still advances over their index).

- [ ] **Step 1: Write the failing tests**

Create `crates/raft/src/metadata_fetch.rs`:

```rust
//! Encoding committed `__cluster_metadata` log entries as Kafka record
//! batches for the observer-fetch RPC (Component B).
//!
//! Each openraft log entry becomes one `RecordBatch` with
//! `base_offset == log_id.index` and `last_offset_delta == 0`. A
//! `Normal` entry's `AppData.records` become one `Record` each (via the
//! `crabka_metadata` bridge); `Blank`/`Membership` entries become empty
//! batches so the observer's fetch offset still advances past them.

use bytes::{BufMut, Bytes, BytesMut};
use openraft::{Entry, EntryPayload};

use crabka_metadata::to_kafka_record;
use crabka_protocol::records::owned::RecordBatch;

use crate::types::TypeConfig;

/// A committed-range read result handed back by the controller's
/// metadata-fetch path. `records` is a concatenation of `RecordBatch`es
/// (one per log entry); `log_start_offset` and `high_watermark` are
/// openraft log indices.
#[derive(Debug, Clone)]
pub struct MetadataFetchSlice {
    pub records: Bytes,
    pub log_start_offset: u64,
    pub high_watermark: u64,
}

/// Encode committed log entries as concatenated Kafka record batches,
/// stopping once `max_bytes` would be exceeded (but always emitting at
/// least the first entry so the observer makes progress).
#[must_use]
pub fn encode_committed_records(entries: &[Entry<TypeConfig>], max_bytes: usize) -> Bytes {
    let mut out = BytesMut::new();
    for (i, entry) in entries.iter().enumerate() {
        let records = match &entry.payload {
            EntryPayload::Normal(data) => data
                .records
                .iter()
                .filter_map(|r| to_kafka_record(r).ok())
                .collect(),
            EntryPayload::Blank | EntryPayload::Membership(_) => Vec::new(),
        };
        let batch = RecordBatch {
            base_offset: i64::try_from(entry.log_id.index).unwrap_or(i64::MAX),
            last_offset_delta: 0,
            records,
            ..Default::default()
        };
        let mut scratch = BytesMut::new();
        if batch.encode(&mut scratch).is_err() {
            break;
        }
        // Always emit the first batch; afterwards respect max_bytes.
        if i > 0 && out.len() + scratch.len() > max_bytes {
            break;
        }
        out.put_slice(&scratch);
    }
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{from_kafka_record, MetadataRecord, TopicRecord};
    use crabka_protocol::records::owned::RecordBatch as OwnedBatch;
    use openraft::LogId;
    use uuid::Uuid;

    use crate::types::AppData;

    fn normal_entry(index: u64, topic: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: openraft::LeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(AppData {
                records: vec![MetadataRecord::V1Topic(TopicRecord {
                    name: topic.into(),
                    topic_id: Uuid::from_u128(u128::from(index)),
                    partitions: 1,
                    replication_factor: 1,
                })],
            }),
        }
    }

    fn blank_entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: openraft::LeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    fn decode_all(mut buf: &[u8]) -> Vec<OwnedBatch> {
        let mut out = Vec::new();
        while !buf.is_empty() {
            let batch = OwnedBatch::decode(&mut buf).expect("decode batch");
            out.push(batch);
        }
        out
    }

    #[test]
    fn encodes_one_batch_per_entry_with_base_offset() {
        let entries = vec![normal_entry(1, "a"), blank_entry(2), normal_entry(3, "b")];
        let bytes = encode_committed_records(&entries, usize::MAX);
        let batches = decode_all(&bytes);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].base_offset, 1);
        assert_eq!(batches[1].base_offset, 2);
        assert_eq!(batches[2].base_offset, 3);
        // Blank entry -> empty batch.
        assert_eq!(batches[1].records.len(), 0);
        // Normal entry -> one decodable MetadataRecord.
        let rec = from_kafka_record(&batches[0].records[0]).expect("decode record");
        assert!(matches!(rec, MetadataRecord::V1Topic(t) if t.name == "a"));
    }

    #[test]
    fn max_bytes_truncates_but_always_emits_first() {
        let entries = vec![normal_entry(1, "a"), normal_entry(2, "b")];
        // max_bytes = 1 forces truncation after the first batch.
        let bytes = encode_committed_records(&entries, 1);
        let batches = decode_all(&bytes);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].base_offset, 1);
    }
}
```

- [ ] **Step 2: Wire the module in**

In `crates/raft/src/lib.rs`, add near the other `mod` declarations and `pub use` lines:

```rust
mod metadata_fetch;
pub use metadata_fetch::{encode_committed_records, MetadataFetchSlice};
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-raft metadata_fetch`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/metadata_fetch.rs crates/raft/src/lib.rs
git commit -m "feat(raft): encode committed metadata log entries as Kafka record batches"
```

---

### Task 5: Stash `Arc<RaftLogStore>` in `ControllerHandle` + `metadata_records` method

**Files:**
- Modify: `crates/raft/src/controller.rs`

- [ ] **Step 1: Add the `log_store` field to `ControllerHandle`**

In `crates/raft/src/controller.rs`, add a field to the `ControllerHandle` struct (after `dialer`):

```rust
    /// Clone of the openraft storage adapter. Used by
    /// [`Self::metadata_records`] to serve committed log entries to
    /// broker-only observers over `API_KEY_METADATA_FETCH`.
    log_store: Arc<RaftLogStore>,
```

- [ ] **Step 2: Populate it in `Controller::start`**

In `Controller::start`, the `ControllerHandle { .. }` constructor at the end of the function — add `log_store: log_store.clone(),` (the local `log_store` is already an `Arc<RaftLogStore>` created earlier in the function and cloned into `Raft::new`):

```rust
        Ok(ControllerHandle {
            raft,
            state_machine,
            leader: leader_rx,
            shutdown,
            listener_task: Mutex::new(Some(listener_task)),
            leader_pump_task: Mutex::new(Some(leader_pump_task)),
            voters: config.voters.clone(),
            client_id: config.client_id.clone(),
            dialer,
            log_store: log_store.clone(),
        })
```

- [ ] **Step 3: Write the failing test for `metadata_records`**

Add to the test module in `crates/raft/src/controller.rs` (the `bootstrap_mode_tests` module already imports `super::*` and `TempDir`):

```rust
    #[tokio::test]
    async fn metadata_records_serves_committed_topic() {
        use crabka_metadata::{from_kafka_record, MetadataRecord, TopicRecord};
        use crabka_protocol::records::owned::RecordBatch;
        use uuid::Uuid;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        // Wait to become leader.
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");

        let slice = ctrl.metadata_records(0, usize::MAX).await;
        assert!(slice.high_watermark >= 1);
        // Decode the batches and confirm topic "t" is present somewhere.
        let mut buf: &[u8] = &slice.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            for r in &batch.records {
                if let Ok(MetadataRecord::V1Topic(t)) = from_kafka_record(r) {
                    if t.name == "t" {
                        found = true;
                    }
                }
            }
        }
        assert!(found, "topic 't' must appear in fetched metadata records");
        ctrl.shutdown().await;
    }
```

- [ ] **Step 4: Implement `metadata_records`**

Add this method inside `impl ControllerHandle` (e.g. after `quorum_state`):

```rust
    /// Read committed `__cluster_metadata` entries starting at
    /// `fetch_offset` (an openraft log index), encoded as Kafka record
    /// batches for an observer. Entries beyond the current high watermark
    /// (last applied/committed index) are never served. `max_bytes` caps
    /// the encoded payload (at least one batch is always emitted so the
    /// observer makes progress).
    #[must_use]
    pub async fn metadata_records(
        &self,
        fetch_offset: u64,
        max_bytes: usize,
    ) -> crate::metadata_fetch::MetadataFetchSlice {
        let high_watermark = self
            .raft
            .metrics()
            .borrow()
            .last_applied
            .as_ref()
            .map_or(0, |l| l.index);
        let log_start_offset = self.log_store.log_start_index().await;
        let entries = if fetch_offset > high_watermark {
            Vec::new()
        } else {
            self.log_store.read_range(fetch_offset..=high_watermark).await
        };
        let records = crate::metadata_fetch::encode_committed_records(&entries, max_bytes);
        crate::metadata_fetch::MetadataFetchSlice {
            records,
            log_start_offset,
            high_watermark,
        }
    }
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p crabka-raft metadata_records_serves_committed_topic`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/controller.rs
git commit -m "feat(raft): ControllerHandle::metadata_records serves committed log as record batches"
```

---

## Phase 3 — Controller-Listener RPC

### Task 6: `API_KEY_METADATA_FETCH` wire types

**Files:**
- Modify: `crates/raft/src/wire.rs`

- [ ] **Step 1: Write the failing round-trip tests**

In `crates/raft/src/wire.rs`, find the existing `#[cfg(test)] mod tests` (or add one) and append:

```rust
    #[test]
    fn metadata_fetch_request_round_trips() {
        let req = CrabkaMetadataFetchRequest {
            fetch_offset: 42,
            max_bytes: 1_048_576,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        let mut cur: &[u8] = &out;
        let got = CrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap();
        assert_eq!(got, req);
    }

    #[test]
    fn metadata_fetch_response_round_trips() {
        let resp = CrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 3,
            log_start_offset: 1,
            high_watermark: 99,
            records: bytes::Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        let got = CrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap();
        assert_eq!(got, resp);
    }
```

- [ ] **Step 2: Add the API key constant**

After `pub const API_KEY_SUBMIT_CHANGE: i16 = 1003;` add:

```rust
/// Observer metadata fetch (Component B). The body carries a
/// `fetch_offset` (openraft log index) + `max_bytes`; the response
/// carries committed `__cluster_metadata` entries encoded as Kafka
/// record batches, plus `log_start_offset` / `high_watermark` and a
/// `leader_hint` so the observer can retarget the quorum.
pub const API_KEY_METADATA_FETCH: i16 = 1004;
```

- [ ] **Step 3: Add the request/response types**

After the `CrabkaSubmitChangeResponse` impl block (around line 335), add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaMetadataFetchRequest {
    /// Next openraft log index the observer wants.
    pub fetch_offset: i64,
    /// Soft cap on the encoded record-batch payload.
    pub max_bytes: i32,
}

impl CrabkaMetadataFetchRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) {
        out.put_i64(self.fetch_offset);
        out.put_i32(self.max_bytes);
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const LEN: usize = 8 + 4;
        if buf.remaining() < LEN {
            return Err(ProtocolError::UnexpectedEof {
                needed: LEN - buf.remaining(),
            });
        }
        Ok(Self {
            fetch_offset: buf.get_i64(),
            max_bytes: buf.get_i32(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaMetadataFetchResponse {
    /// 0 = success; 1 = this node cannot serve (not leader/not a voter) —
    /// consult `leader_hint`.
    pub error_code: i16,
    /// Leader id the responder believes is current; -1 = unknown.
    pub leader_hint: i64,
    /// Lowest retained log index on the responder.
    pub log_start_offset: i64,
    /// Highest committed (applied) log index on the responder.
    pub high_watermark: i64,
    /// Concatenated Kafka `RecordBatch`es (one per log entry).
    pub records: Bytes,
}

impl CrabkaMetadataFetchResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        out.put_i64(self.log_start_offset);
        out.put_i64(self.high_watermark);
        out.put_i32(
            i32::try_from(self.records.len())
                .map_err(|_| ProtocolError::InvalidValue("records length exceeds i32::MAX"))?,
        );
        out.put_slice(&self.records);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        const FIXED: usize = 2 + 8 + 8 + 8 + 4;
        if buf.remaining() < FIXED {
            return Err(ProtocolError::UnexpectedEof {
                needed: FIXED - buf.remaining(),
            });
        }
        let error_code = buf.get_i16();
        let leader_hint = buf.get_i64();
        let log_start_offset = buf.get_i64();
        let high_watermark = buf.get_i64();
        let len = buf.get_i32();
        let len = usize::try_from(len)
            .map_err(|_| ProtocolError::InvalidValue("negative records length"))?;
        if buf.remaining() < len {
            return Err(ProtocolError::UnexpectedEof {
                needed: len - buf.remaining(),
            });
        }
        let records = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
        Ok(Self {
            error_code,
            leader_hint,
            log_start_offset,
            high_watermark,
            records,
        })
    }
}
```

- [ ] **Step 4: Re-export from lib.rs**

In `crates/raft/src/lib.rs`, the existing `pub use wire::{...}` block lists the exported wire types. Add `API_KEY_METADATA_FETCH`, `CrabkaMetadataFetchRequest`, `CrabkaMetadataFetchResponse` to that list.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p crabka-raft metadata_fetch_request_round_trips metadata_fetch_response_round_trips`
Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/wire.rs crates/raft/src/lib.rs
git commit -m "feat(raft): API_KEY_METADATA_FETCH wire request/response types"
```

---

### Task 7: Server-side dispatch for `API_KEY_METADATA_FETCH`

**Files:**
- Modify: `crates/raft/src/server.rs`
- Modify: `crates/raft/src/controller.rs` (pass `log_store` into `server::run`)

The metadata-fetch handler needs the `RaftLogStore` and `raft` (for high watermark + leader hint). Thread an `Arc<RaftLogStore>` through `server::run` → `handle_conn` → `dispatch`.

- [ ] **Step 1: Thread `log_store` into `server::run`**

In `crates/raft/src/server.rs`, change the `run` signature to accept the log store and pass it down:

```rust
pub(crate) async fn run(
    listener: TcpListener,
    raft: Arc<Raft>,
    log_store: Arc<crate::log_store::RaftLogStore>,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
) {
```

Inside the accept loop, clone `log_store` alongside `raft` before the `tokio::spawn`, and pass it to `handle_conn`:

```rust
                        let raft = raft.clone();
                        let log_store = log_store.clone();
                        let shutdown = shutdown.clone();
                        let handshake = handshake.clone();
                        tokio::spawn(async move {
                            let boxed: Box<dyn crate::DuplexStream> = if let Some(hs) = handshake {
                                match hs.upgrade(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "handshake failed");
                                        return;
                                    }
                                }
                            } else {
                                Box::new(stream) as Box<dyn crate::DuplexStream>
                            };
                            if let Err(e) = handle_conn(boxed, raft, log_store, shutdown).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
```

- [ ] **Step 2: Thread `log_store` through `handle_conn` and `dispatch`**

Change `handle_conn` to accept and forward `log_store`:

```rust
async fn handle_conn<S>(
    mut stream: S,
    raft: Arc<Raft>,
    log_store: Arc<crate::log_store::RaftLogStore>,
    shutdown: CancellationToken,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
```

and update the dispatch call site inside `handle_conn`:

```rust
                let resp = dispatch(api_key, &body, &raft, &log_store).await?;
```

Change `dispatch`'s signature:

```rust
async fn dispatch(
    api_key: i16,
    body: &[u8],
    raft: &Raft,
    log_store: &Arc<crate::log_store::RaftLogStore>,
) -> Result<Bytes, RaftError> {
```

- [ ] **Step 3: Add the `API_KEY_METADATA_FETCH` arm + handler**

Add to the `match api_key` in `dispatch`, alongside `API_KEY_SUBMIT_CHANGE`:

```rust
        API_KEY_METADATA_FETCH => dispatch_metadata_fetch(body, raft, log_store).await,
```

Add the import to the `use crate::wire::{...}` block at the top of `server.rs`:
`API_KEY_METADATA_FETCH, CrabkaMetadataFetchRequest, CrabkaMetadataFetchResponse`.

Add the handler function (after `dispatch_submit_change`):

```rust
/// Serve a slice of committed `__cluster_metadata` entries to a
/// broker-only observer. Reads `[fetch_offset, high_watermark]` from the
/// log store, encodes each entry as a Kafka record batch, and returns
/// them plus `log_start_offset`, `high_watermark`, and a `leader_hint`.
async fn dispatch_metadata_fetch(
    body: &[u8],
    raft: &Raft,
    log_store: &Arc<crate::log_store::RaftLogStore>,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = CrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let metrics = raft.metrics().borrow().clone();
    let high_watermark = metrics.last_applied.as_ref().map_or(0, |l| l.index);
    let leader_hint = metrics
        .current_leader
        .map_or(-1, |l| i64::try_from(l).unwrap_or(-1));
    let log_start_offset = log_store.log_start_index().await;

    let fetch_offset = u64::try_from(req.fetch_offset.max(0)).unwrap_or(0);
    let max_bytes = usize::try_from(req.max_bytes.max(0)).unwrap_or(0);
    let entries = if fetch_offset > high_watermark {
        Vec::new()
    } else {
        log_store.read_range(fetch_offset..=high_watermark).await
    };
    let records = crate::metadata_fetch::encode_committed_records(&entries, max_bytes);

    let resp = CrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        log_start_offset: i64::try_from(log_start_offset).unwrap_or(i64::MAX),
        high_watermark: i64::try_from(high_watermark).unwrap_or(i64::MAX),
        records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}
```

- [ ] **Step 4: Update `Controller::start` to pass `log_store` into `server::run`**

In `crates/raft/src/controller.rs`, the `tokio::spawn(server::run(...))` call (step 6 of `start`) must pass the new `log_store` argument:

```rust
        let listener_task = tokio::spawn(server::run(
            listener,
            raft.clone(),
            log_store.clone(),
            shutdown.clone(),
            config.handshake.clone(),
        ));
```

- [ ] **Step 5: Build to verify the threading compiles**

Run: `cargo build -p crabka-raft`
Expected: compiles clean (no other callers of `server::run` exist — it is `pub(crate)` and only invoked from `controller.rs`).

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/server.rs crates/raft/src/controller.rs
git commit -m "feat(raft): serve API_KEY_METADATA_FETCH from the controller listener"
```

---

### Task 8: `ControllerHandle::fetch_metadata_from` client method

**Files:**
- Modify: `crates/raft/src/controller.rs`

A client helper that dials a controller-listener addr and issues one `API_KEY_METADATA_FETCH`. Mirrors `forward_submit_to`. The observer (Phase 4) reuses the same dial pattern, but exposing this on `ControllerHandle` gives an in-crate integration test against a live listener.

- [ ] **Step 1: Write the failing test**

Add to the `bootstrap_mode_tests` module in `controller.rs`:

```rust
    #[tokio::test]
    async fn fetch_metadata_from_returns_committed_records() {
        use crabka_metadata::{from_kafka_record, MetadataRecord, TopicRecord};
        use crabka_protocol::records::owned::RecordBatch;
        use uuid::Uuid;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "fetched".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");

        let addr = ctrl.voter_addr(1).expect("self addr");
        let resp = ctrl
            .fetch_metadata_from(addr, 0, 1_048_576)
            .await
            .expect("fetch");
        assert_eq!(resp.error_code, 0);
        assert!(resp.high_watermark >= 1);

        let mut buf: &[u8] = &resp.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            for r in &batch.records {
                if let Ok(MetadataRecord::V1Topic(t)) = from_kafka_record(r) {
                    if t.name == "fetched" {
                        found = true;
                    }
                }
            }
        }
        assert!(found);
        ctrl.shutdown().await;
    }
```

(Note: `ControllerConfig::for_tests` sets `voters` to include the self addr; confirm `voter_addr(1)` resolves. If `for_tests` leaves `voters` empty, build the addr from `config.controller_listen_addr` instead — read `for_tests` at `crates/raft/src/config.rs:95` to confirm.)

- [ ] **Step 2: Implement `fetch_metadata_from`**

Add inside `impl ControllerHandle` (after `forward_submit_to`):

```rust
    /// Dial a controller-listener `addr` and issue one
    /// `API_KEY_METADATA_FETCH`. Used by broker-only observers (and the
    /// in-crate integration test) to pull committed `__cluster_metadata`
    /// entries. Routes through the same [`OutboundDialer`] as
    /// `forward_submit_to`, so TLS/SASL terminates before the first frame.
    ///
    /// # Errors
    /// - [`RaftError::Network`] if the dial or request fails.
    /// - [`RaftError::Protocol`] if the response cannot be decoded.
    pub async fn fetch_metadata_from(
        &self,
        addr: SocketAddr,
        fetch_offset: u64,
        max_bytes: u32,
    ) -> Result<crate::wire::CrabkaMetadataFetchResponse, RaftError> {
        let req = crate::wire::CrabkaMetadataFetchRequest {
            fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
            max_bytes: i32::try_from(max_bytes).unwrap_or(i32::MAX),
        };
        let mut body = Vec::with_capacity(12);
        req.encode_v0(&mut body);

        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        // node_id 0 is a placeholder; the dialer only needs the addr for a
        // one-shot metadata fetch (no per-node identity is consulted).
        let conn = self
            .dialer
            .dial(0, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_METADATA_FETCH,
                0,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        crate::wire::CrabkaMetadataFetchResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }
```

(Confirm `OutboundDialer::dial`'s first parameter type by reading `crates/raft/src/network.rs:62-90`; the `forward_submit_to` call passes a `NodeId`. If the dialer requires the *real* target node id rather than a placeholder, pass the leader id the observer is targeting instead — for this in-crate test, pass `1`.)

- [ ] **Step 3: Run the test**

Run: `cargo test -p crabka-raft fetch_metadata_from_returns_committed_records`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/controller.rs
git commit -m "feat(raft): ControllerHandle::fetch_metadata_from client for observer fetch"
```

---

## Phase 4 — `MetadataObserver`

### Task 9: `MetadataObserver` fetch loop

**Files:**
- Create: `crates/broker/src/metadata_observer.rs`
- Modify: `crates/broker/src/lib.rs`

The observer is a background task that repeatedly fetches committed `__cluster_metadata` from the controller quorum, decodes each batch into `MetadataRecord`s, applies them to a local `MetadataImage` (mirroring the state machine's validate-then-apply), and publishes each new image on its own `watch` channel. It owns its fetch offset (next openraft log index) and fails over across voters using the `leader_hint`.

- [ ] **Step 1: Write the failing integration-style test**

Create `crates/broker/src/metadata_observer.rs`:

```rust
//! Broker-only metadata observer (Component B).
//!
//! A broker-only KRaft node is not an openraft voter — it keeps its
//! `MetadataImage` current by *fetching* the committed `__cluster_metadata`
//! log from the controller quorum over `API_KEY_METADATA_FETCH`, decoding
//! each record batch through the `crabka_metadata` Kafka-record bridge, and
//! applying records exactly as the controller state machine would.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_metadata::{from_kafka_record, MetadataImage};
use crabka_protocol::records::owned::RecordBatch;
use crabka_raft::{NodeId, OutboundDialer};

/// Static configuration for the observer.
#[derive(Clone)]
pub struct ObserverConfig {
    /// Controller-listener voter map (id, addr) from `controller_quorum_voters`.
    pub voters: Vec<(NodeId, SocketAddr)>,
    /// Outbound dialer (same TLS/SASL path as the raft transport).
    pub dialer: Arc<dyn OutboundDialer>,
    /// `client_id` for the dial handshake.
    pub client_id: String,
    /// Cluster UUID for the initial empty image.
    pub cluster_id: uuid::Uuid,
    /// Soft cap per fetch.
    pub max_bytes: u32,
    /// Idle poll interval once caught up to the high watermark.
    pub poll_interval: Duration,
}

/// Handle to a running observer. Holds the image watch and the background
/// fetch task.
pub struct MetadataObserver {
    image: watch::Sender<Arc<MetadataImage>>,
    leader: watch::Sender<Option<NodeId>>,
    shutdown: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl MetadataObserver {
    /// Start the observer loop. The image watch begins at an empty image
    /// for `cluster_id`; callers subscribe via [`Self::watch_image`].
    #[must_use]
    pub fn start(config: ObserverConfig) -> Arc<Self> {
        let (image_tx, _) = watch::channel(Arc::new(MetadataImage::new(config.cluster_id)));
        let (leader_tx, _) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let observer = Arc::new(Self {
            image: image_tx,
            leader: leader_tx,
            shutdown: shutdown.clone(),
            task: tokio::sync::Mutex::new(None),
        });
        let task = tokio::spawn(run_loop(config, observer.clone(), shutdown));
        // Stash the handle for clean shutdown.
        if let Ok(mut guard) = observer.task.try_lock() {
            *guard = Some(task);
        }
        observer
    }

    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.image.borrow().clone()
    }

    #[must_use]
    pub fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image.subscribe()
    }

    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.subscribe()
    }

    /// Stop the fetch loop and drain the task.
    pub async fn cancel(&self) {
        self.shutdown.cancel();
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
    }
}

/// One iteration: fetch from `addr` at `fetch_offset`, decode + apply, and
/// return the new fetch offset (or `None` on a transport error so the
/// caller fails over).
async fn fetch_once(
    config: &ObserverConfig,
    addr: SocketAddr,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> Option<u64> {
    let req = crabka_raft::CrabkaMetadataFetchRequest {
        fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
        max_bytes: i32::try_from(config.max_bytes).unwrap_or(i32::MAX),
    };
    let mut body = Vec::with_capacity(12);
    req.encode_v0(&mut body);

    let opts = crabka_client_core::ConnectionOptions {
        client_id: config.client_id.clone(),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = match config.dialer.dial(target, &addr.to_string(), opts).await {
        Ok(c) => c,
        Err(e) => {
            debug!(%addr, error = %e, "observer dial failed");
            return None;
        }
    };
    let resp_body = match conn
        .raw_request(
            crabka_raft::API_KEY_METADATA_FETCH,
            0,
            bytes::Bytes::from(body),
        )
        .await
    {
        Ok(b) => b,
        Err(e) => {
            debug!(%addr, error = %e, "observer fetch request failed");
            conn.close();
            return None;
        }
    };
    conn.close();

    let mut cur: &[u8] = &resp_body;
    let resp = match crabka_raft::CrabkaMetadataFetchResponse::decode_v0(&mut cur) {
        Ok(r) => r,
        Err(e) => {
            warn!(%addr, error = %e, "observer response decode failed");
            return None;
        }
    };
    if resp.error_code != 0 {
        // Responder can't serve; fail over (caller picks the next voter,
        // preferring leader_hint).
        return None;
    }

    // Decode batches, apply records onto a fresh image clone.
    let mut next: MetadataImage = (*image_tx.borrow()).clone();
    let mut new_offset = fetch_offset;
    let mut buf: &[u8] = &resp.records;
    while !buf.is_empty() {
        let batch = match RecordBatch::decode(&mut buf) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "observer batch decode failed");
                break;
            }
        };
        let index = u64::try_from(batch.base_offset.max(0)).unwrap_or(0);
        for r in &batch.records {
            match from_kafka_record(r) {
                Ok(rec) => {
                    // Mirror the state machine: validate then apply, skip
                    // on reject (committed entries should always validate,
                    // but a concurrent delete could race).
                    if let Err(e) = next.validate(&rec) {
                        warn!(error = %e, "observer skipped record failing validation");
                        continue;
                    }
                    next.apply(&rec);
                }
                Err(e) => warn!(error = %e, "observer failed to decode record"),
            }
        }
        new_offset = index + 1;
    }
    if new_offset != fetch_offset {
        let _ = image_tx.send_replace(Arc::new(next));
    }
    Some(new_offset.max(fetch_offset))
}

async fn run_loop(config: ObserverConfig, observer: Arc<MetadataObserver>, shutdown: CancellationToken) {
    let mut fetch_offset: u64 = 0;
    let mut target_idx: usize = 0;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        if config.voters.is_empty() {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        }
        let (target, addr) = config.voters[target_idx % config.voters.len()];
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, addr, target, fetch_offset, &observer.image) => r,
        };
        match result {
            Some(new_offset) => {
                let _ = observer.leader.send_replace(Some(target));
                if new_offset == fetch_offset {
                    // Caught up — idle poll.
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(config.poll_interval) => {}
                    }
                } else {
                    fetch_offset = new_offset;
                }
            }
            None => {
                // Transport error / not-serving — try the next voter.
                target_idx = target_idx.wrapping_add(1);
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(config.poll_interval) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, TopicRecord};
    use crabka_raft::{BootstrapMode, Controller, ControllerConfig};
    use tempfile::TempDir;
    use uuid::Uuid;

    #[tokio::test]
    async fn observer_replicates_committed_topic() {
        // 1. Start a single-node controller (the metadata quorum).
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(1, dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        let mut leader_rx = ctrl.watch_leader();
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
        let ctrl_addr = ctrl.voter_addr(1).expect("self addr");
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "observed".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })])
        .await
        .expect("submit");

        // 2. Start an observer pointed at the controller (PLAINTEXT path).
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(1, ctrl_addr)],
            dialer: Arc::new(crabka_raft::PlaintextDialer),
            client_id: "test-observer".into(),
            cluster_id: Uuid::nil(),
            max_bytes: 1_048_576,
            poll_interval: Duration::from_millis(50),
        });

        // 3. Wait for the observer's image to carry the topic.
        let mut img_rx = observer.watch_image();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if img_rx.borrow().topic("observed").is_some() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("observer did not replicate topic within 5s");
            }
            let _ = tokio::time::timeout(Duration::from_millis(200), img_rx.changed()).await;
        }

        observer.cancel().await;
        ctrl.shutdown().await;
    }
}
```

- [ ] **Step 2: Confirm `crabka_raft` re-exports `PlaintextDialer`**

The test uses `crabka_raft::PlaintextDialer`. Check: `grep -n "PlaintextDialer" crates/raft/src/lib.rs`. If it is not re-exported, add `pub use network::PlaintextDialer;` to `crates/raft/src/lib.rs` (it is already `pub` in `network.rs`).

- [ ] **Step 3: Wire the module in**

In `crates/broker/src/lib.rs`, add (near other `mod` declarations):

```rust
pub mod metadata_observer;
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p crabka-broker observer_replicates_committed_topic`
Expected: PASS (the observer fetches index 1, decodes the topic, publishes the new image).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/metadata_observer.rs crates/broker/src/lib.rs crates/raft/src/lib.rs
git commit -m "feat(broker): MetadataObserver fetches and applies __cluster_metadata for broker-only nodes"
```

---

## Phase 5 — `MetadataSource` Abstraction + Broker Wiring

### Task 10: `MetadataSource` trait + impls

**Files:**
- Create: `crates/broker/src/metadata_source.rs`
- Modify: `crates/broker/src/lib.rs`

Handlers and `BrokerHandle` reach metadata exclusively through this trait. `ControllerHandle` (combined/controller nodes) and `ObserverSource` (broker-only nodes) both satisfy it, so no handler code changes — only the type of the `Broker.controller` field changes (Task 11).

The trait must cover every method currently called on `broker.controller` / `self._broker.controller`: `current_image`, `watch_image`, `watch_leader`, `quorum_state`, `submit_change`, `change_membership`, `add_learner`, `cancel`.

- [ ] **Step 1: Write the trait + impls**

Create `crates/broker/src/metadata_source.rs`:

```rust
//! `MetadataSource` — the metadata authority a broker reads from and
//! writes through. Combined/controller nodes back it with a live
//! `ControllerHandle` (openraft voter); broker-only nodes back it with a
//! `MetadataObserver` (true KRaft observer) plus a write-forwarding path
//! to the controller quorum. Handlers depend only on this trait.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;

use crabka_metadata::{MetadataImage, MetadataRecord};
use crabka_raft::{ControllerHandle, NodeId, QuorumState, RaftError};

use crate::metadata_observer::MetadataObserver;

#[async_trait::async_trait]
pub trait MetadataSource: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>>;
    fn quorum_state(&self) -> QuorumState;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError>;
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError>;
    async fn add_learner(&self, node_id: NodeId, addr: SocketAddr) -> Result<(), RaftError>;
    async fn cancel(&self);
}

#[async_trait::async_trait]
impl MetadataSource for ControllerHandle {
    fn current_image(&self) -> Arc<MetadataImage> {
        ControllerHandle::current_image(self)
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        ControllerHandle::watch_image(self)
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        ControllerHandle::watch_leader(self)
    }
    fn quorum_state(&self) -> QuorumState {
        ControllerHandle::quorum_state(self)
    }
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        ControllerHandle::submit_change(self, records).await
    }
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        ControllerHandle::change_membership(self, new_voters).await
    }
    async fn add_learner(&self, node_id: NodeId, addr: SocketAddr) -> Result<(), RaftError> {
        ControllerHandle::add_learner(self, node_id, addr).await
    }
    async fn cancel(&self) {
        ControllerHandle::cancel(self).await;
    }
}

/// Broker-only metadata source: reads from a [`MetadataObserver`], writes
/// by forwarding to the controller quorum.
pub struct ObserverSource {
    observer: Arc<MetadataObserver>,
    /// Forwarder for writes — a `ControllerHandle`-less submit path that
    /// dials the controller quorum. We reuse the observer's voter map +
    /// dialer via the `writer`.
    writer: Arc<dyn MetadataWriter>,
}

/// Write side for broker-only nodes: forward a batch to the controller
/// quorum leader.
#[async_trait::async_trait]
pub trait MetadataWriter: Send + Sync {
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError>;
}

impl ObserverSource {
    #[must_use]
    pub fn new(observer: Arc<MetadataObserver>, writer: Arc<dyn MetadataWriter>) -> Self {
        Self { observer, writer }
    }
}

#[async_trait::async_trait]
impl MetadataSource for ObserverSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.observer.current_image()
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.observer.watch_image()
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.observer.watch_leader()
    }
    fn quorum_state(&self) -> QuorumState {
        // A broker-only node is not a voter; surface a leader-only view
        // derived from the observer's leader watch. Per-voter progress is
        // unknown here (DescribeQuorum on a broker-only node forwards to a
        // controller in a later component).
        QuorumState {
            current_term: 0,
            last_applied_index: 0,
            current_leader: *self.observer.watch_leader().borrow(),
            voters: Vec::new(),
            per_voter_matched_index: std::collections::BTreeMap::new(),
        }
    }
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        self.writer.submit_change(records).await
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        // Quorum reconfiguration from a broker-only node is KIP-853 work
        // (a later component). For now it is not the leader.
        Err(RaftError::NotLeader { current_leader: None })
    }
    async fn add_learner(&self, _node_id: NodeId, _addr: SocketAddr) -> Result<(), RaftError> {
        Err(RaftError::NotLeader { current_leader: None })
    }
    async fn cancel(&self) {
        self.observer.cancel().await;
    }
}
```

- [ ] **Step 2: Implement a quorum-forwarding `MetadataWriter`**

Append to `crates/broker/src/metadata_source.rs` a concrete forwarder that dials the controller quorum and issues `API_KEY_SUBMIT_CHANGE` (mirroring `ControllerHandle::forward_submit_to`, but with no local raft — it always forwards):

```rust
use crabka_raft::OutboundDialer;

/// Forwards metadata writes from a broker-only node to the controller
/// quorum. Tries the leader hint first (from the observer), then walks the
/// voter list. Mirrors the `API_KEY_SUBMIT_CHANGE` request the controller
/// already serves.
pub struct QuorumForwarder {
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub dialer: Arc<dyn OutboundDialer>,
    pub client_id: String,
    pub leader: watch::Receiver<Option<NodeId>>,
}

impl QuorumForwarder {
    async fn try_submit(
        &self,
        target: NodeId,
        addr: SocketAddr,
        body: &[u8],
    ) -> Result<crabka_raft::CrabkaSubmitChangeResponse, RaftError> {
        let opts = crabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            ..crabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(target, &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(
                crabka_raft::API_KEY_SUBMIT_CHANGE,
                0,
                bytes::Bytes::copy_from_slice(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();
        let mut cur: &[u8] = &resp_body;
        crabka_raft::CrabkaSubmitChangeResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }
}

#[async_trait::async_trait]
impl MetadataWriter for QuorumForwarder {
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        let payload = <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(&records)
            .map_err(RaftError::from)?;
        let req = crabka_raft::CrabkaSubmitChangeRequest {
            records: bytes::Bytes::from(payload),
        };
        let mut body = Vec::with_capacity(req.records.len() + 4);
        req.encode_v0(&mut body).map_err(RaftError::Protocol)?;

        // Order targets: leader hint first, then the rest of the voters.
        let hint = *self.leader.borrow();
        let mut order: Vec<(NodeId, SocketAddr)> = Vec::new();
        if let Some(l) = hint {
            if let Some(t) = self.voters.iter().find(|(id, _)| *id == l) {
                order.push(*t);
            }
        }
        for v in &self.voters {
            if Some(v.0) != hint {
                order.push(*v);
            }
        }

        let mut last_err = RaftError::NotLeader { current_leader: hint };
        for (target, addr) in order {
            match self.try_submit(target, addr, &body).await {
                Ok(resp) if resp.error_code == 0 => return Ok(()),
                Ok(resp) if resp.error_code == 2 => {
                    return Err(RaftError::Metadata(
                        crabka_metadata::MetadataError::InvalidRecord("validation failed"),
                    ));
                }
                Ok(resp) => {
                    // not leader / other — retarget using the hint.
                    last_err = RaftError::NotLeader {
                        current_leader: (resp.leader_hint >= 0)
                            .then(|| u64::try_from(resp.leader_hint).unwrap_or(0)),
                    };
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }
}
```

(Confirm the `crabka_raft` re-exports used here: `CrabkaSubmitChangeRequest`, `CrabkaSubmitChangeResponse`, `OutboundDialer` — they are already in the `pub use wire::{...}` / `pub use network::OutboundDialer` blocks. Confirm `RaftError::from` accepts a wincode error: `grep -n "impl From" crates/raft/src/error.rs` — `forward_submit_to` uses `RaftError::from` on the same wincode serialize error, so it exists.)

- [ ] **Step 3: Wire the module in**

In `crates/broker/src/lib.rs`, add:

```rust
pub mod metadata_source;
```

- [ ] **Step 4: Build to verify the trait + impls compile**

Run: `cargo build -p crabka-broker`
Expected: compiles clean. (The trait is not yet used by `Broker`; Task 11 swaps the field.)

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/metadata_source.rs crates/broker/src/lib.rs
git commit -m "feat(broker): MetadataSource trait with ControllerHandle + observer-backed impls"
```

---

### Task 11: Retype `Broker.controller` and branch startup on role

**Files:**
- Modify: `crates/broker/src/broker.rs`

This is the integration step. The `Broker.controller` field changes type from `Arc<crabka_raft::ControllerHandle>` to `Arc<dyn crate::metadata_source::MetadataSource>`. Because every method called on it (`current_image`, `submit_change`, `watch_image`, `watch_leader`, `quorum_state`, `change_membership`, `add_learner`, `cancel`) is on the trait, no handler files change. The `controller_cell` (used by `raft_handshake` for SCRAM lookups on the controller listener) keeps the concrete `Arc<ControllerHandle>` and is set only for controller-role nodes.

- [ ] **Step 1: Change the field type**

In `crates/broker/src/broker.rs`, the `Broker` struct field (~line 31):

```rust
    /// Metadata authority for this broker. For combined/controller nodes
    /// this is a live openraft `ControllerHandle`; for broker-only nodes
    /// it is an observer-backed source that fetches `__cluster_metadata`
    /// and forwards writes to the controller quorum. Handlers reach it via
    /// the `MetadataSource` trait, so the concrete backing is invisible to
    /// them.
    pub(crate) controller: Arc<dyn crate::metadata_source::MetadataSource>,
```

- [ ] **Step 2: Branch the controller/observer construction**

In `Broker::start` (around line 893–915), replace the unconditional `Controller::start` + field assignment with a role branch. Keep `controller_cell` set only when this node is a controller. The new code:

```rust
        let metadata: Arc<dyn crate::metadata_source::MetadataSource> = if config.is_controller() {
            let controller_cfg = crabka_raft::ControllerConfig {
                node_id: config.node_id,
                voters: config.controller_quorum_voters.clone(),
                controller_listen_addr: config.controller_listen_addr,
                log_dir: config.log_dir.join("__cluster_metadata"),
                election_timeout: config.controller_election_timeout,
                heartbeat_interval: config.controller_heartbeat_interval,
                client_id: format!("crabka-broker-{}-controller", config.broker_id),
                bootstrap_mode: config.bootstrap_mode,
                cluster_id: config.cluster_id,
                dialer: raft_dialer.clone(),
                handshake: handshake_opt,
            };
            let controller = Arc::new(
                crabka_raft::Controller::start(controller_cfg)
                    .await
                    .map_err(|e| BrokerError::Startup(e.to_string()))?,
            );
            let _ = controller_cell.set(controller.clone());
            controller as Arc<dyn crate::metadata_source::MetadataSource>
        } else {
            // Broker-only node: no openraft voter. Start the observer and a
            // write-forwarder to the controller quorum.
            let dialer = raft_dialer
                .clone()
                .expect("broker-only node requires a raft dialer");
            let observer = crate::metadata_observer::MetadataObserver::start(
                crate::metadata_observer::ObserverConfig {
                    voters: config.controller_quorum_voters.clone(),
                    dialer: dialer.clone(),
                    client_id: format!("crabka-broker-{}-observer", config.broker_id),
                    cluster_id: config.cluster_id.unwrap_or_else(uuid::Uuid::nil),
                    max_bytes: 1_048_576,
                    poll_interval: std::time::Duration::from_millis(100),
                },
            );
            let forwarder = crate::metadata_source::QuorumForwarder {
                voters: config.controller_quorum_voters.clone(),
                dialer,
                client_id: format!("crabka-broker-{}-writer", config.broker_id),
                leader: observer.watch_leader(),
            };
            Arc::new(crate::metadata_source::ObserverSource::new(
                observer,
                Arc::new(forwarder),
            )) as Arc<dyn crate::metadata_source::MetadataSource>
        };
        let controller = metadata;
```

Notes for the implementer:
- `raft_dialer` is currently `Some(Arc::new(InterBrokerDialer::new(...)))` (always `Some`). If it is unconditionally `Some`, the `.expect(...)` is safe; otherwise construct a `PlaintextDialer` fallback. Read lines 886–891 to confirm.
- `handshake_opt` is moved into the controller config — it is only consumed on the controller branch, which is correct (broker-only nodes don't run a controller listener).
- The rest of `start` already refers to the local binding `controller` (self-registration `controller.submit_change(...)`, `controller.watch_leader()`, bootstrap records). Those all resolve through the `MetadataSource` trait now. The `let controller = metadata;` alias preserves those references verbatim, minimizing the diff.

- [ ] **Step 3: Confirm the `controller_cell` type is unchanged**

`controller_cell` must stay `OnceCell<Arc<crabka_raft::ControllerHandle>>` (concrete) so `raft_handshake` SCRAM lookups still compile. Read its declaration (`grep -n "controller_cell" crates/broker/src/broker.rs`) and confirm it is set only inside the `is_controller()` branch now. Broker-only nodes leave it unset (correct: they have no controller listener to authenticate).

- [ ] **Step 4: Build the broker crate**

Run: `cargo build -p crabka-broker`
Expected: compiles clean. If any call site used a `ControllerHandle`-only method not on the trait, the compiler flags it — add that method to the `MetadataSource` trait (and both impls) and rebuild. (Per the audit, the only methods used are the eight already on the trait.)

- [ ] **Step 5: Run the existing broker test suite (combined-node regression)**

Run: `cargo test -p crabka-broker --lib`
Expected: all existing lib tests pass. Combined-role brokers (the default for every existing test) still construct a real `ControllerHandle` under the trait, so behavior is unchanged.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): broker-only nodes use observer metadata source instead of a controller"
```

---

## Phase 6 — Integration Test

### Task 12: Role-separated cluster integration test

**Files:**
- Create: `crates/broker/tests/role_separation_observer.rs`

End-to-end: a controller-only node forms the quorum; a broker-only node observes metadata via fetch; a `CreateTopics` against the broker-only node forwards to the controller and then propagates back to the broker's image; the broker-only node is not in the voter set.

- [ ] **Step 1: Read the existing multi-node broker integration test for the harness pattern**

Read `crates/broker/tests/quorum.rs` (the canonical in-process multi-node cluster harness) and its `crates/broker/tests/support` module. Copy the exact patterns it uses: `cluster_lock()` test-binary serialization, the `Vec<(BrokerHandle, BrokerConfig, TempDir)>` cluster shape, loopback port allocation, `wait_for_leader`, `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, the `#![cfg(not(target_os = "windows"))]` gate, and the 2-minute deadlines. **Do not invent config fields** — mirror the existing harness. Build each node's `BrokerConfig` the same way `quorum.rs` does, then set `roles` per node (`vec![NodeRole::Controller]` for the controller-only node, `vec![NodeRole::Broker]` for the broker-only node) and point the broker-only node's `controller_quorum_voters` at the controller's listener.

- [ ] **Step 2: Write the integration test**

Create `crates/broker/tests/role_separation_observer.rs`. Adapt the `BrokerConfig` construction to match the harness found in Step 1; the shape is:

```rust
//! Component B integration test: a controller-only node + a broker-only
//! observer. The observer replicates metadata via fetch (not openraft),
//! a CreateTopics forwarded through it lands on the controller and
//! propagates back, and the observer never joins the voter set.

use std::collections::BTreeSet;
use std::time::Duration;

use crabka_broker::config::NodeRole;

// NOTE: replace the config construction below with the exact helper used
// by the existing raft-cluster integration tests (Step 1). The assertions
// are the contract this test must enforce.

#[tokio::test]
async fn broker_only_node_observes_and_forwards() {
    // 1. Start a controller-only node (roles = [Controller], Bootstrap).
    //    Start a broker-only node (roles = [Broker], Join) whose
    //    controller_quorum_voters point at the controller's listener.
    //
    // 2. Wait for the broker-only node's metadata image to reflect the
    //    controller's bootstrap state (observer is fetching).
    //
    // 3. Create a topic by submitting through the broker-only node's
    //    handle (forwarded to the controller quorum).
    //
    // 4. Assert the topic appears in the broker-only node's image within a
    //    timeout (proves observer fetch propagation).
    //
    // 5. Assert the broker-only node is NOT in the controller's voter set
    //    (controller.quorum_state().voters does not contain the broker id).

    // ---- concrete assertions (fill in harness-specific setup) ----
    // let topic = "rolesep-observed";
    // broker_only.create_topic(topic, 1, 1).await.expect("create via broker-only");
    // wait_until(Duration::from_secs(10), || {
    //     broker_only.has_partition(topic, 0)
    // }).await;
    // let voters: BTreeSet<u64> = controller.quorum_voters().into_iter().collect();
    // assert!(!voters.contains(&broker_only_id));
    let _ = (NodeRole::Broker, Duration::from_secs(1), BTreeSet::<u64>::new());
}
```

The implementer must replace the commented scaffold with the real harness calls discovered in Step 1, keeping these **hard assertions**:
1. The broker-only node's image contains the created topic after forwarding (`has_partition` returns true within a timeout).
2. The created topic was committed by the controller (its `current_image` has it).
3. The broker-only node's id is absent from the controller's voter set (`quorum_state().voters`).

- [ ] **Step 3: Run the integration test**

Run: `cargo test -p crabka-broker --test role_separation_observer`
Expected: PASS — topic created via the broker-only node propagates to its image; the broker-only node is not a voter.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/role_separation_observer.rs
git commit -m "test(broker): role-separated cluster — observer fetch + write forwarding"
```

---

## Final Verification

### Task 13: Workspace check, clippy, fmt

**Files:** none (verification only)

- [ ] **Step 1: Full workspace build + test**

Run: `cargo test --workspace`
Expected: all tests pass.

- [ ] **Step 2: Clippy with warnings denied**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. Fix any `doc_markdown` (backtick `KRaft`, `__cluster_metadata`, type names in docs), `must_use`, or `too_many_lines` lints inline.

- [ ] **Step 3: Format check**

Run: `cargo fmt --all && cargo fmt --all --check`
Expected: clean (CI gates on `cargo fmt --check`).

- [ ] **Step 4: Commit any fixups**

```bash
git add -A
git commit -m "chore(component-b): clippy + fmt fixups"
```

---

## Notes for the Implementer

- **Field name `controller` retained intentionally.** The `Broker.controller` field is typed `Arc<dyn MetadataSource>` but keeps its name to avoid churning ~60 handler files and ~130 call sites. The trait name carries the abstraction; the field's doc comment explains the dual backing. This is a deliberate low-churn call, not an oversight.
- **Offset model.** The metadata-fetch offset is the openraft *log index*. One log entry = one record batch (`base_offset == index`, `last_offset_delta == 0`), possibly containing multiple `MetadataRecord`s. The observer advances `fetch_offset = last_batch.base_offset + 1`. `Blank`/`Membership` entries emit empty batches so the observer steps over them.
- **Snapshot/log-start reset (§4.4) is v1-minimal.** `log_start_offset` is surfaced but the observer always starts at 0 and reads forward; full snapshot-rebuild on falling behind `log_start_offset` is deferred (snapshots are themselves deferred in `state_machine.rs`). Do not add snapshot handling in this plan.
- **Combined nodes do not run the observer.** Only `is_controller() == false` nodes start `MetadataObserver`. Combined (`[Controller, Broker]`, the default) and controller-only nodes get metadata through the openraft state-machine apply path exactly as before.
- **No backwards-compat shims** (CLAUDE.md): the bridge wire format, the new RPC, and the trait are introduced cleanly with no V2-alongside-V1 variants or feature flags.
- **Parallelism (CLAUDE.md execution guidance):** Phases are largely sequential by dependency. Within Phase 1, Task 1 → Task 2 are sequential (same crate). Tasks 6 (wire.rs) and Task 3/4 (log_store.rs / metadata_fetch.rs) touch different files in `crates/raft` and can be dispatched in parallel, but Task 5 and Task 7 depend on them. Task 9 depends on Phases 1–3. Tasks 10–11 depend on Task 9. Default to sequential unless the per-task file sets are disjoint and dependency-free.
