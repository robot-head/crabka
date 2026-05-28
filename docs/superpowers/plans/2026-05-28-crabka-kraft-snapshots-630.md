# KRaft metadata snapshots (KIP-630) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Snapshot the `@metadata` Raft log per KIP-630 — persist the `MetadataImage` as a canonical `.checkpoint` artifact, compact the log behind it, let lagging controllers catch up via openraft InstallSnapshot, and serve snapshots over the public `FetchSnapshot` API (key 59).

**Architecture:** One on-disk artifact (`<offset>-<epoch>.checkpoint` + sibling `.meta` sidecar) is shared by openraft's internal catch-up and the api-59 handler. The `MetadataImage` serializes to a sequence of `MetadataRecord`s wrapped in Kafka `RecordBatch`es with header/footer control batches; record *values* use the existing bincode framing. openraft index maps 1:1 to Kafka offset, so a snapshot at applied index `N` has `end_offset = N + 1`.

**Tech Stack:** Rust, openraft 0.9 (storage-v2), `crabka-protocol` record codec, `crabka-log` segment store, tokio.

**Reference spec:** `docs/superpowers/specs/2026-05-28-crabka-kraft-snapshots-630-design.md`

---

## File structure

| File | Responsibility | Slice |
|------|----------------|-------|
| `crates/metadata/src/image.rs` | `to_records()` / `from_records()` — image ⇄ record sequence | S1 |
| `crates/raft/src/snapshot.rs` (new) | `SnapshotId`, file naming, `SnapshotWriter`, `SnapshotReader`, byte-range read | S1 |
| `crates/raft/src/lib.rs` | register `mod snapshot` | S1 |
| `crates/raft/src/state_machine.rs` | `build_snapshot`, `get_current_snapshot`, `begin_receiving_snapshot`, `install_snapshot`; snapshot dir | S2, S3 |
| `crates/raft/src/log_store.rs` | real `purge` → `truncate_to`; `last_purged` precision | S2 |
| `crates/raft/src/config.rs` | snapshot dir wiring; trigger config fields | S2, S4 |
| `crates/raft/src/wire.rs` | chunked `InstallSnapshot` request/response bodies | S3 |
| `crates/raft/src/server.rs` | dispatch `API_KEY_INSTALL_SNAPSHOT` → `raft.install_snapshot` | S3 |
| `crates/raft/src/network.rs` | outbound `RaftNetwork::install_snapshot` | S3 |
| `crates/raft/src/controller.rs` | background trigger task (bytes + interval) | S4 |
| `crates/broker/src/handlers/fetch_snapshot.rs` (new) | api-59 handler | S5 |
| `crates/broker/src/handlers/mod.rs` | register key 59 | S5 |

**Execution batches** (per CLAUDE.md): `[S1]` → `[S2]` → `[S3, S4, S5]` in parallel. The batch-3 file sets do not overlap (`wire/server/network/state_machine` vs `config/controller` vs `broker/handlers`). Within S2, `state_machine.rs` is also touched by S3, so S3 runs after S2.

---

## Slice S1 — Snapshot format (writer + reader)

Self-contained format layer. No openraft involvement. Foundation for all other slices.

### Task S1.1: `MetadataImage::to_records` / `from_records`

**Files:**
- Modify: `crates/metadata/src/image.rs`
- Test: inline `#[cfg(test)]` in `crates/metadata/src/image.rs`

- [ ] **Step 1: Write the failing test**

Add to the existing test module in `image.rs` (find `mod tests`; if absent, add one):

```rust
#[test]
fn to_records_from_records_round_trips() {
    use crate::{MetadataRecord, TopicRecord};
    use uuid::Uuid;

    let cid = Uuid::from_u128(7);
    let mut image = MetadataImage::new(cid);
    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: Uuid::from_u128(42),
        partitions: 3,
        replication_factor: 2,
    });
    image.apply(&topic);

    let records = image.to_records();
    let rebuilt = MetadataImage::from_records(cid, &records);
    assert_eq!(rebuilt, image);
}
```

If `MetadataImage` does not already derive `PartialEq`, add `#[derive(... PartialEq)]` to the struct (its fields are all `PartialEq` containers).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-metadata to_records_from_records_round_trips`
Expected: FAIL — `no method named to_records` / `no function from_records`.

- [ ] **Step 3: Implement `to_records` and `from_records`**

Add to `impl MetadataImage` in `image.rs`. `to_records` emits the minimal record sequence whose in-order `apply` reproduces the image; order matters (topics before their partitions, brokers before configs that reference them). Emit in dependency order:

```rust
/// Serialize the image into the record sequence whose in-order `apply`
/// reconstructs it. Used to write KRaft snapshots (KIP-630).
#[must_use]
pub fn to_records(&self) -> Vec<crate::MetadataRecord> {
    use crate::MetadataRecord as R;
    let mut out = Vec::new();
    for (_, b) in &self.brokers {
        out.push(R::V1BrokerRegistration(b.clone()));
    }
    for (node_id, kvs) in &self.broker_configs {
        for (k, v) in kvs {
            out.push(R::V1BrokerConfig(crate::BrokerConfigRecord {
                node_id: *node_id,
                config_name: k.clone(),
                config_value: Some(v.clone()),
            }));
        }
    }
    for (_, t) in &self.topics {
        out.push(R::V1Topic(t.clone()));
    }
    for (_, p) in &self.partitions {
        out.push(R::V1Partition(p.clone()));
    }
    for (topic, overrides) in &self.topic_configs {
        out.push(R::V1TopicConfig(crate::TopicConfigRecord {
            topic: topic.clone(),
            overrides: overrides.clone(),
        }));
    }
    for c in &self.scram_credentials_records() {
        out.push(R::V1ScramCredential(c.clone()));
    }
    for acl in self.all_acls() {
        out.push(R::V1AccessControlEntry(acl.clone()));
    }
    for q in &self.client_quota_records() {
        out.push(R::V1ClientQuota(q.clone()));
    }
    for t in &self.delegation_token_records() {
        out.push(R::V1DelegationToken(t.clone()));
    }
    out
}

/// Rebuild an image from a record sequence (the inverse of `to_records`).
#[must_use]
pub fn from_records(cluster_id: uuid::Uuid, records: &[crate::MetadataRecord]) -> Self {
    let mut image = Self::new(cluster_id);
    for r in records {
        image.apply(r);
    }
    image
}
```

NOTE for the implementer: the exact field names (`self.brokers`, `self.topics`, `self.partitions`, `self.topic_configs`, `self.broker_configs`, `self.scram_credentials`, `self.client_quotas`, `self.delegation_tokens`, `self.acls_literal`/`acls_prefixed`) are defined on `MetadataImage` (see `image.rs` struct, ~line 56). Add small private helpers (`scram_credentials_records`, `client_quota_records`, `delegation_token_records`) that reconstruct the original `*Record` from the stored map entries if the image stores a derived form rather than the raw record. If a map already stores the record verbatim, iterate it directly instead. Confirm each `apply` arm is the exact inverse of what you emit.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-metadata to_records_from_records_round_trips`
Expected: PASS.

- [ ] **Step 5: Add a multi-record-type round-trip test**

```rust
#[test]
fn to_records_round_trips_all_variants() {
    use crate::*;
    use uuid::Uuid;
    let cid = Uuid::from_u128(1);
    let mut image = MetadataImage::new(cid);
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(), topic_id: Uuid::from_u128(9), partitions: 1, replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "t".into(), partition: 0, leader: 1, replicas: vec![1], isr: vec![1],
        leader_epoch: 0, adding_replicas: vec![], removing_replicas: vec![],
    }));
    let rebuilt = MetadataImage::from_records(cid, &image.to_records());
    assert_eq!(rebuilt, image);
}
```

Run: `cargo test -p crabka-metadata to_records`
Expected: PASS (both tests). Extend coverage to every variant your image actually stores; if a variant has no read-back path on the image yet, leave a focused test asserting the variants you do support.

- [ ] **Step 6: Commit**

```bash
git add crates/metadata/src/image.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(metadata): MetadataImage to_records/from_records for snapshots (S1.1)"
```

### Task S1.2: `SnapshotId` + filename format

**Files:**
- Create: `crates/raft/src/snapshot.rs`
- Modify: `crates/raft/src/lib.rs` (add `mod snapshot;`)
- Test: inline in `crates/raft/src/snapshot.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/raft/src/snapshot.rs` with only:

```rust
//! KIP-630 metadata snapshot artifact: `<offset>-<epoch>.checkpoint`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotId {
    pub end_offset: i64,
    pub epoch: i32,
}

impl SnapshotId {
    pub(crate) fn file_name(self) -> String {
        format!("{:020}-{:010}.checkpoint", self.end_offset, self.epoch)
    }

    pub(crate) fn parse(name: &str) -> Option<Self> {
        let stem = name.strip_suffix(".checkpoint")?;
        let (off, ep) = stem.split_once('-')?;
        Some(Self {
            end_offset: off.parse().ok()?,
            epoch: ep.parse().ok()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_id_name_round_trips() {
        let id = SnapshotId { end_offset: 1847, epoch: 3 };
        assert_eq!(id.file_name(), "00000000000000001847-0000000003.checkpoint");
        assert_eq!(SnapshotId::parse(&id.file_name()), Some(id));
    }
}
```

Add `mod snapshot;` to `crates/raft/src/lib.rs` near the other `mod` declarations.

- [ ] **Step 2: Run test to verify it fails, then passes**

Run: `cargo test -p crabka-raft snapshot_id_name_round_trips`
Expected: PASS (this task's code already satisfies it; if it fails, fix the `format!` width). The point is to lock the naming before writer/reader depend on it.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/snapshot.rs crates/raft/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): SnapshotId + checkpoint filename format (S1.2)"
```

### Task S1.3: `SnapshotWriter` / `SnapshotReader` round-trip

**Files:**
- Modify: `crates/raft/src/snapshot.rs`
- Test: inline in `crates/raft/src/snapshot.rs`

The `.checkpoint` file is a concatenation of encoded `RecordBatch`es (use `RecordBatch::encode` from `crabka_protocol::records::owned`, `owned.rs:478`):
1. header control batch (one `Record`, key = control-type bytes, value = header payload),
2. one data batch whose `Record`s carry `value = bincode(MetadataRecord)`,
3. footer control batch.

Control record key layout (KIP-630 control-record shape): `i16 version (0)` + `i16 type` where type 3 = SNAPSHOT_HEADER, 4 = SNAPSHOT_FOOTER. Header value: `i16 version (0)` + `i64 last_contained_log_timestamp`. Footer value: `i16 version (0)`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn writer_reader_round_trips_image() {
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    let cid = Uuid::from_u128(5);
    let mut image = MetadataImage::new(cid);
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(), topic_id: Uuid::from_u128(2), partitions: 1, replication_factor: 1,
    }));

    let id = SnapshotId { end_offset: 10, epoch: 1 };
    let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).expect("serialize");

    let records = SnapshotReader::read_records(&bytes).expect("read records");
    let rebuilt = MetadataImage::from_records(cid, &records);
    assert_eq!(rebuilt, image);
    let _ = id;
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft writer_reader_round_trips_image`
Expected: FAIL — `SnapshotWriter`/`SnapshotReader` undefined.

- [ ] **Step 3: Implement writer + reader**

Add to `snapshot.rs`:

```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as _, Serialize as _};

use crabka_metadata::MetadataRecord;
use crabka_protocol::records::owned::{Record, RecordBatch};

const CTRL_VERSION: i16 = 0;
const CTRL_TYPE_HEADER: i16 = 3;
const CTRL_TYPE_FOOTER: i16 = 4;

pub(crate) struct SnapshotWriter;

impl SnapshotWriter {
    /// Serialize an image into the canonical `.checkpoint` byte stream:
    /// header control batch, one data batch of bincode `MetadataRecord`s,
    /// footer control batch.
    pub(crate) fn serialize(
        image: &crabka_metadata::MetadataImage,
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, crate::error::RaftError> {
        let mut out = BytesMut::new();

        // Header control batch (base_offset 0).
        let mut header_key = Vec::with_capacity(4);
        header_key.put_i16(CTRL_VERSION);
        header_key.put_i16(CTRL_TYPE_HEADER);
        let mut header_val = Vec::with_capacity(10);
        header_val.put_i16(CTRL_VERSION);
        header_val.put_i64(last_contained_log_timestamp);
        encode_control_batch(&mut out, 0, header_key, header_val);

        // Data batch (base_offset 1).
        let records = image.to_records();
        let data_records: Vec<Record> = records
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let payload = <SerdeCompat<MetadataRecord>>::serialize(r)?;
                Ok(Record {
                    offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
                    value: Some(Bytes::from(payload)),
                    ..Default::default()
                })
            })
            .collect::<Result<_, crate::error::RaftError>>()?;
        let last_delta = data_records.len().saturating_sub(1);
        let mut data_batch = RecordBatch {
            base_offset: 1,
            last_offset_delta: i32::try_from(last_delta).unwrap_or(0),
            records: data_records,
            ..Default::default()
        };
        data_batch
            .encode(&mut out)
            .map_err(crate::error::RaftError::from)?;

        // Footer control batch.
        let footer_base = 2 + i64::try_from(last_delta).unwrap_or(0);
        let mut footer_key = Vec::with_capacity(4);
        footer_key.put_i16(CTRL_VERSION);
        footer_key.put_i16(CTRL_TYPE_FOOTER);
        let mut footer_val = Vec::with_capacity(2);
        footer_val.put_i16(CTRL_VERSION);
        encode_control_batch(&mut out, footer_base, footer_key, footer_val);

        Ok(out.freeze())
    }
}

fn encode_control_batch(out: &mut BytesMut, base_offset: i64, key: Vec<u8>, value: Vec<u8>) {
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: 0,
        attributes: crabka_protocol::records::header::Attributes::default().with_control(true),
        records: vec![Record {
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        }],
        ..Default::default()
    };
    // encode never fails for in-memory buffers with valid records.
    let _ = batch.encode(out);
}

pub(crate) struct SnapshotReader;

impl SnapshotReader {
    /// Parse all data-batch records back into `MetadataRecord`s, skipping
    /// the header/footer control batches.
    pub(crate) fn read_records(
        bytes: &[u8],
    ) -> Result<Vec<MetadataRecord>, crate::error::RaftError> {
        let mut cur = bytes;
        let mut out = Vec::new();
        while cur.has_remaining() {
            let batch = RecordBatch::decode(&mut cur).map_err(crate::error::RaftError::from)?;
            if batch.attributes.is_control_batch() {
                continue;
            }
            for rec in &batch.records {
                if let Some(v) = &rec.value {
                    let r = <SerdeCompat<MetadataRecord>>::deserialize(v)
                        .map_err(crate::error::RaftError::from)?;
                    out.push(r);
                }
            }
        }
        Ok(out)
    }
}
```

If `RaftError` lacks a `From<RecordsError>` arm, add one in `crates/raft/src/error.rs` (match the existing `From<ProtocolError>` / `From<wincode...>` pattern there).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft writer_reader_round_trips_image`
Expected: PASS.

- [ ] **Step 5: Add an empty-image round-trip test**

```rust
#[test]
fn writer_reader_round_trips_empty_image() {
    use crabka_metadata::MetadataImage;
    use uuid::Uuid;
    let cid = Uuid::from_u128(0);
    let image = MetadataImage::new(cid);
    let bytes = SnapshotWriter::serialize(&image, 0).unwrap();
    let records = SnapshotReader::read_records(&bytes).unwrap();
    assert_eq!(MetadataImage::from_records(cid, &records), image);
}
```

Run: `cargo test -p crabka-raft writer_reader`
Expected: PASS (both).

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/snapshot.rs crates/raft/src/error.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): SnapshotWriter/Reader canonical checkpoint format (S1.3)"
```

### Task S1.4: byte-range read for FetchSnapshot serving

**Files:**
- Modify: `crates/raft/src/snapshot.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn byte_range_returns_expected_slice() {
    let buf = (0u8..=255).collect::<Vec<u8>>();
    let slice = SnapshotReader::byte_range(&buf, 10, 5);
    assert_eq!(slice, &buf[10..15]);
    // Position past EOF yields empty.
    assert!(SnapshotReader::byte_range(&buf, 1000, 5).is_empty());
    // max larger than remaining clamps to EOF.
    assert_eq!(SnapshotReader::byte_range(&buf, 250, 100), &buf[250..]);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft byte_range_returns_expected_slice`
Expected: FAIL — `byte_range` undefined.

- [ ] **Step 3: Implement**

```rust
impl SnapshotReader {
    /// Return up to `max` bytes starting at `position`. Clamps to EOF;
    /// returns empty if `position` is past the end.
    pub(crate) fn byte_range(bytes: &[u8], position: usize, max: usize) -> &[u8] {
        let start = position.min(bytes.len());
        let end = start.saturating_add(max).min(bytes.len());
        &bytes[start..end]
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft byte_range_returns_expected_slice`
Expected: PASS.

- [ ] **Step 5: Run the full raft crate test suite + clippy**

Run: `cargo test -p crabka-raft && cargo clippy -p crabka-raft -- -D warnings`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/snapshot.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): snapshot byte-range read for FetchSnapshot serving (S1.4)"
```

---

## Slice S2 — Snapshot generation + log truncation

Depends on S1. Wires `build_snapshot` / `get_current_snapshot` and turns `purge` into real log truncation.

### Task S2.1: thread the snapshot directory into the state machine

**Files:**
- Modify: `crates/raft/src/state_machine.rs`
- Modify: `crates/raft/src/controller.rs` (construct with dir)

- [ ] **Step 1: Add a `snapshot_dir` field + constructor arg**

In `state_machine.rs`, extend `CrabkaStateMachine`:

```rust
pub(crate) struct CrabkaStateMachine {
    image: watch::Sender<Arc<MetadataImage>>,
    last_applied: Mutex<Option<LogId<NodeId>>>,
    last_membership: Mutex<StoredMembership<NodeId, Node>>,
    cluster_id: Uuid,
    snapshot_dir: std::path::PathBuf,
}

impl CrabkaStateMachine {
    pub(crate) fn new(cluster_id: Uuid, snapshot_dir: std::path::PathBuf) -> Self {
        let initial = Arc::new(MetadataImage::new(cluster_id));
        let (image, _rx) = watch::channel(initial);
        Self {
            image,
            last_applied: Mutex::new(None),
            last_membership: Mutex::new(StoredMembership::default()),
            cluster_id,
            snapshot_dir,
        }
    }
}
```

In `controller.rs` `Controller::start`, change the construction (~line 435) to pass the metadata partition dir (same dir the log store uses):

```rust
let snapshot_dir = config.log_dir.join("@metadata-0");
let state_machine = Arc::new(CrabkaStateMachine::new(
    config.cluster_id.unwrap_or_else(Uuid::nil),
    snapshot_dir,
));
```

Update the existing `CrabkaStateMachine::new(...)` test call sites (the `apply_publishes_image_to_watcher` test) to pass `std::env::temp_dir()` or a `TempDir` path.

- [ ] **Step 2: Build to confirm signature change compiles**

Run: `cargo build -p crabka-raft`
Expected: PASS (after fixing call sites).

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/state_machine.rs crates/raft/src/controller.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "refactor(raft): give state machine a snapshot dir (S2.1)"
```

### Task S2.2: implement `build_snapshot` + sidecar meta

**Files:**
- Modify: `crates/raft/src/state_machine.rs`
- Test: inline in `crates/raft/src/state_machine.rs`

The sidecar `<id>.checkpoint.meta` stores bincode of `(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>)`. `build_snapshot` writes both files atomically (temp + rename) and returns `Snapshot { meta, snapshot: Box<Cursor<Vec<u8>>> }`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn build_snapshot_writes_checkpoint_and_meta() {
    use crabka_metadata::{MetadataRecord, TopicRecord};
    let dir = tempfile::TempDir::new().unwrap();
    let sm = Arc::new(CrabkaStateMachine::new(Uuid::nil(), dir.path().to_path_buf()));
    let log_id = LogId { leader_id: openraft::LeaderId::new(1, 1), index: 5 };
    sm.apply_entry(log_id, &AppData {
        records: vec![MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(), topic_id: Uuid::from_u128(1), partitions: 1, replication_factor: 1,
        })],
    }).await;

    let mut builder = sm.clone();
    let snap = builder.build_snapshot().await.expect("build");
    assert_eq!(snap.meta.last_log_id, Some(log_id));
    // end_offset = applied index + 1 = 6, epoch from term = 1.
    let entries = std::fs::read_dir(dir.path()).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(entries.iter().any(|n| n == "00000000000000000006-0000000001.checkpoint"));
    assert!(entries.iter().any(|n| n.ends_with(".checkpoint.meta")));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft build_snapshot_writes_checkpoint_and_meta`
Expected: FAIL — currently returns `snapshot_unsupported`.

- [ ] **Step 3: Implement `build_snapshot`**

Replace the stub `RaftSnapshotBuilder` impl. Use the `SnapshotId` epoch from the last-applied log id's term:

```rust
impl RaftSnapshotBuilder<TypeConfig> for Arc<CrabkaStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let last_applied = *self.last_applied.lock().await;
        let membership = self.last_membership.lock().await.clone();
        let image = self.current_image();

        let end_offset = last_applied.map_or(0, |l| i64::try_from(l.index).unwrap_or(i64::MAX) + 1);
        let epoch = last_applied
            .map_or(0, |l| i32::try_from(l.leader_id.term).unwrap_or(i32::MAX));
        let id = crate::snapshot::SnapshotId { end_offset, epoch };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let bytes = crate::snapshot::SnapshotWriter::serialize(&image, now_ms)
            .map_err(|e| io_storage_err(&e))?;

        let snapshot_id = format!("{}-{}", end_offset, epoch);
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: snapshot_id.clone(),
        };

        crate::snapshot::persist(&self.snapshot_dir, id, &bytes, &meta)
            .map_err(|e| io_storage_err(&e))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(io::Cursor::new(bytes.to_vec())),
        })
    }
}
```

Add a `persist` helper + an `io_storage_err` mapper. In `snapshot.rs`:

```rust
use std::path::Path;
use openraft::{SnapshotMeta, StorageError};

pub(crate) fn persist(
    dir: &Path,
    id: SnapshotId,
    bytes: &[u8],
    meta: &SnapshotMeta<crate::types::NodeId, crate::types::Node>,
) -> Result<(), crate::error::RaftError> {
    let ckpt = dir.join(id.file_name());
    write_atomic(&ckpt, bytes)?;
    let meta_bytes = <serde_wincode::SerdeCompat<
        SnapshotMeta<crate::types::NodeId, crate::types::Node>,
    >>::serialize(meta)?;
    write_atomic(&dir.join(format!("{}.meta", id.file_name())), &meta_bytes)?;
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), crate::error::RaftError> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).map_err(crabka_log::LogError::Io)?;
    std::fs::rename(&tmp, path).map_err(crabka_log::LogError::Io)?;
    Ok(())
}
```

In `state_machine.rs` add:

```rust
fn io_storage_err(e: &crate::error::RaftError) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(None, AnyError::new(e)).into()
}
```

NOTE: confirm `SnapshotMeta` implements `serde::Serialize`/`Deserialize` under the openraft `serde` feature (it does in 0.9). Confirm the `StorageIOError::write_snapshot` constructor name against the installed openraft 0.9 (the read variant `read_snapshot` is already used at `state_machine.rs:106`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft build_snapshot_writes_checkpoint_and_meta`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/state_machine.rs crates/raft/src/snapshot.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): build_snapshot writes canonical checkpoint + sidecar (S2.2)"
```

### Task S2.3: implement `get_current_snapshot`

**Files:**
- Modify: `crates/raft/src/state_machine.rs`, `crates/raft/src/snapshot.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn get_current_snapshot_loads_latest() {
    let dir = tempfile::TempDir::new().unwrap();
    let sm = Arc::new(CrabkaStateMachine::new(Uuid::nil(), dir.path().to_path_buf()));
    assert!(sm.clone().get_current_snapshot().await.unwrap().is_none());

    let log_id = LogId { leader_id: openraft::LeaderId::new(1, 1), index: 3 };
    sm.apply_entry(log_id, &AppData { records: vec![] }).await;
    let _ = sm.clone().build_snapshot().await.unwrap();

    let loaded = sm.clone().get_current_snapshot().await.unwrap().expect("some");
    assert_eq!(loaded.meta.last_log_id, Some(log_id));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft get_current_snapshot_loads_latest`
Expected: FAIL — returns `Ok(None)` unconditionally.

- [ ] **Step 3: Implement**

Add a `load_latest` helper in `snapshot.rs` that scans `dir` for `*.checkpoint` files, picks the highest `(end_offset, epoch)` via `SnapshotId::parse`, reads the `.checkpoint` bytes and the `.meta` sidecar:

```rust
pub(crate) fn load_latest(
    dir: &Path,
) -> Result<Option<(SnapshotId, Vec<u8>, SnapshotMeta<crate::types::NodeId, crate::types::Node>)>, crate::error::RaftError> {
    let mut best: Option<SnapshotId> = None;
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(crabka_log::LogError::Io(e).into()),
    };
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(id) = SnapshotId::parse(&name) {
            if best.is_none_or(|b| (id.end_offset, id.epoch) > (b.end_offset, b.epoch)) {
                best = Some(id);
            }
        }
    }
    let Some(id) = best else { return Ok(None) };
    let bytes = std::fs::read(dir.join(id.file_name())).map_err(crabka_log::LogError::Io)?;
    let meta_bytes = std::fs::read(dir.join(format!("{}.meta", id.file_name())))
        .map_err(crabka_log::LogError::Io)?;
    let meta = <serde_wincode::SerdeCompat<
        SnapshotMeta<crate::types::NodeId, crate::types::Node>,
    >>::deserialize(&meta_bytes)?;
    Ok(Some((id, bytes, meta)))
}
```

Then in `state_machine.rs`:

```rust
async fn get_current_snapshot(
    &mut self,
) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
    match crate::snapshot::load_latest(&self.snapshot_dir).map_err(|e| io_storage_err(&e))? {
        None => Ok(None),
        Some((_id, bytes, meta)) => Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(io::Cursor::new(bytes)),
        })),
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft get_current_snapshot_loads_latest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/state_machine.rs crates/raft/src/snapshot.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): get_current_snapshot loads latest checkpoint (S2.3)"
```

### Task S2.4: turn `purge` into real log truncation

**Files:**
- Modify: `crates/raft/src/log_store.rs`
- Test: inline in `crates/raft/src/log_store.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn purge_advances_log_start_offset() {
    let dir = TempDir::new().unwrap();
    let store = RaftLogStore::open(dir.path().to_path_buf()).await.unwrap();
    // Append 5 blank entries at indices 1..=5.
    let entries: Vec<Entry<TypeConfig>> = (1..=5u64).map(|i| Entry {
        log_id: LogId { leader_id: LeaderId::new(1, 1), index: i },
        payload: EntryPayload::<TypeConfig>::Blank,
    }).collect();
    store.append(entries).await.unwrap();

    // Purge through index 3.
    let purge_id = LogId { leader_id: LeaderId::new(1, 1), index: 3 };
    RaftLogStorage::purge(&mut store.clone_arc_for_test(), purge_id).await.unwrap();

    let state = RaftLogStorage::get_log_state(&mut store.clone_arc_for_test()).await.unwrap();
    assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(3));
}
```

NOTE: `RaftLogStore`'s trait impls are on `Arc<RaftLogStore>`. The store in `open` returns `RaftLogStore`; wrap in `Arc` in the test (`let store = Arc::new(...)`) and call `RaftLogStorage::purge(&mut store.clone(), ...)`. Drop the `clone_arc_for_test` shim — just use `Arc::new` + `.clone()`. Adjust the test accordingly.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft purge_advances_log_start_offset`
Expected: FAIL — `purge` is a no-op so `last_purged_log_id` stays `None`.

- [ ] **Step 3: Implement real `purge`**

Add a method on `RaftLogStore` and call it from the trait `purge`:

```rust
pub(crate) async fn purge_upto(&self, index: u64) -> Result<(), RaftError> {
    let mut cache = self.cache.lock().await;
    let mut log = self.log.lock().await;
    // Drop cached entries at or below `index`.
    cache.entries.retain(|&k, _| k > index);
    cache.last_purged = cache.last_purged.max(index);
    // Advance the on-disk log start offset past the snapshotted prefix.
    // truncate_to(offset) advances log_start_offset to `offset`; the
    // metadata offset == openraft index, so the first retained offset is
    // index + 1.
    let new_start = i64::try_from(index + 1).unwrap_or(i64::MAX);
    log.set_log_start_offset(new_start)?;
    Ok(())
}
```

In the trait impl, replace the no-op:

```rust
async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
    RaftLogStore::purge_upto(self, log_id.index)
        .await
        .map_err(|e| err_write(&e))
}
```

And fix `get_log_state`'s `last_purged_log_id` to reflect `last_purged` precisely (it already derives from `last_purged`; verify the `index = last_purged` mapping rather than `last_purged - 1` now that `purge_upto` stores the purged-through index directly):

```rust
let last_purged_log_id = (last_purged > 0).then(|| LogId {
    leader_id: openraft::LeaderId::new(0, 0),
    index: last_purged,
});
```

NOTE: confirm `Log::set_log_start_offset` deletes fully-superseded segments (per the log crate; `set_log_start_offset` at `log.rs:210`). If it only advances the marker without deleting segment files, additionally call `Log::delete_local_segments_through(new_start - 1)` (`log.rs:721`). Verify against the log crate's behavior and pick the call that both advances the start offset and frees disk.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft purge_advances_log_start_offset`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/log_store.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): purge truncates metadata log behind snapshot (S2.4)"
```

### Task S2.5: end-to-end restart-recovery integration test

**Files:**
- Create/modify: `crates/raft/tests/snapshot.rs`

- [ ] **Step 1: Write the test**

A single-node controller: submit several records, trigger a snapshot via `raft.trigger().snapshot()`, assert the `.checkpoint` exists and the log was truncated; shut down; restart in `Rejoin`; assert the recovered image matches.

```rust
use std::time::Duration;
use crabka_raft::{Controller, ControllerConfig};
use crabka_metadata::{MetadataRecord, TopicRecord};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn snapshot_then_restart_recovers_image() {
    let dir = TempDir::new().unwrap();
    let cid = Uuid::from_u128(123);

    {
        let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
        cfg.cluster_id = Some(cid);
        let ctrl = Controller::start(cfg).await.unwrap();
        // Wait for leadership.
        tokio::time::sleep(Duration::from_millis(800)).await;
        ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(), topic_id: Uuid::from_u128(9), partitions: 1, replication_factor: 1,
        })]).await.unwrap();

        ctrl.trigger_snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(ctrl.current_image().topic("t").is_some());
        ctrl.shutdown().await;
    }

    // Restart in Rejoin; the on-disk snapshot + log tail must rebuild "t".
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.cluster_id = Some(cid);
    cfg.bootstrap_mode = crabka_raft::BootstrapMode::Rejoin;
    let ctrl = Controller::start(cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(ctrl.current_image().topic("t").is_some());
    ctrl.shutdown().await;
}
```

- [ ] **Step 2: Add `ControllerHandle::trigger_snapshot`**

In `controller.rs`:

```rust
/// Force a snapshot now (used by tests and the S4 trigger task).
pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
    self.raft.trigger().snapshot().await
        .map_err(|e| RaftError::Openraft(format!("{e:?}")))
}
```

Also set openraft's snapshot policy so the engine does not auto-snapshot on its own log-count heuristic (S4 owns triggering). In `Controller::start`'s `openraft::Config` (~line 445):

```rust
snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(u64::MAX),
max_in_snapshot_log_to_keep: 0,
```

NOTE: confirm the `SnapshotPolicy` variant + `max_in_snapshot_log_to_keep` field names against openraft 0.9. `LogsSinceLast(u64::MAX)` effectively disables automatic snapshots; `max_in_snapshot_log_to_keep: 0` tells openraft to purge the log fully up to the snapshot's last_log_id after a snapshot, which is what invokes our `purge`.

- [ ] **Step 3: Run to verify it passes**

Run: `cargo test -p crabka-raft --test snapshot snapshot_then_restart_recovers_image`
Expected: PASS. If openraft does not call `purge` after a manual snapshot, verify `max_in_snapshot_log_to_keep` is 0 and that `applied_state`/`get_log_state` report consistent ids.

- [ ] **Step 4: Run full raft suite + clippy, then commit**

Run: `cargo test -p crabka-raft && cargo clippy -p crabka-raft -- -D warnings`

```bash
git add crates/raft/tests/snapshot.rs crates/raft/src/controller.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(raft): snapshot generation + restart recovery e2e (S2.5)"
```

---

## Slice S3 — InstallSnapshot (follower catch-up)

Depends on S2. Runs in batch 3 (parallel with S4, S5). Touches `state_machine.rs`, `wire.rs`, `server.rs`, `network.rs`, and a new test.

### Task S3.1: state-machine receive + install

**Files:**
- Modify: `crates/raft/src/state_machine.rs`
- Test: inline in `crates/raft/src/state_machine.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn install_snapshot_rebuilds_image() {
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    // Producer SM builds a snapshot.
    let src_dir = tempfile::TempDir::new().unwrap();
    let src = Arc::new(CrabkaStateMachine::new(Uuid::nil(), src_dir.path().to_path_buf()));
    let log_id = LogId { leader_id: openraft::LeaderId::new(1, 1), index: 4 };
    src.apply_entry(log_id, &AppData { records: vec![
        MetadataRecord::V1Topic(TopicRecord { name: "t".into(), topic_id: Uuid::from_u128(1), partitions: 1, replication_factor: 1 }),
    ]}).await;
    let snap = src.clone().build_snapshot().await.unwrap();

    // Fresh SM installs it.
    let dst_dir = tempfile::TempDir::new().unwrap();
    let dst = Arc::new(CrabkaStateMachine::new(Uuid::nil(), dst_dir.path().to_path_buf()));
    let mut dst_mut = dst.clone();
    let buf = dst_mut.begin_receiving_snapshot().await.unwrap();
    let _ = buf; // openraft fills this; here we install from the produced bytes.
    let data = Box::new(io::Cursor::new(snap.snapshot.into_inner()));
    dst_mut.install_snapshot(&snap.meta, data).await.unwrap();

    assert!(dst.current_image().topic("t").is_some());
    let (applied, _) = dst_mut.applied_state().await.unwrap();
    assert_eq!(applied, Some(log_id));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft install_snapshot_rebuilds_image`
Expected: FAIL — both methods return `snapshot_unsupported`.

- [ ] **Step 3: Implement**

```rust
async fn begin_receiving_snapshot(
    &mut self,
) -> Result<Box<io::Cursor<Vec<u8>>>, StorageError<NodeId>> {
    Ok(Box::new(io::Cursor::new(Vec::new())))
}

async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<NodeId, Node>,
    snapshot: Box<io::Cursor<Vec<u8>>>,
) -> Result<(), StorageError<NodeId>> {
    let bytes = snapshot.into_inner();
    let records = crate::snapshot::SnapshotReader::read_records(&bytes)
        .map_err(|e| io_storage_err(&e))?;
    let image = MetadataImage::from_records(self.cluster_id, &records);
    let _ = self.image.send_replace(Arc::new(image));
    *self.last_applied.lock().await = meta.last_log_id;
    *self.last_membership.lock().await = meta.last_membership.clone();

    // Persist so a restart after install finds the snapshot on disk.
    if let Some(id) = snapshot_id_from_meta(meta) {
        crate::snapshot::persist(&self.snapshot_dir, id, &bytes, meta)
            .map_err(|e| io_storage_err(&e))?;
    }
    Ok(())
}
```

Add a helper deriving `SnapshotId` from the meta's `last_log_id`:

```rust
fn snapshot_id_from_meta(
    meta: &SnapshotMeta<NodeId, Node>,
) -> Option<crate::snapshot::SnapshotId> {
    meta.last_log_id.map(|l| crate::snapshot::SnapshotId {
        end_offset: i64::try_from(l.index).unwrap_or(i64::MAX) + 1,
        epoch: i32::try_from(l.leader_id.term).unwrap_or(i32::MAX),
    })
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft install_snapshot_rebuilds_image`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/state_machine.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): install_snapshot rebuilds image from checkpoint (S3.1)"
```

### Task S3.2: chunked InstallSnapshot wire bodies

**Files:**
- Modify: `crates/raft/src/wire.rs`
- Test: inline `round_trip` in `crates/raft/src/wire.rs`

openraft 0.9's `RaftNetwork::install_snapshot` ships `InstallSnapshotRequest { vote, meta, offset, data, done }`. Replace the stub `CrabkaInstallSnapshotRequest` with a real body that carries these. `meta` is bincode of `SnapshotMeta`; `vote` is bincode of `Vote<NodeId>`.

- [ ] **Step 1: Replace the stub with a real request/response and a round-trip test**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaInstallSnapshotRequest {
    pub vote: Bytes,   // bincode Vote<NodeId>
    pub meta: Bytes,   // bincode SnapshotMeta<NodeId, Node>
    pub offset: i64,
    pub data: Bytes,
    pub done: bool,
}

impl CrabkaInstallSnapshotRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        put_len_prefixed(out, &self.vote)?;
        put_len_prefixed(out, &self.meta)?;
        out.put_i64(self.offset);
        put_len_prefixed(out, &self.data)?;
        out.put_u8(u8::from(self.done));
        Ok(())
    }
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        let vote = get_len_prefixed(buf)?;
        let meta = get_len_prefixed(buf)?;
        if buf.remaining() < 8 { return Err(ProtocolError::UnexpectedEof { needed: 8 - buf.remaining() }); }
        let offset = buf.get_i64();
        let data = get_len_prefixed(buf)?;
        if buf.remaining() < 1 { return Err(ProtocolError::UnexpectedEof { needed: 1 }); }
        let done = buf.get_u8() != 0;
        Ok(Self { vote, meta, offset, data, done })
    }
}
```

Add `put_len_prefixed`/`get_len_prefixed` helpers (i32 length + bytes; mirror `CrabkaSubmitChangeRequest`'s pattern). Keep `CrabkaInstallSnapshotResponse` but add a `vote: Bytes` (bincode `Vote<NodeId>`) alongside `error_code` so the leader can decode the follower's vote. Update the existing `install_snapshot_stub_round_trip` test to the new shape.

- [ ] **Step 2: Run round-trip test**

Run: `cargo test -p crabka-raft -- install_snapshot`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/wire.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): real InstallSnapshot wire bodies (S3.2)"
```

### Task S3.3: server dispatch + outbound network

**Files:**
- Modify: `crates/raft/src/server.rs`, `crates/raft/src/network.rs`

- [ ] **Step 1: Implement server-side dispatch**

Replace the `API_KEY_INSTALL_SNAPSHOT` arm in `server.rs::dispatch` to decode the request, reconstruct openraft's `InstallSnapshotRequest`, call `raft.install_snapshot(req)`, and encode the response:

```rust
API_KEY_INSTALL_SNAPSHOT => {
    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};
    let mut cur = body;
    let req = CrabkaInstallSnapshotRequest::decode_v0(&mut cur)?;
    let vote: openraft::Vote<NodeId> = <SerdeCompat<openraft::Vote<NodeId>>>::deserialize(&req.vote)?;
    let meta: openraft::SnapshotMeta<NodeId, Node> =
        <SerdeCompat<openraft::SnapshotMeta<NodeId, Node>>>::deserialize(&req.meta)?;
    let or_req = openraft::raft::InstallSnapshotRequest {
        vote, meta, offset: u64::try_from(req.offset).unwrap_or(0),
        data: req.data.to_vec(), done: req.done,
    };
    let resp = raft.install_snapshot(or_req).await
        .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
    let vote_bytes = <SerdeCompat<openraft::Vote<NodeId>>>::serialize(&resp.vote)?;
    let mut out = Vec::new();
    CrabkaInstallSnapshotResponse { error_code: 0, vote: Bytes::from(vote_bytes) }.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}
```

Add `NodeId`/`Node` imports to `server.rs`. NOTE: confirm `InstallSnapshotRequest` field types against openraft 0.9 (`offset: u64`, `data: Vec<u8>`, `done: bool`); adjust conversions if the installed version differs.

- [ ] **Step 2: Implement outbound `RaftNetwork::install_snapshot`**

Replace the stub in `network.rs`:

```rust
async fn install_snapshot(
    &mut self,
    rpc: InstallSnapshotRequest<TypeConfig>,
    _option: RPCOption,
) -> Result<InstallSnapshotResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>> {
    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};
    let conn = self.factory.connect(self.target, &self.addr).await
        .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
    let vote = <SerdeCompat<Vote<NodeId>>>::serialize(&rpc.vote)
        .map_err(|e| net_err(&e))?;
    let meta = <SerdeCompat<openraft::SnapshotMeta<NodeId, Node>>>::serialize(&rpc.meta)
        .map_err(|e| net_err(&e))?;
    let mut body = Vec::new();
    crate::wire::CrabkaInstallSnapshotRequest {
        vote: Bytes::from(vote), meta: Bytes::from(meta),
        offset: i64::try_from(rpc.offset).unwrap_or(i64::MAX),
        data: Bytes::from(rpc.data), done: rpc.done,
    }.encode_v0(&mut body).map_err(|e| net_err_proto(&e))?;
    let resp_body = conn.raw_request(crate::wire::API_KEY_INSTALL_SNAPSHOT, 0, Bytes::from(body))
        .await.map_err(|e| RPCError::Network(NetworkError::new(e)))?;
    let mut cur: &[u8] = &resp_body;
    let resp = crate::wire::CrabkaInstallSnapshotResponse::decode_v0(&mut cur).map_err(|e| net_err_proto(&e))?;
    let vote: Vote<NodeId> = <SerdeCompat<Vote<NodeId>>>::deserialize(&resp.vote).map_err(|e| net_err(&e))?;
    Ok(InstallSnapshotResponse { vote })
}
```

Add small `net_err` / `net_err_proto` mappers returning `RPCError::Network(NetworkError::new(...))` typed for the `InstallSnapshotError` error param. Add `API_KEY_INSTALL_SNAPSHOT` to the `wire::` import list in `network.rs`.

- [ ] **Step 3: Build**

Run: `cargo build -p crabka-raft`
Expected: PASS (fix any openraft type mismatches per the NOTEs).

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/server.rs crates/raft/src/network.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): wire InstallSnapshot RPC server + outbound (S3.3)"
```

### Task S3.4: multi-node catch-up integration test

**Files:**
- Modify: `crates/raft/tests/snapshot.rs`

- [ ] **Step 1: Write the test**

Bootstrap node 1, add node 2 as a learner, submit records, trigger a snapshot on the leader (which truncates its log), then verify node 2's image converges to include the snapshotted topic. Model the multi-node setup on `crates/raft/tests/single_node.rs` / any existing 3-node smoke test. Keep it a learner (no `change_membership`) to isolate the snapshot-install path.

```rust
#[tokio::test]
async fn lagging_learner_catches_up_via_snapshot() {
    // ... start node 1 (Bootstrap), start node 2 (Join), add_learner(2),
    // submit a topic on node 1, trigger_snapshot on node 1,
    // then poll node 2's current_image() until topic("t").is_some()
    // with a bounded timeout. Assert it converges.
}
```

- [ ] **Step 2: Run**

Run: `cargo test -p crabka-raft --test snapshot lagging_learner_catches_up_via_snapshot`
Expected: PASS. If the learner replicates via append-entries before the snapshot is needed, force the gap by triggering the snapshot + `purge` before adding the learner so its only path to the prefix is InstallSnapshot.

- [ ] **Step 3: Full suite + clippy, commit**

Run: `cargo test -p crabka-raft && cargo clippy -p crabka-raft -- -D warnings`

```bash
git add crates/raft/tests/snapshot.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(raft): lagging learner catches up via snapshot (S3.4)"
```

---

## Slice S4 — Kafka-faithful triggers

Depends on S2. Batch 3 (parallel). Touches `config.rs`, `controller.rs`, and the broker config surface.

### Task S4.1: trigger config fields

**Files:**
- Modify: `crates/raft/src/config.rs`

- [ ] **Step 1: Add fields with Kafka defaults**

Add to `ControllerConfig`:

```rust
/// KIP-630 `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
pub max_bytes_between_snapshots: u64,
/// KIP-630 `metadata.log.max.snapshot.interval.ms` (default 1 h; 0 = disabled).
pub max_snapshot_interval: Duration,
```

In `for_tests`, set `max_bytes_between_snapshots: 20 * 1024 * 1024` and `max_snapshot_interval: Duration::from_secs(3600)`. Update any other `ControllerConfig { .. }` literal construction sites (grep `ControllerConfig {`) to set the new fields.

- [ ] **Step 2: Build**

Run: `cargo build -p crabka-raft`
Expected: PASS after updating construction sites.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/src/config.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): snapshot trigger config fields (S4.1)"
```

### Task S4.2: background trigger task

**Files:**
- Modify: `crates/raft/src/controller.rs`
- Test: `crates/raft/tests/snapshot.rs`

The task wakes on an interval tick OR polls the metadata log byte growth since the last snapshot. On the leader, when either threshold is crossed, call `raft.trigger().snapshot()`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn byte_threshold_triggers_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.max_bytes_between_snapshots = 1; // trigger almost immediately
    cfg.max_snapshot_interval = Duration::from_secs(3600);
    let ctrl = Controller::start(cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    ctrl.submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(), topic_id: Uuid::from_u128(9), partitions: 1, replication_factor: 1,
    })]).await.unwrap();
    // Poll for a .checkpoint file to appear within a bounded window.
    let meta_dir = dir.path().join("@metadata-0");
    let mut found = false;
    for _ in 0..40 {
        if std::fs::read_dir(&meta_dir).into_iter().flatten().flatten()
            .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint")) { found = true; break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "expected an automatic snapshot");
    ctrl.shutdown().await;
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-raft --test snapshot byte_threshold_triggers_snapshot`
Expected: FAIL — no trigger task yet, no checkpoint appears.

- [ ] **Step 3: Implement the task**

In `Controller::start`, after the leader-pump task, spawn a snapshot trigger task. Track bytes via the openraft metrics `last_applied` index delta as a proxy, or read the metadata log byte size from disk. Simplest robust signal: compare `last_applied.index` against the index of the last snapshot, and trigger when `(bytes since) >= max_bytes_between_snapshots` or the interval elapses. Use the log dir size on disk for the byte estimate:

```rust
let raft_for_snap = raft.clone();
let shutdown_for_snap = shutdown.clone();
let meta_dir = config.log_dir.join("@metadata-0");
let max_bytes = config.max_bytes_between_snapshots;
let interval = config.max_snapshot_interval;
let snapshot_task = tokio::spawn(async move {
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    let mut last_snapshot_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
            () = shutdown_for_snap.cancelled() => break,
            _ = tick.tick() => {
                let m = raft_for_snap.metrics().borrow().clone();
                let is_leader = m.current_leader == Some(m.id);
                if !is_leader { continue; }
                let log_bytes = dir_log_bytes(&meta_dir);
                let interval_elapsed = interval > Duration::ZERO
                    && last_snapshot_at.elapsed() >= interval;
                if log_bytes >= max_bytes || interval_elapsed {
                    if raft_for_snap.trigger().snapshot().await.is_ok() {
                        last_snapshot_at = tokio::time::Instant::now();
                    }
                }
            }
        }
    }
});
```

Add a `dir_log_bytes` helper summing `.log` segment file sizes in the metadata dir, and store `snapshot_task` on `ControllerHandle` so `shutdown`/`cancel` drain it (mirror `leader_pump_task`). NOTE: `m.id` is the local node id in openraft's `RaftMetrics`; confirm the field name.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-raft --test snapshot byte_threshold_triggers_snapshot`
Expected: PASS.

- [ ] **Step 5: Add an interval-trigger test, then commit**

Add `interval_triggers_snapshot` (set `max_bytes` huge, `max_snapshot_interval` ~300ms, submit one record, expect a checkpoint). Run the suite + clippy.

```bash
git add crates/raft/src/controller.rs crates/raft/tests/snapshot.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): config-driven snapshot trigger task (S4.2)"
```

### Task S4.3: surface configs through the broker config

**Files:**
- Modify: broker config crate (grep for where `ControllerConfig` is constructed in `crates/broker`, e.g. `crates/broker/src/broker.rs`)

- [ ] **Step 1: Map broker config keys to `ControllerConfig`**

Find where the broker builds `ControllerConfig` and populate `max_bytes_between_snapshots` / `max_snapshot_interval` from the broker's config map, parsing `metadata.log.max.record.bytes.between.snapshots` and `metadata.log.max.snapshot.interval.ms`, defaulting to 20 MiB / 1 h when unset.

```rust
max_bytes_between_snapshots: cfg.get_u64("metadata.log.max.record.bytes.between.snapshots")
    .unwrap_or(20 * 1024 * 1024),
max_snapshot_interval: Duration::from_millis(
    cfg.get_u64("metadata.log.max.snapshot.interval.ms").unwrap_or(3_600_000)),
```

Match the existing config-accessor pattern in the broker (the exact accessor name differs; follow neighbours).

- [ ] **Step 2: Build the broker, run its tests, commit**

Run: `cargo build -p crabka-broker && cargo test -p crabka-broker --lib`

```bash
git add crates/broker
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): wire metadata snapshot configs to controller (S4.3)"
```

---

## Slice S5 — FetchSnapshot API (key 59)

Depends on S1 + S2. Batch 3 (parallel). Touches the broker handlers only.

### Task S5.1: expose the snapshot artifact to the broker

**Files:**
- Modify: `crates/raft/src/controller.rs` (read-access to the latest snapshot)

The handler needs (a) the available `SnapshotId` and (b) byte-range access. Add a method on `ControllerHandle` returning the latest snapshot's id + a byte slice for a requested position.

- [ ] **Step 1: Add `ControllerHandle::read_snapshot_range`**

```rust
pub struct SnapshotSlice {
    pub end_offset: i64,
    pub epoch: i32,
    pub total_size: i64,
    pub bytes: bytes::Bytes,
}

impl ControllerHandle {
    /// Read up to `max_bytes` of the latest metadata snapshot starting at
    /// `position`. `None` when no snapshot exists. Used by the api-59
    /// FetchSnapshot handler.
    pub fn read_snapshot_range(
        &self,
        position: i64,
        max_bytes: i32,
    ) -> Option<SnapshotSlice> {
        let dir = self.snapshot_dir.clone();
        let (id, bytes, _meta) = crate::snapshot::load_latest(&dir).ok().flatten()?;
        let pos = usize::try_from(position.max(0)).unwrap_or(0);
        let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let slice = crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
        Some(SnapshotSlice {
            end_offset: id.end_offset,
            epoch: id.epoch,
            total_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            bytes: bytes::Bytes::copy_from_slice(slice),
        })
    }
}
```

Store `snapshot_dir` on `ControllerHandle` (set it in `start` from `config.log_dir.join("@metadata-0")`). Re-export `SnapshotSlice` from `crates/raft/src/lib.rs`.

- [ ] **Step 2: Build + commit**

Run: `cargo build -p crabka-raft`

```bash
git add crates/raft/src/controller.rs crates/raft/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): expose snapshot byte-range read to broker (S5.1)"
```

### Task S5.2: FetchSnapshot handler

**Files:**
- Create: `crates/broker/src/handlers/fetch_snapshot.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (add `pub(crate) mod fetch_snapshot;`)
- Modify: `crates/broker/src/codes.rs` (add the three KIP-630 codes)
- Test: inline in the handler module

Model the handler on `describe_quorum.rs`: validate the topic is `__cluster_metadata`, partition 0; otherwise the partition row gets an error code. Use the generated `FetchSnapshotRequest`/`FetchSnapshotResponse` types.

**Concrete generated types** (from `crates/protocol/src/owned/fetch_snapshot_response.rs`):
- `FetchSnapshotResponse { throttle_time_ms: i32, error_code: i16, topics: Vec<TopicSnapshot>, node_endpoints: Vec<NodeEndpoint>, unknown_tagged_fields }`
- `TopicSnapshot { name: String, partitions: Vec<PartitionSnapshot>, .. }`
- `PartitionSnapshot { index: i32, error_code: i16, snapshot_id: SnapshotId, size: i64, position: i64, unaligned_records: crate::records::RecordsPayload, current_leader: LeaderIdAndEpoch, .. }` — note the field is `index` (not `partition`), and `unaligned_records` is a **`RecordsPayload`**, not `Bytes`.
- `SnapshotId { end_offset: i64, epoch: i32, .. }`, `LeaderIdAndEpoch { leader_id: i32, leader_epoch: i32, .. }`

**Critical — `unaligned_records` must be `RecordsPayload::Legacy(bytes)`, not `from_bytes`.** `RecordsPayload::from_bytes` peeks the magic byte at offset 16 and, when it looks like a v2 batch, decodes exactly **one** `RecordBatch` and drops trailing bytes. A snapshot is *multiple* concatenated batches (header control + data + footer), and a paged byte-range slice is by design **not** batch-aligned (that's what "unaligned" means in KIP-630). Routing it through `from_bytes` would silently truncate the response to the first batch. Construct `crabka_protocol::records::RecordsPayload::Legacy(slice.bytes)` directly — its `encode_to` writes the held bytes verbatim, preserving exact snapshot bytes across pages.

**Step 0: Add the KIP-630 error codes to `codes.rs`** (canonical Apache Kafka `Errors.java` values):

```rust
/// `SNAPSHOT_NOT_FOUND` (98, KIP-630) — the requested `SnapshotId` is not
/// available on this node.
pub const SNAPSHOT_NOT_FOUND: i16 = 98;
/// `POSITION_OUT_OF_RANGE` (99, KIP-630) — the requested `position` is past
/// the end of the snapshot.
pub const POSITION_OUT_OF_RANGE: i16 = 99;
/// `INCONSISTENT_CLUSTER_ID` (104) — the request's `cluster_id` does not
/// match this cluster's id.
pub const INCONSISTENT_CLUSTER_ID: i16 = 104;
```

- [ ] **Step 1: Write the failing test**

`build_response` is a **pure** helper so it can be unit-tested without a live `Broker`. Signature:

```rust
fn build_response(
    local_cluster_id: uuid::Uuid,
    req: &FetchSnapshotRequest,
    resolve: &dyn Fn(i64, i32) -> Option<SnapshotSlice>,
) -> FetchSnapshotResponse
```

`resolve(position, max_bytes)` is the snapshot lookup; in `handle` it wraps `broker.controller.read_snapshot_range`, and in the test it returns a fixed slice. The test decodes/constructs a request and asserts the response carries the right `SnapshotId`, `size`/`position`, and verbatim records.

```rust
#[test]
fn build_response_serves_requested_range() {
    use crabka_protocol::owned::fetch_snapshot_request::{
        FetchSnapshotRequest, PartitionSnapshot as ReqPartition, SnapshotId as ReqSnapshotId,
        TopicSnapshot as ReqTopic,
    };
    use uuid::Uuid;
    let cid = Uuid::from_u128(7);
    // Request __cluster_metadata/0 at position 0. cluster_id None skips the
    // mismatch check; the mismatch path gets its own focused test.
    let req = FetchSnapshotRequest {
        replica_id: -1,
        max_bytes: 1024,
        topics: vec![ReqTopic {
            name: CLUSTER_METADATA_TOPIC.into(),
            partitions: vec![ReqPartition {
                partition: 0,
                current_leader_epoch: 0,
                snapshot_id: ReqSnapshotId { end_offset: 6, epoch: 1, ..Default::default() },
                position: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        cluster_id: None,
        ..Default::default()
    };
    let resolve = |_pos: i64, _max: i32| Some(SnapshotSlice {
        end_offset: 6, epoch: 1, total_size: 100, bytes: bytes::Bytes::from_static(b"abc"),
    });
    let resp = build_response(cid, &req, &resolve);
    let part = &resp.topics[0].partitions[0];
    assert_eq!(resp.error_code, 0);
    assert_eq!(part.error_code, 0);
    assert_eq!(part.snapshot_id.end_offset, 6);
    assert_eq!(part.snapshot_id.epoch, 1);
    assert_eq!(part.size, 100);
    assert_eq!(part.position, 0);
    // unaligned_records preserves the bytes verbatim (Legacy variant).
    let mut buf = bytes::BytesMut::new();
    part.unaligned_records.encode_to(&mut buf).unwrap();
    assert_eq!(&buf[..], b"abc");
}
```

NOTE: match how Crabka renders `cluster_id` on the wire (the same `current_image().cluster_id` formatting `describe_quorum`/other handlers use) so the equality check in `build_response` is apples-to-apples.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker fetch_snapshot`
Expected: FAIL — module/function undefined.

- [ ] **Step 3: Implement the handler**

```rust
//! `FetchSnapshot` (api_key 59, KIP-630). Serves the latest metadata
//! snapshot for `__cluster_metadata` partition 0 from the controller's
//! canonical `.checkpoint` artifact, paged by `Position`.

use bytes::Bytes;
use crabka_protocol::owned::fetch_snapshot_request::FetchSnapshotRequest;
use crabka_protocol::owned::fetch_snapshot_response::{
    FetchSnapshotResponse, LeaderIdAndEpoch, PartitionSnapshot, SnapshotId, TopicSnapshot,
};
use crabka_protocol::records::RecordsPayload;
use crabka_protocol::{Decode, Encode};
use crabka_raft::SnapshotSlice;

use crate::broker::Broker;
use crate::codes::{INCONSISTENT_CLUSTER_ID, INVALID_TOPIC_EXCEPTION, SNAPSHOT_NOT_FOUND};
use crate::error::BrokerError;

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    _ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = FetchSnapshotRequest::decode(&mut cur, version)?;
    let max_bytes = req.max_bytes;
    let resolve = |position: i64, _max: i32| broker.controller.read_snapshot_range(position, max_bytes);
    let local_cluster_id = broker.controller.current_image().cluster_id;
    let resp = build_response(local_cluster_id, &req, &resolve);
    let mut out = bytes::BytesMut::new();
    resp.encode(&mut out, version)?;
    Ok(out.freeze())
}
```

`build_response(local_cluster_id, req, resolve)` iterates `req.topics`/`partitions`, building one `TopicSnapshot`/`PartitionSnapshot` per requested row:

- **`cluster_id` mismatch**: when `req.cluster_id` is `Some` and != `local_cluster_id` (formatted the same way Crabka renders it on the wire), set the **top-level** `FetchSnapshotResponse.error_code = INCONSISTENT_CLUSTER_ID` and return early with empty `topics`.
- **Non-metadata topic** (name != `__cluster_metadata` or `partition.index != 0`): partition row with `error_code = INVALID_TOPIC_EXCEPTION`, empty `unaligned_records` (`RecordsPayload::default()`), zeroed `snapshot_id`/`size`/`position`.
- **Metadata topic, partition 0**: call `resolve(partition.position, req.max_bytes)`:
  - `None` → partition row `error_code = SNAPSHOT_NOT_FOUND`.
  - `Some(slice)` → `error_code = 0`, `snapshot_id = SnapshotId { end_offset: slice.end_offset, epoch: slice.epoch, ..Default::default() }`, `size = slice.total_size`, `position = partition.position`, `unaligned_records = RecordsPayload::Legacy(slice.bytes)` (verbatim — see the critical note above), `current_leader = LeaderIdAndEpoch::default()` (populate `leader_id`/`leader_epoch` from `broker.controller.quorum_state()` if readily available, else leave default — the JVM client tolerates a default here on the snapshot path).

`read_snapshot_range` already clamps `position` past EOF to an empty slice, so a `Some(slice)` with empty `bytes` is a valid end-of-snapshot page (`error_code = 0`), not `POSITION_OUT_OF_RANGE`. The `POSITION_OUT_OF_RANGE` code is reserved for a future strict-bounds check and is intentionally **not** emitted here.

Set the top-level response `throttle_time_ms = 0`, `node_endpoints = vec![]`, `error_code = 0` (unless the cluster-id early-return above fired). Encode with the request's `version`.

NOTE on `handle` signature: the dispatcher wraps handlers as `HandlerFn` (a `fn` returning `BoxFuture`). Follow exactly how `describe_quorum::handle` is registered (it takes `ctx`); match the registration wrapper used for the `ctx`-taking handlers.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker fetch_snapshot`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/fetch_snapshot.rs crates/broker/src/handlers/mod.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): FetchSnapshot (api 59) handler (S5.2)"
```

### Task S5.3: register the handler + advertise api 59

**Files:**
- Modify: wherever `HandlerTable::register` is called during `Broker::start` (grep `\.register(`), and the `ApiVersions` advertised set.

- [ ] **Step 1: Register key 59**

Register key 59 by **copying the exact `describe_quorum` registration line** and changing only the api key (`55` → `59`) and the module (`describe_quorum` → `fetch_snapshot`). `fetch_snapshot::handle` has the same `(broker, version, correlation_id, req_bytes, ctx)` signature as `describe_quorum::handle`, so whatever closure/wrapper threads `ctx` into the `HandlerFn` for `describe_quorum` works verbatim here. Do not invent a new wrapper shape — mirror the neighbour so the `ctx` plumbing matches.

- [ ] **Step 2: Advertise 59 in ApiVersions**

Ensure `api_versions` includes key 59 with min 0 / max 1 (the generated `MIN_VERSION`/`MAX_VERSION`). Follow how `describe_quorum` (55) is advertised.

- [ ] **Step 3: Integration test — round-trip a full fetch**

Add a broker-level test (model on existing handler integration tests) that boots a broker, creates a topic (so the metadata image is non-empty), triggers a snapshot, then sends a `FetchSnapshot` request for `__cluster_metadata`/0 at position 0 and asserts the returned `unaligned_records`, when concatenated across paged requests, parse back via `SnapshotReader::read_records` into the expected records. If a full broker harness is heavy, assert at minimum that the handler returns `error_code 0` and non-empty `unaligned_records` after a snapshot exists.

- [ ] **Step 4: Run broker tests + clippy, commit**

Run: `cargo test -p crabka-broker && cargo clippy -p crabka-broker -- -D warnings`

```bash
git add crates/broker
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): register + advertise FetchSnapshot api 59 (S5.3)"
```

---

## Final verification (after all slices)

- [ ] `cargo test --workspace` passes.
- [ ] `cargo clippy --workspace -- -D warnings` clean.
- [ ] Manual: boot a single-node broker, create topics until the byte threshold trips, confirm a `.checkpoint` appears in `@metadata-0/` and old `.log` segments are deleted, restart, confirm topics survive.

## Notes on openraft 0.9 specifics to verify at implementation time

These are real API-shape checks, not deferred work — confirm against the version in `Cargo.lock` and adjust the shown code:
- `StorageIOError::write_snapshot` constructor signature (parallel to the already-used `read_snapshot`).
- `openraft::Config` fields `snapshot_policy` (`SnapshotPolicy::LogsSinceLast`) and `max_in_snapshot_log_to_keep`.
- `InstallSnapshotRequest` / `InstallSnapshotResponse` field types (`offset: u64`, `data: Vec<u8>`, `done: bool`, response `vote`).
- `RaftMetrics` field for the local node id (`id`) used by the leader check in the trigger task.
- `Raft::trigger().snapshot()` return type for the error mapping.
