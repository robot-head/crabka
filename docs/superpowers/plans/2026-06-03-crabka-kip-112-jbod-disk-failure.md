# KIP-112 JBOD Disk-Failure Completion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a runtime log-dir failure precisely fail the affected partitions over to a healthy replica (new leader from the surviving alive ISR, offline replica dropped from ISR), and shut a broker down when all its log dirs fail — via a focused KIP-858 directory-assignment slice.

**Architecture:** Each `log.dir` gets a stable UUID. The internal `PartitionRecord` gains a per-replica `directories` vector. Brokers report their replica's dir UUID to the controller (`AssignReplicasToDirs`, api key 73) and report offline dir UUIDs on the periodic `BrokerHeartbeat` (`offline_log_dirs`, already on the wire). The controller leader maps an offline dir UUID to exactly the affected partitions and elects new leaders, reusing the existing `compute_failover_changes` election/recovery policy. All-dirs-offline is a local shutdown trigger.

**Tech Stack:** Rust 2024, tokio, `crabka-metadata` (serde_wincode internal records + KIP-631 byte-exact `kraft_translate`), `crabka-protocol` (generated Kafka codecs), openraft-backed controller via `MetadataSource`.

**Design spec:** [docs/superpowers/specs/2026-06-03-crabka-kip-112-jbod-disk-failure-design.md](../specs/2026-06-03-crabka-kip-112-jbod-disk-failure-design.md)

**Conventions (read before starting):**
- Greenfield, no back-compat shims (CLAUDE.md): change schemas in place, no `#[serde(default)]`, no migration.
- Run `cargo fmt` before every commit; CI gates on `cargo fmt --check`.
- Clippy gate is `cargo clippy --workspace --all-targets -- -D warnings`.
- Tests use `assert2::assert`. Commit with inline identity:
  `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit`.
- Working dir for all commands: the worktree root
  `/Users/mattstone/git/crabka/.claude/worktrees/sharp-pare-6f61d3`. Use `git -C <root>` in subagents.

---

## File structure

**Created:**
- `crates/broker/src/log_dir_id.rs` — per-`log.dir` UUID persistence + a `path ↔ uuid` map.
- `crates/broker/src/handlers/assign_replicas_to_dirs.rs` — controller-side api-key-73 handler.
- `crates/broker/src/assign_dirs.rs` — broker-side: build + send `AssignReplicasToDirs` for locally-hosted partitions.
- `crates/broker/tests/jbod_disk_failure.rs` — integration tests (runtime offline flip, all-dirs shutdown).

**Modified:**
- `crates/metadata/src/records.rs` — add `PartitionRecord.directories`.
- `crates/metadata/src/kraft_translate.rs` — emit/decode `directories` at PartitionRecord v1.
- `crates/broker/src/leader_election.rs` — `compute_offline_dir_failover_changes` + carry `directories` in all reconstructions.
- `crates/broker/src/handlers/{alter_partition,broker_heartbeat,create_topics}.rs`, `reassignment.rs`, `leader_rebalance.rs`, `unclean_recovery.rs` — carry `directories`; wire failover + assignment-update.
- `crates/broker/src/handlers/mod.rs` — register handler 73.
- `crates/broker/src/api_catalog.rs` — advertise api 73.
- `crates/broker/src/heartbeat/client.rs` — send `offline_log_dirs`.
- `crates/broker/src/log_dir_status.rs` — fix stale module doc.
- `crates/broker/src/broker.rs` — build the dir-id map; thread it; all-dirs self-shutdown; test seam.
- `README.md`, `STATUS.md` — status flip + slice entry.

---

## Batch 1 — metadata field + dir identity (parallel: Task 1 ∥ Task 2; no file overlap)

### Task 1: Add `PartitionRecord.directories` and keep the workspace compiling

**Files:**
- Modify: `crates/metadata/src/records.rs:18-34` (struct + derive), `:281-293` (round-trip test)
- Modify (carry the field, keep build green): `crates/metadata/src/kraft_translate.rs:692-720,939-956`; `crates/broker/src/leader_election.rs` (every `PartitionRecord { … }`); `crates/broker/src/handlers/alter_partition.rs:187-196`; `crates/broker/src/handlers/broker_heartbeat.rs` (via `select_replacement_leader_for_shutdown` in leader_election); `crates/broker/src/handlers/create_topics.rs` (partition assignment build); `crates/broker/src/reassignment.rs`; `crates/broker/src/leader_rebalance.rs`; `crates/broker/src/unclean_recovery.rs`; any `PartitionRecord { … }` in test modules (e.g. `replicator_supervisor.rs` tests, `leader_election.rs` tests).

- [ ] **Step 1: Add the field + `Default` derive (so fresh-construction sites can use `..Default::default()`).**

In `crates/metadata/src/records.rs`, change the derive line and struct:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch. Bumped on every leader change.
    /// Older on-disk metadata is not migrated.
    pub leader_epoch: i32,
    /// Replicas being added in an in-flight reassignment. Empty when no
    /// reassignment in flight. KIP-455.
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed in an in-flight reassignment. Empty when
    /// no reassignment in flight. KIP-455.
    pub removing_replicas: Vec<NodeId>,
    /// KIP-858: the log-directory UUID hosting each replica, parallel to
    /// [`Self::replicas`] (same index order). `Uuid::nil()` is
    /// `DirectoryId.UNASSIGNED` — the owning broker has not yet reported
    /// its `AssignReplicasToDirs` for this replica. The controller maps a
    /// broker's failed-dir UUID to the partitions it must fail over by
    /// matching this against the broker's replica slot.
    pub directories: Vec<Uuid>,
}
```

- [ ] **Step 2: Extend the existing round-trip test to assert the new field round-trips.**

In `crates/metadata/src/records.rs`, `partition_record_round_trip`:

```rust
    #[test]
    fn partition_record_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::nil()],
        });
        assert!(round_trip(&r) == r);
    }
```

- [ ] **Step 3: Run the metadata test; expect a COMPILE error in `kraft_translate.rs` + the broker crate (missing field).**

Run: `cargo test -p crabka-metadata partition_record_round_trip 2>&1 | head -40`
Expected: compile errors `missing field `directories` in initializer of `PartitionRecord``.

- [ ] **Step 4: Fix every `PartitionRecord { … }` construction site so the workspace compiles.**

Find them: `git -C <root> grep -n "PartitionRecord {" -- 'crates/**/*.rs'` and `rg "PartitionRecord \{" crates/`.

Rule:
- **Copy sites** (building a new record from an existing `pr`/`part_rec`): add `directories: pr.directories.clone(),` (preserve the assignment across leader/ISR changes). This includes all branches in `leader_election.rs` (`compute_failover_changes`, `select_new_leader_for_partition`, `select_replacement_leader_for_shutdown`, the unclean branches), `handlers/alter_partition.rs:187`, `reassignment.rs`, `leader_rebalance.rs`, `unclean_recovery.rs`.
- **Fresh sites** (new partition with no dir knowledge yet — `create_topics.rs`, test fixtures): add `directories: vec![],` (or `..Default::default()` where the literal already omits other fields).
- **`kraft_translate.rs:946` (`partition_from_kraft`)**: add `directories: vec![],` for now — Task 4 implements the real decode.

Example for `leader_election.rs:76` (`compute_failover_changes`, clean-elect branch):

```rust
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: new_leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch + 1,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                }));
```

- [ ] **Step 5: Build the whole workspace; expect success.**

Run: `cargo build --workspace 2>&1 | tail -20`
Expected: `Finished`. If any `missing field directories` remains, fix it.

- [ ] **Step 6: Run metadata + broker unit tests for the touched modules.**

Run: `cargo test -p crabka-metadata && cargo test -p crabka-broker --lib leader_election`
Expected: PASS.

- [ ] **Step 7: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(metadata): add PartitionRecord.directories (KIP-858), carry through all reconstructions"
```

---

### Task 2: Per-`log.dir` UUID identity

**Files:**
- Create: `crates/broker/src/log_dir_id.rs`
- Modify: `crates/broker/src/lib.rs` (add `mod log_dir_id;`)

A `log.dir`'s id lives in `<dir>/meta.properties.json` under key `directory_id`
(same file `bootstrap::read_directory_id` reads). Today only the primary dir has
one; extra JBOD dirs have none. This module reads each dir's id, minting + persisting
one for any dir that lacks it, and exposes `path ↔ uuid`.

- [ ] **Step 1: Write the failing test (create the file with only tests + stubs).**

Create `crates/broker/src/log_dir_id.rs`:

```rust
//! Per-`log.dir` stable UUIDs (KIP-858 directory ids).
//!
//! Each configured `log.dir` carries a `directory_id` in its
//! `meta.properties.json`. The primary/metadata dir's id is minted by
//! `crabka format`; extra JBOD dirs are minted + persisted here on first
//! boot. The resulting `path -> uuid` map lets the broker stamp
//! `AssignReplicasToDirs` and `offline_log_dirs` with stable ids that the
//! controller can map back to partitions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use uuid::Uuid;

/// Immutable per-dir UUID table built once at startup.
#[derive(Clone, Debug, Default)]
pub struct LogDirIds {
    by_path: HashMap<PathBuf, Uuid>,
}

impl LogDirIds {
    /// Resolve (reading or minting) a stable UUID for every dir in
    /// `log_dirs`. A dir whose `meta.properties.json` already carries a
    /// `directory_id` keeps it; a dir without one (fresh JBOD disk) gets a
    /// new v4 UUID persisted into a `meta.properties.json` in that dir.
    pub fn resolve(log_dirs: &[PathBuf]) -> Self {
        let mut by_path = HashMap::new();
        for dir in log_dirs {
            let id = read_or_mint(dir);
            by_path.insert(dir.clone(), id);
        }
        Self { by_path }
    }

    #[must_use]
    pub fn id_for(&self, dir: &Path) -> Option<Uuid> {
        self.by_path.get(dir).copied()
    }

    /// All `(path, uuid)` pairs, sorted by path for deterministic output.
    #[must_use]
    pub fn entries(&self) -> Vec<(PathBuf, Uuid)> {
        let mut v: Vec<_> = self
            .by_path
            .iter()
            .map(|(p, u)| (p.clone(), *u))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// UUIDs of the supplied dirs, skipping any not in the table.
    #[must_use]
    pub fn ids_for(&self, dirs: &[PathBuf]) -> Vec<Uuid> {
        dirs.iter().filter_map(|d| self.id_for(d)).collect()
    }
}

/// Read `<dir>/meta.properties.json`'s `directory_id`, or mint + persist a
/// fresh one. On any IO/parse failure minting still returns a stable
/// in-memory id (best-effort: the partition stays usable; only the
/// faithful-wire reporting degrades).
fn read_or_mint(dir: &Path) -> Uuid {
    let path = dir.join("meta.properties.json");
    if let Ok(bytes) = std::fs::read(&path)
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(id) = v["directory_id"].as_str().and_then(|s| s.parse().ok())
    {
        return id;
    }
    let id = Uuid::new_v4();
    // Persist, merging into any existing object so we don't clobber a
    // cluster_id/version written by `crabka format`.
    let mut obj = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    obj.insert("directory_id".into(), serde_json::json!(id.to_string()));
    obj.entry("version").or_insert(serde_json::json!(1));
    if let Ok(serialized) = serde_json::to_vec_pretty(&serde_json::Value::Object(obj)) {
        let _ = std::fs::create_dir_all(dir);
        let _ = std::fs::write(&path, serialized);
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use tempfile::tempdir;

    #[test]
    fn mints_and_persists_for_dir_without_meta() {
        let tmp = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        let first = ids.id_for(tmp.path()).expect("minted");
        assert!(tmp.path().join("meta.properties.json").exists());
        // Second resolve reads the persisted id back — stable across boots.
        let ids2 = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        assert!(ids2.id_for(tmp.path()) == Some(first));
    }

    #[test]
    fn reads_existing_directory_id_without_clobbering_siblings() {
        let tmp = tempdir().unwrap();
        let id = Uuid::from_u128(0xABCD);
        std::fs::write(
            tmp.path().join("meta.properties.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "cluster_id": "c-1",
                "directory_id": id.to_string(),
                "version": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let ids = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        assert!(ids.id_for(tmp.path()) == Some(id));
        // cluster_id preserved.
        let v: serde_json::Value = serde_json::from_slice(
            &std::fs::read(tmp.path().join("meta.properties.json")).unwrap(),
        )
        .unwrap();
        assert!(v["cluster_id"] == "c-1");
    }

    #[test]
    fn distinct_dirs_get_distinct_ids() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[a.path().to_path_buf(), b.path().to_path_buf()]);
        assert!(ids.id_for(a.path()) != ids.id_for(b.path()));
        assert!(ids.entries().len() == 2);
    }
}
```

- [ ] **Step 2: Register the module.** In `crates/broker/src/lib.rs`, add `mod log_dir_id;` near the other `mod` declarations (alphabetical with `log_dir`, `log_dir_status`). Make it `pub mod log_dir_id;` if the integration test needs it later (it will — keep it `pub`).

- [ ] **Step 3: Run the tests; expect PASS.**

Run: `cargo test -p crabka-broker --lib log_dir_id`
Expected: 3 tests PASS.

- [ ] **Step 4: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(broker): per-log-dir stable UUIDs (KIP-858 directory ids)"
```

---

## Batch 2 — KRaft round-trip + registry wiring (parallel: Task 3 ∥ Task 4; Task 3 needs Task 1, Task 4 needs Task 2)

### Task 3: Emit/decode `directories` at PartitionRecord v1 in `kraft_translate`

**Files:**
- Modify: `crates/metadata/src/kraft_translate.rs:410-421` (per-record emit version), `:692-720` (`partition_to_kraft`), `:939-956` (`partition_from_kraft`)
- Test: add a snapshot round-trip test in `crates/metadata/src/kraft_translate.rs` tests module

The generated KRaft `PartitionRecord` encodes `directories` only at version ≥ 1
(`crates/protocol/generated/PartitionRecord.owned.rs:111-118,341-350`), but
`to_kraft_values` envelopes every record at apiVersion 0 (`:417`). Emit partition
records at v1; keep all others at v0.

- [ ] **Step 1: Write the failing round-trip test.**

In the `kraft_translate.rs` tests module add (adjust imports to the module's existing style):

```rust
    #[test]
    fn partition_record_directories_survive_kraft_round_trip() {
        use crate::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
        let topic_id = uuid::Uuid::from_u128(0x11);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id,
            partitions: 1,
            replication_factor: 2,
        }));
        let rec = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            leader_epoch: 4,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![uuid::Uuid::from_u128(0xAA), uuid::Uuid::from_u128(0xBB)],
        });
        // Encode to KIP-631 value bytes, then decode back.
        let values = super::to_kraft_values(&rec, &image).expect("encode");
        assert!(values.len() == 1);
        let decoded = super::from_kraft_value(&values[0], &image).expect("decode");
        assert!(decoded == rec, "directories must survive the KRaft round trip");
    }
```

- [ ] **Step 2: Run it; expect FAIL (directories lost — decoded vec is empty).**

Run: `cargo test -p crabka-metadata partition_record_directories_survive_kraft_round_trip 2>&1 | tail -20`
Expected: FAIL (`decoded == rec` is false; decoded `directories` is `[]`).

- [ ] **Step 3: Populate `directories` in `partition_to_kraft`.**

Add a `directories` argument is unnecessary — `PartitionRecord` already carries it. Edit `partition_to_kraft` (`:706-719`) to map the UUIDs:

```rust
    Ok(KPartitionRecord {
        partition_id: p.partition,
        topic_id: to_kuuid(topic_id),
        replicas: cast(&p.replicas, "partition replicas")?,
        isr: cast(&p.isr, "partition isr")?,
        removing_replicas: cast(&p.removing_replicas, "partition removing_replicas")?,
        adding_replicas: cast(&p.adding_replicas, "partition adding_replicas")?,
        leader: i32::try_from(p.leader).map_err(|_| TranslateError::Invalid {
            field: "partition leader",
            detail: format!("leader {} exceeds i32", p.leader),
        })?,
        leader_epoch: p.leader_epoch,
        directories: p.directories.iter().map(|u| to_kuuid(*u)).collect(),
        ..Default::default()
    })
```

(`to_kuuid` already exists in this module — it converts `uuid::Uuid` → the protocol uuid; confirm its signature and reuse it.)

- [ ] **Step 4: Emit partition records at apiVersion 1.**

In `to_kraft_values` (`:410-421`), choose the envelope version per record. Replace the body:

```rust
pub fn to_kraft_values(
    rec: &MetadataRecord,
    image: &MetadataImage,
) -> Result<Vec<Bytes>, TranslateError> {
    to_kraft_records(rec, image)?
        .iter()
        .map(|kr| {
            // KIP-858: PartitionRecord carries `directories` only at v1+.
            // Every other modeled record still frames at the defaulted v0.
            let version = if matches!(kr, KraftMetadataRecord::Partition(_)) {
                1
            } else {
                0
            };
            kr.encode_value(version)
                .map_err(|e| TranslateError::Encode(e.to_string()))
        })
        .collect()
}
```

(Confirm the `KraftMetadataRecord` partition variant name — likely `Partition`. `git -C <root> grep -n "KraftMetadataRecord::" crates/metadata/src/kraft_translate.rs | head`.)

- [ ] **Step 5: Decode `directories` in `partition_from_kraft`.**

Edit `partition_from_kraft` (`:946-955`) to read them back:

```rust
    Ok(PartitionRecord {
        topic,
        partition: p.partition_id,
        leader: p.leader as u64,
        replicas: cast(&p.replicas),
        isr: cast(&p.isr),
        leader_epoch: p.leader_epoch,
        adding_replicas: cast(&p.adding_replicas),
        removing_replicas: cast(&p.removing_replicas),
        directories: p.directories.iter().map(|u| from_kuuid(*u)).collect(),
    })
```

(`from_kuuid` already exists — it's used for `topic_id` at `:943`.)

- [ ] **Step 6: Run the round-trip test + the full metadata suite.**

Run: `cargo test -p crabka-metadata 2>&1 | tail -25`
Expected: PASS, including the new test. If other `kraft_translate` golden/byte tests fail because partition records are now v1, update their expected bytes/version — that is the intended faithful change (note it in the commit message).

- [ ] **Step 7: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(metadata): emit/decode PartitionRecord.directories at KRaft v1 (snapshot round-trip)"
```

---

### Task 4: Dir-id map plumbed through the broker + broker registration `log_dirs`

**Files:**
- Modify: `crates/broker/src/log_dir_status.rs:1-22` (fix stale module doc only)
- Modify: `crates/broker/src/broker.rs` (build `LogDirIds` at startup near the probe `:1433`; store on the broker shared state; populate `BrokerRegistrationRequest`/record `log_dirs` at self-registration)

- [ ] **Step 1: Fix the stale doc comment** in `crates/broker/src/log_dir_status.rs`. Replace the module-doc bullet 3 and the "Only startup-time detection is wired up currently" paragraph (`:17-22`) with:

```rust
//! 3. Runtime write/fsync failures flip a dir online → offline mid-life:
//!    `crate::partition_writer::flag_storage_failure` calls
//!    [`LogDirRegistry::mark_offline`] on any `LogError::Io` from a
//!    partition mutation, so a disk that dies under live traffic is
//!    refused thereafter without restarting the broker.
//!
//! Both startup probing and runtime offline-flips are wired; the registry
//! is shared (`DashMap`) so a flip is visible immediately to every handler,
//! the heartbeat client (which reports offline dir UUIDs to the
//! controller), and JBOD placement.
```

- [ ] **Step 2: Build `LogDirIds` at startup and store it.**

In `crates/broker/src/broker.rs`, right after the probe at `:1433`:

```rust
        let log_dir_status = crate::log_dir_status::LogDirRegistry::probe(&config.all_log_dirs());
        // KIP-858: resolve a stable UUID per configured log.dir (minting +
        // persisting for any extra JBOD dir that lacks one). Shared with the
        // heartbeat client (offline_log_dirs) and the assignment reporter.
        let log_dir_ids = crate::log_dir_id::LogDirIds::resolve(&config.all_log_dirs());
```

Add a field `pub(crate) log_dir_ids: crate::log_dir_id::LogDirIds` to the `Broker` struct (near `log_dir_status` at `:126`) and set it in the struct initializer.

- [ ] **Step 3: Populate `log_dirs` on the broker's self-registration record.**

Find where the broker builds its own `BrokerRegistrationRecord` / `BrokerRegistrationRequest` (`git -C <root> grep -n "BrokerRegistrationRecord\|BrokerRegistrationRequest" crates/broker/src/`). At that site set the online dir UUIDs:

```rust
        log_dirs: log_dir_ids.ids_for(&log_dir_status.online_subset(&config.all_log_dirs()))
            .into_iter()
            .map(|u| crabka_protocol::primitives::uuid::Uuid(*u.as_bytes()))
            .collect(),
```

(Match the exact type at the site — the metadata `BrokerRegistrationRecord` in `records.rs` has no `log_dirs` field today; if registration flows only through the wire `BrokerRegistrationRequest`, set it there. If the record needs the field for the image, that is out of scope — `log_dirs` on the registration *request* is enough for faithfulness. Keep it minimal: only set the wire field if it already exists; otherwise skip this step and note it.)

- [ ] **Step 4: Build + targeted tests.**

Run: `cargo build -p crabka-broker 2>&1 | tail -15 && cargo test -p crabka-broker --lib log_dir_status`
Expected: builds; log_dir_status tests PASS.

- [ ] **Step 5: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(broker): build per-dir UUID map at startup; report log_dirs at registration; fix stale doc"
```

---

## Batch 3 — failover logic + assignment handler (parallel: Task 5 ∥ Task 6; both need Task 1)

### Task 5: `compute_offline_dir_failover_changes` (pure)

**Files:**
- Modify: `crates/broker/src/leader_election.rs` (new function near `compute_failover_changes:49`; tests in the existing `tests` module)

- [ ] **Step 1: Write the failing unit tests** in the `leader_election.rs` tests module (mirror the `compute_failover_changes` test helpers `img_with_partition` / `liveness_with_alive`, but set `directories`). Add a helper that sets directories:

```rust
    fn img_with_dirs(
        topic: &str,
        leader: NodeId,
        replicas: &[NodeId],
        isr: &[NodeId],
        dirs: &[uuid::Uuid],
    ) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: dirs.to_vec(),
        }));
        img
    }

    #[tokio::test]
    async fn offline_dir_elects_alive_isr_member_when_leader_dir_failed() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        // broker 1 is leader; its replica sits on dir `bad`. brokers 2,3 alive.
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[bad, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] { l.record_heartbeat(n).await; }
        let offline: std::collections::BTreeSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img, 1, &offline, &l, &crate::metrics::BrokerMetrics::new(),
        ).await;
        let pr = match &plan.changes[0] { MetadataRecord::V1Partition(p) => p, _ => panic!() };
        assert!(pr.leader == 2);
        assert!(pr.isr == vec![2, 3]); // broker 1 dropped
        assert!(pr.leader_epoch == 6);
    }

    #[tokio::test]
    async fn offline_dir_leaves_healthy_dir_partition_untouched() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        // broker 1 leads, but its replica is on the GOOD dir; only `bad` is offline.
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] { l.record_heartbeat(n).await; }
        let offline: std::collections::BTreeSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img, 1, &offline, &l, &crate::metrics::BrokerMetrics::new(),
        ).await;
        assert!(plan.changes.is_empty());
    }

    #[tokio::test]
    async fn offline_dir_shrinks_isr_for_non_leader_replica() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        // broker 2's replica on `bad`; broker 1 leads (healthy). 2 leaves ISR, no epoch bump.
        let img = img_with_dirs("t", 1, &[1, 2, 3], &[1, 2, 3], &[good, bad, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] { l.record_heartbeat(n).await; }
        let offline: std::collections::BTreeSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img, 2, &offline, &l, &crate::metrics::BrokerMetrics::new(),
        ).await;
        let pr = match &plan.changes[0] { MetadataRecord::V1Partition(p) => p, _ => panic!() };
        assert!(pr.leader == 1);
        assert!(pr.isr == vec![1, 3]);
        assert!(pr.leader_epoch == 5); // unchanged
    }

    #[tokio::test]
    async fn offline_dir_idempotent_after_failover() {
        let bad = uuid::Uuid::from_u128(0xDEAD);
        let good = uuid::Uuid::from_u128(0x1);
        // Post-failover image: leader already moved to 2, broker 1 out of ISR.
        let img = img_with_dirs("t", 2, &[1, 2, 3], &[2, 3], &[bad, good, good]);
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for n in [1u64, 2, 3] { l.record_heartbeat(n).await; }
        let offline: std::collections::BTreeSet<uuid::Uuid> = [bad].into_iter().collect();
        let plan = super::compute_offline_dir_failover_changes(
            &img, 1, &offline, &l, &crate::metrics::BrokerMetrics::new(),
        ).await;
        assert!(plan.changes.is_empty(), "no repeat change once broker 1 is no longer leader/ISR");
    }
```

- [ ] **Step 2: Run; expect FAIL (function not defined).**

Run: `cargo test -p crabka-broker --lib leader_election::tests::offline_dir 2>&1 | tail -20`
Expected: FAIL — `cannot find function compute_offline_dir_failover_changes`.

- [ ] **Step 3: Implement the function** in `crates/broker/src/leader_election.rs` (place it just after `compute_failover_changes`, factoring the shared election logic mentally — it mirrors the dead-broker scan but the trigger is "this broker's replica dir is offline" rather than "broker is dead"):

```rust
/// Compute failover changes for partitions whose replica on `broker` lives
/// on a now-offline log directory (`offline_uuids`). KIP-112: a broker stays
/// alive after a disk failure, so the dead-broker scan never fires — this
/// scan does, driven by the broker's `offline_log_dirs` heartbeat.
///
/// For each affected partition:
/// - if `broker` is the leader, elect a new leader from the alive ISR minus
///   `broker` (same clean / KIP-966 / KIP-841 policy as
///   [`compute_failover_changes`]), drop `broker` from ISR, bump epoch;
/// - if `broker` is a non-leader ISR member, drop it from ISR (no epoch bump).
///
/// Pure; idempotent (after the change `broker` is neither leader nor in ISR,
/// so a repeat yields an empty plan).
pub(crate) async fn compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &std::collections::BTreeSet<uuid::Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let alive = liveness.alive_snapshot().await;
    for (_, pr) in image.all_partitions() {
        // Is `broker`'s replica of this partition on an offline dir?
        let Some(slot) = pr.replicas.iter().position(|n| *n == broker) else {
            continue;
        };
        let on_offline = pr
            .directories
            .get(slot)
            .is_some_and(|d| offline_uuids.contains(d));
        if !on_offline {
            continue;
        }
        // Drop `broker` (its replica is gone) and any other dead replica.
        let mut alive_isr: Vec<NodeId> = Vec::with_capacity(pr.isr.len());
        for n in &pr.isr {
            if *n != broker && alive.contains(n) {
                alive_isr.push(*n);
            }
        }
        if pr.leader == broker {
            if let Some(&new_leader) = alive_isr.first() {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: new_leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch + 1,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                }));
            } else {
                // Empty ISR after dropping the offline replica — same recovery
                // policy as the dead-broker path.
                match resolve_recovery_strategy(image, &pr.topic) {
                    RecoveryStrategy::Balanced | RecoveryStrategy::Aggressive => {
                        recoveries.push((
                            pr.topic.clone(),
                            pr.partition,
                            resolve_recovery_strategy(image, &pr.topic),
                        ));
                    }
                    RecoveryStrategy::None if unclean_election_enabled(image, &pr.topic) => {
                        let mut elected: Option<NodeId> = None;
                        for &n in &pr.replicas {
                            if n != broker && alive.contains(&n) {
                                elected = Some(n);
                                break;
                            }
                        }
                        if let Some(new_leader) = elected {
                            warn!(
                                topic = %pr.topic, partition = pr.partition, leader = new_leader,
                                "offline-dir unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                            );
                            metrics.record_unclean_leader_election();
                            changes.push(MetadataRecord::V1Partition(PartitionRecord {
                                topic: pr.topic.clone(),
                                partition: pr.partition,
                                leader: new_leader,
                                replicas: pr.replicas.clone(),
                                isr: vec![new_leader],
                                leader_epoch: pr.leader_epoch + 1,
                                adding_replicas: pr.adding_replicas.clone(),
                                removing_replicas: pr.removing_replicas.clone(),
                                directories: pr.directories.clone(),
                            }));
                        }
                    }
                    RecoveryStrategy::None => {
                        warn!(
                            topic = %pr.topic, partition = pr.partition,
                            "offline dir on leader, no live ISR replica; partition unavailable"
                        );
                    }
                }
            }
        } else if alive_isr.len() < pr.isr.len() {
            changes.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                leader: pr.leader,
                replicas: pr.replicas.clone(),
                isr: alive_isr,
                leader_epoch: pr.leader_epoch,
                adding_replicas: pr.adding_replicas.clone(),
                removing_replicas: pr.removing_replicas.clone(),
                directories: pr.directories.clone(),
            }));
        }
    }
    FailoverPlan { changes, recoveries }
}
```

- [ ] **Step 4: Run; expect PASS.**

Run: `cargo test -p crabka-broker --lib leader_election 2>&1 | tail -20`
Expected: all `leader_election` tests PASS (new + existing).

- [ ] **Step 5: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(broker): compute_offline_dir_failover_changes — controller-side disk-failure failover (KIP-112)"
```

---

### Task 6: `AssignReplicasToDirs` controller handler (api key 73)

**Files:**
- Create: `crates/broker/src/handlers/assign_replicas_to_dirs.rs`
- Modify: `crates/broker/src/handlers/mod.rs:256` (register `73`), and add `mod assign_replicas_to_dirs;` to the handlers module list
- Modify: `crates/broker/src/api_catalog.rs` (advertise api 73)

First confirm the generated request/response shape:
`git -C <root> grep -n "pub " crates/protocol/generated/AssignReplicasToDirsRequest.owned.rs | head -40` and the `...Response.owned.rs`. The request is `{ broker_id, broker_epoch, directories: Vec<DirectoryData> }`, `DirectoryData { id: Uuid, topics: Vec<TopicData> }`, `TopicData { topic_id: Uuid, partitions: Vec<PartitionData> }`, `PartitionData { partition_index: i32 }` (KIP-858 shape — verify exact field names before writing).

- [ ] **Step 1: Write the handler** (mirror `handlers/alter_partition.rs`: leader-gated, resolve topic by id, build `V1Partition` changes, `submit_change`). For each reported `(topic_id, partition, dir_id)`, set the reporting broker's slot in `directories`:

```rust
//! `AssignReplicasToDirs` (`api_key=73`, KIP-858). A broker reports, for each
//! of its replicas, which log-directory UUID currently hosts it. The
//! controller records this in `PartitionRecord.directories[broker_slot]` so a
//! later `offline_log_dirs` heartbeat can be mapped back to exactly the
//! affected partitions for failover.
//!
//! Leader-only (`NOT_CONTROLLER` otherwise), mirroring `alter_partition`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
use crabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse;
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
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AssignReplicasToDirsRequest::decode(&mut cur, version)?;

        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == node_id);
        if !is_leader {
            let resp = AssignReplicasToDirsResponse {
                error_code: codes::NOT_CONTROLLER,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        let broker_slot_id = u64::try_from(req.broker_id).unwrap_or(u64::MAX);
        let image = controller.current_image();
        let mut changes: Vec<MetadataRecord> = Vec::new();

        for dir in &req.directories {
            let dir_uuid = uuid::Uuid::from_bytes(dir.id.0);
            for t in &dir.topics {
                let Some(topic_name) = image
                    .topics()
                    .find(|tr| tr.topic_id.as_bytes() == &t.topic_id.0)
                    .map(|tr| tr.name.clone())
                else {
                    continue;
                };
                for p in &t.partitions {
                    let Some(pr) = image.partition(&topic_name, p.partition_index) else {
                        continue;
                    };
                    let Some(slot) = pr.replicas.iter().position(|n| *n == broker_slot_id) else {
                        continue; // reporting broker isn't a replica — ignore
                    };
                    let mut directories = pr.directories.clone();
                    if directories.len() < pr.replicas.len() {
                        directories.resize(pr.replicas.len(), uuid::Uuid::nil());
                    }
                    if directories[slot] == dir_uuid {
                        continue; // already recorded — no-op, avoid churn
                    }
                    directories[slot] = dir_uuid;
                    changes.push(MetadataRecord::V1Partition(PartitionRecord {
                        topic: topic_name.clone(),
                        partition: pr.partition,
                        leader: pr.leader,
                        replicas: pr.replicas.clone(),
                        isr: pr.isr.clone(),
                        leader_epoch: pr.leader_epoch,
                        adding_replicas: pr.adding_replicas.clone(),
                        removing_replicas: pr.removing_replicas.clone(),
                        directories,
                    }));
                }
            }
        }

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        let resp = AssignReplicasToDirsResponse {
            error_code: codes::NONE,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

(Verify exact field names against the generated structs — `dir.id`, `dir.topics`, `t.topic_id`, `t.partitions`, `p.partition_index`, and the response's `error_code`/`throttle_time_ms`. Adjust if the codegen named them differently.)

- [ ] **Step 2: Register the handler + module.** In `crates/broker/src/handlers/mod.rs`: add `pub(crate) mod assign_replicas_to_dirs;` with the other handler modules, and `t.register(73, assign_replicas_to_dirs::handle);` next to `t.register(56, alter_partition::handle);` (`:256`).

- [ ] **Step 3: Advertise api 73** in `crates/broker/src/api_catalog.rs` — add `v!(assign_replicas_to_dirs_request),` to the controller/admin api list (find where `alter_partition_request` is listed; if it isn't, add it to `admin_apis()`).

- [ ] **Step 4: Add a handler unit test** (in the new file's `#[cfg(test)] mod tests`) that builds a single-broker image, calls the controller via a lightweight path, and asserts the directory slot is set. Simplest: factor the slot-update loop into a pure helper `fn assignment_changes(image, broker_id, &req) -> Vec<MetadataRecord>` and unit-test that helper directly (no network), mirroring how `alter_partition::handle_partition` is structured. Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};

    #[test]
    fn sets_reporting_brokers_directory_slot() {
        let topic_id = uuid::Uuid::from_u128(0x7);
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(), topic_id, partitions: 1, replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(), partition: 0, leader: 1,
            replicas: vec![1, 2], isr: vec![1, 2], leader_epoch: 0,
            adding_replicas: vec![], removing_replicas: vec![],
            directories: vec![uuid::Uuid::nil(), uuid::Uuid::nil()],
        }));
        let dir = uuid::Uuid::from_u128(0xAA);
        // broker 2 reports its replica is on `dir`.
        let changes = assignment_changes(&image, 2, topic_id, 0, dir);
        let pr = match &changes[0] { MetadataRecord::V1Partition(p) => p, _ => panic!() };
        assert!(pr.directories == vec![uuid::Uuid::nil(), dir]);
    }
}
```

Extract `assignment_changes(image, broker_id, topic_id, partition, dir_uuid) -> Vec<MetadataRecord>` as the per-assignment helper and have `handle` call it in the loop.

- [ ] **Step 5: Build + test.**

Run: `cargo test -p crabka-broker --lib assign_replicas_to_dirs && cargo build -p crabka-broker 2>&1 | tail -10`
Expected: PASS / builds.

- [ ] **Step 6: fmt + commit.**

```bash
cargo fmt
git -C <root> add -A
git -C <root> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "feat(broker): AssignReplicasToDirs handler (KIP-858 api key 73)"
```

---

## Batch 4 — wiring (sequential within batch; deps noted)

### Task 7: Broker reports dir assignment after materialization

**Files:**
- Create: `crates/broker/src/assign_dirs.rs`
- Modify: `crates/broker/src/replicator_supervisor.rs:218-257` (after the materialize loop, collect assignments and send); `crates/broker/src/broker.rs` (pass `log_dir_ids` into the supervisor constructor)
- Depends on: Task 4 (`log_dir_ids`), Task 6 (handler exists)

- [ ] **Step 1:** Add `assign_dirs.rs` with a function that, given the local partitions and their owning dirs + the `LogDirIds` map, builds an `AssignReplicasToDirsRequest` and sends it to the controller leader. Mirror `isr_maintenance::send_alter_partition:139-244` for the controller-leader address resolution and `crabka_client_core::Client` send. Signature:

```rust
pub(crate) async fn report_assignments(
    controller: &std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &crate::partition_registry::PartitionRegistry,
    log_dir_ids: &crate::log_dir_id::LogDirIds,
    broker_id: i32,
) -> Result<(), String>;
```

It iterates the partitions this broker hosts, reads each partition's owning dir
(`part.log_dir.load()`) → UUID via `log_dir_ids.id_for`, groups by `(dir_uuid, topic_id)`,
and sends one `AssignReplicasToDirs`. Group `topic_id` is resolved from
`controller.current_image()`. Skip partitions whose dir has no UUID.

- [ ] **Step 2:** Call it from the supervisor reconcile after the materialize loop (`:257`), but only report assignments that changed since the last reconcile (track a `DashMap<(String,i32), Uuid>` of last-reported dir on the supervisor to avoid re-sending every tick). On the first reconcile and after a KIP-113 swap (dir changes), the entry differs → report.

- [ ] **Step 3:** Add a unit test for the grouping/build logic (pure): given a set of `(topic, partition, dir_uuid)` and an image, assert the request groups partitions by dir + topic correctly. Keep the network send out of the unit test (test only the request builder).

- [ ] **Step 4:** `cargo test -p crabka-broker --lib assign_dirs && cargo build -p crabka-broker`. Expected PASS/builds.

- [ ] **Step 5: fmt + commit** (`feat(broker): report AssignReplicasToDirs on partition materialization`).

---

### Task 8: Heartbeat sends `offline_log_dirs`

**Files:**
- Modify: `crates/broker/src/heartbeat/client.rs:17-36` (Config), `:87-97` (request build)
- Modify: `crates/broker/src/broker.rs` (heartbeat client spawn `:1617` — pass `log_dir_status` + `log_dir_ids`)
- Depends on: Task 4

- [ ] **Step 1:** Add to `heartbeat::client::Config`:

```rust
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
```

- [ ] **Step 2:** In `run`, before building the request, compute the offline dir UUIDs:

```rust
        let offline_log_dirs: Vec<crabka_protocol::primitives::uuid::Uuid> = cfg
            .log_dir_status
            .offline()
            .into_iter()
            .filter_map(|(path, _reason)| cfg.log_dir_ids.id_for(&path))
            .map(|u| crabka_protocol::primitives::uuid::Uuid(*u.as_bytes()))
            .collect();
```

and set `offline_log_dirs` on the `BrokerHeartbeatRequest` literal (`:89-96`), replacing `..Default::default()` appropriately:

```rust
            .send(BrokerHeartbeatRequest {
                broker_id: cfg.broker_id,
                broker_epoch: 0,
                current_metadata_offset: 0,
                want_fence: false,
                want_shut_down,
                offline_log_dirs,
                ..Default::default()
            })
```

- [ ] **Step 3:** At the spawn site in `broker.rs:1617`, populate the two new Config fields with `log_dir_status.clone()` and `log_dir_ids.clone()`.

- [ ] **Step 4:** `cargo build -p crabka-broker`. Expected builds. (Behavioral assertion comes in the Task 11 integration test.)

- [ ] **Step 5: fmt + commit** (`feat(broker): heartbeat reports offline_log_dirs UUIDs (KIP-858)`).

---

### Task 9: Wire failover into the heartbeat handler

**Files:**
- Modify: `crates/broker/src/handlers/broker_heartbeat.rs:59-90`
- Depends on: Task 5

- [ ] **Step 1:** After recording the heartbeat / handling `want_shut_down` (`:65-76`), add the offline-dir failover scan:

```rust
        // KIP-112: a broker that reports offline log dirs is still alive, so
        // the dead-broker scan never fires. Map the offline dir UUIDs to the
        // reporting broker's affected partitions and fail them over.
        if !req.offline_log_dirs.is_empty() {
            let offline: std::collections::BTreeSet<uuid::Uuid> = req
                .offline_log_dirs
                .iter()
                .map(|u| uuid::Uuid::from_bytes(u.0))
                .collect();
            let image = controller.current_image();
            let plan = crate::leader_election::compute_offline_dir_failover_changes(
                &image,
                broker_id_u64,
                &offline,
                &liveness,
                &broker.metrics, // confirm the metrics handle on Broker; clone before the async move
            )
            .await;
            if !plan.changes.is_empty()
                && let Err(e) = controller.submit_change(plan.changes).await
            {
                tracing::warn!(error = %e, "offline-dir failover submit_change failed");
            }
            // Offset-aware recoveries (KIP-966), fire-and-forget — mirror on_broker_dead.
            // (Thread the UncleanRecoveryHandle into the handler if not already available;
            //  if it's not reachable here, log plan.recoveries and defer — note it.)
        }
```

Note: `broker.metrics` and a recovery handle may need to be cloned into the
async block alongside `liveness`/`controller` at the top of `handle` (mirror how
`liveness`/`controller` are cloned at `:30-31`). Confirm the `Broker` field name
for metrics (`git -C <root> grep -n "metrics" crates/broker/src/broker.rs | head`).

- [ ] **Step 2:** `cargo build -p crabka-broker && cargo test -p crabka-broker --lib broker_heartbeat`. Expected builds/PASS.

- [ ] **Step 3: fmt + commit** (`feat(broker): heartbeat handler fails over partitions on reported offline dirs (KIP-112)`).

---

### Task 10: All-log-dirs-offline → self-shutdown

**Files:**
- Modify: `crates/broker/src/heartbeat/client.rs` (after computing `offline_log_dirs`, detect all-offline and latch `want_shutdown`) OR `crates/broker/src/broker.rs` (a dedicated check). Prefer the heartbeat client since it already has `log_dir_status` + the cadence.
- Depends on: Task 8

- [ ] **Step 1:** In `heartbeat::client::run`, after building `offline_log_dirs`, compare against all configured dirs. Add a `pub all_log_dirs: Vec<PathBuf>` and `pub want_shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>` to `Config` (the client already holds the `should_shutdown` sender and a `want_shutdown` receiver; add the sender so it can self-latch). When every configured dir is offline:

```rust
        let all_offline = !cfg.all_log_dirs.is_empty()
            && cfg.all_log_dirs.iter().all(|d| cfg.log_dir_status.is_offline(d));
        if all_offline {
            tracing::error!(
                "all log dirs offline — initiating broker self-shutdown (KIP-112)"
            );
            let _ = cfg.want_shutdown_tx.send(true);
        }
```

Latching `want_shutdown` makes the existing controlled-shutdown drain run via the
next heartbeats; the broker's normal shutdown path then completes. (If the
single-broker case has no controller to drain against because its own metadata
dir is offline, also cancel `supervisor_shutdown` directly — thread that token in
and cancel it after latching. Confirm the cleanest trigger by reading
`broker.rs:790-851`.)

- [ ] **Step 2:** Thread the new Config fields at the spawn site (`broker.rs:1617`): `all_log_dirs: config.all_log_dirs()`, `want_shutdown_tx: want_shutdown.clone()` (the `Arc<watch::Sender<bool>>` at `broker.rs:110`).

- [ ] **Step 3:** `cargo build -p crabka-broker`. Expected builds. (Behavior verified in Task 12.)

- [ ] **Step 4: fmt + commit** (`feat(broker): self-shutdown when all log dirs go offline (KIP-112)`).

---

## Batch 5 — integration tests + docs

### Task 11: Test seam + runtime offline-flip integration test

**Files:**
- Modify: `crates/broker/src/broker.rs` (add a `#[cfg(any(test, feature = "test-helpers"))]` method on `BrokerHandle`)
- Create: `crates/broker/tests/jbod_disk_failure.rs`
- Depends on: Tasks 1–10

- [ ] **Step 1: Add the test seam** on `BrokerHandle` (mirror the existing test-only helpers noted at `broker.rs` — `test_truncate_local_log` etc.):

```rust
    /// Test-only: flip a configured log dir offline at runtime, as though a
    /// live fsync had failed. Drives the KIP-112 offline path without real
    /// EIO injection (unreliable cross-platform).
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_mark_log_dir_offline(&self, dir: &std::path::Path) -> bool {
        self._broker
            .log_dir_status
            .mark_offline(dir, "test-injected storage failure")
    }
```

- [ ] **Step 2: Write the integration test** `crates/broker/tests/jbod_disk_failure.rs` (model the harness on `crates/broker/tests/jbod.rs`: two-dir broker, create a topic, wait for partitions). Steps: start a two-dir broker, create a 1-partition topic placed on the `extra` dir (or create enough partitions that at least one lands on `extra`; assert which dir holds it), flip that dir offline via `handle.test_mark_log_dir_offline(extra_path)`, then assert a produce to that partition returns `KAFKA_STORAGE_ERROR` (error code 56). Use the raw-wire produce helper pattern from `jbod.rs`/`integration.rs`. Skeleton:

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

// ... imports mirroring tests/jbod.rs ...

#[tokio::test]
async fn produce_to_partition_on_offline_dir_returns_storage_error() {
    let (handle, _primary, extra, addr) = start_two_dir_broker().await;
    // create topic with enough partitions that at least one lands on `extra`,
    // wait for materialization, find a partition whose dir is `extra`.
    // ... (reuse jbod.rs helpers: create_topic, wait_all_partitions, count_topic_dirs) ...
    let flipped = handle.test_mark_log_dir_offline(extra.path());
    assert!(flipped);
    // produce to a partition known to be on `extra`; expect error_code == 56.
    let err = produce_one(addr, "t", partition_on_extra, b"hello").await;
    assert!(err == 56, "KAFKA_STORAGE_ERROR expected, got {err}");
    handle.shutdown().await;
}
```

Write `produce_one` as a minimal raw Produce v9 round-trip returning the
partition error code (copy the framing from `tests/jbod.rs::round_trip` + a
ProduceRequest from `crabka_protocol::owned::produce_request`). If pinning a
partition to `extra` is awkward, instead flip **all** dirs offline and assert any
produce returns 56 — simpler and still exercises the runtime path.

- [ ] **Step 3:** `cargo test -p crabka-broker --test jbod_disk_failure 2>&1 | tail -20`. Expected PASS.

- [ ] **Step 4: fmt + commit** (`test(broker): runtime offline-flip returns KAFKA_STORAGE_ERROR (KIP-112)`).

---

### Task 12: All-dirs-offline self-shutdown integration test

**Files:**
- Modify: `crates/broker/tests/jbod_disk_failure.rs`
- Depends on: Task 10, Task 11 (seam)

- [ ] **Step 1: Write the test:** start a single-dir broker, flip its only dir offline via the seam, and assert the broker shuts itself down within a timeout. Detect shutdown by awaiting the handle's shutdown signal or by polling that the listener stops accepting (whichever the harness exposes — check `BrokerHandle` for a shutdown-await / `should_shutdown` subscriber; if none is public, add a `#[cfg(any(test, feature = "test-helpers"))] pub fn test_should_shutdown_rx(&self) -> watch::Receiver<bool>` and assert it flips true).

```rust
#[tokio::test]
async fn all_dirs_offline_triggers_self_shutdown() {
    let primary = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    // single dir; keep heartbeat interval short for the test if configurable.
    let handle = Broker::start(cfg).await.expect("start");
    assert!(handle.test_mark_log_dir_offline(primary.path()));
    // expect want_shutdown/should_shutdown to latch within a few heartbeat ticks.
    let shut = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handle.test_await_self_shutdown(), // add this test helper if needed
    )
    .await;
    assert!(shut.is_ok(), "broker did not self-shut-down on all-dirs-offline");
}
```

- [ ] **Step 2:** `cargo test -p crabka-broker --test jbod_disk_failure all_dirs_offline 2>&1 | tail -20`. Expected PASS. If the self-shutdown depends on a controller drain that can't complete in single-broker (no replacement leader), ensure Task 10 also directly cancels `supervisor_shutdown`/listener so the broker stops regardless — adjust Task 10 if this test reveals a hang.

- [ ] **Step 3: fmt + commit** (`test(broker): broker self-shuts-down when all log dirs fail (KIP-112)`).

---

### Task 13: Docs — README + STATUS

**Files:**
- Modify: `README.md:423` (KIP-112 row), `STATUS.md` (new slice entry)

- [ ] **Step 1:** In `README.md`, change the KIP-112 row status cell from `⚠️` to `✅`:

```markdown
| [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112) | Handle disk failure for JBOD | ✅ |
```

- [ ] **Step 2:** Add a STATUS.md slice entry (match the existing slice-entry format, newest-appropriate location) summarizing: per-log-dir UUIDs, `PartitionRecord.directories` (KIP-858), `AssignReplicasToDirs` (key 73), `offline_log_dirs` heartbeat reporting, `compute_offline_dir_failover_changes` controller failover, all-dirs self-shutdown, runtime offline-flip already wired in `partition_writer`. Note the boundary: live multi-broker E2E deferred to Linux CI; PartitionRecord KRaft emission bumped v0→v1.

- [ ] **Step 3: Full workspace gate.**

Run: `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20 && cargo test -p crabka-metadata && cargo test -p crabka-broker --lib 2>&1 | tail -20`
Expected: fmt clean, clippy clean, tests PASS.

- [ ] **Step 4: fmt + commit** (`docs: mark KIP-112 ✅; STATUS slice for JBOD disk-failure`).

---

## Final verification (after all tasks)

- [ ] `cargo fmt --check` — clean.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — clean (watch `large_futures` if `BrokerConfig`/handler futures grew).
- [ ] `cargo test -p crabka-metadata` — PASS (records + kraft round-trip).
- [ ] `cargo test -p crabka-broker --lib` — PASS (log_dir_id, leader_election, assign_replicas_to_dirs, assign_dirs).
- [ ] `cargo test -p crabka-broker --test jbod --test jbod_disk_failure --test alter_replica_log_dirs` — PASS.
- [ ] Manually confirm no `PartitionRecord { … }` literal anywhere omits `directories`
      (`git -C <root> grep -n "PartitionRecord {" -- 'crates/**/*.rs'` then eyeball).

## Self-review notes (spec coverage)

- Spec Component 1 (per-dir UUIDs) → Task 2, Task 4.
- Component 2 (`PartitionRecord.directories` + snapshot round-trip) → Task 1, Task 3.
- Component 3 (`AssignReplicasToDirs`) → Task 6 (handler), Task 7 (send).
- Component 4 (heartbeat `offline_log_dirs`) → Task 8.
- Component 5 (controller failover) → Task 5 (logic), Task 9 (wiring).
- Component 6 (all-dirs self-shutdown) → Task 10.
- Component 7 (cleanup + tests + docs) → Task 4 (doc fix), Tasks 11–13.
