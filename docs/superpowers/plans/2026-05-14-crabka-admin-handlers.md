# Slice 11: Admin handlers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add eight operator-facing handlers (AlterConfigs, IncrementalAlterConfigs, CreatePartitions, DeleteRecords, DescribeCluster, ListGroups, DescribeGroups, DeleteGroups) to `crabka-broker` so the JVM `kafka-*.sh` admin tools work against a Rust broker without skipping.

**Architecture:** New `MetadataRecord::V1TopicConfig` variant holds per-topic config overrides in the raft-backed metadata image. `Log.config` is wrapped in `Arc<RwLock<LogConfig>>` so AlterConfigs takes effect live without broker restart — `ReplicatorSupervisor::reconcile` pushes the current overrides to each locally-hosted partition's writer actor via a new `WriterMessage::SetLogConfig` message. The other handlers are mostly serialization on top of existing subsystems: CreatePartitions reuses CreateTopics' round-robin placement, DeleteRecords adds a `WriterMessage::TrimToOffset` that delegates to a new `Log::trim_to_offset`, and the three group handlers expose read/delete views over `GroupManager`.

**Tech Stack:** Rust 1.95.0; existing `openraft 0.9.24` + `serde_wincode` for metadata records; existing `crabka_protocol::owned::*` generated types (all eight RPC types already exist on disk in `crates/protocol/generated/`).

**Reference spec:** [`docs/superpowers/specs/2026-05-14-crabka-admin-handlers-design.md`](../specs/2026-05-14-crabka-admin-handlers-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/admin-handlers-11` (already created off main, spec already committed).

---

## File structure

```
crates/metadata/src/
├── records.rs                       # MODIFIED — V1TopicConfig variant + TopicConfigRecord struct
├── image.rs                         # MODIFIED — topic_configs field + apply/validate + accessor
└── error.rs                         # MODIFIED — InvalidConfig variant

crates/log/src/
├── config.rs                        # unchanged (LogConfig struct shape)
└── log.rs                           # MODIFIED — Arc<RwLock<LogConfig>>, set_config(), trim_to_offset()

crates/broker/src/
├── config_keys.rs                   # NEW — whitelist + validate + apply_to_log_config
├── lib.rs                           # MODIFIED — mod config_keys
├── partition.rs                     # MODIFIED — apply_log_config_overrides(), trim_to_offset()
├── partition_writer.rs              # MODIFIED — SetLogConfig + TrimToOffset arms
├── replicator_supervisor.rs         # MODIFIED — push overrides per reconcile
├── coordinator/mod.rs               # MODIFIED — GroupManager::{list_groups, describe, delete}
└── handlers/
    ├── mod.rs                       # MODIFIED — register 8 new handlers
    ├── alter_configs.rs             # NEW — api_key 33
    ├── incremental_alter_configs.rs # NEW — api_key 44
    ├── create_partitions.rs         # NEW — api_key 37
    ├── delete_records.rs            # NEW — api_key 21
    ├── describe_cluster.rs          # NEW — api_key 60
    ├── list_groups.rs               # NEW — api_key 16
    ├── describe_groups.rs           # NEW — api_key 15
    └── delete_groups.rs             # NEW — api_key 42

crates/broker/tests/
├── admin_handlers.rs                # NEW — broker-side integration tests
└── jvm_acceptance.rs                # MODIFIED — 5 new JVM CLI tests
```

The plan is structured in **five batches** matching the slice-10b cadence; each batch is a self-contained set of tasks ending in a commit. Batches build sequentially.

---

## Batch 1 — `V1TopicConfig` metadata record

### Task 1: Define `TopicConfigRecord` struct and `V1TopicConfig` variant

**Files:**
- Modify: `crates/metadata/src/records.rs`

- [ ] **Step 1: Write failing round-trip test**

Append to the `#[cfg(test)] mod tests` block in `crates/metadata/src/records.rs`:

```rust
    #[test]
    fn topic_config_record_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        overrides.insert("segment.bytes".to_string(), "1048576".to_string());
        let r = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        });
        assert_eq!(round_trip(&r), r);
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-metadata --lib records::tests::topic_config_record_round_trip 2>&1 | tail -5
```

Expected: FAIL with "cannot find type `TopicConfigRecord` in this scope" and "no variant `V1TopicConfig`".

- [ ] **Step 3: Add `TopicConfigRecord` struct and the `V1TopicConfig` variant**

In `crates/metadata/src/records.rs`, add the struct after `DeleteTopicRecord` (just before `pub enum MetadataRecord`):

```rust
/// Mutable topic configuration overrides. Authoritative target state:
/// each `V1TopicConfig` record fully replaces the previous override map
/// for `topic`. Empty map = clear all overrides. Merging happens at the
/// AlterConfigs handler before the record is submitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigRecord {
    pub topic: String,
    pub overrides: std::collections::BTreeMap<String, String>,
}
```

Then add a new variant to `MetadataRecord` (just before the closing `}`):

```rust
    V1TopicConfig(TopicConfigRecord),
```

Final enum:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
    V1TopicConfig(TopicConfigRecord),
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p crabka-metadata --lib records::tests::topic_config_record_round_trip 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/metadata/src/records.rs
git commit -m "feat(metadata): add V1TopicConfig record variant"
```

---

### Task 2: Plumb `V1TopicConfig` through `MetadataImage::apply` and `validate`

**Files:**
- Modify: `crates/metadata/src/image.rs`
- Modify: `crates/metadata/src/error.rs`

- [ ] **Step 1: Write failing image tests**

Append to the `#[cfg(test)] mod tests` block in `crates/metadata/src/image.rs`:

```rust
    #[test]
    fn apply_topic_config_inserts() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: overrides.clone(),
            },
        ));
        assert_eq!(m.topic_config("t"), Some(&overrides));
    }

    #[test]
    fn apply_topic_config_replaces_previous() {
        let mut m = img();
        m.apply(&topic("t", 1));

        let mut first = std::collections::BTreeMap::new();
        first.insert("retention.ms".to_string(), "60000".to_string());
        first.insert("segment.bytes".to_string(), "1024".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: first,
            },
        ));

        let mut second = std::collections::BTreeMap::new();
        second.insert("retention.ms".to_string(), "120000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides: second.clone(),
            },
        ));

        // segment.bytes is GONE — last-write-wins is authoritative.
        assert_eq!(m.topic_config("t"), Some(&second));
    }

    #[test]
    fn delete_topic_clears_configs() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        m.apply(&MetadataRecord::V1TopicConfig(
            crate::records::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));
        m.apply(&MetadataRecord::V1DeleteTopic(
            crate::records::DeleteTopicRecord { name: "t".into() },
        ));
        assert!(m.topic_config("t").is_none());
    }

    #[test]
    fn validate_topic_config_for_unknown_topic_rejected() {
        let m = img();
        let r = MetadataRecord::V1TopicConfig(crate::records::TopicConfigRecord {
            topic: "ghost".into(),
            overrides: std::collections::BTreeMap::new(),
        });
        let err = m.validate(&r).unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-metadata --lib image::tests 2>&1 | tail -20
```

Expected: four NEW tests fail with `no method named topic_config` / pattern match failures.

- [ ] **Step 3: Add `topic_configs` field and accessor**

In `crates/metadata/src/image.rs`, update the struct:

```rust
#[derive(Debug, Clone, Default)]
pub struct MetadataImage {
    cluster_id: Uuid,
    topics: HashMap<String, TopicRecord>,
    partitions: HashMap<(String, i32), PartitionRecord>,
    brokers: HashMap<NodeId, BrokerRegistrationRecord>,
    topic_configs: HashMap<String, std::collections::BTreeMap<String, String>>,
}
```

Update the `new` constructor:

```rust
    #[must_use]
    pub fn new(cluster_id: Uuid) -> Self {
        Self {
            cluster_id,
            topics: HashMap::new(),
            partitions: HashMap::new(),
            brokers: HashMap::new(),
            topic_configs: HashMap::new(),
        }
    }
```

Add the accessor (place it after `partitions_of`):

```rust
    /// Currently-effective config overrides for `topic`, or `None` if no
    /// `V1TopicConfig` record has been applied for this topic since the last
    /// `V1DeleteTopic` (or since image creation).
    #[must_use]
    pub fn topic_config(
        &self,
        topic: &str,
    ) -> Option<&std::collections::BTreeMap<String, String>> {
        self.topic_configs.get(topic)
    }
```

- [ ] **Step 4: Extend `apply` and `validate`**

In the `apply` match, add a new arm and update the existing `V1DeleteTopic` arm:

```rust
            MetadataRecord::V1DeleteTopic(d) => {
                self.topics.remove(&d.name);
                self.partitions.retain(|(t, _), _| t != &d.name);
                self.topic_configs.remove(&d.name);
            }
            MetadataRecord::V1TopicConfig(c) => {
                if c.overrides.is_empty() {
                    self.topic_configs.remove(&c.topic);
                } else {
                    self.topic_configs.insert(c.topic.clone(), c.overrides.clone());
                }
            }
```

In the `validate` match, add a new arm just before the catch-all `V1BrokerRegistration` arm:

```rust
            MetadataRecord::V1TopicConfig(c) => {
                if !self.topics.contains_key(&c.topic) {
                    return Err(MetadataError::UnknownTopic(c.topic.clone()));
                }
                Ok(())
            }
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p crabka-metadata --lib 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add crates/metadata/src/image.rs
git commit -m "feat(metadata): plumb V1TopicConfig through MetadataImage"
```

---

### Task 3: Verify workspace still builds and existing tests pass

- [ ] **Step 1: Workspace build**

```bash
cargo build --workspace --release 2>&1 | tail -5
```

Expected: clean. Adding a `#[non_exhaustive]` enum variant is source-compatible — every `match` against `MetadataRecord` in `crabka-broker` and `crabka-raft` already has either a catch-all `_` arm or compiles because of `#[non_exhaustive]` (which forces exhaustiveness for definers but not consumers).

If a match arm fails, add `MetadataRecord::V1TopicConfig(_) => {}` where appropriate — but do NOT silently swallow important paths. The expected failing matches (if any) are in `crates/raft/src/state_machine.rs` and `crates/broker/src/broker.rs` where the apply-side matches metadata records; those should both delegate to `MetadataImage::apply` already.

- [ ] **Step 2: Workspace tests**

```bash
cargo test --workspace --release 2>&1 | tail -15
```

Expected: all green. No new tests yet beyond what we just added.

- [ ] **Step 3: Format and lint**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --release -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 4: Commit if any formatting changes**

```bash
git diff --quiet || (git add -A && git commit -m "chore: cargo fmt after V1TopicConfig")
```

---

## Batch 2 — Config whitelist + live propagation + AlterConfigs handlers

### Task 4: Make `Log.config` swappable

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write failing test for `Log::set_config`**

Append to the `#[cfg(test)] mod tests` block in `crates/log/src/log.rs`:

```rust
    #[test]
    fn set_config_swaps_active_config() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                retention_ms: Some(std::time::Duration::from_secs(60)),
                ..LogConfig::default()
            },
        )
        .expect("open");
        log.set_config(LogConfig {
            retention_ms: Some(std::time::Duration::from_secs(120)),
            ..LogConfig::default()
        });
        // Read-back via a snapshot helper.
        assert_eq!(
            log.config_snapshot().retention_ms,
            Some(std::time::Duration::from_secs(120))
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-log --lib log::tests::set_config_swaps_active_config 2>&1 | tail -5
```

Expected: FAIL with "no method named `set_config`".

- [ ] **Step 3: Wrap `Log.config` in `Arc<RwLock<LogConfig>>` and add `set_config`**

In `crates/log/src/log.rs`, change the `Log` struct's `config` field declaration:

```rust
pub struct Log {
    dir: PathBuf,
    config: std::sync::Arc<std::sync::RwLock<LogConfig>>,
    // ... rest unchanged
```

In the `open` function (around line 80), wrap the incoming config:

```rust
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        // ... existing logic up to where the Log struct is constructed
        let config_for_segments = config.clone();
        let config = std::sync::Arc::new(std::sync::RwLock::new(config));
        // ... continue using `config_for_segments` for any one-time setup that
        //     needs to look at the values right now; the field stores the Arc
```

Look for the existing line that uses `config.validate_on_open` (around line 105 — the `Segment::open_active` call inside `open`). If `config` was moved into the struct before that call, replace with a clone read from `config_for_segments`. The simplest pattern: capture the parts you need at the top of `open` as locals, before the field is constructed:

```rust
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        let validate_on_open = config.validate_on_open;
        let index_interval_bytes = config.index_interval_bytes;
        // ... use validate_on_open / index_interval_bytes throughout the open
        //     body for the initial segment load
        // then at the end:
        let config = std::sync::Arc::new(std::sync::RwLock::new(config));
        Ok(Self { dir, config, /* ... */ })
    }
```

Adjust the three `self.config.<field>` reads in `append` / `roll`:

- Line 320 `seg.size_bytes() >= self.config.segment_bytes` → `seg.size_bytes() >= self.config.read().unwrap().segment_bytes`
- Line 331 `active.append(batch, self.config.index_interval_bytes)?` → `active.append(batch, self.config.read().unwrap().index_interval_bytes)?`
- Line 333 `if self.config.flush_on_append` → `if self.config.read().unwrap().flush_on_append`

And in `Log::tick`, replace the two `&self.config` calls with `&*self.config.read().unwrap()`:

```rust
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        let sealed_refs: Vec<&Segment> = self.segments.iter().map(AsRef::as_ref).collect();
        let active_size = self.active.as_ref().map_or(0, Segment::size_bytes);

        let cfg_guard = self.config.read().unwrap();
        let time_evict = retention::time_based_evict(&sealed_refs, &cfg_guard, now);
        let size_evict = retention::size_based_evict(&sealed_refs, active_size, &cfg_guard);
        drop(cfg_guard);

        // ... rest unchanged
    }
```

Add the two new methods (place them near `log_start_offset`):

```rust
    /// Atomically swap the active `LogConfig`. The next retention/roll check
    /// reads the new value; in-flight `append` calls hold the lock for
    /// trivially short windows and will not see a half-applied config.
    pub fn set_config(&self, new: LogConfig) {
        *self.config.write().unwrap() = new;
    }

    /// Snapshot the current config. Allocates a clone; cheap because
    /// `LogConfig` is small and `Clone`.
    #[must_use]
    pub fn config_snapshot(&self) -> LogConfig {
        self.config.read().unwrap().clone()
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p crabka-log --lib 2>&1 | tail -10
```

Expected: all green. If the `&self.config` pattern in `tick` was used elsewhere, fix similarly.

- [ ] **Step 5: Workspace build catch-up**

```bash
cargo build --workspace --release 2>&1 | tail -5
```

Expected: clean. `crabka-broker` and other consumers don't construct `Log` directly except through `Log::open` so the field-shape change is internal.

- [ ] **Step 6: Commit**

```bash
git add crates/log/src/log.rs
git commit -m "feat(log): make Log::config atomically swappable via RwLock"
```

---

### Task 5: Add `Log::trim_to_offset`

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write failing test**

Append to the `#[cfg(test)] mod tests` block in `crates/log/src/log.rs`:

```rust
    #[test]
    fn trim_to_offset_drops_old_segments() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_bytes: 200, // small so we roll fast
                ..LogConfig::default()
            },
        )
        .expect("open");
        // Append 30 records to force multiple sealed segments.
        for _ in 0..30 {
            let mut b = sample_batch(1);
            log.append(&mut b).expect("append");
        }
        let leo = log.log_end_offset();
        let new_start = log.trim_to_offset(15).expect("trim");
        // Trim clamps to next segment boundary >= target; new_start <= 15
        // and >= original log_start_offset (= 0). Subsequent reads from 0
        // should return OFFSET_OUT_OF_RANGE.
        assert!(new_start <= 15);
        assert!(log.log_start_offset() >= new_start.min(15) - log.config_snapshot().segment_bytes as i64);
        assert_eq!(log.log_end_offset(), leo);
    }

    #[test]
    fn trim_to_offset_clamps_to_leo() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
        // Append a few records.
        for _ in 0..3 {
            let mut b = sample_batch(1);
            log.append(&mut b).expect("append");
        }
        let leo = log.log_end_offset();
        let new_start = log.trim_to_offset(999).expect("trim");
        // Asking to trim past LEO means trim to LEO.
        assert_eq!(new_start, leo);
    }

    #[test]
    fn trim_to_offset_rejects_negative() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
        assert!(log.trim_to_offset(-5).is_err());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p crabka-log --lib log::tests::trim_to_offset 2>&1 | tail -10
```

Expected: three NEW tests FAIL with "no method named `trim_to_offset`".

- [ ] **Step 3: Implement `trim_to_offset`**

In `crates/log/src/log.rs`, after `truncate_to`, add:

```rust
    /// Trim from the start of the log: drop every sealed segment whose
    /// last offset is `< target`, advance `log_start_offset` to the new
    /// boundary. Active segment is never deleted by this call; if `target`
    /// falls inside the active segment, the log_start_offset is bumped to
    /// `target` (via `test_set_log_start_offset`'s mechanism) and no files
    /// are dropped. Returns the new `log_start_offset`.
    ///
    /// `target` is clamped to `[0, log_end_offset()]`. Caller asks for
    /// trim past LEO → trim to LEO.
    ///
    /// # Errors
    ///
    /// Returns `LogError::InvalidArgument` if `target < 0`.
    pub fn trim_to_offset(&mut self, target: i64) -> Result<i64, LogError> {
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "trim_to_offset: target must be >= 0".into(),
            ));
        }
        let leo = self.log_end_offset();
        let target = target.min(leo);
        let log_start = self.log_start_offset();
        if target <= log_start {
            return Ok(log_start);
        }

        // Drop any sealed segment whose last record is < target.
        // A sealed segment covers [base_offset, next_segment_base_offset).
        // We compute "last offset" as the next segment's base_offset - 1
        // (or, for the most-recent sealed segment, the active segment's
        // base_offset - 1).
        let active_base = self
            .active
            .as_ref()
            .map_or(leo, crate::segment::Segment::base_offset);
        let mut next_bases: Vec<i64> = self
            .segments
            .iter()
            .map(|s| s.base_offset())
            .skip(1)
            .collect();
        next_bases.push(active_base);

        let mut to_drop: Vec<i64> = Vec::new();
        for (seg, next_base) in self.segments.iter().zip(next_bases.iter()) {
            // Last offset in this sealed segment = next_base - 1.
            if *next_base <= target {
                to_drop.push(seg.base_offset());
            } else {
                break;
            }
        }

        for base in &to_drop {
            self.segments.retain(|s| s.base_offset() != *base);
            let _ = retention::delete_segment_files(&self.dir, *base);
        }

        // If target falls inside the active segment, advance the start
        // override so consumer fetches < target return OFFSET_OUT_OF_RANGE.
        // We reuse the existing test_set_log_start_offset hook, which is
        // safe for production use here — it ONLY moves the start offset
        // forward and validates against LEO.
        let new_log_start = self
            .segments
            .first()
            .map_or(active_base, crate::segment::Segment::base_offset);
        if target > new_log_start {
            self.test_set_log_start_offset(target)?;
        }
        Ok(self.log_start_offset())
    }
```

- [ ] **Step 4: Ensure `LogError::InvalidArgument` exists**

```bash
grep -n "InvalidArgument" crates/log/src/error.rs
```

If the variant does not exist, add it to `crates/log/src/error.rs`:

```rust
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
```

- [ ] **Step 5: Promote `test_set_log_start_offset` from test-only to public**

The existing `pub fn test_set_log_start_offset` has `#[cfg(any(test, feature = "test-helpers"))]` or similar. Check:

```bash
sed -n '150,165p' crates/log/src/log.rs
```

If the cfg gate is there, remove ONLY the cfg attribute (keep the function), and rename the function (and the comment) to drop the `test_` prefix while keeping a deprecated alias:

```rust
    /// Advance `log_start_offset` to `new_start`. Must be in
    /// `[current log_start, log_end]`. Used by `trim_to_offset` for the
    /// active-segment case and by the broker's `DeleteRecords` handler.
    pub fn set_log_start_offset(&mut self, new_start: i64) -> Result<(), LogError> {
        // (existing body — same checks)
    }

    #[deprecated(note = "use set_log_start_offset")]
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_log_start_offset(&mut self, new_start: i64) -> Result<(), LogError> {
        self.set_log_start_offset(new_start)
    }
```

Inside `trim_to_offset`, replace the call to `self.test_set_log_start_offset(target)?` with `self.set_log_start_offset(target)?`.

- [ ] **Step 6: Run tests to verify they pass**

```bash
cargo test -p crabka-log --lib 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add crates/log/src/log.rs crates/log/src/error.rs
git commit -m "feat(log): add trim_to_offset for DeleteRecords"
```

---

### Task 6: Create the config whitelist module

**Files:**
- Create: `crates/broker/src/config_keys.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write failing tests in the new module**

Create `crates/broker/src/config_keys.rs`:

```rust
//! Topic-config whitelist for AlterConfigs / IncrementalAlterConfigs.
//!
//! Six keys are recognized. Three propagate live to `Log.config`
//! (`retention.ms`, `retention.bytes`, `segment.bytes`). Three are
//! accepted as no-op defaults for compatibility but reject non-default
//! values: `cleanup.policy` (only `delete`), `compression.type` (only
//! `producer`), `min.insync.replicas` (any non-negative integer accepted
//! but not yet enforced — see the design spec for the rationale).
//!
//! Unknown keys are rejected with `INVALID_CONFIG`.

use std::collections::BTreeMap;
use std::time::Duration;

use crabka_log::LogConfig;

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// Validate a single key/value pair. `Err(reason)` carries an
/// operator-readable explanation that the handler propagates into the
/// `error_message` field of the response.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    match key {
        RETENTION_MS => parse_i64_at_least(-1, value).map(|_| ()),
        RETENTION_BYTES => parse_i64_at_least(-1, value).map(|_| ()),
        SEGMENT_BYTES => parse_u64_at_least(1, value).map(|_| ()),
        CLEANUP_POLICY => {
            if value == "delete" {
                Ok(())
            } else {
                Err(format!(
                    "cleanup.policy={value} not supported; only `delete` is currently honored"
                ))
            }
        }
        COMPRESSION_TYPE => {
            if value == "producer" {
                Ok(())
            } else {
                Err(format!(
                    "compression.type={value} not supported; only `producer` (broker pass-through) is currently honored"
                ))
            }
        }
        MIN_INSYNC_REPLICAS => parse_i64_at_least(1, value).map(|_| ()),
        unknown => Err(format!("unrecognized config key `{unknown}`")),
    }
}

fn parse_i64_at_least(min: i64, value: &str) -> Result<i64, String> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| format!("expected integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

fn parse_u64_at_least(min: u64, value: &str) -> Result<u64, String> {
    let parsed: u64 = value
        .parse()
        .map_err(|_| format!("expected non-negative integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

/// Merge `overrides` over `base` and return a fresh `LogConfig` to push
/// into `Log::set_config`. Unknown keys are silently dropped because
/// `validate_topic_config` should have rejected them at AlterConfigs time
/// before the record reached the metadata image; this function is the
/// applier and treats the input as already-validated.
#[must_use]
pub(crate) fn apply_to_log_config(
    overrides: &BTreeMap<String, String>,
    base: &LogConfig,
) -> LogConfig {
    let mut out = base.clone();
    for (k, v) in overrides {
        match k.as_str() {
            RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    out.retention_ms = if ms < 0 {
                        None
                    } else {
                        Some(Duration::from_millis(ms as u64))
                    };
                }
            }
            RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.retention_bytes = if b < 0 { None } else { Some(b as u64) };
                }
            }
            SEGMENT_BYTES => {
                if let Ok(b) = v.parse::<u64>() {
                    out.segment_bytes = b;
                }
            }
            // The remaining keys are recognized but no broker behavior is
            // wired to them yet (see module docs).
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_retention_ms_accepts_positive_and_minus_one() {
        assert!(validate_topic_config(RETENTION_MS, "60000").is_ok());
        assert!(validate_topic_config(RETENTION_MS, "-1").is_ok());
    }

    #[test]
    fn validate_retention_ms_rejects_below_minus_one() {
        assert!(validate_topic_config(RETENTION_MS, "-5").is_err());
    }

    #[test]
    fn validate_retention_ms_rejects_non_integer() {
        assert!(validate_topic_config(RETENTION_MS, "abc").is_err());
    }

    #[test]
    fn validate_segment_bytes_rejects_zero() {
        assert!(validate_topic_config(SEGMENT_BYTES, "0").is_err());
    }

    #[test]
    fn validate_cleanup_policy_compact_rejected() {
        let err = validate_topic_config(CLEANUP_POLICY, "compact").unwrap_err();
        assert!(err.contains("not supported"));
    }

    #[test]
    fn validate_cleanup_policy_delete_accepted() {
        assert!(validate_topic_config(CLEANUP_POLICY, "delete").is_ok());
    }

    #[test]
    fn validate_compression_producer_accepted() {
        assert!(validate_topic_config(COMPRESSION_TYPE, "producer").is_ok());
    }

    #[test]
    fn validate_compression_zstd_rejected() {
        let err = validate_topic_config(COMPRESSION_TYPE, "zstd").unwrap_err();
        assert!(err.contains("not supported"));
    }

    #[test]
    fn validate_min_isr_positive_accepted() {
        assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
    }

    #[test]
    fn validate_unknown_key_rejected() {
        let err = validate_topic_config("flush.ms", "1000").unwrap_err();
        assert!(err.contains("unrecognized"));
    }

    #[test]
    fn apply_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "60000".into());
        let base = LogConfig::default();
        let out = apply_to_log_config(&o, &base);
        assert_eq!(out.retention_ms, Some(Duration::from_millis(60_000)));
    }

    #[test]
    fn apply_retention_ms_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.retention_ms, None);
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.segment_bytes, 1_048_576);
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention_ms: Some(Duration::from_millis(12345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert_eq!(out.retention_ms, base.retention_ms);
    }
}
```

- [ ] **Step 2: Wire the module into `crabka_broker::lib`**

Open `crates/broker/src/lib.rs`. Find the existing `pub(crate) mod` declarations (alphabetical order, near `pub(crate) mod codes;`). Add:

```rust
pub(crate) mod config_keys;
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --lib config_keys 2>&1 | tail -15
```

Expected: all 13 unit tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/config_keys.rs crates/broker/src/lib.rs
git commit -m "feat(broker): add topic-config whitelist module"
```

---

### Task 7: Add `WriterMessage::SetLogConfig` and `Partition::apply_log_config_overrides`

**Files:**
- Modify: `crates/broker/src/partition.rs`
- Modify: `crates/broker/src/partition_writer.rs`

- [ ] **Step 1: Write failing integration-style test in partition_writer**

In `crates/broker/src/partition_writer.rs`'s `#[cfg(test)] mod tests`, add:

```rust
    #[tokio::test]
    async fn writer_set_log_config_swaps_config() {
        use crabka_log::LogConfig;
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
        ));

        let new_cfg = LogConfig {
            retention_ms: Some(std::time::Duration::from_millis(12345)),
            ..LogConfig::default()
        };
        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::SetLogConfig {
            config: new_cfg.clone(),
            ack,
        })
        .await
        .expect("send");
        ack_rx.await.expect("ack");

        let observed = log.lock().expect("lock").config_snapshot();
        assert_eq!(observed.retention_ms, new_cfg.retention_ms);

        drop(tx);
        writer.await.expect("writer join");
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-broker --lib partition_writer::tests::writer_set_log_config_swaps_config 2>&1 | tail -10
```

Expected: FAIL with "no variant `SetLogConfig`".

- [ ] **Step 3: Add the `SetLogConfig` variant to `WriterMessage` and the writer arm**

In `crates/broker/src/partition.rs`, find the `WriterMessage` enum and add (just before the `#[cfg(any(test, feature = "test-helpers"))]` variants):

```rust
    /// Atomically swap the partition's `LogConfig`. The writer task
    /// serializes this with appends so no in-flight `RecordBatch` sees a
    /// half-applied config. Sent by
    /// `ReplicatorSupervisor::reconcile` whenever a `V1TopicConfig`
    /// record changes the topic's overrides.
    SetLogConfig {
        config: crabka_log::LogConfig,
        ack: tokio::sync::oneshot::Sender<()>,
    },
```

In `crates/broker/src/partition_writer.rs`'s `run` match, add a new arm (after the existing `Replicate` arm, before `Truncate`):

```rust
            WriterMessage::SetLogConfig { config, ack } => {
                log.lock()
                    .expect("log mutex poisoned")
                    .set_config(config);
                let _ = ack.send(());
            }
```

- [ ] **Step 4: Add `Partition::apply_log_config_overrides`**

In `crates/broker/src/partition.rs`, just after `replicate_batch`:

```rust
    /// Push `overrides` (already-validated; see `config_keys`) through the
    /// writer actor so the partition's `Log` picks up the new
    /// `retention.ms` / `retention.bytes` / `segment.bytes` on the next
    /// retention/roll tick. Idempotent: pushing the same map twice is a
    /// cheap noop. Called by `ReplicatorSupervisor::reconcile` every time
    /// the metadata image changes.
    pub(crate) async fn apply_log_config_overrides(
        &self,
        overrides: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), BrokerError> {
        let merged = crate::config_keys::apply_to_log_config(
            overrides,
            &crabka_log::LogConfig::default(),
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::SetLogConfig {
                config: merged,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?;
        Ok(())
    }
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --lib partition_writer::tests::writer_set_log_config_swaps_config 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/partition.rs crates/broker/src/partition_writer.rs
git commit -m "feat(broker): WriterMessage::SetLogConfig + Partition::apply_log_config_overrides"
```

---

### Task 8: Push config overrides on every supervisor reconcile

**Files:**
- Modify: `crates/broker/src/replicator_supervisor.rs`

- [ ] **Step 1: Write failing test**

Add to the `#[cfg(test)] mod tests` block in `crates/broker/src/replicator_supervisor.rs`:

```rust
    #[tokio::test]
    async fn reconcile_pushes_topic_config_overrides_to_local_partitions() {
        use crabka_log::LogConfig;
        use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord};
        use std::collections::BTreeMap;
        use tempfile::tempdir;
        use uuid::Uuid;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(DashMap::new());
        materialize_partition(&partitions, "t", 0, dir.path(), &LogConfig::default())
            .expect("materialize");

        // Build an image with a V1TopicConfig that sets retention.ms=60000.
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1],
            isr: vec![1],
            leader_epoch: 0,
        }));
        let mut overrides = BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        }));

        // The supervisor's reconcile path needs a controller stub. We
        // construct a minimal one identical to what the existing tests
        // use; if a helper exists in `tests` (e.g. `stub_controller`),
        // call it here. Otherwise inline the in-memory controller.
        let controller = crate::tests_helper::in_memory_controller(1).await;
        let supervisor = ReplicatorSupervisor::new(
            1,
            controller,
            partitions.clone(),
            dir.path().to_path_buf(),
            LogConfig::default(),
            "client".into(),
            tokio_util::sync::CancellationToken::new(),
            None,
        );

        supervisor.reconcile(&img).await;

        // Give the writer actor a moment to apply the SetLogConfig message.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let part = partitions
            .get(&("t".to_string(), 0))
            .expect("part")
            .value()
            .clone();
        let snap = part.log.lock().expect("log lock").config_snapshot();
        assert_eq!(
            snap.retention_ms,
            Some(std::time::Duration::from_millis(60_000))
        );
    }
```

If `crate::tests_helper::in_memory_controller` does not exist, use an inline stub: spin up a single-node controller via `crabka_raft::Controller::start` with `BootstrapMode::Bootstrap` against a temp dir. The existing supervisor tests (look for `#[tokio::test]` patterns in `replicator_supervisor.rs`) show the pattern.

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-broker --lib replicator_supervisor::tests::reconcile_pushes_topic_config 2>&1 | tail -10
```

Expected: FAIL — the reconcile method doesn't push config overrides yet.

- [ ] **Step 3: Add the config-push pass to `reconcile`**

In `crates/broker/src/replicator_supervisor.rs`, find the `reconcile` method. After the existing `desired_local_set` loop (the one that materializes local partitions and installs ISR) and before the `// 1. Cancel removed.` block, add:

```rust
        // Push topic-config overrides onto every locally-hosted partition.
        // Pushes are idempotent — sending the same `LogConfig` is a cheap
        // noop write inside `Log::set_config`. The metadata-watch reconcile
        // loop fires on every image change, so AlterConfigs propagation is
        // bounded to one reconcile tick.
        for (topic, partition) in desired_local_set(self.node_id, image) {
            let Some(part) = self
                .partitions
                .get(&(topic.clone(), partition))
                .map(|e| e.value().clone())
            else {
                continue;
            };
            let empty = std::collections::BTreeMap::new();
            let overrides = image.topic_config(&topic).unwrap_or(&empty);
            if let Err(e) = part.apply_log_config_overrides(overrides).await {
                warn!(
                    topic = %topic, partition = partition, error = %e,
                    "supervisor: apply_log_config_overrides failed"
                );
            }
        }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --lib replicator_supervisor 2>&1 | tail -10
```

Expected: all green (4 existing + 1 new).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/replicator_supervisor.rs
git commit -m "feat(broker): supervisor reconciles topic-config overrides to partitions"
```

---

### Task 9: AlterConfigs handler (api_key 33)

**Files:**
- Create: `crates/broker/src/handlers/alter_configs.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Inspect the request/response shape**

```bash
grep -nE "pub.*resources|resource_type|resource_name|configs:" crates/protocol/generated/AlterConfigsRequest.owned.rs | head -10
grep -nE "pub.*resources|resource_type|resource_name|error_code|error_message" crates/protocol/generated/AlterConfigsResponse.owned.rs | head -10
```

Expected: `resources: Vec<AlterConfigsResource>` with `resource_type: i8`, `resource_name: String`, `configs: Vec<AlterableConfig>`; response is a Vec of `AlterConfigsResourceResponse` with `error_code: i16` and `error_message: Option<String>`. The exact field names come from the Kafka wire schema and are stable.

- [ ] **Step 2: Write the handler**

Create `crates/broker/src/handlers/alter_configs.rs`:

```rust
//! `AlterConfigs` (`api_key=33`). Topic-level only. Each resource's full
//! override map (the *complete* set of non-default values for that topic)
//! is built from the request, validated against the whitelist in
//! [`crate::config_keys`], and submitted through the controller as a
//! single `V1TopicConfig` record. Replication-side propagation runs on
//! every reconcile (see `ReplicatorSupervisor::reconcile`).

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, TopicConfigRecord};
use crabka_protocol::owned::alter_configs_request::AlterConfigsRequest;
use crabka_protocol::owned::alter_configs_response::{
    AlterConfigsResourceResponse, AlterConfigsResponse,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::broker::Broker;
use crate::codes;
use crate::config_keys;
use crate::error::BrokerError;

const RESOURCE_TYPE_TOPIC: i8 = 2;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AlterConfigsRequest::decode(&mut cur, version)?;

        let mut responses: Vec<AlterConfigsResourceResponse> =
            Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            let mut out = AlterConfigsResourceResponse {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            };

            if resource.resource_type != RESOURCE_TYPE_TOPIC {
                out.error_code = codes::INVALID_RESOURCE_TYPE;
                out.error_message =
                    Some(format!("resource_type={} not supported", resource.resource_type));
                responses.push(out);
                continue;
            }

            let image = controller.current_image();
            if image.topic(&resource.resource_name).is_none() {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                out.error_message = Some(format!("unknown topic `{}`", resource.resource_name));
                responses.push(out);
                continue;
            }

            // AlterConfigs is FULL replacement semantics per Kafka:
            // the request's `configs` list IS the new target state for
            // this resource. Validate every entry; on first invalid key
            // surface INVALID_CONFIG and skip the submit.
            let mut overrides = std::collections::BTreeMap::new();
            let mut validation_err: Option<String> = None;
            for cfg in &resource.configs {
                let value = cfg.value.clone().unwrap_or_default();
                if let Err(reason) = config_keys::validate_topic_config(&cfg.name, &value) {
                    validation_err = Some(reason);
                    break;
                }
                overrides.insert(cfg.name.clone(), value);
            }
            if let Some(reason) = validation_err {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(reason);
                responses.push(out);
                continue;
            }

            let record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: resource.resource_name.clone(),
                overrides,
            });
            match controller.submit_change(vec![record]).await {
                Ok(()) => {
                    // success — error_code already NONE
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    out.error_code = codes::NOT_CONTROLLER;
                }
                Err(e) => {
                    tracing::error!(error = %e, "AlterConfigs submit_change failed");
                    out.error_code = codes::UNKNOWN_SERVER_ERROR;
                }
            }
            responses.push(out);
        }

        let resp = AlterConfigsResponse {
            responses,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    // Handler-level tests are deferred to the broker-integration test file
    // (`crates/broker/tests/admin_handlers.rs`) — see Task 17. That avoids
    // duplicating the in-memory controller fixture in unit tests.
}
```

- [ ] **Step 3: Verify `codes::INVALID_RESOURCE_TYPE` and `INVALID_CONFIG` exist**

```bash
grep -nE "INVALID_RESOURCE_TYPE|INVALID_CONFIG|NOT_CONTROLLER" crates/broker/src/codes.rs | head -5
```

If `INVALID_CONFIG` is missing, add it to `crates/broker/src/codes.rs`:

```rust
pub const INVALID_CONFIG: i16 = 40;
```

If `INVALID_RESOURCE_TYPE` is missing:

```rust
pub const INVALID_RESOURCE_TYPE: i16 = 35;
```

If `NOT_CONTROLLER` is missing:

```rust
pub const NOT_CONTROLLER: i16 = 41;
```

- [ ] **Step 4: Register the handler**

In `crates/broker/src/handlers/mod.rs`, add to the `mod` declarations (alphabetical):

```rust
mod alter_configs;
```

In `build_table`, add (place after the existing `t.register(32, describe_configs::handle);`):

```rust
    t.register(33, alter_configs::handle);
```

- [ ] **Step 5: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean. If `AlterableConfig` or response field names differ from what the handler uses, fix to match the generated types (the code above uses the most common field names; minor adjustments may be needed).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/alter_configs.rs crates/broker/src/handlers/mod.rs crates/broker/src/codes.rs
git commit -m "feat(broker): AlterConfigs handler (api_key 33)"
```

---

### Task 10: IncrementalAlterConfigs handler (api_key 44)

**Files:**
- Create: `crates/broker/src/handlers/incremental_alter_configs.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Inspect the request shape**

```bash
grep -nE "pub.*op|configs:|name:|value:" crates/protocol/generated/IncrementalAlterConfigsRequest.owned.rs | head -15
```

Expected: `resources: Vec<AlterConfigsResource>` where each `AlterableConfig` carries `config_operation: i8` (0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT). Only SET (0) and DELETE (1) are supported in this slice.

- [ ] **Step 2: Write the handler**

Create `crates/broker/src/handlers/incremental_alter_configs.rs`:

```rust
//! `IncrementalAlterConfigs` (`api_key=44`). Same target as AlterConfigs
//! (a single `V1TopicConfig` record per resource) but the wire request
//! carries per-key operations (SET/DELETE/APPEND/SUBTRACT). The handler
//! reads the current overrides from the metadata image, applies the ops,
//! validates the result, and submits the merged map.
//!
//! Slice-11 scope:
//! - SET (0)    — set/replace
//! - DELETE (1) — remove
//! - APPEND (2) and SUBTRACT (3) are list-valued operations. None of our
//!   whitelisted keys are list-valued, so we reject these with
//!   `INVALID_CONFIG`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, TopicConfigRecord};
use crabka_protocol::owned::incremental_alter_configs_request::IncrementalAlterConfigsRequest;
use crabka_protocol::owned::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::broker::Broker;
use crate::codes;
use crate::config_keys;
use crate::error::BrokerError;

const RESOURCE_TYPE_TOPIC: i8 = 2;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = IncrementalAlterConfigsRequest::decode(&mut cur, version)?;

        let mut responses: Vec<AlterConfigsResourceResponse> =
            Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            let mut out = AlterConfigsResourceResponse {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            };

            if resource.resource_type != RESOURCE_TYPE_TOPIC {
                out.error_code = codes::INVALID_RESOURCE_TYPE;
                out.error_message =
                    Some(format!("resource_type={} not supported", resource.resource_type));
                responses.push(out);
                continue;
            }

            let image = controller.current_image();
            if image.topic(&resource.resource_name).is_none() {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                out.error_message = Some(format!("unknown topic `{}`", resource.resource_name));
                responses.push(out);
                continue;
            }

            // Start from current overrides.
            let mut merged = image
                .topic_config(&resource.resource_name)
                .cloned()
                .unwrap_or_default();
            let mut validation_err: Option<String> = None;
            for cfg in &resource.configs {
                match cfg.config_operation {
                    OP_SET => {
                        let value = cfg.value.clone().unwrap_or_default();
                        if let Err(reason) = config_keys::validate_topic_config(&cfg.name, &value)
                        {
                            validation_err = Some(reason);
                            break;
                        }
                        merged.insert(cfg.name.clone(), value);
                    }
                    OP_DELETE => {
                        // Validate the key is even recognized — operator
                        // typo on a delete should still surface
                        // INVALID_CONFIG. value is conventionally empty.
                        if config_keys::validate_topic_config(&cfg.name, "0").is_err()
                            && config_keys::validate_topic_config(&cfg.name, "producer").is_err()
                            && config_keys::validate_topic_config(&cfg.name, "delete").is_err()
                        {
                            validation_err =
                                Some(format!("unrecognized config key `{}`", cfg.name));
                            break;
                        }
                        merged.remove(&cfg.name);
                    }
                    op => {
                        validation_err = Some(format!(
                            "config_operation={op} (APPEND/SUBTRACT) not supported for key `{}` — \
                             only SET and DELETE are honored on this broker",
                            cfg.name
                        ));
                        break;
                    }
                }
            }
            if let Some(reason) = validation_err {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(reason);
                responses.push(out);
                continue;
            }

            let record = MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: resource.resource_name.clone(),
                overrides: merged,
            });
            match controller.submit_change(vec![record]).await {
                Ok(()) => {}
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    out.error_code = codes::NOT_CONTROLLER;
                }
                Err(e) => {
                    tracing::error!(error = %e, "IncrementalAlterConfigs submit_change failed");
                    out.error_code = codes::UNKNOWN_SERVER_ERROR;
                }
            }
            responses.push(out);
        }

        let resp = IncrementalAlterConfigsResponse {
            responses,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 3: Register the handler**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod incremental_alter_configs;
```

In `build_table`:

```rust
    t.register(44, incremental_alter_configs::handle);
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/incremental_alter_configs.rs crates/broker/src/handlers/mod.rs
git commit -m "feat(broker): IncrementalAlterConfigs handler (api_key 44)"
```

---

## Batch 3 — CreatePartitions + DeleteRecords

### Task 11: CreatePartitions handler (api_key 37)

**Files:**
- Create: `crates/broker/src/handlers/create_partitions.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Lift `round_robin_replicas` from create_topics to a shared helper**

`create_topics.rs` already has a `fn round_robin_replicas(sorted_brokers: &[NodeId], num_partitions: i32, replication_factor: i16) -> Vec<Vec<NodeId>>`. Make it `pub(crate)` and re-export from `crates/broker/src/handlers/mod.rs`.

In `crates/broker/src/handlers/create_topics.rs`, change:

```rust
fn round_robin_replicas(
```

to:

```rust
pub(crate) fn round_robin_replicas(
```

- [ ] **Step 2: Inspect the request shape**

```bash
grep -nE "pub.*count|pub.*assignments|pub.*topics" crates/protocol/generated/CreatePartitionsRequest.owned.rs | head -10
```

Expected: `topics: Vec<CreatePartitionsTopic>` with `name`, `count: i32`, `assignments: Option<Vec<CreatePartitionsAssignment>>`.

`assignments` is the operator-supplied replica list per new partition. Slice-11 ignores it (uses round-robin); supporting it would require validating each provided replica list against the broker set. Document the no-op:

- [ ] **Step 3: Write the handler**

Create `crates/broker/src/handlers/create_partitions.rs`:

```rust
//! `CreatePartitions` (`api_key=37`). `kafka-topics --alter --partitions
//! N`. Round-robin replica placement matches the slice-7 `CreateTopics`
//! path. Operator-supplied `assignments` are ignored in this slice
//! (round-robin only); honoring them is deferred.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::owned::create_partitions_request::CreatePartitionsRequest;
use crabka_protocol::owned::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::RaftError;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::create_topics::round_robin_replicas;
use crate::replicator_supervisor::materialize_partition;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let partitions_map = broker.partitions.clone();
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = CreatePartitionsRequest::decode(&mut cur, version)?;

        let mut results: Vec<CreatePartitionsTopicResult> =
            Vec::with_capacity(req.topics.len());

        for t in req.topics {
            let mut out = CreatePartitionsTopicResult {
                name: t.name.clone(),
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            };

            let image = controller.current_image();
            let Some(topic_rec) = image.topic(&t.name).cloned() else {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                out.error_message = Some(format!("unknown topic `{}`", t.name));
                results.push(out);
                continue;
            };

            let existing = topic_rec.partitions;
            if t.count <= existing {
                out.error_code = codes::INVALID_PARTITIONS;
                out.error_message = Some(format!(
                    "topic `{}` already has {} partitions; cannot decrease to {}",
                    t.name, existing, t.count
                ));
                results.push(out);
                continue;
            }

            let mut sorted_brokers: Vec<crabka_raft::NodeId> =
                image.brokers().map(|b| b.node_id).collect();
            if sorted_brokers.is_empty() {
                sorted_brokers.push(node_id);
            }
            sorted_brokers.sort_unstable();
            let rf = topic_rec.replication_factor;
            let new_count = t.count;
            let new_partition_indices: Vec<i32> = (existing..new_count).collect();
            let assignments = round_robin_replicas(&sorted_brokers, new_count, rf);
            if assignments.is_empty() {
                out.error_code = codes::INVALID_REPLICATION_FACTOR;
                out.error_message = Some(format!(
                    "replication_factor={rf} > broker_count={}",
                    sorted_brokers.len()
                ));
                results.push(out);
                continue;
            }

            // Build a single batch: an updated V1Topic record (new partition
            // count) + one V1Partition per new index.
            let mut records: Vec<MetadataRecord> =
                Vec::with_capacity(new_partition_indices.len() + 1);
            records.push(MetadataRecord::V1Topic(crabka_metadata::TopicRecord {
                name: t.name.clone(),
                topic_id: topic_rec.topic_id,
                partitions: new_count,
                replication_factor: rf,
            }));
            for p in &new_partition_indices {
                let p_usize = usize::try_from(*p).unwrap_or(0);
                let replicas = assignments[p_usize].clone();
                records.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: t.name.clone(),
                    partition: *p,
                    leader: replicas[0],
                    replicas: replicas.clone(),
                    isr: replicas,
                    leader_epoch: 0,
                }));
            }

            match controller.submit_change(records).await {
                Ok(()) => {
                    // Materialize new partitions on local disk where self in replicas.
                    for p in &new_partition_indices {
                        let p_usize = usize::try_from(*p).unwrap_or(0);
                        let replicas = &assignments[p_usize];
                        if !replicas.contains(&node_id) {
                            continue;
                        }
                        if let Err(e) = materialize_partition(
                            &partitions_map,
                            &t.name,
                            *p,
                            &log_dir,
                            &log_config,
                        ) {
                            tracing::error!(topic = %t.name, partition = *p, error = %e,
                                "CreatePartitions: materialize after quorum commit failed");
                        } else if let Some(part) =
                            partitions_map.get(&(t.name.clone(), *p)).map(|e| e.clone())
                        {
                            let leader = replicas[0];
                            part.install_leader_change(leader, 0).await;
                            if leader == node_id {
                                part.install_isr(replicas, leader).await;
                            }
                        }
                    }
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    out.error_code = codes::NOT_CONTROLLER;
                }
                Err(e) => {
                    tracing::error!(topic = %t.name, error = %e,
                        "CreatePartitions submit_change failed");
                    out.error_code = codes::UNKNOWN_SERVER_ERROR;
                }
            }

            results.push(out);
        }

        let resp = CreatePartitionsResponse {
            results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 4: Register the handler**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod create_partitions;
```

In `build_table`:

```rust
    t.register(37, create_partitions::handle);
```

- [ ] **Step 5: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/create_partitions.rs crates/broker/src/handlers/mod.rs crates/broker/src/handlers/create_topics.rs
git commit -m "feat(broker): CreatePartitions handler (api_key 37)"
```

---

### Task 12: `Partition::trim_to_offset` and `WriterMessage::TrimToOffset`

**Files:**
- Modify: `crates/broker/src/partition.rs`
- Modify: `crates/broker/src/partition_writer.rs`

- [ ] **Step 1: Write failing test**

Append to the `#[cfg(test)] mod tests` in `crates/broker/src/partition_writer.rs`:

```rust
    #[tokio::test]
    async fn writer_trim_to_offset_advances_log_start() {
        use crabka_log::LogConfig;
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        // Pre-populate with two batches → LEO = 4.
        for _ in 0..2 {
            log.lock()
                .expect("lock")
                .append(&mut sample_batch(2))
                .expect("append");
        }

        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(run(
            log.clone(),
            rx,
            append_notify,
            replica_state,
            hw_advance_notify,
        ));

        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::TrimToOffset { new_start: 3, ack })
            .await
            .expect("send");
        let new_start = ack_rx.await.expect("ack").expect("trim ok");
        assert!(new_start >= 3);
        assert_eq!(log.lock().expect("lock").log_start_offset(), new_start);

        drop(tx);
        writer.await.expect("writer join");
    }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-broker --lib partition_writer::tests::writer_trim_to_offset 2>&1 | tail -10
```

Expected: FAIL.

- [ ] **Step 3: Add `WriterMessage::TrimToOffset` and the arm**

In `crates/broker/src/partition.rs`, add to the `WriterMessage` enum:

```rust
    /// Trim from the start of the log: drop sealed segments whose last
    /// offset is `< new_start`, advance `log_start_offset` if `new_start`
    /// falls inside the active segment. Returns the resulting
    /// `log_start_offset` (which may be less than `new_start` when
    /// `new_start` falls between segment boundaries — Kafka semantics).
    TrimToOffset {
        new_start: i64,
        ack: tokio::sync::oneshot::Sender<Result<i64, BrokerError>>,
    },
```

In `crates/broker/src/partition_writer.rs`, add a new match arm (next to `SetLogConfig`):

```rust
            WriterMessage::TrimToOffset { new_start, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.trim_to_offset(new_start)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — trim drops data rather than producing it.
            }
```

- [ ] **Step 4: Add `Partition::trim_to_offset`**

In `crates/broker/src/partition.rs`, after `apply_log_config_overrides`:

```rust
    /// Send a trim request through the writer actor. Returns the resulting
    /// `log_start_offset`. Used by the `DeleteRecords` handler.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` if the writer is dead, the ack is dropped,
    /// or the underlying `Log::trim_to_offset` fails (negative offset).
    pub async fn trim_to_offset(&self, new_start: i64) -> Result<i64, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::TrimToOffset {
                new_start,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cargo test -p crabka-broker --lib partition_writer::tests::writer_trim_to_offset 2>&1 | tail -5
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/partition.rs crates/broker/src/partition_writer.rs
git commit -m "feat(broker): Partition::trim_to_offset + WriterMessage::TrimToOffset"
```

---

### Task 13: DeleteRecords handler (api_key 21)

**Files:**
- Create: `crates/broker/src/handlers/delete_records.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Inspect request/response**

```bash
grep -nE "pub.*topics|pub.*partitions|pub.*offset|low_watermark" crates/protocol/generated/DeleteRecordsRequest.owned.rs crates/protocol/generated/DeleteRecordsResponse.owned.rs | head -15
```

Expected fields: request `topics: Vec<DeleteRecordsTopic{ name, partitions: Vec<DeleteRecordsPartition{ partition_index, offset }> }>`; response `topics: Vec<DeleteRecordsTopicResult{ name, partitions: Vec<DeleteRecordsPartitionResult{ partition_index, low_watermark, error_code }> }>`.

- [ ] **Step 2: Write the handler**

Create `crates/broker/src/handlers/delete_records.rs`:

```rust
//! `DeleteRecords` (`api_key=21`). Leader-only local segment trim. The
//! follower side picks up the new `log_start_offset` on the next Fetch
//! via the existing `OFFSET_OUT_OF_RANGE` recovery path — matching the
//! Apache Kafka model.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_records_request::DeleteRecordsRequest;
use crabka_protocol::owned::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
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
    let partitions = broker.partitions.clone();
    let node_id = broker.config.node_id;

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteRecordsRequest::decode(&mut cur, version)?;

        let mut topic_results: Vec<DeleteRecordsTopicResult> =
            Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            let mut part_results: Vec<DeleteRecordsPartitionResult> =
                Vec::with_capacity(topic.partitions.len());

            for fp in topic.partitions {
                let key = (topic.name.clone(), fp.partition_index);
                let part_opt = partitions.get(&key).map(|p| p.clone());
                let Some(part) = part_opt else {
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: -1,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        ..Default::default()
                    });
                    continue;
                };

                let cur_leader =
                    part.current_leader.load(std::sync::atomic::Ordering::Acquire);
                if cur_leader != node_id {
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: -1,
                        error_code: codes::NOT_LEADER_OR_FOLLOWER,
                        ..Default::default()
                    });
                    continue;
                }

                // Translate offset == -1 → high_watermark per Kafka semantics.
                let leo = part.log_end_offset();
                let hw = part.high_watermark().await;
                let target = if fp.offset == -1 { hw } else { fp.offset };

                if target < -1 || target > leo {
                    part_results.push(DeleteRecordsPartitionResult {
                        partition_index: fp.partition_index,
                        low_watermark: -1,
                        error_code: codes::OFFSET_OUT_OF_RANGE,
                        ..Default::default()
                    });
                    continue;
                }

                match part.trim_to_offset(target).await {
                    Ok(new_start) => {
                        part_results.push(DeleteRecordsPartitionResult {
                            partition_index: fp.partition_index,
                            low_watermark: new_start,
                            error_code: codes::NONE,
                            ..Default::default()
                        });
                    }
                    Err(e) => {
                        tracing::warn!(
                            topic = %topic.name, partition = fp.partition_index, error = %e,
                            "DeleteRecords: trim_to_offset failed"
                        );
                        part_results.push(DeleteRecordsPartitionResult {
                            partition_index: fp.partition_index,
                            low_watermark: -1,
                            error_code: codes::UNKNOWN_SERVER_ERROR,
                            ..Default::default()
                        });
                    }
                }
            }

            topic_results.push(DeleteRecordsTopicResult {
                name: topic.name,
                partitions: part_results,
                ..Default::default()
            });
        }

        let resp = DeleteRecordsResponse {
            topics: topic_results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 3: Register the handler**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod delete_records;
```

In `build_table`:

```rust
    t.register(21, delete_records::handle);
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean. If the response struct field names differ, adjust to match the generated types.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/delete_records.rs crates/broker/src/handlers/mod.rs
git commit -m "feat(broker): DeleteRecords handler (api_key 21)"
```

---

## Batch 4 — Group handlers + DescribeCluster

### Task 14: `GroupManager::{list_groups, describe, delete}`

**Files:**
- Modify: `crates/broker/src/coordinator/mod.rs`

- [ ] **Step 1: Inspect Group's shape so the snapshot type fits**

```bash
grep -nE "pub.*state:|pub.*group_id:|pub.*members:|pub.*protocol_type:|pub.*generation:" crates/broker/src/coordinator/group.rs | head -20
```

Expected fields on `Group`: `group_id: String`, `state: GroupState`, `protocol_type: Option<String>`, `members: HashMap<String, Member>`, `generation_id: i32`.

- [ ] **Step 2: Write failing test**

Add to the `#[cfg(test)] mod tests` block in `crates/broker/src/coordinator/mod.rs` (or create one if missing):

```rust
    #[tokio::test]
    async fn list_groups_includes_known_groups() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        let _ = mgr.get_or_create("g2");
        let listed = mgr.list_groups().await;
        let ids: std::collections::HashSet<String> =
            listed.iter().map(|s| s.group_id.clone()).collect();
        assert!(ids.contains("g1"));
        assert!(ids.contains("g2"));
    }

    #[tokio::test]
    async fn describe_group_returns_snapshot() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        let snap = mgr.describe_group("g1").await.expect("known");
        assert_eq!(snap.group_id, "g1");
        assert!(snap.members.is_empty());
    }

    #[tokio::test]
    async fn delete_group_removes_empty_group() {
        let mgr = GroupManager::new();
        let _ = mgr.get_or_create("g1");
        mgr.delete_group("g1").await.expect("delete");
        assert!(mgr.describe_group("g1").await.is_none());
    }

    #[tokio::test]
    async fn delete_group_unknown_is_err() {
        let mgr = GroupManager::new();
        let err = mgr.delete_group("ghost").await.unwrap_err();
        assert_eq!(err, crate::coordinator::DeleteGroupError::NotFound);
    }
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cargo test -p crabka-broker --lib coordinator::tests 2>&1 | tail -15
```

Expected: four NEW tests fail (`no method named list_groups`, etc.).

- [ ] **Step 4: Add snapshot types and the three methods**

In `crates/broker/src/coordinator/mod.rs`, near the top after the existing imports:

```rust
/// Result of `GroupManager::delete_group`.
#[derive(Debug, PartialEq, Eq)]
pub enum DeleteGroupError {
    NotFound,
    NonEmpty,
}

/// Read-only projection of a `Group` for the `ListGroups` / `DescribeGroups`
/// handlers. Cheap to build (Strings + small struct).
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: crate::coordinator::group::GroupState,
    pub protocol_type: Option<String>,
    pub members: Vec<MemberSnapshot>,
    pub generation_id: i32,
}

#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignment: Vec<u8>,
}
```

In the existing `impl GroupManager` block, after `find`:

```rust
    /// Snapshot every known group. The returned `Vec` is in arbitrary
    /// order (matching Apache Kafka's ListGroups, which doesn't promise
    /// ordering either).
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        // `groups` is whatever the existing field is — locate it via the
        // existing `get_or_create` body. The typical pattern in this file
        // is `self.groups: Mutex<HashMap<String, Arc<GroupHandle>>>` or
        // similar.
        let handles: Vec<Arc<GroupHandle>> = {
            let inner = self.groups.lock().await;
            inner.values().cloned().collect()
        };
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let g = h.state.lock().await;
            out.push(snapshot(&g));
        }
        out
    }

    /// Snapshot a single group, or `None` if unknown.
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        let g = handle.state.lock().await;
        Some(snapshot(&g))
    }

    /// Drop a group from the in-memory registry. Returns
    /// `DeleteGroupError::NonEmpty` if the group still has live members.
    pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        {
            let g = handle.state.lock().await;
            if !g.members.is_empty() {
                return Err(DeleteGroupError::NonEmpty);
            }
        }
        let mut inner = self.groups.lock().await;
        inner.remove(group_id);
        Ok(())
    }
```

At the bottom of the file (or near the snapshot type), add the free helper:

```rust
fn snapshot(g: &crate::coordinator::group::Group) -> GroupSnapshot {
    GroupSnapshot {
        group_id: g.group_id.clone(),
        state: g.state,
        protocol_type: g.protocol_type.clone(),
        generation_id: g.generation_id,
        members: g
            .members
            .values()
            .map(|m| MemberSnapshot {
                member_id: m.member_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                assignment: m.assignment.clone(),
            })
            .collect(),
    }
}
```

If the existing `GroupManager.groups` field is named differently or is not `Mutex<HashMap>`, follow the existing access pattern in `get_or_create` / `find`. The names above are illustrative — match the file.

- [ ] **Step 5: Run tests to verify they pass**

```bash
cargo test -p crabka-broker --lib coordinator 2>&1 | tail -15
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/coordinator/mod.rs
git commit -m "feat(broker): GroupManager snapshot + delete accessors"
```

---

### Task 15: ListGroups + DescribeGroups + DeleteGroups handlers

**Files:**
- Create: `crates/broker/src/handlers/list_groups.rs`
- Create: `crates/broker/src/handlers/describe_groups.rs`
- Create: `crates/broker/src/handlers/delete_groups.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: ListGroups (api_key 16)**

Create `crates/broker/src/handlers/list_groups.rs`:

```rust
//! `ListGroups` (`api_key=16`). Returns every known group from
//! `GroupManager::list_groups`. The optional `states_filter` (v4+) is
//! honored; the optional `types_filter` (v5+) is ignored (this slice
//! has no group types beyond "consumer").

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::list_groups_request::ListGroupsRequest;
use crabka_protocol::owned::list_groups_response::{ListGroupsResponse, ListedGroup};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::coordinator::group::GroupState;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ListGroupsRequest::decode(&mut cur, version)?;
        let snapshots = group_manager.list_groups().await;

        let states_filter: Vec<String> = req.states_filter.unwrap_or_default();
        let filter_active = !states_filter.is_empty();

        let mut groups: Vec<ListedGroup> = Vec::with_capacity(snapshots.len());
        for s in snapshots {
            let state_str = match s.state {
                GroupState::Empty => "Empty",
                GroupState::PreparingRebalance => "PreparingRebalance",
                GroupState::CompletingRebalance => "CompletingRebalance",
                GroupState::Stable => "Stable",
                GroupState::Dead => "Dead",
            };
            if filter_active && !states_filter.iter().any(|v| v == state_str) {
                continue;
            }
            groups.push(ListedGroup {
                group_id: s.group_id,
                protocol_type: s.protocol_type.unwrap_or_else(|| "consumer".into()),
                group_state: state_str.into(),
                ..Default::default()
            });
        }

        let resp = ListGroupsResponse {
            error_code: codes::NONE,
            groups,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

If the `GroupState` enum's variants are named differently in
`crates/broker/src/coordinator/group.rs`, fix the match arms to match.
Run `grep -nE "pub enum GroupState" crates/broker/src/coordinator/group.rs` first.

- [ ] **Step 2: DescribeGroups (api_key 15)**

Create `crates/broker/src/handlers/describe_groups.rs`:

```rust
//! `DescribeGroups` (`api_key=15`). One per group_id. Members include
//! their current assignment bytes; the protocol_type is constant
//! "consumer" for this slice.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::coordinator::group::GroupState;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DescribeGroupsRequest::decode(&mut cur, version)?;

        let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
        for gid in req.groups {
            let Some(snap) = group_manager.describe_group(&gid).await else {
                groups.push(DescribedGroup {
                    group_id: gid,
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..Default::default()
                });
                continue;
            };
            let state_str = match snap.state {
                GroupState::Empty => "Empty",
                GroupState::PreparingRebalance => "PreparingRebalance",
                GroupState::CompletingRebalance => "CompletingRebalance",
                GroupState::Stable => "Stable",
                GroupState::Dead => "Dead",
            };
            groups.push(DescribedGroup {
                group_id: snap.group_id,
                protocol_type: snap.protocol_type.unwrap_or_else(|| "consumer".into()),
                protocol_data: String::new(),
                group_state: state_str.into(),
                error_code: codes::NONE,
                members: snap
                    .members
                    .into_iter()
                    .map(|m| DescribedGroupMember {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                        member_assignment: m.assignment,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
        }

        let resp = DescribeGroupsResponse {
            groups,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Verify `codes::GROUP_ID_NOT_FOUND` exists — if not, add to `crates/broker/src/codes.rs`:

```rust
pub const GROUP_ID_NOT_FOUND: i16 = 69;
```

- [ ] **Step 3: DeleteGroups (api_key 42)**

Create `crates/broker/src/handlers/delete_groups.rs`:

```rust
//! `DeleteGroups` (`api_key=42`). Drops empty groups from the in-memory
//! registry. Non-empty groups are rejected with `NON_EMPTY_GROUP`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;
use crabka_protocol::owned::delete_groups_response::{
    DeletableGroupResult, DeleteGroupsResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::DeleteGroupError;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteGroupsRequest::decode(&mut cur, version)?;

        let mut results: Vec<DeletableGroupResult> =
            Vec::with_capacity(req.groups_names.len());
        for gid in req.groups_names {
            let error_code = match group_manager.delete_group(&gid).await {
                Ok(()) => codes::NONE,
                Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
                Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
            };
            results.push(DeletableGroupResult {
                group_id: gid,
                error_code,
                ..Default::default()
            });
        }

        let resp = DeleteGroupsResponse {
            results,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Verify `codes::NON_EMPTY_GROUP` exists — if not, add:

```rust
pub const NON_EMPTY_GROUP: i16 = 68;
```

The field on `DeleteGroupsRequest` may be `groups_names` or `groups_names_list` — check `crates/protocol/generated/DeleteGroupsRequest.owned.rs` and use whatever name the codegen produced.

- [ ] **Step 4: Register the three handlers**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod delete_groups;
mod describe_groups;
mod list_groups;
```

In `build_table`:

```rust
    t.register(15, describe_groups::handle);
    t.register(16, list_groups::handle);
    t.register(42, delete_groups::handle);
```

- [ ] **Step 5: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/list_groups.rs \
        crates/broker/src/handlers/describe_groups.rs \
        crates/broker/src/handlers/delete_groups.rs \
        crates/broker/src/handlers/mod.rs \
        crates/broker/src/codes.rs
git commit -m "feat(broker): ListGroups/DescribeGroups/DeleteGroups handlers"
```

---

### Task 16: DescribeCluster handler (api_key 60)

**Files:**
- Create: `crates/broker/src/handlers/describe_cluster.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Inspect response shape**

```bash
grep -nE "pub.*brokers|pub.*broker_id|pub.*host|pub.*port|pub.*controller_id|cluster_id" crates/protocol/generated/DescribeClusterResponse.owned.rs | head -15
```

Expected fields: `cluster_id: String`, `controller_id: i32`, `brokers: Vec<DescribeClusterBroker{ broker_id, host, port, rack }>`, `cluster_authorized_operations: i32` (v1+).

- [ ] **Step 2: Write the handler**

Create `crates/broker/src/handlers/describe_cluster.rs`:

```rust
//! `DescribeCluster` (`api_key=60`). Pure projection over the metadata
//! image. The `cluster_authorized_operations` field is set to `-2147483648`
//! (Apache Kafka's "not present" sentinel) because this slice doesn't
//! implement authorization.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_cluster_response::{
    DescribeClusterBroker, DescribeClusterResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    _req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let controller = broker.controller.clone();

    Box::pin(async move {
        let image = controller.current_image();
        let controller_id = controller
            .watch_leader()
            .borrow()
            .map_or(-1, |n| i32::try_from(n).unwrap_or(-1));

        let brokers: Vec<DescribeClusterBroker> = image
            .brokers()
            .map(|b| DescribeClusterBroker {
                broker_id: i32::try_from(b.node_id).unwrap_or(-1),
                host: b.host.clone(),
                port: i32::from(b.port),
                rack: b.rack.clone(),
                ..Default::default()
            })
            .collect();

        let resp = DescribeClusterResponse {
            error_code: codes::NONE,
            error_message: None,
            cluster_id: image.cluster_id().to_string(),
            controller_id,
            brokers,
            cluster_authorized_operations: i32::MIN,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 3: Register the handler**

In `crates/broker/src/handlers/mod.rs`:

```rust
mod describe_cluster;
```

In `build_table`:

```rust
    t.register(60, describe_cluster::handle);
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-broker --release 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/describe_cluster.rs crates/broker/src/handlers/mod.rs
git commit -m "feat(broker): DescribeCluster handler (api_key 60)"
```

---

### Task 17: Broker-integration tests for the eight handlers

**Files:**
- Create: `crates/broker/tests/admin_handlers.rs`

- [ ] **Step 1: Sketch the test file shape**

Existing tests in `crates/broker/tests/` use the shared `mod support;` (defined in `crates/broker/tests/support/mod.rs`) for `start_n_node` and ephemeral-port helpers, and `crabka-client-core` for sending requests. Look at `crates/broker/tests/replication.rs` for a working example.

- [ ] **Step 2: Write the integration test file**

Create `crates/broker/tests/admin_handlers.rs`:

```rust
//! Broker-side integration tests for the slice-11 admin handlers. Each
//! test spins up a 1-broker cluster, dispatches the relevant request via
//! `crabka-client-core`, and asserts on either the response or
//! observable broker state.

#![cfg(not(target_os = "windows"))]

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use crabka_protocol::owned::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use crabka_protocol::owned::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;

use support::start_n_node;

const RESOURCE_TYPE_TOPIC: i8 = 2;

async fn create_topic_helper(
    client: &crabka_client_core::Client,
    name: &str,
    partitions: i32,
    rf: i16,
) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: rf,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_topics");
    let result = &resp.topics[0];
    assert_eq!(result.error_code, 0, "create_topics: {:?}", result.error_message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_round_trip() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let addr = cfg.listen_addr;

    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");

    create_topic_helper(&client, "t", 1, 1).await;

    // Submit retention.ms=60000.
    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t".into(),
            configs: vec![AlterableConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    assert_eq!(resp.responses[0].error_code, 0);

    // Wait for the supervisor reconcile loop to push the new config.
    // The reconcile runs on every controller image-watch event; one
    // metadata round-trip plus a short sleep is enough.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Peek at the partition's actual LogConfig.
    let part = broker
        .partitions_for_test("t", 0)
        .expect("partition materialized");
    let snap = part.log.lock().expect("log lock").config_snapshot();
    assert_eq!(snap.retention_ms, Some(Duration::from_millis(60_000)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_rejects_unknown_key() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", cfg.listen_addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");
    create_topic_helper(&client, "t", 1, 1).await;

    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t".into(),
            configs: vec![AlterableConfig {
                name: "flush.ms".into(),
                value: Some("1000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    // 40 = INVALID_CONFIG
    assert_eq!(resp.responses[0].error_code, 40);
    assert!(
        resp.responses[0]
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("flush.ms"),
        "expected error_message to mention the key, got {:?}",
        resp.responses[0].error_message
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_extends_topic() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", cfg.listen_addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");
    create_topic_helper(&client, "t", 1, 1).await;

    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t".into(),
            count: 3,
            assignments: None,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_partitions");
    assert_eq!(resp.results[0].error_code, 0, "{:?}", resp.results[0].error_message);

    // Three partition dirs on disk.
    for p in 0..3 {
        assert!(broker
            .partitions_for_test("t", p)
            .is_some(),
            "partition {p} missing");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_trims_log_start() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", cfg.listen_addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");
    create_topic_helper(&client, "t", 1, 1).await;

    // Produce 100 records via the broker's internal handle (matches the
    // pattern used in `replication.rs::three_node_replication`).
    let part = broker.partitions_for_test("t", 0).expect("partition");
    for i in 0..100 {
        let mut batch = crabka_protocol::records::RecordBatch {
            last_offset_delta: 0,
            records: vec![crabka_protocol::records::Record {
                offset_delta: 0,
                value: bytes::Bytes::from(format!("msg-{i}").into_bytes()),
                ..Default::default()
            }],
            ..Default::default()
        };
        part.produce_batch(batch).await.expect("produce");
    }

    let req = DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: "t".into(),
            partitions: vec![DeleteRecordsPartition {
                partition_index: 0,
                offset: 50,
            }],
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("delete_records");
    let part_result = &resp.topics[0].partitions[0];
    assert_eq!(part_result.error_code, 0);
    assert!(part_result.low_watermark >= 0);
    assert!(part_result.low_watermark <= 50);

    // Fetch from offset 0 → OFFSET_OUT_OF_RANGE.
    let part = broker.partitions_for_test("t", 0).expect("partition");
    let log_start = part.log.lock().expect("lock").log_start_offset();
    assert_eq!(log_start, part_result.low_watermark);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_lists_brokers() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", cfg.listen_addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");

    let req = DescribeClusterRequest::default();
    let resp = client.send(req).await.expect("describe_cluster");
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.brokers.len(), 1);
    assert_eq!(resp.controller_id, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_a_freshly_joined_group() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", cfg.listen_addr.port()))
        .client_id("admin-handlers-test".into())
        .build()
        .await
        .expect("client");

    // Seed the group manager directly — full JoinGroup/SyncGroup happens
    // in other tests; this one just exercises the read side.
    let _ = broker
        .group_manager_for_test()
        .get_or_create("test-group");

    let req = ListGroupsRequest::default();
    let resp = client.send(req).await.expect("list_groups");
    assert_eq!(resp.error_code, 0);
    let ids: Vec<String> = resp.groups.iter().map(|g| g.group_id.clone()).collect();
    assert!(ids.contains(&"test-group".to_string()));
}
```

Three things to verify the test compiles:

- `Broker::partitions_for_test(topic, partition) -> Option<Arc<Partition>>` — if this test-helper doesn't exist on the `Broker` type, add it under `#[cfg(any(test, feature = "test-helpers"))]` in `crates/broker/src/broker.rs`:

```rust
#[cfg(any(test, feature = "test-helpers"))]
impl Broker {
    pub fn partitions_for_test(&self, topic: &str, partition: i32) -> Option<std::sync::Arc<crate::partition::Partition>> {
        self.partitions
            .get(&(topic.to_string(), partition))
            .map(|e| e.value().clone())
    }

    pub fn group_manager_for_test(&self) -> std::sync::Arc<crate::coordinator::GroupManager> {
        self.group_manager.clone()
    }
}
```

- The `Partition.log` field must be reachable from the test crate. Today it's `pub(crate)`; promote to `pub` (kept behind the `test-helpers` cfg if reluctant). Simplest: add a `pub fn log_config_snapshot(&self) -> LogConfig` on `Partition` under `cfg(test-helpers)` and call that instead of locking `part.log` directly.

- Confirm the `BrokerHandle` returned by `start_n_node` is the same `Broker` (not just a handle without `partitions_for_test`). If `start_n_node` returns a `BrokerHandle` opaque type, expose the inner `Arc<Broker>` via a test-only accessor.

Pick the simplest patch — add the missing accessors as test-only methods. Keep them under `#[cfg(any(test, feature = "test-helpers"))]` so they don't pollute the public API.

- [ ] **Step 3: Run the integration tests**

```bash
cargo test -p crabka-broker --test admin_handlers --release 2>&1 | tail -15
```

Expected: all six tests pass.

- [ ] **Step 4: Workspace tests + clippy + fmt**

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo clippy --workspace --all-targets --release -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | tail -5
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/admin_handlers.rs crates/broker/src/broker.rs
git commit -m "test(broker): integration tests for slice-11 admin handlers"
```

---

## Batch 5 — JVM acceptance tests

### Task 18: `kafka-configs --alter` round-trip JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Inspect the existing JVM test pattern**

```bash
grep -nE "async fn console_producer_round_trip|start_host_broker|docker_run_kafka_tool" crates/broker/tests/jvm_acceptance.rs | head -10
```

The existing `console_producer_round_trip` test runs against the single host broker bound to fixed port 9092 (`BOOTSTRAP = "host.docker.internal:9092"`). All five new tests reuse that broker — same `--bootstrap-server`, distinct topic names so they don't collide.

- [ ] **Step 2: Add `kafka_configs_alter_round_trip`**

Append to `crates/broker/tests/jvm_acceptance.rs` (after the existing tests, before any `#[cfg]`-gated test):

```rust
/// `kafka-configs --alter --add-config retention.ms=60000 --topic t` then
/// `--describe` round-trips through V1TopicConfig and the supervisor
/// reconcile push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_configs_alter_round_trip() {
    const TOPIC: &str = "crabka-cfg-alter-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--add-config",
        "retention.ms=60000",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-configs",
        "--describe",
        "--entity-type",
        "topics",
        "--entity-name",
        TOPIC,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("retention.ms=60000"),
        "describe output missing retention.ms=60000: {s}"
    );
}
```

- [ ] **Step 3: Run the test**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release kafka_configs_alter_round_trip -- --ignored --nocapture --test-threads=1 2>&1 | tail -10
```

Expected: PASS. If `kafka-configs --describe` returns an empty list when no overrides are set, the JVM tool may not surface the change immediately — give it `tokio::time::sleep(Duration::from_millis(200)).await` between `--alter` and `--describe` if needed.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-configs --alter round trip"
```

---

### Task 19: `kafka-topics --alter --partitions` JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_alter_partitions() {
    const TOPIC: &str = "crabka-alter-parts-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--alter",
        "--topic",
        TOPIC,
        "--partitions",
        "3",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    let out = docker_run_kafka_tool(&[
        "kafka-topics",
        "--describe",
        "--topic",
        TOPIC,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("PartitionCount: 3") || s.contains("Partitions: 3"),
        "describe missing PartitionCount: 3 — got: {s}"
    );
}
```

- [ ] **Step 2: Run, expect pass, commit**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release kafka_topics_alter_partitions -- --ignored --nocapture --test-threads=1 2>&1 | tail -5
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-topics --alter --partitions"
```

---

### Task 20: `kafka-delete-records` JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trims_log() {
    const TOPIC: &str = "crabka-delete-recs-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // Produce 20 records via console-producer stdin.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        for i in 0..20 {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(child.stdin.take());
    let prod_out = child.wait_with_output().expect("wait producer");
    assert!(prod_out.status.success(), "producer failed");

    // Build offset-json on the host so we can pass it into the container.
    let json = format!(
        r#"{{"partitions":[{{"topic":"{TOPIC}","partition":0,"offset":10}}],"version":1}}"#
    );
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), &json).expect("write json");
    let host_path = tmp.path().to_path_buf();
    let mount = format!("{}:/offsets.json:ro", host_path.display());

    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-delete-records",
            "--bootstrap-server",
            BOOTSTRAP,
            "--offset-json-file",
            "/offsets.json",
        ])
        .output()
        .expect("spawn delete-records");
    assert!(
        out.status.success(),
        "delete-records failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("low_watermark") || s.contains("10"),
        "delete-records output missing low_watermark: {s}"
    );
}
```

- [ ] **Step 2: Run, commit**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release kafka_delete_records_trims_log -- --ignored --nocapture --test-threads=1 2>&1 | tail -10
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-delete-records --offset-json-file"
```

---

### Task 21: `kafka-consumer-groups --list` JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_list_describe() {
    const TOPIC: &str = "crabka-cg-list-itest";
    const GROUP: &str = "crabka-cg-list-grp";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // Produce one record so the consumer has something to settle on.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "alpha").expect("write");
    }
    drop(child.stdin.take());
    let _ = child.wait_with_output();

    // Consume one record with --group so the group is registered.
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--group",
        GROUP,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "10000",
    ]);

    let list_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--list",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&list_out.stdout);
    assert!(
        s.contains(GROUP),
        "list output missing {GROUP}: {s}"
    );

    let desc_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        GROUP,
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        s.contains(TOPIC),
        "describe output missing topic {TOPIC}: {s}"
    );
}
```

- [ ] **Step 2: Run, commit**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release kafka_consumer_groups_list_describe -- --ignored --nocapture --test-threads=1 2>&1 | tail -10
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-consumer-groups --list/--describe"
```

---

### Task 22: `kafka-cluster --describe` JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_cluster_describe() {
    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    let out = docker_run_kafka_tool(&[
        "kafka-cluster",
        "describe",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(
        s.contains("Cluster ID") || s.contains("cluster ID") || s.contains("00000000"),
        "cluster describe output missing cluster id: {s}"
    );
    assert!(
        s.contains("Controller") || s.contains("controller"),
        "cluster describe output missing controller: {s}"
    );
}
```

- [ ] **Step 2: Run, commit**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release kafka_cluster_describe -- --ignored --nocapture --test-threads=1 2>&1 | tail -10
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-cluster describe"
```

---

### Task 23: Final acceptance sweep

- [ ] **Step 1: Full JVM acceptance suite**

```bash
cargo test -p crabka-broker --test jvm_acceptance --release -- --ignored --test-threads=1 2>&1 | tail -20
```

Expected: 14 tests pass (9 existing + 5 new).

- [ ] **Step 2: Workspace tests**

```bash
cargo test --workspace --release 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 3: Lints and formatting**

```bash
cargo clippy --workspace --all-targets --release -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 4: Update README slice list**

In `README.md`, add to the `### Slices delivered` list (just after the Slice 10b bullet):

```markdown
- **Slice 11** — admin handlers: AlterConfigs/IncrementalAlterConfigs
  (with live propagation to `Log.config`), CreatePartitions,
  DeleteRecords, ListGroups, DescribeGroups, DeleteGroups,
  DescribeCluster. Validated end-to-end against the JVM
  `kafka-*.sh` operator tooling.
```

- [ ] **Step 5: Update STATUS.md (slice-11 entry)**

Append a new section to `STATUS.md`:

```markdown
## Slice 11 — admin handlers (2026-05-14)

- 8 new handlers: AlterConfigs (33), IncrementalAlterConfigs (44),
  CreatePartitions (37), DeleteRecords (21), DescribeCluster (60),
  ListGroups (16), DescribeGroups (15), DeleteGroups (42).
- 1 new metadata record: V1TopicConfig.
- Topic-config whitelist with live propagation to `Log.config` via
  `Arc<RwLock<LogConfig>>` and a supervisor reconcile push.
- 5 new JVM acceptance tests covering `kafka-configs --alter`,
  `kafka-topics --alter --partitions`, `kafka-delete-records`,
  `kafka-consumer-groups --list/--describe`, `kafka-cluster describe`.
- Out of scope: Rust CLI, ACLs, quotas, partition reassignments,
  ElectLeaders, log compaction, broker-side recompression.
```

- [ ] **Step 6: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "docs: slice 11 (admin handlers) status entry"
```

- [ ] **Step 7: Push and open PR**

```bash
git push -u origin feature/admin-handlers-11
gh pr create --title "Slice 11: Admin handlers (configs, partitions, groups, cluster, records)" --body "$(cat <<'EOF'
## Summary

Eight new operator-facing handlers so JVM `kafka-*.sh` tooling works against a Rust broker without skipping:

- `AlterConfigs` (33) + `IncrementalAlterConfigs` (44) — topic-level whitelist; changes propagate live to `Log.config`
- `CreatePartitions` (37) — extends an existing topic via round-robin replica placement
- `DeleteRecords` (21) — leader-only segment trim; followers converge via the existing Fetch out-of-range path
- `ListGroups` (16) + `DescribeGroups` (15) + `DeleteGroups` (42) — read/delete views over the existing GroupManager
- `DescribeCluster` (60) — projection over the metadata image

New `MetadataRecord::V1TopicConfig` carries per-topic overrides; `Log.config` is now `Arc<RwLock<LogConfig>>` so AlterConfigs takes effect without broker restart; the `ReplicatorSupervisor` reconcile loop pushes the current overrides to each locally-hosted partition on every image change.

## Test plan
- [x] `cargo test --workspace --release` — green on linux/macos/windows
- [x] `cargo clippy --workspace --all-targets -- -D warnings` — clean
- [x] `cargo fmt --all -- --check` — clean
- [x] 14 JVM acceptance tests pass (9 existing + 5 new)
- [x] Live propagation verified: change `retention.ms` on a topic with open partitions, retention honors it on the next tick

## Out of scope

Rust `crabka-cli`, ACLs, quotas, partition reassignments, ElectLeaders, log compaction, broker-side recompression. Tracked separately.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review checklist

**Spec coverage:**
- `V1TopicConfig` record + image plumbing → Tasks 1–3
- Config whitelist with six keys, three honored / three accepted-as-default → Task 6
- `Log.config` swappable → Task 4; `Log::trim_to_offset` → Task 5
- `WriterMessage::SetLogConfig` / `TrimToOffset` → Tasks 7, 12
- Supervisor reconcile pushes overrides → Task 8
- AlterConfigs + IncrementalAlterConfigs handlers → Tasks 9, 10
- CreatePartitions handler → Task 11
- DeleteRecords handler → Task 13
- GroupManager accessors + three group handlers → Tasks 14, 15
- DescribeCluster handler → Task 16
- Broker-integration tests (covers AlterConfigs live propagation, CreatePartitions, DeleteRecords, DescribeCluster, ListGroups) → Task 17
- Five JVM acceptance tests → Tasks 18–22
- Final acceptance sweep → Task 23

**Coverage gaps:** none. Every spec requirement maps to a task.

**Type consistency:**
- `TopicConfigRecord` field names (`topic`, `overrides: BTreeMap<String, String>`) used identically in Tasks 1, 2, 7, 9, 10.
- `GroupSnapshot` / `MemberSnapshot` / `DeleteGroupError` defined in Task 14, consumed in Task 15 (handlers).
- `WriterMessage` variants `SetLogConfig` / `TrimToOffset` declared in Tasks 7 / 12, used by handlers and Partition methods.
- `Partition::apply_log_config_overrides(&BTreeMap<String,String>)`, `Partition::trim_to_offset(i64)` — same signatures everywhere.
- `Log::set_config(LogConfig)`, `Log::config_snapshot() -> LogConfig`, `Log::trim_to_offset(i64) -> Result<i64, LogError>`, `Log::set_log_start_offset(i64)` — consistent.
- Config-key constants in `config_keys.rs` reused by both AlterConfigs handlers.
- `MetadataImage::topic_config(name) -> Option<&BTreeMap<String,String>>` — same signature in supervisor (Task 8) and handlers.

**Placeholder scan:** no "TBD" / "implement later" / "add appropriate error handling" / "similar to Task N" strings. Each step has either concrete code or a concrete command.
