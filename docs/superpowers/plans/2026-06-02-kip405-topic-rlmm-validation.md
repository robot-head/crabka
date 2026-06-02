# KIP-405 Topic-Backed RLMM: Promote & JVM-Validate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the durable topic-backed `RemoteLogMetadataManager` the first-class default for tiered storage, harden its bootstrap to be fail-closed, JVM-validate it end-to-end, and flip KIP-405 ⚠️→✅.

**Architecture:** Replace the broker's `Option<KafkaRlmmConfig>` RLMM selector with an explicit `RlmmKind` enum that defaults to topic-backed when tiered storage is enabled. Boot the `SwappableRlmm` on a fail-closed `NotReadyRlmm` stub (returns retryable `NotReady` for every method) instead of a silently-accepting in-memory placeholder, and retry the topic-backed bootstrap with backoff until it succeeds. Validate with two new Docker JVM acceptance tests (single-broker restart durability; multi-broker metadata sharing).

**Tech Stack:** Rust 2024 (workspace), `tokio`, `crabka-broker`, `crabka-remote-storage`, `crabka-remote-storage-topic`, `crabka-operator`; `assert2` for test assertions; `testcontainers` + `confluentinc/cp-kafka:7.8.8` + MinIO for JVM acceptance.

**Spec:** `docs/superpowers/specs/2026-06-02-kip405-topic-rlmm-validation-design.md`

---

## Conventions (read once)

- **Git identity is unset locally.** Commit with overrides, never `git config`:
  ```bash
  git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "..."
  ```
  End every commit message body with:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  ```
- **Before any push / final commit:** `cargo fmt --all` (CI gates on `cargo fmt --check`).
- **Lint gate:** `cargo clippy --workspace --all-targets -- -D warnings` (the `--all-targets` matters — `--lib` misses `#[cfg(test)]` lints).
- **All test assertions use `assert2`** (`use assert2::assert;` / `check!`), matching the workspace convention.
- **Worktree:** all work happens in this worktree on branch `claude/gifted-hertz-695b08`. Subagents: run git with `git -C <worktree-path>` and assert the branch before committing — a bare `git` may land on `main`.
- **JVM/Docker tests** are `#[ignore = "requires Docker"]`; run them explicitly:
  ```bash
  cargo test -p crabka-broker --test jvm_acceptance <name> -- --ignored --nocapture
  ```

---

## File Structure & Batches

Tasks are grouped into batches; within a batch the file sets are disjoint and tasks run in parallel (per `CLAUDE.md` subagent-driven development). Wait for a batch to finish + review before the next.

| Batch | Task | Primary files | Depends on |
|---|---|---|---|
| 1 | T1: `RlmmKind` enum + config plumbing | `crates/broker/src/config.rs`, `crates/broker/src/file_config.rs` | — |
| 1 | T2: `NotReadyRlmm` stub | `crates/remote-storage-topic/src/not_ready.rs`, `…/src/lib.rs` | — |
| 2 | T3: Broker bootstrap retry + fail-closed placeholder + `RlmmKind` wiring + metric | `crates/broker/src/broker.rs`, `crates/broker/src/metrics.rs` | T1, T2 |
| 3 | T4: Fail-closed window in-process test | `crates/broker/tests/tiered_storage_topic_rlmm.rs` | T3 |
| 3 | T5: Operator default-to-topic-backed | `crates/operator/src/controller/listeners.rs` (+ `crd/kafka.rs` if needed) | T1 |
| 4 | T6: JVM acceptance — T1 restart durability (+ restart harness) | `crates/broker/tests/jvm_acceptance.rs` | T3 |
| 4 | T7: JVM acceptance — T2 multi-broker metadata sharing | `crates/broker/tests/jvm_acceptance.rs` (sequential after T6 — same file) | T3, T6 |
| 5 | T8: README + STATUS + matrix flip | `README.md`, `STATUS.md` | all |

---

## Task 1: `RlmmKind` enum + config plumbing

**Files:**
- Modify: `crates/broker/src/config.rs` (replace `remote_log_metadata_kafka` field; add `RlmmKind`; `KafkaRlmmConfig::default`; update `for_tests`/`default` builders; update unit tests `~887-911`)
- Modify: `crates/broker/src/file_config.rs:879-893` (map `[remote_storage.kafka_metadata]` → `RlmmKind`)
- Modify: `crates/broker/src/lib.rs` (export `RlmmKind` next to `KafkaRlmmConfig`)

**Context:** Today `BrokerConfig.remote_log_metadata_kafka: Option<KafkaRlmmConfig>` selects the RLMM — `None`⇒in-memory, `Some`⇒topic-backed (`config.rs:443`). We replace it with `RlmmKind` so topic-backed is the production default and in-memory is an explicit opt-out. `KafkaRlmmConfig` keeps its fields (`config.rs:453-482`); `bootstrap` and `snapshot_dir` may be empty sentinels filled in at broker start.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `crates/broker/src/config.rs` (alongside `kafka_rlmm_config_carries_snapshot_settings`):

```rust
#[test]
fn production_default_selects_topic_backed_rlmm() {
    let c = BrokerConfig::default();
    assert!(matches!(c.remote_log_metadata, RlmmKind::TopicBacked(_)));
}

#[test]
fn test_default_selects_in_memory_rlmm() {
    // In-process integration tests have no real listener to loop back to,
    // so the test default must stay in-memory.
    let c = BrokerConfig::for_tests();
    assert!(matches!(c.remote_log_metadata, RlmmKind::InMemory));
}

#[test]
fn kafka_rlmm_config_default_has_sane_topic_settings() {
    let c = KafkaRlmmConfig::default();
    assert!(c.num_partitions == 50);
    assert!(c.replication == 3);
    assert!(c.bootstrap.is_empty()); // derived at broker start
    assert!(c.snapshot_dir.as_os_str().is_empty()); // derived from log.dir
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-broker --lib config::tests::production_default_selects_topic_backed_rlmm`
Expected: FAIL — `RlmmKind` / `remote_log_metadata` not found (compile error).

- [ ] **Step 3: Add the `RlmmKind` enum and `KafkaRlmmConfig::default`**

In `crates/broker/src/config.rs`, immediately after the `KafkaRlmmConfig` struct definition (after line ~482), add:

```rust
/// Which [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
/// the broker runs when tiered storage is enabled.
///
/// Topic-backed is the production default (matches Kafka's
/// `TopicBasedRemoteLogMetadataManager`, which is the only production RLMM).
/// In-memory is an explicit opt-out for in-process integration tests that
/// have no real listener to loop the metadata client back to. This field is
/// ignored entirely when [`BrokerConfig::remote_storage_backend`] is `None`.
#[derive(Debug, Clone)]
pub enum RlmmKind {
    /// Durable `__remote_log_metadata`-backed manager. `cfg.bootstrap` and
    /// `cfg.snapshot_dir` may be left empty; the broker derives them at start
    /// from the inter-broker listener and `log.dir` respectively.
    TopicBacked(KafkaRlmmConfig),
    /// Non-durable in-process manager. Tests only.
    InMemory,
}

impl Default for KafkaRlmmConfig {
    fn default() -> Self {
        Self {
            bootstrap: String::new(),
            num_partitions: 50,
            replication: 3,
            snapshot_interval: DEFAULT_RLMM_SNAPSHOT_INTERVAL,
            snapshot_dir: std::path::PathBuf::new(),
            security: None,
        }
    }
}
```

- [ ] **Step 4: Replace the field and update both builders**

In `crates/broker/src/config.rs`:

1. Replace the field declaration (lines ~436-443) — remove `remote_log_metadata_kafka` and add:
```rust
    /// KIP-405: which RLMM the broker runs when tiered storage is enabled.
    /// Defaults to [`RlmmKind::TopicBacked`] in production; [`RlmmKind::InMemory`]
    /// for in-process tests. Ignored when `remote_storage_backend` is `None`.
    pub remote_log_metadata: RlmmKind,
```

2. In `for_tests()` replace `remote_log_metadata_kafka: None,` (line ~621) with:
```rust
            // In-process tests have no loopback listener for the metadata
            // client; use the in-memory RLMM fixture.
            remote_log_metadata: RlmmKind::InMemory,
```

3. In `default()` replace `remote_log_metadata_kafka: None,` (line ~875) with:
```rust
            // Production default: the durable topic-backed RLMM. Bootstrap
            // address + snapshot dir are derived at start.
            remote_log_metadata: RlmmKind::TopicBacked(KafkaRlmmConfig::default()),
```

- [ ] **Step 5: Update the `file_config.rs` mapping**

In `crates/broker/src/file_config.rs`, replace the `if let Some(km) = &rs.kafka_metadata { … }` block (lines ~880-893) with one that always selects topic-backed when tiering is enabled, taking overrides from the sub-table when present, and only choosing in-memory on an explicit opt-out:

```rust
            // KIP-405: topic-backed RLMM is the default whenever tiered
            // storage is enabled. `[remote_storage.kafka_metadata]` only
            // overrides the topic knobs; `in_memory = true` is the explicit
            // (test/dev) opt-out.
            if cfg.remote_storage_backend.is_some() {
                let km = rs.kafka_metadata.as_ref();
                if km.is_some_and(|k| k.in_memory) {
                    cfg.remote_log_metadata = crate::config::RlmmKind::InMemory;
                } else {
                    cfg.remote_log_metadata =
                        crate::config::RlmmKind::TopicBacked(crate::config::KafkaRlmmConfig {
                            bootstrap: km.map(|k| k.bootstrap.clone()).unwrap_or_default(),
                            num_partitions: km.and_then(|k| k.num_partitions).unwrap_or(50),
                            replication: km.and_then(|k| k.replication).unwrap_or(3),
                            snapshot_interval: crate::config::DEFAULT_RLMM_SNAPSHOT_INTERVAL,
                            snapshot_dir: cfg.log_dir.join("remote-log-metadata"),
                            // Derived at runtime from the inter-broker listener.
                            security: None,
                        });
                }
            }
```

Then add an `in_memory` flag to the `FileKafkaRlmmConfig` struct (`file_config.rs:~153`). Locate the struct and add:
```rust
    /// Explicit opt-out: run the non-durable in-memory RLMM instead of the
    /// topic-backed default. Tests / single-node dev only.
    #[serde(default)]
    pub in_memory: bool,
```
(`bootstrap` on `FileKafkaRlmmConfig` should become optional/defaulted if it isn't already — check the struct; if `bootstrap: String` is required, change to `#[serde(default)] pub bootstrap: String`.)

- [ ] **Step 6: Export `RlmmKind`**

In `crates/broker/src/lib.rs`, find the `pub use config::{… KafkaRlmmConfig …};` line (~180) and add `RlmmKind`:
```rust
pub use config::{BootstrapMode, BrokerConfig, KafkaRlmmConfig, RemoteStorageBackend, RlmmKind};
```

- [ ] **Step 7: Update the existing config unit tests that construct `KafkaRlmmConfig`**

The two tests at `config.rs:~887-911` construct `KafkaRlmmConfig { … }` with all fields — they still compile (fields unchanged). No change needed unless the struct changed. Confirm they still build.

- [ ] **Step 8: Fix all other construction sites that set `remote_log_metadata_kafka`**

Grep and update every remaining reference (tests set it to `Some(KafkaRlmmConfig{..})` → `RlmmKind::TopicBacked(KafkaRlmmConfig{..})`):
```bash
grep -rn "remote_log_metadata_kafka" crates/
```
Expected hits to fix: `crates/broker/tests/tiered_storage_topic_rlmm.rs:69,386`, `crates/broker/tests/tiered_storage_metadata_assign.rs`. Change each `cfg.remote_log_metadata_kafka = Some(KafkaRlmmConfig{..});` to `cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig{..});` and add the import.

- [ ] **Step 9: Run tests + clippy + fmt**

Run:
```bash
cargo test -p crabka-broker --lib config::tests
cargo build -p crabka-broker --all-targets
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo fmt --all
```
Expected: the three new tests PASS; workspace compiles; clippy clean.

- [ ] **Step 10: Commit**

```bash
git -C <worktree> add crates/broker/src/config.rs crates/broker/src/file_config.rs crates/broker/src/lib.rs crates/broker/tests/tiered_storage_topic_rlmm.rs crates/broker/tests/tiered_storage_metadata_assign.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
feat(broker): RlmmKind enum — topic-backed RLMM is the tiered-storage default

Replace remote_log_metadata_kafka: Option<KafkaRlmmConfig> with an explicit
RlmmKind { TopicBacked(KafkaRlmmConfig), InMemory }. Production default is
TopicBacked; for_tests() and explicit `in_memory = true` opt into InMemory.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `NotReadyRlmm` fail-closed stub

**Files:**
- Create: `crates/remote-storage-topic/src/not_ready.rs`
- Modify: `crates/remote-storage-topic/src/lib.rs` (add `mod not_ready;` + `pub use`)
- Test: inline `#[cfg(test)]` in `not_ready.rs`

**Context:** During the topic-backed bootstrap window the `SwappableRlmm` placeholder is currently an `InmemoryRemoteLogMetadataManager` that *silently accepts* writes which are lost on `swap()`. We replace it with a stub that returns `RemoteStorageError::NotReady { partition }` for every method. The copy task calls `add_remote_log_segment_metadata(CopySegmentStarted)` **before** copying segment data and `continue`s on RLMM error (`crates/broker/src/remote_log_manager.rs:132`+), so a `NotReady` add means nothing is tiered until durable metadata is available. Reads already treat `NotReady` as retryable (`fetch.rs:1139`, `list_offsets.rs:96/127`). The error variant exists at `crates/remote-storage/src/error.rs:70`:
```rust
NotReady { partition: i32 },
```
Trait shape: `crabka_remote_storage::RemoteLogMetadataManager` (`crates/remote-storage/src/metadata_manager.rs:30-116`) — 7 methods.

- [ ] **Step 1: Write the failing test**

Create `crates/remote-storage-topic/src/not_ready.rs` with the test at the bottom (write the whole file in this step so the test compiles against the impl in the next):

```rust
//! [`NotReadyRlmm`] — a [`RemoteLogMetadataManager`] that fails closed.
//!
//! Used as the [`crate::SwappableRlmm`] placeholder while the topic-backed
//! manager is still bootstrapping. Every method returns
//! [`RemoteStorageError::NotReady`], which the broker treats as retryable:
//! the copy task skips tiering a segment (its first call —
//! `add_remote_log_segment_metadata` — fails before any data is copied to
//! the RSM, so no orphaned remote objects), and remote reads return a
//! retryable error until [`crate::SwappableRlmm::swap`] installs the real
//! manager.

use crabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemotePartitionDeleteMetadata, RemoteStorageError, TopicIdPartition,
};

/// A fail-closed [`RemoteLogMetadataManager`] placeholder.
#[derive(Debug, Default)]
pub struct NotReadyRlmm;

impl NotReadyRlmm {
    /// Construct the stub.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl RemoteLogMetadataManager for NotReadyRlmm {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: metadata.remote_log_segment_id().topic_id_partition().partition(),
        })
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: update.remote_log_segment_id().topic_id_partition().partition(),
        })
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: i32,
        _offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition(),
        })
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: i32,
    ) -> Result<Option<i64>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition(),
        })
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition(),
        })
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: i32,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition(),
        })
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: metadata.topic_id_partition().partition(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use uuid::Uuid;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 3)
    }

    #[test]
    fn reads_return_not_ready_with_partition() {
        let m = NotReadyRlmm::new();
        let err = m.remote_log_segment_metadata(&tp(), 0, 0).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));

        let err = m.list_remote_log_segments(&tp()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));

        let err = m.highest_offset_for_epoch(&tp(), 0).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }
}
```

> **Note:** verify the accessor names on `RemoteLogSegmentMetadata` / `RemoteLogSegmentMetadataUpdate` / `RemotePartitionDeleteMetadata` (e.g. `remote_log_segment_id()`, `topic_id_partition()`, `partition()`) against `crates/remote-storage/src/metadata.rs`. Adjust the field/accessor calls if the real API differs — the test pins the observable behavior (`NotReady { partition: 3 }`), which is what matters.

- [ ] **Step 2: Wire the module**

In `crates/remote-storage-topic/src/lib.rs`, add near the other `mod`/`pub use` lines:
```rust
mod not_ready;
pub use not_ready::NotReadyRlmm;
```

- [ ] **Step 3: Run test to verify it fails, then passes**

Run: `cargo test -p crabka-remote-storage-topic not_ready`
Expected: compiles and PASSES (impl + test landed together). If accessor names were wrong it FAILS to compile — fix per the note, then PASS.

- [ ] **Step 4: clippy + fmt**

Run:
```bash
cargo clippy -p crabka-remote-storage-topic --all-targets -- -D warnings
cargo fmt --all
```
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/remote-storage-topic/src/not_ready.rs crates/remote-storage-topic/src/lib.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
feat(remote-storage-topic): NotReadyRlmm fail-closed placeholder

A RemoteLogMetadataManager that returns RemoteStorageError::NotReady for
every method. Used as the SwappableRlmm placeholder during the topic-backed
bootstrap window so the copy task skips tiering (no orphaned RSM objects)
and remote reads return a retryable error until the real manager swaps in.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Broker bootstrap retry + fail-closed placeholder + `RlmmKind` wiring + metric

**Files:**
- Modify: `crates/broker/src/broker.rs` (kickoff `~2056-2120`; RLMM construction `~2147-2156`; `bootstrap_topic_rlmm` `~2452-2495`)
- Modify: `crates/broker/src/metrics.rs` (add a bootstrap-attempt counter)
- Test: inline broker unit test for the retry loop + a metrics unit test

**Context:** Three changes, all gated on `RlmmKind::TopicBacked`:
1. The kickoff (`broker.rs:2056`) currently maps off `config.remote_log_metadata_kafka.as_ref()`. Re-key it to match `RlmmKind::TopicBacked(cfg)`, and derive `snapshot_dir` from `config.log_dir` when empty.
2. The RLMM construction (`broker.rs:2147-2156`) currently boots `SwappableRlmm::new(InmemoryRemoteLogMetadataManager)`. Boot it on `NotReadyRlmm` instead, and select the swappable path on `RlmmKind::TopicBacked` (vs plain in-memory on `RlmmKind::InMemory`).
3. `bootstrap_topic_rlmm` (`broker.rs:2452`) is one-shot: on `start` error it `warn!`s and returns. Wrap the `KafkaMetadataEventLog::start` + `TopicBasedRemoteLogMetadataManager::start` in a bounded-backoff retry loop until success or shutdown, incrementing a metric each attempt.

- [ ] **Step 1: Add the retry-attempt metric (write the failing metrics test first)**

In `crates/broker/src/metrics.rs`, add to the `#[cfg(test)] mod tests` (near `tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set`, ~1090):
```rust
#[test]
fn tiered_storage_rlmm_bootstrap_attempts_counts_up() {
    let m = BrokerMetrics::new_for_test();
    assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 0);
    m.tiered_storage_rlmm_bootstrap_attempts.inc();
    m.tiered_storage_rlmm_bootstrap_attempts.inc();
    assert!(m.tiered_storage_rlmm_bootstrap_attempts.get() == 2);
}
```
(Use whatever test-constructor the other metrics tests use — match the existing `m` setup in `tiered_storage_rlmm_topic_backed_defaults_zero_and_can_be_set`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker --lib metrics::tests::tiered_storage_rlmm_bootstrap_attempts_counts_up`
Expected: FAIL — field not found.

- [ ] **Step 3: Add the metric field**

In `crates/broker/src/metrics.rs`, mirror the `tiered_storage_rlmm_topic_backed` gauge wiring (declared `:234`, constructed `:299`, registered `:540-547`, moved into the struct literal `:614`). Add a `Counter` named `tiered_storage_rlmm_bootstrap_attempts`:
- struct field (near :234):
  ```rust
  /// KIP-405: number of topic-backed RLMM bootstrap attempts. Stays > 0
  /// and flat once `tiered_storage_rlmm_topic_backed` flips to 1; a value
  /// that keeps climbing means the bootstrap is stuck retrying.
  pub tiered_storage_rlmm_bootstrap_attempts: Counter,
  ```
- construct (near :299): `let tiered_storage_rlmm_bootstrap_attempts = Counter::default();`
- register (near :540) with metric name `"crabka_broker_tiered_storage_rlmm_bootstrap_attempts"`.
- move into the struct literal (near :614).

(Confirm `Counter` is already imported in `metrics.rs`; if not, add it next to `Gauge`.)

- [ ] **Step 4: Run the metrics test to verify it passes**

Run: `cargo test -p crabka-broker --lib metrics::tests::tiered_storage_rlmm_bootstrap_attempts_counts_up`
Expected: PASS.

- [ ] **Step 5: Re-key the kickoff on `RlmmKind` + derive snapshot_dir**

In `crates/broker/src/broker.rs`, change the kickoff binding (line ~2056-2057) from:
```rust
        let kafka_swap_kickoff: Option<KafkaSwapKickoff> =
            config.remote_log_metadata_kafka.as_ref().map(|cfg| {
```
to a form that only builds a kickoff for the topic-backed kind:
```rust
        let kafka_swap_kickoff: Option<KafkaSwapKickoff> = match &config.remote_log_metadata {
            crate::config::RlmmKind::InMemory => None,
            crate::config::RlmmKind::TopicBacked(cfg) => Some({
```
Keep the existing body (`listeners`, `inter`, `proto`, `security`, `bootstrap` derivation) unchanged. In the constructed `KafkaRlmmConfig { … }` (line ~2111-2118), derive `snapshot_dir` when empty:
```rust
                    snapshot_dir: if cfg.snapshot_dir.as_os_str().is_empty() {
                        config.log_dir.join("remote-log-metadata")
                    } else {
                        cfg.snapshot_dir.clone()
                    },
```
Close the `match` arm correctly (the `Some({ … })` wraps the `KafkaSwapKickoff { … }` value; end with `})` and `};`).

- [ ] **Step 6: Boot the swappable on `NotReadyRlmm`, gated on `RlmmKind`**

In `crates/broker/src/broker.rs`, replace the RLMM construction block (lines ~2145-2156). Today:
```rust
            let placeholder: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                Arc::new(crabka_remote_storage::InmemoryRemoteLogMetadataManager::new());
            let (rlmm, kafka_swap_target): (...) =
                if config.remote_log_metadata_kafka.is_some() {
                    let swap = Arc::new(crabka_remote_storage_topic::SwappableRlmm::new(placeholder));
                    let typed: Arc<dyn ...> = swap.clone();
                    (typed, Some(swap))
                } else {
                    (placeholder, None)
                };
```
Replace with:
```rust
            let (rlmm, kafka_swap_target): (
                Arc<dyn crabka_remote_storage::RemoteLogMetadataManager>,
                Option<Arc<crabka_remote_storage_topic::SwappableRlmm>>,
            ) = match &config.remote_log_metadata {
                crate::config::RlmmKind::TopicBacked(_) => {
                    // Fail-closed: until the topic-backed manager swaps in, every
                    // RLMM call returns NotReady. The copy task skips tiering
                    // (no orphaned RSM objects) and remote reads retry.
                    let not_ready: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        Arc::new(crabka_remote_storage_topic::NotReadyRlmm::new());
                    let swap = Arc::new(crabka_remote_storage_topic::SwappableRlmm::new(not_ready));
                    let typed: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        swap.clone();
                    (typed, Some(swap))
                }
                crate::config::RlmmKind::InMemory => {
                    let placeholder: Arc<dyn crabka_remote_storage::RemoteLogMetadataManager> =
                        Arc::new(crabka_remote_storage::InmemoryRemoteLogMetadataManager::new());
                    (placeholder, None)
                }
            };
```

- [ ] **Step 7: Add the retry loop to `bootstrap_topic_rlmm`**

In `crates/broker/src/broker.rs`, the body around lines 2469-2495 starts the log + manager once. Replace the two one-shot `match … start(…)` blocks with a single retry loop that increments the metric per attempt and backs off on failure. Insert before the existing `let log = …`:

```rust
    // Retry the topic-backed bootstrap with bounded backoff until it
    // succeeds or the broker shuts down. Until then the SwappableRlmm stays
    // on the fail-closed NotReadyRlmm placeholder.
    const RLMM_BOOTSTRAP_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(10);
    let mut backoff = std::time::Duration::from_millis(250);
    let (log, manager) = loop {
        metrics.tiered_storage_rlmm_bootstrap_attempts.inc();
        let attempt = async {
            let log = crabka_remote_storage_topic::KafkaMetadataEventLog::start(log_cfg.clone())
                .await?;
            let manager = crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
                log.clone(),
                runtime.clone(),
                cfg.cfg.snapshot_dir.clone(),
                cfg.cfg.snapshot_interval,
            )
            .await?;
            Ok::<_, crabka_remote_storage_topic::TopicRlmmError>((log, manager))
        }
        .await;
        match attempt {
            Ok(pair) => break pair,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis(),
                    "topic-backed RLMM bootstrap attempt failed; retrying"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {
                        tracing::debug!("topic-backed RLMM bootstrap cancelled during backoff");
                        return;
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(RLMM_BOOTSTRAP_BACKOFF_MAX);
            }
        }
    };
```
Then delete the old one-shot `let log = match … { … return; }` and `let manager = match … { … return; }` blocks. Keep everything after `swap.swap(manager.clone()); metrics.tiered_storage_rlmm_topic_backed.set(1); …` unchanged.

> **Type-check notes:** confirm the error type returned by `KafkaMetadataEventLog::start` / `TopicBasedRemoteLogMetadataManager::start` (the `crabka_remote_storage_topic` error enum — check `crates/remote-storage-topic/src/error.rs`; it may be `TopicRlmmError` or similar). Use the actual name in the `Ok::<_, …>` annotation. Confirm `KafkaMetadataEventLog::start` takes `log_cfg` by value (it does today, `:2469`) — `.clone()` it inside the loop since the loop may run more than once. Confirm `KafkaMetadataEventLog` is `Clone` (it returns `Arc<Self>`, so `log.clone()` is an `Arc` clone). `runtime` is a `tokio::runtime::Handle` (Clone). If `TopicBasedRemoteLogMetadataManager::start` consumes `log`, pass `log.clone()` and break with the manager only.

- [ ] **Step 8: Write a unit test for the retry loop**

The retry loop is embedded in `bootstrap_topic_rlmm`, which needs a live listener — not unit-testable in isolation. Instead, extract the retry/backoff decision into a tiny pure helper and unit-test that. Add to `broker.rs`:
```rust
/// Next backoff after a failed RLMM bootstrap attempt: double, capped.
fn next_rlmm_backoff(cur: std::time::Duration, max: std::time::Duration) -> std::time::Duration {
    (cur * 2).min(max)
}
```
Use it in the loop (`backoff = next_rlmm_backoff(backoff, RLMM_BOOTSTRAP_BACKOFF_MAX);`). Add a unit test in `broker.rs`'s test module:
```rust
#[test]
fn rlmm_backoff_doubles_then_caps() {
    use std::time::Duration;
    let max = Duration::from_secs(10);
    assert!(next_rlmm_backoff(Duration::from_millis(250), max) == Duration::from_millis(500));
    assert!(next_rlmm_backoff(Duration::from_secs(8), max) == max); // 16s capped to 10s
    assert!(next_rlmm_backoff(max, max) == max);
}
```

- [ ] **Step 9: Build, test, clippy, fmt**

Run:
```bash
cargo build -p crabka-broker --all-targets
cargo test -p crabka-broker --lib metrics::tests rlmm_backoff_doubles_then_caps
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo fmt --all
```
Expected: compiles; tests PASS; clippy clean. Watch `clippy::large_futures` on `Broker::start` (the `BrokerConfig`/futures size is sensitive — see `KafkaRlmmConfig.security` already being `Box`ed).

- [ ] **Step 10: Commit**

```bash
git -C <worktree> add crates/broker/src/broker.rs crates/broker/src/metrics.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
feat(broker): fail-closed + retrying topic-backed RLMM bootstrap

Boot the SwappableRlmm on NotReadyRlmm (not a silently-accepting in-memory
placeholder) and retry KafkaMetadataEventLog/TopicBasedRemoteLogMetadataManager
start with bounded backoff until success or shutdown. Re-key RLMM selection on
RlmmKind. Add tiered_storage_rlmm_bootstrap_attempts counter.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Fail-closed window in-process test

**Files:**
- Modify: `crates/broker/tests/tiered_storage_topic_rlmm.rs`

**Context:** Prove the fail-closed guarantee end-to-end in-process: with `RlmmKind::TopicBacked` but the metadata listener unreachable (so the manager never swaps in), the copy task must **not** tier any segment (the `add` returns `NotReady` before any RSM write), and remote reads must return a retryable error rather than serving stale/empty data. The existing tests in this file (`:69`, `:386`) already boot a topic-backed broker over loopback; model the new test on them but point the RLMM bootstrap at a dead address.

- [ ] **Step 1: Write the failing test**

Add a test that configures `RlmmKind::TopicBacked` with a bootstrap pointing at an unused port (so `KafkaMetadataEventLog::start` keeps failing and the swap never happens), enables tiered storage with a Local backend + tiny segment size, produces enough to seal ≥1 segment, waits a few RLM ticks, and asserts **no objects** were written to the local tier directory (nothing tiered with non-durable metadata):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_task_skips_tiering_while_rlmm_not_ready() {
    // Local tiered backend in a temp dir we can inspect.
    let tier_dir = tempfile::tempdir().expect("tier dir");
    let log_dir = tempfile::tempdir().expect("log dir");

    let mut cfg = BrokerConfig::for_tests();
    cfg.log_dir = log_dir.path().to_path_buf();
    cfg.remote_storage_backend =
        Some(RemoteStorageBackend::Local { dir: tier_dir.path().to_path_buf() });
    cfg.remote_log_manager_interval = std::time::Duration::from_millis(200);
    // Topic-backed, but bootstrap points at a closed port → never swaps in.
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: "127.0.0.1:1".into(),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: std::time::Duration::from_secs(60),
        snapshot_dir: log_dir.path().join("rlmm-snap"),
        security: None,
    });

    let handle = Broker::start(cfg).await.expect("broker starts");

    // Create a tiered topic with a tiny segment size and produce enough to
    // seal multiple segments. (Reuse this file's existing produce/create
    // helpers — see the loopback tests at the top of the file.)
    create_tiered_topic_and_produce(&handle, "t-notready", /*records*/ 200).await;

    // Give the RLM copy task several ticks.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Fail-closed: nothing tiered, because every add() returned NotReady
    // before any RSM copy_log_segment_data.
    let tiered_objects = count_files_recursive(tier_dir.path());
    assert!(
        tiered_objects == 0,
        "expected no tiered objects while RLMM not ready, found {tiered_objects}"
    );

    handle.shutdown().await;
}
```

> Reuse the file's existing topic-create + produce helper (the loopback tests already have one — match its name/signature; if it's inline, factor a small `create_tiered_topic_and_produce` helper). Add a `count_files_recursive(path) -> usize` helper at the bottom of the test module (walk with `std::fs::read_dir` recursively, count regular files).

- [ ] **Step 2: Run to verify it fails (or passes) and reason about it**

Run: `cargo test -p crabka-broker --test tiered_storage_topic_rlmm copy_task_skips_tiering_while_rlmm_not_ready -- --nocapture`
Expected after Task 3: PASS (fail-closed holds). If it FAILS with `tiered_objects > 0`, the `add`-before-copy invariant is violated — inspect `remote_log_manager.rs` ordering and fix so `add_remote_log_segment_metadata` precedes `copy_log_segment_data` (the fix belongs in Task 3; loop back).

- [ ] **Step 3: clippy + fmt + commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo fmt --all
git -C <worktree> add crates/broker/tests/tiered_storage_topic_rlmm.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
test(broker): fail-closed — copy task skips tiering while RLMM not ready

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Operator default-to-topic-backed

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs:3115-3130` (render logic) + the relevant tests `~3832-3920`
- Modify (if needed): `crates/operator/src/crd/kafka.rs` (`MetadataManagerSpec` default semantics)

**Context:** The operator renders `[remote_storage.kafka_metadata]` only when `metadataManager.kind == Topic` (`listeners.rs:3121`), defaulting to in-memory when `metadataManager` is unset (comment `:3119`). Flip it: when tiered storage is enabled, emit the topic-backed block **by default** (unset `metadataManager` ⇒ topic-backed), and only omit it on an explicit `InMemory`.

- [ ] **Step 1: Write the failing test**

In `crates/operator/src/controller/listeners.rs` tests, add (next to `render_broker_toml_emits_kafka_metadata_when_topic_rlmm_set`):
```rust
#[test]
fn render_broker_toml_emits_kafka_metadata_by_default_when_tiered_and_mm_unset() {
    // Tiered storage on (Local), metadataManager entirely unset → topic-backed
    // RLMM block must still be rendered (it's the production default).
    let spec = kafka_spec_with_tiered_local_and_metadata_manager(None);
    let t = render_broker_toml(&spec /* + whatever args the sibling tests pass */);
    assert!(
        t.contains("[remote_storage.kafka_metadata]"),
        "expected kafka_metadata block by default, got:\n{t}"
    );
}
```
(Model the helper + `render_broker_toml` call exactly on the existing `render_broker_toml_emits_kafka_metadata_when_topic_rlmm_set` test at `:3832`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-operator --lib render_broker_toml_emits_kafka_metadata_by_default_when_tiered_and_mm_unset`
Expected: FAIL — block omitted when `metadataManager` is `None`.

- [ ] **Step 3: Flip the render default**

In `crates/operator/src/controller/listeners.rs` (~3117-3127), change the condition so an unset `metadataManager` renders the topic block. Today (paraphrased):
```rust
if let Some(mm) = &spec.metadata_manager
    && let crate::crd::kafka::MetadataManagerType::Topic = mm.kind
{
    let topic = mm.topic.as_ref().expect("MetadataManagerType::Topic requires metadataManager.topic");
    writeln!(out, "[remote_storage.kafka_metadata]")?;
    // … bootstrap/num_partitions/replication …
}
```
Change to: render the block whenever tiered storage is enabled and the manager is **not** explicitly `InMemory`, using `mm.topic` overrides when present and defaults otherwise:
```rust
// KIP-405: topic-backed RLMM is the default when tiered storage is on.
// Only an explicit `type: InMemory` opts out.
let is_in_memory = spec
    .metadata_manager
    .as_ref()
    .is_some_and(|mm| matches!(mm.kind, crate::crd::kafka::MetadataManagerType::InMemory));
if /* tiered storage enabled — reuse the same guard the [remote_storage] block uses */ tiered_enabled
    && !is_in_memory
{
    let topic = spec
        .metadata_manager
        .as_ref()
        .and_then(|mm| mm.topic.as_ref());
    writeln!(out, "[remote_storage.kafka_metadata]")?;
    if let Some(t) = topic {
        // bootstrap / num_partitions / replication overrides (as today)
    }
    // If `in_memory` opt-out flag is supported in the broker TOML, do NOT
    // emit it here (default path = topic-backed).
}
```
(If the existing `MetadataManagerType` enum has no `InMemory` variant, add one to `crd/kafka.rs` and a serde rename matching the CRD. Keep `Topic` as-is.)

- [ ] **Step 4: Keep the explicit-in-memory test green**

Ensure `render_broker_toml_omits_kafka_metadata_when_rlmm_inmemory` (`:3897`) still passes (explicit `InMemory` omits the block). Run both:
```bash
cargo test -p crabka-operator --lib render_broker_toml
```
Expected: the new test PASSES; the omit-on-InMemory and emit-on-Topic tests still PASS.

- [ ] **Step 5: clippy + fmt + commit**

```bash
cargo clippy -p crabka-operator --all-targets -- -D warnings
cargo fmt --all
git -C <worktree> add crates/operator/src/controller/listeners.rs crates/operator/src/crd/kafka.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
feat(operator): default to topic-backed RLMM when tiered storage is enabled

Render [remote_storage.kafka_metadata] whenever tiering is on and the CRD's
metadataManager is not explicitly InMemory, matching the broker's new default.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: JVM acceptance — T1 single-broker restart durability (+ restart harness)

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (parameterize `start_host_broker_with_minio_tier`; add restart helper; add `tiered_storage_topic_rlmm_survives_restart`)

**Context:** This is the high-signal proof that topic-backed ≠ in-memory. Extend the MinIO harness (`:7856`) to (a) take an `RlmmKind` so the existing `tiered_storage_round_trip_through_minio` keeps its in-memory intent, and (b) support restart against the same `log.dir`. The existing test currently calls `start_host_broker_with_minio_tier(s3)` and relies on `..BrokerConfig::default()` — with Task 1's flipped default that would silently become topic-backed, so pinning it explicitly is required.

- [ ] **Step 1: Parameterize the helper; pin the existing test to InMemory**

In `crates/broker/tests/jvm_acceptance.rs`, change `start_host_broker_with_minio_tier` (`:7856`) to take the RLMM kind and return the `BrokerConfig` (so a restart can reuse it):
```rust
async fn start_host_broker_with_minio_tier(
    s3: crabka_remote_storage::S3Config,
    rlmm: crabka_broker::RlmmKind,
) -> (crabka_broker::BrokerHandle, tempfile::TempDir, crabka_broker::BrokerConfig) {
    // … existing body …
    let config = BrokerConfig {
        // … existing fields …
        remote_storage_backend: Some(crabka_broker::RemoteStorageBackend::S3(s3)),
        remote_log_manager_interval: std::time::Duration::from_secs(1),
        remote_log_metadata: rlmm,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config.clone()).await.expect("start broker");
    (handle, dir, config)
}
```
Update the existing `tiered_storage_round_trip_through_minio` call site (`:7931`) to pass `crabka_broker::RlmmKind::InMemory` and ignore the returned config (`let (broker, _dir, _cfg) = …`). This preserves that test's original single-broker in-memory intent.

> `BrokerConfig` must be `Clone` for restart — it is (`#[derive(Debug, Clone)]`). The `s3` config also moves into the broker; if you need it again for restart, clone it before the first call.

- [ ] **Step 2: Add a restart helper**

Add near the other host-broker helpers:
```rust
/// Restart a host broker against the same log.dir + config (same listen
/// addr, same advertised). The caller must have shut the previous handle
/// down first so the port is free.
async fn restart_host_broker(config: crabka_broker::BrokerConfig) -> crabka_broker::BrokerHandle {
    Broker::start(config).await.expect("restart broker")
}
```

- [ ] **Step 3: Write the restart durability test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn tiered_storage_topic_rlmm_survives_restart() {
    const TOPIC: &str = "crabka-tiered-restart-itest";
    const RECORDS: usize = 200;

    let _minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);
    let s3 = /* same S3Config as tiered_storage_round_trip_through_minio (:7913) */;

    // Boot with the TOPIC-BACKED RLMM (the production default).
    let (broker, _dir, config) =
        start_host_broker_with_minio_tier(s3, crabka_broker::RlmmKind::TopicBacked(
            crabka_broker::KafkaRlmmConfig {
                bootstrap: String::new(),       // derived from the listener
                num_partitions: 5,
                replication: 1,
                snapshot_interval: std::time::Duration::from_secs(2),
                snapshot_dir: std::path::PathBuf::new(), // derived from log.dir
                security: None,
            },
        ))
        .await;
    nc_check_connectivity();

    // Create the tiered topic + produce (reuse the create/produce block from
    // tiered_storage_round_trip_through_minio: remote.storage.enable=true,
    // segment.bytes=2048, local.retention.bytes=1, RECORDS records).
    create_tiered_topic_via_jvm(TOPIC);
    produce_via_jvm(TOPIC, RECORDS);

    // Wait until ≥2 segments are in MinIO (reuse the `mc ls` poll loop :8044).
    wait_for_tiered_segments(MINIO_BUCKET, 2).await;

    // Restart the broker against the same log.dir — in-memory RLMM would lose
    // all segment metadata here; topic-backed recovers it from
    // __remote_log_metadata + the on-disk snapshot.
    broker.shutdown().await;
    let broker = restart_host_broker(config).await;
    nc_check_connectivity();

    // Consume from the remote tier post-restart; all records must be readable.
    let consumed = consume_via_jvm(TOPIC, RECORDS, /*timeout_ms*/ 30_000);
    assert!(
        consumed >= RECORDS,
        "expected ≥{RECORDS} records from the remote tier after restart, got {consumed}"
    );

    broker.shutdown().await;
}
```

> Reuse the existing JVM produce/consume/`mc ls` plumbing from `tiered_storage_round_trip_through_minio` (`:7903-8093`). If those steps are inline in that test, factor the create-topic / produce / wait-for-segments / consume blocks into small helpers (`create_tiered_topic_via_jvm`, `produce_via_jvm`, `wait_for_tiered_segments`, `consume_via_jvm`) and call them from both tests. The `consume_via_jvm` helper returns the count of records read (parse `--max-messages` output / line count).

- [ ] **Step 4: Run locally (Docker required)**

Run:
```bash
cargo test -p crabka-broker --test jvm_acceptance tiered_storage_topic_rlmm_survives_restart -- --ignored --nocapture
```
Expected: PASS — all 200 records consumed from the remote tier after restart. Also re-run the existing test to confirm the helper change didn't break it:
```bash
cargo test -p crabka-broker --test jvm_acceptance tiered_storage_round_trip_through_minio -- --ignored --nocapture
```

- [ ] **Step 5: clippy + fmt + commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo fmt --all
git -C <worktree> add crates/broker/tests/jvm_acceptance.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
test(broker): JVM acceptance — topic-backed RLMM survives broker restart

Boot the topic-backed RLMM + MinIO S3, tier+evict segments via a JVM producer,
restart the broker against the same log.dir, and consume all records from the
remote tier — proving __remote_log_metadata + snapshot durability. Parameterize
start_host_broker_with_minio_tier on RlmmKind; pin the existing round-trip test
to InMemory.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: JVM acceptance — T2 multi-broker metadata sharing

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs` (add `tiered_storage_topic_rlmm_multi_broker_metadata_sharing`; add a multi-broker + MinIO start helper)

**Context:** Sequential after Task 6 (same file). Prove a broker can serve a remote read from metadata it consumed off `__remote_log_metadata` without having tiered the segment itself. Use a 2-broker cluster (model on `start_two_sasl_brokers`, `:3689`, but plaintext + MinIO tier on both, replicated topic). Tier on the leader; move leadership to broker B (controlled shutdown of the leader, as in `acks_all_survives_leader_crash`, `:1474`); consume via B.

- [ ] **Step 1: Add a 2-broker + MinIO start helper**

Add to `crates/broker/tests/jvm_acceptance.rs`, modeled on `start_two_sasl_brokers` (`:3689`) but plaintext and with the S3 tiered backend + topic-backed RLMM on **both** brokers:
```rust
async fn start_two_brokers_with_minio_tier(
    s3: crabka_remote_storage::S3Config,
) -> (
    crabka_broker::BrokerHandle,
    crabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    // Broker 1: listen 0.0.0.0:9092 advertised host.docker.internal:9092,
    //           controller 0.0.0.0:9093.
    // Broker 2: listen 0.0.0.0:9094 advertised host.docker.internal:9094,
    //           controller 0.0.0.0:9095.
    // controller_quorum_voters = [(1, .:9093), (2, .:9095)] on both.
    // Each: remote_storage_backend = S3(s3.clone()),
    //       remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig::default()),
    //       remote_log_manager_interval = 1s,
    //       heartbeat_interval_ms = 200, heartbeat_timeout_ms = 2_000,
    //       replica_lag_time_max_ms = 2_000  (fast election on leader loss).
    // Spawn BOTH Broker::start futures concurrently (tokio::spawn) and join —
    // Broker::start blocks until the quorum has a committed leader, so awaiting
    // only broker 1 would deadlock (see three_node_jvm_round_trip :605).
}
```
(Copy the concurrent-spawn-and-join idiom verbatim from `start_two_sasl_brokers` / `three_node_jvm_round_trip`.)

- [ ] **Step 2: Write the multi-broker test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)]
async fn tiered_storage_topic_rlmm_multi_broker_metadata_sharing() {
    const TOPIC: &str = "crabka-tiered-multi-itest";
    const RECORDS: usize = 200;

    let _minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);
    let s3 = /* same S3Config shape as Task 6 */;

    let (b1, b2, _d1, _d2) = start_two_brokers_with_minio_tier(s3).await;
    nc_check_connectivity();

    // rf=2 topic so both brokers replicate the partition; tiering on.
    create_tiered_topic_via_jvm_rf(TOPIC, /*partitions*/ 1, /*rf*/ 2);
    produce_via_jvm(TOPIC, RECORDS);
    wait_for_tiered_segments(MINIO_BUCKET, 2).await;

    // Determine the partition leader and shut it down so leadership moves to
    // the other broker. (Reuse the leader-discovery + kill pattern from
    // acks_all_survives_leader_crash :1474 — or, simpler, shut down b1 and
    // address b2's advertised port directly for the consume.)
    b1.shutdown().await;
    // Allow the surviving broker to win the election + the RLMM reconciler to
    // pick up the metadata partitions for the now-led user partition.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Consume via the SURVIVING broker (b2): it serves the remote read using
    // metadata it consumed from __remote_log_metadata, never having tiered
    // the segment itself. Point the consumer at b2's advertised port.
    let consumed = consume_via_jvm_at(TOPIC, RECORDS, B2_BOOTSTRAP, 30_000);
    assert!(
        consumed >= RECORDS,
        "expected ≥{RECORDS} records served from the remote tier by the surviving broker, got {consumed}"
    );

    b2.shutdown().await;
}
```

> Add a `consume_via_jvm_at(topic, expected, bootstrap, timeout_ms)` variant of `consume_via_jvm` that targets a specific advertised `host.docker.internal:<port>`. `create_tiered_topic_via_jvm_rf` is the rf-parameterized variant of the Task 6 helper.

- [ ] **Step 3: Run locally (Docker required)**

Run:
```bash
cargo test -p crabka-broker --test jvm_acceptance tiered_storage_topic_rlmm_multi_broker_metadata_sharing -- --ignored --nocapture
```
Expected: PASS — the surviving broker serves all records from the remote tier. (Multi-broker JVM runs on the local Mac Docker setup per the project's environment.)

- [ ] **Step 4: clippy + fmt + commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo fmt --all
git -C <worktree> add crates/broker/tests/jvm_acceptance.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
test(broker): JVM acceptance — multi-broker tiered metadata sharing

Two-broker cluster + MinIO + topic-backed RLMM; tier on the leader, fail it
over, and serve the remote read from the surviving broker using metadata it
consumed off __remote_log_metadata.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: README + STATUS + matrix flip

**Files:**
- Modify: `README.md` (prose `:101-111`; feature table `:184`; KIP table `:405`)
- Modify: `STATUS.md` (prepend a new slice entry)

**Context:** Only after Tasks 1-7 pass (locally, with Docker for T1/T2). Correct the stale prose, flip both KIP-405 rows, and document the validation + the explicit non-goal (no JVM `__remote_log_metadata` record interop).

- [ ] **Step 1: Rewrite the stale prose** (`README.md:101-111`)

Replace the "Tiered storage (KIP-405) is partial … not yet wired into the broker" sentence with prose describing the topic-backed RLMM as the default durable RLMM, and state the non-goal. Suggested:
```markdown
Tiered storage (KIP-405) is complete for Crabka-only clusters: the
`crabka-remote-storage-topic` topic-backed `RemoteLogMetadataManager`
(`__remote_log_metadata`) is the default whenever tiered storage is enabled,
with the in-memory manager retained only as an explicit test/dev opt-out. The
copy/read/retention paths, snapshots, dynamic per-broker metadata-partition
assignment, and TLS/SASL on the metadata client are all in tree and
JVM-validated (single-broker restart durability + multi-broker metadata
sharing over MinIO/S3). Deliberate non-goal: the `__remote_log_metadata`
record format is **not** byte-compatible with the JVM's `RemoteLogMetadataSerde`,
so a mixed JVM+Crabka tiered cluster sharing the internal topic is not
supported (real clusters run a single RLMM implementation).
```

- [ ] **Step 2: Flip the feature table row** (`README.md:184`)

Change:
```markdown
| Tiered storage (KIP-405) | ⚠️ |
```
to:
```markdown
| Tiered storage (KIP-405) | ✅ |
```

- [ ] **Step 3: Flip the KIP table row** (`README.md:405`)

Change:
```markdown
| [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405) | Kafka tiered storage | ⚠️ |
```
to:
```markdown
| [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405) | Kafka tiered storage | ✅ |
```

- [ ] **Step 4: Add a STATUS.md slice entry**

Prepend (just under the top, matching the existing newest-first slice format) a `## Slice — Tiered storage: topic-backed RLMM promoted & JVM-validated (KIP-405) (2026-06-02)` entry summarizing: RlmmKind default flip, fail-closed NotReadyRlmm + retry bootstrap, T1 restart durability + T2 multi-broker JVM tests, operator default, and the explicit `__remote_log_metadata` JVM-record-interop non-goal. Match the prose style of the existing KIP-405 slice entries (48a–48f-alt).

- [ ] **Step 5: Full workspace gate**

Run:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: fmt clean, clippy clean, all non-Docker tests PASS.

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add README.md STATUS.md
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
docs: KIP-405 tiered storage ✅ — topic-backed RLMM promoted & JVM-validated

Flip the KIP-405 feature + KIP rows ⚠️→✅, correct the stale "not wired" prose,
and document the topic-backed RLMM default, the JVM validation, and the
deliberate no-mixed-cluster-record-interop non-goal.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- W1 (topic-backed default + RlmmKind + auto-derived bootstrap) → Tasks 1, 3 (kickoff snapshot_dir derivation; bootstrap address already derived at `broker.rs:2106`), 5 (operator default).
- W2 (fail-closed + retry) → Tasks 2 (NotReadyRlmm), 3 (retry loop + placeholder), 4 (fail-closed window test).
- W3 (T1 restart durability, T2 multi-broker) → Tasks 6, 7.
- W4 (README + STATUS + matrix) → Task 8.
- Non-goal (no JVM record interop) → stated in Task 8 prose.

**Placeholder scan:** Each code step shows real code. Two `verify the accessor names` / `confirm the error type` notes are intentional API-confirmation guards, not work placeholders — the surrounding code and the behavior-pinning assertions are concrete.

**Type consistency:** `RlmmKind` / `RlmmKind::TopicBacked` / `RlmmKind::InMemory`, `KafkaRlmmConfig` (+ `::default()`), `NotReadyRlmm::new()`, `tiered_storage_rlmm_bootstrap_attempts`, `start_host_broker_with_minio_tier(s3, rlmm) -> (.., .., BrokerConfig)` used consistently across tasks.

**Known follow-ups for the implementer (not blockers):** confirm `FileKafkaRlmmConfig.bootstrap` optionality (Task 1 Step 5); confirm the `crabka_remote_storage_topic` start-error type name (Task 3 Step 7); confirm `MetadataManagerType::InMemory` variant exists or add it (Task 5 Step 3).
