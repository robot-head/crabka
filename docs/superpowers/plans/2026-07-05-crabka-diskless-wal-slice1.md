# Diskless WAL — Slice 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Install the `WalStore` seam behind the partition writer and move a diskless-mode topic's `acks=all` durability off the ISR high-watermark onto a WAL durable-commit (`fsync`), reusing `ReplicaState` as the client-facing watermark — plus the Delta A stateright proof (`wal_acked` never lost).

**Architecture:** Everything lands behind the existing `writer_tx` mpsc channel. The wire handler (`process_partition`) and the `acks=all` gate (`finalize_ack`/`await_hw_at_least`) are untouched. The partition writer's Produce arm branches on a per-topic `diskless` flag: the classic path is unchanged; the diskless path appends to the local `Log` (offsets stay local), `fsync`s via a new `WalStore`, then advances `ReplicaState`'s HW from the *durable* offset. A single-node `LocalFsyncWal` is the Slice-1 medium; later slices swap it for a replicated/object-store WAL without touching callers.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `tokio`, `async-trait`, `stateright` (dev, model checking), `assert2`, `mockall` where a seam needs mocking, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice1-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice1-design.md).

---

## Invariants

1. **Wire path untouched.** Nothing above `writer_tx` changes: `crates/broker/src/handlers/produce.rs` (`finalize_ack`, `:778-784`), `await_hw_at_least` (`partition.rs:538`), and Fetch/Metadata handlers stay byte-identical. Guaranteed by only editing the writer, `ReplicaState`, and new files.
2. **Classic path byte-identical.** The writer's Produce arm keeps its exact existing behavior when the topic is not diskless (`wal: None`). The diskless branch is additive.
3. **Offsets stay local.** Slice 1 assigns offsets via `Log::log_end_offset()` (through the existing `append_produce_batch`). No KRaft offsets (Slice 2).
4. **Durability ordering.** For a diskless partition, the HW must advance only *after* the `fsync` completes — never before. This is the crux of Delta A.
5. **`acks=1` latency preserved.** The offset oneshot is resolved right after append (before `fsync`), so `acks=0/1` do not wait for durability; only the `acks=all` HW-gate does.
6. **Single-node medium only.** `LocalFsyncWal` survives crash-restart, NOT node/disk loss. The spec and proof scope say so; do not claim more.
7. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** `Log::sync()`; the `WalStore` seam + `LocalFsyncWal`; `recompute_hw_for_wal_durable`; the diskless flag + writer branch + wiring; the `wal_acked` stateright extension; unit + behavioral + model tests.
- **Deferred:** KRaft offsets (S2), object-store flush + index (S3), diskless fetch over shared objects (S4), crash-mid-flush atomicity + stateless recovery (S5), quorum/multi-AZ + concurrent-sequencer proof + Jepsen (S6), any Creusot kernel, exposing `diskless` via `kafka-configs`/CreateTopics.

---

## File Structure

- **Create `crates/broker/src/wal/mod.rs`** — the `WalStore` trait. One responsibility: the durability seam.
- **Create `crates/broker/src/wal/local_fsync.rs`** — `LocalFsyncWal` (wraps `Arc<Mutex<Log>>`).
- **Modify `crates/broker/src/lib.rs`** (or `mod.rs`) — `mod wal;`.
- **Modify `crates/log/src/log.rs`** — add `Log::sync()`.
- **Modify `crates/broker/src/replica_state.rs`** — add `recompute_hw_for_wal_durable`.
- **Modify `crates/broker/src/partition_writer.rs`** — thread `wal: Option<Arc<dyn WalStore>>` into `run`; branch the Produce arm.
- **Modify `crates/broker/src/partition.rs`** — construct `LocalFsyncWal` for diskless topics; pass `wal` at every `partition_writer::run` call site.
- **Modify the production `Partition` constructor** — read the `diskless` topic-config flag from the metadata image.
- **Modify `crates/broker/src/data_path_model.rs`** — `wal_acked` ghost + `WalSync` action + `wal_acked_durable` property + a diskless model config + test.

---

## Task 1: `Log::sync()` — explicit fsync of the active segment

`Log` exposes no public durability call today; the diskless WAL needs one it controls (independent of `flush_on_append`). Mirror the flush the append path already performs at `crates/log/src/log.rs:579`.

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write the failing test**

In `crates/log/src/log.rs`'s `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn sync_persists_appended_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut sample_batch(3)).unwrap();
            log.sync().unwrap(); // fsync without relying on flush_on_append
        }
        // Reopen from disk: the synced records are present.
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.log_end_offset() == Offset(3));
    }
```

(If `sample_batch`/`Offset` are not already in scope in this test module, reuse the crate's existing test batch helper — grep the module for how other tests build a `RecordBatch`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-log sync_persists_appended_records`
Expected: FAIL — no method `sync` on `Log`.

- [ ] **Step 3: Implement `Log::sync()`**

Add to `impl Log` in `crates/log/src/log.rs`. Mirror how `append` flushes the active segment under `flush_on_append` (`log.rs:579`): call the same active-segment flush (`Segment::flush` → `sync_data`, `segment.rs:939-940`).

```rust
    /// Flush and `fsync` the active segment to stable storage, independent of
    /// [`LogConfig::flush_on_append`]. Used by the diskless WAL path to make
    /// appended records durable before acknowledging a produce.
    ///
    /// # Errors
    /// Returns a [`LogError`] if the underlying segment flush fails.
    pub fn sync(&self) -> Result<(), LogError> {
        self.active_segment_flush()
    }
```

where `active_segment_flush()` is the existing internal call the `flush_on_append` branch uses at `log.rs:579` (reuse it; if that logic is inline, extract it into a private `fn active_segment_flush(&self) -> Result<(), LogError>` and call it from both the append path and `sync`). Do not change the append path's behavior — only extract-and-reuse.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-log sync_persists_appended_records`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/log/src/log.rs
git commit -m "feat(log): add Log::sync() to fsync the active segment on demand"
```

---

## Task 2: The `WalStore` seam + `LocalFsyncWal`

**Files:**
- Create: `crates/broker/src/wal/mod.rs`
- Create: `crates/broker/src/wal/local_fsync.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Declare the module**

In `crates/broker/src/lib.rs` (alongside the other `mod` declarations), add:

```rust
mod wal;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/broker/src/wal/local_fsync.rs` with its test module first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use tokio::sync::Mutex;

    use super::*;
    use crate::partition::ProduceData;

    fn wal(dir: &std::path::Path) -> LocalFsyncWal {
        let log = Arc::new(Mutex::new(Log::open(dir, LogConfig::default()).unwrap()));
        LocalFsyncWal::new(log)
    }

    #[tokio::test]
    async fn append_assigns_sequential_offsets_then_sync_advances_durable() {
        let dir = tempfile::tempdir().unwrap();
        let w = wal(dir.path());
        let (results, leo) = w.append(vec![sample_owned(2), sample_owned(3)]).await.unwrap();
        assert!(results.iter().all(Result::is_ok));
        assert!(leo == crabka_ids::Offset(5)); // 2 + 3 records
        // Durable watermark only advances after sync_durable.
        let durable = w.sync_durable(leo).await.unwrap();
        assert!(durable == leo);
    }

    // Builds an owned RecordBatch of `n` records — reuse the crate's existing
    // test batch helper if one is importable; otherwise a minimal builder.
    fn sample_owned(n: i32) -> ProduceData {
        ProduceData::Owned(crate::testutil::sample_batch(n))
    }
}
```

(If `crate::testutil::sample_batch` does not exist, reuse the batch builder that `partition_writer.rs` tests use — `sample_batch` at `partition_writer.rs:565` — by pulling it into a shared test helper or copying its body into this module's test.)

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-broker wal::local_fsync`
Expected: FAIL — `LocalFsyncWal`/`WalStore` undefined.

- [ ] **Step 4: Define the `WalStore` trait**

Create `crates/broker/src/wal/mod.rs`:

```rust
//! The WAL durability seam. Slice 1 of the diskless broker: a two-phase
//! "append (assign offsets) then make durable" contract that the partition
//! writer drives for diskless-mode topics. The Slice-1 implementation is
//! [`local_fsync::LocalFsyncWal`]; later slices swap it for a replicated /
//! object-store-backed WAL without changing the writer or the ack gate.

mod local_fsync;

use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::Offset;

pub use local_fsync::LocalFsyncWal;

use crate::{error::BrokerError, partition::ProduceData};

/// A durability medium behind the partition writer.
///
/// Two-phase so the writer can resolve the produce offset (for `acks=0/1`)
/// before durability completes, then gate `acks=all` on `sync_durable`:
/// 1. [`WalStore::append`] assigns offsets and returns the post-append LEO,
///    WITHOUT waiting for durability.
/// 2. [`WalStore::sync_durable`] makes everything up to `leo` durable and
///    returns the (monotonic) durable LEO.
#[async_trait]
pub trait WalStore: Send + Sync {
    /// Append a group of batches, assigning offsets. Not yet durable.
    async fn append(
        &self,
        datas: Vec<ProduceData>,
    ) -> Result<(Vec<Result<Offset, BrokerError>>, Offset), BrokerError>;

    /// Make all records up to `leo` durable; return the durable LEO. Never
    /// regresses the durable watermark.
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError>;
}

/// Convenience alias for an injected WAL medium (present only for diskless topics).
pub type SharedWal = Arc<dyn WalStore>;
```

- [ ] **Step 5: Implement `LocalFsyncWal` above its test module**

Insert at the TOP of `crates/broker/src/wal/local_fsync.rs`:

```rust
//! Slice-1 WAL medium: a single-node, `fsync`-durable WAL that reuses the
//! partition's existing local `Log`. Offsets are assigned locally (Slice 2
//! moves them to KRaft); durability is a local `fsync` (Slice 6 upgrades to a
//! cross-AZ quorum). Survives crash-restart, NOT node/disk loss.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::Offset;
use crabka_log::Log;
use tokio::sync::Mutex;

use super::WalStore;
use crate::{error::BrokerError, partition::ProduceData};

/// A [`WalStore`] backed by the partition's local `Log` plus an explicit
/// `fsync` (`Log::sync`).
pub struct LocalFsyncWal {
    log: Arc<Mutex<Log>>,
}

impl LocalFsyncWal {
    #[must_use]
    pub fn new(log: Arc<Mutex<Log>>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WalStore for LocalFsyncWal {
    async fn append(
        &self,
        datas: Vec<ProduceData>,
    ) -> Result<(Vec<Result<Offset, BrokerError>>, Offset), BrokerError> {
        // Reuse the exact offset-assigning append the classic path uses, so
        // offsets stay locally assigned and identical to a classic topic.
        crate::partition_writer::run_produce_append_batch(self.log.clone(), datas).await
    }

    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        let log = self.log.clone();
        // fsync off the async poller (mirrors run_produce_append_batch's
        // block_in_place / spawn_blocking discipline).
        let res = match tokio::runtime::Handle::current().runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| log.blocking_lock().sync())
            }
            _ => {
                tokio::task::spawn_blocking(move || log.blocking_lock().sync())
                    .await
                    .map_err(|e| BrokerError::from(crate::partition_writer::storage_failure_error(
                        "wal fsync task panicked",
                        &e,
                    )))?
            }
        };
        res.map_err(BrokerError::from)?;
        Ok(leo)
    }
}
```

Notes for the implementer:
- `run_produce_append_batch` and `storage_failure_error` are currently private to `partition_writer.rs` (`:138`, `:87`). Make them `pub(crate)` so the WAL module can reuse the exact append + error-shaping logic (do not duplicate them).
- `Log::sync()` is Task 1. `blocking_lock()` is `tokio::sync::Mutex`'s sync lock, valid inside `block_in_place`/`spawn_blocking`.

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-broker wal::local_fsync`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/wal/ crates/broker/src/lib.rs crates/broker/src/partition_writer.rs
git commit -m "feat(broker): add WalStore seam + LocalFsyncWal (single-node fsync WAL)"
```

---

## Task 3: `ReplicaState::recompute_hw_for_wal_durable`

A durability-gated HW advance, distinct in name from the append-driven one so the diskless call site documents that the advance follows an `fsync`. Same HW math (`compute_hw`).

**Files:**
- Modify: `crates/broker/src/replica_state.rs`

- [ ] **Step 1: Write the failing test**

In `crates/broker/src/replica_state.rs`'s `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn wal_durable_advances_hw_to_durable_offset_for_singleton_isr() {
        let mut st = ReplicaState::new();
        st.install_isr_for_test(&[]); // no followers: ISR = {leader}
        let hw = st.recompute_hw_for_wal_durable(off(5));
        assert!(hw == off(5)); // HW = leader durable LEO when there are no followers
    }
```

(Use whatever test constructor the module already uses to set an empty follower set / singleton ISR — grep the test module for how existing HW tests seed `ReplicaState`. `off(..)` is the module's existing offset shorthand at `replica_state.rs:159`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker wal_durable_advances_hw`
Expected: FAIL — no method `recompute_hw_for_wal_durable`.

- [ ] **Step 3: Implement it**

In `impl ReplicaState`, next to `recompute_hw_for_leader_append` (`replica_state.rs:126`):

```rust
    /// Recompute the high watermark after the WAL has made records durable up
    /// to `durable_leo`. Identical HW arithmetic to
    /// [`Self::recompute_hw_for_leader_append`], but named separately because
    /// the diskless path advances the HW only AFTER an `fsync` — durability,
    /// not mere append, is what the `acks=all` gate then observes.
    pub(crate) fn recompute_hw_for_wal_durable(&mut self, durable_leo: Offset) -> Offset {
        self.hw = self.compute_hw(durable_leo);
        self.hw
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker wal_durable_advances_hw`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/replica_state.rs
git commit -m "feat(broker): add ReplicaState::recompute_hw_for_wal_durable"
```

---

## Task 4: Thread the diskless flag + `WalStore` into the writer

Add an optional `wal` to `partition_writer::run` and to `Partition`; read the `diskless` topic-config flag in the production constructor. This task only wires the plumbing (no behavior change yet — the writer ignores `wal` until Task 5), so the workspace stays green.

**Files:**
- Modify: `crates/broker/src/partition_writer.rs`
- Modify: `crates/broker/src/partition.rs`
- Modify: the production `Partition` constructor (find it: `grep -rn "partition_writer::run" crates/broker/src` — every call site takes the new arg).

- [ ] **Step 1: Add the `wal` parameter to `run`**

In `crates/broker/src/partition_writer.rs`, extend `run`'s signature (`:156-167`) with a trailing parameter:

```rust
    producer_state: Arc<ProducerState>,
    wal: Option<crate::wal::SharedWal>,
) {
```

Do not use it yet (Task 5). Add `let _ = &wal;` at the top of `run` if clippy flags it as unused this task, and remove that line in Task 5.

- [ ] **Step 2: Pass `wal` at every call site**

- In `crates/broker/src/partition.rs` test helper `test_partition_with_writer` (`:669`) and any other test spawn, pass `None` as the final argument.
- In the **production** `Partition` constructor, derive the flag and medium:

```rust
    let diskless = image
        .topic_config(&topic)
        .and_then(|c| c.get("crabka.diskless"))
        .map(|v| v == "true")
        .unwrap_or(false);
    let wal: Option<crate::wal::SharedWal> = diskless
        .then(|| Arc::new(crate::wal::LocalFsyncWal::new(log.clone())) as crate::wal::SharedWal);
```

pass `wal` as the final argument to `partition_writer::run(...)`. (`image` is the metadata image available at partition construction; `topic_config` returns `Option<&BTreeMap<String,String>>` — `crates/metadata/src/image.rs:218`. If the constructor doesn't already hold the image, thread the resolved `diskless: bool` in from where the partition is created — the metadata image is available at broker/partition-set construction.)

- [ ] **Step 3: Store `wal` on `Partition` if the writer is respawned**

If `Partition` re-spawns its writer anywhere (grep for other `partition_writer::run` calls), add a `wal: Option<crate::wal::SharedWal>` field to `Partition` so respawns reuse it. If the writer is spawned exactly once at construction, skip this step.

- [ ] **Step 4: Verify the workspace compiles and is green**

Run: `cargo test -p crabka-broker`
Expected: PASS — pure plumbing; no behavior changed.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/partition_writer.rs crates/broker/src/partition.rs
git commit -m "feat(broker): thread optional WalStore + diskless flag into partition writer"
```

---

## Task 5: Branch the writer's Produce arm for diskless

Insert the diskless durability path: append → resolve offsets → `append_notify` → `wal.sync_durable` (fsync) → `recompute_hw_for_wal_durable` → notify. The classic path (`wal: None`) is byte-identical.

**Files:**
- Modify: `crates/broker/src/partition_writer.rs`

- [ ] **Step 1: Write the failing test**

In `crates/broker/src/partition_writer.rs` tests (near `writer_appends_and_acks`, `:620`), add a diskless variant that asserts the HW (and thus an `acks=all` release) only advances after a durable append. Drive it through the public channel:

```rust
    #[tokio::test]
    async fn diskless_writer_acks_all_gates_on_durable_hw() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let wal: Option<crate::wal::SharedWal> =
            Some(Arc::new(crate::wal::LocalFsyncWal::new(log.clone())));
        let replica_state = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
        let hw_advance_notify = Arc::new(Notify::new());
        // ... spawn run(...) with `wal`, empty ISR (singleton), like writer_appends_and_acks ...
        // produce one batch of 3 records via WriterMessage::Produce
        // await the offset oneshot -> Ok(Offset(0))
        // after the writer processes it, the HW must be 3 (durable) and
        // await_hw_at_least(Offset(3)) resolves.
        // Assert: replica_state.lock().await.hw == off(3).
    }
```

(Mirror the exact spawn/produce scaffolding of `writer_appends_and_acks` at `:620-666`, adding `wal` as the final `run` arg. The assertion is that the durable HW reaches the produced LEO.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker diskless_writer_acks_all_gates_on_durable_hw`
Expected: FAIL — diskless topics currently take the classic (non-fsync) path; the test's durability assertion or wiring differs. (It may compile-fail first if `run` doesn't branch yet — that's the failing state.)

- [ ] **Step 3: Branch the Produce arm**

In `crates/broker/src/partition_writer.rs`, in the `WriterMessage::Produce` arm, after the ack fan-out + `append_notify.notify_waiters()` and in place of the current `if any_ok { ... recompute_hw_for_leader_append ... }` block (`:250-264`), branch on `wal`:

```rust
                if any_ok {
                    // Wake long-poll fetchers once for the whole group.
                    append_notify.notify_waiters();

                    let advanced = if let Some(wal) = &wal {
                        // DISKLESS: make the group durable (fsync) BEFORE advancing
                        // the HW, so an acks=all ack means fsync-durable, not just
                        // page-cache-present. Offsets were already resolved to acks
                        // above (acks=0/1 do not wait for this fsync).
                        match wal.sync_durable(leo).await {
                            Ok(durable) => {
                                let mut st = replica_state.lock().await;
                                let prev = st.hw;
                                st.recompute_hw_for_wal_durable(durable) > prev
                            }
                            Err(e) => {
                                flag_storage_failure(&e, &log_dir, &log_dir_status);
                                false
                            }
                        }
                    } else {
                        // CLASSIC: unchanged — advance HW from the appended LEO.
                        let mut st = replica_state.lock().await;
                        let prev = st.hw;
                        st.recompute_hw_for_leader_append(leo) > prev
                    };

                    if advanced {
                        hw_advance_notify.notify_waiters();
                    }
                }
```

Remove the temporary `let _ = &wal;` from Task 4. Note the ack fan-out (`:238-248`) is unchanged and still runs *before* this block, so `acks=0/1` are already resolved.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker diskless_writer_acks_all_gates_on_durable_hw`
Expected: PASS.

- [ ] **Step 5: Run the full writer suite (classic path unregressed)**

Run: `cargo test -p crabka-broker partition_writer`
Expected: PASS — `writer_appends_and_acks`, the grouping test, and the multi-thread test all stay green (classic `wal: None` path unchanged).

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/partition_writer.rs
git commit -m "feat(broker): diskless writer path — fsync then durable HW advance"
```

---

## Task 6: Behavioral test — the fsync-durability semantic

Prove the one new observable behavior: a diskless `RF=1, acks=all` record whose produce was acknowledged survives a crash-restart (reopen the log from disk), where a classic `RF=1` produce (page-cache only, `flush_on_append=false`) would not be guaranteed to.

**Files:**
- Modify: `crates/broker/src/partition_writer.rs` (or a broker integration test module — place it where `Partition`/writer scaffolding is reachable).

- [ ] **Step 1: Write the test**

```rust
    #[tokio::test]
    async fn diskless_acked_record_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = Arc::new(Mutex::new(
                Log::open(dir.path(), LogConfig::default()).unwrap(),
            ));
            let wal: Option<crate::wal::SharedWal> =
                Some(Arc::new(crate::wal::LocalFsyncWal::new(log.clone())));
            // spawn run(...) with `wal`; produce one batch; await the acks=all
            // release (HW reaches the LEO). Then drop the writer/log (simulated
            // crash: no further flush).
        }
        // Reopen the log from the same dir: the acked record is durably present.
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.log_end_offset() >= Offset(1));
    }
```

This exercises behavior (durability across reopen), not source text. It is the Slice-1 "new semantic" acceptance check.

- [ ] **Step 2: Run to verify it passes**

Run: `cargo test -p crabka-broker diskless_acked_record_survives_reopen`
Expected: PASS — the `sync_durable` fsync in Task 5 made the record durable before the acks=all release.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/partition_writer.rs
git commit -m "test(broker): diskless acks=all record survives crash-restart"
```

---

## Task 7: Stateright — the `wal_acked` ghost + Delta A property

Extend the durability model with a diskless mode: a `wal_acked` ghost (records the fsync-durable prefix, mirroring how `committed` tracks the HWM prefix) and an always-property that a `wal_acked` record is never lost. This is the Slice-1 shipping-gate check.

**Files:**
- Modify: `crates/broker/src/data_path_model.rs`

- [ ] **Step 1: Add the ghost field + a diskless model flag**

In `DpState` (`data_path_model.rs:59-68`) add a ghost, and include it in `proj()` (`:75-86`) so it participates in state identity:

```rust
    wal_acked: Vec<u8>, // ghost: wal_acked[off] = epoch, for offsets made WAL-durable (diskless mode)
```

In `DpModel` (`:310-313`) add:

```rust
    diskless: bool, // true drives the WAL durability path instead of ISR-HWM
```

Initialize `wal_acked: vec![]` in `init_states` (`:319-330`).

- [ ] **Step 2: Add the `WalSync` action**

In `enum Act` (`:296-308`) add:

```rust
    WalSync, // diskless: make the leader's appended prefix fsync-durable
```

In `actions` (`:332-385`), offer it only in diskless mode with a live leader:

```rust
        if self.diskless && leader_live {
            acts.push(Act::WalSync);
        }
```

- [ ] **Step 3: Model `WalSync` in `next_state`**

In `next_state` (`:387-450`) add an arm. `WalSync` records the leader's whole current log into `wal_acked` (the fsync makes the appended prefix durable — offsets stay local, so the durable prefix is the leader's LEO):

```rust
            Act::WalSync => {
                // fsync makes the leader's appended prefix durable. Record it in
                // the wal_acked ghost (mirrors AdvanceHwm recording `committed`).
                let leader_log = &s.log[s.leader as usize];
                while s.wal_acked.len() < leader_log.len() {
                    let off = s.wal_acked.len();
                    s.wal_acked.push(leader_log[off]);
                }
            }
```

- [ ] **Step 4: Add the Delta A property**

In `properties` (`:452`), add an always-property (grouped with `committed_durable`):

```rust
            Property::always("wal_acked_durable", |_, s: &DpState| {
                let lg = &s.log[s.leader as usize];
                s.wal_acked
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
```

This asserts every WAL-acked record is still present, unchanged, in the leader's durable log — i.e. no `wal_acked` record is ever lost. (Because Slice 1 is single-node `RF=1`, the acking broker's log persists across `Die`/`Revive`; the property must survive every interleaving the checker explores.)

- [ ] **Step 5: Add a diskless checker configuration + test**

Mirror the existing `data_clean`/`data_unclean` check tests (`:540/:551`). Add a `diskless: true, unclean: false` model config and assert the checker passes with `wal_acked_durable` holding and `wal_acked` sometimes non-empty:

```rust
    #[test]
    fn data_diskless_wal_acked_never_lost() {
        let model = DpModel { base: Instant::now(), unclean: false, diskless: true };
        // ... run the same BFS checker harness the data_clean test uses,
        // asserting the `wal_acked_durable` always-property holds and adding a
        // `Property::sometimes("wal_acked_progress", |_, s| !s.wal_acked.is_empty())`
        // to prove the state space actually exercises WAL durability.
    }
```

Set the existing (`diskless: false`) `DpModel` constructions in `data_clean`/`data_unclean` accordingly (add `diskless: false`).

- [ ] **Step 6: Run the model checker**

Run: `cargo test -p crabka-broker data_diskless_wal_acked_never_lost -- --nocapture`
Expected: PASS — the checker explores the diskless state space and `wal_acked_durable` holds on every reachable state. If it finds a counterexample, that is the model doing its job: it means the durability ordering (append → fsync → record `wal_acked`) admits a loss interleaving — reconcile the `WalSync`/`next_state` ordering with the real writer (Task 5) until the property holds and is meaningful. Do NOT weaken the property to force a pass.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/data_path_model.rs
git commit -m "test(broker): stateright wal_acked ghost + Delta A no-loss property"
```

---

## Task 8: Final gate — format, lint, full test + model sweep

**Files:** none (formatting only).

- [ ] **Step 1: Format**

Run: `cargo +nightly fmt`

- [ ] **Step 2: Format check**

Run: `cargo +nightly fmt --check`
Expected: no diff.

- [ ] **Step 3: Clippy (pedantic, deny warnings)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Full test + model sweep**

Run: `cargo nextest run -p crabka-log -p crabka-broker` (or `cargo test` for those crates)
Expected: PASS — including the diskless writer, reopen-durability, and `data_diskless_wal_acked_never_lost` model check.

- [ ] **Step 5: Commit (only if formatting changed anything)**

```bash
git add -A
git commit -m "style(broker): cargo +nightly fmt"
```

---

## Self-Review

**1. Spec coverage:**
- `WalStore` seam behind `writer_tx` → Task 2. ✅
- `acks=all` off ISR-HW onto WAL durable-commit → Task 5 (fsync before HW advance) + Task 3 (`recompute_hw_for_wal_durable`). ✅
- `fsync`-durability-not-replication semantic (diskless `RF=1 acks=all` crash-safe) → Task 6 (survives reopen). ✅
- Hybrid HWM reuse (same `ReplicaState`, WAL-sourced advance) → Task 3 + Task 5. ✅
- Delta A stateright proof (`wal_acked` never lost) → Task 7. ✅
- Diskless flag internal (topic-config key, not wire) → Task 4. ✅
- Non-goals (KRaft offsets, object flush, quorum, Creusot) → untouched; Scope boundary states them. ✅

**2. Spec correction surfaced:** the spec said "no `crates/log` source change," but `Log` has no public `sync()` — Task 1 adds a minimal one (mirroring the existing `flush_on_append` flush at `log.rs:579`). Flagged, not silent.

**3. Placeholder scan:** Code steps show complete code. The three places that say "mirror the existing X at line N / reuse the crate's test helper" (Task 1 `sample_batch`, Task 5/6 spawn scaffolding, Task 4 constructor image access) point at exact existing code to copy rather than leaving a blank — acceptable because the referenced code is named and located. No `TBD`/`TODO`.

**4. Type consistency:** `WalStore::{append, sync_durable}` signatures (Task 2) match their call sites (Task 5). `run_produce_append_batch`/`storage_failure_error` are made `pub(crate)` (Task 2) before the WAL module and writer both use them. `recompute_hw_for_wal_durable(Offset) -> Offset` (Task 3) matches Task 5's call. `run`'s new `wal: Option<SharedWal>` param (Task 4) matches every call site and the Task 5 branch. `DpState.wal_acked`, `Act::WalSync`, `DpModel.diskless` (Task 7) are introduced together and used consistently in `proj`/`actions`/`next_state`/`properties`.

**5. Invariant check:** wire path and classic path untouched (Invariants 1-2 — only writer branch + new files). Offsets stay local (Invariant 3 — `append` reuses `run_produce_append_batch`). HW advances only after fsync in the diskless branch (Invariant 4 — Task 5 orders `sync_durable` before `recompute_hw_for_wal_durable`). `acks=1` unaffected (Invariant 5 — ack fan-out precedes the durability block). Single-node medium only (Invariant 6 — `LocalFsyncWal` doc + proof scope). Each task ends green (Invariant 7).
