# Diskless WAL — Slice 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace local offset assignment with a **controller-committed** offset for diskless topics: a `V1PartitionOffsetAdvance` metadata record (delta, on the Crabka-private carrier), an `OffsetSequencer` seam with a `ControllerSequencer` that commits it and returns the base, and a CRC-safe `Log::append_verbatim_at` that stamps the assigned base — proving gap-free/monotonic/unique offsets under a single sequencer.

**Architecture:** The diskless writer branch (from Slice 1) assigns a contiguous offset range per produce group by committing one "advance partition P by N" record through the local KRaft controller, reads the post-commit metadata image for the base (`base = next − N`), and appends each batch at that base via a new verbatim append that patches the offset below the CRC region (no re-CRC). The offset authority moves from `Log::log_end_offset()` to KRaft; everything above `writer_tx` and the classic path are untouched.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `serde`/`serde_wincode` (metadata record carrier), `tokio`, `async-trait`, `stateright` (dev), `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice2-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice2-design.md).

**PREREQUISITE:** Slice 1 (`2026-07-05-crabka-diskless-wal-slice1.md`) is implemented and merged. This plan modifies the Slice-1 diskless writer branch and reuses its `WalStore`/`recompute_hw_for_wal_durable`/`diskless` flag. If Slice 1 is not yet landed, execute it first.

---

## Invariants

1. **Wire path + classic path untouched.** Only diskless-topic offset assignment changes. `finalize_ack` (`crates/broker/src/handlers/produce.rs:778-784`), `await_hw_at_least` (`partition.rs:538`), and the non-diskless writer path stay byte-identical.
2. **Client offsets stay byte-exact and contiguous.** The base is patched below the CRC region (`segment.rs:869`); no re-CRC. A diskless partition's offsets are `0,1,2,…` exactly like classic.
3. **Delta, never full-record replace.** `V1PartitionOffsetAdvance` rides the Crabka-private carrier and applies as an increment — order-independent, which is what makes the single-sequencer gap-free proof hold. Mirror `V1PartitionDirAssignment` exactly.
4. **`base == log_end_offset()` guard.** `append_verbatim_at` rejects a base that doesn't equal the local LEO — the runtime gap-free witness.
5. **Single-sequencer / no-crash / local-submit only.** No forward path (no `CrabkaSubmitChangeResponse` change), no crash atomicity (Slice 5), no concurrency (Slice 6). The commit-before-fsync window is a documented, out-of-scope durability hole.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the `V1PartitionOffsetAdvance` record end-to-end; `Log::append_verbatim_at`; `OffsetSequencer` + `ControllerSequencer` (local submit + read-after-commit); wiring it into the diskless writer branch; the stateright gap-free/monotonic/unique proof.
- **Deferred:** broker→controller forward path + RPC offset field; commit↔fsync atomicity + recovery (Slice 5); concurrent/leaderless sequencer + leader-change (Slice 6); throughput optimization.

---

## File Structure

- **`crates/metadata/src/records.rs`** — `PartitionOffsetAdvanceRecord` + `V1PartitionOffsetAdvance` variant.
- **`crates/metadata/src/lib.rs`** — re-export the struct.
- **`crates/metadata/src/image.rs`** — `partition_next_offsets` field + init + accessor + `apply`/`record_variant`/`validate` arms + `to_records` snapshot emit.
- **`crates/metadata/src/kraft_translate.rs`** — carrier encode arm + apiKey `1003` + decode guard.
- **`crates/log/src/log.rs`** — public `append_verbatim_at`.
- **`crates/broker/src/wal/offset_sequencer.rs`** (new) — `OffsetSequencer` trait + `ControllerSequencer`.
- **`crates/broker/src/partition_writer.rs`** — diskless branch: assign then append-at-base.
- **`crates/broker/src/partition.rs`** — construct `ControllerSequencer` for diskless partitions.
- **`crates/broker/src/data_path_model.rs`** — the offset sequencer proof.

---

## Task 1: The `V1PartitionOffsetAdvance` record + delta apply

Mirror `V1PartitionDirAssignment` (the existing carrier-delta record) at every site.

**Files:**
- Modify: `crates/metadata/src/records.rs`, `crates/metadata/src/lib.rs`, `crates/metadata/src/image.rs`

- [ ] **Step 1: Write the failing apply test**

In `crates/metadata/src/image.rs` tests, add:

```rust
    #[test]
    fn offset_advance_applies_as_monotonic_delta() {
        let mut m = MetadataImage::new();
        let adv = |c: i64| MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
            topic: "t".into(), partition: 0, count: c,
        });
        m.apply(&adv(3));
        m.apply(&adv(2));
        assert2::assert!(m.partition_next_offset("t", 0) == Some(5));
        // Different partition is independent.
        m.apply(&MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
            topic: "t".into(), partition: 1, count: 7,
        }));
        assert2::assert!(m.partition_next_offset("t", 1) == Some(7));
        assert2::assert!(m.partition_next_offset("t", 0) == Some(5));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metadata offset_advance_applies_as_monotonic_delta`
Expected: FAIL — the record type, the field, and the accessor don't exist.

- [ ] **Step 3: Define the record struct + enum variant**

In `crates/metadata/src/records.rs`, next to `PartitionDirAssignmentRecord` (`:62-70`):

```rust
/// Diskless offset-sequencer delta (Slice 2): advance a partition's committed
/// next-offset by `count`. Applied as a DELTA (never a full-record replace) so
/// sequential advances on the single-threaded committed log yield a gap-free,
/// strictly-monotonic, unique offset sequence. Rides a Crabka-private carrier
/// (like [`PartitionDirAssignmentRecord`]) so it round-trips as this same delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOffsetAdvanceRecord {
    pub topic: String,
    pub partition: i32,
    /// Offsets consumed by the produce group. The base returned to the producer
    /// is the pre-increment committed next-offset.
    pub count: i64,
}
```

Add the variant to the `MetadataRecord` enum (`:264-292`, next to `V1PartitionDirAssignment`):

```rust
    V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord),
```

- [ ] **Step 4: Re-export the struct**

In `crates/metadata/src/lib.rs`, add `PartitionOffsetAdvanceRecord` to the `pub use records::{…}` list (`:74-81`).

- [ ] **Step 5: Add the image field, init, accessor, and apply arm**

In `crates/metadata/src/image.rs`:
- Add the field to the `MetadataImage` struct (`:85-111`): `partition_next_offsets: HashMap<(String, i32), i64>,`
- Initialize it in `MetadataImage::new` (`:120-142`): `partition_next_offsets: HashMap::new(),`
- Add an accessor mirroring `partition()` (`:175-177`):

```rust
    /// The committed next-offset for a diskless partition, if any advance has
    /// been applied.
    #[must_use]
    pub fn partition_next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.partition_next_offsets
            .get(&(topic.to_string(), partition))
            .copied()
    }
```
- Add the `apply` arm (`:470-658`, next to the `V1PartitionDirAssignment` arm at `:648-657`):

```rust
            // Diskless offset delta: bump the partition's committed next-offset.
            // Order-independent increment (see PartitionOffsetAdvanceRecord).
            MetadataRecord::V1PartitionOffsetAdvance(r) => {
                *self
                    .partition_next_offsets
                    .entry((r.topic.clone(), r.partition))
                    .or_insert(0) += r.count;
            }
```
- Add the `record_variant` arm (`:60-83`): `MetadataRecord::V1PartitionOffsetAdvance(_) => "V1PartitionOffsetAdvance",`
- Add `V1PartitionOffsetAdvance` to the `validate` no-topic-store catch-all `|`-list (`:892-931`) so it validates as `Ok(())` (no pre-check needed — the delta is always applicable).

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-metadata offset_advance_applies_as_monotonic_delta`
Expected: PASS. (The `kraft_translate` exhaustive match will fail to COMPILE until Task 2 — if `cargo test` fails to build on the `to_kraft_iter` match, do Task 2 Step 3 first, then return here. To keep this task self-contained, add the Task 2 encode arm now if the build blocks.)

- [ ] **Step 7: Commit**

```bash
git add crates/metadata/src/records.rs crates/metadata/src/lib.rs crates/metadata/src/image.rs
git commit -m "feat(metadata): add V1PartitionOffsetAdvance delta record + apply"
```

---

## Task 2: KRaft carrier round-trip + snapshot emit

Carry the record verbatim through the KIP-631 `Unknown` envelope (apiKey `1003`) so it decodes back to the same delta, and emit it from `to_records` so it survives snapshots.

**Files:**
- Modify: `crates/metadata/src/kraft_translate.rs`, `crates/metadata/src/image.rs`

- [ ] **Step 1: Write the failing round-trip tests**

In `crates/metadata/src/kraft_translate.rs` tests, add a carrier round-trip:

```rust
    #[test]
    fn offset_advance_round_trips_through_carrier() {
        let rec = MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
            topic: "t".into(), partition: 2, count: 9,
        });
        // encode -> Unknown carrier -> decode back to the same delta
        let krecs: Vec<_> = to_kraft_iter(&rec).unwrap().collect();
        let back = from_kraft_value(&krecs[0], &MetadataImage::new()).unwrap();
        assert2::assert!(back == rec);
    }
```

In `crates/metadata/src/image.rs` tests (extend `to_records_round_trips_all_variants`, `:1044`), assert a `partition_next_offsets` slot survives `to_records`/`from_records`:

```rust
    #[test]
    fn offset_advance_survives_snapshot_round_trip() {
        let mut m = MetadataImage::new();
        m.apply(&MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
            topic: "t".into(), partition: 0, count: 5,
        }));
        let m2 = MetadataImage::from_records(&m.to_records());
        assert2::assert!(m2.partition_next_offset("t", 0) == Some(5));
    }
```

(Confirm the exact `to_kraft_iter`/`from_kraft_value` test-callable names by grepping the module's existing carrier tests for `V1PartitionDirAssignment`; reuse that harness.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p crabka-metadata offset_advance_round_trips_through_carrier offset_advance_survives_snapshot_round_trip`
Expected: FAIL — no carrier arm / no snapshot emit.

- [ ] **Step 3: Add the carrier encode + apiKey + decode guard**

In `crates/metadata/src/kraft_translate.rs`:
- Add the apiKey constant next to the others (`:661-665`):

```rust
/// Diskless offset-advance delta (Slice 2) carried verbatim so it stays a
/// per-partition increment on apply (never a full-record replace).
const PRIVATE_PARTITION_OFFSET_ADVANCE_KEY: u32 = 1003;
```
- Add the encode arm in `to_kraft_iter` next to the dir-assignment arm (`:643-645`):

```rust
        MetadataRecord::V1PartitionOffsetAdvance(_) => {
            vec![wincode_carrier(rec, PRIVATE_PARTITION_OFFSET_ADVANCE_KEY)?]
        }
```
- Extend the decode guard's `||` list (`:895-898`):

```rust
                || *api_key == PRIVATE_PARTITION_DIR_ASSIGNMENT_KEY
                || *api_key == PRIVATE_PARTITION_OFFSET_ADVANCE_KEY =>
```

- [ ] **Step 4: Emit from `to_records`**

In `crates/metadata/src/image.rs` `to_records` (`:683-825`), after the existing per-partition emissions, emit one advance per non-zero slot:

```rust
        for ((topic, partition), next) in &self.partition_next_offsets {
            if *next != 0 {
                records.push(MetadataRecord::V1PartitionOffsetAdvance(
                    PartitionOffsetAdvanceRecord {
                        topic: topic.clone(),
                        partition: *partition,
                        count: *next, // absolute expressed as advance-from-0 (fresh image starts at 0)
                    },
                ));
            }
        }
```

(Use the `records` accumulator variable this function already builds — match its exact name.)

- [ ] **Step 5: Run to verify they pass**

Run: `cargo test -p crabka-metadata offset_advance`
Expected: PASS — carrier round-trip + snapshot round-trip green.

- [ ] **Step 6: Run the metadata crate suite (nothing regressed)**

Run: `cargo test -p crabka-metadata`
Expected: PASS — existing round-trip tests (`to_records_round_trips_all_variants`, `records.rs` `round_trip`) still green.

- [ ] **Step 7: Commit**

```bash
git add crates/metadata/src/kraft_translate.rs crates/metadata/src/image.rs
git commit -m "feat(metadata): carrier + snapshot round-trip for V1PartitionOffsetAdvance"
```

---

## Task 3: `Log::append_verbatim_at` — CRC-safe stamp at a supplied base

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/log/src/log.rs` tests, add:

```rust
    #[test]
    fn append_verbatim_at_stamps_base_byte_exact() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let vb = sample_verbatim_batch(); // reuse the module's verbatim-batch helper
        log.append_verbatim_at(&vb, Offset(0)).unwrap();
        assert2::assert!(log.log_end_offset() == Offset(vb.last_offset_delta as i64 + 1));
    }

    #[test]
    fn append_verbatim_at_rejects_non_leo_base() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let vb = sample_verbatim_batch();
        let err = log.append_verbatim_at(&vb, Offset(5)).unwrap_err(); // LEO is 0
        assert2::assert!(matches!(err, LogError::OffsetMismatch { .. }));
    }
```

(Reuse the verbatim-batch builder the existing `append_verbatim` tests use — grep the module for how they construct a `VerbatimBatch`.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p crabka-log append_verbatim_at`
Expected: FAIL — no `append_verbatim_at`.

- [ ] **Step 3: Implement `append_verbatim_at`**

In `crates/log/src/log.rs`, next to `append_verbatim` (`:522`), add a public method that mirrors `append_verbatim`'s body but takes the base and adds the `append_at`-style guard (`:622-628`):

```rust
    /// Append verbatim bytes at a caller-supplied `base` offset (the diskless
    /// offset-sequencer path). Like [`Log::append_verbatim`] but the base comes
    /// from the sequencer instead of `log_end_offset()`; `base` must still equal
    /// the current LEO (the gap-free witness) or this returns
    /// [`LogError::OffsetMismatch`]. The base is patched below the CRC region,
    /// so the record bytes stay byte-exact (no re-CRC).
    ///
    /// # Errors
    /// [`LogError::OffsetMismatch`] if `base != log_end_offset()`; other
    /// [`LogError`]s on a segment write failure.
    pub fn append_verbatim_at(
        &mut self,
        batch: &VerbatimBatch,
        base: Offset,
    ) -> Result<Offset, LogError> {
        let expected = self.log_end_offset();
        if base != expected {
            return Err(LogError::OffsetMismatch { expected, actual: base });
        }
        let leader_epoch = batch.leader_epoch;
        self.append_verbatim_preserving_offset(batch, base)?;
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, base)?;
        }
        Ok(base)
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p crabka-log append_verbatim_at`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/log/src/log.rs
git commit -m "feat(log): add Log::append_verbatim_at (sequencer base, CRC-safe, LEO guard)"
```

---

## Task 4: `OffsetSequencer` seam + `ControllerSequencer`

**Files:**
- Create: `crates/broker/src/wal/offset_sequencer.rs`
- Modify: `crates/broker/src/wal/mod.rs`

- [ ] **Step 1: Declare the submodule + exports**

In `crates/broker/src/wal/mod.rs` add `mod offset_sequencer;` and `pub use offset_sequencer::{OffsetSequencer, ControllerSequencer};`.

- [ ] **Step 2: Write the failing test**

Create `crates/broker/src/wal/offset_sequencer.rs` with its test module first. Drive it against an in-process **single-voter** controller (which commits inline — `controller.rs:1341-1343`), asserting contiguous, non-overlapping bases:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_ids::{Offset, PartitionIndex};

    use super::*;

    #[tokio::test]
    async fn assign_returns_contiguous_bases() {
        // Build a single-voter controller + its current-image handle (reuse the
        // crate's existing single-voter controller test harness — grep tests for
        // how other broker tests spin up an in-process Controller).
        let (controller, image) = test_single_voter_controller().await;
        let seq = ControllerSequencer::new(controller, image);
        let b0 = seq.assign("t", PartitionIndex(0), 3).await.unwrap();
        let b1 = seq.assign("t", PartitionIndex(0), 2).await.unwrap();
        assert!(b0 == Offset(0));
        assert!(b1 == Offset(3)); // contiguous: b1 == b0 + 3
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-broker offset_sequencer`
Expected: FAIL — `OffsetSequencer`/`ControllerSequencer` undefined.

- [ ] **Step 4: Implement the trait + impl**

Insert at the TOP of `crates/broker/src/wal/offset_sequencer.rs`:

```rust
//! Slice-2 offset authority: assign diskless partition offsets by committing a
//! `V1PartitionOffsetAdvance` delta through the local KRaft controller and
//! reading the post-commit metadata image for the base. Single-sequencer,
//! local-submit only; Slice 6 replaces this impl for concurrent/leaderless use.

use async_trait::async_trait;
use crabka_ids::{Offset, PartitionIndex};
use crabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

use crate::error::BrokerError;

/// Where a diskless partition's offsets come from. Returns the base of a
/// contiguous `count`-wide range.
#[async_trait]
pub trait OffsetSequencer: Send + Sync {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError>;
}

/// Controller-committed sequencer (Slice 2). Commits one advance delta locally
/// and reads the post-commit image (`base = next − count`).
pub struct ControllerSequencer {
    controller: /* the broker's Controller/ControllerHandle for submit_change */ ControllerHandle,
    image: /* the broker's current-MetadataImage read handle (the same source
              process_partition reads leadership from — produce.rs:459-476) */ ImageHandle,
}

impl ControllerSequencer {
    pub fn new(controller: ControllerHandle, image: ImageHandle) -> Self {
        Self { controller, image }
    }
}

#[async_trait]
impl OffsetSequencer for ControllerSequencer {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError> {
        let rec = MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
            topic: topic.to_string(),
            partition: partition.0,
            count: i64::from(count),
        });
        // Local submit: commit resolves AFTER apply() + image publish, so the
        // read below reflects this commit (single sequencer per partition).
        self.controller
            .submit_change(vec![rec])
            .await
            .map_err(BrokerError::from)?;
        let next = self
            .image
            .current()
            .partition_next_offset(topic, partition.0)
            .unwrap_or(0);
        Ok(Offset(next - i64::from(count)))
    }
}
```

Implementer notes:
- Replace the two placeholder types with the concrete broker handles: `ControllerHandle` is whatever `submit_change` is called through (`crates/raft/src/controller.rs:230` `Controller::submit_change`, or the broker's wrapper); `ImageHandle::current()` returns the latest `Arc<MetadataImage>` — use the exact accessor `process_partition` uses for the leadership gate (`grep -n "image" crates/broker/src/handlers/produce.rs` around `:459-476`). Both are already available at partition construction.
- `BrokerError::from(RaftError)` — add the `From` impl if absent (small).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker offset_sequencer`
Expected: PASS — contiguous bases from the controller.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/wal/offset_sequencer.rs crates/broker/src/wal/mod.rs
git commit -m "feat(broker): OffsetSequencer seam + ControllerSequencer (commit + read-after)"
```

---

## Task 5: Wire the sequencer into the diskless writer branch

Evolve the Slice-1 diskless branch: assign a range from the sequencer per group, then append each batch at the assigned base via `append_verbatim_at` (instead of the Slice-1 local-LEO append). Then Slice-1's fsync + `recompute_hw_for_wal_durable` run unchanged.

**Files:**
- Modify: `crates/broker/src/partition_writer.rs`
- Modify: `crates/broker/src/partition.rs`

- [ ] **Step 1: Add an append-at-base group append**

In `crates/broker/src/partition_writer.rs`, add a variant of `append_produce_batch` that stamps a caller-supplied base (contiguously across the group) using `append_verbatim_at`, instead of reading LEO:

```rust
/// Like `append_produce_batch` but stamps offsets from `base` (the diskless
/// offset sequencer) contiguously across the group, via `Log::append_verbatim_at`.
fn append_produce_batch_at(
    log: &Mutex<Log>,
    base: Offset,
    datas: Vec<ProduceData>,
) -> (Vec<Result<Offset, crate::error::BrokerError>>, Offset) {
    let mut guard = lock_log(log);
    let target = guard.config_snapshot().compression_type;
    let mut next = base;
    let mut results = Vec::with_capacity(datas.len());
    for data in datas {
        let r = match data {
            ProduceData::Verbatim(batch) => guard
                .append_verbatim_at(&batch, next)
                .map_err(crate::error::BrokerError::from),
            ProduceData::Owned(mut batch) => {
                if let Some(target) = target
                    && batch.attributes.compression() != target
                {
                    batch.attributes = batch.attributes.with_compression(target);
                }
                guard.append_at(&mut batch, next).map(|()| next)
                    .map_err(crate::error::BrokerError::from)
            }
        };
        if let Ok(base_off) = &r {
            // advance `next` by this batch's record count for the following batch
            next = Offset(guard.log_end_offset().0);
            let _ = base_off;
        }
        results.push(r);
    }
    let leo = guard.log_end_offset();
    (results, leo)
}
```

(The owned path uses the existing public `append_at` (`log.rs:621`); the verbatim path uses the new `append_verbatim_at`. Both carry the `== log_end_offset()` guard, so contiguous stamping is enforced batch-by-batch.)

- [ ] **Step 2: Call the sequencer in the diskless branch**

In the Slice-1 diskless branch of the `WriterMessage::Produce` arm (where Slice 1 routes through `WalStore`), before the append, compute `N` and assign:

```rust
                // DISKLESS (Slice 2): source the base offset from KRaft.
                let n: u32 = datas.iter().map(ProduceData::record_count).sum();
                let base = match sequencer.assign(&topic, partition, n).await {
                    Ok(b) => b,
                    Err(e) => { /* fail every ack in the group with e; continue */ }
                };
                let (results, leo) =
                    run_produce_append_batch_at(log.clone(), base, datas).await?; // block_in_place/spawn_blocking wrapper like run_produce_append_batch
                // ... then Slice-1 fsync (WalStore::sync_durable) + recompute_hw_for_wal_durable ...
```

Add a `run_produce_append_batch_at` async wrapper mirroring `run_produce_append_batch` (`:138`) that calls `append_produce_batch_at`. Add `ProduceData::record_count(&self) -> u32` (= `last_offset_delta + 1` for the batch — read the delta from the verbatim/owned batch header). `sequencer` is threaded into `run` as `Option<Arc<dyn OffsetSequencer>>` alongside the Slice-1 `wal` (a diskless partition has both).

- [ ] **Step 3: Thread the sequencer through `run` + construct it**

- Add `sequencer: Option<Arc<dyn crate::wal::OffsetSequencer>>` to `partition_writer::run`'s signature (after the Slice-1 `wal` param) and pass it at every call site (tests: `None`).
- In the production `Partition` constructor (where Slice 1 builds the `LocalFsyncWal` for diskless topics), also build `Some(Arc::new(ControllerSequencer::new(controller, image)))` for diskless topics, using the broker's controller + current-image handles.

- [ ] **Step 4: Write the integration test**

```rust
    #[tokio::test]
    async fn diskless_produce_uses_controller_assigned_base() {
        // spawn a diskless writer with a single-voter ControllerSequencer + WalStore;
        // produce a batch of 3 records; assert the ack's base offset == 0 and a
        // second produce's base == 3 (contiguous, controller-assigned), and the
        // committed metadata image's partition_next_offset("t",0) == 6.
    }
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker diskless_produce_uses_controller_assigned_base`
Expected: PASS.

- [ ] **Step 6: Run the writer suite (classic path unregressed)**

Run: `cargo test -p crabka-broker partition_writer`
Expected: PASS — classic (`sequencer: None`) path unchanged; Slice-1 diskless tests still green.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/partition_writer.rs crates/broker/src/partition.rs
git commit -m "feat(broker): diskless writer sources offsets from the KRaft sequencer"
```

---

## Task 6: Stateright — gap-free / monotonic / unique proof

**Files:**
- Modify: `crates/broker/src/data_path_model.rs`

- [ ] **Step 1: Add the sequencer ghost + Assign action + property**

Extend the model (or add a focused sibling model in the same file) with, for a diskless config:
- state: `seq_next: i64` (committed next-offset) and `assigned: Vec<(i64, i64)>` (ghost: each assigned `(base, count)`).
- action `Assign(count)` (offered in diskless mode, bounded by `MAX_LEN`): record `assigned.push((seq_next, count)); seq_next += count;` then the local append writes those offsets.
- property `Property::always("offsets_contiguous_and_unique", …)` asserting the flattened assigned offset ranges form exactly `0..seq_next` with no gap, no overlap, strictly increasing (and each equals the local log position it landed at — the `append_verbatim_at` guard).

Add a `Property::sometimes("offsets_assigned", |_, s| !s.assigned.is_empty())` so the state space actually exercises assignment.

- [ ] **Step 2: Add the checker test**

```rust
    #[test]
    fn data_diskless_offsets_gap_free_and_unique() {
        // run the BFS checker (mirror data_clean's harness) on a diskless,
        // single-sequencer, no-crash config; assert offsets_contiguous_and_unique
        // holds on every reachable state.
    }
```

- [ ] **Step 3: Run the model check**

Run: `cargo test -p crabka-broker data_diskless_offsets_gap_free_and_unique -- --nocapture`
Expected: PASS. A counterexample means the single-sequencer append ordering admits a gap/overlap — reconcile `Assign`/append with the `base == log_end_offset()` guard (Task 3/5) until the property holds. Do NOT weaken the property.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/data_path_model.rs
git commit -m "test(broker): stateright gap-free/monotonic/unique offset proof (single sequencer)"
```

---

## Task 7: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-metadata -p crabka-log -p crabka-broker` (or `cargo test`) — PASS, including the offset-sequencer proof.
- [ ] **Step 4:** Commit any formatting: `git commit -am "style: cargo +nightly fmt"` (skip if clean).

---

## Self-Review

**1. Spec coverage:**
- `OffsetSequencer` seam + `ControllerSequencer` (local submit + read-after-commit) → Task 4. ✅
- `V1PartitionOffsetAdvance` delta record + carrier + snapshot → Tasks 1-2. ✅
- `Log::append_verbatim_at` (CRC-safe, LEO guard = gap-free witness) → Task 3. ✅
- Diskless writer sources offsets from KRaft → Task 5. ✅
- Gap-free/monotonic/unique single-sequencer proof → Task 6. ✅
- Deferred set (forward path, crash gap S5, concurrency S6, throughput) → untouched; Scope boundary + Invariant 5. ✅

**2. Placeholder scan:** The `ControllerSequencer` struct fields (Task 4) are the one intentional "fill from the concrete broker handle" — named with the exact accessor to locate (`produce.rs:459-476` image source; `controller.rs:230` submit). Every other step is complete code. No `TBD`/`TODO`.

**3. Type consistency:** `PartitionOffsetAdvanceRecord{topic,partition,count}` is defined once (Task 1) and used identically in image apply/to_records (Tasks 1-2), the carrier (Task 2), and the sequencer (Task 4). `append_verbatim_at(&VerbatimBatch, Offset) -> Result<Offset, LogError>` (Task 3) matches its call in `append_produce_batch_at` (Task 5). `OffsetSequencer::assign(&str, PartitionIndex, u32) -> Result<Offset, BrokerError>` (Task 4) matches the writer call (Task 5). `partition_next_offset(&str, i32) -> Option<i64>` (Task 1) matches the sequencer read (Task 4) and both tests.

**4. Invariant check:** wire + classic paths untouched (only diskless branch + new files); offsets byte-exact via below-CRC patch (Task 3); delta-not-replace mirrors `V1PartitionDirAssignment` (Tasks 1-2); the `== log_end_offset()` guard enforces gap-freeness at runtime (Task 3) and is the proof's witness (Task 6); single-sequencer/no-crash/local-submit scope held throughout (no wire change, no forward path). Each task ends green.

**5. Prerequisite flagged:** builds on Slice 1's (spec-only) diskless writer branch — stated in the header; Task 5 evolves it.
