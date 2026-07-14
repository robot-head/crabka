# Chapter Gres G-3: Checkpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Checkpoints bound spin-up and WAL growth: the writer snapshots the store between commit-groups, a checkpointer streams it to the bucket (manifest last), the WAL truncates behind it, and recovery restores checkpoint + tail instead of full history.

**Architecture:** A `SnapshotKv` streaming seam in `crabka-pgkv` (fjall MVCC snapshot / mem clone); part+manifest objects under immutable `gres/<tenant>/ckpt/<offset>-<epoch>/` prefixes via the existing `ObjectOps`; truncation through a new public `AdminClient::delete_records`; restore via fjall ingestion with a `write_batch` fallback; a Stateright model of the fence/checkpoint/truncate/recover protocol as the gate.

**Tech Stack:** fjall 3.x (`Database::snapshot`, `Keyspace::start_ingestion`), `crabka-object-store` (`ObjectStoreConfig`/`ObjectOps`, `InMemory` for tests), `sha2` checksums, `serde_json` manifests, `stateright`.

## Global Constraints

- **Prerequisites:** G-1 and G-2 plans landed. Verify every signature against the landed tree; the fjall snapshot/ingestion APIs (`Database::snapshot() -> Snapshot`, `Snapshot::iter(&Keyspace)`, `Keyspace::start_ingestion() -> Ingestion` with `write`/`write_tombstone`/`finish`, strictly-ascending keys) were verified against fjall 3.1.6 and are present throughout fjall 3.1.x — the workspace pin is whatever G-1 imported (the donor's `fjall = "3.1.5"`); these APIs exist there too, so no version bump is required (confirm at execution time).
- **Spec:** [2026-07-09-crabka-gres-g3-checkpoints-design.md](../specs/2026-07-09-crabka-gres-g3-checkpoints-design.md).
- **Broker facts (verified):** `DeleteRecords` trims leader-locally, advances log start (`low_watermark` in the response), physically deletes sealed segments; `offset == -1` means HW; `target > LEO` → `OFFSET_OUT_OF_RANGE`; fetch below log start surfaces as `ClientError::Server { error_code: 1 }` through `fetch_partition_with_isolation`.
- **Ordering invariants (the spec's; the model pins them):** snapshot between commit-groups stamped `(covered_offset, journal_seq, epoch)`; parts before manifest; manifest before DeleteRecords; DeleteRecords before prune; recovery refuses on log-start-beyond-newest-manifest, checksum mismatch, or `journal_seq` gap.
- Lints/format/commit/test conventions as in the G-2 plan (pedantic `-D warnings`, `cargo +nightly fmt`, `assert2`, condition-driven waits, conventional commits with the Claude trailer).

---

## Batch 1 — three independent foundations (run Tasks 1–3 in parallel; disjoint crates)

### Task 1: `SnapshotKv` seam + restore helpers in `crabka-pgkv`

**Files:** Modify `crates/pgkv/src/store.rs` (or new `src/snapshot.rs` + module wiring), `src/fjall_store.rs`, `crates/pgkv/README.md`.

**Interfaces:**
- Produces:
```rust
/// A consistent point-in-time, key-ordered stream of the whole store.
pub trait KvSnapshot: Send {
    /// Next pair in ascending key order; `None` at the end.
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvError>;
}
/// Stores that can produce online snapshots (both backends do).
pub trait SnapshotKv: Kv {
    fn snapshot(&self) -> Result<Box<dyn KvSnapshot>, KvError>;
}
/// Bulk restore from a strictly-ascending sorted stream.
/// FjallKv uses fjall ingestion; MemKv folds write_batch chunks.
pub trait RestoreKv: Kv {
    fn restore_sorted(&self, pairs: &mut dyn KvSnapshot) -> Result<u64, KvError>; // returns pair count
}
```
- `FjallKv`: `snapshot()` via `self.db.snapshot()` + `snap.iter(&self.keyspace)` (hold both in the boxed iterator; map fjall errors to `KvError::Io`); `restore_sorted` via `keyspace.start_ingestion()` → `write` per pair → `finish()` → `db.persist(PersistMode::SyncAll)`. `MemKv`: clone-map iterator; chunked `write_batch` (4096-op chunks) restore.

Steps: failing tests first (snapshot sees pre-snapshot state while concurrent writes proceed — spawn writes during iteration and assert the snapshot excludes them, on both backends; restore round-trip equals source store; restore rejects unsorted input loudly on the mem path and documents fjall's panic-on-unsorted as the contract), implement, `cargo nextest run -p crabka-pgkv`, clippy/fmt, README section, commit `feat(pgkv): online snapshot and sorted-restore seams`.

### Task 2: `AdminClient::delete_records`

**Files:** Modify `crates/client-admin/src/topics.rs` (+ `lib.rs` re-exports), a new integration test in `crates/client-admin/tests/` following that crate's in-process-broker test idiom.

**Interfaces:**
- Produces (shape mirrors `create_topics`):
```rust
pub struct DeleteRecordsOp { pub topic: String, pub partition: i32, pub offset: i64 } // -1 = high watermark
pub struct DeleteRecordsOutcome { pub topic: String, pub partition: i32, pub error_code: i16, pub low_watermark: i64 }
pub async fn delete_records(&mut self, ops: &[DeleteRecordsOp], timeout_ms: i32) -> Result<Vec<DeleteRecordsOutcome>, AdminError>
```
Builds `crabka_protocol::owned::delete_records_request::*` (grouping ops by topic), routes to the partition leader the way the existing topic methods do, flattens per-partition results.

Steps: failing integration test (produce 100 → `delete_records(offset 50)` → outcome `error_code == 0 && low_watermark >= 50`; then `fetch_partition` at offset 0 errors with `ClientError::Server { error_code: 1 }` — mirror the assertions of `crates/broker/tests/admin_handlers.rs::delete_records_trims_log_start`), implement, unit test for request-building (whole-struct comparison per house test style), docs (`## Errors` rustdoc), clippy/fmt, commit `feat(client-admin): DeleteRecords admin API`.

### Task 3: Part + manifest codec in `crabka-gres-substrate`

**Files:** Create `crates/gres-substrate/src/checkpoint/mod.rs`, `checkpoint/codec.rs`, `checkpoint/manifest.rs`; wire modules in `lib.rs`; extend `src/error.rs` (`Checkpoint(String)`, `TornTruncation { log_start: i64, newest_manifest: i64 }`, `ChecksumMismatch { part: String }` variants).

**Interfaces:**
- Part framing: a part object is a sequence of `[u32 klen][key][u32 vlen][value]` pairs (big-endian, bounds-checked decode — reuse the GRW1 `Reader` by making it `pub(crate)`), targeted `part_max_bytes` (default 64 MiB) per part.
- Manifest (`serde_json`, versioned):
```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub format_version: u32,          // 1
    pub tenant: String,
    pub covered_offset: i64,          // WAL offset this checkpoint covers through (exclusive replay start)
    pub journal_seq: u64,             // next engine seq at the snapshot instant
    pub producer_epoch: i16,
    pub wal_generation: u64,          // 0 in G-3; G-5 topic parking bumps it (fresh topic ⇒ tail replays from 0)
    pub parts: Vec<PartEntry>,        // in key order
    pub total_pairs: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartEntry { pub name: String, pub pairs: u64, pub sha256_hex: String }
```
- Key layout helpers: `ckpt_prefix(tenant) -> String` (`gres/<tenant>/ckpt/`), `ckpt_dir(tenant, wal_generation, covered_offset, epoch)` (`…/ckpt/{generation:010}-{offset:020}-{epoch:05}/` — generation leads so lexical order is `(generation, offset)`; panel amendment C3), `manifest_key(dir)`, `part_key(dir, index)` (`part-{index:05}`).

Steps: failing tests (part codec proptest round-trip incl. multi-part splitting at the size threshold; manifest serde round-trip; unknown `format_version` refuses with a named error), implement, nextest/clippy/fmt, commit `feat(gres): checkpoint part and manifest codec`.

---

## Batch 2 — the checkpointer and restore (serial; needs all of Batch 1)

### Task 4: Writer snapshot control + checkpointer task + truncate + prune

**Files:** Modify `crates/gres-substrate/src/writer.rs` (control message) and `crates/gres-substrate/Cargo.toml` (`crabka-object-store`, `crabka-client-admin`, `sha2` deps), create `crates/gres-substrate/src/checkpoint/checkpointer.rs`; modify `crates/gres/src/main.rs` + `crates/gres/Cargo.toml` (bucket + threshold flags).

**Interfaces:**
- Writer: the request channel becomes `enum WalMsg { Batch(WalRequest), Checkpoint(oneshot::Sender<SnapshotHandle>) }`; between groups the writer answers `Checkpoint` with `SnapshotHandle { snapshot: Box<dyn KvSnapshot>, covered_offset: i64, journal_seq: u64, producer_epoch: i16, horizon_xid: u64 }` — `covered_offset` is the last WAL offset whose group has been applied (the writer records the max acked `RecordMetadata.offset` per group; add that bookkeeping), `horizon_xid` is `min(oldest xid visible to any active snapshot, clog_scan_lo)` — the ProcArray term via a narrow public accessor (no active sessions ⇒ next xid), floored by the recovery watermark, which by the SP20 invariant never passes a non-terminal clog entry, so in-doubt `Prepared` markers are structurally protected from the horizon (panel amendment C4; Task 4b consumes it) — taken synchronously in the loop so no group is in flight at the instant.
- Checkpoint-lag metric *(scaling-review amendment)*: the shared `CheckpointStats` also exposes `lag_bytes`/`lag_frames` since the last durable manifest, logged with a loud warning above a configured threshold — sustained lag means WAL ingress is outrunning upload bandwidth, a detected-not-handled condition per the spec (the operator's lever is graduation).
- Checkpointer (`spawn_checkpointer(handle, store, ops: Arc<dyn ObjectOps>, admin, cfg) -> JoinHandle`): trigger on `frames_since_last >= cfg.frames_threshold || bytes_since_last >= cfg.bytes_threshold` (counters fed by the writer through a shared `Arc<CheckpointStats>`); on trigger — request snapshot → stream parts (`ObjectOps::put` per part; sha256 while streaming) → put `MANIFEST` last → `admin.delete_records(&[DeleteRecordsOp { topic, partition: 0, offset: covered_offset + 1 }], …)` → list `ckpt_prefix` and prune to the newest 2 dirs in `(generation, offset)` lexical order, never deleting the current generation's newest (NotFound-tolerant, the blockstore prune idiom; panel amendment C3 — offset-only pruning deletes the first post-parking checkpoint) → reset counters. Every step logs; failures abort the attempt (retry at next trigger) without touching earlier checkpoints.
- Gres bin: `--bucket-*` flags (or a TOML section mirroring the broker's `[remote_storage]` shape) → `ObjectStoreConfig`; `--checkpoint-frames`/`--checkpoint-bytes` thresholds; checkpointer spawned only in substrate mode with a bucket configured (substrate-without-bucket remains valid = G-2 behavior, full-replay).

Steps: failing unit test for the writer control message (checkpoint between groups: enqueue batches, request snapshot, assert stamped offsets equal the applied prefix and the snapshot excludes post-request batches); implement writer change; failing checkpointer integration test against `ObjectStoreConfig::InMemory` + in-process broker (drive frames past threshold → assert parts+manifest exist, DeleteRecords advanced log start (`fetch` at 0 now errors code 1), old prefixes pruned to 2); implement; nextest/clippy/fmt; commit `feat(gres): checkpointer with manifest-last upload and WAL truncation`.

### Task 4b: The garbage horizon — vacuum-into-checkpoint

**Files:** Modify `crates/pgmvcc/src/{xid,visibility,version}.rs` (frozen sentinel), `crates/gres-substrate/src/checkpoint/checkpointer.rs` (rewrite rules in the part-streaming path), new `crates/gres-substrate/src/checkpoint/horizon.rs`; tests in both crates.

**Interfaces:**
- `crabka-pgmvcc` gains `pub const FROZEN_XID: Xid` (mirror PostgreSQL's FrozenTransactionId concept; pick a reserved value below the engine's first allocatable xid — inspect the vendored `Xid` domain and the donor's first-xid convention before choosing) with `satisfies_mvcc` treating `xmin == FROZEN_XID` as committed-for-everyone without a clog lookup. Unit tests pin the visibility table for frozen tuples.
- `horizon.rs`: `pub fn rewrite_for_checkpoint(key: &[u8], value: &[u8], horizon: u64, clog: &impl ClogLookup) -> RewriteDecision` where `RewriteDecision::{Keep, Drop, Replace(Vec<u8>)}` implements the three spec rules — drop row versions with committed `xmax < horizon`; freeze committed `xmin < horizon` (rewrite the tuple header's xmin to `FROZEN_XID`); pass non-row keys through except clog entries with `xid < horizon`, which drop. (Key classification via the existing `crabka-pgkv` helpers + the mvcc version-key/tuple-header codecs — reuse the donor's `version.rs`/`rowenc` accessors; never hand-parse layouts.)
- The checkpointer pipes every snapshot pair through `rewrite_for_checkpoint` before part framing; `total_pairs`/checksums reflect the rewritten stream.

Steps: TDD the rewrite rules (dead-below-horizon dropped; dead-at-or-above kept; freeze rewrites byte-exactly; aborted-xmin versions below horizon dropped — they are invisible to everyone; clog pruning) → the **visibility-equivalence property test** (random committed/aborted/in-flight histories: for every key and every snapshot at-or-above the horizon, pre-rewrite and post-rewrite stores answer `satisfies_mvcc` identically) → wire into the checkpointer → the Task 5/6 suites re-run green including a checkpoint-restore cycle under conformance parity. Commit `feat(gres): vacuum-into-checkpoint garbage horizon`.

### Task 5: Restore-aware recovery

**Files:** Modify `crates/gres-substrate/src/recover.rs`, create `src/checkpoint/restore.rs`.

**Interfaces:**
- `restore_latest(ops: &dyn ObjectOps, tenant, store: &dyn RestoreKv, current_generation: u64) -> Result<Option<RestoredFrom>, SubstrateError>` where `RestoredFrom { wal_generation: u64, covered_offset: i64, journal_seq: u64 }`: list manifests under `ckpt_prefix`, pick highest `(wal_generation, covered_offset)` (lexical order of the zero-padded dirs gives it; panel amendment C3), download+verify parts (count + sha256 against the manifest; any mismatch → `ChecksumMismatch`, try the next-newest manifest once, else fail), feed a merged sorted stream into `store.restore_sorted`. Replay start and refusal are generation-aware: `restored.wal_generation == current_generation` ⇒ tail from `covered_offset + 1` with the torn-truncation check; older generation ⇒ fresh topic, tail from 0, torn-truncation check skipped (the whole tail is the fresh log).
- `recover(...)` gains the pre-replay restore: fence → barrier (unchanged, per the amended G-2) → `restore_latest` → replay from `covered_offset + 1` with `expected = journal_seq` (instead of 0/0) → on `ClientError::Server { error_code: 1 }` (fetch below log start): if no checkpoint restored or `log_start > covered_offset + 1`, fail with `TornTruncation` — never skip ahead silently.

Steps: failing integration test (workload → checkpoint → more workload → kill → recover: assert replay's first fetch offset is `covered_offset + 1` via harness instrumentation, state exact); torn-truncation test (delete the newest manifest object from the InMemory bucket after truncation → recovery fails with `TornTruncation`, never serves); restore-equivalence proptest (random op sequences → checkpoint+restore+tail-replay equals reference fold — both backends); implement; nextest/clippy/fmt; commit `feat(gres): checkpoint-restore recovery with torn-truncation refusal`.

---

## Batch 3 — proof (run Tasks 6 and 7 in parallel; disjoint files)

### Task 6: Crash-anywhere integration suite

**Files:** Create `crates/gres-substrate/tests/checkpoint_crashes.rs` (uses the Task-5 harness helpers; extract shared helpers into `tests/harness/mod.rs` as needed).

Inject a step-boundary failpoint into the checkpointer via a test-only hook (a `#[cfg(test)]`-free seam: the checkpointer takes an optional `Arc<dyn Fn(Step) -> bool>` "abort_after" callback, `pub` but doc-hidden, defaulting to never — mirrors how the donor instrumented deterministic tests). For each `Step` in `{Snapshot, PartsUploaded, ManifestWritten, Truncated, Pruned}`: run workload → force checkpoint aborting after the step → kill → recover → assert exact acked state and that recovery chose the correct source (pre-manifest aborts recover from the previous checkpoint + longer tail; post-manifest aborts recover from the new one). Also: zombie-checkpointer race (old compute finishes an upload after being fenced → successor recovery still correct; its manifest is at a ≤ offset and harmless). Commit `test(gres): crash-anywhere checkpoint suite`.

### Task 7: The Stateright model (the gate)

**Files:** Create `crates/gres-substrate/tests/checkpoint_model.rs`; add `stateright` to gres-substrate dev-deps (workspace dep exists).

Model (pure, mirroring `crates/cluster`'s donor precedent style — no I/O, canonical sorted state for fingerprinting): state = journal (Vec of abstract frames) **per wal_generation**, `log_start`, checkpoints as `(wal_generation, covered, epoch, manifest_present)`, compute processes with `{epoch, applied_prefix, phase}`; actions = append-group (active writer only), each checkpoint step as its own action, crash (any process, any time), start-successor (fence = epoch bump), **park-and-recreate (wal_generation bump, offsets reset — the panel-C3 dimension)**, recover-steps (pick-manifest by `(generation, offset)`, restore, replay, serve), zombie-checkpoint-step, prune-step. Properties: (1) a serving compute's state equals the reference fold of the acked journal prefix; (2) no reachable state has every recovery path refused unless the bucket lost a manifest (the injected-loss action is what makes `TornTruncation` reachable — assert refusal, not corruption); (3) `log_start ≤ newest manifest-present checkpoint's covered_offset + 1` in every reachable state without injected loss. Bound: ≤ 3 computes, ≤ 2 checkpoints, small journal; BFS within the donor's model budgets. Steps: write the model test failing on a deliberately-wrong invariant first (sanity that the checker checks), then correct invariants green. Commit `test(gres): Stateright model of the checkpoint/fence/recover protocol`.

---

## Batch 4 — wrap-up (serial)

### Task 8: Docs + CI touch-ups

**Files:** Modify `crates/gres-substrate/README.md`, `crates/gres/README.md` (bucket/threshold flags), `.github/workflows/ci.yml` only if the new tests exceed the existing `gres-integration` job's timeout (raise `timeout-minutes`; the package list already includes `crabka-gres-substrate`; the `gres` changes-filter already covers the crate). Confirm `cargo nextest run --workspace --profile ci --lib --bins` and the two gres CI jobs green locally where runnable. Commit `docs(gres): checkpoint configuration and recovery documentation`.

## Completion checklist (maps to the G-3 gate)

- Spin-up bounded: recovery provably replays from `covered_offset + 1`, not 0 (Task 5's instrumented assert), with the crash-anywhere suite green (Task 6).
- The Stateright model checks the protocol exhaustively within bounds (Task 7).
- WAL bounded: truncation + prune verified (Task 4); torn truncation refuses (Task 5).
- `AdminClient::delete_records` shipped as a first-class client feature (Task 2).
