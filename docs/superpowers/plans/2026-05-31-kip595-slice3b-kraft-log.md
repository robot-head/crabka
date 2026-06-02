# KIP-595 Slice 3b — KRaft Replicated Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `KraftLog`, a thin facade over `crabka_log::Log` that adds high-watermark tracking, committed-read filtering for `Fetch`, divergence lookup, and the 3a `LogView` impl — validated standalone and by re-backing the 3a multi-node consensus simulation with real on-disk `KraftLog` instances.

**Architecture:** A new `crates/raft/src/kraft/log.rs` wrapping `crabka_log::Log` + an in-memory `hwm`. Additive: openraft's `log_store.rs` is untouched (replaced in 3c). The 3a consensus core (`crates/raft/src/kraft/core.rs`) is unchanged; 3b only supplies the real `LogView` and the byte-level log ops the core's `Action`s map onto.

**Tech Stack:** Rust, `crabka_log::{Log, LogConfig, LogError}`, `crabka_protocol::records::RecordBatch`, the 3a `kraft` module, `tempfile` (dev-dep) + `assert2`.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice3b-kraft-log-design.md](../specs/2026-05-31-kip595-slice3b-kraft-log-design.md)

---

## Background the implementer needs

- This is additive logic under `crates/raft/src/kraft/`. Do NOT touch openraft (`log_store.rs`, `controller.rs`, `network.rs`, `server.rs`), the wire, or `kraft_spike.rs`.
- **crabka-log API** (`crates/log/src/log.rs`), all confirmed:
  - `crabka_log::Log::open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Log, LogError>`
  - `log.append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError>` (assigns + returns the base offset; records `batch.partition_leader_epoch` in the epoch checkpoint)
  - `log.append_at(&mut self, batch: &mut RecordBatch, offset: i64) -> Result<(), LogError>` (validates `offset == log_end_offset`)
  - `log.read_raw(&self, fetch_offset: i64, limit_offset: i64, max_bytes: usize) -> Result<RawRead, LogError>` — verbatim bytes for batches with `base_offset < limit_offset`. `RawRead { start_offset: i64, bytes: bytes::Bytes, total: usize }`.
  - `log.read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutput, LogError>` — decoded `ReadOutput { start_offset, batches: Vec<RecordBatch> }` (use in tests to inspect batches).
  - `log.truncate_to(&mut self, offset: i64) -> Result<(), LogError>`
  - `log.log_start_offset() -> i64`, `log.log_end_offset() -> i64`
  - `log.epoch_checkpoint() -> &LeaderEpochCheckpoint`; `.end_offset_for_epoch(epoch: i32, log_end_offset: i64) -> i64` (returns `-1` for unknown epoch); `.latest_epoch() -> Option<i32>`
- **`RaftError`** (`crates/raft/src/error.rs`) already has `From<crabka_log::LogError>` (used throughout `log_store.rs`). Return `Result<_, RaftError>` from `KraftLog`.
- **3a `LogView`** (`crates/raft/src/kraft/types.rs`): `end_offset()->i64`, `last_epoch()->LeaderEpoch`, `end_offset_for_epoch(LeaderEpoch)->Option<i64>`. `LeaderEpoch = u32`. Convert at the i32/u32 boundary.
- **`RecordBatch`** (`crabka_protocol::records::RecordBatch`): has `base_offset`, `partition_leader_epoch: i32`, `attributes`, `records`, etc. Build test batches with `Attributes::default()` and one or more `Record`s.
- The 3a multi-node simulation lives in `crates/raft/tests/kraft_sim.rs` (a `Sim` harness + 3 tests). Task 5 generalizes it to run over a real `KraftLog`.

## File Structure

| Path | Responsibility |
|------|----------------|
| `crates/raft/src/kraft/log.rs` | `KraftLog` facade + `LogView` impl + inline unit tests. |
| `crates/raft/src/kraft/mod.rs` | add `pub mod log;` + re-export `KraftLog`. |
| `crates/raft/tests/kraft_log_sim.rs` | Core-over-real-`KraftLog` integration tests (or extend `kraft_sim.rs`). |

---

## Task 1: `KraftLog::open` + accessors

**Files:** create `crates/raft/src/kraft/log.rs`; modify `crates/raft/src/kraft/mod.rs`.

- [ ] **Step 1: Write the failing test**

Append to `crates/raft/src/kraft/log.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn open_tmp() -> (KraftLog, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open");
        (log, dir)
    }

    #[test]
    fn opens_empty_at_offset_zero() {
        let (log, _dir) = open_tmp();
        assert!(log.log_start_offset() == 0);
        assert!(log.log_end_offset() == 0);
        assert!(log.hwm() == 0);
    }
}
```

- [ ] **Step 2: Run it** → FAIL (`KraftLog` undefined). `cargo test -p crabka-raft kraft::log`.

- [ ] **Step 3: Implement**

`crates/raft/src/kraft/log.rs`:

```rust
//! `KraftLog`: the real replicated metadata log behind the 3a `LogView` seam.
//! A thin facade over `crabka_log::Log` that adds high-watermark tracking,
//! committed-read filtering for KIP-595 `Fetch`, and divergence lookup. Wired
//! into the controller (replacing openraft's log_store) in slice 3c.

use std::path::Path;

use crabka_log::{Log, LogConfig, RawRead};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::kraft::types::{LeaderEpoch, LogView};

pub struct KraftLog {
    log: Log,
    /// Highest committed offset (consensus state; crabka-log does not track it).
    hwm: i64,
}

impl KraftLog {
    /// Open or create the metadata log under `dir/@metadata-0`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, RaftError> {
        let log_dir = dir.as_ref().join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(crabka_log::LogError::Io)?;
        let log = Log::open(&log_dir, LogConfig::default())?;
        let hwm = log.log_start_offset();
        Ok(Self { log, hwm })
    }

    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        self.log.log_start_offset()
    }
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        self.log.log_end_offset()
    }
    #[must_use]
    pub fn hwm(&self) -> i64 {
        self.hwm
    }
}
```

`crates/raft/src/kraft/mod.rs`: add `pub mod log;` and `pub use log::KraftLog;`.

- [ ] **Step 4: Run** → PASS. Also `cargo build -p crabka-raft`. (Confirm `tempfile` is a dev-dependency of `crates/raft`; `log_store`/snapshot tests use it. If not, add `tempfile` under `[dev-dependencies]`.)

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/log.rs crates/raft/src/kraft/mod.rs
git commit -m "feat(raft): KraftLog facade scaffold over crabka-log"
```

---

## Task 2: append / append_at + read-back

**Files:** `crates/raft/src/kraft/log.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn append_assigns_sequential_offsets_and_reads_back() {
    let (mut log, _dir) = open_tmp();
    let off0 = log.append(&mut batch(0, /*epoch*/ 1, b"a")).unwrap();
    let off1 = log.append(&mut batch(0, 1, b"b")).unwrap();
    assert!(off0 == 0 && off1 == 1);
    assert!(log.log_end_offset() == 2);
    // read back decoded
    let out = log.read_decoded(0, 1 << 20).unwrap();
    assert!(out.len() == 2);
    assert!(out[0].partition_leader_epoch == 1);
}

#[test]
fn append_at_preserves_leader_offset() {
    let (mut log, _dir) = open_tmp();
    // follower applies a leader-assigned batch at offset 0
    log.append_at(&mut batch(0, 2, b"x"), 0).unwrap();
    assert!(log.log_end_offset() == 1);
    assert!(log.read_decoded(0, 1 << 20).unwrap()[0].partition_leader_epoch == 2);
}

// test helper
fn batch(base: i64, epoch: i32, value: &[u8]) -> RecordBatch {
    use crabka_protocol::records::{Attributes, Record};
    RecordBatch {
        base_offset: base,
        partition_leader_epoch: epoch,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: vec![Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value)),
            headers: Vec::new(),
        }],
    }
}
```

- [ ] **Step 2: Run** → FAIL (`append`/`append_at`/`read_decoded` undefined).

- [ ] **Step 3: Implement** (add to `impl KraftLog`)

```rust
/// Leader path: append a batch; crabka-log assigns the offset and records the
/// batch's `partition_leader_epoch`. Returns the assigned base offset.
pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, RaftError> {
    Ok(self.log.append(batch)?)
}

/// Follower path: append a batch at the leader-assigned `offset`.
pub fn append_at(&mut self, batch: &mut RecordBatch, offset: i64) -> Result<(), RaftError> {
    self.log.append_at(batch, offset)?;
    Ok(())
}

/// Decoded read (used by tests + replication apply). Reads from `offset`.
pub fn read_decoded(&self, offset: i64, max_bytes: usize) -> Result<Vec<RecordBatch>, RaftError> {
    Ok(self.log.read(offset, max_bytes)?.batches)
}
```

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/log.rs
git commit -m "feat(raft): KraftLog append/append_at + decoded read-back"
```

---

## Task 3: `LogView` impl

**Files:** `crates/raft/src/kraft/log.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn logview_reports_end_offset_and_last_epoch() {
    let (mut log, _dir) = open_tmp();
    log.append(&mut batch(0, 1, b"a")).unwrap();
    log.append(&mut batch(0, 3, b"b")).unwrap(); // epoch jumps to 3
    assert!(LogView::end_offset(&log) == 2);
    assert!(LogView::last_epoch(&log) == 3);
}

#[test]
fn logview_end_offset_for_epoch_maps_unknown_to_none() {
    let (mut log, _dir) = open_tmp();
    log.append(&mut batch(0, 1, b"a")).unwrap(); // epoch 1 @ [0,1)
    log.append(&mut batch(0, 2, b"b")).unwrap(); // epoch 2 @ [1,2)
    // epoch 1 ends where epoch 2 starts (offset 1); epoch 2 is current → end 2.
    assert!(LogView::end_offset_for_epoch(&log, 1) == Some(1));
    assert!(LogView::end_offset_for_epoch(&log, 2) == Some(2));
    // unknown future epoch → None
    assert!(LogView::end_offset_for_epoch(&log, 9).is_none());
}

#[test]
fn empty_log_last_epoch_is_zero() {
    let (log, _dir) = open_tmp();
    assert!(LogView::last_epoch(&log) == 0);
}
```

- [ ] **Step 2: Run** → FAIL (`LogView` not impl'd for `KraftLog`).

- [ ] **Step 3: Implement**

```rust
impl LogView for KraftLog {
    fn end_offset(&self) -> i64 {
        self.log.log_end_offset()
    }
    fn last_epoch(&self) -> LeaderEpoch {
        // crabka-log epochs are i32 and non-negative; 0 for an empty log.
        u32::try_from(self.log.epoch_checkpoint().latest_epoch().unwrap_or(0)).unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
        let log_end = self.log.log_end_offset();
        let epoch_i32 = i32::try_from(epoch).ok()?;
        match self.log.epoch_checkpoint().end_offset_for_epoch(epoch_i32, log_end) {
            -1 => None,
            off => Some(off),
        }
    }
}
```

(Confirm `end_offset_for_epoch` of an unknown epoch returns `-1` from crabka-log; the test `9 → None` pins this. If crabka-log instead returns `log_end` for a too-large epoch, adjust the test to match crabka-log's documented contract and note it.)

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/log.rs
git commit -m "feat(raft): LogView impl for KraftLog"
```

---

## Task 4: HWM tracking, committed read, truncation

**Files:** `crates/raft/src/kraft/log.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn read_committed_never_returns_bytes_past_hwm() {
    let (mut log, _dir) = open_tmp();
    for _ in 0..5 { log.append(&mut batch(0, 1, b"x")).unwrap(); } // offsets 0..5
    log.advance_hwm(3);
    let r = log.read_committed(0, 1 << 20).unwrap();
    // bytes contain only batches with base_offset < 3 (offsets 0,1,2)
    let decoded = log.read_decoded(0, 1 << 20).unwrap();
    let committed: Vec<_> = decoded.into_iter().filter(|b| b.base_offset < 3).collect();
    assert!(committed.len() == 3);
    assert!(r.start_offset == 0);
    // total committed bytes equals the size of the first 3 batches
    assert!(!r.bytes.is_empty());
}

#[test]
fn advance_hwm_is_monotonic_and_clamped_to_log_end() {
    let (mut log, _dir) = open_tmp();
    log.append(&mut batch(0, 1, b"x")).unwrap(); // log_end = 1
    log.advance_hwm(5);                            // clamp to log_end
    assert!(log.hwm() == 1);
    log.advance_hwm(0);                            // never regress
    assert!(log.hwm() == 1);
}

#[test]
fn truncate_to_drops_log_end_and_hwm() {
    let (mut log, _dir) = open_tmp();
    for _ in 0..5 { log.append(&mut batch(0, 1, b"x")).unwrap(); }
    log.advance_hwm(5);
    log.truncate_to(2).unwrap();
    assert!(log.log_end_offset() == 2);
    assert!(log.hwm() == 2); // hwm clamped down to the truncation point
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

```rust
/// Serve KIP-595 `Fetch`: verbatim batch bytes in `[offset, min(hwm, log_end))`.
pub fn read_committed(&self, offset: i64, max_bytes: usize) -> Result<RawRead, RaftError> {
    let limit = self.hwm.min(self.log.log_end_offset());
    Ok(self.log.read_raw(offset, limit, max_bytes)?)
}

/// Advance the high watermark (monotonic; never past the log end).
pub fn advance_hwm(&mut self, new_hwm: i64) {
    let clamped = new_hwm.min(self.log.log_end_offset());
    if clamped > self.hwm {
        self.hwm = clamped;
    }
    debug_assert!(self.hwm <= self.log.log_end_offset());
}

/// Truncate the log so no record at offset `>= offset` remains; clamp HWM down.
pub fn truncate_to(&mut self, offset: i64) -> Result<(), RaftError> {
    debug_assert!(offset >= self.log.log_start_offset());
    self.log.truncate_to(offset)?;
    self.hwm = self.hwm.min(offset);
    Ok(())
}
```

- [ ] **Step 4: Run** → PASS. Run all `kraft::log` tests: `cargo test -p crabka-raft kraft::log`.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/log.rs
git commit -m "feat(raft): KraftLog HWM tracking, committed read, truncation"
```

---

## Task 5: Core-over-real-log integration

**Files:** create `crates/raft/tests/kraft_log_sim.rs` (and, if needed, refactor `crates/raft/tests/kraft_sim.rs` to share the harness).

This is the headline acceptance. First READ `crates/raft/tests/kraft_sim.rs` to understand the existing `Sim` harness (node = `QuorumStateMachine` + fake log + per-node deadline; message bus translating `Action`→`Event`; `partition`/`heal`/`leader_append`/`run_until_stable`).

- [ ] **Step 1: Generalize the harness to a pluggable log**

Make the per-node log abstracted behind a small trait the harness uses for the log operations it performs (append a leader batch, read committed bytes / decoded batches for replication, apply a fetched batch via `append_at`, truncate, advance hwm, and the `LogView` queries the core needs). Implement it for both the existing in-memory fake (so the 3a tests keep passing unchanged) and for `KraftLog`. Keep the existing `kraft_sim.rs` tests green.

- [ ] **Step 2: Write the KraftLog-backed integration tests**

In `crates/raft/tests/kraft_log_sim.rs`, build a `Sim` whose nodes use real `KraftLog` instances (one `tempfile::tempdir()` per node) and assert:

```rust
#[test]
fn voters_logs_byte_identical_up_to_hwm_over_real_log() {
    let mut sim = Sim::new_with_kraft_log(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    let leader = sim.leaders()[0];
    sim.leader_append(leader, /*records*/ 5); // appends 5 real batches in the leader's epoch
    sim.run_until_stable(10_000);
    let hwm = sim.leader_high_watermark(leader);
    assert!(hwm >= 5);
    // every voter's committed bytes (read_committed(0, hwm)) are byte-identical
    let leader_bytes = sim.committed_bytes(leader);
    for v in sim.voters() {
        assert!(sim.committed_bytes(v) == leader_bytes, "voter {v} log diverges from leader");
    }
}

#[test]
fn follower_truncates_real_log_on_divergence_then_reconverges() {
    let mut sim = Sim::new_with_kraft_log(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    let leader = sim.leaders()[0];
    // give a follower a conflicting-epoch tail the leader doesn't have
    let f = sim.voters().into_iter().find(|&v| v != leader).unwrap();
    sim.inject_conflicting_tail(f, /*epoch*/ 1, /*records*/ 2);
    sim.run_until_stable(10_000);
    // the follower's KraftLog was truncated and re-replicated to match the leader
    assert!(sim.committed_bytes(f) == sim.committed_bytes(leader));
}

#[test]
fn hwm_agrees_and_never_exceeds_any_voter_log_end() {
    let mut sim = Sim::new_with_kraft_log(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    let leader = sim.leaders()[0];
    sim.leader_append(leader, 3);
    sim.run_until_stable(10_000);
    let hwm = sim.leader_high_watermark(leader);
    for v in sim.voters() {
        assert!(hwm <= sim.log_end_offset(v), "hwm {hwm} exceeds voter {v} log end");
    }
}
```

(Method names — `new_with_kraft_log`, `committed_bytes`, `inject_conflicting_tail`, `log_end_offset`, `voters`, `leader_high_watermark` — are harness methods you add in Step 1. The replication model: on a follower `SendFetch`/`ReceiveFetch`, copy the leader's committed batches the follower is missing into the follower's `KraftLog` via `append_at`, respecting epochs, so `read_committed` bytes converge. `leader_append` appends real `RecordBatch`es stamped with the leader's current epoch.)

- [ ] **Step 3: Run**

Run: `cargo test -p crabka-raft --test kraft_log_sim -- --nocapture` and `cargo test -p crabka-raft --test kraft_sim` (the original 3a sim must still pass after the harness refactor).
Expected: all pass, no hangs. A divergence test that never re-converges, or logs that aren't byte-identical, indicates a real `KraftLog`/core-integration bug — debug it; if it's in the 3a core not the harness, STOP and report.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/tests/kraft_log_sim.rs crates/raft/tests/kraft_sim.rs
git commit -m "test(raft): 3a consensus core drives a real KraftLog (election + replication + truncation)"
```

---

## Task 6: Capstone — fmt, clippy, regression

- [ ] **Step 1:** `cargo fmt --all && cargo fmt --all --check` → clean.
- [ ] **Step 2:** `cargo clippy -p crabka-raft --tests` → clean (the `kraft` module is hand-written; keep it warning-free).
- [ ] **Step 3:** `cargo test -p crabka-raft` (incl. `--features kraft-spike`) → all pass; openraft path + Slice-0/3a untouched and green.
- [ ] **Step 4:** Commit any fmt fixes: `git add -A && git commit -m "chore(raft): fmt KraftLog" || echo "nothing to commit"`.

---

## Self-Review Notes

- **Spec coverage:** `KraftLog` facade (open/append/append_at/read_committed/truncate_to/advance_hwm/accessors) → Tasks 1,2,4; `LogView` impl → Task 3; standalone unit tests → Tasks 1–4; core-over-real-log integration (byte-identical logs up to HWM, HWM agreement, divergence truncation re-converges) → Task 5; capstone → Task 6. Snapshot/log-start edge correctly deferred (spec §error-handling) — 3b returns the available range only. All spec sections covered.
- **Type consistency:** `KraftLog`, `open`, `append`, `append_at`, `read_decoded`, `read_committed`, `advance_hwm`, `truncate_to`, `hwm`, `log_end_offset`, `log_start_offset` defined once (Tasks 1,2,4) and used consistently; `LogView` impl (Task 3) matches the 3a trait exactly.
- **crabka-log signatures** verified against `crates/log/src/log.rs`; the `end_offset_for_epoch` unknown→`-1`→`None` mapping is pinned by a test (Task 3) — if crabka-log's contract differs for too-large epochs, the implementer adjusts the test and notes it.
- **Isolation:** Tasks 1–4 add one file; Task 5 adds a test file + a harness refactor that keeps the existing 3a sim green. openraft + broker untouched → green tree at every commit. Task 5's harness method names are defined in its own Step 1 (not placeholders — the harness is built there).
