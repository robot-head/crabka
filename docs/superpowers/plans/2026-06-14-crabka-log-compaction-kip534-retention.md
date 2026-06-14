# KIP-534 Log-Compaction Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the confirmed control-batch key-dedup data-loss bug in the log compactor and implement full KIP-534 tombstone + transaction-marker retention (bit-6 delete-horizon wire format, `delete.retention.ms`, full `CleanedTransactionMetadata` marker eligibility, ProducerStateManager-driven RETAIN_EMPTY), proven by a RED→GREEN stateright model + large-N proptest.

**Architecture:** Extract pure, filesystem-free decision cores in `crates/log/src/compact.rs` (`should_index_key`, `retain_decision`, `compute_horizon`, `rewrite_batch_horizon`, `txn_data_fully_gone`) that both production and the verification call. Surface the delete-horizon on the wire via attributes bit 6 + repurposed `base_timestamp`. Thread a `CompactionContext { now, active_producers }` from the broker into `Log::compact`. The compactor reads sealed segments' `.txnindex` to build `CleanedTransactionMetadata` and rebuilds the survivor index.

**Tech Stack:** Rust, `stateright` 0.31, `proptest` (both already workspace deps in `crates/log`).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-log-compaction-kip534-retention-design.md`

**Verification discipline:** stateright runs are watchdog-guarded (3 GB / 150 s, `target_state_count`/`timeout` caps — `[[feedback_bound_model_checkers]]`). proptest is bounded sampling. `cargo +nightly fmt` per-crate (Windows deep-path workaround — `[[reference_windows_fmt_path_length]]`); clippy `-D warnings`; doc-comment code identifiers need backticks (`doc_markdown`). Greenfield: no back-compat shims — just change formats (`[[feedback_no_back_compat]]`).

---

## File Structure

- `crates/protocol/src/records/header.rs` — **modify**: `Attributes` delete-horizon bit-6 accessors + tests.
- `crates/protocol/src/records/owned.rs` — **modify**: `RecordBatch::delete_horizon_ms()` + `with_delete_horizon()` + tests.
- `crates/protocol/tests/proptest_records.rs` — **modify**: emit bit-6 batches in the arbitrary strategy (if the file/strategy exists; otherwise add a round-trip proptest here).
- `crates/log/src/config.rs` — **modify**: `LogConfig.delete_retention_ms`.
- `crates/broker/src/config_keys.rs` — **modify**: `delete.retention.ms` validate/recognize/apply/docs.
- `crates/log/src/compact.rs` — **modify**: pure cores + bug fix + horizon emission + `CleanedTransactionMetadata` + RETAIN_EMPTY; wire the model module.
- `crates/log/src/log.rs` — **modify**: `Log::compact(&mut self, ctx: &CompactionContext)` + `CompactionContext`.
- `crates/broker/src/producer_state.rs` — **modify**: `active_snapshot` for one partition.
- `crates/broker/src/partition_writer.rs` + `crates/broker/src/cleaner.rs` — **modify**: build + pass `CompactionContext`.
- `crates/log/src/compact_model.rs` — **create**: stateright model (`#[cfg(test)]` descendant) with the legacy-shim RED witness + GREEN real-core tests.
- `crates/log/tests/compact_retention_proptest.rs` — **create**: large-N fuzz over the pure cores + wire round-trip.

Batches: **B1** {Task W, Task C} parallel · **B2** {Task K} · **B3** {Task M, Task P} parallel.

---

## Batch 1 — Task W: Wire format (attributes bit 6 + delete-horizon)

**Files:** modify `crates/protocol/src/records/header.rs`, `crates/protocol/src/records/owned.rs`, `crates/protocol/tests/proptest_records.rs`.

- [ ] **Step 1: Read the current `Attributes` + `RecordBatch`**

Read `crates/protocol/src/records/header.rs` (the `Attributes(i16)` newtype, its `is_control_batch`/`is_transactional`/`with_control`/`with_transactional` methods, the `attr_case!` test macro at ~line 147, and the doc comment at ~line 20). Read `crates/protocol/src/records/owned.rs` `RecordBatch` (fields: `attributes`, `base_timestamp: i64`, `max_timestamp`, `records: Vec<Record>`; `Record` has `timestamp_delta: i32`).

- [ ] **Step 2: Write failing tests for the bit-6 accessors**

In `header.rs` `#[cfg(test)] mod tests`, add:

```rust
#[test]
fn delete_horizon_bit_round_trips() {
    let a = Attributes::default();
    assert!(!a.has_delete_horizon());
    let a = a.with_delete_horizon(true);
    assert!(a.has_delete_horizon());
    // bit 6 == 0x40; orthogonal to control/transactional bits.
    assert!(a.0 & 0x40 != 0);
    let a = a.with_control(true).with_transactional(true);
    assert!(a.has_delete_horizon() && a.is_control_batch() && a.is_transactional());
    let a = a.with_delete_horizon(false);
    assert!(!a.has_delete_horizon() && a.is_control_batch());
}
```

Run: `cargo test -p crabka-protocol --lib delete_horizon_bit_round_trips` → FAIL (methods missing).

- [ ] **Step 3: Implement the bit-6 accessors**

In `header.rs`, in `impl Attributes`:

```rust
/// KIP-534 delete-horizon flag (bit 6, mask 0x40). When set, the batch's
/// `base_timestamp` field carries the delete horizon (the time after which
/// retained tombstones/markers in the batch may be removed) instead of the
/// first record's timestamp. Matches Apache Kafka `DELETE_HORIZON_FLAG_MASK`.
pub const DELETE_HORIZON_BIT: i16 = 1 << 6;

#[must_use]
pub fn has_delete_horizon(self) -> bool {
    self.0 & Self::DELETE_HORIZON_BIT != 0
}

#[must_use]
pub fn with_delete_horizon(self, set: bool) -> Self {
    if set {
        Self(self.0 | Self::DELETE_HORIZON_BIT)
    } else {
        Self(self.0 & !Self::DELETE_HORIZON_BIT)
    }
}
```

Update the bit-layout doc comment (~line 20) to list `bit 6: has_delete_horizon`. Extend the `attr_case!`-based tests and any `all_set` case to include bit 6.

Run the test → PASS.

- [ ] **Step 4: Write failing tests for `RecordBatch` horizon accessors**

In `owned.rs` `#[cfg(test)] mod tests` (use the existing helpers for building a batch with records carrying timestamps):

```rust
#[test]
fn with_delete_horizon_stamps_and_preserves_record_timestamps() {
    // base_timestamp 1000; two records at abs ts 1000 and 1005 (deltas 0, 5).
    let mut b = RecordBatch { base_timestamp: 1000, ..RecordBatch::default() };
    b.records = vec![
        Record { offset_delta: 0, timestamp_delta: 0, ..Default::default() },
        Record { offset_delta: 1, timestamp_delta: 5, ..Default::default() },
    ];
    assert!(b.delete_horizon_ms().is_none());

    let horizon = 9999;
    let b = b.with_delete_horizon(horizon);
    assert!(b.attributes.has_delete_horizon());
    assert!(b.base_timestamp == horizon);
    assert!(b.delete_horizon_ms() == Some(horizon));
    // Absolute record timestamps are unchanged: base + delta.
    let abs: Vec<i64> = b.records.iter()
        .map(|r| b.base_timestamp + i64::from(r.timestamp_delta)).collect();
    assert!(abs == vec![1000, 1005]);
}

#[test]
fn delete_horizon_round_trips_through_encode_decode() {
    let mut b = RecordBatch { base_timestamp: 1000, last_offset_delta: 1, ..RecordBatch::default() };
    b.records = vec![
        Record { offset_delta: 0, timestamp_delta: 0, key: Some(bytes::Bytes::from_static(b"k")), ..Default::default() },
        Record { offset_delta: 1, timestamp_delta: 5, key: Some(bytes::Bytes::from_static(b"k")), ..Default::default() },
    ];
    let b = b.with_delete_horizon(9999);
    let mut buf = bytes::BytesMut::with_capacity(b.encoded_len());
    b.encode(&mut buf).unwrap();
    let mut cur: &[u8] = &buf;
    let d = RecordBatch::decode(&mut cur).unwrap();
    assert!(d.attributes.has_delete_horizon());
    assert!(d.delete_horizon_ms() == Some(9999));
    let abs: Vec<i64> = d.records.iter().map(|r| d.base_timestamp + i64::from(r.timestamp_delta)).collect();
    assert!(abs == vec![9999, 10004]); // deltas rewritten to abs - horizon → 1000-9999=-8999, 1005-9999=-8994
}
```

Note: after `with_delete_horizon(9999)`, `base_timestamp` becomes 9999 and deltas become `original_abs - 9999` (i.e. `-8999`, `-8994`), so `base + delta` reconstructs `1000`, `1005`. The second test asserts the *reconstructed originals* survive the round-trip — adjust the literal `abs` vec to the originals `[1000, 1005]` (the `[9999, 10004]` above is wrong; the reconstruction must equal the originals). Use `assert!(abs == vec![1000, 1005]);`.

Run → FAIL (methods missing).

- [ ] **Step 5: Implement `RecordBatch` horizon accessors**

In `owned.rs` `impl RecordBatch`:

```rust
/// KIP-534 delete horizon, if the delete-horizon attribute bit is set.
/// `base_timestamp` is repurposed to carry it (no separate wire field).
#[must_use]
pub fn delete_horizon_ms(&self) -> Option<i64> {
    self.attributes
        .has_delete_horizon()
        .then_some(self.base_timestamp)
}

/// Stamp the delete horizon: set attribute bit 6, move the horizon into
/// `base_timestamp`, and rewrite every record's `timestamp_delta` so the
/// reconstructed absolute timestamps (`base_timestamp + delta`) are unchanged.
#[must_use]
pub fn with_delete_horizon(mut self, horizon_ms: i64) -> Self {
    let old_base = self.base_timestamp;
    for r in &mut self.records {
        let abs = old_base + i64::from(r.timestamp_delta);
        // Deltas are i32 on the wire; compaction horizons keep them in range
        // for realistic timestamps. Saturate defensively.
        let new_delta = (abs - horizon_ms).clamp(i64::from(i32::MIN), i64::from(i32::MAX));
        r.timestamp_delta = new_delta as i32;
    }
    self.base_timestamp = horizon_ms;
    self.attributes = self.attributes.with_delete_horizon(true);
    self
}
```

Run both tests → PASS (fix the `abs == vec![1000, 1005]` assertion as noted).

- [ ] **Step 6: Extend the record-batch proptest strategy (if present)**

Read `crates/protocol/tests/proptest_records.rs`. If it has an arbitrary `RecordBatch` strategy + round-trip property, add a branch that sets bit 6 + a random horizon via `with_delete_horizon`, so the existing encode/decode round-trip fuzz also covers the flag. If no such strategy file exists, add a minimal proptest here:

```rust
proptest! {
    #[test]
    fn delete_horizon_batches_round_trip(base in -1i64..1_000_000, horizon in 0i64..1_000_000, deltas in proptest::collection::vec(0i32..10_000, 0..8)) {
        let mut b = crabka_protocol::records::RecordBatch { base_timestamp: base, last_offset_delta: deltas.len().saturating_sub(1) as i32, ..Default::default() };
        b.records = deltas.iter().enumerate().map(|(i, &d)| crabka_protocol::records::Record {
            offset_delta: i as i32, timestamp_delta: d, key: Some(bytes::Bytes::from_static(b"k")), ..Default::default()
        }).collect();
        let originals: Vec<i64> = b.records.iter().map(|r| b.base_timestamp + i64::from(r.timestamp_delta)).collect();
        let b = b.with_delete_horizon(horizon);
        let mut buf = bytes::BytesMut::new(); b.encode(&mut buf).unwrap();
        let mut cur: &[u8] = &buf; let d = crabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        prop_assert_eq!(d.delete_horizon_ms(), Some(horizon));
        let got: Vec<i64> = d.records.iter().map(|r| d.base_timestamp + i64::from(r.timestamp_delta)).collect();
        prop_assert_eq!(got, originals);
    }
}
```

- [ ] **Step 7: fmt + clippy + commit**

`cargo +nightly fmt -p crabka-protocol`; `cargo clippy -p crabka-protocol --all-targets -- -D warnings`; `cargo test -p crabka-protocol --lib records::`. Then:

```bash
git add crates/protocol/src/records/header.rs crates/protocol/src/records/owned.rs crates/protocol/tests/proptest_records.rs
git commit -m "feat(protocol)!: KIP-534 delete-horizon attribute bit + RecordBatch accessors"
```

---

## Batch 1 — Task C: `delete.retention.ms` config

**Files:** modify `crates/log/src/config.rs`, `crates/broker/src/config_keys.rs`.

- [ ] **Step 1: Failing test for the LogConfig default**

In `crates/log/src/config.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn default_delete_retention_is_24h() {
    let c = LogConfig::default();
    assert!(c.delete_retention_ms == std::time::Duration::from_hours(24));
}
```

Run: `cargo test -p crabka-log --lib default_delete_retention_is_24h` → FAIL (field missing).

- [ ] **Step 2: Add the field + default**

In `LogConfig` (struct ~lines 28-75): add

```rust
/// KIP-534. After a tombstone or transaction marker first becomes
/// compaction-eligible, retain it for at least this long before deletion
/// (the "delete horizon" grace window). Default 24h.
pub delete_retention_ms: Duration,
```

In the `Default` impl (~lines 77-98): add `delete_retention_ms: Duration::from_hours(24),`. Run → PASS.

- [ ] **Step 3: Failing tests for the broker config key**

Read `crates/broker/src/config_keys.rs` (the const keys, `validate_topic_config`, `is_recognized`, `apply_to_log_config`, `topic_config_docs`, and the `topic_config_docs_cover_known_keys` test). Add:

```rust
#[test]
fn validate_delete_retention_ms_accepts_nonneg_rejects_negative() {
    assert!(validate_topic_config(DELETE_RETENTION_MS, "0").is_ok());
    assert!(validate_topic_config(DELETE_RETENTION_MS, "86400000").is_ok());
    assert!(validate_topic_config(DELETE_RETENTION_MS, "-1").is_err());
}

#[test]
fn apply_delete_retention_ms_propagates() {
    let mut base = LogConfig::default();
    let out = apply_to_log_config(base.clone(), DELETE_RETENTION_MS, "12345");
    assert!(out.delete_retention_ms == std::time::Duration::from_millis(12345));
}
```

(Match the exact signatures of `validate_topic_config` / `apply_to_log_config` in the file — adapt the call shape.) Run → FAIL.

- [ ] **Step 4: Wire the key**

Add `pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";`. In `validate_topic_config` add an arm that parses an i64 and rejects `< 0` (reuse the existing non-negative-int validator helper, e.g. the one `retention.ms` uses but with min 0). In `is_recognized` add `DELETE_RETENTION_MS` (update the "N keys" doc count). In `apply_to_log_config` add an arm: parse i64 ms → `out.delete_retention_ms = Duration::from_millis(ms as u64)`. In `topic_config_docs` add `TopicConfigDoc { key: DELETE_RETENTION_MS, value_type: "long (ms)", default: Some("86400000"), kip: Some("KIP-534"), description: "How long tombstones and transaction markers are retained after becoming compaction-eligible." }` and add the key to the `topic_config_docs_cover_known_keys` recognized list. Run all the new + existing config-key tests → PASS.

- [ ] **Step 5: fmt + clippy + commit**

`cargo +nightly fmt -p crabka-log -p crabka-broker`; `cargo clippy -p crabka-log -p crabka-broker --all-targets -- -D warnings`. Then:

```bash
git add crates/log/src/config.rs crates/broker/src/config_keys.rs
git commit -m "feat(log): add delete.retention.ms config (KIP-534, default 24h)"
```

---

## Batch 2 — Task K: Cleaner rework + cross-crate wiring

**Files:** modify `crates/log/src/compact.rs`, `crates/log/src/log.rs`, `crates/broker/src/producer_state.rs`, `crates/broker/src/partition_writer.rs`, `crates/broker/src/cleaner.rs`. **Depends on Tasks W + C.** Single implementer (the `Log::compact` signature change ripples through the broker call sites; keep the repo compiling).

### Sub-step A — the pure decision cores in `compact.rs`

- [ ] **Step 1: Failing unit tests for the cores (TDD)**

Add to `compact.rs` a `#[cfg(test)] mod core_tests`:

```rust
use super::*;

#[test]
fn control_batch_key_is_never_indexed() {
    // THE BUG FIX: a control-batch record's key must not enter the dedup map.
    assert!(!should_index_key(Some(b"\x00\x00\x00\x01"), /*is_control*/ true));
    assert!(should_index_key(Some(b"k"), false));
    assert!(!should_index_key(None, false));
}

#[test]
fn tombstone_sets_horizon_then_deletes_after_expiry() {
    let rec = RecordMeta { has_key: true, has_value: false };
    let batch_no_h = BatchMeta { is_control: false, producer_id: -1, existing_horizon: None };
    // newest-for-key tombstone, no horizon → SetHorizon(now+ret)
    assert!(retain_decision(rec, batch_no_h, true, TxnDataState::NotTransactional, 100, 50)
        == RetainDecision::SetHorizon(150));
    // horizon present, now < horizon → Keep
    let batch_h = BatchMeta { existing_horizon: Some(150), ..batch_no_h };
    assert!(retain_decision(rec, batch_h, true, TxnDataState::NotTransactional, 149, 50) == RetainDecision::Keep);
    // horizon present, now >= horizon → Delete
    assert!(retain_decision(rec, batch_h, true, TxnDataState::NotTransactional, 150, 50) == RetainDecision::Delete);
    // superseded tombstone (not newest) → Delete regardless
    assert!(retain_decision(rec, batch_no_h, false, TxnDataState::NotTransactional, 0, 50) == RetainDecision::Delete);
}

#[test]
fn marker_retained_while_data_survives_then_ages() {
    let rec = RecordMeta { has_key: true, has_value: false }; // control record has a key, no value
    let ctrl_no_h = BatchMeta { is_control: true, producer_id: 7, existing_horizon: None };
    // data survives → Keep
    assert!(retain_decision(rec, ctrl_no_h, false, TxnDataState::DataSurvives, 100, 50) == RetainDecision::Keep);
    // data gone, no horizon → SetHorizon
    assert!(retain_decision(rec, ctrl_no_h, false, TxnDataState::DataFullyGone, 100, 50)
        == RetainDecision::SetHorizon(150));
    // data gone, horizon present, now >= horizon → Delete
    let ctrl_h = BatchMeta { existing_horizon: Some(150), ..ctrl_no_h };
    assert!(retain_decision(rec, ctrl_h, false, TxnDataState::DataFullyGone, 150, 50) == RetainDecision::Delete);
}

#[test]
fn live_data_kept_nullkey_dropped() {
    let live = RecordMeta { has_key: true, has_value: true };
    let b = BatchMeta { is_control: false, producer_id: -1, existing_horizon: None };
    assert!(retain_decision(live, b, true, TxnDataState::NotTransactional, 0, 50) == RetainDecision::Keep);
    assert!(retain_decision(live, b, false, TxnDataState::NotTransactional, 0, 50) == RetainDecision::Delete);
    let nullkey = RecordMeta { has_key: false, has_value: true };
    assert!(retain_decision(nullkey, b, false, TxnDataState::NotTransactional, 0, 50) == RetainDecision::Delete);
}

#[test]
fn rewrite_batch_horizon_preserves_absolute_timestamps() {
    let (new_base, deltas) = rewrite_batch_horizon(1000, &[0, 5, 20], 9999);
    assert!(new_base == 9999);
    let abs: Vec<i64> = deltas.iter().map(|d| new_base + d).collect();
    assert!(abs == vec![1000, 1005, 1020]);
}
```

Run → FAIL (cores missing).

- [ ] **Step 2: Implement the cores**

Add to `compact.rs` (module level):

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecordMeta {
    pub has_key: bool,
    pub has_value: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchMeta {
    pub is_control: bool,
    pub producer_id: i64,
    pub existing_horizon: Option<i64>,
}

/// Whether a transaction's data records survive this clean (drives marker
/// eligibility). For non-transactional batches: `NotTransactional`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TxnDataState {
    NotTransactional,
    DataSurvives,
    DataFullyGone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetainDecision {
    Keep,
    SetHorizon(i64),
    Delete,
}

/// build_offset_map filter. Control-batch records carry a control-type key
/// (commit/abort marker) that must NEVER enter the dedup map — indexing it
/// would key-dedup markers against each other and silently drop them
/// (the read_committed data-loss bug this slice fixes). Null-key data is also
/// never indexed.
pub(crate) fn should_index_key(key: Option<&[u8]>, is_control_batch: bool) -> bool {
    !is_control_batch && key.is_some()
}

pub(crate) fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

/// Reinterpret per-record timestamp deltas when stamping a delete horizon into
/// `base_timestamp`, preserving each record's absolute timestamp.
pub(crate) fn rewrite_batch_horizon(
    base_timestamp: i64,
    deltas: &[i64],
    horizon: i64,
) -> (i64, Vec<i64>) {
    let new_deltas = deltas
        .iter()
        .map(|d| (base_timestamp + d) - horizon)
        .collect();
    (horizon, new_deltas)
}

/// The single keep/age/delete oracle. Pure and total; production and the
/// stateright model + proptest all call this.
pub(crate) fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        // Markers are never key-deduped. Eligible only once the transaction's
        // data is gone, then after a delete-horizon grace window.
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => match batch.existing_horizon {
                Some(h) if now_ms >= h => RetainDecision::Delete,
                Some(_) => RetainDecision::Keep,
                None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
            },
        };
    }
    // Data records.
    if !rec.has_key {
        return RetainDecision::Delete; // null-key data is never retained
    }
    if !is_newest_for_key {
        return RetainDecision::Delete; // superseded by a newer record for this key
    }
    if rec.has_value {
        return RetainDecision::Keep; // newest live value for the key
    }
    // Newest record for the key is a tombstone → age it via the horizon.
    match batch.existing_horizon {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}
```

Run the core tests → PASS.

### Sub-step B — `CleanedTransactionMetadata` + fix `build_offset_map`

- [ ] **Step 3: Fix `build_offset_map` (the bug) + a regression test**

Change `build_offset_map` (lines ~54-71) to skip control batches and use `should_index_key`:

```rust
for batch in read_all_batches(seg)? {
    if batch.attributes.is_control_batch() {
        continue; // control records (txn markers) are not compaction keys
    }
    for record in &batch.records {
        if !should_index_key(record.key.as_deref(), false) {
            continue;
        }
        let key_bytes = record.key.as_ref().expect("should_index_key guaranteed Some");
        let absolute = batch.base_offset + i64::from(record.offset_delta);
        map.insert(key_bytes.clone(), absolute);
    }
}
```

Add a regression test in `build_map_tests` that a control batch's key is NOT in the map (build a segment with a commit-marker control batch via a helper that sets `attributes = Attributes::default().with_control(true)` and a 4-byte key; assert `map` is empty).

- [ ] **Step 4: Implement `CleanedTransactionMetadata`**

Add a struct that, given the sealed segments and the surviving-data outcome, classifies each control batch's `TxnDataState`. Build it in two passes within the rewrite pipeline:

```rust
/// Tracks, across the segments being compacted, which producers' transactional
/// data survives this clean — so a marker is discardable only when its
/// transaction's data is fully gone. Seeded from the sealed segments' .txnindex.
pub(crate) struct CleanedTransactionMetadata {
    /// producer_id -> any committed data record for that producer survives.
    data_survives: std::collections::HashSet<i64>,
    /// Aborted-txn entries observed (rebuilt for survivors' .txnindex).
    aborted: Vec<crate::txn_index::AbortedTxn>,
}

impl CleanedTransactionMetadata {
    pub(crate) fn txn_state(&self, producer_id: i64) -> TxnDataState {
        if self.data_survives.contains(&producer_id) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }
}

/// `producer_id` set whose data survived; pure helper for the marker precondition.
pub(crate) fn txn_data_fully_gone(producer_id: i64, data_survives: &std::collections::HashSet<i64>) -> bool {
    !data_survives.contains(&producer_id)
}
```

Build `data_survives` during the keep decision: as `rewrite_segments` keeps a transactional data record (`batch.producer_id >= 0 && !batch.attributes.is_control_batch()` and the record is kept), insert `batch.producer_id`. Because a marker may appear in an earlier offset than some surviving data of the same producer is impossible (markers terminate a txn), do a **first pass** over all segments computing which producer_ids have a surviving (kept) data record, *then* a **second pass** that emits records and applies `retain_decision` with the now-complete `txn_state`. Seed `aborted` from each sealed segment's `.txnindex` (open `TxnIndex::open(seg.txn_index_path())`), and re-collect the entries whose aborted data still partially survives for the survivor index.

- [ ] **Step 5: Route `rewrite_segments` through the cores + horizon emission + RETAIN_EMPTY**

Rewrite the per-record loop (lines ~243-251) to compute, per record, `RecordMeta`/`BatchMeta`/`is_newest_for_key`/`txn_state`, call `retain_decision`, and act:
- `Keep` → push the record.
- `SetHorizon(h)` → push the record AND mark the batch to be stamped (stamp once per batch: set bit 6, `base_timestamp = h`, rewrite deltas via `rewrite_batch_horizon` — or via `RecordBatch::with_delete_horizon(h)` on the rebuilt batch).
- `Delete` → drop.

After building `kept` for a batch: if `kept.is_empty()` but the batch is the **last batch of an active producer** (`ctx.active_producers.contains_key(&batch.producer_id)`) or the last batch of the consolidated output, emit a bare header (RETAIN_EMPTY): re-encode the batch with `records: vec![]` preserving `producer_id/producer_epoch/base_sequence/base_offset`. Otherwise skip.

Rebuild the survivor `.txnindex`: after the rewrite, write the retained `aborted` entries to the new segment's `.txnindex` (via `TxnIndex::open(new_seg.txn_index_path())` + `append`), so `Log::aborted_in_range` keeps working post-compaction.

Keep the existing `last_offset_delta` recompute + absolute-offset preservation; the emitted batch additionally carries the (possibly stamped) horizon.

- [ ] **Step 6: Unit tests for the compaction pipeline**

Extend `rewrite_tests`: (a) two commit markers (different offsets) **both survive** a compaction when their txn data survives (the bug-fix end-to-end); (b) a tombstone with no horizon gets bit 6 + `base_timestamp == now + delete_retention_ms`; (c) a marker whose data is fully gone + horizon elapsed is dropped; (d) RETAIN_EMPTY emits a bare header for an active producer's emptied batch. Use a `now`/`delete_retention_ms` passed via the new context. Run → PASS.

### Sub-step C — `Log::compact(ctx)` + broker wiring

- [ ] **Step 7: `CompactionContext` + new `Log::compact` signature**

In `crates/log/src/log.rs`:

```rust
/// Inputs the compactor needs beyond the log itself: a deterministic clock
/// (mirrors retention's injected `now`) and a producer-state snapshot so
/// RETAIN_EMPTY can keep the last batch of each still-active producer.
pub struct CompactionContext {
    pub now: std::time::SystemTime,
    /// producer_id -> last batch base_offset for producers still active.
    pub active_producers: std::collections::HashMap<i64, i64>,
}
```

Change `pub fn compact(&mut self)` → `pub fn compact(&mut self, ctx: &CompactionContext)`. Derive `now_ms = retention::now_ms(ctx.now)` and read `delete_retention_ms` from `self.config`. Pass `now_ms`, `delete_retention_ms`, and `&ctx.active_producers` into `rewrite_segments` (extend its signature). Update the existing in-crate `compact()` call sites/tests to pass a context (a test helper `CompactionContext { now: SystemTime::now(), active_producers: HashMap::new() }`, or a fixed epoch for determinism).

- [ ] **Step 8: Producer-state snapshot in the broker**

In `crates/broker/src/producer_state.rs`, add:

```rust
/// Snapshot of still-active producers for one (topic, partition): producer_id
/// -> last batch base_offset. Used by compaction RETAIN_EMPTY. A producer is
/// active if its last activity is within `producer_id_expiration_ms` of `now`.
pub fn active_snapshot(&self, topic: &str, partition: i32, now_ms: i64, expiration_ms: i64)
    -> std::collections::HashMap<i64, i64>
{
    // walk by_topic[topic][partition].entries; include pid -> entry.base_offset
    // where now_ms - entry.last_activity_ms <= expiration_ms.
}
```

(Match the real internal map shape from the file. Add a unit test that an expired producer is excluded and an active one is included.)

- [ ] **Step 9: Build + pass `CompactionContext` from the writer/cleaner**

In `crates/broker/src/partition_writer.rs` `WriterMessage::Compact` handler: build `CompactionContext { now: SystemTime::now(), active_producers: <producer_state>.active_snapshot(topic, partition, now_ms, expiration_ms) }` and call `log.compact(&ctx)`. Source `expiration_ms` from the existing producer-id expiration setting (or a sane default constant if none exists yet — document it). In `crates/broker/src/cleaner.rs`, ensure the tick passes its wall-clock through (no behavioral change beyond threading).

- [ ] **Step 10: Build the whole workspace + run existing suites**

`cargo build --workspace`; `cargo test -p crabka-log --lib compact`; `cargo test -p crabka-log --lib txn_index`; `cargo test -p crabka-broker --lib producer_state`. Expected: all green (existing compaction/txn-index/producer-state behavior preserved except the intended marker-retention change).

- [ ] **Step 11: fmt + clippy + commit**

`cargo +nightly fmt -p crabka-log -p crabka-broker`; `cargo clippy -p crabka-log -p crabka-broker --all-targets -- -D warnings`. Then:

```bash
git add crates/log/src/compact.rs crates/log/src/log.rs crates/broker/src/producer_state.rs crates/broker/src/partition_writer.rs crates/broker/src/cleaner.rs
git commit -m "feat(log): KIP-534 cleaner — retain+age tombstones/markers, fix control-batch dedup"
```

---

## Batch 3 — Task M: stateright model (RED→GREEN)

**Files:** create `crates/log/src/compact_model.rs`; wire it in `compact.rs`. **Depends on Task K.**

- [ ] **Step 1: Wire the module**

Append to `crates/log/src/compact.rs`:

```rust
#[cfg(test)]
#[path = "compact_model.rs"]
mod compact_model;
```

- [ ] **Step 2: Write the model**

Create `crates/log/src/compact_model.rs` following the `leader_epoch_model.rs` template. Module doc explains the bug + the KIP-534 contract. Use:

```rust
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use stateright::{Checker, Model, Property};
use super::{BatchMeta, RecordMeta, RetainDecision, TxnDataState, compute_horizon, retain_decision, should_index_key};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);
```

**State:** an abstract log = `Vec<Entry>` where `Entry { key: Option<u8>, kind: EntryKind, horizon: Option<i64> }`, `EntryKind ∈ { Data{ value: Option<u8> }, Marker{ producer_id: u8, commit: bool } }`; an abstract `clock: i64`; witness flags `saw_tombstone_aged_out`, `saw_marker_aged_out`, `saw_marker_retained_for_live_data`, `saw_horizon_stamped`, `saw_control_not_deduped`.

**Actions:** `AppendData(key, value)`, `AppendTombstone(key)`, `AppendCommit(pid)`, `AppendAbort(pid)` (key/value/pid ∈ {0,1}); `Tick(dt)` (dt ∈ {1,2}); `Compact`.

**`Compact` transition** (the oracle): build `offset_map` (newest abs offset per key over Data entries, using `should_index_key(key, is_control=false)`), build `data_survives: HashSet<pid>` (a pid whose newest-for-key committed data entry would be kept), then for each entry compute `RecordMeta`/`BatchMeta`/`is_newest`/`txn_state` and apply `retain_decision` at `clock`/`delete_retention_ms`. Produce the next `Vec<Entry>` (Keep → carry, SetHorizon(h) → carry with `horizon=Some(h)`, Delete → drop). Set witness flags.

**Safety asserts** in `next_state` on `Compact` (panic on violation):
1. **control-not-deduped**: every surviving distinct marker from the input that was Kept/SetHorizon is present in the output exactly once; no two same-(producer/commit) markers merged. (This is what the legacy shim violates.)
2. **marker-data-precedence**: for every pid with a surviving Data entry in the output, that pid's marker (if it was in the input) is in the output.
3. **tombstone-aging**: no surviving tombstone has `horizon.is_some() && clock >= horizon`.
4. **idempotent-stamp**: an entry that already had `horizon=Some(_)` is never re-stamped to a different horizon.
5. **no-data-loss**: every key with a newest live Data(value=Some) entry in the input has a live entry in the output.
6. **timestamp-preserved**: drive `rewrite_batch_horizon` on a sample and assert reconstruction (can be a standalone assert in the model or covered by the proptest).

**Properties:** `Property::always` for a structural invariant (e.g. horizons monotone / output ⊆ input keys); `Property::sometimes` for each of the 5 witnesses.

**Bounds:** `compaction_basic` (keys/pids {0,1}, len ≤ 5, clock ≤ 6); `compaction_wide` (len ≤ 8, clock ≤ 10). `run(model, label)` asserts `max_depth < MAX_DEPTH` + `state_count < MAX_STATES` then `assert_properties()`.

- [ ] **Step 3: The legacy-shim RED witness (committed, CI-green)**

Add a `legacy_retain` fn in the model module reproducing the **buggy** behavior (control-batch records ARE key-deduped: treat markers as keyed data with key = control type, dedup to newest), and a `#[should_panic(expected = "control")]` test `legacy_control_dedup_violates_safety` that runs the model `Compact` transition using `legacy_retain` and expects the **control-not-deduped** assert to fire. This permanently documents the bug as a CI-green witness. Record the captured counterexample (e.g. "two commit markers pid 0 and pid 1 → older dropped") in a comment + the commit message.

- [ ] **Step 4: Run under the watchdog**

`cargo +nightly fmt -p crabka-log`; `cargo clippy -p crabka-log --all-targets -- -D warnings`; build `cargo test -p crabka-log --lib compact_model --no-run`. Run `compaction_basic` + `compaction_wide` + `legacy_control_dedup_violates_safety` under the host memory watchdog (PowerShell: launch the test exe with `compact_model:: --nocapture --test-threads=1`, poll `WorkingSet64`, kill > 3 GB / > 150 s). Confirm: exhaustive (`state_count < MAX_STATES`, `max_depth < MAX_DEPTH`), all real-core asserts hold, all 5 `sometimes` witnesses satisfied, and the legacy `#[should_panic]` test passes (RED witness). Scale `compaction_wide` up while exhaustive.

- [ ] **Step 5: Commit**

```bash
git add crates/log/src/compact.rs crates/log/src/compact_model.rs
git commit -m "test(log): stateright model of KIP-534 compaction retention (RED legacy witness + GREEN)"
```

---

## Batch 3 — Task P: proptest fuzz at large N

**Files:** create `crates/log/tests/compact_retention_proptest.rs`. **Depends on Task K.** (Integration-test file so it can `use crabka_log::...`; if the cores are `pub(crate)`, instead add a `#[cfg(test)] mod retention_fuzz` in `compact.rs` mirroring `producer_state.rs`'s `fuzz` module. Prefer the in-crate module so the `pub(crate)` cores are reachable.)

- [ ] **Step 1: Write the fuzz module**

In `compact.rs` add `#[cfg(test)] mod retention_fuzz` with a `proptest!` that generates a random op sequence (AppendData/Tombstone/Commit/Abort with small key/pid alphabets, interleaved Tick/Compact) up to ~200 ops, random `delete_retention_ms`, random clock jumps; apply via the same pure cores (reuse a reference applier mirroring the model's `Compact`). Assert every `Compact`:
- convergence/idempotence at a fixed clock: `compact(compact(L)) == compact(L)`;
- monotone shrink + no-data-loss (every live newest key preserved);
- marker safety (survives iff data survives; never deleted before `clock >= horizon`);
- tombstone aging (present iff no horizon or `clock < horizon`);
- single horizon stamping (never re-stamped to a different value);
- a wire round-trip: build a real `RecordBatch::with_delete_horizon(h)`, encode→decode, assert `delete_horizon_ms() == Some(h)` and reconstructed absolute timestamps equal the originals.

- [ ] **Step 2: Run + fmt + clippy + commit**

`cargo test -p crabka-log --lib retention_fuzz` (256+ cases); `cargo +nightly fmt -p crabka-log`; `cargo clippy -p crabka-log --all-targets -- -D warnings`. Then:

```bash
git add crates/log/src/compact.rs
git commit -m "test(log): proptest fuzz of KIP-534 compaction retention at large N"
```

---

## Self-Review

**Spec coverage:** wire format (Task W) ✓; config (Task C) ✓; bug fix in `should_index_key`/`build_offset_map` (Task K Steps 2-3) ✓; two-pass horizon + `retain_decision` (K Steps 2,5) ✓; full `CleanedTransactionMetadata` + survivor `.txnindex` rebuild (K Step 4-5) ✓; RETAIN_EMPTY via producer-state snapshot (K Steps 5,8-9) ✓; `CompactionContext` cross-crate wiring (K Steps 7-9) ✓; stateright RED→GREEN model (Task M) ✓; proptest + wire round-trip (Task P) ✓; watchdog + fmt + clippy discipline (M Step 4, all commit steps) ✓.

**Placeholder scan:** the cores, accessors, config wiring, and model/proptest structure have concrete code. The two genuinely code-heavy integration points — `CleanedTransactionMetadata`'s two-pass survivor computation (K Step 4-5) and the producer-state snapshot internals (K Step 8) — give the algorithm + signatures + the pinning tests rather than every line, because they must match internal map shapes the implementer reads in-file; the TDD tests (K Step 6, Step 8) pin the behavior. Not hidden TODOs.

**Type consistency:** `should_index_key(Option<&[u8]>, bool)`, `retain_decision(RecordMeta, BatchMeta, bool, TxnDataState, i64, i64) -> RetainDecision`, `compute_horizon(i64,i64)->i64`, `rewrite_batch_horizon(i64,&[i64],i64)->(i64,Vec<i64>)`, `RecordBatch::{delete_horizon_ms,with_delete_horizon}`, `Attributes::{DELETE_HORIZON_BIT,has_delete_horizon,with_delete_horizon}`, `CompactionContext{now,active_producers}`, `Log::compact(&CompactionContext)`, `ProducerState::active_snapshot(...) -> HashMap<i64,i64>` — used consistently across W/C/K/M/P. ✓
