# Log Compaction — KIP-534 Tombstone & Transaction-Marker Retention — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright model of a pure core), bundled with the production feature that
makes the core correct.
**Predecessors:** raft, share-group (#514), ISR (#515), failover (#516), reassignment (#520),
KIP-848 reconciliation (#521 — found a real bug), KIP-98 EOS txn coordinator (#523),
idempotent-producer + log-truncation data-plane models (#524).

## Goal

Implement **full KIP-534 retention** in crabka's log compactor and prove it with a stateright model +
proptest:

1. **Fix a confirmed, reachable data-loss bug.** The compactor key-dedups *control batches*
   (transaction commit/abort markers), collapsing every commit marker in a log to one survivor and
   every abort marker to one — silently breaking `read_committed` exactly-once on
   `cleanup.policy=compact` topics that receive transactional writes.
2. **Retain tombstones and txn markers for `delete.retention.ms`** via Kafka's two-pass delete-horizon
   mechanism, byte-exact on the wire (attributes bit 6 + repurposed `base_timestamp`).
3. **Full-fidelity marker eligibility** (`CleanedTransactionMetadata` over the sealed segments'
   `.txnindex`) and **ProducerStateManager-driven RETAIN_EMPTY** (keep the last batch per active
   producer to preserve its sequence/epoch).
4. A **stateright model** that RED-flags the current bug and GREENs after the fix, plus proves the
   aging/retention invariants; and a **proptest** fuzz at large N over the same pure cores + a wire
   round-trip.

## Background — the bug, confirmed in source

- Markers are control batches with a fixed per-type key: [`build_marker_batch`](../../../crates/broker/src/txn/marker.rs)
  emits `with_control(true)` + a 4-byte record key `version(i16)+type(i16)` — `00 00 00 01` for *every*
  commit, `00 00 00 00` for *every* abort.
- Markers are written into partition logs in production:
  [`write_txn_markers::handle`](../../../crates/broker/src/txn/handlers/write_txn_markers.rs) and the
  local fan-out in [`end_txn.rs`](../../../crates/broker/src/txn/handlers/end_txn.rs) call
  `build_marker_batch` then `produce_batch`.
- Compaction runs on `cleanup.policy=compact` topics via the cleaner ticker →
  [`Log::compact`](../../../crates/log/src/log.rs) → [`build_offset_map` + `rewrite_segments`](../../../crates/log/src/compact.rs).
- Both `build_offset_map` and `rewrite_segments` branch only on `record.key.is_none()` and **never**
  consult `batch.attributes.is_control_batch()`. So all same-type markers key-dedup to the newest
  offset; older markers are dropped.

`read_committed` correctness depends on markers; dropping them is data-loss-equivalent. Keeping markers
forever is always safe — the only cost is unbounded accumulation, which KIP-534 aging reclaims.

## Current architecture (relevant facts)

- `Log` owns only the **active** segment's `TxnIndex` (`active_txn_index`); sealed segments each carry a
  `.txnindex` file (`Segment::txn_index_path()`), reopened on demand. `TxnIndex` entries are
  `AbortedTxn { start_offset, last_offset, producer_id }`.
- `Log::compact(&mut self)` consolidates **all sealed segments into one** new segment (never the active
  segment). This single-consolidated-output design is retained (multi-round grouping is out of scope).
- **Producer state lives in `crates/broker`** (`ProducerState`, the idempotent-dedup state modeled in
  #524: per `(topic, partition, producer_id)` → `ProducerEntry { epoch, last_sequence, last_offset,
  base_offset, last_activity_ms }`, with `expire_older_than`). The log crate has no access to it today.
- `RecordBatch` (crates/protocol) is v2-only (decode rejects `magic != 2`); the header carries
  `attributes: Attributes(i16)`, `base_timestamp: i64`, `max_timestamp: i64`, `producer_id`,
  `producer_epoch`, `base_sequence`, and per-record `timestamp_delta` where
  `record_ts = base_timestamp + timestamp_delta`.

---

## Layer 1 — Wire format (byte-exact, verified vs Apache Kafka trunk)

**Delete-horizon flag = attributes bit 6, mask `0x40`** — the lowest previously-unused bit, below
`Control`(5) / `Transactional`(4) / `TimestampType`(3) / `Compression`(0–2). Matches Kafka's
`DefaultRecordBatch.DELETE_HORIZON_FLAG_MASK = 0x40`.

**Horizon storage: repurpose `base_timestamp` — no new wire field.** When bit 6 is set, the i64
`base_timestamp` field *is* the delete-horizon (`deleteHorizonMs`); when clear, there is no horizon.
This is exactly Kafka's `deleteHorizonMs()`.

**`crates/protocol/src/records/header.rs` (`Attributes`):**
- `pub const DELETE_HORIZON_BIT: i16 = 1 << 6;`
- `pub fn has_delete_horizon(self) -> bool`
- `pub fn with_delete_horizon(self, b: bool) -> Self`
- doc-comment update + extend the `attr_case!` / `all_set` tests for bit 6.

**`crates/protocol/src/records/owned.rs` (`RecordBatch`):** keep the wire field named `base_timestamp`
(do **not** add a phantom `delete_horizon_ms` struct field — it would have no wire slot). Add ergonomic
accessors:
- `pub fn delete_horizon_ms(&self) -> Option<i64>` → `has_delete_horizon().then_some(self.base_timestamp)`.
- `pub fn with_delete_horizon(self, horizon_ms: i64) -> Self` → sets bit 6, writes
  `base_timestamp = horizon_ms`, and **rewrites every record's `timestamp_delta`** (below).

**Delta reinterpretation (load-bearing).** Because `record_ts = base_timestamp + timestamp_delta`, when
the cleaner stamps a horizon into `base_timestamp` it must rewrite each surviving record's delta to
`original_abs_ts - horizon` so reconstructed timestamps are unchanged. `max_timestamp` is untouched.
CRC coverage already spans `attributes` + `base_timestamp`, so no CRC-logic change — only the bit-6
accessors are added; the existing encoder/decoder round-trips `base_timestamp` verbatim.

**Down-conversion:** none — crabka is v2-only and the cleaner always writes v2.

---

## Layer 2 — Config

Add `delete.retention.ms` end-to-end (Kafka default **86,400,000 ms = 24h**).

**`crates/log/src/config.rs` (`LogConfig`):** add `pub delete_retention_ms: Duration` (concrete, not
`Option` — Kafka has no "unlimited" sentinel for this key); default `Duration::from_hours(24)`. Add a
`default_delete_retention_is_24h` test.

**`crates/broker/src/config_keys.rs`:**
- `pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";`
- `validate_topic_config`: parse i64 ms, reject `< 0` (min 0).
- `is_recognized`: add to the matches list (update the "N keys" doc count).
- `apply_to_log_config`: set `out.delete_retention_ms = Duration::from_millis(..)`.
- `topic_config_docs`: add a `TopicConfigDoc` (type `long (ms)`, default `"86400000"`, KIP-534) and add
  the key to the `topic_config_docs_cover_known_keys` assertion list.
- Seed the broker-wide default where other `LogConfig` defaults (segment/retention) are constructed.

Topic-level key only (the broker-level `log.cleaner.delete.retention.ms` default is just the seed value;
no separate broker-config surface needed).

---

## Layer 3 — Cleaner rework

Replace the single-pass key-dedup in `crates/log/src/compact.rs` with a two-pass horizon mechanism. All
keep/age/delete logic lives in **pure, filesystem-free cores** that production *and* the model/proptest
call.

### `now` and producer-state injection (cross-crate interface)

`Log::compact` gains a clock and a producer-state snapshot:

```rust
/// Snapshot of broker producer state for one partition, passed into compaction
/// so RETAIN_EMPTY can keep the last batch of each still-active producer.
pub struct CompactionContext {
    pub now: SystemTime,
    /// producer_id -> last batch base_offset, for producers still active
    /// (not yet expired by producer.id.expiration.ms). Empty = none active.
    pub active_producers: HashMap<i64, i64>,
}

pub fn compact(&mut self, ctx: &CompactionContext) -> Result<(), LogError>;
```

`now_ms` is derived via the existing `retention::now_ms(now)` helper (same injection pattern as
`Log::tick`). `delete_retention_ms` is read from the `LogConfig` the `Log` already owns. The broker side
([`partition_writer.rs`](../../../crates/broker/src/partition_writer.rs) `WriterMessage::Compact` handler,
fed by [`cleaner.rs`](../../../crates/broker/src/cleaner.rs)) builds `CompactionContext` from
`SystemTime::now()` + a snapshot of the partition's `ProducerState` (producers whose
`last_activity_ms` is within `producer.id.expiration.ms`).

### Pure decision cores (the wrap-real targets)

```rust
struct RecordMeta { has_key: bool, has_value: bool }       // tombstone = has_key && !has_value
struct BatchMeta  { is_control: bool, producer_id: i64, existing_horizon: Option<i64> }
enum   RetainDecision { Keep, SetHorizon(i64), Delete }

/// build_offset_map filter; THE BUG FIX lives here (control batches never indexed).
fn should_index_key(key: Option<&[u8]>, is_control_batch: bool) -> bool;

/// now+retention, saturating.
fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64;

/// byte-exact delta reinterpretation; preserves absolute record timestamps.
fn rewrite_batch_horizon(base_timestamp: i64, deltas: &[i64], horizon: i64)
    -> (i64 /*=horizon*/, Vec<i64> /*new deltas*/);

/// the single keep/age/delete oracle, pure & total.
fn retain_decision(
    rec: RecordMeta, batch: BatchMeta, is_newest_for_key: bool,
    txn: TxnDataState, now_ms: i64, delete_retention_ms: i64,
) -> RetainDecision;
```

`retain_decision` rules (faithful to Kafka `LogCleaner.shouldRetainRecord` + `checkBatchRetention`):

- **Data, keyed, value=Some:** `Keep` iff newest-for-key (offset-map hit); else `Delete`. (Unchanged.)
- **Data, keyed, value=None (tombstone):** if not newest-for-key → `Delete`. If newest: horizon present
  and `now_ms >= horizon` → `Delete`; no horizon yet → `SetHorizon(compute_horizon(..))`; horizon present
  and `now_ms < horizon` → `Keep`.
- **Data, key=None:** `Delete` (null-key data is never indexed/kept — matches today).
- **Control batch (marker):** never key-dedup. `can_discard = txn.is_fully_gone()`. If `!can_discard`
  → `Keep` (txn data still present). If `can_discard`: horizon present and `now_ms >= horizon` →
  `Delete`; else → `SetHorizon` (Kafka's "marker for empty txn" path — stamp one grace window, retain).

### Full `CleanedTransactionMetadata` (chosen fidelity)

Built in pass 1 over the sealed segments being compacted, in offset order:

- Seed aborted transactions from each sealed segment's `.txnindex` (`AbortedTxn` priority queue sorted
  by `start_offset`), reachable in-crate via `Segment::txn_index_path()` + `TxnIndex::open`.
- Track, per `producer_id`, whether any data record of its transaction **survives** this clean
  (committed data that is newest-for-key) vs. is fully removed (aborted data is dropped; superseded
  committed data is dropped). `TxnDataState ∈ { NotTransactional, DataSurvives, DataFullyGone }`.
- A marker is `can_discard` iff its producer's transaction has **no surviving data** at the point its
  marker is read.
- **Rebuild the survivor `.txnindex`:** aborted-txn entries whose data still partially survives are
  re-appended to the new segment's `.txnindex`, so fetch-time aborted-record filtering
  (`Log::aborted_in_range`) keeps working after compaction.

### RETAIN_EMPTY via producer state (chosen fidelity)

When a batch loses all records but is the **last batch of a still-active producer** (present in
`ctx.active_producers`) or the last batch of the cleaning round, emit a bare v2 header (empty `records`)
preserving `producer_id / producer_epoch / base_sequence / base_offset` — so producer sequence/epoch and
the log-end offset survive. crabka's batch struct already carries these fields.

`rewrite_segments` keeps its `last_offset_delta` recompute and absolute-offset preservation; the emitted
batch additionally carries the (possibly stamped) horizon attributes + `base_timestamp`.

---

## Stateright model (`crates/log/src/compact_model.rs`)

`#[cfg(test)] #[path="compact_model.rs"] mod compact_model;`, watchdog-bounded
(`MAX_STATES=200_000`, `MAX_DEPTH=40`, `timeout=Duration::from_mins(2)`), following the
`leader_epoch_model.rs` template (a `run(model,label)` that asserts depth/state caps then
`assert_properties()`).

**State:** an abstract log = ordered `Vec` of entries `{ key: Option<u8>, kind: Data{value:Option<u8>}
| Marker{producer_id, commit|abort}, horizon: Option<i64> }`; an abstract `clock: i64`; and an
`active_producers` set; plus non-vacuity witness flags.

**Actions:** `AppendData(key,value)`, `AppendTombstone(key)`, `AppendCommit(pid)`, `AppendAbort(pid)`
(bounded alphabet: keys/pids ∈ {0,1}); `Tick(dt∈{1,2})`; `Compact` (runs the pure cores over the whole
abstract log at the current clock: build offset-map + `CleanedTransactionMetadata` via
`should_index_key`, map each entry through `retain_decision`, produce the next log).

**Safety asserts** (per-transition, on `Compact`):
1. **control-not-deduped** (RED-flags the bug): two markers with the same control key are never merged
   or dropped against each other; a marker is removed only via the txn-data-gone + horizon path.
2. **marker-data-precedence:** if any data record for `pid` survives, `pid`'s marker survives too.
3. **tombstone-aging:** a newest-for-key tombstone is retained while `clock < horizon`; deleted once a
   horizon exists and `clock >= horizon` (no survivor has `horizon.is_some() && clock >= horizon`).
4. **idempotent-stamp:** a batch's horizon is stamped at most once; re-compacting a horizon-bearing
   batch before expiry is a no-op (stable convergence).
5. **no-data-loss:** every key with a live (newest, value-present) record in the input still has it in
   the output.
6. **timestamp-preserved:** drive `rewrite_batch_horizon` and assert reconstructed `base+delta` equals
   the original absolute timestamp for every record.

**Non-vacuity (`Property::sometimes`):** `saw_tombstone_aged_out`, `saw_marker_aged_out`,
`saw_marker_retained_for_live_data`, `saw_horizon_stamped`, `saw_control_not_deduped` (two same-key
markers both present after a compact).

**RED → GREEN (committed witness, chosen):** land the model first wired to the current buggy logic via a
thin `legacy_retain` shim; run it and capture the failing counterexample on assert #1/#2; commit that as
the documented RED witness (TCM/DPM precedent). Then switch the cores to the fixed `retain_decision` and
show GREEN. Two configs: `compaction_basic` (keys/pids {0,1}, len ≤ 5, clock ≤ 6) and `compaction_wide`
(len ≤ 8, clock ≤ 10), scaled up while exhaustive under the watchdog.

## proptest fuzz (large N)

A proptest module over the **same pure cores** (256–1024 cases, mirroring #524): random op sequences
(append data/tombstone/commit/abort with small key/pid alphabets, interleaved `Tick`/`Compact`) up to
~200 ops, random `delete_retention_ms` and clock jumps. Invariants every `Compact`: convergence/
idempotence at a fixed clock; monotone shrink + no-data-loss; marker safety (survives iff data survives;
never deleted before `clock >= horizon`); tombstone aging; single horizon stamping. Plus a **wire
round-trip**: build a real `RecordBatch` with bit 6 + horizon + rewritten deltas, encode→decode, assert
struct equality, `delete_horizon_ms()` returns the stamped value, and reconstructed per-record absolute
timestamps equal the originals. Extend the protocol crate's `RecordBatch` arbitrary strategy to emit
bit-6 batches so the existing batch round-trip fuzz also exercises the flag.

## Faithfulness notes

- **Byte-exact (high confidence):** bit 6 mask `0x40`; `base_timestamp` repurposing; delta
  reinterpretation; `max_timestamp` and CRC untouched; v2-only (no down-conversion). `delete.retention.ms`
  default 86,400,000.
- **Algorithm:** modeled on Kafka 3.9/4.x `LogCleaner` (`shouldRetainRecord`, `checkBatchRetention`,
  `CleanedTransactionMetadata`, retain-last-batch-per-producer).
- **Single-consolidated-output compaction** (crabka's existing design) is a faithful analogue of Kafka's
  multi-segment rounds for horizon aging (the horizon is wall-clock, not segment-relative); "last batch
  of the cleaning round" maps to "last batch of the consolidated output". Multi-round grouping is out of
  scope.
- **Legacy magic<2 horizon heuristic:** intentionally omitted (crabka is v2-only, greenfield).

## Out of scope (YAGNI)

- The produce/fetch I/O paths and the cleaner ticker's scheduling policy (only the per-record
  keep/age/delete decision + the marker/producer retention are in scope).
- Multi-segment grouped cleaning rounds.
- Compression of compacted output (orthogonal).

## Verification discipline

- Every stateright run is fenced (`within_boundary` + `target_state_count` + `timeout(from_mins(2))`)
  and executed under the host memory watchdog (kill > 3 GB / > 150 s) while bounds are tuned —
  `[[feedback_bound_model_checkers]]`. proptest runs are bounded sampling (no watchdog).
- `cargo +nightly fmt` per-crate (Windows deep-path workaround — `[[reference_windows_fmt_path_length]]`);
  `cargo clippy --all-targets -- -D warnings` clean.
- Conventional commits: `feat(protocol)!` (wire bit), `feat(log)`, `feat(broker)`, `test(log)`.

## Success criteria

1. Control batches are never key-deduped; the regression is captured first as a RED stateright witness,
   then GREEN after the fix.
2. `delete.retention.ms` honored end-to-end; tombstones and markers retained then aged out via the
   bit-6 horizon, byte-exact on the wire.
3. Full `CleanedTransactionMetadata` marker eligibility + ProducerStateManager-driven RETAIN_EMPTY, with
   the survivor `.txnindex` rebuilt so aborted-record filtering survives compaction.
4. stateright model exhaustive + green under the watchdog (basic + wide); proptest green at large N; the
   wire round-trip (incl. delete-horizon batches) green.
5. Existing compaction, txn-index, and record-batch suites pass unchanged; fmt + clippy clean.
