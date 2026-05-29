# Tiered Storage 48m — Read-committed on remote fetch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a `READ_COMMITTED` consumer fetching from the remote tier receive the same `aborted_transactions` list it would get from the local log, by consuming each remote segment's `.txnindex`.

**Architecture:** Add a pure `parse_txn_index` decoder beside the existing `parse_offset_index` / `parse_time_index` helpers in `RemoteReader`, plus a `RemoteReader::aborted_transactions` method that locates the finished segment via the same RLMM lookup `fetch_batch` uses, fetches `IndexType::Transaction`, parses it, and filters to entries overlapping the returned batch's offset range (treating a `NotFound`-class error as "no aborts"). Wire `try_remote_read` in `fetch.rs` to call it for read-committed consumer fetches and set `out.aborted_transactions = Some(...)`, mirroring the local `do_read` path.

**Tech Stack:** Rust, tokio, zerocopy, crabka workspace crates

---

## File structure

- `crates/broker/src/remote_reader.rs` (Modify) — add `AbortedTxnEntry` struct, `AbortedTxnIndexEntry` zerocopy layout, `parse_txn_index`, `txn_overlaps`, and the `RemoteReader::aborted_transactions` async method; add unit + integration tests.
- `crates/broker/src/handlers/fetch.rs` (Modify) — in `try_remote_read`, after a batch is found, compute the batch's last offset and, for `p.read_committed && !p.is_follower_fetch`, call `reader.aborted_transactions` and set `p.out.aborted_transactions = Some(...)`.

No `crates/log`, `crates/remote-storage`, config, or wire-format changes.

---

### Task 1: `AbortedTxnEntry` + `parse_txn_index` decoder

**Files:**
- `crates/broker/src/remote_reader.rs` (Modify)

Mirrors `parse_offset_index` / `parse_time_index`: a `#[repr(C)]` zerocopy layout (24 bytes BE: `start_offset` i64, `last_offset` i64, `producer_id` i64), a public-in-crate struct, and a parser that truncates trailing partial bytes.

- [ ] Add the unit test. Append it inside the existing `#[cfg(test)] mod tests` block in `crates/broker/src/remote_reader.rs` (after `parse_time_index_round_trips_known_entries`, before the integration-test section that starts with `use crabka_log::{Log, LogConfig};`):

```rust
    #[test]
    fn parse_txn_index_round_trips_known_entries() {
        // Mirror TxnIndex::append: 8B start_offset BE, 8B last_offset BE,
        // 8B producer_id BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend_from_slice(&start.to_be_bytes());
            buf.extend_from_slice(&last.to_be_bytes());
            buf.extend_from_slice(&pid.to_be_bytes());
        }
        let entries = parse_txn_index(&buf);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].start_offset, 0);
        assert_eq!(entries[0].last_offset, 4);
        assert_eq!(entries[0].producer_id, 1000);
        assert_eq!(entries[1].start_offset, 10);
        assert_eq!(entries[1].last_offset, 14);
        assert_eq!(entries[1].producer_id, 2000);
    }

    #[test]
    fn parse_txn_index_truncates_trailing_partial_bytes() {
        let mut buf = Vec::new();
        for v in [0_i64, 4, 1000] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        // 5 trailing bytes that don't complete a 24-byte entry.
        buf.extend_from_slice(&[0xAA; 5]);
        let entries = parse_txn_index(&buf);
        assert_eq!(entries.len(), 1, "partial trailing entry ignored");
        assert_eq!(entries[0].producer_id, 1000);
    }

    #[test]
    fn parse_txn_index_empty_is_empty() {
        assert!(parse_txn_index(&[]).is_empty());
    }
```

- [ ] Run the test and confirm it FAILS to compile (symbols don't exist yet):

```
cargo test -p crabka-broker parse_txn_index
```

Expected: FAIL — `cannot find function parse_txn_index in this scope` / `cannot find type AbortedTxnEntry`.

- [ ] Add the zerocopy layout. Insert after the `TimeIndexEntry` block (after the `const _: () = assert!(std::mem::size_of::<TimeIndexEntry>() == 12);` line, around line 45):

```rust
/// 24 bytes per entry: start_offset i64 BE + last_offset i64 BE + producer_id
/// i64 BE. Mirrors `crabka_log::txn_index::AbortedTxnRaw` so the remote-tier
/// copy of a `.txnindex` file decodes through the same byte layout the local
/// index was written with.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AbortedTxnIndexEntry {
    start_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    producer_id: I64<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<AbortedTxnIndexEntry>() == 24);
```

- [ ] Add the public-in-crate `AbortedTxnEntry` struct. Insert immediately after the `AbortedTxnIndexEntry` const-assert above:

```rust
/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) producer_id: i64,
}
```

- [ ] Add the parser. Insert after `parse_time_index` (after its closing brace, around line 252, before `relative_offset_for_timestamp`):

```rust
/// Parse Kafka's transaction-index format (24 bytes / entry: start_offset i64
/// BE, last_offset i64 BE, producer_id i64 BE). Trailing bytes that don't
/// complete a 24-byte entry are ignored.
#[must_use]
pub(crate) fn parse_txn_index(bytes: &[u8]) -> Vec<AbortedTxnEntry> {
    let truncated_len = (bytes.len() / 24) * 24;
    let entries = <[AbortedTxnIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .expect("len is multiple of 24 and AbortedTxnIndexEntry is Unaligned");
    entries
        .iter()
        .map(|e| AbortedTxnEntry {
            start_offset: e.start_offset.get(),
            last_offset: e.last_offset.get(),
            producer_id: e.producer_id.get(),
        })
        .collect()
}
```

- [ ] Run the test and confirm it PASSES:

```
cargo test -p crabka-broker parse_txn_index
```

Expected: PASS — 3 tests (`parse_txn_index_round_trips_known_entries`, `parse_txn_index_truncates_trailing_partial_bytes`, `parse_txn_index_empty_is_empty`).

- [ ] Format, then commit:

```
cargo fmt
git add crates/broker/src/remote_reader.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(remote-reader): parse_txn_index + AbortedTxnEntry"
```

---

### Task 2: overlap filter `txn_overlaps`

**Files:**
- `crates/broker/src/remote_reader.rs` (Modify)

A pure boundary check mirroring `TxnIndex::aborted_in_range`'s overlap test, but expressed against an **inclusive** `[from_offset, to_offset]` range (the design doc's `start_offset <= to_offset && last_offset >= from_offset`), since `aborted_transactions` is called with the batch's inclusive last offset.

- [ ] Add the unit test. Insert it in the `#[cfg(test)] mod tests` block right after the `parse_txn_index_empty_is_empty` test added in Task 1:

```rust
    #[test]
    fn txn_overlaps_boundaries() {
        let e = AbortedTxnEntry {
            start_offset: 10,
            last_offset: 14,
            producer_id: 1,
        };
        // Range fully before the entry → excluded.
        assert!(!txn_overlaps(&e, 0, 9), "range ends just before entry");
        // Range touching the entry's first offset → included.
        assert!(txn_overlaps(&e, 0, 10), "range ends on entry start");
        // Range fully inside the entry → included.
        assert!(txn_overlaps(&e, 11, 13), "range inside entry");
        // Range touching the entry's last offset → included.
        assert!(txn_overlaps(&e, 14, 100), "range starts on entry last");
        // Range fully after the entry → excluded.
        assert!(!txn_overlaps(&e, 15, 100), "range starts just after entry");
        // Range fully covering the entry → included.
        assert!(txn_overlaps(&e, 0, 100), "range covers entry");
    }
```

- [ ] Run the test and confirm it FAILS to compile:

```
cargo test -p crabka-broker txn_overlaps_boundaries
```

Expected: FAIL — `cannot find function txn_overlaps in this scope`.

- [ ] Add the function. Insert immediately after `parse_txn_index` (before `relative_offset_for_timestamp`):

```rust
/// Whether an aborted-transaction entry overlaps the inclusive offset range
/// `[from_offset, to_offset]`. Mirrors `TxnIndex::aborted_in_range`'s overlap
/// test against an inclusive range: the entry's `[start, last]` intersects
/// `[from, to]` iff `start <= to && last >= from`.
#[must_use]
pub(crate) fn txn_overlaps(entry: &AbortedTxnEntry, from_offset: i64, to_offset: i64) -> bool {
    entry.start_offset <= to_offset && entry.last_offset >= from_offset
}
```

- [ ] Run the test and confirm it PASSES:

```
cargo test -p crabka-broker txn_overlaps_boundaries
```

Expected: PASS — 1 test.

- [ ] Format, then commit:

```
cargo fmt
git add crates/broker/src/remote_reader.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(remote-reader): txn_overlaps inclusive-range filter"
```

---

### Task 3: `RemoteReader::aborted_transactions` method

**Files:**
- `crates/broker/src/remote_reader.rs` (Modify)

Locates the finished segment covering `from_offset` via the same `rlmm.remote_log_segment_metadata` lookup `fetch_batch` uses, fetches `IndexType::Transaction`, parses it, and returns overlapping entries. A `NotFound`-class fetch error (the index is optional) yields an empty Vec. No covering finished segment yields an empty Vec.

The integration test for this method goes through the full RSM/RLMM plumbing and is written in Task 4 (it needs the harness extension). This task adds the method plus a focused unit test that exercises the empty-on-missing-segment path without I/O.

- [ ] Add a unit test for the no-segment path. Insert in the `#[cfg(test)] mod tests` block within the integration-test section (after `fetch_batch_returns_none_when_segment_not_in_rlmm`, which already constructs an empty RSM/RLMM reader):

```rust
    #[tokio::test]
    async fn aborted_transactions_empty_when_no_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        // RLMM is empty → no covering segment → empty list, not an error.
        let got = reader
            .aborted_transactions(&tp(), 0, 0, 100)
            .await
            .expect("ok");
        assert!(got.is_empty());
    }
```

- [ ] Run the test and confirm it FAILS to compile:

```
cargo test -p crabka-broker aborted_transactions_empty_when_no_segment
```

Expected: FAIL — `no method named aborted_transactions found for struct RemoteReader`.

- [ ] Add the method. Insert in `impl RemoteReader` immediately after `fetch_batch` (after its closing brace at ~line 105, before `earliest_offset`):

```rust
    /// Aborted transactions overlapping the inclusive offset range
    /// `[from_offset, to_offset]` in the finished remote segment covering
    /// `from_offset`. Returns an empty `Vec` when no finished segment covers
    /// the offset, when the segment carries no transaction index
    /// (`SegmentNotFound` from `fetch_index`), or when nothing overlaps.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: i32,
        from_offset: i64,
        to_offset: i64,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        let Some(metadata) = self
            .rlmm
            .remote_log_segment_metadata(tp, leader_epoch, from_offset)?
        else {
            return Ok(Vec::new());
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(Vec::new());
        }

        let index_bytes = match self
            .fetch_index_blocking(metadata, IndexType::Transaction)
            .await
        {
            Ok(bytes) => bytes,
            // The transaction index is optional: a segment with no aborted
            // transactions has no `.txnindex`, surfaced as SegmentNotFound.
            Err(RemoteStorageError::SegmentNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let entries = parse_txn_index(&index_bytes);
        Ok(entries
            .into_iter()
            .filter(|e| txn_overlaps(e, from_offset, to_offset))
            .collect())
    }
```

- [ ] Run the test and confirm it PASSES:

```
cargo test -p crabka-broker aborted_transactions_empty_when_no_segment
```

Expected: PASS — 1 test.

- [ ] Format, then commit:

```
cargo fmt
git add crates/broker/src/remote_reader.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(remote-reader): aborted_transactions segment lookup + filter"
```

---

### Task 4: integration test through `LocalTieredStorage`

**Files:**
- `crates/broker/src/remote_reader.rs` (Test)

Mirrors the existing `populated_reader` harness but writes a real `.txnindex` file next to the first sealed segment so the copy path carries it to the remote tier. Asserts: read-committed remote read returns the abort; the same reader with no `.txnindex` returns an empty (but `Some`-shaped) list; an absent index → empty.

Because the `RemoteReader` API returns `Vec<AbortedTxnEntry>` (the `Some(...)` wrapping happens in `fetch.rs`, covered by reading-code review in Task 5), the integration assertions are: non-empty list when a `.txnindex` exists and overlaps; empty list when the segment has no `.txnindex`.

- [ ] Add a harness variant that injects a `.txnindex`. Insert in the integration-test section, immediately after the existing `populated_reader` function (after its closing brace at ~line 533):

```rust
    /// Like `populated_reader`, but before copying, writes a single aborted-txn
    /// entry into the first sealed segment's `.txnindex` (24 BE bytes:
    /// start_offset, last_offset, producer_id) so the copy path carries it to
    /// the remote tier. Returns the reader, the log, and the
    /// `(start_offset, last_offset, producer_id)` written.
    fn populated_reader_with_abort(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log, (i64, i64, i64)) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_bytes: 256,
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch_of(2, 64);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        // Write a `.txnindex` next to the first sealed segment's `.log` so the
        // export below picks it up. The abort covers the whole first segment.
        let first = &exports[0];
        let abort = (first.base_offset, first.last_offset, 7777_i64);
        let mut txn_bytes = Vec::new();
        txn_bytes.extend_from_slice(&abort.0.to_be_bytes());
        txn_bytes.extend_from_slice(&abort.1.to_be_bytes());
        txn_bytes.extend_from_slice(&abort.2.to_be_bytes());
        let txn_path = first.log_path.with_extension("txnindex");
        std::fs::write(&txn_path, &txn_bytes).unwrap();

        // Re-derive exports so the first one now carries the txnindex path.
        let exports = log.tierable_segments();
        assert!(
            exports[0].transaction_index_path.is_some(),
            "first segment must now carry a .txnindex"
        );

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        for ex in &exports {
            let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
            let epochs: BTreeMap<i32, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(0, ex.base_offset)])
            } else {
                ex.leader_epochs.iter().copied().collect()
            };
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
                ex.base_offset,
                ex.last_offset,
                ex.max_timestamp,
                1,
                ex.max_timestamp,
                i32::try_from(ex.size_bytes).unwrap_or(i32::MAX),
                RemoteLogSegmentState::CopySegmentStarted,
                epochs.clone(),
            )
            .unwrap();
            rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
            let mut s = String::from("0\n");
            let _ = writeln!(s, "{}", epochs.len());
            for (e, st) in &epochs {
                let _ = writeln!(s, "{e} {st}");
            }
            let data = crabka_remote_storage::LogSegmentData {
                log_segment: ex.log_path.clone(),
                offset_index: ex.offset_index_path.clone(),
                time_index: ex.time_index_path.clone(),
                transaction_index: ex.transaction_index_path.clone(),
                producer_snapshot_index: None,
                leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
            };
            rsm.copy_log_segment_data(&md, &data).unwrap();
            rlmm.update_remote_log_segment_metadata(
                crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                    remote_log_segment_id: id,
                    event_timestamp_ms: ex.max_timestamp,
                    custom_metadata: None,
                    state: RemoteLogSegmentState::CopySegmentFinished,
                    broker_id: 1,
                },
            )
            .unwrap();
        }

        (RemoteReader::new(rsm, rlmm), log, abort)
    }
```

- [ ] Add the integration test. Insert immediately after the harness variant above:

```rust
    #[tokio::test]
    async fn aborted_transactions_returns_copied_abort() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log, abort) = populated_reader_with_abort(log_dir.path(), remote_dir.path());
        let (start, last, pid) = abort;

        // Query the first segment's offset range → the abort overlaps.
        let got = reader
            .aborted_transactions(&tp(), 0, start, last)
            .await
            .expect("ok");
        assert_eq!(got.len(), 1, "the copied abort is returned");
        assert_eq!(got[0].start_offset, start);
        assert_eq!(got[0].last_offset, last);
        assert_eq!(got[0].producer_id, pid);
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_segment_has_no_txnindex() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        // The default harness writes no `.txnindex` for any segment.
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        let seg = &exports[0];

        let got = reader
            .aborted_transactions(&tp(), 0, seg.base_offset, seg.last_offset)
            .await
            .expect("ok");
        assert!(
            got.is_empty(),
            "segment with no .txnindex yields an empty list, not an error"
        );
    }
```

- [ ] Run the integration tests and confirm they PASS:

```
cargo test -p crabka-broker aborted_transactions_returns_copied_abort aborted_transactions_empty_when_segment_has_no_txnindex
```

Expected: PASS — 2 tests. (The `LocalTieredStorage` copy carries the `.txnindex`; `fetch_index(Transaction)` returns its bytes for the first segment and `SegmentNotFound` for segments without one, which the method maps to an empty list.)

- [ ] Format, then commit:

```
cargo fmt
git add crates/broker/src/remote_reader.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(remote-reader): aborted_transactions integration through LocalTieredStorage"
```

---

### Task 5: wire `try_remote_read` in `fetch.rs`

**Files:**
- `crates/broker/src/handlers/fetch.rs` (Modify)

After `fetch_batch` returns a batch, compute the batch's inclusive last offset and, when `p.read_committed && !p.is_follower_fetch`, call `reader.aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)` and set `p.out.aborted_transactions = Some(...)`. `Some(empty)` is the correct read-committed signal (matches the local `do_read` path and Apache Kafka). Read-uncommitted leaves it `None`. `AbortedTransaction`, `tp`, and `leader_epoch` are all already in scope inside `try_remote_read`.

The behavior here is verified by reading-code review against the local path at `fetch.rs:934`; there is no new unit test in this task. `try_remote_read` takes a live `&Broker` + `&Partition` with a tiered log, which has no focused test harness in this module, so the three observable properties are covered as follows:

- read-committed remote fetch returns the abort → the data path (`aborted_transactions` returning the copied entry through real RSM/RLMM plumbing) is covered by `aborted_transactions_returns_copied_abort` in Task 4.
- missing `.txnindex` → `Some(empty)` → the empty-list data path is covered by `aborted_transactions_empty_when_segment_has_no_txnindex` in Task 4; the `Some(...)` wrapping is the `if`-guard added below.
- read-uncommitted → `None` → guaranteed by the `if p.read_committed && !p.is_follower_fetch` guard below, which mirrors the identical guard at `fetch.rs:963`; when it's false, `p.out.aborted_transactions` is left at whatever `do_read` set (`None` for read-uncommitted).

The integration of all three through the live handler is additionally exercised by the existing broker fetch tests run in Task 6.

- [ ] Read the current `try_remote_read` `Ok(Some(batch))` arm (around `crates/broker/src/handlers/fetch.rs:1010`) to confirm the surrounding bindings (`reader`, `tp`, `leader_epoch`, `p`) before editing.

- [ ] Replace the `Ok(Some(batch)) => { ... }` arm. The current arm is:

```rust
        Ok(Some(batch)) => {
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            p.out.error_code = codes::NONE;
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.
            p.out.records = Some(batch.into());
            Some(bytes_est)
        }
```

Replace it with:

```rust
        Ok(Some(batch)) => {
            let bytes_est = <RecordBatch as Encode>::encoded_len(&batch, 0);
            p.out.error_code = codes::NONE;
            // `log_start_offset` / HW / LSO stay at whatever `do_read`
            // wrote out (the local view); the remote tier doesn't change
            // those pointers.

            // KIP-405 read-committed: surface the aborted-transaction list
            // from the segment's `.txnindex` so the consumer drops aborted
            // records client-side, exactly as the local path does at the
            // `aborted_in_range` call in `do_read`. `Some(empty)` is the
            // correct read-committed signal (read-uncommitted leaves it
            // `None`). The batch's inclusive last offset bounds the query.
            if p.read_committed && !p.is_follower_fetch {
                let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
                let aborts = reader
                    .aborted_transactions(&tp, leader_epoch, p.fetch_offset, batch_last_offset)
                    .await
                    .unwrap_or_default();
                p.out.aborted_transactions = Some(
                    aborts
                        .into_iter()
                        .map(|e| AbortedTransaction {
                            producer_id: e.producer_id,
                            first_offset: e.start_offset,
                            ..Default::default()
                        })
                        .collect(),
                );
            }

            p.out.records = Some(batch.into());
            Some(bytes_est)
        }
```

- [ ] Confirm the crate builds (this verifies `AbortedTransaction` is in scope — it is imported at the top of `fetch.rs` — and that `batch` is still owned when `batch.into()` runs, since the abort computation only reads `base_offset` / `last_offset_delta` by value before the move):

```
cargo build -p crabka-broker
```

Expected: PASS — no errors.

- [ ] Run the existing fetch handler tests to confirm no regression:

```
cargo test -p crabka-broker --lib handlers::fetch
```

Expected: PASS — existing fetch tests unchanged.

- [ ] Format, then commit:

```
cargo fmt
git add crates/broker/src/handlers/fetch.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(fetch): populate aborted_transactions on remote read-committed fetch"
```

---

### Task 6: verification gates

**Files:** none (verification only)

- [ ] Run formatting check:

```
cargo fmt --all --check
```

Expected: PASS — no diff. If it fails, run `cargo fmt` and amend the relevant commit, then re-run.

- [ ] Run clippy across the workspace:

```
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: PASS — no warnings.

- [ ] Run the broker crate tests:

```
cargo test -p crabka-broker
```

Expected: PASS — including the four new `parse_txn_index*`, `txn_overlaps_boundaries`, `aborted_transactions_empty_when_no_segment`, `aborted_transactions_returns_copied_abort`, and `aborted_transactions_empty_when_segment_has_no_txnindex` tests.

- [ ] Run the full workspace test suite to confirm no regressions:

```
cargo test --workspace
```

Expected: PASS — no regressions.

- [ ] If any gate produced changes, format and commit them:

```
cargo fmt
git add -A
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "chore(48m): verification fixups"
```
