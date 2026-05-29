# Decode-Free Pass-Through Fetch Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve Fetch responses by forwarding verbatim on-disk record-batch bytes (no decode, no re-encode), returning multiple batches per partition, while keeping replication and consumers correct.

**Architecture:** A header-only scan walks v2 batch boundaries (`base_offset`, `batch_length`, `last_offset_delta`) to compute a contiguous byte range of whole, in-window batches, returned as a `bytes::Bytes` slice. `RecordsPayload` gains a `Raw(Bytes)` pass-through variant and its `V2` arm becomes multi-batch (`Vec<RecordBatch>`). The broker fetch path emits `Raw`; consumers, the replicator, the down-converter, and produce iterate the multi-batch form.

**Tech Stack:** Rust, `bytes`, `zerocopy` (`RecordBatchHeader`), tokio. Spec: `docs/superpowers/specs/2026-05-28-crabka-fetch-decode-free-passthrough-design.md`.

---

## Reference facts (do not re-derive)

- v2 batch on-disk layout: `base_offset:i64(8) | batch_length:i32(4) | partition_leader_epoch:i32(4) | magic:i8(1) | crc:u32(4) | attributes:i16(2) | last_offset_delta:i32(4) | base_timestamp:i64 | max_timestamp:i64 | producer_id:i64 | producer_epoch:i16 | base_sequence:i32 | records_count:i32 | records[]`.
- **Batch stride on disk = `12 + batch_length`** (8 for `base_offset` + 4 for the `batch_length` field + the `batch_length` bytes that follow). The fixed header is `HEADER_LEN = 61` bytes.
- `crabka_protocol::records` re-exports `RecordBatchHeader`, `HEADER_LEN`, `RecordBatch`, `RecordsPayload`, `RecordsPayloadBorrowed`, `RecordBatchBorrowed`, `RecordsError`.
- `RecordBatchHeader::ref_from_bytes(&slice_of_exactly_HEADER_LEN)` (zerocopy) yields `&RecordBatchHeader`; fields read via `.base_offset.get()` etc.
- `LogError::Corrupt(String)` exists; use it for malformed headers.
- The full set of `as_v2()` call sites to migrate: `crates/client-consumer/src/poll.rs:104`, `crates/broker/src/replicator.rs:314`, `crates/broker/src/handlers/produce.rs:187` and `:331`, `crates/broker/src/handlers/fetch.rs:426`.

## Crate compile coupling (important for batching)

Changing `RecordsPayload` (Task 1) breaks every `as_v2()` caller until updated.
- `crabka-protocol` compiles in isolation after Task 1 (its own unit tests updated).
- `crabka-log` (Task 2) does **not** use `RecordsPayload` — fully independent of Task 1.
- `crabka-client-consumer` (Task 3) depends only on `crabka-protocol`.
- `crabka-broker` (Task 4) needs **all four** of its touched files coherent to compile; its checkpoint build runs after the whole task.

**Batches (per CLAUDE.md, parallel where file sets are disjoint):**
- **Batch 1 (parallel):** Task 1 (`crabka-protocol`) + Task 2 (`crabka-log`).
- **Batch 2 (parallel):** Task 3 (`crabka-client-consumer`) + Task 4 (`crabka-broker`). Both require Batch 1.
- **Batch 3:** Task 5 (workspace integration tests + verification).

## File Structure

- `crates/protocol/src/records/payload.rs` — **modify.** `RecordsPayload`: `V2(Vec<RecordBatch>)` + new `Raw(Bytes)`; multi-batch `from_bytes`; `as_v2() -> Option<&[RecordBatch]>`. Mirror `RecordsPayloadBorrowed` (multi-batch `V2`, no `Raw`).
- `crates/log/src/segment.rs` — **modify.** Add `RawSegmentRead` + `Segment::read_raw`.
- `crates/log/src/log.rs` — **modify.** Add `RawRead` + `Log::read_raw` (segment-spanning).
- `crates/client-consumer/src/poll.rs` — **modify.** Iterate all batches.
- `crates/broker/src/replicator.rs` — **modify.** Replicate all batches.
- `crates/broker/src/handlers/produce.rs` — **modify.** Multi-batch metrics + single-batch extraction from `Vec`.
- `crates/broker/src/handlers/fetch_downconvert.rs` — **modify.** Add `down_convert_payload_for_fetch`.
- `crates/broker/src/handlers/fetch.rs` — **modify.** `do_read` uses `Log::read_raw` → `Raw`; remove server-side batch filtering; update the v<4 down-convert call site.
- `crates/broker/tests/` — **add** an integration test for multi-batch round-trip + read_committed raw pass-through.

---

## Task 1: `RecordsPayload` multi-batch + `Raw` variant (crate `crabka-protocol`)

**Files:**
- Modify: `crates/protocol/src/records/payload.rs`

- [ ] **Step 1: Update the failing unit tests in `payload.rs` to the new shape**

Replace the existing `from_bytes_dispatches_v2`, `roundtrip_v2`, `encode_decode_via_traits`, `borrowed_dispatches`, `from_record_batch`, and `owned_default_is_empty_v2` tests' expectations to match `V2(Vec<RecordBatch>)`, and add a new multi-batch test. Concretely, add this test and adjust the `V2(parsed)` match arms in existing tests to bind a slice:

```rust
#[test]
fn from_bytes_parses_all_batches() {
    // Two v2 batches concatenated must both decode.
    let mut b0 = sample_v2();
    b0.base_offset = 0;
    let mut b1 = sample_v2();
    b1.base_offset = 1;
    let mut buf = BytesMut::new();
    b0.encode(&mut buf).unwrap();
    b1.encode(&mut buf).unwrap();
    let p = RecordsPayload::from_bytes(buf.freeze()).unwrap();
    let batches = p.as_v2().expect("v2");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].base_offset, 0);
    assert_eq!(batches[1].base_offset, 1);
}

#[test]
fn raw_passthrough_roundtrips() {
    let mut b = sample_v2();
    b.base_offset = 7;
    let mut wire = BytesMut::new();
    b.encode(&mut wire).unwrap();
    let wire = wire.freeze();
    let p = RecordsPayload::Raw(wire.clone());
    assert_eq!(p.payload_len(), wire.len());
    let mut out = BytesMut::new();
    p.encode_to(&mut out).unwrap();
    assert_eq!(&out[..], &wire[..]); // verbatim
    assert!(p.as_v2().is_none());    // Raw is unparsed
}
```

In the existing tests that did `RecordsPayload::V2(parsed) => assert_eq!(parsed, rb)`, change to:
```rust
RecordsPayload::V2(batches) => assert_eq!(batches, vec![rb]),
RecordsPayload::Raw(_) => panic!("expected V2"),
RecordsPayload::Legacy(_) => panic!("expected V2"),
```
and in `owned_default_is_empty_v2` assert the default is an **empty** `V2`:
```rust
assert!(matches!(p, RecordsPayload::V2(ref v) if v.is_empty()));
```

- [ ] **Step 2: Run tests to verify they fail to compile / fail**

Run: `cargo test -p crabka-protocol records::payload 2>&1 | tail -20`
Expected: compile errors (`V2` arity / `as_v2` type mismatch) or assertion failures.

- [ ] **Step 3: Rewrite the owned `RecordsPayload`**

Replace the `enum RecordsPayload` and its `impl` block (the owned half, roughly lines 25–123) with:

```rust
/// Owned form of a records-field payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordsPayload {
    /// Zero or more parsed v2 batches (the records field is a *sequence*).
    V2(Vec<RecordBatch>),
    /// Verbatim, already-wire-format v2 bytes (one or more batches),
    /// forwarded without parsing. Produced by the fetch pass-through path.
    Raw(Bytes),
    /// Opaque pre-v2 bytes (v0/v1 `MessageSet`). Decode with
    /// `crabka_records_legacy::decode_message_set`.
    Legacy(Bytes),
}

impl RecordsPayload {
    /// Construct from raw records-field bytes. When the bytes look like v2,
    /// decode *every* batch in the field; otherwise keep as opaque legacy.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, RecordsError> {
        if looks_like_v2(&bytes) {
            let mut cur: &[u8] = &bytes;
            let mut batches = Vec::new();
            while !cur.is_empty() {
                batches.push(RecordBatch::decode(&mut cur)?);
            }
            Ok(Self::V2(batches))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    /// Wire size of the records-field bytes (no outer length prefix).
    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(batches) => batches.iter().map(RecordBatch::encoded_len).sum(),
            Self::Raw(b) | Self::Legacy(b) => b.len(),
        }
    }

    /// Write the payload bytes into `buf` (caller owns the outer framing).
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(batches) => {
                for b in batches {
                    b.encode(buf)?;
                }
                Ok(())
            }
            Self::Raw(b) | Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
        }
    }

    /// Borrow the parsed v2 batches, if this is a parsed `V2` payload.
    /// Returns `None` for `Raw` (intentionally unparsed) and `Legacy`.
    #[must_use]
    pub fn as_v2(&self) -> Option<&[RecordBatch]> {
        match self {
            Self::V2(batches) => Some(batches),
            Self::Raw(_) | Self::Legacy(_) => None,
        }
    }

    /// Borrow as raw legacy bytes, if that's what this payload is.
    #[must_use]
    pub fn as_legacy(&self) -> Option<&Bytes> {
        match self {
            Self::Legacy(b) => Some(b),
            Self::V2(_) | Self::Raw(_) => None,
        }
    }
}

impl From<RecordBatch> for RecordsPayload {
    fn from(rb: RecordBatch) -> Self {
        Self::V2(vec![rb])
    }
}

impl From<Vec<RecordBatch>> for RecordsPayload {
    fn from(v: Vec<RecordBatch>) -> Self {
        Self::V2(v)
    }
}

impl Default for RecordsPayload {
    fn default() -> Self {
        Self::V2(Vec::new())
    }
}
```

Leave the `Encode`/`Decode` trait impls (they delegate to `encode_to`/`from_bytes`) unchanged.

- [ ] **Step 4: Rewrite the borrowed `RecordsPayloadBorrowed` (multi-batch, no `Raw`)**

Replace the borrowed enum + impl (`from_slice`, `payload_len`, `encode_to`, `to_owned`, `Default`):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordsPayloadBorrowed<'a> {
    V2(Vec<RecordBatchBorrowed<'a>>),
    Legacy(&'a [u8]),
}

impl<'a> RecordsPayloadBorrowed<'a> {
    pub fn from_slice(bytes: &'a [u8]) -> Result<Self, RecordsError> {
        if looks_like_v2(bytes) {
            let mut cur: &'a [u8] = bytes;
            let mut batches = Vec::new();
            while !cur.is_empty() {
                let rb = <RecordBatchBorrowed<'a> as crate::DecodeBorrow<'a>>::decode_borrow(&mut cur, 0)
                    .map_err(|e| RecordsError::RecordParse(format!("borrowed v2 decode: {e}")))?;
                batches.push(rb);
            }
            Ok(Self::V2(batches))
        } else {
            Ok(Self::Legacy(bytes))
        }
    }

    #[must_use]
    pub fn payload_len(&self) -> usize {
        match self {
            Self::V2(batches) => batches.iter().map(|rb| crate::Encode::encoded_len(rb, 0)).sum(),
            Self::Legacy(b) => b.len(),
        }
    }

    pub fn encode_to<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        match self {
            Self::V2(batches) => {
                for rb in batches {
                    crate::Encode::encode(rb, buf, 0)
                        .map_err(|e| RecordsError::RecordParse(format!("borrowed v2 encode: {e}")))?;
                }
                Ok(())
            }
            Self::Legacy(b) => {
                buf.put_slice(b);
                Ok(())
            }
        }
    }

    pub fn to_owned(&self) -> Result<RecordsPayload, RecordsError> {
        match self {
            Self::V2(batches) => {
                let mut owned = Vec::with_capacity(batches.len());
                for rb in batches {
                    owned.push(rb.to_owned()?);
                }
                Ok(RecordsPayload::V2(owned))
            }
            Self::Legacy(b) => Ok(RecordsPayload::Legacy(Bytes::copy_from_slice(b))),
        }
    }
}

impl Default for RecordsPayloadBorrowed<'_> {
    fn default() -> Self {
        Self::V2(Vec::new())
    }
}
```

Update the borrowed tests (`borrowed_dispatches`, `borrowed_v2_payload_len_and_encode`, `borrowed_encode_decode_via_traits`, `borrowed_default_is_empty_v2`) to match `V2(Vec)` (e.g. `assert!(matches!(p, RecordsPayloadBorrowed::V2(ref v) if v.len() == 1))` and bind the owned result via `RecordsPayload::V2(batches) => assert_eq!(batches[0].base_offset, 42)`).

- [ ] **Step 5: Run protocol tests**

Run: `cargo test -p crabka-protocol 2>&1 | tail -25`
Expected: PASS (all payload + records tests green).

- [ ] **Step 6: Commit**

```bash
git add crates/protocol/src/records/payload.rs
git commit -m "feat(protocol): multi-batch RecordsPayload + Raw pass-through variant"
```

---

## Task 2: `read_raw` header-scan in the log crate (crate `crabka-log`)

**Files:**
- Modify: `crates/log/src/segment.rs`
- Modify: `crates/log/src/log.rs`

This task is independent of Task 1 (the log crate does not use `RecordsPayload`).

- [ ] **Step 1: Write failing segment tests**

Append to the `#[cfg(test)] mod tests` in `crates/log/src/segment.rs` (use the file's existing batch/segment test helpers — find how other tests build batches and a temp segment; mirror them). Add:

```rust
#[test]
fn read_raw_is_byte_exact_and_multi_batch() {
    // Build a segment with 3 single-record batches at offsets 0,1,2.
    let (dir, mut seg) = test_segment(); // existing helper that returns (TempDir, Segment)
    let mut wire = bytes::BytesMut::new();
    for off in 0..3i64 {
        let mut b = test_batch_at(off); // existing helper: one record, base_offset = off
        seg.append(&b, 0).unwrap();
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();

    // Read everything below limit_offset = 3 with a generous budget.
    let r = seg.read_raw(0, 3, 10 * 1024 * 1024).unwrap();
    assert_eq!(r.start_offset, 0);
    assert_eq!(r.last_offset, 2);
    assert_eq!(&r.bytes[..], &wire[..], "raw bytes must equal the on-disk concatenation");

    // The raw bytes decode back to 3 batches.
    let mut cur: &[u8] = &r.bytes;
    let mut n = 0;
    while !cur.is_empty() {
        crabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        n += 1;
    }
    assert_eq!(n, 3);
    drop(dir);
}

#[test]
fn read_raw_clamps_at_limit_offset() {
    let (dir, mut seg) = test_segment();
    for off in 0..3i64 {
        seg.append(&test_batch_at(off), 0).unwrap();
    }
    // limit_offset = 2 ⇒ only batches with base_offset < 2 (offsets 0,1).
    let r = seg.read_raw(0, 2, 10 * 1024 * 1024).unwrap();
    assert_eq!(r.last_offset, 1);
    drop(dir);
}

#[test]
fn read_raw_returns_at_least_one_batch_over_budget() {
    let (dir, mut seg) = test_segment();
    seg.append(&test_batch_at(0), 0).unwrap();
    // max_bytes = 1 (tiny) must still yield the single batch.
    let r = seg.read_raw(0, 1, 1).unwrap();
    assert_eq!(r.start_offset, 0);
    assert_eq!(r.last_offset, 0);
    assert!(!r.bytes.is_empty());
    drop(dir);
}
```

If `test_segment`/`test_batch_at` helpers do not already exist in the test module, write minimal local helpers: create a temp dir with `tempfile::TempDir`, `Segment::create(dir.path(), 0)`, and build a `RecordBatch { base_offset: off, records: vec![Record{ value: Some(Bytes::from_static(b"v")), ..Default::default() }], ..Default::default() }`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p crabka-log read_raw 2>&1 | tail -20`
Expected: FAIL — `read_raw` / `RawSegmentRead` not found.

- [ ] **Step 3: Implement `RawSegmentRead` + `Segment::read_raw`**

Add near the top of `crates/log/src/segment.rs` (imports) — `RecordBatchHeader`, `HEADER_LEN` are already reachable via `crabka_protocol::records`:

```rust
use bytes::Bytes;
use crabka_protocol::records::{RecordBatchHeader, HEADER_LEN};
```

Add the result type (module level):

```rust
/// Verbatim, decode-free output of [`Segment::read_raw`].
#[derive(Debug, Clone)]
pub struct RawSegmentRead {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: i64,
    /// Last absolute offset covered by `bytes` (`start_offset - 1` if empty).
    pub last_offset: i64,
    /// Verbatim `.log` bytes — one or more complete v2 batches.
    pub bytes: Bytes,
}

impl RawSegmentRead {
    fn empty() -> Self {
        Self { start_offset: 0, last_offset: -1, bytes: Bytes::new() }
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}
```

Add the method in `impl Segment`:

```rust
/// Read a contiguous run of **complete, verbatim** record-batch bytes
/// beginning at the batch containing `fetch_offset`, including only
/// batches whose `base_offset < limit_offset`, up to roughly `max_bytes`
/// (always at least one batch — Kafka's anti-stall rule). No record
/// decoding: only fixed batch headers are read to find boundaries.
pub fn read_raw(
    &self,
    fetch_offset: i64,
    limit_offset: i64,
    max_bytes: usize,
) -> Result<RawSegmentRead, LogError> {
    if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
        return Ok(RawSegmentRead::empty());
    }
    let target_rel = u32::try_from((fetch_offset - self.base_offset).max(0))
        .map_err(|_| LogError::BadSegmentName("target offset out of range".into()))?;
    let start_pos = u64::from(self.offset_index.lookup(target_rel));

    let first_read = max_bytes.max(HEADER_LEN);
    let mut buf: Vec<u8> = Vec::with_capacity(first_read.min(4 * 1024 * 1024));
    self.read_log_range(start_pos, &mut buf, first_read)?;

    let mut pos = 0usize;
    let mut range_start: Option<usize> = None;
    let mut range_end = 0usize;
    let mut start_offset = fetch_offset;
    let mut last_offset = fetch_offset - 1;

    loop {
        if pos + HEADER_LEN > buf.len() {
            break;
        }
        let hdr = RecordBatchHeader::ref_from_bytes(&buf[pos..pos + HEADER_LEN])
            .map_err(|_| LogError::Corrupt("record batch header".into()))?;
        let base = hdr.base_offset.get();
        let batch_len = usize::try_from(hdr.batch_length.get().max(0)).unwrap_or(0);
        let total = 12 + batch_len;
        let batch_last = base + i64::from(hdr.last_offset_delta.get());

        if batch_last < fetch_offset {
            pos += total; // leading batch entirely before the requested offset
            continue;
        }
        if base >= limit_offset {
            break; // outside the visible window (HW / LSO / LEO)
        }
        if pos + total > buf.len() {
            // First matching batch's body is truncated by the budget read.
            // Honor at-least-one-batch by reading exactly this batch.
            if range_start.is_none() {
                let mut one: Vec<u8> = Vec::with_capacity(total);
                self.read_log_range(start_pos + pos as u64, &mut one, total)?;
                if one.len() < total {
                    break; // genuinely partial on disk — exclude.
                }
                return Ok(RawSegmentRead {
                    start_offset: base,
                    last_offset: batch_last,
                    bytes: Bytes::from(one),
                });
            }
            break; // already have ≥1 batch; stop before the truncated one.
        }

        if range_start.is_none() {
            range_start = Some(pos);
            start_offset = base;
        }
        range_end = pos + total;
        last_offset = batch_last;
        pos += total;

        if range_end - range_start.expect("set above") >= max_bytes {
            break;
        }
    }

    match range_start {
        Some(s) => Ok(RawSegmentRead {
            start_offset,
            last_offset,
            bytes: Bytes::from(buf).slice(s..range_end),
        }),
        None => Ok(RawSegmentRead::empty()),
    }
}
```

- [ ] **Step 4: Run segment tests**

Run: `cargo test -p crabka-log read_raw 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Write failing `Log::read_raw` test**

In `crates/log/src/log.rs` test module, add (use the existing `Log` test harness in that file — find how other `log.read` tests construct a `Log` and append batches; mirror it):

```rust
#[test]
fn log_read_raw_spans_and_is_byte_exact() {
    let (dir, mut log) = test_log(); // existing helper pattern
    let mut wire = bytes::BytesMut::new();
    for off in 0..4i64 {
        let mut b = test_batch_at(off);
        log.append(&mut b).unwrap();
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();
    let log_end = log.log_end_offset();

    let r = log.read_raw(0, log_end, 10 * 1024 * 1024).unwrap();
    assert_eq!(r.start_offset, 0);
    assert_eq!(r.total, wire.len());
    assert_eq!(&r.bytes[..], &wire[..]);
    drop(dir);
}
```

- [ ] **Step 6: Run to verify failure**

Run: `cargo test -p crabka-log log_read_raw 2>&1 | tail -20`
Expected: FAIL — `read_raw`/`RawRead` not found on `Log`.

- [ ] **Step 7: Implement `RawRead` + `Log::read_raw`**

Add to `crates/log/src/log.rs` (it already imports `Segment`; ensure `RawSegmentRead` is imported, e.g. `use crate::segment::{Segment, RawSegmentRead};` — adjust to the file's existing import style; add `use bytes::{Bytes, BytesMut};` and `use crabka_protocol::records::HEADER_LEN;` if not present):

```rust
/// Verbatim, decode-free output of [`Log::read_raw`].
#[derive(Debug, Clone)]
pub struct RawRead {
    pub start_offset: i64,
    pub bytes: Bytes,
    pub total: usize,
}

impl RawRead {
    fn empty(off: i64) -> Self {
        Self { start_offset: off, bytes: Bytes::new(), total: 0 }
    }
}
```

In `impl Log`:

```rust
/// Like [`Log::read`] but returns verbatim wire bytes (no decode), walking
/// sealed segments then the active segment. Includes only batches with
/// `base_offset < limit_offset`, up to roughly `max_bytes` (≥ one batch).
pub fn read_raw(
    &self,
    fetch_offset: i64,
    limit_offset: i64,
    max_bytes: usize,
) -> Result<RawRead, LogError> {
    let log_start = self.log_start_offset();
    if fetch_offset < log_start {
        return Err(LogError::OffsetTooLow { requested: fetch_offset, log_start });
    }
    if fetch_offset >= limit_offset {
        return Ok(RawRead::empty(fetch_offset));
    }

    let mut chunks: Vec<Bytes> = Vec::new();
    let mut start_offset = fetch_offset;
    let mut current = fetch_offset;
    let mut remaining = max_bytes;
    let mut got_first = false;

    for seg in &self.segments {
        if seg.last_offset() < current {
            continue;
        }
        let r = seg.read_raw(current, limit_offset, remaining.max(HEADER_LEN))?;
        if !r.is_empty() {
            if !got_first {
                start_offset = r.start_offset;
                got_first = true;
            }
            remaining = remaining.saturating_sub(r.bytes.len());
            current = r.last_offset + 1;
            chunks.push(r.bytes);
            if remaining == 0 || current >= limit_offset {
                break;
            }
        }
    }

    if (remaining > 0 || !got_first)
        && current < limit_offset
        && let Some(active) = &self.active
        && current <= active.last_offset()
    {
        let r = active.read_raw(current, limit_offset, remaining.max(HEADER_LEN))?;
        if !r.is_empty() {
            if !got_first {
                start_offset = r.start_offset;
            }
            chunks.push(r.bytes);
        }
    }

    let bytes = match chunks.len() {
        0 => Bytes::new(),
        1 => chunks.pop().expect("len==1"),
        _ => {
            let total: usize = chunks.iter().map(Bytes::len).sum();
            let mut b = BytesMut::with_capacity(total);
            for c in &chunks {
                b.extend_from_slice(c);
            }
            b.freeze()
        }
    };
    let total = bytes.len();
    Ok(RawRead { start_offset, bytes, total })
}
```

Export the new types if the crate re-exports log/segment items (check `crates/log/src/lib.rs`; add `RawRead`, `RawSegmentRead` to its `pub use` lists if other types from `log.rs`/`segment.rs` are re-exported there).

- [ ] **Step 8: Run log tests**

Run: `cargo test -p crabka-log 2>&1 | tail -25`
Expected: PASS (all log tests including the new read_raw ones).

- [ ] **Step 9: Commit**

```bash
git add crates/log/src/segment.rs crates/log/src/log.rs crates/log/src/lib.rs
git commit -m "feat(log): decode-free read_raw header scan for verbatim batch bytes"
```

---

## Task 3: Consumer iterates all batches (crate `crabka-client-consumer`)

**Files:**
- Modify: `crates/client-consumer/src/poll.rs`

Requires Task 1.

- [ ] **Step 1: Update the batch-handling block**

Replace the single-batch block at `crates/client-consumer/src/poll.rs:104-118` (`let Some(batch) = payload.as_v2() else { continue }; for r in &batch.records { ... }`) with an outer loop over all batches:

```rust
// Legacy MessageSet payloads are skipped here; the consumer only
// handles v2 batches in this slice.
let Some(batches) = payload.as_v2() else {
    continue;
};
for batch in batches {
    for r in &batch.records {
        let offset = batch.base_offset + i64::from(r.offset_delta);
        out.push(ConsumerRecord {
            topic: topic_name.clone(),
            partition: part.partition_index,
            offset,
            timestamp: batch.base_timestamp + r.timestamp_delta,
            key: r.key.clone(),
            value: r.value.clone(),
        });
        offsets.insert((topic_name.clone(), part.partition_index), offset + 1);
    }
}
```

- [ ] **Step 2: Build + test the crate**

Run: `cargo test -p crabka-client-consumer 2>&1 | tail -25`
Expected: PASS (crate compiles against the new `as_v2() -> Option<&[RecordBatch]>`).

- [ ] **Step 3: Commit**

```bash
git add crates/client-consumer/src/poll.rs
git commit -m "feat(consumer): consume all batches per partition in poll"
```

---

## Task 4: Broker fetch pass-through, multi-batch consumers, down-convert (crate `crabka-broker`)

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`
- Modify: `crates/broker/src/replicator.rs`
- Modify: `crates/broker/src/handlers/fetch_downconvert.rs`
- Modify: `crates/broker/src/handlers/fetch.rs`

Requires Task 1 + Task 2. The crate will not fully compile until all four files are updated; the checkpoint build (Step 9) validates the whole crate. Do the edits in the order below.

- [ ] **Step 1: `produce.rs` — multi-batch metrics**

At `crates/broker/src/handlers/produce.rs:187-189`, replace the single-batch metric:

```rust
if let Some(batches) = p.records.as_ref().and_then(RecordsPayload::as_v2) {
    topic_messages += batches.iter().map(|b| b.records.len() as u64).sum::<u64>();
}
```

- [ ] **Step 2: `produce.rs` — extract the single producer batch from the `Vec`**

At `crates/broker/src/handlers/produce.rs:331-349`, change the `V2` arm. Producers send one batch per partition (documented contract); take it, treat empty as `INVALID_REQUEST`:

```rust
let mut batch = match payload {
    RecordsPayload::V2(batches) => match batches.into_iter().next() {
        Some(rb) => rb,
        None => {
            out.error_code = codes::INVALID_REQUEST;
            return Ok(out);
        }
    },
    RecordsPayload::Raw(bytes) => {
        // A producer that sent verbatim v2 bytes: decode the sole batch.
        match RecordsPayload::from_bytes(bytes).ok().and_then(|p| match p {
            RecordsPayload::V2(mut v) => v.drain(..).next(),
            _ => None,
        }) {
            Some(rb) => rb,
            None => {
                out.error_code = codes::INVALID_REQUEST;
                return Ok(out);
            }
        }
    }
    RecordsPayload::Legacy(bytes) => match crabka_records_legacy::legacy_to_v2(&bytes) {
        Ok(rb) => {
            if !topic_name.is_empty() {
                metrics.record_produce_message_conversion(topic_name);
            }
            rb
        }
        Err(e) => {
            tracing::warn!(error = %e, "legacy_to_v2 failed");
            out.error_code = codes::INVALID_RECORD;
            return Ok(out);
        }
    },
};
```

(The `Raw` arm is defensive: the broker decodes `ProduceRequest` into owned `RecordsPayload`, which yields `V2`, so `Raw` is not expected on produce — but handling it keeps the match total and robust.)

- [ ] **Step 3: `replicator.rs` — replicate every batch**

At `crates/broker/src/replicator.rs:312-333`, replace the single-batch arm so the follower appends all batches in order:

```rust
codes::NONE => {
    if let Some(batches) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
        let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) else {
            warn!(topic = %cfg.topic, partition = cfg.partition,
                "replicator: local partition vanished between fetches");
            return LoopAction::Continue;
        };
        for batch in batches {
            let batch_bytes = batch.encoded_len();
            if let Err(e) = entry.value().replicate_batch(batch.clone()).await {
                warn!(error = %e, topic = %cfg.topic, partition = cfg.partition,
                    "replicator: replicate_batch failed");
                break;
            }
            cfg.metrics.record_replication_in(
                &cfg.topic,
                cfg.partition,
                u64::try_from(batch_bytes).unwrap_or(0),
            );
        }
    }
    LoopAction::Continue
}
```

- [ ] **Step 4: `fetch_downconvert.rs` — add a payload-level multi-batch converter**

In `crates/broker/src/handlers/fetch_downconvert.rs`, add (keep the existing per-batch `down_convert_for_fetch` as-is — it is reused):

```rust
use bytes::{Bytes, BytesMut};

/// Down-convert a whole records-field payload for a Fetch v<4 requester.
///
/// Obtains the batch list (`Raw` is decoded here — the only place `Raw` is
/// parsed, and only for legacy clients), down-converts each non-dropped
/// batch, and concatenates the resulting legacy MessageSet bytes. Returns
/// `Ok(None)` when every batch was dropped (e.g. all control batches).
pub(crate) fn down_convert_payload_for_fetch(
    payload: &RecordsPayload,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    // Materialize the batches to convert.
    let batches: Vec<RecordBatch> = match payload {
        RecordsPayload::V2(b) => b.clone(),
        RecordsPayload::Raw(bytes) => match RecordsPayload::from_bytes(bytes.clone()) {
            Ok(RecordsPayload::V2(b)) => b,
            // Legacy-looking or undecodable raw: surface as corrupt.
            _ => return Err(crate::codes::CORRUPT_MESSAGE),
        },
        // Already legacy bytes — pass straight through.
        RecordsPayload::Legacy(_) => return Ok(Some(payload.clone())),
    };

    let mut out = BytesMut::new();
    for batch in &batches {
        match down_convert_for_fetch(batch, request_version)? {
            Some(RecordsPayload::Legacy(b)) => out.extend_from_slice(&b),
            Some(RecordsPayload::V2(_) | RecordsPayload::Raw(_)) => {
                // version < 4 always yields Legacy from down_convert_for_fetch.
                return Err(crate::codes::CORRUPT_MESSAGE);
            }
            None => {} // control batch dropped
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RecordsPayload::Legacy(Bytes::from(out))))
    }
}
```

Add a unit test in that file's `mod tests`:

```rust
#[test]
fn payload_multi_batch_concats_legacy() {
    // Two uncompressed batches → one concatenated legacy MessageSet.
    let b0 = make_batch(CompressionType::None, vec![sample_record("a", "1")]);
    let b1 = make_batch(CompressionType::None, vec![sample_record("b", "2")]);
    let payload = RecordsPayload::V2(vec![b0, b1]);
    let out = down_convert_payload_for_fetch(&payload, 3).unwrap().expect("some");
    let bytes = match out {
        RecordsPayload::Legacy(b) => b,
        _ => panic!("expected Legacy"),
    };
    let mut cur: &[u8] = &bytes;
    let recs = crabka_records_legacy::decode_message_set(&mut cur, bytes.len()).unwrap();
    assert_eq!(recs.len(), 2);
}
```

Note: `down_convert_for_fetch`'s own `version >= 4` early-return now produces `RecordsPayload::V2(batch.clone())` which must become `RecordsPayload::V2(vec![batch.clone()])`. Update that line (`fetch_downconvert.rs:23`) and its `version_gte_4_returns_v2_unchanged` test (match `V2(v) => assert_eq!(v, vec![batch])`).

- [ ] **Step 5: `fetch.rs` — `do_read` uses `read_raw` and drops server-side filtering**

In `crates/broker/src/handlers/fetch.rs`, rewrite the `do_read` body (lines ~859-963). Replace the `Option<RecordBatch>` plumbing with a `Bytes` plumbing. The new body:

```rust
let hw = part.high_watermark().await;
let (log_start, log_end, lso, raw, aborted_txns): (
    i64,
    i64,
    i64,
    Option<crabka_log::RawRead>,
    Vec<AbortedTransaction>,
) = {
    let log = part.log.lock().expect("log mutex poisoned");
    let log_start = log.log_start_offset();
    let log_end = log.log_end_offset();
    let lso = log.lso();
    let upper_bound = if is_follower_fetch { log_end } else { hw };
    let effective_lso = if read_committed && !is_follower_fetch {
        lso.min(hw)
    } else {
        lso
    };

    if fetch_offset < log_start {
        out.error_code = codes::OFFSET_OUT_OF_RANGE;
        out.log_start_offset = log_start;
        out.high_watermark = if is_follower_fetch { log_end } else { hw };
        out.last_stable_offset = if read_committed && !is_follower_fetch {
            effective_lso
        } else if is_follower_fetch {
            log_end
        } else {
            hw
        };
        return Ok(0);
    }

    // Visibility window upper bound (exclusive), at batch granularity.
    let limit_offset = if is_follower_fetch {
        log_end
    } else if read_committed {
        effective_lso
    } else {
        hw
    };

    if fetch_offset >= upper_bound {
        (log_start, log_end, lso, None, Vec::new())
    } else {
        let read_max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let raw = log.read_raw(fetch_offset, limit_offset, read_max)?;

        let aborted = if read_committed && !is_follower_fetch {
            // Aborted-txn list for [fetch_offset, effective_lso). No
            // server-side batch filtering: the consumer drops control /
            // aborted batches using this list (Kafka behavior).
            log.aborted_in_range(fetch_offset, effective_lso)
                .into_iter()
                .map(|e| AbortedTransaction {
                    producer_id: e.producer_id,
                    first_offset: e.start_offset,
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };

        let raw = if raw.total > 0 { Some(raw) } else { None };
        (log_start, log_end, lso, raw, aborted)
    }
};

out.error_code = codes::NONE;
out.high_watermark = if is_follower_fetch { log_end } else { hw };
out.log_start_offset = log_start;
out.last_stable_offset = if read_committed && !is_follower_fetch {
    lso.min(hw)
} else if is_follower_fetch {
    log_end
} else {
    hw
};

if read_committed && !is_follower_fetch {
    out.aborted_transactions = Some(aborted_txns);
}

let bytes_est = raw.as_ref().map_or(0, |r| r.total);
out.records = raw.map(|r| RecordsPayload::Raw(r.bytes));
Ok(bytes_est)
```

Then fix the imports at `fetch.rs:24`: `use crabka_protocol::records::{RecordBatch, RecordsPayload};` — `RecordBatch` may now be unused in `do_read`; keep it only if still referenced elsewhere in the file (the remote-tier path at ~1002 still uses `<RecordBatch as Encode>` — so keep the import). Remove the now-unused `aborted_pids`/`visible_batch` logic entirely (it's replaced above).

- [ ] **Step 6: `fetch.rs` — update the v<4 down-convert call site**

At `crates/broker/src/handlers/fetch.rs:422-459`, replace the per-batch `as_v2().cloned()` block with the payload-level converter:

```rust
if version < 4 {
    for topic_resp in &mut responses {
        for part in &mut topic_resp.partitions {
            if let Some(payload) = part.records.take() {
                match crate::handlers::fetch_downconvert::down_convert_payload_for_fetch(
                    &payload, version,
                ) {
                    Ok(Some(converted)) => {
                        if converted.payload_len() > 0 {
                            part.records = Some(converted);
                        }
                        if !topic_resp.topic.is_empty() {
                            broker
                                .metrics
                                .record_fetch_message_conversion(&topic_resp.topic);
                        }
                    }
                    Ok(None) => {
                        // Everything dropped (control batches) — records stays None.
                    }
                    Err(error_code) => {
                        part.error_code = error_code;
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 7: Update the `do_read` doc-comment**

The doc block above `do_read` (`fetch.rs:840-848`) describes the old filtering; update it to state that records are returned as verbatim `Raw` bytes clamped at the visibility window (`HW` for read_uncommitted consumers, `lso.min(hw)` for read_committed, `LEO` for followers), and that read_committed performs **no** server-side batch filtering (the consumer drops aborted/control batches via `aborted_transactions`). Also update the module doc at `fetch.rs:4` that claims "returns at most the first RecordBatch".

- [ ] **Step 8: Check for other `RecordBatch`-typed assumptions in fetch.rs**

Grep within the file for any remaining references to the removed `batch_opt`/`visible_batch` names and the `RecordBatch` plumbing in `do_read`; ensure the remote-tier path (`fetch.rs:1007 p.out.records = Some(batch.into())`) still compiles — `batch.into()` now yields `V2(vec![batch])`, which is correct.

Run: `grep -n "batch_opt\|visible_batch\|aborted_pids" crates/broker/src/handlers/fetch.rs`
Expected: no matches.

- [ ] **Step 9: Build + test the broker crate**

Run: `cargo test -p crabka-broker 2>&1 | tail -40`
Expected: PASS. If existing fetch/produce/replication unit tests assert single-batch `V2(RecordBatch)` shapes, update those assertions to the `V2(Vec)` / `Raw` forms (they are correctness-equivalent).

- [ ] **Step 10: Commit**

```bash
git add crates/broker/src/handlers/produce.rs crates/broker/src/replicator.rs \
        crates/broker/src/handlers/fetch_downconvert.rs crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): decode-free Raw fetch pass-through + multi-batch consumers"
```

---

## Task 5: Integration verification (workspace)

**Files:**
- Add: `crates/broker/tests/fetch_passthrough.rs` (or extend an existing fetch integration test file if one fits better — check `crates/broker/tests/` first).

Requires Tasks 1–4.

- [ ] **Step 1: Write a multi-batch round-trip integration test**

Model it on an existing broker integration test (inspect `crates/broker/tests/` for the harness that starts a broker / produces / fetches — reuse it). The test must:
1. Produce 3 separate batches to one partition (3 produce requests, or one per batch).
2. Issue a single Fetch (v ≥ 4, read_uncommitted) with a large `max_bytes`.
3. Decode the response `records` via `RecordsPayload::from_bytes` (the client side) and assert it yields **3** batches with the expected offsets and values.

```rust
// Pseudocode shape — adapt to the existing harness:
let resp = fetch(&broker, topic, 0, /*fetch_offset*/ 0, /*max_bytes*/ 8 * 1024 * 1024).await;
let part = &resp.responses[0].partitions[0];
let payload = part.records.as_ref().expect("records");
let batches = payload.as_v2().expect("v2");
assert_eq!(batches.len(), 3);
assert_eq!(batches[0].base_offset, 0);
assert_eq!(batches[2].base_offset, 2);
```

- [ ] **Step 2: Write a read_committed raw-passthrough assertion**

Add a test (or extend an existing transactions integration test) asserting that under `isolation_level=1`, a control/aborted batch is **present** in the fetched bytes (no server-side drop) and `aborted_transactions` is populated. If the existing transaction test suite already asserts the *old* server-side-filtered behavior, update those expectations to the new Kafka-aligned behavior (batches present + aborted list drives client filtering).

- [ ] **Step 3: Run the new integration tests**

Run: `cargo test -p crabka-broker --test fetch_passthrough 2>&1 | tail -30`
Expected: PASS.

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace 2>&1 | tail -40`
Expected: PASS. Investigate and fix any failures (likely test assertions still expecting single-batch `V2(RecordBatch)` or server-side read_committed filtering).

- [ ] **Step 5: Format + lint (CI gates on `cargo fmt --check`)**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets 2>&1 | tail -30`
Expected: no diff from fmt; no clippy errors.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/tests/
git commit -m "test(broker): multi-batch fetch pass-through + read_committed raw integration"
```

---

## Self-Review notes (already reconciled with the spec)

- **Spec §1 read_raw** → Task 2. **§2 RecordsPayload + borrowed** → Task 1. **§3 do_read** → Task 4 Steps 5-7. **§4 decode-side** (poll/replicator/produce/down-convert) → Task 3 + Task 4 Steps 1-4,6. **§5 unchanged** (socket/codec) → untouched. **Testing** → Tasks 2,4,5.
- Type/name consistency: `RecordsPayload::{V2(Vec<RecordBatch>), Raw(Bytes), Legacy(Bytes)}`, `as_v2() -> Option<&[RecordBatch]>`, `Segment::read_raw -> RawSegmentRead`, `Log::read_raw -> RawRead` with field `total`, `down_convert_payload_for_fetch` — used consistently across tasks.
- At-least-one-batch, `limit_offset` clamp (`base_offset < limit_offset`), and verbatim-bytes invariants are encoded in Task 2 tests.
