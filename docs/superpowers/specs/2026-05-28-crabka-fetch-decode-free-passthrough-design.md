# Crabka — Decode-Free Pass-Through Fetch Path

**Date:** 2026-05-28
**Status:** Design approved, ready for implementation plan
**Scope:** Userspace decode-elimination on the Fetch read path. (Kernel `sendfile`/`splice`
zero-copy-to-socket is explicitly **deferred** to a later phase.)

## Problem

Every Fetch request currently pays a full decode + re-encode cost per record batch:

1. `Segment::read` reads the `.log` byte range into a heap buffer, then runs a decode loop
   parsing **every batch into a `RecordBatch` struct** with per-record allocations
   ([`crates/log/src/segment.rs:204-221`](../../../crates/log/src/segment.rs)).
2. `do_read` wraps the result in `RecordsPayload::V2(RecordBatch)`
   ([`crates/broker/src/handlers/fetch.rs:962`](../../../crates/broker/src/handlers/fetch.rs)).
3. Response encoding calls `RecordBatch::encode`, **re-serializing** the batch back into
   wire bytes.

This is pure waste: the on-disk format **is** the Kafka v2 wire format. Records are stored
verbatim at produce time ([`segment.rs:247`](../../../crates/log/src/segment.rs) append, no
re-encoding), so the decode → reconstruct → re-encode round trip reproduces bytes that were
already correct on disk.

Two secondary problems will be fixed along the way:

- **One-batch-per-fetch limitation.** The current `do_read` returns at most a single batch
  per partition (`.find()`/`.next()` at [`fetch.rs:931`](../../../crates/broker/src/handlers/fetch.rs)),
  unlike Kafka which returns all complete batches that fit in `max_bytes`.
- **`read_committed` wire divergence.** Crabka filters control + aborted batches *server-side*
  ([`fetch.rs:912-926`](../../../crates/broker/src/handlers/fetch.rs)). Real Kafka sends those
  batches raw and lets the *consumer* drop them using the `aborted_transactions` list.

## Goals

- Eliminate record decoding and re-encoding on the Fetch path.
- Carry raw, already-wire-format `Bytes` slices from the segment buffer straight into the
  Fetch response.
- Return multiple batches per partition, up to `max_bytes` (Kafka throughput behavior).
- Bring `read_committed` into Kafka-correct alignment (no server-side batch filtering).
- Preserve Kafka wire-protocol byte exactness.

## Non-Goals

- Kernel `sendfile`/`splice` zero-copy-to-socket (deferred phase).
- `mmap`-backed segment reads (natural partner to `sendfile`; deferred).
- Eliminating the single copy of records into the assembled response buffer (also requires
  the deferred vectored/`sendfile` write path).
- Produce path, append, or indexing changes.

## Core Technique — Header-Only Scan

The v2 `RecordBatch` on-disk layout begins with a fixed-size header:

```
base_offset:i64(8) | batch_length:i32(4) | partition_leader_epoch:i32(4) | magic:i8(1) |
crc:u32(4) | attributes:i16(2) | last_offset_delta:i32(4) | base_timestamp:i64 |
max_timestamp:i64 | producer_id:i64 | producer_epoch:i16 | base_sequence:i32 |
record_count:i32 | records[]
```

`batch_length` covers every byte **after** the `batch_length` field itself, so a batch's
total on-disk size is `8 + 4 + batch_length = 12 + batch_length`. Walking from one batch to
the next requires reading only `base_offset` (bytes 0..8) and `batch_length` (bytes 8..12).
Clamping decisions additionally need `last_offset_delta` (bytes 23..27).

The scan **never touches the records body, never decodes, never re-encodes.** Bytes are
forwarded verbatim:

- CRC remains valid (we don't modify any byte).
- `base_offset` is already absolute on disk — no rewriting needed (identical to how
  `sendfile`-based Kafka serves bytes straight from the file).

## Design

### 1. Log crate — new raw read API

**`Segment::read_raw(fetch_offset, limit_offset, max_bytes) -> Result<RawSegmentRead, LogError>`**

```rust
pub struct RawSegmentRead {
    pub start_offset: i64,  // base_offset of the first included batch
    pub last_offset: i64,   // last offset covered by the returned bytes
    pub bytes: Bytes,       // verbatim slice of the segment buffer
}
```

Behavior:

1. Index-locate the start position for `fetch_offset` (existing `offset_index.lookup`).
2. Read the log range from that position into one buffer; freeze to `Bytes`.
3. Header-walk the buffer:
   - **Skip** leading batches whose `batch_last = base_offset + last_offset_delta < fetch_offset`
     (sparse-index slack; first batch may start before the requested offset).
   - **Include** batches while `base_offset < limit_offset`, advancing the end cursor by
     `12 + batch_length` each step.
   - **Stop** when `base_offset >= limit_offset`, when accumulated bytes `>= max_bytes`, or at
     end of buffer / first partial trailing batch.
   - **At-least-one-batch guarantee:** always include the first matching batch even if it
     alone exceeds `max_bytes` (Kafka's anti-stall rule).
4. Return `bytes.slice(start..end)` — no copy.

**`Log::read_raw(fetch_offset, limit_offset, max_bytes) -> Result<RawRead, LogError>`**

```rust
pub struct RawRead {
    pub start_offset: i64,
    pub bytes: Bytes,
    pub total: usize,
}
```

Walks sealed segments then the active segment (mirrors existing `Log::read` span logic).
The common single-segment fetch returns one `Bytes` with no concatenation. The rare
cross-segment fetch concatenates chunks into one buffer once. The `Log` mutex is held across
the read exactly as today (no change to locking behavior).

### 2. Protocol crate — new payload variant

Add a third `RecordsPayload` variant in
[`crates/protocol/src/records/payload.rs`](../../../crates/protocol/src/records/payload.rs):

```rust
pub enum RecordsPayload {
    V2(RecordBatch),  // existing — parsed
    Legacy(Bytes),    // existing — pre-v2 MessageSet, round-tripped verbatim
    Raw(Bytes),       // new — already-wire-format v2 bytes, pass through verbatim
}
```

- `payload_len(Raw(b)) = b.len()`
- `encode_to(Raw(b)) = put_slice(b)`
- `as_v2(Raw(_)) = None` (it is intentionally unparsed)

`Raw` differs from `Legacy` semantically: `Legacy` is pre-v2 MessageSet bytes; `Raw` is one
or more v2 batches forwarded without parsing.

### 3. Broker — `do_read` rewrite

Replace the decode-based block in
[`crates/broker/src/handlers/fetch.rs`](../../../crates/broker/src/handlers/fetch.rs):

- Compute `limit_offset` per isolation level:
  - **follower fetch:** `log_end` (LEO)
  - **`read_committed` consumer:** `lso.min(hw)` (effective LSO)
  - **`read_uncommitted` consumer:** `hw`
- `let raw = log.read_raw(fetch_offset, limit_offset, max_bytes)?;`
- `out.records = (raw.total > 0).then(|| RecordsPayload::Raw(raw.bytes));`
- **No server-side batch filtering.** Control + aborted batches flow through raw.
- `aborted_transactions` is still populated from `log.aborted_in_range(fetch_offset, effective_lso)`
  exactly as today, for `read_committed`. The consumer does the dropping.
- `bytes_est = raw.total` for the cross-partition `max_bytes` budget and long-poll `min_bytes`
  accounting.
- `high_watermark` / `last_stable_offset` / `log_start_offset` metadata set exactly as before.

Clamping at batch granularity is correct because HW and LSO always advance on batch
boundaries — a batch is either fully visible or not. Inclusion test is `base_offset < limit_offset`.

### 4. Unchanged

- **Socket write path:** the Fetch response is still assembled into a single framed `Bytes`
  via `LengthDelimitedCodec`. The raw records bytes are copied into the response buffer once
  during encode; removing that final copy is the deferred `sendfile` phase.
- Produce path, append, offset/time indexing: untouched.

**Net effect:** one bulk read copy (file → userspace) + one copy into the response buffer;
**zero decode, zero re-encode, zero per-record allocation.**

## Testing

### Header-scan unit tests (log crate)
- Batch-boundary walking matches a full decode's boundaries.
- `last_offset` / `start_offset` math correct, including multi-batch ranges.
- `max_bytes` honored, with the at-least-one-batch guarantee verified.
- Clamp at `limit_offset` (batch with `base_offset == limit` excluded).
- Mid-batch `fetch_offset` includes the whole containing batch; leading batches fully below
  `fetch_offset` are skipped.
- Partial trailing batch is excluded.

### Byte-exactness tests
- Produce N batches, `read_raw`, assert returned bytes are **verbatim-equal** to the
  concatenation of the on-disk batch bytes.
- Assert the existing `RecordBatch::decode` parses the raw output identically to the batches
  produced.

### `read_committed` correctness (the new Kafka-aligned behavior)
- Aborted + control batches now **appear** in the raw bytes.
- `aborted_transactions` list is populated from the txn index.
- Effective-LSO clamp (`lso.min(hw)`) excludes undecided batches.

### HW clamp + cross-segment
- Batches at/above HW excluded byte-exactly for `read_uncommitted`.
- Fetch spanning a segment boundary concatenates correctly and parses cleanly.

### Regression
- Existing Fetch integration tests still pass (client decodes the raw bytes unchanged).

## To Verify During Planning

- **Consumers of `out.records` / `as_v2()` on the Fetch response path** — incremental fetch
  session (KIP-227) handling and response filtering in `fetch.rs`. Confirm none assume a
  parsed `V2` payload; if any do, adapt them to operate on `Raw` (size/offset metadata only).
- **Down-conversion:** the current code always re-encodes v2 regardless of client fetch
  version, implying Crabka is v2-only on the wire. Confirm there is no v0/v1 down-conversion
  path. If one is ever added, it must bypass the `Raw` pass-through (decode + down-convert).
- Confirm `attributes`/`producer_id` are not needed by the header scan itself (they are only
  relevant to the txn index, which is queried separately and unchanged).
