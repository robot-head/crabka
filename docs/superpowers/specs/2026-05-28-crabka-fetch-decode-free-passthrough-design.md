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

- **One-batch-per-fetch limitation, end to end.** The pipeline is single-batch at *every*
  stage, not just the server read:
  - `do_read` returns at most one batch per partition (`.find()`/`.next()` at
    [`fetch.rs:931`](../../../crates/broker/src/handlers/fetch.rs)).
  - `RecordsPayload::from_bytes` decodes only the **first** batch off the wire and discards
    trailing bytes ([`payload.rs:42-45`](../../../crates/protocol/src/records/payload.rs)).
  - Crabka's own consumer reads one batch per partition
    ([`poll.rs:104`](../../../crates/client-consumer/src/poll.rs)).
  - The **replicator** replicates one batch per fetch
    ([`replicator.rs:314`](../../../crates/broker/src/replicator.rs)).

  Making only the server emit multi-batch would let Crabka's own replicator and consumer
  **silently drop every batch after the first** — a replication-correctness regression.
  Multi-batch therefore requires decode-side changes too (see "Decode-side multi-batch").
- **`read_committed` wire divergence.** Crabka filters control + aborted batches *server-side*
  ([`fetch.rs:912-926`](../../../crates/broker/src/handlers/fetch.rs)). Real Kafka sends those
  batches raw and lets the *consumer* drop them using the `aborted_transactions` list.

> **Correction to an earlier draft:** Crabka is **not** v2-only. There is a real
> down-conversion path for Fetch v<4 clients
> ([`fetch.rs:422-459`](../../../crates/broker/src/handlers/fetch.rs) →
> [`fetch_downconvert.rs`](../../../crates/broker/src/handlers/fetch_downconvert.rs)),
> and an up-conversion path on Produce. The pass-through design must coexist with both.

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

### 2. Protocol crate — multi-batch payload + Raw variant

Redesign `RecordsPayload` in
[`crates/protocol/src/records/payload.rs`](../../../crates/protocol/src/records/payload.rs)
so the records field is modeled as a **sequence** of batches, and add a pass-through variant:

```rust
pub enum RecordsPayload {
    V2(Vec<RecordBatch>),   // CHANGED: was V2(RecordBatch). Zero or more parsed v2 batches.
    Raw(Bytes),             // NEW: verbatim wire bytes holding 1+ v2 batches. Pass-through.
    Legacy(Bytes),          // unchanged: pre-v2 MessageSet, round-tripped verbatim.
}
```

Methods:
- `from_bytes(bytes)`: when it looks like v2, **loop** `RecordBatch::decode` until the buffer
  is exhausted, collecting all batches → `V2(vec)` (was: decode one). Else `Legacy`.
- `payload_len()`: `V2` → sum of `encoded_len`; `Raw` → `bytes.len()`; `Legacy` → `len`.
- `encode_to()`: `V2` → encode each batch; `Raw` → `put_slice`; `Legacy` → `put_slice`.
- `as_v2(&self) -> Option<&[RecordBatch]>` (was `Option<&RecordBatch>`): `V2` → slice; else `None`.
- `From<RecordBatch>` → `V2(vec![rb])`; add `From<Vec<RecordBatch>>` → `V2(v)`.

`Raw` is the **server fetch-emit** variant: `do_read` puts verbatim segment bytes here, and
they reach the wire with zero decode/encode. It differs from `Legacy` (pre-v2 MessageSet) —
`Raw` is one or more *v2* batches forwarded as-is.

`RecordsPayloadBorrowed` is mirrored: `V2(Vec<RecordBatchBorrowed>)`, multi-batch `from_slice`,
`to_owned` → `RecordsPayload::V2(Vec)`. (The borrowed form is a decode-side type used by the
generated codec; it does not need a `Raw` variant.)

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

### 4. Decode-side multi-batch (consumers, replicator, produce, down-convert)

Because `RecordsPayload::V2` now carries a `Vec`, every consumer that previously read a single
batch must iterate. All of these are off the broker fetch hot path (the hot path is `Raw`
pass-through, which never decodes):

- **Consumer** [`poll.rs:104`](../../../crates/client-consumer/src/poll.rs): replace the single
  `as_v2()` batch with `for batch in payload.as_v2().into_iter().flatten()`, emitting records
  from every batch and advancing `next_offsets` to the last record's offset + 1.
- **Replicator** [`replicator.rs:314`](../../../crates/broker/src/replicator.rs): iterate
  `as_v2()` and `replicate_batch` each batch in order; accrue replication-in bytes per batch.
  (Critical: without this, followers drop all but the first batch.)
- **Produce** [`produce.rs:331`](../../../crates/broker/src/handlers/produce.rs): the `V2` arm
  becomes `V2(v)`; take the sole producer batch (`v.into_iter().next()`, the documented
  one-batch-per-partition contract — empty → `INVALID_REQUEST`). Metrics at
  [`produce.rs:187`](../../../crates/broker/src/handlers/produce.rs) sum `records.len()` across
  the slice.
- **Down-conversion** [`fetch.rs:422-459`](../../../crates/broker/src/handlers/fetch.rs) +
  [`fetch_downconvert.rs`](../../../crates/broker/src/handlers/fetch_downconvert.rs): for Fetch
  v<4 the records arriving from `do_read` are now `Raw`. Add
  `down_convert_payload_for_fetch(payload, version)` that obtains the batch list (`Raw` →
  decode all via `from_bytes`; `V2` → the slice), down-converts each non-dropped batch with the
  existing per-batch `down_convert_for_fetch`, and concatenates the resulting `Legacy` bytes
  into a single `Legacy` payload (or `None` if every batch was dropped). This is the **only**
  place that decodes `Raw`, and only for legacy clients — the modern v4+ path stays pass-through.

### 5. Unchanged

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

### Multi-batch decode (protocol + consumers)
- `from_bytes` on multi-batch v2 bytes yields all batches; round-trips byte-exactly.
- A multi-batch `Raw` response is consumed end-to-end: `poll.rs` emits every record, and the
  replicator appends every batch.

### `read_committed` correctness (the new Kafka-aligned behavior)
- Aborted + control batches now **appear** in the raw bytes.
- `aborted_transactions` list is populated from the txn index.
- Effective-LSO clamp (`lso.min(hw)`) excludes undecided batches.

### HW clamp + cross-segment
- Batches at/above HW excluded byte-exactly for `read_uncommitted`.
- Fetch spanning a segment boundary concatenates correctly and parses cleanly.

### Down-conversion (Fetch v<4)
- A multi-batch `Raw` response down-converts to a single concatenated `Legacy` MessageSet.
- Control batches within the multi-batch set are dropped; the rest convert.

### Regression
- Existing Fetch + Produce + replication integration tests still pass.

## Verified During Planning

- **`as_v2()` consumers** are exactly: `poll.rs:104`, `replicator.rs:314`, `produce.rs:187`,
  and the down-convert site `fetch.rs:426`. All are addressed in §4. The `replicator.rs` and
  `poll.rs` hits are on the **decode** side (they parse the wire response back to `V2`), so the
  server's `Raw` emit does not break them — but they must iterate batches to honor multi-batch.
- **Down-conversion exists** (`fetch.rs:422-459` → `fetch_downconvert.rs`); the earlier
  "v2-only" assumption was wrong. The `Raw` path coexists with it per §4.
- **Header scan needs only** `base_offset`, `batch_length`, `last_offset_delta` — all present
  in the zerocopy `RecordBatchHeader` (`HEADER_LEN = 61`, batch stride `= 12 + batch_length`).
  `attributes`/`producer_id` are not needed by the scan; the aborted-txn list is built from the
  txn index separately and unchanged.
