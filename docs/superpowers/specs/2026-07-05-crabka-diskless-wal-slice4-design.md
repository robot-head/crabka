# Diskless WAL — Slice 4: fetch-from-object + enable trim — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (fourth slice of a 6-slice milestone). A **scaffolding slice** — single-leader; it closes the diskless read loop but leaves leaderless serving to Slice 6.

## Context — where this sits

Fourth slice of the diskless-broker WAL milestone (see [Slice 1](2026-07-05-crabka-diskless-wal-slice1-design.md) for the decomposition). Slice 3 built the per-broker flusher (acked WAL tails → shared objects) + the `WalIndexCache` offset→object index, and a **gated-off** local-WAL trim seam. Slice 4 makes trimming *safe and enabled*: serve a Fetch for a **trimmed** offset from object storage, fix `ListOffsets` so trimming doesn't corrupt the consumer-visible earliest, then flip the trim gate on. Hot reads (offset still in the local `Log`) continue via the existing Fetch/sendfile path unchanged.

**Prerequisites (unlanded):** Slices 1–3 (the `diskless` flag, `high_watermark`-from-WAL-durable, the flusher + `WalIndexCache`, the gated trim seam). Land them first.

**Ordering is load-bearing.** Slice 4 must land in this order: **(1)** the diskless cold read; **(2)** the `ListOffsets` earliest fix; **(3)** enable trim. Enabling trim before (1)+(2) would make trimmed data unreadable *and* corrupt `ListOffsets EARLIEST`.

## Design Goals

- **Serve cold reads from object storage:** a Fetch whose `fetch_offset` is below the local floor (trimmed) is served from the shared WAL object via `WalIndexCache` → ranged GET → byte-exact Kafka batches.
- **Keep `ListOffsets` wire-correct after trim:** consumer-visible `EARLIEST` must report the earliest *object-covered* offset, not the advanced local floor.
- **Enable local-WAL trimming** (flip Slice 3's `trim_safety_lag`), bounding local disk growth now that a cold-read fallback exists.
- **Preserve the ack path and the hot read path** exactly (hot fetches still use sendfile; produce/ack unchanged).

### Non-goals (Slice 4)

- **No in-memory hot-tail cache.** In single-leader Slice 4 the local `Log` *is* the hot tail, and `trim_safety_lag` keeps a live local tail, so the hot/sendfile path never touches objects. The cache earns its keep only for leaderless/stateless serving — **Slice 6**.
- **No transactional diskless.** Diskless topics reject transactional produce this slice (see below); read_committed cold reads are trivially correct (no aborts). Transactional diskless (an aborted-txn manifest in the object) is a later refinement.
- **No object retention/GC.** Flushed objects are kept indefinitely — which is *why* the earliest object-covered offset stays at 0.
- **No crash atomicity** of trim-vs-index (Slice 5); **no leaderless serving** (Slice 6).

## Architecture Overview

```
Fetch (handlers/fetch.rs)
  do_read → local Log read           ← HOT: offset ≥ local floor → sendfile/FileRegions (UNCHANGED)
     │  if fetch_offset < log_start:  out.error_code = OFFSET_OUT_OF_RANGE   (fetch.rs:1031,1131)
     ▼
  OFFSET_OUT_OF_RANGE dispatch (fetch.rs:514-518)
     ├── try_remote_read  → KIP-405 tiered (remote.storage.enable)          ← UNCHANGED
     └── try_diskless_read → NEW (diskless topic mode; mutually exclusive):
            WalIndexCache.lookup(tp, fetch_offset) → (object_key, byte_start, byte_len)
            store.get_range(object_key, byte_start .. byte_start+byte_len)
            first_batch_at_or_after(&run, fetch_offset)   ← reuse landed scan (remote_reader.rs:432)
            → RecordsPayload::Raw(owned bytes)            ← NOT sendfile
                                                            (HW/LSO/log_start = local do_read values)

ListOffsets (handlers/list_offsets.rs)
  EARLIEST (-2)   = min(local_start, WalIndexCache.earliest_covered(tp))   ← NEW diskless branch
  EARLIEST_LOCAL  = local_log_start (advanced by trim)
  LATEST (-1)     = local_end                                             (unchanged)

Trim (Slice 3 seam) — FlushConfig.trim_safety_lag: None → Some(lag)       ← ENABLED (step 3)
  trims only up to offsets already projected in WalIndexCache (cold-readable)
```

## Key Design Decisions

### Cold read at the existing `OFFSET_OUT_OF_RANGE` seam

The local read runs first; when `fetch_offset < log.log_start_offset()` the visibility window sets `OFFSET_OUT_OF_RANGE` (`fetch.rs:1031`, written at `:1110-1116`, `ReadPlan::OffsetOutOfRange` at `:1131`). The existing dispatch `if p.out.error_code == OFFSET_OUT_OF_RANGE && let Some(bytes) = try_remote_read(...)` (`fetch.rs:514-518`, re-tried on long-poll at `:1471-1473`) gains a sibling `try_diskless_read(broker, p, &part)`, tried when the partition is a diskless topic. The two are **mutually exclusive** (KIP-405 tiered `remote.storage.enable` vs diskless topic mode), so no double-coverage. *Alternative rejected:* overloading `try_remote_read` — it is hard-wired to the RLMM + per-segment `.index` machinery (`fetch.rs:1277-1283`), none of which the run-granular diskless index uses.

### The cold read body — new, not a reuse of `RemoteReader::fetch_batch`

`try_diskless_read` does: `WalIndexCache.lookup(tp, fetch_offset)` → `(object_key, byte_start, byte_len)` (a per-`(tp)` `BTreeMap` floor lookup that *replaces* both KIP-405 steps — segment metadata *and* the sparse `.index` position, `remote_reader.rs:172-177,373-382` — because the diskless index hands back absolute byte offsets directly); ranged GET `store.get_range(object_key, byte_start .. byte_start+byte_len)` (half-open, as `s3.rs:392-419` already handles); then **reuse the landed** `first_batch_at_or_after(&run, fetch_offset)` (`remote_reader.rs:432-444`, `RecordBatch::decode` walk, `last_offset >= floor`) to position onto the covering batch. Return the raw run slice from that batch boundary as `RecordsPayload::Raw` (owned `Bytes`), taking the same vectored-copy drain the non-sendfile fetch path uses — **never** a `FileRegion`/sendfile descriptor (the bytes came off the network, not a pinned inode), mirroring `try_remote_read` (`fetch.rs:1368`). On success `error_code = NONE`; HW/LSO/log_start stay the local `do_read` values; a miss leaves `OFFSET_OUT_OF_RANGE` (retryable). No per-batch index is needed — batches self-delimit and the client skips records `< fetch_offset`.

### Byte-exactness — return the raw slice, no decode→re-encode

The run is unmodified verbatim v2 batches (the flusher wrote acked tails verbatim). Returning the raw slice from the covering batch boundary is byte-identical to what the hot path would have shipped. *Decision:* return the raw run slice, not a decoded-then-re-encoded `RecordBatch`. A decode→re-encode round-trip byte-exact test is the wire-compat gate regardless.

### The `ListOffsets` earliest fix — layer it, don't split `Log`

The current `Log` has a **single** log-start pointer: `local_log_start_offset()` delegates to `log_start_offset()` (`log.rs:1188-1190`), and `trim_to_offset` advances it via `set_log_start_offset` (`:1129`). So trim advancing the local floor *also* advances the consumer-visible earliest — which would make `ListOffsets EARLIEST` skip past object-covered data (a wire-compat violation; Slice 3's "trim doesn't touch consumer-visible offsets" is false against the current code). *Fix (KIP-405 precedent, chosen):* leave `Log` as-is and compute consumer-visible `EARLIEST` at the **ListOffsets layer** — `EARLIEST (-2)` for a diskless partition = `min(local_start, WalIndexCache.earliest_covered(tp))`, mirroring how tiered already does `earliest = min(local_start, reader.earliest_offset)` (`list_offsets.rs:142,149-150`). `EARLIEST_LOCAL (-4)` = the advanced local floor (`:164`); `LATEST` unchanged. With no object retention this slice, `earliest_covered` stays 0, so `EARLIEST` stays 0 after trim. *Alternative rejected:* splitting the two pointers inside `Log` — a deeper change to a core, heavily-tested type that diverges from the landed KIP-405 layering.

### Enable trim — after (1)+(2), gated on index durability

Flip Slice 3's `FlushConfig.trim_safety_lag` from `None` to `Some(lag)`. The Slice-3 gate `trim_target = min(flushed_frontier, hw − lag)` fires `WriterMessage::TrimToOffset` (`partition.rs:329` → `partition_writer.rs:367-388` → `Log::trim_to_offset`). Slice 4 tightens the gate: trim only up to an offset whose index entry is **already projected into `WalIndexCache`** (index durability, not merely object PUT) — guaranteeing every trimmed offset is cold-readable, so there is no `[below-floor ∧ cache-miss]` hole. The `lag` keeps a live local tail so the hot path never touches objects. Full crash atomicity across trim-vs-index is Slice 5.

### Non-transactional diskless

Diskless topics **reject transactional produce** in Slice 4 (a transactional batch, or `AddPartitionsToTxn` naming a diskless topic, is refused with a clear error). Rationale: the shared WAL object carries no `.txnindex`, so a cold read cannot surface aborted transactions; the KIP-405 cold path fills `aborted_transactions` from a per-segment `.txnindex` (`fetch.rs:1335-1366`) that has no diskless analogue. With no aborts: `LSO = HW`, and read_committed cold reads return an empty aborted list — correct. *Alternative deferred:* an aborted-txn manifest in the object body — full transactional diskless, later.

## Integration

- **`crates/broker/src/handlers/fetch.rs`** — add `try_diskless_read` at the `OFFSET_OUT_OF_RANGE` seam (`:514-518`, and the long-poll re-read `:1471-1473`); the diskless predicate; return `RecordsPayload::Raw`.
- **`crates/broker/src/diskless/`** (Slice 3) — the cold-reader helper (lookup + get_range + `first_batch_at_or_after`); a `WalIndexCache::earliest_covered(tp)` accessor.
- **`crates/broker/src/handlers/list_offsets.rs`** — the diskless `EARLIEST` = `min(local_start, earliest_covered)` branch.
- **`crates/broker/src/diskless/flusher.rs`** (Slice 3) — set `trim_safety_lag = Some(lag)`; tighten the trim gate to index-projected offsets.
- **Produce / txn path** — reject transactional produce for diskless topics; `LSO = HW` for diskless.
- **`Log`** — unchanged (no pointer split).
- **Ack path + hot sendfile path** — untouched.

## Kafka / KIP compliance

- **Fetch byte-exact.** Cold reads return unmodified verbatim v2 batches from the covering batch boundary; clients skip records `< fetch_offset` exactly as for local reads.
- **`ListOffsets` correct.** `EARLIEST ≤` any still-fetchable offset (local or object); `EARLIEST_LOCAL` exposes the local floor; `LATEST` unaffected. This is the wire-compat shipping gate.
- **Transactions.** Diskless is non-transactional this slice; `read_committed` behaves correctly (no aborts). The rejection of transactional produce to diskless topics is an explicit, observable error, not silent misbehavior.
- **KIP-405 unaffected.** Tiered topics still route via `try_remote_read`; the diskless predicate never steals their reads.

## Testing

- **Trimmed-then-fetched byte-exact:** produce → flush+index → trim so offset `O` is below the local floor but object-covered; fetch at `O` returns bytes byte-identical to the pre-trim local batch(es); include a mid-batch fetch (`base < O ≤ batch_last`) to prove batch-boundary positioning + client-skip. Behavior test — no source reads.
- **Union coverage, no gap / no overlap:** every offset in `[earliest_object, hw)` is served exactly once — `[earliest_object, local_floor)` cold, `[local_floor, hw)` local; pin the boundary offset (`O == local_floor` and `O == local_floor−1`) to exactly one path.
- **`ListOffsets` after trim:** `EARLIEST == earliest_object` (0), `EARLIEST_LOCAL == advanced local floor`, `LATEST` unchanged; `EARLIEST ≤` any fetchable offset.
- **Ack + hot path untouched:** produce/ack semantics unchanged; a hot fetch (offset still local) still takes the sendfile/`FileRegions` path, not the owned-bytes cold path.
- **Routing:** a cache miss / genuinely-uncovered offset returns `OFFSET_OUT_OF_RANGE` (retryable); KIP-405 tiered reads still route via `try_remote_read`.
- **Transactions rejected:** transactional produce (or `AddPartitionsToTxn`) to a diskless topic is refused with the expected error.

## Risks (carried into the plan)

- **Positioning off-by-one → wrong bytes.** The run scan must return the batch where `base ≤ O ≤ base+last_offset_delta` and include its full bytes. Mitigation: reuse the landed, tested `first_batch_at_or_after` verbatim; add a mid-batch fetch test.
- **Trim-before-index-durable coverage gap.** Trimming past an offset not yet in `WalIndexCache` creates a hole (below floor ∧ miss). Mitigation: trim only up to index-projected offsets.
- **`ListOffsets EARLIEST` regression** (wire-compat gate): must land the ListOffsets min-branch *before* enabling trim.
- **Cold-read latency / read-amplification:** a cold fetch adds a `get_range` round trip; the run byte-range isolates one partition, but mis-sized ranges amplify. Mitigation: range strictly `[byte_start, byte_start+byte_len)`; cap by `max_bytes`.
- **Response-shape mismatch:** cold bytes must take the `RecordsPayload::Raw` drain, never the sendfile `FileRegion` arm.
- **Boundary double-coverage:** the two cold predicates (tiered vs diskless) must be mutually exclusive; the boundary offset pinned to one path.

## Resolved decisions (from brainstorming)

- **Log-start fix:** ListOffsets-layer anchor (`min(local_start, earliest_covered)`), no `Log` pointer split (KIP-405 precedent).
- **Transactions:** diskless is non-transactional in Slice 4 (reject transactional produce); read_committed cold reads trivially correct.
- **Cold read:** `try_diskless_read` at the `OFFSET_OUT_OF_RANGE` seam; `WalIndexCache.lookup` → `get_range` → reuse `first_batch_at_or_after` → `RecordsPayload::Raw`.
- **Byte-exactness:** raw run slice from the batch boundary (no decode→re-encode).
- **Hot-tail cache:** deferred to Slice 6.
- **Ordering:** cold read → ListOffsets fix → enable trim.
- **Enable trim:** `trim_safety_lag = Some(lag)`, gated on index-projected offsets.
