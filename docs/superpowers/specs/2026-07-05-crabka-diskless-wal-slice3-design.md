# Diskless WAL — Slice 3: shared object-store flush + offset→object index — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (third slice of a 6-slice milestone). A **scaffolding slice** — it writes flush objects + the index, but serves no reads from object storage (Slice 4) and does not ship diskless on its own.

## Context — where this sits

Third slice of the diskless-broker WAL milestone (see [Slice 1](2026-07-05-crabka-diskless-wal-slice1-design.md) for the decomposition). Slice 1 made diskless topics durable by local `fsync` with the `acks=all` gate on the WAL durable watermark; Slice 2 moved offset assignment to KRaft. Slice 3 is where diskless data first reaches **object storage**: a per-broker background worker batches acked WAL records from many partitions into shared object-storage objects and records an offset→object index.

**Framing (roadmap tension #3, AutoMQ-shaped):** the flush is **async / background, *after* the ack**. The produce/ack path is unchanged — it still gates on Slice-1 local `fsync` durability. The flush moves data local-WAL → object storage to enable later read-serving (Slice 4) and local-WAL trimming; flush latency never touches produce latency.

**Prerequisites (unlanded):** Slices 1 and 2 are spec-only. This slice consumes the Slice-1 `diskless` per-topic flag and the WAL-durable high watermark, and the per-broker leadership model. Land Slices 1–2 first.

**Substrate note:** `crabka-object-store` as landed exposes only `build_object_store(cfg) -> Arc<dyn ObjectStore>` + config/error (`crates/object-store/src/lib.rs:14-19`); the `ObjectOps` trait is a separate, unlanded plan. Slice 3 builds directly on the raw `Arc<dyn ObjectStore>` and does **not** depend on `ObjectOps`.

## Design Goals

- **Land acked WAL data in object storage** via a per-broker background flusher: batch many partitions' hot records into one shared, immutable object on a size-OR-time trigger.
- **Record an offset→(object, byte-range) index** on a new internal topic so Slice 4 can later resolve "which object + byte-range covers `(partition, offset)`".
- **Build the local-WAL trim seam** (gated off) so Slice 4 can enable trimming once object-read exists.
- **Prove faithful recoverability:** the flushed object + index reconstruct each partition's records byte-exactly; the flush watermark never trims un-flushed data.
- **Guarantee the ack path is untouched** — produce latency and semantics are identical whether or not a flush is in flight.

### Non-goals (Slice 3)

- **No fetch-from-object.** Slice 3 *writes* objects and *projects* the index; it does not serve reads from object storage. Fetch still reads the local `Log`. (Slice 4.)
- **No trimming enabled.** The trim seam is built and wired, but disabled by default — there is no object-read fallback yet, so trimming a still-needed offset would make it permanently unreadable. (Slice 4 enables it.)
- **No crash-mid-flush atomicity.** The PUT→index→trim ordering is honored under clean shutdown; atomicity across a crash between those steps is **Slice 5**.
- **No S3-PUT-primitive extraction.** The flusher uses the raw `object_store` PUT directly; extracting the private `S3RemoteStorage` PUT helpers into `crabka-object-store` (to share one engine with KIP-405) is deferred to avoid touching the landed tiered path.
- **No diskless+tiered coexistence** on one partition — a diskless topic is not also KIP-405-tiered, so there is one trim authority.

## Architecture Overview

```
per-broker flush worker (new tokio task, modeled on remote_log_manager::run/tick_all)
  every tick:  registry.arcs() → filter led diskless partitions (current_leader == node_id)
     for each led diskless partition:
        Log::read_raw(flushed_offset, high_watermark, budget)   ← decode-free verbatim tail, < HW (acked)
        append the verbatim Bytes run into the cross-partition accumulator
        record (topic_id, partition, [first,last] offsets, [byte_start,byte_len]) entry
     when Σbytes ≥ 8 MiB OR 250 ms elapsed with pending:
        PUT one object  diskless-wal/<broker_id>/<flush_uuid>
             = [header] · runs · [footer manifest] · [footer_len][magic]
        publish one WalFlushRecord{object_key, entries} to __diskless_wal_index
        on success: advance each partition's flushed_offset
        (trim gated OFF this slice)

ack path (produce.rs / partition_writer / WalStore fsync)  ← UNTOUCHED; async flush runs behind it
```

## Key Design Decisions

### Per-broker flusher, leader-filtered

A new `tokio`-spawned worker, structurally mirroring the KIP-405 tiered driver `remote_log_manager::run`/`tick_all` (`crates/broker/src/remote_log_manager.rs:55-121`): a `tokio::time::interval` ticker + `CancellationToken`, snapshotting `PartitionRegistry::arcs()` before any await and filtering to partitions this broker leads (`partition.current_leader.load(...) == node_id`, `remote_log_manager.rs:90`; `Partition.current_leader` at `partition.rs:199`). It is a **new task, distinct from** `tick_all` (which is per-sealed-segment, per-partition). *Why per-broker, not a cluster role:* the flush source is the broker's **local** `Log` (`Partition.log`, `partition.rs:192`) — a separate role cannot read another broker's local segments, so leadership is the natural sharding. *Failure mode:* a stuck flusher stops advancing `flushed_offset`, growing the local WAL — an availability risk (disk fill), not a data-loss risk (acks are unaffected). Flush lag must be observable and panics surfaced.

### Read source = `read_raw` up to the HW

The flushable window per partition is `Log::read_raw(flushed_offset, high_watermark, budget)` (`crates/log/src/log.rs:818-894`) — the same decode-free, byte-exact v2-batch primitive the Fetch handler uses (`handlers/fetch.rs:1179`), returning owned `Bytes` (not the `sendfile` sibling `read_raw_desc`, since the body is uploaded). The **upper bound is the high watermark** (`Partition::high_watermark()`, `partition.rs:432`), which in Slice-1/2 mode only advances after `fsync` — so the flusher never uploads un-acked/un-durable bytes. The **lower bound** is a new per-partition `flushed_offset` cursor. Read under `partition.log.lock()`, copy out the `Bytes`, drop the lock before awaiting the PUT.

### Combined object + Crabka-private framing

One object holds **many partitions'** verbatim runs — the inverse of KIP-405 (one object = one partition's sealed segment). The object is `<prefix?>/diskless-wal/<broker_id>/<flush_uuid>` (immutable per-flush key — no overwrite, sidestepping read-after-overwrite consistency; partitions are *not* in the key). Its body is a Crabka-private frame: `[magic+version header] · concatenated per-partition verbatim v2-batch runs · [footer manifest of (topic_id, partition, first_offset, last_offset, byte_start, byte_len) × N] · [footer_len][magic] trailer`. Footer-at-end because the manifest is only known after all runs accumulate — a single forward pass matching the streaming `WriteMultipart` shape. The concatenated runs stay byte-exact v2 batches (they self-delimit) so Slice-4 verbatim Fetch works. Written on the raw `Arc<dyn ObjectStore>` from `build_object_store`: `store.put(PutPayload::from(bytes))` for the common sub-8 MiB flush, `store.put_multipart` + `WriteMultipart::new_with_chunk_size` for backlog catch-up (mirroring `s3.rs:282-305` as a *pattern*, not a call). *Why a new path, not the RSM:* the RSM SPI keys every method on `RemoteLogSegmentMetadata → (topic_id, partition, uuid)` (`storage_manager.rs:81-139`) with no multi-partition seam, and its `CustomMetadata` is deliberately `Ok(None)` because keys are derivable — whereas the WAL's byte-ranges *must* be recorded. Bending the RSM would corrupt the KIP-405 contract `remote_reader.rs` depends on.

### Offset→object index on a new internal topic

The index lives on a new `__diskless_wal_index` internal topic, one `WalFlushRecord { object_key: String, format_version: u16, entries: Vec<WalIndexEntry { topic_id, partition, first_offset: i64, last_offset: i64, byte_start: u64, byte_len: u32 }> }` per flush object, reusing the **remote-storage-topic RLMM event-sourced-projection pattern** — `MetadataEventLog`/`KafkaMetadataEventLog` (`crates/remote-storage-topic/src/kafka_log.rs`: `ensure_topic:383`, `partition_fetch_loop:485`), the pump projection (`manager.rs:674-722`), read-your-writes `publish_and_wait` (`manager.rs:539-556`), snapshot + committed-offset resume (`manager.rs:317-334`), and the `NotReadyRlmm`/`SwappableRlmm` fail-closed boot facade. Per-broker partition assignment (`partitioning.rs:20-35`, `reconcile_assignment` `manager.rs:434-521`) fans the index load out across brokers.

*Why a topic, not KRaft records:* a per-flush index at 250 ms cadence × hundreds of partitions is high-volume, data-plane-frequency churn. KRaft records replay in full into every broker's resident `MetadataImage` and every snapshot and funnel through the single controller — that fits Slice 2's per-partition offset *counter*, but the byte-range index is the opposite shape and would make the controller a data-plane bottleneck. The topic partitions the load and lets each broker consume only its assignment. *Retention:* `cleanup.policy=compact` + tombstone-on-trim keyed by `object_key` (an object's entries are dropped once its offsets are trimmed away) — the RLMM's default `delete` + infinite retention would grow unbounded. *Replay is idempotent* (re-applying a `WalFlushRecord` is a no-op — mirror the RLMM benign-replay invariant, `manager.rs:281-289`).

### Lookup projection (built, consumed by Slice 4)

The projection keeps, per `(topic_id, partition)`, a `BTreeMap<first_offset, WalIndexEntry>`; the lookup "which object+range covers `(partition, offset)`" is `range(..=offset).next_back()` (greatest `first_offset ≤ offset`), verify `offset ≤ last_offset`, return `(object_key, byte_start, byte_len)` — the direct analog of `RemoteLogMetadataCache::segment_for` (`crates/remote-storage/src/cache.rs:118-133`). Slice 3 *builds and populates* this cache; **Slice 4 wires it into Fetch.**

### `flushed_offset` cursor + trim seam (gated off)

A new per-partition `flushed_offset` (durable-in-object frontier) advances **only after** the object PUT is durably committed **and** its `WalFlushRecord` is recorded — never trim past what is flushed. On a partial commit (PUT succeeds, index write fails) `flushed_offset` does **not** advance; the un-indexed object is a harmless orphan (GC-able later), not a coverage gap. Trimming reuses the existing, end-to-end-wired `WriterMessage::TrimToOffset` (`partition.rs:122-125`) → `Log::trim_to_offset` (`log.rs:1076-1134`, which also co-advances `local_log_start_offset`, `:1186-1190`) — **not** KIP-405's `delete_local_segments_through` (`log.rs:1263`), whose whole-segment, `CopySegmentFinished`-gated model doesn't fit offset/byte-range flush granularity. The gate `trim_target = min(flushed_offset, still_consumable_floor)` is **disabled by default** in Slice 3 (`FLUSH_TRIM_SAFETY_LAG` effectively infinite): with no object-read fallback until Slice 4, trimming a still-needed offset is permanent unreadability. Trim does not touch the ack path or consumer-visible offsets (HW is unaffected; only the low end of the readable range moves).

## Integration

- **`crates/broker/src/`** — a new flush-worker module (mirror `remote_log_manager.rs` structure); a per-partition `flushed_offset` cursor (home alongside the WAL state); the combined-object writer + footer codec; wiring the `__diskless_wal_index` producer/projection.
- **`crates/log/src/log.rs`** — no change to `read_raw`/`trim_to_offset`; reused as-is.
- **`crates/broker/src/partition.rs` / `partition_writer.rs`** — reuse `high_watermark()`, `trim_to_offset`/`WriterMessage::TrimToOffset` (unchanged; trim issued only when the gate opens, which is never by default this slice).
- **Object storage** — `build_object_store(cfg)` for the flush target; `object_store::memory::InMemory` as the test backend.
- **Index topic** — new `__diskless_wal_index`, provisioned like the RLMM `ensure_topic` (`kafka_log.rs:383`), with `cleanup.policy=compact`.
- **Ack path (`produce.rs`, `partition_writer` Produce arm, `WalStore`)** — **untouched.**

## Kafka / KIP compliance

- **Wire-compat inviolable.** The flush, the object framing, and the index topic are entirely internal; clients cannot observe them. Fetch still serves byte-exact records from the local log this slice.
- **Verbatim byte-exactness.** Flushed runs are unmodified v2 record batches (from `read_raw`), so Slice-4 fetch-from-object can serve them byte-for-byte.
- **AutoMQ-shaped durability.** Ack = local `fsync` (Slice 1); the object flush is a later async lifecycle step. This is the intended diskless durability model; the object copy strengthens it (Slice 6 adds the quorum medium).

## Testing

- **Faithful recoverability (behavior, not source):** write known verbatim v2 batches into several partitions' WAL tails, run a flush against an `InMemory` object store, `get` the object back, and reconstruct each partition's run using **both** the footer manifest and the external `WalFlushRecord`; assert byte-exact equality with what `read_raw` produced, and that footer and index agree on every `(tp, offset-range, byte-range)`.
- **Watermark never trims un-flushed:** assert `flushed_offset` advances only after a committed PUT + recorded index entry; inject a PUT failure and assert `flushed_offset` does not advance and no trim is issued; assert union coverage (local log ∪ committed objects) over `[log_start_offset, hw)` has no gap.
- **Index monotonicity + idempotent replay:** per-partition entries are non-overlapping and contiguous-forward (`entry.first_offset == prev.last_offset + 1` across successive flushes); the floor lookup returns the unique covering entry; re-applying a `WalFlushRecord` is a no-op.
- **Trigger correctness:** flush fires on `≥ 8 MiB` and independently on `≥ 250 ms` with pending bytes; an empty tick is a no-op.
- **Combine faithfulness:** one object carrying N partitions round-trips all N runs with the footer offsets exactly delimiting each run.
- **Ack path untouched:** produce latency/semantics are unchanged with or without a flush in flight; the flusher reads only `< hw` (never observes un-acked data).

## Risks (carried into the plan)

- **Combined-object framing** is new machinery with no analog today; footer off-by-one/endianness bugs silently corrupt Slice-4 fetch. Covered by the byte-exact round-trip test.
- **Index volume:** even on a topic, 250 ms × hundreds of partitions is high churn — `cleanup.policy=compact` + tombstone-on-trim + snapshot resume are required, not optional.
- **Trim-vs-consumer safety:** with no object-read until Slice 4, trimming is pure risk — hence disabled by default; the `still_consumable_floor` must be conservative when Slice 4 enables it.
- **Flush-worker failure:** a stuck worker grows the local WAL unbounded (disk-fill availability risk); needs bounded-backlog/backpressure + observability; a single spawned task per broker is an SPOF whose panic must surface.
- **Orphan objects:** a PUT-then-index-failure leaves an un-indexed object; Slice 3 leaves it as a harmless (GC-able) orphan by not advancing `flushed_offset` — full atomicity is Slice 5.

## Resolved decisions (from brainstorming)

- **Index location:** new internal topic `__diskless_wal_index` (RLMM event-sourced pattern), `cleanup.policy=compact` + tombstone-on-trim.
- **Flusher placement:** per-broker background worker, leader-filtered.
- **Write path:** new path on raw `Arc<dyn ObjectStore>` (not RSM); PUT-primitive extraction deferred.
- **Object layout:** immutable `diskless-wal/<broker_id>/<flush_uuid>` + header/runs/footer-manifest/trailer.
- **Combine scope:** all led diskless partitions with pending tail, rolled at the 8 MiB trigger.
- **Trigger:** `FLUSH_INTERVAL = 250 ms`, `FLUSH_MAX_BYTES = 8 MiB`.
- **Trim:** seam built, **disabled by default** (Slice 4 enables once object-read exists).
- **Ack path:** untouched; flush is async/background (AutoMQ-shaped).
