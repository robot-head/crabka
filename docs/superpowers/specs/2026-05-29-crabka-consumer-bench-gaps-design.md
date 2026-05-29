# Crabka — Consumer benchmark-gap fixes: Fetch-decode robustness + cold-coordinator retry

**Status:** Approved (design)
**Date:** 2026-05-29
**Scope:** `crabka-protocol` (records payload decode), `crabka-client-consumer`
(group-join + decode), supporting tests. No on-disk format or wire-format
changes.

## Background

The Kubernetes benchmark harness (`bench/`) runs a single Rust load driver
(`crates/bench-driver/`, built on `crabka-client-consumer` /
`crabka-client-producer`) **unmodified against both stacks** — Apache Kafka
via Strimzi and Crabka via its own operator. Running it surfaced two
consumer-side gaps that are invisible in crabka→crabka testing but break
crabka→Kafka and degrade failover behavior:

1. **Consumer Fetch-decode bug** — the consumer cannot decode Apache Kafka's
   Fetch responses when the final record batch is truncated.
2. **No retry on cold-coordinator errors** — a loading or relocating group
   coordinator is fatal to the consumer instead of being retried.

Both are Kafka-compatibility defects. Per `CLAUDE.md`, Kafka wire/behavior
compatibility is the constraint that matters; greenfield back-compat shims do
not apply here.

## Gap 1 — Tolerate a truncated trailing record batch on decode

### Root cause

`RecordsPayload::from_bytes`
([`crates/protocol/src/records/payload.rs:41`](../../../crates/protocol/src/records/payload.rs))
decodes the records field as a sequence:

```rust
let mut cur: &[u8] = &bytes;
let mut batches = Vec::new();
while !cur.is_empty() {
    batches.push(RecordBatch::decode(&mut cur)?); // errors on an incomplete tail
}
```

Apache Kafka legitimately returns a **partial final record batch** in a Fetch
response when a partition's byte budget (`partition_max_bytes`) is hit
mid-batch. The official Kafka consumer stops iterating when the remaining
bytes are insufficient for a complete batch and silently discards the
fragment, re-fetching it on the next request from the next offset.

Crabka's decoder instead propagates a decode error for the trailing fragment.
That bubbles up: `RecordsPayload::decode` → `FetchResponse` decode →
`Client::send` → `Consumer::poll` all return `Err`, stalling the consumer.

Crabka's *own* broker hides this because its decode-free pass-through
(`Log::read_raw`, [`crates/broker/src/handlers/fetch.rs`](../../../crates/broker/src/handlers/fetch.rs))
serves only whole v2 batches and excludes a partial trailing batch. So
crabka→crabka works and crabka→Kafka does not — exactly the asymmetry the
benchmark exposed.

### Fix

A **lenient** decode that checks each batch for completeness before decoding
and stops (dropping the remainder) at the first incomplete batch.

A v2 batch on the wire is `base_offset:i64` (8) + `batch_length:i32` (4) +
`batch_length` more bytes — total wire size `12 + batch_length`. Completeness
check, per iteration:

- If `cur.remaining() < 12`, there is not even a full header → stop.
- Peek `batch_length` (the `i32` at offset 8). If
  `cur.remaining() < 12 + batch_length` → incomplete trailing batch → stop and
  discard the remainder.
- Otherwise decode the batch and continue.

This matches Kafka's `nextBatch == null` termination semantics.

### Scoping — lenient applies to Fetch responses only

Strict decode **must remain strict for Produce-request validation**: a
truncated batch in an inbound Produce is `CORRUPT_MESSAGE`, not something to
silently trim. Produce requests are length-delimited, so a complete request
never contains a legitimately truncated trailing batch — leniency there would
let a malformed produce be partially accepted, a data-integrity regression.

Therefore the lenient behavior is exposed as a distinct decode entry point
(e.g. `RecordsPayload::from_fetch_bytes`, or a `lenient_trailing` flag) and is
used **only** where the client decodes a `FetchResponse` records field.
`from_bytes` (and the borrowed `from_slice`) used by the broker's Produce path
stay strict.

**Open mechanism (resolve in planning):** the `FetchResponse` codec is
generated and calls the generic `RecordsPayload::Decode` impl, which cannot
today distinguish "I am decoding a Fetch response" from "I am decoding a
Produce request." The plan must choose how to route Fetch-response records
through the lenient path — options include a Fetch-specific decode helper
invoked by the consumer, a thread-local/explicit decode mode, or a typed
wrapper. This is the single design risk to settle before implementation.

### Secondary: offset advance past fully-dropped batches

In [`crates/client-consumer/src/poll.rs:179-190`](../../../crates/client-consumer/src/poll.rs)
`next_offsets` is advanced only inside the per-record loop. When every batch in
a partition response is dropped (all control/aborted batches, or an empty
batch with `last_offset_delta > 0`), the offset is not advanced and the
consumer re-fetches the same offset indefinitely.

Advance `next_offsets` to the highest decoded batch's
`base_offset + last_offset_delta + 1` regardless of how many records were
emitted. Low benchmark impact (no scenario uses transactions or compaction)
but it is the same decode loop and closes a real stall.

## Gap 2 — Bounded retry on cold-coordinator errors

### Root cause

[`crates/client-core/src/lib.rs:39`](../../../crates/client-core/src/lib.rs)
declares automatic mid-request retry out of scope. As a result a group
coordinator that is loading (`COORDINATOR_LOAD_IN_PROGRESS` = 14), unavailable
(`COORDINATOR_NOT_AVAILABLE` = 15), or relocating (`NOT_COORDINATOR` = 16) is
fatal:

- `Consumer::build()`'s inline JoinGroup/SyncGroup
  ([`crates/client-consumer/src/consumer.rs:111-221`](../../../crates/client-consumer/src/consumer.rs))
  surfaces the error and the consumer never starts.
- `join_and_sync`
  ([`crates/client-consumer/src/coordinator.rs:374`](../../../crates/client-consumer/src/coordinator.rs))
  returns `ConsumerError::Server(code)` for any non-zero, non-79 code.

In the benchmark this kills the consumer task for an entire run; during the
`failover` scenario (coordinator moves to a freshly-elected broker) it shows up
as dropped messages.

Real Kafka clients retry FindCoordinator/JoinGroup/Heartbeat on these codes
with backoff up to a timeout.

### Fix

A bounded retry-with-backoff helper in `crabka-client-consumer`, applied to the
group-coordinator request path:

- **Retriable conditions:** error codes 14, 15, 16, and transient transport
  `ClientError::Disconnected` (the coordinator broker bouncing during
  failover).
- **Backoff:** exponential with jitter, base ~100 ms, cap ~1 s.
- **Deadline:** `coordinator_load_timeout`, defaulting to the client
  `request_timeout` (~30 s). On expiry, surface the last error unchanged.
- Introduce named consumer-side constants for 14/15/16 (the consumer currently
  uses bare numerals 22/25/27/79).

### Targeted refactor — unify the two join paths

There are currently two JoinGroup/SyncGroup implementations: the inline block
in `Consumer::build()` and `join_and_sync` in the coordinator task. Unify them
into one shared join helper so the retry logic lives in exactly one place and
both initial join and rejoin benefit.

Heartbeat's existing code-14 → `Transient` handling
([`crates/client-consumer/src/coordinator.rs:151`](../../../crates/client-consumer/src/coordinator.rs))
is retained, adjusted to retry a couple of times before falling back to waiting
a full heartbeat tick.

### Out of scope

Producer leader-failover retry (`NOT_LEADER_OR_FOLLOWER`, a different code path
through partition-leader resolution) is a separate concern from the group
coordinator and is not part of this work. Flag as a possible follow-up.

## Testing (TDD)

**Gap 1 (unit, socket-free):**
- A Fetch response with multiple complete batches plus a truncated trailing
  fragment decodes all complete batches and discards the fragment.
- A truncated **Produce** records field still errors strictly
  (`CORRUPT_MESSAGE` path) — guards the scoping.
- Offset-advance past a partition whose batches are all dropped/empty.

**Gap 2 (mock/in-memory coordinator):**
- A coordinator returning code 14 N times then 0 — `build()` and rejoin
  succeed after backoff.
- Exceeding `coordinator_load_timeout` surfaces the last error.
- The `bench-driver` is left unchanged: once the library retries, no outer
  retry loop is needed in `run_consumer`.

## Non-goals

- No on-disk or Kafka wire-format changes.
- No producer-side coordinator/leader retry.
- No new benchmark scenarios; the existing harness is the validation vehicle.
