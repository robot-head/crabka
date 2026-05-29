# Consumer Benchmark-Gap Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the two consumer gaps the benchmark surfaced — the consumer choking on Apache Kafka's truncated trailing record batch, and the consumer dying on cold/relocating group-coordinator errors.

**Architecture:** Gap 1 splits records-field decoding by message direction in the code generator: **response** records (Fetch/FetchSnapshot/ShareFetch) decode leniently (drop an incomplete trailing batch, matching the JVM consumer); **request** records (Produce) stay strict (a truncated batch is still `CORRUPT_MESSAGE`). Gap 2 adds one bounded retry-with-backoff helper in `crabka-client-consumer`, applied at every group-coordinator RPC so codes 14/15/16 and transient disconnects are retried instead of fatal.

**Tech Stack:** Rust, `bytes`, `zerocopy` (RecordBatchHeader), `tokio` (time), the in-house `crabka-protocol-codegen` generator. Spec: `docs/superpowers/specs/2026-05-29-crabka-consumer-bench-gaps-design.md`.

---

## Execution batching

Two independent tracks. Per `CLAUDE.md`, dispatch non-overlapping tasks in parallel batches.

- **Batch 1 (parallel):** Task 1 (`crates/protocol/src/records/payload.rs`) and Task 5 (`crates/client-consumer/src/coordinator.rs`) — disjoint files.
- **Batch 2 (parallel):** Task 2 (codegen + regenerated files; needs Task 1), Task 3 (`crates/client-consumer/src/poll.rs`; independent), Task 6 (`crates/client-consumer/src/consumer.rs`; needs Task 5).
- **Batch 3:** Task 7 (`crates/client-consumer/src/coordinator.rs`; needs Task 5, same file as it so sequential), Task 4 (`crates/client-consumer/tests/`; needs Task 2).

Run `cargo fmt` before every commit (CI gates on `cargo fmt --check`).

---

# Track A — Gap 1: Fetch-decode robustness

## Task 1: Lenient records parse on `RecordsPayload`

**Files:**
- Modify: `crates/protocol/src/records/payload.rs`
- Test: `crates/protocol/src/records/payload.rs` (inline `#[cfg(test)] mod tests`)

Background: `RecordBatch::decode` (`crates/protocol/src/records/owned.rs:376`) returns `RecordsError::HeaderTooShort` / `RecordsError::BodyTooShort` when the buffer holds fewer bytes than a complete batch needs, and other errors (`CrcMismatch`, `UnsupportedMagic`, `RecordParse`, `ZerocopyFailure`) for corrupt-but-complete data. We treat the "too short" pair as an incomplete trailing batch to drop; everything else still errors.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module at the bottom of `crates/protocol/src/records/payload.rs`:

```rust
    #[test]
    fn from_fetch_bytes_drops_incomplete_trailing_batch() {
        // Two complete batches followed by a truncated third (only a few
        // bytes of its header). Kafka sends this when the partition byte
        // budget cuts the final batch; the consumer must keep the two
        // complete batches and drop the fragment.
        let mut b0 = sample_v2();
        b0.base_offset = 0;
        let mut b1 = sample_v2();
        b1.base_offset = 1;
        let mut buf = BytesMut::new();
        b0.encode(&mut buf).unwrap();
        b1.encode(&mut buf).unwrap();
        buf.extend_from_slice(&[0u8; 7]); // partial trailing batch header
        let p = RecordsPayload::from_fetch_bytes(buf.freeze()).unwrap();
        let batches = p.as_v2().expect("v2");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].base_offset, 0);
        assert_eq!(batches[1].base_offset, 1);
    }

    #[test]
    fn from_fetch_bytes_still_errors_on_corrupt_batch() {
        // A complete-looking batch whose CRC is wrong must still error, even
        // leniently — leniency only forgives truncation, not corruption.
        let rb = sample_v2();
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        let mut bytes = buf.to_vec();
        // Corrupt a body byte after the header (HEADER_LEN = 61) to break CRC.
        bytes[61] ^= 0xFF;
        let err = RecordsPayload::from_fetch_bytes(Bytes::from(bytes)).unwrap_err();
        assert!(matches!(err, RecordsError::CrcMismatch { .. }));
    }

    #[test]
    fn from_fetch_bytes_legacy_passes_through() {
        let bytes = legacy_bytes();
        let p = RecordsPayload::from_fetch_bytes(bytes.clone()).unwrap();
        assert_eq!(p.as_legacy(), Some(&bytes));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-protocol from_fetch_bytes`
Expected: FAIL — `from_fetch_bytes` is not a method on `RecordsPayload`.

- [ ] **Step 3: Implement `from_fetch_bytes` + `decode_lenient`**

In `crates/protocol/src/records/payload.rs`, inside `impl RecordsPayload` (next to `from_bytes`), add:

```rust
    /// Decode a **response-side** records field, tolerating a truncated
    /// trailing batch. Kafka returns a partial final `RecordBatch` when a
    /// partition's fetch byte budget is hit mid-batch; the JVM consumer stops
    /// at the first incomplete batch and re-fetches it from the next offset.
    /// We mirror that: decode every complete batch, and on the first
    /// `HeaderTooShort` / `BodyTooShort` stop and drop the remainder. A
    /// *corrupt* complete batch (bad CRC/magic/content) still errors — leniency
    /// forgives truncation only. Strict [`from_bytes`](Self::from_bytes) is
    /// retained for Produce-request validation.
    pub fn from_fetch_bytes(bytes: Bytes) -> Result<Self, RecordsError> {
        if !looks_like_v2(&bytes) {
            return Ok(Self::Legacy(bytes));
        }
        let mut cur: &[u8] = &bytes;
        let mut batches = Vec::new();
        while !cur.is_empty() {
            match RecordBatch::decode(&mut cur) {
                Ok(rb) => batches.push(rb),
                Err(RecordsError::HeaderTooShort { .. } | RecordsError::BodyTooShort { .. }) => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Self::V2(batches))
    }

    /// `Decode`-shaped lenient entry point the generated codec calls for
    /// records fields in **response** messages. Consumes the whole sliced
    /// field buffer (the caller has already framed it) and parses leniently
    /// via [`from_fetch_bytes`](Self::from_fetch_bytes).
    pub fn decode_lenient<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, crate::ProtocolError> {
        let bytes = buf.copy_to_bytes(buf.remaining());
        Self::from_fetch_bytes(bytes).map_err(Into::into)
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-protocol records::payload`
Expected: PASS (the three new tests plus the existing payload tests).

- [ ] **Step 5: Format + commit**

```bash
cargo fmt -p crabka-protocol
git add crates/protocol/src/records/payload.rs
git commit -m "feat(protocol): lenient RecordsPayload decode for truncated trailing Fetch batches"
```

---

## Task 2: Codegen request/response split for records fields

**Files:**
- Modify: `crates/protocol-codegen/src/emit/owned.rs`
- Regenerate (do not hand-edit): `crates/protocol/generated/**` and `crates/protocol/src/{owned,common}/**`
- Snapshots: `crates/protocol-codegen/tests/snapshots/**`

Depends on Task 1 (`RecordsPayload::decode_lenient` must exist or the regenerated code won't compile).

The records-field decode is emitted by `decode_call` in `crates/protocol-codegen/src/emit/owned.rs` (the two `("records", _)` arms near line 1655). The records field lives in **common structs** (`PartitionData` for FetchResponse, `PartitionProduceData` for ProduceRequest); both the primary-struct decode (`emit_decode_impl`) and the common-struct decode (`emit_common_struct_file`) are emitted from within `pub fn emit(spec, ...)` (line 27), where `spec.message_type` is available. We thread a `lenient_records: bool` (true iff the message is a `Response`) down to `decode_call`.

- [ ] **Step 1: Thread `lenient_records` from `emit()` down to `decode_call`**

Read these functions first (they form the call chain): `emit` (line 27), `emit_decode_impl` (757), `emit_common_struct_file` (94), `emit_decode_one` (1137), `decode_call_with_buf` (1396), `decode_call` (1593).

Edits:

1. In `emit()`, immediately after `let res_map = ...`, add:

```rust
    let lenient_records = matches!(spec.message_type, crate::ir::MessageType::Response);
```

(`MessageType` is already imported at the top of the file via `use crate::ir::{... MessageType ...}`.)

2. Add a `lenient_records: bool` parameter to each of `emit_decode_impl`, `emit_common_struct_file`, `emit_decode_one`, and `decode_call_with_buf`, and to `decode_call`. Pass `lenient_records` through unchanged at every call site:
   - `emit()` → `emit_decode_impl(&mut primary, spec, &res_map, lenient_records)`
   - `emit()` → `emit_common_struct_file(&cs.name, &cs.fields, cs_flex_min, &common_res_map, schemas_version, lenient_records)`
   - inside `emit_decode_impl` and `emit_common_struct_file`: forward `lenient_records` to each `emit_decode_one(...)` / `decode_call(...)` call
   - inside `emit_decode_one`: forward to `decode_call(&f.field_type, is_nullable(f), res_map, lenient_records)` and the nullable-split `decode_call(...)` calls
   - inside `decode_call_with_buf`: forward to its inner `decode_call(...)`
   - inside `decode_call`: recursive array calls pass `lenient_records` through (records is never nested in an array, so the value is immaterial there, but keep it consistent).

- [ ] **Step 2: Branch the `decode_call` records arms on `lenient_records`**

In `decode_call`, replace the two existing `("records", false)` and `("records", true)` arms with leniency-aware versions:

```rust
        ("records", false) => if lenient_records {
            "{ \
                let __rb_bytes = if flex { get_compact_bytes_owned(buf)? } else { get_bytes_owned(buf)? }; \
                let mut __rb_cur: &[u8] = &__rb_bytes; \
                crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)? \
            }".into()
        } else {
            "{ \
                let __rb_bytes = if flex { get_compact_bytes_owned(buf)? } else { get_bytes_owned(buf)? }; \
                let mut __rb_cur: &[u8] = &__rb_bytes; \
                <crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)? \
            }".into()
        },
        ("records", true) => if lenient_records {
            "{ \
                let __rb_opt = if flex { get_compact_nullable_bytes_owned(buf)? } else { get_nullable_bytes_owned(buf)? }; \
                match __rb_opt { \
                    None => None, \
                    Some(__rb_bytes) => { \
                        let mut __rb_cur: &[u8] = &__rb_bytes; \
                        Some(crate::records::RecordsPayload::decode_lenient(&mut __rb_cur, version)?) \
                    } \
                } \
            }".into()
        } else {
            "{ \
                let __rb_opt = if flex { get_compact_nullable_bytes_owned(buf)? } else { get_nullable_bytes_owned(buf)? }; \
                match __rb_opt { \
                    None => None, \
                    Some(__rb_bytes) => { \
                        let mut __rb_cur: &[u8] = &__rb_bytes; \
                        Some(<crate::records::RecordsPayload as crate::Decode>::decode(&mut __rb_cur, version)?) \
                    } \
                } \
            }".into()
        },
```

> Assumption (holds for Kafka schemas): no records-bearing **common struct** is shared between a request and a response message (Produce uses `PartitionProduceData`, Fetch uses `PartitionData`), so a single `lenient_records` value per common struct is unambiguous.

- [ ] **Step 3: Build the codegen crate**

Run: `cargo build -p crabka-protocol-codegen`
Expected: compiles. (Fix any missed call site the compiler flags — it will name the function missing the new argument.)

- [ ] **Step 4: Regenerate the protocol sources**

Run: `bash tools/regenerate.sh`
Expected: prints "Regenerated." and rewrites files under `crates/protocol/generated` and `crates/protocol/src`.

- [ ] **Step 5: Verify the split landed correctly**

Run:
```bash
grep -n "decode_lenient" crates/protocol/generated/common/owned/PartitionData.owned.rs
grep -n "RecordsPayload as crate::Decode>::decode" crates/protocol/generated/common/owned/PartitionProduceData.owned.rs
```
Expected: `PartitionData` (Fetch response) decode uses `RecordsPayload::decode_lenient`; `PartitionProduceData` (Produce request) decode still uses the strict `<RecordsPayload as crate::Decode>::decode`. If the second grep returns nothing, confirm the file/struct name with `ls crates/protocol/generated/common/owned/ | grep -i produce` and re-grep.

- [ ] **Step 6: Refresh and run codegen snapshots**

Run:
```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen
cargo test -p crabka-protocol-codegen
```
Expected: snapshots refresh on the first run, all green on the second.

- [ ] **Step 7: Build the protocol crate against regenerated sources**

Run: `cargo build -p crabka-protocol && cargo test -p crabka-protocol`
Expected: compiles (proves `decode_lenient` wired in correctly) and existing protocol tests pass.

- [ ] **Step 8: Format + commit**

```bash
cargo fmt
git add crates/protocol-codegen/src/emit/owned.rs crates/protocol-codegen/tests/snapshots crates/protocol/generated crates/protocol/src
git commit -m "feat(codegen): decode response records leniently, keep request records strict"
```

---

## Task 3: Advance fetch offset past fully-dropped batches

**Files:**
- Modify: `crates/client-consumer/src/poll.rs:103-194`
- Test: `crates/client-consumer/src/poll.rs` (inline test) — see Step 1.

Today `next_offsets` is bumped only inside the per-record loop (`poll.rs:189`). When a partition's batches are all dropped (control/aborted) or a decoded batch spans more offsets than it has surviving records, the offset never advances and the consumer re-fetches the same offset forever. Advance to the highest decoded batch's `base_offset + last_offset_delta + 1` regardless of how many records were emitted. (A truncated trailing batch dropped by lenient decode is simply absent from `batches`, so it is correctly re-fetched next poll.)

- [ ] **Step 1: Write the failing test**

Add a `#[cfg(test)] mod tests` block (or extend an existing one) at the bottom of `crates/client-consumer/src/poll.rs`. This exercises the pure offset-advance helper introduced in Step 3, so it does not need a live broker:

```rust
#[cfg(test)]
mod offset_advance_tests {
    use crabka_protocol::records::{RecordBatch, RecordsPayload};

    #[test]
    fn advance_target_uses_last_offset_delta_not_record_count() {
        // A batch spanning offsets 10..=14 (last_offset_delta = 4) but carrying
        // zero surviving records must still advance the fetch offset to 15.
        let batch = RecordBatch {
            base_offset: 10,
            last_offset_delta: 4,
            records: vec![],
            ..Default::default()
        };
        let payload = RecordsPayload::V2(vec![batch]);
        let batches = payload.as_v2().unwrap();
        assert_eq!(super::next_offset_after(batches), Some(15));
    }

    #[test]
    fn advance_target_none_for_empty() {
        let payload = RecordsPayload::V2(vec![]);
        assert_eq!(super::next_offset_after(payload.as_v2().unwrap()), None);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-client-consumer next_offset_after`
Expected: FAIL — `next_offset_after` is not defined.

- [ ] **Step 3: Add the helper and use it in `poll()`**

In `crates/client-consumer/src/poll.rs`, add this free function above `impl Consumer`:

```rust
/// The offset to fetch next after consuming `batches`: one past the highest
/// `base_offset + last_offset_delta` across all decoded batches. `None` when
/// there are no batches (offset unchanged). Used so the consumer advances past
/// control/aborted batches that emit no records, instead of re-fetching them.
fn next_offset_after(batches: &[crabka_protocol::records::RecordBatch]) -> Option<i64> {
    batches
        .iter()
        .map(|b| b.base_offset + i64::from(b.last_offset_delta) + 1)
        .max()
}
```

Then, in the decode loop, remove the per-record `offsets.insert(...)` at `poll.rs:189` and set the offset once per partition from the helper. Concretely, delete this line inside the `for r in &batch.records` loop:

```rust
                        offsets.insert((topic_name.clone(), part.partition_index), offset + 1);
```

and, immediately after the `for batch in batches { ... }` loop closes (still inside the `for part in &topic.partitions` body, after the partition's batches are processed), add:

```rust
                if let Some(next) = next_offset_after(batches) {
                    offsets.insert((topic_name.clone(), part.partition_index), next);
                }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-consumer next_offset_after offset_advance`
Expected: PASS.

- [ ] **Step 5: Format + commit**

```bash
cargo fmt -p crabka-client-consumer
git add crates/client-consumer/src/poll.rs
git commit -m "fix(consumer): advance fetch offset past control/empty batches via last_offset_delta"
```

---

## Task 4: End-to-end lenient-decode regression test

**Files:**
- Create: `crates/client-consumer/tests/truncated_fetch_decode.rs`

This proves the wire path: a `FetchResponse` whose records field ends in a truncated batch decodes (via the regenerated lenient codec) without error, keeping the complete batches. Depends on Task 2.

- [ ] **Step 1: Write the test**

```rust
//! Regression: a Fetch response with a truncated trailing record batch (as
//! Apache Kafka returns when a partition byte budget is hit) must decode the
//! complete batches and drop the fragment, rather than failing the whole
//! response decode and stalling the consumer.

use bytes::{Bytes, BytesMut};
use crabka_protocol::owned::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use crabka_protocol::records::{Record, RecordBatch, RecordsPayload};
use crabka_protocol::{Decode, Encode};

fn batch(base_offset: i64, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn fetch_response_with_truncated_trailing_batch_decodes_complete_batches() {
    // Build the records-field bytes by hand: one complete batch + a truncated
    // second batch (only part of its header).
    let mut field = BytesMut::new();
    batch(0, b"hello").encode(&mut field).unwrap();
    field.extend_from_slice(&[0u8; 9]); // partial trailing batch

    let resp = FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic: "t".into(),
            partitions: vec![PartitionData {
                partition_index: 0,
                high_watermark: 2,
                records: Some(RecordsPayload::Raw(field.freeze())),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Encode at a modern Fetch version, then decode — the decode path is the
    // one the consumer uses, and must tolerate the truncated tail.
    let version = 13;
    let mut wire = BytesMut::new();
    resp.encode(&mut wire, version).unwrap();
    let mut cur: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cur, version).expect("lenient decode");

    let part = &decoded.responses[0].partitions[0];
    let batches = part.records.as_ref().unwrap().as_v2().expect("v2");
    assert_eq!(batches.len(), 1, "complete batch kept, fragment dropped");
    assert_eq!(batches[0].base_offset, 0);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p crabka-client-consumer --test truncated_fetch_decode`
Expected: PASS. (If `RecordsPayload::Raw` round-trips through encode to the exact field bytes — it does, per `payload.rs` `raw_passthrough_roundtrips` — the decoded side re-parses them leniently.)

- [ ] **Step 3: Commit**

```bash
git add crates/client-consumer/tests/truncated_fetch_decode.rs
git commit -m "test(consumer): truncated trailing Fetch batch decodes without stalling"
```

---

# Track B — Gap 2: Cold-coordinator retry

## Task 5: Coordinator-retry helper + error-code constants

**Files:**
- Modify: `crates/client-consumer/src/coordinator.rs`
- Test: `crates/client-consumer/src/coordinator.rs` (inline `#[cfg(test)] mod tests`)

The helper retries a single coordinator RPC on the three retriable coordinator codes and on transient disconnects, with capped exponential backoff, until a deadline; on deadline it surfaces the last response/error so the caller's existing error handling runs. Backoff is deterministic (no jitter) so it is testable under `tokio::time` pause; jitter is unnecessary for the single-/low-cardinality consumer counts in scope and can be added later if a thundering herd ever shows up.

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `crates/client-consumer/src/coordinator.rs`:

```rust
#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Resp {
        error_code: i16,
    }

    #[tokio::test(start_paused = true)]
    async fn retries_until_coordinator_finishes_loading() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(Duration::from_secs(30), |r: &Resp| r.error_code, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                // COORDINATOR_LOAD_IN_PROGRESS (14) thrice, then success.
                Ok::<_, ConsumerError>(Resp { error_code: if n < 3 { 14 } else { 0 } })
            }
        })
        .await
        .unwrap();
        assert_eq!(r.error_code, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn surfaces_last_response_after_deadline() {
        let r = with_coordinator_retry(Duration::from_secs(1), |r: &Resp| r.error_code, || async {
            Ok::<_, ConsumerError>(Resp { error_code: 15 })
        })
        .await
        .unwrap();
        // Deadline hit while still retriable: return the last response so the
        // caller's `error_code != 0` handling surfaces it.
        assert_eq!(r.error_code, 15);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retriable_code_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let r = with_coordinator_retry(Duration::from_secs(30), |r: &Resp| r.error_code, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, ConsumerError>(Resp { error_code: 25 }) } // UNKNOWN_MEMBER_ID
        })
        .await
        .unwrap();
        assert_eq!(r.error_code, 25);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-client-consumer retry_tests`
Expected: FAIL — `with_coordinator_retry` and the constants are undefined.

- [ ] **Step 3: Add constants + helper**

Near the top of `crates/client-consumer/src/coordinator.rs` (after the `use` block), add:

```rust
/// Retriable group-coordinator error codes. The coordinator is loading its
/// state (`14`), not yet available (`15`), or has moved to another broker
/// (`16`). Kafka clients retry these with backoff rather than failing.
pub(crate) const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub(crate) const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub(crate) const NOT_COORDINATOR: i16 = 16;

/// How long `with_coordinator_retry` keeps retrying a cold coordinator before
/// surfacing the last error. Matches a typical client `request.timeout.ms`.
pub(crate) const COORDINATOR_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

fn is_retriable_coordinator_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

/// Send a group-coordinator RPC, retrying on cold-coordinator codes
/// (14/15/16) and transient `Disconnected` transport errors with capped
/// exponential backoff until `timeout` elapses. `make` rebuilds the request
/// each attempt (so it can be re-sent); `code` reads the response's
/// `error_code`. On deadline, returns the last response (so the caller's
/// `error_code` handling runs) or `CoordinatorUnavailable` if the last attempt
/// was a disconnect.
pub(crate) async fn with_coordinator_retry<R, F, Fut>(
    timeout: Duration,
    code: impl Fn(&R) -> i16,
    make: F,
) -> Result<R, ConsumerError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<R, ConsumerError>>,
{
    let start = tokio::time::Instant::now();
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_millis(1000);
    loop {
        match make().await {
            Ok(r) if !is_retriable_coordinator_code(code(&r)) => return Ok(r),
            Ok(r) => {
                if start.elapsed() >= timeout {
                    return Ok(r);
                }
            }
            Err(ConsumerError::Client(crabka_client_core::ClientError::Disconnected)) => {
                if start.elapsed() >= timeout {
                    return Err(ConsumerError::CoordinatorUnavailable);
                }
            }
            Err(e) => return Err(e),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-consumer retry_tests`
Expected: PASS (all three; `start_paused` auto-advances the backoff sleeps so the tests finish instantly).

- [ ] **Step 5: Format + commit**

```bash
cargo fmt -p crabka-client-consumer
git add crates/client-consumer/src/coordinator.rs
git commit -m "feat(consumer): bounded retry helper for cold-coordinator errors"
```

---

## Task 6: Apply coordinator retry in `Consumer::build()`

**Files:**
- Modify: `crates/client-consumer/src/consumer.rs:119-235`

Depends on Task 5. The initial JoinGroup (`r1`), the second JoinGroup (`r2`), and the SyncGroup (`r3`) currently `client.send(...).await?` once; wrap each in `with_coordinator_retry` so a cold coordinator at startup is retried instead of killing the consumer build. The `use` for the helper: add `use crate::coordinator::{with_coordinator_retry, COORDINATOR_RETRY_TIMEOUT};` to the imports if not already present.

- [ ] **Step 1: Wrap the first JoinGroup**

Replace the `let r1 = client.send(JoinGroupRequest { ... }).await?;` block (`consumer.rs:119-133`) with:

```rust
        // 1. First JoinGroup — empty member_id, expect MEMBER_ID_REQUIRED (79)
        //    or a regular response; either way the broker hands us a member_id.
        //    Retry a cold/relocating coordinator (14/15/16) with backoff.
        let r1 = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
            let group_id = group_id.clone();
            let protocol_name = protocol_name.clone();
            let subscription_bytes = subscription_bytes.clone();
            let client = &client;
            async move {
                client
                    .send(JoinGroupRequest {
                        group_id,
                        protocol_type: "consumer".into(),
                        member_id: String::new(),
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocols: vec![JoinGroupRequestProtocol {
                            name: protocol_name,
                            metadata: subscription_bytes,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .map_err(ConsumerError::from)
            }
        })
        .await?;
```

- [ ] **Step 2: Wrap the second JoinGroup**

Replace the `let r2 = client.send(JoinGroupRequest { ... }).await?;` block (`consumer.rs:146-161`) with the same shape, using `member_id.clone()`:

```rust
        // 2. Second JoinGroup with the assigned member_id.
        let r2 = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
            let group_id = group_id.clone();
            let protocol_name = protocol_name.clone();
            let subscription_bytes = subscription_bytes.clone();
            let member_id = member_id.clone();
            let client = &client;
            async move {
                client
                    .send(JoinGroupRequest {
                        group_id,
                        protocol_type: "consumer".into(),
                        member_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocols: vec![JoinGroupRequestProtocol {
                            name: protocol_name,
                            metadata: subscription_bytes,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .map_err(ConsumerError::from)
            }
        })
        .await?;
```

- [ ] **Step 3: Wrap the SyncGroup**

Replace the `let r3 = client.send(SyncGroupRequest { ... }).await?;` block (`consumer.rs:221-234`) with:

```rust
        // 4. SyncGroup — leader installs assignments; everyone receives
        //    their own assignment in the response. Retry a cold coordinator.
        let r3 = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
            let group_id = group_id.clone();
            let protocol_name = protocol_name.clone();
            let member_id = member_id.clone();
            let assignments_for_sync = assignments_for_sync.clone();
            let generation_id = r2.generation_id;
            let client = &client;
            async move {
                client
                    .send(SyncGroupRequest {
                        group_id,
                        generation_id,
                        member_id,
                        protocol_type: Some("consumer".into()),
                        protocol_name: Some(protocol_name),
                        assignments: assignments_for_sync,
                        ..Default::default()
                    })
                    .await
                    .map_err(ConsumerError::from)
            }
        })
        .await?;
```

> `assignments_for_sync` must be cloned per attempt because the closure is `Fn`. `SyncGroupRequestAssignment` derives `Clone` (generated). If the borrow checker complains that `assignments_for_sync` is moved, confirm it is no longer used after this block (it isn't) and the per-attempt `.clone()` inside the closure satisfies `Fn`.

- [ ] **Step 4: Build + run the consumer integration tests**

Run: `cargo test -p crabka-client-consumer`
Expected: compiles and the existing integration tests (which spin a real broker) still pass — the retry wrapper is transparent when the coordinator answers on the first attempt.

- [ ] **Step 5: Format + commit**

```bash
cargo fmt -p crabka-client-consumer
git add crates/client-consumer/src/consumer.rs
git commit -m "fix(consumer): retry cold-coordinator errors during initial group join"
```

---

## Task 7: Apply coordinator retry in `join_and_sync` (rejoin path)

**Files:**
- Modify: `crates/client-consumer/src/coordinator.rs:352-375`

Depends on Task 5 (same file). The rejoin path's first join (`r1`), the second join inside the `79` branch (`r2`), and the SyncGroup later in `join_and_sync` must use the retry helper too, so a coordinator that relocates during a rebalance (e.g. the `failover` benchmark scenario) is retried instead of failing the rejoin. The heartbeat path already treats unexpected codes (including 14) as `HeartbeatOutcome::Transient` and retries on the next tick, so it needs no change.

- [ ] **Step 1: Wrap the rejoin joins**

In `join_and_sync`, replace the first-join block (`coordinator.rs:353-374`, from `let r1 = state.client.send(make_join(...))` through the closing `};` of the `join_resp` binding) with a retry-wrapped version. Because `make_join` already builds the request from a member_id argument, the closure is small:

```rust
    // First join: if we have no member_id, expect MEMBER_ID_REQUIRED (79) and
    // capture the broker-assigned id, then issue a second join. Retry a cold or
    // relocating coordinator (14/15/16) with backoff on each send.
    let r1 = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
        let req = make_join(state.member_id.clone());
        let client = &state.client;
        async move { client.send(req).await.map_err(ConsumerError::from) }
    })
    .await?;
    let join_resp = if r1.error_code == 0 {
        r1
    } else if r1.error_code == 79 {
        let assigned_id = r1.member_id.clone();
        if assigned_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed(
                "broker did not assign a member_id".into(),
            ));
        }
        state.member_id.clone_from(&assigned_id);
        let r2 = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
            let req = make_join(assigned_id.clone());
            let client = &state.client;
            async move { client.send(req).await.map_err(ConsumerError::from) }
        })
        .await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }
        r2
    } else {
        return Err(ConsumerError::Server(r1.error_code));
    };
```

> `make_join` must be a closure/fn that returns a fresh `JoinGroupRequest` each call (it already is — it's invoked twice today). Capturing `&state.client` by reference inside the `Fn` is fine. If `make_join` borrows `state` and that conflicts with the `&state.client` borrow, inline the request construction into the closure instead (mirror the field set from the existing `make_join`).

- [ ] **Step 2: Wrap the rejoin SyncGroup**

Find the `SyncGroup` send in `join_and_sync` (search for `SyncGroupRequest` below the assignment computation in `coordinator.rs`). Wrap it identically:

```rust
    let sync_resp = with_coordinator_retry(COORDINATOR_RETRY_TIMEOUT, |r| r.error_code, || {
        let group_id = state.group_id.clone();
        let member_id = state.member_id.clone();
        let chosen_protocol = chosen_protocol.clone();
        let assignments_for_sync = assignments_for_sync.clone();
        let client = &state.client;
        async move {
            client
                .send(SyncGroupRequest {
                    group_id,
                    generation_id,
                    member_id,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some(chosen_protocol),
                    assignments: assignments_for_sync,
                    ..Default::default()
                })
                .await
                .map_err(ConsumerError::from)
        }
    })
    .await?;
```

Match the field names to the existing `SyncGroupRequest` literal already in `join_and_sync` (read it first; the variable names `generation_id`, `chosen_protocol`, `assignments_for_sync` are defined earlier in that function). Keep whatever the code does next with the sync response (assignment decode) unchanged, renaming the binding if the existing one differs.

- [ ] **Step 3: Build + test**

Run: `cargo test -p crabka-client-consumer`
Expected: compiles and existing rebalance/cooperative tests pass (retry is transparent on the happy path).

- [ ] **Step 4: Format + commit**

```bash
cargo fmt -p crabka-client-consumer
git add crates/client-consumer/src/coordinator.rs
git commit -m "fix(consumer): retry cold/relocating coordinator during rebalance rejoin"
```

---

# Final verification

- [ ] **Step 1: Full workspace build + test**

Run: `cargo test --workspace`
Expected: green.

- [ ] **Step 2: Format + lint gates (CI parity)**

Run:
```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```
Expected: no diffs, no warnings.

- [ ] **Step 3: Confirm the regenerated protocol files are committed**

Run: `git status --porcelain crates/protocol/generated crates/protocol/src`
Expected: empty (everything regenerated in Task 2 is committed).

---

## Spec coverage check

- Gap 1 root cause (truncated trailing batch) → Tasks 1, 2, 4.
- Gap 1 "lenient on Fetch responses only, Produce stays strict" → Task 2 (request/response split) + Task 1 (strict `from_bytes` retained).
- Gap 1 secondary offset-advance → Task 3.
- Gap 2 retry on codes 14/15/16 + transient `Disconnected` with bounded backoff → Tasks 5, 6, 7.
- Gap 2 named constants for 14/15/16 → Task 5.
- Heartbeat code-14 handling → unchanged (already `Transient`/retry-next-tick); noted in Task 7.
- Out of scope (producer leader-failover retry; full join-orchestration merge) → not included; retry centralized in the single `with_coordinator_retry` helper applied at all coordinator RPC sites, which satisfies the spec's "retry in one place / both paths benefit" intent without the riskier orchestration merge.
