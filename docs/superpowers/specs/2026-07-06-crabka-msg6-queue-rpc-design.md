# MSG-6: The gateway queue surface (share-group RPC) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The messaging cycle's sixth slice — the net-new gateway work MSG-5 flagged and the [application-SDK umbrella](2026-07-06-crabka-app-sdk-umbrella-design.md) pinned as `gated_on: "gateway-sharegroup-rpc"`. Landing this flips the `queues` module from stub to live across **all five SDKs** via one contract minor bump.

## Context — the differentiator finally gets a door

The broker's KIP-932 stack is fully built and JVM-validated (`ShareFetch`/`ShareAcknowledge`, the `AcquisitionState` machine, `delivery_count`, lock expiry with automatic redelivery, poison-pill archiving), and the native Rust `ShareConsumer` exists (`crates/client-consumer/src/share/`: `poll`, explicit `acknowledge(record, Accept|Release|Reject)`, staged-ack `commit`, `renew`, `close`). What's missing is exactly one thing: **a gateway surface**, so non-Kafka-wire clients (the five SDKs, serverless functions) can consume queues. This slice adds it — a thin adapter from Connect RPCs onto the native `ShareConsumer`, no broker changes.

## Design Goals

- **Unary RPCs, pull-shaped** — `QueueAcquire` / `QueueAcknowledge` / `QueueRenew` — matching the umbrella's pull-shaped `queues` API and working over **plain HTTP/1.1** (no bidi, no h2c requirement: the queue module becomes the easiest surface for every SDK, including C++ and any future browser story).
- **Safety by broker semantics, not gateway heroics:** the gateway holds a `ShareConsumer` per session, but a lost/idle/crashed session needs no cleanup protocol — **un-acked acquisition locks expire broker-side (group-configured, default 30 s) and records redeliver with `delivery_count` incremented**. The KIP-932 state machine is the safety net; the gateway session table is just an optimization.
- **Byte-exact delivery incl. headers:** queue messages carry key/value/headers verbatim — which requires the one native-crate addition this slice owns (below).
- **Server-enforced Read ACL** on group + topics, exactly as `Subscribe` does.

## The one native-crate addition: `ShareConsumerRecord.headers`

`ShareConsumerRecord` today carries `topic/partition/offset/timestamp/key/value/delivery_count` — **no headers** (verified), the same gap MSG-1 closed on the classic path. A CloudEvent consumed as a queue message needs its `ce_*` headers, so this slice adds `headers: Vec<(String, Option<Bytes>)>` to `ShareConsumerRecord` and materializes them in the share poll path — MSG-1 is the exact precedent (lossless internally; the proto map's documented null→empty / dup→last-wins policy at the gRPC hop).

## The RPC surface (gateway.proto additions)

```proto
rpc QueueAcquire(QueueAcquireRequest) returns (QueueAcquireResponse);
rpc QueueAcknowledge(QueueAcknowledgeRequest) returns (QueueAcknowledgeResponse);
rpc QueueRenew(QueueRenewRequest) returns (QueueRenewResponse);

message QueueAcquireRequest {
  string group_id = 1;
  repeated string topics = 2;
  uint32 max_messages = 3;   // capped by gateway config
  uint32 wait_ms = 4;        // long-poll bound, capped by gateway config
  string session_id = 5;     // empty on first call; server-issued thereafter
}
message QueuedMessage {
  string topic = 1; int32 partition = 2; int64 offset = 3;
  optional bytes key = 4; bytes value = 5; map<string, bytes> headers = 6;
  int64 timestamp_ms = 7;
  int32 delivery_count = 8;  // 1 on first delivery (KIP-932)
}
message QueueAcquireResponse { string session_id = 1; repeated QueuedMessage messages = 2; }

enum QueueAckType { QUEUE_ACK_TYPE_UNSPECIFIED = 0; ACCEPT = 1; RELEASE = 2; REJECT = 3; }
message QueueAckEntry { string topic = 1; int32 partition = 2; int64 offset = 3; QueueAckType type = 4; }
message QueueAcknowledgeRequest { string session_id = 1; repeated QueueAckEntry entries = 2; }
message QueueAckResult { QueueAckEntry entry = 1; optional ErrorInfo error = 2; } // e.g. lock expired
message QueueAcknowledgeResponse { repeated QueueAckResult results = 1; }

message QueueRenewRequest { string session_id = 1; repeated QueueAckEntry entries = 2; } // type ignored
message QueueRenewResponse { repeated QueueAckResult results = 1; }
```

**Semantics:** `Acquire` with an empty `session_id` starts an explicit-ack `ShareConsumer` for `(principal, group)` and issues a session id; subsequent `Acquire`s poll it (long-poll up to `wait_ms`). `Acknowledge` stages the entries (`acknowledge(record, type)`) and `commit()`s in one call — per-entry results surface broker verdicts (an expired lock → `INVALID_RECORD_STATE` mapped to a per-entry error, never a whole-call failure). `Renew` maps to the native `renew` for long-processing messages. An unknown/expired `session_id` → `FailedPrecondition("queue session expired; re-acquire")` — the client just calls `Acquire` again; nothing was lost (locks expired, records will redeliver).

## Session lifecycle (the one stateful piece)

A gateway `QueueSessionTable`: `session_id → { ShareConsumer, last_used }`, idle-evicted (config, default 60 s) by closing the consumer — whereupon the broker releases its locks and redelivers. Session ids are unguessable (random 128-bit) and bound to the authenticated principal (a session used by a different principal → `PermissionDenied`). v1 is single-gateway (session affinity is trivial); multi-gateway session routing is deferred with the rest of gateway HA.

## Contract impact (the stub→live transition, as the umbrella designed)

Contract **v1 → v1.1** (minor, additive): the `stub_queues` vector is **retired** and replaced by live vectors (`queue_roundtrip`: produce → acquire → accept → not redelivered; `queue_release_redelivers` with `delivery_count == 2`; `queue_reject_archives`; `queue_lock_expiry_redelivers`; `queue_session_expiry_reacquire`). SDKs implement the module and declare v1.1. **One umbrella-contract refinement:** the sketched `AcquireOptions.lockDuration` is dropped — lock duration is group-level broker config in KIP-932, not per-acquire; the SDKs expose `renew()` instead. No DLQ exists and none is implied: `Reject` archives immediately; exhausted `max_delivery_attempts` (default 5) archives — SDK docs state both.

## Non-goals

Streaming/push queue delivery (unary pull is the contract; a push binding can layer later); per-acquire lock duration (KIP-932 group config); a DLQ (archived records are skipped — external drainage via admin tooling, as the broker grounding recorded); multi-gateway session affinity; broker changes of any kind.

## Integration

- **`crates/client-consumer/src/share/`** — the `headers` addition (types.rs + poll.rs), MSG-1-precedent.
- **`crates/grpc-gateway`** — `gateway.proto` additions; `src/queue.rs` (the session table + RPC handlers); ACL gate reused from `Subscribe`; gateway config (`queue_max_messages`, `queue_wait_ms_cap`, `queue_session_idle_secs`).
- **`crates/sdk-conformance`** — the v1.1 vectors (a follow-on task in the umbrella's crate once this lands).
- **The five SDKs** — each flips its `queues` stub to the live module in a small follow-on task per language (the interfaces were pinned for exactly this moment).

## Kafka / wire compliance

The gateway consumes via the native `ShareConsumer` over the standard KIP-932 wire — no broker or protocol changes; delivered values/headers byte-exact; `delivery_count` passed through verbatim.

## Testing

- **Session table units:** issue/lookup/idle-evict; principal binding; expired-session error shape.
- **Integration (in-process broker + gateway):** the five v1.1 vector behaviors, plus per-entry ack errors (ack after forced lock expiry → `INVALID_RECORD_STATE` surfaced per-entry), headers round-trip (a `ce_*`-headered record acquired with headers intact — the native addition proven end-to-end), ACL denial.
- **The JVM cross-check:** a record acquired-and-released via the gateway is re-acquired by the JVM `KafkaShareConsumer` with `delivery_count == 2` (riding the existing `jvm_share_groups` harness) — the two consumers interoperate on one group.

## Risks

- **Session-table memory under abandonment** — bounded by idle eviction + a max-sessions cap (config; `ResourceExhausted` past it).
- **Long-poll `wait_ms` vs Connect/h1 timeouts** — capped (default 30 s) below typical proxy idle timeouts; documented.
- **The headers addition touches the share poll path** — small, MSG-1-shaped, but it is broker-adjacent client code: covered by the JVM cross-check and byte-exact tests.
- **Contract-bump mechanics are the first live exercise of the umbrella's evolution story** — retiring `stub_queues` while five SDKs still pass v1.0 requires the harness to select vectors by declared contract version (already implied by `Hello{contract_major}`; the minor needs adding — a named task).

## Resolved decisions

Unary pull (`Acquire`/`Acknowledge`/`Renew`) over h1 — no bidi anywhere in the queue path; gateway session table with broker-lock-expiry as the safety net; the `ShareConsumerRecord.headers` addition (MSG-1 precedent); per-entry ack results; `lockDuration` dropped from the contract in favor of `renew`; contract v1.1 with version-selected vectors; no DLQ implied.
