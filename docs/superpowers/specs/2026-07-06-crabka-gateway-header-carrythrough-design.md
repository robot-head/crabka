# Gateway header carry-through (MSG-1) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. First slice of the **serverless messaging cycle** (Chapter A / roadmap Chapter 4), and the named prerequisite for the CloudEvents binding (MSG-2).

## Context — the messaging cycle, and where this sits

Grounding the [serverless-backend vision](2026-07-06-crabka-serverless-backend-vision-design.md)'s messaging chapter against the actual tree corrected a key assumption: **KIP-932 share groups are already fully built and JVM-validated** — `ShareFetch`/`ShareAcknowledge`, the per-partition `AcquisitionState` machine (Available→Acquired→Acknowledged→Archived), redelivery via `delivery_count`, poison-pill archiving at `max_delivery_attempts` (default 5), and `__share_group_state` persistence all exist (`crates/broker/src/handlers/share_fetch.rs`, `crates/broker/src/share_partition/state.rs`, tests in `crates/broker/tests/share_consume.rs`). The serverless "message → function, per-message ack + redelivery" primitive is therefore **not** the gap.

The real messaging-cycle work decomposes into five slices:

| Slice | What | State | Prereqs |
|-------|------|-------|---------|
| **MSG-1** | **Header carry-through on gateway consume egress** | this doc | — |
| MSG-2 | CloudEvents Kafka+HTTP binding (binary+structured, ce-* semantics, produce-in parsing) | net-new | MSG-1 |
| MSG-3 | Per-offset explicit ack in the Subscribe stream (lighter than share groups) | advisory today → load-bearing | — |
| MSG-4 | KEDA share-group backlog scaler (fleet-wide lag, the two `-1` "unknown" causes) | **the differentiated slice**, partial | — (parallel track) |
| MSG-5 | Polyglot serverless messaging SDK over the gateway | net-new packaging | MSG-1, MSG-3 |

Only MSG-2 (needs MSG-1's headers) and MSG-5 (needs MSG-1+MSG-3) have hard prerequisites. **MSG-4 is the one genuine differentiation** and has no prerequisite — it should run as a parallel track, not be buried behind the parity slices. This slice, **MSG-1**, is first because it is a small, self-contained, TDD-friendly fix to a **documented correctness bug** that blocks the highest-interop slice (MSG-2).

**The bug.** Record headers exist wire-exact at every layer *except* the gateway's consume egress: the broker record carries `RecordHeader { key: String, value: Option<Bytes> }` (`crates/protocol/src/records/owned.rs:18-21`), the native consumer materializes them as `ConsumerRecord.headers: Vec<Header>` (`crates/client-consumer/src/consumer.rs:117`), and the gateway proto already declares `Inbound.headers` (`gateway.proto:130`). But the gateway **drops them on the way out**: `DecodedConsumerRecord` (`crates/grpc-gateway/src/consume.rs:16-25`) has no `headers` field, so `poll()` never copies `r.headers`; `inbound_from_decoded_record` then hardcodes `headers: HashMap::new()` (`streaming.rs:153`); and `render_envelope` omits them behind a now-false comment "`ConsumerRecord` exposes none" (`outbound.rs:283-285`). Any header-riding feature — CloudEvents binary mode, tracing propagation, dedup keys — is silently broken end-to-end. The design docs already flag this drop as the blocking bug.

## Design Goals

- **Restore headers on both consume-egress paths:** the gRPC `Subscribe` stream (`Inbound`) and the outbound webhook/HTTP envelope (`render_envelope`).
- **Lossless internally:** `DecodedConsumerRecord` and the outbound JSON envelope carry headers faithfully — key + `Option<Bytes>` value, order-preserving, duplicate-key-preserving.
- **Defined, tested policy at the one lossy hop:** the gRPC `Inbound.headers` `map<string,bytes>` cannot represent a null value or a duplicate key; MSG-1 populates it for the common case with an explicit, tested policy (null value → empty bytes; duplicate key → last-wins, matching proto3 map semantics) — never a silent drop-to-empty.
- **Behavior-tested:** a produced header round-trips out through `Subscribe` *and* through the outbound envelope (assert the delivered stream/envelope, not the source).

## Non-goals

- **CloudEvents ce-* semantics** (attribute extraction/validation, binary↔structured conversion) — MSG-2.
- **Produce-in header parsing** (`webhook.rs:198,249` hardcode `headers: vec![]`; `to_gateway_record` at `handlers.rs:153`) — MSG-2 owns the ingress/ce-* side; MSG-1 is consume-egress only, keeping the file sets disjoint.
- **Proto reshape** of `Inbound.headers`/`Record.headers` from `map<string,bytes>` to a lossless `repeated Header` — deferred (see Risks). It couples to the produce path (`GatewayRecord.headers: Vec<(String, Bytes)>`, `types.rs:23`) and is **not** required for MSG-2, because CloudEvents ce-* attributes are single-valued and non-null, which the map represents exactly.
- Per-offset ack (MSG-3), the KEDA scaler (MSG-4), the SDK (MSG-5).

## Architecture Overview

```
Broker record (headers wire-exact)  ──►  native Consumer  ──►  ConsumerRecord.headers: Vec<Header{key, Option<Bytes>}>
                                                                 │                    │
                                        ConsumeSession::poll ────┤                    ├──── outbound.rs render_envelope
                                                                 ▼                    ▼      (native ConsumerRecord —
                                        DecodedConsumerRecord.headers: Vec<(String, Option<Bytes>)>   lossless JSON array)
                                                                 │
                                        inbound_from_decoded_record
                                                                 ▼
                                        pb::Inbound.headers: map<string,bytes>  (common-case policy; MSG-1's one lossy hop)
```

Two egress paths, one shared source (`Vec<Header>` on the native record):
1. **gRPC `Subscribe`** decodes through `ConsumeSession::poll → DecodedConsumerRecord → inbound_from_decoded_record → pb::Inbound`.
2. **Outbound webhook/HTTP** renders the native `ConsumerRecord` directly through `render_envelope` (it never touches `DecodedConsumerRecord`).

## Key Design Decisions

### `DecodedConsumerRecord` carries headers losslessly

Add `headers: Vec<(String, Option<bytes::Bytes>)>` to `DecodedConsumerRecord` (`consume.rs:16-25`), mirroring the native `Header { key, value: Option<Bytes> }`. `poll()` copies `r.headers` alongside the fields it already copies (topic/partition/offset/timestamp/key/value/schema/json). Keeping the internal shape lossless means MSG-2 layers ce-* semantics on a faithful carrier without re-touching this type. *Alternative rejected:* storing `Vec<(String, Bytes)>` (flattening null→empty here) — loses the null distinction one layer too early, forcing MSG-2 to re-plumb.

### `Inbound.headers` map: common-case population with a defined lossy-edge policy

`inbound_from_decoded_record` (`streaming.rs:146-160`) builds the `map<string,bytes>` from the decoded headers instead of `HashMap::new()`. Because proto3 maps cannot express a null value or a duplicate key, MSG-1 fixes a **documented, tested** policy: a null header value maps to empty bytes; duplicate keys resolve last-wins (proto3 map insertion semantics). This covers every real header case — all CloudEvents ce-* attributes, tracing headers, and dedup keys are single-valued and non-null — while being explicit that the map is not byte-exact for pathological duplicate/null headers (the deferred reshape closes that; see Risks).

### Outbound envelope carries headers losslessly

`render_envelope` (`outbound.rs:280-296`) gains a `headers` field in the JSON envelope, built from the native `ConsumerRecord.headers`. JSON can represent them faithfully — an ordered array of `{ "key": ..., "value": <base64 | null> }` (value base64 to match the envelope's existing key/value base64 convention; `null` for a null value). The stale "`ConsumerRecord` exposes none" comment is corrected. This path is fully lossless (no proto map in the way).

### Why not reshape the proto now

The lossless fix would change `Inbound.headers` and `Record.headers` to `repeated Header { string key = 1; optional bytes value = 2; }`. Greenfield policy ("just change it") would normally favor that — but it is deliberately deferred because: (1) `Record.headers` is the **produce-ingress** field, read by `to_gateway_record` into `GatewayRecord.headers: Vec<(String, Bytes)>` (`handlers.rs:153`, `types.rs:23`) and written by the produce path — reshaping it pulls the produce/ce-* ingress into MSG-1, colliding with MSG-2's file set; and (2) the map is **exactly adequate for MSG-2**, since ce-* attributes are single-valued and non-null. So MSG-1 keeps the proto and hands MSG-2 a working carrier; the reshape is a later, isolated change if fully byte-exact general-header pass-through over gRPC is ever required.

## Integration

- **`crates/grpc-gateway/src/consume.rs:16-25`** — add `headers: Vec<(String, Option<Bytes>)>` to `DecodedConsumerRecord`.
- **`crates/grpc-gateway/src/consume.rs:80-89`** — in `poll()`, copy `r.headers` (native `Header{key,value}`) into the new field.
- **`crates/grpc-gateway/src/streaming.rs:146-160`** — in `inbound_from_decoded_record`, replace `headers: HashMap::new()` (`:153`) with the map built from the decoded headers under the defined policy.
- **`crates/grpc-gateway/src/outbound.rs:280-296`** — thread the native `ConsumerRecord.headers` into `render_envelope`'s JSON; correct the stale comment.
- **`crates/client-consumer/src/consumer.rs:117`** — source of truth (`ConsumerRecord.headers`), unchanged; MSG-1 must not drop it.

## Kafka / wire compliance

- **The Kafka consume path is unchanged** — headers are already wire-exact at the broker/native-consumer layers; MSG-1 fixes only the gateway's *own* gRPC/HTTP surface, which is not the Kafka wire.
- **Restores proxy fidelity** — the gateway exists to proxy Kafka records; dropping headers made it lossy against the records it serves. This closes that gap for the common case and documents the one remaining lossy edge (the gRPC map).

## Testing

- **Subscribe egress:** produce a record with a header `("ce-type", "x")` (and a second header, and a null-valued header); a `Subscribe` stream delivers an `Inbound` whose `headers` map contains the header — assert the delivered frame, not the source.
- **Outbound egress:** the outbound webhook envelope for the same record contains a `headers` array carrying the header key + base64 value (and `null` for the null-valued one) — assert the rendered envelope bytes.
- **Null-value policy:** a header with a null value appears in the `Inbound` map as empty bytes and in the outbound envelope as `null` — the defined, tested policy, never a silent drop.
- **Duplicate-key policy:** two headers with the same key — the `Inbound` map holds last-wins (documented); `DecodedConsumerRecord` and the outbound envelope retain both (lossless).
- **No-header record:** a record with no headers yields an empty map / empty array, not an error.

## Risks (carried into the plan)

- **gRPC map lossiness (deferred reshape):** `Inbound.headers` `map<string,bytes>` cannot express null values or duplicate keys. MSG-1 defines+tests the policy and documents the limit; a `repeated Header` reshape (lossless) is deferred and explicitly **not** required by MSG-2 (ce-* attributes are single-valued/non-null). The lossless internal `DecodedConsumerRecord` shape means the reshape, if ever done, touches only the proto + the two egress converters, not the decode.
- **Two egress paths, one fix:** the gRPC and outbound-webhook paths use *different* record types (`DecodedConsumerRecord` vs native `ConsumerRecord`); both must be covered, so the plan tests each independently.

## Resolved decisions (from grounding)

- **Scope:** consume-egress header restore only; produce-in/ce-* is MSG-2; the file set is `consume.rs` + `streaming.rs:146-160` + `outbound.rs:280-296` (disjoint from MSG-2's `webhook.rs`/`handlers.rs`/`types.rs`).
- **Internal shape:** `DecodedConsumerRecord.headers: Vec<(String, Option<Bytes>)>` (lossless); outbound JSON envelope lossless.
- **Proto:** unchanged (`map<string,bytes>`) with a defined+tested null/duplicate policy; the lossless `repeated Header` reshape is deferred and not on MSG-2's critical path.
- **Correction to the vision:** KIP-932 share groups are already built — the messaging cycle is interop/parity work plus the one differentiated KEDA scaler (MSG-4), not a queue-primitive build.
