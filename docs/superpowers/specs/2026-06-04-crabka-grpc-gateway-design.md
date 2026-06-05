# Crabka gRPC + HTTP Gateway — Design & Roadmap

## Goal

Build a standalone **`crabka-grpc-gateway`** service that lets non-Kafka
applications produce to and consume from *real* Kafka topics over **gRPC /
Connect-RPC** and **HTTP webhooks (JSON)**, with server-side **exactly-once
deduplication** of caller-initiated duplicate sends. The gateway speaks the
ordinary Kafka wire protocol to the broker using Crabka's own native idempotent
producer and group consumer — **the broker is never modified and stays
byte-exact.**

The unifying model: **multiple protocol front-ends over one shared
produce / consume / dedup core.**

```
            ┌──────────────── crabka-grpc-gateway (standalone binary) ───────────────┐
            │                                                                          │
  gRPC ───► │  Connect/gRPC front-end ┐                                                │
  client    │                          │                                              │
  HTTP ───► │  Webhook-in front-end ───┼──►  SHARED CORE                              │
  POST      │                          │     • produce core (idempotent producers)    │   Kafka wire
            │  Webhook-out delivery ◄──┘     • dedup engine (active-active EOS)  ──────┼─►  Crabka
  HTTP  ◄── │  (egress consumer→POST)        • consume core (group subscribe/commit)   │   broker
  callback  │                                • codec seam (Raw now, SchemaReg later)   │   (unmodified)
            │                                • trusted-proxy authorizer                │
            └──────────────────────────────────────────────────────────────────────────┘
```

## Decisions captured during brainstorm

- **Role:** Kafka-over-gRPC/HTTP **gateway** into real Kafka topics (not a
  standalone messaging system, not internal control-plane RPC).
- **Placement:** standalone gateway crate built on Crabka's **native client
  crates**; broker untouched. (Rejected: in-broker listener; bundled subcommand.)
- **RPC stack:** **Connect-RPC over axum** (the `connectrpc-axum` stack the
  rebalancer already uses) — wire-compatible with gRPC, and gives gRPC-Web +
  HTTP/JSON for free. (Rejected: raw tonic.)
- **Send API:** **both** unary and client-streaming produce.
- **Receive API:** **streaming subscribe backed by a Kafka consumer group**,
  at-least-once via client ack → offset commit. (Rejected: stateless unary poll.)
- **Dedup model:** **Kafka-backed** dedup store (compacted internal topic),
  keyed by a caller-supplied `idempotency_key`. (Rejected: in-memory; lean-on-
  Kafka-sessions.)
- **Dedup correctness:** **strict exactly-once in v1.**
- **EOS mutual-exclusion model:** **active-active ownership sharding** —
  idempotency_keys sharded across all gateway replicas via a consumer group on
  the dedup topic; non-owner requests forwarded gateway→gateway. (Rejected:
  single active writer.)
- **v1 cross-cutting scope:** gRPC transport TLS/mTLS; metrics & tracing; caller
  identity → Kafka ACL; operator deployment — **all in v1.**
- **Identity → ACL:** **trusted-proxy authorizer** — gateway evaluates the
  caller's authorization against cached broker ACLs and produces as its own
  service principal. (Rejected: per-caller delegation tokens — would explode the
  transactional-producer matrix under active-active EOS.)
- **HTTP webhook:** **both directions** — inbound receiver (POST JSON → produce)
  and outbound delivery (consume → POST JSON to a URL).
- **Payloads:** opaque bytes (pass-through) in v1. **Schema Registry**
  (Avro / JSON Schema / Protobuf) integration is **deferred** — it is a separate
  in-flight component; the gateway leaves a codec seam for it (see end).

## Non-goals (v1)

- No changes to the broker or to Kafka wire bytes.
- No schema validation / encoding — payloads are opaque bytes (codec seam left
  for the Schema Registry component when it lands).
- No exactly-once for **outbound HTTP** delivery — impossible over HTTP;
  at-least-once + an event-id header for receiver-side dedup.
- No arbitrary caller-supplied outbound URLs — outbound targets are
  operator-configured and host-allow-listed (SSRF protection).

## Architecture

### Process shape

A single binary, `crabka-grpc-gateway`. One **axum** server hosts the
Connect/gRPC service *and* the inbound webhook HTTP routes on the same listener
(content-negotiated). The **outbound webhook delivery** subsystem runs as
background tasks (one consumer group per subscription) in the same process. A
health/readiness endpoint reports per-dedup-partition warm-up state.

### Crate layout

```
crates/grpc-gateway/
  Cargo.toml                      # connectrpc-axum, prost, tower, axum, reqwest,
                                  # rustls; crabka-client-core/-producer/-consumer,
                                  # crabka-security, crabka-authz (factored, see §4)
  build.rs                        # connectrpc_axum_build, system-protoc + fetch fallback
  proto/crabka/gateway/v1/gateway.proto
  src/
    lib.rs   bin/gateway.rs
    config.rs                     # listeners, broker bootstrap, TLS, dedup, webhooks
    core/
      produce.rs                  # produce core: keyed→dedup, unkeyed→plain
      dedup/
        mod.rs                    # engine entry, per-key locks, routing
        store.rs                  # compacted-topic materialized map (read_committed)
        ownership.rs              # consumer-group ownership + membership routing table
        txn.rs                    # transactional record+claim, txn.id-per-partition
      consume.rs                  # group subscribe + commit
      codec.rs                    # RecordCodec trait + RawCodec (SchemaRegistryCodec later)
      authz.rs                    # trusted-proxy authorizer (uses crabka-authz)
    frontend/
      grpc.rs                     # Connect/gRPC service impl (Send/SendStream/Subscribe)
      webhook_in.rs               # HTTP POST → produce (signature verify, JSONPath map)
      webhook_out/
        mod.rs                    # subscription manager
        delivery.rs               # consume→POST, retries/backoff, DLQ, ordering
        sign.rs                   # outbound HMAC signing
    telemetry.rs                  # Prometheus metrics + OTLP tracing
    forward.rs                    # internal gateway→gateway client (owner forwarding)
```

### Reused Crabka building blocks (no new Kafka-protocol code)

| Need | Reuse |
|---|---|
| Bootstrap / connection pool | `crabka-client-core` (`bootstrap`, `pool`, `transport`) |
| Idempotent + transactional produce | `crabka-client-producer` (`InitProducerId`, `(pid,epoch,seq)`, txn) |
| Group consume + commit | `crabka-client-consumer` (`subscribe`, `poll`, `commit_sync/async`) |
| TLS / mTLS / principal / hot reload | `crabka-security` (`tls`, `mtls`, `principal`, `reload`) |
| ACL evaluation (trusted-proxy) | factor `crates/broker/src/authorizer` → shared `crabka-authz` |
| JSON field extraction (webhook-in) | `jsonpath-rust` (already a workspace dep) |
| Outbound HTTP client | `reqwest` (already in the dep graph via OTLP) |

## Components

### 1. Produce core

Entry point for every front-end's "produce one record" call. Branch:

- **Keyed** (`idempotency_key` present) → dedup engine (§2) → strict EOS.
- **Unkeyed** → plain native idempotent producer (`acks=all`), no ownership /
  forwarding / transaction. Scales freely across all replicas.

Partitioner mirrors Kafka's default (key-hash murmur2 when a record key is
present, sticky otherwise). Per-record results (`partition`, `offset`,
`deduplicated`, optional `error`) — never whole-batch failure.

### 2. Dedup engine (active-active EOS) — the crux

**Claim topic.** Internal compacted topic `__crabka_grpc_dedup`, `N`
partitions, `cleanup.policy=compact,delete`, `retention.ms = dedup_window_ms`.
Key = `idempotency_key`; value = `{topic, partition, offset, produce_ts}`. The
retention bound is *both* the topic size bound and the dedup-window guarantee.

**Ownership sharding (mutual exclusion).** Gateway replicas form a consumer
group (`__crabka_grpc_gateway_dedup_owners`) subscribed to `__crabka_grpc_dedup`.
Partition assignment **is** ownership: the owner of dedup-partition `p` is the
sole writer for every key with `hash(key) % N == p`. On each assignment, every
replica publishes its `{node_id, advertised_addr, owned_partitions, epoch}` to a
compacted membership topic `__crabka_grpc_gateway_membership`, which all replicas
tail into a routing table `dedup_partition → owner_addr`.

**Materialized map (atomic-claim visibility).** Each owner consumes its assigned
partitions with **`read_committed`** isolation, building
`DashMap<key, (topic, partition, offset)>` per owned partition — so only
committed `(record + claim)` pairs are ever visible. On (re)assignment the owner
reads the partition to its high-water mark **before serving** keys for `p`
(per-partition warm-up readiness gate).

**Routing.** A replica receiving a keyed Send computes `p = hash(key) % N`.
Owns `p` → handle locally. Else → **forward** the record (with caller-identity
context, §4) to the owner via the internal gateway→gateway client (`forward.rs`).
During warm-up / rebalance gaps the owner answers `UNAVAILABLE`; the origin
re-resolves and retries.

**Strict-EOS write path (owner, on a map miss):**
1. acquire the sharded **per-key lock**;
2. re-check the map (may have filled while waiting);
3. still missing → using the partition's **transactional producer**
   (`transactional.id = "crabka-grpc-dedup-{p}"`): `beginTxn` → produce the data
   record to the user topic (`acks=all`) → produce the claim to
   `__crabka_grpc_dedup[p]` → `commitTxn`;
4. update the local map with the committed offset;
5. release the lock; return `(partition, offset, deduplicated=false)`.

Map **hit** → return the cached `(partition, offset, deduplicated=true)` without
producing.

**Why this is strictly exactly-once:**
- *Single writer per key* — ownership sharding ⇒ no two replicas race a key.
- *Atomic claim* — record + claim land in one transaction; `read_committed`
  materialization hides partial state; a crash mid-txn aborts ⇒ clean retry.
- *No cold-start gap* — per-partition warm-up gate before serving.
- *No zombies* — `transactional.id` pinned to the dedup-partition ⇒ on ownership
  move the new owner's `InitProducerId` bumps the epoch and **fences** the old
  owner (the KIP-447 pattern Crabka already implements).

**The trade:** availability, not correctness — keys for a partition are briefly
`UNAVAILABLE` during its warm-up / rebalance. This is the intended EOS posture
and is documented to callers (retry on `UNAVAILABLE`).

**Batching note.** A Send batch may span dedup-partitions (different keys) and
topic-partitions. v1 routes **per record** to its owner and aggregates per-record
results; an owner may coalesce same-owner records into one transaction.
(Optimization, not required for correctness.)

### 3. Consume core

Wraps `crabka-client-consumer`. A subscription joins a Kafka consumer group,
runs a poll loop, and yields records to whichever front-end requested them
(gRPC `Subscribe` or an outbound-webhook subscription). At-least-once: offsets
commit after delivery is acknowledged (client ack for gRPC; successful POST for
webhook-out). Multiple consumers sharing a `group_id` get Kafka-native partition
load-balancing.

### 4. Trusted-proxy authorizer (identity → ACL)

The caller authenticates to the gateway (mTLS client cert → principal via
`crabka-security::principal`, or a bearer token). For each produce/consume the
gateway evaluates **the caller's** authorization against a **cached snapshot of
broker ACLs** and then performs the Kafka operation as its **own** service
principal. ACL evaluation reuses Crabka's existing broker-side authorizer logic,
**factored out of `crates/broker/src/authorizer` into a shared `crabka-authz`
crate** so both broker and gateway share one implementation (allow/deny
precedence, literal/prefix resource patterns, operation matrix). Broker audit
shows the gateway principal plus an `on-behalf-of` header carrying the real
caller principal. Forwarded (gateway→gateway) requests carry the resolved caller
principal so the owner authorizes identically.

ACL snapshot freshness: the gateway maintains the ACL cache via the same
metadata/`DescribeAcls` path admin clients use, refreshed on change. (Open
question: push vs poll refresh — see Risks.)

### 5. Codec seam (Schema Registry-ready)

`trait RecordCodec { fn encode(...) -> Bytes; fn decode(...) -> (Bytes, SchemaMeta); }`.
v1 ships `RawCodec` (identity). Front-ends call only through the codec, so the
later `SchemaRegistryCodec` is a drop-in (see Deferred section).

### 6. gRPC / Connect front-end

```proto
syntax = "proto3";
package crabka.gateway.v1;

service Gateway {
  rpc Send(SendRequest) returns (SendResponse);                 // unary, 1..N records
  rpc SendStream(stream SendRequest) returns (stream SendAck);  // streaming produce
  rpc Subscribe(stream SubscribeFrame) returns (stream Inbound);// bidi: records down, acks up
}

enum Acks { ACKS_ALL = 0; ACKS_LEADER = 1; ACKS_NONE = 2; }

message Record {
  string topic = 1;
  optional bytes key = 2;
  bytes value = 3;
  map<string, bytes> headers = 4;
  optional int32 partition = 5;          // explicit partition override
  optional int64 timestamp_ms = 6;
  optional string idempotency_key = 7;   // present ⇒ strict-EOS dedup
}
message SendRequest  { repeated Record records = 1; Acks acks = 2; }
message RecordResult { int32 partition = 1; int64 offset = 2; bool deduplicated = 3; ErrorInfo error = 4; }
message SendResponse { repeated RecordResult results = 1; }
message SendAck      { repeated RecordResult results = 1; }

message SubscribeFrame {
  oneof frame {
    Start start = 1;                     // group_id, topics[], auto_commit
    Ack   ack   = 2;                     // topic, partition, offset to commit
  }
}
message Inbound { string topic = 1; int32 partition = 2; int64 offset = 3;
                  optional bytes key = 4; bytes value = 5;
                  map<string, bytes> headers = 6; int64 timestamp_ms = 7; }
message ErrorInfo { int32 code = 1; string message = 2; bool retriable = 3; }
```

`Subscribe` is bidi: a `Start` frame opens the group subscription; `auto_commit=false`
⇒ the caller sends `Ack` frames and the gateway commits those offsets
(at-least-once); `auto_commit=true` ⇒ periodic auto-commit.

### 7. HTTP webhook inbound front-end

Routes on the shared axum server: a generic `POST /v1/produce/{topic}` and
configurable named endpoints `POST /v1/webhooks/{name}`. Per-endpoint config:
target topic, **signature scheme** (HMAC-SHA256 over the raw body, header name,
secret, optional timestamp tolerance for replay protection), **idempotency
source** (HTTP header name or a JSONPath into the body — e.g. a provider
event-id like `X-GitHub-Delivery`), and **key/header mapping** (JSONPath / header
selection via `jsonpath-rust`).

Flow: enforce a body-size limit → verify signature (401 on mismatch) → extract
`idempotency_key` → build `Record{ topic, value = raw JSON bytes, key, headers }`
→ run through the **same produce + dedup core** (so provider redeliveries dedup
to exactly-once into Kafka) → respond `200 {partition, offset, deduplicated}`.

### 8. HTTP webhook outbound delivery subsystem

**Subscriptions** (operator/admin-configured; later a CR): `{ name, source_topics[],
target_url, signing_secret, retry{max_attempts, base_backoff, max_backoff, jitter},
concurrency_per_partition (default 1 = in-order), filter (optional JSONPath/header
predicate), dead_letter_topic, tls/headers }`.

Each subscription runs a consumer group `__crabka_grpc_wh_{name}`. Per record:
render a JSON envelope `{topic, partition, offset, timestamp, key, headers, value}`
(value parsed as JSON when valid, else base64) → POST to `target_url` with
`X-Crabka-Event-Id` (= `topic-partition-offset`, for receiver dedup),
`X-Crabka-Signature` (HMAC-SHA256 of the body with `signing_secret`), and
`X-Crabka-Timestamp`.

**Delivery semantics — at-least-once, ordered:**
- `2xx` → delivered; commit the partition's contiguous-delivered prefix.
- non-`2xx` / timeout → retry with exponential backoff + jitter up to
  `max_attempts`. The partition is **head-of-line blocked** while retrying (no
  offset advance past the failing record) to preserve order + at-least-once.
- exhaustion → produce the record + failure metadata to `dead_letter_topic`,
  then advance and commit.
- **Backpressure:** bounded in-flight per partition; pause partition fetch when
  full.
- **SSRF protection:** `target_url` host/scheme must be on a configured
  allow-list; TLS verification on by default. (No runtime caller-supplied URLs.)
- Exactly-once is impossible over HTTP → at-least-once + `X-Crabka-Event-Id` for
  the receiver to dedup.

### 9. TLS / mTLS

rustls listener via `crabka-security` with hot cert reload (as the broker does).
Optional **mTLS**; the client cert → principal feeds the trusted-proxy authorizer
(§4). Config-driven per listener.

### 10. Telemetry

Prometheus: `gateway_sends_total{result}`, `gateway_dedup_hits_total`,
`gateway_produce_latency_seconds`, `gateway_forward_total{outcome}`,
`gateway_txn_total{commit|abort}`, `gateway_active_subscriptions`,
`gateway_owned_partitions`, `gateway_webhook_in_total{result}`,
`gateway_webhook_out_total{result}`, `gateway_webhook_retries_total`,
`gateway_dead_letter_total`. OTLP spans across send→dedup→(forward)→txn→commit
and webhook-in/out, propagating trace context from gRPC/HTTP metadata. Matches
the broker's existing telemetry stack.

## Data flow

### Send (keyed, strict EOS)
`Send/SendStream | webhook-in → produce core → hash(key)%N → own? handle :
forward → per-key lock → map check → (miss) txn{record→user-topic, claim→dedup} →
commit → map update → result`.

### Subscribe (gRPC, at-least-once)
`Start → join group → poll loop → stream Inbound → caller Ack → commit offset`.

### Webhook inbound
`POST → size-limit → verify HMAC → extract idempotency_key → Record → produce+dedup
core → 200 {partition, offset, deduplicated}`.

### Webhook outbound
`group poll → filter → render JSON envelope → sign → POST → 2xx? commit prefix :
backoff-retry (head-of-line) → exhausted? → DLQ + commit`.

## Error handling

- Per-record error vectors; never whole-batch failure.
- Broker error codes → gRPC/Connect status + `ErrorInfo{code, retriable}`.
- `UNAVAILABLE` during ownership warm-up/rebalance — caller retries; origin
  re-resolves owner.
- Transaction abort → retriable.
- Webhook-in signature failure → `401`; oversize body → `413`.
- Webhook-out exhausted retries → dead-letter topic (never silent drop).

## Security

- TLS/mTLS on all network-facing listeners; hot reload.
- Inbound webhook **signature verification** (HMAC + timestamp tolerance).
- Outbound webhook **HMAC signing** + TLS verify + **SSRF host allow-list**.
- Authorization via the trusted-proxy authorizer (§4); `on-behalf-of` auditing.
- Internal topics (`__crabka_grpc_dedup`, `__crabka_grpc_gateway_membership`)
  owned by the gateway service principal.

## Testing strategy

- **Unit:** dedup store, ownership hashing, routing table, per-key locking,
  codec seam, signature verify/sign, JSONPath mapping, backoff schedule.
- **Integration (in-process broker / testcontainer):**
  - records land — read back via the native consumer **and** a JVM Kafka consumer
    (byte-level produce proof);
  - dedup under sequential, concurrent, cross-replica, and **crash-injection**
    (kill between record and claim → no double-write after recovery);
  - EOS epoch-fencing on ownership change (zombie owner cannot commit);
  - at-least-once consume + commit; rebalance + forwarding correctness;
  - webhook-in signature accept/reject + dedup of provider redeliveries;
  - webhook-out at-least-once, ordering under endpoint failure, DLQ on
    exhaustion, SSRF allow-list enforcement.
- **Multi-replica** tests for ownership/forwarding/rebalance warm-up.

## Roadmap (phases)

The spec covers the whole design; the implementation plan slices it. Phases are
ordered so each builds on the last; file sets are disjoint enough for
parallel-batch subagent execution within a phase (see File-set sketch).

- **P0 — Skeleton.** Crate, proto, `build.rs` (connectrpc), config, axum server
  bootstrap (Connect + HTTP routes), health/readiness.
- **P1 — Send + Subscribe (no dedup).** Unary + streaming Send via native
  producer (`acks=all`); bidi Subscribe via group consumer + commit; `RawCodec`;
  single service principal.
- **P2 — Dedup core (single-owner).** Compacted claim topic, `read_committed`
  materialized map, per-key lock, transactional record+claim, warm-up gate.
- **P3 — Active-active sharding.** Dedup-partition ownership via consumer group,
  membership routing topic, key→owner routing, gateway→gateway forwarding,
  per-partition rebalance warm-up, `transactional.id`-per-partition fencing.
- **P4 — TLS / mTLS.**
- **P5 — Identity → ACL.** Factor `crabka-authz`; trusted-proxy authorizer;
  ACL-snapshot cache; on-behalf-of auditing; identity forwarding.
- **P6 — Webhook inbound.** HTTP route, signature verification, JSONPath
  mapping, dedup integration.
- **P7 — Webhook outbound.** Subscription model, delivery engine (retries,
  backoff, ordering, backpressure), DLQ, HMAC signing, SSRF allow-list.
- **P8 — Telemetry.** Prometheus + OTLP across all front-ends (woven in
  incrementally from P1; consolidated and gap-filled here).
- **P9 — Operator deployment.** `KafkaGrpcGateway` CR → Deployment + Service +
  TLS-secret wiring. **Likely its own spec** (per Crabka's per-component operator
  spec pattern).
- **Deferred — Schema Registry codec** (drop-in once that component lands).

P2→P3 deliberately builds dedup single-owner first, then layers sharding — the
incremental path to the active-active target.

## File-set sketch (for parallel-batch implementation)

Disjoint sets that can run concurrently within a phase:

- **P0:** `Cargo.toml`, `build.rs`, `proto/…/gateway.proto`, `config.rs`,
  `bin/gateway.rs`, `lib.rs`.
- **P1:** `core/produce.rs` + `frontend/grpc.rs` (send) ∥ `core/consume.rs` +
  Subscribe half of `frontend/grpc.rs` ∥ `core/codec.rs`.
- **P2:** `core/dedup/store.rs` ∥ `core/dedup/txn.rs` ∥ `core/dedup/mod.rs`
  (locks) — `store`/`txn` are disjoint; `mod` integrates after.
- **P3:** `core/dedup/ownership.rs` + `forward.rs` (membership/routing) —
  depends on P2, so a later batch.
- **P5:** new `crabka-authz` crate (factor) ∥ `core/authz.rs` glue.
- **P6:** `frontend/webhook_in.rs` (depends on produce+dedup core).
- **P7:** `frontend/webhook_out/{mod,delivery,sign}.rs` (depends on consume core).
- **P8:** `telemetry.rs` + metric call-sites (touches many files — run solo or
  last in its batch).

## Risks & open questions

- **Owner discovery mechanism.** Plan: compacted `__crabka_grpc_gateway_membership`
  topic materialized by all replicas. Alternative: piggyback on the group
  assignor's output. Confirm during P3.
- **ACL-evaluation fidelity.** The trusted-proxy authorizer must mirror Kafka
  ACL semantics exactly; factoring `crabka-authz` out of the broker keeps one
  source of truth. ACL-cache refresh (push vs poll) is open — start with poll +
  change-driven refresh.
- **Transactional-producer pool.** Bounded by `N` dedup-partitions per owner;
  lazily created, idle-evicted. Tune `N` for the target throughput.
- **Warm-up availability windows** on rebalance — intended EOS trade; document
  and surface as `UNAVAILABLE`.
- **Outbound head-of-line blocking** on a stuck endpoint — DLQ after
  `max_attempts` is the escape valve; `concurrency_per_partition > 1` trades
  ordering for throughput.
- **Operator CR shape** deferred to the P9 spec.
- **Schema Registry client interface** unknown until that component is far
  enough along — codec seam isolates the dependency.

## Deferred: Schema Registry integration (notes for when it lands)

A separate in-flight component will provide **Avro / JSON Schema / Protobuf**
schema management. Integration is purely additive via the codec seam (§5):

- **`SchemaRegistryCodec`** wraps the registry *client*; `produce.rs` /
  `consume.rs` / webhook front-ends are unchanged — only the injected codec
  differs. Dependency direction is gateway → registry client, never the reverse.
- **Confluent wire framing** for JVM serde interop: encode values as
  `[0x00 magic][4-byte big-endian schema id][payload]` (Protobuf adds its
  message-index varints); decode reads the schema id from that framing. A JVM
  consumer with a Confluent deserializer reads what the gateway produced, and
  vice-versa.
- **Proto additions (later):** `Record` grows
  `oneof { bytes raw; StructuredValue structured }` + a `schema{subject, id,
  format}` selector; `Inbound` gains a decoded value + schema metadata. Default
  subject strategy = `TopicNameStrategy` (`<topic>-value` / `-key`). Greenfield ⇒
  these can be added freely when the time comes.
- **Webhook tie-in:** inbound JSON can be validated against a JSON Schema
  subject; outbound can decode Avro/Protobuf records to JSON for delivery.
- **Dedup is unaffected** — it keys on `idempotency_key` and operates on
  already-encoded bytes, so it composes with either codec.
- Registration / compatibility (BACKWARD / FORWARD / FULL) is the registry's
  job; the gateway only calls register / lookup.
