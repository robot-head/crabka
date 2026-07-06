# Polyglot serverless messaging SDK (MSG-5) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The **packaging capstone** of the [serverless messaging cycle](2026-07-06-crabka-gateway-header-carrythrough-design.md) — an idiomatic client library over the gateway's Connect-RPC surface. Ships **Go first**; establishes the polyglot foundation the other languages reuse.

## Context — the thinnest face, positioned honestly

MSG-5 packages the gateway's `Send`/`Subscribe` RPCs into a BaaS developer surface: `publish`, `publishEvent` (CloudEvents), `subscribe`-with-filter, and (eventually) per-message ack + topic auto-provision. It is **parity-tier packaging, not a differentiator** — the vision docs call the SDK "the thinnest face … a thin DX wrapper over landed plumbing … parity-or-behind SQS/Pub-Sub/Supabase Queues." Its entire value is **exposure**: making the substrate wins (KIP-932 competing-consumer queues, CloudEvents, SQL filtering, the broker-native backlog signal, diskless economics) reachable through one clean surface — the same topic on the same bucket is simultaneously a pub/sub channel, a work queue, a CloudEvents endpoint, a CDC stream, and an observability WAL. Sell the substrate; ship the SDK as its demo vehicle. Do not let this slice's near-readiness inflate claims.

The transport is decisive and already fixed by the gateway: it is a **Connect-RPC server** (`.build_connect()`, `lib.rs:60`; advertises `application/{json,proto,connect+json,connect+proto}`, no `application/grpc`) served over **HTTP/1.1 only** (`serve.rs:81-86` — no h2/ALPN). So SDKs are thin wrappers over **buf-generated Connect stubs** (connect-go, Connect-ES, connect-python) — never plain gRPC or gRPC-Web.

## The transport constraint (shapes the whole slice)

Connect requires **HTTP/2 for streaming** RPCs (`connectrpc-axum` `handler.rs:546`), and `Subscribe` is proto-declared **bidi** (`stream SubscribeFrame returns stream Inbound`, `gateway.proto:6`). A standards-compliant connect-go client treats it as a bidi stream and **requires HTTP/2** — even the `auto_commit`-only path (send one `SubscribeStart`, then read the `Inbound` stream). The gateway serves **HTTP/1.1 only** today (`serve.rs:81-86`; the code comment names the missing `auto::Builder`+ALPN). So:

- **`publish`/`publishEvent` (unary `Send`)** work over h1 unchanged — no gateway change.
- **`subscribe` (bidi)** needs the gateway to speak **HTTP/2**. MSG-5 therefore includes the small, already-flagged **h2c enablement**: swap the plaintext listener from `axum::serve` (h1) to `hyper_util … auto::Builder` (h1 + h2 auto-negotiated), so a connect-go client can open the `Subscribe` stream over HTTP/2 cleartext. Without it, a non-Rust Connect client cannot subscribe at all (the Rust in-process client tolerates h1 full-duplex; connect-go does not).
- **Manual per-message ack** — the interleaved client-`SubscribeAck` / server-`Inbound` full-duplex (`streaming.rs:298-321`) — stays **deferred**: even over h2 the commit semantics are advisory today (`gateway.proto:108-115`), so true per-offset ack is gated on **MSG-3**, and the SDK surfaces it as experimental. MSG-5's subscribe defaults to `auto_commit=true`.

## Design Goals

- **Connect codegen, no bespoke transport:** buf → connect-go stubs; the SDK is a thin ergonomic layer over them.
- **Go-first surface:** `publish`/`publishEvent` (unary, over h1) + `subscribe`-with-filter (bidi with `auto_commit`, over the h2c the gateway gains here) — the subset usable from a standard connect-go client.
- **Enable h2c on the gateway** so non-Rust Connect clients can open streams (the flagged `auto::Builder` swap) — a small, contained change that unblocks all SDK streaming.
- **Real test harness:** SDK tests run against a live gateway (a new gateway OCI image + a compose/testcontainers harness), not in-process Rust — grounded in the CI reality (Go toolchain present; no node/python; no gateway image).
- **Honest surface labeling:** experimental/deferred parts (manual ack, share-consume, auto-provision, CE-consume) are marked as such, gated on named net-new gateway work.

## Non-goals (deferred, each named)

- **TypeScript (Connect-ES) + Python SDKs** — gated on adding `setup-node`/`setup-python` to CI (absent today); the buf config + `sdks/` layout are built language-agnostic so they slot in.
- **Manual per-message ack** (the full-duplex `SubscribeAck`-interleaving path) — even over the h2c MSG-5 enables, its commit semantics are advisory until **MSG-3**; surfaced as experimental, not shipped.
- **`SendStream`** (bidi batch produce) — unary `Send` covers v1 publish; the full-duplex batch path is deferred with manual ack.
- **`EnsureTopic`/Admin gateway RPC** — topic auto-provision (only Kafka-wire `AdminClient::create_topics` exists; the gateway proto has no Admin RPC).
- **Share-group Subscribe mode on the gateway** — the KIP-932 queue wedge lives only in the native `client-consumer/src/share/`; `Subscribe` uses classic `ConsumeSession`. Surfacing it is net-new gateway proto+handler work.
- **SDK-side CloudEvents *consume*** — blocked on MSG-1 (`Inbound.headers` is a hardcoded empty map, `streaming.rs:153`); `publishEvent` (send) works today (transparent), receiving `ce_*` does not.
- **Full SQL filter** — `FieldPredicate` is JSONPath **EQUALS-only** and matches only decoded structured records (`streaming.rs:89-103`); the SDK filter builder must not imply SQL.

## Architecture Overview

```
sdks/go  (thin ergonomic wrapper)
  publish(topic, value, opts)         → unary  Send(SendRequest{records:[Record], acks})
  publishEvent(topic, cloudEvent)     → unary  Send  (binary-mode CE: ce_<name> underscore headers,
                                                       datacontenttype→content-type, data→Record.raw)
  subscribe(topic, {group, filter})   → bidi Subscribe(SubscribeStart{auto_commit=true, predicates})
        └─ sends one Start frame, then ranges the server Inbound{topic,partition,offset,key,value,headers,…}
  config: endpoint URL + optional bearer token / mTLS cert   (anonymous fallback for dev)
        │
        ▼  Connect protocol — unary Send over HTTP/1.1; Subscribe over HTTP/2 (h2c)
  crabka-gateway  (plaintext listener: axum::serve → hyper_util auto::Builder for h1+h2)  ──(Kafka wire)──►  broker
        ▲
  buf generate (buf.yaml + buf.gen.yaml, rooted at crates/grpc-gateway/proto)
        → sdks/go/gen/…  (connect-go + protoc-gen-go stubs; drift-checked in CI)

  test harness (new): packaging/apko/crabka-gateway.yaml (OCI image) + docker-compose
        → Go CI job: docker run gateway+broker, `go test ./sdks/go/...` round-trip
```

## Key Design Decisions

### Connect codegen via buf (not gRPC, not the Rust build path)

The wire is the Connect protocol; SDKs generate stubs with **buf** (`buf.gen.yaml` invoking `protoc-gen-connect-go` + `protoc-gen-go`). The repo has no buf config today (Rust codegen is `connectrpc-axum-build` + vendored protoc inside `build.rs`, `OUT_DIR`-only). The `.proto` stays canonical at `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto` — **never copied**; buf roots there. buf also brings `buf lint`/`buf breaking` proto governance the repo lacks. Generated stubs live in `sdks/<lang>/gen/` and are drift-checked in CI (mirror `codegen-check.yml`'s regenerate-then-`git diff`).

### Go first

Over the conventional TS-first instinct, for three grounded reasons: (1) CI already installs Go (`actions/setup-go@v6` in 5 workflows) so a Go SDK job needs no new toolchain, whereas there is **no** `setup-node`/`setup-python` anywhere; (2) connect-go is the reference Connect implementation and the gateway's own compatibility target (`lib.rs:56-59`); (3) a Go h1 client handles unary `Send` + server-streaming `Subscribe` cleanly — exactly the h1-safe surface. TS (Connect-ES) is the highest-DX follow-up but browser Connect can't do server-streaming over h1 without care and there is no gRPC-Web proxy — a browser SDK may need a separate path. Python is third. **Rust is not a target** — the native crates already *are* the Rust client (and speak Kafka-wire, not Connect). One `gateway.proto` is the single source of truth; do not fork per language.

### The ergonomic surface maps 4 verbs onto the h1-safe RPCs

- **`publish(topic, value, {key?, headers?, partition?, idempotencyKey?})`** → unary `Send`; returns `RecordResult{partition, offset, deduplicated, error?}`. Structured publish uses `Record.structured` + `SchemaSelector` for JSON→registry serialization.
- **`publishEvent(topic, cloudEvent)`** → the MSG-2 binary-mode CE representation, set directly on `Record.headers` (Connect/gRPC Send is transparent — underscore `ce_<name>`, no hyphen translation): required `ce_id`/`ce_source`/`ce_type`/`ce_specversion`, `datacontenttype`→bare `content-type`, `data`→`Record.raw`. Never emit `ce_datacontenttype`.
- **`subscribe(topic, {group, filter?}, handler)`** → `SubscribeStart{group_id, topics, auto_commit:true, predicates}` then ranges the server `Inbound` stream; the filter builder exposes `FieldPredicate{path (JSONPath), op:EQUALS, value}` — **documented as equality-only, structured-records-only**.
- **Manual per-message ack, share-consume, topic auto-provision** are surfaced as **experimental/unimplemented** with doc links to the gating gateway work — never silently promised.

### The test harness is net-new and leads the plan

There is **no** gateway OCI image (the `publish-images` matrix is broker/operator/schema-registry/bench-driver) and current gateway tests are **in-process Rust** (no network endpoint an external Go process can reach). So MSG-5 builds: (1) `packaging/apko/crabka-gateway.yaml` + a `publish-images` matrix entry (entrypoint `/usr/bin/gateway`); (2) a docker-compose/testcontainers harness launching gateway+broker; (3) a Go CI job (reusing `setup-go`) that runs a real `publish → subscribe(auto_commit) → assert` round-trip against `localhost`. Tests exercise behavior against the live gateway — never read SDK source text (CLAUDE.md).

### Auth config

SDK config takes an endpoint URL + optional **bearer token** (`Authorization: Bearer <JWS>`, `sub`+`exp`) and/or **mTLS** client cert; bearer overrides the mTLS principal; anonymous fallback for dev. Bearer tokens are **unsecured JWS (`alg:none`) — dev/test only**; the SDK docs must not imply production token validation. mTLS-in-a-browser is impossible, so a future browser TS SDK steers to bearer.

## Integration

- **`gateway.proto:4-8,59-73,101-115,34-43`** — the codegen contract; `Record`/`SubscribeStart`/`SubscribeAck`/`FieldPredicate` shapes the wrappers target. **No proto change** for MSG-5 v1 (CE rides existing `Record.headers`).
- **`crates/grpc-gateway/src/serve.rs:81-86`** — swap the plaintext `axum::serve` (h1) for `hyper_util … auto::Builder` (h1+h2c) so connect-go can open the bidi `Subscribe` stream. The one gateway code change in MSG-5.
- **`sdks/`** (new top-level, sibling to `crates/`) — out of the Cargo workspace + release-plz; `sdks/go` first.
- **`buf.yaml` + `buf.gen.yaml`** (repo root, new) — Connect codegen.
- **`packaging/apko/crabka-gateway.yaml`** (new) + `publish-images.yml` — the gateway OCI image.
- **`.github/workflows/`** — a Go SDK job (reuses `setup-go`); a buf drift check.

## Kafka / wire compliance

- **SDK ↔ gateway is Connect; gateway ↔ broker is unchanged Kafka wire.** No Kafka/KIP byte change; the SDK is a gateway client.
- **CloudEvents byte-exactness (send side):** the conformance target — a CE published via the SDK (binary mode, `ce_*` headers) is consumed byte-exact by a stock JVM Kafka client — is the vision doc's success checkpoint and is testable today (send is transparent; receive needs MSG-1).

## Testing

- **CE header mapping (pure Go unit):** `publishEvent` produces a `Record` with `ce_id/ce_source/ce_type/ce_specversion` underscore headers, `content-type` from `datacontenttype`, `data` in `raw`, and **no** `ce_datacontenttype`.
- **Live round-trip (integration, against the compose gateway):** `publish(topic, v)` then `subscribe(topic, {group})` receives `v` byte-exact; a `FieldPredicate` equality filter delivers only matches.
- **CE→JVM conformance:** a CE published via the SDK is consumed byte-exact by a stock JVM Kafka client (send side). **Caveat:** the JVM differential suite rewrites tracked protocol corpus fixtures — a conformance job reusing that path must restore them ([[differential-corpus-side-effect]]).
- **buf drift:** `buf generate` produces no diff against the committed stubs.

## Risks (carried into the plan)

- **Transport (highest):** `Subscribe` is bidi and connect-go needs HTTP/2 for it, so MSG-5 must land the h2c listener swap (`serve.rs:81-86`) for any SDK subscribe to work — verify a connect-go bidi Subscribe over h2c end-to-end early (this is the riskiest assumption). Unary `publish` is unaffected. Manual per-message ack stays deferred (advisory semantics until MSG-3); do not over-promise per-message at-least-once.
- **CI polyglot gap:** TS/Python SDK CI is blocked until `setup-node`/`setup-python` + the gateway image + the compose harness land; Go reuses `setup-go`.
- **Deferred gateway RPCs:** share-consume and topic auto-provision are net-new gateway proto+handler work, not SDK packaging — the SDK exposes them as unimplemented until then.
- **Filter/CE-consume/bearer honesty:** `FieldPredicate` is EQUALS-only + structured-only; CE-consume needs MSG-1; bearer is dev/test-only — all must be documented as such, not implied.

## Resolved decisions (from grounding)

- **Transport:** Connect protocol via buf (connect-go); never gRPC/gRPC-Web.
- **Surface:** unary `publish`/`publishEvent` (h1) + `subscribe`+filter (bidi, `auto_commit`, over the h2c MSG-5 enables); manual ack/share-consume/auto-provision deferred to named gateway work.
- **Transport:** MSG-5 enables h2c on the gateway plaintext listener (`serve.rs:81-86` `auto::Builder`) so connect-go can open the bidi `Subscribe` stream.
- **Language:** Go first (CI + connect-go reference); TS/Python gated on CI toolchains; Rust not a target.
- **Layout:** new `sdks/` (outside the Cargo workspace) + repo-root buf config; `.proto` canonical, stubs drift-checked.
- **Testing:** a new gateway OCI image + compose harness + a Go round-trip CI job; behavior tests only.
- **Positioning:** parity packaging; value is exposing the substrate, not novelty.
