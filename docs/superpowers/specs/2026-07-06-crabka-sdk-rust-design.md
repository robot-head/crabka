# The Rust SDK — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Language cycle under the [application-SDK umbrella](2026-07-06-crabka-app-sdk-umbrella-design.md). Implements contract v1 in Rust; definition of done = the conformance suite green through the Rust adapter.

## Context — why this exists next to the native crates

Crabka already ships Rust clients — the native Kafka-wire crates (`client-producer`/`client-consumer`/…). The umbrella pinned the decision this spec executes: the app SDK **rides the gateway** like every other language, because the native path *bypasses gateway-enforced subscription filters* and diverges from the contract's semantics (CE handling, error taxonomy, stub behavior). The two surfaces coexist deliberately: **`crabka-app-sdk` = the BaaS contract surface; the native crates = the infrastructure-grade Kafka client.** The rustdoc states this split on the front page.

The net-new piece is the transport: **no Connect *client* exists for Rust** (we ship the connectrpc-axum *server*). This cycle hand-rolls a minimal Connect client — unary + bidi streaming over hyper h2c — sized to exactly what the SDK needs, with the protocol knowledge already in-house.

## Design Goals

- **A minimal in-house Connect client** (`connect_client` module): unary = HTTP POST with `content-type: application/proto`, prost bodies; streaming = the Connect enveloped-message framing (1-byte flags + u32 length prefix; the end-of-stream trailer frame carrying the JSON error/trailers) over a hyper h2c connection. Only what `Send`/`Subscribe` need — not a general-purpose Connect implementation.
- **The umbrella contract verbatim:** six modules on a `CrabkaClient`; `CrabkaError` with `Unimplemented { module, gated_on }` carrying the pinned slugs; CE mapping vector-identical; subscribe as a `futures::Stream<Item = Result<Inbound, CrabkaError>>`.
- **Workspace placement with rationale:** the crate lives at **`crates/app-sdk`** (`crabka-app-sdk`, `publish = false` + the private release-plz entry) — inside the workspace for lints/deps/CI-for-free. The umbrella's "SDKs live under `sdks/`" rule targeted foreign-ecosystem packaging (npm/Gradle/CMake); a Rust crate's ecosystem *is* the workspace. The umbrella doc gains one clarifying line.
- **Proto types shared, not duplicated:** a `build.rs` compiling the gateway proto with prost (the `connectrpc-axum-build`-adjacent pattern already used server-side), messages only — the service stubs are the hand-rolled client's job.

## Non-goals

A general-purpose `connect-rust` library (extract later if a second consumer appears); wasm targets; manual per-offset ack beyond the experimental flag; crates.io publication (the allowlist governs; revisit with SDK release engineering).

## Architecture

```
crates/app-sdk  (crabka-app-sdk — publish = false; tokio + hyper + prost)
├── build.rs                    prost codegen of the gateway proto (messages only)
├── src/connect_client.rs       unary POST + enveloped-stream framing over hyper h2c
│                               (flags|len prefix; EndStream trailer → ConnectError{code,msg})
├── src/error.rs                CrabkaError: Transport | Unauthenticated | InvalidArgument
│                               | NotFound | ServerError | Unimplemented{module, gated_on}
├── src/client.rs               CrabkaClient::builder(endpoint).bearer(...).build()
│                               → .messaging() / .queues() / .database() / .auth() / .blob()
├── src/messaging.rs            publish / publish_event (CE mapping) /
│                               subscribe(topics, group, filter) -> impl Stream<Inbound>
├── src/stubs.rs                the three gated stubs (pinned slugs); auth = config only
└── src/bin/conformance_adapter.rs   JSON-lines stdio ↔ the SDK
```

## Integration

- **`crates/app-sdk`** — new workspace member; **`publish = false` + release-plz private entry** (the allowlist gate).
- **The umbrella doc** — one-line clarification of the placement rule (Rust in-workspace).
- **CI:** no new toolchain — the existing Rust matrix builds/tests it; an `sdk-rust` leg of the conformance job runs the suite with `--adapter target/…/conformance_adapter`.
- **The harness/vectors unchanged.**

## Testing

Unit tests for the framing codec (envelope round-trip, trailer-frame error decode — the one genuinely new wire code in this cycle, tested against captured bytes from the in-process gateway), the CE mapping, and the taxonomy; **the conformance suite is the gate**.

## Risks

- **The hand-rolled framing is the risk concentration** — mitigated by testing against *our own server's actual bytes* (captured from connectrpc-axum in-process — an oracle we fully control) and by the suite.
- **Two-Rust-surfaces confusion** — mitigated by the front-page rustdoc split statement and the crate name (`app-sdk` vs the `client-*` crates).
- **h2c client config** (hyper prior-knowledge) — smoke-first, as in every language cycle.

## Resolved decisions

Gateway-riding (umbrella-pinned; native crates remain the infra client); a minimal in-house Connect client scoped to Send/Subscribe; `crates/app-sdk` in-workspace with the placement rationale; prost message codegen shared with the server's proto; suite as the gate.
