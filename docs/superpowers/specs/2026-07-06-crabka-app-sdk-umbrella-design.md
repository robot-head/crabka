# The Crabka application SDK — umbrella contract + Go reference — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Umbrella design (Chapter F's SDK face). Defines the **language-agnostic module contract** every SDK implements, the **conformance suite** that enforces it, and the **Go reference** cycle — with TS, Java, Rust, and C++ following in their own cycles against the frozen contract. Extends [MSG-5](2026-07-06-crabka-polyglot-messaging-sdk-design.md) (the messaging-SDK foundation: `sdks/` layout, buf codegen, the Connect-transport ground truth, the gateway h2c prerequisite) into the full application-SDK surface.

## Context — the decisions that shape this

Three user decisions fix the frame: **(1) full-vision surface with stubs** — every module's interface is defined day one; unbuilt modules fail with documented, machine-readable `Unimplemented` errors naming their gating work; **(2) phased, Go reference first** — one cycle ships the contract + conformance suite + Go; each other language is its own later cycle; **(3) contract-as-conformance-suite** (approach A) — semantic uniformity is proven by *execution against a live system*, the same discipline as the JVM-differential and Postgres standby oracles, applied to our own surface.

Honesty constraints inherited from the substrate: over the landed gateway only publish/CloudEvents/subscribe-with-filter/webhooks are serveable; queue-consume needs a net-new gateway share-group RPC (broker side fully built); the database face needs Chapter C executed; identity needs Chapter E designed; blob has no server API (Chapter B deprioritized).

## The module contract

One client object, six modules. Identical **semantics** everywhere; idiomatic **shape** per language (Go returns `(T, error)`, TS returns promises, Rust returns `Result` — the conformance suite tests behavior, not signatures).

| Module | v1 state | Surface | Gated on |
|---|---|---|---|
| `client` | live | endpoint config; credentials (bearer / mTLS); the error taxonomy; telemetry hooks (pluggable logger/metrics callbacks) | — |
| `messaging` | **live** | `publish(topic, value, opts)`, `publishEvent(topic, cloudEvent)` (binary-mode CE: `ce_*` underscore headers per MSG-2), `subscribe(topics, {group, filter})` (auto-commit; `filter` = the Chapter-G surface, EQUALS-only today — documented); manual per-offset ack **experimental** | ack: MSG-3 + the h2c listener |
| `queues` | stub | `acquire(topic, {group, max, lockDuration})` → messages with `deliveryCount`; per-message `ack` / `release` / `reject` — the API mirrors the **landed KIP-932 broker semantics** so the stub interface will not churn when the RPC lands | the gateway share-group RPC |
| `database` | stub | **connection handoff only**: `connect(name)` → Postgres connection parameters (host/port/db/credentials) for the language's standard driver. The SDK never wraps SQL — the compute speaks plain Postgres wire | Chapter C execution + a control plane |
| `auth` | split | credential **configuration** is live (bearer token — dev/test-grade unsecured JWS, documented; mTLS cert paths). Identity APIs (sign-in, sessions, user management) are **versioned out entirely** — no stubbed guess for Chapter E to break | Chapter E design |
| `blob` | stub | `put(key, bytes)`, `get(key)`, `list(prefix)` | Chapter B server API |

## The error taxonomy (what makes stubs honest)

A closed set, mapped from Connect error codes, identical across languages:

`Transport` · `Unauthenticated` · `InvalidArgument` · `NotFound` · `ServerError` · **`Unimplemented { module, gated_on }`**

`gated_on` is the **design-doc slug** of the gating work (e.g. `"gateway-sharegroup-rpc"`, `"pg5-compute"`, `"chapter-e-auth"`) — machine-readable, so the conformance suite asserts every stub's exact error across every language. A stub is a contract, not a TODO: its module, message, and slug are vector-pinned.

## The conformance suite (the enforcement)

- **`crates/sdk-conformance`** (`crabka-sdk-conformance`, **`publish = false`** + the release-plz private entry) — a Rust harness that boots **broker + gateway in-process** (the existing integration-test pattern — no containers, no OCI image needed) and drives any SDK through a per-language **adapter CLI** over **JSON-lines on stdio**: the harness writes command objects (`{"cmd": "publish", "topic": …}`), the adapter calls its SDK and replies (`{"ok": …}` / `{"error": {"kind": "unimplemented", "module": "queues", "gated_on": …}}`). Placed under `crates/` so the `members = ["crates/*"]` glob and workspace lints apply unchanged.
- **Vectors** (`crates/sdk-conformance/vectors/*.json`, versioned with the contract) cover: publish→subscribe round-trip (byte-exact value); `publishEvent` CE mapping (`ce_id/ce_source/ce_type/ce_specversion` underscore headers, `content-type` from `datacontenttype`, never `ce_datacontenttype`); filter delivery **and** non-delivery; every stub's exact `Unimplemented{module, gated_on}`; credential config (bearer header present; anonymous fallback); error mapping (unknown topic, bad argument, unreachable endpoint → `Transport`).
- **The suite version is the contract version** (semver; vectors additive within a major). Each SDK declares the contract version it passes; a green suite is a language cycle's definition of done. The Go reference hardens the vectors before any port begins.

## The Go reference (this cycle)

Extends MSG-5's `sdks/go` — MSG-5's plan remains the messaging core (buf stubs, `publish`/`publishEvent`/`subscribe`, the gateway h2c enablement). This cycle adds: the module layout (`client` + the five modules), the error taxonomy + stubs, the `conformance-adapter` CLI, and a green suite wired into CI (reusing `setup-go`; the harness is in-process Rust, so the job needs no Docker).

## The per-language cycle map (later cycles, each against the frozen contract)

- **TypeScript** — Connect-ES (mature). CI prerequisite: `setup-node`.
- **Java** — connect-kotlin on the JVM; CI already carries Gradle (the JVM oracle).
- **Rust** — a gateway **Connect client** (hand-rolled over hyper; the protocol knowledge is in-house — we ship the connectrpc-axum server). **Deliberately not** a re-export of the native Kafka-wire crates: the native path bypasses gateway-enforced filters and diverges semantically; the native crates remain the infrastructure-grade client.
- **C++** — the expensive cell, last and eyes open: no Connect client exists anywhere; hand-rolled unary + server-streaming transport, CMake, a new CI toolchain job.

Each cycle = its own spec+plan: transport client, the module layout, the adapter, suite green, contract version declared.

## Kafka / wire compliance

The SDK is a gateway client — the Kafka wire is untouched. The CE mapping must stay byte-faithful to MSG-2's binding (vector-pinned); delivered record values are byte-exact (vector-pinned round-trip).

## Testing

The conformance suite **is** the test strategy (behavior against a live gateway — never SDK source-text assertions). Additionally per-language unit tests for pure logic (CE mapping, config parsing) follow each language's conventions; the harness self-tests with a built-in mock adapter before any real SDK exists.

## Risks

- **Contract churn is now five-times expensive** — the reason for Go-first hardening and semver discipline; vectors are additive within a major.
- **The stub-shaped `queues` API is a bet** that the gateway share-group RPC will mirror broker KIP-932 semantics — grounded (the broker state machine is landed and JVM-validated), but the RPC design could still adjust names/fields: acceptable, that's a minor-version vector update, not an interface rewrite.
- **`database` handoff needs a control plane that doesn't exist** — the stub returns `Unimplemented{gated_on: "chapter-f-control-plane"}`; the interface (connection parameters) is the least-speculative possible shape.
- **Adapter-protocol drift** — versioned alongside the vectors; the mock adapter pins it.

## Resolved decisions

- **Surface:** full vision with stubs (user); six modules; identity versioned out, not stubbed.
- **Phasing:** Go reference first (user); TS → Java → Rust → C++ as separate cycles.
- **Enforcement:** approach A — the conformance suite with per-language adapter CLIs over JSON-stdio; suite version = contract version.
- **Rust:** gateway-Connect client, not native-crate re-export.
- **Errors:** closed taxonomy; `Unimplemented{module, gated_on}` with design-doc slugs.
- **Placement:** harness at `crates/sdk-conformance` (workspace glob + lints unchanged); SDKs stay under `sdks/`.
