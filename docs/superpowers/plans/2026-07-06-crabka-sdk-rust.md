# The Rust SDK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `crabka-app-sdk` (`crates/app-sdk`, in-workspace, `publish = false`): a minimal in-house Connect client (unary + enveloped bidi streaming over hyper h2c), the six-module contract surface with pinned stubs, a `Stream`-based subscribe, and a conformance adapter — suite green with no new CI toolchain.

**Architecture:** prost message codegen off the gateway proto; `connect_client` implements exactly the framing `Send`/`Subscribe` need, oracle-tested against our own connectrpc-axum server's bytes; the idiomatic layer mirrors the vector-pinned semantics; the umbrella harness/vectors unchanged.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `tokio`, `hyper`/`hyper-util` (h2c client), `prost` (+ build codegen), `bytes`, `futures`, `serde_json` (adapter), `assert2`/`nextest`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-sdk-rust-design.md`](../specs/2026-07-06-crabka-sdk-rust-design.md).

**PREREQUISITES (unlanded):** the umbrella executed (harness + Go-hardened vectors) and MSG-5's gateway h2c listener.

---

## Invariants

1. **The framing is oracle-tested** — envelope/trailer codecs are validated against bytes captured from the in-process connectrpc-axum gateway, not against our own reading of the spec.
2. **Semantics from the vectors** — the suite is the gate; vectors never edited to fit Rust.
3. **Stubs carry the pinned slugs** byte-identically; **two-surfaces clarity** — the front-page rustdoc states the app-sdk vs native-crates split.
4. **New-crate hygiene:** `publish = false` + the release-plz private entry; workspace lints apply (no `unsafe`).
5. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the crate + prost codegen; the Connect client (unary + bidi streaming + trailer errors); client/taxonomy/messaging/stubs; the adapter; the CI leg; the umbrella placement-rule clarification.
- **Deferred:** a general connect-rust library; wasm; manual ack; publication.

---

## Task 1: Crate scaffold + prost codegen + the umbrella clarification

- [ ] **Step 1:** Scaffold `crates/app-sdk` (`publish = false`, workspace lints; deps above) with a `build.rs` compiling `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto` via prost (messages only). Add the release-plz private entry. One-line placement clarification in the umbrella spec.
- [ ] **Step 2:** A trivial type-visibility test (`pb::SendRequest::default()` constructs) compiles green; `./tools/check-publish-allowlist.sh` → 0; commit.

```bash
git add crates/app-sdk release-plz.toml docs/superpowers/specs/2026-07-06-crabka-app-sdk-umbrella-design.md
git commit -m "feat(app-sdk): crate scaffold with prost codegen off the gateway proto"
```

---

## Task 2: The Connect client — unary + enveloped streaming (oracle-tested)

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn envelope_round_trips() {
        let msg = b"payload";
        let framed = envelope::encode(0x00, msg);
        assert!(framed[0] == 0 && u32::from_be_bytes(framed[1..5].try_into().unwrap()) == 7);
        let_assert!(Frame::Message(m) = envelope::decode_one(&framed).unwrap());
        assert!(m.as_ref() == msg);
    }

    #[test]
    fn end_stream_trailer_decodes_connect_error() {
        // flags 0x02 frame with {"error":{"code":"not_found","message":"…"}}
        let framed = envelope::encode(0x02, br#"{"error":{"code":"not_found","message":"x"}}"#);
        let_assert!(Frame::EndStream(t) = envelope::decode_one(&framed).unwrap());
        assert!(t.error.unwrap().code == "not_found");
    }

    #[tokio::test]
    async fn oracle_unary_and_stream_against_our_own_server() {
        // Boot the in-process gateway (grpc-gateway test harness pattern) on h2c;
        // unary Send round-trips; Subscribe: send one Start frame, receive Inbound
        // frames, server close yields a clean EndStream. THE framing oracle.
    }
```

- [ ] **Step 2:** Implement `connect_client.rs`: a hyper h2c connection (`http2_only`, prior knowledge); `unary<Req, Resp>(path, req)` (POST, `content-type: application/proto`, non-200/error-body → `ConnectError{code}`); `bidi(path)` returning a sender half (envelope-encode outgoing prost messages) + a `Stream` of decoded frames (message → prost decode; `EndStream` → terminate with trailer error mapping); bearer header injection.
- [ ] **Step 3:** Green (incl. the oracle test); commit.

```bash
git add crates/app-sdk/src
git commit -m "feat(app-sdk): minimal Connect client (unary + enveloped bidi), oracle-tested"
```

---

## Task 3: Taxonomy, client, messaging, stubs

- [ ] **Step 1: Failing tests:** stub calls yield `CrabkaError::Unimplemented{module: "queues", gated_on: "gateway-sharegroup-rpc"}` (and the database/blob slugs); Connect codes map (`not_found` → `NotFound`, transport I/O → `Transport`); the CE mapping test (identical assertions to Go/TS: `ce_*` underscore, `content-type`, never `ce_datacontenttype`).
- [ ] **Step 2:** Implement `CrabkaClient::builder()`, `error.rs`, `messaging.rs` (`publish`, `publish_event`, `subscribe -> impl Stream<Item = Result<Inbound, CrabkaError>>` — one `SubscribeStart{auto_commit: true}` frame then the mapped inbound stream; drop closes), `stubs.rs` from a shared macro/factory; the front-page rustdoc split statement.
- [ ] **Step 3:** Green; commit.

```bash
git add crates/app-sdk/src
git commit -m "feat(app-sdk): client, taxonomy, messaging, gated stubs"
```

---

## Task 4: The conformance adapter + suite green

- [ ] **Step 1:** `src/bin/conformance_adapter.rs`: JSON-lines stdio loop (reusing the harness's `protocol.rs` types via a dev-dependency on `crabka-sdk-conformance` — one wire definition, zero drift) → the SDK; `Hello{contract_major: 1, language: "rust"}`; `Subscribe`/`NextMessage` bridged via a buffered channel.
- [ ] **Step 2:** Run the real suite with the Rust adapter → **all vectors PASS** (fix the SDK, never the vectors).
- [ ] **Step 3:** Commit.

```bash
git add crates/app-sdk
git commit -m "feat(app-sdk): conformance adapter; suite green (contract v1)"
```

---

## Task 5: CI leg + final gate

- [ ] **Step 1:** Extend the conformance CI job with the Rust leg (build the adapter with the workspace toolchain — no new setup); the crate rides the existing Rust matrix for fmt/clippy/tests automatically.
- [ ] **Step 2:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-app-sdk --all-targets -- -D warnings`; `cargo nextest run -p crabka-app-sdk`; allowlist → 0. Commit.

```bash
git add .github/workflows
git commit -m "ci(app-sdk): rust conformance leg"
```

---

## Self-Review

**1. Spec coverage:** scaffold + codegen + placement clarification (Task 1); the oracle-tested Connect client (Task 2); taxonomy/messaging/stubs + the two-surfaces rustdoc (Task 3); adapter sharing the harness protocol types (Task 4); zero-new-toolchain CI (Task 5). General connect-rust/wasm/publication deferred — Scope boundary. ✅
**2. Placeholder scan:** framing tests concrete (flag bytes, trailer JSON); the oracle test names its harness pattern; slugs pinned in tests. No `TBD`.
**3. Type consistency:** `CrabkaError::Unimplemented{module, gated_on}` ↔ the shared wire types (Task 4 reuses `protocol.rs` — drift impossible); prost messages come from the same proto the gateway serves.
**4. Invariant check:** oracle-tested framing (Task 2); vectors unedited (Task 4); slugs pinned (Task 3); hygiene + no-unsafe via workspace lints (Tasks 1, 5).
**5. Prerequisites flagged:** umbrella + h2c listener (header). The adapter's dev-dependency on the harness crate is intra-workspace — no cycle (harness never depends on the SDK).
