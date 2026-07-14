# The Crabka application SDK — umbrella + Go reference — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The language-agnostic SDK contract made executable: a Rust conformance harness (`crabka-sdk-conformance`) driving per-language adapter CLIs over JSON-stdio against an in-process broker+gateway, contract vectors v1, and the Go reference SDK (module layout, error taxonomy, stubs with `Unimplemented{module, gated_on}`, the adapter) passing the suite in CI.

**Architecture:** The harness boots broker+gateway in-process (existing test pattern — no containers), spawns an adapter subprocess, and exchanges JSON lines (`{"cmd": …}` → `{"ok": …}|{"error": …}`). A built-in mock adapter self-tests the harness before any SDK exists. The Go SDK extends MSG-5's `sdks/go` with the app-SDK module layout and ships `conformance-adapter`.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0, harness: `serde_json`, `tokio::process`, the in-process `Broker::start` + gateway router), Go 1.2x + connect-go (per MSG-5), `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`, `gofmt`/`go vet`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-app-sdk-umbrella-design.md`](../specs/2026-07-06-crabka-app-sdk-umbrella-design.md).

**PREREQUISITES (unlanded):** **MSG-5 executed** (the Go messaging core: buf stubs, `publish`/`publishEvent`/`subscribe`, and the gateway h2c listener — this plan's Go tasks build on those). The harness tasks (1–3) have no unbuilt prerequisites.

---

## Invariants

1. **Behavior over signatures:** the suite tests semantics through the adapter; it never inspects SDK source.
2. **Stubs are vector-pinned:** every unbuilt module's call returns exactly `Unimplemented{module, gated_on: <design-doc slug>}` — asserted, not documented-only.
3. **The suite version is the contract version** — vectors additive within a major; the harness refuses an adapter declaring a different major.
4. **In-process substrate:** the harness boots broker+gateway itself; CI needs no Docker and no new toolchains (Go is already present).
5. **New-crate hygiene:** `crabka-sdk-conformance` is `publish = false` + release-plz private entry.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the harness crate + adapter protocol + mock-adapter self-test; vectors v1 (messaging round-trip, CE mapping, filter, stubs, config, error mapping); the Go module layout + taxonomy + stubs + adapter; the CI job.
- **Deferred:** TS/Java/Rust/C++ cycles; the queues RPC; identity APIs; manual-ack vectors (experimental until MSG-3); the control plane.

---

## File Structure

- **`crates/sdk-conformance/`** (new crate `crabka-sdk-conformance`): `src/{lib.rs, protocol.rs, harness.rs, mock_adapter.rs}`, `src/bin/conformance.rs`, `vectors/v1/*.json`, `tests/self_test.rs`.
- **`sdks/go/`** (extends MSG-5): `crabka/{client.go, errors.go, queues.go, database.go, auth.go, blob.go}` (+ MSG-5's messaging files), `cmd/conformance-adapter/main.go`.
- **`release-plz.toml`** — the private entry.
- **`.github/workflows/`** — the SDK conformance job.

**Batching:** Task 1 (protocol + mock) → Task 2 (harness+substrate) → Task 3 (vectors). Task 4 (Go layout/stubs) is parallel with 1–3 once MSG-5's Go core exists. Task 5 (Go adapter) needs 3 + 4. Task 6 (CI) last.

---

## Task 1: The adapter protocol + mock-adapter self-test

**Files:**
- Create: `crates/sdk-conformance/{Cargo.toml, src/lib.rs, src/protocol.rs, src/mock_adapter.rs}`
- Modify: `release-plz.toml`

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn protocol_round_trips() {
        let cmd = Command::Publish { topic: "t".into(), value_b64: "aGk=".into(), headers: vec![] };
        let line = serde_json::to_string(&cmd).unwrap();
        assert!(serde_json::from_str::<Command>(&line).unwrap() == cmd);
        let err: Response = serde_json::from_str(
            r#"{"error":{"kind":"unimplemented","module":"queues","gated_on":"gateway-sharegroup-rpc"}}"#,
        ).unwrap();
        let_assert!(Response::Error(e) = err);
        assert!(e.kind == ErrorKind::Unimplemented && e.gated_on.as_deref() == Some("gateway-sharegroup-rpc"));
    }

    #[tokio::test]
    async fn mock_adapter_answers_hello_and_publish() {
        let mut a = spawn_mock_adapter().await;                      // in-process, same protocol
        let hello = a.call(Command::Hello).await.unwrap();
        let_assert!(Response::Hello { contract_major: 1, .. } = hello);
        let_assert!(Response::Ok(_) = a.call(Command::Publish { .. }).await.unwrap());
    }
```

- [ ] **Step 2: Implement**

`protocol.rs`: `Command` (tagged enum: `Hello`, `Configure{endpoint, bearer?}`, `Publish{topic, value_b64, headers}`, `PublishEvent{topic, event}`, `Subscribe{topics, group, filter?}`, `NextMessage{timeout_ms}`, `QueueAcquire{..}`, `QueueAck{..}`, `DbConnect{name}`, `AuthSignIn{..}`, `BlobPut{..}`, `BlobGet{..}`) and `Response` (`Hello{contract_major, language}`, `Ok(Value)`, `Message{topic, partition, offset, value_b64, headers}`, `Error{kind, module?, gated_on?, message}`), all serde JSON-lines. `mock_adapter.rs`: an in-process adapter answering canned responses — pins the protocol before any SDK exists. `Cargo.toml` `publish = false` + the release-plz private entry.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-sdk-conformance` → PASS; `./tools/check-publish-allowlist.sh` → 0.

```bash
git add crates/sdk-conformance release-plz.toml
git commit -m "feat(sdk-conformance): adapter protocol + mock-adapter self-test"
```

---

## Task 2: The harness — in-process substrate + subprocess adapters

**Files:**
- Create: `crates/sdk-conformance/src/harness.rs`, `src/bin/conformance.rs`, `tests/self_test.rs`

- [ ] **Step 1: Write the failing test** (`tests/self_test.rs`)

Boot the harness with the **mock adapter as a subprocess** (`cargo run --bin conformance -- --adapter <mock binary>` shape, or spawn the mock binary directly): the harness starts an in-process broker + gateway (`Broker::start(BrokerConfig::for_tests)` + the gateway router on an ephemeral port — the `grpc-gateway` integration pattern), sends `Hello` → validates `contract_major`, sends `Configure{endpoint}`, runs a trivial scenario, and reports a structured pass/fail summary. Assert: a mock that answers wrongly produces a **named** vector failure (harness diagnostics carry the vector id + expected/got).

- [ ] **Step 2: Implement**

`harness.rs`: substrate boot (broker + gateway in-process; gateway serving h2c per MSG-5's listener); adapter subprocess management (`tokio::process`, line-buffered stdio, per-call timeout → `AdapterTimeout`); scenario runner consuming vector files; the summary report. `bin/conformance.rs`: CLI (`--adapter <path>`, `--vectors <dir>`, `--filter <id>`).

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-sdk-conformance --test self_test` → PASS.

```bash
git add crates/sdk-conformance
git commit -m "feat(sdk-conformance): harness with in-process substrate + subprocess adapters"
```

---

## Task 3: Vectors v1

**Files:**
- Create: `crates/sdk-conformance/vectors/v1/*.json`

- [ ] **Step 1: Author the vectors** (each: id, setup, adapter commands, expected responses/effects):

`messaging_roundtrip` (publish → subscribe → byte-exact value); `ce_binary_mapping` (publishEvent → the consumed record carries `ce_id/ce_source/ce_type/ce_specversion` + `content-type`, never `ce_datacontenttype` — the harness consumes via a native Rust client to inspect raw headers); `filter_delivers_matches_only` (two records, one matching structured filter → exactly one `Message`); `stub_queues`/`stub_database`/`stub_blob` (each call → `Unimplemented{module, gated_on}` with the pinned slugs `gateway-sharegroup-rpc` / `chapter-f-control-plane` / `chapter-b-blob-api`); `auth_bearer_config` (Configure with bearer → the gateway sees the `Authorization` header — assert via a gateway-side test hook or an authz-required call succeeding); `error_mapping` (unknown topic → `NotFound`; unreachable endpoint → `Transport`; bad argument → `InvalidArgument`).

- [ ] **Step 2: Extend the mock adapter** to pass every vector (canned but correct behavior — proves vectors are runnable and unambiguous before Go exists). Run: the full suite against the mock → PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/sdk-conformance/vectors crates/sdk-conformance/src
git commit -m "feat(sdk-conformance): contract vectors v1 (mock-validated)"
```

---

## Task 4 (∥ 1–3, after MSG-5's Go core): Go module layout + taxonomy + stubs

**Files:**
- Create: `sdks/go/crabka/{client.go, errors.go, queues.go, database.go, auth.go, blob.go}`

- [ ] **Step 1: Write the failing Go unit tests**

```go
func TestStubModulesReturnGatedUnimplemented(t *testing.T) {
    c := crabka.New("http://localhost:0", nil)
    _, err := c.Queues().Acquire(context.Background(), "t", crabka.AcquireOptions{})
    var u *crabka.UnimplementedError
    if !errors.As(err, &u) || u.Module != "queues" || u.GatedOn != "gateway-sharegroup-rpc" {
        t.Fatalf("want gated Unimplemented, got %v", err)
    }
    // same shape for Database().Connect ("chapter-f-control-plane") and Blob().Put ("chapter-b-blob-api")
}

func TestErrorTaxonomyMapsConnectCodes(t *testing.T) { /* NotFound / InvalidArgument / Transport mapping */ }
```

- [ ] **Step 2: Implement** — `Client` with module accessors (`Messaging()` wraps MSG-5's publish/subscribe; `Queues()`/`Database()`/`Blob()` are stubs; `Auth()` exposes credential config only); `errors.go`: the closed taxonomy incl. `UnimplementedError{Module, GatedOn}`; Connect-code → taxonomy mapping.
- [ ] **Step 3: Verify + commit**

Run: `cd sdks/go && go test ./...` → PASS.

```bash
git add sdks/go
git commit -m "feat(sdk-go): app-SDK module layout, error taxonomy, gated stubs"
```

---

## Task 5: The Go adapter + suite green

**Files:**
- Create: `sdks/go/cmd/conformance-adapter/main.go`

- [ ] **Step 1:** Implement the adapter: JSON-lines stdio loop translating `Command`s onto the Go SDK (`Hello` reports `contract_major: 1, language: "go"`; `Subscribe`/`NextMessage` bridge the stream to pull semantics with a buffered channel; every SDK error maps to the wire taxonomy).
- [ ] **Step 2:** Run the real suite: `cargo run -p crabka-sdk-conformance --bin conformance -- --adapter sdks/go/bin/conformance-adapter --vectors crates/sdk-conformance/vectors/v1` → **all vectors PASS**. Any mismatch: fix the SDK (or a genuinely ambiguous vector — then fix the vector *and* re-run the mock).
- [ ] **Step 3: Commit**

```bash
git add sdks/go
git commit -m "feat(sdk-go): conformance adapter; suite green (contract v1)"
```

---

## Task 6: CI + final gate

- [ ] **Step 1:** A workflow job (reuses `setup-go`; no Docker): build the harness + the Go adapter, run the suite, fail on any vector. Wire the buf drift check from MSG-5's plan if not yet present.
- [ ] **Step 2:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-sdk-conformance --all-targets -- -D warnings`; `cargo nextest run -p crabka-sdk-conformance`; `cd sdks/go && gofmt -l . && go vet ./... && go test ./...`; `./tools/check-publish-allowlist.sh` — all green. Commit.

```bash
git add .github/workflows
git commit -m "ci(sdk): conformance suite job (harness + Go adapter)"
```

---

## Self-Review

**1. Spec coverage:** the adapter protocol + mock self-test (Task 1); the in-process harness (Task 2); vectors v1 covering round-trip/CE/filter/stubs/config/error-mapping (Task 3); the Go module layout + taxonomy + gated stubs (Task 4); the adapter + green suite (Task 5); CI without new toolchains (Task 6). Identity-versioned-out honored (no `AuthSignIn` vector in v1 — the command exists in the protocol for future majors, unexercised). Deferred set untouched — Scope boundary. ✅

**2. Placeholder scan:** protocol shapes, vector ids + pinned slugs, and Go error types are concrete; the two-sided validation (mock passes vectors before Go attempts them) removes vector ambiguity by construction. No `TBD`.

**3. Type consistency:** `Command`/`Response`/`ErrorKind` (Task 1) are what the harness speaks (Task 2), the vectors encode (Task 3), and the Go adapter implements (Task 5); `UnimplementedError{Module, GatedOn}` (Task 4) serializes to the wire `{kind, module, gated_on}` (Task 1); slugs identical in Tasks 3 and 4.

**4. Invariant check:** behavior-only (the suite drives a live gateway through the adapter); stubs vector-pinned (Tasks 3–4); contract-major enforced at `Hello` (Tasks 1–2); in-process substrate, no Docker (Task 2, Task 6); allowlist green (Tasks 1, 6). Each task green before commit.

**5. Prerequisites flagged:** MSG-5's Go core + h2c (header; Tasks 4–5 depend on it, Tasks 1–3 do not). Batching: 1 → 2 → 3 with 4 parallel (post-MSG-5) → 5 → 6.
