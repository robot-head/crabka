# The C++ SDK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sdks/cpp` (`crabka::` C++20, CMake, Linux-first): a minimal Connect transport on nghttp2 (unary + full-duplex bidi, h2c), the six-module contract surface with pinned stubs and an `expected`-style error surface, a blocking `MessageStream`, and the conformance adapter — suite green in a new sanitizer-enabled `sdk-cpp` CI job.

**Architecture:** the envelope codec is pure and tested against **committed byte vectors captured from the Rust cycle's oracle** (our own connectrpc-axum server); the nghttp2 session runs on a dedicated I/O thread; everything above transport follows the cross-language formula; the umbrella harness/vectors unchanged.

**Tech Stack:** C++20, CMake, nghttp2, protobuf (`protoc-gen-cpp` via buf), Catch2 + nlohmann/json (FetchContent), ASan/TSan in CI.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-sdk-cpp-design.md`](../specs/2026-07-06-crabka-sdk-cpp-design.md).

**PREREQUISITES (unlanded):** the umbrella executed (harness + vectors, Go-hardened); MSG-5's h2c listener; **the Rust cycle's framing byte-captures committed** (its oracle test writes them — a one-task add there if not yet done).

---

## Invariants

1. **Codec proven against captured bytes** — the envelope/trailer codec passes the Rust-captured vectors before any live networking exists.
2. **Sanitizers always on in CI** — ctest under ASan+UBSan (TSan in a second config for the transport tests); a sanitizer finding is a failing build.
3. **No raw pointers cross the public API**; semantics from the suite — vectors never edited to fit C++.
4. **Stubs carry the pinned slugs** byte-identically.
5. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** CMake + codegen; the envelope codec; the nghttp2 transport (unary + bidi + shutdown); client/taxonomy/messaging/stubs; the adapter; the `sdk-cpp` CI job.
- **Deferred:** Windows/macOS; TLS; a general Connect-C++ lib; packaging; manual ack.

---

## Task 1: CMake scaffold + codegen + the committed framing vectors

- [ ] **Step 1:** Add the `protoc-gen-cpp` block to `buf.gen.yaml` (`out: sdks/cpp/gen`); `buf generate`; scaffold `sdks/cpp` (CMake: C++20, `find_package(Protobuf)`, nghttp2 via pkg-config; Catch2 + nlohmann/json FetchContent; ASan/UBSan option wired).
- [ ] **Step 2:** Commit the framing byte-captures from the Rust oracle into `sdks/cpp/test/vectors/` (message frame, multi-frame stream, EndStream-with-error, EndStream-clean) with a `manifest.txt` documenting their provenance (which oracle test, which gateway rev).
- [ ] **Step 3:** `cmake -B build && cmake --build build` green (empty lib + gen compiles); commit.

```bash
git add buf.gen.yaml sdks/cpp
git commit -m "feat(sdk-cpp): cmake scaffold, protobuf codegen, committed framing vectors"
```

---

## Task 2: The envelope codec (pure, vector-proven)

- [ ] **Step 1: Write the failing Catch2 tests**

```cpp
TEST_CASE("envelope round trips") {
    auto framed = crabka::envelope::encode(0x00, as_bytes("payload"));
    REQUIRE(framed[0] == 0x00);
    REQUIRE(read_u32_be(&framed[1]) == 7);
    auto frame = crabka::envelope::decode_one(framed);
    REQUIRE(std::holds_alternative<crabka::envelope::Message>(frame));
}
TEST_CASE("end-stream trailer yields the connect error") {
    auto frame = decode_vector_file("endstream_not_found.bin");   // Rust-captured
    auto& es = std::get<crabka::envelope::EndStream>(frame);
    REQUIRE(es.error && es.error->code == "not_found");
}
TEST_CASE("all captured vectors decode byte-exactly") { /* iterate test/vectors */ }
```

- [ ] **Step 2:** Implement `envelope.{hpp,cc}` (pure: no I/O, no allocation surprises; partial-input → `need_more(n)`); green under ASan; commit.

```bash
git add sdks/cpp
git commit -m "feat(sdk-cpp): Connect envelope codec, proven against captured vectors"
```

---

## Task 3: The nghttp2 transport (unary + bidi + shutdown)

- [ ] **Step 1: Failing tests** — transport integration tests against a **locally spawned gateway** (the conformance harness gains a `--serve-only` flag if not present — a one-flag addition to the umbrella crate, letting any language's transport tests hit a real endpoint): unary `Send` round-trips; a bidi `Subscribe` sends one Start frame, receives N messages, `close()` shuts the stream and the session cleanly (TSan-clean); a dead endpoint yields `Error{kind=Transport}` within the timeout.
- [ ] **Step 2:** Implement `transport.{hpp,cc}`: one nghttp2 session on a dedicated I/O thread (poll loop over a socketpair for cross-thread wakeups); `unary(path, bytes) -> expected<bytes, Error>`; `open_bidi(path)` returning a handle with `send(bytes)`, a bounded inbound queue, and `close()`; WINDOW_UPDATE management on the read path; bearer header injection.
- [ ] **Step 3:** Green under ASan + TSan; commit.

```bash
git add sdks/cpp crates/sdk-conformance
git commit -m "feat(sdk-cpp): nghttp2 h2c transport (unary + full-duplex bidi)"
```

---

## Task 4: Taxonomy, client, messaging, stubs

- [ ] **Step 1: Failing tests:** stub calls return `Error{kind: Unimplemented, module: "queues", gated_on: "gateway-sharegroup-rpc"}` (+ the database/blob slugs); Connect-code mapping; the CE mapping (identical assertions to every language: `ce_*` underscore, `content-type` from `datacontenttype`, never `ce_datacontenttype`).
- [ ] **Step 2:** Implement `crabka::Client` (builder-style: endpoint, bearer; owns the transport thread), `errors`, `messaging` (`publish`/`publish_event` → unary; `subscribe(topics, group, filter)` → `MessageStream` with `next(timeout) -> expected<Inbound, Error>` / `close()`, fed by the bidi handle after one auto-commit `SubscribeStart`), `stubs.cc` from a shared helper.
- [ ] **Step 3:** Green; commit.

```bash
git add sdks/cpp
git commit -m "feat(sdk-cpp): client, taxonomy, messaging, gated stubs"
```

---

## Task 5: The conformance adapter + suite green

- [ ] **Step 1:** `adapter/main.cc`: getline JSON-lines loop (nlohmann) → the SDK; `Hello{contract_major: 1, language: "cpp"}`; `Subscribe`/`NextMessage` via the `MessageStream`; errors through the taxonomy→wire mapping.
- [ ] **Step 2:** Run the real suite with `--adapter sdks/cpp/build/adapter` → **all vectors PASS** (fix the SDK, never the vectors).
- [ ] **Step 3:** Commit.

```bash
git add sdks/cpp
git commit -m "feat(sdk-cpp): conformance adapter; suite green (contract v1)"
```

---

## Task 6: CI + final gate

- [ ] **Step 1:** The `sdk-cpp` workflow job: apt deps (`libnghttp2-dev`, `protobuf-compiler`, `libprotobuf-dev`), two build configs (ASan+UBSan for ctest; TSan for the transport tests), then the conformance suite. The buf drift check extends to `sdks/cpp/gen`.
- [ ] **Step 2:** clang-format (a checked `.clang-format`) clean; ctest green in both configs; commit.

```bash
git add .github/workflows sdks/cpp/.clang-format
git commit -m "ci(sdk-cpp): sanitizer-enabled build + conformance job"
```

---

## Self-Review

**1. Spec coverage:** scaffold/codegen/committed vectors (Task 1); the pure codec proven against captures (Task 2); the nghttp2 transport with duplex + shutdown + TSan (Task 3); the formula layer (Task 4); adapter + suite gate (Task 5); the sanitizer CI job (Task 6). Windows/TLS/packaging deferred — Scope boundary. ✅
**2. Placeholder scan:** codec tests concrete against named vector files; the threading model and `--serve-only` harness addition are explicit; slugs pinned. No `TBD`.
**3. Type consistency:** `Error{kind, module, gated_on}` ↔ the wire `{kind, module, gated_on}` (Task 5); the envelope types (Task 2) are what the transport pumps (Task 3) and messaging consumes (Task 4); CE assertions identical cross-language.
**4. Invariant check:** vectors-before-networking (Task 2 precedes 3); sanitizers non-negotiable (Tasks 3, 6); no raw pointers in public headers (Task 4 review point); suite unedited (Task 5).
**5. Prerequisites flagged:** umbrella, h2c listener, and the Rust-captured vectors (header — with the one-task fallback if the Rust cycle hasn't committed them). Last in phase order; nothing gates on this cycle.
