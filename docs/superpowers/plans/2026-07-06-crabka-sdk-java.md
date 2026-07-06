# The Java SDK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `dev.crabka:crabka-sdk` — a Kotlin-core, Java-facade SDK implementing contract v1 (connect-kotlin + okhttp h2c transport, six modules, pinned stubs, `CompletableFuture`/`Iterator` surface) with a Gradle `installDist` conformance adapter, suite green in an `sdk-java` CI job reusing the existing JVM toolchain.

**Architecture:** connect-kotlin generated clients under `sdks/java/gen`; a Kotlin `internal/` core; a Java-visible facade proven by **Java-written** tests; the umbrella harness/vectors unchanged.

**Tech Stack:** Kotlin JVM + okhttp (`H2_PRIOR_KNOWLEDGE`), connect-kotlin, Gradle (Kotlin DSL, `application` plugin), JUnit 5, buf.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-sdk-java-design.md`](../specs/2026-07-06-crabka-sdk-java-design.md).

**PREREQUISITES (unlanded):** the umbrella executed (harness + Go-hardened vectors) and MSG-5's gateway h2c listener.

---

## Invariants

1. **The facade is Java-proven** — public-surface tests are written in Java; a Kotlin-only leak fails its own tests first.
2. **Semantics from the vectors** — the suite is the gate; vectors are never edited to fit the JVM (ambiguity → mock + Go first).
3. **Stubs carry the pinned slugs** byte-identically (`gateway-sharegroup-rpc`, `chapter-f-control-plane`, `chapter-b-blob-api`).
4. **h2c proven first**; generated code drift-checked, never hand-edited.
5. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the Gradle project + codegen; the h2c smoke; core + facade (client/errors/messaging/stubs); the adapter; the CI job.
- **Deferred:** Android; Maven publishing; Reactor/Flow bindings; manual ack.

---

## Task 1: Gradle project + codegen + the h2c transport smoke

- [ ] **Step 1:** Scaffold `sdks/java` (Kotlin JVM plugin, JDK 21 toolchain, deps: connect-kotlin runtime, okhttp, protobuf-kotlin; JUnit 5). Add the connect-kotlin + protobuf plugin blocks to `buf.gen.yaml` (`out: sdks/java/gen`); `buf generate`; commit `gen/`.
- [ ] **Step 2: Smoke-first** — an integration-tagged test: an okhttp client with `protocols = listOf(Protocol.H2_PRIOR_KNOWLEDGE)` completes a unary `Send` against a locally running gateway (exercised for real via the suite in Task 4; the tagged test documents and pins the transport config). Failure re-scopes transport before module work.
- [ ] **Step 3:** `gradle build` green; commit.

```bash
git add buf.gen.yaml sdks/java
git commit -m "feat(sdk-java): gradle scaffold, connect-kotlin codegen, h2c transport config"
```

---

## Task 2: Taxonomy + client facade (Java-proven)

- [ ] **Step 1: Failing JUnit tests — written in Java:**

```java
@Test void stubModulesThrowGatedUnimplemented() {
    CrabkaClient c = CrabkaClient.builder().endpoint("http://localhost:1").build();
    CompletionException e = assertThrows(CompletionException.class,
        () -> c.queues().acquire("t", AcquireOptions.defaults()).join());
    UnimplementedException u = assertInstanceOf(UnimplementedException.class, e.getCause());
    assertEquals("queues", u.module());
    assertEquals("gateway-sharegroup-rpc", u.gatedOn());
}
@Test void connectCodesMapToTaxonomy() { /* NOT_FOUND -> NotFoundException; UNAVAILABLE -> TransportException */ }
```

- [ ] **Step 2:** Implement `CrabkaClient.builder()` (endpoint, bearer, mTLS paths) → module accessors; the `CrabkaException` hierarchy incl. `UnimplementedException(module, gatedOn)`; the connect-kotlin→taxonomy mapping; the three stub modules from a shared factory; `auth()` = credential config only (bearer via an okhttp interceptor).
- [ ] **Step 3:** Green; commit.

```bash
git add sdks/java
git commit -m "feat(sdk-java): Java-proven client facade, taxonomy, gated stubs"
```

---

## Task 3: Messaging — publish, publishEvent, subscribe

- [ ] **Step 1: Failing tests (Java):** CE mapping (`ce_*` underscore headers, `content-type` from `datacontenttype`, never `ce_datacontenttype`); `publish` returns a `RecordResult`; `subscribe` returns a `MessageStream implements Iterator<Inbound>, AutoCloseable` whose `close()` tears the stream down.
- [ ] **Step 2:** Implement in the Kotlin core (unary `Send`; the bidi stream sending one `SubscribeStart{autoCommit=true}` and pumping `Inbound` into a bounded `BlockingQueue` the `Iterator` drains); the facade exposes `CompletableFuture<RecordResult>` publish + the blocking `MessageStream` (serverless-consumer-shaped).
- [ ] **Step 3:** Green; commit.

```bash
git add sdks/java
git commit -m "feat(sdk-java): messaging module (publish, CloudEvents, subscribe)"
```

---

## Task 4: The conformance adapter + suite green

- [ ] **Step 1:** `AdapterMain.kt` under the `application` plugin (`installDist` → `build/install/adapter/bin/adapter`): JSON-lines stdio loop → the SDK; `Hello{contract_major: 1, language: "java"}`; `Subscribe`/`NextMessage` bridged via the `MessageStream`; errors through the taxonomy→wire mapping.
- [ ] **Step 2:** Run the real suite: `cargo run -p crabka-sdk-conformance --bin conformance -- --adapter sdks/java/build/install/adapter/bin/adapter …` → **all vectors PASS** (fix the SDK, never the vectors).
- [ ] **Step 3:** Commit.

```bash
git add sdks/java
git commit -m "feat(sdk-java): conformance adapter; suite green (contract v1)"
```

---

## Task 5: CI + final gate

- [ ] **Step 1:** An `sdk-java` workflow job mirroring the oracle's JVM setup (`setup-java` + Gradle cache): `gradle build test installDist` → harness + suite. The buf drift check extends to `sdks/java/gen`.
- [ ] **Step 2:** ktlint/spotless (match whatever the repo's JVM tooling already uses in the oracle, else add spotless) clean; commit.

```bash
git add .github/workflows
git commit -m "ci(sdk-java): gradle build + conformance job"
```

---

## Self-Review

**1. Spec coverage:** scaffold/codegen/h2c-first (Task 1); the Java-proven facade + taxonomy + pinned stubs (Task 2); messaging with the blocking `MessageStream` surface (Task 3); adapter + suite gate (Task 4); CI on the existing JVM toolchain (Task 5). Android/publishing/reactive deferred — Scope boundary. ✅
**2. Placeholder scan:** Java test bodies for the decisive facade behaviors; transport risk is the named first task. No `TBD`.
**3. Type consistency:** `UnimplementedException(module, gatedOn)` ↔ the wire `{kind, module, gated_on}` (Task 4); slugs identical to the umbrella; the CE mapping mirrors the vector-pinned semantics.
**4. Invariant check:** Java-written facade tests (Task 2); vectors unedited (Task 4 rule); h2c-first (Task 1); gen drift-checked (Tasks 1, 5).
**5. Prerequisites flagged:** umbrella + h2c listener (header); CI needs no new toolchain (the one JVM advantage).
