# The TypeScript SDK — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `@crabka/sdk` for Node ≥ 20 implementing contract v1 — Connect-ES transport (h2c), the six-module surface with vector-pinned stubs, a JSON-stdio conformance adapter — suite green in a new `sdk-ts` CI job (which introduces `setup-node`).

**Architecture:** `protoc-gen-es` stubs via the repo buf config; a thin idiomatic layer (`createClient` → module objects; `AsyncIterable` subscribe over the bidi stream); the umbrella's harness and vectors unchanged.

**Tech Stack:** TypeScript (ESM, Node ≥ 20), `@connectrpc/connect` + `@connectrpc/connect-node`, `@bufbuild/protobuf`, vitest, buf.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-sdk-ts-design.md`](../specs/2026-07-06-crabka-sdk-ts-design.md).

**PREREQUISITES (unlanded):** the umbrella executed (harness + vectors v1 + the Go reference having hardened them) and MSG-5's gateway h2c listener.

---

## Invariants

1. **Semantics from the vectors, shape from the language** — the suite is the gate; no vector is edited to fit TS (a genuinely ambiguous vector goes back through the mock + Go first).
2. **Stubs carry the pinned slugs** (`gateway-sharegroup-rpc`, `chapter-f-control-plane`, `chapter-b-blob-api`) byte-identically.
3. **h2c proven first** — the transport smoke test precedes all module work.
4. **Generated code is drift-checked**, never hand-edited.
5. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** buf plugin block + gen; the transport smoke; client/config/errors; messaging (publish/publishEvent/subscribe); stubs; the adapter; the CI job (`setup-node`).
- **Deferred:** browser build; manual ack; npm publish workflow; framework helpers.

---

## Task 1: Package scaffold + codegen + the h2c transport smoke

**Files:**
- Create: `sdks/ts/{package.json, tsconfig.json, vitest.config.ts}`; Modify: `buf.gen.yaml`
- Create: `sdks/ts/test/transport.smoke.test.ts`

- [ ] **Step 1:** Add the `buf.build/bufbuild/es` plugin block (`out: sdks/ts/gen, opt: [target=ts]`); `buf generate`; commit `gen/`. Scaffold the package (ESM, `"engines": {"node": ">=20"}`, deps `@connectrpc/connect`, `@connectrpc/connect-node`, `@bufbuild/protobuf`; dev vitest + tsx).
- [ ] **Step 2: The riskiest assumption first** — a smoke test (integration-tagged; runs against a locally started gateway binary or is exercised via the conformance harness in Task 4): `createConnectTransport({ httpVersion: "2", baseUrl })` against the plaintext h2c gateway completes a unary `Send`. Failure here re-scopes the transport before any module work.
- [ ] **Step 3:** `npm run build` + `vitest run` green; commit.

```bash
git add buf.gen.yaml sdks/ts
git commit -m "feat(sdk-ts): package scaffold, connect-es codegen, h2c transport smoke"
```

---

## Task 2: `errors.ts` + `client.ts` (config + taxonomy)

- [ ] **Step 1: Failing vitest units**

```ts
test("connect codes map to the taxonomy", () => {
  expect(fromConnectError(new ConnectError("x", Code.NotFound))).toBeInstanceOf(NotFoundError);
  expect(fromConnectError(new ConnectError("x", Code.Unavailable))).toBeInstanceOf(TransportError);
});
test("stub errors carry pinned slugs", () => {
  const c = createClient({ endpoint: "http://localhost:1" });
  return expect(c.queues.acquire("t", {})).rejects.toMatchObject({
    name: "UnimplementedError", module: "queues", gatedOn: "gateway-sharegroup-rpc",
  });
});
```

- [ ] **Step 2:** Implement the `CrabkaError` hierarchy (incl. `UnimplementedError{module, gatedOn}`), `fromConnectError`, and `createClient({endpoint, bearerToken?})` returning `{messaging, queues, database, auth, blob}` — `queues`/`database`/`blob` from a stub factory with the three pinned slugs; `auth` = credential config only (the bearer flows into every request header via a Connect interceptor).
- [ ] **Step 3:** Green; commit.

```bash
git add sdks/ts/src sdks/ts/test
git commit -m "feat(sdk-ts): client, error taxonomy, gated stubs"
```

---

## Task 3: `messaging.ts` — publish, publishEvent, subscribe

- [ ] **Step 1: Failing units** for the pure CE mapping (mirrors the Go tests): `publishEvent` builds a `Record` with `ce_id/ce_source/ce_type/ce_specversion` headers, `content-type` from `datacontenttype`, **never** `ce_datacontenttype`, data as raw bytes.
- [ ] **Step 2:** Implement `publish(topic, value, opts)` (unary `Send`, `RecordResult` mapping), `publishEvent`, and `subscribe(topics, {group, filter})` → open the bidi stream, send one `SubscribeStart{autoCommit: true, predicates}`, yield `Inbound` messages as an `AsyncIterable`; iterator termination aborts the stream. `filter` builder = the EQUALS-only `FieldPredicate` (documented).
- [ ] **Step 3:** Green; commit.

```bash
git add sdks/ts/src sdks/ts/test
git commit -m "feat(sdk-ts): messaging module (publish, CloudEvents, subscribe)"
```

---

## Task 4: The conformance adapter + suite green

- [ ] **Step 1:** `src/conformance-adapter.ts`: readline-over-stdin JSON loop → SDK calls → protocol responses (`Hello{contract_major: 1, language: "ts"}`; `Subscribe`/`NextMessage` bridged through a buffered queue; every error through the taxonomy→wire mapping). Build to `sdks/ts/bin/conformance-adapter` (a `#!/usr/bin/env node` entry).
- [ ] **Step 2:** Run the real suite: `cargo run -p crabka-sdk-conformance --bin conformance -- --adapter sdks/ts/bin/conformance-adapter --vectors crates/sdk-conformance/vectors/v1` → **all vectors PASS**. Fix the SDK, never the vectors (ambiguity → mock + Go first).
- [ ] **Step 3:** Commit.

```bash
git add sdks/ts
git commit -m "feat(sdk-ts): conformance adapter; suite green (contract v1)"
```

---

## Task 5: CI (`setup-node`) + final gate

- [ ] **Step 1:** A `sdk-ts` workflow job: `actions/setup-node@v4` (Node 22) → `npm ci && npm run build && vitest run` → build the harness → run the suite with the TS adapter. This is the repo's first Node toolchain — the umbrella flagged it.
- [ ] **Step 2:** `npm run lint`/`tsc --noEmit` clean; the buf drift check covers `sdks/ts/gen`; commit.

```bash
git add .github/workflows
git commit -m "ci(sdk-ts): setup-node + conformance job"
```

---

## Self-Review

**1. Spec coverage:** codegen + scaffold + the h2c smoke-first discipline (Task 1); taxonomy + config + pinned stubs (Task 2); messaging incl. CE mapping + `AsyncIterable` subscribe (Task 3); adapter + suite gate (Task 4); `setup-node` CI (Task 5). Browser/npm-publish/manual-ack deferred — Scope boundary. ✅
**2. Placeholder scan:** test bodies for the decisive behaviors; the transport risk is a named first task, not a footnote. No `TBD`.
**3. Type consistency:** `UnimplementedError{module, gatedOn}` (Task 2) serializes to the harness wire shape (Task 4); the CE mapping mirrors the vector-pinned Go semantics (Task 3); slugs identical to the umbrella's.
**4. Invariant check:** vectors unedited (Task 4 rule); slugs pinned (Task 2 test); h2c-first (Task 1); gen drift-checked (Tasks 1, 5).
**5. Prerequisites flagged:** umbrella + Go-hardened vectors + the h2c listener (header).
