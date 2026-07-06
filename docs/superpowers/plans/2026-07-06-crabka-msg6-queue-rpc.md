# MSG-6: The gateway queue surface — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unary `QueueAcquire`/`QueueAcknowledge`/`QueueRenew` on the gateway, adapting the native `ShareConsumer` through a principal-bound session table — plus the `ShareConsumerRecord.headers` addition — proven by the five queue behaviors, per-entry ack errors, a headers round-trip, and a JVM cross-consumer check. Unblocks the `queues` module (contract v1.1) across all five SDKs.

**Architecture:** `crates/grpc-gateway/src/queue.rs` owns a `QueueSessionTable` (`session_id → ShareConsumer`, idle-evicted; broker lock expiry is the safety net); the three RPCs are thin adapters (`poll` / stage-acks-then-`commit` / `renew`). No broker changes.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `crabka-client-consumer` (share), connectrpc-axum (the gateway idiom), the in-process `Broker::start` harness + the `jvm_share_groups` differential harness, `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-msg6-queue-rpc-design.md`](../specs/2026-07-06-crabka-msg6-queue-rpc-design.md).

**PREREQUISITES:** none unlanded — the broker KIP-932 stack and the native `ShareConsumer` are built. (MSG-1 is precedent, not prerequisite: the headers addition here is on the *share* path.) The v1.1 vector work (Task 6) touches the umbrella crate once it exists; Tasks 1–5 are independent of it.

---

## Invariants

1. **No broker changes** — the gateway is a `ShareConsumer` client over the standard KIP-932 wire.
2. **Lost sessions lose nothing** — every session-loss path (idle eviction, gateway restart, abandoned client) ends in broker lock expiry + redelivery with `delivery_count` incremented; tested, not assumed.
3. **Per-entry ack verdicts** — a broker rejection (e.g. `INVALID_RECORD_STATE` after lock expiry) surfaces on that entry, never as a whole-call failure.
4. **Byte-exact + headers** — values and the newly-carried headers round-trip verbatim.
5. **Sessions are principal-bound** — cross-principal use → `PermissionDenied`; ids are 128-bit random.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the `ShareConsumerRecord.headers` addition; the proto RPCs; the session table; the three handlers + config caps; the integration + JVM cross-check; the v1.1 vector/harness-versioning task.
- **Deferred:** push delivery; per-acquire lock duration; DLQ; multi-gateway affinity; the per-SDK stub-flips (one small task each, in the SDK cycles' repos of record).

---

## File Structure

- **`crates/client-consumer/src/share/{types.rs, poll.rs}`** — the headers addition (Task 1).
- **`crates/grpc-gateway/proto/…/gateway.proto`** — the three RPCs + messages (Task 2).
- **`crates/grpc-gateway/src/queue.rs`** (new) — session table + handlers (Tasks 3–4).
- **`crates/grpc-gateway/tests/queue.rs`** (new) — integration + JVM cross-check (Task 5).
- **`crates/sdk-conformance`** — v1.1 vectors + version-selected vector loading (Task 6, umbrella-gated).

**Batching:** Task 1 (`client-consumer`) ∥ Task 2 (proto) — disjoint. Task 3 (session table) after 2; Task 4 (handlers) after 1+3; Task 5 after 4; Task 6 independent-late.

---

## Task 1 (∥ Task 2): `ShareConsumerRecord.headers`

**Files:**
- Modify: `crates/client-consumer/src/share/types.rs`, `src/share/poll.rs`

- [ ] **Step 1: Write the failing test** — produce a record with headers `[("ce-type"→"order"), ("nullv"→None)]`; a `ShareConsumer` in explicit mode `poll`s it; assert `rec.headers == vec![("ce-type", Some(b"order")), ("nullv", None)]` (lossless, order-preserving — the MSG-1 internal shape). Extend an existing share integration test (`crates/broker/tests/share_consume.rs` or the crate's own harness) rather than building a new one.
- [ ] **Step 2: Implement** — `pub headers: Vec<(String, Option<Bytes>)>` on `ShareConsumerRecord`; materialize in the share poll decode exactly as classic `poll.rs` does for `ConsumerRecord.headers` (`:560` is the template). Fix any struct-literal sites.
- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-client-consumer share && cargo test -p crabka-broker --test share_consume` → PASS.

```bash
git add crates/client-consumer crates/broker/tests
git commit -m "feat(client-consumer): carry record headers on the share-group poll path"
```

---

## Task 2 (∥ Task 1): The proto surface

- [ ] **Step 1:** Add the three RPCs + messages from the spec verbatim (`QueueAcquireRequest/Response`, `QueuedMessage` with `map<string,bytes> headers` + `delivery_count`, `QueueAckType`, `QueueAckEntry`, per-entry `QueueAckResult`, `QueueRenewRequest/Response`) to `gateway.proto`; regenerate.
- [ ] **Step 2:** `cargo build -p crabka-grpc-gateway` green (handlers arrive in Task 4 — connectrpc-axum tolerates unregistered RPCs until the builder wires them; if the generated builder *requires* all handlers, stub them returning `Unimplemented` in this task and note it). Commit.

```bash
git add crates/grpc-gateway/proto
git commit -m "feat(gateway): queue RPC surface (Acquire/Acknowledge/Renew) in the proto"
```

---

## Task 3: The session table

**Files:**
- Create: `crates/grpc-gateway/src/queue.rs`

- [ ] **Step 1: Write the failing unit tests**

```rust
    #[tokio::test]
    async fn issues_and_resolves_principal_bound_sessions() {
        let t = QueueSessionTable::new(test_cfg());
        let id = t.insert(principal("a"), fake_session()).await;
        assert!(id.len() >= 32);                                  // 128-bit hex/uuid
        let_assert!(Ok(_) = t.get(&principal("a"), &id).await);
        let_assert!(Err(QueueError::PermissionDenied) = t.get(&principal("b"), &id).await);
    }
    #[tokio::test]
    async fn idle_sessions_evict_and_lookup_says_expired() {
        // insert; advance tokio time past queue_session_idle_secs; sweeper evicts (consumer closed);
        // get -> QueueError::SessionExpired (maps to FailedPrecondition "re-acquire").
    }
    #[tokio::test]
    async fn max_sessions_cap_is_resource_exhausted() { /* cap = 2; third insert -> ResourceExhausted */ }
```

- [ ] **Step 2: Implement** — `QueueSessionTable` (`DashMap<SessionId, Entry{principal, consumer: Mutex<ShareConsumer>, last_used: AtomicI64}>`), 128-bit random ids, an idle sweeper task (the gateway's existing background-task idiom), a max-sessions cap; config fields (`queue_max_messages`, `queue_wait_ms_cap`, `queue_session_idle_secs`, `queue_max_sessions`) on the gateway config with defaults (256 / 30 000 / 60 / 10 000).
- [ ] **Step 3: Verify + commit**

```bash
git add crates/grpc-gateway/src/queue.rs crates/grpc-gateway/src
git commit -m "feat(gateway): principal-bound queue session table with idle eviction"
```

---

## Task 4: The three handlers

- [ ] **Step 1: Write the failing integration test skeleton** (`tests/queue.rs`, in-process broker + gateway): produce 3 records → `QueueAcquire{group, topics, max_messages: 10, wait_ms: 2000, session_id: ""}` → a session id + 3 `QueuedMessage`s with `delivery_count == 1` and byte-exact values.
- [ ] **Step 2: Implement** — `queue_acquire`: resolve-or-create the session (Read-ACL gate on group+topics, the `Subscribe` pattern; `ShareConsumer::start` in **Explicit** mode on first use), clamp `max_messages`/`wait_ms` to config, `poll`, map records (headers via the MSG-1 map policy: null→empty, dup→last-wins). `queue_acknowledge`: resolve session; per entry find the acquired record, `acknowledge(record, type)`; `commit()`; map per-entry broker errors into `QueueAckResult.error` (an entry the session never acquired → `InvalidArgument` per-entry). `queue_renew`: same shape onto `renew`. Unknown session → `FailedPrecondition("queue session expired; re-acquire")`.
- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-grpc-gateway --test queue` → PASS (the acquire case).

```bash
git add crates/grpc-gateway/src crates/grpc-gateway/tests
git commit -m "feat(gateway): QueueAcquire/Acknowledge/Renew handlers over ShareConsumer"
```

---

## Task 5: The five behaviors + headers + the JVM cross-check

**Files:**
- Modify: `crates/grpc-gateway/tests/queue.rs`

- [ ] **Step 1: Write the behavior tests**
  - `accept_is_not_redelivered`; `release_redelivers_with_delivery_count_2`; `reject_archives` (never seen again); `lock_expiry_redelivers` (short `record_lock_duration` in the test group config; no ack; re-acquire shows `delivery_count == 2`); `session_expiry_reacquire` (idle-evict, then a fresh Acquire gets the unacked records back — invariant 2 proven).
  - `ack_after_lock_expiry_is_per_entry_error` (forced expiry → `QueueAckResult.error` on that entry, sibling entry succeeds).
  - `headers_round_trip` (a `ce_*`-headered record acquired with headers intact — Task 1 proven end-to-end).
  - `acl_denial` (deny-all authorizer → `PermissionDenied` on Acquire).
- [ ] **Step 2: The JVM cross-check** — extend the existing `jvm_share_groups` differential harness: gateway-acquire + `Release` a record, then the JVM `KafkaShareConsumer` on the same group re-acquires it with `delivery_count == 2`. **Memory note:** the JVM differential suite rewrites tracked protocol corpus fixtures — restore them after the run.
- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-grpc-gateway --test queue` (+ the JVM harness job) → PASS.

```bash
git add crates/grpc-gateway/tests
git commit -m "test(gateway): queue behaviors, per-entry ack errors, headers, JVM cross-check"
```

---

## Task 6 (umbrella-gated): Contract v1.1 vectors + version-selected loading

- [ ] **Step 1:** In `crates/sdk-conformance`: teach the harness to select vectors by the adapter's declared contract version (`Hello` gains `contract_minor`; vectors gain a `since`/`until` version range — `stub_queues` gets `until: 1.0`, the five live queue vectors get `since: 1.1`).
- [ ] **Step 2:** Author the five v1.1 vectors mirroring Task 5's behaviors through the adapter protocol's `QueueAcquire`/`QueueAck` commands; validate with the mock adapter at both declared versions (a 1.0 mock still passes `stub_queues`; a 1.1 mock passes the live set).
- [ ] **Step 3:** Commit. (The per-SDK stub-flips are one small task each inside the language cycles — out of this plan's scope, now unblocked.)

```bash
git add crates/sdk-conformance
git commit -m "feat(sdk-conformance): contract v1.1 queue vectors with version-selected loading"
```

---

## Task 7: Final gate

- [ ] `cargo +nightly fmt --check`; `cargo clippy -p crabka-grpc-gateway -p crabka-client-consumer --all-targets -- -D warnings`; `cargo nextest run -p crabka-grpc-gateway -p crabka-client-consumer` (+ the JVM differential job) — all green; corpus fixtures restored; `./tools/check-publish-allowlist.sh` → 0. Commit any formatting.

---

## Self-Review

**1. Spec coverage:** the headers addition (Task 1); the proto surface (Task 2); the principal-bound session table + config caps (Task 3); the three adapters with per-entry verdicts (Task 4); the five behaviors + lock-expiry safety + headers + ACL + the JVM cross-check (Task 5); the stub→live contract mechanics with version-selected vectors (Task 6). Deferred set (push delivery, per-acquire lock, DLQ, affinity, per-SDK flips) untouched — Scope boundary. ✅
**2. Placeholder scan:** proto verbatim in the spec; session-table tests concrete; the connectrpc-axum builder caveat in Task 2 has its fallback named; the corpus-restore memory note is in Task 5. No `TBD`.
**3. Type consistency:** `ShareAckType::{Accept,Release,Reject}` ↔ `QueueAckType` (Tasks 1–4); `ShareConsumerRecord.headers: Vec<(String, Option<Bytes>)>` → the proto map under the MSG-1 policy (Tasks 1, 4); `QueueError::{SessionExpired, PermissionDenied}` mapped to Connect codes consistently (Tasks 3–4).
**4. Invariant check:** no broker changes (all tasks are client/gateway); lost-sessions-lose-nothing proven (Task 5's expiry tests); per-entry verdicts (Tasks 4–5); byte-exact + headers (Tasks 1, 5); principal binding (Task 3); every task green.
**5. Prerequisites flagged:** none unlanded for Tasks 1–5; Task 6 gated on the umbrella crate existing — stated. Batching: (1 ∥ 2) → 3 → 4 → 5, with 6 independent-late → 7.
