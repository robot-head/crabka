# Slice 17b: RequestContext + tuple-quota plumbing — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Bundle per-request connection state (`principal`, `peer`, `client_id`) into a `RequestContext` struct passed to every inline-intercept handler. Close the slice-16 gap where 5 quota call sites pass `""` for `client_id`, defeating `(user, client-id)` tuple quotas.

**Architecture:** New `crates/broker/src/handlers/context.rs` defines `RequestContext<'a>`. Each of the 30 inline-intercept handlers + their `handle_*_frame` callers in `dispatch.rs` swap `principal: &Principal, peer: &SocketAddr` for `ctx: &RequestContext<'_>`. Frame fns compute `client_id` via the existing `peek_client_id(frame).unwrap_or("")` helper. Five quota call sites then replace `""` with `ctx.client_id`. One broker integration test confirms tuple quotas now fire.

**Tech Stack:** Rust 1.95.0. No new dependencies. Reuses slice 13 inline-intercept dispatch, slice 16 quota lookup + `peek_client_id` helper (already in `dispatch.rs:2254`).

**Reference spec:** [`docs/superpowers/specs/2026-05-16-crabka-request-context-17b-design.md`](../specs/2026-05-16-crabka-request-context-17b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/request-context-17b` already created with spec committed at `7339c9e`.

---

## File structure

```
crates/broker/src/handlers/
├── context.rs                                      # NEW — RequestContext
├── mod.rs                                          # MODIFIED — re-export
├── produce.rs                                      # MODIFIED — sig + ctx.client_id + 1 unit test
├── fetch.rs                                        # MODIFIED — sig + ctx.client_id + 1 unit test
├── create_topics.rs                                # MODIFIED — sig + ctx.client_id + 1 unit test
├── delete_topics.rs                                # MODIFIED — sig + ctx.client_id
├── create_partitions.rs                            # MODIFIED — sig + ctx.client_id
├── [17 other Family A modules]                     # MODIFIED — sig only
└── [8 Family B modules]                            # MODIFIED — sig only
crates/broker/src/txn/handlers/
├── add_partitions_to_txn.rs                        # MODIFIED — sig only
├── end_txn.rs                                      # MODIFIED — sig only
└── txn_offset_commit.rs                            # MODIFIED — sig only
crates/broker/src/network/dispatch.rs               # MODIFIED — 30 frame fns
crates/broker/tests/
└── tuple_quota_enforcement.rs                      # NEW — 1 broker integration test
```

4 tasks across 3 batches. T1 must complete before T2; T3 and T4 are parallel after T2.

---

## Batch 1 — RequestContext struct (T1 alone)

### Task 1: Define `RequestContext` + re-export

**Files:**
- Create: `crates/broker/src/handlers/context.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Create `crates/broker/src/handlers/context.rs`**

```rust
//! Per-request connection metadata threaded through every inline-intercept
//! handler.

use std::net::SocketAddr;

use crabka_security::Principal;

/// Per-request connection metadata. Constructed once per frame in
/// `network::dispatch` from the authenticated `ConnectionAuth`, the
/// accept-time peer `SocketAddr`, and the frame's `client_id` header.
pub(crate) struct RequestContext<'a> {
    pub principal: &'a Principal,
    pub peer: &'a SocketAddr,
    /// Frame's `client_id` header. Empty string when the wire field is
    /// null (`-1` length) or zero-length. Matches the existing
    /// `peek_client_id(frame).unwrap_or("")` convention used for the
    /// `request_percentage` quota in the dispatch loop.
    pub client_id: &'a str,
}
```

- [ ] **Step 2: Re-export from `crates/broker/src/handlers/mod.rs`**

Find the line `pub(crate) mod acl_wire;` near the top of the per-module declarations. Just above it, add:

```rust
pub(crate) mod context;
pub(crate) use context::RequestContext;
```

- [ ] **Step 3: Compile**

```
cargo check -p crabka-broker
```

Expected: clean compile (the struct isn't used yet; `dead_code` lint is suppressed for `handlers` via the existing `#![allow(dead_code)]` at the top of `mod.rs`).

- [ ] **Step 4: Commit**

```
git add crates/broker/src/handlers/context.rs crates/broker/src/handlers/mod.rs
git commit -m "feat(slice-17b): RequestContext struct" -m "Bundles principal/peer/client_id for inline-intercept handlers. Wired up in T2."
```

---

## Batch 2 — Atomic signature conversion (T2 alone)

### Task 2: Convert all 30 handlers + 30 frame fns to `RequestContext`

This is one atomic commit: every handler's signature and every frame fn that calls it must move together for the crate to compile.

**Files (all MODIFIED, 31 in total):**
- `crates/broker/src/network/dispatch.rs` (30 frame fns)
- Family A handlers (20):
  - `crates/broker/src/handlers/produce.rs`
  - `crates/broker/src/handlers/fetch.rs`
  - `crates/broker/src/handlers/metadata.rs`
  - `crates/broker/src/handlers/offset_commit.rs`
  - `crates/broker/src/handlers/offset_fetch.rs`
  - `crates/broker/src/handlers/join_group.rs`
  - `crates/broker/src/handlers/describe_groups.rs`
  - `crates/broker/src/handlers/list_groups.rs`
  - `crates/broker/src/handlers/create_topics.rs`
  - `crates/broker/src/handlers/delete_topics.rs`
  - `crates/broker/src/handlers/delete_records.rs`
  - `crates/broker/src/handlers/init_producer_id.rs`
  - `crates/broker/src/txn/handlers/add_partitions_to_txn.rs`
  - `crates/broker/src/txn/handlers/end_txn.rs`
  - `crates/broker/src/txn/handlers/txn_offset_commit.rs`
  - `crates/broker/src/handlers/alter_configs.rs`
  - `crates/broker/src/handlers/create_partitions.rs`
  - `crates/broker/src/handlers/delete_groups.rs`
  - `crates/broker/src/handlers/incremental_alter_configs.rs`
  - `crates/broker/src/handlers/describe_cluster.rs`
- Family B handlers (10):
  - `crates/broker/src/handlers/describe_acls.rs`
  - `crates/broker/src/handlers/create_acls.rs`
  - `crates/broker/src/handlers/delete_acls.rs`
  - `crates/broker/src/handlers/alter_partition_reassignments.rs`
  - `crates/broker/src/handlers/list_partition_reassignments.rs`
  - `crates/broker/src/handlers/describe_client_quotas.rs`
  - `crates/broker/src/handlers/alter_client_quotas.rs`
  - `crates/broker/src/handlers/describe_user_scram_credentials.rs`
  - `crates/broker/src/handlers/alter_user_scram_credentials.rs`
  - `crates/broker/src/handlers/elect_leaders.rs`

- [ ] **Step 1: Verify the current handler signature shapes**

Run:
```
git grep -n "pub(crate) async fn handle(" crates/broker/src/handlers crates/broker/src/txn/handlers
```

Two shapes appear:
- **Family A (20 modules):** `(broker: &Broker, version: i16, _correlation_id: i32, req_bytes: &[u8], principal: &Principal, peer: &SocketAddr) -> Result<Bytes, BrokerError>`
- **Family B (10 modules):** `(broker: &Broker, req: <Type>, principal: &Principal, peer: &SocketAddr, api_version: i16) -> Result<..., ...>`

If any module's signature deviates from one of these two shapes, stop and report — it's outside the slice-17b mechanical scope.

- [ ] **Step 2: Update every Family A handler signature**

For each Family A file, replace the two-parameter pair `principal: &Principal, peer: &SocketAddr` with `ctx: &crate::handlers::RequestContext<'_>`. Worked example for `crates/broker/src/handlers/produce.rs`:

Before:
```rust
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
```

After:
```rust
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
```

Then inside each handler body, mechanical rewrite:
- `principal.name` → `ctx.principal.name`
- `principal,` (as ACL request field, e.g. `AuthorizationRequest { principal, ... }`) → `principal: ctx.principal,`
- `&principal` (passed by-ref) → `ctx.principal`
- `peer,` (as ACL request field `host: peer,`) → `host: ctx.peer,`
- `peer` (any other use) → `ctx.peer`

Remove unused `use std::net::SocketAddr;` and `use crabka_security::Principal;` imports if nothing else in the file references them. Keep them if other code in the module uses those types (e.g., helper functions still take `&Principal`).

- [ ] **Step 3: Update every Family B handler signature**

For each Family B file (e.g., `crates/broker/src/handlers/describe_acls.rs`):

Before:
```rust
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeAclsRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
```

After:
```rust
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeAclsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
```

Apply the same in-body rewrite (`principal` → `ctx.principal`, `peer` → `ctx.peer`).

**Note on `alter_user_scram_credentials::handle`:** Its return type is `AlterUserScramCredentialsResponse` (not `Result<Bytes, ...>`) — leave that as-is; only the parameter list changes.

- [ ] **Step 4: Update every `handle_*_frame` in `dispatch.rs`**

For each of the 30 frame fns at `crates/broker/src/network/dispatch.rs` (line numbers from the current tree — verify with `git grep -n '^async fn handle_\w\+_frame' crates/broker/src/network/dispatch.rs`):

Worked example for `handle_produce_frame` (Family A):

Before:
```rust
async fn handle_produce_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 0);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });

    let resp_body = crate::handlers::produce::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &principal,
        peer,
    )
    .await?;
    Ok(encode_response(api_key, correlation_id, body_flexible, &resp_body))
}
```

After:
```rust
async fn handle_produce_frame(
    broker: &Broker,
    frame: &[u8],
    auth: &crate::network::auth::ConnectionAuth,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let (api_key, api_version, correlation_id, body) = parse_request_header(frame)?;
    debug_assert_eq!(api_key, 0);
    let body_flexible = handler_body_flexible(api_key, api_version);

    let principal = auth
        .principal()
        .cloned()
        .unwrap_or_else(|| crabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            mechanism: crabka_security::SaslMechanism::Plain,
        });
    let client_id = peek_client_id(frame).unwrap_or("");
    let ctx = crate::handlers::RequestContext {
        principal: &principal,
        peer,
        client_id,
    };

    let resp_body = crate::handlers::produce::handle(
        broker,
        api_version,
        correlation_id,
        body,
        &ctx,
    )
    .await?;
    Ok(encode_response(api_key, correlation_id, body_flexible, &resp_body))
}
```

For Family B frame fns, the handler call site changes from `(broker, req, &principal, peer, api_version)` to `(broker, req, &ctx, api_version)` — same shape, same insertion point for the `let client_id = ...; let ctx = ...` block.

Apply this transform to every `handle_*_frame` in the file (30 total). Do them all in the same edit pass; the crate won't compile until the matching handler-side signature change is also done.

- [ ] **Step 5: Compile**

```
cargo check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean. Type mismatches at frame-fn call sites mean a handler was missed; missing-method errors on `ctx.principal` / `ctx.peer` mean an in-body rewrite was missed.

- [ ] **Step 6: Run handler unit tests**

```
cargo test -p crabka-broker --lib handlers::
```

Expected: all existing handler unit tests pass. No semantic change in this task — only the parameter packaging changed.

- [ ] **Step 7: Run broker integration tests (smoke)**

```
cargo test -p crabka-broker --test '*' -- --test-threads=1
```

Expected: green. If any test fails it points at a missed rewrite (e.g., handler still references the removed `principal` parameter via `principal.name`).

- [ ] **Step 8: Commit**

```
git add crates/broker/src/handlers crates/broker/src/txn/handlers crates/broker/src/network/dispatch.rs
git commit -m "refactor(slice-17b): RequestContext for inline-intercept handlers" -m "30 handlers + 30 frame fns. principal+peer collapsed into ctx; client_id now plumbed through (ctx.client_id, unused in this commit). No behavior change."
```

---

## Batch 3 — Quota fix + integration test (parallel: T3, T4)

### Task 3: Wire `ctx.client_id` into 5 quota call sites + unit tests

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs:445` (the `consume_producer_quota` call) + add unit test
- Modify: `crates/broker/src/handlers/fetch.rs:351` (the `consume_consumer_quota` call) + add unit test
- Modify: `crates/broker/src/handlers/create_topics.rs:302` (the `consume_controller_mutation_quota` call) + add unit test
- Modify: `crates/broker/src/handlers/delete_topics.rs:160` (the `consume_controller_mutation_quota` call)
- Modify: `crates/broker/src/handlers/create_partitions.rs:211` (the `consume_controller_mutation_quota` call)

- [ ] **Step 1: Replace `""` with `ctx.client_id` at 5 sites**

For `produce.rs`, find:
```rust
    // ── KIP-13 producer_byte_rate enforcement ───────────────────────
    // client_id is not yet threaded into this handler (T11 wires it through
    // dispatch); use "" so that user-only and default quotas still fire.
    let delay = consume_producer_quota(
        &image,
        &broker.quota_buckets,
        &principal.name,
        "",
        total_produce_bytes,
    );
```
(Note: after T2, `&principal.name` is already `&ctx.principal.name`.)

Replace with:
```rust
    // ── KIP-13 producer_byte_rate enforcement ───────────────────────
    let delay = consume_producer_quota(
        &image,
        &broker.quota_buckets,
        &ctx.principal.name,
        ctx.client_id,
        total_produce_bytes,
    );
```

For `fetch.rs`, find:
```rust
        let delay = consume_consumer_quota(
            &image,
            &broker.quota_buckets,
            &principal.name,
            "",
            total_bytes,
        );
```
(After T2: `&principal.name` is `&ctx.principal.name`.) Replace `""` with `ctx.client_id`.

For `create_topics.rs`, find:
```rust
        // KIP-599: consume controller_mutation_rate quota. client_id is not
        // threaded through HandlerTable (slice-16 known limitation); pass ""
        // so that (user)-only and default quotas still fire.
        let delay = crate::quota::consume_controller_mutation_quota(
            &image,
            &broker.quota_buckets,
            &principal.name,
            "", // client_id not threaded through HandlerTable — see slice 16 known limitation
            mutation_count,
        );
```

Replace with:
```rust
        // KIP-599: consume controller_mutation_rate quota.
        let delay = crate::quota::consume_controller_mutation_quota(
            &image,
            &broker.quota_buckets,
            &ctx.principal.name,
            ctx.client_id,
            mutation_count,
        );
```

For `delete_topics.rs:160` and `create_partitions.rs:211`, perform the identical transform: drop the "client_id not threaded" comment, replace the `""` argument with `ctx.client_id`.

- [ ] **Step 2: Add unit test in `produce.rs` for tuple-quota enforcement**

Find the existing `#[cfg(test)] mod tests` block in `produce.rs`. Append:

```rust
    #[test]
    fn consume_producer_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{
            AlterClientQuotaRecord, ClientQuotaEntity, ClientQuotaEntityType, MetadataImage,
            MetadataRecord,
        };
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V0AlterClientQuota(AlterClientQuotaRecord {
            entity: ClientQuotaEntity {
                entries: vec![
                    (ClientQuotaEntityType::User, Some("alice".into())),
                    (ClientQuotaEntityType::ClientId, Some("app-x".into())),
                ],
            },
            key: "producer_byte_rate".into(),
            value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::default();
        // Tuple match → 4096 bytes overage at 1024 B/s → throttle > 0.
        let delay_match = super::consume_producer_quota(&img, &buckets, "alice", "app-x", 4096);
        assert!(delay_match > std::time::Duration::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}");
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::default();
        let delay_other = super::consume_producer_quota(&img, &buckets2, "alice", "other", 4096);
        assert_eq!(delay_other, std::time::Duration::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}");
    }
```

(Adjust the metadata record name and field names if `git grep -n 'V0AlterClientQuota\|AlterClientQuotaRecord' crates/metadata/src` returns a different shape. The slice-16 PR landed these; the names must already exist.)

- [ ] **Step 3: Add unit test in `fetch.rs` for consumer-side tuple quota**

Append to the existing test module in `fetch.rs`:

```rust
    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{
            AlterClientQuotaRecord, ClientQuotaEntity, ClientQuotaEntityType, MetadataImage,
            MetadataRecord,
        };
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V0AlterClientQuota(AlterClientQuotaRecord {
            entity: ClientQuotaEntity {
                entries: vec![
                    (ClientQuotaEntityType::User, Some("alice".into())),
                    (ClientQuotaEntityType::ClientId, Some("app-x".into())),
                ],
            },
            key: "consumer_byte_rate".into(),
            value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::default();
        let delay_match = super::consume_consumer_quota(&img, &buckets, "alice", "app-x", 4096);
        assert!(delay_match > std::time::Duration::ZERO);
        let buckets2 = crate::quota::QuotaBuckets::default();
        let delay_other = super::consume_consumer_quota(&img, &buckets2, "alice", "other", 4096);
        assert_eq!(delay_other, std::time::Duration::ZERO);
    }
```

- [ ] **Step 4: Add unit test in `create_topics.rs` for controller-mutation tuple quota**

Append to the existing test module in `create_topics.rs`:

```rust
    #[test]
    fn consume_controller_mutation_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{
            AlterClientQuotaRecord, ClientQuotaEntity, ClientQuotaEntityType, MetadataImage,
            MetadataRecord,
        };
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V0AlterClientQuota(AlterClientQuotaRecord {
            entity: ClientQuotaEntity {
                entries: vec![
                    (ClientQuotaEntityType::User, Some("alice".into())),
                    (ClientQuotaEntityType::ClientId, Some("app-x".into())),
                ],
            },
            key: "controller_mutation_rate".into(),
            value: Some(1.0),
        }));
        let buckets = crate::quota::QuotaBuckets::default();
        let delay_match = crate::quota::consume_controller_mutation_quota(
            &img, &buckets, "alice", "app-x", 10,
        );
        assert!(delay_match > std::time::Duration::ZERO);
        let buckets2 = crate::quota::QuotaBuckets::default();
        let delay_other = crate::quota::consume_controller_mutation_quota(
            &img, &buckets2, "alice", "other", 10,
        );
        assert_eq!(delay_other, std::time::Duration::ZERO);
    }
```

- [ ] **Step 5: Compile + run tests**

```
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib handlers::produce::tests::consume_producer_quota_tuple_match_overage_throttles
cargo test -p crabka-broker --lib handlers::fetch::tests::consume_consumer_quota_tuple_match_overage_throttles
cargo test -p crabka-broker --lib handlers::create_topics::tests::consume_controller_mutation_quota_tuple_match_overage_throttles
```

Expected: all 3 tests pass.

- [ ] **Step 6: Commit**

```
git add crates/broker/src/handlers/produce.rs crates/broker/src/handlers/fetch.rs crates/broker/src/handlers/create_topics.rs crates/broker/src/handlers/delete_topics.rs crates/broker/src/handlers/create_partitions.rs
git commit -m "fix(slice-17b): wire ctx.client_id into 5 quota call sites" -m "Closes the slice-16 known limitation: (user,client-id) tuple quotas now fire on Produce/Fetch/CreateTopics/DeleteTopics/CreatePartitions. 3 unit tests cover the 3 quota types."
```

---

### Task 4: Broker integration test — tuple quota fires end-to-end

**Files:**
- Create: `crates/broker/tests/tuple_quota_enforcement.rs`

- [ ] **Step 1: Locate the integration-test idiom**

Read `crates/broker/tests/describe_user_scram_credentials.rs` for the canonical single-broker SASL/PLAIN test scaffold (`spawn_broker_with_admin`, `connect_admin_client`, etc.). Slice 17a established the pattern; reuse it.

If a `AlterClientQuotas` helper exists in `crates/broker/tests/`, identify it via `git grep -n 'AlterClientQuotas' crates/broker/tests`. (Slice 16 landed integration tests using this API; reuse the helper if present.)

- [ ] **Step 2: Write the failing test**

Create `crates/broker/tests/tuple_quota_enforcement.rs`:

```rust
//! Slice 17b — tuple quota enforcement end-to-end.

use std::time::Duration;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tuple_quota_throttles_only_matching_client_id() {
    let cluster = common::spawn_single_broker_sasl_plain().await;

    // Provision (user=alice, client-id=app-x) producer_byte_rate=1024 via
    // AlterClientQuotas as the admin principal.
    common::alter_client_quota_tuple(
        &cluster,
        /* user */ "alice",
        /* client_id */ "app-x",
        /* key */ "producer_byte_rate",
        /* value */ 1024.0,
    )
    .await
    .expect("alter client quota");

    // Produce ~4 KB as (alice, client.id=app-x) → expect throttle.
    let resp_match = common::produce_bytes_as(
        &cluster,
        /* user */ "alice",
        /* client_id */ "app-x",
        /* topic */ "t",
        /* payload */ vec![b'x'; 4096],
    )
    .await
    .expect("produce match");
    assert!(
        resp_match.throttle_time_ms > 0,
        "expected throttle for matching (user, client_id); got {}",
        resp_match.throttle_time_ms
    );

    // Produce ~4 KB as (alice, client.id=other) → no tuple match.
    let resp_other = common::produce_bytes_as(
        &cluster,
        "alice",
        "other",
        "t",
        vec![b'x'; 4096],
    )
    .await
    .expect("produce other");
    assert_eq!(
        resp_other.throttle_time_ms, 0,
        "expected no throttle for non-matching client_id; got {}",
        resp_other.throttle_time_ms
    );
}
```

**If `common/mod.rs` and the named helpers don't exist** in the broker tests dir, adapt the test to whatever scaffold the slice-16 tuple tests used. Look at `git grep -ln 'AlterClientQuotasRequest' crates/broker/tests` to find a working AlterClientQuotas-emitting test, copy its setup verbatim, then layer the tuple+produce assertion on top.

- [ ] **Step 3: Run the test, watch it pass**

```
cargo test -p crabka-broker --test tuple_quota_enforcement -- --nocapture
```

Expected: PASS. (T3's call-site fix is what makes the matching path throttle; T2 alone wouldn't.)

If it fails with `throttle_time_ms == 0` for the matching case, the call-site replacement in T3 was missed or the AlterClientQuotas record didn't apply — diagnose by adding a one-line `dbg!()` on `ctx.client_id` inside `produce::handle` and re-running.

- [ ] **Step 4: Commit**

```
git add crates/broker/tests/tuple_quota_enforcement.rs
git commit -m "test(slice-17b): broker integration test for (user,client-id) tuple quota" -m "Asserts producer_byte_rate=1024 throttles a 4KB produce by (alice,app-x) and does not throttle (alice,other). Covers the end-to-end fix from T3."
```

---

## Final review (after all 4 tasks)

- [ ] **Step 1: Full clippy + test sweep**

```
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: all green.

- [ ] **Step 2: Verify no `""` placeholder remains for client_id in quota calls**

```
git grep -n 'consume_producer_quota\|consume_consumer_quota\|consume_controller_mutation_quota' crates/broker/src
```

Every call site should now pass `ctx.client_id` (or a real `&str` in tests), not `""`.

- [ ] **Step 3: Push branch and open PR**

Confirm with the user before pushing or opening a PR.
