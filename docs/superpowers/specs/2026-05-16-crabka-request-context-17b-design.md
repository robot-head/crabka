# Slice 17b: RequestContext + tuple-quota plumbing — Design

**Status:** Approved 2026-05-16.

**Goal:** Bundle per-request connection state (`principal`, `peer`, `client_id`) into a single `RequestContext` struct passed to every inline-intercept handler. Use it to close the slice-16 gap where 5 quota call sites pass `""` for `client_id`, defeating `(user, client-id)` tuple quotas.

**Out of scope:**
- `HandlerTable`-routed handlers (`list_offsets`, `find_coordinator`, etc.) — they don't need ctx; no quota call sites in them.
- Changing the `HandlerFn` typedef. Inline-intercept handlers don't go through the table, so the table stays as-is.
- KIP-219 cross-broker throttle propagation.
- Quota refresh / config plumbing — slices 16 / 16b / 16c already complete.

---

## 1. Background

Slice 16 implemented `(user, client-id)` tuple-quota lookup and bucket charging correctly in `crate::quota::lookup_quota_with_key`. The Kafka entity-precedence algorithm correctly orders `(user, client-id)` ahead of `(user)` and the defaults. But 5 of the inline-intercept handlers — Produce, Fetch, CreateTopics, DeleteTopics, CreatePartitions — pass `client_id: ""` at the call site:

```rust
// crates/broker/src/handlers/produce.rs:445
let delay = consume_producer_quota(
    &image,
    &broker.quota_buckets,
    &principal.name,
    "",                       // <-- BUG: tuple quotas can never match
    total_produce_bytes,
);
```

The `peek_client_id` helper (`crates/broker/src/network/dispatch.rs:2254`) already exists and is used in the dispatch loop's `request_percentage` charging path (line 826). It just isn't piped down into the handler frames.

Slice 17b plumbs `client_id` through every inline-intercept handler by introducing a `RequestContext` struct that bundles it with the existing `&Principal` + `&SocketAddr` parameters those handlers already take. The struct collapses three positional parameters into one and creates a natural extension point for any future per-request connection metadata.

---

## 2. The `RequestContext` struct

`crates/broker/src/handlers/context.rs` (new):

```rust
use std::net::SocketAddr;

use crabka_security::Principal;

/// Per-request connection metadata threaded through every inline-intercept
/// handler. Constructed once per frame in `network::dispatch` from the
/// authenticated `ConnectionAuth`, the accept-time peer `SocketAddr`, and
/// the frame's `client_id` header field.
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

Re-exported from `crates/broker/src/handlers/mod.rs`:

```rust
pub(crate) mod context;
pub(crate) use context::RequestContext;
```

---

## 3. Handler signature change

Every inline-intercept handler currently has the shape:

```rust
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError>;
```

becomes:

```rust
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError>;
```

In-handler references `principal` / `peer` become `ctx.principal` / `ctx.peer`. New `ctx.client_id` is available where needed (5 sites in this slice).

### Inline-intercept handler inventory (22 modules)

From `crates/broker/src/handlers/mod.rs` comments + the `handle_*_frame` fns in `dispatch.rs`:

| Module | `api_key` |
|---|---|
| `produce` | 0 |
| `fetch` | 1 |
| `metadata` | 3 |
| `offset_commit` | 8 |
| `offset_fetch` | 9 |
| `join_group` | 11 |
| `describe_groups` | 15 |
| `list_groups` | 16 |
| `create_topics` | 19 |
| `delete_topics` | 20 |
| `delete_records` | 21 |
| `init_producer_id` | 22 |
| `add_partitions_to_txn` (in `txn::handlers`) | 24 |
| `end_txn` (in `txn::handlers`) | 26 |
| `txn_offset_commit` (in `txn::handlers`) | 28 |
| `alter_configs` | 33 |
| `create_partitions` | 37 |
| `delete_groups` | 42 |
| `incremental_alter_configs` | 44 |
| `describe_user_scram_credentials` | 50 |
| `alter_user_scram_credentials` | 51 |
| `describe_cluster` | 60 |

Each module's `handle` plus its `handle_*_frame` caller in `dispatch.rs` is touched in one mechanical pass. No behavior change for the ~17 non-quota handlers — pure parameter repack.

---

## 4. Frame fn change in `dispatch.rs`

Every `handle_*_frame` currently builds `principal` + uses `peer`, then calls the handler. The new shape constructs a `RequestContext` once:

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

`peek_client_id` stays in `dispatch.rs` (it's already there). It does NOT need to become `pub(crate)` outside the module — frame fns are in the same module.

---

## 5. Semantic fix: 5 quota call sites

The actual bug being closed. After step 3, each handler has `ctx.client_id` available:

| File:line | Before | After |
|---|---|---|
| `crates/broker/src/handlers/produce.rs:449` | `""` | `ctx.client_id` |
| `crates/broker/src/handlers/fetch.rs:355` | `""` | `ctx.client_id` |
| `crates/broker/src/handlers/create_topics.rs:306` | `""` | `ctx.client_id` |
| `crates/broker/src/handlers/delete_topics.rs:160` | `""` | `ctx.client_id` |
| `crates/broker/src/handlers/create_partitions.rs:211` | `""` | `ctx.client_id` |

Drop the stale `// client_id is not yet threaded ...` / `// slice-16 known limitation` comments at those sites.

---

## 6. Testing

### Per-handler unit tests (3 — Produce, Fetch, one CRUD as a representative)

Add to each of `produce.rs`, `fetch.rs`, and `create_topics.rs` a unit test that confirms tuple-quota routing. The test:

1. Builds a `MetadataImage` with an `AlterClientQuotas` record setting `(user=alice, client-id=app-x) producer_byte_rate=1024` (or the relevant quota type).
2. Calls `consume_*_quota(image, buckets, "alice", "app-x", bytes_over_quota)` directly.
3. Asserts `Duration > 0` (throttle observed) — and the comparison test with `client_id="other"` returns `Duration::ZERO` because no tuple matches and no `(user=alice)`-only quota exists.

The other CRUD handlers (`delete_topics`, `create_partitions`) and the txn / group / config / SCRAM handlers don't get new unit tests — the parameter repack is mechanical and compile-checked.

### Broker integration test (1)

`crates/broker/tests/tuple_quota_enforcement.rs` (new):

`tuple_quota_throttles_only_matching_client_id`:

1. Single-broker SASL/PLAIN cluster with admin user provisioned (slice-12 idiom).
2. `AlterClientQuotas` for `(user=alice, client-id=app-x)` setting `producer_byte_rate = 1024`.
3. Produce 4 KB as `(alice, client.id=app-x)` → assert `throttle_time_ms > 0` on the response.
4. Produce 4 KB as `(alice, client.id=other)` → assert `throttle_time_ms == 0` (no tuple match, no (user=alice)-only quota).

### No JVM acceptance change

Slice 16 already exercises `AlterClientQuotas` against the JVM `kafka-configs` tool. Tuple-quota *enforcement* is wire-internal — JVM tools don't expose throttle timings on the client side in a way that's easy to assert on, and the broker-side integration test above covers it.

---

## 7. File structure & task layout

```
crates/broker/src/handlers/
├── context.rs                                       # NEW — RequestContext
├── mod.rs                                           # MODIFIED — re-export
├── produce.rs                                       # MODIFIED — sig + ctx.client_id + 1 test
├── fetch.rs                                         # MODIFIED — sig + ctx.client_id + 1 test
├── create_topics.rs                                 # MODIFIED — sig + ctx.client_id + 1 test
├── delete_topics.rs                                 # MODIFIED — sig + ctx.client_id
├── create_partitions.rs                             # MODIFIED — sig + ctx.client_id
├── metadata.rs                                      # MODIFIED — sig (no client_id use)
├── offset_commit.rs                                 # MODIFIED — sig
├── offset_fetch.rs                                  # MODIFIED — sig
├── join_group.rs                                    # MODIFIED — sig
├── describe_groups.rs                               # MODIFIED — sig
├── list_groups.rs                                   # MODIFIED — sig
├── delete_records.rs                                # MODIFIED — sig
├── init_producer_id.rs                              # MODIFIED — sig
├── alter_configs.rs                                 # MODIFIED — sig
├── delete_groups.rs                                 # MODIFIED — sig
├── incremental_alter_configs.rs                     # MODIFIED — sig
├── describe_user_scram_credentials.rs               # MODIFIED — sig
├── alter_user_scram_credentials.rs                  # MODIFIED — sig
└── describe_cluster.rs                              # MODIFIED — sig
crates/broker/src/txn/handlers/
├── add_partitions_to_txn.rs                         # MODIFIED — sig
├── end_txn.rs                                       # MODIFIED — sig
└── txn_offset_commit.rs                             # MODIFIED — sig
crates/broker/src/network/dispatch.rs                # MODIFIED — 22 frame fns
crates/broker/tests/
└── tuple_quota_enforcement.rs                       # NEW — 1 integration test
```

Implementation plan target: ~4 tasks.

- **T1.** Add `RequestContext` struct in `handlers/context.rs` + re-export from `handlers/mod.rs`.
- **T2.** Mechanical signature conversion across all 22 inline-intercept handlers + their `handle_*_frame` callers in `dispatch.rs` (one atomic commit; cross-file signature must move together).
- **T3.** Semantic fix at 5 quota sites + drop stale comments + add 3 unit tests (Produce, Fetch, CreateTopics).
- **T4.** Broker integration test in `crates/broker/tests/tuple_quota_enforcement.rs`.

T1 must precede T2. T3 and T4 can run in parallel after T2 (T3 edits handler files, T4 only adds a new test file).
