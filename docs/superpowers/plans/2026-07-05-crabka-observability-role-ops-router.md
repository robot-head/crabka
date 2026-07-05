# Crabka Observability Role Ops Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce maintained LOC in `crates/observability/src/lib.rs` by consolidating duplicated distributor, querier, and compactor operational HTTP routes.

**Architecture:** Add a small static `RoleOps` metadata type and install common role operations routes through one helper. Shared handlers read role metadata via Axum `Extension<RoleOps>`, so they do not depend on `DistributorState`, `QuerierState`, or `CompactorDeleteState`.

**Tech Stack:** Rust 1.96.1, Axum `Router`, Axum `Extension`, Tokio tests, `tower::ServiceExt`, `assert2`, `cargo +nightly fmt`.

## Global Constraints

- Preserve every existing public HTTP route for distributor, querier, and compactor.
- Preserve existing response statuses, bodies, and content types for operational endpoints.
- Do not change Loki/OTLP ingest behavior.
- Do not change LogQL query behavior.
- Do not change compaction, delete request materialization, object-store IO, or hot-tail polling.
- Do not perform a broad module split unless required by the router refactor.
- Do not introduce macros for route generation.
- Tests must exercise behavior, not source text.

---

## File Structure

- Modify `crates/observability/src/lib.rs`: add `RoleOps`, shared role operations route installer, shared role-aware config/services/metrics/ring handlers, and replace repeated role route registration.
- Modify `crates/observability/tests/http.rs`: add behavior tests that pin common operations endpoints for all roles and role-specific ring aliases.
- No new production modules: this refactor is intended to reduce total LOC, not move code between files.

---

### Task 1: Pin Role Operations HTTP Behavior

**Files:**
- Modify: `crates/observability/tests/http.rs`

**Interfaces:**
- Consumes: existing public functions `distributor_router`, `loki_router`, `build_service_router`, `QuerierState::new`, `InMemoryWalSink`, `ServiceConfig`, `ServiceDependencies`, `Role`, and `QuerierIndexSource`.
- Produces: tests named `role_operations_routes_match_existing_behavior` and `role_ring_alias_routes_remain_available`.

- [ ] **Step 1: Add test helpers near the existing `service_router_builds_distributor_role` test**

Add this code in `crates/observability/tests/http.rs` before `service_router_builds_distributor_role`:

```rust
fn minimal_service_config(target: Role) -> ServiceConfig {
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    }
}

async fn get_response(app: axum::Router, uri: &str) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn post_form_response(
    app: axum::Router,
    uri: &str,
    body: &'static str,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
}
```

- [ ] **Step 2: Add tests for shared operations endpoints**

Add this code after the helpers:

```rust
#[tokio::test]
async fn role_operations_routes_match_existing_behavior() {
    let distributor = build_service_router(
        &minimal_service_config(Role::Distributor),
        ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
        None,
    )
    .await
    .unwrap();
    let querier = loki_router(QuerierState::new(".", LabelIndex::default(), BlockIndex::default()));
    let compactor = build_service_router(
        &minimal_service_config(Role::Compactor),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for (name, app) in [
        ("distributor", distributor),
        ("querier", querier),
        ("compactor", compactor),
    ] {
        let response = get_response(app.clone(), "/ready").await;
        assert!(response.status() == StatusCode::OK, "{name} /ready status");
        assert!(text_body(response).await == "ready\n", "{name} /ready body");

        let response = get_response(app.clone(), "/config").await;
        assert!(response.status() == StatusCode::OK, "{name} /config status");
        assert!(text_body(response).await == "target: all\n", "{name} /config body");

        let response = get_response(app.clone(), "/config?mode=defaults").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} /config?mode=defaults status"
        );
        assert!(
            text_body(response).await == "target: all\nauth_enabled: true\n",
            "{name} /config?mode=defaults body"
        );

        let response = get_response(app.clone(), "/config?mode=diff").await;
        assert!(
            response.status() == StatusCode::INTERNAL_SERVER_ERROR,
            "{name} /config?mode=diff status"
        );
        assert!(
            text_body(response).await == "unsupported type <nil>\n",
            "{name} /config?mode=diff body"
        );

        let response = get_response(app.clone(), "/services").await;
        assert!(response.status() == StatusCode::OK, "{name} /services status");
        let body = text_body(response).await;
        assert!(body.contains("server => Running"), "{name} /services server row");
        assert!(body.contains("distributor => Running"), "{name} /services distributor row");

        let response = get_response(app.clone(), "/memberlist").await;
        assert!(response.status() == StatusCode::OK, "{name} /memberlist status");
        assert!(
            text_body(response).await == "This instance doesn't use memberlist.",
            "{name} /memberlist body"
        );

        let response = get_response(app.clone(), "/metrics").await;
        assert!(response.status() == StatusCode::OK, "{name} /metrics status");
        assert!(
            text_body(response).await.contains("# HELP"),
            "{name} /metrics body"
        );

        let response = get_response(app.clone(), "/loki/api/v1/status/buildinfo").await;
        assert!(response.status() == StatusCode::OK, "{name} buildinfo status");
        assert!(
            text_body(response).await.contains("crabka"),
            "{name} buildinfo body"
        );

        let response = post_form_response(app.clone(), "/log_level", "log_level=verbose").await;
        assert!(
            response.status() == StatusCode::BAD_REQUEST,
            "{name} invalid log_level status"
        );
        assert!(
            text_body(response).await.contains("unrecognized log level"),
            "{name} invalid log_level body"
        );
    }
}

#[tokio::test]
async fn role_ring_alias_routes_remain_available() {
    let distributor = build_service_router(
        &minimal_service_config(Role::Distributor),
        ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
        None,
    )
    .await
    .unwrap();
    let querier = loki_router(QuerierState::new(".", LabelIndex::default(), BlockIndex::default()));
    let compactor = build_service_router(
        &minimal_service_config(Role::Compactor),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for (app, uri, expected) in [
        (distributor.clone(), "/ring", "crabka-distributor"),
        (distributor, "/distributor/ring", "crabka-distributor"),
        (querier.clone(), "/ring", "crabka-querier"),
        (querier.clone(), "/scheduler/ring", "crabka-scheduler"),
        (querier, "/ruler/ring", "Ruler Ring"),
        (compactor.clone(), "/ring", "crabka-compactor"),
        (compactor, "/compactor/ring", "crabka-compactor"),
    ] {
        let response = get_response(app, uri).await;
        assert!(response.status() == StatusCode::OK, "{uri} status");
        assert!(text_body(response).await.contains(expected), "{uri} body");
    }
}
```

- [ ] **Step 3: Run tests to verify the current behavior is pinned**

Run:

```bash
cargo test -p crabka-observability role_operations_routes_match_existing_behavior
cargo test -p crabka-observability role_ring_alias_routes_remain_available
```

Expected: both tests pass before the refactor. If either test fails because a pinned body differs from current behavior, inspect the current handler response and update the expected string to the current behavior before continuing.

- [ ] **Step 4: Commit the behavior tests**

Run:

```bash
git add crates/observability/tests/http.rs
git commit -m "test(observability): pin role ops endpoints"
```

---

### Task 2: Introduce Shared Role Operations Routes

**Files:**
- Modify: `crates/observability/src/lib.rs`

**Interfaces:**
- Consumes: tests from Task 1.
- Produces:
  - `#[derive(Clone, Copy)] struct RoleOps { target: &'static str, ring_component: &'static str, role_ring_path: Option<&'static str> }`
  - `const DISTRIBUTOR_OPS: RoleOps`
  - `const QUERIER_OPS: RoleOps`
  - `const COMPACTOR_OPS: RoleOps`
  - `fn with_role_ops_routes<S>(router: Router<S>, ops: RoleOps) -> Router<S>`
  - `async fn role_config(Extension(ops): Extension<RoleOps>, RawQuery(raw_query): RawQuery) -> Response`
  - `async fn role_services(Extension(ops): Extension<RoleOps>) -> Response`
  - `async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response`
  - `async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response`

- [ ] **Step 1: Import Axum `Extension`**

In `crates/observability/src/lib.rs`, change the Axum imports from:

```rust
extract::{
    Path, RawQuery, State,
    ws::{Message, WebSocket, WebSocketUpgrade},
},
```

to:

```rust
extract::{
    Path, RawQuery, State,
    ws::{Message, WebSocket, WebSocketUpgrade},
},
Extension,
```

- [ ] **Step 2: Add role metadata and the shared route installer**

Add this code immediately before `fn distributor_router_with_sink`:

```rust
#[derive(Clone, Copy)]
struct RoleOps {
    target: &'static str,
    ring_component: &'static str,
    role_ring_path: Option<&'static str>,
}

const DISTRIBUTOR_OPS: RoleOps = RoleOps {
    target: "distributor",
    ring_component: "crabka-distributor",
    role_ring_path: Some("/distributor/ring"),
};

const QUERIER_OPS: RoleOps = RoleOps {
    target: "querier",
    ring_component: "crabka-querier",
    role_ring_path: None,
};

const COMPACTOR_OPS: RoleOps = RoleOps {
    target: "compactor",
    ring_component: "crabka-compactor",
    role_ring_path: Some("/compactor/ring"),
};

fn with_role_ops_routes<S>(mut router: Router<S>, ops: RoleOps) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router = router
        .route("/ready", get(ready))
        .route("/log_level", get(log_level).post(log_level_post))
        .route("/metrics", get(role_metrics))
        .route("/config", get(role_config))
        .route("/services", get(role_services))
        .route("/memberlist", get(memberlist_status))
        .route("/ring", get(role_ring))
        .route("/loki/api/v1/status/buildinfo", get(build_info));
    if let Some(path) = ops.role_ring_path {
        router = router.route(path, get(role_ring));
    }
    router.layer(Extension(ops))
}
```

- [ ] **Step 3: Replace distributor common route registration**

In `distributor_router_with_sink`, replace this start of the router chain:

```rust
Router::new()
    .route("/ready", get(ready))
    .route("/log_level", get(log_level).post(log_level_post))
    .route("/metrics", get(distributor_metrics))
    .route("/config", get(distributor_config))
    .route("/services", get(distributor_services))
    .route("/memberlist", get(memberlist_status))
    .route("/flush", post(flush_ingester_chunks))
    .route("/ring", get(distributor_ring))
```

with:

```rust
with_role_ops_routes(Router::new(), DISTRIBUTOR_OPS)
    .route("/flush", post(flush_ingester_chunks))
```

Also remove this duplicate distributor ring route from the same chain because the helper installs it:

```rust
.route("/distributor/ring", get(distributor_ring))
```

- [ ] **Step 4: Replace querier common route registration**

In `loki_router`, replace this start of the router chain:

```rust
Router::new()
    .route("/ready", get(ready))
    .route("/log_level", get(log_level).post(log_level_post))
    .route("/metrics", get(querier_metrics))
    .route("/config", get(querier_config))
    .route("/services", get(querier_services))
    .route("/memberlist", get(memberlist_status))
    .route("/ring", get(querier_ring))
    .route("/loki/api/v1/status/buildinfo", get(build_info))
```

with:

```rust
with_role_ops_routes(Router::new(), QUERIER_OPS)
    .route("/loki/api/v1/rules", get(loki_rules))
```

Keep `/scheduler/ring` and `/ruler/ring` routes unchanged.

- [ ] **Step 5: Replace compactor common route registration**

In `compactor_router_with_delete_requests`, replace this start of the router chain:

```rust
Router::new()
    .route("/ready", get(ready))
    .route("/log_level", get(log_level).post(log_level_post))
    .route("/metrics", get(compactor_metrics))
    .route("/config", get(compactor_config))
    .route("/services", get(compactor_services))
    .route("/memberlist", get(memberlist_status))
    .route("/ring", get(compactor_ring))
    .route("/compactor/ring", get(compactor_ring))
```

with:

```rust
with_role_ops_routes(Router::new(), COMPACTOR_OPS)
```

- [ ] **Step 6: Replace role-specific wrapper handlers with role-aware handlers**

Delete these functions:

```rust
async fn querier_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config("querier", raw_query.as_deref())
}

async fn distributor_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config("distributor", raw_query.as_deref())
}

async fn compactor_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config("compactor", raw_query.as_deref())
}
```

Replace them with:

```rust
async fn role_config(Extension(ops): Extension<RoleOps>, RawQuery(raw_query): RawQuery) -> Response {
    status_config(ops.target, raw_query.as_deref())
}
```

Delete these functions:

```rust
async fn querier_services() -> Response {
    status_services("querier")
}

async fn distributor_services() -> Response {
    status_services("distributor")
}

async fn compactor_services() -> Response {
    status_services("compactor")
}
```

Replace them with:

```rust
async fn role_services(Extension(ops): Extension<RoleOps>) -> Response {
    status_services(ops.target)
}
```

Delete these functions:

```rust
async fn querier_metrics() -> Response {
    status_metrics("querier")
}

async fn distributor_metrics() -> Response {
    status_metrics("distributor")
}

async fn compactor_metrics() -> Response {
    status_metrics("compactor")
}
```

Replace them with:

```rust
async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response {
    status_metrics(ops.target)
}
```

Delete these functions:

```rust
async fn distributor_ring() -> Response {
    ring_status_page("crabka-distributor")
}

async fn querier_ring() -> Response {
    ring_status_page("crabka-querier")
}

async fn compactor_ring() -> Response {
    ring_status_page("crabka-compactor")
}
```

Replace them with:

```rust
async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response {
    ring_status_page(ops.ring_component)
}
```

Keep these specialized ring handlers unchanged:

```rust
async fn scheduler_ring() -> Response {
    ring_status_page("crabka-scheduler")
}

async fn ruler_ring() -> Response {
    ruler_status_page()
}
```

- [ ] **Step 7: Run the pinned route tests**

Run:

```bash
cargo test -p crabka-observability role_operations_routes_match_existing_behavior
cargo test -p crabka-observability role_ring_alias_routes_remain_available
```

Expected: both tests pass.

- [ ] **Step 8: Run formatting check**

Run: `cargo +nightly fmt --all -- --check`

Expected: command exits successfully. If it reports diffs, run `cargo +nightly fmt --all`, then rerun the check.

- [ ] **Step 9: Commit shared role ops router**

Run:

```bash
git add crates/observability/src/lib.rs
git commit -m "refactor(observability): share role ops routes"
```

---

### Task 3: Full Observability Verification And Cleanup

**Files:**
- Modify: `crates/observability/src/lib.rs` only if Task 2 left unused imports or formatting fallout.
- Modify: `crates/observability/tests/http.rs` only if Task 1 tests need minor compile adjustments after Task 2.

**Interfaces:**
- Consumes: `with_role_ops_routes`, `RoleOps`, and tests from Tasks 1 and 2.
- Produces: verified final branch with fewer maintained LOC in `crates/observability/src/lib.rs`.

- [ ] **Step 1: Check for stale role wrapper names**

Run: `git grep -n -E "querier_config|distributor_config|compactor_config|querier_services|distributor_services|compactor_services|querier_metrics|distributor_metrics|compactor_metrics|querier_ring|distributor_ring|compactor_ring" -- crates/observability/src/lib.rs`

Expected: exit code 1 with no output, meaning no deleted wrapper names remain. Matches for `scheduler_ring` and `ruler_ring` are acceptable if a local implementation broadens the search, because those handlers stay.

- [ ] **Step 2: Run the full observability test suite**

Run: `cargo test -p crabka-observability`

Expected: all tests pass.

- [ ] **Step 3: Run crate clippy**

Run: `cargo clippy -p crabka-observability --all-targets -- -D warnings`

Expected: command exits successfully with no warnings.

- [ ] **Step 4: Measure LOC reduction**

Run: `git diff --stat HEAD~2..HEAD -- crates/observability/src/lib.rs crates/observability/tests/http.rs`

Expected: `crates/observability/src/lib.rs` shows net fewer lines. `crates/observability/tests/http.rs` may show additions because behavior coverage was added.

- [ ] **Step 5: Commit cleanup only if needed**

If Step 1, Step 2, or Step 3 required code cleanup after Task 2's commit, run:

```bash
git add crates/observability/src/lib.rs crates/observability/tests/http.rs
git commit -m "fix(observability): clean up role ops refactor"
```

If no files changed, skip this commit.
