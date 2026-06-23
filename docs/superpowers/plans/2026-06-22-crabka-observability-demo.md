# Crabka Full-Signal Observability Demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A one-command `docker compose up` demo where Grafana queries Crabka's four observability backends (metrics, traces, logs, profiles), Crabka exports all four of its own signals into those backends, and a purpose-built `crabka-client-streams` orders pipeline runs its Kafka traffic on Crabka and is fully instrumented.

**Architecture:** One `crabka-broker` is triple-duty (demo app event bus + WAL for all four backends + self-observed subject). One Grafana Alloy collects every signal from both sources (Crabka components + the demo app) and writes to the four backends, which persist through the broker (WAL) and a shared MinIO bucket (blocks). Spec: [docs/superpowers/specs/2026-06-22-crabka-observability-demo-design.md](docs/superpowers/specs/2026-06-22-crabka-observability-demo-design.md).

**Tech Stack:** Rust (workspace, edition 2024), axum 0.8, `crabka-telemetry` (OTLP), `pprof` (CPU profiling), `tikv-jemallocator` + `jemalloc_pprof` (heap profiling), `crabka-client-streams` + `crabka-schema-serde` (proto/Streams), `object_store` (S3/MinIO), Docker Compose, Grafana + Grafana Alloy + MinIO.

## Global Constraints

Every task implicitly includes these:

- **Workspace:** `members = ["crates/*"]` (root `Cargo.toml`) — a new `crates/observability-demo-app/` is auto-included; no workspace edit needed. `version = "0.3.8"`, `edition = "2024"`, `rust-version = "1.96.0"`. Reference shared deps as `{ workspace = true }`.
- **No backwards-compatibility shims** (greenfield per `CLAUDE.md`). When a format/enum/flag changes, just change it.
- **Conventional commits** drive release-plz: `feat:` minor, `fix:` patch, `chore:`/`docs:`/`test:` no bump. Use them on every commit.
- **`cargo fmt` before every commit.** On this Windows deep worktree, workspace-wide `cargo +nightly fmt --all` fails with OS error 206 (path too long) — format per crate: `cargo +nightly fmt -p <crate>`.
- **`heap-profiling` is a default-OFF cargo feature.** Only the demo Docker image builds with `--features heap-profiling`. Normal/bench/prod builds keep the system allocator. CPU profiling is always available (no feature).
- **`publish = false`** on every new/observability crate touched (Task 1 + demo app). Never publish demo or LGTM+P backend crates.
- **Manual showcase, no CI job.** Verification = `cargo build`/`cargo test`/`cargo clippy` for code, and `docker compose up` + `curl` for the fixture.
- **In-container Kafka clients use the advertised listener `broker:9092`.** The broker runs with `--advertised-listener=broker:9092`.
- **MinIO S3 wiring (uniform across all four backends):** flag `--object-store-url s3://crabka-blocks/<signal>`; env consumed by `object_store`: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP=true`, `AWS_REGION=us-east-1`.
- **Tenant:** all Grafana datasources send `X-Scope-OrgID: demo`; ingest paths carry the same header. Use the tenant string `demo` throughout.
- **Demo image retains debug symbols** (no `strip`) so CPU/heap flamegraphs symbolize.

---

## Batch Plan

| Batch | Tasks | Parallel? | Rationale |
|---|---|---|---|
| **A — Foundations** | 1, 2, 3 | Yes (disjoint files) | publish flags, telemetry profiling module, uniform S3 in the backends |
| **B — App + self-instrumentation** | 4, 5, 6, 7 | Yes (disjoint crates) | instrument logs binary; demo app; broker profiling; other service binaries |
| **C — Fixture & containers** | 8, then 9/10/11, then 12 | Partial | image first; compose/alloy/grafana; smoke last |

Dispatch each batch's tasks concurrently (one message, multiple agents), review, then proceed. Within Batch C, Task 8 (image) precedes 9–11, and Task 12 (smoke) is last.

---

## Batch A — Foundations

### Task 1: Mark observability backend crates `publish = false`

**Files:**
- Modify: `crates/metrics/Cargo.toml`, `crates/metrics-service/Cargo.toml`, `crates/promql/Cargo.toml`, `crates/logql/Cargo.toml`, `crates/observability-spike/Cargo.toml`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing (build-config only).

- [ ] **Step 1: Add `publish = false` to each `[package]` table**

In each of the five `Cargo.toml` files, add `publish = false` directly under the `[package]` line (these crates currently inherit the default `publish = true`). Example for `crates/metrics/Cargo.toml`:

```toml
[package]
name = "crabka-metrics"
publish = false
version.workspace = true
edition.workspace = true
# ... rest unchanged
```

Apply the identical one-line addition to `crates/metrics-service/Cargo.toml`, `crates/promql/Cargo.toml`, `crates/logql/Cargo.toml`, and `crates/observability-spike/Cargo.toml`.

- [ ] **Step 2: Verify the flags and that the workspace still builds**

Run: `cargo metadata --format-version 1 --no-deps | python -c "import json,sys; d=json.load(sys.stdin); print([p['name'] for p in d['packages'] if p['name'] in ('crabka-metrics','crabka-metrics-service','crabka-promql','crabka-logql','crabka-observability-spike') and p['publish']==[]])"`
Expected: `['crabka-metrics', 'crabka-metrics-service', 'crabka-promql', 'crabka-logql', 'crabka-observability-spike']` (cargo represents `publish = false` as `publish: []`).

Run: `cargo build -p crabka-metrics -p crabka-promql -p crabka-logql`
Expected: builds succeed.

- [ ] **Step 3: Commit**

```bash
cargo +nightly fmt -p crabka-metrics -p crabka-metrics-service -p crabka-promql -p crabka-logql -p crabka-observability-spike
git add crates/metrics/Cargo.toml crates/metrics-service/Cargo.toml crates/promql/Cargo.toml crates/logql/Cargo.toml crates/observability-spike/Cargo.toml
git commit -m "chore: mark observability backend crates publish=false"
```

---

### Task 2: `crabka-telemetry` in-process profiling module + `heap-profiling` feature

**Files:**
- Create: `crates/telemetry/src/profiling.rs`
- Modify: `crates/telemetry/src/lib.rs` (add `pub mod profiling;`)
- Modify: `crates/telemetry/Cargo.toml` (deps + `[features]`)
- Test: `crates/telemetry/tests/profiling.rs`

**Interfaces:**
- Consumes: nothing.
- Produces (used by Tasks 5, 6, 7):
  - `crabka_telemetry::profiling::pprof_router() -> axum::Router` — routes `GET /debug/pprof/profile` (CPU, always) and, under `heap-profiling`, `GET /debug/pprof/heap`.
  - `crabka_telemetry::profiling::serve_admin(addr: std::net::SocketAddr, extra: axum::Router) -> std::io::Result<()>` — spawns an admin server merging `pprof_router()` with `extra`; returns once bound.
  - `crabka_telemetry::profiling::serve_admin_from_env(default_addr: &str) -> std::io::Result<()>` — reads `CRABKA_ADMIN_LISTEN_ADDR` or uses `default_addr`, then `serve_admin(addr, Router::new())`.

- [ ] **Step 1: Add dependencies and the feature to `crates/telemetry/Cargo.toml`**

Add to `[dependencies]`:

```toml
axum = { workspace = true, features = ["query", "tokio", "http1"] }
prost = { workspace = true }
pprof = { version = "0.14", default-features = false, features = ["prost-codec"] }
serde = { workspace = true, features = ["derive"] }
tokio = { workspace = true, features = ["net", "rt", "time", "macros"] }
# heap-profiling only (jemalloc_pprof provides the PROF_CTL global it needs;
# the jemalloc allocator itself is supplied by each binary, not this lib):
jemalloc_pprof = { version = "0.7", optional = true }
```

Add a `[features]` table:

```toml
[features]
heap-profiling = ["dep:jemalloc_pprof"]
```

- [ ] **Step 2: Write the failing test**

Create `crates/telemetry/tests/profiling.rs`:

```rust
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt; // for `oneshot`

#[tokio::test]
async fn cpu_profile_endpoint_returns_pprof_bytes() {
    let app = crabka_telemetry::profiling::pprof_router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/debug/pprof/profile?seconds=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    // A pprof CPU profile is a non-empty gzip/protobuf blob.
    assert!(!body.is_empty(), "expected a non-empty pprof profile");
}
```

Add `tower = { workspace = true, features = ["util"] }` to `crates/telemetry/[dev-dependencies]`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p crabka-telemetry --test profiling`
Expected: FAIL to compile — `profiling` module does not exist.

- [ ] **Step 4: Implement the profiling module**

Create `crates/telemetry/src/profiling.rs`:

```rust
//! In-process profiling admin server.
//!
//! Always serves a CPU pprof profile at `GET /debug/pprof/profile?seconds=N`.
//! When the `heap-profiling` feature is enabled (jemalloc), also serves a heap
//! pprof profile at `GET /debug/pprof/heap`. Grafana Alloy `pyroscope.scrape`
//! pulls both. The same admin server can carry extra routes (e.g. `/metrics`).

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use prost::Message as _;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct CpuQuery {
    seconds: Option<u64>,
}

/// CPU profile in pprof protobuf, sampled for `?seconds=N` (default 30, clamped 1..=60).
async fn cpu_profile(Query(q): Query<CpuQuery>) -> axum::response::Response {
    let seconds = q.seconds.unwrap_or(30).clamp(1, 60);
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("profiler: {e}")).into_response();
        }
    };
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("report: {e}")).into_response(),
    };
    let profile = match report.pprof() {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("pprof: {e}")).into_response(),
    };
    let body = profile.encode_to_vec();
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        body,
    )
        .into_response()
}

#[cfg(feature = "heap-profiling")]
async fn heap_profile() -> axum::response::Response {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "jemalloc profiling not enabled (build with --features heap-profiling and set MALLOC_CONF)",
        )
            .into_response();
    };
    let mut ctl = ctl.lock().await;
    if !ctl.activated() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "jemalloc prof not active").into_response();
    }
    match ctl.dump_pprof() {
        Ok(pprof) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            pprof,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("heap dump: {e}")).into_response(),
    }
}

/// The pprof routes: CPU always; heap under the `heap-profiling` feature.
#[must_use]
pub fn pprof_router() -> Router {
    let router = Router::new().route("/debug/pprof/profile", get(cpu_profile));
    #[cfg(feature = "heap-profiling")]
    let router = router.route("/debug/pprof/heap", get(heap_profile));
    router
}

/// Bind an admin HTTP server on `addr` serving `pprof_router()` merged with
/// `extra` (e.g. a `/metrics` route). Spawns the server and returns once bound.
pub async fn serve_admin(addr: SocketAddr, extra: Router) -> std::io::Result<()> {
    let app = pprof_router().merge(extra);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "profiling admin server listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::warn!(error = %e, "admin server error");
        }
    });
    Ok(())
}

/// Like [`serve_admin`] but resolves the bind address from
/// `CRABKA_ADMIN_LISTEN_ADDR`, falling back to `default_addr`.
pub async fn serve_admin_from_env(default_addr: &str) -> std::io::Result<()> {
    let raw = std::env::var("CRABKA_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let addr: SocketAddr = raw
        .parse()
        .unwrap_or_else(|e| panic!("invalid CRABKA_ADMIN_LISTEN_ADDR `{raw}`: {e}"));
    serve_admin(addr, Router::new()).await
}
```

Add to `crates/telemetry/src/lib.rs` (near the top, after the module docs):

```rust
pub mod profiling;
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-telemetry --test profiling`
Expected: PASS (the CPU profile blob is non-empty).

- [ ] **Step 6: Verify the heap feature compiles**

Run: `cargo build -p crabka-telemetry --features heap-profiling`
Expected: builds (the heap route compiles; jemalloc allocator is supplied by binaries, not this lib).

- [ ] **Step 7: Commit**

```bash
cargo +nightly fmt -p crabka-telemetry
git add crates/telemetry/
git commit -m "feat(telemetry): in-process pprof admin server (CPU always, heap under feature)"
```

---

### Task 3: Uniform S3 object store in `crabka-profiles`, `crabka-metrics`, `crabka-metrics-service`, and switch `crabka-traces` to `parse_url_opts`

**Files:**
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`
- Modify: `crates/metrics/src/bin/crabka-metrics.rs`
- Modify: `crates/metrics-service/src/main.rs`
- Modify: `crates/traces/src/bin/crabka-traces.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: each binary accepts `--object-store-url <url>` where `<url>` may be `s3://bucket/prefix` (MinIO) or `file:///path` or `memory:///`. The S3 store is built via `object_store::parse_url_opts(&url, std::env::vars())` so `AWS_*` env applies.

The canonical pattern is `crabka-observability`'s `build_configured_object_store` (`crates/observability/src/lib.rs:1667`). For non-`file`/non-`s3` schemes, `parse_url_opts(&url, std::env::vars())` covers all cases.

- [ ] **Step 1: Profiles — replace local-FS with URL parsing**

In `crates/profiles/src/bin/crabka-profiles.rs`, change the CLI field:

```rust
// BEFORE:
#[arg(long, default_value = ".crabka-profiles-blocks")]
object_store_dir: std::path::PathBuf,
// AFTER:
#[arg(long, default_value = "file://./.crabka-profiles-blocks")]
object_store_url: String,
```

Add a helper near the top of the file:

```rust
fn build_object_store(
    url: &str,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = url::Url::parse(url)?;
    let (store, _prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(std::sync::Arc::from(store))
}
```

Replace every `let store = LocalFileSystem::new_with_prefix(&cli.object_store_dir)?; let store: Arc<dyn ObjectStore> = Arc::new(store);` (BlockBuilder, Querier, QueryFrontend, Compactor arms) with:

```rust
let store = build_object_store(&cli.object_store_url)?;
```

Remove the now-unused `LocalFileSystem` import if the compiler flags it. Add `url = { workspace = true }` to `crates/profiles/Cargo.toml` `[dependencies]` if not already present (check first; `object_store` is already a dep).

> **Object-key prefix note:** the existing code reads/writes fixed keys like `index/profiles.json` directly on the store. With `s3://crabka-blocks/profiles`, `parse_url_opts` returns a store rooted at the bucket and a `prefix` of `profiles`. If the profiles binary ignored the prefix before (it used a prefixed `LocalFileSystem`), prepend the parsed `prefix` to those keys, OR pass the full prefix in the URL and keep keys relative. Confirm by checking whether `ProfileIndex::load(&store, "index/profiles.json")` should become `format!("{prefix}/index/profiles.json")`. Mirror exactly how `crabka-traces`'s `ConfiguredObjectStore::object_key` composes `prefix + key` (`crates/traces/src/bin/crabka-traces.rs` `build_object_store`/`object_key`).

- [ ] **Step 2: Profiles — verify build**

Run: `cargo build -p crabka-profiles`
Expected: builds. Then `crabka-profiles --target querier --object-store-url memory:/// --help` is not needed; a compile is the gate.

- [ ] **Step 3: Metrics ingest/compactor — URL parsing**

In `crates/metrics/src/bin/crabka-metrics.rs`, only the **Compactor** arm constructs an object store (the Distributor writes to the WAL/Kafka, not the object store — verified at `crabka-metrics.rs:220` vs `:269`). Rename the arg `object_store_dir: PathBuf` → `object_store_url: String` (default `file://./.crabka-metrics-blocks`), and in the Compactor arm replace `LocalFileSystem::new_with_prefix(&cli.object_store_dir)` with the same `build_object_store` helper (add it to this file too):

```rust
fn build_object_store(
    url: &str,
) -> Result<std::sync::Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error>> {
    let parsed = url::Url::parse(url)?;
    let (store, _prefix) = object_store::parse_url_opts(&parsed, std::env::vars())?;
    Ok(std::sync::Arc::from(store))
}
```

Apply the same prefix-composition note as Step 1.

- [ ] **Step 4: Metrics querier (`metrics-service`) — URL parsing**

In `crates/metrics-service/src/main.rs`, the querier/query-frontend/ruler build:

```rust
let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&cli.object_store_dir)?);
let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
    Arc::clone(&store),
    url::Url::parse("file:///").expect("valid file object store URL"),
    &cli.manifest_prefix,
    WalHead::new(),
);
```

Change the CLI field `object_store_dir: PathBuf` → `object_store_url: String` (default `file://./.crabka-metrics-blocks`). Replace the store construction in all three `run_*` functions with:

```rust
let object_store_url = url::Url::parse(&cli.object_store_url)?;
let (store, _prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
let store: Arc<dyn ObjectStore> = Arc::from(store);
let metric_store = crabka_metrics_service::RefreshingMetricBlockStore::new(
    Arc::clone(&store),
    object_store_url.clone(),
    &cli.manifest_prefix,
    WalHead::new(),
);
```

(Pass the real `object_store_url` to `RefreshingMetricBlockStore::new` instead of the hardcoded `file:///`.) Remove the unused `LocalFileSystem` import / `std::fs::create_dir_all` calls that assumed a local dir (guard the `create_dir_all` behind a `file://` scheme check, or drop it — for S3 there's no dir to create). Add `object_store = { workspace = true }` to `crates/metrics-service/Cargo.toml` `[dependencies]` if not present.

- [ ] **Step 5: Traces — switch `parse_url` → `parse_url_opts`**

In `crates/traces/src/bin/crabka-traces.rs`, `build_object_store` uses `object_store::parse_url(&root)?`. Change to:

```rust
let (store, prefix) = object_store::parse_url_opts(&root, std::env::vars())?;
```

so the MinIO endpoint/credential env is applied (bare `parse_url` ignores env options).

- [ ] **Step 6: Build all four**

Run: `cargo build -p crabka-profiles -p crabka-metrics -p crabka-metrics-service -p crabka-traces`
Expected: all build.

- [ ] **Step 7: Commit**

```bash
cargo +nightly fmt -p crabka-profiles -p crabka-metrics -p crabka-metrics-service -p crabka-traces
git add crates/profiles/ crates/metrics/ crates/metrics-service/ crates/traces/
git commit -m "feat(observability): uniform --object-store-url (S3/MinIO) across all four backends"
```

---

## Batch B — App + self-instrumentation

Dispatch Tasks 4–7 concurrently (disjoint crates), review, then proceed to Batch C.

### Task 4: Instrument the existing `crabka-observability` logs binary

**Files:**
- Modify: `crates/observability/src/main.rs` (the EXISTING logs binary)
- Modify: `crates/observability/Cargo.toml` (add `crabka-telemetry` dep + `heap-profiling` feature + optional jemalloc)

**Interfaces:**
- Consumes: `crabka_observability::{ServiceConfig, build_service_dependencies, serve_service}` (already used by `src/main.rs`); `crabka_telemetry::{init, OtlpConfig}`, `crabka_telemetry::profiling::serve_admin_from_env`.
- Produces: the existing `crabka-observability` binary (the logs service) now emits OTLP traces + JSON logs and exposes `/debug/pprof/*` on `:9404`. S3/MinIO is handled **inside** `build_service_dependencies` (it reads `config.object_store_url` via `parse_url_opts`), so no object-store wiring is needed here. Compose invokes it as `crabka-observability --target {distributor,compactor,querier} ...`.

> **The logs service binary already exists** (verified) — `crates/observability/src/main.rs`:
> ```rust
> use clap::Parser;
> use crabka_observability::{ServiceConfig, build_service_dependencies, serve_service};
> #[tokio::main]
> async fn main() -> Result<(), Box<dyn std::error::Error>> {
>     let config = ServiceConfig::parse();
>     let dependencies = build_service_dependencies(&config).await?;
>     serve_service(config, dependencies, None).await?;
>     Ok(())
> }
> ```
> `ServiceConfig` derives `clap::Parser` with `--target {distributor,compactor,querier}`, `--listen-addr` (default `127.0.0.1:3100`), `--object-store-url`, `--wal-bootstrap-server`, `--wal-topic`, `--index-prefix`, etc. The binary name is `crabka-observability`. **This task instruments it; it does NOT create a new binary.** (Depends on Task 2's profiling module — hence Batch B.)

- [ ] **Step 1: Add deps + feature to `crates/observability/Cargo.toml`**

```toml
[features]
heap-profiling = ["crabka-telemetry/heap-profiling", "dep:tikv-jemallocator"]

[dependencies]
crabka-telemetry = { version = "0.3.8", path = "../telemetry" }
tikv-jemallocator = { version = "0.6", optional = true, features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }
```

(`axum`, `clap`, `object_store`, `tokio`, `url` are already dependencies; `crabka-observability` is already `publish = false`.)

- [ ] **Step 2: Instrument `crates/observability/src/main.rs`**

Replace the file with:

```rust
//! `crabka-observability` — role-selectable Loki-compatible logs service,
//! self-instrumented (OTLP traces + JSON logs + CPU/heap pprof).

#[cfg(feature = "heap-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "heap-profiling")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use clap::Parser;
use crabka_observability::{ServiceConfig, build_service_dependencies, serve_service};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "crabka-logs",
            env!("CARGO_PKG_VERSION"),
            "crabka-logs",
        ),
        "crabka_observability=info,info",
        "info",
        "crabka-logs",
    )?;
    // CPU/heap profiling admin server (Alloy pyroscope.scrape target).
    crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;

    let config = ServiceConfig::parse();
    let dependencies = build_service_dependencies(&config).await?;
    serve_service(config, dependencies, None).await?;

    telemetry.shutdown();
    Ok(())
}
```

> The `None` third arg to `serve_service` is the object-store override slot; the S3 store is built inside `build_service_dependencies` from `config.object_store_url` (`build_configured_object_store`, `crates/observability/src/lib.rs:1667`, uses `parse_url_opts` for non-`file` schemes), so MinIO works with no extra wiring. `serve_service` blocks until shutdown; `telemetry.shutdown()` runs on graceful exit.

- [ ] **Step 3: Verify build (both modes) + `--help`**

Run: `cargo build -p crabka-observability` then `cargo build -p crabka-observability --features heap-profiling`
Run: `cargo run -p crabka-observability -- --help`
Expected: both build; help lists `--target`, `--listen-addr`, `--object-store-url`, `--wal-bootstrap-server`, etc.

- [ ] **Step 4: Smoke-run the querier + profiling endpoint**

Run: `cargo run -p crabka-observability -- --target querier --listen-addr 127.0.0.1:3100 &` ; `sleep 2` ; `curl -s -H "X-Scope-OrgID: demo" http://127.0.0.1:3100/loki/api/v1/labels` ; `curl -s "http://127.0.0.1:9404/debug/pprof/profile?seconds=1" -o /tmp/logs-cpu.pb && wc -c /tmp/logs-cpu.pb` ; `kill %1` ; `rm -f /tmp/logs-cpu.pb`
Expected: Loki labels JSON + a non-empty pprof blob.

- [ ] **Step 5: Commit**

```bash
cargo +nightly fmt -p crabka-observability
git add crates/observability/
git commit -m "feat(observability): self-instrument the crabka-observability logs binary (OTLP + pprof)"
```

---

### Task 5: Orders-analytics demo app (`crates/observability-demo-app`)

**Files:**
- Create: `crates/observability-demo-app/Cargo.toml`
- Create: `crates/observability-demo-app/build.rs`
- Create: `crates/observability-demo-app/proto/order.proto`
- Create: `crates/observability-demo-app/src/lib.rs` (Order re-export, generator, topology helper, FILE_DESCRIPTOR_SET_BYTES)
- Create: `crates/observability-demo-app/src/main.rs` (roles: `produce`, `stream`, `consume`)
- Test: `crates/observability-demo-app/src/lib.rs` (`#[cfg(test)]`)

**Interfaces:**
- Consumes: `crabka_client_streams::{StreamsApp, StreamsBuilder, DefaultSerde, SchemaSerde, StringSerde, TopologyTestDriver, Consumed}`, `crabka_schema_serde::format::protobuf::ProtobufSerde`, `crabka_schema_serde::{SchemaCache, RegistryClient, CacheConfig, set_default_registry}`, `crabka_client_producer::{Producer, ProducerRecord, Acks}`, `crabka_client_consumer::Consumer`, `crabka_telemetry`.
- Produces: a `observability-demo-app` binary with `--role {produce,stream,consume}`, all on `crabka-broker` + the schema registry, instrumented for all four signals. `publish = false`.

- [ ] **Step 1: Cargo.toml (publish=false, proto codegen deps, instrumentation)**

Create `crates/observability-demo-app/Cargo.toml`:

```toml
[package]
name = "observability-demo-app"
publish = false
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Instrumented orders-analytics Kafka-Streams demo app for the Crabka observability demo"

[lints]
workspace = true

[[bin]]
name = "observability-demo-app"
path = "src/main.rs"

[features]
heap-profiling = ["crabka-telemetry/heap-profiling", "dep:tikv-jemallocator"]

[dependencies]
crabka-client-streams = { version = "0.3.8", path = "../client-streams" }
crabka-client-producer = { version = "0.3.8", path = "../client-producer" }
crabka-client-consumer = { version = "0.3.8", path = "../client-consumer" }
crabka-schema-serde = { version = "0.3.8", path = "../schema-serde" }
crabka-telemetry = { version = "0.3.8", path = "../telemetry" }
bytes = { workspace = true }
clap = { workspace = true, features = ["derive", "env"] }
prost = { workspace = true }
prost-reflect = { workspace = true }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "time", "signal"] }
tracing = { workspace = true }
tikv-jemallocator = { version = "0.6", optional = true, features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }

[build-dependencies]
protox = "0.9"
prost-build = { workspace = true }
prost-reflect = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
```

> Confirm `prost-build`, `prost-reflect` exist in `[workspace.dependencies]`; `crabka-schema-registry` uses `prost-reflect` as a workspace dep, and `client-streams` examples use `protox`. If `prost-build` is not a workspace dep, pin `prost-build = "0.14"` here to match `prost = "0.14"`.

- [ ] **Step 2: proto + build.rs codegen (mirrors `examples/gen/regenerate.sh`)**

Create `crates/observability-demo-app/proto/order.proto`:

```protobuf
syntax = "proto3";
package demo;

message Order {
  string order_id = 1;
  string category = 2;
  double amount = 3;
  string currency = 4;
  int64  ts_ms = 5;
}
```

Create `crates/observability-demo-app/build.rs`:

```rust
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=proto/order.proto");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // protox compiles to a FileDescriptorSet without needing protoc installed.
    let fds = protox::compile(["proto/order.proto"], ["proto"]).expect("protox compile");

    let fds_path = out_dir.join("file_descriptor_set.bin");
    std::fs::write(&fds_path, protox::prost::Message::encode_to_vec(&fds)).expect("write fds");

    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(fds.clone())
        .expect("descriptor pool");

    let mut cfg = prost_build::Config::new();
    cfg.out_dir(&out_dir);
    for message in pool.all_messages() {
        let full = message.full_name().to_string();
        cfg.type_attribute(&full, "#[derive(::prost_reflect::ReflectMessage)]")
            .type_attribute(&full, format!("#[prost_reflect(message_name = \"{full}\")]"))
            .type_attribute(
                &full,
                "#[prost_reflect(file_descriptor_set_bytes = \"crate::FILE_DESCRIPTOR_SET_BYTES\")]",
            );
    }
    cfg.compile_fds(fds).expect("prost compile");
}
```

- [ ] **Step 3: Write the failing test (generator + topology shape)**

Create `crates/observability-demo-app/src/lib.rs`:

```rust
//! Orders-analytics demo: a deterministic order generator + the Streams topology
//! shape. The real proto/registry/broker run lives in `main.rs`.

pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

mod order {
    include!(concat!(env!("OUT_DIR"), "/demo.rs"));
}
pub use order::Order;

/// The category keys the generator cycles through.
pub const CATEGORIES: &[&str] = &["books", "electronics", "grocery", "toys", "garden"];

/// Deterministic order for index `i` (no RNG — varied but reproducible).
#[must_use]
pub fn order_at(i: u64) -> Order {
    let category = CATEGORIES[(i as usize) % CATEGORIES.len()];
    // A few anomalous (zero-amount) orders to drive warn logs / error spans.
    let amount = if i % 17 == 0 { 0.0 } else { ((i % 200) as f64) + 0.99 };
    Order {
        order_id: format!("o-{i:010}"),
        category: category.to_string(),
        amount,
        currency: "USD".to_string(),
        ts_ms: 0, // stamped at send time in main.rs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, StringSerde, TopologyTestDriver};

    #[test]
    fn order_at_is_deterministic_and_cycles_categories() {
        assert_eq!(order_at(0).category, "books");
        assert_eq!(order_at(1).category, "electronics");
        assert_eq!(order_at(5).category, "books");
        assert_eq!(order_at(0).order_id, "o-0000000000");
        assert_eq!(order_at(17).amount, 0.0, "every 17th order is anomalous");
    }

    #[test]
    fn count_topology_aggregates_by_category() {
        // Validate the group_by_key -> count -> to_stream -> to chain (the same
        // structure main.rs uses with proto serdes) using registry-free StringSerde.
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["orders"])
            .group_by_key()
            .count("orders-by-category-store")
            .to_stream()
            .to("order-counts");
        let built = b.build("orders-analytics-test").expect("build topology");
        let mut driver = TopologyTestDriver::new(&built).expect("driver");

        for (k, v) in [("books", "a"), ("books", "b"), ("toys", "c")] {
            driver.pipe_input(
                "orders",
                Consumed::with(StringSerde, StringSerde),
                Some(k.to_string()),
                v.to_string(),
                0,
            );
        }
        // read_output pops ONE deserialized record per call:
        //   fn read_output<KS, VS>(&mut self, topic, produced: impl Into<Produced<KS,VS>>)
        //       -> Option<(Option<KS::Target>, VS::Target)>
        // Type params are inferred from the `produced` arg — pass the serdes, not turbofish.
        let mut books_count: i64 = 0;
        while let Some((key, value)) =
            driver.read_output("order-counts", (StringSerde, crabka_client_streams::I64Serde))
        {
            if key.as_deref() == Some("books") {
                books_count = value; // keep the latest emitted count for "books"
            }
        }
        assert_eq!(books_count, 2, "two 'books' orders → count 2");
    }
}
```

> `read_output`'s real signature (`crates/client-streams/src/test_driver.rs:425`): `pub fn read_output<KS, VS>(&mut self, topic: &str, produced: impl Into<Produced<KS, VS>>) -> Option<(Option<KS::Target>, VS::Target)>`. It returns one record per call (loop until `None`); type params are inferred from the serde tuple, so do NOT write a turbofish. `crabka_client_streams::I64Serde` is the value serde for `count`'s `i64` output.

- [ ] **Step 4: Run the test to verify it fails**

Run: `cargo test -p observability-demo-app`
Expected: FAIL to compile (lib not yet complete / proto not generated) or test assertion mismatch — confirm it fails before proceeding.

- [ ] **Step 5: Make the lib test pass**

Resolve any codegen/import issues until both `lib.rs` tests pass. The generated `demo.rs` (from `package demo;`) defines `Order`; `include!` it under `mod order`.

Run: `cargo test -p observability-demo-app`
Expected: PASS.

- [ ] **Step 6: Write `main.rs` (produce / stream / consume roles + instrumentation)**

Create `crates/observability-demo-app/src/main.rs`:

```rust
//! Instrumented orders-analytics demo. Three roles, all on crabka-broker +
//! the schema registry, emitting metrics(logs/traces/profiles) via crabka libs.

#[cfg(feature = "heap-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "heap-profiling")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_client_streams::{DefaultSerde, SchemaSerde};
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache, set_default_registry};
use observability_demo_app::{Order, order_at};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Role {
    Produce,
    Stream,
    Consume,
}

#[derive(Debug, Parser)]
#[command(name = "observability-demo-app")]
struct Cli {
    #[arg(long, value_enum)]
    role: Role,
    #[arg(long, env = "CRABKA_DEMO_BOOTSTRAP", default_value = "127.0.0.1:9092")]
    bootstrap: String,
    #[arg(long, env = "CRABKA_DEMO_REGISTRY", default_value = "http://127.0.0.1:8081")]
    registry: String,
    #[arg(long, default_value = "orders")]
    input_topic: String,
    #[arg(long, default_value = "order-counts")]
    output_topic: String,
    #[arg(long, env = "CRABKA_DEMO_ORDERS_PER_SEC", default_value_t = 50)]
    orders_per_sec: u64,
}

// Proto Order resolves against the process default registry.
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, ProtobufSerde<Order>>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    let telemetry = crabka_telemetry::init(
        crabka_telemetry::OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "demo-app",
            env!("CARGO_PKG_VERSION"),
            "observability-demo-app",
        ),
        "observability_demo_app=info,info",
        "info",
        "observability-demo-app",
    )?;
    crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;

    match cli.role {
        Role::Produce => run_produce(&cli).await?,
        Role::Stream => run_stream(&cli).await?,
        Role::Consume => run_consume(&cli).await?,
    }
    telemetry.shutdown();
    Ok(())
}

#[tracing::instrument(skip(cli))]
async fn run_produce(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cache = SchemaCache::new(RegistryClient::new(cli.registry.clone()), CacheConfig::default());
    set_default_registry(cache.clone());
    let serde: SchemaSerde<Order, ProtobufSerde<Order>> =
        SchemaSerde::new(ProtobufSerde::<Order>::value(&cache));
    // Intern the value subject for the input topic, then resolve ids.
    crabka_client_streams::Serde::prepare(
        &serde,
        &cli.input_topic,
        crabka_client_streams::processor::serde::SerdeRole::Value,
    );
    cache.prewarm().await?;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(cli.bootstrap.clone())
            .acks(Acks::All)
            .build()
            .await?,
    );

    if cli.orders_per_sec == 0 {
        tracing::warn!("CRABKA_DEMO_ORDERS_PER_SEC=0 — producer paused");
        futures_idle().await;
        return Ok(());
    }
    let period = Duration::from_secs_f64(1.0 / cli.orders_per_sec as f64);
    let mut tick = tokio::time::interval(period);
    let mut i: u64 = 0;
    loop {
        tick.tick().await;
        let mut order = order_at(i);
        order.ts_ms = i64::try_from(i).unwrap_or(i64::MAX); // monotonic demo clock
        if order.amount == 0.0 {
            tracing::warn!(order_id = %order.order_id, "anomalous zero-amount order");
        }
        let value = crabka_client_streams::Serde::serialize(&serde, &cli.input_topic, &order);
        producer
            .send(ProducerRecord {
                topic: cli.input_topic.clone(),
                key: Some(bytes::Bytes::from(order.category.clone().into_bytes())),
                value: Some(value),
                ..Default::default()
            })
            .await
            .await??;
        i += 1;
        if i % 100 == 0 {
            tracing::info!(produced = i, "orders produced");
        }
    }
}

async fn run_stream(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = crabka_client_streams::StreamsApp::builder()
        .bootstrap(cli.bootstrap.clone())
        .application_id("orders-analytics")
        .schema_registry(cli.registry.clone())
        .build();
    let topology = app.streams_builder();
    topology
        .stream::<String, Order>([cli.input_topic.as_str()])
        .group_by_key()
        .count("orders-by-category-store")
        .to_stream()
        .to(cli.output_topic.clone());
    tracing::info!("orders-analytics streams app starting");
    let mut streams = app.run(topology).await?;
    // Run until Ctrl-C.
    tokio::signal::ctrl_c().await.ok();
    streams.close().await?;
    Ok(())
}

async fn run_consume(cli: &Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // `Consumer` uses a `bon` builder; `subscribe` is a builder PARAMETER
    // (Vec<String>), and the finisher is `.build().await` (no separate
    // `.subscribe()` call). See crates/client-consumer/src/consumer.rs:206.
    let consumer = crabka_client_consumer::Consumer::builder()
        .bootstrap(cli.bootstrap.clone())
        .group_id("orders-analytics-consumer")
        .subscribe([cli.output_topic.clone()])
        .build()
        .await?;
    loop {
        let records = consumer.poll(Duration::from_millis(500)).await?;
        for record in records {
            tracing::info!(
                topic = %cli.output_topic,
                key = ?record.key,
                "consumed aggregated count"
            );
        }
    }
}

async fn futures_idle() {
    // Park forever (used when production is paused).
    std::future::pending::<()>().await;
}
```

> **Verify these consumer/serde call shapes during implementation** against the real APIs: `Consumer::builder()`/`subscribe`/`poll` (`crates/client-consumer/src/lib.rs`), the `Serde` trait import path for `prepare`/`serialize`, and `SerdeRole` path (`crates/client-streams/src/processor/serde.rs`). The research confirms `ProtobufSerde::<T>::value(&cache)`, `SchemaSerde::new(...)`, `SchemaCache::new`, `RegistryClient::new`, `set_default_registry`, `cache.prewarm()`, and `StreamsApp::builder()...build()` + `app.run(topology)`. Adjust the consumer block to the real `Consumer` API if names differ; its only job is to drive consumer-lag metrics + end-to-end traces.

- [ ] **Step 7: Build the app (both feature modes)**

Run: `cargo build -p observability-demo-app`
Expected: builds.

Run: `cargo build -p observability-demo-app --features heap-profiling`
Expected: builds (jemalloc allocator + heap route compile).

- [ ] **Step 8: Commit**

```bash
cargo +nightly fmt -p observability-demo-app
git add crates/observability-demo-app/
git commit -m "feat(demo): orders-analytics client-streams app (proto + 4-signal instrumentation)"
```

---

### Task 6: Broker self-profiling (jemalloc feature + pprof routes on `:9404`)

**Files:**
- Modify: `crates/broker/Cargo.toml` (`[features]` + optional jemalloc dep)
- Modify: `crates/broker/src/bin/broker.rs` (jemalloc global allocator under feature)
- Modify: `crates/broker/src/metrics_server.rs` (merge pprof routes into the `/metrics` server)

**Interfaces:**
- Consumes: `crabka_telemetry::profiling::pprof_router()`.
- Produces: the broker's `:9404` admin server now serves `/metrics` **and** `/debug/pprof/{profile,heap}`.

The broker already inits `crabka_telemetry` (traces+logs) and serves `/metrics` on `:9404`. This task adds profiles.

- [ ] **Step 1: Add the `heap-profiling` feature + jemalloc dep to `crates/broker/Cargo.toml`**

```toml
[features]
heap-profiling = ["crabka-telemetry/heap-profiling", "dep:tikv-jemallocator"]

[dependencies]
tikv-jemallocator = { version = "0.6", optional = true, features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }
```

- [ ] **Step 2: Add the jemalloc global allocator (feature-gated) to `crates/broker/src/bin/broker.rs`**

At the top of the file (after the `//!` doc comment, before `use`):

```rust
#[cfg(feature = "heap-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "heap-profiling")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
```

- [ ] **Step 3: Merge pprof routes into the metrics server router**

In `crates/broker/src/metrics_server.rs`, the `router` function is:

```rust
pub fn router(registry: SharedRegistry) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .with_state(registry)
}
```

Change it to merge the pprof routes:

```rust
pub fn router(registry: SharedRegistry) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .with_state(registry)
        .merge(crabka_telemetry::profiling::pprof_router())
}
```

Confirm `crabka-telemetry` is a dependency of `crabka-broker` (it is — `crates/broker/Cargo.toml:35`). The broker passes its own `heap-profiling` feature through to telemetry, so the heap route appears on `:9404` when the broker is built with `--features heap-profiling`.

- [ ] **Step 4: Verify build (both modes)**

Run: `cargo build -p crabka-broker`
Run: `cargo build -p crabka-broker --features heap-profiling`
Expected: both build.

- [ ] **Step 5: Smoke-test the endpoints**

Run: `cargo run -p crabka-broker --bin crabka-broker -- --listen-addr 127.0.0.1:9092 --log-dir ./.tmp-broker-data &` ; wait 3s ; `curl -s http://127.0.0.1:9404/metrics | head -1` ; `curl -s "http://127.0.0.1:9404/debug/pprof/profile?seconds=1" -o /tmp/cpu.pb && wc -c /tmp/cpu.pb` ; `kill %1` ; `rm -rf ./.tmp-broker-data /tmp/cpu.pb`
Expected: `/metrics` returns OpenMetrics text; the pprof fetch writes a non-empty file.

- [ ] **Step 6: Commit**

```bash
cargo +nightly fmt -p crabka-broker
git add crates/broker/
git commit -m "feat(broker): expose CPU/heap pprof on the :9404 admin server"
```

---

### Task 7: Service-binary self-instrumentation (telemetry + profiling admin)

**Files:**
- Modify: `crates/metrics/src/bin/crabka-metrics.rs`, `crates/metrics/Cargo.toml`
- Modify: `crates/metrics-service/src/main.rs`, `crates/metrics-service/Cargo.toml`
- Modify: `crates/traces/src/bin/crabka-traces.rs`, `crates/traces/Cargo.toml`
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`, `crates/profiles/Cargo.toml`
- Modify: `crates/schema-registry/src/bin/schema-registry.rs`, `crates/schema-registry/Cargo.toml` (profiling admin only)

(The logs binary `crabka-observability` is instrumented in Task 4, not here.)

**Interfaces:**
- Consumes: `crabka_telemetry::{init, OtlpConfig}`, `crabka_telemetry::profiling::serve_admin_from_env`.
- Produces: each service binary emits OTLP traces + JSON logs (via telemetry) and exposes `/debug/pprof/*` on an admin port (default `0.0.0.0:9404`), so Alloy collects traces/logs/profiles from every Crabka service.

Apply the SAME three changes to each binary `main` (metrics-service, crabka-metrics, crabka-traces, crabka-profiles). `schema-registry` gets only (b) (it keeps its existing logfmt logging; add the profiling admin server).

**(a) Cargo.toml — add the feature + deps** (each crate; skip telemetry dep where already present):

```toml
[features]
heap-profiling = ["crabka-telemetry/heap-profiling", "dep:tikv-jemallocator"]

[dependencies]
crabka-telemetry = { version = "0.3.8", path = "../telemetry" } # add if absent
tikv-jemallocator = { version = "0.6", optional = true, features = ["profiling", "unprefixed_malloc_on_supported_platforms"] }
```

**(b) main top — jemalloc global allocator (feature-gated):**

```rust
#[cfg(feature = "heap-profiling")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "heap-profiling")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
```

**(c) Inside `main`, replace `tracing_subscriber::fmt()...init()` with telemetry init + spawn the admin server.** For example, in `crates/metrics-service/src/main.rs`:

```rust
// BEFORE:
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .try_init()
    .ok();
// AFTER:
let _telemetry = crabka_telemetry::init(
    crabka_telemetry::OtlpConfig::from_env(
        |k| std::env::var(k).ok(),
        "metrics-service",
        env!("CARGO_PKG_VERSION"),
        "crabka-metrics-service",
    ),
    "crabka_metrics_service=info,info",
    "info",
    "crabka-metrics-service",
)?;
crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;
```

Use the matching service name per binary (`crabka-metrics`, `crabka-traces`, `crabka-profiles`). Keep `_telemetry` alive for the process lifetime (bind it in `main`, not a helper).

> **`crabka-metrics` and `crabka-metrics-service` already need a telemetry dep** — add `crabka-telemetry = { version = "0.3.8", path = "../telemetry" }` to all four crates' `[dependencies]` (none currently depend on it; verified).
>
> **`crabka-traces` `main` returns `ExitCode`, not `Result`** (`crates/traces/src/bin/crabka-traces.rs:154`), so `init(...)?` / `serve_admin_from_env(...).await?` cannot go in `main`. Put change (c) at the top of `async fn run(cli: Cli)` (which returns `Result`) instead — bind `let _telemetry = crabka_telemetry::init(...)?;` and `crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;` there, before the `match cli.target`. The jemalloc allocator + `malloc_conf` (b) still go at file top.

- [ ] **Step 1: metrics-service** — apply (a)(b)(c) in `main`. Build: `cargo build -p crabka-metrics-service` and `--features heap-profiling`.
- [ ] **Step 2: crabka-metrics** — apply (a)(b)(c) in `main` (add telemetry dep). Build: `cargo build -p crabka-metrics` and `--features heap-profiling`.
- [ ] **Step 3: crabka-traces** — apply (a) + (b) at file top, and (c) at the top of `run()` (NOT `main`, which returns `ExitCode`). Build: `cargo build -p crabka-traces` and `--features heap-profiling`.
- [ ] **Step 4: crabka-profiles** — apply (a)(b)(c) in `main`. Build: `cargo build -p crabka-profiles` and `--features heap-profiling`.
- [ ] **Step 5: schema-registry** — apply (a)(b) and add `crabka_telemetry::profiling::serve_admin_from_env("0.0.0.0:9404").await?;` to its `main` after the existing logfmt setup (do NOT remove its logfmt logging). Add `crabka-telemetry` dep. Build: `cargo build -p crabka-schema-registry` and `--features heap-profiling`.

- [ ] **Step 6: Workspace clippy gate**

Run: `cargo clippy --workspace --all-targets`
Expected: no errors. Fix any unused-import/dead-code warnings introduced by the swaps.

- [ ] **Step 7: Commit**

```bash
cargo +nightly fmt -p crabka-metrics -p crabka-metrics-service -p crabka-traces -p crabka-profiles -p crabka-schema-registry
git add crates/metrics/ crates/metrics-service/ crates/traces/ crates/profiles/ crates/schema-registry/
git commit -m "feat(observability): self-instrument service binaries (OTLP traces/logs + pprof admin)"
```

---

## Batch C — Fixture & containerization

All files live under `demo/observability/` (new). Author Task 8 first; 9/10/11 can be authored together; Task 12 (smoke) is last.

### Task 8: Single all-binaries Docker image

**Files:**
- Create: `demo/observability/Dockerfile`
- Create: `demo/observability/.dockerignore`

**Interfaces:**
- Produces: an image tag `crabka-demo:latest` containing `crabka-broker`, `crabka-metrics`, `crabka-metrics-service`, `crabka-traces`, `crabka-observability` (the logs binary), `crabka-profiles`, `crabka-schema-registry`, and `observability-demo-app`, all built `--release --features heap-profiling`, with debug symbols retained.

- [ ] **Step 1: Write `demo/observability/.dockerignore`**

```
.git
target
.claude
**/target
docs
website
bench
```

- [ ] **Step 2: Write `demo/observability/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
# Single image with every Crabka binary the demo needs, built with heap
# profiling and debug symbols (for readable flamegraphs).
FROM rust:1.96-bookworm AS build
WORKDIR /src
# jemalloc build needs a C toolchain (already in bookworm) + make.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
# Keep debug symbols in release for symbolized CPU/heap profiles.
ENV CARGO_PROFILE_RELEASE_DEBUG=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --features heap-profiling \
      -p crabka-broker \
      -p crabka-metrics -p crabka-metrics-service \
      -p crabka-traces \
      -p crabka-observability \
      -p crabka-profiles \
      -p crabka-schema-registry \
      -p observability-demo-app && \
    mkdir -p /out && \
    for b in crabka-broker crabka-metrics crabka-metrics-service crabka-traces \
             crabka-observability crabka-profiles crabka-schema-registry observability-demo-app; do \
      cp "target/release/$b" /out/; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/* /usr/local/bin/
# jemalloc heap profiling is enabled at link time via the malloc_conf export.
WORKDIR /data
```

> **Cargo feature note:** `--features heap-profiling` with multiple `-p` packages requires each package to define a `heap-profiling` feature (Tasks 2, 5, 6, 7 add them). If cargo rejects `--features` across packages that lack it, build per-package in a loop instead, or add `heap-profiling` to every listed crate. Confirm during implementation; the per-package loop is the safe fallback.

- [ ] **Step 3: Build the image**

Run: `docker build -f demo/observability/Dockerfile -t crabka-demo:latest .`
Expected: image builds; `docker run --rm crabka-demo:latest crabka-broker --help` prints help.

- [ ] **Step 4: Commit**

```bash
git add demo/observability/Dockerfile demo/observability/.dockerignore
git commit -m "feat(demo): single all-binaries Docker image (heap-profiling, debug symbols)"
```

---

### Task 9: `docker-compose.yml`

**Files:**
- Create: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: the `crabka-demo:latest` image (Task 8); Alloy config (Task 10); Grafana provisioning (Task 11); MinIO bootstrap (Task 12).
- Produces: the full stack. Service DNS names: `broker`, `schema-registry`, `minio`, `metrics-distributor`, `metrics-compactor`, `metrics-querier`, `traces-distributor`, `traces-block-builder`, `traces-querier`, `logs-distributor`, `logs-compactor`, `logs-querier`, `profiles-distributor`, `profiles-block-builder`, `profiles-querier`, `alloy`, `grafana`, `demo-produce`, `demo-stream`, `demo-consume`.

Shared env anchor for OTLP + S3 + admin (YAML anchors keep it DRY):

- [ ] **Step 1: Write `demo/observability/docker-compose.yml`**

```yaml
name: crabka-observability-demo

x-s3-env: &s3-env
  AWS_ACCESS_KEY_ID: minioadmin
  AWS_SECRET_ACCESS_KEY: minioadmin
  AWS_ENDPOINT_URL: http://minio:9000
  AWS_ALLOW_HTTP: "true"
  AWS_REGION: us-east-1

x-otlp-env: &otlp-env
  CRABKA_OTLP_ENDPOINT: http://alloy:4317
  CRABKA_ADMIN_LISTEN_ADDR: 0.0.0.0:9404

x-crabka-image: &crabka-image
  image: crabka-demo:latest
  restart: unless-stopped

services:
  broker:
    <<: *crabka-image
    command: ["crabka-broker", "--listen-addr=0.0.0.0:9092", "--advertised-listener=broker:9092", "--log-dir=/data", "--process-roles=controller,broker"]
    environment:
      <<: *otlp-env
      CRABKA_METRICS_LISTEN_ADDR: 0.0.0.0:9404
    ports: ["9092:9092", "9404:9404"]
    volumes: ["broker-data:/data"]
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:9404/metrics"]
      interval: 5s
      timeout: 3s
      retries: 30

  minio:
    image: minio/minio:latest
    command: ["server", "/data", "--console-address", ":9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports: ["9000:9000", "9001:9001"]
    volumes: ["minio-data:/data"]
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 30

  minio-setup:
    image: minio/mc:latest
    depends_on:
      minio: { condition: service_healthy }
    entrypoint: ["/bin/sh", "/bootstrap.sh"]
    volumes: ["./minio/bootstrap.sh:/bootstrap.sh:ro"]

  schema-registry:
    <<: *crabka-image
    command: ["crabka-schema-registry", "--bootstrap-servers=broker:9092", "--listen-addr=0.0.0.0:8081", "--schemas-topic-rf=1"]
    environment: { <<: *otlp-env }
    depends_on:
      broker: { condition: service_healthy }
    ports: ["8081:8081"]

  # ---- METRICS (Prometheus/Mimir) ----
  metrics-distributor:
    # crabka-metrics uses --bootstrap (not --wal-bootstrap); the distributor
    # writes to the WAL (Kafka), not the object store, so no --object-store-url.
    <<: *crabka-image
    command: ["crabka-metrics", "--target=distributor", "--listen=0.0.0.0:4041", "--bootstrap=broker:9092"]
    environment: { <<: *otlp-env }
    depends_on:
      broker: { condition: service_healthy }

  metrics-compactor:
    <<: *crabka-image
    command: ["crabka-metrics", "--target=compactor", "--object-store-url=s3://crabka-blocks/metrics", "--bootstrap=broker:9092"]
    environment: { <<: [*s3-env, *otlp-env] }
    depends_on:
      minio-setup: { condition: service_completed_successfully }
      broker: { condition: service_healthy }

  metrics-querier:
    # crabka-metrics-service uses --wal-bootstrap (its own flag name).
    <<: *crabka-image
    command: ["crabka-metrics-service", "--target=querier", "--listen=0.0.0.0:9090", "--object-store-url=s3://crabka-blocks/metrics", "--manifest-prefix=metrics", "--wal-bootstrap=broker:9092"]
    environment: { <<: [*s3-env, *otlp-env] }
    ports: ["9090:9090"]
    depends_on:
      minio-setup: { condition: service_completed_successfully }
      broker: { condition: service_healthy }

  # ---- TRACES (Tempo) ----
  traces-distributor:
    <<: *crabka-image
    command: ["crabka-traces", "--target=distributor", "--bootstrap=broker:9092", "--listen=0.0.0.0:3200", "--grpc-listen=0.0.0.0:4317", "--otlp-http-listen=0.0.0.0:4318"]
    environment: { <<: *otlp-env }
    depends_on:
      broker: { condition: service_healthy }

  traces-block-builder:
    <<: *crabka-image
    command: ["crabka-traces", "--target=block-builder", "--bootstrap=broker:9092", "--object-store-url=s3://crabka-blocks/traces"]
    environment: { <<: [*s3-env, *otlp-env] }
    depends_on:
      minio-setup: { condition: service_completed_successfully }
      broker: { condition: service_healthy }

  traces-querier:
    <<: *crabka-image
    command: ["crabka-traces", "--target=querier", "--listen=0.0.0.0:3200", "--object-store-url=s3://crabka-blocks/traces"]
    environment: { <<: [*s3-env, *otlp-env] }
    ports: ["3200:3200"]
    depends_on:
      minio-setup: { condition: service_completed_successfully }

  # ---- LOGS (Loki) — binary is `crabka-observability` (the logs service) ----
  logs-distributor:
    <<: *crabka-image
    command: ["crabka-observability", "--target=distributor", "--listen-addr=0.0.0.0:3100", "--wal-bootstrap-server=broker:9092", "--object-store-url=s3://crabka-blocks/logs"]
    environment: { <<: [*s3-env, *otlp-env] }
    depends_on:
      broker: { condition: service_healthy }
      minio-setup: { condition: service_completed_successfully }

  logs-compactor:
    <<: *crabka-image
    command: ["crabka-observability", "--target=compactor", "--wal-bootstrap-server=broker:9092", "--object-store-url=s3://crabka-blocks/logs", "--index-prefix=logs"]
    environment: { <<: [*s3-env, *otlp-env] }
    depends_on:
      broker: { condition: service_healthy }
      minio-setup: { condition: service_completed_successfully }

  logs-querier:
    <<: *crabka-image
    command: ["crabka-observability", "--target=querier", "--listen-addr=0.0.0.0:3100", "--object-store-url=s3://crabka-blocks/logs", "--index-prefix=logs"]
    environment: { <<: [*s3-env, *otlp-env] }
    ports: ["3100:3100"]
    depends_on:
      minio-setup: { condition: service_completed_successfully }

  # ---- PROFILES (Pyroscope) ----
  profiles-distributor:
    <<: *crabka-image
    command: ["crabka-profiles", "--target=distributor", "--listen=0.0.0.0:4040", "--bootstrap=broker:9092"]
    environment: { <<: *otlp-env }
    depends_on:
      broker: { condition: service_healthy }

  profiles-block-builder:
    <<: *crabka-image
    command: ["crabka-profiles", "--target=block-builder", "--bootstrap=broker:9092", "--object-store-url=s3://crabka-blocks/profiles"]
    environment: { <<: [*s3-env, *otlp-env] }
    depends_on:
      minio-setup: { condition: service_completed_successfully }
      broker: { condition: service_healthy }

  profiles-querier:
    <<: *crabka-image
    command: ["crabka-profiles", "--target=querier", "--listen=0.0.0.0:4040", "--object-store-url=s3://crabka-blocks/profiles"]
    environment: { <<: [*s3-env, *otlp-env] }
    ports: ["4040:4040"]
    depends_on:
      minio-setup: { condition: service_completed_successfully }

  # ---- COLLECTOR + GRAFANA ----
  alloy:
    image: grafana/alloy:v1.5.1
    command: ["run", "--server.http.listen-addr=0.0.0.0:12345", "/etc/alloy/config.alloy"]
    volumes:
      - "./alloy/config.alloy:/etc/alloy/config.alloy:ro"
      - "/var/run/docker.sock:/var/run/docker.sock:ro"
    ports: ["12345:12345"]
    depends_on:
      broker: { condition: service_healthy }

  grafana:
    image: grafana/grafana:11.4.0
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_FEATURE_TOGGLES_ENABLE: "flameGraph traceqlEditor"
    ports: ["3000:3000"]
    volumes:
      - "./grafana/provisioning:/etc/grafana/provisioning:ro"
    depends_on:
      - metrics-querier
      - traces-querier
      - logs-querier
      - profiles-querier

  # ---- DEMO APP ----
  demo-produce:
    <<: *crabka-image
    command: ["observability-demo-app", "--role=produce"]
    environment:
      <<: *otlp-env
      CRABKA_DEMO_BOOTSTRAP: broker:9092
      CRABKA_DEMO_REGISTRY: http://schema-registry:8081
      CRABKA_DEMO_ORDERS_PER_SEC: "50"
    depends_on:
      broker: { condition: service_healthy }
      schema-registry: { condition: service_started }

  demo-stream:
    <<: *crabka-image
    command: ["observability-demo-app", "--role=stream"]
    environment:
      <<: *otlp-env
      CRABKA_DEMO_BOOTSTRAP: broker:9092
      CRABKA_DEMO_REGISTRY: http://schema-registry:8081
    depends_on:
      broker: { condition: service_healthy }
      schema-registry: { condition: service_started }

  demo-consume:
    <<: *crabka-image
    command: ["observability-demo-app", "--role=consume"]
    environment:
      <<: *otlp-env
      CRABKA_DEMO_BOOTSTRAP: broker:9092
      CRABKA_DEMO_REGISTRY: http://schema-registry:8081
    depends_on:
      broker: { condition: service_healthy }

volumes:
  broker-data:
  minio-data:
```

> **Flags verified against the binaries:** `crabka-metrics` uses `--bootstrap` + `--listen` (the distributor writes to the WAL only — no `--object-store-url`; the compactor takes `--object-store-url`); `crabka-metrics-service` uses `--listen` + `--wal-bootstrap` + `--object-store-url` + `--manifest-prefix` and serves the Prometheus API on its `--listen` port (`9090` here); the logs binary is `crabka-observability` with `--listen-addr`/`--wal-bootstrap-server`/`--object-store-url`/`--index-prefix`; `crabka-traces`/`crabka-profiles` take `--bootstrap` + `--listen` + `--object-store-url`. The metrics remote-write path (`/api/v1/push`, Task 10) is registered by the distributor (`crates/metrics/src/distributor/mod.rs:418`, which also serves `/api/v1/write`).

- [ ] **Step 2: Validate compose syntax**

Run: `docker compose -f demo/observability/docker-compose.yml config >/dev/null && echo OK`
Expected: `OK` (no YAML/anchor errors). (Full `up` is exercised in Task 12.)

- [ ] **Step 3: Commit**

```bash
git add demo/observability/docker-compose.yml
git commit -m "feat(demo): docker-compose stack (broker, 4 backends, minio, schema-registry, alloy, grafana, demo app)"
```

---

### Task 10: Grafana Alloy collector config

**Files:**
- Create: `demo/observability/alloy/config.alloy`

**Interfaces:**
- Consumes: scrape/collect from every Crabka process `:9404` admin port + the demo app, the broker `:9404` `/metrics`, container stdout logs (Docker socket), and OTLP pushed by Crabka processes.
- Produces: writes metrics → `metrics-distributor`, traces → `traces-distributor`, logs → `logs-distributor`, profiles → `profiles-distributor`.

- [ ] **Step 1: Write `demo/observability/alloy/config.alloy`**

```alloy
// ---------------- OTLP receive (traces + logs from Crabka processes) ----------------
otelcol.receiver.otlp "in" {
  grpc { endpoint = "0.0.0.0:4317" }
  http { endpoint = "0.0.0.0:4318" }
  output {
    traces = [otelcol.exporter.otlphttp.traces.input]
    logs   = [otelcol.exporter.otlphttp.logs.input]
  }
}

otelcol.exporter.otlphttp "traces" {
  client {
    endpoint = "http://traces-distributor:4318"
    headers  = { "X-Scope-OrgID" = "demo" }
    tls { insecure = true }
  }
}

// Crabka also self-ships logs to stdout (JSON); the OTLP logs path is optional.
otelcol.exporter.otlphttp "logs" {
  client {
    endpoint = "http://logs-distributor:3100/otlp"
    headers  = { "X-Scope-OrgID" = "demo" }
    tls { insecure = true }
  }
}

// ---------------- Metrics: scrape every Crabka :9404 + demo app ----------------
prometheus.scrape "crabka" {
  targets = [
    { __address__ = "broker:9404",               job = "broker" },
    { __address__ = "metrics-distributor:9404",  job = "metrics-distributor" },
    { __address__ = "metrics-querier:9404",      job = "metrics-querier" },
    { __address__ = "traces-distributor:9404",   job = "traces-distributor" },
    { __address__ = "logs-distributor:9404",     job = "logs-distributor" },
    { __address__ = "profiles-distributor:9404", job = "profiles-distributor" },
    { __address__ = "demo-produce:9404",         job = "demo-produce" },
    { __address__ = "demo-stream:9404",          job = "demo-stream" },
  ]
  scrape_interval = "15s"
  forward_to = [prometheus.remote_write.crabka.receiver]
}

prometheus.remote_write "crabka" {
  endpoint {
    url     = "http://metrics-distributor:4041/api/v1/push"
    headers = { "X-Scope-OrgID" = "demo" }
  }
}

// ---------------- Logs: tail container stdout → Loki push ----------------
discovery.docker "containers" {
  host = "unix:///var/run/docker.sock"
}

loki.source.docker "containers" {
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.docker.containers.targets
  forward_to = [loki.write.crabka.receiver]
}

loki.write "crabka" {
  endpoint {
    url     = "http://logs-distributor:3100/loki/api/v1/push"
    headers = { "X-Scope-OrgID" = "demo" }
  }
}

// ---------------- Profiles: scrape every Crabka :9404 pprof → Pyroscope ----------------
pyroscope.scrape "crabka" {
  targets = [
    { __address__ = "broker:9404",               service_name = "broker" },
    { __address__ = "metrics-distributor:9404",  service_name = "metrics-distributor" },
    { __address__ = "traces-distributor:9404",   service_name = "traces-distributor" },
    { __address__ = "logs-distributor:9404",     service_name = "logs-distributor" },
    { __address__ = "profiles-distributor:9404", service_name = "profiles-distributor" },
    { __address__ = "schema-registry:9404",      service_name = "schema-registry" },
    { __address__ = "demo-produce:9404",         service_name = "demo-produce" },
    { __address__ = "demo-stream:9404",          service_name = "demo-stream" },
  ]
  profiling_config {
    profile.process_cpu { enabled = true }   // GET /debug/pprof/profile
    profile.memory      { enabled = true }   // GET /debug/pprof/heap
  }
  forward_to = [pyroscope.write.crabka.receiver]
}

pyroscope.write "crabka" {
  endpoint {
    url     = "http://profiles-distributor:4040"
    headers = { "X-Scope-OrgID" = "demo" }
  }
}
```

> **Confirm during implementation (Alloy is external; syntax is version-pinned to `grafana/alloy:v1.5.1`):**
> 1. The metrics remote-write path — `crabka-metrics` distributor may serve `/api/v1/push` (Mimir) or `/api/v1/write` (Prometheus). The golden `grafana_e2e` test pushes to `/api/v1/write`; the first survey said `/api/v1/push`. Read `crates/metrics/src/distributor` route registration and set the real path.
> 2. `pyroscope.scrape`'s `profile.process_cpu`/`profile.memory` default endpoints are `/debug/pprof/profile` and `/debug/pprof/heap` — matches Task 2's routes. Verify against the pinned Alloy version's reference and adjust block names if needed.
> 3. The logs OTLP endpoint (`/otlp`) is optional; Crabka's primary log path is stdout→`loki.source.docker`→`loki.write`. If the logs distributor has no OTLP route, drop the `otelcol.exporter.otlphttp.logs` block and route OTLP `logs` output to nothing.

- [ ] **Step 2: Validate Alloy config syntax**

Run: `docker run --rm -v "$(pwd)/demo/observability/alloy/config.alloy:/c.alloy:ro" grafana/alloy:v1.5.1 fmt /c.alloy >/dev/null && echo OK`
Expected: `OK` (Alloy parses/formats the file).

- [ ] **Step 3: Commit**

```bash
git add demo/observability/alloy/config.alloy
git commit -m "feat(demo): Alloy config collecting all four signals from both sources"
```

---

### Task 11: Grafana datasource + dashboard provisioning

**Files:**
- Create: `demo/observability/grafana/provisioning/datasources/crabka.yaml`
- Create: `demo/observability/grafana/provisioning/dashboards/dashboards.yaml`
- Create: `demo/observability/grafana/provisioning/dashboards/crabka-self.json`

**Interfaces:**
- Consumes: the four querier services (Task 9).
- Produces: four provisioned datasources (Prometheus/Tempo/Loki/Pyroscope) + a starter dashboard. Datasource shapes are copied from the golden integration tests.

- [ ] **Step 1: Write `demo/observability/grafana/provisioning/datasources/crabka.yaml`**

```yaml
apiVersion: 1
datasources:
  - name: Crabka Metrics
    uid: crabka-prom
    type: prometheus
    access: proxy
    url: http://metrics-querier:9090
    isDefault: true
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: demo
    editable: false
  - name: Crabka Traces
    uid: crabka-tempo
    type: tempo
    access: proxy
    url: http://traces-querier:3200
    jsonData:
      httpMethod: GET
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: demo
    editable: false
  - name: Crabka Logs
    uid: crabka-loki
    type: loki
    access: proxy
    url: http://logs-querier:3100
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: demo
    editable: false
  - name: Crabka Profiles
    uid: crabka-pyroscope
    type: grafana-pyroscope-datasource
    access: proxy
    url: http://profiles-querier:4040
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: demo
    editable: false
```

(Datasource `type` strings and the `X-Scope-OrgID` header wiring are verbatim from the golden tests: Prometheus `crates/metrics-service/tests/grafana_integration.rs:68`, Tempo `crates/traces/tests/grafana_e2e.rs:1067`, Loki `crates/integration-tests/tests/grafana_e2e.rs:325`, Pyroscope `crates/profiles/tests/pyroscope_differential.rs:159`.)

- [ ] **Step 2: Write the dashboard provider `demo/observability/grafana/provisioning/dashboards/dashboards.yaml`**

```yaml
apiVersion: 1
providers:
  - name: crabka
    orgId: 1
    folder: Crabka
    type: file
    disableDeletion: false
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
      foldersFromFilesStructure: false
```

- [ ] **Step 3: Write a starter dashboard `demo/observability/grafana/provisioning/dashboards/crabka-self.json`**

A minimal but valid dashboard with one panel per signal (Explore is the primary tool; this proves provisioning works):

```json
{
  "uid": "crabka-self",
  "title": "Crabka observes Crabka",
  "schemaVersion": 39,
  "version": 1,
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1, "type": "timeseries", "title": "Broker — scraped series count",
      "datasource": { "type": "prometheus", "uid": "crabka-prom" },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [ { "refId": "A", "expr": "count({job=\"broker\"})" } ]
    },
    {
      "id": 2, "type": "logs", "title": "Crabka logs",
      "datasource": { "type": "loki", "uid": "crabka-loki" },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "targets": [ { "refId": "A", "expr": "{service_name=~\".+\"}" } ]
    },
    {
      "id": 3, "type": "traces", "title": "Recent traces",
      "datasource": { "type": "tempo", "uid": "crabka-tempo" },
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [ { "refId": "A", "queryType": "traceql", "query": "{}" } ]
    },
    {
      "id": 4, "type": "flamegraph", "title": "Broker CPU profile",
      "datasource": { "type": "grafana-pyroscope-datasource", "uid": "crabka-pyroscope" },
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "targets": [ { "refId": "A", "profileTypeId": "process_cpu:cpu:nanoseconds:cpu:nanoseconds", "labelSelector": "{service_name=\"broker\"}" } ]
    }
  ]
}
```

> The flamegraph `profileTypeId` and log/trace label selectors are best-effort; Explore is the canonical way to browse. Tune panel queries after the stack is up (Task 12). Keep the JSON valid (Grafana skips invalid dashboards silently).

- [ ] **Step 4: Validate JSON**

Run: `python -c "import json; json.load(open('demo/observability/grafana/provisioning/dashboards/crabka-self.json')); print('OK')"`
Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add demo/observability/grafana/
git commit -m "feat(demo): Grafana datasource + dashboard provisioning for all four signals"
```

---

### Task 12: MinIO bootstrap, README, and end-to-end smoke verification

**Files:**
- Create: `demo/observability/minio/bootstrap.sh`
- Create: `demo/observability/README.md`

**Interfaces:**
- Consumes: everything above.
- Produces: a working `docker compose up` and a documented manual smoke check.

- [ ] **Step 1: Write `demo/observability/minio/bootstrap.sh`**

```sh
#!/bin/sh
set -eu
# Create the shared blocks bucket used by all four backends.
mc alias set local "${AWS_ENDPOINT_URL:-http://minio:9000}" \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"
mc mb --ignore-existing local/crabka-blocks
echo "minio bootstrap: crabka-blocks ready"
```

(The `minio-setup` service mounts and runs this; `AWS_ENDPOINT_URL`/creds come from the compose env or defaults.)

- [ ] **Step 2: Write `demo/observability/README.md`**

````markdown
# Crabka full-signal observability demo

One `docker compose up` brings up Grafana over Crabka's four observability
backends (metrics, traces, logs, profiles). Crabka exports all four of its own
signals into those backends, and an instrumented `crabka-client-streams` orders
pipeline runs its Kafka traffic on Crabka.

## Run

```bash
cd demo/observability
docker compose up --build      # first run builds the crabka-demo image (~10 min)
```

Then open Grafana at <http://localhost:3000> (anonymous admin).

Tune the load with `CRABKA_DEMO_ORDERS_PER_SEC` on the `demo-produce` service
(default 50; `0` pauses). Lower it on a constrained host. Plan on **≥ 8 GB**
of Docker memory (~20 containers).

## What you should see

- **Explore → Crabka Metrics** (Prometheus): `{job="broker"}` — the broker's own metrics.
- **Explore → Crabka Logs** (Loki): `{service_name="broker"}` and `{service_name="demo-produce"}` — JSON logs.
- **Explore → Crabka Traces** (Tempo): TraceQL `{}` — broker + demo-app spans.
- **Explore → Crabka Profiles** (Pyroscope): service `broker` / `demo-stream` — CPU + heap flamegraphs.
- The **“Crabka observes Crabka”** dashboard (folder *Crabka*) shows one panel per signal.

## Smoke check (all four signals, both sources)

```bash
# metrics
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:9090/api/v1/query?query=up' | head -c 200
# logs
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:3100/loki/api/v1/labels'
# traces (TraceQL search)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:3200/api/search?q=%7B%7D' | head -c 200
# profiles (Pyroscope label names)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:4040/querier.v1.QuerierService/LabelNames' -X POST -H 'content-type: application/json' -d '{}' | head -c 200
```

## Layout

- `docker-compose.yml` — the stack
- `Dockerfile` — single image with every Crabka binary + the demo app
- `alloy/config.alloy` — Alloy collects all four signals from both sources
- `grafana/provisioning/` — datasources + starter dashboard
- `minio/bootstrap.sh` — creates the `crabka-blocks` bucket
````

(The querier host ports the smoke check needs — `metrics-querier:9090`, `traces-querier:3200`, `logs-querier:3100`, `profiles-querier:4040` — are already published in the Task 9 `docker-compose.yml`.)

- [ ] **Step 3: Bring the stack up**

Run: `cd demo/observability && docker compose up --build -d`
Expected: image builds; all containers start; `docker compose ps` shows `broker`, `minio`, backends, `alloy`, `grafana`, and the three `demo-*` services healthy/running. Allow a few minutes for the broker→backends→demo ordering.

- [ ] **Step 4: Run the smoke check**

Run the four `curl` commands from the README.
Expected: metrics returns a JSON `data.result`; logs returns label names; traces search returns JSON; profiles returns label names. Each confirms a live querier serving Crabka-originated data.

- [ ] **Step 5: Verify in Grafana (manual)**

Open <http://localhost:3000>, then for each datasource run the Explore query from the README and confirm data appears for both `broker` (Crabka self) and `demo-*` (the app). Confirm the “Crabka observes Crabka” dashboard renders.

- [ ] **Step 6: Tear down and commit**

```bash
cd demo/observability && docker compose down -v
git add demo/observability/minio/bootstrap.sh demo/observability/README.md demo/observability/docker-compose.yml
git commit -m "feat(demo): MinIO bootstrap, README, and querier host ports; smoke-verified"
```

---

## Self-Review checklist (run by the implementer before declaring done)

- [ ] Every Crabka process exposes `/debug/pprof/profile` (+`/heap` in the demo image) on `:9404`, and Alloy `pyroscope.scrape` collects them.
- [ ] All four backends point at `s3://crabka-blocks/<signal>` and start without local-FS fallbacks.
- [ ] `crabka-observability` (logs) serves the Loki API on `:3100`; `metrics-querier` serves the Prometheus API on `:9090`; `traces-querier` Tempo on `:3200`; `profiles-querier` Pyroscope on `:4040`.
- [ ] The demo app produces proto orders (registry-framed), the stream app aggregates, the consumer reads — visible as traces with spans across `demo-produce`/`demo-stream`/`demo-consume`.
- [ ] `cargo clippy --workspace --all-targets` is clean; `cargo build --release --features heap-profiling` succeeds for every binary in the image.
- [ ] No crate that should be private is publishable: demo app + metrics/metrics-service/promql/logql/observability-spike are `publish = false`.
- [ ] The three "Confirm during implementation" notes (metrics remote-write path; metrics CLI flag names; Alloy `pyroscope.scrape`/Loki-OTLP syntax) are resolved against the real code/version.
