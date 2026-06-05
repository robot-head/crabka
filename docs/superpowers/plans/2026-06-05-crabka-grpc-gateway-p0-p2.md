# Crabka gRPC Gateway — P0–P2 Implementation Plan (skeleton → send/consume cores → single-owner EOS dedup)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `crabka-grpc-gateway` crate and implement, with full tests, the produce/consume/codec core engines, a unary `Send` Connect-RPC endpoint, and single-owner exactly-once deduplication — a working gateway that produces to and consumes from real Kafka topics with EOS dedup on a single instance.

**Architecture:** A standalone binary built **only** on Crabka's native client crates (`crabka-client-core/-producer/-consumer/-admin`) — the broker is never modified. Thin Connect-RPC handlers adapt proto ⇄ plain-Rust core engines (`ProduceCore`, consume core, `RecordCodec`). Dedup uses a compacted internal claim topic `__crabka_grpc_dedup`; on a miss the owner writes the data record + claim in **one Kafka transaction** (per-dedup-partition `transactional.id`), and a `read_committed` materialized map provides crash-recovery warm-up.

**Tech Stack:** Rust 2024, `connectrpc-axum` 0.1 + `axum` 0.8 + `prost` 0.14 (mirroring `crates/rebalancer`), `tokio`, `dashmap`, `bytes`, `serde_json`. Tests use the in-process broker harness (`crabka-broker` `test-helpers`) + `assert2`; JVM differential via Docker `cp-kafka` (`#[ignore]`).

---

## Scope

**In this plan (P0–P2):**
- P0 — crate skeleton, `gateway.proto` (unary `Send` only), `build.rs`, config, error/types, axum server + health/readiness.
- P1 — `RecordCodec`/`RawCodec`; `ProduceCore` (keyed→dedup, unkeyed→plain idempotent, `acks=all`); unary `Send` Connect handler; consume core (group subscribe + commit) as a tested library component.
- P2 — single-owner EOS dedup: ensure compacted `__crabka_grpc_dedup`; `DedupStore` (`read_committed` materialized map + warm-up); `DedupEngine` (per-partition transactional record+claim write); readiness gating.

**Explicitly deferred to later plans (do NOT attempt here):**
- Streaming wire: `SendStream` (client-streaming) + `Subscribe` (bidi). **Blocked on** verifying `connectrpc-axum` 0.1.1 streaming support — see Risks. The consume core built here is the foundation for it.
- P3 active-active ownership sharding (ownership consumer-group, `__crabka_grpc_gateway_membership` routing topic, gateway→gateway forwarding, per-partition rebalance warm-up, cross-instance fencing).
- P4–P9 (TLS/mTLS, identity→ACL, webhook in/out, telemetry, operator). Leave the `RecordCodec` seam for the Schema Registry component; do not wire it.

**Spec:** `docs/superpowers/specs/2026-06-04-crabka-grpc-gateway-design.md`.

## File structure (created/modified in this plan)

```
crates/grpc-gateway/
  Cargo.toml                       # T1  workspace-lint crate; native-client + connectrpc deps
  build.rs                         # T2  connectrpc_axum_build, system-protoc + fetch fallback
  proto/crabka/gateway/v1/gateway.proto  # T2  unary Send only (this plan)
  src/
    lib.rs                         # T1/T2/T5  pb module, re-exports, router()
    bin/gateway.rs                 # T5  main: args, build clients, ensure dedup topic, serve
    config.rs                      # T3  GatewayConfig + clap Args
    error.rs                       # T4  GatewayError (thiserror)
    types.rs                       # T4  GatewayRecord, RecordOutcome
    codec.rs                       # T6  RecordCodec trait + RawCodec
    state.rs                       # T7  AppState (producer, codec, dedup, config)
    produce.rs                     # T7  ProduceCore
    handlers.rs                    # T8  unary Send Connect handler (proto ⇄ core)
    consume.rs                     # T9  consume core (group subscribe + commit)
    health.rs                      # T5  /healthz /readyz
    dedup/
      mod.rs                       # T12 DedupEngine + ClaimValue + key hashing
      store.rs                     # T11 DedupStore (read_committed materialized map)
      topic.rs                     # T10 ensure compacted dedup topic
  tests/
    unit_basics.rs                 # T6/T12 hashing, codec, ClaimValue serde (no broker)
    integration_send.rs            # T7  produce→read-back via native consumer (in-process broker)
    integration_consume.rs         # T9  produce then consume-core subscribe+commit
    integration_dedup.rs           # T12 sequential/concurrent dup + restart warm-up dedup
    jvm_differential.rs            # T8  #[ignore] Docker: JVM console-consumer reads gateway output
```

## Batch plan (parallel subagent execution per CLAUDE.md)

Dispatch tasks **within a batch** concurrently only where file sets are disjoint; finish a batch, review, then start the next.

- **Batch A — P0.** T1 (create crate) must land first. Then T2 / T3 / T4 are **file-disjoint** → run concurrently. Then T5.
- **Batch B — P1.** T6 (`codec.rs`) ∥ T9 (`consume.rs`) are disjoint and can run with T7 (`produce.rs`+`state.rs`). T8 (`handlers.rs`) depends on T7 + proto → after T7.
- **Batch C — P2.** T10 (`dedup/topic.rs`) ∥ T11 (`dedup/store.rs`) disjoint → concurrent. T12 (`dedup/mod.rs` + wire into `produce.rs`/`state.rs`) after T11. Then readiness gating (folded into T12/T5).

Per-task **Files** lists make the disjoint sets explicit.

---

## Batch A — P0: skeleton

### Task 1: Create the crate

**Files:**
- Create: `crates/grpc-gateway/Cargo.toml`
- Create: `crates/grpc-gateway/src/lib.rs`
- Create: `crates/grpc-gateway/src/bin/gateway.rs`

The workspace `Cargo.toml` uses `members = ["crates/*"]`, so creating the directory auto-registers the crate — do **not** edit the root manifest.

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "crabka-grpc-gateway"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "gRPC / Connect-RPC + HTTP gateway into Crabka (Kafka) topics"

[lints]
workspace = true

[[bin]]
name = "crabka-grpc-gateway"
path = "src/bin/gateway.rs"

[dependencies]
crabka-client-core = { version = "0.2", path = "../client-core" }
crabka-client-producer = { version = "0.2", path = "../client-producer" }
crabka-client-consumer = { version = "0.2", path = "../client-consumer" }
crabka-client-admin = { version = "0.2", path = "../client-admin" }
connectrpc-axum.workspace = true
prost.workspace = true
pbjson.workspace = true
axum.workspace = true
bytes.workspace = true
dashmap.workspace = true
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "signal", "time", "sync"] }
tokio-util = { workspace = true, features = ["rt"] }
tracing.workspace = true
tracing-subscriber.workspace = true
clap = { workspace = true, features = ["env", "derive"] }
anyhow.workspace = true
thiserror.workspace = true
uuid = { workspace = true }

[build-dependencies]
connectrpc-axum-build = { workspace = true, features = ["fetch-protoc"] }

[dev-dependencies]
assert2 = { workspace = true }
crabka-broker = { version = "0.2", path = "../broker", features = ["test-helpers"] }
tempfile.workspace = true
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "macros", "time", "sync"] }
```

- [ ] **Step 2: Write a placeholder `src/lib.rs`**

```rust
//! `crabka-grpc-gateway` — gRPC / Connect-RPC + HTTP gateway into Crabka topics.
//!
//! Built entirely on the native client crates; the broker is never modified.
```

- [ ] **Step 3: Write a placeholder `src/bin/gateway.rs`**

```rust
fn main() {
    // Replaced in Task 5 with the real server bootstrap.
    println!("crabka-grpc-gateway");
}
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles clean (a binary that prints a line).

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/Cargo.toml crates/grpc-gateway/src/lib.rs crates/grpc-gateway/src/bin/gateway.rs
git commit -m "feat(gateway): scaffold crabka-grpc-gateway crate"
```

---

### Task 2: Proto + codegen (unary `Send` only)

**Files:**
- Create: `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`
- Create: `crates/grpc-gateway/build.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (add `pb` module)

- [ ] **Step 1: Write the proto** (unary `Send` only this plan; streaming RPCs are added in the streaming follow-on plan)

```proto
syntax = "proto3";
package crabka.gateway.v1;

service Gateway {
  rpc Send(SendRequest) returns (SendResponse);
}

enum Acks {
  ACKS_ALL = 0;
  ACKS_LEADER = 1;
  ACKS_NONE = 2;
}

message Record {
  string topic = 1;
  optional bytes key = 2;
  bytes value = 3;
  map<string, bytes> headers = 4;
  optional int32 partition = 5;
  optional int64 timestamp_ms = 6;
  optional string idempotency_key = 7;
}

message SendRequest {
  repeated Record records = 1;
  Acks acks = 2;
}

message ErrorInfo {
  int32 code = 1;
  string message = 2;
  bool retriable = 3;
}

message RecordResult {
  int32 partition = 1;
  int64 offset = 2;
  bool deduplicated = 3;
  optional ErrorInfo error = 4;
}

message SendResponse {
  repeated RecordResult results = 1;
}
```

- [ ] **Step 2: Write `build.rs`** (mirrors `crates/rebalancer/build.rs`)

```rust
//! Generates Connect-RPC server stubs + prost message types from the
//! `.proto`. Prefers a system `protoc`; falls back to a vendored fetch
//! only when none is found (keeps `--offline` working with system protoc).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/crabka/gateway/v1/gateway.proto";
    let mut builder = connectrpc_axum_build::compile_protos(&[proto], &["proto"]);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}

fn system_protoc_available() -> bool {
    if std::env::var_os("PROTOC").is_some() {
        return true;
    }
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}
```

- [ ] **Step 3: Add the `pb` module to `src/lib.rs`** (append)

```rust
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.gateway.v1.rs"));
}
```

- [ ] **Step 4: Verify codegen compiles**

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles; `OUT_DIR/crabka.gateway.v1.rs` generated. (If `protoc` is missing the build fetches it via `fetch-protoc`.)

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/proto crates/grpc-gateway/build.rs crates/grpc-gateway/src/lib.rs
git commit -m "feat(gateway): gateway.proto (unary Send) + connectrpc codegen"
```

---

### Task 3: Config

**Files:**
- Create: `crates/grpc-gateway/src/config.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod config;`)

- [ ] **Step 1: Write `config.rs`**

```rust
//! Gateway configuration, parsed from CLI flags / env in `bin/gateway.rs`.

use std::net::SocketAddr;

/// Runtime configuration for the gateway process.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// `host:port,host:port,...` of brokers for bootstrap.
    pub bootstrap: String,
    /// Connect-RPC + HTTP listen address.
    pub listen_addr: SocketAddr,
    /// Base `client.id` for the native clients this gateway opens.
    pub client_id: String,
    /// Internal compacted topic that stores dedup claims.
    pub dedup_topic: String,
    /// Partition count of the dedup topic (also the ownership shard count in P3).
    pub dedup_partitions: u32,
    /// Dedup window: claim-topic `retention.ms` and the dedup guarantee horizon.
    pub dedup_window_ms: i64,
    /// `transactional.id` prefix; the per-partition id is `{prefix}-{p}`.
    pub dedup_txn_id_prefix: String,
}

impl GatewayConfig {
    /// Replication factor requested for the dedup topic at create time.
    /// Kept here so `bin` and tests agree; broker may downgrade.
    pub const DEDUP_TOPIC_REPLICATION: i16 = 3;
}
```

- [ ] **Step 2: Add `pub mod config;` to `src/lib.rs`**, then verify

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/grpc-gateway/src/config.rs crates/grpc-gateway/src/lib.rs
git commit -m "feat(gateway): GatewayConfig"
```

---

### Task 4: Error + core types

**Files:**
- Create: `crates/grpc-gateway/src/error.rs`
- Create: `crates/grpc-gateway/src/types.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod error; pub mod types;`)

- [ ] **Step 1: Write `error.rs`**

```rust
//! Gateway error type. Wraps native-client errors so handlers can map to
//! Connect status without leaking client internals.

use crabka_client_consumer::ConsumerError;
use crabka_client_producer::ProducerError;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("producer error: {0}")]
    Producer(#[from] ProducerError),
    #[error("producer send was canceled before acknowledgement")]
    ProducerCanceled,
    #[error("consumer error: {0}")]
    Consumer(#[from] ConsumerError),
    #[error("dedup store is not yet warmed up")]
    NotReady,
    #[error("dedup claim could not be (de)serialized: {0}")]
    Claim(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}
```

> If `ProducerError` / `ConsumerError` are not the exact exported names, check `crates/client-producer/src/error.rs` and `crates/client-consumer/src/error.rs` and use the actual public enum names — they are re-exported from each crate root.

- [ ] **Step 2: Write `types.rs`**

```rust
//! Protocol-agnostic record types. Every front-end (gRPC now; webhooks
//! later) converts into `GatewayRecord` and consumes `RecordOutcome`, so
//! the core engines never depend on a wire format.

use bytes::Bytes;

/// One record to produce, independent of transport.
#[derive(Debug, Clone)]
pub struct GatewayRecord {
    pub topic: String,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
    /// Explicit partition override; `None` ⇒ producer's partitioner.
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
    /// Present ⇒ the record is deduplicated by this key (EOS path).
    pub idempotency_key: Option<String>,
}

/// Result of producing one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordOutcome {
    pub partition: i32,
    pub offset: i64,
    /// True ⇒ a prior record with the same `idempotency_key` already
    /// existed; this call did not produce anything new.
    pub deduplicated: bool,
}
```

- [ ] **Step 3: Add modules to `src/lib.rs`**, then verify

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/grpc-gateway/src/error.rs crates/grpc-gateway/src/types.rs crates/grpc-gateway/src/lib.rs
git commit -m "feat(gateway): GatewayError + GatewayRecord/RecordOutcome"
```

---

### Task 5: Health endpoints + server bootstrap

**Files:**
- Create: `crates/grpc-gateway/src/health.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod health;` + `router`)
- Modify: `crates/grpc-gateway/src/bin/gateway.rs` (real `main`)

`/readyz` returns 503 until a shared `AtomicBool` readiness flag is set (Task 12 flips it once the dedup store is warm). `/healthz` is always 200 once serving.

- [ ] **Step 1: Write `health.rs`**

```rust
//! Liveness/readiness endpoints. `/readyz` gates on the dedup store warm-up.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

/// Shared readiness flag, flipped to `true` once the gateway can serve
/// dedup'd traffic correctly (dedup store warmed).
#[derive(Clone, Default)]
pub struct Readiness(pub Arc<AtomicBool>);

impl Readiness {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub fn set_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

pub fn router(readiness: Readiness) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(|State(r): State<Readiness>| async move {
                if r.is_ready() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .with_state(readiness)
}
```

- [ ] **Step 2: Add `pub mod health;` to `src/lib.rs`** and verify build

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.

- [ ] **Step 3: Write the real `bin/gateway.rs`** (server wired; Send service is added in Task 8, so for now serve health only — the `lib::router` merge is completed in Task 8)

```rust
//! `crabka-grpc-gateway` entry point.

use std::net::SocketAddr;

use clap::Parser;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::health::{self, Readiness};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "crabka-grpc-gateway", version, about = "gRPC/HTTP gateway into Crabka topics")]
struct Args {
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(long, env = "CRABKA_GATEWAY_LISTEN_ADDR", default_value = "0.0.0.0:9400")]
    listen_addr: SocketAddr,
    #[arg(long, env = "CRABKA_GATEWAY_CLIENT_ID", default_value = "crabka-grpc-gateway")]
    client_id: String,
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_TOPIC", default_value = "__crabka_grpc_dedup")]
    dedup_topic: String,
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_PARTITIONS", default_value_t = 16)]
    dedup_partitions: u32,
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_WINDOW_MS", default_value_t = 86_400_000)]
    dedup_window_ms: i64,
    #[arg(long, env = "CRABKA_GATEWAY_DEDUP_TXN_PREFIX", default_value = "crabka-grpc-dedup")]
    dedup_txn_id_prefix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_grpc_gateway=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let config = GatewayConfig {
        bootstrap: args.bootstrap_servers,
        listen_addr: args.listen_addr,
        client_id: args.client_id,
        dedup_topic: args.dedup_topic,
        dedup_partitions: args.dedup_partitions,
        dedup_window_ms: args.dedup_window_ms,
        dedup_txn_id_prefix: args.dedup_txn_id_prefix,
    };

    let readiness = Readiness::new();
    // Task 8 replaces this with `lib::router(state).merge(health::router(..))`
    // and Task 12 spawns dedup-store warm-up that calls `readiness.set_ready()`.
    let app = health::router(readiness.clone());
    readiness.set_ready(); // no dedup wired yet in this task

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;
    Ok(())
}
```

- [ ] **Step 4: Build + smoke-run**

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.
Run: `cargo run -p crabka-grpc-gateway -- --bootstrap-servers 127.0.0.1:9092 --listen-addr 127.0.0.1:0 &` then `curl -s -o /dev/null -w '%{http_code}' http://<bound-addr>/healthz`
Expected: `200`. (Kill the process after.)

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/health.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/src/bin/gateway.rs
git commit -m "feat(gateway): health/readiness endpoints + server bootstrap"
```

---

## Batch B — P1: codec, produce core, unary Send, consume core

### Task 6: `RecordCodec` + `RawCodec`

**Files:**
- Create: `crates/grpc-gateway/src/codec.rs`
- Create/append: `crates/grpc-gateway/tests/unit_basics.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod codec;`)

- [ ] **Step 1: Write the failing test** (`tests/unit_basics.rs`)

```rust
use bytes::Bytes;
use crabka_grpc_gateway::codec::{RawCodec, RecordCodec};

#[test]
fn raw_codec_is_identity() {
    let codec = RawCodec;
    let v = Bytes::from_static(b"hello");
    assert_eq!(codec.encode_value("t", v.clone()), v);
    assert_eq!(codec.decode_value("t", v.clone()), v);
}
```

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `cargo test -p crabka-grpc-gateway --test unit_basics raw_codec_is_identity`
Expected: FAIL — `codec` module / `RawCodec` not found.

- [ ] **Step 3: Write `codec.rs`**

```rust
//! Pluggable record codec. v1 ships `RawCodec` (identity, opaque bytes).
//! The deferred Schema Registry component adds a `SchemaRegistryCodec`
//! that implements this same trait — front-ends/cores never change.

use bytes::Bytes;

/// Encodes/decodes record values on the way to/from Kafka.
pub trait RecordCodec: Send + Sync + 'static {
    fn encode_value(&self, topic: &str, value: Bytes) -> Bytes;
    fn decode_value(&self, topic: &str, value: Bytes) -> Bytes;
}

/// Identity codec — opaque pass-through. The only codec in P0–P2.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawCodec;

impl RecordCodec for RawCodec {
    fn encode_value(&self, _topic: &str, value: Bytes) -> Bytes {
        value
    }
    fn decode_value(&self, _topic: &str, value: Bytes) -> Bytes {
        value
    }
}
```

- [ ] **Step 4: Add `pub mod codec;` to `src/lib.rs`; run the test**

Run: `cargo test -p crabka-grpc-gateway --test unit_basics raw_codec_is_identity`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/codec.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/tests/unit_basics.rs
git commit -m "feat(gateway): RecordCodec trait + RawCodec"
```

---

### Task 7: `ProduceCore` (plain idempotent path) + `AppState`

**Files:**
- Create: `crates/grpc-gateway/src/produce.rs`
- Create: `crates/grpc-gateway/src/state.rs`
- Create: `crates/grpc-gateway/tests/integration_send.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod produce; pub mod state;`)

This task implements the **unkeyed** (plain) produce path and leaves a `dedup: Option<Arc<DedupEngine>>` hole that Task 12 fills. Tests use the in-process broker.

- [ ] **Step 1: Write the failing integration test** (`tests/integration_send.rs`)

```rust
//! Produce via ProduceCore against an in-process broker; read the record
//! back with a native consumer to prove it landed.

use std::time::Duration;

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{Consumer, IsolationLevel};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::types::GatewayRecord;
use std::collections::BTreeMap;
use std::sync::Arc;
use tempfile::TempDir;

async fn boot() -> (Broker, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()]).await.expect("admin");
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions,
        replicas: 1,
        configs: BTreeMap::new(),
    };
    admin.create_topics(&[spec], 10_000).await.expect("create_topics");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_plain_then_read_back() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "send-itest", 1).await;

    let core = ProduceCore::new(&bootstrap, "gw-itest", Arc::new(RawCodec))
        .await
        .expect("core");

    let outcome = core
        .produce(GatewayRecord {
            topic: "send-itest".into(),
            key: None,
            value: Bytes::from_static(b"payload-1"),
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        })
        .await
        .expect("produce");
    check!(outcome.partition == 0);
    check!(outcome.deduplicated == false);

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("send-itest-reader")
        .subscribe(vec!["send-itest".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await
        .expect("consumer");

    let mut seen = vec![];
    for _ in 0..20 {
        let recs = consumer.poll(Duration::from_millis(500)).await.expect("poll");
        for r in recs {
            seen.push(r.value.unwrap_or_default());
        }
        if !seen.is_empty() {
            break;
        }
    }
    check!(seen.iter().any(|v| v.as_ref() == b"payload-1"));

    broker.shutdown().await;
}
```

> The native `Consumer` defaults `auto_offset_reset` to `Latest`; this reader is created **after** the produce and may need `AutoOffsetReset::Earliest`. If the loop sees nothing, add `.auto_offset_reset(crabka_client_consumer::AutoOffsetReset::Earliest)` to the builder (confirm the exact enum path in `crates/client-consumer/src/builder.rs`).

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --test integration_send produce_plain_then_read_back`
Expected: FAIL — `ProduceCore` not found.

- [ ] **Step 3: Write `produce.rs`** (plain path; dedup hole)

```rust
//! Core produce engine. Keyed records (with an `idempotency_key`) go
//! through the dedup engine for EOS; unkeyed records take the plain
//! idempotent path (`acks=all`). Transport-agnostic — front-ends convert
//! to `GatewayRecord` and receive `RecordOutcome`.

use std::sync::Arc;

use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};

use crate::codec::RecordCodec;
use crate::error::GatewayError;
use crate::types::{GatewayRecord, RecordOutcome};

pub struct ProduceCore {
    producer: Arc<Producer>,
    codec: Arc<dyn RecordCodec>,
    /// Filled by Task 12. `None` ⇒ keyed records take the plain path too.
    dedup: Option<Arc<crate::dedup::DedupEngine>>,
}

impl ProduceCore {
    /// Build a plain idempotent producer (`acks=all`, no transactional id).
    pub async fn new(
        bootstrap: &str,
        client_id: &str,
        codec: Arc<dyn RecordCodec>,
    ) -> Result<Self, GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self { producer: Arc::new(producer), codec, dedup: None })
    }

    /// Inject the dedup engine (Task 12).
    pub fn with_dedup(mut self, dedup: Arc<crate::dedup::DedupEngine>) -> Self {
        self.dedup = Some(dedup);
        self
    }

    pub fn codec(&self) -> &Arc<dyn RecordCodec> {
        &self.codec
    }

    /// Produce one record, routing keyed records to dedup when configured.
    pub async fn produce(&self, rec: GatewayRecord) -> Result<RecordOutcome, GatewayError> {
        let value = self.codec.encode_value(&rec.topic, rec.value.clone());
        match (&self.dedup, &rec.idempotency_key) {
            (Some(dedup), Some(_key)) => dedup.dedup_produce(&rec, value).await,
            _ => self.produce_plain(&rec, value).await,
        }
    }

    async fn produce_plain(
        &self,
        rec: &GatewayRecord,
        value: bytes::Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        let prec = to_producer_record(rec, value);
        let rx = self.producer.send(prec).await;
        let meta = rx
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;
        Ok(RecordOutcome { partition: meta.partition, offset: meta.offset, deduplicated: false })
    }
}

/// Map a `GatewayRecord` to the native `ProducerRecord`.
pub(crate) fn to_producer_record(rec: &GatewayRecord, value: bytes::Bytes) -> ProducerRecord {
    ProducerRecord {
        topic: rec.topic.clone(),
        partition: rec.partition,
        key: rec.key.clone(),
        value: Some(value),
        headers: rec
            .headers
            .iter()
            .map(|(k, v)| Header { key: k.clone(), value: Some(v.clone()) })
            .collect(),
        timestamp_ms: rec.timestamp_ms,
    }
}
```

> Confirm `Header`'s exact shape in `crates/client-producer/src/record.rs` (fields `key: String`, `value: Option<Bytes>`). If `ProducerRecord`/`Header` aren't re-exported from the crate root, import from `crabka_client_producer::record::{...}`.

- [ ] **Step 4: Write `state.rs`** (shared application state)

```rust
//! Shared, cheaply-cloneable handles for Connect handlers.

use std::sync::Arc;

use crate::config::GatewayConfig;
use crate::produce::ProduceCore;

#[derive(Clone)]
pub struct AppState {
    pub produce: Arc<ProduceCore>,
    pub config: Arc<GatewayConfig>,
}
```

- [ ] **Step 5: Add `pub mod produce; pub mod state;` to `src/lib.rs`**. Because `produce.rs` references `crate::dedup::DedupEngine`, add a temporary stub module so it compiles until Task 12 (append to `src/lib.rs`):

```rust
pub mod dedup {
    //! Replaced in Task 12. Stub keeps `ProduceCore` compiling.
    use crate::error::GatewayError;
    use crate::types::{GatewayRecord, RecordOutcome};

    pub struct DedupEngine;

    impl DedupEngine {
        pub async fn dedup_produce(
            &self,
            _rec: &GatewayRecord,
            _value: bytes::Bytes,
        ) -> Result<RecordOutcome, GatewayError> {
            Err(GatewayError::Other("dedup not wired yet".into()))
        }
    }
}
```

- [ ] **Step 6: Run the test**

Run: `cargo test -p crabka-grpc-gateway --test integration_send produce_plain_then_read_back`
Expected: PASS (the broker boots in-process; record round-trips).

- [ ] **Step 7: Commit**

```bash
git add crates/grpc-gateway/src/produce.rs crates/grpc-gateway/src/state.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/tests/integration_send.rs
git commit -m "feat(gateway): ProduceCore plain idempotent path + AppState"
```

---

### Task 8: Unary `Send` Connect handler + server merge + JVM differential

**Files:**
- Create: `crates/grpc-gateway/src/handlers.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod handlers;` + `router`)
- Modify: `crates/grpc-gateway/src/bin/gateway.rs` (build `AppState`, merge routers)
- Create: `crates/grpc-gateway/tests/jvm_differential.rs`

- [ ] **Step 1: Write `handlers.rs`** (proto ⇄ core)

```rust
//! Connect-RPC handlers — thin adapters: proto in, `GatewayRecord` to the
//! core, `RecordOutcome` back to proto.

use std::sync::Arc;

use axum::Extension;
use bytes::Bytes;
use connectrpc_axum::{ConnectError, ConnectRequest, ConnectResponse};

use crate::pb;
use crate::state::AppState;
use crate::types::GatewayRecord;

pub async fn send(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::SendRequest>,
) -> Result<ConnectResponse<pb::SendResponse>, ConnectError> {
    let msg = req.into_inner();
    let mut results = Vec::with_capacity(msg.records.len());
    for r in msg.records {
        let rec = GatewayRecord {
            topic: r.topic,
            key: r.key.map(Bytes::from),
            value: Bytes::from(r.value),
            headers: r.headers.into_iter().map(|(k, v)| (k, Bytes::from(v))).collect(),
            partition: r.partition,
            timestamp_ms: r.timestamp_ms,
            idempotency_key: r.idempotency_key,
        };
        let result = match state.produce.produce(rec).await {
            Ok(o) => pb::RecordResult {
                partition: o.partition,
                offset: o.offset,
                deduplicated: o.deduplicated,
                error: None,
            },
            Err(e) => pb::RecordResult {
                partition: -1,
                offset: -1,
                deduplicated: false,
                error: Some(pb::ErrorInfo { code: 1, message: e.to_string(), retriable: false }),
            },
        };
        results.push(result);
    }
    Ok(ConnectResponse::new(pb::SendResponse { results }))
}
```

> The exact type names `ConnectRequest`/`ConnectResponse`/`ConnectError` and `ConnectResponse::new`/`into_inner` come from `connectrpc-axum`; confirm against `crates/rebalancer/src/api/handlers.rs` (it uses the same imports). Match whatever that file imports.

- [ ] **Step 2: Add `pub mod handlers;` and `router` to `src/lib.rs`** (the service builder name is codegen output `pb::gateway_connect::GatewayServiceBuilder` — confirm the exact path from the generated `OUT_DIR/crabka.gateway.v1.rs`, mirroring how the rebalancer uses `RebalancerServiceBuilder`)

```rust
use std::sync::Arc;

pub fn router(state: Arc<state::AppState>) -> axum::Router {
    pb::gateway_connect::GatewayServiceBuilder::<()>::new()
        .send(handlers::send)
        .build()
        .layer(axum::Extension(state))
}
```

- [ ] **Step 3: Update `bin/gateway.rs`** — build `ProduceCore`/`AppState` and merge routers. Replace the router/serve section of `main`:

```rust
    use std::sync::Arc;
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::state::AppState;

    let produce = ProduceCore::new(&config.bootstrap, &config.client_id, Arc::new(RawCodec)).await?;
    let state = Arc::new(AppState { produce: Arc::new(produce), config: Arc::new(config.clone()) });

    let readiness = Readiness::new();
    readiness.set_ready(); // dedup wired in Task 12
    let app = crabka_grpc_gateway::router(state).merge(health::router(readiness.clone()));

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;
    Ok(())
```

- [ ] **Step 4: Build**

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles. If the `GatewayServiceBuilder` path differs, fix the `router` to match the generated module name.

- [ ] **Step 5: Write the JVM differential test** (`tests/jvm_differential.rs`, `#[ignore]`, Docker) — mirrors `crates/broker/tests/jvm_acceptance.rs`

```rust
//! Differential: produce a record via the gateway's ProduceCore against a
//! host-advertised in-process broker, then read it back with the JVM
//! `kafka-console-consumer` from a cp-kafka container. Proves byte-level
//! produce correctness against a real JVM client. Requires Docker.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::types::GatewayRecord;
use std::collections::BTreeMap;

const LISTEN: &str = "0.0.0.0:9092";
const BOOTSTRAP: &str = "host.docker.internal:9092";
const IMAGE: &str = "confluentinc/cp-kafka:7.5.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_consumer_reads_gateway_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listen_addr = LISTEN.parse().unwrap();
    config.advertised_listener = BOOTSTRAP.into();
    let broker = Broker::start(config).await.expect("broker");

    let mut admin = AdminClient::connect(&[BOOTSTRAP.to_string()]).await.expect("admin");
    admin
        .create_topics(
            &[CreateTopicSpec { name: "gw-jvm".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }],
            10_000,
        )
        .await
        .expect("create");

    let core = ProduceCore::new(BOOTSTRAP, "gw-jvm", Arc::new(RawCodec)).await.expect("core");
    core.produce(GatewayRecord {
        topic: "gw-jvm".into(), key: None, value: Bytes::from_static(b"jvm-sees-this"),
        headers: vec![], partition: None, timestamp_ms: None, idempotency_key: None,
    })
    .await
    .expect("produce");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let out = Command::new("docker")
        .args(["run", "--rm", "--add-host=host.docker.internal:host-gateway", IMAGE,
               "kafka-console-consumer", "--bootstrap-server", BOOTSTRAP, "--topic", "gw-jvm",
               "--from-beginning", "--max-messages", "1", "--timeout-ms", "10000"])
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .output().expect("docker run");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("jvm-sees-this"), "JVM consumer output: {s:?} / err {}", String::from_utf8_lossy(&out.stderr));

    broker.shutdown().await;
}
```

> Per project memory, single-broker JVM round-trips via `host.docker.internal` work on macOS; only multi-broker data replication does not. Confirm `BrokerConfig` field names (`listen_addr`, `advertised_listener`) against `crates/broker/tests/jvm_acceptance.rs`.

- [ ] **Step 6: Run the wire build + the in-process tests (skip Docker)**

Run: `cargo test -p crabka-grpc-gateway`
Expected: unit + `integration_send` pass; `jvm_differential` is `#[ignore]` (not run).

- [ ] **Step 7: Commit**

```bash
git add crates/grpc-gateway/src/handlers.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/src/bin/gateway.rs crates/grpc-gateway/tests/jvm_differential.rs
git commit -m "feat(gateway): unary Send Connect handler + JVM differential test"
```

---

### Task 9: Consume core (group subscribe + commit)

**Files:**
- Create: `crates/grpc-gateway/src/consume.rs`
- Create: `crates/grpc-gateway/tests/integration_consume.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (`pub mod consume;`)

A tested library component. No wire yet (the Subscribe/poll wire ships in the streaming follow-on plan).

- [ ] **Step 1: Write the failing test** (`tests/integration_consume.rs`)

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::consume::ConsumeSession;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::types::GatewayRecord;
use tempfile::TempDir;

async fn boot() -> (Broker, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_receives_then_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.unwrap();
    admin.create_topics(&[CreateTopicSpec{ name: "consume-itest".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }], 10_000).await.unwrap();

    let core = ProduceCore::new(&bootstrap, "gw-c", Arc::new(RawCodec)).await.unwrap();
    core.produce(GatewayRecord{ topic: "consume-itest".into(), key: None, value: Bytes::from_static(b"c1"), headers: vec![], partition: None, timestamp_ms: None, idempotency_key: None }).await.unwrap();

    let mut session = ConsumeSession::new(&bootstrap, "gw-consume-group", "gw-c", vec!["consume-itest".to_string()]).await.unwrap();

    let mut got = vec![];
    for _ in 0..20 {
        let batch = session.poll(Duration::from_millis(500)).await.unwrap();
        for r in batch { got.push(r.value.clone()); }
        if !got.is_empty() { break; }
    }
    check!(got.iter().any(|v| v.as_ref() == b"c1"));
    session.commit().await.unwrap();

    broker.shutdown().await;
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --test integration_consume subscribe_receives_then_commits`
Expected: FAIL — `ConsumeSession` not found.

- [ ] **Step 3: Write `consume.rs`**

```rust
//! Consume core: a group-subscribed session that yields records and commits
//! offsets. The streaming/poll wire (later plan) drives this. Records are
//! decoded through the codec on the way out.

use std::sync::Arc;
use std::time::Duration;

use crabka_client_consumer::{Consumer, ConsumerRecord, IsolationLevel};

use crate::codec::{RawCodec, RecordCodec};
use crate::error::GatewayError;

pub struct ConsumeSession {
    consumer: Consumer,
    codec: Arc<dyn RecordCodec>,
}

impl ConsumeSession {
    pub async fn new(
        bootstrap: &str,
        group_id: &str,
        client_id: &str,
        topics: Vec<String>,
    ) -> Result<Self, GatewayError> {
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .group_id(group_id.to_string())
            .subscribe(topics)
            .isolation_level(IsolationLevel::ReadCommitted)
            .build()
            .await?;
        Ok(Self { consumer, codec: Arc::new(RawCodec) })
    }

    /// Poll a batch; record values are decoded through the codec.
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, GatewayError> {
        let mut batch = self.consumer.poll(timeout).await?;
        for r in &mut batch {
            if let Some(v) = r.value.take() {
                r.value = Some(self.codec.decode_value(&r.topic, v));
            }
        }
        Ok(batch)
    }

    /// Commit current positions (at-least-once: call after delivery is acked).
    pub async fn commit(&self) -> Result<(), GatewayError> {
        self.consumer.commit_sync().await?;
        Ok(())
    }
}
```

- [ ] **Step 4: Add `pub mod consume;` to `src/lib.rs`; run the test**

Run: `cargo test -p crabka-grpc-gateway --test integration_consume subscribe_receives_then_commits`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/consume.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/tests/integration_consume.rs
git commit -m "feat(gateway): consume core (group subscribe + commit)"
```

---

## Batch C — P2: single-owner exactly-once dedup

### Task 10: Ensure the compacted dedup topic

**Files:**
- Create: `crates/grpc-gateway/src/dedup/topic.rs`
- Modify: `crates/grpc-gateway/src/lib.rs` (replace the stub `dedup` module with `pub mod dedup;` and create `dedup/mod.rs` shell — see Task 12; for this task add `pub mod topic;` under it)

> Ordering note: Task 12 turns `dedup` from the Task-7 inline stub into a real `src/dedup/mod.rs`. To let T10 and T11 proceed before T12, first do T12 Step 0 (create the `src/dedup/` directory + a minimal real `mod.rs`), then T10/T11/T12 edit sibling files. T12 Step 0 is listed in Task 12.

- [ ] **Step 1: Write `dedup/topic.rs`**

```rust
//! Idempotent creation of the internal compacted dedup-claim topic.
//! `cleanup.policy=compact,delete` + `retention.ms=window` bounds both the
//! topic size and the dedup horizon.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::error::GatewayError;

const INVALID_REPLICATION_FACTOR: i16 = 38;
const TOPIC_ALREADY_EXISTS: i16 = 36;

pub async fn ensure_dedup_topic(
    bootstrap: &str,
    name: &str,
    partitions: u32,
    window_ms: i64,
    replication: i16,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect(&addrs)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact,delete".to_string());
    configs.insert("retention.ms".to_string(), window_ms.to_string());
    configs.insert("min.cleanable.dirty.ratio".to_string(), "0.01".to_string());
    configs.insert("segment.ms".to_string(), "60000".to_string());

    create_with_rf(&mut admin, name, partitions, replication, &configs).await
}

async fn create_with_rf(
    admin: &mut AdminClient,
    name: &str,
    partitions: u32,
    rf: i16,
    configs: &BTreeMap<String, String>,
) -> Result<(), GatewayError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: i32::try_from(partitions).unwrap_or(i32::MAX),
        replicas: i32::from(rf),
        configs: configs.clone(),
    };
    let outcomes = admin
        .create_topics(&[spec], 10_000)
        .await
        .map_err(|e| GatewayError::Other(format!("create_topics: {e}")))?;
    for o in outcomes {
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0) | Some(TOPIC_ALREADY_EXISTS) => {}
            Some(INVALID_REPLICATION_FACTOR) if rf > 1 => {
                return Box::pin(create_with_rf(admin, name, partitions, 1, configs)).await;
            }
            Some(code) => {
                return Err(GatewayError::Other(format!("create dedup topic failed: code {code}")));
            }
        }
    }
    Ok(())
}
```

> Confirm the create-topics outcome shape against `crates/client-admin/src/topics.rs` (the rebalancer reads `o.error.as_ref().map(|e| e.code)` — match that). `CreateTopicSpec` fields: `name`, `partitions: i32`, `replicas: i32`, `configs: BTreeMap<String,String>`.

- [ ] **Step 2: Build** (after Task 12 Step 0 created `dedup/mod.rs` with `pub mod topic;`)

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/grpc-gateway/src/dedup/topic.rs crates/grpc-gateway/src/dedup/mod.rs
git commit -m "feat(gateway): ensure compacted dedup-claim topic"
```

---

### Task 11: `DedupStore` — read_committed materialized map + warm-up

**Files:**
- Create: `crates/grpc-gateway/src/dedup/store.rs`
- Modify: `crates/grpc-gateway/src/dedup/mod.rs` (`pub mod store;`, export `ClaimValue`)
- Create: `crates/grpc-gateway/tests/integration_dedup.rs` (warm-up test added here; dup tests added in Task 12)

- [ ] **Step 1: Write the failing test** (`tests/integration_dedup.rs`)

```rust
//! Dedup: warm-up reconstructs the claim map from the compacted topic, so a
//! post-restart duplicate is recognized.

use std::sync::Arc;
use std::time::Duration;

use assert2::check;
use crabka_broker::{Broker, BrokerConfig};
use crabka_grpc_gateway::dedup::store::{ClaimValue, DedupStore};
use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
use tempfile::TempDir;

async fn boot() -> (Broker, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warmup_reads_existing_claims() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, topic, 4, 3_600_000, 1).await.unwrap();

    // Write one claim directly via a fresh store's writer helper, then drop it.
    let store = Arc::new(DedupStore::new(4));
    store
        .write_claim(&bootstrap, "gw-dedup-writer", topic, "key-A", &ClaimValue { topic: "user".into(), partition: 0, offset: 7 })
        .await
        .unwrap();

    // New store warms up from the topic and sees key-A.
    let store2 = Arc::new(DedupStore::new(4));
    store2.warm_up(&bootstrap, "gw-dedup-warm", topic).await.unwrap();
    check!(store2.is_ready());
    let got = store2.get("key-A");
    check!(got.is_some());
    check!(got.unwrap().offset == 7);
    check!(store2.get("absent").is_none());

    let _ = Duration::from_millis(1);
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p crabka-grpc-gateway --test integration_dedup warmup_reads_existing_claims`
Expected: FAIL — `DedupStore` not found.

- [ ] **Step 3: Write `dedup/store.rs`**

```rust
//! Materialized view of the compacted dedup-claim topic. In single-owner
//! P2, the owner updates the map locally on each commit AND rebuilds it from
//! the topic at startup (`warm_up`) for crash recovery. P3 replaces the
//! local update with a continuous read_committed tail across owners.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::error::GatewayError;

/// The value stored under each `idempotency_key` claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimValue {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct DedupStore {
    map: DashMap<String, ClaimValue>,
    partitions: u32,
    ready: AtomicBool,
}

impl DedupStore {
    pub fn new(partitions: u32) -> Self {
        Self { map: DashMap::new(), partitions, ready: AtomicBool::new(false) }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub fn get(&self, key: &str) -> Option<ClaimValue> {
        self.map.get(key).map(|v| v.clone())
    }

    /// Apply a claim to the in-memory map (called locally after a commit).
    pub fn apply(&self, key: String, value: ClaimValue) {
        self.map.insert(key, value);
    }

    /// Rebuild the map from the compacted topic, then mark ready. Reads with
    /// `read_committed` from earliest until caught up (two consecutive empty
    /// polls). Single-member unique group ⇒ all partitions assigned.
    pub async fn warm_up(
        self: &Arc<Self>,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
    ) -> Result<(), GatewayError> {
        let group = format!("{client_id}-{}", Uuid::new_v4());
        let mut consumer = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .group_id(group)
            .subscribe(vec![dedup_topic.to_string()])
            .isolation_level(IsolationLevel::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await?;

        let mut empty_polls = 0;
        while empty_polls < 2 {
            let batch = consumer.poll(Duration::from_millis(500)).await?;
            if batch.is_empty() {
                empty_polls += 1;
                continue;
            }
            empty_polls = 0;
            for r in batch {
                let Some(key_bytes) = r.key else { continue };
                let key = String::from_utf8_lossy(&key_bytes).into_owned();
                match r.value {
                    None => {
                        self.map.remove(&key);
                    }
                    Some(v) => {
                        let claim: ClaimValue = serde_json::from_slice(&v)?;
                        self.map.insert(key, claim);
                    }
                }
            }
        }
        self.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Test/helper writer: produce a single claim record (compacted topic key
    /// = idempotency key, value = JSON `ClaimValue`) to its hashed partition.
    pub async fn write_claim(
        &self,
        bootstrap: &str,
        client_id: &str,
        dedup_topic: &str,
        key: &str,
        value: &ClaimValue,
    ) -> Result<(), GatewayError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap.to_string())
            .client_id(client_id.to_string())
            .enable_idempotence(true)
            .acks(Acks::All)
            .build()
            .await?;
        let partition = i32::try_from(crate::dedup::partition_for(key, self.partitions)).unwrap_or(0);
        let prec = ProducerRecord {
            topic: dedup_topic.to_string(),
            partition: Some(partition),
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: Some(Bytes::from(serde_json::to_vec(value)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        let rx = producer.send(prec).await;
        rx.await.map_err(|_| GatewayError::ProducerCanceled)?.map_err(GatewayError::Producer)?;
        self.apply(key.to_string(), value.clone());
        Ok(())
    }
}
```

> Confirm `AutoOffsetReset` is exported from `crabka_client_consumer` root (the digest shows it as a builder default type; if it lives at `crabka_client_consumer::builder::AutoOffsetReset`, import from there).

- [ ] **Step 4: Add `pub mod store;` to `dedup/mod.rs`; run the test**

Run: `cargo test -p crabka-grpc-gateway --test integration_dedup warmup_reads_existing_claims`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/grpc-gateway/src/dedup/store.rs crates/grpc-gateway/src/dedup/mod.rs crates/grpc-gateway/tests/integration_dedup.rs
git commit -m "feat(gateway): DedupStore materialized map + warm-up"
```

---

### Task 12: `DedupEngine` — per-partition transactional record+claim, wired into `ProduceCore`

**Files:**
- Modify/replace: `crates/grpc-gateway/src/lib.rs` (remove the Task-7 inline `dedup` stub; add `pub mod dedup;`)
- Create: `crates/grpc-gateway/src/dedup/mod.rs` (real engine + key hashing + `partition_for`)
- Modify: `crates/grpc-gateway/src/produce.rs` (no change to the API; the stub goes away)
- Modify: `crates/grpc-gateway/src/bin/gateway.rs` (build dedup, warm up, gate readiness, inject into `ProduceCore`)
- Append: `crates/grpc-gateway/tests/integration_dedup.rs` (dup tests)
- Append: `crates/grpc-gateway/tests/unit_basics.rs` (hash determinism + ClaimValue serde)

- [ ] **Step 0 (do FIRST, unblocks T10/T11): create the real `dedup` module dir.** Delete the inline `pub mod dedup { ... }` stub added in Task 7 from `src/lib.rs`, replace with `pub mod dedup;`, and create `src/dedup/mod.rs`:

```rust
//! Single-owner exactly-once dedup engine.

pub mod store;
pub mod topic;

/// Deterministic FNV-1a-64 over the key, modulo partition count. Stable
/// across processes/restarts (unlike `DefaultHasher`'s per-run state), so a
/// given key always maps to the same dedup partition.
#[must_use]
pub fn partition_for(key: &str, partitions: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(partitions.max(1))) as u32
}
```

(Then T10 and T11 can land their files. The `DedupEngine` struct below is appended in Step 3.)

- [ ] **Step 1: Write the failing unit test** (append to `tests/unit_basics.rs`)

```rust
#[test]
fn partition_for_is_deterministic_and_bounded() {
    use crabka_grpc_gateway::dedup::partition_for;
    let a = partition_for("order-42", 16);
    let b = partition_for("order-42", 16);
    assert_eq!(a, b);
    assert!(a < 16);
    // Different keys generally land in different partitions (smoke).
    let spread: std::collections::HashSet<u32> =
        (0..100).map(|i| partition_for(&format!("k{i}"), 16)).collect();
    assert!(spread.len() > 1);
}

#[test]
fn claim_value_round_trips() {
    use crabka_grpc_gateway::dedup::store::ClaimValue;
    let c = ClaimValue { topic: "t".into(), partition: 3, offset: 99 };
    let bytes = serde_json::to_vec(&c).unwrap();
    let back: ClaimValue = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(c, back);
}
```

- [ ] **Step 2: Write the failing dup integration test** (append to `tests/integration_dedup.rs`)

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_idempotency_key_produces_once() {
    use std::collections::BTreeMap;
    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::dedup::DedupEngine;
    use crabka_grpc_gateway::dedup::store::DedupStore;
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::types::GatewayRecord;

    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1).await.unwrap();
    let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.unwrap();
    admin.create_topics(&[CreateTopicSpec{ name: "dedup-user".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }], 10_000).await.unwrap();

    let store = Arc::new(DedupStore::new(4));
    store.warm_up(&bootstrap, "gw-warm", dedup_topic).await.unwrap();
    let engine = Arc::new(DedupEngine::new(&bootstrap, "gw-dedup", "crabka-grpc-dedup", dedup_topic.to_string(), 4, store.clone()));
    let core = ProduceCore::new(&bootstrap, "gw-prod", Arc::new(RawCodec)).await.unwrap().with_dedup(engine);

    let mk = || GatewayRecord{ topic: "dedup-user".into(), key: None, value: Bytes::from_static(b"once"), headers: vec![], partition: None, timestamp_ms: None, idempotency_key: Some("idem-1".into()) };

    let first = core.produce(mk()).await.unwrap();
    let second = core.produce(mk()).await.unwrap();
    assert_eq!(first.deduplicated, false);
    assert_eq!(second.deduplicated, true);
    assert_eq!(first.partition, second.partition);
    assert_eq!(first.offset, second.offset);

    // Exactly one record landed in the user topic.
    let mut consumer = Consumer::builder().bootstrap(bootstrap.clone()).group_id("dedup-count").subscribe(vec!["dedup-user".to_string()]).isolation_level(IsolationLevel::ReadCommitted).auto_offset_reset(AutoOffsetReset::Earliest).build().await.unwrap();
    let mut count = 0;
    for _ in 0..10 { count += consumer.poll(std::time::Duration::from_millis(500)).await.unwrap().len(); }
    assert_eq!(count, 1);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_produce_once() {
    use std::collections::BTreeMap;
    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::dedup::DedupEngine;
    use crabka_grpc_gateway::dedup::store::DedupStore;
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::types::GatewayRecord;

    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1).await.unwrap();
    let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.unwrap();
    admin.create_topics(&[CreateTopicSpec{ name: "dedup-conc".into(), partitions: 1, replicas: 1, configs: BTreeMap::new() }], 10_000).await.unwrap();

    let store = Arc::new(DedupStore::new(4));
    store.warm_up(&bootstrap, "gw-warm2", dedup_topic).await.unwrap();
    let engine = Arc::new(DedupEngine::new(&bootstrap, "gw-dedup2", "crabka-grpc-dedup", dedup_topic.to_string(), 4, store.clone()));
    let core = Arc::new(ProduceCore::new(&bootstrap, "gw-prod2", Arc::new(RawCodec)).await.unwrap().with_dedup(engine));

    let mut handles = vec![];
    for _ in 0..8 {
        let core = core.clone();
        handles.push(tokio::spawn(async move {
            core.produce(GatewayRecord{ topic: "dedup-conc".into(), key: None, value: Bytes::from_static(b"x"), headers: vec![], partition: None, timestamp_ms: None, idempotency_key: Some("same".into()) }).await.unwrap()
        }));
    }
    let mut deduped = 0;
    for h in handles { if h.await.unwrap().deduplicated { deduped += 1; } }
    assert_eq!(deduped, 7, "exactly one of 8 should be the original producer");

    let mut consumer = Consumer::builder().bootstrap(bootstrap.clone()).group_id("dedup-conc-count").subscribe(vec!["dedup-conc".to_string()]).isolation_level(IsolationLevel::ReadCommitted).auto_offset_reset(AutoOffsetReset::Earliest).build().await.unwrap();
    let mut count = 0;
    for _ in 0..10 { count += consumer.poll(std::time::Duration::from_millis(500)).await.unwrap().len(); }
    assert_eq!(count, 1);

    broker.shutdown().await;
}
```

- [ ] **Step 3: Append the `DedupEngine` to `dedup/mod.rs`**

```rust
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::error::GatewayError;
use crate::produce::to_producer_record;
use crate::types::{GatewayRecord, RecordOutcome};

use self::store::{ClaimValue, DedupStore};

/// A lazily-initialized transactional producer pinned to one dedup partition.
/// One in-flight transaction at a time ⇒ the `Mutex` serializes that
/// partition's record+claim transactions.
type TxnSlot = Mutex<Option<Producer>>;

pub struct DedupEngine {
    bootstrap: String,
    client_id: String,
    txn_id_prefix: String,
    dedup_topic: String,
    partitions: u32,
    slots: Vec<TxnSlot>,
    store: Arc<DedupStore>,
}

impl DedupEngine {
    pub fn new(
        bootstrap: &str,
        client_id: &str,
        txn_id_prefix: &str,
        dedup_topic: String,
        partitions: u32,
        store: Arc<DedupStore>,
    ) -> Self {
        let slots = (0..partitions.max(1)).map(|_| Mutex::new(None)).collect();
        Self {
            bootstrap: bootstrap.to_string(),
            client_id: client_id.to_string(),
            txn_id_prefix: txn_id_prefix.to_string(),
            dedup_topic,
            partitions: partitions.max(1),
            slots,
            store,
        }
    }

    /// EOS produce: fast-path map hit returns the cached offset; a miss takes
    /// the partition's transactional producer and writes the data record +
    /// claim atomically, then updates the local map.
    pub async fn dedup_produce(
        &self,
        rec: &GatewayRecord,
        value: Bytes,
    ) -> Result<RecordOutcome, GatewayError> {
        if !self.store.is_ready() {
            return Err(GatewayError::NotReady);
        }
        let key = rec
            .idempotency_key
            .as_deref()
            .ok_or_else(|| GatewayError::Other("dedup_produce called without idempotency_key".into()))?;

        // Fast path: already claimed.
        if let Some(c) = self.store.get(key) {
            return Ok(RecordOutcome { partition: c.partition, offset: c.offset, deduplicated: true });
        }

        let p = partition_for(key, self.partitions);
        let mut slot = self.slots[p as usize].lock().await;

        // Re-check under the lock (another task may have just claimed it).
        if let Some(c) = self.store.get(key) {
            return Ok(RecordOutcome { partition: c.partition, offset: c.offset, deduplicated: true });
        }

        // Lazily init the partition's transactional producer.
        if slot.is_none() {
            let txn_id = format!("{}-{}", self.txn_id_prefix, p);
            let producer = Producer::builder()
                .bootstrap(self.bootstrap.clone())
                .client_id(format!("{}-dedup-{}", self.client_id, p))
                .enable_idempotence(true)
                .acks(Acks::All)
                .transactional_id(Some(txn_id))
                .build()
                .await?;
            producer.init_transactions().await?;
            *slot = Some(producer);
        }
        let producer = slot.as_ref().expect("just initialized");

        producer.begin_transaction().await?;

        // 1. data record → user topic
        let data = to_producer_record(rec, value);
        let meta = producer
            .send(data)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;

        // 2. claim → dedup topic (partition p), key = idempotency key
        let claim = ClaimValue { topic: rec.topic.clone(), partition: meta.partition, offset: meta.offset };
        let claim_rec = ProducerRecord {
            topic: self.dedup_topic.clone(),
            partition: Some(i32::try_from(p).unwrap_or(0)),
            key: Some(Bytes::from(key.as_bytes().to_vec())),
            value: Some(Bytes::from(serde_json::to_vec(&claim)?)),
            headers: vec![],
            timestamp_ms: None,
        };
        producer
            .send(claim_rec)
            .await
            .await
            .map_err(|_| GatewayError::ProducerCanceled)?
            .map_err(GatewayError::Producer)?;

        producer.commit_transaction().await?;

        // Single-owner: update the local map directly.
        self.store.apply(key.to_string(), claim);
        Ok(RecordOutcome { partition: meta.partition, offset: meta.offset, deduplicated: false })
    }
}
```

- [ ] **Step 4: Remove the Task-7 stub from `src/lib.rs`** (the inline `pub mod dedup { ... }`) — it was already replaced by `pub mod dedup;` in Step 0. Confirm there is exactly one `dedup` declaration.

- [ ] **Step 5: Run unit + dedup tests**

Run: `cargo test -p crabka-grpc-gateway --test unit_basics`
Expected: PASS (hashing + serde).
Run: `cargo test -p crabka-grpc-gateway --test integration_dedup`
Expected: PASS (warm-up + sequential dup + concurrent dup; exactly one user-topic record each).

- [ ] **Step 6: Wire dedup into the binary** — update `bin/gateway.rs` to ensure the topic, build+warm the store, gate readiness, and inject the engine. Replace the produce/state/readiness section:

```rust
    use std::sync::Arc;
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::dedup::DedupEngine;
    use crabka_grpc_gateway::dedup::store::DedupStore;
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::state::AppState;

    ensure_dedup_topic(
        &config.bootstrap,
        &config.dedup_topic,
        config.dedup_partitions,
        config.dedup_window_ms,
        GatewayConfig::DEDUP_TOPIC_REPLICATION,
    )
    .await?;

    let store = Arc::new(DedupStore::new(config.dedup_partitions));
    let readiness = Readiness::new();
    {
        let store = store.clone();
        let readiness = readiness.clone();
        let bootstrap = config.bootstrap.clone();
        let client_id = format!("{}-dedup-warm", config.client_id);
        let dedup_topic = config.dedup_topic.clone();
        tokio::spawn(async move {
            match store.warm_up(&bootstrap, &client_id, &dedup_topic).await {
                Ok(()) => readiness.set_ready(),
                Err(e) => tracing::error!(error = %e, "dedup warm-up failed; /readyz stays 503"),
            }
        });
    }

    let engine = Arc::new(DedupEngine::new(
        &config.bootstrap,
        &config.client_id,
        &config.dedup_txn_id_prefix,
        config.dedup_topic.clone(),
        config.dedup_partitions,
        store,
    ));
    let produce = ProduceCore::new(&config.bootstrap, &config.client_id, Arc::new(RawCodec))
        .await?
        .with_dedup(engine);
    let state = Arc::new(AppState { produce: Arc::new(produce), config: Arc::new(config.clone()) });

    let app = crabka_grpc_gateway::router(state).merge(health::router(readiness));

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "gateway listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;
    Ok(())
```

- [ ] **Step 7: Full crate build + test + lints**

Run: `cargo build -p crabka-grpc-gateway`
Expected: compiles.
Run: `cargo test -p crabka-grpc-gateway`
Expected: all non-`#[ignore]` tests pass.
Run: `cargo fmt -p crabka-grpc-gateway` then `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`
Expected: clean (fmt no diff; clippy no warnings).

- [ ] **Step 8: Commit**

```bash
git add crates/grpc-gateway/src/dedup/mod.rs crates/grpc-gateway/src/lib.rs crates/grpc-gateway/src/bin/gateway.rs crates/grpc-gateway/tests/integration_dedup.rs crates/grpc-gateway/tests/unit_basics.rs
git commit -m "feat(gateway): single-owner EOS DedupEngine wired into ProduceCore + binary"
```

---

## Final verification (run before declaring P0–P2 done)

- [ ] `cargo build -p crabka-grpc-gateway` — clean.
- [ ] `cargo test -p crabka-grpc-gateway` — unit + `integration_send` + `integration_consume` + `integration_dedup` pass.
- [ ] `cargo fmt --check -p crabka-grpc-gateway` — no diff (CI gate).
- [ ] `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings` — clean (CI gate; `--all-targets` catches test-only lints).
- [ ] (Optional, Docker) `cargo test -p crabka-grpc-gateway --test jvm_differential -- --ignored` — JVM `kafka-console-consumer` reads a gateway-produced record.

## Self-review (completed during planning)

- **Spec coverage (P0–P2 scope):** crate skeleton ✓ (T1–T5); unary Send ✓ (T2,T8); produce core / RawCodec / acks=all ✓ (T6,T7); consume core (group subscribe + commit) ✓ (T9); compacted dedup topic ✓ (T10); read_committed materialized map + warm-up gate ✓ (T11); transactional record+claim + per-partition `transactional.id` + per-key fast-path/per-partition serialization ✓ (T12); readiness gating ✓ (T12 Step 6). Streaming wire + P3 sharding intentionally **out of scope** (see Scope + Risks) — not gaps.
- **Type consistency:** `GatewayRecord`/`RecordOutcome` (T4) used identically in T7/T8/T12; `ClaimValue` (T11) used in T12; `partition_for` (T12 Step 0) used in T11/T12; `to_producer_record` (T7) reused in T12; `ProduceCore::new`/`with_dedup`/`produce` signatures consistent across T7/T8/T12; `DedupStore::new/get/apply/is_ready/warm_up/write_claim` consistent across T11/T12; `DedupEngine::new/dedup_produce` consistent T7-stub→T12-real.
- **Placeholders:** none — every code step has complete code; the few ">" callouts are *verification instructions* for exact upstream names (re-exports, codegen module path, `AutoOffsetReset` location), not deferred logic. Each names the exact file to check.
- **Crash-injection caveat:** true mid-transaction kill is not reproducible with the in-process harness; P2 verifies the equivalent invariant via warm-up-after-write (restart dedup) + concurrent dedup. Full process-kill crash-injection is added with P3's multi-process tests — noted, not silently skipped.

## Risks / things the implementer must verify against upstream

1. **`connectrpc-axum` streaming support (BLOCKS the streaming follow-on, not this plan).** This plan ships only **unary** `Send`. Before planning `SendStream`/`Subscribe`, confirm whether `connectrpc-axum` 0.1.1 generates streaming server stubs: inspect the generated `OUT_DIR/crabka.gateway.v1.rs` after adding a streaming RPC, or the crate docs/source. If unsupported, options: bump the dep, add a narrow `tonic` service for streaming only, or expose unary `Poll`/`Commit` for consume.
2. **Codegen module path** for the service builder (`pb::gateway_connect::GatewayServiceBuilder`) — confirm the exact generated path from `OUT_DIR/crabka.gateway.v1.rs` (mirror the rebalancer's `RebalancerServiceBuilder` usage in `crates/rebalancer/src/api/mod.rs`).
3. **Native re-export names** — `ProducerRecord`, `Header`, `RecordMetadata`, `Acks`, `IsolationLevel`, `AutoOffsetReset`, `ConsumerRecord`, `ProducerError`, `ConsumerError`. The handler/core code imports them from crate roots; if any live in a submodule, adjust the `use`. Each call site notes the file to check.
4. **`AdminClient` create-topics outcome shape** — `o.error.as_ref().map(|e| e.code)` mirrors the rebalancer; confirm in `crates/client-admin/src/topics.rs`.
5. **`BrokerConfig::for_tests` + `test-helpers` feature** — confirm the helper exists and `advertised_listener`/`listen_addr` field names (used by `jvm_differential.rs`) against `crates/broker/tests/jvm_acceptance.rs`.
6. **Warm-up "caught up" heuristic** (two empty `read_committed` polls) is adequate for single-owner P2; P3 replaces it with end-offset comparison for continuous cross-owner tailing.

## What lands after this plan

- **Streaming plan:** `SendStream` + bidi `Subscribe` wire (gated on Risk 1), driving the consume core from Task 9.
- **P3 plan:** active-active ownership sharding — ownership consumer-group over `__crabka_grpc_dedup`, `__crabka_grpc_gateway_membership` routing topic, key→owner forwarding (`forward.rs`), per-partition rebalance warm-up (continuous `read_committed` tail replaces local `apply`), cross-instance `transactional.id` fencing, multi-process crash-injection tests.
- **P4–P9:** TLS/mTLS, identity→ACL (`crabka-authz`), webhook in/out, telemetry, operator — and the `SchemaRegistryCodec` drop-in via the `RecordCodec` seam.
