# crabka-traces Slice 4 — Ingest service (OTLP/Jaeger/Zipkin distributor + WAL + block-builder + live-store)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ingest half of the traces backend — the four push doors (OTLP traces, Jaeger gRPC/Thrift, Zipkin, Tempo-native `/api/push`) decoded to an internal `Span`, fanned into `SpanRecord`s on a `trace_id`-partitioned Kafka WAL by the **distributor** role; the **block-builder** consumer group that groups spans by `trace_id` over a flush window, computes the nested-set columns via a DFS pre-order, and writes span Parquet blocks + `TraceIndex` updates to object storage (write-then-commit, idempotent keys); and the **live-store** consumer group that assembles recent traces in memory as a DataFusion `MemTable`, rebuildable from offsets. Ship a role-selectable `crabka-traces --target distributor|block-builder|live-store` binary (later targets stubbed).

**Architecture:** This slice creates the new `crabka-traces` crate and adds the `wire`, `span`, `wal`, `distributor`, `blockbuilder`, and `livestore` modules. The `wire` module owns the three decode surfaces — OTLP traces (`opentelemetry-proto` 0.32 trace types), Jaeger (gRPC + Thrift), Zipkin JSON (`/api/v2/spans`) — each lowering into one internal `Span`. The `span` module defines that `Span` + the `nested_set` DFS pre-order that the block-builder runs at flush, plus the Arrow batch builder for the flattened span block schema (matching the slice-1 span columns). The `wal` module defines `SpanRecord` — the WAL topic record (serde + `serde-wincode`, the codebase convention) that **Slices 5/6/7 consume** — `TRACES_WAL_TOPIC`, and `partition_key = hash(trace_id)` (the RF1 dedup-avoidance invariant). The `distributor` is an axum 0.8 server (four routers + receivers); the `blockbuilder` and `livestore` are Kafka consumer-group loops. A real Crabka broker is only needed for the produce/consume round-trip test, which uses the in-process broker test-support (no Docker).

```
OTLP /v1/traces ─┐
Jaeger gRPC/Thrift├─→ distributor (axum) ─→ validate ─→ produce SpanRecord ─→ __crabka_traces_wal
Zipkin /api/v2/spans                              (key = hash(trace_id))            │  (all spans of a trace → one partition)
Tempo /api/push ─┘                                                                 │
                                ┌──────────────────────────────────────────────────┴──────────────┐
                                ▼ (consumer group)                                                 ▼ (consumer group)
                       block-builder                                                          live-store
                       group by trace_id over window                                          assemble recent traces by trace_id
                       DFS pre-order → nested_set_left/right/parent_id                         → DataFusion MemTable (hot tier)
                       → span RecordBatch → BlockWriter::write_block                           → rebuildable from offsets
                       → TraceIndex (bloom + tag sets) → object storage
                       → commit offsets  (block+index FIRST, then commit)
```

**Tech Stack:** Rust 2024 · `opentelemetry-proto` 0.32 (`gen-tonic-messages`, **`trace`** — this slice adds the `trace` feature) · `axum` 0.8 (`http1`, `tokio`) · `serde_json` 1 (Zipkin JSON) · `thrift` (Jaeger Thrift compact) · `bytes` 1 · `arrow` 59 · `datafusion` (git pin, for the live-store `MemTable`) · `object_store` 0.13 · `crabka-blockstore` (slice-1 generalized: `BlockWriter`/`BlockMeta`/`TraceIndex`/span schema) · `crabka-client-producer` · `crabka-client-consumer` · `crabka-client-admin` · `serde` + `serde-wincode` (`wincode::Serialize`) · `clap` 4 · `tokio` · `tracing`. Tests: `assert2`, `proptest`, `tempfile`, `object_store::memory::InMemory`; the broker round-trip test uses `crates/broker/tests/support` (in-process, no Docker).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change `SpanRecord`/`Span`/enums/wire-internal types freely; no shims, no migration code, no `#[serde(default)]`. (Only Kafka **client** wire compat matters — and the OTLP/Jaeger/Zipkin byte-exactness on the HTTP edge.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-traces --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-traces` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Arrow version identity:** use `arrow` 59 directly. The span batches this slice builds are consumed by `crabka-blockstore::BlockWriter::write_block` without conversion. The live-store `MemTable` uses `datafusion::arrow` re-exports to keep type identity at the DataFusion boundary.
- **opentelemetry-proto generated types are the source of truth.** The trace field names this plan quotes (`Span.trace_id: Vec<u8>`, `Span.parent_span_id: Vec<u8>`, `span::SpanKind`, `Status`, `span::Event`, `span::Link`) are pinned by behavior tests; if a generated field name differs, **align to the generated type**, never fabricate.
- **The `hash(trace_id)` partition invariant is non-negotiable** (spec §1, §5.2). All spans of a trace MUST land in one partition. The producer MurmurHash2-partitions on `key`; set `key = trace_id` raw bytes (or a stable hash of them) and leave `partition: None`. A test pins that two spans sharing a `trace_id` produce the same key.
- **Kafka wire-protocol exactness** is preserved automatically by producing/consuming through the existing `crabka-client-producer`/`crabka-client-consumer` clients — do not hand-roll protocol frames.

---

## Dependency & slice roadmap

**Depends on (consume exactly — do not re-implement):**
- **`crabka-blockstore` (slice-1 generalized)** — `BlockStore`, `BlockWriter::new(store: Arc<dyn object_store::ObjectStore>)` + `BlockWriter::write_block(tenant:&str, object_key:&str, schema:SchemaRef, batches:&[RecordBatch]) -> Result<BlockMeta>`, `BlockMeta`, the **`TraceIndex`** impl (`BlockIndex`) with `add_trace_block`/`add_tags`/`save`, and the **span block schema builder** `span_block_schema() -> SchemaRef` + column-name constants (`SCOL_TRACE_ID`, `SCOL_SPAN_ID`, `SCOL_PARENT_SPAN_ID`, `SCOL_NESTED_SET_LEFT`, `SCOL_NESTED_SET_RIGHT`, `SCOL_PARENT_ID`, `SCOL_NAME`, `SCOL_KIND`, `SCOL_START_NANO`, `SCOL_DURATION_NANOS`, `SCOL_STATUS_CODE`, `SCOL_ROOT_SERVICE_NAME`, `SCOL_ROOT_SPAN_NAME`, …). **Verify the exact `TraceIndex` + span-schema API against the slice-1 traces plan (`docs/superpowers/plans/2026-06-18-crabka-traces-slice1-blockstore.md`) before consuming; if a name differs, align to it.** `Labels`/`LabelMatcher`/`MatchOp` remain available.
- **`crabka-client-producer`** — `Producer::builder().bootstrap(..).build().await? -> Result<Producer, ProducerError>`; `Producer::send(ProducerRecord) -> impl Future<Output = oneshot::Receiver<Result<RecordMetadata, ProducerError>>>` (the call is `async`; await it, then await the returned `oneshot::Receiver` for the ack: `producer.send(rec).await.await??`); `ProducerRecord { topic:String, partition:Option<i32>, key:Option<Bytes>, value:Option<Bytes>, headers:Vec<Header>, timestamp_ms:Option<i64> }` (`Default`); `Producer::flush()`. **The producer hashes `key` with MurmurHash2 to choose a partition** — set `key` = the trace-id partition key and leave `partition: None`. (Verified against `crates/client-producer/src/{record,producer}.rs`.)
- **`crabka-client-consumer`** — `Consumer::builder().bootstrap(..).group_id(..).subscribe([..]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`; `Consumer::poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`; `Consumer::commit_sync() -> Result<(), ConsumerError>`; `ConsumerRecord { topic, partition:i32, offset:i64, key:Option<Bytes>, value:Option<Bytes>, .. }`. (Verified against `crates/client-consumer/src/{consumer,poll,commit}.rs`.)
- **`crabka-client-admin`** — `create_topics(&[CreateTopicSpec { name, partitions, replicas, configs }], timeout_ms) -> Result<Vec<CreateTopicOutcome>, AdminError>` (for tests + bootstrapping the WAL topic).
- **`crabka-broker` (dev-dependency, tests only)** — `BrokerConfig::for_tests(PathBuf)`, `Broker::start(config).await -> Result<BrokerHandle, BrokerError>`, `BrokerHandle::listen_addr()` (public; verified in `crates/broker/tests/support/mod.rs`).

**THIS slice defines (Slices 5/6/7 consume):** `SpanRecord` + `Span` (Tasks 3, 5) — the WAL topic record and the internal span model. `TRACES_WAL_TOPIC = "__crabka_traces_wal"`. `partition_key(trace_id: &[u8;16]) -> Bytes`. The block-builder's `span_batch(spans) -> RecordBatch` and `assign_nested_set(spans) -> Vec<NestedSet>`. The live-store's `LiveStore` (`MemTable` provider + offset rebuild).

**The 8 traces slices** (this plan = Slice 4):
1. Blockstore generalization + span schema + `TraceIndex`. 2. `crabka-traceql` core. 3. TraceQL completeness. **4. Ingest service *(this plan)*.** 5. Querier + Tempo HTTP API. 6. Query-frontend. 7. Metrics-generator. 8. Hardening.

---

## File structure (`crates/traces/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; ingest + blockstore + client deps; `opentelemetry-proto` with `trace` feature |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/error.rs` | `TracesError` + per-edge status-code mapping |
| `src/span/mod.rs` | internal `Span`, `SpanKind`, `StatusCode`, `KeyValue`, `EventRecord`, `LinkRecord` |
| `src/span/nested_set.rs` | `assign_nested_set` — DFS pre-order → `nested_set_left/right/parent_id` |
| `src/span/batch.rs` | `span_batch(&[Span]) -> RecordBatch` over the slice-1 span schema |
| `src/wire/mod.rs` | `WireFormat`, content negotiation, `WireError` + status codes |
| `src/wire/otlp.rs` | OTLP `TracesData` → `Vec<Span>` |
| `src/wire/zipkin.rs` | Zipkin v2 JSON `/api/v2/spans` → `Vec<Span>` |
| `src/wire/jaeger.rs` | Jaeger Thrift `Batch` + gRPC `PostSpansRequest` → `Vec<Span>` |
| `src/wal.rs` | `SpanRecord`, `TRACES_WAL_TOPIC`, `partition_key`, encode/decode |
| `src/distributor/mod.rs` | axum router (`/v1/traces`, `/api/v2/spans`, `/api/push`, Jaeger), serve, limits, produce |
| `src/blockbuilder.rs` | consumer-group loop → group by trace_id over window → blocks → index → commit |
| `src/livestore.rs` | consumer-group loop → in-memory recent traces → `MemTable`, rebuildable |
| `src/bin/crabka-traces.rs` | `clap` role-selectable entrypoint (`--target`) |
| `tests/ingest_roundtrip.rs` | end-to-end distributor → WAL → block-builder → block (in-process broker) |

Each file has one responsibility; `livestore.rs` and `blockbuilder.rs` are the only files that touch DataFusion / the blockstore writer, isolating the churn-prone surfaces.

---

### Task 1: Crate scaffold + dependency wiring + `trace` feature

**Files:**
- Create: `crates/traces/Cargo.toml`
- Create: `crates/traces/src/lib.rs`
- Create: `crates/traces/src/error.rs`
- Modify: root `Cargo.toml` (workspace members + add `trace` to the `opentelemetry-proto` feature set; add `thrift` if absent)

**Interfaces:**
- Produces: a compiling `crabka-traces` crate; `pub enum TracesError` (`thiserror`) with `fn status_code(&self) -> u16`; `pub fn crate_smoke() -> bool` (placeholder, removed in Task 2) so there is a test to run.

- [x] **Step 1: Add the crate to the workspace + enable trace types**

In root `Cargo.toml` `[workspace] members`, add `"crates/traces"`. Change the `opentelemetry-proto` workspace line to enable the trace types (it currently enables only `metrics`):

```toml
opentelemetry-proto = { version = "0.32", default-features = false, features = ["gen-tonic-messages", "metrics", "trace"] }
```

> **Verify against `~/.cargo/registry/src/*/opentelemetry-proto-0.32.0/Cargo.toml`:** the `trace` feature gates `opentelemetry_proto::tonic::trace::v1` (confirmed: `lib.rs` has `#[cfg(feature = "trace")] pub mod trace`). Adding `trace` is purely additive — the metrics signal's `metrics`-only usage is unaffected (greenfield, no compat concern). If `thrift` is not already a `[workspace.dependencies]` entry, add `thrift = "0.17"` for the Jaeger compact-Thrift decode (Task 9); confirm before relying on it.

- [x] **Step 2: Create `crates/traces/Cargo.toml`**

```toml
[package]
name = "crabka-traces"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Crabka traces ingest service (distributor + block-builder + live-store) — Grafana-Tempo replacement"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-traces"
readme = "README.md"
keywords = ["observability", "tracing", "tempo", "traceql", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
arrow = { workspace = true }
datafusion = { workspace = true }
object_store = { workspace = true }
opentelemetry-proto = { workspace = true }
prost = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "time", "signal"] }
tower = { workspace = true }
futures = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde-wincode = { workspace = true }
wincode = { workspace = true }
thrift = { workspace = true }
clap = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
url = { workspace = true }
crabka-blockstore = { path = "../blockstore" }
crabka-client-producer = { path = "../client-producer" }
crabka-client-consumer = { path = "../client-consumer" }
crabka-client-admin = { path = "../client-admin" }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
crabka-broker = { path = "../broker" }
crabka-client-core = { path = "../client-core" }
```

> **Verify each `{ workspace = true }` resolves** against root `Cargo.toml` (`serde_json`, `serde-wincode`, `wincode`, `tower`, `futures`, `url`, `bytes` are already workspace deps — used by sibling crates). `thrift` is added in Step 1 if absent.

- [x] **Step 3: Create `src/error.rs`**

```rust
//! Crate-wide error + the ingest-edge HTTP status mapping.

/// Errors across the traces ingest pipeline.
#[derive(Debug, thiserror::Error)]
pub enum TracesError {
    #[error("unsupported content-type: {0}")]
    UnsupportedContentType(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("wal codec: {0}")]
    Wal(String),
    #[error("produce failed: {0}")]
    Produce(String),
    #[error("block build failed: {0}")]
    Block(String),
}

impl TracesError {
    /// Map to the ingest-edge HTTP status (Tempo-shaped 4xx).
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            TracesError::UnsupportedContentType(_) => 415,
            TracesError::Decode(_)
            | TracesError::Invalid(_)
            | TracesError::TooLarge { .. } => 400,
            TracesError::Wal(_) | TracesError::Produce(_) | TracesError::Block(_) => 500,
        }
    }
}
```

- [x] **Step 4: Create `src/lib.rs` + a smoke test**

```rust
//! Crabka traces ingest service: distributor (OTLP/Jaeger/Zipkin/Tempo-native
//! push doors) → `trace_id`-partitioned WAL, the block-builder consumer group
//! (span Parquet blocks + `TraceIndex`), and the live-store hot tier.
#![forbid(unsafe_code)]

pub mod error;

pub use error::TracesError;

/// Placeholder so the crate has a test until Task 2 lands real modules.
#[must_use]
pub fn crate_smoke() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn smoke() {
        assert!(crate_smoke());
    }

    #[test]
    fn status_codes_map() {
        assert!(TracesError::UnsupportedContentType("x".into()).status_code() == 415);
        assert!(TracesError::Decode("x".into()).status_code() == 400);
    }
}
```

- [x] **Step 5: Build + run**

Run: `cargo test -p crabka-traces`
Expected: compiles; 2 tests PASS.

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add Cargo.toml Cargo.lock crates/traces/
git commit -m "feat(traces): scaffold crabka-traces crate + trace-proto feature + error type"
```

---

### Task 2: Internal `Span` model

**Files:**
- Create: `crates/traces/src/span/mod.rs`
- Modify: `crates/traces/src/lib.rs` (declare `pub mod span;`, drop the placeholder)

**Interfaces:**
- Produces (consumed by every `wire/*` decoder, the WAL record, and the block-builder):
  - `struct Span { pub trace_id:[u8;16], pub span_id:[u8;8], pub parent_span_id:Option<[u8;8]>, pub name:String, pub kind:SpanKind, pub start_ns:i64, pub duration_ns:i64, pub status:StatusCode, pub status_message:String, pub resource_attrs:Vec<KeyValue>, pub span_attrs:Vec<KeyValue>, pub events:Vec<EventRecord>, pub links:Vec<LinkRecord>, pub instrumentation_scope:String }` (`Clone, Debug, PartialEq`)
  - `enum SpanKind { Unspecified, Internal, Server, Client, Producer, Consumer }` (`as_i32`/`from_i32`)
  - `enum StatusCode { Unset, Ok, Error }` (`as_i32`/`from_i32`)
  - `struct KeyValue { pub key:String, pub value:AttrValue }`; `enum AttrValue { Str(String), Int(i64), Double(f64), Bool(bool), Bytes(Vec<u8>) }`
  - `struct EventRecord { pub time_unix_nano:i64, pub name:String, pub attrs:Vec<KeyValue> }`
  - `struct LinkRecord { pub trace_id:[u8;16], pub span_id:[u8;8], pub attrs:Vec<KeyValue> }`
  - All derive `serde::{Serialize, Deserialize}` (the WAL record encodes them).
  - `Span::is_root(&self) -> bool` (`parent_span_id.is_none()`).

- [x] **Step 1: Write the failing test**

In `crates/traces/src/span/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn span(parent: Option<[u8; 8]>) -> Span {
        Span {
            trace_id: [1u8; 16],
            span_id: [2u8; 8],
            parent_span_id: parent,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }],
            events: vec![],
            links: vec![],
            instrumentation_scope: "tracer".into(),
        }
    }

    #[test]
    fn root_detection() {
        assert!(span(None).is_root());
        assert!(!span(Some([3u8; 8])).is_root());
    }

    #[test]
    fn kind_round_trips_i32() {
        for k in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            assert!(SpanKind::from_i32(k.as_i32()) == k);
        }
    }

    #[test]
    fn status_round_trips_i32() {
        for s in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert!(StatusCode::from_i32(s.as_i32()) == s);
        }
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib span`
Expected: FAIL — `cannot find type Span`.

- [x] **Step 3: Implement `span/mod.rs`**

Prepend above the `tests` module:

```rust
//! The internal span model every push door lowers into. OTLP is the reference
//! shape (spec §4.1); Jaeger/Zipkin map onto it. The WAL record serializes this.

use serde::{Deserialize, Serialize};

pub mod batch;
pub mod nested_set;

/// OTLP span kind (spec §4.1 enum order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        self as i32
    }
    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }
}

/// OTLP status code (spec §4.1: `unset|ok|error`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        self as i32
    }
    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }
}

/// A typed attribute value (spec §4.1; arrays handled at block-build).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// One attribute key/value pair.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AttrValue,
}

/// A span event (`Event.time_since_start_nano` is reconstructed at block-build).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub time_unix_nano: i64,
    pub name: String,
    pub attrs: Vec<KeyValue>,
}

/// A span link (the linked trace/span).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinkRecord {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attrs: Vec<KeyValue>,
}

/// The internal span. One per OTLP/Jaeger/Zipkin span; one WAL record per span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: SpanKind,
    pub start_ns: i64,
    pub duration_ns: i64,
    pub status: StatusCode,
    pub status_message: String,
    pub resource_attrs: Vec<KeyValue>,
    pub span_attrs: Vec<KeyValue>,
    pub events: Vec<EventRecord>,
    pub links: Vec<LinkRecord>,
    pub instrumentation_scope: String,
}

impl Span {
    /// A root span has no parent (spec §6.3: roots share `parent_id = 0`).
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }
}
```

Add empty `pub mod batch;` / `pub mod nested_set;` files now (filled in Tasks 4 and 5) or comment them out until those tasks; declaring them empty keeps the tree compiling — create `batch.rs`/`nested_set.rs` with just a `//!` doc line for now.

- [x] **Step 4: Declare + run**

`lib.rs`: replace the placeholder with `pub mod span; pub use span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};`. Remove `crate_smoke` and its test (keep the `status_codes_map` test).

Run: `cargo test -p crabka-traces --lib span`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): internal Span model (OTLP-shaped, serde-derived)"
```

---

### Task 3: `SpanRecord` — the WAL topic record (Slices 5/6/7 consume this)

**Files:**
- Create: `crates/traces/src/wal.rs`
- Modify: `crates/traces/src/lib.rs`

**Interfaces:**
- Produces (the SHARED CONTRACT this slice owns):
  - `const TRACES_WAL_TOPIC: &str = "__crabka_traces_wal"`
  - `struct SpanRecord { pub tenant: String, pub span: Span }` (`serde`, `Clone`, `Debug`, `PartialEq`)
  - `SpanRecord::encode(&self) -> Result<Vec<u8>, TracesError>` / `SpanRecord::decode(&[u8]) -> Result<SpanRecord, TracesError>` (via `serde-wincode`).
  - `fn partition_key(trace_id: &[u8; 16]) -> Bytes` — the produce key; **all spans of a trace land on one partition** (spec §5.2 invariant). The producer MurmurHash2-partitions on this key.

- [x] **Step 1: Write the failing tests**

Create `crates/traces/src/wal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};
    use assert2::assert;

    fn span(trace_id: [u8; 16]) -> Span {
        Span {
            trace_id,
            span_id: [2u8; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![],
            events: vec![],
            links: vec![],
            instrumentation_scope: "tracer".into(),
        }
    }

    #[test]
    fn record_round_trips() {
        let rec = SpanRecord {
            tenant: "t1".into(),
            span: span([7u8; 16]),
        };
        let bytes = rec.encode().unwrap();
        let back = SpanRecord::decode(&bytes).unwrap();
        assert!(back == rec);
    }

    #[test]
    fn same_trace_id_same_partition_key() {
        let tid = [9u8; 16];
        // Two different spans of the SAME trace MUST get the same key (spec §5.2).
        let k1 = partition_key(&tid);
        let k2 = partition_key(&tid);
        let k3 = partition_key(&[10u8; 16]);
        assert!(k1 == k2);
        assert!(k1 != k3);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib wal`
Expected: FAIL — `cannot find type SpanRecord`.

- [x] **Step 3: Implement `wal.rs`**

```rust
//! The traces WAL topic record. Produced by the distributor, consumed by the
//! block-builder + live-store (this slice) and the querier hot head (Slice 5).
//! Encoded with `serde-wincode` (the codebase convention; see
//! `crates/broker/src/bootstrap.rs`).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::TracesError;
use crate::span::Span;

/// The traces WAL topic name.
pub const TRACES_WAL_TOPIC: &str = "__crabka_traces_wal";

/// One span's WAL record: tenant + the OTLP-derived span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub tenant: String,
    pub span: Span,
}

impl SpanRecord {
    /// Encode via `serde-wincode` (matches the broker's metadata-record codec).
    pub fn encode(&self) -> Result<Vec<u8>, TracesError> {
        <serde_wincode::SerdeCompat<SpanRecord> as wincode::Serialize>::serialize(self)
            .map_err(|e| TracesError::Wal(e.to_string()))
    }

    /// Decode a `SpanRecord` from its `serde-wincode` bytes.
    pub fn decode(bytes: &[u8]) -> Result<SpanRecord, TracesError> {
        <serde_wincode::SerdeCompat<SpanRecord>>::deserialize(bytes)
            .map_err(|e| TracesError::Wal(e.to_string()))
    }
}

/// The produce key: the raw `trace_id`. Because the producer MurmurHash2-hashes
/// `key` to pick a partition, every span of one trace lands on one partition —
/// the RF1 dedup-avoidance invariant (spec §1, §5.2). We pass the raw 16 bytes
/// rather than a pre-hash so the hashing stays the producer's single source.
#[must_use]
pub fn partition_key(trace_id: &[u8; 16]) -> Bytes {
    Bytes::copy_from_slice(trace_id)
}
```

> **Verify the `serde-wincode` call shape** against `crates/broker/src/bootstrap.rs` / `crates/metadata/src/kraft_translate.rs`: `<SerdeCompat<T> as wincode::Serialize>::serialize(&value) -> Result<Vec<u8>, _>` and `<SerdeCompat<T>>::deserialize(&[u8]) -> Result<T, _>`. If the trait import differs, match the codebase exactly (the metrics slice-4 plan's Task 5 used the identical shape). The partition-key bytes are an internal contract; keep them deterministic and equal for equal `trace_id`.

- [x] **Step 4: Declare + run**

`lib.rs`: `pub mod wal; pub use wal::{partition_key, SpanRecord, TRACES_WAL_TOPIC};`

Run: `cargo test -p crabka-traces --lib wal`
Expected: PASS (2 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): SpanRecord WAL record + serde-wincode codec + trace_id partition key (slice-4 contract)"
```

---

### Task 4: Nested-set DFS pre-order (`assign_nested_set`)

**Files:**
- Create: `crates/traces/src/span/nested_set.rs` (overwrite the placeholder)

**Interfaces:**
- Produces (consumed by the block-builder batch builder, Task 11):
  - `struct NestedSet { pub left: i32, pub right: i32, pub parent_id: i32 }`
  - `fn assign_nested_set(spans: &[Span]) -> Vec<NestedSet>` — modified pre-order traversal over each trace's span tree (spec §4.1, §6.3): an ancestor's `[left,right]` strictly contains every descendant's; `parent_id(child) == parent.left`; **roots share `parent_id = 0`** (the sentinel). Output is index-aligned with `spans`.

> The block-builder calls this on the spans of **one trace** (already grouped by `trace_id`). Multiple roots / orphan parents (a span whose `parent_span_id` is not in the set — a late/missing parent) are treated as roots (`parent_id = 0`), matching the spec's two-roots-are-siblings rule and the late-span reality. Disconnected forests get disjoint `[left,right]` ranges from one shared counter, so structural predicates never cross trees within the trace partition (they can't — same `trace_id`).

- [x] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};
    use assert2::assert;

    fn span(id: u8, parent: Option<u8>) -> Span {
        Span {
            trace_id: [1u8; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![],
            events: vec![],
            links: vec![],
            instrumentation_scope: String::new(),
        }
    }

    #[test]
    fn ancestor_interval_contains_descendants() {
        // tree: 1 (root) -> 2 -> 3 ; 1 -> 4
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(2)),
            span(4, Some(1)),
        ];
        let ns = assign_nested_set(&spans);
        let root = &ns[0];
        // every other node's [left,right] is strictly inside the root's.
        for child in &ns[1..] {
            assert!(child.left > root.left);
            assert!(child.right < root.right);
        }
        // 3 is inside 2.
        assert!(ns[2].left > ns[1].left);
        assert!(ns[2].right < ns[1].right);
        // 4 is NOT inside 2.
        assert!(!(ns[3].left > ns[1].left && ns[3].right < ns[1].right));
    }

    #[test]
    fn child_parent_id_equals_parent_left() {
        let spans = vec![span(1, None), span(2, Some(1))];
        let ns = assign_nested_set(&spans);
        assert!(ns[1].parent_id == ns[0].left);
    }

    #[test]
    fn roots_share_zero_parent_id() {
        // two roots (or an orphan whose parent is missing) → parent_id 0.
        let spans = vec![span(1, None), span(2, Some(99))]; // 99 absent → orphan root
        let ns = assign_nested_set(&spans);
        assert!(ns[0].parent_id == 0);
        assert!(ns[1].parent_id == 0);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib span::nested_set`
Expected: FAIL — `cannot find function assign_nested_set`.

- [x] **Step 3: Implement `nested_set.rs`**

```rust
//! Modified pre-order traversal (nested-set model). At block-build the
//! block-builder runs this over each trace's spans to fill the structural
//! `nested_set_left/right/parent_id` columns (spec §4.1, §6.3) — the columns
//! that turn TraceQL structural operators into cheap integer self-join
//! predicates instead of tree walks.

use std::collections::HashMap;

use crate::span::Span;

/// One span's nested-set assignment (index-aligned with the input spans).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}

/// Assign `[left, right]` intervals + `parent_id` via DFS pre-order over the
/// span tree of ONE trace. `left` is assigned on entry, `right` on exit, so an
/// ancestor strictly contains every descendant. `parent_id(child)` is the
/// parent's `left`; roots (and orphans whose parent is absent) get `0`.
#[must_use]
pub fn assign_nested_set(spans: &[Span]) -> Vec<NestedSet> {
    // span_id -> index, to resolve parent links and build the children map.
    let mut index_of: HashMap<[u8; 8], usize> = HashMap::with_capacity(spans.len());
    for (i, s) in spans.iter().enumerate() {
        index_of.insert(s.span_id, i);
    }

    // children[i] = indices of spans whose parent is span i (preserving input
    // order for deterministic intervals). Roots = spans with no parent, or whose
    // parent is not present in this trace's set (orphan → treated as root).
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
    let mut roots: Vec<usize> = Vec::new();
    for (i, s) in spans.iter().enumerate() {
        match s.parent_span_id.and_then(|p| index_of.get(&p).copied()) {
            Some(parent_idx) => children[parent_idx].push(i),
            None => roots.push(i),
        }
    }

    let mut out = vec![NestedSet::default(); spans.len()];
    let mut counter: i32 = 1;
    // Iterative DFS with explicit (node, parent_left, entered?) frames.
    // parent_left = the left value of the parent (0 for roots = sentinel).
    let mut stack: Vec<(usize, i32, bool)> =
        roots.iter().rev().map(|&r| (r, 0, false)).collect();

    while let Some((node, parent_left, entered)) = stack.pop() {
        if entered {
            out[node].right = counter;
            counter += 1;
        } else {
            out[node].left = counter;
            out[node].parent_id = parent_left;
            counter += 1;
            // Re-push this node as "exit" AFTER its children, then push children.
            stack.push((node, parent_left, true));
            let my_left = out[node].left;
            for &c in children[node].iter().rev() {
                stack.push((c, my_left, false));
            }
        }
    }
    out
}
```

> **Determinism:** intervals depend on (a) input order of roots and (b) input order of children — both preserved here. The block-builder must sort spans into a stable order before calling (Task 11 sorts by `(start_ns, span_id)` within a trace) so blocks are reproducible (crash-recovery idempotency, spec §9). The `i32` counter caps at ~2.1B half-steps per trace — far beyond any real trace; if a trace somehow exceeds it, that is a pathological input the limiter (Task 8/distributor) rejects upstream.

- [x] **Step 4: Run**

Run: `cargo test -p crabka-traces --lib span::nested_set`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): nested-set DFS pre-order (left/right/parent_id, roots share sentinel 0)"
```

---

### Task 5: OTLP traces decode (`wire/otlp.rs`)

**Files:**
- Create: `crates/traces/src/wire/mod.rs` (`WireFormat`, `WireError`, content negotiation)
- Create: `crates/traces/src/wire/otlp.rs`
- Modify: `crates/traces/src/lib.rs`

**Interfaces:**
- Consumes: `opentelemetry_proto::tonic::trace::v1::{TracesData, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status, span::{Event, Link, SpanKind as OtlpKind}}`, `opentelemetry_proto::tonic::common::v1::{KeyValue as OtlpKv, AnyValue, any_value::Value}`.
- Produces:
  - `enum WireFormat { Otlp, Zipkin, Jaeger }`
  - `fn negotiate(path: &str, content_type: Option<&str>) -> Result<WireFormat, WireError>`
  - `enum WireError` (re-uses `TracesError` mapping) with `fn status_code(&self) -> u16`
  - `fn decode_otlp(data: &TracesData) -> Result<Vec<Span>, WireError>` — flattens `ResourceSpans → ScopeSpans → Span` to internal `Span`s, copying resource attrs onto every span row (spec §4.1 denormalization), converting `trace_id`/`span_id` `Vec<u8>` → fixed arrays, `start_time_unix_nano`/`end_time_unix_nano` → `start_ns`/`duration_ns`.

- [x] **Step 1: Write the failing tests**

Create `crates/traces/src/wire/otlp.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{AttrValue, SpanKind, StatusCode};
    use assert2::assert;
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value, AnyValue, KeyValue as OtlpKv,
    };
    use opentelemetry_proto::tonic::trace::v1::{
        span::SpanKind as OtlpKind, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status,
        TracesData,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn kv(k: &str, v: &str) -> OtlpKv {
        OtlpKv {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.into())),
            }),
        }
    }

    fn data() -> TracesData {
        let otlp_span = OtlpSpan {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            parent_span_id: vec![], // root: empty
            name: "GET /".into(),
            kind: OtlpKind::Server as i32,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_500,
            attributes: vec![kv("http.method", "GET")],
            status: Some(Status {
                code: 1, // STATUS_CODE_OK
                message: String::new(),
            }),
            ..Default::default()
        };
        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "api")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![otlp_span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn decodes_one_span_with_resource_attrs() {
        let spans = decode_otlp(&data()).unwrap();
        assert!(spans.len() == 1);
        let s = &spans[0];
        assert!(s.trace_id == [1u8; 16]);
        assert!(s.span_id == [2u8; 8]);
        assert!(s.parent_span_id == None);
        assert!(s.kind == SpanKind::Server);
        assert!(s.status == StatusCode::Ok);
        assert!(s.start_ns == 1_000);
        assert!(s.duration_ns == 500); // end - start
        // resource attr is carried on the span.
        assert!(s.resource_attrs.iter().any(|a| a.key == "service.name"
            && a.value == AttrValue::Str("api".into())));
        assert!(s.span_attrs.iter().any(|a| a.key == "http.method"));
    }

    #[test]
    fn rejects_wrong_length_trace_id() {
        let mut d = data();
        d.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![1u8; 8]; // wrong
        assert!(decode_otlp(&d).is_err());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib wire::otlp`
Expected: FAIL — `cannot find function decode_otlp`.

- [x] **Step 3: Implement `wire/mod.rs`**

```rust
//! Push-door wire surfaces: format negotiation + the three decoders, each
//! lowering into the internal `Span` (spec §5.1).

pub mod jaeger;
pub mod otlp;
pub mod zipkin;

use crate::error::TracesError;

/// Which push door a request arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    Otlp,
    Zipkin,
    Jaeger,
}

/// Ingest-edge wire error (delegates the status mapping to `TracesError`).
pub type WireError = TracesError;

/// Pick the decoder from the request path + content-type. `/v1/traces` is OTLP
/// (HTTP-protobuf), `/api/v2/spans` is Zipkin JSON, the Jaeger collector paths
/// (`/api/traces`, gRPC `PostSpans`) are Jaeger.
pub fn negotiate(path: &str, content_type: Option<&str>) -> Result<WireFormat, WireError> {
    match path {
        "/v1/traces" => Ok(WireFormat::Otlp),
        "/api/v2/spans" => Ok(WireFormat::Zipkin),
        "/api/traces" => Ok(WireFormat::Jaeger),
        // Tempo-native /api/push carries OTLP protobuf.
        "/api/push" => Ok(WireFormat::Otlp),
        other => Err(WireError::UnsupportedContentType(format!(
            "{other} (content-type {})",
            content_type.unwrap_or("none")
        ))),
    }
}
```

Create placeholder `wire/zipkin.rs` and `wire/jaeger.rs` with a `//!` doc line (filled in Tasks 6, 9) so the module tree compiles.

- [x] **Step 4: Implement `wire/otlp.rs`** (prepend above `tests`)

```rust
//! OTLP `TracesData` → `Vec<Span>`. Flattens `ResourceSpans → ScopeSpans →
//! Span`, copying resource attrs onto every span row (spec §4.1 denormalization).

use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue as OtlpKv};
use opentelemetry_proto::tonic::trace::v1::{span::SpanKind as OtlpKind, Status, TracesData};

use super::WireError;
use crate::span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};

fn fixed16(bytes: &[u8]) -> Result<[u8; 16], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("trace_id must be 16 bytes, got {}", bytes.len())))
}

fn fixed8(bytes: &[u8]) -> Result<[u8; 8], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Invalid(format!("span_id must be 8 bytes, got {}", bytes.len())))
}

fn any_to_attr(v: &AnyValue) -> Option<AttrValue> {
    match v.value.as_ref()? {
        Value::StringValue(s) => Some(AttrValue::Str(s.clone())),
        Value::IntValue(i) => Some(AttrValue::Int(*i)),
        Value::DoubleValue(d) => Some(AttrValue::Double(*d)),
        Value::BoolValue(b) => Some(AttrValue::Bool(*b)),
        Value::BytesValue(b) => Some(AttrValue::Bytes(b.clone())),
        // Array/KVList values are flattened to JSON text downstream; defer to
        // the array-aware block builder (spec §4.1) — keep the raw string here.
        _ => None,
    }
}

fn kvs(attrs: &[OtlpKv]) -> Vec<KeyValue> {
    attrs
        .iter()
        .filter_map(|a| {
            let value = any_to_attr(a.value.as_ref()?)?;
            Some(KeyValue { key: a.key.clone(), value })
        })
        .collect()
}

fn status_of(s: Option<&Status>) -> (StatusCode, String) {
    match s {
        Some(st) => (StatusCode::from_i32(st.code), st.message.clone()),
        None => (StatusCode::Unset, String::new()),
    }
}

fn kind_of(k: i32) -> SpanKind {
    // OTLP SpanKind enum order matches our SpanKind order (Unspecified..Consumer).
    SpanKind::from_i32(if k == OtlpKind::Unspecified as i32 { 0 } else { k })
}

/// Decode OTLP `TracesData` into internal spans.
pub fn decode_otlp(data: &TracesData) -> Result<Vec<Span>, WireError> {
    let mut out = Vec::new();
    for rs in &data.resource_spans {
        let resource_attrs = rs
            .resource
            .as_ref()
            .map(|r| kvs(&r.attributes))
            .unwrap_or_default();
        for ss in &rs.scope_spans {
            let scope_name = ss
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            for sp in &ss.spans {
                let parent = if sp.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed8(&sp.parent_span_id)?)
                };
                let (status, status_message) = status_of(sp.status.as_ref());
                let events = sp
                    .events
                    .iter()
                    .map(|e| EventRecord {
                        time_unix_nano: i64::try_from(e.time_unix_nano).unwrap_or(i64::MAX),
                        name: e.name.clone(),
                        attrs: kvs(&e.attributes),
                    })
                    .collect();
                let links = sp
                    .links
                    .iter()
                    .map(|l| {
                        Ok(LinkRecord {
                            trace_id: fixed16(&l.trace_id)?,
                            span_id: fixed8(&l.span_id)?,
                            attrs: kvs(&l.attributes),
                        })
                    })
                    .collect::<Result<Vec<_>, WireError>>()?;
                out.push(Span {
                    trace_id: fixed16(&sp.trace_id)?,
                    span_id: fixed8(&sp.span_id)?,
                    parent_span_id: parent,
                    name: sp.name.clone(),
                    kind: kind_of(sp.kind),
                    start_ns: i64::try_from(sp.start_time_unix_nano).unwrap_or(i64::MAX),
                    duration_ns: i64::try_from(
                        sp.end_time_unix_nano.saturating_sub(sp.start_time_unix_nano),
                    )
                    .unwrap_or(i64::MAX),
                    status,
                    status_message,
                    resource_attrs: resource_attrs.clone(),
                    span_attrs: kvs(&sp.attributes),
                    events,
                    links,
                    instrumentation_scope: scope_name.clone(),
                });
            }
        }
    }
    Ok(out)
}
```

> **Verify against the generated `opentelemetry-proto` 0.32 trace types** (`~/.cargo/registry/src/*/opentelemetry-proto-0.32.0/src/proto/tonic/opentelemetry.proto.trace.v1.rs`): `Span.trace_id`/`span_id`/`parent_span_id` are `Vec<u8>` (confirmed); `Span.events`/`links` are `Vec<span::Event>`/`Vec<span::Link>`; `Status { code: i32, message: String }`; `span::SpanKind` enum order is `Unspecified, Internal, Server, Client, Producer, Consumer` (matches our `SpanKind`). `AnyValue::value` is `Option<any_value::Value>` with `StringValue/BoolValue/IntValue/DoubleValue/BytesValue/ArrayValue/KvlistValue`. If a field name differs, align the code — do not change the asserted behavior.

- [x] **Step 5: Declare + run**

`lib.rs`: `pub mod wire; pub use wire::{negotiate, WireFormat};`

Run: `cargo test -p crabka-traces --lib wire::otlp`
Expected: PASS (2 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): OTLP traces decode + push-door negotiation"
```

---

### Task 6: Zipkin v2 JSON decode (`wire/zipkin.rs`)

**Files:**
- Create: `crates/traces/src/wire/zipkin.rs` (overwrite the placeholder)

**Interfaces:**
- Produces:
  - `fn decode_zipkin(body: &[u8]) -> Result<Vec<Span>, WireError>` — parse the Zipkin v2 `[ {span}, ... ]` JSON array (`/api/v2/spans`), mapping each Zipkin span to an internal `Span`: hex `traceId`/`id`/`parentId` → fixed byte arrays (Zipkin `traceId` may be 16 *or* 32 hex chars → 8 or 16 bytes, left-padded to 16); `timestamp` (epoch micros) → `start_ns`; `duration` (micros) → `duration_ns`; `kind` (`SERVER`/`CLIENT`/…) → `SpanKind`; `localEndpoint.serviceName` → a `service.name` resource attr; `tags` map → span attrs.

- [x] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{AttrValue, SpanKind};
    use assert2::assert;

    const BODY: &str = r#"[
      {
        "traceId": "0000000000000001",
        "id": "0000000000000002",
        "name": "get /",
        "timestamp": 1000,
        "duration": 500,
        "kind": "SERVER",
        "localEndpoint": { "serviceName": "api" },
        "tags": { "http.method": "GET" }
      }
    ]"#;

    #[test]
    fn decodes_zipkin_span() {
        let spans = decode_zipkin(BODY.as_bytes()).unwrap();
        assert!(spans.len() == 1);
        let s = &spans[0];
        // 16-hex-char traceId → 8 bytes, left-padded into [u8;16].
        assert!(s.trace_id[15] == 1);
        assert!(s.span_id[7] == 2);
        assert!(s.kind == SpanKind::Server);
        // micros → nanos.
        assert!(s.start_ns == 1_000_000);
        assert!(s.duration_ns == 500_000);
        assert!(s.resource_attrs.iter().any(|a| a.key == "service.name"
            && a.value == AttrValue::Str("api".into())));
        assert!(s.span_attrs.iter().any(|a| a.key == "http.method"));
    }

    #[test]
    fn rejects_odd_length_hex_id() {
        let bad = r#"[ { "traceId": "xyz", "id": "0000000000000002", "name": "x" } ]"#;
        assert!(decode_zipkin(bad.as_bytes()).is_err());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib wire::zipkin`
Expected: FAIL — `cannot find function decode_zipkin`.

- [x] **Step 3: Implement `wire/zipkin.rs`**

```rust
//! Zipkin v2 JSON (`POST /api/v2/spans`) → `Vec<Span>` (spec §5.1).

use std::collections::BTreeMap;

use serde::Deserialize;

use super::WireError;
use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

#[derive(Deserialize)]
struct ZipkinEndpoint {
    #[serde(rename = "serviceName")]
    service_name: Option<String>,
}

#[derive(Deserialize)]
struct ZipkinSpan {
    #[serde(rename = "traceId")]
    trace_id: String,
    id: String,
    #[serde(rename = "parentId")]
    parent_id: Option<String>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    timestamp: i64, // epoch micros
    #[serde(default)]
    duration: i64, // micros
    #[serde(default)]
    kind: Option<String>,
    #[serde(rename = "localEndpoint")]
    local_endpoint: Option<ZipkinEndpoint>,
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

/// Decode a hex id into a fixed byte array, left-padded to `N`.
fn hex_fixed<const N: usize>(hex: &str) -> Result<[u8; N], WireError> {
    if hex.len() % 2 != 0 || hex.len() > N * 2 {
        return Err(WireError::Invalid(format!("bad hex id {hex:?}")));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.as_bytes().chunks_exact(2);
    for pair in &mut chars {
        let s = std::str::from_utf8(pair).map_err(|_| WireError::Invalid("non-utf8 hex".into()))?;
        bytes.push(u8::from_str_radix(s, 16).map_err(|_| WireError::Invalid(format!("bad hex {s}")))?);
    }
    let mut out = [0u8; N];
    out[N - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

fn zipkin_kind(k: Option<&str>) -> SpanKind {
    match k {
        Some("SERVER") => SpanKind::Server,
        Some("CLIENT") => SpanKind::Client,
        Some("PRODUCER") => SpanKind::Producer,
        Some("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

/// Decode a Zipkin v2 JSON span array.
pub fn decode_zipkin(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let raw: Vec<ZipkinSpan> =
        serde_json::from_slice(body).map_err(|e| WireError::Decode(e.to_string()))?;
    let mut out = Vec::with_capacity(raw.len());
    for z in raw {
        let resource_attrs = z
            .local_endpoint
            .and_then(|e| e.service_name)
            .map(|svc| {
                vec![KeyValue {
                    key: "service.name".into(),
                    value: AttrValue::Str(svc),
                }]
            })
            .unwrap_or_default();
        let span_attrs = z
            .tags
            .into_iter()
            .map(|(k, v)| KeyValue { key: k, value: AttrValue::Str(v) })
            .collect();
        let parent = match z.parent_id {
            Some(p) => Some(hex_fixed::<8>(&p)?),
            None => None,
        };
        out.push(Span {
            trace_id: hex_fixed::<16>(&z.trace_id)?,
            span_id: hex_fixed::<8>(&z.id)?,
            parent_span_id: parent,
            name: z.name,
            kind: zipkin_kind(z.kind.as_deref()),
            start_ns: z.timestamp.saturating_mul(1_000),
            duration_ns: z.duration.saturating_mul(1_000),
            status: StatusCode::Unset,
            status_message: String::new(),
            resource_attrs,
            span_attrs,
            events: vec![],
            links: vec![],
            instrumentation_scope: String::new(),
        });
    }
    Ok(out)
}
```

> **Zipkin v2 shape reference:** the openzipkin v2 JSON span list is the stable contract (`traceId`/`id`/`parentId` are lowercase-hex; `timestamp`/`duration` are epoch+span micros; `kind` ∈ `{CLIENT,SERVER,PRODUCER,CONSUMER}`; `localEndpoint.serviceName`; `tags` is a flat string→string map; `annotations` carry timestamped events — events mapping is `// TODO(slice4-zipkin-annotations)`, a focused follow-on; the harder OTLP path carries events fully). Cross-check against the openzipkin `zipkin2.v2` JSON doc; if Grafana's Zipkin reporter sends 32-hex traceIds, the `hex_fixed::<16>` left-pad already handles 8-or-16-byte ids.

- [x] **Step 4: Run**

Run: `cargo test -p crabka-traces --lib wire::zipkin`
Expected: PASS (2 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): Zipkin v2 JSON decode"
```

---

### Task 7: Span Arrow batch builder (`span/batch.rs`)

**Files:**
- Create: `crates/traces/src/span/batch.rs` (overwrite the placeholder)

**Interfaces:**
- Consumes: the slice-1 `crabka_blockstore::{span_block_schema, SCOL_*}` constants + `assign_nested_set` (Task 4).
- Produces:
  - `fn span_batch(spans: &[Span]) -> Result<RecordBatch, TracesError>` — builds one `RecordBatch` over the slice-1 span schema for a set of spans **already grouped by trace and ordered**, filling identity columns, the nested-set columns (via `assign_nested_set` per trace), span intrinsics, and the trace-denormalized root columns (root service/name, trace start/duration). Generic attrs + events/links are encoded into their list/struct columns.

> **This task is bounded against the slice-1 span schema (churn-prone).** The exact Arrow column list + `span_block_schema()` is owned by the slice-1 traces-blockstore plan. Implement against the column-name constants it exports; if a column is named/typed differently, align to slice-1. The behavior the tests pin (identity bytes round-trip, nested-set columns present and consistent, root columns denormalized) is stable regardless of exact arrow builder calls.

- [x] **Step 1: Write the failing test (against the real schema)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};
    use assert2::assert;
    use arrow::array::{Array, FixedSizeBinaryArray, Int32Array, StringArray};

    fn span(id: u8, parent: Option<u8>, root_svc: &str) -> Span {
        Span {
            trace_id: [1u8; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: SpanKind::Server,
            start_ns: i64::from(id) * 10,
            duration_ns: 5,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str(root_svc.into()),
            }],
            span_attrs: vec![],
            events: vec![],
            links: vec![],
            instrumentation_scope: String::new(),
        }
    }

    fn col<'a, A: 'static>(b: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a A {
        let idx = b.schema().index_of(name).unwrap();
        b.column(idx).as_any().downcast_ref::<A>().unwrap()
    }

    #[test]
    fn builds_batch_with_identity_and_nested_set() {
        use crabka_blockstore::{
            SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID, SCOL_ROOT_SERVICE_NAME,
            SCOL_SPAN_ID, SCOL_TRACE_ID,
        };
        // 1 (root) -> 2
        let spans = vec![span(1, None, "api"), span(2, Some(1), "api")];
        let batch = span_batch(&spans).unwrap();
        assert!(batch.num_rows() == 2);

        let tids = col::<FixedSizeBinaryArray>(&batch, SCOL_TRACE_ID);
        assert!(tids.value(0) == &[1u8; 16]);
        let sids = col::<FixedSizeBinaryArray>(&batch, SCOL_SPAN_ID);
        assert!(sids.value(0) == &[1u8; 8]);

        let left = col::<Int32Array>(&batch, SCOL_NESTED_SET_LEFT);
        let right = col::<Int32Array>(&batch, SCOL_NESTED_SET_RIGHT);
        let pid = col::<Int32Array>(&batch, SCOL_PARENT_ID);
        // child(2) interval strictly inside root(1).
        assert!(left.value(1) > left.value(0));
        assert!(right.value(1) < right.value(0));
        // child's parent_id == root's left.
        assert!(pid.value(1) == left.value(0));
        // root's parent_id is the sentinel 0.
        assert!(pid.value(0) == 0);

        let svc = col::<StringArray>(&batch, SCOL_ROOT_SERVICE_NAME);
        assert!(svc.value(0) == "api");
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib span::batch`
Expected: FAIL — `cannot find function span_batch` (or, if slice-1 constants aren't exported yet, a missing-import error — that means slice 1 isn't merged; this slice depends on it).

- [x] **Step 3: Implement `span/batch.rs`**

Structure (fill the builders against the slice-1 schema; the arrow builder calls are the churn-prone part — pin the column names from `crabka_blockstore`):

```rust
//! Build one span `RecordBatch` over the slice-1 span block schema. Spans must
//! arrive grouped by trace and ordered (the block-builder sorts them); we run
//! the nested-set DFS per trace and denormalize the root columns onto each row.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    span_block_schema, SCOL_DURATION_NANOS, SCOL_KIND, SCOL_NAME, SCOL_NESTED_SET_LEFT,
    SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID, SCOL_PARENT_SPAN_ID, SCOL_ROOT_SERVICE_NAME,
    SCOL_ROOT_SPAN_NAME, SCOL_SPAN_ID, SCOL_START_NANO, SCOL_STATUS_CODE, SCOL_TRACE_ID,
};

use super::nested_set::assign_nested_set;
use super::Span;
use crate::error::TracesError;
use crate::span::AttrValue;

/// Find the root service name + root span name for a trace (the root span, or
/// the earliest-start span if no explicit root), denormalized onto every row.
fn root_info(spans: &[Span]) -> (String, String, i64, i64) {
    let root = spans
        .iter()
        .find(|s| s.is_root())
        .or_else(|| spans.iter().min_by_key(|s| s.start_ns));
    let svc = root
        .and_then(|r| {
            r.resource_attrs
                .iter()
                .find(|a| a.key == "service.name")
                .and_then(|a| match &a.value {
                    AttrValue::Str(s) => Some(s.clone()),
                    _ => None,
                })
        })
        .unwrap_or_default();
    let name = root.map(|r| r.name.clone()).unwrap_or_default();
    let start = spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
    let end = spans.iter().map(|s| s.start_ns + s.duration_ns).max().unwrap_or(0);
    (svc, name, start, end - start)
}

/// Build a span `RecordBatch` from spans of ONE trace (already ordered).
pub fn span_batch(spans: &[Span]) -> Result<RecordBatch, TracesError> {
    let schema = span_block_schema();
    let ns = assign_nested_set(spans);
    let (root_svc, root_name, _trace_start, _trace_dur) = root_info(spans);

    let n = spans.len();
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut left = Int32Builder::with_capacity(n);
    let mut right = Int32Builder::with_capacity(n);
    let mut parent_id = Int32Builder::with_capacity(n);
    let mut name = StringBuilder::new();
    let mut kind = Int32Builder::with_capacity(n);
    let mut start_ns = Int64Builder::with_capacity(n);
    let mut duration_ns = Int64Builder::with_capacity(n);
    let mut status_code = Int32Builder::with_capacity(n);
    let mut root_service = StringBuilder::new();
    let mut root_span = StringBuilder::new();

    for (i, s) in spans.iter().enumerate() {
        trace_id.append_value(s.trace_id).map_err(|e| TracesError::Block(e.to_string()))?;
        span_id.append_value(s.span_id).map_err(|e| TracesError::Block(e.to_string()))?;
        // parent_span_id is nullable: roots get a null.
        match s.parent_span_id {
            Some(p) => parent_span_id.append_value(p).map_err(|e| TracesError::Block(e.to_string()))?,
            None => parent_span_id.append_null(),
        }
        left.append_value(ns[i].left);
        right.append_value(ns[i].right);
        parent_id.append_value(ns[i].parent_id);
        name.append_value(&s.name);
        kind.append_value(s.kind.as_i32());
        start_ns.append_value(s.start_ns);
        duration_ns.append_value(s.duration_ns);
        status_code.append_value(s.status.as_i32());
        root_service.append_value(&root_svc);
        root_span.append_value(&root_name);
    }

    // Assemble columns in the schema's declared order, keyed by name.
    let by_name: Vec<(&str, ArrayRef)> = vec![
        (SCOL_TRACE_ID, Arc::new(trace_id.finish())),
        (SCOL_SPAN_ID, Arc::new(span_id.finish())),
        (SCOL_PARENT_SPAN_ID, Arc::new(parent_span_id.finish())),
        (SCOL_NESTED_SET_LEFT, Arc::new(left.finish())),
        (SCOL_NESTED_SET_RIGHT, Arc::new(right.finish())),
        (SCOL_PARENT_ID, Arc::new(parent_id.finish())),
        (SCOL_NAME, Arc::new(name.finish())),
        (SCOL_KIND, Arc::new(kind.finish())),
        (SCOL_START_NANO, Arc::new(start_ns.finish())),
        (SCOL_DURATION_NANOS, Arc::new(duration_ns.finish())),
        (SCOL_STATUS_CODE, Arc::new(status_code.finish())),
        (SCOL_ROOT_SERVICE_NAME, Arc::new(root_service.finish())),
        (SCOL_ROOT_SPAN_NAME, Arc::new(root_span.finish())),
    ];
    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            by_name
                .iter()
                .find(|(n, _)| *n == f.name())
                .map(|(_, a)| a.clone())
                .ok_or_else(|| TracesError::Block(format!("missing column {}", f.name())))
        })
        .collect::<Result<Vec<_>, TracesError>>()?;
    RecordBatch::try_new(schema, columns).map_err(|e| TracesError::Block(e.to_string()))
}
```

> **Verify against the slice-1 span schema:** the column constants + `span_block_schema()` are slice-1's. This implementation assumes the schema contains exactly the listed columns. If slice-1 adds the generic-attribute LIST columns + the events/links struct columns to the mandatory schema (spec §4.1), extend `by_name` to build them (empty/null lists are valid) — the `ok_or_else("missing column")` guard makes a schema mismatch a loud test failure, not silent corruption. Attribute-promotion dedicated columns are configured at block-build; the default-empty promotion set means this slice writes only the listed columns — promotion wiring is `// TODO(slice4-attr-promotion)`.

- [x] **Step 4: Run**

Run: `cargo test -p crabka-traces --lib span::batch`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): span Arrow batch builder (identity + nested-set + denormalized root columns)"
```

---

### Task 8: Distributor axum server — routes, limits, produce

**Files:**
- Create: `crates/traces/src/distributor/mod.rs`
- Modify: `crates/traces/src/lib.rs`

**Interfaces:**
- Produces:
  - `struct TenantLimits { pub max_spans_per_request: usize, pub max_attr_value_len: usize }` (`Default`)
  - `trait WalSink: Send + Sync { async fn append(&self, rec: SpanRecord) -> Result<(), TracesError>; }` (async-trait or `impl Future` return; use the codebase's async-trait convention)
  - `struct KafkaSink { producer: Arc<Producer> }` impl `WalSink` — builds a `ProducerRecord { topic: TRACES_WAL_TOPIC, key: Some(partition_key(&trace_id)), value: Some(encoded), partition: None, .. }` and awaits the ack.
  - `struct DistributorState { sink: Arc<dyn WalSink>, limits: TenantLimits, max_decompressed: usize }`
  - `fn router(state: Arc<DistributorState>) -> axum::Router` — routes `POST /v1/traces`, `POST /api/v2/spans`, `POST /api/push`.
  - `fn validate(spans: &[Span], limits: &TenantLimits) -> Result<(), TracesError>`
  - `async fn produce_spans(sink: &dyn WalSink, tenant: &str, spans: Vec<Span>) -> Result<(), TracesError>`
  - `async fn serve(addr: SocketAddr, state: Arc<DistributorState>, shutdown: CancellationToken) -> std::io::Result<SocketAddr>`

**Tenant** comes from the `X-Scope-OrgID` header (Tempo convention).

- [x] **Step 1: Write the failing handler tests (in-process, no broker)**

Use `tower::ServiceExt::oneshot` (the `metrics_server.rs` pattern) with a recording fake `WalSink` so no broker is needed.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use prost::Message;
    use std::sync::Arc;
    use tower::ServiceExt as _;

    // OTLP request body: a TracesData protobuf (NOT snappy — OTLP/HTTP is raw
    // protobuf with optional gzip; we accept identity here).
    fn otlp_body() -> Vec<u8> {
        use opentelemetry_proto::tonic::trace::v1::{
            ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData,
        };
        TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![OtlpSpan {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        name: "GET /".into(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_500,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn otlp_push_returns_200_and_appends() {
        let state = test_state(); // recording fake sink behind DistributorState
        let app = router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        assert!(state.appended_count() == 1);
    }

    #[tokio::test]
    async fn zipkin_push_returns_202_and_appends() {
        let state = test_state();
        let app = router(state.clone());
        let body = r#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"x","timestamp":1,"duration":1}]"#;
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v2/spans")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "t")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Zipkin returns 202 Accepted on success.
        assert!(resp.status() == StatusCode::ACCEPTED);
        assert!(state.appended_count() == 1);
    }

    #[tokio::test]
    async fn over_span_limit_is_400() {
        let mut limits = TenantLimits::default();
        limits.max_spans_per_request = 0;
        let state = test_state_with_limits(limits);
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "t")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::BAD_REQUEST);
    }
}
```

> Provide `test_state()` / `test_state_with_limits()` helpers building a `DistributorState` over a `RecordingSink` (an `Arc<Mutex<Vec<SpanRecord>>>` counting appends).

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib distributor::tests`
Expected: FAIL — `cannot find function router`.

- [x] **Step 3: Implement `distributor/mod.rs`**

Implement:
- `WalSink` trait + `KafkaSink` (`async fn append`: build `ProducerRecord { topic: TRACES_WAL_TOPIC.into(), key: Some(partition_key(&rec.span.trace_id)), value: Some(Bytes::from(rec.encode()?)), partition: None, ..Default::default() }`, then `producer.send(record).await.await.map_err(...)?.map_err(...)?`).
- `DistributorState { sink, limits, max_decompressed }`, generic-free via `Arc<dyn WalSink>`.
- `router()`: `Router::new().route("/v1/traces", post(otlp_push)).route("/api/push", post(otlp_push)).route("/api/v2/spans", post(zipkin_push)).with_state(state)`.
- `otlp_push` handler: read `X-Scope-OrgID` (default `"anonymous"` if absent — Tempo's single-tenant fallback; multi-tenant strictness is a Slice-8 limit), read body bytes (handle `Content-Encoding: gzip` by decoding via `flate2` if present — `// TODO(slice4-gzip)` if `flate2` not a dep; identity works for the tests), `TracesData::decode` → `decode_otlp` → `validate` → `produce_spans` → return `200 OK` with an empty OTLP `ExportTraceServiceResponse` body. Map `TracesError::status_code()` to the HTTP status on error.
- `zipkin_push` handler: read tenant + body → `decode_zipkin` → validate → produce → return `202 Accepted` (Zipkin's success code).
- `validate`: enforce `max_spans_per_request` (over → `TracesError::Invalid` → 400) and `max_attr_value_len`; rate-limit integration with Crabka quotas is `// TODO(slice4-quota)` (structural caps enforced now).
- `produce_spans`: for each span, `sink.append(SpanRecord { tenant: tenant.into(), span }).await?`. (For throughput a real distributor pipelines appends + flushes; await per record here for test determinism.)
- `serve()`: mirror `crates/broker/src/metrics_server.rs::run` — bind `TcpListener`, `axum::serve(listener, router(state)).with_graceful_shutdown(...)`, return the bound `SocketAddr`. TLS is the `grpc-gateway/serve.rs` pattern, deferred to hardening — plaintext here, note `// TODO(slice4-tls)`.

Body extraction: axum `body::Bytes` extractor gives raw bytes; do not enable an axum decompression layer (we own decode).

> **Verify the producer `.send` ack pattern** (`producer.send(rec).await.await??` — the outer await resolves partition routing, the returned `oneshot::Receiver` awaits the broker ack) against `crates/client-producer/src/producer.rs` (confirmed: `send(&self, ProducerRecord) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>>`, and the call is `async`). For the async-trait shape, follow the codebase convention (`#[async_trait::async_trait]` if used elsewhere, else `-> impl Future`).

- [x] **Step 4: Run**

Run: `cargo test -p crabka-traces --lib distributor`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): distributor axum server — OTLP/Zipkin routes, limits, trace_id-keyed WAL produce"
```

---

### Task 9: Jaeger receivers (Thrift compact + gRPC) — decode + wire

**Files:**
- Create/overwrite: `crates/traces/src/wire/jaeger.rs`
- Modify: `crates/traces/src/distributor/mod.rs` (add the Jaeger HTTP route)

**Interfaces:**
- Produces:
  - `fn decode_jaeger_thrift(body: &[u8]) -> Result<Vec<Span>, WireError>` — decode a Jaeger `Batch` (compact-Thrift, the `thrift_http` `14268` `/api/traces` body) → `Vec<Span>`: Jaeger `traceIdLow`/`traceIdHigh` (i64 pair) → `[u8;16]`; `spanId`/`parentSpanId` (i64) → `[u8;8]`; `process.serviceName` → `service.name` resource attr; `tags` (Jaeger `KeyValue`) → span attrs; `startTime`+`duration` (micros) → ns; `references` (CHILD_OF) → `parent_span_id`.
  - the Jaeger gRPC path (`collector.PostSpans`) is structurally identical once the protobuf model is decoded; this slice implements the **Thrift HTTP** receiver fully and the gRPC receiver is `// TODO(slice4-jaeger-grpc)` (the gRPC server wiring belongs with the Slice-5 gRPC surface; the decode core is shared).

> **Bounded against the `thrift` crate (churn-prone).** The Jaeger `Batch`/`Span`/`Process`/`Tag` Thrift structs come from the Jaeger IDL (`jaeger.thrift`). Either (a) vendor the generated Rust from `jaeger.thrift` via the `thrift` compiler into `src/wire/jaeger_gen.rs` (preferred: a committed generated file, no build-time thrift dep), or (b) hand-decode the compact-Thrift fields you need. Pin field IDs against the canonical `jaeger.thrift`. This task provides the STRUCTURE + a behavior-pinning test over a known-bytes `Batch`; do not fabricate `thrift`-crate API signatures — verify against the `thrift` crate version pinned in Step 1 (Task 1).

- [x] **Step 1: Write the failing test (round-trip a Batch the encoder produced)**

Build a `Batch` via the generated/vendored Jaeger types, compact-encode it, then assert `decode_jaeger_thrift` recovers the span. (Using the encoder to produce the bytes pins the decode against the real wire shape without hand-writing hex.)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::SpanKind;
    use assert2::assert;

    // Build a minimal Jaeger Batch, compact-encode, decode back.
    // (jaeger_gen::{Batch, Span, Process, SpanRef, SpanRefType} are the
    //  vendored/generated Jaeger thrift types; see Step 2.)
    #[test]
    fn decodes_jaeger_thrift_batch() {
        let bytes = super::test_support::encode_sample_batch(); // helper, Step 2
        let spans = decode_jaeger_thrift(&bytes).unwrap();
        assert!(spans.len() == 1);
        let s = &spans[0];
        // traceIdHigh=0, traceIdLow=1 → [u8;16] with low 8 bytes = 1.
        assert!(s.trace_id[15] == 1);
        assert!(s.span_id[7] == 2);
        assert!(s.parent_span_id == Some([0, 0, 0, 0, 0, 0, 0, 9]));
        assert!(s.start_ns == 1_000_000); // 1000 micros → ns
        assert!(s.kind == SpanKind::Server || s.kind == SpanKind::Internal);
        assert!(s
            .resource_attrs
            .iter()
            .any(|a| a.key == "service.name"));
    }
}
```

- [x] **Step 2: Decide the Jaeger type source + write the encoder helper**

Generate Rust from `jaeger.thrift` (`thrift --gen rs jaeger.thrift`) and commit it as `src/wire/jaeger_gen.rs` (option a), or hand-roll the compact-Thrift reader for the `Batch{ process: Process{ serviceName, tags }, spans: [Span{ traceIdLow, traceIdHigh, spanId, parentSpanId, operationName, references, flags, startTime, duration, tags, logs }] }` subset (option b). Add `test_support::encode_sample_batch()` building the `Batch` in the test above. **Verify the `thrift` crate's `TCompactInputProtocol`/`TCompactOutputProtocol` + generated `*::read_from_in_protocol`/`write_to_out_protocol` signatures** against the pinned `thrift` version before relying on them.

- [x] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib wire::jaeger`
Expected: FAIL — `cannot find function decode_jaeger_thrift`.

- [x] **Step 4: Implement `decode_jaeger_thrift`**

Map the decoded `Batch` to `Vec<Span>`:
- `trace_id`: `traceIdHigh` (i64) into bytes `[0..8]`, `traceIdLow` into `[8..16]` (big-endian).
- `span_id`/`parent_span_id`: i64 → big-endian `[u8;8]`; `parent_span_id` from the first `CHILD_OF` reference (or the legacy `parentSpanId` field), `None` if zero/absent.
- `start_ns`: `startTime` (micros) × 1000; `duration_ns`: `duration` (micros) × 1000.
- `resource_attrs`: `process.serviceName` → `service.name`, plus `process.tags`.
- `span_attrs`: span `tags` (Jaeger `Tag` typed values → `AttrValue`).
- `kind`: derive from a `span.kind` tag if present (Jaeger encodes kind as a tag), else `Internal`.
- `status`: `error` tag `true` → `StatusCode::Error`, else `Unset`.

> Keep the mapping in one `fn jaeger_span_to_internal(span, process) -> Result<Span, WireError>` so the gRPC path (later) reuses it.

- [x] **Step 5: Wire the HTTP route**

In `distributor/mod.rs`, add `.route("/api/traces", post(jaeger_push))` and a `jaeger_push` handler (tenant + body → `decode_jaeger_thrift` → validate → produce → `202 Accepted`).

Run: `cargo test -p crabka-traces --lib wire::jaeger distributor`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): Jaeger Thrift receiver decode + /api/traces route"
```

---

### Task 10: Live-store — recent-traces `MemTable`, rebuildable from offsets

**Files:**
- Create: `crates/traces/src/livestore.rs`
- Modify: `crates/traces/src/lib.rs`

**Interfaces:**
- Produces:
  - `struct LiveStore { /* tenant -> Vec<Span> ring/window, retention_ns */ }`
  - `LiveStore::new(retention_ns: i64) -> Self`
  - `LiveStore::ingest(&mut self, rec: SpanRecord)` — append the span to its tenant's recent set, evicting spans older than `retention_ns` from `now`.
  - `LiveStore::trace_by_id(&self, tenant: &str, trace_id: &[u8;16]) -> Vec<Span>` — assemble a recent trace.
  - `LiveStore::mem_table(&self, tenant: &str) -> Result<datafusion::datasource::MemTable, TracesError>` — expose the tenant's recent spans as a DataFusion `MemTable` over the slice-1 span schema (reuses `span_batch`).
  - `async fn run(consumer: Consumer, store: Arc<RwLock<LiveStore>>, shutdown: CancellationToken) -> Result<(), TracesError>` — the consumer-group loop: poll → decode `SpanRecord` → `ingest`. **No offset commit needed for correctness** (live-store is rebuildable; it commits periodically only to bound replay on restart). On restart it replays from the committed offset (or earliest) to rebuild.

- [x] **Step 1: Write the failing tests (no broker)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{Span, SpanKind, StatusCode};
    use crate::wal::SpanRecord;
    use assert2::assert;

    fn rec(tid: [u8; 16], sid: u8, start_ns: i64) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            span: Span {
                trace_id: tid,
                span_id: [sid; 8],
                parent_span_id: None,
                name: "s".into(),
                kind: SpanKind::Server,
                start_ns,
                duration_ns: 1,
                status: StatusCode::Ok,
                status_message: String::new(),
                resource_attrs: vec![],
                span_attrs: vec![],
                events: vec![],
                links: vec![],
                instrumentation_scope: String::new(),
            },
        }
    }

    #[test]
    fn assembles_recent_trace_by_id() {
        let mut ls = LiveStore::new(i64::MAX);
        ls.ingest(rec([1u8; 16], 1, 100));
        ls.ingest(rec([1u8; 16], 2, 200));
        ls.ingest(rec([2u8; 16], 3, 150));
        let t1 = ls.trace_by_id("t", &[1u8; 16]);
        assert!(t1.len() == 2);
        let t2 = ls.trace_by_id("t", &[2u8; 16]);
        assert!(t2.len() == 1);
        // wrong tenant sees nothing.
        assert!(ls.trace_by_id("other", &[1u8; 16]).is_empty());
    }

    #[test]
    fn evicts_outside_retention_window() {
        // retention 50ns: when a span at start 1000 arrives, a span at start 900
        // is outside the window and evicted.
        let mut ls = LiveStore::new(50);
        ls.ingest(rec([1u8; 16], 1, 900));
        ls.ingest(rec([1u8; 16], 2, 1000));
        let t = ls.trace_by_id("t", &[1u8; 16]);
        assert!(t.len() == 1);
        assert!(t[0].span_id == [2u8; 8]);
    }

    #[test]
    fn exposes_mem_table() {
        let mut ls = LiveStore::new(i64::MAX);
        ls.ingest(rec([1u8; 16], 1, 100));
        let mt = ls.mem_table("t").unwrap();
        // a MemTable over the span schema with one partition of one row.
        use datafusion::datasource::TableProvider;
        assert!(mt.schema().index_of(crabka_blockstore::SCOL_TRACE_ID).is_ok());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib livestore`
Expected: FAIL — `cannot find type LiveStore`.

- [x] **Step 3: Implement `livestore.rs`**

```rust
//! Live-store: the hot tier. Consumes the WAL, assembles recent traces by
//! trace_id in memory, exposes them as a DataFusion `MemTable`, and is fully
//! rebuildable from offsets (spec §5.4) — it holds no durable state.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::MemTable;

use crate::error::TracesError;
use crate::span::{batch::span_batch, Span};
use crate::wal::SpanRecord;

/// In-memory recent-traces store, per tenant, windowed by retention.
pub struct LiveStore {
    /// tenant -> (trace_id -> spans).
    by_tenant: HashMap<String, HashMap<[u8; 16], Vec<Span>>>,
    /// Highest span start seen, to drive window eviction (monotone-ish clock).
    max_start_ns: i64,
    retention_ns: i64,
}

impl LiveStore {
    #[must_use]
    pub fn new(retention_ns: i64) -> Self {
        Self {
            by_tenant: HashMap::new(),
            max_start_ns: 0,
            retention_ns,
        }
    }

    /// Append a span; evict spans older than `retention_ns` behind the frontier.
    pub fn ingest(&mut self, rec: SpanRecord) {
        self.max_start_ns = self.max_start_ns.max(rec.span.start_ns);
        let tid = rec.span.trace_id;
        self.by_tenant
            .entry(rec.tenant)
            .or_default()
            .entry(tid)
            .or_default()
            .push(rec.span);
        self.evict();
    }

    fn evict(&mut self) {
        if self.retention_ns == i64::MAX {
            return;
        }
        let cutoff = self.max_start_ns - self.retention_ns;
        for traces in self.by_tenant.values_mut() {
            traces.retain(|_, spans| {
                spans.retain(|s| s.start_ns > cutoff);
                !spans.is_empty()
            });
        }
    }

    /// Assemble a recent trace's spans (empty if not present / wrong tenant).
    #[must_use]
    pub fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Vec<Span> {
        self.by_tenant
            .get(tenant)
            .and_then(|t| t.get(trace_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Expose a tenant's recent spans as a DataFusion `MemTable`.
    pub fn mem_table(&self, tenant: &str) -> Result<MemTable, TracesError> {
        let schema = crabka_blockstore::span_block_schema();
        let mut batches = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                // span_batch sorts/assigns nested-set per trace internally? No —
                // it expects ordered input; sort a clone by (start_ns, span_id).
                let mut ordered = spans.clone();
                ordered.sort_by_key(|s| (s.start_ns, s.span_id));
                batches.push(span_batch(&ordered)?);
            }
        }
        MemTable::try_new(schema, vec![batches]).map_err(|e| TracesError::Block(e.to_string()))
    }
}
```

The `run` loop:

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crabka_client_consumer::Consumer;

/// Consumer-group loop: poll WAL records, decode, ingest into the live-store.
pub async fn run(
    mut consumer: Consumer,
    store: Arc<RwLock<LiveStore>>,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let records = consumer
            .poll(std::time::Duration::from_millis(500))
            .await
            .map_err(|e| TracesError::Produce(e.to_string()))?;
        if records.is_empty() {
            continue;
        }
        let mut guard = store.write().await;
        for r in records {
            if let Some(value) = r.value.as_ref() {
                let rec = SpanRecord::decode(value)?;
                guard.ingest(rec);
            }
        }
        drop(guard);
        // Periodic commit bounds replay on restart; correctness doesn't need it.
        let _ = consumer.commit_sync().await;
    }
}
```

> **Verify `MemTable::try_new(schema, Vec<Vec<RecordBatch>>)` and `TableProvider::schema()`** against the pinned DataFusion rev (`0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf`); `MemTable` lives at `datafusion::datasource::MemTable` (or `datafusion::catalog::MemTable` on some revs). If the path differs, align to the rev. Add `tokio-util` (CancellationToken) to deps if not already present; otherwise use a `tokio::sync::watch` shutdown signal. The eviction uses a frontier-relative cutoff (spec §5.4 "~30–60 min"); wall-clock eviction is a `// TODO(slice4-wallclock-evict)` refinement.

- [x] **Step 4: Declare + run**

`lib.rs`: `pub mod livestore; pub use livestore::LiveStore;`

Run: `cargo test -p crabka-traces --lib livestore`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): live-store hot tier — recent-traces MemTable, retention eviction, rebuildable loop"
```

---

### Task 11: Block-builder — WAL consumer-group → group by trace_id → blocks → index

**Files:**
- Create: `crates/traces/src/blockbuilder.rs`
- Modify: `crates/traces/src/lib.rs`

**Interfaces:**
- Produces:
  - `fn object_key(tenant: &str, partition: i32, min_offset: i64, max_offset: i64, window_start_ns: i64) -> String` — deterministic idempotent key.
  - `fn group_by_trace(records: &[SpanRecord]) -> BTreeMap<(String, [u8; 16]), Vec<Span>>` — group by `(tenant, trace_id)`, each group sorted by `(start_ns, span_id)` (stable nested-set input).
  - `async fn build_blocks(writer: &BlockWriter, index: &mut TraceIndex, tenant: &str, partition: i32, records: &[SpanRecord], offset_range: (i64, i64)) -> Result<Vec<BlockMeta>, TracesError>` — group → per-trace `span_batch` → concat into one block per tenant → `write_block` → `TraceIndex` updates (trace-id bloom + tag sets).
  - `async fn run(consumer: Consumer, writer: BlockWriter, index: Arc<Mutex<TraceIndex>>, store, index_key: &str, window: Duration, shutdown: CancellationToken) -> Result<(), TracesError>` — poll → accumulate a window → build blocks → **save index + write blocks FIRST**, THEN `commit_sync` (crash-safety order, spec §9). Late spans naturally land in a later window/block; the read path + compactor merge them.

- [x] **Step 1: Write the failing tests (no broker, InMemory object store)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{Span, SpanKind, StatusCode};
    use crate::wal::SpanRecord;
    use assert2::assert;

    fn rec(tenant: &str, tid: [u8; 16], sid: u8, parent: Option<u8>, start_ns: i64) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            span: Span {
                trace_id: tid,
                span_id: [sid; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: "s".into(),
                kind: SpanKind::Server,
                start_ns,
                duration_ns: 1,
                status: StatusCode::Ok,
                status_message: String::new(),
                resource_attrs: vec![],
                span_attrs: vec![],
                events: vec![],
                links: vec![],
                instrumentation_scope: String::new(),
            },
        }
    }

    #[test]
    fn object_key_is_deterministic() {
        let a = object_key("t", 0, 10, 20, 1000);
        let b = object_key("t", 0, 10, 20, 1000);
        let c = object_key("t", 0, 10, 21, 1000);
        assert!(a == b);
        assert!(a != c);
    }

    #[test]
    fn group_by_trace_orders_spans() {
        let recs = vec![
            rec("t", [1u8; 16], 2, Some(1), 200),
            rec("t", [1u8; 16], 1, None, 100),
        ];
        let grouped = group_by_trace(&recs);
        let g = &grouped[&("t".to_string(), [1u8; 16])];
        // sorted by start_ns: span 1 (100) before span 2 (200).
        assert!(g[0].span_id == [1u8; 8]);
        assert!(g[1].span_id == [2u8; 8]);
    }

    #[tokio::test]
    async fn build_blocks_writes_span_block() {
        use object_store::memory::InMemory;
        use object_store::ObjectStore;
        use std::sync::Arc;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = crabka_blockstore::BlockWriter::new(store.clone());
        let mut index = crabka_blockstore::TraceIndex::new();

        let recs = vec![
            rec("t", [1u8; 16], 1, None, 100),
            rec("t", [1u8; 16], 2, Some(1), 200),
        ];
        let metas = build_blocks(&writer, &mut index, "t", 0, &recs, (10, 20)).await.unwrap();
        assert!(!metas.is_empty());
        assert!(metas[0].row_count == 2);
        assert!(metas[0].tenant == "t");
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib blockbuilder`
Expected: FAIL — `cannot find function object_key` (or a missing `TraceIndex::new`/`BlockWriter::new` import — that means slice 1 isn't merged).

- [x] **Step 3: Implement `blockbuilder.rs`**

Implement:
- `object_key`: `format!("traces/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet")` — deterministic from the WAL offset range + window, so a re-process after a crash overwrites identical bytes (idempotent; spec §9).
- `group_by_trace`: `BTreeMap<(String, [u8;16]), Vec<Span>>`; push by `(tenant, trace_id)`, then `sort_by_key(|s| (s.start_ns, s.span_id))` each group (stable nested-set input, Task 4 determinism note).
- `build_blocks`: group → for each `(tenant, trace_id)` group call `span_batch(&spans)` (Task 7) → concat all of one tenant's batches into one block (`arrow::compute::concat_batches`) → `writer.write_block(tenant, &object_key(...), schema, &[concatenated])` → for each trace, `index.add_trace_block(tenant, &trace_id, &meta.object_key)` (bloom) + `index.add_tags(tenant, &meta.object_key, &tag_names, &tag_values)` (tag sets/blooms). Return the `BlockMeta`s.
- `run`: loop `consumer.poll(window)` → `SpanRecord::decode` each → accumulate by source `(partition, offset range)` until the window elapses → `build_blocks` → `index.save(&store, index_key)` → **then** `consumer.commit_sync()`. On shutdown token: build+save+commit a final window, then break.

```rust
//! Block-builder role: consume the WAL, group spans by (tenant, trace_id) over
//! a flush window, build span Parquet blocks (+ nested-set columns via the DFS)
//! and TraceIndex updates, write them, THEN commit offsets (crash-safety order,
//! spec §9). Late spans land in a later block; the read path/compactor merge.
```

> **Verify the `TraceIndex` API** (`TraceIndex::new`, `add_trace_block`, `add_tags`, `save(&store, key)`) and `BlockWriter::write_block` return `BlockMeta { tenant, object_key, min_ts, max_ts, row_count, .. }` against the slice-1 traces-blockstore plan — these are slice-1's exact names. If `add_trace_block`/`add_tags` are spelled differently (e.g. `insert_trace`/`record_tags`), align to slice-1. `arrow::compute::concat_batches(&schema, &batches)` concatenates per-trace batches into one block; verify it against arrow 59. The tag-name/value extraction iterates each span's `resource_attrs`/`span_attrs` keys/values.

- [x] **Step 4: Declare + run**

`lib.rs`: `pub mod blockbuilder; pub use blockbuilder::{build_blocks, group_by_trace, object_key};`

Run: `cargo test -p crabka-traces --lib blockbuilder`
Expected: PASS (3 tests).

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): block-builder — WAL group-by-trace → span blocks + TraceIndex (write-then-commit)"
```

---

### Task 12: Role-selectable binary

**Files:**
- Create: `crates/traces/src/bin/crabka-traces.rs`
- Modify: `crates/traces/Cargo.toml` (`[[bin]]` if needed; clap already a dep)

**Interfaces:**
- Produces: a binary with `--target distributor|block-builder|live-store` (later targets `querier|query-frontend|compactor|metrics-generator` stubbed with a "not implemented until slice N" message + exit 2). Distributor wires a real `Producer` + `serve`; block-builder/live-store wire a `Consumer` + `run`.

- [x] **Step 1: Write the failing test (arg parsing)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use clap::Parser;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "distributor"]).unwrap();
        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-traces", "--target", "block-builder"]).unwrap();
        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-traces", "--target", "bogus"]).is_err());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --bin crabka-traces`
Expected: FAIL — `cannot find type Cli`.

- [x] **Step 3: Implement the binary**

`#[derive(Parser)] struct Cli { #[arg(long)] target: Target, #[arg(long, default_value = "127.0.0.1:3200")] listen: String, #[arg(long, default_value = "127.0.0.1:9092")] bootstrap: String, ... }` and `#[derive(Clone, ValueEnum)] enum Target { Distributor, BlockBuilder, LiveStore, Querier, QueryFrontend, Compactor, MetricsGenerator }` (clap kebab-cases the variants → `block-builder`, `live-store`, `metrics-generator`).

`main`: parse `Cli`; `tracing_subscriber` init; wire a `CancellationToken` to `tokio::signal::ctrl_c`; match `target`:
- `Distributor` → `Producer::builder().bootstrap(&cli.bootstrap).build().await?`, wrap in `KafkaSink`, build `DistributorState`, `distributor::serve(cli.listen.parse()?, state, shutdown).await?`.
- `BlockBuilder` → `Consumer::builder().bootstrap(&cli.bootstrap).group_id("crabka-traces-block-builder").subscribe([TRACES_WAL_TOPIC.to_string()]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`, build a `BlockWriter` + `TraceIndex` over the configured object store (memory for now; real object-store config is `// TODO(slice4-objstore-config)`), `blockbuilder::run(...).await?`.
- `LiveStore` → `Consumer::builder()...group_id("crabka-traces-live-store")...`, `livestore::run(consumer, Arc::new(RwLock::new(LiveStore::new(retention_ns))), shutdown).await?`.
- `Querier | QueryFrontend | Compactor | MetricsGenerator` → `eprintln!` + `std::process::exit(2)` with "target not implemented until slice {5|6|7}".

> Keep `main` thin; testable logic lives in the modules. **Note:** the three consumer groups (`block-builder`, `live-store`, and — later — `metrics-generator`) use distinct `group_id`s on the same WAL topic, with independent offsets (spec §3.2) — that is why RF1 is safe.

- [x] **Step 4: Run**

Run: `cargo test -p crabka-traces --bin crabka-traces`
Expected: PASS (3 tests). Then `cargo build -p crabka-traces --bin crabka-traces` compiles.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): role-selectable crabka-traces binary (distributor|block-builder|live-store)"
```

---

### Task 13: End-to-end broker round-trip (in-process)

**Files:**
- Create: `crates/traces/tests/ingest_roundtrip.rs`
- Create: `crates/traces/tests/support/mod.rs` (minimal in-process broker start helper)

**Interfaces:**
- Consumes the public API: `distributor::{router, KafkaSink, DistributorState}`, `Producer`, `Consumer`, `blockbuilder::{build_blocks, group_by_trace}`, `SpanRecord`, `TRACES_WAL_TOPIC`, blockstore `BlockWriter`/`TraceIndex`.

This is the one test that needs a real broker. Use the in-process broker test-support — `crabka_broker::{Broker, BrokerConfig}` + `BrokerHandle::listen_addr()` are public (verified in `crates/broker/tests/support/mod.rs`), so no Docker is needed and it runs in CI.

- [x] **Step 1: Write the support helper**

`crates/traces/tests/support/mod.rs` (path-included submodule; `crabka-broker`/`crabka-client-core` are dev-deps from Task 1):

```rust
//! Minimal in-process broker for the traces ingest round-trip test.
#![allow(dead_code)]

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub bootstrap: String,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    InProcess { broker, bootstrap, _tempdir: tempdir }
}
```

> **Verify `BrokerConfig::for_tests` + `Broker::start` + `BrokerHandle::listen_addr` are public** in `crabka-broker` (confirmed reachable in the broker's own test-support; they are imported from the crate root `crabka_broker::{...}`). If `BrokerConfig::for_tests` turns out to be test-only/`pub(crate)`, fall back to `#[ignore = "requires Docker"]` + `testcontainers` cp-kafka (the `crates/client-core/tests` pattern).

- [x] **Step 2: Write the round-trip test**

```rust
//! End-to-end: POST an OTLP TracesData body through the distributor to a real
//! WAL topic, consume it, and build a span block — asserting the SpanRecord
//! round-trips and a block lands with the expected rows.

mod support;

#[tokio::test]
async fn otlp_lands_as_span_block() {
    // 1. start in-process broker (support::start)
    // 2. admin: create TRACES_WAL_TOPIC with N partitions
    // 3. build DistributorState with a real KafkaSink(Producer::builder().bootstrap(..).build())
    // 4. POST a snappy-free OTLP TracesData body to router() via oneshot → assert 200
    // 5. build a Consumer(group) on TRACES_WAL_TOPIC, poll until the record arrives,
    //    decode → assert SpanRecord.span.trace_id matches and is the same partition
    // 6. build_blocks over the polled records into an InMemory BlockWriter+TraceIndex
    // 7. assert the BlockMeta row_count / tenant
}
```

Fill the body using the verified producer/consumer/admin APIs (Task 1 deps). Key assertions: the distributor returns 200; the consumer reads back a `SpanRecord` whose `span.trace_id` matches; `build_blocks` produces a `BlockMeta` with the right `row_count`. Also assert (the invariant test): two spans with the same `trace_id` produce to the **same partition** (poll both back from one partition).

- [x] **Step 3: Run**

Run: `cargo test -p crabka-traces --test ingest_roundtrip`
Expected: PASS (the WAL record round-trips, same-trace spans share a partition, and a block is written).

- [x] **Step 4: Whole-crate gate**

Run: `cargo test -p crabka-traces && cargo clippy -p crabka-traces --all-targets && cargo fmt -p crabka-traces --check`
Expected: all PASS, no warnings, formatting clean.

- [x] **Step 5: Commit**

```bash
git add crates/traces/
git commit -m "test(traces): end-to-end OTLP → WAL → block-builder → span block round-trip"
```

---

## Self-review

**Spec coverage (against §5 ingest + §3 architecture + §11 Slice 4):**
- Four push doors → internal `Span`: OTLP (Task 5), Zipkin v2 JSON (Task 6), Jaeger Thrift (Task 9); Tempo-native `/api/push` routes to the OTLP decoder (Task 5/8). gRPC OTLP + Jaeger gRPC servers are flagged as Slice-5 gRPC-surface follow-ons (`// TODO(slice4-otlp-grpc)`/`// TODO(slice4-jaeger-grpc)`) — the decode cores are done.
- `SpanRecord` (the slice-4 contract) + `TRACES_WAL_TOPIC` + `partition_key = trace_id` → Task 3, with the dedup-avoidance invariant pinned by `same_trace_id_same_partition_key` (Task 3) and the round-trip partition assertion (Task 13).
- distributor (axum, four routes, validate/limits, `X-Scope-OrgID` tenant, trace_id-keyed produce, format-correct success codes 200/202) → Tasks 8, 9.
- nested-set DFS pre-order (left/right/parent_id, roots share sentinel 0, ancestor-contains-descendant) computed at block-build → Task 4, consumed by the batch builder → Task 7.
- block-builder (consumer group, group-by-trace over a window, span blocks via blockstore `BlockWriter`, `TraceIndex` bloom + tag sets, write-then-commit + deterministic idempotent key, late-spans-to-later-block) → Task 11.
- live-store (consumer group, recent-traces by trace_id, DataFusion `MemTable`, retention eviction, rebuildable from offsets) → Task 10.
- role-selectable binary (distributor|block-builder|live-store; later targets stubbed) → Task 12.
- in-process broker round-trip test (no Docker) → Task 13.

**Deviations flagged (deferred with explicit TODO markers, not silently dropped):**
- OTLP gRPC + Jaeger gRPC server wiring → Slice-5 gRPC surface; decode cores done (`// TODO(slice4-otlp-grpc)`/`// TODO(slice4-jaeger-grpc)`).
- Zipkin `annotations` → events (`// TODO(slice4-zipkin-annotations)`); the richer OTLP events/links path is implemented.
- Attribute-promotion dedicated columns (default-empty promotion set) → `// TODO(slice4-attr-promotion)`; the mandatory span schema is written fully.
- gzip body decoding on the OTLP door (`// TODO(slice4-gzip)`) — identity bodies work for the tests; per-tenant rate-limit/429 via Crabka quotas (`// TODO(slice4-quota)`) — structural caps (400/415) enforced now; TLS on the distributor (`// TODO(slice4-tls)`).
- Wall-clock retention eviction in the live-store (`// TODO(slice4-wallclock-evict)`); frontier-relative eviction works and is tested.

**Placeholder scan:** no "TBD"/"similar to Task N" without code. The churn-prone surfaces are each bounded with a "verify against X" note + a behavior-pinning test, never fabricated:
- the generated `opentelemetry-proto` 0.32 trace field names (Task 5) — pinned by the OTLP decode test, verified against the registry source;
- the `thrift` crate's compact-protocol + generated-Jaeger-type API (Task 9) — STRUCTURE + a round-trip-the-encoder test, verify against the pinned `thrift` version;
- the slice-1 span schema + `SCOL_*` constants + `TraceIndex`/`BlockWriter` API (Tasks 7, 11) — pinned by the batch + block tests, verify against the slice-1 traces-blockstore plan;
- the `serde-wincode` call shape (Task 3) — verify against `crates/broker/src/bootstrap.rs`;
- the producer `.send` ack pattern (Task 8) — verified against `crates/client-producer/src/producer.rs`;
- DataFusion `MemTable::try_new` path (Task 10) — verify against the pinned rev;
- in-process broker test-support reachability (Task 13) — verified public, with a Docker fallback.

**Type consistency:** `Span` (Task 2) is consumed unchanged by every `wire/*` decoder (Tasks 5, 6, 9), the WAL record (Task 3), the batch builder (Task 7), the live-store (Task 10), and the block-builder (Task 11). `SpanRecord`/`partition_key`/`TRACES_WAL_TOPIC` (Task 3) are consumed by the distributor produce path (Task 8), both consumer loops (Tasks 10, 11), and the round-trip test (Task 13). `assign_nested_set`/`NestedSet` (Task 4) feed `span_batch` (Task 7), which feeds both the block-builder (Task 11) and the live-store `MemTable` (Task 10). `TracesError::status_code()` is the single ingest status mapping (Task 1), used by every handler (Tasks 8, 9). The blockstore API (`BlockWriter::new`/`write_block`, `TraceIndex`, `span_block_schema`, `SCOL_*`) matches the slice-1 traces-blockstore plan exactly (Tasks 7, 11).

**Known risks (flagged, not hidden):**
- **slice-1 dependency** — Tasks 7/10/11 consume the slice-1 generalized blockstore (span schema + `TraceIndex`). This slice cannot land before slice 1; the batch/block tests are the loud failure if the slice-1 API drifts. **Verify the exact span-schema column constants + `TraceIndex` method names against the slice-1 plan before starting Tasks 7/11.**
- **`thrift` crate API churn** — Jaeger Thrift decode (Task 9) is the least-stable surface; the encode-then-decode round-trip test pins behavior, and the gRPC path (the simpler protobuf model) is deferred to keep the slice scoped.
- **`opentelemetry-proto` `trace` feature** — Task 1 adds `trace` to the workspace feature set; this is additive and does not touch the metrics signal's `metrics`-only usage. Pinned by the OTLP round-trip test (a codegen/feature break is a compile error).
- **the `trace_id` partition invariant** is the load-bearing correctness claim of the whole pipeline (RF1 dedup-avoidance); it is asserted both as a unit test (Task 3) and an end-to-end same-partition assertion (Task 13).
