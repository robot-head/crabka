# crabka-profiles Slice 4 — Ingest service (push.v1 + /ingest + OTLP profiles + distributor + block-builder)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the ingest half of the profiles backend — the three push doors (Connect `push.v1.PusherService/Push`, legacy HTTP `POST /ingest`, and OTLP `ProfilesService/Export` `v1development`) decoded to an internal profile, validated + relabeled + **multi-value-split into one series per pprof sample type**, sharded by `(tenant, series_fingerprint)` onto a Kafka WAL by the **distributor** role; and the **block-builder** consumer group that groups records over a flush window, builds the samples fact table + a dedup per-block `SymbolDb`, and writes Parquet blocks + the symbol-DB artifact + `ProfileIndex` updates to object storage (write-then-commit, idempotent keys). Ship a role-selectable `crabka-profiles --target distributor|block-builder` binary (later targets stubbed).

**Architecture:** This slice creates the new `crabka-profiles` crate and adds the `wire`, `ingest`, `wal`, `distributor`, and `blockbuilder` modules. The `wire` module owns the prost/Connect codegen: `push.v1` (Alloy `pyroscope.write`), the OTLP `profiles/v1development` proto (**vendored + commit-pinned — churns hard**), and the `perftools.profiles` pprof proto — built via `connectrpc-axum-build` (the grpc-gateway/rebalancer pattern). The `ingest` module lowers each door into a `DecodedProfile` (one decoded pprof per series-stream) and performs the **multi-value split** (one `__profile_type__` per `sample_type[]`) + validation/relabel. The `wal` module defines `ProfileRecord` — the WAL topic record (serde + `serde-wincode`, the codebase convention) that **Slices 5/6/7 consume** — `PROFILES_WAL_TOPIC`, and `partition_key = hash(tenant, series_fingerprint)`. The `distributor` is an axum 0.8 server hosting the Connect `push.v1` + OTLP `Export` builders alongside a plain `/ingest` route. The `blockbuilder` is a Kafka consumer-group loop that interns each record's symbol set into a per-block `SymbolDb` (`crabka-pprof`) and writes the samples fact table (slice-1 `PCOL_*` schema) via `crabka-blockstore::BlockWriter`. A real Crabka broker is only needed for the produce/consume round-trip test, which uses the in-process broker test-support (no Docker).

```
Alloy push.v1 ─────────┐
SDKs /ingest (pprof/jfr)├─→ distributor (axum) ─→ relabel ─→ require service_name+__name__ ─→ decode ─┐
OTLP /v1development ────┘     (Connect + OTLP builders + plain /ingest route)                        │
                                                                                                     ▼
                                                            MULTI-VALUE SPLIT (one series per sample_type → 5-part __profile_type__)
                                                            + __session_id__ modulo-hash cap + label limits
                                                                                                     │ produce ProfileRecord
                                                                                                     ▼  key = hash(tenant, series_fp)
                                                                                          __crabka_profiles_wal
                                                                                                     │ (consumer group)
                                                                                                     ▼
                                                                                   block-builder
                                                                                   group over flush window
                                                                                   → intern symbols into per-block SymbolDb
                                                                                   → samples fact table (PCOL_* RecordBatch)
                                                                                   → BlockWriter::write_block + symdb artifact
                                                                                   → ProfileIndex updates → object storage
                                                                                   → commit offsets  (block+index FIRST, then commit)
```

**Tech Stack:** Rust 2024 · `prost` 0.14 (workspace) · `connectrpc-axum` + `connectrpc-axum-build` (build.rs codegen — the grpc-gateway/rebalancer pattern) · `axum` 0.8 (`http1`, `tokio`) · `flate2` 1 (gunzip the `raw_profile`) · `multer` (multipart `/ingest`) · `serde_json` 1 (`sample_type_config`) · `bytes` 1 · `arrow` 59 · `crabka-blockstore` (slice-1: `BlockWriter`/`BlockMeta`/`ProfileIndex`/`PCOL_*` schema + symbol-DB artifact constants) · `crabka-pprof` (slices 2–3: `PprofProfile`, `SymbolDb`, `ProfileType`, `Frame`) · `crabka-client-producer` · `crabka-client-consumer` · `crabka-client-admin` · `serde` + `serde-wincode` (`wincode::Serialize`) · `clap` 4 · `tokio` · `thiserror` · `tracing`. Tests: `assert2`, `proptest`, `tempfile`, `object_store::memory::InMemory`; the broker round-trip test uses `crates/broker/tests/support` (in-process, no Docker).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change `ProfileRecord`/`DecodedProfile`/enums/wire-internal types freely; no shims, no migration code, no `#[serde(default)]`. (Only Kafka **client** wire compat matters — and the `push.v1`/`/ingest`/OTLP byte-exactness on the HTTP edge.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-profiles --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-profiles` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Arrow version identity:** use `arrow` 59 directly. The samples-fact-table batches this slice builds are consumed by `crabka-blockstore::BlockWriter::write_block` without conversion.
- **prost/connect generated types are the source of truth.** The `push.v1`/OTLP/pprof field names this plan quotes (`RawProfileSeries.labels`, `RawSample.raw_profile`, `RawSample.id`, `ProfilesDictionary.string_table`, …) are pinned by behavior tests; if a generated field name differs, **align to the generated `OUT_DIR` type**, never fabricate.
- **OTLP profiles `v1development` is experimental and churns hard.** Vendor the proto at a **commit-pinned** tag (comment at the top of the file) and behavior-pin it with a prost round-trip test. Do not fabricate field numbers — verify against the pinned rev.
- **The `(tenant, series_fingerprint)` partition invariant is non-negotiable** (spec §5.3). All samples of one series in one tenant MUST land in one partition (per-series order). The producer MurmurHash2-partitions on `key`; set `key = partition_key(tenant, fp)` and leave `partition: None`. A test pins that two records sharing `(tenant, fp)` produce the same key.
- **Kafka wire-protocol exactness** is preserved automatically by producing/consuming through the existing `crabka-client-producer`/`crabka-client-consumer` clients — do not hand-roll protocol frames.

---

## Dependency & slice roadmap

**Depends on (consume exactly — do not re-implement):**
- **`crabka-blockstore` (slice 1, profiles-generalized)** — `BlockStore`, `BlockWriter::new(store: Arc<dyn object_store::ObjectStore>)` + `BlockWriter::write_block(tenant:&str, object_key:&str, schema:SchemaRef, batches:&[RecordBatch]) -> Result<BlockMeta>`, `BlockMeta`, the **`ProfileIndex`** impl (`BlockIndex`) with the label-postings + `__profile_type__` index + stacktrace-partition-map update methods + `save`, the **samples fact-table schema builder** + the **`PCOL_*` column constants** (`PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, `PCOL_TOTAL_VALUE`, `PCOL_SPAN_ID`, `PCOL_TRACE_ID`) plus the mandatory `COL_FINGERPRINT`/`COL_TIMESTAMP`, and the **symbol-DB on-block artifact** key/suffix constant. `Labels`/`LabelMatcher`/`MatchOp`/`SeriesFingerprint` remain available. **Verify the exact `ProfileIndex` + samples-schema + symdb-artifact API against the slice-1 plan before consuming; if a name differs, align to it.**
- **`crabka-pprof` (slices 2–3)** — `PprofProfile` (`decode(&[u8]) -> Result<PprofProfile, ProfileError>` / `encode(&self) -> Vec<u8>`), the `perftools.profiles` wire model with `sample_type[]`/`sample[].value[]`/`location[]`/`function[]`/`mapping[]`/`string_table[]`; `SymbolDb` (`intern_stacktrace(partition:u64, location_refs:&[u32]) -> u32`, `resolve`, `encode()/decode()`); `ProfileType { name, sample_type, sample_unit, period_type, period_unit }` (`parse(&str)`/`Display` 5-part colon form); `Frame { function:String, file:String, line:i32 }`; `ProfileError`. **Verify against the slice-2 plan; align to generated names if they differ.**
- **`crabka-client-producer`** — `Producer::builder().bootstrap(..).build().await? -> Result<Producer, ProducerError>`; `Producer::send(ProducerRecord) -> impl Future<Output = oneshot::Receiver<Result<RecordMetadata, ProducerError>>>` (the call is `async`; await it, then await the returned `oneshot::Receiver` for the ack: `producer.send(rec).await.await??`); `ProducerRecord { topic:String, partition:Option<i32>, key:Option<Bytes>, value:Option<Bytes>, headers:Vec<Header>, timestamp_ms:Option<i64> }` (`Default`); `Producer::flush()`. **The producer hashes `key` with MurmurHash2 to choose a partition** — set `key` = `partition_key(tenant, fp)` and leave `partition: None`. (Verify against `crates/client-producer/src/{record,producer}.rs`.)
- **`crabka-client-consumer`** — `Consumer::builder().bootstrap(..).group_id(..).subscribe([..]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`; `Consumer::poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`; `Consumer::commit_sync() -> Result<(), ConsumerError>`; `ConsumerRecord { topic, partition:i32, offset:i64, key:Option<Bytes>, value:Option<Bytes>, .. }`. (Verify against `crates/client-consumer/src/{consumer,poll,commit}.rs`.)
- **`crabka-client-admin`** — `create_topics(&[CreateTopicSpec { name, partitions, replicas, configs }], timeout_ms) -> Result<Vec<CreateTopicOutcome>, AdminError>` (for tests + bootstrapping the WAL topic).
- **`connectrpc-axum` / `connectrpc-axum-build`** — `compile_protos(&[proto], &[include]).fetch_protoc(None, None)?.compile()?` in `build.rs` (fall back to vendored protoc only when system `protoc` is absent — the grpc-gateway/rebalancer guard); generated `pb::<svc>_connect::<Svc>ServiceBuilder::<()>::new().<method>(handler).build() -> axum::Router`; handlers are `async fn(Extension<Arc<State>>, ConnectRequest<Req>) -> Result<ConnectResponse<Resp>, ConnectError>`. (Verified against `crates/grpc-gateway/{build.rs,src/lib.rs,src/handlers.rs}` + `crates/rebalancer/build.rs`.)
- **`crabka-broker` (dev-dependency, tests only)** — `BrokerConfig::for_tests(PathBuf)`, `Broker::start(config).await -> Result<BrokerHandle, BrokerError>`, `BrokerHandle::listen_addr()` (public; verified in `crates/broker/tests/support/mod.rs`).

**THIS slice defines (Slices 5/6/7 consume):** `ProfileRecord` (Task 5) — the WAL topic record (tenant + series `Labels` + `profile_type:String` + decoded payload `samples[(stacktrace_location_refs, value, span_id, trace_id)]` + the symbol set to merge into the block symdb). `PROFILES_WAL_TOPIC = "__crabka_profiles_wal"`. `partition_key(tenant:&str, fp:u64) -> Bytes`. `DecodedProfile` + the multi-value split (`split_sample_types`). The block-builder's `intern_record`/`samples_batch`/`object_key`.

**The 8 profiles slices** (this plan = Slice 4):
1. Blockstore `ProfileIndex` + samples schema + symbol-DB artifact. 2. `crabka-pprof` core. 3. Engine completeness. **4. Ingest service *(this plan)*.** 5. Querier + Connect `querier.v1` + legacy render. 6. Query-frontend. 7. Native symbolization. 8. Hardening.

---

## File structure (`crates/profiles/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; ingest + blockstore + pprof + client deps; `[build-dependencies] connectrpc-axum-build` |
| `build.rs` | connect/prost-codegen `push.v1` + OTLP `profiles/v1development` + pprof protos → `OUT_DIR` |
| `proto/push/v1/push.proto` | vendored Pyroscope `push.v1.PusherService` |
| `proto/opentelemetry/proto/profiles/v1development/profiles.proto` | vendored OTLP profiles (**commit-pinned**) + the `ProfilesService` collector proto |
| `src/lib.rs` | module decls + public re-exports + crate docs; the `pb` codegen include |
| `src/error.rs` | `ProfilesError` + per-edge HTTP status mapping |
| `src/wire/mod.rs` | `pb` re-exports + the prost round-trip behavior-pin tests |
| `src/ingest/mod.rs` | `DecodedProfile`, `RawProfile`, relabel + require-service_name + label limits + `__session_id__` cap |
| `src/ingest/split.rs` | `split_sample_types` — one series per pprof `sample_type[]` → 5-part `__profile_type__` |
| `src/ingest/push_v1.rs` | `push.v1` `PushRequest` → `Vec<RawProfile>` (gunzip + pprof decode) |
| `src/ingest/otlp.rs` | OTLP `ExportProfilesServiceRequest` (`ProfilesDictionary` interned) → `Vec<RawProfile>` |
| `src/ingest/legacy.rs` | `/ingest` query+multipart → `RawProfile` (pprof/jfr/groups; `sample_type_config`) |
| `src/wal.rs` | `ProfileRecord`, `PROFILES_WAL_TOPIC`, `partition_key`, encode/decode |
| `src/distributor/mod.rs` | axum router (Connect `push.v1` + OTLP `Export` builders + plain `/ingest`), serve, limits, produce |
| `src/blockbuilder.rs` | consumer-group loop → intern symdb → samples batch → block + symdb + `ProfileIndex` → commit |
| `src/bin/crabka-profiles.rs` | `clap` role-selectable entrypoint (`--target`) |
| `tests/ingest_roundtrip.rs` | end-to-end distributor → WAL → block-builder → block (in-process broker) |

Each file has one responsibility; `blockbuilder.rs` is the only file that touches the blockstore writer + `SymbolDb` interning, isolating the churn-prone surface.

---

### Task 1: Crate scaffold + dependency wiring + error type

**Files:**
- Create: `crates/profiles/Cargo.toml`
- Create: `crates/profiles/src/lib.rs`
- Create: `crates/profiles/src/error.rs`
- Modify: root `Cargo.toml` (`[workspace] members` += `"crates/profiles"`; add `flate2`, `multer` to `[workspace.dependencies]` if absent)

**Interfaces:**
- Produces: a compiling `crabka-profiles` crate; `pub enum ProfilesError` (`thiserror`) with `fn status_code(&self) -> u16`; `pub fn crate_smoke() -> bool` (placeholder, removed in Task 3) so there is a test to run.

- [ ] **Step 1: Add the crate to the workspace + ingest deps**

In root `Cargo.toml` `[workspace] members`, add `"crates/profiles"`. Under `[workspace.dependencies]`, add (only those not already present — `prost`, `connectrpc-axum*`, `axum`, `bytes`, `serde*`, `tokio`, `clap`, `object_store`, `arrow` are already workspace deps used by sibling crates):

```toml
flate2 = "1"
multer = "3"
```

> **Verify against root `Cargo.toml`:** `connectrpc-axum` + `connectrpc-axum-build` must already be workspace deps (grpc-gateway/rebalancer use them) — if they are pinned per-crate there, mirror that exact spec. `flate2` decompresses the `push.v1` `raw_profile` (gzipped pprof). `multer` parses the `/ingest` multipart body. If either is already present, reuse it; do not duplicate.

- [ ] **Step 2: Create `crates/profiles/Cargo.toml`**

```toml
[package]
name = "crabka-profiles"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Crabka profiles ingest service (distributor + block-builder) — Grafana-Pyroscope replacement"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-profiles"
readme = "README.md"
keywords = ["observability", "profiling", "pyroscope", "pprof", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
arrow = { workspace = true }
object_store = { workspace = true }
prost = { workspace = true }
connectrpc-axum = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "time", "signal"] }
tower = { workspace = true }
futures = { workspace = true }
bytes = { workspace = true }
flate2 = { workspace = true }
multer = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde-wincode = { workspace = true }
wincode = { workspace = true }
clap = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
url = { workspace = true }
crabka-blockstore = { path = "../blockstore" }
crabka-pprof = { path = "../pprof" }
crabka-client-producer = { path = "../client-producer" }
crabka-client-consumer = { path = "../client-consumer" }
crabka-client-admin = { path = "../client-admin" }

[build-dependencies]
connectrpc-axum-build = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
crabka-broker = { path = "../broker" }
crabka-client-core = { path = "../client-core" }
```

> **Verify each `{ workspace = true }` resolves** against root `Cargo.toml`. `crabka-pprof` is the slice-2/3 crate; if its path differs (`../pprof`), align to the actual crate dir.

- [ ] **Step 3: Create `src/error.rs`**

```rust
//! Crate-wide error + the ingest-edge HTTP status mapping.

/// Errors across the profiles ingest pipeline.
#[derive(Debug, thiserror::Error)]
pub enum ProfilesError {
    #[error("unsupported content-type/format: {0}")]
    UnsupportedFormat(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("gunzip failed: {0}")]
    Gunzip(String),
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
    #[error("pprof: {0}")]
    Pprof(String),
}

impl ProfilesError {
    /// Map to the ingest-edge HTTP status (Pyroscope-shaped 4xx).
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            ProfilesError::UnsupportedFormat(_) => 415,
            ProfilesError::Decode(_)
            | ProfilesError::Gunzip(_)
            | ProfilesError::Invalid(_)
            | ProfilesError::Pprof(_)
            | ProfilesError::TooLarge { .. } => 400,
            ProfilesError::Wal(_) | ProfilesError::Produce(_) | ProfilesError::Block(_) => 500,
        }
    }
}

impl From<crabka_pprof::ProfileError> for ProfilesError {
    fn from(e: crabka_pprof::ProfileError) -> Self {
        ProfilesError::Pprof(e.to_string())
    }
}
```

> **Verify `crabka_pprof::ProfileError` is the slice-2 public name** before relying on the `From` impl; if it is re-exported under a different path, align the `use`/`From`.

- [ ] **Step 4: Create `src/lib.rs` + a smoke test**

```rust
//! Crabka profiles ingest service: distributor (push.v1 / `/ingest` / OTLP
//! `v1development` profiles doors) → `(tenant, series_fingerprint)`-partitioned
//! WAL, and the block-builder consumer group (samples fact table + dedup
//! per-block `SymbolDb` + `ProfileIndex`).
#![forbid(unsafe_code)]

pub mod error;

pub use error::ProfilesError;

/// Placeholder so the crate has a test until Task 3 lands real modules.
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
        assert!(ProfilesError::UnsupportedFormat("x".into()).status_code() == 415);
        assert!(ProfilesError::Decode("x".into()).status_code() == 400);
    }
}
```

- [ ] **Step 5: Build + run**

Run: `cargo test -p crabka-profiles`
Expected: compiles; 2 tests PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add Cargo.toml Cargo.lock crates/profiles/
git commit -m "feat(profiles): scaffold crabka-profiles crate + error type"
```

---

### Task 2: Vendored protos + Connect/prost codegen (`push.v1` + OTLP + pprof)

**Files:**
- Create: `crates/profiles/build.rs`
- Create: `crates/profiles/proto/push/v1/push.proto`
- Create: `crates/profiles/proto/types/v1/types.proto` (`LabelPair` shared message)
- Create: `crates/profiles/proto/opentelemetry/proto/profiles/v1development/profiles.proto`
- Create: `crates/profiles/proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto`
- Create: `crates/profiles/src/wire/mod.rs` (the codegen include + round-trip tests)
- Modify: `crates/profiles/src/lib.rs` (declare `pub mod wire;` + the `pb` include)

**Interfaces:**
- Produces: generated message + Connect-server types reachable as `crate::wire::pb::push::v1::{PushRequest, PushResponse, RawProfileSeries, RawSample, LabelPair}`, `crate::wire::pb::push::v1::pusher_service_connect::PusherServiceServiceBuilder`, `crate::wire::pb::otlp_profiles::{ExportProfilesServiceRequest, ExportProfilesServiceResponse, ProfilesData, ProfilesDictionary, Sample, Stack, ...}`, and `crate::wire::pb::otlp_profiles::profiles_service_connect::ProfilesServiceServiceBuilder`.

- [ ] **Step 1: Vendor the `push.v1` proto**

The pprof proto itself is owned by `crabka-pprof` (slice 2 — `PprofProfile`); this crate does **not** re-generate `perftools.profiles`. We only need the `push.v1` envelope (the `raw_profile` is gzipped pprof bytes the envelope carries) and OTLP.

Create `crates/profiles/proto/types/v1/types.proto`:

```proto
syntax = "proto3";
package types.v1;
// Vendored from mirror.gcr.io/grafana/pyroscope api/types/v1/types.proto @ <PIN A TAG/COMMIT>.

message LabelPair {
  string name = 1;
  string value = 2;
}
```

Create `crates/profiles/proto/push/v1/push.proto`:

```proto
syntax = "proto3";
package push.v1;
// Vendored from mirror.gcr.io/grafana/pyroscope api/push/v1/push.proto @ <PIN A TAG/COMMIT>.

import "types/v1/types.proto";

service PusherService {
  rpc Push(PushRequest) returns (PushResponse) {}
}

message PushRequest {
  repeated RawProfileSeries series = 1;
}

message PushResponse {}

message RawProfileSeries {
  repeated types.v1.LabelPair labels = 1;
  repeated RawSample samples = 2;
}

message RawSample {
  // gzipped pprof
  bytes raw_profile = 1;
  string ID = 2;
}
```

> **Verify field numbers/names against the pinned mirror.gcr.io/grafana/pyroscope tag.** `RawSample.raw_profile` @1 (gzipped pprof) and `RawSample.ID` @2 are byte-load-bearing for Alloy `pyroscope.write` compatibility. prost lowercases `ID` → `id`; the round-trip test below pins whatever the generated field name is.

- [ ] **Step 2: Vendor the OTLP `v1development` profiles protos (commit-pinned)**

Create `crates/profiles/proto/opentelemetry/proto/profiles/v1development/profiles.proto` and the collector `profiles_service.proto`, vendored from `open-telemetry/opentelemetry-proto` at a **pinned commit** (comment it at the top of each file). The minimal message set the distributor decodes (interned dictionary model):

```proto
syntax = "proto3";
package opentelemetry.proto.profiles.v1development;
// Vendored from open-telemetry/opentelemetry-proto
// opentelemetry/proto/profiles/v1development/profiles.proto @ <PIN A COMMIT SHA>.
// EXPERIMENTAL — this proto churns; behavior-pinned by a prost round-trip test.

message ProfilesData {
  repeated ResourceProfiles resource_profiles = 1;
  ProfilesDictionary dictionary = 2;
}

message ProfilesDictionary {
  repeated Mapping mapping_table = 1;
  repeated Location location_table = 2;
  repeated Function function_table = 3;
  repeated Link link_table = 4;
  repeated string string_table = 5;
  repeated KeyValue attribute_table = 6;
  repeated Stack stack_table = 7;
}

message ResourceProfiles {
  repeated ScopeProfiles scope_profiles = 2;
}
message ScopeProfiles {
  repeated Profile profiles = 2;
}

message Profile {
  repeated ValueType sample_type = 1;
  repeated Sample sample = 2;
  ValueType period_type = 8;
  int64 period = 9;
}

message ValueType { int32 type_strindex = 1; int32 unit_strindex = 2; }

message Sample {
  int32 stack_index = 1;
  repeated int32 values = 2;
  repeated int32 attribute_indices = 3;
  int32 link_index = 4;
  repeated uint64 timestamps_unix_nano = 5;
}

message Stack { repeated int32 location_indices = 1; }
message Location { uint64 address = 2; repeated Line line = 3; }
message Line { int32 function_index = 1; int64 line = 2; }
message Function { int32 name_strindex = 1; }
message Mapping { uint64 memory_start = 1; int32 filename_strindex = 5; }
message Link { bytes trace_id = 1; bytes span_id = 2; }
message KeyValue { int32 key_strindex = 1; AnyValue value = 2; }
message AnyValue { oneof value { string string_value = 1; int64 int_value = 2; } }
```

```proto
syntax = "proto3";
package opentelemetry.proto.collector.profiles.v1development;
// Vendored @ <SAME PIN>. The OTLP profiles collector service.

import "opentelemetry/proto/profiles/v1development/profiles.proto";

service ProfilesService {
  rpc Export(ExportProfilesServiceRequest) returns (ExportProfilesServiceResponse) {}
}

message ExportProfilesServiceRequest {
  repeated opentelemetry.proto.profiles.v1development.ResourceProfiles resource_profiles = 1;
  opentelemetry.proto.profiles.v1development.ProfilesDictionary dictionary = 2;
}
message ExportProfilesServiceResponse {}
```

> **Verify EVERY field number against the pinned commit** before relying on it — the `v1development` shape moved the dictionary out of `ProfilesData` and renamed `*_strindex` fields across revisions. This is the single highest-churn surface in the slice; the round-trip test pins the generated shape, not a fabricated one. If a field differs, fix the `.proto` and let the generated names follow. Keep the vendored set **minimal** (only what the distributor reads) — do not vendor the full proto.

- [ ] **Step 3: Create `build.rs` (Connect + prost codegen, system-protoc-with-fallback)**

```rust
//! Generates Connect-RPC server stubs + prost message types from the vendored
//! `push.v1` + OTLP `profiles/v1development` protos. Prefers a system `protoc`;
//! falls back to a vendored fetch only when none is found (mirrors
//! crates/grpc-gateway/build.rs so `--offline` works with system protoc).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/push/v1/push.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ];
    let includes = ["proto"];
    let mut builder = connectrpc_axum_build::compile_protos(&protos, &includes);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }
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

> **Verify the `connectrpc_axum_build::compile_protos(...).fetch_protoc(...).compile()` chain** against `crates/grpc-gateway/build.rs` (it is the exact pattern). Imported protos (`types/v1/types.proto`, `profiles.proto`) are compiled transitively via the `proto` include dir — they do not need to be in the `protos` list, but they DO need `rerun-if-changed` if you want incremental rebuilds (optional).

- [ ] **Step 4: Create `src/wire/mod.rs` with the codegen include + round-trip tests**

```rust
//! Generated message + Connect-server types from the vendored protos, plus
//! behavior-pinning round-trip tests (the generated shape is the source of
//! truth — these tests fail loudly if a vendored field number drifts).

/// Generated protobuf + Connect server stubs (the `OUT_DIR` includes).
#[allow(clippy::pedantic, clippy::style)]
pub mod pb {
    /// Pyroscope `push.v1.PusherService`.
    pub mod push {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/push.v1.rs"));
        }
    }
    /// OTLP collector profiles `v1development` (service + messages).
    pub mod otlp_profiles {
        include!(concat!(
            env!("OUT_DIR"),
            "/opentelemetry.proto.collector.profiles.v1development.rs"
        ));
    }
    /// OTLP profiles messages (`ProfilesDictionary`, `Sample`, `Stack`, …).
    pub mod otlp_profiles_msg {
        include!(concat!(
            env!("OUT_DIR"),
            "/opentelemetry.proto.profiles.v1development.rs"
        ));
    }
    /// Shared `types.v1.LabelPair`.
    pub mod types {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/types.v1.rs"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use prost::Message;

    #[test]
    fn push_request_round_trips_via_prost() {
        let req = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![pb::types::v1::LabelPair {
                    name: "__name__".into(),
                    value: "process_cpu".into(),
                }],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: vec![1, 2, 3],
                    id: "abc".into(),
                }],
            }],
        };
        let bytes = req.encode_to_vec();
        let back = pb::push::v1::PushRequest::decode(bytes.as_slice()).unwrap();
        assert!(back.series.len() == 1);
        assert!(back.series[0].samples[0].raw_profile == vec![1, 2, 3]);
    }

    #[test]
    fn otlp_profiles_dictionary_round_trips() {
        use pb::otlp_profiles_msg::{ProfilesDictionary, Sample, Stack};
        let dict = ProfilesDictionary {
            string_table: vec![String::new(), "samples".into(), "count".into()],
            stack_table: vec![Stack { location_indices: vec![0, 1] }],
            ..Default::default()
        };
        let bytes = dict.encode_to_vec();
        let back = ProfilesDictionary::decode(bytes.as_slice()).unwrap();
        assert!(back.string_table[0].is_empty());
        assert!(back.stack_table[0].location_indices == vec![0, 1]);
        let s = Sample { stack_index: 0, values: vec![5], ..Default::default() };
        let sb = s.encode_to_vec();
        assert!(Sample::decode(sb.as_slice()).unwrap().values == vec![5]);
    }
}
```

> **The `include!` module filenames are prost's package-name convention** (`/push.v1.rs`, `/opentelemetry.proto.collector.profiles.v1development.rs`, `/opentelemetry.proto.profiles.v1development.rs`, `/types.v1.rs`). If `cargo build` reports a missing file, list `OUT_DIR` (`cargo build -p crabka-profiles -v` prints it) and use the actual generated filenames — they are the source of truth. The `connect` submodule name (`pusher_service_connect` / `profiles_service_connect`) is the codegen's snake_case-of-the-service convention; confirm against the grpc-gateway generated module (`gateway_connect`).

- [ ] **Step 5: Declare the module + build**

In `lib.rs` add `pub mod wire;`.

Run: `cargo test -p crabka-profiles --lib wire::tests`
Expected: compiles (first build invokes `protoc`); both prost round-trip tests PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add Cargo.toml Cargo.lock crates/profiles/
git commit -m "feat(profiles): vendor push.v1 + OTLP profiles protos + connect/prost codegen"
```

---

### Task 3: `DecodedProfile` / `RawProfile` + relabel + require-service_name + label limits

**Files:**
- Create: `crates/profiles/src/ingest/mod.rs`
- Modify: `crates/profiles/src/lib.rs` (declare `pub mod ingest;`, drop the placeholder)

**Interfaces:**
- Produces (consumed by every `ingest/*` door + the distributor):
  - `struct RawProfile { pub labels: crabka_blockstore::Labels, pub profile: crabka_pprof::PprofProfile }` — one decoded pprof + its series labels, BEFORE the multi-value split.
  - `struct DecodedProfile { pub labels: crabka_blockstore::Labels, pub profile_type: String, pub samples: Vec<DecodedSample> }` — AFTER the split (one per sample type).
  - `struct DecodedSample { pub stacktrace_location_refs: Vec<u32>, pub value: i64, pub timestamp_ns: i64, pub span_id: Option<u64>, pub trace_id: Option<Vec<u8>> }`.
  - `struct TenantLimits { pub max_label_names_per_series: usize, pub max_label_value_len: usize, pub session_id_buckets: u64 }` (`Default`).
  - `struct RelabelConfig { pub source_labels: Vec<String>, pub regex: String, pub target_label: String, pub replacement: String, pub action: RelabelAction }`; `enum RelabelAction { Replace, Keep, Drop }`.
  - `fn require_service_name(labels: &mut Labels)` — inject `service_name="unknown_service"` when empty; mirror `__name__` from the metric name.
  - `fn cap_session_id(labels: &mut Labels, buckets: u64)` — replace `__session_id__` value with `modulo-hash(value) % buckets` (cardinality cap).
  - `fn enforce_limits(labels: &Labels, limits: &TenantLimits) -> Result<(), ProfilesError>`.
  - `fn apply_relabel(labels: &mut Labels, configs: &[RelabelConfig]) -> bool` — returns `false` if a `Drop`/`Keep` rule rejects the series.

- [ ] **Step 1: Write the failing tests**

Create `crates/profiles/src/ingest/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::Labels;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    #[test]
    fn require_service_name_injects_unknown() {
        let mut l = labels(&[("__name__", "process_cpu")]);
        require_service_name(&mut l);
        assert!(l.get("service_name") == Some("unknown_service"));
    }

    #[test]
    fn require_service_name_keeps_existing() {
        let mut l = labels(&[("__name__", "process_cpu"), ("service_name", "api")]);
        require_service_name(&mut l);
        assert!(l.get("service_name") == Some("api"));
    }

    #[test]
    fn session_id_is_modulo_hashed() {
        let mut a = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut a, 16);
        let v = a.get("__session_id__").unwrap();
        let n: u64 = v.parse().unwrap();
        assert!(n < 16);
        // stable: same input → same bucket.
        let mut b = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut b, 16);
        assert!(b.get("__session_id__") == a.get("__session_id__"));
    }

    #[test]
    fn enforce_limits_rejects_too_many_labels() {
        let limits = TenantLimits { max_label_names_per_series: 1, ..Default::default() };
        let l = labels(&[("a", "1"), ("b", "2")]);
        assert!(enforce_limits(&l, &limits).is_err());
    }

    #[test]
    fn relabel_drop_rejects_series() {
        let mut l = labels(&[("env", "dev"), ("__name__", "cpu")]);
        let cfg = RelabelConfig {
            source_labels: vec!["env".into()],
            regex: "dev".into(),
            target_label: String::new(),
            replacement: String::new(),
            action: RelabelAction::Drop,
        };
        assert!(!apply_relabel(&mut l, &[cfg]));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib ingest`
Expected: FAIL — `cannot find function require_service_name`.

- [ ] **Step 3: Implement `ingest/mod.rs`**

Prepend above the `tests` module. The `Labels` API is insert/get/iter (no `remove`), so rewrites rebuild a fresh `Labels` (the metrics slice-4 HA `strip_replica_label` does the same).

```rust
//! The decode target every push door lowers into, plus the distributor's
//! pre-WAL pipeline: relabel → require `service_name`/`__name__` → label limits
//! → `__session_id__` cardinality cap. The multi-value split lives in `split.rs`.

pub mod split;

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::error::ProfilesError;

/// One decoded pprof + its series labels, BEFORE the multi-value split.
#[derive(Debug, Clone)]
pub struct RawProfile {
    pub labels: Labels,
    pub profile: PprofProfile,
}

/// One series after the multi-value split: a single `__profile_type__`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedProfile {
    pub labels: Labels,
    pub profile_type: String,
    pub samples: Vec<DecodedSample>,
}

/// One sample's raw payload (un-symbolized; resolved at query time).
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// Per-tenant ingest limits.
#[derive(Debug, Clone)]
pub struct TenantLimits {
    pub max_label_names_per_series: usize,
    pub max_label_value_len: usize,
    pub session_id_buckets: u64,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_names_per_series: 30,
            max_label_value_len: 2048,
            session_id_buckets: 1024,
        }
    }
}

/// A Prometheus-style relabel rule (subset of `relabel_configs`).
#[derive(Debug, Clone)]
pub struct RelabelConfig {
    pub source_labels: Vec<String>,
    pub regex: String,
    pub target_label: String,
    pub replacement: String,
    pub action: RelabelAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelabelAction {
    Replace,
    Keep,
    Drop,
}

/// Inject `service_name="unknown_service"` when empty (spec §4.3). `__name__` is
/// assumed already set from the metric name by the door decoder.
pub fn require_service_name(labels: &mut Labels) {
    if labels.get("service_name").unwrap_or("").is_empty() {
        labels.insert("service_name", "unknown_service");
    }
}

/// Cardinality-cap `__session_id__` via modulo-hash (spec §4.3). FNV-1a over the
/// raw value `% buckets`; rewrites the label in place if present.
pub fn cap_session_id(labels: &mut Labels, buckets: u64) {
    let Some(raw) = labels.get("__session_id__").map(str::to_owned) else {
        return;
    };
    let buckets = buckets.max(1);
    let bucket = fnv1a(raw.as_bytes()) % buckets;
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != "__session_id__" {
            rebuilt.insert(name.clone(), value.clone());
        }
    }
    rebuilt.insert("__session_id__", bucket.to_string());
    *labels = rebuilt;
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// Enforce per-tenant structural caps (→ 400). Rate-limit (429) integrates with
/// Crabka quotas in hardening: `// TODO(slice4-quota)`.
pub fn enforce_limits(labels: &Labels, limits: &TenantLimits) -> Result<(), ProfilesError> {
    if labels.len() > limits.max_label_names_per_series {
        return Err(ProfilesError::Invalid(format!(
            "too many label names: {} > {}",
            labels.len(),
            limits.max_label_names_per_series
        )));
    }
    for (name, value) in labels.iter() {
        if value.len() > limits.max_label_value_len {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` value exceeds {} bytes",
                limits.max_label_value_len
            )));
        }
    }
    Ok(())
}

/// Apply relabel rules in order. Returns `false` if a `Drop` matches or a `Keep`
/// does not — the series is dropped. `Replace` rewrites `target_label`.
pub fn apply_relabel(labels: &mut Labels, configs: &[RelabelConfig]) -> bool {
    for cfg in configs {
        let joined = cfg
            .source_labels
            .iter()
            .map(|n| labels.get(n).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(";");
        let re = match regex_anchored(&cfg.regex) {
            Ok(re) => re,
            Err(_) => continue,
        };
        let matched = re.is_match(&joined);
        match cfg.action {
            RelabelAction::Drop if matched => return false,
            RelabelAction::Keep if !matched => return false,
            RelabelAction::Replace if matched => {
                let mut rebuilt = Labels::new();
                for (n, v) in labels.iter() {
                    if n != &cfg.target_label {
                        rebuilt.insert(n.clone(), v.clone());
                    }
                }
                if !cfg.replacement.is_empty() {
                    rebuilt.insert(cfg.target_label.clone(), cfg.replacement.clone());
                }
                *labels = rebuilt;
            }
            _ => {}
        }
    }
    true
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
}
```

> **`regex` dep:** add `regex = { workspace = true }` to `crates/profiles/Cargo.toml` `[dependencies]` (it is a workspace dep used by blockstore). The relabel `Replace` here is the minimal subset the distributor needs; the full `relabel_configs` grammar (modulus/hashmod/labelmap) is a hardening follow-on — `// TODO(slice4-relabel-full)`. The structural caps + drop/keep/replace are implemented and tested now.

- [ ] **Step 4: Declare + run**

`lib.rs`: `pub mod ingest;` and (after split.rs lands in Task 4) the re-exports. For now run:

Run: `cargo test -p crabka-profiles --lib ingest`
Expected: FAIL to compile until `pub mod split;` exists — create an empty `crates/profiles/src/ingest/split.rs` with `//! multi-value split (Task 4).` so the module resolves, then re-run. Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): DecodedProfile + relabel/require-service_name/limits/session-cap"
```

---

### Task 4: `split_sample_types` — multi-value split → one series per profile type

**Files:**
- Modify: `crates/profiles/src/ingest/split.rs`
- Modify: `crates/profiles/src/ingest/mod.rs` (re-export)

**Interfaces:**
- Consumes: `RawProfile`, `crabka_pprof::{PprofProfile, ProfileType}`, `DecodedProfile`, `DecodedSample`.
- Produces:
  - `fn split_sample_types(raw: &RawProfile) -> Result<Vec<DecodedProfile>, ProfilesError>` — for each `sample_type[i]` build one `DecodedProfile` whose `profile_type` is the 5-part `name:sample_type:sample_unit:period_type:period_unit` string and whose samples take `value[i]` from each pprof sample. Sets `__profile_type__` + `__period_type__`/`__period_unit__` labels.

> This is exactly phlaredb's `CreateProfileLabels` looping `sample_type` (spec §4.3): a Go heap profile (`sample_type = [alloc_objects, alloc_space, inuse_objects, inuse_space]`) yields **4** series. The pprof `name` for the profile type comes from the `__name__` label; `period_type`/`period_unit` come from the pprof `period_type`.

- [ ] **Step 1: Write the failing test**

Replace `crates/profiles/src/ingest/split.rs`:

```rust
//! Multi-value split: one pprof with N `sample_type[]` → N `DecodedProfile`s,
//! each a single 5-part `__profile_type__`. Mirrors phlaredb `CreateProfileLabels`.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::RawProfile;
    use assert2::assert;
    use crabka_blockstore::Labels;
    use crabka_pprof::PprofProfile;

    // Build a 2-sample-type pprof in-memory via crabka-pprof's builder/model.
    // (Use whatever constructor crabka-pprof exposes; here we decode a fixture.)
    fn two_type_profile() -> PprofProfile {
        // A pprof with sample_type=[alloc_objects:count, alloc_space:bytes],
        // one sample with value=[3, 4096], one location ref [7].
        crate::wire::test_fixtures::heap_profile_2types()
    }

    #[test]
    fn split_yields_one_series_per_sample_type() {
        let mut labels = Labels::new();
        labels.insert("__name__", "memory");
        labels.insert("service_name", "api");
        let raw = RawProfile { labels, profile: two_type_profile() };

        let out = split_sample_types(&raw).unwrap();
        assert!(out.len() == 2);

        let types: Vec<&str> = out.iter().map(|p| p.profile_type.as_str()).collect();
        assert!(types.iter().any(|t| t.starts_with("memory:alloc_objects:count:")));
        assert!(types.iter().any(|t| t.starts_with("memory:alloc_space:bytes:")));

        // The first series takes value[0], the second value[1].
        let objs = out.iter().find(|p| p.profile_type.contains("alloc_objects")).unwrap();
        let space = out.iter().find(|p| p.profile_type.contains("alloc_space")).unwrap();
        assert!(objs.samples[0].value == 3);
        assert!(space.samples[0].value == 4096);
        assert!(objs.samples[0].stacktrace_location_refs == vec![7]);

        // __profile_type__ label is set on each split series.
        assert!(objs.labels.get("__profile_type__") == Some(objs.profile_type.as_str()));
    }
}
```

> **Verify `crabka-pprof`'s in-memory `PprofProfile` constructor.** If slice 2 exposes a builder, use it; otherwise decode a small committed `.pb.gz` fixture (`tests/fixtures/heap.pb.gz`) via `PprofProfile::decode`. Add a `pub(crate) mod test_fixtures` under `wire` (or `ingest`) returning the parsed fixture so multiple tests reuse it. Do not fabricate `PprofProfile` field access — read the slice-2 model and use its public getters.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib ingest::split`
Expected: FAIL — `cannot find function split_sample_types`.

- [ ] **Step 3: Implement `split.rs`**

Prepend above the `tests` module. Read the `crabka-pprof` `PprofProfile` accessors (`sample_type()`, `samples()`, each sample's `value[]` + `location_id[]`/`stacktrace location refs`, `string_table`, `period_type`) — align to the slice-2 public API.

```rust
use crabka_blockstore::Labels;
use crabka_pprof::{ProfileType, PprofProfile};

use crate::error::ProfilesError;
use crate::ingest::{DecodedProfile, DecodedSample, RawProfile};

/// Split one multi-value pprof into one `DecodedProfile` per `sample_type[]`.
pub fn split_sample_types(raw: &RawProfile) -> Result<Vec<DecodedProfile>, ProfilesError> {
    let profile = &raw.profile;
    let name = raw.labels.get("__name__").unwrap_or("").to_string();
    if name.is_empty() {
        return Err(ProfilesError::Invalid("missing __name__".into()));
    }

    // 5-part profile-type components shared by all split series.
    let (period_type, period_unit) = profile.period_type_strings();

    let sample_types = profile.sample_types(); // Vec<(type_str, unit_str)>
    let mut out = Vec::with_capacity(sample_types.len());

    for (i, (stype, sunit)) in sample_types.iter().enumerate() {
        let pt = ProfileType {
            name: name.clone(),
            sample_type: stype.clone(),
            sample_unit: sunit.clone(),
            period_type: period_type.clone(),
            period_unit: period_unit.clone(),
        };
        let profile_type = pt.to_string(); // 5-part colon form via Display

        let mut labels = raw.labels.clone();
        labels.insert("__profile_type__", profile_type.clone());
        labels.insert("__period_type__", period_type.clone());
        labels.insert("__period_unit__", period_unit.clone());

        let mut samples = Vec::new();
        for s in profile.samples() {
            let value = s.value_at(i).ok_or_else(|| {
                ProfilesError::Decode(format!("sample value[{i}] missing"))
            })?;
            samples.push(DecodedSample {
                stacktrace_location_refs: s.location_refs().to_vec(),
                value,
                timestamp_ns: s.timestamp_ns().unwrap_or(0),
                span_id: s.span_id(),
                trace_id: s.trace_id(),
            });
        }
        out.push(DecodedProfile { labels, profile_type, samples });
    }
    Ok(out)
}
```

> **Pin the `crabka-pprof` accessor names.** Slice 2 (`PprofProfile`) defines `sample_types() -> Vec<(String, String)>` and `string(idx)` today; the rest of the accessors this split needs (`period_type_strings`, `samples`, `value_at`, `location_refs`, `timestamp_ns`, `span_id`, `trace_id`) are **NOT yet on the slice-2 `PprofProfile`** — they are a genuine producer-API gap, not a rename. Add them to slice 2 (greenfield) as part of this task. If slice 2 returns string-table indices instead of resolved strings, resolve them here against `profile.string(idx)`. The behavior the test pins (N series, value[i] alignment, 5-part type) is correct regardless; **never fabricate getters** — add the missing ones to slice 2's `PprofProfile` and note them.

- [ ] **Step 4: Re-export + run**

`ingest/mod.rs`: `pub use split::split_sample_types;`

Run: `cargo test -p crabka-profiles --lib ingest::split`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): multi-value split — one series per pprof sample_type"
```

---

### Task 5: `ProfileRecord` — the WAL topic record (Slices 5/6/7 consume this)

**Files:**
- Create: `crates/profiles/src/wal.rs`
- Modify: `crates/profiles/src/lib.rs`

**Interfaces:**
- Produces (the SHARED CONTRACT this slice owns):
  - `const PROFILES_WAL_TOPIC: &str = "__crabka_profiles_wal"`
  - `struct ProfileRecord { pub tenant: String, pub labels: Vec<(String, String)>, pub profile_type: String, pub samples: Vec<WalSample>, pub symbols: WalSymbolSet }` (`serde`, `Clone`, `Debug`, `PartialEq`)
  - `struct WalSample { pub stacktrace_location_refs: Vec<u32>, pub value: i64, pub timestamp_ns: i64, pub span_id: Option<u64>, pub trace_id: Option<Vec<u8>> }`
  - `struct WalSymbolSet { pub strings: Vec<String>, pub functions: Vec<WalFunction>, pub locations: Vec<WalLocation>, pub mappings: Vec<WalMapping> }` — the profile's symbol tables (string-index encoded) the block-builder merges into the per-block `SymbolDb`.
  - `struct WalFunction { pub name: u32, pub system_name: u32, pub filename: u32, pub start_line: i64 }`
  - `struct WalLocation { pub address: u64, pub mapping_id: u32, pub lines: Vec<(u32, i64)> }` (`(function_id, line)`; multiple = inlined frames)
  - `struct WalMapping { pub memory_start: u64, pub memory_limit: u64, pub file_offset: u64, pub filename: u32, pub build_id: u32, pub has_functions: bool }`
  - `ProfileRecord::encode(&self) -> Result<Vec<u8>, ProfilesError>` / `ProfileRecord::decode(&[u8]) -> Result<ProfileRecord, ProfilesError>` (via `serde-wincode`).
  - `ProfileRecord::series_fingerprint(&self) -> u64` (via blockstore `Labels::fingerprint`).
  - `fn partition_key(tenant: &str, fp: u64) -> bytes::Bytes` — the produce key; hash of `(tenant, fp)`.

> The `WalSymbolSet` carries the profile's location/function/mapping/string tables (index-encoded, exactly pprof's shape) so the block-builder can `intern_stacktrace` each record's `stacktrace_location_refs` into the per-block `SymbolDb` without re-reading the original pprof. This keeps the WAL self-contained (a block-builder consumes the WAL alone).

- [ ] **Step 1: Write the failing tests**

Create `crates/profiles/src/wal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn sym() -> WalSymbolSet {
        WalSymbolSet {
            strings: vec![String::new(), "main".into(), "main.go".into()],
            functions: vec![WalFunction { name: 1, system_name: 1, filename: 2, start_line: 10 }],
            locations: vec![WalLocation { address: 0x40, mapping_id: 0, lines: vec![(0, 12)] }],
            mappings: vec![WalMapping {
                memory_start: 0,
                memory_limit: 0x1000,
                file_offset: 0,
                filename: 2,
                build_id: 0,
                has_functions: true,
            }],
        }
    }

    fn record() -> ProfileRecord {
        ProfileRecord {
            tenant: "t1".into(),
            labels: vec![
                ("__name__".into(), "process_cpu".into()),
                ("service_name".into(), "api".into()),
            ],
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0],
                value: 1500,
                timestamp_ns: 1_700_000_000_000_000_000,
                span_id: Some(42),
                trace_id: Some(vec![0xaa; 16]),
            }],
            symbols: sym(),
        }
    }

    #[test]
    fn record_round_trips() {
        let rec = record();
        let bytes = rec.encode().unwrap();
        let back = ProfileRecord::decode(&bytes).unwrap();
        assert!(back == rec);
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = record();
        let mut b = a.clone();
        b.labels = vec![
            ("service_name".into(), "api".into()),
            ("__name__".into(), "process_cpu".into()),
        ];
        assert!(a.series_fingerprint() == b.series_fingerprint());
    }

    #[test]
    fn partition_key_is_stable_and_distinct() {
        let k1 = partition_key("t", 42);
        let k2 = partition_key("t", 42);
        let k3 = partition_key("t", 43);
        let k4 = partition_key("u", 42);
        assert!(k1 == k2);
        assert!(k1 != k3);
        assert!(k1 != k4);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib wal`
Expected: FAIL — `cannot find type ProfileRecord`.

- [ ] **Step 3: Implement `wal.rs`**

```rust
//! The profiles WAL topic record. Produced by the distributor, consumed by the
//! block-builder (this slice) and the querier's hot tail (Slice 5). Encoded with
//! `serde-wincode` (the codebase convention; see `crates/broker/src/bootstrap.rs`).

use bytes::Bytes;
use crabka_blockstore::Labels;
use serde::{Deserialize, Serialize};

use crate::error::ProfilesError;

/// The profiles WAL topic name.
pub const PROFILES_WAL_TOPIC: &str = "__crabka_profiles_wal";

/// One sample's raw payload (un-symbolized; resolved at query time).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// A function entry; string fields are indices into `WalSymbolSet.strings`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalFunction {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

/// A location: an address + lines `(function_id, line)`; multiple lines = inlined.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalLocation {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<(u32, i64)>,
}

/// A mapping (binary). `has_functions == false` marks an unsymbolized mapping.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalMapping {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub has_functions: bool,
}

/// The profile's symbol tables, index-encoded (pprof shape). The block-builder
/// merges these into the per-block `SymbolDb`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalSymbolSet {
    pub strings: Vec<String>,
    pub functions: Vec<WalFunction>,
    pub locations: Vec<WalLocation>,
    pub mappings: Vec<WalMapping>,
}

/// A single profiles WAL record (one series, one profile type).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileRecord {
    pub tenant: String,
    pub labels: Vec<(String, String)>,
    pub profile_type: String,
    pub samples: Vec<WalSample>,
    pub symbols: WalSymbolSet,
}

impl ProfileRecord {
    /// Encode via `serde-wincode` (matches the broker's metadata-record codec).
    pub fn encode(&self) -> Result<Vec<u8>, ProfilesError> {
        <serde_wincode::SerdeCompat<ProfileRecord> as wincode::Serialize>::serialize(self)
            .map_err(|e| ProfilesError::Wal(e.to_string()))
    }

    /// Decode a `ProfileRecord` from its `serde-wincode` bytes.
    pub fn decode(bytes: &[u8]) -> Result<ProfileRecord, ProfilesError> {
        <serde_wincode::SerdeCompat<ProfileRecord>>::deserialize(bytes)
            .map_err(|e| ProfilesError::Wal(e.to_string()))
    }

    /// Series fingerprint via the blockstore `Labels` hash (order-independent).
    #[must_use]
    pub fn series_fingerprint(&self) -> u64 {
        let mut l = Labels::new();
        for (name, value) in &self.labels {
            l.insert(name.clone(), value.clone());
        }
        l.fingerprint()
    }
}

/// The produce key for a WAL record: hash of `(tenant, fingerprint)` so all
/// samples of one series in one tenant land on one partition (per-series order).
/// The producer MurmurHash2-partitions on this key.
#[must_use]
pub fn partition_key(tenant: &str, fp: u64) -> Bytes {
    let mut buf = Vec::with_capacity(tenant.len() + 8);
    buf.extend_from_slice(tenant.as_bytes());
    buf.extend_from_slice(&fp.to_be_bytes());
    Bytes::from(buf)
}
```

> **Verify the `serde-wincode` call shape** against `crates/broker/src/bootstrap.rs` (`<SerdeCompat<T> as wincode::Serialize>::serialize(&value) -> Result<Vec<u8>, _>` and `<SerdeCompat<T>>::deserialize(&[u8]) -> Result<T, _>` — confirmed: `bootstrap.rs:104` / `metadata_source.rs:254`). The partition key bytes are an internal contract (the producer hashes them); deterministic layout, free to change.

- [ ] **Step 4: Declare + run**

`lib.rs`: `mod wal; pub use wal::{partition_key, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample, WalSymbolSet, PROFILES_WAL_TOPIC};`

Run: `cargo test -p crabka-profiles --lib wal`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): ProfileRecord WAL topic record + serde-wincode codec (slice-4 contract)"
```

---

### Task 6: `push.v1` door — `PushRequest` → `Vec<RawProfile>` (gunzip + pprof decode)

**Files:**
- Create: `crates/profiles/src/ingest/push_v1.rs`
- Modify: `crates/profiles/src/ingest/mod.rs`

**Interfaces:**
- Consumes: `pb::push::v1::PushRequest`, `pb::types::v1::LabelPair`, `crabka_pprof::PprofProfile`.
- Produces:
  - `fn decode_push(req: &pb::push::v1::PushRequest, max_decompressed: usize) -> Result<Vec<RawProfile>, ProfilesError>` — for each `RawProfileSeries`, build `Labels` from `labels[]`, then for each `RawSample` gunzip `raw_profile` and `PprofProfile::decode` → one `RawProfile`.
  - `fn gunzip(body: &[u8], max_output: usize) -> Result<Vec<u8>, ProfilesError>` — `flate2::read::GzDecoder`, output-size-capped.

- [ ] **Step 1: Write the failing tests**

Create `crates/profiles/src/ingest/push_v1.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;
    use std::io::Write;

    fn gzip(raw: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(raw).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn gunzip_round_trips_and_caps() {
        let raw = b"the quick brown fox";
        let gz = gzip(raw);
        assert!(gunzip(&gz, 1 << 20).unwrap() == raw);
        assert!(gunzip(&gz, 4).is_err()); // output cap exceeded
    }

    #[test]
    fn decode_push_gunzips_and_parses_pprof() {
        // A committed minimal pprof fixture (gzipped) embedded in the envelope.
        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let req = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![
                    pb::types::v1::LabelPair { name: "__name__".into(), value: "process_cpu".into() },
                    pb::types::v1::LabelPair { name: "service_name".into(), value: "api".into() },
                ],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: gzip(&pprof_bytes),
                    id: "s1".into(),
                }],
            }],
        };
        let out = decode_push(&req, 1 << 20).unwrap();
        assert!(out.len() == 1);
        assert!(out[0].labels.get("__name__") == Some("process_cpu"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib ingest::push_v1`
Expected: FAIL — `cannot find function decode_push`.

- [ ] **Step 3: Implement `push_v1.rs`**

```rust
//! Connect `push.v1.PusherService/Push` decode: each `RawSample.raw_profile` is
//! a gzipped pprof; gunzip → `PprofProfile::decode` → one `RawProfile` per sample.

use std::io::Read;

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::error::ProfilesError;
use crate::ingest::RawProfile;
use crate::wire::pb;

/// Gunzip a gzipped body with an output-size cap (rejects decompression bombs).
pub fn gunzip(body: &[u8], max_output: usize) -> Result<Vec<u8>, ProfilesError> {
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| ProfilesError::Gunzip(e.to_string()))?;
        if n == 0 {
            break;
        }
        if out.len() + n > max_output {
            return Err(ProfilesError::TooLarge { limit: max_output });
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

/// Decode a `push.v1` `PushRequest` into per-(series, sample) `RawProfile`s.
pub fn decode_push(
    req: &pb::push::v1::PushRequest,
    max_decompressed: usize,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let mut out = Vec::new();
    for series in &req.series {
        let mut labels = Labels::new();
        for lp in &series.labels {
            labels.insert(lp.name.clone(), lp.value.clone());
        }
        for sample in &series.samples {
            let raw = gunzip(&sample.raw_profile, max_decompressed)?;
            let profile = PprofProfile::decode(&raw)?;
            out.push(RawProfile { labels: labels.clone(), profile });
        }
    }
    Ok(out)
}
```

> **Verify `PprofProfile::decode(&[u8]) -> Result<_, ProfileError>`** against slice 2 (the `?` relies on the `From<ProfileError> for ProfilesError` in Task 1). Add a committed minimal pprof fixture under `crates/profiles/tests/fixtures/` and a `pub(crate) mod test_fixtures` in `wire/mod.rs` returning its bytes (`include_bytes!`), reused by Tasks 4/6/11. Generate the fixture from a real Go pprof or via slice-2's encoder — do not hand-author proto bytes.

- [ ] **Step 4: Re-export + run**

`ingest/mod.rs`: `pub mod push_v1; pub use push_v1::{decode_push, gunzip};`

Run: `cargo test -p crabka-profiles --lib ingest::push_v1`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): push.v1 door — gunzip raw_profile + pprof decode"
```

---

### Task 7: OTLP `v1development` door + legacy `/ingest` door

**Files:**
- Create: `crates/profiles/src/ingest/otlp.rs`
- Create: `crates/profiles/src/ingest/legacy.rs`
- Modify: `crates/profiles/src/ingest/mod.rs`

**Interfaces:**
- Produces:
  - `fn decode_otlp(req: &pb::otlp_profiles::ExportProfilesServiceRequest) -> Result<Vec<RawProfile>, ProfilesError>` — resolve the interned `ProfilesDictionary` (`string_table`, `stack_table`, `location_table`, `function_table`, `mapping_table`) into a `PprofProfile`-equivalent `RawProfile` per `Profile`, deriving `__name__`/`service_name` from resource attributes.
  - `struct IngestQuery { pub name: String, pub labels: Vec<(String, String)>, pub format: IngestFormat, pub sample_rate: u32 }`; `enum IngestFormat { Pprof, Jfr, Groups }`.
  - `fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError>` — parse `?name=app{k="v"}&format=pprof&sampleRate=100`; empty/unknown `format` → `Groups`.
  - `async fn decode_ingest_multipart(query: &IngestQuery, content_type: &str, body: bytes::Bytes, max: usize) -> Result<RawProfile, ProfilesError>` — multipart `profile` part (pprof) + optional `sample_type_config` JSON part; `jfr`/`groups` paths stubbed with explicit errors (see scope guard).

> **Scope guard:** the `pprof` `/ingest` path (multipart `profile` + `sample_type_config`) is implemented + tested fully. The `jfr` (multipart `jfr` + `labels`) and `groups`/`tree`/`lines`/`speedscope` folded-text paths return `ProfilesError::UnsupportedFormat` with a `// TODO(slice4-ingest-jfr)` / `// TODO(slice4-ingest-folded)` marker — JFR parsing is a focused follow-on (it needs a JFR chunk reader); the `pprof` multipart door is the one Alloy/SDK pprof uploads use and is the load-bearing one for this slice. Do not silently accept-and-drop an unsupported format.

- [ ] **Step 1: Write the failing tests**

Create `crates/profiles/src/ingest/otlp.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;

    #[test]
    fn otlp_resolves_dictionary_into_rawprofile() {
        use pb::otlp_profiles_msg::{
            Function, Location, Line, ProfilesDictionary, Profile, ResourceProfiles,
            ScopeProfiles, Sample, Stack, ValueType,
        };
        let dict = ProfilesDictionary {
            string_table: vec![
                String::new(), "samples".into(), "count".into(), "main".into(),
            ],
            function_table: vec![Function { name_strindex: 3 }],
            location_table: vec![Location { address: 0x40, line: vec![Line { function_index: 0, line: 1 }] }],
            stack_table: vec![Stack { location_indices: vec![0] }],
            ..Default::default()
        };
        let profile = Profile {
            sample_type: vec![ValueType { type_strindex: 1, unit_strindex: 2 }],
            sample: vec![Sample {
                stack_index: 0,
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_000],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req = pb::otlp_profiles::ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                scope_profiles: vec![ScopeProfiles { profiles: vec![profile], ..Default::default() }],
                ..Default::default()
            }],
            dictionary: Some(dict),
        };
        let out = decode_otlp(&req).unwrap();
        assert!(out.len() == 1);
        // Has at least the synthesized sample_type "samples".
        assert!(!out[0].profile.sample_types().is_empty());
    }
}
```

Create `crates/profiles/src/ingest/legacy.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn parse_query_extracts_name_labels_format() {
        let q = parse_ingest_query("name=myapp{env=\"prod\",team=\"core\"}&format=pprof&sampleRate=97")
            .unwrap();
        assert!(q.name == "myapp");
        assert!(q.labels.contains(&("env".to_string(), "prod".to_string())));
        assert!(matches!(q.format, IngestFormat::Pprof));
        assert!(q.sample_rate == 97);
    }

    #[test]
    fn unknown_format_defaults_to_groups() {
        let q = parse_ingest_query("name=app").unwrap();
        assert!(matches!(q.format, IngestFormat::Groups));
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p crabka-profiles --lib ingest::otlp ingest::legacy`
Expected: FAIL — `cannot find function decode_otlp`.

- [ ] **Step 3: Implement `otlp.rs`**

Resolve the interned dictionary into a `PprofProfile` via slice-2's `PprofProfile` constructor (or build the pprof tables directly and `decode` the re-encoded bytes — prefer a slice-2 builder if it exists). The key mapping: OTLP `Sample.stack_index` → `Stack.location_indices` → pprof location refs; `string_table`/`function_table`/`location_table` map 1:1 onto pprof's tables.

```rust
//! OTLP `v1development` profiles → `Vec<RawProfile>`. Resolves the interned
//! `ProfilesDictionary` into a pprof-equivalent profile per OTLP `Profile`.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::error::ProfilesError;
use crate::ingest::RawProfile;
use crate::wire::pb;

pub fn decode_otlp(
    req: &pb::otlp_profiles::ExportProfilesServiceRequest,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let dict = req
        .dictionary
        .as_ref()
        .ok_or_else(|| ProfilesError::Invalid("OTLP profiles missing dictionary".into()))?;
    let mut out = Vec::new();
    for rp in &req.resource_profiles {
        // service.name from resource attributes (resolved via dict.string_table).
        let service = resolve_service_name(rp, dict);
        for sp in &rp.scope_profiles {
            for p in &sp.profiles {
                // Build a pprof-equivalent profile from the interned dictionary.
                let profile = PprofProfile::from_otlp(p, dict)
                    .map_err(|e| ProfilesError::Decode(e.to_string()))?;
                let mut labels = Labels::new();
                labels.insert("service_name", service.clone());
                // __name__ derives from the first sample_type type string.
                if let Some((name, _)) = profile.sample_types().first() {
                    labels.insert("__name__", name.clone());
                }
                out.push(RawProfile { labels, profile });
            }
        }
    }
    Ok(out)
}

fn resolve_service_name(
    _rp: &pb::otlp_profiles_msg::ResourceProfiles,
    _dict: &pb::otlp_profiles_msg::ProfilesDictionary,
) -> String {
    // TODO(slice4-otlp-resource): walk resource.attributes for `service.name`
    // (string-index keyed). Until then `unknown_service` (require_service_name
    // also enforces this downstream).
    "unknown_service".to_string()
}
```

> **`PprofProfile::from_otlp(profile, dict)` is a slice-2 constructor you may need to add** — the OTLP dictionary → pprof-tables mapping belongs in `crabka-pprof` (it owns the pprof model), not here. If slice 2 lacks it, add it there as part of this task (greenfield) with its own unit test, and consume it here. Do NOT reconstruct the pprof tables inline in this crate — that duplicates the model. Verify the OTLP field names (`stack_index`, `location_indices`, `name_strindex`, `type_strindex`) against the Task-2 generated types.

- [ ] **Step 4: Implement `legacy.rs`** (query parse + pprof multipart)

```rust
//! Legacy `POST /ingest` door: query (`?name=app{labels}&format=...`) + multipart
//! body (the `profile` pprof part + an optional `sample_type_config` JSON part).

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::error::ProfilesError;
use crate::ingest::RawProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFormat {
    Pprof,
    Jfr,
    Groups,
}

#[derive(Debug, Clone)]
pub struct IngestQuery {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub format: IngestFormat,
    pub sample_rate: u32,
}

/// Parse the `/ingest` query string. `name=app{k="v",...}` carries the app name
/// + inline labels; `format` ∈ pprof/jfr/...; empty/unknown → groups.
pub fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError> {
    let mut name = String::new();
    let mut labels = Vec::new();
    let mut format = IngestFormat::Groups;
    let mut sample_rate = 100;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let v = urldecode(v);
        match k {
            "name" => {
                let (app, lbls) = split_app_labels(&v)?;
                name = app;
                labels = lbls;
            }
            "format" => {
                format = match v.as_str() {
                    "pprof" => IngestFormat::Pprof,
                    "jfr" => IngestFormat::Jfr,
                    _ => IngestFormat::Groups,
                };
            }
            "sampleRate" => sample_rate = v.parse().unwrap_or(100),
            _ => {}
        }
    }
    if name.is_empty() {
        return Err(ProfilesError::Invalid("missing ?name".into()));
    }
    Ok(IngestQuery { name, labels, format, sample_rate })
}

/// `app{k="v",k2="v2"}` → ("app", [("k","v"),("k2","v2")]).
fn split_app_labels(s: &str) -> Result<(String, Vec<(String, String)>), ProfilesError> {
    let Some(open) = s.find('{') else {
        return Ok((s.to_string(), Vec::new()));
    };
    let app = s[..open].to_string();
    let inner = s[open + 1..]
        .strip_suffix('}')
        .ok_or_else(|| ProfilesError::Invalid("unterminated label set".into()))?;
    let mut labels = Vec::new();
    for kv in inner.split(',').filter(|p| !p.is_empty()) {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| ProfilesError::Invalid("bad label pair".into()))?;
        labels.push((k.trim().to_string(), v.trim().trim_matches('"').to_string()));
    }
    Ok((app, labels))
}

fn urldecode(s: &str) -> String {
    // minimal: %XX + '+' → space. (Pyroscope query values are simple.)
    let mut out = String::with_capacity(s.len());
    let mut bytes = s.bytes().peekable();
    while let Some(b) = bytes.next() {
        match b {
            b'+' => out.push(' '),
            b'%' => {
                let hi = bytes.next();
                let lo = bytes.next();
                if let (Some(h), Some(l)) = (hi, lo) {
                    if let Ok(n) = u8::from_str_radix(&format!("{}{}", h as char, l as char), 16) {
                        out.push(n as char);
                        continue;
                    }
                }
            }
            _ => out.push(b as char),
        }
    }
    out
}

/// Decode the multipart `/ingest` body into one `RawProfile` (pprof path).
pub async fn decode_ingest_multipart(
    query: &IngestQuery,
    content_type: &str,
    body: bytes::Bytes,
    max: usize,
) -> Result<RawProfile, ProfilesError> {
    if query.format != IngestFormat::Pprof {
        // TODO(slice4-ingest-jfr) / TODO(slice4-ingest-folded)
        return Err(ProfilesError::UnsupportedFormat(format!("{:?}", query.format)));
    }
    let boundary = multer::parse_boundary(content_type)
        .map_err(|e| ProfilesError::Invalid(e.to_string()))?;
    let mut mp = multer::Multipart::new(
        futures::stream::once(async move { Ok::<_, std::io::Error>(body) }),
        boundary,
    );
    let mut pprof_bytes: Option<Vec<u8>> = None;
    while let Some(field) = mp.next_field().await.map_err(|e| ProfilesError::Invalid(e.to_string()))? {
        let fname = field.name().unwrap_or("").to_string();
        let data = field.bytes().await.map_err(|e| ProfilesError::Invalid(e.to_string()))?;
        if data.len() > max {
            return Err(ProfilesError::TooLarge { limit: max });
        }
        if fname == "profile" {
            pprof_bytes = Some(data.to_vec());
        }
        // "sample_type_config" JSON part: parsed for units/aggregation overrides;
        // unused in the minimal pprof path (// TODO(slice4-sample-type-config)).
    }
    let raw = pprof_bytes.ok_or_else(|| ProfilesError::Invalid("missing `profile` part".into()))?;
    let profile = PprofProfile::decode(&raw)?;
    let mut labels = Labels::new();
    labels.insert("__name__", query.name.clone());
    for (k, v) in &query.labels {
        labels.insert(k.clone(), v.clone());
    }
    Ok(RawProfile { labels, profile })
}
```

> **Verify the `multer` API** (`parse_boundary`, `Multipart::new(stream, boundary)`, `next_field`, `field.name`, `field.bytes`) against the resolved `multer` version. If `multer` is not the chosen multipart crate in the workspace, use whatever the repo already depends on (axum's `Multipart` extractor wraps `multer` — the handler in Task 8 can use `axum::extract::Multipart` directly instead, simplifying this). The `urldecode` is intentionally minimal; if the repo has a urlencoding dep, use it.

- [ ] **Step 5: Re-export + run**

`ingest/mod.rs`: `pub mod otlp; pub mod legacy; pub use otlp::decode_otlp; pub use legacy::{decode_ingest_multipart, parse_ingest_query, IngestFormat, IngestQuery};`

Run: `cargo test -p crabka-profiles --lib ingest`
Expected: PASS (all ingest tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): OTLP v1development + legacy /ingest pprof doors"
```

---

### Task 8: Distributor — Connect `push.v1` + OTLP `Export` builders + `/ingest` route, produce

**Files:**
- Create: `crates/profiles/src/distributor/mod.rs`
- Modify: `crates/profiles/src/lib.rs`

**Interfaces:**
- Produces:
  - `trait WalSink: Send + Sync { async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError>; }` (the recording-fake seam for tests; `KafkaSink` wraps the producer).
  - `struct DistributorState { pub sink: Arc<dyn WalSink>, pub limits: TenantLimits, pub relabel: Vec<RelabelConfig>, pub max_decompressed: usize }` (the axum `State`/`Extension`).
  - `fn router(state: Arc<DistributorState>) -> axum::Router` — Connect `push.v1` + OTLP `ProfilesService` builders **plus** a plain `POST /ingest` route.
  - `async fn process_raw(state: &DistributorState, tenant: &str, raws: Vec<RawProfile>) -> Result<(), ProfilesError>` — the shared pipeline: relabel → require service_name → limits → session cap → `split_sample_types` → build `ProfileRecord`s → `sink.append`.
  - `async fn serve(addr, state, shutdown) -> std::io::Result<SocketAddr>`.

**Tenant** comes from the `X-Scope-OrgID` header (Pyroscope/Mimir convention).

- [ ] **Step 1: Write the failing handler tests (in-process, recording sink, no broker)**

Create the test module in `distributor/mod.rs`. Use a `Vec`-collecting fake `WalSink` and drive the Connect handler directly (or via `tower::ServiceExt::oneshot` on `router`).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<ProfileRecord>>);
    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
            self.0.lock().unwrap().push(rec);
            Ok(())
        }
    }

    fn state_with(sink: Arc<RecordingSink>) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink,
            limits: TenantLimits::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
        })
    }

    #[tokio::test]
    async fn push_splits_and_appends_one_record_per_sample_type() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        // a 2-sample-type pprof, gzipped into a push.v1 request (see Task 6 fixture)
        let raws = vec![crate::wire::test_fixtures::raw_profile_2types()];
        process_raw(&state, "tenant-a", raws).await.unwrap();
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 2); // one per sample type
        assert!(recs.iter().all(|r| r.tenant == "tenant-a"));
        assert!(recs.iter().all(|r| r.labels.iter().any(|(k, _)| k == "service_name")));
    }

    #[tokio::test]
    async fn relabel_drop_skips_the_series() {
        let sink = Arc::new(RecordingSink::default());
        let mut st = (*state_with(sink.clone())).clone_for_test();
        st.relabel = vec![RelabelConfig {
            source_labels: vec!["__name__".into()],
            regex: "process_cpu".into(),
            target_label: String::new(),
            replacement: String::new(),
            action: RelabelAction::Drop,
        }];
        let raws = vec![crate::wire::test_fixtures::raw_profile_cpu()];
        process_raw(&Arc::new(st), "t", raws).await.unwrap();
        assert!(sink.0.lock().unwrap().is_empty());
    }
}
```

> Provide a tiny `clone_for_test`/`DistributorState` constructor as needed; the point is the recording sink asserts the split fan-out (2 records) and the relabel drop (0 records) without a broker.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib distributor`
Expected: FAIL — `cannot find function process_raw`.

- [ ] **Step 3: Implement `distributor/mod.rs`**

Implement:
- `WalSink` trait (use `async_trait` — add `async-trait = { workspace = true }` to deps if not present; metrics/traces slices use the same seam) + `KafkaSink { producer: Arc<Producer> }` whose `append` builds a `ProducerRecord { topic: PROFILES_WAL_TOPIC.into(), key: Some(partition_key(&rec.tenant, rec.series_fingerprint())), value: Some(Bytes::from(rec.encode()?)), partition: None, ..Default::default() }` and `producer.send(record).await.await??` (verify the ack pattern against `crates/client-producer/src/producer.rs`).
- `process_raw`: for each `RawProfile`: `apply_relabel(&mut labels, &state.relabel)` → if `false` skip; `require_service_name`; `enforce_limits`; `cap_session_id(.., state.limits.session_id_buckets)`; `split_sample_types` → for each `DecodedProfile` build a `ProfileRecord { tenant, labels: profile.labels.iter()…collect(), profile_type, samples: …, symbols: extract_symbols(&raw.profile) }` and `state.sink.append(rec).await?`.
- `extract_symbols(&PprofProfile) -> WalSymbolSet`: map the pprof string/function/location/mapping tables 1:1 to `WalSymbolSet` (slice-2 getters). This is the symbol set the block-builder interns.
- `router()`: combine the Connect builders + `/ingest`:
  ```rust
  pub fn router(state: Arc<DistributorState>) -> axum::Router {
      let push = pb::push::v1::pusher_service_connect::PusherServiceServiceBuilder::<()>::new()
          .push(push_handler)
          .build();
      let otlp = pb::otlp_profiles::profiles_service_connect::ProfilesServiceServiceBuilder::<()>::new()
          .export(export_handler)
          .build();
      axum::Router::new()
          .route("/ingest", axum::routing::post(ingest_handler))
          .merge(push)
          .merge(otlp)
          .layer(axum::Extension(state))
  }
  ```
  (Verify the generated builder + method names against `OUT_DIR` and the grpc-gateway `gateway_connect::GatewayServiceBuilder::<()>::new().send(...)` pattern.)
- `push_handler(Extension(state), req: ConnectRequest<pb::push::v1::PushRequest>) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError>`: read tenant from `req` headers (the connect request exposes headers; verify), `decode_push` → `process_raw` → `Ok(ConnectResponse::new(PushResponse {}))` (empty).
- `export_handler`: `decode_otlp` → `process_raw` → empty `ExportProfilesServiceResponse`.
- `ingest_handler(Extension(state), headers, RawQuery(q), Multipart)`: `parse_ingest_query` → `decode_ingest_multipart` → `process_raw` → `200 OK`. Map `ProfilesError::status_code()` to the response.
- `serve()`: mirror `crates/broker/src/metrics_server.rs::run` — bind `TcpListener`, `axum::serve(listener, router(state)).with_graceful_shutdown(...)`, return the bound `SocketAddr`. TLS is `// TODO(slice4-tls)`.

> **Verify how the Connect handler reads request headers for the tenant** (`X-Scope-OrgID`). In grpc-gateway the principal/peer arrive as `Extension`s injected by a layer; for the tenant, add a small `axum` middleware that extracts `X-Scope-OrgID` into a request extension, or read it from the `ConnectRequest` if `connectrpc-axum` surfaces headers. Confirm the mechanism against `connectrpc-axum`'s `ConnectRequest` API before wiring; default tenant to `"anonymous"` only if multi-tenancy is disabled (note `// TODO(slice4-tenant-required)`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib distributor`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): distributor — push.v1/OTLP/ingest doors, split, WAL produce"
```

---

### Task 9: Block-builder — WAL consumer-group → samples fact table + dedup SymbolDb + ProfileIndex

**Files:**
- Create: `crates/profiles/src/blockbuilder.rs`
- Modify: `crates/profiles/src/lib.rs`

**Interfaces:**
- Produces:
  - `fn object_key(tenant: &str, partition: i32, min_offset: i64, max_offset: i64, min_ts: i64, max_ts: i64) -> String` — deterministic idempotent block key; the symbol-DB artifact key is the block key + the slice-1 symdb suffix.
  - `fn intern_record(symdb: &mut SymbolDb, rec: &ProfileRecord) -> Result<Vec<u32>, ProfilesError>` — merge `rec.symbols` into `symdb`, intern each sample's `stacktrace_location_refs` → a `stacktrace_id` per sample (returns one id per sample, in order). Records the `stacktrace_partition` chosen.
  - `fn samples_batch(rows: &[BuiltSample]) -> Result<arrow::record_batch::RecordBatch, ProfilesError>` — build the `PCOL_*` samples fact table (slice-1 schema) from `(fp, ts, profile_type, stacktrace_id, value, stacktrace_partition, total_value, span_id, trace_id)` rows.
  - `struct BuiltSample { /* the 9 fact-table columns */ }`.
  - `async fn build_block(blockstore, symdb_store, tenant, partition, records, offset_range) -> Result<Vec<BlockMeta>, ProfilesError>` — intern → batch → `write_block` + write symdb artifact + `ProfileIndex` updates.
  - `async fn run(consumer, blockstore, index_key, shutdown) -> Result<(), ProfilesError>` — poll → decode → build → **save index + write block + symdb**, THEN `commit_sync` (crash-safety order).

- [ ] **Step 1: Write the failing unit tests (no broker)**

Create `crates/profiles/src/blockbuilder.rs`. Test the pure pieces (`object_key` determinism, `intern_record` dedup, `samples_batch` schema match, `build_block` against an in-memory `object_store`).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{ProfileRecord, WalSample, WalSymbolSet};
    use assert2::assert;
    use crabka_pprof::SymbolDb;

    fn rec(name: &str, value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "t".into(),
            labels: vec![("__name__".into(), name.into()), ("service_name".into(), "api".into())],
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0, 1],
                value,
                timestamp_ns: 1_700_000_000_000_000_000,
                span_id: None,
                trace_id: None,
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "a".into(), "b".into()],
                functions: vec![],
                locations: vec![],
                mappings: vec![],
            },
        }
    }

    #[test]
    fn object_key_is_deterministic() {
        let a = object_key("t", 0, 10, 20, 100, 200);
        let b = object_key("t", 0, 10, 20, 100, 200);
        let c = object_key("t", 0, 10, 21, 100, 200);
        assert!(a == b);
        assert!(a != c);
    }

    #[test]
    fn intern_record_dedups_identical_stacks() {
        let mut symdb = SymbolDb::default();
        let r = rec("cpu", 5);
        let ids1 = intern_record(&mut symdb, &r).unwrap();
        let ids2 = intern_record(&mut symdb, &r).unwrap();
        // Identical stacks intern to the same stacktrace id (parent-pointer dedup).
        assert!(ids1 == ids2);
    }

    #[tokio::test]
    async fn build_block_writes_samples_and_symdb() {
        use std::sync::Arc;
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = crabka_blockstore::BlockStore::new(store.clone(), base);

        let records = vec![rec("cpu", 5), rec("cpu", 7)];
        let metas = build_block(&mut bs, &store, "t", 0, &records, (10, 20)).await.unwrap();
        assert!(!metas.is_empty());
        assert!(metas[0].tenant == "t");
        assert!(metas[0].row_count == 2);
        // The symbol-DB artifact landed next to the block.
        let symdb_key = format!("{}{}", metas[0].object_key, crabka_blockstore::SYMDB_SUFFIX);
        assert!(store.head(&object_store::path::Path::from(symdb_key)).await.is_ok());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib blockbuilder`
Expected: FAIL — `cannot find function object_key`.

- [ ] **Step 3: Implement `blockbuilder.rs`**

Implement:
- `object_key`: `format!("blocks/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet")` (deterministic ⇒ idempotent overwrite). The symdb artifact key = `format!("{key}{}", crabka_blockstore::SYMDB_SUFFIX)`.
- `intern_record`: register `rec.symbols` tables into the `SymbolDb` (offsetting indices into the symdb's growing tables), choose a `stacktrace_partition` (per-tenant or per-block — match the slice-1 convention; default a single partition `0` for a block, noted `// TODO(slice4-stacktrace-partition-policy)`), then `symdb.intern_stacktrace(partition, &offset_refs)` per sample → `Vec<u32>`.
- `samples_batch`: build Arrow arrays for the slice-1 `PCOL_*` columns (`COL_FINGERPRINT` UInt64, `COL_TIMESTAMP` Int64, `PCOL_PROFILE_TYPE` Dictionary<Utf8>, `PCOL_STACKTRACE_ID` UInt64, `PCOL_VALUE` Int64, `PCOL_STACKTRACE_PARTITION` UInt64, `PCOL_TOTAL_VALUE` Int64, `PCOL_SPAN_ID` UInt64 nullable, `PCOL_TRACE_ID` Binary nullable) via the slice-1 schema builder. `total_value` = the per-profile sum of values for that series+type (precomputed for SelectSeries).
- `build_block`: intern all records into one block `SymbolDb`, build the samples batch, `bs.writer().write_block(tenant, &object_key(...), schema, &[batch])`, `symdb_store.put(symdb_key, symdb.encode())`, then `bs.index_mut()` profile-index updates (`add_series` per distinct series, the `__profile_type__` index, `add_block`/stacktrace-partition-map per the slice-1 `ProfileIndex` API). Return the `BlockMeta`s.
- `run`: loop `consumer.poll(timeout)` → `ProfileRecord::decode` each value → accumulate by `(partition, offset range)` over a flush window → `build_block` → `bs.index().save(&store, index_key)` → **then** `consumer.commit_sync()`. Block + symdb + index write **precedes** the offset commit (spec §5.4 crash-safety: idempotent keys make a re-process safe). On shutdown token, final flush + commit.

```rust
//! Block-builder role: consume the WAL, intern each record's symbol set into a
//! per-block `SymbolDb`, build the `PCOL_*` samples fact table, write the block +
//! symbol-DB artifact + `ProfileIndex`, THEN commit offsets (crash-safety order).
```

> **Verify** `BlockStore::new(store, base)`, `bs.writer()`, `bs.index_mut()`, `bs.index().save(&store, key)`, the `ProfileIndex` update method names, the samples-schema builder, the `PCOL_*` constants, and `SYMDB_SUFFIX` against the slice-1 plan — align to it if a name differs. `SymbolDb::{default, intern_stacktrace, encode}` come from slice 2. The Dictionary<Utf8> builder for `PCOL_PROFILE_TYPE` is the fiddliest Arrow piece (`arrow::array::StringDictionaryBuilder<Int32Type>` or the slice-1 helper) — verify against arrow 59 and pin by the `build_block` round-trip (the block reads back with the right `row_count`). If slice 1 exposes a `samples_batch`-style helper, reuse it instead of hand-building.

- [ ] **Step 4: Declare + run**

`lib.rs`: `mod blockbuilder; pub use blockbuilder::{build_block, intern_record, object_key, run};`

Run: `cargo test -p crabka-profiles --lib blockbuilder`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): block-builder — WAL to samples fact table + dedup SymbolDb + ProfileIndex"
```

---

### Task 10: Role-selectable binary

**Files:**
- Create: `crates/profiles/src/bin/crabka-profiles.rs`
- Modify: `crates/profiles/Cargo.toml` (`[[bin]]` if needed; clap already a dep)

**Interfaces:**
- Produces: a binary with `--target distributor|block-builder` (other targets — `querier`, `query-frontend`, `compactor`, `symbolizer` — stubbed with a "not yet implemented in this slice" message). Distributor wires a real `Producer` + `serve`; block-builder wires a `Consumer` + `BlockStore` + `run`.

- [ ] **Step 1: Write the failing test (arg parsing)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use clap::Parser;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "distributor"]).unwrap();
        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn parses_block_builder_target() {
        let cli = Cli::try_parse_from(["crabka-profiles", "--target", "block-builder"]).unwrap();
        assert!(matches!(cli.target, Target::BlockBuilder));
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-profiles", "--target", "bogus"]).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --bin crabka-profiles`
Expected: FAIL — `cannot find type Cli`.

- [ ] **Step 3: Implement the binary**

`#[derive(Parser)] struct Cli { #[arg(long)] target: Target, #[arg(long, default_value = "127.0.0.1:4040")] listen: String, #[arg(long, default_value = "127.0.0.1:9092")] bootstrap: String }` + `#[derive(Clone, ValueEnum)] enum Target { Distributor, BlockBuilder, Querier, QueryFrontend, Compactor, Symbolizer }` (clap renames `BlockBuilder` → `block-builder`). `main`: parse, `tracing_subscriber` init, `CancellationToken` from `tokio::signal::ctrl_c`, match `target`:
- `Distributor` → `Producer::builder().bootstrap(&cli.bootstrap).build().await?`, wrap in `KafkaSink`, build `DistributorState`, `distributor::serve(cli.listen.parse()?, state, shutdown).await?`.
- `BlockBuilder` → `Consumer::builder().bootstrap(&cli.bootstrap).group_id("crabka-profiles-block-builder").subscribe([PROFILES_WAL_TOPIC.to_string()]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`, build a `BlockStore` over the configured object store (memory for now; real config `// TODO(slice4-objstore-config)`), `blockbuilder::run(consumer, blockstore, "index/profiles.json", shutdown).await?`.
- `Querier | QueryFrontend | Compactor | Symbolizer` → `eprintln!` + `std::process::exit(2)` with "target not implemented until slice {N}".

> Keep `main` thin; testable logic lives in the modules.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --bin crabka-profiles`
Expected: PASS (3 tests). Then `cargo build -p crabka-profiles --bin crabka-profiles` compiles.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): role-selectable crabka-profiles binary (distributor|block-builder)"
```

---

### Task 11: End-to-end broker round-trip (in-process broker)

**Files:**
- Create: `crates/profiles/tests/ingest_roundtrip.rs`
- Create: `crates/profiles/tests/support/mod.rs` (minimal in-process broker start, if needed)

**Interfaces:**
- Consumes the public API: `distributor::{router, process_raw, KafkaSink}`, `Producer`, `Consumer`, `blockbuilder::build_block`, `ProfileRecord`, `PROFILES_WAL_TOPIC`, blockstore.

This is the one test that needs a real broker. Use the in-process broker test-support (`BrokerConfig::for_tests` + `Broker::start`) — no Docker, runs in CI. Mark any Docker-only path `#[ignore]`.

- [ ] **Step 1: Decide the harness**

`crates/broker/tests/support/mod.rs` is path-included by the broker's own tests; `crabka-profiles` cannot `use` it directly. Replicate the few lines into `crates/profiles/tests/support/mod.rs`: `BrokerConfig::for_tests(tempdir)`, `Broker::start(config).await`, `handle.listen_addr()` — with `crabka-broker` as a `dev-dependency` (Task 1). **Verify** `BrokerConfig::for_tests` + `Broker::start` + `listen_addr` are `pub` in `crabka-broker` (they are — confirmed against `crates/broker/tests/support/mod.rs` usage). If they are test-only, fall back to `#[ignore = "requires Docker"]` + testcontainers cp-kafka.

- [ ] **Step 2: Write the round-trip test**

```rust
//! End-to-end: push a 2-sample-type pprof through the distributor to a real WAL
//! topic, run the block-builder consumer, and assert a block + symdb land with
//! the expected rows (one row per (sample, sample_type) split).

#[tokio::test]
async fn push_v1_lands_as_block() {
    // 1. start broker (in-process via support::start())
    // 2. admin: create PROFILES_WAL_TOPIC with N partitions
    // 3. build DistributorState with a real KafkaSink(Producer)
    // 4. process_raw a 2-sample-type RawProfile -> assert 2 records produced
    // 5. build a Consumer(group) on PROFILES_WAL_TOPIC, poll until records arrive,
    //    decode -> assert ProfileRecord round-trips (tenant/labels/profile_type)
    // 6. build_block over the polled records into an InMemory blockstore
    // 7. assert the BlockMeta row_count == 2 and the symdb artifact exists
}
```

Fill in the body using the verified producer/consumer/admin APIs. Key assertions: the split produces 2 records; the consumer reads back `ProfileRecord`s whose `series_fingerprint()` matches; `build_block` produces a `BlockMeta` with `row_count == 2` and the symdb artifact at the expected key.

- [ ] **Step 3: Run**

Run: `cargo test -p crabka-profiles --test ingest_roundtrip`
Expected: PASS (records round-trip; a block + symdb are written).

- [ ] **Step 4: Whole-crate gate**

Run: `cargo test -p crabka-profiles && cargo clippy -p crabka-profiles --all-targets && cargo fmt -p crabka-profiles --check`
Expected: all PASS (ignored Docker tests skipped), no warnings, formatting clean.

- [ ] **Step 5: Commit**

```bash
git add crates/profiles/
git commit -m "test(profiles): end-to-end push.v1 -> WAL -> block-builder -> block round-trip"
```

---

## Self-review

**Spec coverage (against §5 ingest + §3 architecture + §11 Slice 4):**
- `push.v1.PusherService/Push` (gzipped-pprof `raw_profile` → gunzip → pprof decode) → Tasks 2, 6, 8.
- Legacy `POST /ingest` (`?name=app{labels}&format=...`, multipart `profile` pprof + `sample_type_config`) → Tasks 7, 8.
- OTLP `ProfilesService/Export` `v1development` (interned `ProfilesDictionary` → pprof-equivalent) → Tasks 2, 7, 8.
- Distributor pipeline: `relabel_configs` → require `service_name`/`__name__` (inject `unknown_service`) → label limits → `__session_id__` modulo-hash cap → **multi-value split (one series per sample type → 5-part `__profile_type__`)** + per-sample-label split → shard by `(tenant, series_fingerprint)` → produce → Tasks 3, 4, 8.
- `ProfileRecord` (the slice-4 contract) + `PROFILES_WAL_TOPIC` + partition key → Task 5.
- Block-builder (consumer-group, intern dedup `SymbolDb`, samples fact table via `PCOL_*` schema, `ProfileIndex`, symbol-DB artifact, write-then-commit, deterministic idempotent key) → Task 9.
- Role-selectable `--target distributor|block-builder` binary (later targets stubbed) → Task 10.
- In-process broker round-trip test (testcontainers `#[ignore]` Docker fallback) → Task 11.

**Deviations flagged (deferred with explicit TODO markers, not silently dropped):**
- JFR + folded-text (`groups`/`tree`/`lines`/`speedscope`) `/ingest` formats — Task 7 returns `UnsupportedFormat` with `// TODO(slice4-ingest-jfr)`/`-folded`; the load-bearing `pprof` multipart path is done+tested.
- Full `relabel_configs` grammar (modulus/hashmod/labelmap) — Task 3 `// TODO(slice4-relabel-full)`; drop/keep/replace + structural caps are implemented/tested.
- OTLP resource-attribute `service.name` resolution — Task 7 `// TODO(slice4-otlp-resource)`; `require_service_name` enforces `unknown_service` downstream regardless.
- `sample_type_config` JSON application (units/aggregation overrides) — Task 7 `// TODO(slice4-sample-type-config)`; the pprof bytes are decoded fully.
- Stacktrace-partition policy (one partition per block vs sharded) — Task 9 `// TODO(slice4-stacktrace-partition-policy)`; default single-partition is correct and tested.
- TLS, per-tenant rate-limit (429 via Crabka quotas), tenant-required enforcement — `// TODO(slice4-tls)`/`-quota)`/`-tenant-required)`; structural caps (415/400) are enforced.

**Placeholder scan:** no "TBD"/"similar to Task N" without code. The churn-prone surfaces — the OTLP `v1development` proto field numbers (Task 2, commit-pinned + round-trip test), the `connectrpc-axum-build` codegen + generated builder/method names (Tasks 2/8, verified against grpc-gateway), the `crabka-pprof` `PprofProfile`/`SymbolDb` accessors (Tasks 4/6/7/9, consumer-side, verify-against-slice-2), the `serde-wincode` call shape (Task 5, verified at `bootstrap.rs:104`), the producer `.send` ack pattern (Task 8), the Arrow Dictionary<Utf8> builder for `PCOL_PROFILE_TYPE` (Task 9), the `multer` multipart API (Task 7), and the in-process broker harness reachability (Task 11) — are each bounded with an explicit "verify against X" note and pinned by a behavior test, never fabricated.

**Type consistency:** `RawProfile` (Task 3) is produced by all three doors (Tasks 6/7) and consumed by `process_raw` (Task 8) → `split_sample_types` (Task 4) → `DecodedProfile`. `ProfileRecord`/`WalSample`/`WalSymbolSet`/`partition_key` (Task 5) are consumed by the distributor produce path (Task 8) and the block-builder (Task 9). `ProfilesError::status_code()` is the single ingest status mapping (Task 1), used by the distributor (Task 8). The slice-1 blockstore API (`BlockStore::new`/`writer`/`index_mut`/`write_block`/`ProfileIndex` updates/`save`/`PCOL_*`/`SYMDB_SUFFIX`) and the slice-2 `crabka-pprof` API (`PprofProfile`/`SymbolDb`/`ProfileType`/`Frame`/`ProfileError`) are consumed exactly as the dependency roadmap pins them — verify-against notes flag every consumer-side assumption.

**Known risks (flagged, not hidden):**
- **OTLP `v1development` proto churn** — the single highest-churn surface; commit-pinned (tag comment) + behavior-pinned by the Task-2 round-trip test, so a drift is a failing test, not silent corruption. Contained to `proto/` + `wire/` + `ingest/otlp.rs`.
- **`crabka-pprof` consumer-side assumptions** — Tasks 4/6/7/9 reuse the slice-2 `PprofProfile::sample_types()`/`string()` + `SymbolDb::intern_stacktrace` (which exist) and assume additional per-sample/period getters (`samples`, `value_at`, `location_refs`, `timestamp_ns`, `span_id`, `trace_id`, `period_type_strings`) + `PprofProfile::from_otlp` that slice 2 does **not** define yet — add these to slice 2 (greenfield) as a companion change. Pinned by the split/intern behavior tests.
- **connect codegen + protoc in CI** — Task 2 `build.rs` needs `protoc`; mirrors grpc-gateway's `system_protoc_available()` + `fetch_protoc` fallback. Pinned by the two prost round-trip tests so a codegen break is a compile error.
- **The `(tenant, series_fingerprint)` partition invariant** — a single test pins `partition_key` determinism; the produce path sets `key` and leaves `partition: None` so the MurmurHash2 partitioner keeps a series together.
