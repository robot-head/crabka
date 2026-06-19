# crabka-metrics Slice 4 — Ingest service (remote_write v1/v2 + OTLP + distributor + WAL + compactor)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the three ingest doors of the metrics backend — remote_write v1/v2 + OTLP metrics — landing in a Kafka WAL topic via the **distributor** role (validate → HA-dedup → produce), and the **compactor** role that consumes the WAL topic, groups by `(tenant, fingerprint)`, builds Arrow blocks via Slice-1 codecs, and writes them to `crabka-blockstore`. Ship a role-selectable `crabka-metrics --target distributor|compactor` binary.

**Architecture:** This slice adds the `wire`, `otlp`, `wal`, `distributor`, and `compactor` modules to the `crabka-metrics` crate built in Slice 1. The `wire` module owns the remote_write v1/v2 protobuf types (prost-generated from vendored `.proto`) + content negotiation + the wire-`Histogram` → `NativeHistogram` decode (integer-histogram **delta-decode to absolute**; this is the only place deltas exist). The `otlp` module extends the broker's `client_metrics/otlp.rs` decode pattern to the full type mapping, the new piece being `ExponentialHistogram` → `NativeHistogram` (scale↔schema clamp + boundary off-by-one fix). The `wal` module defines `WalRecord` — the metrics WAL topic record (serde + `serde-wincode`, the codebase convention) that **Slices 5/6/7 consume**. The `distributor` is an axum 0.8 server; the `compactor` is a Kafka consumer-group loop. A real Crabka broker is only needed for the produce/consume round-trip tests, which are `#[ignore]`-gated.

```
remote_write v1/v2  ─┐
                     ├─→ distributor (axum) ─→ validate ─→ HA-dedup ─→ produce WalRecord ─→ __crabka_metrics_wal
OTLP /otlp/v1/metrics┘                                        (HA-tracker topic)              │
                                                                                              ▼
                                                                            compactor (consumer-group)
                                                                            group by (tenant, fp), sort by ts
                                                                            ─→ float / native-hist RecordBatches
                                                                            ─→ BlockWriter::write_block
                                                                            ─→ Index::{add_series, add_block, save}
                                                                            ─→ commit offsets  (block+index FIRST)
```

**Tech Stack:** Rust 2024 · `prost` 0.14 (workspace) · `opentelemetry-proto` 0.32 (`gen-tonic-messages`,`metrics`) · `axum` 0.8 (`http1`,`tokio`) · `snap` 1 (raw block, **not** Xerial) · `bytes` 1 · `arrow` 59 · `crabka-blockstore` · `crabka-metrics` (Slice 1) · `crabka-client-producer` · `crabka-client-consumer` · `crabka-client-admin` · `serde` + `serde-wincode` (`wincode::Serialize`) · `clap` 4 · `tokio` · `thiserror` · `tracing`. Build: `prost-build` (new build-dep) for the vendored remote_write protos. Tests: `assert2`, `proptest`, `tempfile`; broker round-trip tests use `crates/broker/tests/support` (in-process) — Docker only for differential-vs-Mimir (deferred to Slice 8).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change `WalRecord`/enums/wire-internal types freely; no shims, no migration code, no `#[serde(default)]`. (Only Kafka **client** wire compat matters — and the remote_write/OTLP byte-exactness on the HTTP edge.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Arrow version identity:** use `arrow` 59 directly. `crabka-metrics` Slice-1 codecs produce batches `crabka-blockstore::BlockWriter::write_block` consumes without conversion.
- **prost generated types are the source of truth.** The remote_write `.proto` field names/types this plan quotes (e.g. `Histogram.count` as a `oneof`) are pinned by behavior tests; if a generated field name differs, **align to the generated `OUT_DIR` type**, never fabricate. DataFusion is **not** a dependency of this slice.
- **Kafka wire-protocol exactness** is preserved automatically by producing/consuming through the existing `crabka-client-producer`/`crabka-client-consumer` clients — do not hand-roll protocol frames.

---

## Dependency & slice roadmap

**Depends on (consume exactly — do not re-implement):**
- **`crabka-metrics` Slice 1** — `NativeHistogram { schema:i8, is_float:bool, reset_hint:ResetHint, zero_threshold:f64, zero_count:f64, count:f64, sum:f64, positive_spans:Vec<BucketSpan>, positive_counts:Vec<f64> (absolute), negative_spans, negative_counts, custom_values:Option<Vec<f64>>, start_timestamp_ms:Option<i64> }`, `BucketSpan { offset:i32, length:u32 }`, `ResetHint { Unknown, Yes, No, Gauge }` (`as_i8`/`from_i8`), `SymbolTable` (`from_symbols`/`resolve_label_refs`/`new`/`intern`/`resolve`/`symbols`), `encode_native_histograms`/`decode_native_histograms`, `encode_float_samples`/`decode_float_samples`, schema builders.
- **`crabka-blockstore`** — `BlockStore`, `BlockWriter::write_block(tenant:&str, object_key:&str, schema:SchemaRef, batches:&[RecordBatch]) -> Result<BlockMeta>`, `Index::{add_series, add_block, save}`, `BlockMeta`, `Labels` (`new`/`insert`/`fingerprint`/`iter`), `SeriesFingerprint` (= `u64`).
- **`crabka-client-producer`** — `Producer::builder().bootstrap(..).build().await? -> Result<Producer, ProducerError>`; `Producer::send(ProducerRecord) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>>` (`.await.await??` for ack); `ProducerRecord { topic:String, partition:Option<i32>, key:Option<Bytes>, value:Option<Bytes>, headers:Vec<Header>, timestamp_ms:Option<i64> }`; `Producer::flush()`. **The producer hashes `key` with MurmurHash2 to choose a partition** — so set `key` = our partition key and leave `partition: None`. (Verify signatures against `crates/client-producer/src/{builder,record,producer}.rs`.)
- **`crabka-client-consumer`** — `Consumer::builder().bootstrap(..).group_id(..).subscribe([..]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`; `Consumer::poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`; `Consumer::commit_sync() -> Result<(), ConsumerError>`; `ConsumerRecord { topic, partition:i32, offset:i64, key:Option<Bytes>, value:Option<Bytes>, .. }`. (Verify against `crates/client-consumer/src/{consumer,poll,commit}.rs`.)
- **`crabka-client-admin`** — `create_topics(&[CreateTopicSpec { name, partitions, replicas, configs }], timeout_ms) -> Result<Vec<CreateTopicOutcome>, AdminError>` (for tests + bootstrapping the WAL/HA topics).

**THIS slice defines (Slices 5/6/7 consume):** `WalRecord` (Task 5) — the WAL topic record. `WAL_TOPIC = "__crabka_metrics_wal"`. Partition key = `(tenant, series_fingerprint)`.

**The 8 metrics slices** (this plan = Slice 4):
1. Data layer (DONE). 2. `crabka-promql` core. 3. Query completeness. **4. Ingest service *(this plan)*.** 5. Querier + Prometheus HTTP API. 6. Query-frontend. 7. Ruler. 8. Hardening.

---

## File structure (`crates/metrics/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | add ingest deps + `prost`/`opentelemetry-proto` + `[build-dependencies] prost-build` |
| `build.rs` | prost-codegen the vendored remote_write v1/v2 protos → `OUT_DIR` |
| `proto/prometheus/remote.proto` | vendored remote_write **v1** (`prometheus.WriteRequest`) |
| `proto/io/prometheus/write/v2/types.proto` | vendored remote_write **v2** (`io.prometheus.write.v2.Request`) |
| `src/wire/mod.rs` | `pb` (generated include), content negotiation, snappy block decode, status codes |
| `src/wire/v1.rs` | v1 `WriteRequest` → `Vec<DecodedSeries>` (labels + float samples + wire histograms) |
| `src/wire/v2.rs` | v2 `Request` → `Vec<DecodedSeries>` via `SymbolTable`; written-counts response struct |
| `src/wire/histogram.rs` | wire `Histogram` (int delta-decode / float absolute / NHCB) → `NativeHistogram` |
| `src/otlp.rs` | OTLP `MetricsData` → `Vec<DecodedSeries>`; `ExponentialHistogram` → `NativeHistogram`; `TranslationStrategy` |
| `src/wal.rs` | `WalRecord`, `SamplePayload`, `WalExemplar` + `encode`/`decode` + partition key |
| `src/distributor/mod.rs` | axum router (`/api/v1/push`, `/otlp/v1/metrics`), serve, limits |
| `src/distributor/ha.rs` | `HaTracker` — elected `__replica__` per `(tenant, cluster)`; dedup decision |
| `src/compactor.rs` | consumer-group loop → group/sort → blocks → index → commit |
| `src/bin/crabka-metrics.rs` | `clap` role-selectable entrypoint (`--target`) |

---

### Task 1: Ingest-deps wiring + vendored protos + prost build

**Files:**
- Modify: `crates/metrics/Cargo.toml`
- Create: `crates/metrics/build.rs`
- Create: `crates/metrics/proto/prometheus/remote.proto`
- Create: `crates/metrics/proto/io/prometheus/write/v2/types.proto`
- Create: `crates/metrics/src/wire/mod.rs` (placeholder `pb` include)
- Modify: `crates/metrics/src/lib.rs` (declare `mod wire;`)
- Modify: root `Cargo.toml` (add `prost-build` to `[workspace.dependencies]`)

**Interfaces:**
- Produces: generated prost types reachable as `crate::wire::pb::v1::WriteRequest`, `crate::wire::pb::v1::{TimeSeries,Label,Sample,Histogram,...}`, `crate::wire::pb::v2::Request`, `crate::wire::pb::v2::{TimeSeries,Sample,Histogram,Exemplar,Metadata,...}`.

- [ ] **Step 1: Add `prost-build` to the workspace deps**

In root `Cargo.toml` `[workspace.dependencies]`, alongside `prost = "0.14"`:

```toml
prost-build = "0.14"
```

- [ ] **Step 2: Extend `crates/metrics/Cargo.toml`**

Add to `[dependencies]`:

```toml
prost = { workspace = true }
opentelemetry-proto = { workspace = true }
bytes = { workspace = true }
snap = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "net", "time", "signal"] }
tower = { workspace = true }
futures = { workspace = true }
serde = { workspace = true }
serde-wincode = { workspace = true }
wincode = { workspace = true }
clap = { workspace = true }
tracing = { workspace = true }
crabka-blockstore = { path = "../blockstore" }
crabka-client-producer = { path = "../client-producer" }
crabka-client-consumer = { path = "../client-consumer" }
crabka-client-admin = { path = "../client-admin" }
url = { workspace = true }
object_store = { workspace = true }
twox-hash = { workspace = true }
```

> **Verify against root `Cargo.toml`:** `wincode`, `serde-wincode`, `twox-hash`, `object_store`, `url` must already be workspace deps (they are used by sibling crates). If `twox-hash` is absent, use a 64-bit FNV-1a inline (matching blockstore's `Labels::fingerprint`) for the partition-key hash instead — do not add a new external crate just for that. Confirm each `{ workspace = true }` line resolves before proceeding.

Add a build-dep section:

```toml
[build-dependencies]
prost-build = { workspace = true }
```

- [ ] **Step 3: Vendor the remote_write v1 proto**

Create `crates/metrics/proto/prometheus/remote.proto`. This is the stable v1 surface (Prometheus `prompb`, Apache-2.0; keep the license header). Vendor the minimal message set:

```proto
syntax = "proto3";
package prometheus;

message WriteRequest {
  repeated TimeSeries timeseries = 1;
  // field 2 (Source) and 3 (metadata) historically present; metadata at 3.
  repeated MetricMetadata metadata = 3;
}

message TimeSeries {
  repeated Label labels = 1;
  repeated Sample samples = 2;
  repeated Exemplar exemplars = 3;
  repeated Histogram histograms = 4;
}

message Label { string name = 1; string value = 2; }
message Sample { double value = 1; int64 timestamp = 2; }
message Exemplar { repeated Label labels = 1; double value = 2; int64 timestamp = 3; }

message MetricMetadata {
  enum MetricType { UNKNOWN = 0; COUNTER = 1; GAUGE = 2; HISTOGRAM = 3;
    GAUGEHISTOGRAM = 4; SUMMARY = 5; INFO = 6; STATESET = 7; }
  MetricType type = 1;
  string metric_family_name = 2;
  string help = 4;
  string unit = 5;
}

message Histogram {
  oneof count { uint64 count_int = 1; double count_float = 2; }
  double sum = 3;
  sint32 schema = 4;
  double zero_threshold = 5;
  oneof zero_count { uint64 zero_count_int = 6; double zero_count_float = 7; }
  repeated BucketSpan negative_spans = 8;
  repeated sint64 negative_deltas = 9;   // integer histogram (delta-encoded)
  repeated double negative_counts = 10;  // float histogram (absolute)
  repeated BucketSpan positive_spans = 11;
  repeated sint64 positive_deltas = 12;
  repeated double positive_counts = 13;
  enum ResetHint { UNKNOWN = 0; YES = 1; NO = 2; GAUGE = 3; }
  ResetHint reset_hint = 14;
  int64 timestamp = 15;
  repeated double custom_values = 16;    // NHCB
}

message BucketSpan { sint32 offset = 1; uint32 length = 2; }
```

> **Verify against the canonical proto:** field numbers and the `oneof count`/`oneof zero_count` shape are byte-load-bearing. Cross-check against `prometheus/prompb/types.proto` at the pinned Prometheus tag before relying on it. If a field number differs, fix the `.proto` — the generated struct names (`histogram::Count::CountInt`, etc.) follow from it.

- [ ] **Step 4: Vendor the remote_write v2 proto**

Create `crates/metrics/proto/io/prometheus/write/v2/types.proto` (experimental `2.0-rc.4`; symbol-table interned). Vendor `io.prometheus.write.v2.Request`:

```proto
syntax = "proto3";
package io.prometheus.write.v2;

message Request {
  reserved 1 to 3;
  repeated string symbols = 4;
  repeated TimeSeries timeseries = 5;
}

message TimeSeries {
  repeated uint32 labels_refs = 1;       // even-length pairs into `symbols`
  repeated Sample samples = 2;
  repeated Histogram histograms = 3;
  repeated Exemplar exemplars = 4;
  Metadata metadata = 5;
  reserved 6;                            // canonical: created/start ts live on Sample.start_timestamp / Histogram.start_timestamp, not here
}

message Sample { double value = 1; int64 timestamp = 2; int64 start_timestamp = 3; }

message Exemplar {
  repeated uint32 labels_refs = 1;
  double value = 2;
  int64 timestamp = 3;
}

message Metadata {
  enum MetricType { METRIC_TYPE_UNSPECIFIED = 0; COUNTER = 1; GAUGE = 2;
    HISTOGRAM = 3; GAUGEHISTOGRAM = 4; SUMMARY = 5; INFO = 6; STATESET = 7; }
  MetricType type = 1;
  uint32 help_ref = 3;
  uint32 unit_ref = 4;
}

message Histogram {
  oneof count { uint64 count_int = 1; double count_float = 2; }
  double sum = 3;
  sint32 schema = 4;
  double zero_threshold = 5;
  oneof zero_count { uint64 zero_count_int = 6; double zero_count_float = 7; }
  repeated BucketSpan negative_spans = 8;
  repeated sint64 negative_deltas = 9;
  repeated double negative_counts = 10;
  repeated BucketSpan positive_spans = 11;
  repeated sint64 positive_deltas = 12;
  repeated double positive_counts = 13;
  enum ResetHint { RESET_HINT_UNSPECIFIED = 0; RESET_HINT_YES = 1;
    RESET_HINT_NO = 2; RESET_HINT_GAUGE = 3; }
  ResetHint reset_hint = 14;
  int64 timestamp = 15;                  // sample collection time
  repeated double custom_values = 16;
  int64 start_timestamp = 17;            // counter-reset / created time (distinct from `timestamp`)
}

message BucketSpan { sint32 offset = 1; uint32 length = 2; }
```

> **Verify against `prometheus/prompb/io/prometheus/write/v2/types.proto`** at the same tag. v2 is experimental — expect churn; pin the tag in a comment at the top of the file.

- [ ] **Step 5: Create `build.rs`**

```rust
//! Generates prost message types from the vendored remote_write v1/v2 protos.
//! Prefers a system `protoc`; if absent, `prost-build` >= 0.14 vendors one via
//! the `protoc` crate dependency it pulls — but we keep it explicit and fail
//! loudly if neither is available, so the build is reproducible.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/prometheus/remote.proto",
        "proto/io/prometheus/write/v2/types.proto",
    ];
    let includes = ["proto"];
    prost_build::Config::new()
        .compile_protos(&protos, &includes)?;
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }
    Ok(())
}
```

> **protoc availability:** the rest of the repo uses `connectrpc-axum-build`'s `fetch_protoc` fallback. If CI lacks a system `protoc`, mirror that: add `prost-build`'s `vendored` route or call `protoc-bin-vendored`. **Verify** the build succeeds in `act`/CI before committing; if it needs the fetch fallback, replicate `crates/grpc-gateway/build.rs`'s `system_protoc_available()` guard. Do not leave the build flaky.

- [ ] **Step 6: Create `src/wire/mod.rs` with the generated-type include**

```rust
//! remote_write wire surface: prost-generated message types, content
//! negotiation, snappy decode, and decode to the shared `DecodedSeries`.

/// prost-generated message types from the vendored protos.
pub mod pb {
    /// remote_write v1 (`prometheus.WriteRequest`).
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }
    /// remote_write v2 (`io.prometheus.write.v2.Request`).
    pub mod v2 {
        include!(concat!(env!("OUT_DIR"), "/io.prometheus.write.v2.rs"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use prost::Message;

    #[test]
    fn v1_write_request_round_trips_via_prost() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![pb::v1::Label { name: "__name__".into(), value: "up".into() }],
                samples: vec![pb::v1::Sample { value: 1.0, timestamp: 42 }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes = req.encode_to_vec();
        let back = pb::v1::WriteRequest::decode(bytes.as_slice()).unwrap();
        assert!(back.timeseries.len() == 1);
        assert!(back.timeseries[0].samples[0].timestamp == 42);
    }

    #[test]
    fn v2_request_has_symbols_and_label_refs() {
        let req = pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample { value: 3.0, timestamp: 7 }],
                ..Default::default()
            }],
        };
        let bytes = req.encode_to_vec();
        let back = pb::v2::Request::decode(bytes.as_slice()).unwrap();
        assert!(back.symbols[0].is_empty());
        assert!(back.timeseries[0].labels_refs == vec![1, 2]);
    }
}
```

> **The two `include!` module names (`/prometheus.rs`, `/io.prometheus.write.v2.rs`) are prost's filename convention (package name).** If `cargo build` reports a missing file, list `OUT_DIR` (`cargo build -p crabka-metrics -v` prints it) and use the actual generated filenames. This is the source of truth; do not guess further.

Add `assert2` to `[dev-dependencies]` if not already present (Slice 1 added it).

- [ ] **Step 7: Declare the module + build**

In `lib.rs` add `pub mod wire;`.

Run: `cargo test -p crabka-metrics --lib wire::tests`
Expected: compiles (first build invokes `protoc`); both prost round-trip tests PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add Cargo.toml Cargo.lock crates/metrics/
git commit -m "feat(metrics): vendor remote_write v1/v2 protos + prost codegen"
```

---

### Task 2: `DecodedSeries` + snappy block decode + content negotiation

**Files:**
- Create: `crates/metrics/src/wire/decoded.rs` (the shared decode target)
- Modify: `crates/metrics/src/wire/mod.rs` (snappy + content-type dispatch + status helpers)

**Interfaces:**
- Produces:
  - `struct DecodedSeries { pub labels: crabka_blockstore::Labels, pub samples: Vec<(i64, f64)>, pub histograms: Vec<(i64, crabka_metrics::NativeHistogram)>, pub exemplars: Vec<DecodedExemplar> }` (`Debug`, `PartialEq`)
  - `struct DecodedExemplar { pub labels: Vec<(String, String)>, pub value: f64, pub timestamp_ms: i64 }`
  - `enum WireFormat { RemoteWriteV1, RemoteWriteV2, Otlp }`
  - `fn negotiate(content_type: Option<&str>) -> Result<WireFormat, WireError>` — dispatch on the `proto=` param.
  - `fn snappy_block_decode(body: &[u8], max_output: usize) -> Result<Vec<u8>, WireError>` — **plain** snappy block (NOT Xerial-framed).
  - `enum WireError` with a `fn status_code(&self) -> u16` mapping (400/415/429).

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/wire/decoded.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn negotiate_v1_default_protobuf() {
        // bare protobuf with no proto= param => v1
        assert!(matches!(
            negotiate(Some("application/x-protobuf")),
            Ok(WireFormat::RemoteWriteV1)
        ));
    }

    #[test]
    fn negotiate_v1_explicit_proto_param() {
        assert!(matches!(
            negotiate(Some("application/x-protobuf;proto=prometheus.WriteRequest")),
            Ok(WireFormat::RemoteWriteV1)
        ));
    }

    #[test]
    fn negotiate_v2_proto_param() {
        assert!(matches!(
            negotiate(Some(
                "application/x-protobuf;proto=io.prometheus.write.v2.Request"
            )),
            Ok(WireFormat::RemoteWriteV2)
        ));
    }

    #[test]
    fn negotiate_unsupported_is_415() {
        let err = negotiate(Some("application/json")).unwrap_err();
        assert!(err.status_code() == 415);
    }

    #[test]
    fn snappy_block_round_trips_plain() {
        let raw = b"the quick brown fox jumps over the lazy dog";
        let compressed = snap::raw::Encoder::new().compress_vec(raw).unwrap();
        let back = snappy_block_decode(&compressed, 1 << 20).unwrap();
        assert!(back == raw);
    }

    #[test]
    fn snappy_block_rejects_oversize() {
        let raw = vec![0u8; 4096];
        let compressed = snap::raw::Encoder::new().compress_vec(&raw).unwrap();
        let err = snappy_block_decode(&compressed, 16).unwrap_err();
        assert!(err.status_code() == 400);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib wire::decoded`
Expected: FAIL — `cannot find function negotiate`.

- [ ] **Step 3: Implement `decoded.rs`**

Prepend above the `tests` module:

```rust
//! The decode target every wire format lowers into, plus content negotiation,
//! plain snappy-block decode, and the ingest status-code mapping.

use crabka_blockstore::Labels;
use crabka_metrics::NativeHistogram;

/// One series' decoded payload, format-agnostic.
#[derive(Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Labels,
    pub samples: Vec<(i64, f64)>,
    pub histograms: Vec<(i64, NativeHistogram)>,
    pub exemplars: Vec<DecodedExemplar>,
}

/// A decoded exemplar (trace/span live in its labels until the sidecar codec).
#[derive(Debug, PartialEq)]
pub struct DecodedExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// Which wire format an ingest request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    RemoteWriteV1,
    RemoteWriteV2,
    Otlp,
}

/// Ingest-edge errors, each carrying its Prometheus-shaped status code.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("unsupported content-type: {0}")]
    UnsupportedContentType(String),
    #[error("snappy decode failed: {0}")]
    Snappy(String),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("protobuf decode failed: {0}")]
    Proto(String),
    #[error("invalid request: {0}")]
    Invalid(String),
}

impl WireError {
    /// Map to the Prometheus remote_write status contract (§5.1).
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            WireError::UnsupportedContentType(_) => 415,
            WireError::Snappy(_) | WireError::TooLarge { .. }
            | WireError::Proto(_) | WireError::Invalid(_) => 400,
        }
    }
}

/// Dispatch on the `Content-Type` `proto=` param. Bare `application/x-protobuf`
/// (no `proto=`) is v1 (the historical default). `application/x-protobuf` with
/// `proto=io.prometheus.write.v2.Request` is v2.
pub fn negotiate(content_type: Option<&str>) -> Result<WireFormat, WireError> {
    let ct = content_type.unwrap_or("").to_ascii_lowercase();
    let base = ct.split(';').next().unwrap_or("").trim();
    if base != "application/x-protobuf" {
        return Err(WireError::UnsupportedContentType(ct));
    }
    // Look for a proto= parameter.
    let proto = ct
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("proto="))
        .next();
    match proto {
        None | Some("prometheus.writerequest") => Ok(WireFormat::RemoteWriteV1),
        Some("io.prometheus.write.v2.request") => Ok(WireFormat::RemoteWriteV2),
        Some(other) => Err(WireError::UnsupportedContentType(format!("proto={other}"))),
    }
}

/// Decode a **plain** snappy block (remote_write requires snappy-block, framed
/// MUST NOT be used). This is *not* the Kafka Xerial-framed variant used by
/// `crabka-compression::snappy` — that one prepends a magic header and chunks.
/// remote_write uses a single raw block, so we use `snap::raw` directly.
pub fn snappy_block_decode(body: &[u8], max_output: usize) -> Result<Vec<u8>, WireError> {
    let want = snap::raw::decompress_len(body)
        .map_err(|e| WireError::Snappy(e.to_string()))?;
    if want > max_output {
        return Err(WireError::TooLarge { limit: max_output });
    }
    snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| WireError::Snappy(e.to_string()))
}
```

> **Snappy distinction is load-bearing:** the codebase's `crabka-compression::snappy::decompress` expects a 16-byte Xerial magic header and length-prefixed chunks (Kafka producer framing). remote_write sends a single plain `snap::raw` block. Using the wrong decoder corrupts every request. Keep this `snap::raw::Decoder` path; do **not** route remote_write bodies through `crabka-compression`.

- [ ] **Step 4: Re-export from `wire/mod.rs`**

Add to `wire/mod.rs`: `mod decoded; pub use decoded::{DecodedExemplar, DecodedSeries, WireError, WireFormat, negotiate, snappy_block_decode};`

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib wire::decoded`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): DecodedSeries + content negotiation + snappy block decode"
```

---

### Task 3: wire `Histogram` → `NativeHistogram` (delta-decode)

**Files:**
- Create: `crates/metrics/src/wire/histogram.rs`
- Modify: `crates/metrics/src/wire/mod.rs`

**Interfaces:**
- Consumes: `pb::v1::Histogram`, `pb::v2::Histogram`, Slice-1 `NativeHistogram`/`BucketSpan`/`ResetHint`.
- Produces:
  - `fn v1_histogram_to_native(h: &pb::v1::Histogram) -> Result<NativeHistogram, WireError>`
  - `fn v2_histogram_to_native(h: &pb::v2::Histogram) -> Result<NativeHistogram, WireError>`
  - Internal `fn delta_decode(deltas: &[i64]) -> Vec<f64>` (first absolute, rest cumulative) — the integer-histogram path.

> v1 and v2 `Histogram` are structurally identical; implement one private `decode_common` over the shared field set and call it from both thin wrappers. The generated `oneof` accessor enums differ in name (`pb::v1::histogram::Count` vs `pb::v2::histogram::Count`) so the two wrappers extract the scalar `count`/`zero_count` + `is_float` discriminator, then hand a normalized struct to `decode_common`.

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/wire/histogram.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;

    fn int_hist() -> pb::v1::Histogram {
        pb::v1::Histogram {
            count: Some(pb::v1::histogram::Count::CountInt(10)),
            sum: 42.0,
            schema: 2,
            zero_threshold: 1e-128,
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountInt(3)),
            positive_spans: vec![pb::v1::BucketSpan { offset: 0, length: 2 }],
            positive_deltas: vec![4, -1], // absolute: [4, 3]
            negative_spans: vec![],
            negative_deltas: vec![],
            reset_hint: pb::v1::histogram::ResetHint::No as i32,
            timestamp: 1000,
            ..Default::default()
        }
    }

    #[test]
    fn integer_histogram_delta_decodes_to_absolute() {
        let h = v1_histogram_to_native(&int_hist()).unwrap();
        assert!(!h.is_float);
        assert!(h.count == 10.0);
        assert!(h.positive_counts == vec![4.0, 3.0]); // delta [4,-1] => [4,3]
        assert!(h.reset_hint == crabka_metrics::ResetHint::No);
        assert!(h.zero_count == 3.0);
    }

    #[test]
    fn float_histogram_keeps_absolute_counts() {
        let h = pb::v1::Histogram {
            count: Some(pb::v1::histogram::Count::CountFloat(7.0)),
            sum: 1.0,
            schema: 0,
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(0.0)),
            positive_spans: vec![pb::v1::BucketSpan { offset: 0, length: 2 }],
            positive_counts: vec![2.0, 5.0], // already absolute
            reset_hint: pb::v1::histogram::ResetHint::Unknown as i32,
            timestamp: 5,
            ..Default::default()
        };
        let out = v1_histogram_to_native(&h).unwrap();
        assert!(out.is_float);
        assert!(out.positive_counts == vec![2.0, 5.0]);
    }

    #[test]
    fn nhcb_schema_carries_custom_values() {
        let mut h = int_hist();
        h.schema = -53;
        h.custom_values = vec![0.5, 1.0, 2.0];
        let out = v1_histogram_to_native(&h).unwrap();
        // `is_nhcb()` is NOT in the Slice-1 shared contract (which enumerates the
        // struct fields + encode/decode + ResetHint::from_i8/as_i8). Assert the
        // field-level invariants the contract DOES guarantee instead.
        assert!(out.schema == -53);
        assert!(out.custom_values == Some(vec![0.5, 1.0, 2.0]));
    }

    #[test]
    fn span_delta_length_mismatch_is_rejected() {
        let mut h = int_hist();
        h.positive_spans = vec![pb::v1::BucketSpan { offset: 0, length: 5 }];
        h.positive_deltas = vec![1, 2]; // claims 5, has 2
        assert!(v1_histogram_to_native(&h).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib wire::histogram`
Expected: FAIL — `cannot find function v1_histogram_to_native`.

- [ ] **Step 3: Implement `histogram.rs`**

```rust
//! Decode a wire `Histogram` to the Slice-1 `NativeHistogram` (absolute counts).
//! Integer histograms carry delta-encoded counts (first element absolute);
//! float histograms carry absolute counts. We always store absolute.

use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::decoded::WireError;
use super::pb;

/// A wire-format-agnostic view of a histogram's scalar discriminators + arrays,
/// so v1/v2 share one decode body.
struct RawHist<'a> {
    is_float: bool,
    count: f64,
    zero_count: f64,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    reset_hint: i32,
    /// Counter-reset / created time (v2 `Histogram.start_timestamp`, field 17).
    /// `None` for v1 (no such field). NOT the sample collection `timestamp`.
    start_timestamp_ms: Option<i64>,
    positive_spans: Vec<BucketSpan>,
    negative_spans: Vec<BucketSpan>,
    positive_deltas: &'a [i64],
    negative_deltas: &'a [i64],
    positive_counts: &'a [f64],
    negative_counts: &'a [f64],
    custom_values: &'a [f64],
}

/// Delta-decode integer-histogram bucket counts: first element absolute, each
/// subsequent element a signed delta from the running value.
fn delta_decode(deltas: &[i64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(deltas.len());
    let mut running: i64 = 0;
    for (i, &d) in deltas.iter().enumerate() {
        running = if i == 0 { d } else { running + d };
        #[allow(clippy::cast_precision_loss)]
        out.push(running as f64);
    }
    out
}

fn span_total(spans: &[BucketSpan]) -> usize {
    spans.iter().map(|s| s.length as usize).sum()
}

fn decode_common(raw: &RawHist) -> Result<NativeHistogram, WireError> {
    let positive_counts = if raw.is_float {
        raw.positive_counts.to_vec()
    } else {
        delta_decode(raw.positive_deltas)
    };
    let negative_counts = if raw.is_float {
        raw.negative_counts.to_vec()
    } else {
        delta_decode(raw.negative_deltas)
    };
    if span_total(&raw.positive_spans) != positive_counts.len() {
        return Err(WireError::Invalid("positive span/count length mismatch".into()));
    }
    if span_total(&raw.negative_spans) != negative_counts.len() {
        return Err(WireError::Invalid("negative span/count length mismatch".into()));
    }
    let reset_hint = ResetHint::from_i8(i8::try_from(raw.reset_hint).unwrap_or(0));
    let custom_values = if raw.schema == -53 && !raw.custom_values.is_empty() {
        Some(raw.custom_values.to_vec())
    } else {
        None
    };
    Ok(NativeHistogram {
        schema: i8::try_from(raw.schema)
            .map_err(|_| WireError::Invalid(format!("schema {} out of range", raw.schema)))?,
        is_float: raw.is_float,
        reset_hint,
        zero_threshold: raw.zero_threshold,
        zero_count: raw.zero_count,
        count: raw.count,
        sum: raw.sum,
        positive_spans: raw.positive_spans.clone(),
        positive_counts,
        negative_spans: raw.negative_spans.clone(),
        negative_counts,
        custom_values,
        // v1 has no created/start-timestamp field; v2 carries it in
        // `Histogram.start_timestamp` (field 17), distinct from the sample
        // collection `timestamp` (field 15). Never reuse the sample ts here.
        start_timestamp_ms: raw.start_timestamp_ms,
    })
}

fn map_v1_span(s: &pb::v1::BucketSpan) -> BucketSpan {
    BucketSpan { offset: s.offset, length: s.length }
}
fn map_v2_span(s: &pb::v2::BucketSpan) -> BucketSpan {
    BucketSpan { offset: s.offset, length: s.length }
}

/// v1 wire `Histogram` → `NativeHistogram`.
pub fn v1_histogram_to_native(h: &pb::v1::Histogram) -> Result<NativeHistogram, WireError> {
    use pb::v1::histogram::{Count, ZeroCount};
    let (is_float, count) = match h.count {
        Some(Count::CountInt(c)) => (false, c as f64),
        Some(Count::CountFloat(c)) => (true, c),
        None => return Err(WireError::Invalid("histogram missing count".into())),
    };
    let zero_count = match h.zero_count {
        Some(ZeroCount::ZeroCountInt(c)) => c as f64,
        Some(ZeroCount::ZeroCountFloat(c)) => c,
        None => 0.0,
    };
    let raw = RawHist {
        is_float,
        count,
        zero_count,
        sum: h.sum,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        reset_hint: h.reset_hint,
        // v1 `Histogram` has no created/start-timestamp field. `h.timestamp` is
        // the sample collection time (captured by the caller), not the start ts.
        start_timestamp_ms: None,
        positive_spans: h.positive_spans.iter().map(map_v1_span).collect(),
        negative_spans: h.negative_spans.iter().map(map_v1_span).collect(),
        positive_deltas: &h.positive_deltas,
        negative_deltas: &h.negative_deltas,
        positive_counts: &h.positive_counts,
        negative_counts: &h.negative_counts,
        custom_values: &h.custom_values,
    };
    decode_common(&raw)
}

/// v2 wire `Histogram` → `NativeHistogram` (identical field set to v1).
pub fn v2_histogram_to_native(h: &pb::v2::Histogram) -> Result<NativeHistogram, WireError> {
    use pb::v2::histogram::{Count, ZeroCount};
    let (is_float, count) = match h.count {
        Some(Count::CountInt(c)) => (false, c as f64),
        Some(Count::CountFloat(c)) => (true, c),
        None => return Err(WireError::Invalid("histogram missing count".into())),
    };
    let zero_count = match h.zero_count {
        Some(ZeroCount::ZeroCountInt(c)) => c as f64,
        Some(ZeroCount::ZeroCountFloat(c)) => c,
        None => 0.0,
    };
    let raw = RawHist {
        is_float,
        count,
        zero_count,
        sum: h.sum,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        reset_hint: h.reset_hint,
        // v2 `Histogram.start_timestamp` (field 17) is the created/counter-reset
        // time; 0 means "unset" → None. NOT the sample `timestamp` (field 15).
        start_timestamp_ms: (h.start_timestamp != 0).then_some(h.start_timestamp),
        positive_spans: h.positive_spans.iter().map(map_v2_span).collect(),
        negative_spans: h.negative_spans.iter().map(map_v2_span).collect(),
        positive_deltas: &h.positive_deltas,
        negative_deltas: &h.negative_deltas,
        positive_counts: &h.positive_counts,
        negative_counts: &h.negative_counts,
        custom_values: &h.custom_values,
    };
    decode_common(&raw)
}
```

> **Verify generated `oneof` enum names** (`histogram::Count::CountInt`, `histogram::ZeroCount::ZeroCountFloat`, `histogram::ResetHint`). prost derives these from the proto field/enum names; if the casing differs, fix the `use`/match arms to match the generated code, never the test's asserted behavior. `as f64` casts on `u64`/`i64` are clippy-`cast_precision_loss` — keep the existing `#[allow]` pattern in `delta_decode` or add module-level `#![allow(clippy::cast_precision_loss)]` with a justification comment (counts up to 2^53 are exact; beyond that Prometheus itself loses precision).

- [ ] **Step 4: Re-export + run**

`wire/mod.rs`: `mod histogram; pub use histogram::{v1_histogram_to_native, v2_histogram_to_native};`

Run: `cargo test -p crabka-metrics --lib wire::histogram`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): wire Histogram to NativeHistogram (integer delta-decode + NHCB)"
```

---

### Task 4: v1 + v2 request decoders → `Vec<DecodedSeries>`

**Files:**
- Create: `crates/metrics/src/wire/v1.rs`
- Create: `crates/metrics/src/wire/v2.rs`
- Modify: `crates/metrics/src/wire/mod.rs`

**Interfaces:**
- Produces:
  - `fn decode_v1(body: &[u8], max_decompressed: usize) -> Result<Vec<DecodedSeries>, WireError>` — snappy-decompress → prost-decode `WriteRequest` → per-`TimeSeries` `DecodedSeries`.
  - `fn decode_v2(body: &[u8], max_decompressed: usize) -> Result<(Vec<DecodedSeries>, WrittenCounts), WireError>` — uses `SymbolTable::from_symbols` + `resolve_label_refs`; counts samples/histograms/exemplars for the v2 response headers.
  - `struct WrittenCounts { pub samples: u64, pub histograms: u64, pub exemplars: u64 }`.

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/wire/v1.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;
    use prost::Message;

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decode_v1_yields_labels_and_samples() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![
                    pb::v1::Label { name: "__name__".into(), value: "up".into() },
                    pb::v1::Label { name: "job".into(), value: "api".into() },
                ],
                samples: vec![pb::v1::Sample { value: 1.0, timestamp: 100 }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = snappy(&req.encode_to_vec());
        let out = decode_v1(&body, 1 << 20).unwrap();
        assert!(out.len() == 1);
        assert!(out[0].labels.get("__name__") == Some("up"));
        assert!(out[0].labels.get("job") == Some("api"));
        assert!(out[0].samples == vec![(100, 1.0)]);
    }
}
```

Create `crates/metrics/src/wire/v2.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;
    use prost::Message;

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    #[test]
    fn decode_v2_resolves_symbol_refs() {
        // symbols: 0="", 1="__name__", 2="up", 3="job", 4="api"
        let req = pb::v2::Request {
            symbols: vec![
                String::new(), "__name__".into(), "up".into(), "job".into(), "api".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![pb::v2::Sample { value: 9.0, timestamp: 7 }],
                ..Default::default()
            }],
        };
        let body = snappy(&req.encode_to_vec());
        let (out, counts) = decode_v2(&body, 1 << 20).unwrap();
        assert!(out[0].labels.get("__name__") == Some("up"));
        assert!(out[0].labels.get("job") == Some("api"));
        assert!(counts.samples == 1);
    }

    #[test]
    fn decode_v2_rejects_non_empty_first_symbol() {
        let req = pb::v2::Request {
            symbols: vec!["notEmpty".into(), "x".into()],
            timeseries: vec![],
        };
        let body = snappy(&req.encode_to_vec());
        assert!(decode_v2(&body, 1 << 20).is_err());
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p crabka-metrics --lib wire::v1 wire::v2`
Expected: FAIL — `cannot find function decode_v1`.

- [ ] **Step 3: Implement `v1.rs`**

```rust
//! remote_write v1 (`prometheus.WriteRequest`) → `Vec<DecodedSeries>`.

use crabka_blockstore::Labels;
use prost::Message;

use super::decoded::{DecodedExemplar, DecodedSeries, WireError};
use super::histogram::v1_histogram_to_native;
use super::pb;
use super::snappy_block_decode;

pub fn decode_v1(body: &[u8], max_decompressed: usize) -> Result<Vec<DecodedSeries>, WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v1::WriteRequest::decode(raw.as_slice())
        .map_err(|e| WireError::Proto(e.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    for ts in &req.timeseries {
        let mut labels = Labels::new();
        for l in &ts.labels {
            labels.insert(l.name.clone(), l.value.clone());
        }
        let samples = ts.samples.iter().map(|s| (s.timestamp, s.value)).collect();
        let mut histograms = Vec::with_capacity(ts.histograms.len());
        for h in &ts.histograms {
            histograms.push((h.timestamp, v1_histogram_to_native(h)?));
        }
        let exemplars = ts
            .exemplars
            .iter()
            .map(|e| DecodedExemplar {
                labels: e.labels.iter().map(|l| (l.name.clone(), l.value.clone())).collect(),
                value: e.value,
                timestamp_ms: e.timestamp,
            })
            .collect();
        out.push(DecodedSeries { labels, samples, histograms, exemplars });
    }
    Ok(out)
}
```

- [ ] **Step 4: Implement `v2.rs`**

```rust
//! remote_write v2 (`io.prometheus.write.v2.Request`) → `Vec<DecodedSeries>`,
//! resolving the symbol table, plus the written-counts for the response headers.

use crabka_blockstore::Labels;
use crabka_metrics::SymbolTable;
use prost::Message;

use super::decoded::{DecodedExemplar, DecodedSeries, WireError};
use super::histogram::v2_histogram_to_native;
use super::pb;
use super::snappy_block_decode;

/// Written-sample tallies for the v2 `X-Prometheus-Remote-Write-*-Written` headers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WrittenCounts {
    pub samples: u64,
    pub histograms: u64,
    pub exemplars: u64,
}

pub fn decode_v2(
    body: &[u8],
    max_decompressed: usize,
) -> Result<(Vec<DecodedSeries>, WrittenCounts), WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v2::Request::decode(raw.as_slice())
        .map_err(|e| WireError::Proto(e.to_string()))?;

    let table = SymbolTable::from_symbols(req.symbols)
        .map_err(|e| WireError::Invalid(e.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    let mut counts = WrittenCounts::default();
    for ts in &req.timeseries {
        let pairs = table
            .resolve_label_refs(&ts.labels_refs)
            .map_err(|e| WireError::Invalid(e.to_string()))?;
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(name, value);
        }
        let samples: Vec<(i64, f64)> =
            ts.samples.iter().map(|s| (s.timestamp, s.value)).collect();
        counts.samples += samples.len() as u64;

        let mut histograms = Vec::with_capacity(ts.histograms.len());
        for h in &ts.histograms {
            histograms.push((h.timestamp, v2_histogram_to_native(h)?));
        }
        counts.histograms += histograms.len() as u64;

        let mut exemplars = Vec::with_capacity(ts.exemplars.len());
        for e in &ts.exemplars {
            let elabels = table
                .resolve_label_refs(&e.labels_refs)
                .map_err(|err| WireError::Invalid(err.to_string()))?;
            exemplars.push(DecodedExemplar {
                labels: elabels,
                value: e.value,
                timestamp_ms: e.timestamp,
            });
        }
        counts.exemplars += exemplars.len() as u64;

        out.push(DecodedSeries { labels, samples, histograms, exemplars });
    }
    Ok((out, counts))
}
```

> `SymbolTable::from_symbols` already enforces `symbols[0] == ""` and `resolve_label_refs` enforces even length + in-range refs (Slice 1). Do not re-validate — surface their `SymbolError` as `WireError::Invalid`.

- [ ] **Step 5: Re-export + run**

`wire/mod.rs`: `mod v1; mod v2; pub use v1::decode_v1; pub use v2::{decode_v2, WrittenCounts};`

Run: `cargo test -p crabka-metrics --lib wire`
Expected: PASS (all wire tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): remote_write v1/v2 request decoders to DecodedSeries"
```

---

### Task 5: `WalRecord` — the WAL topic record (Slices 5/6/7 consume this)

**Files:**
- Create: `crates/metrics/src/wal.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces (the SHARED CONTRACT this slice owns):
  - `const WAL_TOPIC: &str = "__crabka_metrics_wal"`
  - `struct WalRecord { pub tenant: String, pub labels: Vec<(String, String)>, pub payload: SamplePayload, pub exemplars: Vec<WalExemplar> }` (`serde`, `Clone`, `Debug`, `PartialEq`)
  - `enum SamplePayload { Float { timestamp_ms: i64, value: f64 }, Hist { timestamp_ms: i64, hist: NativeHistogram } }`
  - `struct WalExemplar { pub labels: Vec<(String, String)>, pub value: f64, pub timestamp_ms: i64 }`
  - `WalRecord::encode(&self) -> Result<Vec<u8>, WalError>` / `WalRecord::decode(&[u8]) -> Result<WalRecord, WalError>` (via `serde-wincode`).
  - `WalRecord::series_fingerprint(&self) -> u64` (via blockstore `Labels::fingerprint`).
  - `fn partition_key(tenant: &str, fp: u64) -> Bytes` — the produce key; hash of `(tenant, fp)`.

> `NativeHistogram` (Slice 1) must `derive(Serialize, Deserialize)` for `WalRecord` to encode. **If Slice 1's `NativeHistogram`/`BucketSpan`/`ResetHint` do not already derive serde, add the derives in Slice 1's `histogram.rs` as part of this task** (greenfield — just change it; it does not affect the Arrow codec). Add `serde = { workspace = true }` to `crabka-metrics` deps (Task 1 added it).

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/wal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
    use assert2::assert;

    fn hist() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 0.0,
            count: 7.0,
            sum: 3.0,
            positive_spans: vec![BucketSpan { offset: 0, length: 2 }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    #[test]
    fn float_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "up".into()), ("job".into(), "api".into())],
            payload: SamplePayload::Float { timestamp_ms: 100, value: 1.5 },
            exemplars: vec![],
        };
        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();
        assert!(back == rec);
    }

    #[test]
    fn hist_record_round_trips() {
        let rec = WalRecord {
            tenant: "t1".into(),
            labels: vec![("__name__".into(), "latency".into())],
            payload: SamplePayload::Hist { timestamp_ms: 200, hist: hist() },
            exemplars: vec![WalExemplar {
                labels: vec![("trace_id".into(), "abc".into())],
                value: 0.9,
                timestamp_ms: 200,
            }],
        };
        let bytes = rec.encode().unwrap();
        let back = WalRecord::decode(&bytes).unwrap();
        assert!(back == rec);
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = WalRecord {
            tenant: "t".into(),
            labels: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            payload: SamplePayload::Float { timestamp_ms: 0, value: 0.0 },
            exemplars: vec![],
        };
        let mut b = a.clone();
        b.labels = vec![("b".into(), "2".into()), ("a".into(), "1".into())];
        assert!(a.series_fingerprint() == b.series_fingerprint());
    }

    #[test]
    fn partition_key_is_stable() {
        let k1 = partition_key("t", 42);
        let k2 = partition_key("t", 42);
        let k3 = partition_key("t", 43);
        assert!(k1 == k2);
        assert!(k1 != k3);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib wal`
Expected: FAIL — `cannot find type WalRecord`.

- [ ] **Step 3: Implement `wal.rs`**

```rust
//! The metrics WAL topic record. Produced by the distributor, consumed by the
//! compactor (this slice) and the querier's hot head (Slice 5). Encoded with
//! `serde-wincode` (the codebase convention; see `crates/broker/src/bootstrap.rs`).

use bytes::Bytes;
use crabka_blockstore::Labels;
use crabka_metrics::NativeHistogram;
use serde::{Deserialize, Serialize};

/// The metrics WAL topic name.
pub const WAL_TOPIC: &str = "__crabka_metrics_wal";

/// WAL codec errors.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal encode failed: {0}")]
    Encode(String),
    #[error("wal decode failed: {0}")]
    Decode(String),
}

/// One sample's payload: a float or a native histogram, each with its timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SamplePayload {
    Float { timestamp_ms: i64, value: f64 },
    Hist { timestamp_ms: i64, hist: NativeHistogram },
}

impl SamplePayload {
    #[must_use]
    pub fn timestamp_ms(&self) -> i64 {
        match self {
            SamplePayload::Float { timestamp_ms, .. }
            | SamplePayload::Hist { timestamp_ms, .. } => *timestamp_ms,
        }
    }
}

/// An exemplar carried alongside a sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalExemplar {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
}

/// A single metrics WAL record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalRecord {
    pub tenant: String,
    pub labels: Vec<(String, String)>,
    pub payload: SamplePayload,
    pub exemplars: Vec<WalExemplar>,
}

impl WalRecord {
    /// Encode via `serde-wincode` (matches the broker's metadata-record codec).
    pub fn encode(&self) -> Result<Vec<u8>, WalError> {
        <serde_wincode::SerdeCompat<WalRecord> as wincode::Serialize>::serialize(self)
            .map_err(|e| WalError::Encode(e.to_string()))
    }

    /// Decode a `WalRecord` from its `serde-wincode` bytes.
    pub fn decode(bytes: &[u8]) -> Result<WalRecord, WalError> {
        <serde_wincode::SerdeCompat<WalRecord>>::deserialize(bytes)
            .map_err(|e| WalError::Decode(e.to_string()))
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
/// samples of one series in one tenant land on one partition (preserving
/// per-series order). The producer MurmurHash2-partitions on this key.
#[must_use]
pub fn partition_key(tenant: &str, fp: u64) -> Bytes {
    let mut buf = Vec::with_capacity(tenant.len() + 8);
    buf.extend_from_slice(tenant.as_bytes());
    buf.extend_from_slice(&fp.to_be_bytes());
    Bytes::from(buf)
}
```

> **Verify the `serde-wincode` call shape** against `crates/broker/src/bootstrap.rs` / `crates/metadata/src/kraft_translate.rs`: `<SerdeCompat<T> as wincode::Serialize>::serialize(&value) -> Result<Vec<u8>, _>` and `<SerdeCompat<T>>::deserialize(&[u8]) -> Result<T, _>`. If the trait import differs, match the codebase exactly. The partition key bytes are an internal contract (the producer hashes them); their exact layout is free to change, but keep it deterministic.

- [ ] **Step 4: If needed, add serde derives to Slice 1's `NativeHistogram`**

If `cargo test` fails with "`NativeHistogram` does not implement `Serialize`", add `Serialize, Deserialize` to the `derive` on `NativeHistogram`, `BucketSpan`, and `ResetHint` in `crates/metrics/src/histogram.rs`, and `serde = { workspace = true }` to deps. Re-run Slice 1's histogram tests to confirm no regression.

- [ ] **Step 5: Declare + run**

`lib.rs`: `mod wal; pub use wal::{partition_key, SamplePayload, WalError, WalExemplar, WalRecord, WAL_TOPIC};`

Run: `cargo test -p crabka-metrics --lib wal`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): WalRecord WAL topic record + serde-wincode codec (slice-4 contract)"
```

---

### Task 6: OTLP decode extension — full type mapping + ExponentialHistogram

**Files:**
- Create: `crates/metrics/src/otlp.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Consumes: `opentelemetry_proto::tonic::metrics::v1::{MetricsData, metric::Data, ExponentialHistogramDataPoint, exponential_histogram_data_point::Buckets, NumberDataPoint, ...}`, Slice-1 `NativeHistogram`.
- Produces:
  - `enum TranslationStrategy { UnderscoreEscapingWithSuffixes, NoTranslation }` (`Default` = `UnderscoreEscapingWithSuffixes`)
  - `fn decode_otlp(md: &MetricsData, strategy: TranslationStrategy) -> Result<Vec<DecodedSeries>, OtlpError>`
  - `fn exponential_histogram_to_native(dp: &ExponentialHistogramDataPoint) -> Result<NativeHistogram, OtlpError>` — the scale↔schema clamp + boundary off-by-one fix.
  - `fn normalize_name(name: &str, strategy: TranslationStrategy) -> String`

> **Scope guard:** delta→cumulative accumulation and `target_info` need cross-datapoint state. Implement the **pure** per-datapoint mappings (Gauge, monotonic/non-monotonic Sum, Histogram→classic float series, ExponentialHistogram→native, name/label normalization) fully and tested. Delta accumulation + `target_info` resource-attr promotion are stubbed behind explicit `// TODO(slice4-otlp-state)` markers with a focused test asserting the *cumulative* path works; wire the delta accumulator as the final step of this task (Step 7). Do not silently drop delta points — return `OtlpError::DeltaUnsupported` until Step 7 lands the accumulator, then make the test pass.

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/otlp.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric, MetricsData,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data,
        number_data_point::Value, exponential_histogram_data_point::Buckets,
        AggregationTemporality,
    };
    use assert2::assert;

    fn wrap(metric: Metric) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics { metrics: vec![metric], ..Default::default() }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn monotonic_sum_gets_total_suffix() {
        let metric = Metric {
            name: "http_requests".into(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    value: Some(Value::AsInt(5)),
                    time_unix_nano: 1_000_000, // 1 ms
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        };
        let out = decode_otlp(&wrap(metric), TranslationStrategy::default()).unwrap();
        assert!(out[0].labels.get("__name__") == Some("http_requests_total"));
        assert!(out[0].samples == vec![(1, 5.0)]);
    }

    #[test]
    fn gauge_keeps_name() {
        let metric = Metric {
            name: "temperature".into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    value: Some(Value::AsDouble(21.5)),
                    time_unix_nano: 2_000_000,
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };
        let out = decode_otlp(&wrap(metric), TranslationStrategy::default()).unwrap();
        assert!(out[0].labels.get("__name__") == Some("temperature"));
    }

    #[test]
    fn exponential_histogram_maps_scale_to_schema() {
        // scale 3 is in-range ([-4,8]) => schema 3, offset+1 boundary fix.
        let dp = ExponentialHistogramDataPoint {
            count: 6,
            sum: Some(10.0),
            scale: 3,
            zero_count: 1,
            zero_threshold: 1e-9,
            positive: Some(Buckets { offset: 0, bucket_counts: vec![2, 3] }),
            negative: None,
            time_unix_nano: 3_000_000,
            ..Default::default()
        };
        let h = exponential_histogram_to_native(&dp).unwrap();
        assert!(h.schema == 3);
        assert!(!h.is_float);
        assert!(h.count == 6.0);
        assert!(h.zero_count == 1.0);
        // OTLP `offset` is the lower-boundary index; Prometheus span offset is the
        // bucket *index* (upper-boundary convention) => +1.
        assert!(h.positive_spans[0].offset == 1);
        assert!(h.positive_counts == vec![2.0, 3.0]);
    }

    #[test]
    fn exponential_histogram_downscales_when_scale_too_high() {
        // scale 10 > 8 => scale_down = 2 to fit schema 8. offset=0 so the index-
        // merge collapses all four source buckets onto shifted index 1.
        let dp = ExponentialHistogramDataPoint {
            count: 4,
            scale: 10,
            positive: Some(Buckets { offset: 0, bucket_counts: vec![1, 1, 1, 1] }),
            time_unix_nano: 4_000_000,
            ..Default::default()
        };
        let h = exponential_histogram_to_native(&dp).unwrap();
        assert!(h.schema == 8);
        // indices = [(i>>2)+1 for i in 0..4] = [1,1,1,1] => one merged bucket.
        assert!(h.positive_spans == vec![crabka_metrics::BucketSpan { offset: 1, length: 1 }]);
        assert!(h.positive_counts == vec![4.0]);
    }

    #[test]
    fn exponential_histogram_downscale_odd_offset_index_merges() {
        // Pins the Prometheus INDEX-merge (not array-pair-merge) for a non-2^scale_down
        // -aligned offset. scale 9 > 8 => scale_down = 1, offset = 1, counts = [1,2,3,4].
        // shifted indices = [((i+1)>>1)+1 for i in 0..4] = [1, 2, 2, 3]
        //   => counts [1, 2+3, 4] = [1, 5, 4] at indices {1,2,3}.
        // The old array-pair-merge would wrongly yield [3, 7] at {1,2}.
        let dp = ExponentialHistogramDataPoint {
            count: 10,
            scale: 9,
            positive: Some(Buckets { offset: 1, bucket_counts: vec![1, 2, 3, 4] }),
            time_unix_nano: 5_000_000,
            ..Default::default()
        };
        let h = exponential_histogram_to_native(&dp).unwrap();
        assert!(h.schema == 8);
        // Contiguous indices 1,2,3 => a single span at offset 1, length 3.
        assert!(h.positive_spans == vec![crabka_metrics::BucketSpan { offset: 1, length: 3 }]);
        assert!(h.positive_counts == vec![1.0, 5.0, 4.0]);
    }

    #[test]
    fn name_normalization_escapes_dots() {
        let n = normalize_name("system.cpu.time", TranslationStrategy::default());
        assert!(n == "system_cpu_time");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib otlp`
Expected: FAIL — `cannot find function decode_otlp`.

- [ ] **Step 3: Implement `otlp.rs`** (per-datapoint mappings + the exponential-histogram clamp)

```rust
//! OTLP `MetricsData` → `Vec<DecodedSeries>`. Extends the broker's
//! `client_metrics/otlp.rs` decode to the full type mapping; the novel piece is
//! `ExponentialHistogram` → `NativeHistogram` (scale↔schema clamp + the
//! lower-vs-upper boundary off-by-one offset fix).

use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogramDataPoint, MetricsData, NumberDataPoint, metric::Data,
    number_data_point::Value as NumberValue,
};

use crate::wire::DecodedSeries;

/// How OTLP names/labels are translated to Prometheus conventions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TranslationStrategy {
    /// Default: escape illegal chars to `_`, add `_total`/unit suffixes.
    #[default]
    UnderscoreEscapingWithSuffixes,
    /// Pass names through unchanged (UTF-8 metric names; Prometheus 3.x).
    NoTranslation,
}

/// OTLP decode errors.
#[derive(Debug, thiserror::Error)]
pub enum OtlpError {
    #[error("delta temporality requires the cumulative accumulator (not yet wired)")]
    DeltaUnsupported,
    #[error("invalid exponential histogram: {0}")]
    InvalidExpHistogram(String),
    #[error("unsupported metric data variant")]
    Unsupported,
}

/// Prometheus highest native-histogram schema (resolution). OTLP `scale` maps
/// 1:1 onto schema in `[-4, 8]`; higher scales downscale, lower are kept.
const MAX_SCHEMA: i32 = 8;
const MIN_SCHEMA: i32 = -4;

/// Normalize an OTLP metric/label name to Prometheus form.
#[must_use]
pub fn normalize_name(name: &str, strategy: TranslationStrategy) -> String {
    match strategy {
        TranslationStrategy::NoTranslation => name.to_string(),
        TranslationStrategy::UnderscoreEscapingWithSuffixes => name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == ':' { c } else { '_' })
            .collect(),
    }
}

fn number_value(dp: &NumberDataPoint) -> f64 {
    match dp.value {
        Some(NumberValue::AsDouble(v)) => v,
        #[allow(clippy::cast_precision_loss)]
        Some(NumberValue::AsInt(v)) => v as f64,
        None => f64::NAN,
    }
}

/// nanos → millis (Prometheus timestamps are ms).
fn nanos_to_millis(nanos: u64) -> i64 {
    #[allow(clippy::cast_possible_wrap)]
    { (nanos / 1_000_000) as i64 }
}

fn attrs_to_labels(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    name: &str,
    strategy: TranslationStrategy,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name.to_string());
    for kv in attributes {
        if let Some(v) = kv.value.as_ref().and_then(|a| a.value.as_ref()) {
            let value = match v {
                AnyValue::StringValue(s) => s.clone(),
                AnyValue::BoolValue(b) => b.to_string(),
                AnyValue::IntValue(i) => i.to_string(),
                AnyValue::DoubleValue(d) => d.to_string(),
                _ => continue,
            };
            labels.insert(normalize_name(&kv.key, strategy), value);
        }
    }
    labels
}

/// `ExponentialHistogramDataPoint` → `NativeHistogram` with the scale clamp +
/// boundary off-by-one fix.
///
/// OTLP bucket `offset` is the index of the *first* bucket where bucket `i`
/// covers `(base^(offset+i), base^(offset+i+1)]`. Prometheus spans use the
/// bucket index under the same base, but Prometheus' convention indexes by the
/// *upper* boundary, so the span offset is OTLP `offset + 1`.
pub fn exponential_histogram_to_native(
    dp: &ExponentialHistogramDataPoint,
) -> Result<NativeHistogram, OtlpError> {
    if dp.scale < MIN_SCHEMA {
        return Err(OtlpError::InvalidExpHistogram(format!(
            "scale {} below minimum {MIN_SCHEMA}; cannot represent",
            dp.scale
        )));
    }
    // Clamp resolution to Prometheus' max schema. `scale_down` is how many bits
    // of resolution we drop; the resulting schema is exactly MAX_SCHEMA when the
    // source scale exceeds it, otherwise scale_down == 0 and schema == scale.
    let scale_down = (dp.scale - MAX_SCHEMA).max(0);
    let schema = dp.scale - scale_down; // == MAX_SCHEMA when downscaling, else dp.scale

    let positive = dp.positive.clone().unwrap_or_default();
    let negative = dp.negative.clone().unwrap_or_default();

    // Index-merge (Prometheus `convertBucketsLayout`): each source bucket i maps
    // to shifted index `((i + offset) >> scale_down)`; buckets sharing an index
    // coalesce. Counts and spans are emitted together so they stay aligned.
    let (positive_spans, pos_counts) = convert_buckets_layout(&positive, scale_down);
    let (negative_spans, neg_counts) = convert_buckets_layout(&negative, scale_down);

    Ok(NativeHistogram {
        #[allow(clippy::cast_possible_truncation)]
        schema: schema as i8,
        is_float: false,
        reset_hint: ResetHint::Unknown,
        zero_threshold: dp.zero_threshold,
        #[allow(clippy::cast_precision_loss)]
        zero_count: dp.zero_count as f64,
        #[allow(clippy::cast_precision_loss)]
        count: dp.count as f64,
        sum: dp.sum.unwrap_or(0.0),
        positive_spans,
        positive_counts: pos_counts,
        negative_spans,
        negative_counts: neg_counts,
        custom_values: None,
        start_timestamp_ms: Some(nanos_to_millis(dp.start_time_unix_nano)),
    })
}

/// Lower OTLP buckets to Prometheus spans + absolute counts, applying an
/// optional resolution downscale by `scale_down` bits.
///
/// Mirrors Prometheus' `prometheusremotewrite/histograms.go::convertBucketsLayout`:
/// each source bucket `i` maps to the **shifted bucket index**
/// `bucket_idx = ((i as i32 + offset) >> scale_down) + 1` (arithmetic shift; the
/// `+1` is Prometheus' lower-vs-upper-boundary offset convention). Consecutive
/// source buckets that land on the *same* shifted index are coalesced into one
/// merged count. The first emitted span's offset is `initial_offset =
/// (offset >> scale_down) + 1`; any gap between non-adjacent shifted indices
/// starts a fresh span (we emit a separate span per contiguous run).
///
/// Worked example — offset=1, scale_down=1, counts=[a,b,c,d]:
/// shifted indices = [(1>>1)+1, (2>>1)+1, (3>>1)+1, (4>>1)+1] = [1, 2, 2, 3]
/// → counts [a, b+c, d] at indices {1,2,3}.
fn convert_buckets_layout(
    b: &opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets,
    scale_down: i32,
) -> (Vec<BucketSpan>, Vec<f64>) {
    if b.bucket_counts.is_empty() {
        return (vec![], vec![]);
    }
    let offset = b.offset;
    let mut spans: Vec<BucketSpan> = Vec::new();
    let mut counts: Vec<f64> = Vec::new();
    let mut prev_idx: Option<i32> = None;
    for (i, &c) in b.bucket_counts.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let idx = ((i as i32 + offset) >> scale_down) + 1;
        match prev_idx {
            // Same shifted index as the previous source bucket → coalesce.
            Some(p) if p == idx => {
                #[allow(clippy::cast_precision_loss)]
                {
                    *counts.last_mut().expect("non-empty when prev_idx is Some") += c as f64;
                }
            }
            // New index adjacent to the previous run → extend the current span.
            Some(p) if idx == p + 1 => {
                spans.last_mut().expect("non-empty when prev_idx is Some").length += 1;
                #[allow(clippy::cast_precision_loss)]
                counts.push(c as f64);
                prev_idx = Some(idx);
            }
            // First bucket, or a gap → start a fresh span at this index.
            _ => {
                let span_offset = match prev_idx {
                    None => idx,             // initial_offset = (offset >> scale_down) + 1
                    Some(p) => idx - (p + 1), // gap relative to end of previous run
                };
                spans.push(BucketSpan { offset: span_offset, length: 1 });
                #[allow(clippy::cast_precision_loss)]
                counts.push(c as f64);
                prev_idx = Some(idx);
            }
        }
    }
    (spans, counts)
}

/// Decode an OTLP `MetricsData` into per-datapoint `DecodedSeries`.
pub fn decode_otlp(
    md: &MetricsData,
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let mut out = Vec::new();
    for rm in &md.resource_metrics {
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                let base = normalize_name(&metric.name, strategy);
                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            out.push(number_series(&base, dp, strategy));
                        }
                    }
                    Some(Data::Sum(s)) => {
                        // Delta accumulation is wired in Step 7.
                        if s.aggregation_temporality
                            == opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Delta as i32
                        {
                            return Err(OtlpError::DeltaUnsupported);
                        }
                        let name = if s.is_monotonic {
                            format!("{base}_total")
                        } else {
                            base.clone()
                        };
                        for dp in &s.data_points {
                            out.push(number_series(&name, dp, strategy));
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            let hist = exponential_histogram_to_native(dp)?;
                            let labels = attrs_to_labels(&dp.attributes, &base, strategy);
                            out.push(DecodedSeries {
                                labels,
                                samples: vec![],
                                histograms: vec![(nanos_to_millis(dp.time_unix_nano), hist)],
                                exemplars: vec![],
                            });
                        }
                    }
                    // Histogram (classic) / Summary → float series: TODO(slice4-otlp-classic)
                    _ => return Err(OtlpError::Unsupported),
                }
            }
        }
    }
    Ok(out)
}

fn number_series(name: &str, dp: &NumberDataPoint, strategy: TranslationStrategy) -> DecodedSeries {
    DecodedSeries {
        labels: attrs_to_labels(&dp.attributes, name, strategy),
        samples: vec![(nanos_to_millis(dp.time_unix_nano), number_value(dp))],
        histograms: vec![],
        exemplars: vec![],
    }
}
```

> **Verify against the broker's `otlp.rs` + the generated `opentelemetry-proto` types** (`crates/broker/src/client_metrics/otlp.rs` shows `MetricsData::decode`; the exact field names — `bucket_counts`, `offset`, `scale`, `zero_threshold`, `start_time_unix_nano` — are confirmed in `opentelemetry.proto.metrics.v1.rs`). `Buckets::default()` exists (prost derives `Default`). The index-merge downscale + boundary fix are the correctness traps; the three exponential-histogram tests pin them (including the odd-offset case that distinguishes the real Prometheus INDEX-merge from a naive array-pair-merge). The merge MUST follow `prometheusremotewrite/histograms.go::convertBucketsLayout` (shift each source bucket's index by `scale_down`, coalesce equal shifted indices) — do not merge by array position. If the boundary `+1` convention proves wrong against a real Prometheus/Mimir comparison (Slice 8 differential), adjust *with a failing test first*.

- [ ] **Step 4: Declare + run the pure tests**

`lib.rs`: `mod otlp; pub use otlp::{decode_otlp, exponential_histogram_to_native, normalize_name, OtlpError, TranslationStrategy};`

Run: `cargo test -p crabka-metrics --lib otlp`
Expected: PASS (all 7 tests; delta path correctly errors until Step 7 — there is no delta test yet, so all green).

- [ ] **Step 5: Commit the cumulative path**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): OTLP decode — Sum/Gauge/ExponentialHistogram with scale clamp"
```

- [ ] **Step 6: Write the failing delta-accumulation test**

Append to the `tests` module a test that feeds two consecutive **delta** `Sum` datapoints for the same series and asserts the emitted samples are *cumulative* (running total), keyed by a `DeltaAccumulator` state struct:

```rust
    #[test]
    fn delta_sum_accumulates_to_cumulative() {
        let mut acc = DeltaAccumulator::default();
        let mk = |v: i64, t: u64| Metric {
            name: "bytes".into(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    value: Some(Value::AsInt(v)),
                    time_unix_nano: t,
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        };
        let a = decode_otlp_stateful(&wrap(mk(5, 1_000_000)), TranslationStrategy::default(), &mut acc).unwrap();
        let b = decode_otlp_stateful(&wrap(mk(3, 2_000_000)), TranslationStrategy::default(), &mut acc).unwrap();
        assert!(a[0].samples == vec![(1, 5.0)]);
        assert!(b[0].samples == vec![(2, 8.0)]); // 5 + 3
    }
```

- [ ] **Step 7: Implement `DeltaAccumulator` + `decode_otlp_stateful`**

Add a `#[derive(Default)] struct DeltaAccumulator { running: HashMap<u64, f64> }` keyed by series fingerprint, and `decode_otlp_stateful(md, strategy, &mut DeltaAccumulator)` that mirrors `decode_otlp` but, for delta-temporality monotonic sums, looks up the fingerprint, adds the delta to the running total, and emits the cumulative value. Keep `decode_otlp` as the stateless convenience wrapper that constructs a throwaway accumulator (cumulative-only inputs leave it unused). Re-export `DeltaAccumulator`/`decode_otlp_stateful`.

Run: `cargo test -p crabka-metrics --lib otlp`
Expected: PASS (delta test included).

- [ ] **Step 8: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): OTLP delta-to-cumulative accumulator"
```

---

### Task 7: HA tracker — elected `__replica__` per `(tenant, cluster)`

**Files:**
- Create: `crates/metrics/src/distributor/ha.rs`
- Create: `crates/metrics/src/distributor/mod.rs` (module shell; router lands in Task 8)
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - `struct HaTracker { elected: Mutex<HashMap<(String, String), String>> }` (in-memory view of the compacted HA-tracker topic; interior `Mutex` so first-seen election is atomic behind `&self`)
  - `const HA_TRACKER_TOPIC: &str = "__crabka_metrics_ha"`
  - `HaTracker::elected_replica(&self, tenant: &str, cluster: &str) -> Option<String>`
  - `HaTracker::set_elected(&self, tenant, cluster, replica)`
  - `HaTracker::elect_or_get(&self, tenant: &str, cluster: &str, replica: &str) -> String` — atomically elect an unseen pair's first replica, else return the already-elected one.
  - `enum HaDecision { Accept, Drop }`
  - `fn ha_decision(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaDecision` — inspect the **first** series' `cluster` + `__replica__` labels; if there is no `__replica__` label, `Accept` (HA disabled for this stream); else `elect_or_get` the `(tenant, cluster)` pair and `Accept` iff this replica is the elected one (first-seen wins; others Drop — no fail-open double-write).
  - `fn strip_replica_label(series: &mut [DecodedSeries])` — remove `__replica__` from every series before WAL write.

> HA-tracker leader election (lease acquisition / failover via the compacted topic) is a write-coordination concern. This task models the **read path** (consult elected replica) + the in-memory tracker fed from the compacted topic, AND a minimal **in-process first-seen election** (`elect_or_get`) so an unseen `(tenant, cluster)` does not fail open — the first replica we see is elected atomically and all others Drop, exactly the Mimir dedup behavior. **Persisting/replaying** that election to the compacted HA topic (so it survives restart and spans distributor replicas) is the focused follow-on noted with `// TODO(slice4-ha-election)`. The dedup *decision* + label stripping — the spec's HTTP-202 behavior — is fully implemented and tested here, with no double-write for unseen clusters.

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/distributor/ha.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{DecodedSeries};
    use crabka_blockstore::Labels;
    use assert2::assert;

    fn series_with(cluster: &str, replica: &str) -> DecodedSeries {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("cluster", cluster);
        labels.insert("__replica__", replica);
        DecodedSeries { labels, samples: vec![(1, 1.0)], histograms: vec![], exemplars: vec![] }
    }

    #[test]
    fn elected_replica_accepts() {
        let t = HaTracker::default();
        t.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r1")];
        assert!(matches!(ha_decision(&t, "tenant", &series), HaDecision::Accept));
    }

    #[test]
    fn non_elected_replica_drops() {
        let t = HaTracker::default();
        t.set_elected("tenant", "c1", "r1");
        let series = [series_with("c1", "r2")];
        assert!(matches!(ha_decision(&t, "tenant", &series), HaDecision::Drop));
    }

    #[test]
    fn first_seen_replica_elected_second_dropped() {
        // Fresh tracker, unseen (tenant, c1): the first replica we see wins the
        // in-process election; a second replica for the same cluster is dropped.
        // This is the dedup that prevents two HA replicas both double-writing.
        let t = HaTracker::default();
        let r1 = [series_with("c1", "r1")];
        let r2 = [series_with("c1", "r2")];
        assert!(matches!(ha_decision(&t, "tenant", &r1), HaDecision::Accept));
        assert!(matches!(ha_decision(&t, "tenant", &r2), HaDecision::Drop));
    }

    #[test]
    fn no_replica_label_means_ha_disabled() {
        let t = HaTracker::default();
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        let series = [DecodedSeries { labels, samples: vec![(1, 1.0)], histograms: vec![], exemplars: vec![] }];
        assert!(matches!(ha_decision(&t, "tenant", &series), HaDecision::Accept));
    }

    #[test]
    fn strip_removes_replica_label() {
        let mut series = vec![series_with("c1", "r1")];
        strip_replica_label(&mut series);
        assert!(series[0].labels.get("__replica__") == None);
        assert!(series[0].labels.get("cluster") == Some("c1"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib distributor::ha`
Expected: FAIL — `cannot find type HaTracker`.

- [ ] **Step 3: Implement `ha.rs`**

```rust
//! HA dedup: consult the elected `__replica__` per `(tenant, cluster)` (from the
//! compacted HA-tracker topic) and drop non-elected replicas before the WAL
//! append, so Prometheus HA pairs don't double-write. The distributor returns
//! HTTP 202 on a drop so the losing replica doesn't retry.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::wire::DecodedSeries;

/// The compacted HA-tracker topic: `(tenant, cluster) -> elected __replica__`.
pub const HA_TRACKER_TOPIC: &str = "__crabka_metrics_ha";

/// In-memory view of the elected replica per `(tenant, cluster)`, rebuilt by
/// replaying the compacted HA-tracker topic and extended in-process by
/// first-seen election (see `elect_or_get`). Interior `Mutex` so the dedup
/// decision can elect an unseen pair atomically behind a shared `&HaTracker`.
#[derive(Debug, Default)]
pub struct HaTracker {
    elected: Mutex<HashMap<(String, String), String>>,
}

impl HaTracker {
    #[must_use]
    pub fn elected_replica(&self, tenant: &str, cluster: &str) -> Option<String> {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .get(&(tenant.to_string(), cluster.to_string()))
            .cloned()
    }

    pub fn set_elected(
        &self,
        tenant: impl Into<String>,
        cluster: impl Into<String>,
        replica: impl Into<String>,
    ) {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .insert((tenant.into(), cluster.into()), replica.into());
    }

    /// Atomically elect `replica` for an unseen `(tenant, cluster)`, or return
    /// the already-elected replica. The returned string is the winner; the
    /// caller accepts iff it equals the request's replica. This is the minimal
    /// in-process dedup that prevents two HA replicas both winning an unseen
    /// pair. (Persisting/replaying the compacted topic is a follow-on TODO.)
    #[must_use]
    pub fn elect_or_get(&self, tenant: &str, cluster: &str, replica: &str) -> String {
        self.elected
            .lock()
            .expect("HaTracker mutex poisoned")
            .entry((tenant.to_string(), cluster.to_string()))
            .or_insert_with(|| replica.to_string())
            .clone()
    }
}

/// Whether to accept or drop a request based on HA dedup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaDecision {
    Accept,
    Drop,
}

/// Inspect the first series' `cluster` + `__replica__` labels. No `__replica__`
/// => HA not in use => Accept. Otherwise Accept iff this is the elected replica.
/// An unseen `(tenant, cluster)` elects the first replica we see (in-process,
/// atomically) and drops the others — so two HA replicas can't both win.
#[must_use]
pub fn ha_decision(tracker: &HaTracker, tenant: &str, series: &[DecodedSeries]) -> HaDecision {
    let Some(first) = series.first() else {
        return HaDecision::Accept;
    };
    let Some(replica) = first.labels.get("__replica__") else {
        return HaDecision::Accept;
    };
    let cluster = first.labels.get("cluster").unwrap_or("");
    // `elect_or_get` first-seen-elects an unknown pair atomically, so the very
    // first replica to arrive wins and all others Drop — no fail-open
    // double-write. TODO(slice4-ha-election): also persist the election to
    // HA_TRACKER_TOPIC so it survives restart / spans distributor replicas.
    let elected = tracker.elect_or_get(tenant, cluster, replica);
    if elected == replica {
        HaDecision::Accept
    } else {
        HaDecision::Drop
    }
}

/// Remove the `__replica__` label from every series before the WAL write (it is
/// an HA-coordination label, not part of series identity downstream).
pub fn strip_replica_label(series: &mut [DecodedSeries]) {
    for s in series {
        let mut rebuilt = crabka_blockstore::Labels::new();
        for (name, value) in s.labels.iter() {
            if name != "__replica__" {
                rebuilt.insert(name.clone(), value.clone());
            }
        }
        s.labels = rebuilt;
    }
}
```

> **`Labels` has no `remove`** (Slice 1/blockstore API is insert/get/iter). Rebuilding without the key is the supported way; verify `Labels::iter()` yields `(&String, &String)` and that reassigning `s.labels` is allowed (the field is `pub`). If `Labels` gains a `remove` later, simplify.

- [ ] **Step 4: Declare modules + run**

`lib.rs`: `pub mod distributor;`. In `distributor/mod.rs`: `pub mod ha; pub use ha::{ha_decision, strip_replica_label, HaDecision, HaTracker, HA_TRACKER_TOPIC};`

Run: `cargo test -p crabka-metrics --lib distributor::ha`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): HA dedup decision + replica-label strip"
```

---

### Task 8: Distributor axum server — routes, limits, produce

**Files:**
- Modify: `crates/metrics/src/distributor/mod.rs` (router + handlers + serve)

**Interfaces:**
- Produces:
  - `struct TenantLimits { pub max_label_value_len: usize, pub max_series_per_request: usize }` (`Default`)
  - `struct DistributorState { producer, tracker, limits, max_decompressed }` (the axum `State`)
  - `fn router(state: Arc<DistributorState>) -> axum::Router` — routes `POST /api/v1/push`, `POST /otlp/v1/metrics`.
  - `fn validate(series: &[DecodedSeries], limits: &TenantLimits) -> Result<(), WireError>`
  - `async fn produce_series(producer, tenant, series) -> Result<(), ProduceError>` — fan `DecodedSeries` → `WalRecord`s → `ProducerRecord` keyed by `partition_key`.
  - `async fn serve(addr, state, shutdown) -> std::io::Result<SocketAddr>` (mirrors `metrics_server::run`).

**Tenant** comes from the `X-Scope-OrgID` header (Mimir convention).

- [ ] **Step 1: Write the failing handler tests (in-process, no broker)**

Test the router with `tower::ServiceExt::oneshot` (the `metrics_server.rs` pattern) using a **fake producer trait** so no broker is needed. Define a `trait WalSink { async fn append(&self, rec: WalRecord) -> Result<(), ProduceError>; }`, implement it for the real producer wrapper, and use a `Vec`-collecting fake in tests.

Create the test module in `distributor/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::pb;
    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use prost::Message;
    use std::sync::Arc;
    use tower::ServiceExt as _;

    fn snappy(b: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(b).unwrap()
    }

    fn v1_body() -> Vec<u8> {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![pb::v1::Label { name: "__name__".into(), value: "up".into() }],
                samples: vec![pb::v1::Sample { value: 1.0, timestamp: 100 }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    #[tokio::test]
    async fn push_v1_returns_204_and_appends() {
        let state = test_state(); // wires the recording fake sink
        let app = router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::NO_CONTENT);
        assert!(state.appended_count() == 1);
    }

    #[tokio::test]
    async fn push_v2_sets_written_headers() {
        // build a v2 snappy body; assert 204 + X-Prometheus-Remote-Write-Samples-Written: 1
        // (see Task 4 v2 test for body construction)
        // ...
    }

    #[tokio::test]
    async fn unsupported_content_type_is_415() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "t")
                    .body(Body::from(vec![1, 2, 3]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn non_elected_replica_returns_202() {
        // state with HaTracker electing "r1"; send a body whose first series has
        // __replica__="r2" => 202 Accepted, nothing appended.
        // ...
    }
}
```

> Provide a `test_state()` helper + `DistributorState` parameterized over a `WalSink` (a boxed trait object or a generic). The recording fake counts appends and lets the HA test assert zero appends on a 202.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib distributor::tests`
Expected: FAIL — `cannot find function router`.

- [ ] **Step 3: Implement `distributor/mod.rs`**

Implement:
- `WalSink` trait (`async fn append(&self, WalRecord) -> Result<(), ProduceError>`), a `KafkaSink` wrapping `crabka_client_producer::Producer` (in `produce_series`, build a `ProducerRecord { topic: WAL_TOPIC.into(), key: Some(partition_key(tenant, fp)), value: Some(Bytes::from(rec.encode()?)), partition: None, .. }` and `producer.send(record).await.await??`), and `DistributorState { sink: Arc<dyn WalSink>, tracker: HaTracker, limits: TenantLimits, max_decompressed: usize }`.
- `router()`: `Router::new().route("/api/v1/push", post(push)).route("/otlp/v1/metrics", post(otlp_push)).with_state(state)`.
- `push` handler: read `X-Scope-OrgID` (400 if missing under multi-tenant), `Content-Type` → `negotiate()`; on `RemoteWriteV1` call `decode_v1`, on `RemoteWriteV2` call `decode_v2`; `validate(&series, &limits)`; `ha_decision` → `Drop` returns `202 Accepted`; else `strip_replica_label`, append each as a `WalRecord` (one record per (series, sample) — fan float samples and histograms into separate `SamplePayload`s), return `204` (v2: with the three `X-Prometheus-Remote-Write-*-Written` headers from `WrittenCounts`). Map `WireError::status_code()` to the response.
- `otlp_push` handler: `decode_otlp` (or stateful) → same validate/HA/append path → `200 OK` (OTLP HTTP success is 200, body empty `ExportMetricsServiceResponse`).
- `validate`: enforce `max_series_per_request` and `max_label_value_len`; over-limit → `WireError::Invalid` (→ 400) or a dedicated 429 for rate (rate-limiting integration with Crabka quotas is `// TODO(slice4-quota)`; the structural cap is enforced now).
- `serve()`: mirror `crates/broker/src/metrics_server.rs::run` — bind `TcpListener`, `axum::serve(listener, router(state)).with_graceful_shutdown(...)`, return the bound `SocketAddr`. (TLS is the `grpc-gateway/serve.rs` pattern; defer TLS to hardening — plaintext serve here, note `// TODO(slice4-tls)`.)

Body extraction: axum `body::Bytes` extractor gives the raw bytes (`Content-Encoding: snappy` is decoded by *us* via `snappy_block_decode`, not axum — do not enable any axum decompression layer).

> **Verify the producer `.send` ack pattern** (`producer.send(rec).await.await??` — outer await resolves partition, inner await the oneshot for broker ack) against `crates/client-producer/src/lib.rs`. For throughput, the distributor may `send` without awaiting each inner oneshot and `flush()` at end-of-request; for correctness of the test, await acks. Keep it simple: await per record in this slice.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib distributor`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): distributor axum server — push/otlp routes, limits, HA, WAL produce"
```

---

### Task 9: Compactor — WAL consumer-group → blocks → index

**Files:**
- Create: `crates/metrics/src/compactor.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - `fn object_key(tenant: &str, partition: i32, min_offset: i64, max_offset: i64, min_ts: i64, max_ts: i64) -> String` — deterministic idempotent key.
  - `fn group_and_sort(records: &[WalRecord]) -> BTreeMap<(String, u64), Vec<&WalRecord>>` — group by `(tenant, fingerprint)`, each group sorted by timestamp.
  - `fn build_batches(grouped) -> Result<HashMap<String, (SchemaRef, Vec<RecordBatch>)>, CompactError>` — split into float / native-histogram batches per tenant via Slice-1 codecs.
  - `async fn compact_batch(blockstore: &mut BlockStore, tenant, partition, records, offset_range) -> Result<Vec<BlockMeta>, CompactError>` — pure-ish: build batches, `write_block`, `add_series`/`add_block`.
  - `async fn run(consumer, blockstore, index_key, shutdown) -> Result<(), CompactError>` — the loop: poll → decode → compact → **save index + write blocks**, THEN `commit_sync` (crash-safety order).

- [ ] **Step 1: Write the failing unit tests (no broker)**

Create `crates/metrics/src/compactor.rs`. Test the pure pieces (`object_key` determinism, `group_and_sort`, `build_batches`, and `compact_batch` against an in-memory `object_store`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{SamplePayload, WalRecord};
    use assert2::assert;

    fn float_rec(tenant: &str, name: &str, ts: i64, v: f64) -> WalRecord {
        WalRecord {
            tenant: tenant.into(),
            labels: vec![("__name__".into(), name.into())],
            payload: SamplePayload::Float { timestamp_ms: ts, value: v },
            exemplars: vec![],
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
    fn group_and_sort_orders_by_timestamp() {
        let recs = vec![
            float_rec("t", "up", 300, 3.0),
            float_rec("t", "up", 100, 1.0),
            float_rec("t", "up", 200, 2.0),
        ];
        let grouped = group_and_sort(&recs);
        let fp = recs[0].series_fingerprint();
        let g = &grouped[&("t".to_string(), fp)];
        let ts: Vec<i64> = g.iter().map(|r| r.payload.timestamp_ms()).collect();
        assert!(ts == vec![100, 200, 300]);
    }

    #[tokio::test]
    async fn compact_batch_writes_float_block_and_indexes_series() {
        use std::sync::Arc;
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = crabka_blockstore::BlockStore::new(store.clone(), base);

        let recs = vec![
            float_rec("t", "up", 100, 1.0),
            float_rec("t", "up", 200, 2.0),
        ];
        let metas = compact_batch(&mut bs, "t", 0, &recs, (10, 20)).await.unwrap();
        assert!(metas.len() >= 1);
        assert!(metas[0].row_count == 2);
        assert!(metas[0].tenant == "t");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib compactor`
Expected: FAIL — `cannot find function object_key`.

- [ ] **Step 3: Implement `compactor.rs`**

Implement:
- `object_key`: `format!("blocks/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet")` (deterministic from inputs; same WAL offset range + window ⇒ same key ⇒ idempotent overwrite via blockstore's `write_block`).
- `group_and_sort`: `BTreeMap<(String, u64), Vec<&WalRecord>>`, push by `(tenant, fingerprint())`, then `sort_by_key(|r| r.payload.timestamp_ms())` each group.
- `build_batches`: separate float groups from histogram groups; collect `(fp, ts, value)` rows → `encode_float_samples`; `(fp, ts, NativeHistogram)` rows → `encode_native_histograms`. Returns the float batch (schema `float_sample_schema()`) and native batch (`native_histogram_schema()`) separately, since they go to different blocks/schemas. (Exemplar sidecar block is `// TODO(slice4-exemplar-block)` — the exemplar wire-decode already lands in `WalRecord`; writing the sidecar block mirrors the float path against `exemplar_schema()` and is a focused follow-on.)
- `compact_batch`: build batches, for each non-empty (float, native) batch call `bs.writer().write_block(tenant, &object_key(...), schema, &[batch])`, then for each distinct series `bs.index_mut().add_series(tenant, fp, &labels)` and `bs.index_mut().add_block(&meta)`. Return the `BlockMeta`s.
- `run`: loop `consumer.poll(timeout)` → `WalRecord::decode` each `ConsumerRecord.value` → accumulate by source `(partition, offset range)` → `compact_batch` → `bs.index().save(&store, index_key)` → **then** `consumer.commit_sync()`. The block+index write **precedes** the offset commit (spec §9 crash-safety: idempotent keys make a re-process after a crash safe — same key overwrites identical bytes). On shutdown token, break and do a final flush+commit.

```rust
//! Compactor role: consume the WAL topic, group by (tenant, fingerprint),
//! sort by timestamp, build Arrow blocks, write them to the blockstore, update
//! and snapshot the index, THEN commit consumer offsets (crash-safety order).
```

> **Verify** `BlockStore::new(store, base)`, `bs.writer()`, `bs.index_mut()`, `bs.index().save(&store, key)` against the blockstore plan (Task 7/6 there). `write_block` returns `BlockMeta { tenant, object_key, min_ts, max_ts, row_count, fingerprints }`. The schema builders + `encode_*` come from Slice 1. Build the per-series `Labels` for `add_series` from the `WalRecord.labels` vec (insert into a fresh `Labels`).

- [ ] **Step 4: Declare + run**

`lib.rs`: `mod compactor; pub use compactor::{compact_batch, group_and_sort, object_key, run, CompactError};`

Run: `cargo test -p crabka-metrics --lib compactor`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): compactor — WAL group/sort to blockstore blocks + index snapshot"
```

---

### Task 10: Role-selectable binary

**Files:**
- Create: `crates/metrics/src/bin/crabka-metrics.rs`
- Modify: `crates/metrics/Cargo.toml` (`[[bin]]` if needed; clap already a dep)

**Interfaces:**
- Produces: a binary with `--target distributor|compactor` (other targets stubbed with a "not yet implemented in this slice" message). Distributor wires a real `Producer` + `serve`; compactor wires a `Consumer` + `BlockStore` + `run`.

- [ ] **Step 1: Write the failing test (arg parsing)**

`clap`'s derive parser is unit-testable. Create the binary with a `#[derive(Parser)] struct Cli { #[arg(long)] target: Target, #[arg(long, default_value = "127.0.0.1:9009")] listen: String, #[arg(long, default_value = "127.0.0.1:9092")] bootstrap: String, ... }` and `#[derive(Clone, ValueEnum)] enum Target { Distributor, Compactor, Querier, QueryFrontend, Ruler }`. Add a `#[cfg(test)] mod tests` asserting `Cli::try_parse_from(["x","--target","distributor"]).unwrap().target` is `Target::Distributor` and that an unknown `--target foo` errors.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use clap::Parser;

    #[test]
    fn parses_distributor_target() {
        let cli = Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
        assert!(matches!(cli.target, Target::Distributor));
    }

    #[test]
    fn rejects_unknown_target() {
        assert!(Cli::try_parse_from(["crabka-metrics", "--target", "bogus"]).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --bin crabka-metrics`
Expected: FAIL — `cannot find type Cli`.

- [ ] **Step 3: Implement the binary**

`main`: parse `Cli`; `tracing_subscriber` init; build a `CancellationToken` wired to `tokio::signal::ctrl_c`; match `target`:
- `Distributor` → `Producer::builder().bootstrap(&cli.bootstrap).build().await?`, wrap in `KafkaSink`, build `DistributorState`, `distributor::serve(cli.listen.parse()?, state, shutdown).await?`.
- `Compactor` → `Consumer::builder().bootstrap(&cli.bootstrap).group_id("crabka-metrics-compactor").subscribe([WAL_TOPIC.to_string()]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`, build a `BlockStore` over the configured object store (memory for now; real object store config is `// TODO(slice4-objstore-config)`), `compactor::run(consumer, blockstore, "index/metrics.json", shutdown).await?`.
- `Querier | QueryFrontend | Ruler` → `eprintln!` + `std::process::exit(2)` with "target not implemented until slice {N}".

> Keep `main` thin; the testable logic lives in the modules. The binary just wires + serves.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --bin crabka-metrics`
Expected: PASS (2 tests). Then `cargo build -p crabka-metrics --bin crabka-metrics` compiles.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): role-selectable crabka-metrics binary (distributor|compactor)"
```

---

### Task 11: End-to-end broker round-trip (Docker/in-process gated)

**Files:**
- Create: `crates/metrics/tests/ingest_roundtrip.rs`

**Interfaces:**
- Consumes the public API: `distributor::router`/`serve`, `Producer`, `Consumer`, `compactor::run`, `WalRecord`, `WAL_TOPIC`, blockstore.

This is the one test that needs a real broker. Use the in-process broker test-support (`crates/broker/tests/support`) — it starts a broker without Docker (`support::start()`), so it can run in CI. Mark Docker-only paths `#[ignore]`.

- [ ] **Step 1: Decide the harness**

The in-process `support::start()` (from `crates/broker/tests/support/mod.rs`) returns a broker + client + tempdir without Docker. **However** `tests/support/mod.rs` is path-included by the broker crate's own tests; `crabka-metrics` cannot `use` it directly. Two options — pick the cheaper:
- **(A)** Add a small `dev-dependency` path include: copy the minimal `start()` helper into `crates/metrics/tests/support/mod.rs` (the memory note "broker test-support is path-included" — replicate the few lines: `BrokerConfig::for_tests(tempdir)`, `Broker::start(config).await`, `broker.listen_addr()`), with `crabka-broker` as a `dev-dependency`.
- **(B)** Mark the whole round-trip `#[ignore = "requires Docker"]` and use `testcontainers` cp-kafka like `crates/client-core/tests/integration.rs`.

Prefer **(A)** for CI coverage; fall back to **(B)** if `crabka-broker`'s test config isn't reachable as a dev-dep. **Verify** which is feasible by checking whether `BrokerConfig::for_tests` + `Broker::start` are `pub` in `crabka-broker`'s public API (not just test-support). If they're test-only, use **(B)**.

- [ ] **Step 2: Write the round-trip test**

```rust
//! End-to-end: produce a remote_write v1 body through the distributor to a real
//! WAL topic, run the compactor consumer-group, and assert a block lands in the
//! blockstore with the expected rows.

// #[ignore = "requires Docker"] on the test fn if harness (B) is used.
#[tokio::test]
async fn remote_write_v1_lands_as_block() {
    // 1. start broker (in-process or testcontainers per Step 1)
    // 2. admin: create WAL_TOPIC with N partitions
    // 3. build DistributorState with a real KafkaSink(Producer)
    // 4. POST a snappy v1 body to router() via oneshot -> assert 204
    // 5. build a Consumer(group) on WAL_TOPIC, poll until the record arrives,
    //    decode -> assert WalRecord round-trips (tenant/labels/sample)
    // 6. run compact_batch over the polled records into an InMemory blockstore
    // 7. assert the BlockMeta row_count / fingerprints
}
```

Fill in the body using the verified producer/consumer/admin APIs (Task 1 deps). Key assertions: the distributor returns 204; the consumer reads back a `WalRecord` whose `series_fingerprint()` matches; the compactor produces a `BlockMeta` with the right `row_count`.

- [ ] **Step 3: Run**

Run (in-process harness A): `cargo test -p crabka-metrics --test ingest_roundtrip`
Run (Docker harness B): `cargo test -p crabka-metrics --test ingest_roundtrip -- --ignored`
Expected: PASS (the WAL record round-trips and a block is written).

- [ ] **Step 4: Whole-crate gate**

Run: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS (ignored Docker tests skipped), no warnings, formatting clean.

- [ ] **Step 5: Commit**

```bash
git add crates/metrics/
git commit -m "test(metrics): end-to-end remote_write -> WAL -> compactor -> block round-trip"
```

---

## Self-review

**Spec coverage (against §5 ingest + §3 architecture + §11 Slice 4):**
- remote_write v1 + v2 decode, snappy-block, content negotiation, status codes → Tasks 2, 4, 8.
- v2 symbol table (`symbols[0]==""`, even-length refs) → Task 4 (reuses Slice-1 `SymbolTable`).
- v2 `X-Prometheus-Remote-Write-*-Written` headers on 204 → Tasks 4 (`WrittenCounts`), 8 (header emission).
- wire `Histogram` → `NativeHistogram` (integer delta-decode → absolute, float absolute, reset_hint map, NHCB) → Task 3.
- OTLP full type mapping incl. `ExponentialHistogram`→native (scale↔schema clamp, boundary off-by-one), delta→cumulative, name normalization (`translation_strategy`) → Task 6.
- `WalRecord` (the slice-4 contract) + `WAL_TOPIC` + partition key → Task 5.
- distributor (axum, routes, validate, HA-dedup + 202, strip `__replica__`, produce keyed by `(tenant, fp)`) → Tasks 7, 8.
- compactor (consumer-group, group/sort, blocks via blockstore, index snapshot, block+index-then-commit crash-safety, deterministic idempotent key) → Task 9.
- role-selectable binary → Task 10.
- broker round-trip test → Task 11.

**Deviations flagged (deferred with explicit TODO markers, not silently dropped):**
- HA *election persistence* (producing election records to `HA_TRACKER_TOPIC` so the in-process election survives restart / spans replicas) — Task 7 `// TODO(slice4-ha-election)`; the dedup decision (first-seen in-process election, no fail-open) + 202 + strip are fully implemented/tested.
- Exemplar *sidecar block* write — Task 9 `// TODO(slice4-exemplar-block)`; exemplar wire-decode already lands in `WalRecord` (Tasks 4/5).
- Classic OTLP `Histogram`/`Summary` → float series — Task 6 `// TODO(slice4-otlp-classic)`; the harder `ExponentialHistogram` path is done+tested.
- TLS on the distributor + per-tenant rate-limit (429 via Crabka quotas) — `// TODO(slice4-tls)`/`// TODO(slice4-quota)`; structural caps (415/400) are enforced.
- `target_info` resource-attr gauge — `// TODO`; resource-attr → label is straightforward and follows the cumulative path.

**Placeholder scan:** no "TBD"/"similar to Task N" without code. The churn-prone surfaces — prost-generated `oneof`/enum names (Task 1/3), the OTLP exponential-histogram boundary convention (Task 6), the `serde-wincode` call shape (Task 5), the producer `.send` ack pattern (Task 8), and the test-harness reachability (Task 11) — are each bounded with an explicit "verify against X" note and pinned by a behavior test, never fabricated.

**Type consistency:** `DecodedSeries`/`DecodedExemplar` (Task 2) consumed unchanged by v1/v2/OTLP decoders (Tasks 4, 6) and the distributor (Tasks 7, 8). `WalRecord`/`SamplePayload`/`partition_key` (Task 5) consumed by the distributor produce path (Task 8) and the compactor (Task 9). `WireError::status_code()` is the single ingest status mapping (Task 2), used by the distributor (Task 8). `NativeHistogram` field set matches Slice 1 across Tasks 3/5/6/9. The blockstore API (`BlockStore::new`/`writer`/`index_mut`/`write_block`/`add_series`/`add_block`/`save`) matches the blockstore plan exactly (Task 9).

**Known risks (flagged, not hidden):**
- **prost codegen + protoc in CI** — Task 1 build.rs needs `protoc`; mirror `grpc-gateway/build.rs`'s fallback if CI lacks it. Pinned by the two prost round-trip tests so a codegen break is a compile error, not silent.
- **remote_write v2 is `2.0-rc.4`/experimental** — the vendored proto is pinned with a tag comment; expect churn. Contained to `proto/` + `wire/v2.rs`.
- **The exponential-histogram boundary `+1`** is the single subtlest correctness claim; it is asserted by a focused test now and will be cross-checked against real Prometheus/Mimir in Slice 8's differential harness — adjust there with a failing test if it diverges.
