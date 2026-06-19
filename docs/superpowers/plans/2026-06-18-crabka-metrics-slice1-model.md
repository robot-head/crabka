# crabka-metrics Slice 1 — Data Layer (block schemas + native-histogram codec + symbol table)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the metrics data layer — the Arrow/Parquet block schemas (float samples, native histograms, exemplars), the native-histogram ⇄ Arrow codec, and the remote_write v2 symbol table — as the foundation the PromQL engine and ingest path build on.

**Architecture:** Pure data-model crate `crabka-metrics` (this slice adds no networking and no DataFusion). Three Arrow schemas on blockstore's signal-agnostic substrate (mandatory `series_fingerprint`+`timestamp` + payload). The hard, novel piece is the native-histogram codec: an in-memory `NativeHistogram` (absolute bucket counts) ⇄ Arrow `List<Struct>`/`List<Float64>` columns, kept absolute at rest (deltas are a wire concern decoded at ingest, a later slice).

**Tech Stack:** Rust 2024 · `arrow` 59 (`array`, `datatypes`, `record_batch`) · `thiserror`. Tests: `assert2`, `proptest`, `tempfile`.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums freely; no shims, no migration code.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Arrow version identity:** use `arrow` 59 directly (`use arrow::...`), matching `crabka-blockstore`. Both unify to one arrow instance, so the schemas this crate produces are consumable by blockstore's `BlockWriter` without conversion.
- **Absolute counts at rest:** native-histogram bucket counts are stored as **absolute** `Float64`. Wire delta-decoding belongs to the ingest slice, not here. The codec round-trips absolute values.

---

## Dependency & slice roadmap

**Depends on:** `crabka-blockstore` (the logs-wedge Phase 1 plan). This slice's *schemas* are plain Arrow `SchemaRef`s and its *codec* produces/consumes `RecordBatch`es, so it is **independently testable without blockstore implemented** — the blockstore dependency only materializes when the compactor (Slice 4) writes these batches as blocks. Note the dependency in the crate but gate nothing on it here.

**The 8 metrics slices** (this plan = Slice 1; each later slice gets its own plan):

1. **Data layer** *(this plan)* — block schemas + native-histogram codec + symbol table.
2. **`crabka-promql` core** — parser + DataFusion operator pattern (`SeriesDivide`/`Normalize`/`Instant`/`Range` + `RangeArray`) + selectors + rate-family + aggregations + binary ops + the `.test` harness.
3. **Query completeness** — `histogram_quantile` (classic + native), full function catalog, subqueries, `@`/`offset`.
4. **Ingest service** — remote_write v1/v2 (wire→`NativeHistogram` decode lives here) + OTLP + Kafka produce + distributor + HA dedup + compactor.
5. **Querier + Prometheus HTTP API** + hot/cold merge.
6. **Query-frontend** — split / shard / cache.
7. **Ruler** — recording + alerting + rule API.
8. **Hardening** — multi-tenancy/limits, remote_read, prometheus/compliance + differential-vs-Mimir.

---

## File structure (`crates/metrics/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/schema.rs` | column-name constants + the three Arrow schema builders |
| `src/histogram.rs` | `NativeHistogram`, `BucketSpan`, `ResetHint` + Arrow codec |
| `src/sample.rs` | float-sample codec |
| `src/exemplar.rs` | `Exemplar` + exemplar-block codec |
| `src/symbols.rs` | `SymbolTable` (remote_write v2 string interning) |

---

### Task 1: Crate scaffold

**Files:**
- Create: `crates/metrics/Cargo.toml`
- Create: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces: a compiling `crabka-metrics` crate with a placeholder test.

- [ ] **Step 1: Create `crates/metrics/Cargo.toml`**

```toml
[package]
name = "crabka-metrics"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Prometheus/Grafana-Mimir-equivalent metrics backend for Crabka (data layer)"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-metrics"
readme = "README.md"
keywords = ["observability", "prometheus", "mimir", "metrics", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
arrow = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
```

- [ ] **Step 2: Create `crates/metrics/src/lib.rs`**

```rust
//! Prometheus/Grafana-Mimir-equivalent metrics backend for Crabka.
//!
//! Slice 1 (this code) is the data layer: the Arrow block schemas, the
//! native-histogram codec, and the remote_write v2 symbol table. No networking,
//! no DataFusion — those arrive in later slices.

/// Placeholder until Task 2 lands real modules.
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
}
```

- [ ] **Step 3: Build and test**

Run: `cargo test -p crabka-metrics`
Expected: compiles, `smoke` PASSES.

- [ ] **Step 4: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): scaffold crabka-metrics crate"
```

---

### Task 2: Column constants + the three Arrow schema builders

**Files:**
- Create: `crates/metrics/src/schema.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - Mandatory column constants (matching blockstore): `COL_FINGERPRINT = "series_fingerprint"`, `COL_TIMESTAMP = "timestamp"`.
  - `pub fn float_sample_schema() -> arrow::datatypes::SchemaRef`
  - `pub fn native_histogram_schema() -> arrow::datatypes::SchemaRef`
  - `pub fn exemplar_schema() -> arrow::datatypes::SchemaRef`
  - native-histogram column-name constants (`COL_NH_SCHEMA`, `COL_NH_IS_FLOAT`, `COL_NH_RESET_HINT`, `COL_NH_ZERO_THRESHOLD`, `COL_NH_ZERO_COUNT`, `COL_NH_COUNT`, `COL_NH_SUM`, `COL_NH_POS_SPANS`, `COL_NH_POS_COUNTS`, `COL_NH_NEG_SPANS`, `COL_NH_NEG_COUNTS`, `COL_NH_CUSTOM_VALUES`, `COL_NH_START_TS`).

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/schema.rs`:

```rust
#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;

    #[test]
    fn float_schema_has_mandatory_and_value() {
        let s = float_sample_schema();
        assert!(s.column_with_name(COL_FINGERPRINT).unwrap().1.data_type() == &DataType::UInt64);
        assert!(s.column_with_name(COL_TIMESTAMP).unwrap().1.data_type() == &DataType::Int64);
        assert!(s.column_with_name("value").unwrap().1.data_type() == &DataType::Float64);
    }

    #[test]
    fn native_histogram_span_columns_are_list_of_struct() {
        let s = native_histogram_schema();
        let (_, field) = s.column_with_name(COL_NH_POS_SPANS).unwrap();
        // List<Struct<offset:Int32, length:UInt32>>
        match field.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::Struct(fields) => {
                    assert!(fields.len() == 2);
                    assert!(fields[0].name() == "offset");
                    assert!(fields[1].name() == "length");
                }
                other => panic!("expected Struct, got {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn exemplar_schema_promotes_trace_and_span() {
        let s = exemplar_schema();
        assert!(s.column_with_name("trace_id").unwrap().1.data_type() == &DataType::Utf8);
        assert!(s.column_with_name("span_id").unwrap().1.data_type() == &DataType::Utf8);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib schema`
Expected: FAIL — `cannot find function float_sample_schema`.

- [ ] **Step 3: Implement `schema.rs`**

Prepend above the `tests` module:

```rust
//! Arrow schemas for the three metric block types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

/// Mandatory blockstore column: series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory blockstore column: sample timestamp in epoch milliseconds (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

// Native-histogram payload columns.
pub const COL_NH_SCHEMA: &str = "schema";
pub const COL_NH_IS_FLOAT: &str = "is_float";
pub const COL_NH_RESET_HINT: &str = "reset_hint";
pub const COL_NH_ZERO_THRESHOLD: &str = "zero_threshold";
pub const COL_NH_ZERO_COUNT: &str = "zero_count";
pub const COL_NH_COUNT: &str = "count";
pub const COL_NH_SUM: &str = "sum";
pub const COL_NH_POS_SPANS: &str = "positive_spans";
pub const COL_NH_POS_COUNTS: &str = "positive_counts";
pub const COL_NH_NEG_SPANS: &str = "negative_spans";
pub const COL_NH_NEG_COUNTS: &str = "negative_counts";
pub const COL_NH_CUSTOM_VALUES: &str = "custom_values";
pub const COL_NH_START_TS: &str = "start_timestamp_ms";

fn fingerprint_field() -> Field {
    Field::new(COL_FINGERPRINT, DataType::UInt64, false)
}

fn timestamp_field() -> Field {
    Field::new(COL_TIMESTAMP, DataType::Int64, false)
}

/// `List<Struct<offset:Int32, length:UInt32>>` — a bucket-span column.
fn span_list_type() -> DataType {
    let struct_fields = Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ]);
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(struct_fields),
        true,
    )))
}

/// `List<Float64>` — a bucket-count column.
fn f64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
}

/// Float samples: counters, gauges, classic-histogram bucket series.
#[must_use]
pub fn float_sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Native (exponential) histograms — absolute bucket counts at rest.
#[must_use]
pub fn native_histogram_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new(COL_NH_SCHEMA, DataType::Int8, false),
        Field::new(COL_NH_IS_FLOAT, DataType::Boolean, false),
        Field::new(COL_NH_RESET_HINT, DataType::Int8, false),
        Field::new(COL_NH_ZERO_THRESHOLD, DataType::Float64, false),
        Field::new(COL_NH_ZERO_COUNT, DataType::Float64, false),
        Field::new(COL_NH_COUNT, DataType::Float64, false),
        Field::new(COL_NH_SUM, DataType::Float64, false),
        Field::new(COL_NH_POS_SPANS, span_list_type(), false),
        Field::new(COL_NH_POS_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_NEG_SPANS, span_list_type(), false),
        Field::new(COL_NH_NEG_COUNTS, f64_list_type(), false),
        Field::new(COL_NH_CUSTOM_VALUES, f64_list_type(), true),
        Field::new(COL_NH_START_TS, DataType::Int64, true),
    ]))
}

/// Exemplar sidecar — trace/span promoted to dedicated columns; remaining
/// exemplar labels as a `Map<Utf8,Utf8>`.
#[must_use]
pub fn exemplar_schema() -> SchemaRef {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    Arc::new(Schema::new(vec![
        fingerprint_field(),
        timestamp_field(),
        Field::new("value", DataType::Float64, false),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("labels", DataType::Map(Arc::new(entries), false), true),
    ]))
}
```

> **Arrow-builder note:** `DataType::List`/`Struct`/`Map` field naming conventions (the inner field named `"item"`, Map's `"entries"`/`"keys"`/`"values"`) must match what the corresponding builders emit in Task 4/7, or `RecordBatch::try_new` fails schema validation. If a builder produces a differently-named inner field at arrow 59, align the schema to the builder (the builder's output is the source of truth).

- [ ] **Step 4: Add module to `lib.rs`**

Replace the placeholder `lib.rs` body's `crate_smoke` + tests with:

```rust
mod schema;

pub use schema::{
    COL_FINGERPRINT, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS,
    COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA,
    COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, COL_TIMESTAMP,
    exemplar_schema, float_sample_schema, native_histogram_schema,
};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib schema`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): Arrow schemas for float/native-histogram/exemplar blocks"
```

---

### Task 3: `NativeHistogram` model

**Files:**
- Create: `crates/metrics/src/histogram.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - `struct BucketSpan { pub offset: i32, pub length: u32 }` (`Clone`,`Debug`,`PartialEq`,`Eq`)
  - `enum ResetHint { Unknown, Yes, No, Gauge }` (`Copy`; `as_i8()`/`from_i8(i8) -> ResetHint`)
  - `struct NativeHistogram` with public fields: `schema: i8`, `is_float: bool`, `reset_hint: ResetHint`, `zero_threshold: f64`, `zero_count: f64`, `count: f64`, `sum: f64`, `positive_spans: Vec<BucketSpan>`, `positive_counts: Vec<f64>` (absolute), `negative_spans: Vec<BucketSpan>`, `negative_counts: Vec<f64>` (absolute), `custom_values: Option<Vec<f64>>`, `start_timestamp_ms: Option<i64>` (`Clone`,`Debug`,`PartialEq`)
  - `pub fn is_nhcb(&self) -> bool` (`schema == -53`)

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/histogram.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn reset_hint_round_trips_i8() {
        for h in [ResetHint::Unknown, ResetHint::Yes, ResetHint::No, ResetHint::Gauge] {
            assert!(ResetHint::from_i8(h.as_i8()) == h);
        }
    }

    #[test]
    fn nhcb_detected_by_schema() {
        let mut h = sample_histogram();
        assert!(!h.is_nhcb());
        h.schema = -53;
        assert!(h.is_nhcb());
    }

    fn sample_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-128,
            zero_count: 3.0,
            count: 10.0,
            sum: 42.5,
            positive_spans: vec![BucketSpan { offset: 0, length: 2 }],
            positive_counts: vec![4.0, 3.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib histogram`
Expected: FAIL — `cannot find type NativeHistogram`.

- [ ] **Step 3: Implement the model**

Prepend above the `tests` module:

```rust
//! In-memory native-histogram representation (absolute bucket counts) and its
//! Arrow codec. Wire delta-decoding (integer histograms) belongs to the ingest
//! slice; this type always holds absolute counts.

/// A run of populated buckets: `offset` (signed gap from the previous span's
/// end, or absolute bucket index for the first span) + `length` (run length).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}

/// Counter-reset semantics carried with each histogram sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

impl ResetHint {
    #[must_use]
    pub fn as_i8(self) -> i8 {
        match self {
            ResetHint::Unknown => 0,
            ResetHint::Yes => 1,
            ResetHint::No => 2,
            ResetHint::Gauge => 3,
        }
    }

    #[must_use]
    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => ResetHint::Yes,
            2 => ResetHint::No,
            3 => ResetHint::Gauge,
            _ => ResetHint::Unknown,
        }
    }
}

/// A native (exponential) histogram sample with **absolute** bucket counts.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogram {
    pub schema: i8,
    pub is_float: bool,
    pub reset_hint: ResetHint,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<BucketSpan>,
    pub positive_counts: Vec<f64>,
    pub negative_spans: Vec<BucketSpan>,
    pub negative_counts: Vec<f64>,
    pub custom_values: Option<Vec<f64>>,
    pub start_timestamp_ms: Option<i64>,
}

impl NativeHistogram {
    /// NHCB (native histogram with custom buckets) sentinel schema.
    #[must_use]
    pub fn is_nhcb(&self) -> bool {
        self.schema == -53
    }
}
```

- [ ] **Step 4: Add module to `lib.rs`**

Add `mod histogram;` and `pub use histogram::{BucketSpan, NativeHistogram, ResetHint};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib histogram`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): NativeHistogram model + ResetHint + BucketSpan"
```

---

### Task 4: Native-histogram Arrow codec (the centerpiece)

**Files:**
- Modify: `crates/metrics/src/histogram.rs` (add `encode`/`decode`)

**Interfaces:**
- Consumes: `native_histogram_schema`, the `COL_NH_*` constants, `NativeHistogram`, `BucketSpan`, `ResetHint`.
- Produces:
  - `pub fn encode_native_histograms(rows: &[(u64, i64, NativeHistogram)]) -> Result<arrow::record_batch::RecordBatch, HistogramCodecError>` — `(fingerprint, timestamp, hist)` rows → a `RecordBatch` matching `native_histogram_schema()`.
  - `pub fn decode_native_histograms(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<(u64, i64, NativeHistogram)>, HistogramCodecError>`.
  - `enum HistogramCodecError` (`thiserror`).

- [ ] **Step 1: Write the failing round-trip test**

Append to the `tests` module in `histogram.rs`:

```rust
    #[test]
    fn encode_decode_round_trips() {
        let h1 = sample_histogram(); // custom_values: None (absent)
        let mut h2 = sample_histogram();
        h2.is_float = true;
        h2.negative_spans = vec![BucketSpan { offset: -1, length: 1 }];
        h2.negative_counts = vec![2.0];
        h2.custom_values = Some(vec![0.5, 1.0, 2.0]); // present, non-empty
        h2.schema = -53;
        h2.start_timestamp_ms = Some(123);
        // Empty-but-present custom_values: NHCB validity hinges on distinguishing
        // absent (None) from empty (`Some(vec![])`), so pin that equivalence class.
        let mut h3 = sample_histogram();
        h3.custom_values = Some(vec![]); // present, empty
        h3.schema = -53;

        let rows = vec![
            (10_u64, 1000_i64, h1.clone()),
            (20_u64, 2000_i64, h2.clone()),
            (30_u64, 3000_i64, h3.clone()),
        ];
        let batch = encode_native_histograms(&rows).unwrap();
        assert!(batch.num_rows() == 3);

        let back = decode_native_histograms(&batch).unwrap();
        assert!(back == rows);
        // Explicit absent-vs-empty assertions: None, Some(non-empty), Some(empty).
        assert!(back[0].2.custom_values == None);
        assert!(back[1].2.custom_values == Some(vec![0.5, 1.0, 2.0]));
        assert!(back[2].2.custom_values == Some(vec![]));
    }

    #[test]
    fn encode_validates_span_count_consistency() {
        let mut bad = sample_histogram();
        bad.positive_spans = vec![BucketSpan { offset: 0, length: 5 }]; // claims 5 buckets
        bad.positive_counts = vec![1.0, 2.0]; // but only 2 counts
        let err = encode_native_histograms(&[(1, 1, bad)]);
        assert!(err.is_err());
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib histogram::tests::encode_decode_round_trips`
Expected: FAIL — `cannot find function encode_native_histograms`.

- [ ] **Step 3: Implement the codec**

Add to `histogram.rs` (above `tests`). This is the fiddliest Arrow code in the slice — building `List<Struct>` and `List<Float64>` columns. Use the builder types and verify their inner-field naming against the schema from Task 2.

```rust
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int8Array,
    Int64Array, Int64Builder, Int8Builder, ListArray, ListBuilder, StructArray, StructBuilder,
    UInt32Builder, UInt64Array, Int32Builder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use crate::schema::{
    COL_FINGERPRINT, COL_NH_COUNT, COL_NH_CUSTOM_VALUES, COL_NH_IS_FLOAT, COL_NH_NEG_COUNTS,
    COL_NH_NEG_SPANS, COL_NH_POS_COUNTS, COL_NH_POS_SPANS, COL_NH_RESET_HINT, COL_NH_SCHEMA,
    COL_NH_START_TS, COL_NH_SUM, COL_NH_ZERO_COUNT, COL_NH_ZERO_THRESHOLD, COL_TIMESTAMP,
    native_histogram_schema,
};

/// Errors from the native-histogram Arrow codec.
#[derive(Debug, thiserror::Error)]
pub enum HistogramCodecError {
    #[error("span/count mismatch: spans claim {spans} buckets, got {counts} counts")]
    SpanCountMismatch { spans: usize, counts: usize },

    #[error("arrow error: {0}")]
    Arrow(String),

    #[error("schema mismatch: column `{0}` missing or wrong type")]
    SchemaMismatch(String),
}

impl From<arrow::error::ArrowError> for HistogramCodecError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Arrow(e.to_string())
    }
}

fn span_bucket_total(spans: &[BucketSpan]) -> usize {
    spans.iter().map(|s| s.length as usize).sum()
}

/// Build the `Field` describing a span-list's inner struct (must match
/// `schema::span_list_type`).
fn span_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("length", DataType::UInt32, false),
    ])
}

fn new_span_list_builder() -> ListBuilder<StructBuilder> {
    let struct_builder = StructBuilder::new(
        span_struct_fields(),
        vec![Box::new(Int32Builder::new()), Box::new(UInt32Builder::new())],
    );
    ListBuilder::new(struct_builder)
}

fn append_spans(builder: &mut ListBuilder<StructBuilder>, spans: &[BucketSpan]) {
    let sb = builder.values();
    for s in spans {
        sb.field_builder::<Int32Builder>(0).unwrap().append_value(s.offset);
        sb.field_builder::<UInt32Builder>(1).unwrap().append_value(s.length);
        sb.append(true);
    }
    builder.append(true);
}

fn append_f64_list(builder: &mut ListBuilder<Float64Builder>, values: &[f64]) {
    for &v in values {
        builder.values().append_value(v);
    }
    builder.append(true);
}

/// Encode `(fingerprint, timestamp, NativeHistogram)` rows into a `RecordBatch`
/// matching [`native_histogram_schema`]. Counts are stored absolute.
pub fn encode_native_histograms(
    rows: &[(u64, i64, NativeHistogram)],
) -> Result<RecordBatch, HistogramCodecError> {
    // Validate span/count consistency up front.
    for (_, _, h) in rows {
        let pos = span_bucket_total(&h.positive_spans);
        if pos != h.positive_counts.len() {
            return Err(HistogramCodecError::SpanCountMismatch {
                spans: pos,
                counts: h.positive_counts.len(),
            });
        }
        let neg = span_bucket_total(&h.negative_spans);
        if neg != h.negative_counts.len() {
            return Err(HistogramCodecError::SpanCountMismatch {
                spans: neg,
                counts: h.negative_counts.len(),
            });
        }
    }

    let mut fp = arrow::array::UInt64Builder::new();
    let mut ts = Int64Builder::new();
    let mut sch = Int8Builder::new();
    let mut is_float = BooleanBuilder::new();
    let mut reset = Int8Builder::new();
    let mut zero_thresh = Float64Builder::new();
    let mut zero_count = Float64Builder::new();
    let mut count = Float64Builder::new();
    let mut sum = Float64Builder::new();
    let mut pos_spans = new_span_list_builder();
    let mut pos_counts = ListBuilder::new(Float64Builder::new());
    let mut neg_spans = new_span_list_builder();
    let mut neg_counts = ListBuilder::new(Float64Builder::new());
    let mut custom = ListBuilder::new(Float64Builder::new());
    let mut start_ts = Int64Builder::new();

    for (f, t, h) in rows {
        fp.append_value(*f);
        ts.append_value(*t);
        sch.append_value(h.schema);
        is_float.append_value(h.is_float);
        reset.append_value(h.reset_hint.as_i8());
        zero_thresh.append_value(h.zero_threshold);
        zero_count.append_value(h.zero_count);
        count.append_value(h.count);
        sum.append_value(h.sum);
        append_spans(&mut pos_spans, &h.positive_spans);
        append_f64_list(&mut pos_counts, &h.positive_counts);
        append_spans(&mut neg_spans, &h.negative_spans);
        append_f64_list(&mut neg_counts, &h.negative_counts);
        match &h.custom_values {
            Some(v) => append_f64_list(&mut custom, v),
            None => custom.append(false), // null list
        }
        match h.start_timestamp_ms {
            Some(v) => start_ts.append_value(v),
            None => start_ts.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fp.finish()),
        Arc::new(ts.finish()),
        Arc::new(sch.finish()),
        Arc::new(is_float.finish()),
        Arc::new(reset.finish()),
        Arc::new(zero_thresh.finish()),
        Arc::new(zero_count.finish()),
        Arc::new(count.finish()),
        Arc::new(sum.finish()),
        Arc::new(pos_spans.finish()),
        Arc::new(pos_counts.finish()),
        Arc::new(neg_spans.finish()),
        Arc::new(neg_counts.finish()),
        Arc::new(custom.finish()),
        Arc::new(start_ts.finish()),
    ];

    Ok(RecordBatch::try_new(native_histogram_schema(), columns)?)
}

fn read_spans(list: &ListArray, row: usize) -> Vec<BucketSpan> {
    let entry = list.value(row);
    let st = entry.as_any().downcast_ref::<StructArray>().expect("span struct");
    let offsets = st.column(0).as_any().downcast_ref::<Int32Array>().expect("offset i32");
    let lengths = st.column(1).as_any().downcast_ref::<arrow::array::UInt32Array>().expect("length u32");
    (0..st.len())
        .map(|i| BucketSpan { offset: offsets.value(i), length: lengths.value(i) })
        .collect()
}

fn read_f64_list(list: &ListArray, row: usize) -> Vec<f64> {
    let entry = list.value(row);
    let arr = entry.as_any().downcast_ref::<Float64Array>().expect("f64 list");
    (0..arr.len()).map(|i| arr.value(i)).collect()
}

/// Decode a `RecordBatch` produced by [`encode_native_histograms`].
pub fn decode_native_histograms(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, NativeHistogram)>, HistogramCodecError> {
    let col = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| HistogramCodecError::SchemaMismatch(name.to_string()))
    };
    let dc = |name: &str| HistogramCodecError::SchemaMismatch(name.to_string());

    let fp = col(COL_FINGERPRINT)?.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| dc(COL_FINGERPRINT))?;
    let ts = col(COL_TIMESTAMP)?.as_any().downcast_ref::<Int64Array>().ok_or_else(|| dc(COL_TIMESTAMP))?;
    let sch = col(COL_NH_SCHEMA)?.as_any().downcast_ref::<Int8Array>().ok_or_else(|| dc(COL_NH_SCHEMA))?;
    let is_float = col(COL_NH_IS_FLOAT)?.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| dc(COL_NH_IS_FLOAT))?;
    let reset = col(COL_NH_RESET_HINT)?.as_any().downcast_ref::<Int8Array>().ok_or_else(|| dc(COL_NH_RESET_HINT))?;
    let zt = col(COL_NH_ZERO_THRESHOLD)?.as_any().downcast_ref::<Float64Array>().ok_or_else(|| dc(COL_NH_ZERO_THRESHOLD))?;
    let zc = col(COL_NH_ZERO_COUNT)?.as_any().downcast_ref::<Float64Array>().ok_or_else(|| dc(COL_NH_ZERO_COUNT))?;
    let count = col(COL_NH_COUNT)?.as_any().downcast_ref::<Float64Array>().ok_or_else(|| dc(COL_NH_COUNT))?;
    let sum = col(COL_NH_SUM)?.as_any().downcast_ref::<Float64Array>().ok_or_else(|| dc(COL_NH_SUM))?;
    let pos_spans = col(COL_NH_POS_SPANS)?.as_any().downcast_ref::<ListArray>().ok_or_else(|| dc(COL_NH_POS_SPANS))?;
    let pos_counts = col(COL_NH_POS_COUNTS)?.as_any().downcast_ref::<ListArray>().ok_or_else(|| dc(COL_NH_POS_COUNTS))?;
    let neg_spans = col(COL_NH_NEG_SPANS)?.as_any().downcast_ref::<ListArray>().ok_or_else(|| dc(COL_NH_NEG_SPANS))?;
    let neg_counts = col(COL_NH_NEG_COUNTS)?.as_any().downcast_ref::<ListArray>().ok_or_else(|| dc(COL_NH_NEG_COUNTS))?;
    let custom = col(COL_NH_CUSTOM_VALUES)?.as_any().downcast_ref::<ListArray>().ok_or_else(|| dc(COL_NH_CUSTOM_VALUES))?;
    let start_ts = col(COL_NH_START_TS)?.as_any().downcast_ref::<Int64Array>().ok_or_else(|| dc(COL_NH_START_TS))?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        out.push((
            fp.value(r),
            ts.value(r),
            NativeHistogram {
                schema: sch.value(r),
                is_float: is_float.value(r),
                reset_hint: ResetHint::from_i8(reset.value(r)),
                zero_threshold: zt.value(r),
                zero_count: zc.value(r),
                count: count.value(r),
                sum: sum.value(r),
                positive_spans: read_spans(pos_spans, r),
                positive_counts: read_f64_list(pos_counts, r),
                negative_spans: read_spans(neg_spans, r),
                negative_counts: read_f64_list(neg_counts, r),
                custom_values: if custom.is_null(r) { None } else { Some(read_f64_list(custom, r)) },
                start_timestamp_ms: if start_ts.is_null(r) { None } else { Some(start_ts.value(r)) },
            },
        ));
    }
    Ok(out)
}
```

> **Arrow-builder verification (do this if compile/test fails):** `StructBuilder::field_builder::<T>(i)`, `ListBuilder::values()`, and the `append(true)` (non-null) / `append(false)` (null list) conventions are arrow-59 API. If a builder method name or the inner list field name (`"item"`) differs, align to the arrow 59 docs and the Task-2 schema — keep the *behavior* (round-trip equality) the test asserts. The `as_*` downcasts in the reader must mirror the builders' output array types.

- [ ] **Step 4: Add re-exports to `lib.rs`**

Extend the histogram re-export: `pub use histogram::{BucketSpan, HistogramCodecError, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib histogram`
Expected: PASS (round-trip + validation tests).

- [ ] **Step 6: Property test — random histograms round-trip**

Create `crates/metrics/tests/histogram_roundtrip.rs`:

```rust
use crabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms,
};
use proptest::prelude::*;

fn arb_span_and_counts() -> impl Strategy<Value = (Vec<BucketSpan>, Vec<f64>)> {
    proptest::collection::vec((0_i32..8, 1_u32..4), 0..3).prop_map(|spans| {
        let spans: Vec<BucketSpan> = spans
            .into_iter()
            .map(|(offset, length)| BucketSpan { offset, length })
            .collect();
        let total: usize = spans.iter().map(|s| s.length as usize).sum();
        let counts = (0..total).map(|i| (i as f64) + 1.0).collect();
        (spans, counts)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn random_native_histograms_round_trip(
        schema in -53_i8..=8,
        is_float in any::<bool>(),
        sum in -1e6_f64..1e6,
        (pos_spans, positive_counts) in arb_span_and_counts(),
        (neg_spans, negative_counts) in arb_span_and_counts(),
    ) {
        let h = NativeHistogram {
            schema,
            is_float,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-100,
            zero_count: 0.0,
            count: positive_counts.iter().chain(&negative_counts).sum(),
            sum,
            positive_spans: pos_spans,
            positive_counts,
            negative_spans: neg_spans,
            negative_counts,
            custom_values: None,
            start_timestamp_ms: None,
        };
        let rows = vec![(7_u64, 99_i64, h.clone())];
        let batch = encode_native_histograms(&rows).unwrap();
        let back = decode_native_histograms(&batch).unwrap();
        prop_assert_eq!(back, rows);
    }
}
```

- [ ] **Step 7: Run the property test**

Run: `cargo test -p crabka-metrics --test histogram_roundtrip`
Expected: PASS (128 cases).

- [ ] **Step 8: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): native-histogram Arrow codec with round-trip property test"
```

---

### Task 5: Float-sample codec

**Files:**
- Create: `crates/metrics/src/sample.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub fn encode_float_samples(rows: &[(u64, i64, f64)]) -> Result<arrow::record_batch::RecordBatch, crate::histogram::HistogramCodecError>`
  - `pub fn decode_float_samples(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<(u64, i64, f64)>, crate::histogram::HistogramCodecError>`

  *(Reuses `HistogramCodecError` as the crate's codec error — rename to `CodecError` is a valid cleanup if preferred; keep one error type.)*

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/sample.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn float_samples_round_trip() {
        let rows = vec![(1_u64, 100_i64, 1.5_f64), (2, 200, -3.0), (1, 300, 0.0)];
        let batch = encode_float_samples(&rows).unwrap();
        assert!(batch.num_rows() == 3);
        let back = decode_float_samples(&batch).unwrap();
        assert!(back == rows);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib sample`
Expected: FAIL — `cannot find function encode_float_samples`.

- [ ] **Step 3: Implement `sample.rs`**

```rust
//! Float-sample block codec (counters, gauges, classic-histogram series).

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::histogram::HistogramCodecError;
use crate::schema::{COL_FINGERPRINT, COL_TIMESTAMP, float_sample_schema};

/// Encode `(fingerprint, timestamp, value)` rows into a float-sample block batch.
pub fn encode_float_samples(
    rows: &[(u64, i64, f64)],
) -> Result<RecordBatch, HistogramCodecError> {
    let fp = UInt64Array::from_iter_values(rows.iter().map(|(f, _, _)| *f));
    let ts = Int64Array::from_iter_values(rows.iter().map(|(_, t, _)| *t));
    let val = Float64Array::from_iter_values(rows.iter().map(|(_, _, v)| *v));
    let columns: Vec<ArrayRef> = vec![Arc::new(fp), Arc::new(ts), Arc::new(val)];
    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}

/// Decode a float-sample block batch.
pub fn decode_float_samples(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, f64)>, HistogramCodecError> {
    let dc = |n: &str| HistogramCodecError::SchemaMismatch(n.to_string());
    let fp = batch
        .column_by_name(COL_FINGERPRINT)
        .ok_or_else(|| dc(COL_FINGERPRINT))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| dc(COL_FINGERPRINT))?;
    let ts = batch
        .column_by_name(COL_TIMESTAMP)
        .ok_or_else(|| dc(COL_TIMESTAMP))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| dc(COL_TIMESTAMP))?;
    let val = batch
        .column_by_name("value")
        .ok_or_else(|| dc("value"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| dc("value"))?;
    Ok((0..batch.num_rows())
        .map(|r| (fp.value(r), ts.value(r), val.value(r)))
        .collect())
}
```

- [ ] **Step 4: Add module + re-exports to `lib.rs`**

Add `mod sample;` and `pub use sample::{decode_float_samples, encode_float_samples};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib sample`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): float-sample block codec"
```

---

### Task 6: Symbol table (remote_write v2 interning)

**Files:**
- Create: `crates/metrics/src/symbols.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces:
  - `struct SymbolTable` (`Default`) with `new()`, `intern(&mut self, s: &str) -> u32`, `resolve(&self, ref_: u32) -> Option<&str>`, `symbols(&self) -> &[String]`, and `from_symbols(Vec<String>) -> Result<SymbolTable, SymbolError>` (validates `symbols[0] == ""`).
  - `fn resolve_label_refs(&self, refs: &[u32]) -> Result<Vec<(String, String)>, SymbolError>` — even-length name/value ref pairs → label pairs.
  - `enum SymbolError` (`thiserror`).

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/symbols.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn intern_is_stable_and_zero_is_empty() {
        let mut t = SymbolTable::new();
        assert!(t.resolve(0) == Some("")); // symbols[0] is always ""
        let a = t.intern("app");
        let b = t.intern("api");
        assert!(t.intern("app") == a); // stable
        assert!(t.resolve(a) == Some("app"));
        assert!(t.resolve(b) == Some("api"));
    }

    #[test]
    fn resolve_label_refs_pairs_names_and_values() {
        let mut t = SymbolTable::new();
        let app = t.intern("app");
        let api = t.intern("api");
        let env = t.intern("env");
        let prod = t.intern("prod");
        let labels = t
            .resolve_label_refs(&[app, api, env, prod])
            .unwrap();
        assert!(labels == vec![("app".into(), "api".into()), ("env".into(), "prod".into())]);
    }

    #[test]
    fn odd_length_refs_rejected() {
        let t = SymbolTable::new();
        assert!(t.resolve_label_refs(&[1]).is_err());
    }

    #[test]
    fn from_symbols_requires_empty_first() {
        assert!(SymbolTable::from_symbols(vec!["x".into()]).is_err());
        assert!(SymbolTable::from_symbols(vec![String::new(), "x".into()]).is_ok());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib symbols`
Expected: FAIL — `cannot find type SymbolTable`.

- [ ] **Step 3: Implement `symbols.rs`**

```rust
//! remote_write v2 string interning. `symbols[0]` is always the empty string;
//! all label names/values and metadata strings are u32 indices into `symbols`.

use std::collections::HashMap;

/// Errors from symbol-table operations.
#[derive(Debug, thiserror::Error)]
pub enum SymbolError {
    #[error("symbols[0] must be the empty string")]
    FirstNotEmpty,

    #[error("label_refs length {0} is not even")]
    OddRefs(usize),

    #[error("symbol ref {0} out of range (len {1})")]
    OutOfRange(u32, usize),
}

/// A string-interning table matching remote_write v2 semantics.
#[derive(Debug)]
pub struct SymbolTable {
    symbols: Vec<String>,
    index: HashMap<String, u32>,
}

impl Default for SymbolTable {
    fn default() -> Self {
        let mut index = HashMap::new();
        index.insert(String::new(), 0_u32);
        Self {
            symbols: vec![String::new()],
            index,
        }
    }
}

impl SymbolTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an existing symbol list (e.g. a received v2 request).
    pub fn from_symbols(symbols: Vec<String>) -> Result<Self, SymbolError> {
        if symbols.first().map(String::as_str) != Some("") {
            return Err(SymbolError::FirstNotEmpty);
        }
        let index = symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), u32::try_from(i).unwrap_or(u32::MAX)))
            .collect();
        Ok(Self { symbols, index })
    }

    /// Intern `s`, returning its stable ref.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&r) = self.index.get(s) {
            return r;
        }
        let r = u32::try_from(self.symbols.len()).expect("symbol table overflow");
        self.symbols.push(s.to_string());
        self.index.insert(s.to_string(), r);
        r
    }

    #[must_use]
    pub fn resolve(&self, ref_: u32) -> Option<&str> {
        self.symbols.get(ref_ as usize).map(String::as_str)
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Resolve even-length `(name_ref, value_ref)` pairs into label pairs.
    pub fn resolve_label_refs(&self, refs: &[u32]) -> Result<Vec<(String, String)>, SymbolError> {
        if refs.len() % 2 != 0 {
            return Err(SymbolError::OddRefs(refs.len()));
        }
        let mut out = Vec::with_capacity(refs.len() / 2);
        for pair in refs.chunks_exact(2) {
            let name = self
                .resolve(pair[0])
                .ok_or(SymbolError::OutOfRange(pair[0], self.symbols.len()))?;
            let value = self
                .resolve(pair[1])
                .ok_or(SymbolError::OutOfRange(pair[1], self.symbols.len()))?;
            out.push((name.to_string(), value.to_string()));
        }
        Ok(out)
    }
}
```

- [ ] **Step 4: Add module + re-exports to `lib.rs`**

Add `mod symbols;` and `pub use symbols::{SymbolError, SymbolTable};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib symbols`
Expected: PASS (4 tests).

- [ ] **Step 6: Final whole-crate gate**

Run: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS, no warnings, formatting clean.

- [ ] **Step 7: Commit**

```bash
git add crates/metrics/
git commit -m "feat(metrics): remote_write v2 symbol table"
```

---

## Self-review

**Spec coverage (against §4 data model + §11 Slice 1):**
- Float-sample block schema + codec → Tasks 2, 5.
- Native-histogram block schema + codec (absolute counts, spans/counts, int/float discriminator, NHCB, reset hint, start ts) → Tasks 2, 3, 4.
- Exemplar block schema (trace_id/span_id promoted, labels Map) → Task 2. *(Exemplar codec deferred to the ingest slice, where exemplar wire-decode lands — flagged below.)*
- Symbol table (remote_write v2 interning, `symbols[0]==""`, even-length refs) → Task 6.
- *Deferred (correctly, to later slices):* wire (proto) decode → `NativeHistogram` (Slice 4 ingest); the PromQL engine (Slice 2); blocks-on-object-storage (uses blockstore's `BlockWriter` — Slice 4 compactor).

**Deviation flagged:** the exemplar *codec* is not in this slice — only its schema. Exemplar encoding is tightly coupled to the ingest decode (wire → exemplar) and the trace_id/span_id normalization (OTLP `bytes` vs Prometheus labels), so it belongs with Slice 4. If a reviewer wants it here, add a Task mirroring Task 5's shape against `exemplar_schema()` with a `MapBuilder` for the `labels` column.

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every step has runnable code or an exact command. The one hand-wave — arrow 59 builder method names for `List<Struct>`/`Map` — is explicitly bounded with verify-against-arrow-59 notes and pinned by round-trip tests, not left vague.

**Type consistency:** `NativeHistogram` field set is identical across Tasks 3/4 and the property test. `HistogramCodecError` is the single codec error type, reused by `sample.rs` (Task 5) — flagged as an intentional shared error. Column constants (`COL_NH_*`, `COL_FINGERPRINT`, `COL_TIMESTAMP`) defined once in Task 2 and referenced unchanged in Tasks 4/5. `SymbolTable` method names (`intern`/`resolve`/`from_symbols`/`resolve_label_refs`/`symbols`) consistent between definition and tests.

**Known risk (flagged):** the `List<Struct>` and `Map` Arrow builder APIs are the churn-prone surface (arrow 59). Contained to `histogram.rs`/`schema.rs` and pinned by round-trip + property tests, so any builder-API drift surfaces as a failing test, not silent corruption.
