# crabka-profiles Slice 1 — Blockstore `ProfileIndex` + profile-samples schema (one-row-per-sample) + symbol-DB artifact

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crabka-blockstore` a *fourth*-signal tenant by adding a `ProfileIndex` (`impl BlockIndex`) — label-series postings that **reuse the metrics `SeriesIndex` label-postings machinery** (do not break `SeriesIndex`/`TraceIndex`; regression-tested), a **profile-type index** (`__profile_type__` → series fingerprints), per-block time-range, and a **stacktrace-partition map**. Define the **flattened profile-samples fact-table** column constants + Arrow/Parquet schema — *one row per SAMPLE* (a deliberate Crabka choice; semantic compat, not phlaredb byte-format) — and the on-block **symbol-DB artifact**: a per-partition parent-pointer stacktrace tree (`node { parent: i32, location_ref: i32 }`, `stacktrace_id = leaf node index`, intern dedups via the tree, resolve climbs parents leaf→root) + dedup tables (locations/functions/mappings/strings, `strings[0] == ""`) with `encode`/`decode`. The headline is the **stacktrace-tree dedup + resolve round-trip property test** — symbols are ~60% of a block's size, so dedup is the dominant lever.

**Architecture:** Pure data layer — no networking, no DataFusion query layer, no Kafka, **no query language** (profiles has none). This slice lands mostly in `crabka-blockstore` (the `ProfileIndex` + the samples-fact-table schema/constants) and *starts* `crabka-pprof` only for the `Frame` + `SymbolDb` types (they are shared: the engine in slice 2 consumes them, and the block-builder in slice 4 interns/resolves through them, so they live in `crabka-pprof` from day one rather than being moved later). The `ProfileIndex`'s label dimension *is* a `SeriesIndex`-style postings index — it **embeds a `SeriesIndex`** for the label/matcher resolution and layers a profile-type index + a stacktrace-partition map on top, so the metrics postings machinery is reused verbatim, not re-implemented. The samples schema is one-row-per-sample (phlaredb is one-row-per-profile with nested `Samples[]`; we flatten for a columnar DataFusion-native fold). The `(stacktrace_partition, stacktrace_id)` slot into the symbol DB is *raw* — never symbolized at rest.

**Tech Stack:** Rust 2024 · `arrow` 59 (`array`, `datatypes`, `record_batch`) · `parquet` 59 · `object_store` 0.13 · `serde` / `serde-wincode` (the symbol-DB artifact codec — workspace convention) · `thiserror`. Tests: `assert2`, `proptest`, `tempfile`, `object_store::memory::InMemory`, `#[tokio::test]` (`tokio` dev-dep features `["macros", "rt-multi-thread"]`). `crabka-pprof` is a *new* crate started here; `crabka-blockstore` is modified in place.

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1 enum variants, no migration code, no default-off feature gates. When the symbol-DB artifact encoding or the samples schema changes, just change it; wipe local data dirs / object-store buckets during development. (Only Kafka wire compat matters — this slice touches none of it.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe` (the parent-pointer tree is a safe `Vec<TreeNode>`; the dedup tables are safe `Vec` + `HashMap`).
- **Lints:** `clippy::pedantic` is `warn` workspace-wide (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` allowed). New code must be clippy-pedantic clean. Run `cargo clippy -p crabka-blockstore --all-targets` (and `-p crabka-pprof`) before each commit.
- **Formatting:** run `cargo fmt -p crabka-blockstore` (and `-p crabka-pprof`) before every commit. **NEVER** run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests, `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked, for the DataFusion-touching slices 5+):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }` (tracks arrow 59 / parquet 59 / object_store 0.13). This slice adds **no** DataFusion dependency — the samples *schema* is plain Arrow `SchemaRef`, materialized into blocks by slice 4's block-builder and queried by slice 5's querier. Note the future dep; gate nothing on it here.
- **Arrow version identity:** import `arrow`/`parquet` directly (`use arrow::...`) as the existing blockstore does; all of arrow/parquet/object_store unify to one instance, so the schemas this slice produces are consumable by blockstore's `BlockWriter` without conversion.
- **Reuse, don't fork, `SeriesIndex`.** The `ProfileIndex` **embeds** a `SeriesIndex` for label/matcher resolution and postings — it does **not** copy the `(name,value) → fingerprints` postings logic. The `SeriesIndex` extraction from the traces slice (`Index` → `SeriesIndex` behind `BlockIndex`) must stay behavior-preserving: every existing `SeriesIndex`/`TraceIndex` test keeps passing unchanged. This is the regression net.
- **One row per SAMPLE.** The samples fact table is a deliberate Crabka flattening — `(series_fingerprint, timestamp, profile_type, stacktrace_id, value, stacktrace_partition, total_value, span_id, trace_id)`, one Arrow row per pprof sample. This is Pyroscope semantic/API compat, **not** phlaredb block-format compat (greenfield — no block-format interop required).
- **Symbols are ~60% of a block; dedup is the lever.** The symbol DB's intern step dedups strings/functions/locations/mappings *and* the parent-pointer stacktrace tree (identical stacks share a path). The headline property test pins the dedup + resolve round-trip.

---

## Dependency & slice roadmap

**Depends on:** `crabka-blockstore` *(as designed in `docs/superpowers/plans/2026-06-18-crabka-blockstore.md`, generalized by the traces slice 1 `docs/superpowers/plans/2026-06-18-crabka-traces-slice1-blockstore-generalize-traceindex.md`)* — the `BlockIndex` trait + `BlockSchema`/`RequiredColumn`/`validate_against`, `SeriesIndex` (the label-postings impl this slice embeds), `BlockStore<I>`, `BlockWriter`/`write_block`/`read_block`, `BlockMeta`, `Labels`/`LabelMatcher`/`MatchOp`, `SeriesFingerprint`, `COL_FINGERPRINT = "series_fingerprint"` (UInt64), `COL_TIMESTAMP = "timestamp"` (Int64). This slice **modifies** blockstore in place (adds `ProfileIndex` + the profile-samples schema) and **starts** `crabka-pprof` (the `Frame` + `SymbolDb` types only). The `crabka-profiles` service crate is **not** started here.

**The 8 profiles slices** (this plan = Slice 1; each later slice gets its own plan; commands use the slice's crate — `crabka-blockstore` + `crabka-pprof` here, `crabka-pprof` for 2–3, `crabka-profiles` for 4–8):

1. **Blockstore `ProfileIndex` + profile-samples schema + symbol-DB artifact** *(this plan)* — `ProfileIndex` (`impl BlockIndex`, embeds `SeriesIndex`) = label postings + `__profile_type__` index + per-block time-range + stacktrace-partition map; the `PCOL_*` samples fact-table constants + `profile_samples_schema()` + `profile_samples_decl()`; the `crabka-pprof` `Frame` + `SymbolDb` (parent-pointer tree + dedup tables + `intern_stacktrace`/`resolve`/`encode`/`decode` + `SymbolSource`). **Freezes:** the `PCOL_*` constants + schema, `ProfileIndex` + its query surface (`add_series`/`resolve`/`profile_types`/`stacktrace_partitions`/`candidate_blocks_for_profile_type`), and the `SymbolDb`/`Frame`/`SymbolSource` public contract.
2. **`crabka-pprof` core** — the pprof model + codec, `ProfileType` parse/`Display`, the `ProfileStore` trait + pinned engine result types, the **MERGE → flamegraph** engine (fold-before-symbolize, `Tree`, 4-ints-per-bar `FlameGraph`). **Consumes** this slice's `SymbolDb`/`Frame`/`SymbolSource` + the `PCOL_*` columns. **No query parser — there is no language.**
3. **Engine completeness** — `SelectSeries` (precomputed `total_value`, step-in-seconds, SUM/AVERAGE), `Diff` (7-ints-per-bar `FlameGraphDiff`), `max_nodes` truncation + synthetic `"other"`, `SelectMergeProfile` → pprof.
4. **Ingest service** — the `distributor` (`push.v1` + `/ingest` + OTLP `v1development` + `relabel` + multi-value split) → `(tenant, series_fingerprint)`-partitioned WAL; the `block-builder` consumer group → the samples fact table + a per-block `SymbolDb` (interning each record's symbol set) + `ProfileIndex` (write-then-commit, idempotent keys). Defines `ProfileRecord`.
5. **Querier + Connect `querier.v1` API + legacy render** — implement `ProfileStore` as the hot/cold UNION over **this slice's** `ProfileIndex` + samples blocks + symbol DBs.
6. **Query-frontend** — split/shard + partial-tree merge.
7. **Native symbolization** (the heavy slice) — query-time `build_id → debuginfod` + DWARF/ELF/`.gopclntab`, behind **this slice's** `SymbolSource` trait.
8. **Hardening** — per-tenant limits, compaction (dedup symbol DBs), differential-vs-Pyroscope, Grafana integration.

---

## File structure

`crates/blockstore/` — modified + new files:

| File | Responsibility | Change |
|---|---|---|
| `src/lib.rs` | module decls + public re-exports | **modify** — re-export `ProfileIndex`, the `PCOL_*` constants, `profile_samples_schema`, `profile_samples_decl` |
| `src/profile_schema.rs` | profile-samples column constants + `profile_samples_schema()` + `profile_samples_decl()` | **create** |
| `src/profile_index.rs` | `ProfileIndex` (impl `BlockIndex`): embedded `SeriesIndex` + `__profile_type__` index + per-block time-range + stacktrace-partition map + snapshot serde | **create** |
| `src/profile_block.rs` | profile-samples row builder: `&[ProfileSampleRow]` → `RecordBatch` matching `profile_samples_schema()` | **create** |

`crates/pprof/` — **new crate** (the language-less engine; this slice lands only the symbol model):

| File | Responsibility | Change |
|---|---|---|
| `Cargo.toml` | crate manifest | **create** |
| `src/lib.rs` | module decls + re-exports + crate docs | **create** |
| `src/error.rs` | `ProfileError` (the engine's error enum; full variant set frozen here) | **create** |
| `src/frame.rs` | `Frame` (a resolved stack frame) + `SymbolSource` trait | **create** |
| `src/symbol_db.rs` | `SymbolDb` (parent-pointer tree + dedup tables) + `intern_stacktrace`/`resolve`/`encode`/`decode` | **create** |

The symbol DB (`symbol_db.rs`) and the nested-set-equivalent tree are pure-compute, fully unit/property-testable without IO. `profile_block.rs` is the only file building Arrow record batches; `profile_index.rs` is pure in-memory + a JSON/wincode snapshot.

---

### Task 1: Profile-samples schema — column constants + `profile_samples_schema()` + `profile_samples_decl()`

**Files:**
- Create: `crates/blockstore/src/profile_schema.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `arrow::datatypes::{DataType, Field, Schema, SchemaRef}`, `BlockSchema`, `RequiredColumn`, `COL_FINGERPRINT`, `COL_TIMESTAMP`.
- Produces:
  - Profile-samples column-name constants: `PCOL_PROFILE_TYPE` (`"profile_type"`), `PCOL_STACKTRACE_ID` (`"stacktrace_id"`), `PCOL_VALUE` (`"value"`), `PCOL_STACKTRACE_PARTITION` (`"stacktrace_partition"`), `PCOL_TOTAL_VALUE` (`"total_value"`), `PCOL_SPAN_ID` (`"span_id"`), `PCOL_TRACE_ID` (`"trace_id"`).
  - `pub fn profile_samples_schema() -> arrow::datatypes::SchemaRef` — the flattened one-row-per-sample Arrow schema (`COL_FINGERPRINT` UInt64, `COL_TIMESTAMP` Int64, `PCOL_PROFILE_TYPE` `Dictionary<Int32,Utf8>`, `PCOL_STACKTRACE_ID` UInt64, `PCOL_VALUE` Int64, `PCOL_STACKTRACE_PARTITION` UInt64, `PCOL_TOTAL_VALUE` Int64, `PCOL_SPAN_ID` UInt64 nullable, `PCOL_TRACE_ID` Binary nullable).
  - `pub fn profile_samples_decl() -> BlockSchema` — the profile signal's declaration: required `series_fingerprint`/`timestamp`/`profile_type`; sort key `[series_fingerprint, profile_type, timestamp]`.

- [ ] **Step 1: Write the failing test**

Create `crates/blockstore/src/profile_schema.rs`:

```rust
#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;
    use crate::block::{COL_FINGERPRINT, COL_TIMESTAMP};

    #[test]
    fn mandatory_columns_match_blockstore() {
        let s = profile_samples_schema();
        assert!(s.column_with_name(COL_FINGERPRINT).unwrap().1.data_type() == &DataType::UInt64);
        assert!(s.column_with_name(COL_TIMESTAMP).unwrap().1.data_type() == &DataType::Int64);
    }

    #[test]
    fn profile_type_is_dictionary_encoded() {
        let s = profile_samples_schema();
        let (_, f) = s.column_with_name(PCOL_PROFILE_TYPE).unwrap();
        match f.data_type() {
            DataType::Dictionary(key, value) => {
                assert!(key.as_ref() == &DataType::Int32);
                assert!(value.as_ref() == &DataType::Utf8);
            }
            other => panic!("expected Dictionary<Int32,Utf8>, got {other:?}"),
        }
    }

    #[test]
    fn raw_stacktrace_slot_columns_are_unsigned() {
        let s = profile_samples_schema();
        assert!(s.column_with_name(PCOL_STACKTRACE_ID).unwrap().1.data_type() == &DataType::UInt64);
        assert!(
            s.column_with_name(PCOL_STACKTRACE_PARTITION).unwrap().1.data_type() == &DataType::UInt64
        );
    }

    #[test]
    fn value_and_total_value_are_int64() {
        let s = profile_samples_schema();
        assert!(s.column_with_name(PCOL_VALUE).unwrap().1.data_type() == &DataType::Int64);
        assert!(s.column_with_name(PCOL_TOTAL_VALUE).unwrap().1.data_type() == &DataType::Int64);
    }

    #[test]
    fn cross_signal_join_keys_are_nullable() {
        let s = profile_samples_schema();
        let span = s.column_with_name(PCOL_SPAN_ID).unwrap().1;
        let trace = s.column_with_name(PCOL_TRACE_ID).unwrap().1;
        assert!(span.data_type() == &DataType::UInt64 && span.is_nullable());
        assert!(trace.data_type() == &DataType::Binary && trace.is_nullable());
    }

    #[test]
    fn decl_requires_fp_type_ts_and_sorts_by_them() {
        let d = profile_samples_decl();
        let names: Vec<&str> = d.required.iter().map(|c| c.name.as_str()).collect();
        assert!(names == vec![COL_FINGERPRINT, PCOL_PROFILE_TYPE, COL_TIMESTAMP]);
        assert!(
            d.sort_key
                == vec![
                    COL_FINGERPRINT.to_string(),
                    PCOL_PROFILE_TYPE.to_string(),
                    COL_TIMESTAMP.to_string()
                ]
        );
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib profile_schema`
Expected: FAIL — `cannot find function profile_samples_schema`.

- [ ] **Step 3: Implement `profile_schema.rs`**

Prepend above the `tests` module. The `profile_type` declaration uses `Dictionary<Int32,Utf8>` because a block holds few distinct profile-type strings against millions of rows — dict-encoding is the natural columnar form (and the `validate_against` declaration matches on it). Note: `validate_against` compares `DataType` exactly, so the declaration's `RequiredColumn` for `profile_type` must carry the same `Dictionary<Int32,Utf8>` type the schema does.

```rust
//! Flattened profile-samples block schema — a deliberate Crabka choice: ONE ROW
//! PER SAMPLE (phlaredb is one-row-per-profile with nested `Samples[]`; we flatten
//! for a columnar DataFusion-native fold). Pyroscope semantic/API compat, NOT
//! phlaredb byte-format compat. The `(stacktrace_partition, stacktrace_id)` slot
//! into the symbol DB is RAW — never symbolized at rest; symbolization happens at
//! query time, after the cheap fold, only for the distinct surviving ids.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::block::{COL_FINGERPRINT, COL_TIMESTAMP};
use crate::block_index::{BlockSchema, RequiredColumn};

/// The 5-part `name:sample_type:sample_unit:period_type:period_unit` profile-type
/// string (dict-encoded — a block holds few distinct types vs many rows).
pub const PCOL_PROFILE_TYPE: &str = "profile_type";
/// Leaf-node index into the symbol-DB partition's parent-pointer tree.
pub const PCOL_STACKTRACE_ID: &str = "stacktrace_id";
/// The sample value for this profile type.
pub const PCOL_VALUE: &str = "value";
/// Which symbol-DB partition resolves this stacktrace id.
pub const PCOL_STACKTRACE_PARTITION: &str = "stacktrace_partition";
/// Precomputed per-profile total (powers `SelectSeries` without a re-fold).
pub const PCOL_TOTAL_VALUE: &str = "total_value";
/// Span association (span-scoped profiling) — nullable.
pub const PCOL_SPAN_ID: &str = "span_id";
/// Trace association — the cross-signal join key — nullable raw bytes.
pub const PCOL_TRACE_ID: &str = "trace_id";

/// `Dictionary<Int32,Utf8>` — the dict-encoded profile-type column type, shared
/// by both the schema and the declaration so `validate_against` matches exactly.
fn profile_type_dict() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// The flattened one-row-per-sample Arrow schema.
#[must_use]
pub fn profile_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // mandatory blockstore columns
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        // profile payload
        Field::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
        Field::new(PCOL_STACKTRACE_ID, DataType::UInt64, false),
        Field::new(PCOL_VALUE, DataType::Int64, false),
        Field::new(PCOL_STACKTRACE_PARTITION, DataType::UInt64, false),
        Field::new(PCOL_TOTAL_VALUE, DataType::Int64, false),
        // cross-signal join keys (nullable)
        Field::new(PCOL_SPAN_ID, DataType::UInt64, true),
        Field::new(PCOL_TRACE_ID, DataType::Binary, true),
    ]))
}

/// The profile signal's `BlockSchema` declaration: `series_fingerprint` (label
/// identity), `profile_type` (the merge dimension), and `timestamp` (the time
/// prefilter) are required; rows are sorted `[series_fingerprint, profile_type,
/// timestamp]` so one series' samples of one type are contiguous.
#[must_use]
pub fn profile_samples_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
            RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            COL_FINGERPRINT.to_string(),
            PCOL_PROFILE_TYPE.to_string(),
            COL_TIMESTAMP.to_string(),
        ],
    }
}
```

> **Arrow-builder note (pin in Task 4):** `Dictionary<Int32,Utf8>` is materialized by a `StringDictionaryBuilder<Int32Type>` in Task 4. If arrow 59 names the dictionary key type or the builder differently, align the *declaration* and *schema* to the builder's actual output `DataType` (the builder is the source of truth) — keep the behavior the Task-4 `batch.schema() == profile_samples_schema()` assertion pins.

- [ ] **Step 4: Wire `lib.rs`**

Add `mod profile_schema;` and re-export:

```rust
pub use profile_schema::{
    PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE,
    PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl, profile_samples_schema,
};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib profile_schema`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): flattened one-row-per-sample profile-samples block schema + PCOL_* constants"
```

---

### Task 2: `ProfileIndex` (impl `BlockIndex`) — embedded `SeriesIndex` + `__profile_type__` index + stacktrace-partition map + snapshot

**Files:**
- Create: `crates/blockstore/src/profile_index.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `BlockIndex`, `BlockMeta`, `SeriesIndex`, `Labels`, `LabelMatcher`, `SeriesFingerprint`, `Result`, `serde`, `object_store`.
- Produces:
  - `pub const LABEL_PROFILE_TYPE: &str = "__profile_type__"` — the reserved label whose value is the 5-part profile-type string.
  - `pub struct ProfileIndex` (`Default`, `Serialize`, `Deserialize`, `impl BlockIndex`) — embeds a `SeriesIndex` (`series: SeriesIndex`) for label/matcher resolution + postings, plus per-tenant `profile_types: BTreeMap<String, BTreeSet<String>>` (`__profile_type__` value → its series fingerprints, as decimal strings) and `block_partitions: BTreeMap<String, Vec<u64>>` (object_key → the stacktrace-partition ids stored in that block's symbol DB).
  - `pub fn new() -> Self`
  - `pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels)` — delegates to the embedded `SeriesIndex::add_series`, and additionally records the `__profile_type__` value → fingerprint mapping when the label is present.
  - `pub fn resolve(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<BTreeSet<SeriesFingerprint>>` — delegates to `SeriesIndex::resolve` (reuse, don't fork).
  - `pub fn profile_types(&self, tenant: &str) -> Vec<String>` — the distinct `__profile_type__` values seen for a tenant (the `ProfileTypes` API source).
  - `pub fn fingerprints_for_profile_type(&self, tenant: &str, profile_type: &str) -> BTreeSet<SeriesFingerprint>` — the `__profile_type__` index lookup.
  - `pub fn add_profile_block(&mut self, tenant: &str, object_key: &str, partitions: Vec<u64>)` — record which stacktrace-partition ids a written block's symbol DB carries.
  - `pub fn stacktrace_partitions(&self, object_key: &str) -> Vec<u64>` — the stacktrace-partition map lookup (which partitions a block's symbol DB resolves).
  - `pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()>` / `pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<ProfileIndex>`.

- [ ] **Step 1: Write the failing tests**

Create `crates/blockstore/src/profile_index.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::from_pairs(pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())))
    }

    fn seed() -> ProfileIndex {
        let mut idx = ProfileIndex::new();
        let cpu = labels(&[
            ("__name__", "process_cpu"),
            ("__profile_type__", "process_cpu:cpu:nanoseconds:cpu:nanoseconds"),
            ("service_name", "checkout"),
        ]);
        let heap = labels(&[
            ("__name__", "memory"),
            ("__profile_type__", "memory:alloc_space:bytes:space:bytes"),
            ("service_name", "checkout"),
        ]);
        idx.add_series("t", cpu.fingerprint(), &cpu);
        idx.add_series("t", heap.fingerprint(), &heap);
        idx
    }

    #[test]
    fn profile_types_lists_distinct_type_strings() {
        let idx = seed();
        let mut types = idx.profile_types("t");
        types.sort();
        assert!(
            types
                == vec![
                    "memory:alloc_space:bytes:space:bytes".to_string(),
                    "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
                ]
        );
        assert!(idx.profile_types("nope").is_empty());
    }

    #[test]
    fn profile_type_index_maps_type_to_its_series() {
        let idx = seed();
        let cpu_fps =
            idx.fingerprints_for_profile_type("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(cpu_fps.len() == 1);
        // The heap type's fingerprint is not in the cpu set.
        let heap_fps =
            idx.fingerprints_for_profile_type("t", "memory:alloc_space:bytes:space:bytes");
        assert!(cpu_fps.is_disjoint(&heap_fps));
    }

    #[test]
    fn resolve_reuses_series_postings() {
        let idx = seed();
        // A label matcher on service_name resolves BOTH series (postings reuse).
        let got = idx
            .resolve("t", &[LabelMatcher::new("service_name", MatchOp::Eq, "checkout")])
            .unwrap();
        assert!(got.len() == 2);
    }

    #[test]
    fn stacktrace_partition_map_records_block_partitions() {
        let mut idx = seed();
        idx.add_profile_block("t", "blocks/p1.parquet", vec![0, 1, 2]);
        assert!(idx.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1, 2]);
        assert!(idx.stacktrace_partitions("blocks/absent.parquet").is_empty());
    }
}
```

> **`Labels`/`LabelMatcher`/`MatchOp` import paths:** the test imports them from `crate::labels` / `crate::matcher` — adjust the `use` lines to wherever blockstore actually defines them (the blockstore plan puts `Labels`/`SeriesFingerprint` in `labels.rs` and `LabelMatcher`/`MatchOp` alongside). The asserted *behavior* is what matters; fix only the paths if they differ.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib profile_index`
Expected: FAIL — `cannot find type ProfileIndex`.

- [ ] **Step 3: Implement `profile_index.rs`**

Prepend above the `tests` module. The `ProfileIndex` **embeds** `SeriesIndex` and delegates all label/matcher/postings work to it — the only net-new state is the `__profile_type__` index + the stacktrace-partition map.

```rust
//! The profiles `BlockIndex` impl. The label dimension IS a `SeriesIndex` postings
//! index — `ProfileIndex` EMBEDS a `SeriesIndex` and delegates label/matcher
//! resolution to it (reuse, don't fork the metrics postings machinery). On top it
//! layers (a) a `__profile_type__` index (`type string -> series fingerprints`,
//! the `ProfileTypes` API source) and (b) a stacktrace-partition map (`object_key
//! -> the partition ids that block's symbol DB resolves`). Per-block time-range
//! is the `SeriesIndex`'s existing block list (reused via the `BlockIndex` trait).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::BlockIndex;
use crate::error::Result;
use crate::index::SeriesIndex;
use crate::labels::{Labels, SeriesFingerprint};
use crate::matcher::LabelMatcher;

/// The reserved label whose value is the 5-part profile-type string. Carried on
/// every profile series; also the `ProfileTypes` API source.
pub const LABEL_PROFILE_TYPE: &str = "__profile_type__";

#[derive(Default, Serialize, Deserialize)]
struct TenantProfileExtras {
    /// `__profile_type__` value -> the series fingerprints carrying it.
    profile_types: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
}

/// Profiles index: an embedded `SeriesIndex` (label postings + per-block time
/// range, reused) plus the profile-specific `__profile_type__` index and the
/// stacktrace-partition map.
#[derive(Default, Serialize, Deserialize)]
pub struct ProfileIndex {
    /// Reused verbatim for label/matcher resolution, postings, block time-range.
    series: SeriesIndex,
    /// Per-tenant `__profile_type__` extras.
    extras: BTreeMap<String, TenantProfileExtras>,
    /// `object_key -> the stacktrace-partition ids stored in that block's symbol DB`.
    block_partitions: BTreeMap<String, Vec<u64>>,
}

impl ProfileIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a series. Delegates label/postings to the embedded `SeriesIndex`,
    /// then records the `__profile_type__` value -> fingerprint mapping if present.
    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        self.series.add_series(tenant, fp, labels);
        if let Some(pt) = labels.get(LABEL_PROFILE_TYPE) {
            self.extras
                .entry(tenant.to_string())
                .or_default()
                .profile_types
                .entry(pt.to_string())
                .or_default()
                .insert(fp);
        }
    }

    /// Resolve label matchers to fingerprints — delegates to `SeriesIndex` (reuse).
    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.resolve(tenant, matchers)
    }

    /// The distinct `__profile_type__` values seen for `tenant`.
    #[must_use]
    pub fn profile_types(&self, tenant: &str) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|e| e.profile_types.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The `__profile_type__` index: fingerprints carrying `profile_type`.
    #[must_use]
    pub fn fingerprints_for_profile_type(
        &self,
        tenant: &str,
        profile_type: &str,
    ) -> BTreeSet<SeriesFingerprint> {
        self.extras
            .get(tenant)
            .and_then(|e| e.profile_types.get(profile_type))
            .cloned()
            .unwrap_or_default()
    }

    /// Record which stacktrace-partition ids a written block's symbol DB carries.
    pub fn add_profile_block(&mut self, _tenant: &str, object_key: &str, partitions: Vec<u64>) {
        self.block_partitions
            .insert(object_key.to_string(), partitions);
    }

    /// The stacktrace-partition map: which partitions a block's symbol DB resolves.
    #[must_use]
    pub fn stacktrace_partitions(&self, object_key: &str) -> Vec<u64> {
        self.block_partitions
            .get(object_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Persist the index as a snapshot to object storage.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        store
            .put(&Path::from(key), object_store::PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    /// Load an index snapshot from object storage.
    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<ProfileIndex> {
        let bytes = store.get(&Path::from(key)).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl BlockIndex for ProfileIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        // Reuse the embedded `SeriesIndex` block list for per-block time-range +
        // the trait-level `candidate_blocks` prefilter. The profile-specific
        // partition map is registered separately via `add_profile_block`.
        BlockIndex::add_block(&mut self.series, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        BlockIndex::candidate_blocks(&self.series, tenant, min_ts, max_ts)
    }

    fn block_count(&self, tenant: &str) -> usize {
        BlockIndex::block_count(&self.series, tenant)
    }
}
```

> **Reuse check:** every `add_series`/`resolve`/`add_block`/`candidate_blocks`/`block_count` call forwards to the embedded `SeriesIndex` — no postings logic is re-implemented. If `SeriesIndex` exposes `label_names`/`label_values`, add thin forwarding methods here too (slice 5's querier needs them); they are one-liners delegating to `self.series`. The `SeriesFingerprint` decimal-string vs raw `u64` choice: the spec's contract said "decimal strings" but the actual `SeriesIndex` already keys postings by `SeriesFingerprint`, so we store `BTreeSet<SeriesFingerprint>` directly (simpler, type-safe) — flagged as a deviation from the prose in the Self-review.

- [ ] **Step 4: Wire `lib.rs`**

Add `mod profile_index;` and `pub use profile_index::{LABEL_PROFILE_TYPE, ProfileIndex};`. Confirm `BlockStore<ProfileIndex>` type-checks (the facade is generic over `BlockIndex`; slice 5's querier adds a `BlockStore<ProfileIndex>` inherent `impl` with a profile-aware scan method).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib profile_index`
Expected: PASS (4 tests).

- [ ] **Step 6: Snapshot round-trip test + `SeriesIndex`/`TraceIndex` regression gate**

Append to the `tests` module in `profile_index.rs`:

```rust
    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        let mut idx = seed();
        idx.add_profile_block("t", "blocks/p1.parquet", vec![0, 1]);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/profiles.json").await.unwrap();
        let loaded = ProfileIndex::load(&store, "index/profiles.json").await.unwrap();
        assert!(loaded.profile_types("t").len() == 2);
        assert!(loaded.stacktrace_partitions("blocks/p1.parquet") == vec![0, 1]);
    }
```

Then run the FULL existing blockstore suite to prove the embed didn't regress `SeriesIndex`/`TraceIndex`:

Run: `cargo test -p crabka-blockstore`
Expected: PASS — all pre-existing `SeriesIndex`/`TraceIndex`/`store`/`block` tests green + the new `profile_index` tests. **No behavior change to the other indexes.**

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): ProfileIndex (embeds SeriesIndex) + __profile_type__ index + stacktrace-partition map"
```

---

### Task 3: Profile-samples row builder — `ProfileSampleRow` → `RecordBatch` matching `profile_samples_schema()`

**Files:**
- Create: `crates/blockstore/src/profile_block.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: every `PCOL_*` constant, `profile_samples_schema`, `COL_FINGERPRINT`, `COL_TIMESTAMP`, `Result`, `BlockStoreError`.
- Produces:
  - `pub struct ProfileSampleRow { pub series_fingerprint: u64, pub timestamp: i64, pub profile_type: String, pub stacktrace_id: u64, pub value: i64, pub stacktrace_partition: u64, pub total_value: i64, pub span_id: Option<u64>, pub trace_id: Option<Vec<u8>> }` (`Clone`, `Debug`, `PartialEq`).
  - `pub fn encode_profile_samples(rows: &[ProfileSampleRow]) -> Result<arrow::record_batch::RecordBatch>` — builds a `RecordBatch` matching `profile_samples_schema()` (the `profile_type` column via a `StringDictionaryBuilder<Int32Type>`).

- [ ] **Step 1: Write the failing test**

Create `crates/blockstore/src/profile_block.rs`:

```rust
#[cfg(test)]
mod tests {
    use arrow::array::{BinaryArray, Int64Array, UInt64Array};
    use assert2::assert;

    use super::*;
    use crate::profile_schema::{
        PCOL_STACKTRACE_ID, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_schema,
    };

    fn row(fp: u64, ts: i64, stack: u64, value: i64, trace: Option<Vec<u8>>) -> ProfileSampleRow {
        ProfileSampleRow {
            series_fingerprint: fp,
            timestamp: ts,
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            stacktrace_id: stack,
            value,
            stacktrace_partition: 0,
            total_value: 1_000,
            span_id: None,
            trace_id: trace,
        }
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![
            row(1, 100, 7, 50, Some(vec![0xAB; 16])),
            row(1, 100, 9, 30, None),
        ];
        let batch = encode_profile_samples(&rows).unwrap();
        assert!(batch.schema() == profile_samples_schema());
        assert!(batch.num_rows() == 2);

        let stacks = batch
            .column_by_name(PCOL_STACKTRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(stacks.value(0) == 7 && stacks.value(1) == 9);

        let values = batch
            .column_by_name(PCOL_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.value(0) == 50);

        let traces = batch
            .column_by_name(PCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(traces.value(0) == [0xAB; 16].as_slice());
        assert!(traces.is_null(1));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib profile_block`
Expected: FAIL — `cannot find type ProfileSampleRow`.

- [ ] **Step 3: Implement `profile_block.rs`**

Prepend above the `tests` module. The fiddly column is `profile_type`'s `Dictionary<Int32,Utf8>` via `StringDictionaryBuilder<Int32Type>`; the rest are scalar builders.

```rust
//! Builds profile-samples `RecordBatch`es (one row per SAMPLE) from in-memory
//! [`ProfileSampleRow`]s, matching [`crate::profile_samples_schema`]. The
//! `profile_type` column is dict-encoded; the `(stacktrace_partition,
//! stacktrace_id)` slot is stored RAW (symbolized only at query time).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Int64Builder, StringDictionaryBuilder, UInt64Builder,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;

use crate::error::{BlockStoreError, Result};
use crate::profile_schema::profile_samples_schema;

/// One profile sample to encode (a flattened pprof sample).
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileSampleRow {
    pub series_fingerprint: u64,
    pub timestamp: i64,
    pub profile_type: String,
    pub stacktrace_id: u64,
    pub value: i64,
    pub stacktrace_partition: u64,
    pub total_value: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// Encode `rows` into a `RecordBatch` matching [`profile_samples_schema`].
pub fn encode_profile_samples(rows: &[ProfileSampleRow]) -> Result<RecordBatch> {
    let mut fp = UInt64Builder::new();
    let mut ts = Int64Builder::new();
    let mut profile_type = StringDictionaryBuilder::<Int32Type>::new();
    let mut stacktrace_id = UInt64Builder::new();
    let mut value = Int64Builder::new();
    let mut partition = UInt64Builder::new();
    let mut total_value = Int64Builder::new();
    let mut span_id = UInt64Builder::new();
    let mut trace_id = BinaryBuilder::new();

    for r in rows {
        fp.append_value(r.series_fingerprint);
        ts.append_value(r.timestamp);
        profile_type
            .append(&r.profile_type)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        stacktrace_id.append_value(r.stacktrace_id);
        value.append_value(r.value);
        partition.append_value(r.stacktrace_partition);
        total_value.append_value(r.total_value);
        match r.span_id {
            Some(s) => span_id.append_value(s),
            None => span_id.append_null(),
        }
        match &r.trace_id {
            Some(t) => trace_id.append_value(t),
            None => trace_id.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fp.finish()),
        Arc::new(ts.finish()),
        Arc::new(profile_type.finish()),
        Arc::new(stacktrace_id.finish()),
        Arc::new(value.finish()),
        Arc::new(partition.finish()),
        Arc::new(total_value.finish()),
        Arc::new(span_id.finish()),
        Arc::new(trace_id.finish()),
    ];

    RecordBatch::try_new(profile_samples_schema(), columns)
        .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))
}
```

> **Arrow-builder verification (do this if compile/test fails — verify against arrow 59):** `StringDictionaryBuilder::<Int32Type>::append(&str) -> Result<i32, ArrowError>` and `.finish()` producing a `DictionaryArray<Int32Type>` whose `DataType` is `Dictionary<Int32,Utf8>`; `BinaryBuilder::append_value(impl AsRef<[u8]>)` / `append_null()`. If a builder method name or the produced dictionary `DataType` differs, align the Task-1 schema/declaration AND this builder so they agree — the `batch.schema() == profile_samples_schema()` assertion is the tripwire that catches drift. The dict-key type (`Int32` here) must match Task 1's `profile_type_dict()`.

- [ ] **Step 4: Wire `lib.rs`**

Add `mod profile_block;` and `pub use profile_block::{ProfileSampleRow, encode_profile_samples};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib profile_block`
Expected: PASS.

- [ ] **Step 6: Round-trip integration test — write a samples block through `BlockWriter`, read it back**

Create `crates/blockstore/tests/profile_block_roundtrip.rs`:

```rust
//! A profile-samples block written through `BlockWriter` (validated against the
//! profile declaration) reads back with the same row count and stacktrace ids.

use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockWriter, ProfileSampleRow, encode_profile_samples, profile_samples_decl,
    profile_samples_schema, read_block, validate_against,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn row(fp: u64, stack: u64, value: i64) -> ProfileSampleRow {
    ProfileSampleRow {
        series_fingerprint: fp,
        timestamp: 1_000,
        profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
        stacktrace_id: stack,
        value,
        stacktrace_partition: 0,
        total_value: 500,
        span_id: None,
        trace_id: None,
    }
}

#[tokio::test]
async fn profile_block_validates_and_round_trips() {
    let batch = encode_profile_samples(&[row(1, 7, 50), row(1, 9, 30)]).unwrap();
    // The samples block satisfies its own declaration.
    validate_against(&batch.schema(), &profile_samples_decl()).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    BlockWriter::new(store.clone())
        .write_block(
            "tenant",
            "blocks/profiles.parquet",
            profile_samples_schema(),
            &[batch],
        )
        .await
        .unwrap();

    let back = read_block(store, "blocks/profiles.parquet").await.unwrap();
    let total: usize = back.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
    let stacks = back[0]
        .column_by_name("stacktrace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(stacks.value(0), 7);
}
```

> **`BlockWriter::write_block` declaration-awareness (shared with the traces slice):** the blockstore plan's `write_block` summary-scans the series `series_fingerprint`/`timestamp` columns. Profile-samples blocks HAVE both mandatory columns (unlike span blocks), so `write_block` works unchanged for the samples table — the summary `fingerprints` are the distinct `series_fingerprint`s and the time bounds are the `timestamp` min/max. No `write_block_with_decl` variant is needed for profiles; if the traces slice already added one, profiles uses the plain `write_block`. Pinned by this round-trip test.

- [ ] **Step 7: Run the round-trip test**

Run: `cargo test -p crabka-blockstore --test profile_block_roundtrip`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): profile-samples row builder (dict-encoded profile_type) + write/read round-trip"
```

---

### Task 4: `crabka-pprof` crate scaffold + `ProfileError` + `Frame` + `SymbolSource`

**Files:**
- Create: `crates/pprof/Cargo.toml`
- Create: `crates/pprof/src/lib.rs`
- Create: `crates/pprof/src/error.rs`
- Create: `crates/pprof/src/frame.rs`
- Modify: root `Cargo.toml` (add `crates/pprof` to `members`)

**Interfaces:**
- Produces:
  - A compiling `crabka-pprof` crate.
  - `pub enum ProfileError { Decode(String), Plan(String), Exec(String), Store(String), Unsupported(String), Symbolize(String) }` (`thiserror`, `Debug`) — the FULL variant set, frozen here; slices 2–7 add no variants.
  - `pub struct Frame { pub function: String, pub file: String, pub line: i32 }` (`Clone`, `Debug`, `PartialEq`, `Eq`) — a resolved stack frame.
  - `pub trait SymbolSource: Send + Sync { fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>; }` — implemented by `SymbolDb` (Task 5) and by the slice-7 symbolizer wrapper.

- [ ] **Step 1: Create `crates/pprof/Cargo.toml`**

```toml
[package]
name = "crabka-pprof"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Language-less continuous-profiling engine (Grafana-Pyroscope-equivalent) for Crabka — symbol DB + flamegraph merge"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pprof"
readme = "README.md"
keywords = ["observability", "pyroscope", "profiling", "flamegraph", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
serde = { workspace = true }
serde-wincode = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
```

- [ ] **Step 2: Write the failing test for `frame.rs` + `error.rs`**

Create `crates/pprof/src/frame.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    struct Fixed(Vec<Frame>);
    impl SymbolSource for Fixed {
        fn resolve(&self, _partition: u64, _id: u32) -> Vec<Frame> {
            self.0.clone()
        }
    }

    #[test]
    fn symbol_source_is_object_safe_and_returns_frames() {
        let src: Box<dyn SymbolSource> = Box::new(Fixed(vec![Frame {
            function: "main".into(),
            file: "main.go".into(),
            line: 10,
        }]));
        let frames = src.resolve(0, 1);
        assert!(frames.len() == 1);
        assert!(frames[0].function == "main");
    }
}
```

Create `crates/pprof/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn error_display_includes_message() {
        let e = ProfileError::Decode("bad pprof".into());
        assert!(format!("{e}").contains("bad pprof"));
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-pprof`
Expected: FAIL — `cannot find type Frame` / `cannot find type ProfileError` (and the crate may not yet be a workspace member — Step 4 fixes that).

- [ ] **Step 4: Implement `error.rs`, `frame.rs`, `lib.rs`, and register the crate**

`crates/pprof/src/error.rs` (above its `tests`):

```rust
//! The profiles engine error type. The FULL variant set is frozen in this slice;
//! slices 2-7 add no variants (they only construct existing ones).

/// Errors across the profiles engine, store, and symbolization paths.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    /// pprof / OTLP / symbol-DB artifact decode failure.
    #[error("decode: {0}")]
    Decode(String),
    /// Query planning failure (selector parse, profile-type lookup).
    #[error("plan: {0}")]
    Plan(String),
    /// Query execution failure (DataFusion fold, scan).
    #[error("exec: {0}")]
    Exec(String),
    /// Storage / object-store / index failure.
    #[error("store: {0}")]
    Store(String),
    /// An unsupported request shape (e.g. an unknown ingest format).
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// Symbolization failure (debuginfod / DWARF / ELF — slice 7).
    #[error("symbolize: {0}")]
    Symbolize(String),
}
```

`crates/pprof/src/frame.rs` (above its `tests`):

```rust
//! A resolved stack frame and the `SymbolSource` resolution boundary. Both the
//! in-block `SymbolDb` and the slice-7 query-time symbolizer implement
//! `SymbolSource`, so the flamegraph engine resolves stacktrace ids uniformly.

/// One resolved frame in a stack: function name, source file, and line. Inlined
/// frames expand to multiple `Frame`s for a single location (innermost first).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: i32,
}

/// Resolves a raw `(partition, stacktrace_id)` slot into a leaf-first frame list.
/// `Send + Sync` so the querier can share an `Arc<dyn SymbolSource>` across tasks.
pub trait SymbolSource: Send + Sync {
    /// Resolve one stacktrace id within `partition` into frames, leaf-first, with
    /// inlined frames expanded. Returns empty if the id is unknown.
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;
}
```

`crates/pprof/src/lib.rs`:

```rust
//! Language-less continuous-profiling engine for Crabka (Grafana-Pyroscope
//! equivalent). Slice 1 (this code) lands only the symbol model: the `Frame`,
//! the `SymbolSource` boundary, the `SymbolDb` (parent-pointer stacktrace tree +
//! dedup tables), and the `ProfileError` enum. The pprof codec, `ProfileType`,
//! `ProfileStore`, and the flamegraph-merge engine arrive in slices 2-3.

mod error;
mod frame;
mod symbol_db;

pub use error::ProfileError;
pub use frame::{Frame, SymbolSource};
pub use symbol_db::SymbolDb;
```

> `mod symbol_db;` is declared now but `symbol_db.rs` is created in Task 5 — to keep this task compiling on its own, either land Tasks 4 and 5 together, or temporarily omit the `symbol_db` line here and add it in Task 5 Step 4. The plan dispatches Tasks 4+5 as one sequential pair (5 depends on 4's `Frame`/`SymbolSource`/`ProfileError`).

Register the crate in the root `Cargo.toml` `[workspace] members` list (add `"crates/pprof"`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-pprof` (with the `mod symbol_db;` line omitted until Task 5, or run after Task 5 lands)
Expected: PASS (`frame` + `error` tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/ Cargo.toml
git commit -m "feat(pprof): scaffold crabka-pprof — ProfileError, Frame, SymbolSource"
```

---

### Task 5: `SymbolDb` — parent-pointer stacktrace tree + dedup tables + `intern_stacktrace`/`resolve`/`encode`/`decode` (the headline)

**Files:**
- Create: `crates/pprof/src/symbol_db.rs`
- Modify: `crates/pprof/src/lib.rs` (ensure `mod symbol_db;` + `pub use symbol_db::SymbolDb;`)

**Interfaces:**
- Consumes: `Frame`, `SymbolSource`, `ProfileError`, `serde`, `serde-wincode`.
- Produces:
  - `pub struct SymbolDb` (`Default`, `Clone`, `Debug`, `serde::Serialize`, `serde::Deserialize`) — per-`u64`-partition parent-pointer stacktrace trees + dedup tables (`strings`/`functions`/`locations`/`mappings`), `strings[0] == ""`.
  - `pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32` — interns a stack (a list of *location* refs, leaf→root) into the partition's parent-pointer tree, returning the **leaf node index** as the `StacktraceId`. Identical stacks dedup to the same leaf (parent-pointer path sharing).
  - `pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame>` — climb parents from the leaf, collect `location_ref`s, expand each location's `lines[]` (inlined frames, innermost-first) into `Frame`s; leaf→root order overall.
  - location/function/mapping/string interners: `pub fn intern_string(&mut self, s: &str) -> u32`, `pub fn intern_function(&mut self, f: FunctionRec) -> u32`, `pub fn intern_location(&mut self, l: LocationRec) -> u32`, `pub fn intern_mapping(&mut self, m: MappingRec) -> u32` (each dedups; returns the table index).
  - record structs: `pub struct FunctionRec { pub name: u32, pub system_name: u32, pub filename: u32, pub start_line: i64 }`; `pub struct LineRec { pub function_id: u32, pub line: i32 }`; `pub struct LocationRec { pub address: u64, pub mapping_id: u32, pub lines: Vec<LineRec> }`; `pub struct MappingRec { pub memory_start: u64, pub memory_limit: u64, pub file_offset: u64, pub filename: u32, pub build_id: u32, pub has_functions: bool, pub has_filenames: bool, pub has_line_numbers: bool, pub has_inline_frames: bool }` (all `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash`, `Serialize`, `Deserialize`).
  - `pub fn encode(&self) -> Vec<u8>` / `pub fn decode(bytes: &[u8]) -> Result<SymbolDb, ProfileError>` — the `symbols.symdb`-equivalent artifact codec (serde + serde-wincode).
  - `impl SymbolSource for SymbolDb` (delegates to the inherent `resolve`).

- [ ] **Step 1: Write the failing tests (dedup + resolve + inline + round-trip)**

Create `crates/pprof/src/symbol_db.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::frame::SymbolSource;

    /// Build a SymbolDb with three single-line locations (func "a","b","c") so a
    /// stack of location refs resolves to those function names leaf-first.
    fn db_with_abc() -> (SymbolDb, [u32; 3]) {
        let mut db = SymbolDb::default();
        let mk = |db: &mut SymbolDb, name: &str| {
            let n = db.intern_string(name);
            let f = db.intern_function(FunctionRec {
                name: n,
                system_name: n,
                filename: db.intern_string(&format!("{name}.go")),
                start_line: 1,
            });
            db.intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec { function_id: f, line: 10 }],
            })
        };
        let a = mk(&mut db, "a");
        let b = mk(&mut db, "b");
        let c = mk(&mut db, "c");
        (db, [a, b, c])
    }

    #[test]
    fn string_zero_is_empty() {
        let db = SymbolDb::default();
        assert!(db.string(0) == "");
    }

    #[test]
    fn identical_stacks_dedup_to_same_leaf() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id1 = db.intern_stacktrace(0, &[a, b, c]);
        let id2 = db.intern_stacktrace(0, &[a, b, c]);
        assert!(id1 == id2); // path-shared, same leaf node index
    }

    #[test]
    fn divergent_stacks_get_distinct_leaves_but_share_prefix() {
        let (mut db, [a, b, c]) = db_with_abc();
        let abc = db.intern_stacktrace(0, &[a, b, c]);
        let abd = db.intern_stacktrace(0, &[a, b]); // a prefix of abc's root path
        assert!(abc != abd);
        // Distinct partitions never share nodes.
        let other = db.intern_stacktrace(1, &[a, b, c]);
        assert!(db.resolve(1, other).len() == 3);
    }

    #[test]
    fn resolve_climbs_leaf_to_root() {
        let (mut db, [a, b, c]) = db_with_abc();
        // Stack passed leaf->root as [a,b,c]: leaf a, then b, then root c.
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|f| f.function.as_str()).collect();
        assert!(names == vec!["a", "b", "c"]);
    }

    #[test]
    fn resolve_expands_inlined_frames_innermost_first() {
        let mut db = SymbolDb::default();
        let outer = db.intern_string("outer");
        let inner = db.intern_string("inner");
        let fo = db.intern_function(FunctionRec {
            name: outer,
            system_name: outer,
            filename: 0,
            start_line: 1,
        });
        let fi = db.intern_function(FunctionRec {
            name: inner,
            system_name: inner,
            filename: 0,
            start_line: 1,
        });
        // One location with two lines: inlined `inner` inside `outer`
        // (pprof Line[] is innermost-first).
        let loc = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![
                LineRec { function_id: fi, line: 5 },
                LineRec { function_id: fo, line: 9 },
            ],
        });
        let id = db.intern_stacktrace(0, &[loc]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|f| f.function.as_str()).collect();
        assert!(names == vec!["inner", "outer"]); // innermost-first
    }

    #[test]
    fn encode_decode_round_trips() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let bytes = db.encode();
        let back = SymbolDb::decode(&bytes).unwrap();
        assert!(back.resolve(0, id) == db.resolve(0, id));
    }

    #[test]
    fn symbol_source_impl_delegates_to_resolve() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let src: &dyn SymbolSource = &db;
        assert!(src.resolve(0, id) == db.resolve(0, id));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib symbol_db`
Expected: FAIL — `cannot find type SymbolDb`.

- [ ] **Step 3: Implement `symbol_db.rs`**

Prepend above the `tests` module. The parent-pointer tree per partition is a `Vec<TreeNode>` where `TreeNode { parent: i32, location_ref: i32 }`; the `stacktrace_id` is the leaf node's index. Interning a stack walks the tree from the roots, following or creating a child whose `location_ref` matches each step, so identical stacks share the same path (the dedup lever). A `children` map keyed by `(parent_node, location_ref)` makes interning O(depth).

```rust
//! The on-block symbol-DB artifact (the `symbols.symdb`-equivalent). Per `u64`
//! partition: a PARENT-POINTER stacktrace tree (`node { parent: i32, location_ref:
//! i32 }`, `stacktrace_id = leaf node index`) plus dedup tables
//! (strings/functions/locations/mappings, `strings[0] == ""`). Interning a stack
//! shares the path with identical stacks — the ~60%-of-block-size dedup lever.
//! Resolving climbs parents from the leaf, expanding each location's inlined
//! `lines[]` (innermost-first) into `Frame`s.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::frame::{Frame, SymbolSource};

/// One inlined line within a location: a function ref + source line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineRec {
    pub function_id: u32,
    pub line: i32,
}

/// A program location: an address in a mapping + its (possibly inlined) lines,
/// innermost-first.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocationRec {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<LineRec>,
}

/// A function (string fields are indices into `strings`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionRec {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

/// A binary mapping (`has_functions == false` marks an unsymbolized mapping to be
/// resolved at query time — slice 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingRec {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub has_functions: bool,
    pub has_filenames: bool,
    pub has_line_numbers: bool,
    pub has_inline_frames: bool,
}

/// One parent-pointer tree node. `parent == -1` marks a root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TreeNode {
    parent: i32,
    location_ref: i32,
}

/// One partition's stacktrace tree. The `children` map (transient — rebuilt on
/// decode) makes interning O(depth); only `nodes` is serialized.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct Partition {
    nodes: Vec<TreeNode>,
    /// `(parent_idx, location_ref) -> child node idx`. Skipped on the wire;
    /// rebuilt from `nodes` after decode.
    #[serde(skip)]
    children: HashMap<(i32, i32), u32>,
}

impl Partition {
    fn rebuild_children(&mut self) {
        self.children.clear();
        for (idx, n) in self.nodes.iter().enumerate() {
            self.children
                .insert((n.parent, n.location_ref), u32::try_from(idx).expect("node idx"));
        }
    }
}

/// The per-block deduplicated symbol DB.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SymbolDb {
    strings: Vec<String>,
    #[serde(skip)]
    string_index: HashMap<String, u32>,
    functions: Vec<FunctionRec>,
    #[serde(skip)]
    function_index: HashMap<FunctionRec, u32>,
    locations: Vec<LocationRec>,
    #[serde(skip)]
    location_index: HashMap<LocationRec, u32>,
    mappings: Vec<MappingRec>,
    #[serde(skip)]
    mapping_index: HashMap<MappingRec, u32>,
    partitions: HashMap<u64, Partition>,
}

impl SymbolDb {
    /// A fresh symbol DB with `strings[0] == ""`.
    #[must_use]
    pub fn new() -> Self {
        let mut db = Self::default();
        db.strings.push(String::new());
        db.string_index.insert(String::new(), 0);
        db
    }

    fn ensure_init(&mut self) {
        if self.strings.is_empty() {
            self.strings.push(String::new());
            self.string_index.insert(String::new(), 0);
        }
    }

    /// Intern `s`, returning its `strings` index (`""` is always 0).
    pub fn intern_string(&mut self, s: &str) -> u32 {
        self.ensure_init();
        if let Some(&i) = self.string_index.get(s) {
            return i;
        }
        let i = u32::try_from(self.strings.len()).expect("string table overflow");
        self.strings.push(s.to_string());
        self.string_index.insert(s.to_string(), i);
        i
    }

    /// Read a string by index (`""` for 0 / out-of-range-safe).
    #[must_use]
    pub fn string(&self, i: u32) -> &str {
        self.strings.get(i as usize).map_or("", String::as_str)
    }

    /// Intern a function record (dedup), returning its table index.
    pub fn intern_function(&mut self, f: FunctionRec) -> u32 {
        if let Some(&i) = self.function_index.get(&f) {
            return i;
        }
        let i = u32::try_from(self.functions.len()).expect("function table overflow");
        self.functions.push(f);
        self.function_index.insert(f, i);
        i
    }

    /// Intern a location record (dedup), returning its table index.
    pub fn intern_location(&mut self, l: LocationRec) -> u32 {
        if let Some(&i) = self.location_index.get(&l) {
            return i;
        }
        let i = u32::try_from(self.locations.len()).expect("location table overflow");
        self.locations.push(l.clone());
        self.location_index.insert(l, i);
        i
    }

    /// Intern a mapping record (dedup), returning its table index.
    pub fn intern_mapping(&mut self, m: MappingRec) -> u32 {
        if let Some(&i) = self.mapping_index.get(&m) {
            return i;
        }
        let i = u32::try_from(self.mappings.len()).expect("mapping table overflow");
        self.mappings.push(m);
        self.mapping_index.insert(m, i);
        i
    }

    /// Intern a stack (LOCATION refs, leaf->root) into `partition`'s parent-pointer
    /// tree, returning the LEAF node index as the `StacktraceId`. Identical stacks
    /// share the path and dedup to the same leaf.
    pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32 {
        let part = self.partitions.entry(partition).or_default();
        // Walk root->leaf, i.e. reverse the leaf->root input.
        let mut parent: i32 = -1;
        for &loc in location_refs.iter().rev() {
            let loc_ref = i32::try_from(loc).expect("location ref fits i32");
            let key = (parent, loc_ref);
            if let Some(&child) = part.children.get(&key) {
                parent = i32::try_from(child).expect("node idx fits i32");
            } else {
                let idx = u32::try_from(part.nodes.len()).expect("node table overflow");
                part.nodes.push(TreeNode { parent, location_ref: loc_ref });
                part.children.insert(key, idx);
                parent = i32::try_from(idx).expect("node idx fits i32");
            }
        }
        // `parent` now holds the leaf node index (the deepest = the input's leaf).
        u32::try_from(parent.max(0)).expect("leaf node idx")
    }

    /// Resolve `stacktrace_id` in `partition` into frames, leaf-first, with inlined
    /// frames expanded (each location's `lines[]` innermost-first).
    #[must_use]
    pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame> {
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut cur = i32::try_from(stacktrace_id).unwrap_or(-1);
        while cur >= 0 {
            let Some(node) = part.nodes.get(cur as usize) else {
                break;
            };
            if let Some(loc) = self.locations.get(node.location_ref as usize) {
                for line in &loc.lines {
                    let func = self.functions.get(line.function_id as usize);
                    out.push(Frame {
                        function: func.map_or("", |f| self.string(f.name)).to_string(),
                        file: func.map_or("", |f| self.string(f.filename)).to_string(),
                        line: line.line,
                    });
                }
            }
            cur = node.parent;
        }
        out
    }

    /// Encode the symbol DB to the `symbols.symdb`-equivalent artifact bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_wincode::to_vec(self).expect("SymbolDb serializes")
    }

    /// Decode a symbol-DB artifact, rebuilding the transient dedup/children maps.
    pub fn decode(bytes: &[u8]) -> Result<SymbolDb, ProfileError> {
        let mut db: SymbolDb =
            serde_wincode::from_slice(bytes).map_err(|e| ProfileError::Decode(e.to_string()))?;
        db.rebuild_indexes();
        Ok(db)
    }

    /// Rebuild the `#[serde(skip)]` dedup/children maps after decode.
    fn rebuild_indexes(&mut self) {
        self.string_index = self
            .strings
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), u32::try_from(i).expect("idx")))
            .collect();
        self.function_index = self
            .functions
            .iter()
            .enumerate()
            .map(|(i, f)| (*f, u32::try_from(i).expect("idx")))
            .collect();
        self.location_index = self
            .locations
            .iter()
            .enumerate()
            .map(|(i, l)| (l.clone(), u32::try_from(i).expect("idx")))
            .collect();
        self.mapping_index = self
            .mappings
            .iter()
            .enumerate()
            .map(|(i, m)| (*m, u32::try_from(i).expect("idx")))
            .collect();
        for part in self.partitions.values_mut() {
            part.rebuild_children();
        }
    }
}

impl SymbolSource for SymbolDb {
    fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame> {
        SymbolDb::resolve(self, partition, stacktrace_id)
    }
}
```

> **`serde-wincode` API note (verify against `serde-wincode` 0.1):** the encode/decode calls assume `serde_wincode::to_vec(&T) -> Result<Vec<u8>, _>` and `serde_wincode::from_slice::<T>(&[u8]) -> Result<T, _>`. If the 0.1 surface names these differently (e.g. `encode_to_vec` / `decode_from_slice`, or requires a config), align the two call sites — the `encode_decode_round_trips` test is the tripwire. The crate is the workspace convention for WAL/artifact codecs (root `Cargo.toml` pins `serde-wincode = "0.1"`); check an existing user in the repo (e.g. a `serde-wincode` call site under `crates/`) for the exact function names before guessing.

> **Default vs new note:** `#[derive(Default)]` gives an empty `strings` vec, so `string(0)` returns `""` (out-of-range-safe) and the first `intern_string` call's `ensure_init` seeds `strings[0] == ""`. `SymbolDb::new()` seeds it eagerly. Tests use `default()`; the `string_zero_is_empty` test passes because `string(0)` is out-of-range-safe on an empty table. The block-builder (slice 4) should call `SymbolDb::new()` so `strings[0] == ""` is materialized before interning real strings.

- [ ] **Step 4: Wire `lib.rs`**

Ensure `mod symbol_db;` and `pub use symbol_db::SymbolDb;` are present, plus re-export the record types: `pub use symbol_db::{FunctionRec, LineRec, LocationRec, MappingRec};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib symbol_db`
Expected: PASS (7 tests).

- [ ] **Step 6: Property test — random distinct stacks round-trip and dedup correctly (the headline)**

Create `crates/pprof/tests/symbol_db_proptest.rs`:

```rust
//! Property: for any set of random stacks interned into a `SymbolDb`, (a) each
//! distinct stack resolves back to its exact frame list leaf->root, (b) interning
//! the same stack twice yields the same id (dedup), and (c) encode/decode
//! preserves every resolution. This pins the ~60%-dedup lever.

use std::collections::HashMap;

use crabka_pprof::{FunctionRec, LineRec, LocationRec, SymbolDb};
use proptest::prelude::*;

/// A stack is a Vec of function-name indices (0..8); we build one single-line
/// location per distinct function and intern stacks of location refs.
fn arb_stacks() -> impl Strategy<Value = Vec<Vec<u8>>> {
    proptest::collection::vec(
        proptest::collection::vec(0u8..8, 1..6), // each stack: 1..6 frames
        1..20,                                   // 1..20 stacks
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn stacks_dedup_resolve_and_round_trip(stacks in arb_stacks()) {
        let mut db = SymbolDb::new();
        // One location per function name 0..8.
        let mut loc_of: HashMap<u8, u32> = HashMap::new();
        for f in 0u8..8 {
            let name = db.intern_string(&format!("fn{f}"));
            let func = db.intern_function(FunctionRec {
                name,
                system_name: name,
                filename: 0,
                start_line: 1,
            });
            let loc = db.intern_location(LocationRec {
                address: u64::from(f),
                mapping_id: 0,
                lines: vec![LineRec { function_id: func, line: 1 }],
            });
            loc_of.insert(f, loc);
        }

        // Intern each stack (leaf->root = as given), record id + expected names.
        let mut ids: Vec<(u32, Vec<String>)> = Vec::new();
        for stack in &stacks {
            let refs: Vec<u32> = stack.iter().map(|f| loc_of[f]).collect();
            let id = db.intern_stacktrace(0, &refs);
            let expected: Vec<String> = stack.iter().map(|f| format!("fn{f}")).collect();
            ids.push((id, expected));
        }

        // (a)+(b): resolve matches, and re-interning is stable (same id).
        for (stack, (id, expected)) in stacks.iter().zip(&ids) {
            let refs: Vec<u32> = stack.iter().map(|f| loc_of[f]).collect();
            prop_assert_eq!(db.intern_stacktrace(0, &refs), *id);
            let got: Vec<String> = db.resolve(0, *id).into_iter().map(|f| f.function).collect();
            prop_assert_eq!(&got, expected);
        }

        // (c): encode/decode preserves every resolution.
        let back = SymbolDb::decode(&db.encode()).unwrap();
        for (id, expected) in &ids {
            let got: Vec<String> = back.resolve(0, *id).into_iter().map(|f| f.function).collect();
            prop_assert_eq!(&got, expected);
        }
    }
}
```

- [ ] **Step 7: Run the property test**

Run: `cargo test -p crabka-pprof --test symbol_db_proptest`
Expected: PASS (256 cases).

- [ ] **Step 8: Final whole-crate gate (both crates)**

Run: `cargo test -p crabka-pprof && cargo test -p crabka-blockstore && cargo clippy -p crabka-pprof -p crabka-blockstore --all-targets && cargo fmt -p crabka-pprof --check && cargo fmt -p crabka-blockstore --check`
Expected: all PASS, no clippy warnings, formatting clean.

- [ ] **Step 9: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): SymbolDb — parent-pointer stacktrace tree + dedup tables + intern/resolve/encode/decode + dedup property test"
```

---

## Self-review

**Spec coverage (against §3.3 crate layout + §4 data model + §11 Slice 1):**
- `ProfileIndex` (`impl BlockIndex`) = label-series postings **reusing the metrics `SeriesIndex`** (embedded, not forked; regression-tested by the full blockstore suite staying green) + `__profile_type__` index (`profile_types`/`fingerprints_for_profile_type`) + per-block time-range (the embedded `SeriesIndex` block list via the `BlockIndex` trait) + stacktrace-partition map (`add_profile_block`/`stacktrace_partitions`) → Task 2.
- The flattened **one-row-per-sample** samples fact-table column constants (`PCOL_*`) + `profile_samples_schema()` (`COL_FINGERPRINT`/`COL_TIMESTAMP` + `PCOL_PROFILE_TYPE` dict + `PCOL_STACKTRACE_ID`/`PCOL_VALUE`/`PCOL_STACKTRACE_PARTITION`/`PCOL_TOTAL_VALUE`/`PCOL_SPAN_ID`/`PCOL_TRACE_ID`) + `profile_samples_decl()` → Task 1; the row builder + `BlockWriter` round-trip → Task 3.
- The on-block **symbol-DB artifact** — per-partition parent-pointer stacktrace tree (`node { parent, location_ref }`, `stacktrace_id = leaf node index`, intern dedups via path-sharing, resolve climbs parents leaf→root with inlined-frame expansion) + dedup tables (strings/functions/locations/mappings, `strings[0] == ""`) + `encode`/`decode` + `SymbolSource` → Tasks 4, 5.
- The headline **stacktrace-tree dedup + resolve round-trip property test** → Task 5 Step 6.

**Deviations flagged (not hidden):**
1. **`__profile_type__` index value type.** The shared-contract prose said "series fingerprints, as decimal strings"; the impl stores `BTreeSet<SeriesFingerprint>` directly (the embedded `SeriesIndex` already keys by `SeriesFingerprint`) — type-safe and simpler, no behavior difference. Flagged so slice 5's querier expects `SeriesFingerprint`, not a string.
2. **`Frame`/`SymbolDb` live in `crabka-pprof` from slice 1, not `crabka-blockstore`.** The spec said the slice "starts `crabka-pprof` only for the `SymbolDb`/`Frame` types if shared" — they ARE shared (slice 2's engine + slice 4's block-builder both consume them), so they land in `crabka-pprof` now rather than in blockstore-then-moved (no-back-compat: no later move/migration). `crabka-blockstore` does NOT depend on `crabka-pprof` in this slice — the samples schema is pure Arrow; the block-builder (slice 4) is what wires the two together.
3. **`write_block` is reused unchanged for profiles** (unlike the traces slice, which needed a declaration-aware variant for span blocks lacking `series_fingerprint`/`timestamp`). Profile-samples blocks carry both mandatory columns, so the existing summary scan works. Pinned by the Task 3 round-trip.
4. **No DataFusion / Kafka / Connect-RPC in this slice.** The samples schema is plain Arrow; the symbol DB is pure compute. The fold-before-symbolize query (DataFusion `GROUP BY (stacktrace_partition, stacktrace_id) → SUM`), the `ProfileStore` UNION, and the Connect `querier.v1` API are slices 2/5 — correctly deferred. `crabka-pprof`'s `Cargo.toml` adds no `datafusion`/`prost`/`connectrpc-axum` deps yet; those arrive with the engine (slice 2) and the service (slice 4).

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every step has runnable code or an exact command. The bounded hand-waves: (a) the arrow-59 `StringDictionaryBuilder<Int32Type>` API (Task 3) — pinned by `batch.schema() == profile_samples_schema()` + a verify-against-arrow-59 note; (b) the `serde-wincode` 0.1 `to_vec`/`from_slice` function names (Task 5) — pinned by the `encode_decode_round_trips` test + a verify-against-the-repo-convention note (check an existing call site, don't guess); (c) the `Labels`/`LabelMatcher` import paths in the `ProfileIndex` tests (Task 2) — flagged to adjust to blockstore's actual module layout, behavior unchanged. None fabricates a signature whose behavior isn't test-pinned.

**Type consistency:** `PCOL_*` constants defined once (Task 1), referenced unchanged in Tasks 2/3. `ProfileSampleRow` field set identical between definition (Task 3) and the round-trip test. `SymbolDb` method set (`intern_string`/`intern_function`/`intern_location`/`intern_mapping`/`intern_stacktrace`/`resolve`/`string`/`encode`/`decode`) consistent between Task 5's definition, its unit tests, and the property test. `Frame { function, file, line }` and `SymbolSource::resolve(partition, stacktrace_id) -> Vec<Frame>` identical between Tasks 4 and 5 — and these are the exact signatures slice 2's engine and slice 7's symbolizer wrapper consume. `FunctionRec`/`LineRec`/`LocationRec`/`MappingRec` field sets are frozen here (slice 4's block-builder constructs them).

**Known risk (flagged, not hidden):** Task 5 (`SymbolDb`) genuinely depends on Task 4's `Frame`/`SymbolSource`/`ProfileError`, so 4+5 are a sequential pair (the `mod symbol_db;` line in `lib.rs` ties them — land them together or stage the `mod` line per the Task 4 Step 4 note). Tasks 1, 2, 3 all touch `crabka-blockstore` but DIFFERENT new files (`profile_schema.rs`, `profile_index.rs`, `profile_block.rs`) plus the shared `lib.rs` re-export block — they can be dispatched as a parallel batch *if* the `lib.rs` edits are coordinated (each adds one `mod` + one `pub use` line; the conflict is only the single `lib.rs` file, so either serialize the three `lib.rs` edits or have one agent own `lib.rs`). Task 2's embed of `SeriesIndex` is the one place a regression could hide — the full `cargo test -p crabka-blockstore` suite staying green (Task 2 Step 6) is the guard that the embed didn't perturb `SeriesIndex`/`TraceIndex`. The `crabka-pprof` tasks (4, 5) touch a disjoint crate and run fully parallel to the blockstore tasks.
