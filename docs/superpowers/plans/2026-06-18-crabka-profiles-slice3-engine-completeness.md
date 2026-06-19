# crabka-pprof Slice 3 — Engine completeness (`SelectSeries` + `Diff` + `max_nodes`/`"other"` + raw-pprof output + `SelectMergeSpanProfile`/`SelectHeatmap`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the `crabka-pprof` engine built in Slice 2 (the pprof model + codec, the `SymbolDb` parent-pointer stacktrace tree + dedup tables + `SymbolSource`, the `ProfileType` parser, the `ProfileStore` trait + the pinned result types, and the **MERGE → flamegraph** path — fold-before-symbolize, `Tree`, the 4-ints-per-bar `FlameGraph`) from "merge works" to "the engine is complete." This slice adds: **SelectSeries** (read the precomputed `PCOL_TOTAL_VALUE` per profile → DataFusion `GROUP BY group_by, floor(timestamp_ms / step_ms)` → `SUM`/`AVERAGE` → `Vec<Series>`, step in **seconds**); **`max_nodes` truncation** with a synthetic `"other"` node (a min-value heap threshold) made byte-exact in the `Tree::to_flamegraph` encoder; **Diff** (two independent MERGEs left/right, structurally aligned with zero-value placeholders so child sets match, encoded as a `FlameGraphDiff` whose levels are **groups of 7** + `left_ticks`/`right_ticks`); **SelectMergeProfile** (the merged raw **pprof bytes**, re-encoded via the Slice-2 `PprofProfile` codec); **SelectMergeSpanProfile** (a `span_selector` filtering MERGE on `PCOL_SPAN_ID`); and **SelectHeatmap** (a 2-D `(time-bucket × value-bucket)` count matrix). Plus the load-bearing distributed invariant: **cross-block partial-tree merge** — each block/scan resolves its raw ids **locally** to a partial symbolized `Tree`, and the engine merges partial trees (`Tree::merge`) — raw stacktrace ids never cross a block boundary.

**Architecture:** This slice is pure `crabka-pprof` extension — **no new crate, no networking, no proto codegen** (the Connect `querier.v1` wire surface is slice 5; this slice produces the engine result types those handlers project). It adds, on top of Slice 2's `ProfileStore`/`ProfileScan`/`FlameEngine`/`Tree`/`SymbolSource` substrate: (1) a **DataFusion time-bucketing aggregation** for `SelectSeries` over the `PCOL_TOTAL_VALUE` column (no re-fold, no symbolization — series are pure float aggregations keyed by `group_by` labels); (2) a **`max_nodes` truncation kernel** folded into `Tree::to_flamegraph` (a min-value heap threshold collapsing the pruned tail into one synthetic `"other"` node per surviving parent, conserving totals); (3) a **diff aligner + 7-ints-per-bar encoder** building one `FlameGraphDiff` from two `Tree`s by walking both trees in lockstep with zero-value placeholders; (4) a **raw-pprof re-encoder** that turns the merged `(frames, value)` set back into a `PprofProfile` and `encode()`s it; (5) a **span-scoped MERGE** that pushes a `PCOL_SPAN_ID` predicate into the same scan path; (6) a **heatmap binning kernel** over `(timestamp_ms, total_value)`.

The load-bearing realization: **everything in this slice is "more aggregations and more encoders" on top of Slice 2's scan/`Tree`/`FlameGraph` substrate.** No new storage seam and no new `ProfileStore` method are required — `SelectSeries` and `SelectHeatmap` are DataFusion aggregations over the `ProfileScan.samples_table` the Slice-2 `store.select()` already hands back; `Diff` and `max_nodes` are pure encoders over the `Tree` the merge path already builds; `SelectMergeProfile` re-uses the Slice-2 `PprofProfile` codec; `SelectMergeSpanProfile` is `select_merge_stacktraces` with one extra predicate. So the slice is dominated by per-method bite-sized TDD, each pinned by a hand-written unit test encoding the exact Pyroscope contract (the 4-/7-ints-per-bar encodings, step-in-seconds, SUM vs AVERAGE, zero-value diff alignment, `"other"`-conserves-total), before any DataFusion plumbing.

**Tech Stack:** Rust 2024 · `datafusion { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }` · `arrow` 59 · `prost` 0.14 (pprof messages, via the Slice-2 codec) · `async-trait` 0.1 · `tokio` (`macros`, `rt-multi-thread`) · `futures` 0.3 · `thiserror` 2. Consumes Slice 2's `crabka-pprof` surface (the engine, `ProfileStore`, `ProfileScan`, `Tree`, `FlameGraph`, `SymbolSource`, the result types) and — only transitively, through the injected `ProfileStore` — `crabka-blockstore`'s `LabelMatcher`/`MatchOp` and the `PCOL_*` column constants. Tests: `assert2`, `proptest`. The `InMemoryProfileStore` test double (frozen by Slice 2) backs every test so the engine is independently testable without ingest/blockstore/querier.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change signatures/enums/registry shapes freely; no shims, no migration code, no default-off feature flags. When a result type or encoder shape changes, change it in place.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-pprof --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-pprof` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. The engine API (`select_series`/`diff`/`select_merge_profile`/`select_merge_stacktraces`) is async; the `InMemoryProfileStore` test double backs every test so the engine is independently testable without ingest/blockstore.
- **DataFusion-internal API churn:** the `rev` is pinned. `SelectSeries`/`SelectHeatmap` are *ordinary* DataFusion `GROUP BY`/aggregation plans built through the `DataFrame`/`SessionContext`/SQL API on the `ProfileScan.samples_table` — **no** custom `UserDefinedLogicalNodeCore` or `ExecutionPlan` is introduced in this slice. Where a `DataFrame` aggregation method, `Expr` builder, or `ScalarValue` extraction signature is needed, give the **structure + behavior** and a behavior-pinning test, with a `// verify against rev 0838a4d` note rather than fabricating an exact upstream signature. The test (input rows → expected `Vec<Series>` / `FlameGraphDiff` / pprof) is the contract; the plumbing is whatever compiles against the pin.
- **Pyroscope-contract fidelity (Kafka-compat does not apply here; *Pyroscope-semantic/wire* compat does):** every encoder/aggregation whose semantics are subtle (the 4-ints-per-bar `xOffsetDelta` rule, the 7-ints-per-bar diff layout, `step` in SECONDS → `step_ms`, SUM vs AVERAGE, zero-value diff alignment, `max_nodes` `"other"` total-conservation, span-scoped filtering, partial-tree merge associativity) gets its exact rule **encoded in a unit test that cites the behavior**, *before* the wider integration test. When Pyroscope's behavior is undocumented or version-dependent, the Slice-8 differential-vs-Pyroscope run is the tiebreaker — flag such cases with a `// verify against Pyroscope <tag>` note rather than guessing silently.
- **Raw ids never cross a block boundary (the distributed invariant):** a `stacktrace_id` is only meaningful within its own block's `SymbolDb` partition. `SelectSeries` never symbolizes (it reads `PCOL_TOTAL_VALUE` floats only). Every path that *does* symbolize (`MERGE`, `Diff`, `SelectMergeProfile`, `SelectMergeSpanProfile`) resolves **per-`ProfileScan`/per-block** to a partial `Tree`/pprof, then merges the *symbolized* partials — never the raw ids. Pin this with a multi-scan merge test.

---

## Dependency & slice roadmap

**Depends on:** **Slice 2 (`crabka-pprof` core)** — this slice consumes its public + crate-internal surface verbatim:

- `pub struct FlameEngine<S: ProfileStore>` with `new(store: Arc<S>, opts: EngineOpts)` and the **`select_merge_stacktraces(tenant, profile_type, label_selector, start_ms, end_ms, max_nodes) -> Result<FlameGraph, ProfileError>`** method (the MERGE path this slice extends with `Diff`/`SelectSeries`/`SelectMergeProfile`/`SelectMergeSpanProfile`).
- `pub struct EngineOpts { pub default_max_nodes: i64 /* 2048 */ }`.
- `#[async_trait] pub trait ProfileStore: Send + Sync` with `select(tenant, profile_type, &[LabelMatcher], start_ms, end_ms) -> Result<ProfileScan, ProfileError>` (+ `label_names`/`label_values`/`profile_types`/`series`).
- `pub struct ProfileScan { pub ctx: datafusion::prelude::SessionContext, pub samples_table: String /* UNION hot+cold */, pub symbols: std::sync::Arc<dyn SymbolSource> }`.
- `pub struct Tree { /* parent/children, total:i64, self_:i64 */ }` with `add_stack(&[Frame], i64)`, `merge(other: Tree)`, `to_flamegraph(self, max_nodes: i64) -> FlameGraph` (**this slice makes the `max_nodes`/`"other"` truncation byte-exact**).
- `pub struct FlameGraph { pub names: Vec<String>, pub levels: Vec<Level>, pub total: i64, pub max_self: i64 }`; `pub struct Level { pub values: Vec<i64> }` (groups of 4: `[xOffsetDelta, total, self, nameIndex]`).
- `pub struct FlameGraphDiff { pub names: Vec<String>, pub levels: Vec<Level>, pub left_ticks: i64, pub right_ticks: i64 }` (levels groups of 7) — **this slice produces it.**
- `Frame { pub function: String, pub file: String, pub line: i32 }`; `pub trait SymbolSource: Send + Sync { fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>; }`.
- `pub struct PprofProfile` with `decode(&[u8]) -> Result<PprofProfile, ProfileError>` / `encode(&self) -> Vec<u8>` (the perftools.profiles wire model) — **`SelectMergeProfile` re-encodes through this.**
- `pub struct ProfileType { name, sample_type, sample_unit, period_type, period_unit }` with `parse(&str)` + `Display`.
- the **`label_selector` → `Vec<LabelMatcher>`** helper (Prometheus matcher-string parse, reusing blockstore `LabelMatcher`/`MatchOp`).
- `pub enum ProfileError { Decode, Plan, Exec, Store, Unsupported, Symbolize }`.
- The `InMemoryProfileStore` test double + the test helpers (`store_with_profiles`, `eval_merge`) the Slice-2 tests established.

The samples fact-table columns (defined by Slice 1, surfaced through `ProfileScan.samples_table`): `COL_FINGERPRINT`, `COL_TIMESTAMP`, `PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, **`PCOL_TOTAL_VALUE`**, **`PCOL_SPAN_ID`**, `PCOL_TRACE_ID`.

> **If a Slice-2 name differs at implementation time:** the *contract above is authoritative for planning*; if Slice 2 landed a renamed symbol (e.g. `Tree::self_` → `Tree::self_value`, or `Series.points` typed differently), adapt this slice's call sites to the real name — the *behavior* each task pins is what matters, not the spelling. Flag any rename in the task's commit message.

**The 8 profiles slices** (this plan = Slice 3; each gets its own plan):

1. Blockstore `ProfileIndex` + profile samples schema (`PCOL_*`) + symbol-DB artifact. *(slice 1 — `cargo test -p crabka-blockstore`)*
2. `crabka-pprof` core — pprof model + codec, `SymbolDb` + `SymbolSource`, `ProfileType`, `ProfileStore` + result types, MERGE → flamegraph (fold-before-symbolize, `Tree`, 4-ints-per-bar). *(slice 2 — `cargo test -p crabka-pprof`)*
3. **Engine completeness** *(this plan)* — `SelectSeries`, `Diff`, `max_nodes`/`"other"`, raw-pprof output, `SelectMergeSpanProfile`, `SelectHeatmap`, cross-block partial-tree merge.
4. Ingest service — distributor (`push.v1` + `/ingest` + OTLP `v1development`) → `(tenant, series_fingerprint)`-WAL; block-builder. *(slices 4–8 — `cargo test -p crabka-profiles`)*
5. Querier + Connect `querier.v1` API + legacy `/pyroscope/render`.
6. Query-frontend — split/shard + partial-tree merge + select-series shard-merge.
7. Native symbolization — debuginfod + DWARF/ELF/`.gopclntab`.
8. Hardening — per-tenant limits/multi-tenancy, compaction, differential-vs-Pyroscope + Grafana integration.

---

## File structure (`crates/pprof/` — extends Slice 2)

| File | Responsibility | New / extended |
|---|---|---|
| `src/engine.rs` | `FlameEngine` — add `select_series`/`diff`/`select_merge_profile`/`select_merge_span_profile`/`select_heatmap` methods + the cross-scan partial-tree merge helper | extended |
| `src/series.rs` | `Series`/`SeriesAgg` (types frozen in Slice 2) + the `SelectSeries` time-bucketing aggregation over `PCOL_TOTAL_VALUE` | extended |
| `src/tree.rs` | `Tree::to_flamegraph` `max_nodes` truncation + synthetic `"other"` kernel (4-ints-per-bar encoder) | extended |
| `src/diff.rs` | the diff aligner (zero-value placeholders) + the 7-ints-per-bar `FlameGraphDiff` encoder | **new** |
| `src/raw_profile.rs` | merged-`(frames,value)` → `PprofProfile` re-encoder (`SelectMergeProfile` body) | **new** |
| `src/heatmap.rs` | `Heatmap` result type + the `(time-bucket × value-bucket)` binning kernel | **new** |
| `src/lib.rs` | module decls + re-exports (`Series`, `SeriesAgg`, `Heatmap`, …) | extended |

---

## Phase A — `SelectSeries` (the no-symbolize float aggregation)

### Task 1: `Series`/`SeriesAgg` types + the pure step-bucketing kernel

**Files:**
- Modify: `crates/pprof/src/series.rs` (created in Slice 2 B3 with the frozen `Series`/`SeriesAgg` types; this task adds the kernel fns)
- Modify: `crates/pprof/src/lib.rs` (extend the existing re-exports if a kernel fn needs to be public)

**Interfaces:**
- Consumes: the Slice-2-frozen `Series`/`SeriesAgg` types (already in `series.rs`; do NOT redeclare them or re-`mod series;`). Nothing from DataFusion yet — this task pins the bucketing arithmetic in isolation.
- Produces (added to the existing `series.rs`):
  - (existing, frozen in Slice 2 — restated for reference) `pub struct Series { pub labels: Vec<(String, String)>, pub points: Vec<(i64, f64)> }` (`(timestamp_ms, value)`); `pub enum SeriesAgg { Sum, Average }` (`Copy`).
  - `pub fn step_bucket_ms(ts_ms: i64, step_ms: i64) -> i64` — the bucket **start** timestamp for `ts_ms`: `(ts_ms.div_euclid(step_ms)) * step_ms`. (Pyroscope's `SelectSeries` aligns points to step-floored epoch-ms boundaries; the emitted point timestamp is the bucket start.)
  - `pub fn step_ms_from_secs(step_secs: f64) -> Result<i64, ProfileError>` — `step` is a `float64` SECONDS on the wire; convert to integer ms (`(step_secs * 1000.0).round() as i64`), rejecting `step_secs <= 0` with `ProfileError::Plan`.
  - `pub fn fold_bucket(agg: SeriesAgg, values: &[i64]) -> f64` — SUM = `sum`; AVERAGE = `sum / len` (NaN-free: empty bucket is never folded — it produces no point).

- [ ] **Step 1: Write the failing kernel test** (encode the exact rules)

Append to the existing `crates/pprof/src/series.rs` (Slice 2 created it with the `Series`/`SeriesAgg` types) — add the kernel tests:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn step_secs_to_ms_rounds_and_rejects_nonpositive() {
        assert!(step_ms_from_secs(15.0).unwrap() == 15_000);
        assert!(step_ms_from_secs(0.5).unwrap() == 500);
        assert!(step_ms_from_secs(0.0).is_err());
        assert!(step_ms_from_secs(-1.0).is_err());
    }

    #[test]
    fn bucket_start_is_step_floored() {
        // step 15s = 15000ms. ts=17000 -> bucket start 15000; ts=15000 -> 15000; ts=14999 -> 0.
        assert!(step_bucket_ms(17_000, 15_000) == 15_000);
        assert!(step_bucket_ms(15_000, 15_000) == 15_000);
        assert!(step_bucket_ms(14_999, 15_000) == 0);
        // negative ts floors toward -inf (div_euclid), not toward zero.
        assert!(step_bucket_ms(-1, 15_000) == -15_000);
    }

    #[test]
    fn fold_sum_vs_average() {
        assert!(fold_bucket(SeriesAgg::Sum, &[2, 3, 5]) == 10.0);
        assert!((fold_bucket(SeriesAgg::Average, &[2, 3, 5]) - 10.0 / 3.0).abs() < 1e-12);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib series`
Expected: FAIL — `cannot find function step_ms_from_secs`.

- [ ] **Step 3: Implement the kernel**

The `Series`/`SeriesAgg` types already exist in `series.rs` (Slice 2 B3 froze them with `Series: Clone+Debug+PartialEq` and `SeriesAgg: Copy`). **Do not redeclare them.** Add only the kernel fns above `tests` (extend the file doc-comment):

```rust
//! `SelectSeries` support: the pure step-bucketing arithmetic over the
//! Slice-2-frozen `Series`/`SeriesAgg` types. `SelectSeries` reads the
//! *precomputed* `PCOL_TOTAL_VALUE` per profile (no re-fold, no symbolization)
//! and folds it into `(timestamp_ms, value)` points bucketed by a step given in
//! SECONDS on the wire. The DataFusion aggregation that drives it lives in
//! `engine.rs`.

use crate::error::ProfileError;

/// Step-floored bucket **start** timestamp (epoch-ms) for `ts_ms`. Uses
/// `div_euclid` so negative timestamps floor toward -infinity (the bucket a
/// sample belongs to never depends on sign).
#[must_use]
pub fn step_bucket_ms(ts_ms: i64, step_ms: i64) -> i64 {
    ts_ms.div_euclid(step_ms) * step_ms
}

/// Convert the wire `step` (a `float64` in SECONDS) to integer milliseconds,
/// rejecting non-positive steps.
pub fn step_ms_from_secs(step_secs: f64) -> Result<i64, ProfileError> {
    if !(step_secs.is_finite() && step_secs > 0.0) {
        return Err(ProfileError::Plan(format!(
            "step must be a positive finite number of seconds, got {step_secs}"
        )));
    }
    Ok((step_secs * 1000.0).round() as i64)
}

/// Fold one bucket's precomputed total values by the chosen aggregation.
#[must_use]
pub fn fold_bucket(agg: SeriesAgg, values: &[i64]) -> f64 {
    let sum: i64 = values.iter().sum();
    match agg {
        SeriesAgg::Sum => sum as f64,
        // Caller guarantees a non-empty bucket (empty buckets emit no point).
        SeriesAgg::Average => sum as f64 / values.len() as f64,
    }
}
```

- [ ] **Step 4: Wiring in `lib.rs`**

`mod series;` + `pub use series::{Series, SeriesAgg};` already exist (Slice 2 B3). No new `lib.rs` re-export is needed: the kernel fns stay crate-internal (`pub`/`pub(crate)` so `engine.rs` can call them across modules). Do **not** re-declare the module or re-export the types.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib series`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): Series/SeriesAgg + step-bucketing kernel (step in seconds)"
```

---

### Task 2: `FlameEngine::select_series` — DataFusion `GROUP BY group_by, step-bucket → SUM/AVERAGE`

**Files:**
- Modify: `crates/pprof/src/engine.rs` (add `select_series`)
- Modify: `crates/pprof/src/series.rs` (add a `pub(crate)` row-assembly helper if the DataFusion result is collected as rows)

**Interfaces:**
- Consumes: `ProfileStore::select` (→ `ProfileScan { ctx, samples_table, .. }`), the `label_selector → Vec<LabelMatcher>` helper, the `PCOL_TOTAL_VALUE`/`PCOL_PROFILE_TYPE`/`COL_TIMESTAMP` columns, `step_ms_from_secs`/`step_bucket_ms`/`fold_bucket`/`Series`/`SeriesAgg` (Task 1).
- Produces:
  - `pub async fn select_series(&self, tenant: &str, profile_type: &str, label_selector: &str, group_by: &[String], step_secs: f64, agg: SeriesAgg, start_ms: i64, end_ms: i64) -> Result<Vec<Series>, ProfileError>` — exactly the Slice-2 §6.5 `FlameEngine` signature.
  - **Semantics:** read `PCOL_TOTAL_VALUE` per profile (no fold over stacktraces, no symbolization); `GROUP BY` the `group_by` label columns **and** the step-floored bucket; aggregate by `SUM`/`AVERAGE`; one `Series` per distinct `group_by` label combination, its `points` sorted ascending by bucket-start timestamp. Empty `group_by` ⇒ one all-up `Series` with empty `labels`.

> **Why `PCOL_TOTAL_VALUE` not `PCOL_VALUE`:** the per-sample `PCOL_VALUE` would require summing every stacktrace row of a profile to get the profile's total; Slice 1 precomputed that per-profile total into `PCOL_TOTAL_VALUE` precisely so `SelectSeries` is a cheap column read, not a re-fold (spec §6.2). Reading `PCOL_VALUE` here would double-aggregate. Pin this in the test (a multi-stacktrace profile must contribute its `total_value` once per profile, not once per stacktrace row).

- [ ] **Step 1: Write the failing engine test** (encode SUM/AVERAGE + total-not-resummed + bucketing)

In `crates/pprof/src/series.rs` test module (or `engine.rs`'s), append — backed by the Slice-2 `InMemoryProfileStore`:

```rust
    use std::sync::Arc;

    use crate::engine::{EngineOpts, FlameEngine};
    use crate::test_support::{store_with_profiles, ProfileFixture};

    // Two profiles in group {service="api"} and one in {service="web"},
    // each carrying a precomputed total_value, spread across two 15s buckets.
    fn fixture() -> impl crate::ProfileStore {
        store_with_profiles(&[
            ProfileFixture::new("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
                .labels(&[("service", "api")]).at_ms(0).total_value(100)
                // two stacktrace rows summing to the SAME profile total -> must count 100 once
                .stack_row(/*partition*/ 0, /*id*/ 1, /*value*/ 60)
                .stack_row(0, 2, 40),
            ProfileFixture::new("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
                .labels(&[("service", "api")]).at_ms(16_000).total_value(50)
                .stack_row(0, 1, 50),
            ProfileFixture::new("process_cpu:cpu:nanoseconds:cpu:nanoseconds")
                .labels(&[("service", "web")]).at_ms(0).total_value(7)
                .stack_row(0, 3, 7),
        ])
    }

    #[tokio::test]
    async fn select_series_sum_buckets_by_step_and_group() {
        let eng = FlameEngine::new(Arc::new(fixture()), EngineOpts { default_max_nodes: 2048 });
        let mut got = eng
            .select_series(
                "t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                "{}", &["service".to_string()], 15.0, SeriesAgg::Sum, 0, 60_000,
            )
            .await
            .unwrap();
        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        // api: bucket@0 = 100 (one profile's total, NOT 60+40+itself), bucket@15000 = 50.
        assert!(got[0].labels == vec![("service".into(), "api".into())]);
        assert!(got[0].points == vec![(0, 100.0), (15_000, 50.0)]);
        // web: bucket@0 = 7.
        assert!(got[1].labels == vec![("service".into(), "web".into())]);
        assert!(got[1].points == vec![(0, 7.0)]);
    }

    #[tokio::test]
    async fn select_series_average_divides_by_profile_count() {
        let eng = FlameEngine::new(Arc::new(fixture()), EngineOpts { default_max_nodes: 2048 });
        let got = eng
            .select_series(
                "t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
                r#"{service="api"}"#, &["service".to_string()], 60.0, SeriesAgg::Average, 0, 60_000,
            )
            .await
            .unwrap();
        // one 60s bucket holds both api profiles -> AVERAGE = (100+50)/2 = 75.
        assert!(got.len() == 1);
        assert!(got[0].points == vec![(0, 75.0)]);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib series`
Expected: FAIL — `no method named select_series`.

- [ ] **Step 3: Implement `select_series`**

In `engine.rs`, add the method. Parse `label_selector` → matchers; call `store.select(tenant, profile_type, &matchers, start_ms, end_ms)` → `ProfileScan`. Build a DataFusion aggregation over `scan.samples_table`: **one row per profile** (the `PCOL_TOTAL_VALUE` is identical across a profile's stacktrace rows, so group to the profile grain first — `DISTINCT`/group on `(COL_FINGERPRINT, COL_TIMESTAMP)` taking `MAX(PCOL_TOTAL_VALUE)` per profile to collapse the duplicated rows), then `GROUP BY group_by-label-columns, step-bucket(COL_TIMESTAMP)` with `SUM`/`AVG`. Collect the result rows and assemble `Vec<Series>` (one per label combination, points sorted by bucket start).

```rust
// pub async fn select_series(
//     &self, tenant: &str, profile_type: &str, label_selector: &str,
//     group_by: &[String], step_secs: f64, agg: SeriesAgg, start_ms: i64, end_ms: i64,
// ) -> Result<Vec<Series>, ProfileError> {
//     let step_ms = step_ms_from_secs(step_secs)?;
//     let matchers = crate::parse_label_selector(label_selector)?;
//     let scan = self.store.select(tenant, profile_type, &matchers, start_ms, end_ms).await?;
//     // Verify DataFrame/SQL builder signatures against rev 0838a4d. Two-stage:
//     //  (1) collapse to profile grain: GROUP BY (fingerprint, timestamp) -> MAX(total_value)
//     //      [+ carry the group_by label columns, which are constant within a profile];
//     //  (2) GROUP BY <group_by label cols>, floor(timestamp/step_ms)*step_ms
//     //      -> SUM(total)  (agg=Sum)  |  SUM(total)/COUNT(*)  (agg=Average).
//     // Then collect batches and fold rows into Series (one per label combo, points sorted).
// }
```

> **DataFusion note (verify against rev 0838a4d):** the step-bucket expression is integer arithmetic on `COL_TIMESTAMP` — express as `(timestamp / step_ms) * step_ms` in SQL (DataFusion integer division floors toward zero; guard the negative-timestamp case the kernel handles by filtering to `[start_ms, end_ms]` with `start_ms >= 0`, which the query API guarantees). If the SQL path is awkward, build the same plan via `DataFrame::aggregate(group_exprs, aggr_exprs)`; the *result rows* (label values + bucket + folded value) are the contract, pinned by Task 1's kernel + this task's engine test. Reuse the kernel `step_bucket_ms`/`fold_bucket` if you collect to rows and fold in Rust instead of SQL — either is acceptable as long as the test passes.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib series`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): FlameEngine::select_series — total_value time-series (step-in-seconds, SUM/AVERAGE)"
```

---

## Phase B — `max_nodes` truncation + the synthetic `"other"` node

### Task 3: `Tree::to_flamegraph` `max_nodes` truncation with a min-value heap threshold + synthetic `"other"`

**Files:**
- Modify: `crates/pprof/src/tree.rs` (extend `Tree::to_flamegraph`; add the pure truncation kernel)

**Interfaces:**
- Consumes: the Slice-2 `Tree` (parent/children, `total`, `self_`) + the 4-ints-per-bar `FlameGraph`/`Level` encoder.
- Produces:
  - `pub fn to_flamegraph(self, max_nodes: i64) -> FlameGraph` — **made byte-exact for truncation**: when the tree has more than `max_nodes` nodes, keep the highest-`total` nodes (a min-value threshold via a binary heap), and for each surviving parent whose children were partly pruned, emit **one synthetic `"other"` child** carrying the **sum of the pruned children's totals** (and their summed `self_`), so the level's totals still sum to the parent's total. `max_nodes <= 0` means "no limit" (Pyroscope treats `0`/negative as unbounded; the default is `2048`).
  - `fn truncate_tree(root: &mut TreeNode, max_nodes: i64)` (or an equivalent pure helper) — the kernel, tested in isolation.

> **`"other"` total-conservation is the load-bearing rule:** Pyroscope's flamegraph truncation never loses value — a pruned subtree's total is rolled into an `"other"` sibling so the parent bar's width is unchanged. The threshold is the `(max_nodes)`-th largest node total; nodes below it collapse. `"other"` itself counts as a node. Pin: a tree with N>max_nodes nodes folds to ≤ max_nodes (+ the `"other"` nodes) with `sum(level totals) == parent.total` at every level, and the root total unchanged.

- [ ] **Step 1: Write the failing kernel + encoder tests**

In `tree.rs`, append to the test module:

```rust
    #[test]
    fn truncate_rolls_pruned_children_into_other_conserving_total() {
        // root -> {a:100, b:10, c:5, d:1}; max_nodes small enough to keep root+a + "other".
        let mut t = Tree::new_root("total");
        t.add_stack(&[frame("a")], 100);
        t.add_stack(&[frame("b")], 10);
        t.add_stack(&[frame("c")], 5);
        t.add_stack(&[frame("d")], 1);
        let fg = t.to_flamegraph(/* max_nodes */ 3); // root + a + other
        // root total unchanged.
        assert!(fg.total == 116);
        // the second level sums to the root total (a=100 + other=16).
        let level1_total: i64 = fg.levels[1].values.chunks(4).map(|c| c[1]).sum();
        assert!(level1_total == 116);
        // "other" name present.
        assert!(fg.names.iter().any(|n| n == "other"));
    }

    #[test]
    fn max_nodes_zero_means_unbounded() {
        let mut t = Tree::new_root("total");
        for i in 0..50 {
            t.add_stack(&[frame(&format!("f{i}"))], i + 1);
        }
        let fg = t.to_flamegraph(0);
        // no "other" synthesized; every leaf kept (51 names incl. root "total").
        assert!(!fg.names.iter().any(|n| n == "other"));
    }

    #[test]
    fn xoffset_delta_is_relative_to_previous_bar_end() {
        // two siblings a(total 3) then b(total 2) under root(total 5).
        let mut t = Tree::new_root("total");
        t.add_stack(&[frame("a")], 3);
        t.add_stack(&[frame("b")], 2);
        let fg = t.to_flamegraph(0);
        // level 1: [xOff,total,self,name] groups. first bar xOff=0; second bar's xOff
        // is the DELTA from the previous bar's end, which for adjacent siblings is 0.
        let l1 = &fg.levels[1].values;
        assert!(l1[0] == 0); // first sibling xOffsetDelta
        assert!(l1[4] == 0); // second sibling xOffsetDelta (delta-from-prev-end, not absolute)
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib flamegraph`
Expected: FAIL — truncation rolls nothing into `"other"` (Slice 2 left `max_nodes` as a stub / ignored).

- [ ] **Step 3: Implement the truncation kernel + wire into the encoder**

Add the kernel: compute every node's `total`; if node count `> max_nodes` and `max_nodes > 0`, find the `max_nodes`-th largest total (a min-heap of size `max_nodes`) as the threshold; walk the tree, and for each parent, partition children into kept (`total >= threshold`) and pruned; if any pruned, append one synthetic child named `"other"` with `total = Σ pruned.total`, `self_ = Σ pruned.self_` (and no grandchildren). Then run the existing 4-ints-per-bar BFS encoder over the truncated tree (`names[0] == "total"` root; `xOffsetDelta` = current bar's start minus the previous bar's end within the level).

```rust
// fn truncate_tree(root: &mut TreeNode, max_nodes: i64) {
//     if max_nodes <= 0 { return; }
//     let n = root.node_count();
//     if n as i64 <= max_nodes { return; }
//     let threshold = kth_largest_total(root, max_nodes); // min-heap of size max_nodes
//     fold_below_threshold(root, threshold); // per-parent: pruned children -> one "other"
// }
//
// pub fn to_flamegraph(mut self, max_nodes: i64) -> FlameGraph {
//     truncate_tree(&mut self.root, max_nodes);
//     self.encode_levels()   // existing Slice-2 4-ints-per-bar BFS encoder
// }
```

> **Threshold edge:** when many nodes tie at the threshold total, Pyroscope keeps them all (the count may slightly exceed `max_nodes` rather than break a tie arbitrarily) — match that (keep `total >= threshold`, do not prune ties to hit the count exactly). The total-conservation invariant (the load-bearing rule) holds regardless. Flag with `// verify against Pyroscope <tag>` and let the Slice-8 differential run confirm the exact tie behavior.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib flamegraph`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): max_nodes flamegraph truncation with total-conserving synthetic \"other\""
```

---

## Phase C — `Diff` (the 7-ints-per-bar `FlameGraphDiff`)

### Task 4: Diff aligner + 7-ints-per-bar `FlameGraphDiff` encoder

**Files:**
- Create: `crates/pprof/src/diff.rs`
- Modify: `crates/pprof/src/lib.rs` (`mod diff;`)

**Interfaces:**
- Consumes: two `Tree`s (left + right), the shared name-interning approach from the Slice-2 `FlameGraph` encoder, `FlameGraphDiff`/`Level`.
- Produces:
  - `pub fn diff_trees(left: Tree, right: Tree, max_nodes: i64) -> FlameGraphDiff` — walk both trees in lockstep over the **union of child sets** at each node (a child present on only one side gets a **zero-value placeholder** on the other), so every bar exists on both sides; encode levels in **groups of 7**: `[xOffLeft, totalLeft, selfLeft, xOffRight, totalRight, selfRight, nameIndex]`; set `left_ticks = left.root.total`, `right_ticks = right.root.total`. `max_nodes` truncation applies to the **merged** structure (collapse to `"other"` using the *combined* left+right total as the ranking key, so a node large on either side survives).

> **Zero-value alignment is the load-bearing rule:** a diff bar must occupy the same `nameIndex` position on both sides even when one side never observed that frame — Pyroscope renders the absence as a 0-width/0-self bar so left/right line up visually. Pin: a frame present only on the right yields `totalLeft == 0 && selfLeft == 0` for that bar, and vice-versa; the `nameIndex` is shared.

- [ ] **Step 1: Write the failing tests** (encode the 7-ints layout + zero alignment + ticks)

Create `crates/pprof/src/diff.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::tree::Tree;
    use crate::test_support::frame;

    #[test]
    fn diff_aligns_right_only_frame_with_zero_left() {
        // left:  root -> a:10
        // right: root -> a:10, b:5   (b is right-only)
        let mut l = Tree::new_root("total");
        l.add_stack(&[frame("a")], 10);
        let mut r = Tree::new_root("total");
        r.add_stack(&[frame("a")], 10);
        r.add_stack(&[frame("b")], 5);

        let d = diff_trees(l, r, 0);
        assert!(d.left_ticks == 10);
        assert!(d.right_ticks == 15);
        // levels are groups of 7.
        for lvl in &d.levels {
            assert!(lvl.values.len() % 7 == 0);
        }
        // find b's bar: totalLeft == 0, totalRight == 5.
        let b_idx = d.names.iter().position(|n| n == "b").unwrap() as i64;
        let level1 = &d.levels[1].values;
        let b_bar = level1.chunks(7).find(|c| c[6] == b_idx).unwrap();
        assert!(b_bar[1] == 0); // totalLeft
        assert!(b_bar[2] == 0); // selfLeft
        assert!(b_bar[4] == 5); // totalRight
        assert!(b_bar[5] == 5); // selfRight
    }

    #[test]
    fn diff_root_is_total_on_both_sides() {
        let mut l = Tree::new_root("total");
        l.add_stack(&[frame("a")], 3);
        let mut r = Tree::new_root("total");
        r.add_stack(&[frame("a")], 9);
        let d = diff_trees(l, r, 0);
        let root = &d.levels[0].values; // one bar
        assert!(root[1] == 3 && root[4] == 9); // totalLeft=3, totalRight=9
        assert!(d.names[root[6] as usize] == "total");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib diff`
Expected: FAIL — `cannot find function diff_trees`.

- [ ] **Step 3: Implement the aligner + encoder**

Build a merged structure: a recursive walk keyed by frame name, where each merged node carries `(total_left, self_left, total_right, self_right)` and the union of children (missing side ⇒ zeros). Apply `max_nodes` truncation on the merged node ranking by `total_left + total_right`, rolling pruned children into a single `"other"` merged node summing both sides. Then BFS-encode in groups of 7, computing `xOffLeft`/`xOffRight` independently (each is the delta from the previous bar's end **within its own side's width accounting**). Intern names into `FlameGraphDiff.names` (`names[0] == "total"`). Set `left_ticks`/`right_ticks` from the roots.

```rust
//! `Diff` — structurally align two merged `Tree`s and encode a `FlameGraphDiff`
//! whose levels are traversed in groups of 7:
//! `[xOffLeft, totalLeft, selfLeft, xOffRight, totalRight, selfRight, nameIndex]`.
//! A child present on only one side is given a zero-value placeholder on the
//! other so every bar lines up by `nameIndex`.

use crate::tree::Tree;
use crate::{FlameGraphDiff, Level};

// struct MergedNode { name: String, total_l: i64, self_l: i64, total_r: i64, self_r: i64,
//                     children: Vec<MergedNode> }
// fn merge_aligned(left: &TreeNode, right: &TreeNode) -> MergedNode { /* union children, zero-fill */ }
// pub fn diff_trees(left: Tree, right: Tree, max_nodes: i64) -> FlameGraphDiff { ... }
```

> **`xOffsetDelta` per side:** each side's `xOffset` is independent (the left flamegraph and right flamegraph each have their own bar widths); within a level, a bar's `xOffLeft` is its left-start minus the previous bar's left-end, same rule the single `FlameGraph` uses, computed separately for right. A zero-`totalLeft` bar contributes 0 width on the left, so subsequent left offsets are unaffected — match Pyroscope's renderer expectation. Flag `// verify against Pyroscope <tag>`; the Slice-8 differential confirms.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib diff`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): diff_trees — zero-aligned 7-ints-per-bar FlameGraphDiff"
```

---

### Task 5: `FlameEngine::diff` — two MERGEs → `diff_trees`

**Files:**
- Modify: `crates/pprof/src/engine.rs` (add `diff` + factor a `merge_to_tree` helper)

**Interfaces:**
- Consumes: the Slice-2 MERGE path (factor the part that yields a symbolized `Tree`, *before* `to_flamegraph`, into a `pub(crate) async fn merge_to_tree(...) -> Result<Tree, ProfileError>`), `diff_trees` (Task 4).
- Produces:
  - `pub async fn diff(&self, tenant: &str, left: (&str, &str, i64, i64), right: (&str, &str, i64, i64), max_nodes: i64) -> Result<FlameGraphDiff, ProfileError>` — exactly the Slice-2 §6.5 signature. Each tuple is `(profile_type, label_selector, start_ms, end_ms)`; resolve each side to a `Tree` via `merge_to_tree` **independently** (own scan, own symbolization), then `diff_trees(left_tree, right_tree, max_nodes)`.

- [ ] **Step 1: Write the failing engine test**

In `engine.rs` test module:

```rust
    #[tokio::test]
    async fn engine_diff_two_windows() {
        // same profile_type, two time windows; window A has only frame `a`, window B has `a`+`b`.
        let store = store_with_profiles(&[
            ProfileFixture::new(PT).labels(&[("svc", "x")]).at_ms(0).total_value(10)
                .stack_row(0, 1, 10), // resolves to frame "a"
            ProfileFixture::new(PT).labels(&[("svc", "x")]).at_ms(30_000).total_value(15)
                .stack_row(0, 1, 10).stack_row(0, 2, 5), // "a"+"b"
        ]);
        let eng = FlameEngine::new(Arc::new(store), EngineOpts { default_max_nodes: 2048 });
        let d = eng
            .diff("t", (PT, "{}", 0, 1), (PT, "{}", 29_000, 60_000), 0)
            .await
            .unwrap();
        assert!(d.left_ticks == 10);
        assert!(d.right_ticks == 15);
        // `b` exists only on the right.
        assert!(d.names.iter().any(|n| n == "b"));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: FAIL — `no method named diff`.

- [ ] **Step 3: Implement `diff` + extract `merge_to_tree`**

Refactor the Slice-2 `select_merge_stacktraces` body so the symbolized-`Tree` construction (resolve selector → scan → fold-before-symbolize → partial trees → `Tree::merge`) is a `pub(crate) async fn merge_to_tree(&self, tenant, profile_type, label_selector, start_ms, end_ms) -> Result<Tree, ProfileError>`, with `select_merge_stacktraces` now just `merge_to_tree(...).await?.to_flamegraph(max_nodes_or_default)`. Implement `diff` as two `merge_to_tree` calls + `diff_trees`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: PASS (and the Slice-2 MERGE tests still pass after the refactor).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): FlameEngine::diff via two independent MERGE-to-Tree + diff_trees"
```

---

## Phase D — raw pprof output, span-scoped MERGE, heatmap, cross-block merge

### Task 6: `FlameEngine::select_merge_profile` — merged raw pprof bytes

**Files:**
- Create: `crates/pprof/src/raw_profile.rs`
- Modify: `crates/pprof/src/engine.rs` (add `select_merge_profile`), `crates/pprof/src/lib.rs` (`mod raw_profile;`)

**Interfaces:**
- Consumes: `merge_to_tree` (Task 5) — or, more precisely, the per-`(frames, value)` symbolized stack set the merge produces; the Slice-2 `PprofProfile` (`encode`), `Frame`, `ProfileType` (to fill the pprof `sample_type`/`period_type` from the 5-part string).
- Produces:
  - `pub async fn select_merge_profile(&self, tenant: &str, profile_type: &str, label_selector: &str, start_ms: i64, end_ms: i64) -> Result<Vec<u8>, ProfileError>` — the Slice-2 §6.5 signature; returns the merged profile as **raw pprof bytes** (the `google.v1.Profile` the Connect `SelectMergeProfile` returns).
  - `pub fn tree_to_pprof(tree: &Tree, profile_type: &ProfileType) -> PprofProfile` — pure: build a single-sample-type pprof from the merged tree's root→leaf paths, one `Sample` per leaf path with its `total`-derived value, the `sample_type`/`unit`/`period_type`/`period_unit` taken from `profile_type`, deduping function/string/location tables.

> **Why rebuild a pprof from the `Tree`, not concat input pprofs:** the merge already folded millions of samples into a deduplicated symbolized tree; emitting the tree as a fresh pprof is the canonical "merged profile" (Pyroscope's `SelectMergeProfile` returns one merged profile, not a bag). Each leaf path of the tree becomes one pprof `Sample` (locations leaf→root) with value = the leaf's self value (totals are implied by the tree structure / re-derivable). Pin: round-trip `encode` then `PprofProfile::decode` yields a profile whose summed sample values equal the tree's root total, and whose `sample_type` matches `profile_type`.

- [ ] **Step 1: Write the failing test**

In `raw_profile.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::tree::Tree;
    use crate::{Frame, PprofProfile, ProfileType};
    use crate::test_support::frame;

    #[test]
    fn tree_to_pprof_round_trips_and_conserves_total() {
        let pt = ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let mut t = Tree::new_root("total");
        t.add_stack(&[frame("leaf_a"), frame("root_fn")], 7); // leaf-first
        t.add_stack(&[frame("leaf_b"), frame("root_fn")], 3);
        let profile = tree_to_pprof(&t, &pt);
        let bytes = profile.encode();
        let back = PprofProfile::decode(&bytes).unwrap();
        // summed sample self-values == tree root total (10).
        assert!(back.total_sample_value() == 10);
        // sample_type reflects the profile type.
        assert!(back.first_sample_type_unit() == ("cpu".to_string(), "nanoseconds".to_string()));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib raw_profile`
Expected: FAIL — `cannot find function tree_to_pprof`.

- [ ] **Step 3: Implement `tree_to_pprof` + the engine method**

`tree_to_pprof`: walk the tree leaf paths; for each leaf with non-zero `self_`, emit a pprof `Sample` whose `location_id[]` is the leaf→root function chain (intern functions/strings/locations into the pprof dedup tables), value = `self_`; fill `sample_type`/`period_type` from `profile_type`. `select_merge_profile`: `merge_to_tree(...).await?`, then `tree_to_pprof(&tree, &ProfileType::parse(profile_type)?).encode()`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib raw_profile engine`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): select_merge_profile — merged Tree -> raw pprof bytes"
```

---

### Task 7: `FlameEngine::select_merge_span_profile` — span-scoped MERGE

**Files:**
- Modify: `crates/pprof/src/engine.rs` (add `select_merge_span_profile` + thread a span predicate through `merge_to_tree`)

**Interfaces:**
- Consumes: `merge_to_tree` (Task 5), the `PCOL_SPAN_ID` column, the `span_selector`.
- Produces:
  - `pub async fn select_merge_span_profile(&self, tenant: &str, profile_type: &str, label_selector: &str, span_selector: &[u64], start_ms: i64, end_ms: i64, max_nodes: i64) -> Result<FlameGraph, ProfileError>` — MERGE restricted to samples whose `PCOL_SPAN_ID` is in `span_selector` (span-scoped profiling, Pyroscope `SelectMergeSpanProfile`). Returns a `FlameGraph` (same shape as `select_merge_stacktraces`). Empty `span_selector` ⇒ `ProfileError::Plan` (a span profile with no span ids is a client error; do not silently return the whole profile).
- Refactor: give `merge_to_tree` an optional `span_ids: Option<&[u64]>` parameter (or a sibling `merge_to_tree_spans`) that adds a `PCOL_SPAN_ID IN (...)` predicate to the scan.

- [ ] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn span_profile_filters_by_span_id() {
        let store = store_with_profiles(&[
            ProfileFixture::new(PT).labels(&[("svc", "x")]).at_ms(0).total_value(10)
                .stack_row_span(0, 1, 6, /*span_id*/ 111)
                .stack_row_span(0, 2, 4, /*span_id*/ 222),
        ]);
        let eng = FlameEngine::new(Arc::new(store), EngineOpts { default_max_nodes: 2048 });
        // only span 111's sample (value 6) should contribute.
        let fg = eng
            .select_merge_span_profile("t", PT, "{}", &[111], 0, 60_000, 0)
            .await
            .unwrap();
        assert!(fg.total == 6);
        // empty span selector is a client error.
        assert!(eng.select_merge_span_profile("t", PT, "{}", &[], 0, 60_000, 0).await.is_err());
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: FAIL — `no method named select_merge_span_profile`.

- [ ] **Step 3: Implement**

Thread `Option<&[u64]>` of span ids into the scan-predicate construction inside `merge_to_tree` (add `PCOL_SPAN_ID IN (...)` when present). `select_merge_span_profile` validates non-empty span ids, calls `merge_to_tree(..., Some(span_selector))`, `to_flamegraph(max_nodes_or_default)`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): select_merge_span_profile — span-id-scoped MERGE"
```

---

### Task 8: `SelectHeatmap` — `(time-bucket × value-bucket)` count matrix

**Files:**
- Create: `crates/pprof/src/heatmap.rs`
- Modify: `crates/pprof/src/engine.rs` (add `select_heatmap`), `crates/pprof/src/lib.rs` (`mod heatmap;`)

**Interfaces:**
- Consumes: `ProfileScan` (`PCOL_TOTAL_VALUE` per profile + `COL_TIMESTAMP`), the step-bucket kernel (Task 1).
- Produces:
  - `pub struct Heatmap { pub start_ms: i64, pub end_ms: i64, pub time_buckets: usize, pub value_buckets: usize, pub min_value: i64, pub max_value: i64, pub counts: Vec<Vec<u64>> /* [time][value] */ }` (`Clone`, `Debug`, `PartialEq`).
  - `pub fn bin_heatmap(points: &[(i64 /*ts_ms*/, i64 /*total_value*/)], start_ms: i64, end_ms: i64, time_buckets: usize, value_buckets: usize) -> Heatmap` — pure binning: time axis split into `time_buckets` even spans over `[start_ms, end_ms)`, value axis split linearly over `[min, max]`, counting profiles per `(time, value)` cell.
  - `pub async fn select_heatmap(&self, tenant: &str, profile_type: &str, label_selector: &str, start_ms: i64, end_ms: i64, time_buckets: usize, value_buckets: usize) -> Result<Heatmap, ProfileError>` — scan profiles (profile grain), collect `(ts, total_value)`, `bin_heatmap`.

> **Heatmap fidelity is the least load-bearing surface (spec §13 open question):** Grafana's *minimum* Pyroscope-datasource surface does not call `SelectHeatmap`; it is here for completeness. Pin the binning arithmetic with the kernel test; confirm the exact response shape (axis count, bound inclusivity) against the pinned Pyroscope tag in Slice 8 before investing further. Flag `// verify against Pyroscope <tag>`.

- [ ] **Step 1: Write the failing kernel test**

In `heatmap.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn bin_counts_profiles_per_time_value_cell() {
        // points over [0, 100) ms, values 0..40; 2 time buckets, 2 value buckets.
        let pts = vec![(0, 0), (10, 5), (60, 30), (90, 35)];
        let h = bin_heatmap(&pts, 0, 100, 2, 2);
        assert!(h.counts.len() == 2 && h.counts[0].len() == 2);
        // time bucket 0 = [0,50): two points, both low value -> counts[0][0] == 2.
        assert!(h.counts[0][0] == 2);
        // time bucket 1 = [50,100): two points, both high value -> counts[1][1] == 2.
        assert!(h.counts[1][1] == 2);
        assert!(h.min_value == 0 && h.max_value == 35);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib heatmap`
Expected: FAIL — `cannot find function bin_heatmap`.

- [ ] **Step 3: Implement the binning kernel + engine method**

`bin_heatmap`: compute `min`/`max` of the value axis; for each point, `t_idx = ((ts - start) * time_buckets) / (end - start)` clamped to `[0, time_buckets)`, `v_idx = ((value - min) * value_buckets) / (max - min)` clamped (handle `max == min` ⇒ all in bucket 0); increment `counts[t_idx][v_idx]`. `select_heatmap`: scan to profile grain (`(fingerprint, timestamp) -> MAX(total_value)`), collect `(ts, total)`, call `bin_heatmap`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib heatmap engine`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): select_heatmap — (time x value) count matrix binning"
```

---

### Task 9: Cross-block partial-tree merge — the distributed invariant

**Files:**
- Modify: `crates/pprof/src/engine.rs` (ensure `merge_to_tree` resolves **per-scan-partition** to partial trees, then `Tree::merge`)

**Interfaces:**
- Consumes: `ProfileScan` (whose `samples_table` may be a UNION of several blocks, each with its **own** `stacktrace_partition` numbering), `SymbolSource::resolve(partition, id)`, `Tree::merge`.
- Produces: the *invariant*, not a new public method — pin that `merge_to_tree` groups the folded `(stacktrace_partition, stacktrace_id) -> SUM(value)` rows, resolves each id **within its own partition** via `scan.symbols.resolve(partition, id)`, builds the `Tree` from the *symbolized* frames, and that two blocks sharing the same partition *number* but different symbol tables never collide (raw ids never cross a block boundary — only symbolized partial trees merge).

> **The load-bearing invariant (spec §6.4):** a `stacktrace_id` is only meaningful within its own block's `SymbolDb` partition. The fold (`GROUP BY (partition, id) -> SUM`) is per-scan; resolution is keyed by `(partition, id)` through the scan's `SymbolSource`; the resulting frames are merged into the `Tree`. When the querier (slice 5) hands a UNION `ProfileScan` whose `SymbolSource` dispatches `(partition, id)` to the right block's symbols, the engine's per-`(partition, id)` resolve is automatically block-correct. This task pins that the engine resolves through `(partition, id)` and never assumes a global id space.

- [ ] **Step 1: Write the failing test** (two partitions, same ids, different symbols)

```rust
    #[tokio::test]
    async fn raw_ids_never_cross_a_partition_boundary() {
        // partition 0 id 1 -> "alpha"; partition 1 id 1 -> "beta" (SAME id, different symbol).
        // a correct merge yields BOTH alpha and beta; a buggy global-id merge would collapse them.
        let store = store_with_two_partition_symbols(&[
            (/*partition*/ 0, /*id*/ 1, "alpha", /*value*/ 5),
            (1, 1, "beta", 7),
        ]);
        let eng = FlameEngine::new(Arc::new(store), EngineOpts { default_max_nodes: 2048 });
        let fg = eng.select_merge_stacktraces("t", PT, "{}", 0, 60_000, 0).await.unwrap();
        assert!(fg.names.iter().any(|n| n == "alpha"));
        assert!(fg.names.iter().any(|n| n == "beta"));
        assert!(fg.total == 12); // 5 + 7, not collapsed
    }
```

- [ ] **Step 2: Run to verify it fails (or passes — confirm behavior)**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: if Slice 2 already keyed resolution by `(partition, id)`, this PASSES and the task is a *pin* (commit the test, note in the message). If it FAILS (Slice 2 resolved by `id` only), fix `merge_to_tree` to key by `(partition, id)` — this is exactly the bug this task exists to prevent.

- [ ] **Step 3: Implement / confirm**

Ensure the fold-then-resolve loop in `merge_to_tree` carries `stacktrace_partition` alongside `stacktrace_id` and calls `scan.symbols.resolve(partition, id)`. If the test already passed, no code change — the test is the regression guard.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib engine`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "test(pprof): pin cross-partition resolve — raw stacktrace ids never cross a block boundary"
```

---

### Task 10: Whole-crate gate + a `Tree::merge` associativity property test

**Files:**
- Create: `crates/pprof/tests/merge_associativity.rs`

**Interfaces:**
- Consumes: `Tree`, `Tree::merge`, `Tree::to_flamegraph`.
- Produces: a property test that merging partial trees in any order/grouping yields the same `FlameGraph` (the distributed-merge correctness the query-frontend, slice 6, relies on).

- [ ] **Step 1: Write the property test**

```rust
use crabka_pprof::test_support::{frame, random_stacks};
use crabka_pprof::Tree;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn merge_is_order_independent(stacks in random_stacks(1..40)) {
        // build one whole tree.
        let mut whole = Tree::new_root("total");
        for (frames, v) in &stacks { whole.add_stack(frames, *v); }

        // build two partials over an arbitrary split, then merge.
        let mid = stacks.len() / 2;
        let mut a = Tree::new_root("total");
        for (frames, v) in &stacks[..mid] { a.add_stack(frames, *v); }
        let mut b = Tree::new_root("total");
        for (frames, v) in &stacks[mid..] { b.add_stack(frames, *v); }
        a.merge(b);

        // same flamegraph (unbounded -> no truncation nondeterminism).
        prop_assert_eq!(whole.to_flamegraph(0), a.to_flamegraph(0));
    }
}
```

- [ ] **Step 2: Run the property test**

Run: `cargo test -p crabka-pprof --test merge_associativity`
Expected: PASS (128 cases). If it fails, the bug is in `Tree::merge` or the encoder's child ordering (the encoder must order children deterministically — e.g. by name — so two merge orders encode identically).

- [ ] **Step 3: Full crate gate**

Run:
```bash
cargo test -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
cargo fmt -p crabka-pprof --check
```
Expected: all PASS, no warnings, formatting clean.

- [ ] **Step 4: Commit**

```bash
git add crates/pprof/
git commit -m "test(pprof): Tree::merge order-independence property + slice-3 whole-crate gate"
```

---

## Self-review

**Spec coverage (against §6 + §11 Slice 3):**

- **`SelectSeries`** — precomputed `PCOL_TOTAL_VALUE` (no re-fold, no symbolize), step in SECONDS → `step_ms`, SUM/AVERAGE, `group_by` → `Vec<Series>` → Tasks 1 (kernel) + 2 (engine). The total-not-resummed rule (read `PCOL_TOTAL_VALUE` once per profile, not per stacktrace row) is pinned by the multi-stacktrace fixture.
- **`max_nodes` truncation + synthetic `"other"`** — min-value heap threshold, total-conservation, `max_nodes <= 0` unbounded, `xOffsetDelta` relative-to-previous-bar-end → Task 3 (folded into `Tree::to_flamegraph`).
- **`Diff`** — two independent MERGEs, zero-value placeholder alignment, 7-ints-per-bar `[xOffLeft, totalLeft, selfLeft, xOffRight, totalRight, selfRight, nameIndex]` + `left_ticks`/`right_ticks` → Tasks 4 (encoder) + 5 (engine).
- **`SelectMergeProfile`** (raw pprof bytes via the Slice-2 `PprofProfile::encode`) → Task 6.
- **`SelectMergeSpanProfile`** (`span_selector` filtering `PCOL_SPAN_ID`) → Task 7.
- **`SelectHeatmap`** (`(time × value)` count matrix) → Task 8.
- **Cross-block partial-tree merge** (raw ids never cross a boundary; resolve per `(partition, id)`; `Tree::merge`) → Task 9 (invariant pin) + Task 10 (merge-associativity property).

**Rule-fidelity (the subtle ones are pinned by a unit test BEFORE any integration):** step-in-seconds → ms + `div_euclid` bucket start + SUM/AVG (Task 1 kernel); `PCOL_TOTAL_VALUE` not resummed (Task 2); `"other"` total-conservation + tie-keeps-all + unbounded-on-zero (Task 3); zero-value diff alignment + 7-ints layout + ticks-from-roots (Task 4); span-empty-is-error (Task 7); heatmap cell binning + `max==min` degenerate (Task 8); cross-partition same-id-different-symbol (Task 9); merge order-independence (Task 10). Each cites the Pyroscope behavior it encodes.

**Churn-prone API handling:** `SelectSeries`/`SelectHeatmap` are *ordinary* DataFusion `GROUP BY`/aggregations over `ProfileScan.samples_table` — **no** new `UserDefinedLogicalNodeCore` or `ExecutionPlan` is introduced (unlike the metrics `HistogramFold` slice). The two DataFusion touchpoints (the two-stage profile-grain → step-bucket aggregation in `select_series`, the profile-grain collect in `select_heatmap`) are given as **structure + `// verify against rev 0838a4d`** and pinned by pure kernels (`step_bucket_ms`/`fold_bucket`/`bin_heatmap`) the engine reuses, so any DataFrame/SQL-builder drift surfaces as a failing engine test, not silent corruption — the kernels themselves are dependency-free and always pass. The pprof re-encode (Task 6) goes through the Slice-2 `PprofProfile` codec (prost 0.14), not a fresh proto. No Connect/proto codegen in this slice (that is slice 5).

**Greenfield / no-back-compat respected:** the `merge_to_tree` extraction (Task 5) *changes* the Slice-2 `select_merge_stacktraces` internals in place (no shim, no V2); `merge_to_tree` gains an `Option<&[u64]>` span parameter in Task 7 by editing the signature, not adding an overload. No feature flags, no migration code. The only thing preserved is the **Pyroscope wire/encoding contract** (4-/7-ints-per-bar, step-in-seconds, profile-type strings) — the constraint that actually matters.

**Parallelization note (for the executor):** Phase A Tasks 1→2 are sequential (2 consumes 1). Task 3 (`tree.rs`) is independent of Phase A → can run in the **same batch** as Task 1. Phase C Task 4 (`diff.rs`, new file) is independent of Tasks 1/3 → batch it with them; Task 5 depends on Task 4 **and** the `merge_to_tree` extraction, so sequence it after. Phase D: Task 6 (`raw_profile.rs`), Task 8 (`heatmap.rs`) are disjoint new files → one parallel batch, but **all of** Tasks 5/6/7/8/9 edit `engine.rs`, so the `engine.rs` method additions must be reconciled (they add disjoint methods + share the `merge_to_tree` helper — append-only, low-conflict, but land them sequentially or merge the method block). Task 9 must follow Task 5 (it constrains `merge_to_tree`). Task 10 is last (whole-crate gate). Recommended batches: **B1** = {1, 3, 4} (disjoint files: `series.rs`, `tree.rs`, `diff.rs`), then **B2** = {2} + {5} (engine, sequential on `merge_to_tree` extraction), then **B3** = {6, 7, 8} (reconcile `engine.rs`), then {9}, then {10}.

**Placeholder scan:** no "TBD"/"add the rest"/"similar to Task N". Every implementation step has runnable code or a precise rule + the exact command to run. The bounded hand-waves — the two DataFusion aggregation plumbings and the pprof re-encode trait calls — are each tagged `// verify against rev 0838a4d` and pinned by a dependency-free kernel test (`step_bucket_ms`/`fold_bucket`/`bin_heatmap`/`fold below threshold`) or the Slice-2 codec round-trip, exactly as the plan's constraints require.
