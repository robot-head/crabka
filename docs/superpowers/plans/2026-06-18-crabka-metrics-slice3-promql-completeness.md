# crabka-metrics Slice 3 — PromQL query completeness (`histogram_quantile` + full function catalog + subqueries + `@`/`offset` + the full `.test` corpus)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the `crabka-promql` engine built in Slice 2 (parser + DataFusion operator pattern + `RangeArray` + selectors + rate-family + the core aggregations + binary ops + the `.test` harness scaffold) from "the hard plumbing works" to "PromQL is complete" — `histogram_quantile` on both the classic `le`-bucket path and the native-histogram path (plus the native accessor functions), the entire remaining function catalog, the remaining aggregations, set operations + vector matching, subqueries, order-independent `@`/`offset`, and then prove it by turning on **all 21 Prometheus `.test` conformance files** through the Slice-2 harness.

**Architecture:** This slice is pure `crabka-promql` extension — no new crate, no networking. It adds: (1) a `HistogramFold` logical+execution operator (classic-bucket path, the GreptimeDB `HistogramFold` node is the reference) and a set of native-histogram `ScalarUDF`s that read the `NativeHistogram` Arrow columns directly; (2) one `ScalarUDF` (or UDAF, where the semantics are aggregating) per remaining catalog function, slotted into the Slice-2 function registry; (3) the remaining aggregation `AggregateUDF`s + a `topk`/`bottomk`/`quantile`/`count_values` selecting-aggregation path; (4) set-op (`and`/`or`/`unless`) and many-to-one/one-to-many matching planning on top of the Slice-2 `SeriesDivide`/binary-op infrastructure; (5) subquery planning (`expr[range:resolution]`) that re-uses `RangeManipulate` over an inner range-query plan, plus order-independent `@`/`offset` folding in `SeriesNormalize`; (6) the `.test` corpus wired in, feature-gating the experimental-function files.

The load-bearing realization: **everything in this slice is "more functions and more planning rules" on top of Slice 2's operators.** No new custom Arrow array and no new custom `ExecutionPlan` are required *except* `HistogramFold` (classic `histogram_quantile`) — every other function is a `ScalarUDF`/`AggregateUDF` over the `RangeArray`-paired or `NativeHistogram` columns Slice 2 already produces. Subqueries are a planner transform, not a new operator. So the slice is dominated by per-function bite-sized TDD, each pinned by a hand-written unit test encoding the exact Prometheus rule, then the whole thing is locked down by the upstream `.test` corpus.

**Tech Stack:** Rust 2024 · `datafusion { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }` · `arrow` 59 · `promql-parser` 0.10 · `thiserror`. Consumes `crabka-metrics` (`NativeHistogram`, `native_histogram_schema`, `decode_native_histograms`) and `crabka-blockstore` (`Labels`, `LabelMatcher`). Tests: `assert2`, `proptest`. `.test` corpus vendored under `crates/promql/testdata/promqltest/` (Apache-2.0, attribution preserved).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change signatures/enums/registry shapes freely; no shims, no migration code, no feature flags that gate new behavior default-off (the *experimental-function* flag below is the one exception — it mirrors Prometheus's own `--enable-feature` tier, not a back-compat gate).
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-promql --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-promql` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **DataFusion-internal API churn:** the `rev` is pinned. Where a `UserDefinedLogicalNodeCore`, `ScalarUDFImpl`, `AggregateUDFImpl`, `ExecutionPlan`, or `ColumnarValue` method signature is needed, give the **structure + behavior** and a behavior-pinning test, with a `// verify against rev 0838a4d` note rather than fabricating an exact upstream signature. The test (Prometheus rule → expected output) is the contract; the trait wiring is whatever compiles against the pin.
- **Prometheus-rule fidelity:** every function whose semantics are subtle (extrapolation, forced-monotonic bucket fold, `without` dropping `__name__`, NaN/Inf handling, `@`/`offset` order-independence, subquery alignment) gets its exact rule **encoded in a unit test** that cites the behavior, *before* the upstream `.test` corpus is turned on. The corpus is the backstop, not the spec.
- **Histograms-ignored aggregations:** `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` ignore native-histogram samples (drop them, emit an `info`-level annotation/warning) — match Prometheus exactly.

---

## Dependency & slice roadmap

**Depends on:** **Slice 2 (`crabka-promql` core)** — this slice consumes its public + crate-internal surface verbatim:

- `PromqlEngine<S: MetricStore>` with `query_instant(tenant, query, time_ms)` and `query_range(tenant, query, start_ms, end_ms, step_ms) -> Result<QueryResult, PromqlError>`.
- `QueryResult { Scalar, InstantVector(Vec<InstantSample>), RangeMatrix(Vec<RangeSeries>), Str }`.
- `SampleValue { Float(f64), Histogram(NativeHistogram) }`.
- Custom operators `SeriesDivide` / `SeriesNormalize` / `InstantManipulate` / `RangeManipulate`, plus the `RangeArray` Arrow array (with its `RangeArray` accessor returning per-step window slices).
- The rate-family `ScalarUDF`s (`rate`/`increase`/`delta`) and the function-registry mechanism they register through.
- The `.test` harness scaffold (load / eval-instant / eval-range, expanding-point syntax, native-histogram literals, the `expect` assertion form) — Slice 2 wired *one or two* corpus files through it; this slice wires the remaining nineteen.
- `promql_parser::parser::parse(query) -> Expr`.

Also consumes `crabka-metrics` (`NativeHistogram`, `BucketSpan`, `ResetHint`, `native_histogram_schema`, `decode_native_histograms`, `COL_NH_*`) and `crabka-blockstore` (`Labels`, `LabelMatcher`).

> **If a Slice-2 name differs at implementation time:** the *contract above is authoritative for planning*; if Slice 2 landed a renamed symbol (e.g. `InstantSample` → `InstantPoint`), adapt the call sites in this slice's tasks to the real name — the *behavior* each task pins is what matters, not the spelling. Flag any rename in the task's commit message.

**The 8 metrics slices** (this plan = Slice 3):

1. Data layer — block schemas + native-histogram codec + symbol table. *(done — Slice 1)*
2. `crabka-promql` core — parser + operator pattern + `RangeArray` + selectors + rate-family + core aggregations + binary ops + `.test` harness. *(done — Slice 2)*
3. **Query completeness** *(this plan)* — `histogram_quantile` (classic + native), full function catalog, remaining aggregations, set ops + vector matching, subqueries, `@`/`offset`, full `.test` corpus.
4. Ingest service — remote_write v1/v2 + OTLP + Kafka produce + distributor + HA dedup + compactor.
5. Querier + Prometheus HTTP API + hot/cold merge.
6. Query-frontend — split / shard / cache.
7. Ruler — recording + alerting + rule API.
8. Hardening — multi-tenancy/limits, remote_read, prometheus/compliance + differential-vs-Mimir.

---

## File structure (`crates/promql/` — extends Slice 2)

| File | Responsibility | New / extended |
|---|---|---|
| `src/functions/mod.rs` | function registry — register every new `ScalarUDF`/`AggregateUDF` from this slice | extended |
| `src/functions/histogram.rs` | `histogram_quantile` native path + `histogram_count`/`_sum`/`_avg`/`_fraction`/`_stddev`/`_stdvar` native accessors | **new** |
| `src/functions/over_time.rs` | the `_over_time` family | **new** |
| `src/functions/math.rs` | `abs`/`ceil`/`floor`/`round`/`exp`/`ln`/`log2`/`log10`/`sqrt`/`sgn`/`clamp*`, trig | **new** |
| `src/functions/instant.rs` | `irate`/`idelta`/`resets`/`changes`/`deriv`/`predict_linear`/`double_exponential_smoothing` | **new** |
| `src/functions/labels.rs` | `label_replace`/`label_join`/`sort`/`sort_desc` | **new** |
| `src/functions/datetime.rs` | `time`/`timestamp`/`day_of_week`/`day_of_month`/`day_of_year`/`days_in_month`/`hour`/`minute`/`month`/`year` (UTC), `vector`/`scalar`/`pi` | **new** |
| `src/functions/absent.rs` | `absent`/`absent_over_time` | **new** |
| `src/operators/histogram_fold.rs` | `HistogramFold` logical node + `ExecutionPlan` + stream (classic `le`-bucket path) | **new** |
| `src/aggregations.rs` | `topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`/`group` | extended |
| `src/planner/binary.rs` | set ops `and`/`or`/`unless`; `group_left()`/`group_right()` many-to-one/one-to-many | extended |
| `src/planner/subquery.rs` | `expr[range:resolution]` planning (nests; resolution defaults to eval interval) | **new** |
| `src/planner/at_offset.rs` | order-independent `@`/`offset` folding (incl. `@ start()` / `@ end()`) | **new** |
| `src/feature.rs` | `experimental` cargo feature gate for the experimental function tier | **new** |
| `testdata/promqltest/*.test` | vendored Prometheus `.test` corpus (21 files) | **new** |
| `tests/promqltest_corpus.rs` | drive every corpus file through the Slice-2 harness | extended |

---

## Phase A — `histogram_quantile`: classic path + native path + native accessors

### Task 1: `HistogramFold` operator — classic `le`-bucket fold (logical node + skeleton)

**Files:**
- Create: `crates/promql/src/operators/histogram_fold.rs`
- Modify: `crates/promql/src/operators/mod.rs` (add `pub mod histogram_fold;`)

**Interfaces:**
- Consumes: a float `InstantVector` whose series carry a `le` label (classic `_bucket` series), grouped by the non-`le` label set; the Slice-2 `SeriesDivide` grouping helper.
- Produces:
  - `pub struct HistogramFold` — a `UserDefinedLogicalNodeCore` carrying `le_column: String` (default `"le"`), `field_column: String` (the value column), `quantile: f64`, plus the grouping label columns. **Folds many `le`-bucket rows for one timestamp+series-group into one quantile value.**
  - `pub struct HistogramFoldExec` — the matching `ExecutionPlan`.
  - `fn fold_buckets(buckets: &mut [(f64 /*le*/, f64 /*cumulative count*/)], q: f64) -> f64` — the **pure** bucket-fold kernel (sort by `le`, force monotonic non-decreasing counts, linear-interpolate within the chosen bucket). Tested in isolation here; the operator wraps it.

> **GreptimeDB reference:** their `promql/src/extension_plan/histogram_fold.rs` `HistogramFold` node is the structural reference for the operator (input partitioned by series-group, `le`-sorted within group, one output row per group/timestamp). We clean-room it against our pin; do not vendor.

- [ ] **Step 1: Write the failing kernel test** (encode the exact Prometheus rule)

Create `crates/promql/src/operators/histogram_fold.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // Prometheus classic histogram_quantile rules, encoded:
    //  - buckets are cumulative; the +Inf bucket is the total count.
    //  - counts are forced monotonic non-decreasing (a lower le must not exceed a higher le).
    //  - the quantile rank = q * total; find the first bucket whose cumulative count >= rank,
    //    then linearly interpolate between that bucket's lower and upper le bound.
    //  - q outside [0,1]: q<0 => -Inf, q>1 => +Inf (Prometheus clamps to bucket bounds).
    //  - fewer than 2 buckets, or no +Inf bucket => NaN.

    fn b(pairs: &[(f64, f64)]) -> Vec<(f64, f64)> {
        pairs.to_vec()
    }

    #[test]
    fn median_interpolates_within_bucket() {
        // le=1 -> 0, le=2 -> 0, le=4 -> 4 (cumulative), +Inf -> 4. total=4, rank=2.
        // first bucket with cum>=2 is le=4 (lower bound 2). interpolate:
        //   2 + (4-2) * (rank - cumBelow)/(cumThis - cumBelow) = 2 + 2*(2-0)/(4-0) = 3.
        let mut buckets = b(&[(1.0, 0.0), (2.0, 0.0), (4.0, 4.0), (f64::INFINITY, 4.0)]);
        assert!(fold_buckets(&mut buckets, 0.5) == 3.0);
    }

    #[test]
    fn forces_monotonic_counts() {
        // a non-monotonic input (le=2 cum=5 then le=4 cum=3) must be clamped to non-decreasing.
        let mut buckets = b(&[(1.0, 0.0), (2.0, 5.0), (4.0, 3.0), (f64::INFINITY, 5.0)]);
        // after forcing monotonic: le=4 becomes >= 5; total stays 5, rank(0.5)=2.5 falls in le=2.
        let q = fold_buckets(&mut buckets, 0.5);
        assert!(q > 1.0 && q <= 2.0);
    }

    #[test]
    fn q_above_one_is_plus_inf_bound() {
        let mut buckets = b(&[(1.0, 1.0), (f64::INFINITY, 2.0)]);
        assert!(fold_buckets(&mut buckets, 1.5) == f64::INFINITY);
    }

    #[test]
    fn fewer_than_two_buckets_is_nan() {
        let mut buckets = b(&[(f64::INFINITY, 1.0)]);
        assert!(fold_buckets(&mut buckets, 0.5).is_nan());
    }

    #[test]
    fn highest_finite_bucket_when_rank_in_inf() {
        // total=4, rank(0.99)=3.96 lands in +Inf bucket => return the highest finite le bound.
        let mut buckets = b(&[(1.0, 1.0), (2.0, 2.0), (f64::INFINITY, 4.0)]);
        assert!(fold_buckets(&mut buckets, 0.99) == 2.0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib operators::histogram_fold`
Expected: FAIL — `cannot find function fold_buckets`.

- [ ] **Step 3: Implement the pure kernel** (this is the byte-exact part)

Prepend above `tests`:

```rust
//! `HistogramFold` — the classic `histogram_quantile` operator over `le`-bucket
//! float series. Folds the per-timestamp set of cumulative `le`-bucket counts for
//! one series-group into a single interpolated quantile value, matching
//! Prometheus's `histogram_quantile` for classic histograms. The native-histogram
//! path lives in `functions::histogram` (it operates on `NativeHistogram` columns
//! directly and never reaches this operator).

/// Fold cumulative `le`-bucket `(upper_bound, cumulative_count)` pairs into the
/// `q`-quantile, matching Prometheus's classic `histogram_quantile`:
///
/// 1. sort ascending by `le`;
/// 2. the last bucket must be `+Inf` and its count is the total; without it, or
///    with fewer than two buckets, the result is `NaN`;
/// 3. force counts non-decreasing (a buggy exposition can report a lower `le`
///    with a higher count);
/// 4. `q < 0 => -Inf`, `q > 1 => +Inf`;
/// 5. `rank = q * total`; find the first bucket whose cumulative count `>= rank`;
/// 6. if that bucket is `+Inf`, return the highest finite `le` bound; otherwise
///    linearly interpolate between the bucket's lower bound (`0` for the first)
///    and its `le`, by `(rank - cum_below) / (cum_this - cum_below)`.
#[must_use]
pub fn fold_buckets(buckets: &mut [(f64, f64)], q: f64) -> f64 {
    if buckets.len() < 2 {
        return f64::NAN;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    // last bucket must be +Inf
    if !buckets[buckets.len() - 1].0.is_infinite() {
        return f64::NAN;
    }
    // force monotonic non-decreasing cumulative counts
    let mut max = f64::NEG_INFINITY;
    for bkt in buckets.iter_mut() {
        if bkt.1 < max {
            bkt.1 = max;
        } else {
            max = bkt.1;
        }
    }
    let total = buckets[buckets.len() - 1].1;
    if total == 0.0 {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    let rank = q * total;
    // index of first bucket with cumulative >= rank
    let b = buckets.iter().position(|&(_, c)| c >= rank).unwrap_or(buckets.len() - 1);
    // if it's the +Inf bucket, return highest finite le
    if buckets[b].0.is_infinite() {
        // highest finite bound is the second-to-last le
        return buckets[buckets.len() - 2].0;
    }
    let bucket_end = buckets[b].0;
    let bucket_start = if b == 0 { 0.0 } else { buckets[b - 1].0 };
    let count_below = if b == 0 { 0.0 } else { buckets[b - 1].1 };
    let count_this = buckets[b].1;
    if count_this == count_below {
        return bucket_start;
    }
    bucket_start + (bucket_end - bucket_start) * (rank - count_below) / (count_this - count_below)
}
```

> **Note:** the first-bucket lower bound is `0` only when `le > 0`; Prometheus special-cases a first bucket with a negative `le` (lower bound is `-Inf`-ish handling). The `.test` corpus `histograms.test` exercises this; if a corpus case fails, refine the `bucket_start` rule for `b == 0` against Prometheus's `bucketQuantile` (`promql/quantile.go`) — leave the kernel test as the pin and add the negative-first-bucket case there.

- [ ] **Step 4: Implement the `HistogramFold` logical node + `HistogramFoldExec`**

Add the `UserDefinedLogicalNodeCore` (`name`/`inputs`/`schema`/`expressions`/`fmt_for_explain`/`with_exprs_and_inputs`) and the `ExecutionPlan` that, per partition (one series-group), buffers the `le`-bucket rows per timestamp, calls `fold_buckets`, and emits one output row per `(group, timestamp)`. Mirror the Slice-2 operator skeletons (`InstantManipulate`/`RangeManipulate`) for the trait wiring.

```rust
// Structure (verify trait method signatures against rev 0838a4d):
//
// #[derive(PartialEq, Eq, Hash)]
// pub struct HistogramFold {
//     input: LogicalPlan,
//     le_column: String,       // default "le"
//     field_column: String,    // value column to read counts from
//     ts_column: String,
//     group_columns: Vec<String>, // the non-le label identity
//     quantile: f64,
//     output_schema: DFSchemaRef,
// }
// impl UserDefinedLogicalNodeCore for HistogramFold { ... }
//
// pub struct HistogramFoldExec { /* input ExecutionPlan + same fields + metrics */ }
// impl ExecutionPlan for HistogramFoldExec {
//     // execute(partition) -> stream that, per series-group, groups rows by ts,
//     // collects (le, count) pairs, calls fold_buckets, yields one row/(group,ts).
// }
```

A focused exec-level test (build a 2-group, 2-timestamp `RecordBatch` of `le`-bucket rows, run the exec, assert the folded values) validates the wiring; keep the kernel tests as the semantic pin.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib operators::histogram_fold`
Expected: PASS (kernel tests + exec test).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): HistogramFold operator — classic histogram_quantile le-bucket fold"
```

---

### Task 2: Wire `histogram_quantile` classic path into the planner

**Files:**
- Modify: `crates/promql/src/functions/mod.rs` (route `histogram_quantile(scalar, vector)` to either fold path)
- Create: `crates/promql/src/functions/histogram.rs` (the dispatch + native path lands next task; classic dispatch here)

**Interfaces:**
- Consumes: `HistogramFold` (Task 1), the Slice-2 planner's function-call lowering hook.
- Produces: planner routing — when the argument vector has a `le` label and float values, lower to a `HistogramFold` node with the scalar quantile; the result drops the `le` label (Prometheus drops `le` from the output series).

- [ ] **Step 1: Write the failing engine-level test**

Create `crates/promql/src/functions/histogram.rs` test module (uses an in-memory `MetricStore` test double from Slice 2 — `tests/support` or the Slice-2 `MemStore`):

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_classic_histogram};

    #[test]
    fn classic_histogram_quantile_drops_le_and_interpolates() {
        // series: http_request_duration_seconds_bucket{le="0.1"|"0.2"|"0.5"|"+Inf"}
        // cumulative counts 1,2,4,4 at t=0 -> median (q=0.5) interpolates to 0.3.
        let store = store_with_classic_histogram();
        let r = eval_instant(&store, "histogram_quantile(0.5, http_request_duration_seconds_bucket)", 0);
        // single output series, no `le` label, value ~0.3
        let s = r.single();
        assert!(s.labels.get("le").is_none());
        assert!((s.value_f64() - 0.3).abs() < 1e-9);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::histogram`
Expected: FAIL — `histogram_quantile` unhandled / panics in planner.

- [ ] **Step 3: Implement the classic dispatch**

In `functions/histogram.rs`, add the planner hook: parse the call `histogram_quantile(q_scalar, vector_arg)`; if `vector_arg`'s schema is the float-sample value column (classic path), produce a `HistogramFold` logical node with `quantile = q_scalar`, `le_column = "le"`, grouping by the vector's label identity minus `le`, output drops `le`. (Native dispatch — when `vector_arg` is the `NativeHistogram` columns — is added in Task 3; for now classic only.)

```rust
// pub fn plan_histogram_quantile(
//     q: f64,
//     input: LogicalPlan,
//     ctx: &PlannerCtx,
// ) -> Result<LogicalPlan, PromqlError> {
//     // detect classic vs native by the input schema's value column type.
//     // classic: wrap in HistogramFold (Task 1). native: Task 3.
// }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::histogram`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): wire classic histogram_quantile through HistogramFold"
```

---

### Task 3: Native `histogram_quantile` + native accessors (`histogram_count`/`_sum`/`_avg`/`_fraction`/`_stddev`/`_stdvar`)

**Files:**
- Modify: `crates/promql/src/functions/histogram.rs` (native path + accessor UDFs)
- Modify: `crates/promql/src/functions/mod.rs` (register the accessor UDFs)

**Interfaces:**
- Consumes: `NativeHistogram`, `decode_native_histograms`, `native_histogram_schema`, `COL_NH_*`.
- Produces (each a `ScalarUDF` over the `NativeHistogram` columns):
  - native `histogram_quantile(q, nh_vector)` — interpolate the quantile *within the exponential bucket* (schema-aware bucket bounds `2^(2^-schema)`); NHCB (`schema == -53`) uses `custom_values` as explicit bounds.
  - `histogram_count(nh)` → the histogram's `count`.
  - `histogram_sum(nh)` → the histogram's `sum`.
  - `histogram_avg(nh)` → `sum / count`.
  - `histogram_fraction(lower, upper, nh)` → fraction of observations in `[lower, upper)` (interpolating partial buckets).
  - `histogram_stddev(nh)` / `histogram_stdvar(nh)` → population stddev/variance estimated from bucket midpoints.

- [ ] **Step 1: Write the failing native-path tests** (encode the exact rules)

Append to the `functions::histogram` test module:

```rust
    use crate::test_support::{eval_instant_nh, nh};

    #[test]
    fn histogram_count_and_sum_read_fields() {
        let h = nh(/* count */ 10.0, /* sum */ 42.0, /* schema */ 0,
                   /* pos buckets at indices */ &[(0, 3.0), (1, 4.0), (2, 3.0)]);
        let store = eval_instant_nh("h", &h);
        assert!(eval_instant(&store, "histogram_count(h)", 0).single().value_f64() == 10.0);
        assert!(eval_instant(&store, "histogram_sum(h)", 0).single().value_f64() == 42.0);
        assert!((eval_instant(&store, "histogram_avg(h)", 0).single().value_f64() - 4.2).abs() < 1e-9);
    }

    #[test]
    fn native_histogram_quantile_within_exponential_bucket() {
        // schema 0 => base 2: bucket i covers (2^(i-1), 2^i].
        // 10 obs spread so the median lands in bucket index 1 => (1, 2]; q=0.5 interpolates.
        let h = nh(10.0, 0.0, 0, &[(0, 5.0), (1, 5.0)]); // cumulative 5,10
        let store = eval_instant_nh("h", &h);
        let v = eval_instant(&store, "histogram_quantile(0.5, h)", 0).single().value_f64();
        // bucket index 0 covers (0.5,1], index 1 covers (1,2]; rank=5 is exactly at the
        // index-0/index-1 boundary => 1.0 (the upper bound of bucket 0).
        assert!((v - 1.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_fraction_between_bounds() {
        // fraction in [0, +Inf) == 1.0; fraction in (-Inf, 0) for all-positive == 0.0.
        let h = nh(10.0, 0.0, 0, &[(0, 5.0), (1, 5.0)]);
        let store = eval_instant_nh("h", &h);
        assert!((eval_instant(&store, "histogram_fraction(0, inf, h)", 0).single().value_f64() - 1.0).abs() < 1e-9);
        assert!(eval_instant(&store, "histogram_fraction(-inf, 0, h)", 0).single().value_f64() == 0.0);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::histogram`
Expected: FAIL — native accessors not registered.

- [ ] **Step 3: Implement native bucket math + the UDFs**

Add a pure helper module computing, for a `NativeHistogram`, the ordered list of `(lower_bound, upper_bound, count)` buckets:

- schema `s` in `[-4, 8]`: factor `f = 2^(2^-s)`; bucket index `i` (1-based within a span, mapped through `positive_spans`/`negative_spans` offsets) has upper bound `f^i`, lower bound `f^(i-1)`. Negative buckets mirror with sign. The zero bucket spans `[-zero_threshold, zero_threshold]`.
- schema `-53` (NHCB): `custom_values[i]` are the explicit upper bounds; bucket `i` is `(custom_values[i-1], custom_values[i]]`.

Then:
- `histogram_quantile(q, nh)` = `fold_buckets`-style interpolation over these native buckets (reuse the *interpolation rule* from Task 1's kernel, generalized to per-bucket `(lower, upper, cumulative)`).
- `histogram_count` = `nh.count`; `histogram_sum` = `nh.sum`; `histogram_avg` = `sum/count` (NaN if `count == 0`).
- `histogram_fraction(lower, upper, nh)` = sum of bucket counts fully inside `[lower, upper)` + interpolated partial buckets, divided by `count`.
- `histogram_stddev`/`histogram_stdvar` = population variance/stddev from bucket midpoints weighted by count (`mean = sum/count`).

Each is a `ScalarUDF` reading the `NativeHistogram` columns via `decode_native_histograms` on the input batch (or a column-direct reader if Slice 2 exposes one). Register all in `functions/mod.rs`.

> **Native quantile fidelity:** Prometheus's native `histogramQuantile` (`promql/quantile.go`) interpolates *linearly within the log-scale bucket bound* exactly as the classic path does between `(lower, upper)`. The `native_histograms.test` corpus is the authority — keep the unit tests above as the fast pin and refine bucket-bound math against any corpus failure with a `// verify against native_histograms.test` note.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::histogram`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): native histogram_quantile + histogram_count/sum/avg/fraction/stddev/stdvar"
```

---

## Phase B — the function catalog

### Task 4: `_over_time` family

**Files:**
- Create: `crates/promql/src/functions/over_time.rs`
- Modify: `crates/promql/src/functions/mod.rs`

**Interfaces:**
- Consumes: the Slice-2 `RangeArray` (each cell = the samples in one step's `(t-range, t]` window) and the `ScalarUDF`-over-`RangeArray` pattern the rate-family uses.
- Produces `ScalarUDF`s, each folding one `RangeArray` cell → one scalar:
  - `avg_over_time`/`min_over_time`/`max_over_time`/`sum_over_time`/`count_over_time`/`last_over_time`/`present_over_time`/`stddev_over_time`/`stdvar_over_time`/`quantile_over_time(q, range)`/`mad_over_time` (median absolute deviation).

- [ ] **Step 1: Write the failing tests** (encode the exact rules)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series};

    #[test]
    fn over_time_basic_folds() {
        // metric m has samples [1,2,3,4] within the last 1m window at t.
        let store = store_with_series("m", &[(0, 1.0), (15, 2.0), (30, 3.0), (45, 4.0)]);
        let at = 60_000; // 60s, window [0,60s]
        assert!(eval_instant(&store, "avg_over_time(m[1m])", at).single().value_f64() == 2.5);
        assert!(eval_instant(&store, "min_over_time(m[1m])", at).single().value_f64() == 1.0);
        assert!(eval_instant(&store, "max_over_time(m[1m])", at).single().value_f64() == 4.0);
        assert!(eval_instant(&store, "sum_over_time(m[1m])", at).single().value_f64() == 10.0);
        assert!(eval_instant(&store, "count_over_time(m[1m])", at).single().value_f64() == 4.0);
        assert!(eval_instant(&store, "last_over_time(m[1m])", at).single().value_f64() == 4.0);
        assert!(eval_instant(&store, "present_over_time(m[1m])", at).single().value_f64() == 1.0);
    }

    #[test]
    fn quantile_over_time_interpolates() {
        let store = store_with_series("m", &[(0, 0.0), (15, 1.0), (30, 2.0), (45, 3.0), (50, 4.0)]);
        // q=0.5 over [0,1,2,3,4] = 2.0 (Prometheus linear quantile on the value list).
        assert!(eval_instant(&store, "quantile_over_time(0.5, m[1m])", 60_000).single().value_f64() == 2.0);
    }

    #[test]
    fn stdvar_and_stddev_over_time() {
        let store = store_with_series("m", &[(0, 2.0), (30, 4.0)]);
        // population variance of [2,4] = 1.0; stddev = 1.0.
        assert!(eval_instant(&store, "stdvar_over_time(m[1m])", 60_000).single().value_f64() == 1.0);
        assert!(eval_instant(&store, "stddev_over_time(m[1m])", 60_000).single().value_f64() == 1.0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::over_time`
Expected: FAIL — functions unregistered.

- [ ] **Step 3: Implement the family**

Each is a `ScalarUDF` whose `invoke` reads the `RangeArray` cell (a `&[f64]` window of values + their timestamps), folds it, and returns `f64`. `present_over_time` → `1.0` if the window is non-empty else absent (no output row); `last_over_time` → the last sample value; `quantile_over_time` uses Prometheus's linear-interpolation quantile over the *sorted value list*; `mad_over_time` → median of `|x - median(window)|`. Empty window → no output sample (Prometheus emits nothing). Register all in `functions/mod.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::over_time`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): the _over_time function family"
```

---

### Task 5: Instant-window functions — `irate`/`idelta`/`resets`/`changes`/`deriv`/`predict_linear`/`double_exponential_smoothing`

**Files:**
- Create: `crates/promql/src/functions/instant.rs`
- Create: `crates/promql/src/feature.rs` (the `experimental` gate)
- Modify: `crates/promql/src/functions/mod.rs`, `crates/promql/Cargo.toml` (add `[features] experimental = []`)

**Interfaces:**
- Consumes: the `RangeArray` window (values + timestamps).
- Produces `ScalarUDF`s:
  - `irate(range)` / `idelta(range)` — use only the **last two** samples in the window; `irate` is counter-reset-aware (per-second), `idelta` is the raw last-two difference (gauge, no reset correction).
  - `resets(range)` — count counter resets (number of decreases).
  - `changes(range)` — count value changes.
  - `deriv(range)` — simple linear regression slope (gauge, per-second).
  - `predict_linear(range, t)` — extrapolate the regression `slope * t + intercept` `t` seconds past the window end.
  - `double_exponential_smoothing(range, sf, tf)` (renamed `holt_winters`) — **behind `#[cfg(feature = "experimental")]`**.

- [ ] **Step 1: Write the failing tests** (encode the exact rules)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series};

    #[test]
    fn irate_uses_last_two_samples_per_second() {
        // counter [10@0s, 20@30s, 30@50s] -> irate uses last two: (30-20)/(50-30)s = 0.5/s.
        let store = store_with_series("c", &[(0, 10.0), (30_000, 20.0), (50_000, 30.0)]);
        assert!((eval_instant(&store, "irate(c[1m])", 60_000).single().value_f64() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn idelta_is_last_two_raw_difference() {
        let store = store_with_series("g", &[(0, 10.0), (30_000, 20.0), (50_000, 17.0)]);
        assert!(eval_instant(&store, "idelta(g[1m])", 60_000).single().value_f64() == -3.0);
    }

    #[test]
    fn resets_and_changes_count_events() {
        let store = store_with_series("c", &[(0, 1.0), (10_000, 2.0), (20_000, 1.0), (30_000, 3.0)]);
        // decreases: 2->1 is one reset. changes: 1->2,2->1,1->3 = 3 changes.
        assert!(eval_instant(&store, "resets(c[1m])", 60_000).single().value_f64() == 1.0);
        assert!(eval_instant(&store, "changes(c[1m])", 60_000).single().value_f64() == 3.0);
    }

    #[test]
    fn predict_linear_extrapolates_regression() {
        // perfect line y = t(seconds): samples [0@0,30@30s,60@60s]; predict 60s past end => 120.
        let store = store_with_series("g", &[(0, 0.0), (30_000, 30.0), (60_000, 60.0)]);
        assert!((eval_instant(&store, "predict_linear(g[2m], 60)", 60_000).single().value_f64() - 120.0).abs() < 1e-6);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::instant`
Expected: FAIL.

- [ ] **Step 3: Implement**

`irate`: last two samples; if the later is smaller (reset), treat the delta as the later value (counter zero-anchor), divide by their time gap in seconds. `idelta`: `last - prev`. `resets`: count `i` where `v[i] < v[i-1]`. `changes`: count `i` where `v[i] != v[i-1]`. `deriv`/`predict_linear`: ordinary least-squares over `(t_seconds_relative, value)`; `deriv` returns the slope, `predict_linear(_, t)` returns `slope * (range_end_relative + t) + intercept` measured from the same origin (match Prometheus's `linearRegression` helper). `double_exponential_smoothing(range, sf, tf)` behind `#[cfg(feature = "experimental")]`; register it only when the feature is on.

Create `feature.rs`:

```rust
//! The experimental-function tier. Mirrors Prometheus's `--enable-feature`
//! gating; these functions are NOT a back-compat shim — they are gated because
//! upstream gates them. Default off.
#[must_use]
pub fn experimental_enabled() -> bool {
    cfg!(feature = "experimental")
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::instant`
Then with the flag: `cargo test -p crabka-promql --features experimental --lib functions::instant`
Expected: both PASS (the latter additionally exercises `double_exponential_smoothing` if a gated test is added).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): irate/idelta/resets/changes/deriv/predict_linear + experimental gate"
```

---

### Task 6: Math, `clamp*`, trig, `sgn`

**Files:**
- Create: `crates/promql/src/functions/math.rs`
- Modify: `crates/promql/src/functions/mod.rs`

**Interfaces:**
- Produces simple element-wise `ScalarUDF`s over instant-vector float values:
  - `abs`/`ceil`/`floor`/`round(v[, to_nearest])`/`exp`/`ln`/`log2`/`log10`/`sqrt`/`sgn`.
  - `clamp(v, min, max)`/`clamp_min(v, min)`/`clamp_max(v, max)`.
  - trig: `sin`/`cos`/`tan`/`asin`/`acos`/`atan`/`sinh`/`cosh`/`tanh`/`asinh`/`acosh`/`atanh`/`deg`/`rad`.

- [ ] **Step 1: Write the failing tests** (pin the subtle ones)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series};

    #[test]
    fn sgn_and_round_to_nearest() {
        let store = store_with_series("m", &[(0, -2.5)]);
        assert!(eval_instant(&store, "sgn(m)", 0).single().value_f64() == -1.0);
        // round to nearest 0.5 of -2.5 => -2.5 (ties round toward +Inf per Prometheus).
        let store2 = store_with_series("m", &[(0, 2.5)]);
        assert!(eval_instant(&store2, "round(m)", 0).single().value_f64() == 3.0); // 2.5 -> 3
    }

    #[test]
    fn clamp_orders_min_max() {
        let store = store_with_series("m", &[(0, 5.0)]);
        assert!(eval_instant(&store, "clamp(m, 1, 3)", 0).single().value_f64() == 3.0);
        assert!(eval_instant(&store, "clamp_min(m, 10)", 0).single().value_f64() == 10.0);
        // clamp with min > max yields empty (Prometheus drops the sample).
        assert!(eval_instant(&store, "clamp(m, 9, 1)", 0).is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::math`
Expected: FAIL.

- [ ] **Step 3: Implement**

Element-wise UDFs delegating to `f64` methods. `round`: `(v / to + 0.5).floor() * to` with `to` defaulting to `1.0` (Prometheus rounds half up). `sgn`: `-1`/`0`/`+1` (and propagate `NaN`). `clamp(v, min, max)`: if `min > max` emit nothing; else `v.clamp(min, max)`. `deg`/`rad` convert radians↔degrees; `pi` is a niladic in datetime.rs (Task 8). Register all.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::math`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): math/clamp/trig/sgn element-wise functions"
```

---

### Task 7: `label_replace`/`label_join`/`sort`/`sort_desc`

**Files:**
- Create: `crates/promql/src/functions/labels.rs`
- Modify: `crates/promql/src/functions/mod.rs`

**Interfaces:**
- Consumes: the instant-vector series labels.
- Produces:
  - `label_replace(v, dst, replacement, src, regex)` — if `regex` fully matches `src`'s value, set `dst` to the expanded `replacement` (with `$1`/`${name}` captures); else pass the series through unchanged. A no-match-or-empty-result that equals an existing label is dropped per Prometheus.
  - `label_join(v, dst, sep, src...)` — set `dst` to the `sep`-joined values of the `src` labels.
  - `sort(v)`/`sort_desc(v)` — stable sort the instant-vector by sample value (only meaningful at the API layer, but must reorder the result series).

- [ ] **Step 1: Write the failing tests** (encode the regex-expansion rule)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_labeled_series};

    #[test]
    fn label_replace_expands_capture_groups() {
        // series m{path="/api/v1/x"}; extract the version into a new `ver` label.
        let store = store_with_labeled_series("m", &[("path", "/api/v1/x")], 7.0);
        let r = eval_instant(&store, r#"label_replace(m, "ver", "$1", "path", "/api/(v\\d+)/.*")"#, 0);
        assert!(r.single().labels.get("ver").as_deref() == Some("v1"));
    }

    #[test]
    fn label_join_concatenates_sources() {
        let store = store_with_labeled_series("m", &[("a", "x"), ("b", "y")], 1.0);
        let r = eval_instant(&store, r#"label_join(m, "ab", "-", "a", "b")"#, 0);
        assert!(r.single().labels.get("ab").as_deref() == Some("x-y"));
    }

    #[test]
    fn sort_orders_ascending_by_value() {
        let store = store_with_series_multi(&[("m{i=\"1\"}", 3.0), ("m{i=\"2\"}", 1.0), ("m{i=\"3\"}", 2.0)]);
        let r = eval_instant(&store, "sort(m)", 0);
        assert!(r.values_f64() == vec![1.0, 2.0, 3.0]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::labels`
Expected: FAIL.

- [ ] **Step 3: Implement**

`label_replace`: compile the `regex` (anchored full-match — Prometheus wraps `^(?:...)$`), match against `src`'s value, expand `$1`/`${name}` into `dst`; on no match, series unchanged. Empty `dst` result removes the label. `label_join`: join with `sep`. `sort`/`sort_desc`: sort the result series by value (`NaN` sorts last in `sort`, first in `sort_desc` — match Prometheus). Use the workspace `regex` crate. Register all.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::labels`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): label_replace/label_join/sort/sort_desc"
```

---

### Task 8: Date/time (UTC), `vector`/`scalar`/`pi`, `absent`/`absent_over_time`

**Files:**
- Create: `crates/promql/src/functions/datetime.rs`
- Create: `crates/promql/src/functions/absent.rs`
- Modify: `crates/promql/src/functions/mod.rs`

**Interfaces:**
- Produces:
  - `time()` — the evaluation timestamp in seconds (scalar).
  - `timestamp(v)` — the timestamp of each sample in `v`, in seconds.
  - `day_of_week`/`day_of_month`/`day_of_year`/`days_in_month`/`hour`/`minute`/`month`/`year` — **UTC** calendar fields, defaulting to `time()` when called with no arg, else over each sample's timestamp.
  - `vector(s)` — scalar → a single instant-vector sample with no labels.
  - `scalar(v)` — instant-vector with exactly one series → its value; else `NaN`.
  - `pi()` — `std::f64::consts::PI`.
  - `absent(v)` — `1`-valued single series (carrying labels derived from `v`'s matchers) iff `v` is empty; else nothing.
  - `absent_over_time(range)` — same, over a range.

- [ ] **Step 1: Write the failing tests** (pin UTC + absent's synthesized labels)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, empty_store, store_with_series};

    #[test]
    fn datetime_fields_are_utc() {
        // 2026-06-19T00:00:00Z = 1781481600 s. hour=0, day_of_week=Friday=5, month=6, year=2026.
        let t = 1_781_481_600_000_i64;
        let store = store_with_series("m", &[(t, 1.0)]);
        assert!(eval_instant(&store, "hour(m)", t).single().value_f64() == 0.0);
        assert!(eval_instant(&store, "month(m)", t).single().value_f64() == 6.0);
        assert!(eval_instant(&store, "year(m)", t).single().value_f64() == 2026.0);
        assert!(eval_instant(&store, "day_of_week(m)", t).single().value_f64() == 5.0);
    }

    #[test]
    fn scalar_and_vector_round_trip() {
        let store = store_with_series("m", &[(0, 7.0)]);
        assert!(eval_instant(&store, "scalar(m)", 0).as_scalar() == 7.0);
        assert!(eval_instant(&store, "vector(3)", 0).single().value_f64() == 3.0);
    }

    #[test]
    fn absent_emits_when_series_missing() {
        let store = empty_store();
        // absent(nonexistent{job="x"}) => single series {job="x"} value 1.
        let r = eval_instant(&store, r#"absent(nonexistent{job="x"})"#, 0);
        assert!(r.single().value_f64() == 1.0);
        assert!(r.single().labels.get("job").as_deref() == Some("x"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::datetime functions::absent`
Expected: FAIL.

- [ ] **Step 3: Implement**

Date/time: convert ms→UTC civil fields without a heavy dep — use a small days-from-civil algorithm (Howard Hinnant's `civil_from_days`) on `floor(ts_ms/1000)`; no `chrono` needed (and the workspace already forbids drift). `time()` reads the eval-context timestamp; niladic date/time funcs default to `time()`. `vector`/`scalar` per the rules above. `pi()` niladic. `absent`/`absent_over_time`: synthesize the output labels from the *equality matchers* in the argument's selector (Prometheus copies `name="value"` matchers into the output series), value `1`, only when the vector is empty.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::datetime functions::absent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): date/time (UTC) + vector/scalar/pi + absent/absent_over_time"
```

---

## Phase C — remaining aggregations, set ops, vector matching

### Task 9: `topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`/`group`

**Files:**
- Modify: `crates/promql/src/aggregations.rs`
- Modify: `crates/promql/src/functions/mod.rs` (registry)

**Interfaces:**
- Consumes: the Slice-2 aggregation infrastructure (`by`/`without` grouping; the existing `sum`/`avg`/`min`/`max`/`count`).
- Produces the remaining aggregation ops:
  - `topk(k, v)`/`bottomk(k, v)` — **selecting** aggregations: keep the `k` highest/lowest series per group, **preserving original labels** (not collapsing to the group key).
  - `quantile(q, v)` — the `q`-quantile across each group's values.
  - `count_values("label", v)` — count occurrences of each distinct value, emitting one series per distinct value carrying that value in `label`.
  - `stddev`/`stdvar`/`group` — population stddev/variance / constant-`1` per group.
- **Histograms ignored** by `stddev`/`stdvar`/`quantile`/`topk`/`bottomk` (drop + annotate).

- [ ] **Step 1: Write the failing tests** (encode the label-preservation + histogram-ignore rules)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series_multi};

    #[test]
    fn topk_preserves_original_labels() {
        let store = store_with_series_multi(&[
            ("m{i=\"a\"}", 1.0), ("m{i=\"b\"}", 3.0), ("m{i=\"c\"}", 2.0),
        ]);
        let r = eval_instant(&store, "topk(2, m)", 0);
        // keeps the two highest (b=3, c=2) WITH their `i` labels, not a collapsed group.
        let mut got: Vec<_> = r.iter().map(|s| (s.labels.get("i").unwrap().to_string(), s.value_f64())).collect();
        got.sort_by(|a, b| b.1.total_cmp(&a.1));
        assert!(got == vec![("b".into(), 3.0), ("c".into(), 2.0)]);
    }

    #[test]
    fn count_values_emits_one_series_per_distinct_value() {
        let store = store_with_series_multi(&[
            ("m{i=\"a\"}", 1.0), ("m{i=\"b\"}", 1.0), ("m{i=\"c\"}", 2.0),
        ]);
        let r = eval_instant(&store, r#"count_values("v", m)"#, 0);
        // two distinct values: v="1" count 2, v="2" count 1.
        let mut pairs: Vec<_> = r.iter().map(|s| (s.labels.get("v").unwrap().to_string(), s.value_f64())).collect();
        pairs.sort();
        assert!(pairs == vec![("1".into(), 2.0), ("2".into(), 1.0)]);
    }

    #[test]
    fn quantile_aggregation_interpolates() {
        let store = store_with_series_multi(&[("m{i=\"1\"}", 0.0), ("m{i=\"2\"}", 2.0), ("m{i=\"3\"}", 4.0)]);
        assert!(eval_instant(&store, "quantile(0.5, m)", 0).single().value_f64() == 2.0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib aggregations`
Expected: FAIL.

- [ ] **Step 3: Implement**

`topk`/`bottomk`: within each group, partial-sort by value (descending/ascending), keep `k`, emit each kept series with its **original** labels. `quantile`: Prometheus linear quantile across the group's values (sorted). `count_values`: bucket by stringified value, emit one series per distinct value with `label=value` and `count`. `stddev`/`stdvar`: population variance/stddev per group. `group`: constant `1.0` per group. For the histogram-ignoring set, filter `SampleValue::Histogram` out of the input with an annotation before aggregating.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib aggregations`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): topk/bottomk/quantile/count_values/stddev/stdvar/group aggregations"
```

---

### Task 10: Set operations `and`/`or`/`unless`

**Files:**
- Modify: `crates/promql/src/planner/binary.rs`

**Interfaces:**
- Consumes: the Slice-2 binary-op planning + `SeriesDivide` label-identity grouping.
- Produces set-op semantics on instant vectors, honoring `on(...)`/`ignoring(...)`:
  - `and` — series from LHS whose match-key appears in RHS (LHS values kept).
  - `or` — all of LHS, plus RHS series whose match-key is absent from LHS.
  - `unless` — LHS series whose match-key does **not** appear in RHS.

- [ ] **Step 1: Write the failing tests** (encode match-key semantics)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series_multi};

    fn fixture() -> impl crate::MetricStore {
        store_with_series_multi(&[
            ("a{i=\"1\"}", 10.0), ("a{i=\"2\"}", 20.0),
            ("b{i=\"2\"}", 99.0), ("b{i=\"3\"}", 99.0),
        ])
    }

    #[test]
    fn and_keeps_lhs_with_matching_key() {
        let r = eval_instant(&fixture(), "a and on(i) b", 0);
        // only i=2 exists in both; value comes from a.
        assert!(r.single().labels.get("i").as_deref() == Some("2"));
        assert!(r.single().value_f64() == 20.0);
    }

    #[test]
    fn unless_drops_matching_key() {
        let r = eval_instant(&fixture(), "a unless on(i) b", 0);
        assert!(r.single().labels.get("i").as_deref() == Some("1"));
        assert!(r.single().value_f64() == 10.0);
    }

    #[test]
    fn or_unions_with_lhs_priority() {
        let r = eval_instant(&fixture(), "a or on(i) b", 0);
        // a{i=1},a{i=2} from LHS; b{i=3} added (i=2 already present from LHS).
        let mut keys: Vec<_> = r.iter().map(|s| s.labels.get("i").unwrap().to_string()).collect();
        keys.sort();
        assert!(keys == vec!["1".to_string(), "2".to_string(), "3".to_string()]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib planner::binary`
Expected: FAIL.

- [ ] **Step 3: Implement**

Compute each side's match-key (the label set selected by `on(...)` or everything-except `ignoring(...)`, default = full label identity). `and`: keep LHS series whose key is in RHS's key set. `unless`: keep LHS series whose key is *not* in RHS's. `or`: all LHS + RHS series whose key isn't already produced by LHS. These are pure series-set operations after grouping by key.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib planner::binary`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): set operations and/or/unless with on/ignoring"
```

---

### Task 11: Many-to-one / one-to-many `group_left()` / `group_right()`

**Files:**
- Modify: `crates/promql/src/planner/binary.rs`

**Interfaces:**
- Consumes: the arithmetic/comparison binary-op path + the match-key machinery from Task 10.
- Produces N-to-1 matching: `group_left(extra...)` lets many LHS series match one RHS series; `group_right(extra...)` mirrors. The "one" side's listed `extra` labels are copied onto the result; the result keeps the "many" side's identity. Duplicate-match on the "one" side without a `group_*` modifier is an error.

- [ ] **Step 1: Write the failing tests** (encode the copy-extra-labels + duplicate-error rules)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, eval_instant_err, store_with_series_multi};

    #[test]
    fn group_left_copies_extra_label_from_one_side() {
        let store = store_with_series_multi(&[
            ("req{path=\"/a\",code=\"200\"}", 4.0),
            ("req{path=\"/b\",code=\"200\"}", 6.0),
            ("cfg{code=\"200\",owner=\"team1\"}", 1.0),
        ]);
        // many req series match one cfg per code; copy `owner` onto each result.
        let r = eval_instant(&store, "req * on(code) group_left(owner) cfg", 0);
        assert!(r.iter().all(|s| s.labels.get("owner").as_deref() == Some("team1")));
        assert!(r.len() == 2);
    }

    #[test]
    fn unmodified_many_to_one_is_an_error() {
        let store = store_with_series_multi(&[
            ("req{path=\"/a\",code=\"200\"}", 4.0),
            ("req{path=\"/b\",code=\"200\"}", 6.0),
            ("cfg{code=\"200\"}", 1.0),
        ]);
        // two LHS match one RHS without group_left => "multiple matches" error.
        assert!(eval_instant_err(&store, "req * on(code) cfg", 0).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib planner::binary`
Expected: FAIL.

- [ ] **Step 3: Implement**

Build the "one" side's `key → series` map. For each "many"-side series, look up its match-key on the one side; if found, apply the binary op, take the many-side's labels (drop `__name__`), and copy the `group_left`/`group_right`-listed `extra` labels from the one side. Without a `group_*` modifier, if any key maps to >1 LHS series → return a `PromqlError` "found duplicate series ... many-to-many matching not allowed".

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib planner::binary`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): group_left/group_right many-to-one vector matching"
```

---

## Phase D — subqueries and `@`/`offset`

### Task 12: Order-independent `@`/`offset` (incl. `@ start()` / `@ end()`)

**Files:**
- Create: `crates/promql/src/planner/at_offset.rs`
- Modify: `crates/promql/src/planner/mod.rs`, and the `SeriesNormalize` invocation site

**Interfaces:**
- Consumes: the Slice-2 `SeriesNormalize` (which already applies `offset`/`@`); this task makes the **combination** order-independent and resolves `start()`/`end()`.
- Produces:
  - `fn resolve_eval_timestamp(modifiers: &AtOffset, query_start_ms: i64, query_end_ms: i64, step_ts_ms: i64) -> i64` — pure resolver: `@ <t>` pins absolute; `@ start()` → `query_start_ms`; `@ end()` → `query_end_ms`; `offset d` shifts the *selection* time by `d`; `@` and `offset` together apply as "evaluate at `@`, then shift the lookback by `offset`" **regardless of source order** (`foo @ 100 offset 5m` ≡ `foo offset 5m @ 100`).

- [ ] **Step 1: Write the failing resolver tests**

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn at_and_offset_order_independent() {
        // @ 100s, offset 5m -> select around (100s - 300s) = -200s, independent of step.
        let a = resolve_selection_ms(&AtOffset::at(100_000).with_offset(300_000), 0, 1_000_000, 0);
        let b = resolve_selection_ms(&AtOffset::offset(300_000).with_at(100_000), 0, 1_000_000, 0);
        assert!(a == b);
        assert!(a == -200_000);
    }

    #[test]
    fn at_start_and_end_resolve_to_query_bounds() {
        let s = resolve_selection_ms(&AtOffset::at_start(), 42_000, 99_000, 0);
        let e = resolve_selection_ms(&AtOffset::at_end(), 42_000, 99_000, 0);
        assert!(s == 42_000);
        assert!(e == 99_000);
    }

    #[test]
    fn bare_step_timestamp_used_when_no_modifier() {
        // no @, no offset: the per-step timestamp drives selection.
        let s = resolve_selection_ms(&AtOffset::none(), 0, 1_000_000, 555_000);
        assert!(s == 555_000);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib planner::at_offset`
Expected: FAIL.

- [ ] **Step 3: Implement**

```rust
//! Order-independent `@`/`offset` resolution. Prometheus evaluates `@`/`offset`
//! as a *selection-time* transform that is commutative in source order: the `@`
//! (or per-step timestamp) sets the base evaluation instant, then `offset`
//! subtracts. `@ start()`/`@ end()` resolve to the range query's bounds.

#[derive(Clone, Copy, Debug, Default)]
pub struct AtOffset {
    at: AtModifier,
    offset_ms: i64,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum AtModifier {
    #[default]
    None,
    Abs(i64),
    Start,
    End,
}

impl AtOffset {
    // constructors used by tests/planner: none/at/offset/at_start/at_end/with_*
    // ...
}

/// Resolve the *selection* timestamp (ms) for a selector under its `@`/`offset`.
#[must_use]
pub fn resolve_selection_ms(
    m: &AtOffset,
    query_start_ms: i64,
    query_end_ms: i64,
    step_ts_ms: i64,
) -> i64 {
    let base = match m.at {
        AtModifier::None => step_ts_ms,
        AtModifier::Abs(t) => t,
        AtModifier::Start => query_start_ms,
        AtModifier::End => query_end_ms,
    };
    base - m.offset_ms
}
```

Wire `resolve_selection_ms` into the `SeriesNormalize` build so the selection time it sorts/aligns around comes from this resolver, not from re-applying `@`/`offset` ad hoc. The planner maps `promql_parser`'s `at_modifier`/`offset` AST fields onto `AtOffset`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib planner::at_offset`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): order-independent @/offset with start()/end()"
```

---

### Task 13: Subqueries `expr[range:resolution]`

**Files:**
- Create: `crates/promql/src/planner/subquery.rs`
- Modify: `crates/promql/src/planner/mod.rs`

**Interfaces:**
- Consumes: the engine's own range-query planning (a subquery is "evaluate `expr` as a range query over the outer step's lookback window, at `resolution` step, then feed the resulting matrix to the outer range function") + `RangeManipulate`.
- Produces: subquery planning — `expr[range:resolution]` where `resolution` defaults to the global eval interval; nests (a subquery inside a subquery); the inner range query's `[start, end]` is the outer evaluation instant's `(t - range, t]` window stepped by `resolution`.

- [ ] **Step 1: Write the failing tests** (encode the alignment + default-resolution + nesting rules)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_instant, store_with_series};

    #[test]
    fn subquery_inner_range_then_outer_aggregate() {
        // rate over a subquery: max_over_time( rate(c[1m])[5m:1m] ) evaluates rate(c[1m])
        // at 1m steps across the last 5m, then takes the max.
        let store = store_with_series("c",
            &[(0, 0.0), (60_000, 60.0), (120_000, 120.0), (180_000, 180.0),
              (240_000, 240.0), (300_000, 300.0)]); // +1/s counter
        let v = eval_instant(&store, "max_over_time(rate(c[1m])[5m:1m])", 300_000).single().value_f64();
        // rate is ~1/s at every inner step => max ~1.0.
        assert!((v - 1.0).abs() < 1e-6);
    }

    #[test]
    fn subquery_resolution_defaults_to_eval_interval() {
        // omitting :resolution uses the engine's default step; the query must still plan+run.
        let store = store_with_series("m", &[(0, 1.0), (60_000, 2.0), (120_000, 3.0)]);
        let r = eval_instant(&store, "avg_over_time(m[2m:])", 120_000);
        assert!(!r.is_empty());
    }

    #[test]
    fn nested_subquery_plans() {
        let store = store_with_series("m", &[(0, 1.0), (60_000, 2.0), (120_000, 3.0), (180_000, 4.0)]);
        // a subquery of a subquery must plan and evaluate without panicking.
        let r = eval_instant(&store, "max_over_time(avg_over_time(m[1m])[3m:1m])", 180_000);
        assert!(!r.is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib planner::subquery`
Expected: FAIL.

- [ ] **Step 3: Implement**

For `expr[range:resolution]` at outer instant `t`: build an *inner range plan* of `expr` over `[t - range, t]` stepped by `resolution` (default = the engine's eval interval), producing a matrix; then materialize that matrix as a `RangeArray` window (the cells are the inner-step samples within the outer `(t-range, t]`) so the outer range function (`rate`/`max_over_time`/…) consumes it exactly as a stored range vector. Nesting falls out by recursion: planning the inner `expr` re-enters subquery planning. Resolution defaults to the engine's configured eval interval when the `:resolution` is empty.

> **Alignment note:** Prometheus aligns subquery inner steps to absolute time (multiples of the resolution from epoch), not relative to `t`. Encode the alignment your inner-range planner uses in the test above; if the `subquery.test` corpus disagrees, switch to epoch-aligned stepping and keep the test as the pin (`// verify against subquery.test`).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib planner::subquery`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): subqueries expr[range:resolution] (nesting, default resolution)"
```

---

## Phase E — the full Prometheus `.test` conformance corpus

### Task 14: Vendor the 21 `.test` files at a pinned Prometheus tag

**Files:**
- Create: `crates/promql/testdata/promqltest/*.test` (21 files)
- Create: `crates/promql/testdata/promqltest/LICENSE` + `crates/promql/testdata/promqltest/ATTRIBUTION.md`
- Modify: `crates/promql/testdata/promqltest/VERSION` (records the pinned tag + upstream path)

**Interfaces:**
- Produces: the vendored corpus. The 21 files (Prometheus `promql/promqltest/testdata/`): `aggregators`, `at_modifier`, `collision`, `functions`, `histograms`, `name_label_dropping`, `native_histograms`, `operators`, `range_queries`, `selectors`, `staleness`, `subquery`, `trig_functions`, `limit`, `info`, plus the remaining files present at the pinned tag (total 21). Some exercise experimental functions — those are gated in Task 16.

- [ ] **Step 1: Record the pin + fetch**

Pick a tagged Prometheus release whose `.test` DSL uses the single new `expect` assertion form the Slice-2 harness implements (per spec §13: "pin to a tagged release to get a single assertion form" — choose the first tag where the migration is complete, e.g. a `v3.x` tag). Write the chosen tag + commit + upstream path to `VERSION`. Copy all `.test` files from `promql/promqltest/testdata/` verbatim. Copy Prometheus's `LICENSE` (Apache-2.0) and write `ATTRIBUTION.md` crediting the Prometheus Authors + the tag.

> **Do not hand-author `.test` content.** These are upstream conformance files — vendor them byte-for-byte. The harness adapts to them, never the reverse.

- [ ] **Step 2: Verify the file set**

Run: `ls crates/promql/testdata/promqltest/*.test | wc -l`
Expected: `21`.

- [ ] **Step 3: Commit**

```bash
git add crates/promql/testdata/
git commit -m "test(promql): vendor Prometheus .test conformance corpus (Apache-2.0, attributed)"
```

---

### Task 15: Turn on the non-experimental corpus files through the Slice-2 harness

**Files:**
- Modify: `crates/promql/tests/promqltest_corpus.rs` (the Slice-2 harness driver)

**Interfaces:**
- Consumes: the Slice-2 `.test` harness (`run_test_file(path)` — load / eval-instant / eval-range / `expect` assertions / native-histogram literals).
- Produces: a `#[test]` per non-experimental corpus file, each running the full file through the harness.

- [ ] **Step 1: Write the failing per-file tests**

In `tests/promqltest_corpus.rs`, add one `#[test]` per non-experimental file (drive via the harness). Example:

```rust
use crabka_promql::testkit::run_test_file;

macro_rules! corpus {
    ($name:ident, $file:literal) => {
        #[test]
        fn $name() {
            run_test_file(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/promqltest/", $file));
        }
    };
}

corpus!(aggregators, "aggregators.test");
corpus!(at_modifier, "at_modifier.test");
corpus!(collision, "collision.test");
corpus!(functions, "functions.test");
corpus!(histograms, "histograms.test");
corpus!(name_label_dropping, "name_label_dropping.test");
corpus!(operators, "operators.test");
corpus!(range_queries, "range_queries.test");
corpus!(selectors, "selectors.test");
corpus!(staleness, "staleness.test");
corpus!(subquery, "subquery.test");
corpus!(trig_functions, "trig_functions.test");
corpus!(limit, "limit.test");
// native_histograms + any experimental-gated files added in Task 16.
```

- [ ] **Step 2: Run to verify it fails (then iterate)**

Run: `cargo test -p crabka-promql --test promqltest_corpus`
Expected: initially FAILS on specific cases — each failure is a precise Prometheus-rule discrepancy in a function this slice (or Slice 2) implemented.

- [ ] **Step 3: Fix discrepancies at their source**

For each failing case, fix the **implementation** (the relevant `functions::*`/`aggregations`/`planner::*` from earlier tasks), not the corpus. Use the systematic-debugging skill: read the failing `.test` stanza, identify which function's rule is off (e.g. an extrapolation edge, a `without` `__name__` drop, an `le` first-bucket bound), fix it, re-run. Add a focused unit test pinning each fixed rule next to the function so the regression can't recur silently.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --test promqltest_corpus`
Expected: PASS for all non-experimental files.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "test(promql): conform to the non-experimental Prometheus .test corpus"
```

---

### Task 16: Gate + turn on the experimental-function corpus files

**Files:**
- Modify: `crates/promql/tests/promqltest_corpus.rs`
- Modify: `crates/promql/Cargo.toml` (ensure `experimental` feature exists)

**Interfaces:**
- Consumes: the `experimental` feature (Task 5) + `native_histograms.test` (Task 3's native path).
- Produces: experimental-gated `#[test]`s that only run under `--features experimental` (for files exercising `double_exponential_smoothing` and any other experimental functions), plus the `native_histograms.test` driver (native histograms are stable; gate only what upstream gates).

- [ ] **Step 1: Write the gated tests**

```rust
corpus!(native_histograms, "native_histograms.test"); // native histograms are stable

#[cfg(feature = "experimental")]
mod experimental {
    use super::*;
    // files (or sub-stanzas) that use double_exponential_smoothing / other
    // --enable-feature functions. If upstream keeps experimental cases inline
    // in functions.test rather than separate files, the harness must skip the
    // `@require experimental` stanzas unless the feature is on — implement that
    // skip in run_test_file and assert it here.
    corpus!(experimental_functions, "functions_experimental.test");
}
```

> **If upstream inlines experimental cases** (e.g. `double_exponential_smoothing` stanzas live inside `functions.test`), do not split the file — instead teach the harness to *skip* experimental stanzas when `experimental_enabled()` is false (a stanza-level gate), and run the whole file under both feature settings. The corpus file stays byte-for-byte upstream; the gate lives in the harness.

- [ ] **Step 2: Run both ways**

Run: `cargo test -p crabka-promql --test promqltest_corpus`
Run: `cargo test -p crabka-promql --features experimental --test promqltest_corpus`
Expected: both PASS (the second additionally exercises experimental stanzas).

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "test(promql): feature-gate + run experimental + native-histogram .test corpus"
```

---

### Task 17: Whole-crate gate + clippy across feature combinations

**Files:** none (verification only).

- [ ] **Step 1: Full test sweep (both feature settings)**

Run:
```bash
cargo test -p crabka-promql
cargo test -p crabka-promql --features experimental
```
Expected: all PASS.

- [ ] **Step 2: Clippy + fmt gate (both feature settings)**

Run:
```bash
cargo clippy -p crabka-promql --all-targets
cargo clippy -p crabka-promql --all-targets --features experimental
cargo fmt -p crabka-promql --check
```
Expected: no warnings, formatting clean.

- [ ] **Step 3: Commit (if any fmt/clippy fixups were needed)**

```bash
git add crates/promql/
git commit -m "chore(promql): clippy/fmt clean across feature combinations for slice 3"
```

---

## Self-review

**Spec coverage (against §6 + §11 Slice 3):**

- `histogram_quantile` **classic** path (`le`-bucket fold, forced-monotonic, linear interp; GreptimeDB `HistogramFold` reference) → Tasks 1–2.
- `histogram_quantile` **native** path + native accessors (`histogram_count`/`_sum`/`_avg`/`_fraction`/`_stddev`/`_stdvar`) → Task 3.
- Full function catalog: `_over_time` family → Task 4; `irate`/`idelta`/`resets`/`changes`/`deriv`/`predict_linear`/`double_exponential_smoothing` (experimental-gated) → Task 5; math/`clamp*`/trig/`sgn` → Task 6; `label_replace`/`label_join`/`sort`/`sort_desc` → Task 7; date/time (UTC)/`vector`/`scalar`/`pi`/`absent`/`absent_over_time` → Task 8.
- Remaining aggregations `topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`/`group` (label-preservation + histogram-ignore rules) → Task 9.
- Set ops `and`/`or`/`unless` → Task 10; many-to-one/one-to-many `group_left()`/`group_right()` → Task 11.
- `@`/`offset` order-independent (incl. `@ start()`/`@ end()`) → Task 12; subqueries `expr[range:resolution]` (nesting, default resolution) → Task 13.
- Full `.test` corpus: vendored 21 files at a pinned tag (Apache-2.0, attributed) → Task 14; non-experimental driven through the Slice-2 harness → Task 15; experimental gated + native-histogram corpus → Task 16; whole-crate gate → Task 17.

**Rule-fidelity (the subtle ones are pinned by a unit test BEFORE the corpus turns on):** classic-bucket interpolation + forced-monotonic + `+Inf`/`q∉[0,1]`/`<2 buckets` edges (Task 1 kernel tests); native exponential bucket bounds + `histogram_fraction` (Task 3); `irate`/`idelta` last-two-sample semantics + `predict_linear` regression (Task 5); `round` half-up + `clamp` min>max-drops (Task 6); `label_replace` anchored-regex capture expansion (Task 7); UTC calendar fields + `absent` synthesized labels (Task 8); `topk` original-label preservation + `count_values` per-distinct-value series (Task 9); set-op match-keys (Task 10); `group_left` extra-label copy + unmodified-many-to-one error (Task 11); `@`/`offset` commutativity + `start()`/`end()` (Task 12); subquery inner-range→outer-fold alignment (Task 13). The corpus (Tasks 15–16) is the backstop that catches everything the hand-written tests miss.

**Churn-prone DataFusion API handling:** only `HistogramFold` (Task 1) is a new custom `UserDefinedLogicalNodeCore` + `ExecutionPlan`; its trait wiring is given as **structure + `// verify against rev 0838a4d`**, pinned by the pure `fold_buckets` kernel tests and an exec-level test, never by fabricated upstream signatures. Every other function is a `ScalarUDF`/`AggregateUDF` slotting into the Slice-2 registry — the same pattern the rate-family already uses, so no new churn surface.

**Greenfield / no-back-compat respected:** the only feature flag (`experimental`) mirrors Prometheus's own `--enable-feature` tier (spec §6.2 — "experimental function tier ... behind a feature flag"), not a back-compat gate; no shims, no `V2` variants, no migration code. Schemas/registry shapes from Slice 2 are extended in place.

**Parallelization note (for the executor):** within Phase B, Tasks 4/6/7/8 touch **disjoint** new files (`over_time.rs`/`math.rs`/`labels.rs`/`datetime.rs`+`absent.rs`) and each only appends a registration line to `functions/mod.rs` — dispatch them as one parallel batch, then reconcile the `functions/mod.rs` registrations (the one shared file). Task 5 also adds `feature.rs` + a `Cargo.toml` feature, so sequence it just before/after that batch to avoid the `Cargo.toml`/`mod.rs` overlap. Tasks 10 + 11 share `planner/binary.rs` → sequential. Phase D Tasks 12 + 13 touch different files (`at_offset.rs` vs `subquery.rs`) but both modify `planner/mod.rs` → run sequentially or reconcile the `mod.rs` wiring. Phase E is inherently sequential (vendor → drive → gate → final). Phase A Tasks 1→2→3 are sequential (each builds on the prior).

**Placeholder scan:** no "TBD"/"add the rest"/"similar to Task N". Every implementation step has runnable code or a precise rule + the exact command to run. The bounded hand-waves — `HistogramFold`/exec trait signatures and native bucket-bound math — are each explicitly tagged `// verify against rev 0838a4d` / `// verify against native_histograms.test` and pinned by a behavior test, exactly as the plan's constraints require.
