# crabka-traces Slice 3 — TraceQL completeness (full structural ops + `select()`/`by()` + typed/regex comparisons + TraceQL metrics + tag discovery)

> **COMPLETION STATUS (as-built):** Done and green. Full structural ops
> (negated/union), pipeline aggregations, the TraceQL-metrics families
> (rate/count/quantile/histogram/compare/topk/bottomk with `trace_id` exemplars),
> and scoped tag discovery are implemented and tested; the golden `.case`
> conformance corpus was broadened (Task 10) to cover typed comparisons, structural
> negated/union, and the metrics families (now 59 cases).
> **Six boxes left unchecked by design:** Task 7 Steps 3–5 and Task 11 Steps 1–3
> concern an `experimental` cargo feature that gates the newer metric functions
> default-off. Per the project rule against default-off feature gates for new
> behavior (CLAUDE.md), those functions ship **always-on** instead — no feature gate
> was added. This resolves the design spec's §13 maturity-gating open question
> (answer: no gate). Residual: tag-discovery and array any/none semantics are
> covered by inline unit tests but not the `.case` corpus (the DSL lacks a
> tag-discovery case kind). See design spec §14.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the `crabka-traceql` engine built in Slice 2 (parser + planner + selectors + the `AND` fast-path pushdown + the `SpanStructuralJoin` lowering for the *core* structural operators `>>`/`<<`/`>`/`<`/`~` + the `SpanStore` trait + the pinned result types) from "the core works" to "TraceQL is complete" — the **negated** (`!>>`/`!<<`/`!>`/`!<`) and **union** (`&>>`/`&<<`/`&>`/`&<`/`&~`) structural forms, full `select()`/`by()`/`coalesce()`/`with()` pipeline completeness, typed and regex comparisons across **every** TraceQL static type (string/int/float/bool/duration/status/kind), **TraceQL metrics** (`| rate()`, `| count_over_time()`, `| quantile_over_time(span:duration, .95) by (...)`, the `_over_time` family, `| compare()`/`| topk()`/`| bottomk()`) producing a Prometheus-shaped `TraceMetricsResponse` with `trace_id` exemplars, and **tag discovery** (`tag_names` by scope, `tag_values` typed) read from the `TraceIndex` — then prove it with a curated golden-query corpus diffed against documented TraceQL semantics (no upstream `.test`-style corpus exists; the differential-vs-Tempo headline check lives in Slice 8).

**Architecture:** This slice is pure `crabka-traceql` extension — no new crate, no networking. It adds: (1) **negated/union structural lowerings** on top of the Slice-2 `SpanStructuralJoin` — negated forms become anti-joins (`LEFT JOIN ... WHERE right IS NULL`) over the same nested-set predicates, union forms return *both* sides' spanSets; (2) the **typed-value coercion layer** — one resolver that maps each TraceQL static type onto an Arrow column predicate (durations parsed to nanos `Int64`, `status`/`kind` to their enum `Int`, regex `=~` fully anchored `^...$`, array `=`/`!=` "any/none" semantics); (3) **pipeline-aggregation completeness** (`select`/`by`/`coalesce`/`with` + the scalar aggregates), each a DataFusion aggregation over the matched spanSets; (4) **TraceQL metrics** — a **time-bucketed aggregation** planner that buckets matched spans into `step_ns` windows, applies the metric function, and shapes Prometheus series + `trace_id` exemplars into `TraceMetricsResponse`; (5) **tag discovery** — `tag_names`/`tag_values` delegated to the `SpanStore`'s `TraceIndex`-backed methods, with scope filtering and typed values; (6) the **golden-query corpus** wired through a small harness diffing engine output against hand-encoded expected results.

The load-bearing realization: **everything in this slice is "more lowerings and more planning rules" on top of Slice 2's `SpanStructuralJoin` + `ScanResult`/`SessionContext` substrate.** No new custom Arrow array and no new custom `ExecutionPlan` are required — negated/union structural ops are *different join modes* over the nested-set columns the block-builder already wrote (slice 1), TraceQL metrics is a *time-bucketing aggregation* over the matched-span DataFusion table, and tag discovery is a *delegation* to `SpanStore` methods that the Slice-2 trait already declares. So the slice is dominated by per-operator/-function bite-sized TDD, each pinned by a hand-written unit test encoding the exact TraceQL rule, then locked down by the curated golden corpus.

**Tech Stack:** Rust 2024 · `datafusion { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }` · `arrow` 59 · `async-trait` · `tokio` (`macros`, `rt-multi-thread`) · `futures` · `regex` 1 · `thiserror`. Consumes Slice 2's `crabka-traceql` surface (engine, `SpanStore`, `SpanStructuralJoin`, result types) and — only transitively, via the injected `SpanStore` — `crabka-blockstore`'s `TraceIndex`. Tests: `assert2`, `proptest`. The golden corpus is hand-authored under `crates/traceql/testdata/golden/`.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change signatures/enums/registry shapes freely; no shims, no migration code, no default-off feature flags (the *experimental-metric* flag below is the one exception — it mirrors Tempo's own per-version maturity tier, not a back-compat gate).
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-traceql --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-traceql` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. The engine API (`search`/`query_range`/`trace_by_id`) is async; the `InMemorySpanStore` test double (frozen by Slice 2) backs every test so the engine is independently testable without ingest/blockstore.
- **DataFusion-internal API churn:** the `rev` is pinned. The negated/union structural lowerings reuse Slice 2's `SpanStructuralJoin`; where a `LogicalPlanBuilder` join method, `JoinType`, `ScalarUDFImpl`, or `AggregateUDFImpl` signature is needed, give the **structure + behavior** and a behavior-pinning test, with a `// verify against rev 0838a4d` note rather than fabricating an exact upstream signature. The test (TraceQL query → expected output) is the contract; the trait wiring is whatever compiles against the pin.
- **TraceQL-rule fidelity (Kafka-compat does not apply here; *Tempo-semantic* compat does):** every operator/function whose semantics are subtle (the sibling distinct-span predicate, the negated anti-join, union returning both spanSets, anchored `=~`, array any/none, `status`/`kind` enum mapping, duration parsing, the single-span rule, `select` not narrowing the match set, metrics step alignment, exemplar gating) gets its exact rule **encoded in a unit test that cites the behavior**, *before* the golden corpus is turned on. The corpus is the backstop, not the spec. When Tempo's behavior is undocumented or version-dependent, the Slice-8 differential-vs-Tempo run is the tiebreaker — flag such cases with a `// verify against Tempo <ver>` note rather than guessing silently.
- **Comparison-operator spelling (lexer-verified, spec §6.1):** single `=` is EQ — **there is no `==`**; `=~` is RE (fully anchored `^...$`), `!~` is NRE. Do not accept `==` anywhere in this slice's typed-comparison layer.

---

## Dependency & slice roadmap

**Depends on:** **Slice 2 (`crabka-traceql` core)** — this slice consumes its public + crate-internal surface verbatim:

- `TraceqlEngine<S: SpanStore>` with `new(store, opts)`, `search(tenant, query, start_ns, end_ns, limit)`, `query_range(tenant, query, start_ns, end_ns, step_ns)`, `trace_by_id(tenant, trace_id)`.
- `EngineOpts { default_limit: usize /*20*/, default_spss: usize /*3*/, max_traces: usize }`.
- `#[async_trait] SpanStore` with `scan` / `trace_by_id` / `tag_names` / `tag_values` (the latter two are the tag-discovery delegation targets this slice surfaces through the engine).
- `ScanResult { ctx: SessionContext, span_table: String }` (the matched-span DataFusion table — possibly a hot+cold UNION view).
- The result model `SearchResponse` / `TraceResult` / `SpanSet` / `SpanRef`, `TraceSpans`, `TagScope` / `ScopedTag` / `TypedValue` / `AttrValue`, `SpanMatcher`, `TraceMetricsResponse`, `TraceqlError { Parse, Plan, Exec, Store, Unsupported }`.
- The **`SpanStructuralJoin`** lowering for the **core** structural operators (descendant `>>` / ancestor `<<` / child `>` / parent `<` / sibling `~`) — a partitioned self-join keyed by `trace_id` over the nested-set columns (`nested_set_left`/`nested_set_right`/`parent_id`, Int32, DFS-preorder, computed at block-build in slice 1). This slice adds the **negated** and **union** modes *on the same join*.
- The Slice-2 parser/AST (the operator tokens `!>>`/`&>>`/… are already lexed; this slice adds their *lowering*).
- The `InMemorySpanStore` test double + the test helpers (`store_with_trace`, `eval_search`, `eval_metrics`) the Slice-2 tests established.

> **If a Slice-2 name differs at implementation time:** the *contract above is authoritative for planning*; if Slice 2 landed a renamed symbol (e.g. `SpanStructuralJoin` → `StructuralJoin`, or `SpanRef.attributes` typed differently), adapt this slice's call sites to the real name — the *behavior* each task pins is what matters, not the spelling. Flag any rename in the task's commit message.

Also consumes, only transitively through the injected `SpanStore`, `crabka-blockstore`'s `TraceIndex` (per-block tag-name/value sets + blooms) — this slice never touches blockstore directly; tag discovery is a `SpanStore::tag_names`/`tag_values` call.

**The 8 traces slices** (this plan = Slice 3; each gets its own plan):

1. Blockstore generalization (`BlockIndex` trait) + flattened span block schema + nested-set columns + `TraceIndex`. *(slice 1)*
2. `crabka-traceql` core — parser + planner + selectors + `AND` fast path + `SpanStructuralJoin` (core structural ops) + `SpanStore` trait + result types. *(slice 2)*
3. **TraceQL completeness** *(this plan)* — negated/union structural ops, `select`/`by`/`coalesce`/`with`, typed+regex comparisons across all static types, TraceQL metrics, tag discovery, golden corpus.
4. Ingest service — distributor (OTLP/Jaeger/Zipkin/`/api/push`) → `trace_id`-partitioned WAL; block-builder; live-store.
5. Querier + Tempo HTTP API — `SpanStore` as hot/cold UNION; `/api/echo`, `/api/v2/traces/{id}`, `/api/search`, `/api/v2/search/tags` + `tag/{tag}/values`, `/api/metrics/query_range` + `query`.
6. Query-frontend — search sharding + queueing.
7. Metrics-generator — span-metrics (RED) + service-graphs → remote_write.
8. Hardening — per-tenant limits + multi-tenancy, differential-vs-Tempo corpus, Grafana integration.

---

## File structure (`crates/traceql/` — extends Slice 2)

| File | Responsibility | New / extended |
|---|---|---|
| `src/planner/structural.rs` | `SpanStructuralJoin` lowering — **add negated (anti-join) + union (both-sided) modes** | extended |
| `src/planner/typed.rs` | typed-value coercion: TraceQL static type → Arrow column predicate (string/int/float/bool/duration/status/kind), `=~`/`!~` anchored regex, array any/none | **new** |
| `src/planner/pipeline.rs` | `select()`/`by()`/`coalesce()`/`with()` + scalar aggregates (`count`/`avg`/`max`/`min`/`sum`) + scalar filters | extended |
| `src/metrics/mod.rs` | TraceQL-metrics planner entry + `TraceMetricsResponse` assembly + exemplar collection | **new** |
| `src/metrics/functions.rs` | `rate`/`count_over_time`/`sum`/`min`/`max`/`avg_over_time`/`quantile_over_time`/`histogram_over_time`/`compare`/`topk`/`bottomk` | **new** |
| `src/metrics/bucket.rs` | pure `step_ns` time-bucketing kernel (bucket index, epoch alignment, boundary rule) | **new** |
| `src/discovery.rs` | `tag_names`/`tag_values` engine surface (scope filter + typed values) over `SpanStore` | **new** |
| `src/feature.rs` | `experimental` cargo feature gate for the experimental TraceQL-metrics tier | **new** |
| `src/engine.rs` | wire `query_range` → metrics planner; expose `tag_names`/`tag_values` engine methods | extended |
| `testdata/golden/*.json` | curated golden-query corpus (query + fixture + expected) | **new** |
| `tests/golden_corpus.rs` | drive every golden file through the engine + diff | **new** |

---

## Phase A — full structural operators (negated + union)

### Task 1: Negated structural ops `!>>`/`!<<`/`!>`/`!<` — anti-join lowering

**Files:**
- Modify: `crates/traceql/src/planner/structural.rs`

**Interfaces:**
- Consumes: the Slice-2 `SpanStructuralJoin` lowering (core ops) + the nested-set predicate helpers (`descendant_pred`/`child_pred`/`sibling_pred` returning the join `Expr`) it exposes; the per-`trace_id` partitioning key.
- Produces:
  - `pub enum StructuralMode { Match, Negated, Union }` (extends/replaces Slice-2's match-only mode) — drives the join type.
  - negated lowering: `A !>> B` returns the RIGHT-hand spans (`B`) that have **no** matching `A` under the structural relation, realized as a per-`trace_id` **anti-join** (`LEFT JOIN A ON <nested-set pred> WHERE A.span_id IS NULL`) — the spans of `{B}` for which *no* span of `{A}` is a structural counterpart. Same nested-set predicate as the positive form, inverted by the anti-join.

> **Structural-operator return rule (spec §6.3):** structural operators relate spans by tree position and **return the RIGHT-hand spans.** `B >> A` is "descendant" returning `B`; the negation `B !>> A` returns the `B` spans with no ancestor `A`. The join is always partitioned by `trace_id` (the same-trace requirement is guaranteed by the partition), so the anti-join's "no match" is scoped within a trace.

- [x] **Step 1: Write the failing test** (encode the anti-join rule against a hand-built trace)

In `crates/traceql/src/planner/structural.rs`, append to the test module:

```rust
#[cfg(test)]
mod negated_tests {
    use assert2::assert;

    use crate::test_support::{eval_search, store_with_trace, Span};

    // trace tree:  root(service=gw) -> a(http) -> b(db)
    //                               \-> c(http)
    // `{ .db } !>> { .http }`  ⇒ return http spans that have NO db descendant.
    // a has descendant b(db) -> excluded.  c(http) has no db descendant -> kept.
    fn fixture() -> impl crate::SpanStore {
        store_with_trace(&[
            Span::root("root").attr("service", "gw"),
            Span::child("a", "root").attr("http.method", "GET"),
            Span::child("b", "a").attr("db.system", "pg"),
            Span::child("c", "root").attr("http.method", "POST"),
        ])
    }

    #[tokio::test]
    async fn negated_descendant_returns_right_spans_without_match() {
        let r = eval_search(&fixture(), "{ span.db.system = \"pg\" } !>> { span.http.method != nil }").await;
        let spans = r.all_span_ids();
        // returns the RIGHT side ({http}) with no db descendant: only `c`.
        assert!(spans == vec![*b"c\0\0\0\0\0\0\0"]);
    }

    #[tokio::test]
    async fn negated_child_uses_parent_id_anti_join() {
        // `{ .db } !> { .http }` ⇒ http spans with no direct db child. a has child b(db) -> excluded; c kept.
        let r = eval_search(&fixture(), "{ span.db.system = \"pg\" } !> { span.http.method != nil }").await;
        assert!(r.all_span_ids() == vec![*b"c\0\0\0\0\0\0\0"]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib planner::structural::negated_tests`
Expected: FAIL — `!>>`/`!>` lower to `TraceqlError::Unsupported` (Slice 2 only did the positive forms).

- [x] **Step 3: Implement the anti-join lowering**

In `structural.rs`, extend the lowering to switch on `StructuralMode`. For `Negated`, build a `LEFT` join of the right-hand (`B`) plan against the left-hand (`A`) plan on `trace_id` + the nested-set predicate, then filter to rows where the `A` side's `span_id` is NULL, projecting only the `B` columns.

```rust
// Structure (verify join builder + JoinType against rev 0838a4d):
//
// pub enum StructuralMode { Match, Negated, Union }
//
// fn lower_structural(
//     left: LogicalPlan,   // {A}
//     right: LogicalPlan,  // {B} — the returned side
//     op: StructuralOp,    // Descendant | Ancestor | Child | Parent | Sibling
//     mode: StructuralMode,
//     ctx: &PlannerCtx,
// ) -> Result<LogicalPlan, TraceqlError> {
//     let pred = nested_set_predicate(op, /*left alias*/, /*right alias*/); // reused from Slice 2
//     let on_trace = col("B.trace_id").eq(col("A.trace_id"));
//     match mode {
//         StructuralMode::Match => /* Slice 2: inner/semi-join, project B, dedup */,
//         StructuralMode::Negated => {
//             // LEFT JOIN A ON (trace_id eq AND pred), keep rows where A.span_id IS NULL, project B.
//             LogicalPlanBuilder::from(right)
//                 .join_detailed(left, JoinType::Left, (on_trace.and(pred)), None)?
//                 .filter(col("A.span_id").is_null())?
//                 .project(/* B columns only */)?
//                 .build()
//         }
//         StructuralMode::Union => /* Task 2 */,
//     }
// }
```

The nested-set `pred` for each op is the Slice-2 helper unchanged (`descendant`: `B.left > A.left && B.right < A.right`; `child`: `B.parent_id == A.left`; `sibling`: `B.parent_id == A.parent_id && B.span_id != A.span_id`). Negation never changes the predicate — only the join type + the NULL filter.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib planner::structural::negated_tests`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): negated structural ops !>>/!<</!>/!< via nested-set anti-join"
```

---

### Task 2: Union structural ops `&>>`/`&<<`/`&>`/`&<`/`&~` — both-sided spanSets

**Files:**
- Modify: `crates/traceql/src/planner/structural.rs`

**Interfaces:**
- Consumes: the Task-1 `StructuralMode` + the same nested-set predicates.
- Produces: the `Union` lowering — `A &>> B` returns **both** the matching `A` spans *and* the matching `B` spans (Tempo's union-of-spanset form), where the positive form `A >> B` returns only `B`. Realized as an inner/semi-join that projects *and unions* both sides' span identities, each retaining its own scope's spanSet, so the `SearchResponse` carries both spans under the trace (matched count includes both).

> **Union semantics (spec §6.3):** the `&`-prefixed forms are "union (both sides returned)." The positive `>>` returns the right side only; `&>>` additionally returns the left side. The output is the *union of the two spanSets* for each trace that satisfies the relation — distinct from `||` (which is a trace-level OR of independent conditions). Build it as the positive join, then union the projected left-span rows with the projected right-span rows (both carry `trace_id`), deduplicating by `(trace_id, span_id)`.

- [x] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod union_tests {
    use assert2::assert;

    use crate::test_support::{eval_search, store_with_trace, Span};

    fn fixture() -> impl crate::SpanStore {
        store_with_trace(&[
            Span::root("root").attr("service", "gw"),
            Span::child("a", "root").attr("http.method", "GET"),
            Span::child("b", "a").attr("db.system", "pg"),
        ])
    }

    #[tokio::test]
    async fn union_descendant_returns_both_sides() {
        // `{ .http } &>> { .db }` ⇒ a (http, has db descendant) AND b (db). positive >> would return only b.
        let r = eval_search(&fixture(), "{ span.http.method != nil } &>> { span.db.system = \"pg\" }").await;
        let mut ids = r.all_span_ids();
        ids.sort();
        assert!(ids == vec![*b"a\0\0\0\0\0\0\0", *b"b\0\0\0\0\0\0\0"]);
    }

    #[tokio::test]
    async fn union_dedups_a_span_matching_both_sides() {
        // a span that satisfies both sides must appear once, not twice.
        let r = eval_search(&fixture(), "{ span.service = \"gw\" } &>> { span.service = \"gw\" }").await;
        assert!(r.all_span_ids().len() == r.distinct_span_ids().len());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib planner::structural::union_tests`
Expected: FAIL — `&>>` lowers to `Unsupported`.

- [x] **Step 3: Implement the union lowering**

In the `StructuralMode::Union` arm: build the positive join (inner/semi, same `pred`), then produce a `UNION` of two projections — one selecting the left side's span identity, one the right's — both carrying `trace_id`; deduplicate by `(trace_id, span_id)` (a `DISTINCT` or grouped projection). The `matched` count on the resulting `SpanSet` counts both sides' distinct spans.

```rust
//         StructuralMode::Union => {
//             let joined = /* positive inner join on (on_trace AND pred) */;
//             let left_spans  = joined.clone().project(/* A.trace_id, A.span_id, A.* */)?;
//             let right_spans = joined.project(/* B.trace_id, B.span_id, B.* */)?;
//             LogicalPlanBuilder::from(left_spans)
//                 .union(right_spans.build()?)?
//                 .distinct()?           // dedup (trace_id, span_id)
//                 .build()
//         }
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib planner::structural::union_tests`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): union structural ops &>>/&<</&>/&</&~ returning both spanSets"
```

---

## Phase B — typed comparisons across all static types

### Task 3: Typed-value coercion — string/int/float/bool + anchored regex + array any/none

**Files:**
- Create: `crates/traceql/src/planner/typed.rs`
- Modify: `crates/traceql/src/planner/mod.rs` (add `pub mod typed;`)

**Interfaces:**
- Consumes: the Slice-2 `SpanMatcher` (scope + key + op + value) and the matched-span column schema (dedicated attribute columns + the generic typed-LIST attribute columns from spec §4.1: `Value: List<Utf8>`, `ValueInt: List<Int64>`, `ValueDouble: List<Float64>`, `ValueBool: List<Bool>`).
- Produces:
  - `pub enum StaticType { String, Int, Float, Bool, Duration, Status, Kind, Nil }`
  - `pub fn coerce_predicate(m: &SpanMatcher, col_type: &arrow::datatypes::DataType) -> Result<datafusion::prelude::Expr, TraceqlError>` — lowers one resolved selector condition to an Arrow column predicate, dispatching on the matcher's op + the inferred `StaticType`. `=` → `Expr::eq`; `!=` → `neq`; `<`/`<=`/`>`/`>=` → ordered compares (typed); `=~` → a **fully anchored** `^(?:...)$` regex match UDF; `!~` → its negation; nil checks (`.foo != nil`) → column-not-null. **Array columns** (the generic typed-LIST attributes) use **any-match** for `=`/`=~` and **none-match** for `!=`/`!~` (spec §4.1/§6.4).

- [x] **Step 1: Write the failing tests** (encode the array + anchored-regex rules)

Create `crates/traceql/src/planner/typed.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_search, store_with_trace, Span};

    fn fixture() -> impl crate::SpanStore {
        store_with_trace(&[
            Span::root("root")
                .attr("http.method", "GET")
                .attr_array("tags", &["red", "blue"]) // array attribute
                .attr_int("http.status_code", 200),
        ])
    }

    #[tokio::test]
    async fn anchored_regex_does_not_match_substring() {
        // =~ is fully anchored ^...$: "GE" must NOT match "GET".
        assert!(eval_search(&fixture(), "{ span.http.method =~ \"GE\" }").await.is_empty());
        assert!(!eval_search(&fixture(), "{ span.http.method =~ \"GE.*\" }").await.is_empty());
        assert!(!eval_search(&fixture(), "{ span.http.method =~ \"GET\" }").await.is_empty());
    }

    #[tokio::test]
    async fn array_attr_any_match_for_eq_none_for_neq() {
        // `tags = "red"` matches if ANY element is "red".
        assert!(!eval_search(&fixture(), "{ span.tags = \"red\" }").await.is_empty());
        // `tags != "red"` matches only if NO element is "red" -> this span has "red" -> excluded.
        assert!(eval_search(&fixture(), "{ span.tags != \"red\" }").await.is_empty());
        // `tags != "green"` -> no element is "green" -> matches.
        assert!(!eval_search(&fixture(), "{ span.tags != \"green\" }").await.is_empty());
    }

    #[tokio::test]
    async fn int_comparison_is_numeric_not_lexical() {
        // 200 >= 100 numerically (lexical "200" < "100" would be wrong).
        assert!(!eval_search(&fixture(), "{ span.http.status_code >= 100 }").await.is_empty());
        assert!(eval_search(&fixture(), "{ span.http.status_code > 200 }").await.is_empty());
    }

    #[tokio::test]
    async fn nil_check_is_column_presence() {
        assert!(!eval_search(&fixture(), "{ span.http.method != nil }").await.is_empty());
        assert!(eval_search(&fixture(), "{ span.does.not.exist != nil }").await.is_empty());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib planner::typed`
Expected: FAIL — `coerce_predicate` missing / array+regex paths unhandled.

- [x] **Step 3: Implement the coercion layer**

```rust
//! Typed-value coercion: lower one resolved TraceQL selector condition onto an
//! Arrow column predicate, dispatching on the comparison op and the static type
//! of the value. `=~`/`!~` are fully anchored (`^(?:...)$`) per spec §6.1; array
//! (generic typed-LIST) columns use any-match for `=`/`=~` and none-match for
//! `!=`/`!~` per spec §4.1/§6.4. There is no `==` token.

use arrow::datatypes::DataType;
use datafusion::prelude::{col, lit, Expr};

use crate::error::TraceqlError;
use crate::matcher::{MatchOp, SpanMatcher};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaticType {
    String,
    Int,
    Float,
    Bool,
    Duration,
    Status,
    Kind,
    Nil,
}

/// Wrap a TraceQL regex into Prometheus/Tempo's fully-anchored form.
#[must_use]
pub fn anchor_regex(pat: &str) -> String {
    format!("^(?:{pat})$")
}

/// Lower one selector condition to an Arrow column predicate.
pub fn coerce_predicate(m: &SpanMatcher, col_type: &DataType) -> Result<Expr, TraceqlError> {
    let column = col(m.column_name()); // resolved scope+key -> physical/attr column
    let is_array = matches!(col_type, DataType::List(_));
    match m.op {
        MatchOp::Eq if is_array => Ok(array_any(&column, m, col_type)?),
        MatchOp::Neq if is_array => Ok(array_none(&column, m, col_type)?),
        MatchOp::Re if is_array => Ok(array_any_regex(&column, m)?),
        MatchOp::Nre if is_array => Ok(array_none_regex(&column, m)?),
        MatchOp::Eq => Ok(column.eq(typed_lit(m, col_type)?)),
        MatchOp::Neq => Ok(column.not_eq(typed_lit(m, col_type)?)),
        MatchOp::Lt => Ok(column.lt(typed_lit(m, col_type)?)),
        MatchOp::Lte => Ok(column.lt_eq(typed_lit(m, col_type)?)),
        MatchOp::Gt => Ok(column.gt(typed_lit(m, col_type)?)),
        MatchOp::Gte => Ok(column.gt_eq(typed_lit(m, col_type)?)),
        MatchOp::Re => Ok(regex_match(&column, &anchor_regex(&m.value_str()?), false)),
        MatchOp::Nre => Ok(regex_match(&column, &anchor_regex(&m.value_str()?), true)),
        MatchOp::NotNil => Ok(column.is_not_null()),
        MatchOp::Nil => Ok(column.is_null()),
    }
}

// typed_lit: parse m.value into a DataFusion literal matching col_type
//   String -> lit(string); Int -> lit(i64); Float -> lit(f64); Bool -> lit(bool).
//   (Duration/Status/Kind handled in Task 4 — they pre-map value to Int/nanos.)
fn typed_lit(m: &SpanMatcher, col_type: &DataType) -> Result<Expr, TraceqlError> {
    match col_type {
        DataType::Utf8 | DataType::LargeUtf8 => Ok(lit(m.value_str()?)),
        DataType::Int64 | DataType::Int32 => Ok(lit(m.value_i64()?)),
        DataType::Float64 => Ok(lit(m.value_f64()?)),
        DataType::Boolean => Ok(lit(m.value_bool()?)),
        other => Err(TraceqlError::Plan(format!("no literal coercion for {other:?}"))),
    }
}

// regex_match: build an anchored regex predicate. Reuse the workspace `regex`
// crate via a ScalarUDF (`traceql_regex_match(col, pat) -> bool`) registered in
// metrics/mod.rs's UDF registry, or DataFusion's built-in regexp_match if its
// anchoring matches; pin behavior with the test above. `negate` flips it.
// verify against rev 0838a4d
fn regex_match(column: &Expr, anchored: &str, negate: bool) -> Expr { /* ... */ unimplemented!() }

// array_any / array_none / array_any_regex / array_none_regex: lower onto an
// array-contains / array-position predicate over the List column. DataFusion's
// `array_has` (any element equals) gives array_any directly; array_none = NOT
// array_has; the regex variants need a per-element UDF (`array_any_regex`).
// verify against rev 0838a4d
fn array_any(column: &Expr, m: &SpanMatcher, col_type: &DataType) -> Result<Expr, TraceqlError> { /* ... */ unimplemented!() }
fn array_none(column: &Expr, m: &SpanMatcher, col_type: &DataType) -> Result<Expr, TraceqlError> { /* ... */ unimplemented!() }
fn array_any_regex(column: &Expr, m: &SpanMatcher) -> Result<Expr, TraceqlError> { /* ... */ unimplemented!() }
fn array_none_regex(column: &Expr, m: &SpanMatcher) -> Result<Expr, TraceqlError> { /* ... */ unimplemented!() }
```

> **DataFusion array-predicate note:** `array_has`/`array_position` are the array-contains primitives at the pin; if a name differs, the *behavior* (any-element-equals for `=`, no-element-equals for `!=`) is the contract pinned by the test — adapt to whatever `array_*` function the pin exposes, or fall back to an `unnest` + `EXISTS` rewrite. Tag with `// verify against rev 0838a4d`.

Replace each `unimplemented!()` with the real lowering (DataFusion `Expr` builders + the registered regex/array UDFs). The test in Step 1 is the behavioral pin.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib planner::typed`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): typed comparisons (string/int/float/bool) + anchored regex + array any/none"
```

---

### Task 4: Duration / status / kind intrinsic types

**Files:**
- Modify: `crates/traceql/src/planner/typed.rs`

**Interfaces:**
- Consumes: the Task-3 `coerce_predicate` + `StaticType`; the intrinsic columns (`duration_nanos: Int64`, `status_code: Int`, `kind: Int`) from the span block schema (spec §4.1).
- Produces:
  - `pub fn parse_duration_ns(s: &str) -> Result<i64, TraceqlError>` — Go-style durations (`1ms`/`500us`/`2s`/`1m30s`/`1h`) → nanoseconds.
  - `pub fn status_to_int(s: &str) -> Result<i64, TraceqlError>` — `unset`→0, `ok`→1, `error`→2 (matching the block-schema enum, spec §4.1).
  - `pub fn kind_to_int(s: &str) -> Result<i64, TraceqlError>` — `unspecified`→0, `internal`→1, `server`→2, `client`→3, `producer`→4, `consumer`→5.
  - extension of `typed_lit` so `span:duration > 200ms` parses `200ms` → `200_000_000` `Int64`, `span:status = error` → `2`, `span:kind = server` → `2`, with ordered compares working numerically.

- [x] **Step 1: Write the failing tests** (pin the parse tables + numeric compare)

```rust
    #[test]
    fn duration_parses_to_nanos() {
        use super::parse_duration_ns;
        assert!(parse_duration_ns("200ms").unwrap() == 200_000_000);
        assert!(parse_duration_ns("1s").unwrap() == 1_000_000_000);
        assert!(parse_duration_ns("500us").unwrap() == 500_000);
        assert!(parse_duration_ns("1m30s").unwrap() == 90_000_000_000);
        assert!(parse_duration_ns("nope").is_err());
    }

    #[test]
    fn status_and_kind_enum_mapping() {
        use super::{kind_to_int, status_to_int};
        assert!(status_to_int("error").unwrap() == 2);
        assert!(status_to_int("ok").unwrap() == 1);
        assert!(kind_to_int("server").unwrap() == 2);
        assert!(kind_to_int("client").unwrap() == 3);
        assert!(kind_to_int("bogus").is_err());
    }

    #[tokio::test]
    async fn duration_status_kind_predicates_evaluate() {
        use crate::test_support::{eval_search, store_with_trace, Span};
        let store = store_with_trace(&[
            Span::root("root").duration_ns(250_000_000).status("error").kind("server"),
        ]);
        assert!(!eval_search(&store, "{ span:duration > 200ms }").await.is_empty());
        assert!(eval_search(&store, "{ span:duration < 100ms }").await.is_empty());
        assert!(!eval_search(&store, "{ span:status = error }").await.is_empty());
        assert!(!eval_search(&store, "{ span:kind = server }").await.is_empty());
        assert!(eval_search(&store, "{ span:kind = client }").await.is_empty());
    }
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib planner::typed`
Expected: FAIL — `parse_duration_ns`/`status_to_int`/`kind_to_int` missing.

- [x] **Step 3: Implement the parse tables + wire into `typed_lit`**

```rust
/// Parse a Go-style duration literal into nanoseconds.
/// Units: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`. Compound like `1m30s` sums.
pub fn parse_duration_ns(s: &str) -> Result<i64, TraceqlError> {
    let mut total: i64 = 0;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            chars.next();
            continue;
        }
        // read the unit (longest-match: ns/us/µs/ms/s/m/h)
        let mut unit = String::new();
        while let Some(&u) = chars.peek() {
            if u.is_ascii_digit() { break; }
            unit.push(u);
            chars.next();
        }
        let val: f64 = num.parse().map_err(|_| TraceqlError::Parse(format!("bad duration number in {s:?}")))?;
        num.clear();
        let mult: f64 = match unit.as_str() {
            "ns" => 1.0,
            "us" | "µs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60_000_000_000.0,
            "h" => 3_600_000_000_000.0,
            other => return Err(TraceqlError::Parse(format!("unknown duration unit {other:?}"))),
        };
        total += (val * mult) as i64;
    }
    if !num.is_empty() {
        return Err(TraceqlError::Parse(format!("duration {s:?} has trailing digits without a unit")));
    }
    Ok(total)
}

/// Map a TraceQL `status` keyword to its block-schema enum int.
pub fn status_to_int(s: &str) -> Result<i64, TraceqlError> {
    match s {
        "unset" => Ok(0),
        "ok" => Ok(1),
        "error" => Ok(2),
        other => Err(TraceqlError::Parse(format!("unknown status {other:?}"))),
    }
}

/// Map a TraceQL `kind` keyword to its block-schema enum int.
pub fn kind_to_int(s: &str) -> Result<i64, TraceqlError> {
    match s {
        "unspecified" => Ok(0),
        "internal" => Ok(1),
        "server" => Ok(2),
        "client" => Ok(3),
        "producer" => Ok(4),
        "consumer" => Ok(5),
        other => Err(TraceqlError::Parse(format!("unknown kind {other:?}"))),
    }
}
```

Extend `typed_lit` (Task 3) so when the matcher's `StaticType` is `Duration`/`Status`/`Kind`, it pre-maps the literal through these helpers to an `Int64`/`Int` literal before building the compare. The `StaticType` is inferred from the intrinsic being compared (`span:duration` → `Duration`, `span:status` → `Status`, `span:kind` → `Kind`).

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib planner::typed`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): duration/status/kind typed comparisons (Go durations + enum mapping)"
```

---

## Phase C — pipeline completeness

### Task 5: `select()` / `by()` / `coalesce()` / `with()` + scalar aggregates

**Files:**
- Modify: `crates/traceql/src/planner/pipeline.rs`

**Interfaces:**
- Consumes: the Slice-2 pipeline scaffolding (`| count()` was the core); the matched-span `ScanResult` table; the `SpanRef.attributes` projection.
- Produces:
  - `select(a, b, ...)` — **additive projection**: the listed attributes are added to every returned `SpanRef.attributes` without narrowing the match set (spec §6.5 — `select` controls *what is returned*, not *what matches*).
  - `by(attrs...)` — group the matched spanSets by the listed attributes (the grouping key for aggregates).
  - the scalar aggregates `count()`/`avg(f)`/`max(f)`/`min(f)`/`sum(f)` + scalar filters (`| count() > N`) — lower to DataFusion aggregations over the matched spans.
  - `coalesce()` (flatten nested spanSets) and `with(...)` (bind a sub-expression) per Tempo's pipeline grammar.

- [x] **Step 1: Write the failing tests** (pin `select`-doesn't-narrow + `by`-groups)

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_search, store_with_trace, Span};

    fn fixture() -> impl crate::SpanStore {
        store_with_trace(&[
            Span::root("root").attr("service", "gw").attr("http.method", "GET"),
            Span::child("a", "root").attr("service", "gw").attr("http.method", "POST"),
            Span::child("b", "root").attr("service", "db"),
        ])
    }

    #[tokio::test]
    async fn select_adds_attributes_without_narrowing() {
        // `{ .service = "gw" } | select(http.method)` returns BOTH gw spans, each with http.method attached.
        let r = eval_search(&fixture(), "{ span.service = \"gw\" } | select(span.http.method)").await;
        assert!(r.distinct_span_ids().len() == 2);
        assert!(r.span_attr("root", "http.method").as_deref() == Some("GET"));
        assert!(r.span_attr("a", "http.method").as_deref() == Some("POST"));
    }

    #[tokio::test]
    async fn count_by_service_groups() {
        // `{ } | count() by (.service)` -> gw:2, db:1.
        let r = eval_search(&fixture(), "{ } | count() by (span.service)").await;
        assert!(r.scalar_by("service", "gw") == 2.0);
        assert!(r.scalar_by("service", "db") == 1.0);
    }

    #[tokio::test]
    async fn count_scalar_filter_drops_groups() {
        // `{ } | count() by (.service) > 1` keeps only gw.
        let r = eval_search(&fixture(), "{ } | count() by (span.service) > 1").await;
        assert!(r.scalar_groups() == vec![("gw".to_string(), 2.0)]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib planner::pipeline`
Expected: FAIL — `select`/`by`/scalar-filter unhandled beyond Slice-2's `count()`.

- [x] **Step 3: Implement**

`select(attrs...)`: add the listed attribute columns to the output projection of the *already-matched* spanSet — it does **not** add to the `WHERE`/match predicates (the match set is whatever the `{}` selector produced). `by(attrs...)`: a DataFusion `GROUP BY` on the listed attribute columns for the aggregate. `count()`/`avg`/`max`/`min`/`sum`: the corresponding DataFusion aggregate over the grouped matched spans (`avg`/`max`/`min`/`sum` take a numeric attribute/intrinsic argument). The scalar filter (`> N`) becomes a `HAVING` on the aggregate. `coalesce()` flattens nested spanSets into one; `with(x = expr)` binds `expr` for reuse in the pipeline. Each lowers to a DataFusion plan node over the matched-span table from `ScanResult`.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib planner::pipeline`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): select/by/coalesce/with + scalar aggregates (count/avg/max/min/sum)"
```

---

## Phase D — TraceQL metrics

### Task 6: Time-bucketing kernel + `rate()` / `count_over_time()`

**Files:**
- Create: `crates/traceql/src/metrics/bucket.rs`
- Create: `crates/traceql/src/metrics/mod.rs`
- Create: `crates/traceql/src/metrics/functions.rs`
- Modify: `crates/traceql/src/lib.rs` (add `pub mod metrics;`)

**Interfaces:**
- Consumes: the matched-span `ScanResult` table (each row a span with `start_unix_nano: Int64`); `EngineOpts`.
- Produces:
  - `pub fn bucket_index(ts_ns: i64, start_ns: i64, step_ns: i64) -> i64` — the pure time-bucketing kernel: which `step_ns` window a span's timestamp falls in, **epoch-aligned** (windows are `[start_ns + k*step_ns, start_ns + (k+1)*step_ns)`), matching Tempo's left-closed/right-open bucketing.
  - `pub fn bucket_starts(start_ns: i64, end_ns: i64, step_ns: i64) -> Vec<i64>` — the series of bucket start timestamps spanning `[start_ns, end_ns]`.
  - `rate()` lowering — `count of matched spans per bucket / (step_ns/1e9)` → a Prometheus-shaped series (one value per bucket).
  - `count_over_time()` lowering — raw count of matched spans per bucket.

- [x] **Step 1: Write the failing kernel + engine tests**

Create `crates/traceql/src/metrics/bucket.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn bucket_index_is_left_closed_right_open() {
        // step 60s; start 0. ts at exactly a boundary belongs to the LATER bucket.
        assert!(bucket_index(0, 0, 60_000_000_000) == 0);
        assert!(bucket_index(59_999_999_999, 0, 60_000_000_000) == 0);
        assert!(bucket_index(60_000_000_000, 0, 60_000_000_000) == 1); // boundary -> next bucket
        assert!(bucket_index(125_000_000_000, 0, 60_000_000_000) == 2);
    }

    #[test]
    fn bucket_starts_span_the_range() {
        // [0, 180s] step 60s -> starts [0, 60s, 120s, 180s].
        let starts = bucket_starts(0, 180_000_000_000, 60_000_000_000);
        assert!(starts == vec![0, 60_000_000_000, 120_000_000_000, 180_000_000_000]);
    }
}
```

Then the engine-level test in `metrics/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{eval_metrics, store_with_spans_at, Span};

    #[tokio::test]
    async fn rate_counts_matched_spans_per_step_per_second() {
        // 6 matching spans evenly over 60s, step 60s -> one bucket, rate = 6/60 = 0.1/s.
        let store = store_with_spans_at("svc", &[0, 10, 20, 30, 40, 50]); // seconds
        let r = eval_metrics(&store, "{ span.service = \"svc\" } | rate()", 0, 60_000_000_000, 60_000_000_000).await;
        assert!((r.single_series_value(0) - 0.1).abs() < 1e-9);
    }

    #[tokio::test]
    async fn count_over_time_is_raw_bucket_count() {
        let store = store_with_spans_at("svc", &[0, 10, 70, 80]); // two in bucket 0, two in bucket 1
        let r = eval_metrics(&store, "{ span.service = \"svc\" } | count_over_time()", 0, 120_000_000_000, 60_000_000_000).await;
        assert!(r.single_series_values() == vec![2.0, 2.0]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib metrics`
Expected: FAIL — `bucket_index`/`eval_metrics` path missing.

- [x] **Step 3: Implement the kernel + the two functions**

```rust
//! Pure time-bucketing for TraceQL metrics. Buckets are epoch-aligned to
//! `start_ns` and left-closed/right-open: a span at timestamp `t` lands in bucket
//! `(t - start_ns) / step_ns`, so a timestamp on a boundary belongs to the LATER
//! bucket — matching Tempo's `query_range` stepping.

/// The bucket index a timestamp falls into.
#[must_use]
pub fn bucket_index(ts_ns: i64, start_ns: i64, step_ns: i64) -> i64 {
    (ts_ns - start_ns) / step_ns
}

/// The start timestamps of every bucket spanning `[start_ns, end_ns]`.
#[must_use]
pub fn bucket_starts(start_ns: i64, end_ns: i64, step_ns: i64) -> Vec<i64> {
    let mut out = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        out.push(t);
        t += step_ns;
    }
    out
}
```

In `metrics/functions.rs`, the `rate()`/`count_over_time()` lowering: group the matched-span table by `bucket_index(start_unix_nano, start_ns, step_ns)`, `COUNT(*)` per bucket; `count_over_time` emits the raw count, `rate` divides by `step_ns as f64 / 1e9`. Fill empty buckets with `0`. In `metrics/mod.rs`, the planner entry assembles each group's per-bucket values into a Prometheus-shaped series (Task 8 shapes `TraceMetricsResponse`).

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib metrics`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): TraceQL-metrics time-bucketing kernel + rate()/count_over_time()"
```

---

### Task 7: `quantile_over_time` / `sum`/`min`/`max`/`avg_over_time` + `by(...)` + `histogram_over_time`/`compare`/`topk`/`bottomk` (experimental-gated)

**Files:**
- Modify: `crates/traceql/src/metrics/functions.rs`
- Create: `crates/traceql/src/feature.rs`
- Modify: `crates/traceql/Cargo.toml` (add `[features] experimental = []`), `crates/traceql/src/metrics/mod.rs`

**Interfaces:**
- Consumes: the Task-6 bucketing kernel + the matched-span table.
- Produces:
  - `quantile_over_time(field, q...)` — per bucket, the `q`-quantile of `field` (e.g. `span:duration`) across matched spans; **multiple quantiles** (`.99, .9, .5`) emit one series each, labeled by `p`.
  - `sum_over_time(field)`/`min_over_time(field)`/`max_over_time(field)`/`avg_over_time(field)` — per-bucket scalar folds of `field`.
  - `by(attrs...)` on any metric — adds the attribute set to the grouping key, emitting one series per `(bucket-grid, group)`.
  - `histogram_over_time(field)` / `compare(...)` / `topk(n)` / `bottomk(n)` — **behind `#[cfg(feature = "experimental")]`** (mirrors Tempo's per-version maturity, spec §6.6; `rate`/`count_over_time`/`quantile`/the `_over_time` family are stable).

- [x] **Step 1: Write the failing tests** (pin quantile + by + p-label)

```rust
    #[tokio::test]
    async fn quantile_over_time_by_emits_per_quantile_series() {
        // durations [100ms..500ms] for svc in one bucket; q=.5 -> ~300ms, q=.9 -> ~460ms.
        let store = store_with_durations("svc", &[100, 200, 300, 400, 500]); // ms
        let r = eval_metrics(
            &store,
            "{ span.service = \"svc\" } | quantile_over_time(span:duration, .5, .9) by (span.service)",
            0, 60_000_000_000, 60_000_000_000,
        ).await;
        // one series per (quantile, group); the .5 series ~ 300ms in nanos.
        assert!((r.series_value(&[("p", "0.5"), ("service", "svc")], 0) - 300_000_000.0).abs() < 5_000_000.0);
        assert!(r.series_value(&[("p", "0.9"), ("service", "svc")], 0) > r.series_value(&[("p", "0.5"), ("service", "svc")], 0));
    }

    #[tokio::test]
    async fn avg_over_time_folds_field_per_bucket() {
        let store = store_with_durations("svc", &[200, 400]); // ms, one bucket
        let r = eval_metrics(&store, "{ span.service = \"svc\" } | avg_over_time(span:duration)", 0, 60_000_000_000, 60_000_000_000).await;
        assert!((r.single_series_value(0) - 300_000_000.0).abs() < 1e-3);
    }
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib metrics`
Expected: FAIL.

- [ ] **Step 3: Implement**

Each `_over_time(field)` is a per-bucket DataFusion aggregate over `field` (`SUM`/`MIN`/`MAX`/`AVG`); `quantile_over_time` uses an `approx_percentile_cont`-style aggregate (or a sorted-value linear quantile) per bucket per quantile, emitting one series per quantile with a `p="<q>"` label. `by(attrs...)` extends the `GROUP BY` with the attribute columns. The experimental functions (`histogram_over_time`/`compare`/`topk`/`bottomk`) are registered only under `#[cfg(feature = "experimental")]`.

Create `feature.rs`:

```rust
//! The experimental TraceQL-metrics tier. Mirrors Tempo's per-version maturity
//! gating (spec §6.6); these functions are NOT a back-compat shim — they are
//! gated because upstream gates them. Default off.
#[must_use]
pub fn experimental_enabled() -> bool {
    cfg!(feature = "experimental")
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib metrics`
Then: `cargo test -p crabka-traceql --features experimental --lib metrics`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): quantile/sum/min/max/avg_over_time + by(...) + experimental metric tier"
```

---

### Task 8: `TraceMetricsResponse` assembly + `trace_id` exemplars + `query_range` wiring

**Files:**
- Modify: `crates/traceql/src/metrics/mod.rs`
- Modify: `crates/traceql/src/engine.rs` (wire `query_range` → metrics planner)

**Interfaces:**
- Consumes: the per-bucket series from Tasks 6–7; `EngineOpts` (`max_exemplars` — added here if Slice 2 left it out, as a new `EngineOpts` field, not a flag).
- Produces:
  - `TraceMetricsResponse` populated as **Prometheus-shaped series** (label set → `[(bucket_ts, value)]`) plus **exemplars** (one `trace_id` + `span_id` + value + ts per bucket, up to `max_exemplars`), gated on a configured `max_exemplars > 0` (spec §6.6 — "exemplars require a configured `max_exemplars`").
  - `TraceqlEngine::query_range(tenant, query, start_ns, end_ns, step_ns)` returns this response.

- [x] **Step 1: Write the failing test** (pin exemplar gating + shape)

```rust
    #[tokio::test]
    async fn query_range_emits_series_and_trace_id_exemplars() {
        let store = store_with_spans_at_trace("svc", &[(0, *b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10")]);
        // max_exemplars > 0 (set via EngineOpts) -> the bucket carries the span's trace_id as an exemplar.
        let r = eval_metrics_with_exemplars(&store, "{ span.service = \"svc\" } | rate()", 0, 60_000_000_000, 60_000_000_000, 1).await;
        assert!(!r.series().is_empty());
        let ex = r.exemplars();
        assert!(ex[0].trace_id == [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]);
    }

    #[tokio::test]
    async fn exemplars_disabled_when_max_is_zero() {
        let store = store_with_spans_at("svc", &[0]);
        let r = eval_metrics_with_exemplars(&store, "{ span.service = \"svc\" } | rate()", 0, 60_000_000_000, 60_000_000_000, 0).await;
        assert!(r.exemplars().is_empty());
    }
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib metrics`
Expected: FAIL — `TraceMetricsResponse` not populated with exemplars / `query_range` not wired.

- [x] **Step 3: Implement the assembly + wiring**

In `metrics/mod.rs`, assemble each group's per-bucket values into a `TraceMetricsResponse` series (Prometheus label set + `(bucket_start_ns, value)` points across the full `bucket_starts` grid, zero-filled). When `max_exemplars > 0`, for each bucket collect up to `max_exemplars` `(trace_id, span_id, value, ts)` exemplars from the spans that contributed to that bucket. In `engine.rs`, route `query_range` to this planner (`search` stays the spanSet path). Add `max_exemplars: usize` to `EngineOpts` if absent.

> **`TraceMetricsResponse` shape note:** the Slice-2 contract pins the *type name* but leaves the field layout to the producer; shape it as `{ series: Vec<MetricSeries { labels: Vec<(String,String)>, points: Vec<(i64 /*ns*/, f64)> }>, exemplars: Vec<Exemplar { trace_id:[u8;16], span_id:[u8;8], value:f64, ts_ns:i64, labels:Vec<(String,String)> }> }`. Slice 5's `/api/metrics/query_range` projects this onto Tempo's JSON. If Slice 2 already fixed the field names, adapt to them and keep the test's behavioral assertions.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib metrics`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): TraceMetricsResponse assembly + trace_id exemplars + query_range wiring"
```

---

## Phase E — tag discovery

### Task 9: `tag_names` by scope + `tag_values` typed (over the `TraceIndex`)

**Files:**
- Create: `crates/traceql/src/discovery.rs`
- Modify: `crates/traceql/src/engine.rs`, `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: the Slice-2 `SpanStore::tag_names(tenant, scope, start_ns, end_ns) -> Vec<ScopedTag>` and `SpanStore::tag_values(tenant, tag, start_ns, end_ns) -> Vec<TypedValue>` (which the querier backs with the `TraceIndex` per-block tag sets in Slice 5).
- Produces:
  - `TraceqlEngine::tag_names(&self, tenant, scope: Option<TagScope>, start_ns, end_ns) -> Result<Vec<ScopedTag>, TraceqlError>` — delegates to the store, filters/groups by `TagScope` (`Resource`/`Span`/`Intrinsic`/`Event`/`Link`/`Instrumentation`); when `scope` is `None`, returns all scopes.
  - `TraceqlEngine::tag_values(&self, tenant, tag, start_ns, end_ns) -> Result<Vec<TypedValue>, TraceqlError>` — delegates, returning each value with its TraceQL static `type_` (`string`/`int`/`float`/`bool`/`duration`/`status`/`kind`).

- [x] **Step 1: Write the failing tests** (pin scope filtering + typed values)

Create `crates/traceql/src/discovery.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::test_support::{store_with_tags, TagScope};
    use crate::EngineOpts;

    #[tokio::test]
    async fn tag_names_filtered_by_scope() {
        // store has resource tag "service", span tag "http.method".
        let store = store_with_tags(&[(TagScope::Resource, "service"), (TagScope::Span, "http.method")]);
        let engine = crate::TraceqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
        let span_only = engine.tag_names("t1", Some(TagScope::Span), 0, i64::MAX).await.unwrap();
        let tags: Vec<&str> = span_only.iter().flat_map(|s| s.tags.iter().map(String::as_str)).collect();
        assert!(tags == vec!["http.method"]);
        assert!(!tags.contains(&"service"));
    }

    #[tokio::test]
    async fn tag_values_carry_static_type() {
        let store = store_with_tag_values("http.status_code", &[("int", "200"), ("int", "404")]);
        let engine = crate::TraceqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
        let vals = engine.tag_values("t1", "http.status_code", 0, i64::MAX).await.unwrap();
        assert!(vals.iter().all(|v| v.type_ == "int"));
        let mut got: Vec<&str> = vals.iter().map(|v| v.value.as_str()).collect();
        got.sort();
        assert!(got == vec!["200", "404"]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib discovery`
Expected: FAIL — `TraceqlEngine::tag_names`/`tag_values` engine methods missing.

- [x] **Step 3: Implement the delegation**

```rust
//! Tag discovery — the engine surface over `SpanStore`'s `TraceIndex`-backed
//! tag-name/value methods. `tag_names` filters/groups by `TagScope`; `tag_values`
//! returns each value with its TraceQL static type. The store (querier, Slice 5)
//! prunes blocks via the per-block tag sets/blooms; this layer only shapes scope
//! filtering and typing.

use crate::error::TraceqlError;
use crate::store::SpanStore;
use crate::types::{ScopedTag, TagScope, TypedValue};
use crate::{EngineOpts, TraceqlEngine};

impl<S: SpanStore> TraceqlEngine<S> {
    /// Discover tag names, optionally restricted to one `TagScope`.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        let all = self.store().tag_names(tenant, scope, start_ns, end_ns).await?;
        // store already honors `scope`; if `scope` is Some, keep only that scope's group.
        Ok(match scope {
            Some(s) => all.into_iter().filter(|t| t.scope == s).collect(),
            None => all,
        })
    }

    /// Discover typed values for one tag.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        self.store().tag_values(tenant, tag, start_ns, end_ns).await
    }
}
```

(`self.store()` is the Slice-2 accessor for the injected `Arc<S>`; if Slice 2 exposed the field differently, use its accessor.) Add `mod discovery;` to `lib.rs`.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib discovery`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): tag discovery — tag_names by scope + typed tag_values"
```

---

## Phase F — the curated golden-query corpus

### Task 10: Build the golden-query corpus + harness

**Files:**
- Create: `crates/traceql/testdata/golden/*.json` (one file per query family)
- Create: `crates/traceql/src/testkit.rs` (the `pub mod testkit` harness)
- Create: `crates/traceql/tests/golden_corpus.rs`
- Modify: `crates/traceql/src/lib.rs` (add `pub mod testkit;`)

**Interfaces:**
- Consumes: the `InMemorySpanStore` test double + the engine (`search`/`query_range`/`tag_names`/`tag_values`).
- Produces: a hand-authored golden corpus — each file is `{ fixture: <spans>, cases: [ { query, kind: "search"|"metrics"|"tags"|"values", expected: <...> } ] }` — and `pub mod testkit` exposing `pub fn load_fixture(..)` + `pub async fn run_golden_file(path)` (loads the fixture into an `InMemorySpanStore`, runs each case, and diffs against `expected`). This is a `pub` crate module (NOT `tests/support`) so the Slice-8 conformance gate can import it as `crabka_traceql::testkit::*`. **There is no upstream TraceQL `.test` corpus** (spec §6/§10) — this is the curated golden set diffed against documented semantics; the differential-vs-real-Tempo check is the Slice-8 headline.

- [x] **Step 1: Author the corpus files** (one family per file, expected values hand-computed against the spec)

Author at minimum:
- `selectors.json` — bare `.foo` (span+resource), `span.`/`resource.`/`parent.`/`event.`/`link.`/`instrumentation.` scopes; the single-span rule (`{A} && {B}` matches a trace where *different* spans satisfy each side).
- `structural.json` — all of `>>`/`<<`/`>`/`<`/`~` + `!>>`/`!<<`/`!>`/`!<` + `&>>`/`&<<`/`&>`/`&<`/`&~`, plus the two-roots-are-siblings edge (spec §6.3) and the sibling distinct-span predicate.
- `typed.json` — string/int/float/bool/duration/status/kind compares, anchored `=~`/`!~`, array any/none.
- `pipeline.json` — `select`/`by`/`coalesce`/`with` + the scalar aggregates + scalar filters.
- `metrics.json` — `rate`/`count_over_time`/`quantile_over_time(... by ...)`/the `_over_time` family + exemplars.
- `discovery.json` — `tag_names` per scope + typed `tag_values`.

> **Do not auto-generate expected values from the engine** (that would test the engine against itself). Hand-compute each `expected` from the documented TraceQL rule the corresponding task pinned; the corpus is an independent second opinion.

- [x] **Step 2: Write the harness + run it (it must fail first if any case is wrong)**

Create `crates/traceql/tests/golden_corpus.rs`:

```rust
use crabka_traceql::testkit::{load_fixture, run_golden_file};

macro_rules! golden {
    ($name:ident, $file:literal) => {
        #[tokio::test]
        async fn $name() {
            run_golden_file(concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/golden/", $file)).await;
        }
    };
}

golden!(selectors, "selectors.json");
golden!(structural, "structural.json");
golden!(typed, "typed.json");
golden!(pipeline, "pipeline.json");
golden!(metrics, "metrics.json");
golden!(discovery, "discovery.json");
```

`run_golden_file` parses the JSON (serde), loads the fixture spans into an `InMemorySpanStore`, dispatches each case by `kind` to `search`/`query_range`/`tag_names`/`tag_values`, and `assert!`s the normalized result equals `expected`. Implement it in the `pub mod testkit` crate module (declared `pub mod testkit;` in `lib.rs`, NOT `tests/support`) reusing the Slice-2 test double — Slice 8 imports `run_corpus_dir`/`Report` from this same module.

Run: `cargo test -p crabka-traceql --test golden_corpus`
Expected: initially may FAIL on a case where a hand-computed expectation reveals an implementation bug — fix the **implementation** (the relevant Phase A–E task's code), add a focused unit test next to it, re-run. Never edit `expected` to match a buggy engine.

- [x] **Step 3: Run to verify it passes**

Run: `cargo test -p crabka-traceql --test golden_corpus`
Expected: PASS for all families.

- [x] **Step 4: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "test(traceql): curated golden-query corpus + harness diffed against documented semantics"
```

---

### Task 11: Whole-crate gate across feature combinations

**Files:** none (verification only).

- [ ] **Step 1: Full test sweep (both feature settings)**

Run:
```bash
cargo test -p crabka-traceql
cargo test -p crabka-traceql --features experimental
```
Expected: all PASS.

- [ ] **Step 2: Clippy + fmt gate (both feature settings)**

Run:
```bash
cargo clippy -p crabka-traceql --all-targets
cargo clippy -p crabka-traceql --all-targets --features experimental
cargo fmt -p crabka-traceql --check
```
Expected: no warnings, formatting clean.

- [ ] **Step 3: Commit (if any fmt/clippy fixups were needed)**

```bash
git add crates/traceql/
git commit -m "chore(traceql): clippy/fmt clean across feature combinations for slice 3"
```

---

## Self-review

**Spec coverage (against §6 + §11 Slice 3):**

- **Full structural ops** — negated `!>>`/`!<<`/`!>`/`!<` (nested-set anti-join) → Task 1; union `&>>`/`&<<`/`&>`/`&<`/`&~` (both-sided spanSets) → Task 2. Both reuse the Slice-2 `SpanStructuralJoin` predicates unchanged, switching only the join mode (`StructuralMode`).
- **Typed/regex comparisons across all static types** — string/int/float/bool + anchored `=~`/`!~` + array any/none → Task 3; duration/status/kind (Go-duration parse + enum mapping) → Task 4. Covers every static type in the slice scope (string/int/float/bool/duration/status/kind).
- **`select()`/`by()` completeness** — additive `select` (doesn't narrow), `by` grouping, `coalesce`/`with`, scalar aggregates + scalar filters → Task 5.
- **TraceQL metrics** — time-bucketing kernel + `rate`/`count_over_time` → Task 6; `quantile_over_time`/the `_over_time` family + `by(...)` + experimental tier → Task 7; `TraceMetricsResponse` (Prometheus-shaped series) + `trace_id` exemplars + `query_range` wiring → Task 8.
- **Tag discovery** — `tag_names` by scope + typed `tag_values` over the `TraceIndex` (via `SpanStore`) → Task 9.
- **Conformance corpus** — research-flagged in the prompt: *no* upstream TraceQL `.test` corpus exists (confirmed against spec §6/§10), so this builds a curated golden corpus diffed against documented semantics → Task 10; the differential-vs-real-Tempo headline belongs to Slice 8 (per spec §10), referenced not duplicated here.

**Rule-fidelity (the subtle ones are pinned by a unit test BEFORE the golden corpus turns on):** the negated anti-join's "no structural counterpart" + the structural return-the-RIGHT-side rule (Task 1); union dedup of a span matching both sides (Task 2); anchored `=~` (substring must not match) + array any/none + numeric-not-lexical int compares + nil-as-presence (Task 3); duration→nanos + status/kind enum ints (Task 4); `select`-doesn't-narrow + `count() by ... > N` HAVING (Task 5); left-closed/right-open epoch-aligned bucketing (Task 6); per-quantile `p`-labeled series (Task 7); exemplar gating on `max_exemplars` (Task 8); scope-filtered tag names + typed values (Task 9). The corpus (Task 10) is the backstop that catches what the hand-written tests miss.

**Consumes (Slice 2, verbatim):** `TraceqlEngine<S: SpanStore>` (+ `new`/`search`/`query_range`/`trace_by_id`), `EngineOpts`, `#[async_trait] SpanStore` (`scan`/`trace_by_id`/`tag_names`/`tag_values`), `ScanResult { ctx, span_table }`, the result model (`SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`, `TraceSpans`, `TagScope`/`ScopedTag`/`TypedValue`/`AttrValue`, `SpanMatcher`, `TraceMetricsResponse`, `TraceqlError`), and the **`SpanStructuralJoin`** lowering for the core structural ops. Nested-set columns (`nested_set_left`/`right`/`parent_id`) come from slice 1's block-builder — this slice only reads them through the join predicates. The contract-vs-spelling escape hatch (adapt call sites if Slice 2 renamed a symbol; pin behavior, not names) is stated in Dependency & slice roadmap.

**Produces (this slice defines / completes):** `StructuralMode` (Match/Negated/Union) on `SpanStructuralJoin`; the typed-coercion layer (`StaticType`, `coerce_predicate`, `anchor_regex`, `parse_duration_ns`, `status_to_int`, `kind_to_int`); pipeline completeness (`select`/`by`/`coalesce`/`with` + scalar aggregates); the TraceQL-metrics planner (`bucket_index`/`bucket_starts`, the metric-function lowerings, `TraceMetricsResponse` population with `trace_id` exemplars, `query_range` wiring); the tag-discovery engine surface (`TraceqlEngine::tag_names`/`tag_values`); `experimental_enabled()` + the `experimental` feature; and the golden corpus + harness. Slice 5's Tempo HTTP API is a pure projection of these onto the Tempo JSON shapes — no new public types leak past `crabka-traceql`.

**Churn-prone DataFusion API handling:** the negated/union lowerings need `JoinType::Left`/`LeftAnti`/`union`/`distinct` on `LogicalPlanBuilder`, the typed layer needs `array_has`/`regexp_match` (or equivalents), and the metrics layer needs per-bucket aggregates (`approx_percentile_cont`); each is given as **structure + `// verify against rev 0838a4d`** and pinned by a TraceQL-query→expected behavioral test, never by a fabricated upstream signature. The pure kernels (`bucket_index`/`bucket_starts`/`parse_duration_ns`/`status_to_int`/`kind_to_int`/`anchor_regex`) carry no DataFusion surface and are pinned directly.

**Greenfield / no-back-compat respected:** the only feature flag (`experimental`) mirrors Tempo's own per-version maturity tier (spec §6.6), not a back-compat gate; no shims, no `V2` variants, no migration code. Slice-2 enums/registry shapes (`StructuralMode`, `EngineOpts`) are extended in place (e.g. `max_exemplars` added as a new `EngineOpts` field, not behind a flag).

**Parallelization note (for the executor):** Phase A Tasks 1→2 share `planner/structural.rs` → sequential. Phase B Tasks 3→4 share `planner/typed.rs` → sequential (Task 4 extends Task 3's `typed_lit`). Phase C Task 5 (`planner/pipeline.rs`) is disjoint from Phase B's files → can run *concurrently with* the Phase B batch. Phase D Tasks 6→7→8 chain through `metrics/` (6 creates the modules, 7 extends `functions.rs`, 8 extends `mod.rs`+`engine.rs`) → sequential. Phase E Task 9 (`discovery.rs`) is disjoint from Phase D except both touch `engine.rs` (Task 8 wires `query_range`, Task 9 adds `tag_names`/`tag_values`) → sequence 9 after 8 or reconcile the `engine.rs` edits. Phase F is inherently last (corpus → harness → gate). Recommended batches: {1}→{2}; {3}→{4} ∥ {5}; {6}→{7}→{8}→{9}; {10}; {11}.

**Placeholder scan:** no "TBD"/"add the rest"/"similar to Task N". Every implementation step has runnable code or a precise rule + the exact command to run. The bounded hand-waves — the join-mode/array/regex/percentile DataFusion builders and the `TraceMetricsResponse` field layout — are each explicitly tagged `// verify against rev 0838a4d` (or a shape note) and pinned by a behavior test, exactly as the plan's constraints require.
