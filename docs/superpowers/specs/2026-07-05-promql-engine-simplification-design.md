# PromQL Engine Simplification Design

## Goal

Refactor and simplify `crates/promql` by reducing the size and mixed responsibilities of
`crates/promql/src/engine.rs` without changing query behavior, public exports, feature flags,
or test expectations.

This first slice focuses on the aggregation area because it is a large self-contained cluster
inside `engine.rs`: parameter parsing, aggregate state, aggregate finishing, histogram-aware
aggregation behavior, and helpers shared by interpreter and physical-operator paths.

## Current State

`engine.rs` is over 21,000 lines and combines public engine types, query planning/evaluation,
store scans, aggregation reducers, histogram helpers, range functions, selector matching, time
modifier logic, caches, and tests. The public crate root only exposes `EngineOpts` and
`PromqlEngine` from this module, so most of the file can be reorganized behind private module
boundaries without public API churn.

The aggregation cluster begins after the main `PromqlEngine` impl and contains helpers such as
`aggregate_k`, `aggregate_quantile`, `apply_simple_aggregate`, `apply_k_aggregate`,
`apply_quantile_aggregate`, `apply_count_values_aggregate`, `AggregateOp`, and
`AggregateState`. These helpers are already free functions or private types, which makes them a
good first extraction target.

## Design

Convert `engine.rs` into an `engine` module directory while preserving the existing external
module path:

- Move the current file to `crates/promql/src/engine/mod.rs`.
- Add `crates/promql/src/engine/aggregation.rs` for aggregation-specific private helpers and
  types.
- Keep `PromqlEngine`, `EngineOpts`, and the main query orchestration code in `engine/mod.rs`.
- Expose moved helpers only with `pub(super)` when `engine/mod.rs` still calls them.
- Keep helpers private inside `aggregation.rs` when they are only implementation details of that
  module.

The first implementation should move aggregation logic only. Histogram helper extraction,
range-function extraction, selector extraction, and test relocation are follow-up slices unless
the aggregation move requires a tiny adjacent helper to preserve compilation.

## Behavior Preservation

No semantic changes are intended. The refactor must preserve:

- Public API from `crates/promql/src/lib.rs`.
- PromQL aggregate semantics for floats, native histograms, mixed sample groups, warnings, and
  experimental aggregate functions.
- Existing feature gates, especially `experimental-functions`.
- Existing error messages and error variants.
- Existing test behavior without rewriting tests to assert source layout.

Any helper signature changes should be mechanical and local to the `engine` module. Avoid adding
compatibility shims or alternate code paths.

## Verification

Run these checks after the first slice:

- `cargo +nightly fmt --check`
- `cargo test -p crabka-promql`

If time permits, also run the workspace clippy command used by the project. If the full PromQL
test suite is too slow or blocked by an unrelated environment issue, run the most targeted
available PromQL tests and record the blocker explicitly.

## Follow-Up Slices

After aggregation is isolated and verified, continue simplifying `engine.rs` by extracting other
cohesive private modules in separate reviewable slices:

- `histogram.rs` for reusable histogram math and bucket compatibility helpers.
- `range_functions.rs` for range vector and over-time helpers.
- `selector.rs` for label matcher compilation and selector time-bound helpers.
- Test support relocation if private-module boundaries make tests hard to navigate.

Each slice should preserve behavior, keep public exports stable, and be verified independently.
