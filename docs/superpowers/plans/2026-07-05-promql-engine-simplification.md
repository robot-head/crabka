# PromQL Engine Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `crates/promql/src/engine.rs` complexity by extracting aggregation internals into a focused private engine submodule while preserving PromQL behavior.

**Architecture:** Keep the public crate API unchanged by converting `engine.rs` into `engine/mod.rs`, then add `engine/aggregation.rs` for aggregate parameter parsing, reducer entry points, aggregate operation/state types, and aggregation-only helpers. `PromqlEngine`, `EngineOpts`, and orchestration remain in `engine/mod.rs`; moved helpers are imported with `use aggregation::{...}` and exposed only as `pub(super)` when needed by `mod.rs`.

**Tech Stack:** Rust 2024, `crabka-promql`, `promql-parser`, `DataFusion`, existing crate-local `MetricStore`, `InstantSample`, `SampleValue`, and `PromqlError` types.

## Global Constraints

- Preserve `crates/promql/src/lib.rs` public exports: `pub use engine::{EngineOpts, PromqlEngine};` must keep compiling.
- Preserve PromQL aggregate behavior for floats, native histograms, mixed sample groups, warnings, and experimental functions.
- Preserve existing feature gates, especially `experimental-functions`.
- Preserve existing error messages and error variants.
- Do not add compatibility shims or alternate behavior paths; Crabka is greenfield.
- Do not rewrite tests to assert source text or file layout.
- Use `cargo +nightly fmt --check` and `cargo test -p crabka-promql` as the first-slice verification gates.

---

## File Structure

- Rename: `crates/promql/src/engine.rs` -> `crates/promql/src/engine/mod.rs`
  - Responsibility: public engine types, query planning/evaluation orchestration, store scans, and imports of focused private engine submodules.
- Create: `crates/promql/src/engine/aggregation.rs`
  - Responsibility: aggregate parameter parsing, aggregate reducer entry points, aggregate state/operation types, and aggregation-local helpers.
- Modify: `docs/superpowers/specs/2026-07-05-promql-engine-simplification-design.md`
  - Responsibility: design record for this refactor slice; already written.
- Modify: `docs/superpowers/plans/2026-07-05-promql-engine-simplification.md`
  - Responsibility: this execution plan.

---

### Task 1: Extract Aggregation Internals

**Files:**
- Rename: `crates/promql/src/engine.rs` -> `crates/promql/src/engine/mod.rs`
- Create: `crates/promql/src/engine/aggregation.rs`

**Interfaces:**
- Consumes: existing private engine helpers/types currently in `engine.rs`, including `aggregate_labels`, `labels_key`, `InstantSample`, `SampleValue`, `PromqlError`, `Result`, `LabelModifier`, `AggregateExpr`, `Call`, `Expr`, and `TokenType`.
- Produces: `pub(super)` aggregation helpers imported by `engine/mod.rs`, including `aggregate_k`, `aggregate_quantile`, `apply_simple_aggregate`, `apply_k_aggregate`, `apply_quantile_aggregate`, `apply_count_values_aggregate`, and feature-gated experimental aggregate helpers when present.

- [ ] **Step 1: Rename the engine module file**

Run:

```bash
mkdir -p crates/promql/src/engine
mv crates/promql/src/engine.rs crates/promql/src/engine/mod.rs
```

Expected: `crates/promql/src/engine/mod.rs` exists and `crates/promql/src/engine.rs` no longer exists. `mod engine;` in `crates/promql/src/lib.rs` resolves to the new directory module without changing `lib.rs`.

- [ ] **Step 2: Declare and import the aggregation submodule**

At the top of `crates/promql/src/engine/mod.rs`, add this near the other module-level declarations, before regular `use` statements:

```rust
mod aggregation;
```

Then add imports for moved helpers after the existing `use` block is adjusted:

```rust
use aggregation::{
    aggregate_k, aggregate_quantile, apply_count_values_aggregate, apply_k_aggregate,
    apply_quantile_aggregate, apply_simple_aggregate,
};
```

If `experimental-functions` helpers are moved, keep their imports feature-gated:

```rust
#[cfg(feature = "experimental-functions")]
use aggregation::{apply_limit_ratio_aggregate, apply_limitk_aggregate};
```

Expected: `engine/mod.rs` can refer to moved aggregation entry points without changing call sites.

- [ ] **Step 3: Move the aggregation cluster into `aggregation.rs`**

Move the aggregation-specific definitions from `engine/mod.rs` into `crates/promql/src/engine/aggregation.rs`. The moved block starts at `string_literal_arg` if it is only used by aggregation tests or aggregate parsing, otherwise at `aggregate_k`, and includes the aggregation reducer helpers and aggregate operation/state definitions through the end of aggregation-only helper functions.

At the top of `aggregation.rs`, import exact dependencies from the parent module and crate. Start with this import block and let the compiler identify any missing or now-unused names:

```rust
use std::cmp::Ordering;
use std::collections::BTreeMap;

use promql_parser::parser::{AggregateExpr, Call, Expr, LabelModifier, TokenType};

use crate::error::{PromqlError, Result};
use crate::result::{InstantSample, SampleValue};

use super::{aggregate_labels, labels_key};
```

For moved functions that `engine/mod.rs` calls, change visibility from private `fn` to `pub(super) fn`. Keep helpers private if only `aggregation.rs` calls them. Example:

```rust
pub(super) fn aggregate_k(aggregate: &AggregateExpr) -> Result<usize> {
    // existing body unchanged
}
```

Expected: moved code bodies are unchanged except for visibility and imports.

- [ ] **Step 4: Preserve feature-gated experimental aggregation**

For every moved function or helper currently guarded by `#[cfg(feature = "experimental-functions")]`, keep the same guard in `aggregation.rs` and on the corresponding `use aggregation::{...}` import in `engine/mod.rs`.

Expected: both default and `experimental-functions` builds keep the same available symbols.

- [ ] **Step 5: Compile-check and fix module boundary errors**

Run:

```bash
cargo check -p crabka-promql
```

Expected: PASS. If it fails, fix only mechanical module-boundary issues:

- Add missing imports in `aggregation.rs` for types used by moved code.
- Change parent helpers used by `aggregation.rs` to `pub(super)` in `engine/mod.rs` only when required.
- Remove imports from `engine/mod.rs` that are no longer used after extraction.
- Do not change logic, match arms, error strings, or warning behavior.

- [ ] **Step 6: Format the refactor**

Run:

```bash
cargo +nightly fmt --check
```

Expected: PASS. If formatting fails, run `cargo +nightly fmt`, then rerun `cargo +nightly fmt --check`.

- [ ] **Step 7: Run targeted PromQL verification**

Run:

```bash
cargo test -p crabka-promql
```

Expected: PASS. If this is too slow or blocked by an unrelated environment issue, run the narrowest failing or relevant PromQL tests and record the exact blocker and command output.

- [ ] **Step 8: Review the diff for behavior drift**

Run:

```bash
git diff -- crates/promql/src/engine/mod.rs crates/promql/src/engine/aggregation.rs docs/superpowers/specs/2026-07-05-promql-engine-simplification-design.md docs/superpowers/plans/2026-07-05-promql-engine-simplification.md
```

Expected: diff shows file movement, private module extraction, visibility/import changes, and documentation only. It should not show changed aggregate formulas, changed error messages, changed warning text, or public API changes.
