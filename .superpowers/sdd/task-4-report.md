# G9 Task 4 report

Status: DONE_WITH_CONCERNS

## Scope and landed-code audit

Base `516478a0` already contained the distributed scan seam in `crates/pgexec/src/plan_dist.rs`, executable predicate/projection/scalar aggregate/top-K support in `crates/pgexec/src/scanner.rs`, the `ScanRange` wire fields in `crates/gres-ranges/src/transport.rs`, range-owner execution and gateway merge in `crates/gres-ranges/src/forward.rs`, and equivalence/integration coverage in `crates/pgexec/tests/distributed_pushdown.rs`. Those landed implementations were retained.

This slice added the missing fakeable `Stats` seam, sequence-counter and checkpoint-metadata adapters, threshold configuration, and deterministic broadcast/co-partitioned/gather join selection with a golden table test.

## RED/GREEN evidence

- RED: `cargo test -p crabka-pgexec --test distributed_pushdown join_strategy_golden --no-run` failed with unresolved imports for `Stats`, statistics adapters, join inputs/config/strategy, and `plan_join`.
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture` passed 34/34 tests, including both new tests and all existing pushdown equivalence/SQL tests.

## Commits

- `d366b7f2 feat(gres): select distributed join strategies from stats`
- final integration/report commit: `feat(gres): distributed pushdown execution`

## Files

- `crates/pgexec/src/plan_dist.rs`
- `crates/pgexec/tests/distributed_pushdown.rs`
- `.superpowers/sdd/task-4-report.md`

Pre-existing dirt in `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` was not staged, modified, or reverted by this work.

## Verification

- `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture`: 34 passed.
- `cargo fmt --all -- --check`: passed; stable rustfmt emitted only repository nightly-option warnings.
- Focused `crabka-gres-ranges` remote top-K tests and `cargo check` for both affected crates/all targets completed successfully in the chained verification gate (exit 0).

## Divergences and concerns

- The brief contains no exact numeric broadcast threshold, so the public default is 64 MiB; tests use an explicit 64-byte threshold and pin boundary behavior.
- GROUP BY partial aggregation/gateway merge is not present in the landed seam and was not safely completed in this slice. Scalar COUNT/SUM/MIN/MAX/AVG-parts remote execution is covered.
- Join selection is a complete deterministic planning seam, but the SQL join executor does not yet dispatch broadcast/co-partitioned remote join fragments.
- The Docker-backed corpus-through-sharding gate was not run in this environment; existing planner-enabled SQL equivalence tests remained green.
- Top-K gateway code globally re-sorts merged range-local K rows; it is behaviorally equivalent and deterministic, but it is not implemented as a heap-based streaming K-way merge.
