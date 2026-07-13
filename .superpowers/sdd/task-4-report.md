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
- Top-K gateway merge now consumes ordered range-local streams incrementally and retains at most K output rows.

## Continuation evidence

- RED: `cargo test -p crabka-pgexec --test distributed_pushdown k_way_top_k --no-run` failed because `merge_top_k_streams` did not exist.
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown k_way_top_k -- --nocapture` passed; the 64-seed whole-value property checks equivalence to global ordering and the output bound.
- GREEN: `cargo test -p crabka-gres-ranges registry_range_scanner_merges_remote_top_k_deterministically --lib` passed.
- Commit `f1f6bfc9 feat(gres): merge range top-k streams incrementally` replaces gateway global re-sort with an incremental range-stream merge retaining at most K output rows.
- Authoritative planner-enabled sharding gate: `./scripts/gres-sharded-conformance.sh` passed and wrote `target/gres-sharded-conformance-artifacts/sharded-conformance.json`. Its four legs passed: sharded visibility, multirange global visibility, pgexec global decisions, and pgexec sharded seams.

Remaining required implementation gaps after continuation: GROUP BY owner partials/gateway merge and actual executor dispatch of the three join strategies. The join planner selection seam alone is insufficient to claim Task 4 complete.
