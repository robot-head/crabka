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

## GROUP BY completion

Status: DONE_WITH_CONCERNS

### RED/GREEN evidence

- RED: `cargo test -p crabka-pgexec --test distributed_pushdown grouped_partial_count_merges_range_groups_in_deterministic_key_order --no-run` failed with `E0599`: no method named `grouped_by` on `PartialAggregateSpec` (exit 101).
- RED: `cargo test -p crabka-pgexec --test distributed_pushdown sql_grouped_aggregates_request_partial_pushdown_and_match_full_scan -- --nocapture` ran the new SQL equivalence test and failed because no recorded scan contained `group_by == vec![2]` (exit 101).
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown grouped_ -- --nocapture` passed 3/3, including 64 generated datasets x COUNT/SUM/MIN/MAX/AVG-parts whole-value equivalence checks.
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture` passed 38/38.
- GREEN: `cargo test -p crabka-gres-ranges registry_range_scanner_merges_remote_grouped_avg_parts --lib -- --nocapture` passed 1/1.
- GREEN: `cargo test -p crabka-gres-ranges loopback_transport_round_trips_scan_range_payload --lib -- --nocapture` passed 1/1.
- GREEN: `cargo test -p crabka-pgexec --all-targets` passed all pgexec unit and integration targets (342 library tests plus every integration binary, including 38/38 distributed pushdown tests).
- GREEN: `cargo test -p crabka-gres-ranges --all-targets -q` passed the 160 library tests and the next 21-test target, then failed in the unrelated process test described under concerns.
- GREEN: `cargo check -p crabka-gres-ranges --all-targets` exited 0.
- GREEN: `cargo fmt --all` and `git diff --check` exited 0; stable rustfmt emitted only the repository's existing nightly-option warnings.

### Files and commits

- Production/tests: `crates/pgexec/src/scanner.rs`, `crates/pgexec/src/plan_dist.rs`, `crates/pgexec/src/exec.rs`, `crates/pgexec/tests/distributed_pushdown.rs`, `crates/gres-ranges/src/transport.rs`, `crates/gres-ranges/src/forward.rs`.
- Production/test commit: `239a6449 feat(gres): push grouped partial aggregates`.
- This report is committed separately.

### Semantics

- `PartialAggregateSpec` and `WirePartialAggregateSpec` carry typed zero-based group-column indexes; the wire field uses `serde(default)` for backward-compatible scalar requests.
- Each owner filters before grouping and returns rows shaped as `[group keys..., aggregate state...]`. COUNT/SUM/MIN/MAX use one state datum; AVG uses exact numeric sum plus checked int8 count.
- The gateway coalesces equal keys across ranges, treats NULL keys as one group, validates partial row shapes, preserves scalar aggregate NULL/type behavior, and finalizes AVG only after global sum/count merge.
- Empty grouped input returns zero rows. Deterministic ordering is ascending by group keys with NULL last. SQL pushdown is intentionally limited to group columns followed by one supported non-DISTINCT aggregate, with optional matching ascending `ORDER BY`; other grouped shapes retain the existing local path.
- Randomized coverage compares partitioned pushdown to a single-range whole-value result for 64 datasets, four owner partitions, NULL keys/values, and all five aggregate functions.

### Concerns

- `cargo test -p crabka-gres-ranges --all-targets -q` and a direct rerun of `real_range_partition_aborts_transfer_and_heal_restores_2pc` fail during compute readiness with `range transfer is unavailable for r0: current range-zero receipt engine is unavailable`. The failure is attributed to the pre-existing dirty `crates/gres-ranges/src/control.rs`, outside this GROUP BY slice; focused remote, transport, library, and compile gates pass.
- `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` were not staged, reverted, or included in either GROUP BY commit.

### Cleanup correction

`cargo fmt --all` created formatter-only spill in `crates/gres-ranges/tests/harness/process.rs`, `crates/gres/src/lib.rs`, `crates/gres/src/split_activation.rs`, and `crates/gres/tests/topology_process_split_crash.rs`. Those four unstaged diffs were removed before handoff. They were not genuine concurrent changes and are not part of the all-target failure attribution; that concern remains attributed only to the pre-existing dirty `crates/gres-ranges/src/control.rs`.
