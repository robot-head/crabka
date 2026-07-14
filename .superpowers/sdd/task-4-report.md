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

## Join protocol slice

Status: PROTOCOL_COMPLETE; OWNER_EXECUTION_AND_SQL_DISPATCH_PENDING.

This slice adds the typed, owned `JoinRangeRequest`/`JoinRangeResult` boundary and an object-safe `RangeScanner::join` seam. The default validates then returns an explicit unsupported error; it never manufactures production rows. `gres-ranges` carries matching `JoinRange` request/response variants, validates requests at the hosted-service boundary, and likewise returns an explicit unsupported response because owner execution is deliberately outside this slice. SQL planning/executor dispatch remains pending.

### RED/GREEN evidence

- RED: `cargo test -p crabka-pgexec join_protocol_tests --lib` failed with missing join types, bounds, validation errors, and `RangeScanner::join` (`E0407`, `E0425`, `E0422`, `E0433`, exit 101).
- GREEN: the same focused pgexec command passed 2/2, including the whole-request fake seam and broadcast-count rejection.
- GREEN: `cargo test -p crabka-gres-ranges join_range --lib` passed 2/2 request/result serde roundtrip and near-limit/rejection tests.
- GREEN: `cargo test -p crabka-gres-ranges bounded_framing_rejects_oversized_join_request --lib` passed 1/1.
- GREEN: `cargo check -p crabka-pgexec --all-targets` and `cargo check -p crabka-gres-ranges --all-targets` exited 0.
- GREEN: `cargo test -p crabka-pgexec --all-targets -q` passed every target (347 library tests plus all integration targets).
- PARTIAL: `cargo test -p crabka-gres-ranges --all-targets -q` passed 164 library tests and the next 21-test target, then the previously documented `real_range_partition_aborts_transfer_and_heal_restores_2pc` process test failed during readiness because the preserved dirty `control.rs` reports the range-zero receipt engine unavailable.
- GREEN: changed-file `rustfmt --edition 2024 --check` and `git diff --check` exited 0; stable rustfmt emitted only repository nightly-option warnings. Whole-worktree `cargo fmt --all -- --check` remains blocked by the preserved pre-existing dirty `crates/gres-ranges/src/control.rs`.

### Files and commit

- `crates/pgexec/src/scanner.rs`, `crates/pgexec/src/lib.rs`
- `crates/gres-ranges/src/transport.rs`, `crates/gres-ranges/src/forward.rs`, `crates/gres-ranges/src/coordinator.rs`, `crates/gres-ranges/src/lib.rs`
- Implementation/test commit: `bddba50d feat(gres): add typed distributed join protocol`.
- This report is committed separately.

### Contract and bounds

- The request binds local/global snapshots, a mandatory nonzero read timestamp, optional own xid/start timestamp, join kind, paired key indexes, selected strategy, both table ids/names/rowid intervals, per-side predicate metadata, output projection, and optional broadcast rows.
- Rows use deterministic tuple bytes ordered by the request projection. Both broadcast and result rows are independently validated.
- Limits: 16 join keys, 256 projection columns, 256 predicates per side, 65,536 xids per snapshot, 8,192 broadcast rows, 256 KiB per encoded row, 65,536 result rows, plus the existing 1 MiB transport frame ceiling.
- Validation also rejects empty/mismatched join keys, invalid table identities/intervals, malformed snapshots, missing read timestamps, and broadcast payloads inconsistent with the chosen strategy.

### Concerns and pending work

- Owner-side join execution and SQL executor dispatch are explicitly still pending; callers receive unsupported/failure rather than fake results.
- The 1 MiB frame limit is intentionally stricter than the aggregate broadcast-row and result-row caps, so realistic RPCs hit bounded framing before theoretical aggregate maxima. Future execution should page/stream results rather than relaxing framing.
- Pre-existing dirty `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` were preserved and excluded from both commits.

## Join owner execution slice

Status: OWNER_AND_GATEWAY_EXECUTION_COMPLETE; SQL_DISPATCH_PENDING.

### RED/GREEN evidence

- RED: `cargo test -p crabka-pgexec join_protocol_tests::materialized --lib` failed with `E0425` because `execute_materialized_join` did not exist.
- RED: `cargo test -p crabka-gres-ranges scan_range_service_executes_broadcast_join_on_owner --lib` reached the real service and failed with `Error { message: "expected scan_range rpc" }`.
- GREEN: `cargo test -p crabka-pgexec join_protocol_tests --lib` passed 5/5, including 64 generated whole-value comparisons of broadcast-left, broadcast-right, and co-partitioned materialized execution against one gathered reference.
- GREEN: `cargo test -p crabka-gres-ranges join_range --lib` passed 2/2 typed wire/bound tests.
- GREEN: `cargo test -p crabka-gres-ranges scan_range_service_executes_broadcast_join_on_owner --lib` passed the exact `RangeService::handle(JoinRange)` owner integration test.
- GREEN: `cargo check -p crabka-pgexec --all-targets` and `cargo check -p crabka-gres-ranges --all-targets` exited 0.
- GREEN: changed-file `rustfmt --edition 2024` and `git diff --check` exited 0; stable rustfmt emitted only the repository's existing nightly-option warnings.

### Commit and semantics

- `f0c79557 feat(gres): execute distributed joins at range owners`.
- The shared production join primitive decodes bounded tuple payloads, filters each side before joining, implements inner equi-join semantics with NULL keys never matching, projects from `[left..., right...]`, rejects invalid key/projection indexes, sorts encoded results deterministically, and enforces per-fragment and merged result bounds.
- Broadcast execution obtains the small side through the existing scan seam, enforces the broadcast-row cap while materializing, and sends that bounded payload to every large-side owner. Owners scan only their requested large-side interval at the request's snapshots/read timestamp.
- Co-partitioned execution sends matching intervals to each owner only after identical catalog hash layout and co-location are proven. Owners independently reject unproven requests; the gateway deterministically falls back to gather when proof is absent.
- Gather obtains both sides through `ScanRange`/local-owner scan paths and executes the same shared join primitive at the gateway. Strategy outputs are merged and sorted identically.
- `HostedRangeService`, `RangeScanService`, and `RegistryRangeScanner` now expose production `JoinRange` execution. SQL planner/executor selection and dispatch are explicitly still pending for the next slice.

### Concerns

- This slice intentionally executes only `JoinKind::Inner`; left/right/full variants are rejected explicitly rather than partially emulated.
- The 1 MiB bounded frame remains stricter than aggregate row-count limits, so large broadcast/result payloads still require future paging/streaming work.
- The preserved dirty `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` were not staged, reverted, or included in the feature commit.
- The full gres-ranges all-target test retains the same unrelated process-test failure already documented by the GROUP BY slice; focused join tests, all library tests, and all-target compilation are green.

## Join SQL dispatch completion

Status: COMPLETE_WITH_DOCUMENTED_FALLBACKS.

### RED/GREEN evidence

- RED: `cargo test -p crabka-pgexec --test distributed_pushdown sql_inner_equi_join_dispatches_selected_broadcast_request --no-run` failed with `E0599` because `SqlEngine` had no injected join statistics/configuration seam (exit 101).
- RED: after adding the seam, `cargo test -p crabka-pgexec --test distributed_pushdown sql_inner_equi_join_dispatches_selected_broadcast_request -- --nocapture` reached SQL execution and failed because the recorded join request count was zero (exit 101).
- GREEN: the same focused broadcast SQL dispatch test passed 1/1 after executor dispatch was added.
- RED: `cargo test -p crabka-pgexec --test distributed_pushdown sql_copartitioned_join_requires_the_join_key_to_match_hash_metadata -- --nocapture` failed because the request selected `CoPartitioned` instead of `Gather` (exit 101).
- GREEN: the same exact-key eligibility test passed 1/1 after requiring each SQL join key to equal its table's exact hash-sharding column list.
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture` passed 42/42 before the final exact-key regression was added; the final all-target run passed the resulting 43-test distributed-pushdown target.
- GREEN: `cargo test -p crabka-pgexec --lib join_protocol_tests -- --nocapture` passed 5/5.
- GREEN: `cargo test -p crabka-gres-ranges registry_range_scanner_executes_sql_join_end_to_end --lib -- --nocapture` passed 1/1 and exercised SQL planning through `RegistryRangeScanner` into production owner/gateway join execution.
- RED/GREEN compatibility correction: the first `cargo test -p crabka-pgexec --all-targets -q` exposed an existing `pg_catalog` join as `42P01`, because the new pre-pass attempted a physical-table lookup before virtual-catalog fallback. `cargo test -p crabka-pgexec --test introspection pg_catalog_exposes_user_tables_columns_types_and_indexes -- --nocapture` reproduced it; treating non-physical relations as ineligible restored that test and the full all-target gate.

### Commit and implementation

- `fa55a0ac feat(gres): dispatch distributed SQL joins`.
- `SqlEngine` owns an `Arc<dyn Stats>` and `PlannerConfig`, clones them into sessions, and binds them to the statement-scoped timestamp scanner. There is no mutable global planner state.
- Eligible plans are direct, physical, sharded, non-foreign inner joins with one qualified column-equality `ON` predicate. The executor calls the existing validated planner, maps its answer to `BroadcastLeft`, `BroadcastRight`, `CoPartitioned`, or `Gather`, constructs a snapshot-bound `JoinRangeRequest`, and calls the production `RangeScanner::join` seam.
- The timestamp decorator overwrites the request's read timestamp and optional own transaction start timestamp with the single statement read point before dispatch. Local/global MVCC snapshots and own xid are copied exactly into the owned request.
- Co-partitioned dispatch requires both the prior identical hash layout/co-location proof and exact equality between the SQL join keys and each side's hash column list. Any missing proof selects gather.
- Successful RPC rows decode into the original `[left..., right...]` relation scope, so the unchanged executor retains residual `WHERE`, SQL projection, aliases, ordering, aggregate, and error behavior. NULL join keys remain non-matching in the shared owner primitive.
- Unsupported join kinds, USING/NATURAL/cross joins, derived/CTE/view/virtual relations, unqualified or non-simple predicates, mixed sharded/unsharded joins, and scanner `Unsupported` responses use the existing recursive local join deterministically. Non-`Unsupported` remote errors remain errors.
- Golden SQL tests inspect the actual `JoinRangeRequest` for every selected strategy. A deterministic 64-row-per-side generated corpus, including NULL keys and duplicate matches, compares whole SQL results for broadcast, co-partitioned, and gather against the unoptimized local engine.

### Final Task 4 verification checklist

- `cargo test -p crabka-pgexec --all-targets -q`: passed every pgexec target (347 library tests and all integration targets, including the final 43-test distributed pushdown target).
- `cargo check -p crabka-gres-ranges --all-targets`: passed.
- `cargo test -p crabka-gres-ranges --lib -q`: passed 165/165.
- `./scripts/gres-sharded-conformance.sh`: passed all four legs and wrote `target/gres-sharded-conformance-artifacts/sharded-conformance.json`.
- Changed-file `rustfmt --edition 2024 --check` and `git diff --check`: passed; stable rustfmt emitted only the repository's existing nightly-option warnings.
- Predicate/projection/scalar and grouped partial aggregation/top-K pushdowns, statistics selection, typed bounded join protocol, owner/gateway join execution, and SQL strategy dispatch are now implemented and covered.
- `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` remained unstaged and were excluded from the feature and report commits.

### Remaining concerns

- SQL distributed dispatch intentionally recognizes only the narrow directly-provable equi-join shape described above; all other correct SQL shapes retain the local executor rather than risking semantic drift.
- Join RPC framing remains capped at 1 MiB. Large broadcast inputs/results fail through the existing bounded framing/row validation path; paging/streaming remains future work.
- The previously documented dirty `control.rs` process-test concern is unchanged. The affected library and all-target compile gates, production registry SQL integration, and authoritative sharding conformance gate are green.

## Independent review remediation

Status: COMPLETE.

### Exact co-partition keys

- RED: `cargo test -p crabka-gres-ranges scan_range_service_rejects_copartitioned_join_on_non_hash_keys --lib -- --nocapture` reached the hosted owner and failed because identical hash layout was accepted for join key indexes `[1]`/`[1]` while both tables shard on column `[0]`.
- GREEN: the same command passes. `co_partitioned_join_keys_match` resolves the catalog hash-column names to ordered indexes and requires exact equality on both sides. The gateway changes an unproved direct/future `CoPartitioned` request to `Gather`; the owner independently returns `0A000`. Existing exact-key SQL selection and production registry integration remain green.

### Runtime statistics sources

- RED: `cargo test -p crabka-pgexec --test distributed_pushdown production_engine_stats_follow_durable_table_sequence --no-run` failed because `SqlEngine` exposed no production stats handle and still constructed an empty map fixture.
- GREEN: the executable test passes before/after real committed inserts. `DurableSequenceStats` reads the authoritative applied `crabka_pgkv::key::seq_key(TableId)` through the engine KV on every estimate; `SqlEngine::with_kv` and `SqlEngine::replicated` install it by default and sessions retain the shared `Stats` handle.
- The landed durable checkpoint source is `crabka_gres_substrate::checkpoint::runtime::CheckpointMetadata`, produced only after manifest/part presence and checksum validation. It now implements the fakeable `Stats` interface read-only, using verified range/tenant `total_bytes` as a conservative per-table upper bound. `CheckpointStats` was deliberately not used: its own contract says its resettable scheduling counters must never be exported as live range statistics. `cargo test -p crabka-gres-substrate verified_checkpoint_metadata_is_a_read_only_stats_source --lib -- --nocapture` passes against the real metadata type.

### Broadcast transport capacity

- RED: `cargo test -p crabka-gres-ranges join_request_transport_capacity_has_exact_materialized_boundary --lib --no-run` failed because `JoinRangeReq` had no transport-capacity decision.
- GREEN: the test binary-searches the exact serialized boundary: the largest full `RangeRequest::JoinRange` JSON (enum/request overhead plus materialized tuple bytes) at or below `MAX_FRAME_BYTES = 1,048,576` is accepted and one additional tuple byte is rejected.
- After the gateway materializes the chosen broadcast side, it encodes the request for every target range before sending any join RPC. If any fully encoded request exceeds 1 MiB (regardless of the 64 MiB planning hint), it clears broadcast rows, changes the strategy to `Gather`, materializes both sides under the unchanged snapshots/read timestamp, and executes the shared join primitive locally. No partial broadcast can occur before this decision.

### Verification and commits

- `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture`: 44/44 passed.
- `cargo test -p crabka-gres-ranges --lib -q`: 167/167 passed.
- `cargo check -p crabka-pgexec --all-targets` and `cargo check -p crabka-gres-ranges --all-targets`: passed.
- `cargo test -p crabka-gres-ranges registry_range_scanner_executes_sql_join_end_to_end --lib -- --nocapture`: passed through production SQL planning and registry owner/gateway execution.
- `./scripts/gres-sharded-conformance.sh`: all four legs passed; artifact refreshed at `target/gres-sharded-conformance-artifacts/sharded-conformance.json`.
- Changed-file `rustfmt --edition 2024`, changed-file check mode, and `git diff --check`: passed with only the repository's stable-rustfmt nightly-option warnings.
- Implementation commit: `11491421 fix(gres): harden distributed join planning`.
- This report is committed separately.

The pre-existing dirty `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` remain unstaged and were neither reverted nor included.

## Checkpoint runtime wiring completion

Status: COMPLETE.

- RED: `cargo test -p crabka-gres-substrate verified_checkpoint_publication_changes_next_join_plan --lib --no-run` failed with unresolved `CombinedStats` and no `CheckpointService::planner_stats` method.
- GREEN: `cargo test -p crabka-gres-substrate verified_checkpoint_publication_changes_next_join_plan --lib -- --nocapture` passed. The integration constructs a production `SqlEngine` over real durable sequence keys, observes `BroadcastLeft` at a pinned 64-byte threshold, completes an authoritative in-process checkpoint, and observes the next plan choose `Gather` from the newly verified checkpoint size. It also constructs a restarted checkpoint service, loads the existing manifest through `latest_checkpoint_metadata`, publishes that verified metadata, and observes the restored estimate.
- `CheckpointPlannerStats` is one shared `RwLock<Option<CheckpointMetadata>>` read adapter, not a second metadata model. `CheckpointService` publishes the exact `CheckpointMetadata` returned by `latest_checkpoint_metadata` only after manifest/part validation and successful prune handling. Publication clones metadata while no async operation is pending; planner reads take a synchronous read lock and never cross an await.
- Production in-memory, live single-range, live multi-range, and staged-successor construction pass the same shared adapter into `CombinedStats` alongside `DurableSequenceStats`. Existing engines and their sessions share that `Arc`, so every successful checkpoint completion updates subsequent plans without rebuilding sessions. Startup calls the same verified metadata loader before exposing the engine, covering restart. Fake `Stats` injection remains unchanged.
- `CheckpointStats` remains only the resettable checkpoint scheduling counter source, honoring its explicit prohibition against use as live range statistics.
- GREEN: `cargo test -p crabka-pgexec --test distributed_pushdown -- --nocapture` passed 44/44.
- GREEN: `cargo check -p crabka-pgexec --all-targets`, `cargo check -p crabka-gres-ranges --all-targets`, `cargo check -p crabka-gres-substrate --all-targets`, and `cargo check -p crabka-gres --all-targets` passed (only existing process-harness dead-code warnings).
- GREEN: `./scripts/gres-sharded-conformance.sh` passed all four legs and refreshed `target/gres-sharded-conformance-artifacts/sharded-conformance.json`.
- GREEN: changed-file rustfmt/check mode and `git diff --check` passed with only stable-rustfmt warnings for repository nightly options.
- Feature commit: `67d5f761 fix(gres): publish checkpoint stats to join planning`.
- This report is committed separately.

The preserved dirty `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` were not staged, reverted, or included.

## Production co-partition selection remediation

Status: COMPLETE.

- RED: `cargo test -p crabka-gres-substrate verified_checkpoint_publication_changes_next_join_plan --lib --no-run` failed because the requested table-aware API did not accept join keys; before the fix the same production `DurableSequenceStats` + `CheckpointPlannerStats` composition returned default `are_co_partitioned = false`, causing an exact catalog-proven pair to Gather.
- GREEN: the executable integration passes. With large pinned estimates and broadcast disabled, matching real `Table` hash columns, bucket count, and co-location group plus exact join indexes select `CoPartitioned`; mismatched group and mismatched key indexes select `Gather`.
- `plan_join_for_tables` now uses `Stats` only to select a broadcast side by size. If no side fits, `co_partitioned_join_keys_match` is the sole eligibility proof. Generic `plan_join` retains its fakeable compatibility seam, but production SQL calls `join_strategy_for_keys`, passing the resolved exact SQL column indexes.
- `cargo test -p crabka-pgexec --test distributed_pushdown sql_copartitioned_join_uses_catalog_proof_when_stats_only_estimate_sizes -- --nocapture` passes and inspects the actual emitted `JoinRangeRequest` strategy. The stats source is `SequenceCounters`, whose co-partition answer is the default false, proving SQL dispatch is catalog-derived.
- Minor disposition: checkpoint manifests expose only range/tenant `total_bytes`, not per-table bytes. `CheckpointPlannerStats` is now explicitly documented and tested as a conservative global upper bound: every table id receives the identical latest verified checkpoint total. No per-table metadata was invented.
- GREEN: full `distributed_pushdown` passed 45/45; pgexec, gres-ranges, and gres-substrate all-target checks passed; `scripts/gres-sharded-conformance.sh` passed all four legs; changed-file rustfmt check and `git diff --check` passed with only repository stable-rustfmt nightly-option warnings.
- Feature commit: `3c3f9c1b fix(gres): derive co-partition joins from catalog`.
- This report is committed separately.

The preserved dirty `.superpowers/sdd/progress.md` and `crates/gres-ranges/src/control.rs` remain the only unstaged changes and were not modified or reverted.
