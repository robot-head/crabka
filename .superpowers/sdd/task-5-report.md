# G9 Task 5 report

Status: BLOCKED

## Landed-seam audit

Base `aa18718bb7f8493a3070edb882254e609c6e7f44` already contained the broad Task 5 vertical slice in the earlier bulk commit `d9489c823`: parser AST and `SHARDED BY HASH`; catalog `HashSharding`; bucket-leading `pgkv`/`pgmvcc` keys; `RangeKey { table_id, bucket, rowid }`; equality/scatter routing; layout co-location validation; registry `HashPlacement`; and the G9 Task 4 co-partitioned join execution arm. Those seams were re-read and tested rather than treated as completion.

The audit found two concrete fail-open/drift gaps, fixed in `2e723943`:

- sharding catalog payloads were not independently versioned, accepted trailing bytes, and accepted empty column/group names;
- range routing duplicated the physical FNV-1a implementation instead of using `pgkv` as the single bucket authority, and validated specs accepted empty names.

`crates/gres-ranges/src/control.rs` remained un-staged and unmodified by this task.

## RED/GREEN evidence

RED:

- `hash_spec_rejects_empty_column_and_group_names` failed because `HashShardSpec::new` accepted malformed names.
- `hash_sharding_decode_rejects_trailing_bytes_and_empty_names` failed first on accepted trailing bytes.

GREEN:

- constructors/decoders now fail closed for empty names, invalid versions, invalid tags/counts and trailing bytes;
- range routing delegates bucket calculation to `crabka_pgkv::key::hash_bucket`;
- the cross-layer corpus pins routing and physical encoding to identical results.

## Hash algorithm and corpus

Algorithm: 64-bit FNV-1a, offset basis `0xcbf29ce484222325`, prime `0x100000001b3`, byte-at-a-time XOR then wrapping multiply. A validated power-of-two bucket count uses `hash & (bucket_count - 1)`.

Pinned 16-bucket corpus (bytes -> bucket): empty -> 5; `a` -> 12; `alpha` -> 11; `alice` -> 7; `[00 ff 01]` -> 3.

## Commits

- `2e723943 feat(gres): hash sharding as bucket intervals`
- this report is committed separately.

## Tests and results

- parser/catalog/pgkv/pgmvcc library suites: 308 passed.
- registry hash validation: 1 passed.
- gres-ranges hash-focused library tests: 5 passed.
- hash-sharded multirange integration: 4 passed.
- co-partitioned join selection/execution focus: 1 passed.
- unchanged G8 `split_model`: 5 passed.
- unchanged G8 `split_nemesis`: 4 passed.
- `git diff --check` for changed feature files: clean.
- `cargo fmt --all -- --check`: blocked by the pre-existing unstaged formatting delta in `crates/gres-ranges/src/control.rs`; the changed feature files are rustfmt-clean.

## Brief-clause review

- Exact hash syntax parsing, power-of-two rejection, AST/catalog roundtrip, bucket-leading pgkv/pgmvcc encoding, equality routing, scatter routing, registry/CRD metadata, layout co-location validation, and the co-partitioned join arm were present on the base and exercised by their existing tests.
- This change adds independent catalog versioning, strict decode completion, validated non-empty names, one authoritative physical/routing hash implementation, and an exact deterministic corpus.
- The unchanged split model and split nemesis prove the generic interval machinery remains green.

## Divergences and concerns

- No new balancer-free operation-sequence property test was added. The landed map constructor validates corresponding group buckets after each reconstructed layout and therefore rejects a violating transition, but the repository still lacks the requested dedicated generated sequence model that performs coordinated group split/move operations.
- The existing G8 split/nemesis tests use ordinary `SHARDED` tables; they were run unchanged, but no practical multiprocess crash gate was run with a `SHARDED BY HASH` workload in this task.
- The parser supports multiple hash columns and the optional existing `COLOCATED WITH group` suffix in addition to the brief's singular spelling. The exact singular spelling is accepted and invalid bucket counts are rejected, but a standalone SQL deparser does not exist in `pgparser`, so no parser-level deparse API was added.
- Because these gaps are explicit Task 5 gates, they preclude an unqualified completion status.

## Continuation audit and blocker

The binding design is singular: `docs/superpowers/specs/2026-07-09-crabka-gres-g9-distributed-maturity-design.md:73` specifies `SHARDED BY HASH (col) BUCKETS n`, and its test gate at line 109 requires hash-spec tables through the whole G8 crash/nemesis suite.

That gate cannot honestly be parameterized onto the current deployed split bridge. The bridge rejects every sharded or hash-sharded table in `crates/gres-ranges/src/tenant.rs:1048-1050` and again at `1170-1172`. The latter physical path also rejects any non-empty predecessor table at `1177-1182`; its checkpoint is explicitly named `local-empty-table-no-data-migration` at line 2100. Consequently there is no real hash-table filtered checkpoint/restore/catch-up/cutover implementation for a process crash/nemesis test to exercise. A test-only parameter would either fail at validation or mock the missing physical path, contrary to the requirement.

Completing the requested gate therefore requires first implementing the missing G8 deployed data-migration bridge for populated sharded tables (filtered checkpoint/restore, tail catch-up, cutover, restart recovery), then enabling hash bucket boundaries and coordinated co-location moves. This is architectural prerequisite work, not a Task 5 test parameterization seam. The repository's own audit records the same blocker in `.superpowers/sdd/audit-g7-g9.md:311-315` and recommends completing the physical operator path before the G9 gates.

Because the parent explicitly disallowed a concerns status and requires a practical non-mock process crash shard, the final status is `BLOCKED` on this absent physical G8 prerequisite. The earlier fast-model and in-process results remain valid but do not satisfy the missing system gate.
