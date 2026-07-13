# G9 hash boundary schema report

Status: FEATURE LANDED; PROCESS MATRIX NOT EXPANDED

## RED / GREEN

RED: `cargo test -p crabka-gres-control hash_boundaries_roundtrip_and_fail_closed_on_invalid_table_shape --lib -- --nocapture` failed because `RangeBoundary::hash` and a bucket component did not exist.

GREEN: registry boundaries now preserve the typed optional bucket, reject bucket boundaries on non-hash tables, reject missing buckets on hash-table boundaries, and reject buckets outside the declared count. The same test pins format-v2 roundtrip and explicit format-v1 rejection. The CLI contract test pins that singular `--bucket` is required for hash tables, forbidden for other tables, and range checked. CRD/operator tests pin JSON shape and bucket-zero preservation across whole-struct conversion.

## Schema and CLI contract

- Tenant registry envelope format is version 2. Version 1 is rejected rather than ambiguously interpreting a hash boundary as `(table_id,rowid)`.
- `RangeBoundary` is `(table_id, bucket: Option<u32>, rowid)`. `None` retains the exact legacy non-hash order. `Some(bucket)` retains bucket zero and orders lexicographically before rowid.
- Kubernetes `GresTenantRangeKey` carries the same optional `bucket` in camel-case JSON and omits it for legacy boundaries.
- `crabka gres split TENANT TABLE ROWID --bucket N` requires exactly one bucket for a hash table. It rejects a missing, out-of-range, or non-hash bucket before journal initiation.
- Split plans, operation records, request digests, and replay receipts serialize the whole typed boundary, so restart/idempotency identity includes the bucket.
- Registry-to-RangeMap conversion uses `RangeKey::hash` for `Some(bucket)` including bucket zero and the legacy constructor only for `None`.

## Verification

- `cargo test -p crabka-gres-control --lib`: 65 passed.
- `cargo test -p crabka-gres-ranges hash --lib`: 5 passed.
- `cargo test -p crabka-gres-ranges --test split_model`: 5 passed.
- `cargo check -p crabka-gres-ranges -p crabka-gres -p crabka-operator -p crabka-cli --all-targets`: passed (existing dead-code warnings in process harnesses).
- Focused CLI hash-boundary contract command: exit 0.
- Focused operator bucket-zero serde/conversion command: exit 0.
- `git diff --cached --check`: clean before the feature commit.

## Protected control.rs proof

The feature's selectively staged `control.rs` diff was 27 lines (21 insertions, 6 deletions), containing only boundary conversion, map matching, and required legacy test-literal fields. After selective staging, the unstaged file remained exactly the pre-task formatting-only diff: 21 lines (14 insertions, 7 deletions) at `request_digest` and two receipt-test formatting sites. It was neither staged nor committed.

## Commits and concerns

- `1f222ac8 feat(gres): version hash bucket boundaries`
- This report is committed separately.

This slice does not add the full external process matrix, as requested. The existing populated hash runtime test and split model remain green, but a new dedicated in-process midpoint bucket physical-fold/restart fixture was not completed in this commit. Therefore this report does not claim the entire G9 Task 5 gate; it claims the previously blocked authoritative boundary vertical seam.
