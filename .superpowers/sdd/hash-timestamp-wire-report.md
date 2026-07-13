# Hash timestamp wire report

Status: PARTIAL

## Root cause and implementation

The timestamp local model and TXD2 durable descriptor already carried `Option<u32>` bucket identity, but both JSON wire structs omitted it. Six production conversions in `forward.rs`/`tenant.rs` consequently either failed to compile after the local field became mandatory or would have reconstructed bucketless physical keys remotely.

`WireTimestampWrite` and `WireTimestampOperation` now carry a serde-defaulted optional bucket. Every prewrite, primary acknowledgement/inspection, resolve, recover, and tenant recovery conversion copies the exact value. Bucket is part of canonical operation ordering, so equal rowids in different buckets remain distinct and deterministic. Catalog-aware validation rejects missing/out-of-range buckets for known hash tables and rejects buckets on known non-hash tables; unknown raw-test tables retain the existing low-level compatibility seam. Legacy JSON without the field decodes only as explicitly bucketless and is rejected when a known hash catalog entry is available.

The focused remote resolve regression now uses buckets 0 and 15 and verifies the exact `HashPrimaryVersion` keys reach committed state. Existing local constructors were updated explicitly as bucketless fixtures rather than relying on compiler-driven erasure.

## RED/GREEN evidence

RED was the reproducible eight production `E0063` failures reported at HEAD `9d7baabd`, followed by the remaining test-constructor failures once production compiled.

GREEN:

- `cargo check -p crabka-gres-ranges --all-targets`: passed.
- `cargo test -p crabka-gres-ranges --lib --no-fail-fast`: 174 passed.
- `cargo test -p crabka-pgexec timestamp_txn --lib --no-fail-fast`: 21 passed.
- catalog bucket validation focus: passed.
- hash remote resolve focus: passed.
- wire whole-struct roundtrips cover `None`, `Some(0)`, and `Some(u32::MAX)`; legacy decode and absent-versus-zero encoding tests passed.
- `git diff --check`: passed.

`cargo fmt --all -- --check` remains noisy/blocked by the pre-existing `crates/gres-ranges/src/control.rs` delta. That file was not edited or staged by this work.

## Remaining strict-gate coverage

The repository still lacks a dedicated production multiprocess test that performs remote hash prewrite plus commit/abort and kills/restarts the child between primary decision and participant recovery. Existing restart recovery and remote timestamp tests are green, and the exact bucket now traverses all conversions they use, but they are not yet combined into the requested single process-runtime hash scenario. This report therefore does not claim the entire parent gate complete.

## Commits

- `e5a8fed4 feat(gres): preserve hash buckets on timestamp wire`
- this report is committed separately.
