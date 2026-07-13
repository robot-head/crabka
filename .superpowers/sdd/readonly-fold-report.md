# Authoritative read-only committed fold

## Contract

`committed_fold_snapshot` samples a single `READ_COMMITTED` end offset, selects the newest
same-generation checkpoint not newer than that sample, verifies its manifest, part digests,
pair counts, tenant, generation, and key order, restores into a new reader-owned `MemKv`, and
applies only committed WAL records through the sampled offset. It checks journal sequence
continuity and validates the generation both before and after the read.

The dependency surface is deliberately read-only: `CheckpointStore`, `CommittedWalReader`,
`CommittedEndSampler`, and `GenerationWitness`. It has no writer, producer, fence, pause,
barrier, checkpoint-service, pruning, or live-cache handle.

Results contain the sampled offset, optional immutable manifest identity, raw sorted key/value
pairs, and replay/checkpoint provenance. Prefix and half-open interval projections are applied
to both checkpoint and WAL operations. Input/output records and bytes are bounded by explicit
limits. Missing genesis history returns `PrunedHistory`; corruption, holes, mismatched identity,
and generation drift fail closed.

## TDD evidence

- RED: the initial wished-for API test failed to compile because the fold types and function did
  not exist.
- GREEN: checkpoint plus tail and genesis-only tests passed after the minimal implementation.
- Added boundary/error coverage for sample exclusion, checkpoint-at-sample, ignoring newer
  checkpoints, pruned history, generation mismatch/drift, uncommitted input, projection, and
  resource limits.

## Verification

- `cargo test -p crabka-gres-substrate readonly_fold::tests --no-fail-fast`: pass (7 tests).
- `cargo test -p crabka-gres-substrate --all-targets --no-fail-fast`: the library (131),
  checkpoint model (2), checkpoint runtime (3), live coordinator (1), G2 acceptance (4), and
  writer fault (6) tests passed. The pre-existing `raw_kv_split_runtime` target failed all four
  tests with unmapped physical table ids / missing staged restore state; this primitive does not
  touch split runtime or table mapping.
- `cargo test -p crabka-gres-substrate --features checkpoint-test-hooks --test checkpoint_crashes
  --no-fail-fast`: pass (12 tests, including corruption/torn truncation and zombie races).
- `cargo clippy -p crabka-gres-substrate --all-targets -- -D warnings`: blocked by the pre-existing
  `clippy::semicolon_if_nothing_returned` violation in `crates/pgwire/src/engine.rs:297`.
- `crates/gres-ranges/src/control.rs` is a protected pre-existing unstaged delta and was neither
  staged nor included in this change.
