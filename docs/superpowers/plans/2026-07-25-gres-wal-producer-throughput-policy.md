# Gres WAL Producer Throughput Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL:
> `superpowers:subagent-driven-development`, with a fresh implementer and
> independent spec/quality reviews per task.

**Goal:** Make generic producer batching controls valid and effective, then
expose the throughput settings that can affect Gres's single-partition WAL.

**Architecture:** Add one refined-type validated generic
`ProducerThroughputPolicy` covering compression, linger, batch bytes, and
cross-partition in-flight requests while preserving the existing builder API
and defaults. Stop waking the sender for every append when linger is nonzero;
wake immediately only for zero linger or when a completed batch is ready.
Gres stores the generic policy but exposes only compression, linger, and batch
bytes. Its max-in-flight value stays at the generic default because the WAL is
partition 0 and the producer's fixed one-request-per-partition ordering rule
makes that setting ineffective.

## Policy

- Compression: `none`.
- Linger: 0 ms.
- Batch bytes: 16,384.
- Generic cross-partition max in flight: 5.
- Linger is a whole-millisecond value in `0..=i32::MAX`.
- Batch bytes are in `1..=i32::MAX`.
- Max in flight is positive.

## Fixed and Deferred Values

- `MAX_IN_FLIGHT_PER_PARTITION = 1` remains a fixed idempotent-ordering
  invariant.
- The sender's 1 ms scheduler floor remains an implementation floor for
  zero-linger scheduling, not a user policy.
- Gres does not expose max in flight because its WAL producer targets only
  partition 0.

## Global Constraints

- Every Cargo command uses
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Follow RED/GREEN/refactor.
- Use `refined_type`; no hand-rolled validation logic at policy boundaries.
- Preserve existing generic builder call syntax and defaults.
- Preserve unrelated dirty files and commit only scoped files.

---

### Task 1: Validate and honor generic producer throughput policy

**Files:** `crates/client-producer/src/{builder,compression,producer,lib}.rs`
and focused tests.

- [ ] Add RED tests for exact defaults, distinctive values, compression string
  parsing, zero/overflow/fractional linger, zero/overflow batch bytes, zero max
  in flight, and pre-I/O builder rejection.
- [ ] Add exported defaults and `ProducerThroughputPolicy`, validated with
  `refined_type`.
- [ ] Preserve builder arguments but validate and forward them through the
  policy.
- [ ] Make nonzero linger real: do not wake on every append; wake when a batch
  rolls ready, zero linger requests immediate sending, and `flush` remains
  immediate.
- [ ] Prove nonzero linger coalesces appends while batch rollover and zero
  linger wake immediately.
- [ ] Verify full client-producer tests, strict Clippy, formatting, and diff
  checks.
- [ ] Commit:
  `feat(producer): configure throughput policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 2: Add Gres WAL throughput CLI/environment policy

**Files:** `crates/gres-substrate/src/recovery.rs`,
`crates/gres/src/lib.rs`, `crates/gres/tests/runtime.rs`, and focused tests.

- [ ] Add optional CLI/environment inputs:
  - `wal-producer-compression`
  - `wal-producer-linger-ms`
  - `wal-producer-batch-bytes`
- [ ] Build one shared effective `ProducerThroughputPolicy` with generic default
  max in flight before listener/recovery I/O.
- [ ] Store it in `LiveRecoveryConfig` and apply all four validated values at
  the sole producer construction; only three are user-facing for Gres.
- [ ] Prove defaults, environment, CLI precedence, exact distinctive flow,
  invalid/inert rejection, hostile-environment isolation, and help.
- [ ] Verify full Gres/substrate tests, strict Clippy, formatting, and diff
  checks.
- [ ] Commit:
  `feat(gres): expose WAL producer throughput`
- [ ] Obtain independent spec and quality approval.

---

### Task 3: Add fleet CRD WAL throughput policy

**Files:** `crates/operator/src/crd/gres.rs`,
`crates/operator/src/controller/gres_tenant.rs`,
`deploy/crds/crabka.io_greses.yaml`, and focused tests.

- [ ] Add optional fields:
  - `walProducerCompression`
  - `walProducerLingerMs`
  - `walProducerBatchBytes`
- [ ] Generate exact enum/range schema and validate through the shared policy.
- [ ] Render all three effective CLI pairs for every compute deployment.
- [ ] Prove exact defaults/overrides/errors and single-/multi-range output.
- [ ] Regenerate all nine CRDs twice and compare exactly.
- [ ] Verify full operator tests, strict Clippy, formatting, and diff checks.
- [ ] Commit:
  `feat(operator): expose WAL producer throughput`
- [ ] Obtain independent spec and quality approval.

---

### Task 4: Audit, verify, and publish

**Files:** `docs/configuration-audit.md`; existing progress files remain
uncommitted.

- [ ] Re-run the repository scanner and focused producer searches.
- [ ] Prove compression/batch/linger live consumption, real linger behavior,
  generic max-in-flight validation, and the fixed/deferred classifications.
- [ ] Run broad affected tests, strict Clippy, help, deterministic all-nine CRD
  generation, formatting, and diff checks; isolate unchanged failures.
- [ ] Commit:
  `docs(gres): record WAL throughput audit`
- [ ] Obtain fresh final review, remediate every finding, push the branch, and
  refresh draft PR #904.

The wider hardcoded-value configuration goal remains active afterward.
