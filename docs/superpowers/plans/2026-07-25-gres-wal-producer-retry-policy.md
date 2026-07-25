# Gres WAL Producer Retry Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL:
> `superpowers:subagent-driven-development`, with a fresh implementer and
> independent spec/quality reviews per task.

**Goal:** Make the generic producer's retry controls real and expose the WAL
producer retry/transaction policy through Gres CLI/environment and fleet CRD.

**Architecture:** Keep the existing producer builder API and defaults. Add only
the missing retry-budget builder inputs, validate the complete policy with
`refined_type`, and pass it into the existing sender/init paths. Store retry
attempts on each prepared batch so the existing `retries` input limits actual
reroutes/resends. Gres owns one effective WAL producer policy in
`LiveRecoveryConfig`; its existing recovery producer construction consumes it.

## Policy

- Request timeout: 30,000 ms.
- Retries after the initial batch send: `i32::MAX`.
- Retry backoff and producer-ID initial backoff: 100 ms.
- Per-batch routing retry budget: 30,000 ms.
- Producer-ID initialization retry timeout: 30,000 ms.
- Producer-ID maximum backoff: 1,000 ms.
- Transaction timeout: 60,000 ms.
- A batch fails when either its retry count or routing budget is exhausted.
- Positive durations must fit the protocol's `i32` millisecond fields where
  applicable; retries are nonnegative; initial backoff cannot exceed its cap.

## Exclusions

- Protocol error codes, disabled identities, and the 1 ms scheduler floor stay
  fixed invariants.
- Compression, linger, batch size, and in-flight limits belong to a later
  throughput policy.
- No new policy abstraction beyond the validated value needed by the existing
  builder/runtime flow.

## Global Constraints

- Every Cargo command uses
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Follow RED/GREEN/refactor.
- Use `refined_type`; no hand-rolled validation newtypes.
- Preserve source-compatible producer builder defaults.
- Preserve unrelated dirty files and commit only scoped files.

---

### Task 1: Make generic producer retry policy effective

**Files:** `crates/client-producer/src/{builder,sender,producer,lib}.rs`,
`crates/client-producer/Cargo.toml`, and focused tests.

- [ ] Add RED tests for defaults, invalid bounds, configured init retry timing,
  retry-count exhaustion, routing-budget exhaustion, and exact transaction
  timeout conversion.
- [ ] Add the missing builder inputs for routing retry budget and producer-ID
  retry timeout/maximum backoff; reuse `retry_backoff` as the initial backoff.
- [ ] Validate the complete policy before connection I/O using `refined_type`.
- [ ] Pass configured values into producer-ID initialization and `SenderConfig`.
- [ ] Track batch retries across resend cycles; make existing `retries` govern
  them and remove the dead stored-only behavior.
- [ ] Replace the transaction-timeout silent fallback with validated exact
  conversion.
- [ ] Verify full client-producer tests, strict all-target/all-feature Clippy,
  formatting, and diff checks.
- [ ] Commit:
  `feat(producer): configure retry policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 2: Add Gres WAL producer CLI/environment policy

**Files:** `crates/gres-substrate/src/{recovery,lib}.rs`,
`crates/gres/src/lib.rs`, `crates/gres/tests/runtime.rs`, and focused tests.

- [ ] Add RED tests for exact defaults, zero/negative/range rejection,
  distinctive values, and `LiveRecoveryConfig` default/replacement.
- [ ] Add optional CLI/environment inputs for the seven effective policy
  values and prove default, environment, and CLI precedence.
- [ ] Build one validated effective policy before listener or recovery I/O.
- [ ] Apply it at the existing single WAL producer construction site.
- [ ] Extend inert-use and hostile-environment parser tests.
- [ ] Verify full Gres/substrate tests, strict Clippy, help, formatting, and
  diff checks.
- [ ] Commit:
  `feat(gres): expose WAL producer retry policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 3: Add fleet CRD WAL producer policy

**Files:** `crates/operator/src/crd/gres.rs`,
`crates/operator/src/controller/gres_tenant.rs`,
`deploy/crds/crabka.io_greses.yaml`, and focused tests.

- [ ] Add RED round-trip, schema-bound, defaults, and exact validation-error
  tests for the seven fields.
- [ ] Add optional compute fields and validate through the shared policy
  defaults/types.
- [ ] Render all seven effective CLI pairs for every recovery-capable compute.
- [ ] Regenerate all nine CRDs and compare a second fresh generation exactly.
- [ ] Verify full operator tests, strict Clippy, formatting, and diff checks.
- [ ] Commit:
  `feat(operator): expose WAL producer retry policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 4: Audit and final verification

**Files:** `docs/configuration-audit.md`; update only, leave uncommitted:
`.superpowers/sdd/progress.md`.

- [ ] Re-run the repository scanner and focused producer-policy searches.
- [ ] Prove all configured fields have live consumers, `retries` is no longer
  inert, retry deadlines/backoffs are not hardcoded in production paths, and
  excluded values are classified.
- [ ] Run broad affected tests, strict Clippy, help, all-nine CRD comparison,
  formatting, and diff checks. Isolate unchanged baseline failures.
- [ ] Commit audit only:
  `docs(gres): record WAL producer retry audit`
- [ ] Obtain a fresh final review and remediate every finding.

The wider hardcoded-value configuration goal remains active afterward.
