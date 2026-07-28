# Gres WAL Admin Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL:
> `superpowers:subagent-driven-development`, with a fresh implementer and
> independent spec/quality reviews per task.

**Goal:** Configure WAL replication factor, topic ensure timeout, and admin
connection/request timeouts through CLI/environment and the fleet CRD.

**Architecture:** Add an options-based `AdminClient` connection path that
persists options across reconnects. Add a validated substrate
`WalAdminPolicy`, default it in `LiveRecoveryConfig`, and reuse Gres's single
recovery constructor helper. Keep existing topic ensure entry points defaulted
while live recovery uses policy-aware variants.

## Global Constraints

- Every Cargo command uses
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Follow RED/GREEN/refactor.
- Use `refined_type`; no hand-rolled validation newtypes.
- Keep WAL partitions, cleanup policy, and infinite retention fixed.
- Do not configure DNS, producer, checkpoint deletion, registry, or generic
  client policy.
- Preserve unrelated dirty files and commit only scoped files.

---

### Task 1: Preserve custom AdminClient options across reconnects

**Files:** `crates/client-admin/src/lib.rs` and focused tests.

- [ ] Add RED tests requiring an options-based connection API and a pure
  options/reconnect seam that preserves client id, security, connect timeout,
  and request timeout.
- [ ] Add `AdminClient::connect_with_options` (or the smallest equivalent).
  Existing `connect`/`connect_secured` delegate with current 5s/30s defaults.
- [ ] Store the full options template in `AdminClient`; controller and
  bootstrap reconnects clone it instead of rebuilding hardcoded defaults.
- [ ] Verify client-admin full tests, strict all-target/all-feature Clippy,
  formatting, and diff checks.
- [ ] Commit:
  `feat(admin): preserve configured connection options`
- [ ] Obtain independent spec and quality approval.

---

### Task 2: Add substrate WAL admin and topic policy

**Files:**

- `crates/gres-substrate/src/topic.rs`
- `crates/gres-substrate/src/recovery.rs`
- `crates/gres-substrate/src/lib.rs`
- constructor fallout tests only

- [ ] Add RED tests for exact four defaults, zero rejection, distinctive
  accessors, and `LiveRecoveryConfig` default/replacement.
- [ ] Implement `WalAdminPolicy` with positive replication factor and ensure
  timeout protocol values plus positive connect/request durations.
- [ ] Extend the narrow `TopicAdmin` seam so creation receives replication
  factor and timeout. Existing ensure functions use `WalAdminPolicy::default`;
  add policy-aware variants for live recovery.
- [ ] Add a single `connect_wal_admin` helper using
  `AdminClient::connect_with_options`. Route all seven recovery admin
  connections through it so reconnects retain the configured options.
- [ ] Ensure live topic creation uses configured replication factor and ensure
  timeout; topic existence checks and metadata resolution remain unchanged.
- [ ] Verify full client-admin/substrate tests, strict Clippy, formatting, and
  diff checks.
- [ ] Commit:
  `feat(gres): configure WAL admin policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 3: Add Gres WAL admin CLI/environment inputs

**Files:**

- `crates/gres/src/lib.rs`
- `crates/gres/tests/runtime.rs`

- [ ] Extend the existing recovery child matrix from six to ten environment
  variables. Prove defaults, production environment parsing, and genuine
  CLI-over-environment precedence.
- [ ] Add optional parser fields:
  - `PositiveI32` replication factor
  - `PositiveI32` ensure timeout
  - `PositiveMillis` admin connect timeout
  - `PositiveMillis` admin request timeout
- [ ] Extend existing inert/pre-I/O validation, zero tests, and unit/integration
  env-disabled parser helpers to all ten variables.
- [ ] Build one effective `WalAdminPolicy` in `SubstrateRuntimeConfig`; apply
  it through the existing shared `live_recovery_config` helper.
- [ ] Run focused and full Gres tests normally and under all ten hostile env
  variables, strict Clippy, help, formatting, and diff checks.
- [ ] Commit:
  `feat(gres): expose WAL admin policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 4: Add fleet CRD WAL admin policy

**Files:**

- `crates/operator/src/crd/gres.rs`
- `crates/operator/src/controller/gres_tenant.rs`
- `deploy/crds/crabka.io_greses.yaml`

- [ ] Add RED round-trip, schema-minimum, defaults, and exact zero-error tests.
- [ ] Add optional fields:
  - `walTopicReplicationFactor: i32`
  - `walTopicEnsureTimeoutMs: i32`
  - `walAdminConnectTimeoutMs: u64`
  - `walAdminRequestTimeoutMs: u64`
- [ ] Validate through `PositiveI32`/`PositiveMillis` using substrate defaults.
- [ ] Render all four effective pairs unconditionally beside existing WAL
  recovery arguments; test exact single- and multi-range output.
- [ ] Regenerate all nine CRDs and compare a second fresh generation exactly.
- [ ] Run full operator tests, strict Clippy, formatting, and diff checks.
- [ ] Commit:
  `feat(operator): expose WAL admin policy`
- [ ] Obtain independent spec and quality approval.

---

### Task 5: Audit and final verification

**Files:**

- commit: `docs/configuration-audit.md`
- update only, leave uncommitted: `.superpowers/sdd/progress.md`

- [ ] Re-run the repository scanner and focused admin/topic searches. Classify
  defaults, configured live flow, fixed durability invariants, tests, and
  deferred owners.
- [ ] Prove old `WAL_TOPIC_REPLICAS` and
  `WAL_TOPIC_ENSURE_TIMEOUT_MS` production use is gone or retained only as
  compatibility aliases, all seven recovery admin connects use the helper,
  reconnects preserve options, and all ten recovery/admin CLI/env/CRD fields
  have live consumers.
- [ ] Run broad affected tests, strict Clippy, help, all-nine CRD comparison,
  formatting, and diff checks. Reproduce/isolate unchanged baseline failures.
- [ ] Commit audit only:
  `docs(gres): record WAL admin audit`
- [ ] Obtain a fresh final review; resolve every finding and refresh evidence.

The wider hardcoded-value configuration goal remains active afterward.
