# Gres WAL Recovery Timeout Policy Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL:
> `superpowers:subagent-driven-development`, with a fresh implementer and
> independent spec/quality reviews for each task.

**Goal:** Configure the raw WAL recovery connection's 10-second connect and
30-second request timeouts through CLI/environment and the fleet CRD.

**Architecture:** Extend the existing validated `RecoveryReadPolicy` with two
durations and a source-compatible builder. Reuse the existing Gres recovery
parser, propagation helper, operator compute policy, and Deployment argument
path. Add no second policy type.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Tokio, kube/schemars, serde,
generated Kubernetes CRDs.

## Global Constraints

- Every Cargo command uses
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- RED precedes production edits.
- Preserve the existing four-argument `RecoveryReadPolicy::new`.
- Use `refined_type`; do not hand-roll numeric validation.
- Keep sampler fetch wait zero and all existing fetch/retry semantics.
- Do not configure DNS, admin, producer, topic, registry, or generic-client
  timeouts.
- Preserve unrelated dirty files; commit only scoped files.

---

### Task 1: Extend the substrate recovery policy

**Files:**

- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify export only if needed: `crates/gres-substrate/src/lib.rs`

**Produces:**

- `DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS: u64 = 10_000`
- `DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS: u64 = 30_000`
- `RecoveryReadPolicy::with_timeouts(connect_ms, request_ms)`
- `connect_timeout()` and `request_timeout()` accessors

- [ ] Add RED tests for exact defaults, zero rejection in either builder
  argument, distinctive durations, and replacement without changing fetch
  fields.
- [ ] Add a small `wal_connection_options` constructor test requiring exact
  client id, security, connect timeout, and request timeout. Use it from
  `open_wal_connection`; do not mock TCP.
- [ ] Implement private `Duration` fields validated through
  `refined_type::rule::GreaterU64<0>`. The existing constructor installs
  validated compiled defaults; `with_timeouts` replaces them.
- [ ] Pass `RecoveryReadPolicy` into both raw connection callers:
  `KafkaCommittedWalReader::open_connection` and `LiveEndDialer::dial`.
- [ ] Prove the sampler still builds a zero-wait fetch request.
- [ ] Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate recovery_read_policy --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-substrate --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] Commit:

```bash
git add crates/gres-substrate
git commit -m "feat(gres): configure recovery connection timeouts"
```

- [ ] Obtain independent spec and quality approval.

---

### Task 2: Add Gres CLI/environment timeout inputs

**Files:**

- Modify: `crates/gres/src/lib.rs`
- Modify shared integration fixture: `crates/gres/tests/runtime.rs`

**Produces:**

- `--wal-recovery-connect-timeout-ms`
  / `CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS`
- `--wal-recovery-request-timeout-ms`
  / `CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS`

- [ ] Extend the existing child-process recovery-policy test from four
  environment variables/values to six. Require defaults, environment values,
  and true CLI-over-environment precedence via raw production Clap parsing.
- [ ] Add RED zero and explicit local-mode rejection assertions for both
  fields, including the pre-listener programmatic path.
- [ ] Add both `Option<PositiveMillis>` fields and include them in the existing
  inert-use validator.
- [ ] Build the existing four-value `RecoveryReadPolicy`, then call
  `with_timeouts` with effective substrate-owned defaults or parser values.
- [ ] Extend both unit and integration env-disabled test parser helpers from
  four fields to six; rerun full library and runtime tests under a hostile
  six-variable environment.
- [ ] Extend the shared recovery-config propagation test with distinctive
  timeouts; add no new constructor path.
- [ ] Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres wal_recovery --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres --all-targets --all-features -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help
cargo fmt --all -- --check
git diff --check
```

- [ ] Repeat Gres library and runtime tests with all six recovery environment
  variables set to valid hostile values.
- [ ] Commit:

```bash
git add crates/gres/src/lib.rs crates/gres/tests/runtime.rs
git commit -m "feat(gres): expose recovery connection timeouts"
```

- [ ] Obtain independent spec and quality approval.

---

### Task 3: Add fleet CRD timeout fields

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Regenerate: `deploy/crds/crabka.io_greses.yaml`

- [ ] Add RED round-trip, schema-minimum, exact-default, and zero-error-path
  tests for `walRecoveryConnectTimeoutMs` and
  `walRecoveryRequestTimeoutMs`.
- [ ] Add optional `u64` fields with schema minimum one and validated
  `PositiveMillis` effective fields using substrate-owned defaults.
- [ ] Extend exact distinctive and omitted/default Deployment assertions for
  both single-range and multi-range modes.
- [ ] Render both pairs unconditionally beside existing recovery arguments.
- [ ] Regenerate all nine CRDs and compare a second fresh generation exactly.
- [ ] Run the full operator test suite, strict all-target/all-feature Clippy,
  formatting, and diff checks.
- [ ] Commit:

```bash
git add crates/operator/src/crd/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_greses.yaml
git commit -m "feat(operator): expose recovery timeouts"
```

- [ ] Obtain independent spec and quality approval.

---

### Task 4: Audit and final verification

**Files:**

- Modify: `docs/configuration-audit.md`
- Update but do not commit: `.superpowers/sdd/progress.md`

- [ ] Re-run the repository scanner and focused timeout searches. Classify:
  shared defaults, configured production flow, tests/harnesses, fixed values,
  and deferred timeout owners.
- [ ] Prove the old recovery `Duration::from_secs(10/30)` literals are absent,
  both raw WAL connection callers receive the policy, and all six
  CLI/environment/CRD recovery fields have one live consumer.
- [ ] Run full affected tests, strict Clippy, help, all-nine CRD comparison,
  formatting, and diff checks. Record any unchanged baseline exception
  precisely.
- [ ] Commit only the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record recovery timeout audit"
```

- [ ] Obtain a fresh final review of the complete range and current dirty
  inventory. Resolve every finding and refresh evidence before READY.

The wider hardcoded-value configuration goal remains active afterward.
