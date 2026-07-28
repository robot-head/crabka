# Gres WAL Recovery Read Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task by
> task with a fresh implementer and independent spec/quality reviews.

**Goal:** Configure Gres WAL recovery fetch wait, partition and response byte
limits, and consecutive empty-fetch retries through CLI/environment and the
fleet CRD.

**Architecture:** `crabka-client-core::IsolatedFetch` carries the whole-response
limit instead of hiding 50 MiB. `crabka-gres-substrate` owns a validated
`RecoveryReadPolicy` and the recovery defaults. Gres resolves optional parser
values once and applies the policy through one recovery-config constructor
helper. The operator validates four optional `spec.compute` fields and always
renders their effective values for substrate computes.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Tokio, kube/schemars, serde,
generated Kubernetes CRDs.

## Global Constraints

- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Follow RED/GREEN/refactor; record the failing test before production edits.
- Use `refined_type` for validation; do not hand-roll numeric newtypes.
- Preserve `LiveRecoveryConfig::new` and positional `fetch_partition` behavior
  for existing callers.
- Preserve the committed-end sampler's zero wait.
- Keep partition zero, read-committed isolation, one-byte minimum, error codes,
  and offset arithmetic fixed.
- Do not configure connection/request timeouts in this slice.
- Preserve unrelated dirty files and commit only each task's scoped files.

---

### Task 1: Make client and substrate recovery limits explicit

**Files:**

- Modify: `crates/client-core/src/fetch.rs`
- Modify: `crates/client-core/src/lib.rs`
- Modify constructor fallout only:
  - `crates/client-streams/src/runtime/io_broker.rs`
  - `crates/client-streams/tests/eos_broker.rs`
  - `crates/gres-control/src/registry.rs`
  - `crates/gres-fdw/src/source.rs`
  - `crates/gres/src/lib.rs`
- Modify: `crates/gres-substrate/Cargo.toml`
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify export: `crates/gres-substrate/src/lib.rs`

**Interfaces:**

- Produces:
  `crabka_client_core::DEFAULT_FETCH_RESPONSE_MAX_BYTES: i32`
- Produces: `IsolatedFetch::max_bytes: i32`
- Produces:
  - `DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS`
  - `DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES`
  - `DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES`
  - `DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES`
- Produces: `RecoveryReadPolicy`
- Produces: `LiveRecoveryConfig::with_read_policy`

- [ ] **Step 1: Add RED client request tests**

Change the existing `build_fetch_request_preserves_single_partition_settings`
test to provide a distinctive `max_bytes` value and assert that exact value
reaches `FetchRequest.max_bytes`. Update the mock transport assertion likewise.
Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core build_fetch_request_preserves_single_partition_settings --lib
```

Expected: compile failure because `IsolatedFetch` has no `max_bytes`.

- [ ] **Step 2: Implement the client field minimally**

Add the named default and `IsolatedFetch::max_bytes`. Use it in
`build_fetch_request`. The positional `fetch_partition` path supplies the
default so its API and behavior remain unchanged. Export the constant.

Update every existing `IsolatedFetch` literal. Non-recovery callers use
`DEFAULT_FETCH_RESPONSE_MAX_BYTES`; the registry paths may use the same named
default because registry response-size policy is outside this slice.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check -p crabka-client-streams -p crabka-gres-control \
    -p crabka-gres-fdw -p crabka-gres
```

- [ ] **Step 3: Add RED substrate policy tests**

Add tests in `recovery.rs` that require:

- exact four defaults;
- zero rejection for each constructor input;
- accessor preservation for distinctive valid values;
- `LiveRecoveryConfig::new` default policy;
- `with_read_policy` replacement;
- normal recovery request wiring for wait and both byte limits;
- committed-end sampling still uses zero wait while retaining configured byte
  limits.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate recovery_read_policy --lib
```

Expected: compile failure because the policy does not exist.

- [ ] **Step 4: Implement the validated substrate policy**

Add the existing workspace `refined_type` dependency. Validate positive `i32`
protocol values and positive `usize` retry values with its rules inside
`RecoveryReadPolicy::new`; keep fields private and expose `const` accessors.
Derive `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`.

Default the policy in `LiveRecoveryConfig::new` and add
`with_read_policy`. Carry it into `KafkaCommittedWalReader`. Replace:

- normal `FETCH_MAX_WAIT_MS`;
- recovery `FETCH_MAX_BYTES` in both partition and whole-response request
  fields;
- `EMPTY_FETCH_RETRIES`.

Use checked or saturating retry-count increment so configured policy cannot
introduce overflow. Keep `END_SAMPLE_MAX_WAIT_MS = 0`.

- [ ] **Step 5: Verify and commit Task 1**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core -p crabka-gres-substrate --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-core -p crabka-gres-substrate \
    -p crabka-client-streams -p crabka-gres-control -p crabka-gres-fdw \
    -p crabka-gres --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Commit:

```bash
git add crates/client-core crates/client-streams crates/gres-control \
  crates/gres-fdw crates/gres-substrate crates/gres
git commit -m "feat(gres): define WAL recovery read policy"
```

Request independent spec and quality reviews before Task 2.

---

### Task 2: Parse and propagate the Gres recovery policy

**Files:**

- Modify: `crates/gres/src/lib.rs`
- Modify constructor call only: `crates/gres/src/split_activation.rs`
- Modify constructor fallout only: `crates/gres/tests/runtime.rs`

**Interfaces:**

- Produces four optional `ServeArgs` fields with the approved CLI/environment
  names.
- Produces: `SubstrateRuntimeConfig::recovery_read_policy`
- Produces one shared `SubstrateRuntimeConfig::live_recovery_config` helper.

- [ ] **Step 1: Add RED parser and validation tests**

Use a child process to scrub all four recovery environment variables for the
default branch, inject distinctive values for the environment branch, and
prove CLI precedence. Assert exact effective defaults and values through
`SubstrateRuntimeConfig`.

Add boundary tests proving:

- zero fails for each CLI field;
- explicit recovery configuration without `--substrate-bootstrap` fails;
- programmatically populated `ServeArgs` also fails before I/O;
- unrelated hostile parent environment cannot change default-mode assertions.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres wal_recovery_read_policy --lib
```

Expected: compile failure because the parser fields do not exist.

- [ ] **Step 2: Implement optional CLI/environment inputs**

Add:

- `--wal-recovery-fetch-max-wait-ms`
- `--wal-recovery-fetch-partition-max-bytes`
- `--wal-recovery-fetch-response-max-bytes`
- `--wal-recovery-empty-fetch-retries`

Use `Option<PositiveI32>` for the three protocol values and
`Option<PositiveUsize>` for retries. Resolve them through substrate-owned
defaults in `SubstrateRuntimeConfig::from_args` and construct
`RecoveryReadPolicy`.

Add one lightweight explicit-use validator and invoke it at the same pre-I/O
boundaries used by the existing range-0 validation.

- [ ] **Step 3: Add RED propagation tests**

Tests must enumerate the runtime recovery construction paths and prove a
distinctive policy reaches:

- range-0 follower bootstrap;
- multi-range recovery configs;
- single-range selection;
- activation discovery and successor recovery;
- staged range transfer recovery.

Prefer testing the shared helper directly plus a caller inventory assertion;
do not add one test-only abstraction per path.

- [ ] **Step 4: Route construction through one helper**

Add one `SubstrateRuntimeConfig::live_recovery_config(tenant, range)` helper
that supplies bootstrap, security, and `with_read_policy`. Replace every Gres
production `LiveRecoveryConfig::new` call, including the child module
`split_activation.rs`, with that helper. Preserve subsequent generation,
endpoint, checkpoint, staging, and replay-seed builders.

- [ ] **Step 5: Verify and commit Task 2**

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

Commit:

```bash
git add crates/gres/src crates/gres/tests
git commit -m "feat(gres): configure WAL recovery reads"
```

Request independent spec and quality reviews before Task 3.

---

### Task 3: Expose the recovery policy through the fleet CRD

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify tests as needed:
  - `crates/operator/tests/reconcile_gres.rs`
  - `crates/operator/tests/reconcile_gres_tenant.rs`
- Regenerate: `deploy/crds/crabka.io_greses.yaml`
- Regenerate: `deploy/crds/crabka.io_grestenants.yaml`

**Interfaces:**

- Produces four optional `GresComputeSpec` camelCase fields.
- Produces four validated values in `EffectiveGresComputePolicy`.
- Produces exact four CLI pairs in every substrate compute Deployment.

- [ ] **Step 1: Add RED CRD policy tests**

Extend compute-policy tests with:

- JSON/YAML round trips for all four fields;
- exact schema minimum one;
- exact substrate-owned defaults;
- an effective-policy error path for zero on every field.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator compute_wal_recovery --lib
```

Expected: compile failure because the CRD fields do not exist.

- [ ] **Step 2: Implement CRD fields and validation**

Use `Option<i32>` for the three Kafka protocol values and `Option<usize>` for
retries, with `schemars(range(min = 1))`. Resolve through
`PositiveI32`/`PositiveUsize` and substrate-owned defaults in
`effective_policy`; errors name the exact camelCase field path.

- [ ] **Step 3: Add RED Deployment tests**

Set distinctive values and assert exact ordered CLI pairs for both:

- single-range substrate compute;
- multi-range/range-control compute.

Also prove omitted CRD fields render the compiled defaults.

- [ ] **Step 4: Render the four effective values**

Append the four pairs to the existing base substrate argument vector before
the range-control-only branch. Do not gate them on checkpoint storage or
multi-range mode.

- [ ] **Step 5: Regenerate and verify all CRDs**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds deploy/crds
crd_verify_dir="$(mktemp -d)"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_verify_dir"
diff -ru deploy/crds "$crd_verify_dir"
```

- [ ] **Step 6: Verify and commit Task 3**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Commit:

```bash
git add crates/operator deploy/crds/crabka.io_greses.yaml \
  deploy/crds/crabka.io_grestenants.yaml
git commit -m "feat(operator): expose WAL recovery reads"
```

Request independent spec and quality reviews before Task 4.

---

### Task 4: Audit and close the WAL recovery read slice

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify: `.superpowers/sdd/progress.md`

- [ ] **Step 1: Re-run the configuration scanners**

Run the repository scanner and focused searches used by the prior Gres audit.
Classify every recovery-adjacent literal and document:

- shared policy defaults;
- configured production consumers;
- fixed protocol/algorithm invariants;
- tests/harnesses;
- deferred connection/request timeout owner.

Focused searches must include `100`, `1_048_576`, `50 * 1024 * 1024`,
`FETCH_MAX_WAIT`, `FETCH_MAX_BYTES`, `EMPTY_FETCH`, `max_wait_ms`,
`partition_max_bytes`, and `max_bytes`.

- [ ] **Step 2: Prove there is no stale recovery hardcoding**

Require:

- no production normal-recovery use of the old constants;
- the sole recovery zero-wait use is the committed-end sampler;
- every Gres production `LiveRecoveryConfig` originates from the configured
  helper;
- all `IsolatedFetch` literals explicitly choose a response limit;
- all four CLI/environment and CRD fields have one live consumer.

- [ ] **Step 3: Run the fresh final gate**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core -p crabka-gres-substrate \
    -p crabka-gres-control -p crabka-gres-fdw -p crabka-gres \
    -p crabka-operator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-core -p crabka-gres-substrate \
    -p crabka-gres-control -p crabka-gres-fdw -p crabka-gres \
    -p crabka-operator --all-targets --all-features -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help
crd_audit_dir="$(mktemp -d)"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_audit_dir"
diff -ru deploy/crds "$crd_audit_dir"
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 4: Record evidence and commit**

Update `docs/configuration-audit.md` with commands, counts, classifications,
and the next unresolved owner. Update only the new progress-ledger entry;
preserve all unrelated dirty ledger content.

Commit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record WAL recovery read audit"
```

Keep `.superpowers/sdd/progress.md` uncommitted with the rest of its existing
workspace bookkeeping.

- [ ] **Step 5: Independent final review**

Provide the complete implementation range and current dirty-file inventory to
a fresh reviewer. Resolve every Critical, Important, and Minor finding, rerun
the affected and final gates, refresh audit evidence if counts change, and
obtain a clean final re-review before declaring this slice complete.

The wider hardcoded-value configuration goal remains active after this slice.
