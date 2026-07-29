# Gres Activator Runtime Configuration Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to implement this plan task by
> task.

**Goal:** Route every Gres activator deployment-policy value through validated
CLI/environment inputs and, when operator-managed, typed `Gres` CRD fields.

**Architecture:** The activator remains a small standalone binary. Clap parses
validated boundary types backed by `refined_type`, then converts them into the
existing `ActivatorConfig`. The operator adds one optional `spec.activator`
block, validates it before any child API writes, and renders the effective
values into the existing Deployment. Operator-derived addresses and fixed
Kubernetes/container identity remain fixed.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, kube/schemars CRDs,
Cargo nextest.

## Constraints

- Preserve current defaults: registry poll `250ms`, cold-start timeout
  `30000ms`, replicas `max(spec.pgdog.replicas, 1)`, and readiness probe period
  `5s`.
- New validated scalar/string newtypes must use `refined_type`; never use
  `Refined::unsafe_new`.
- Every direct-process value gets an environment-backed Clap option.
- Operator-derived listen address, Kafka bootstrap address, and backend
  endpoint template are not independent CRD settings.
- The backend template may be a static endpoint; require only non-empty text,
  not a `{tenant}` placeholder.
- Keep the fixed activator port, container/user IDs, labels, protocol names,
  and Kubernetes object names fixed.
- Use `assert2::assert!`; add no lint suppressions or new third-party
  dependencies. The workspace-owned `refined_type` runtime dependency and
  `temp-env` test helper are allowed.

## Effective Settings

| Setting | Direct CLI/environment | Gres CRD | Default | Constraint |
|---|---|---|---:|---|
| listen | `--listen` / `CRABKA_GRES_ACTIVATOR_LISTEN` | derived | required | `SocketAddr` |
| bootstrap | `--bootstrap` / `CRABKA_GRES_ACTIVATOR_BOOTSTRAP` | derived | required | non-empty |
| registry replication | `--registry-replication-factor` / `CRABKA_GRES_ACTIVATOR_REGISTRY_REPLICATION_FACTOR` | `spec.activator.registryReplicationFactor` | 1 | `1..=32767` |
| registry poll | `--registry-poll-ms` / `CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS` | `spec.activator.registryPollMs` | 250 | `>= 1` |
| cold-start timeout | `--cold-start-timeout-ms` / `CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS` | `spec.activator.coldStartTimeoutMs` | 30000 | `>= 1` |
| backend template | `--backend-endpoint-template` / `CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE` | derived | `{tenant}:5432` | non-empty |
| image | n/a | `spec.activator.image` | operator `--default-gres-activator-image` | non-empty |
| replicas | n/a | `spec.activator.replicas` | `max(pgdog.replicas, 1)` | `>= 1` |
| readiness period | n/a | `spec.activator.readinessProbePeriodSeconds` | 5 | `>= 1` |

---

### Task 1: Validate Direct Activator Inputs

**Files:**

- Modify: `crates/gres-activator/Cargo.toml`
- Create: `crates/gres-activator/src/config_value.rs`
- Modify: `crates/gres-activator/src/lib.rs`
- Modify: `crates/gres-activator/src/main.rs`

- [x] **Step 1: Add failing boundary and precedence tests**

Add tests for positive millisecond and signed-integer newtypes and a non-empty
string newtype:

```rust
#[test]
fn validated_input_boundaries() {
    assert!(PositiveMillis::new(0).is_err());
    assert!(PositiveMillis::new(1).is_ok());
    assert!(ReplicationFactor::new(0).is_err());
    assert!(ReplicationFactor::new(1).is_ok());
    assert!(ReplicationFactor::new(32_767).is_ok());
    assert!(ReplicationFactor::new(32_768).is_err());
    assert!("0".parse::<PositiveMillis>().is_err());
    assert!("1".parse::<PositiveMillis>().is_ok());
    assert!(NonEmptyValue::new(String::new()).is_err());
    assert!("broker:9092".parse::<NonEmptyValue>().is_ok());
}
```

In `main.rs`, test `Args::try_parse_from` for:

- current defaults;
- rejection of zero poll/timeout and empty bootstrap/template;
- environment-only parsing; and
- CLI-over-environment precedence.

Serialize environment-mutating tests with the existing repository test
pattern and restore every variable after the assertion.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator validated_input --no-fail-fast
```

Expected: RED until the types and environment declarations exist.

- [x] **Step 2: Add the minimum refined boundary types**

Use the existing gateway wrapper pattern:

```rust
pub struct PositiveMillis(u64);

impl PositiveMillis {
    pub fn new(value: u64) -> Result<Self, String> {
        refined_type::rule::GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    pub const fn into_value(self) -> u64 {
        self.0
    }
}
```

Implement `FromStr` for Clap. Wrap
`refined_type::rule::NonEmptyString` similarly for owned strings; do not add a
generic validation framework.

- [x] **Step 3: Add environment-backed Clap inputs**

Use:

```rust
#[arg(long, env = "CRABKA_GRES_ACTIVATOR_LISTEN")]
listen: SocketAddr,
#[arg(long, env = "CRABKA_GRES_ACTIVATOR_BOOTSTRAP")]
bootstrap: NonEmptyValue,
#[arg(
    long,
    env = "CRABKA_GRES_ACTIVATOR_REGISTRY_REPLICATION_FACTOR",
    default_value = "1"
)]
registry_replication_factor: ReplicationFactor,
#[arg(
    long,
    env = "CRABKA_GRES_ACTIVATOR_REGISTRY_POLL_MS",
    default_value = "250"
)]
registry_poll_ms: PositiveMillis,
#[arg(
    long,
    env = "CRABKA_GRES_ACTIVATOR_COLD_START_TIMEOUT_MS",
    default_value = "30000"
)]
cold_start_timeout_ms: PositiveMillis,
#[arg(
    long,
    env = "CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE",
    default_value = "{tenant}:5432"
)]
backend_endpoint_template: NonEmptyValue,
```

Convert the validated values directly into the existing `ActivatorConfig`,
and pass the validated replication factor to `Registry::ensure_topic`. Do not
add a second runtime configuration struct. The topic's single partition is a
registry-ordering invariant and remains fixed.

- [x] **Step 4: Verify Task 1**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo nextest run -p crabka-gres-activator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-gres-activator -- --help
```

Commit only Task 1 files.

---

### Task 2: Expose Operator-Managed Activator Policy

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/crd/mod.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/tests/reconcile_gres.rs`
- Modify: `crates/gres-control/src/pgdog.rs`
- Modify: `deploy/crds/crabka.io_greses.yaml`

- [x] **Step 1: Add failing CRD, render, and fail-fast tests**

Specify this optional CRD surface:

```rust
pub struct GresActivatorSpec {
    pub image: Option<String>,
    pub replicas: Option<i32>,
    pub registry_replication_factor: Option<i32>, // schema range 1..=32767
    pub registry_poll_ms: Option<u64>,
    pub cold_start_timeout_ms: Option<u64>,
    pub readiness_probe_period_seconds: Option<i32>,
}
```

Add schema ranges (`min = 1`) to every present field. Test:

- omitted values preserve all current output;
- custom values render exact Deployment replicas, flags, and probe period;
- zero values are rejected before any child `PATCH`;
- custom cold-start timeout also expands the rendered PgDog connection budget
  so the pooler cannot time out before the activator.

Run the focused tests and observe RED before production changes.

- [x] **Step 2: Add the optional typed CRD block**

Add:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub activator: Option<GresActivatorSpec>,
```

Use `Option` fields so existing manifests and serialized fixtures remain
compatible. Re-export the type through the existing CRD module.

- [x] **Step 3: Validate once at the reconcile boundary**

At the start of `reconcile_inner`, validate every present field with
`refined_type` and return `ReconcileError::Malformed` with the exact camel-case
CRD path. Do this before Kubernetes reads or writes.

Use small effective-value helpers or local expressions only where they avoid
duplicating defaults. Do not create a builder.

- [x] **Step 4: Render effective values**

Render explicit:

```text
--registry-poll-ms <effective value>
--cold-start-timeout-ms <effective value>
```

Set the Deployment replica and readiness period from effective values. Keep
listen/bootstrap/template operator-derived.

The PgDog connection attempt budget must continue covering the configured
activator timeout. Add one checked `PgdogTimeouts` helper that derives the
cold-start ceiling from one attempt timeout using its existing default attempt
count; use that helper from the operator. Reject multiplication overflow before
Kubernetes I/O. Do not introduce a second timeout model or duplicate the
attempt-count constant.

- [x] **Step 5: Regenerate and compare CRDs**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator -- gen-crds deploy/crds
```

Only `deploy/crds/crabka.io_greses.yaml` may change.

- [x] **Step 6: Verify Task 2**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo nextest run -p crabka-operator --test reconcile_gres --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator crd::gres --lib
```

Commit only Task 2 files.

---

### Task 3: Audit and Close the Activator Slice

**Files:**

- Modify as findings require:
  `crates/gres-activator/**`,
  `crates/operator/src/{crd,controller}/gres.rs`,
  `deploy/crds/crabka.io_greses.yaml`
- Modify: `docs/configuration-audit.md`

- [x] **Step 1: Run the scanner and inspect every activator production hit**

```bash
tools/audit-runtime-values.sh > /tmp/crabka-runtime-values-gres-activator.txt
rg 'crates/gres-activator|controller/gres.rs|crd/gres.rs' \
  /tmp/crabka-runtime-values-gres-activator.txt
```

Classify every hit as configurable deployment policy, fixed
protocol/serialization/topology invariant, derived value, or test fixture.
Fix any missed configurable production value before documenting closure.

- [x] **Step 2: Run completion gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo nextest run -p crabka-gres-activator -p crabka-operator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-activator -p crabka-operator \
    --all-targets -- -D warnings
cargo +nightly fmt --all -- --check
tmp_dir="$(mktemp -d)"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator -- gen-crds "$tmp_dir"
diff -u deploy/crds/crabka.io_greses.yaml \
  "$tmp_dir/crabka.io_greses.yaml"
git diff --check
```

- [x] **Step 3: Record exact evidence**

Update `docs/configuration-audit.md` with candidate counts, classifications,
commands, and results. Mark only the activator/front-door sub-slice complete;
the remaining Gres control, ranges, substrate, tenant compute, load tools, and
conformance code stay pending.

Commit the audit fixes and documentation separately where practical.
