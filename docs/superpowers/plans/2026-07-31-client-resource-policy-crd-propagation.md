# Kafka Client Resource Policy CRD Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry the validated Kafka client queue/frame policy and approved
fetch minima from every existing owning CRD into its rendered workload.

**Architecture:** Add optional fields to each existing workload-policy struct,
validate them with the existing client-core refined newtypes, and append the
already-supported process flags only when configured. Keep the shared registry
fetch minimum in Kafka registry policy while activator and compute retain
separate process-wide queue/frame pairs.

**Tech Stack:** Rust, Clap, `kube`, `schemars`, `serde`, `crabka-units`,
`refined_type`, Cargo.

## Global Constraints

- Preserve queue `64`, frame maximum `100MiB`, and fetch minimum `1B`.
- Keep the fixed `100MiB` client frame security ceiling non-configurable.
- Queue capacity is dimensionless; frame and fetch values use human
  `ByteSize` syntax and explicit `B` rendering.
- Validate configured values before rendering or Kubernetes mutation.
- Omitted CRD fields render no new argument.
- One process gets one queue/frame pair.
- Use existing client-core refined newtypes; add no duplicate validators.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

---

### Task 1: Add the Activator Process Surface

**Files:**
- Modify: `crates/gres-activator/src/main.rs`

**Interfaces:**
- Consumes:
  `ConnectionDispatchQueueCapacity`, `ClientFrameMax`, `FetchMinBytes`
- Produces: `RegistryOptions::policy() -> Result<RegistryPolicy, String>` with
  the activator's validated client resource policy

- [ ] **Step 1: Write parser and propagation tests**

Add tests that parse defaults, environment values, and overriding CLI values:

```rust
assert_eq!(
    defaults.registry.policy().unwrap(),
    RegistryPolicy::default()
);
assert_eq!(
    configured.registry.policy().unwrap().dispatch_queue_capacity().get(),
    7
);
assert_eq!(
    configured.registry.policy().unwrap().frame_max().size(),
    crabka_units::kibibytes(32)
);
assert_eq!(
    configured.registry.policy().unwrap().reader_fetch_min().size(),
    crabka_units::bytes(3)
);
```

Cover zero queue, `0B`, `1.5B`, and `101MiB` rejection before network I/O.

- [ ] **Step 2: Run the focused test and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator --all-targets --locked
```

Expected: parser fields or policy getters are absent.

- [ ] **Step 3: Add the minimal inputs and typed policy assembly**

Add:

```rust
#[arg(
    long = "client-dispatch-queue-capacity",
    env = "CRABKA_GRES_ACTIVATOR_CLIENT_DISPATCH_QUEUE_CAPACITY",
    default_value_t = crabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY
)]
client_dispatch_queue_capacity: usize,

#[arg(
    long = "client-frame-max",
    env = "CRABKA_GRES_ACTIVATOR_CLIENT_FRAME_MAX",
    default_value = "100MiB"
)]
client_frame_max: ByteSize,

#[arg(
    long = "registry-reader-fetch-min",
    env = "CRABKA_GRES_REGISTRY_READER_FETCH_MIN",
    default_value = "1B"
)]
reader_fetch_min: ByteSize,
```

Validate with the three existing client-core types and apply
`RegistryPolicy::with_client_resource_policy`. Return the validation error
instead of using `expect`.

- [ ] **Step 4: Run tests, strict Clippy, and format**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-activator --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add crates/gres-activator/src/main.rs
git commit -m "feat(gres-activator): expose client policy"
```

---

### Task 2: Add Gres Registry, Activator, and Compute CRD Fields

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/crd/gres.rs`

**Interfaces:**
- Produces:
  - `GresRegistrySpec.reader_fetch_min: Option<ByteSize>`
  - `GresActivatorSpec.client_dispatch_queue_capacity: Option<usize>`
  - `GresActivatorSpec.client_frame_max: Option<ByteSize>`
  - `GresComputeSpec.client_dispatch_queue_capacity: Option<usize>`
  - `GresComputeSpec.client_frame_max: Option<ByteSize>`
  - `GresComputeSpec.fdw_fetch_min: Option<ByteSize>`
  - `GresComputeSpec.wal_recovery_fetch_min: Option<ByteSize>`

- [ ] **Step 1: Write serde, schema, and validation tests**

Use non-default values:

```json
{
  "clientDispatchQueueCapacity": 7,
  "clientFrameMax": "32KiB",
  "fdwFetchMin": "2B",
  "walRecoveryFetchMin": "3B"
}
```

Assert queue schema minimum `1`, byte fields have schema type `string`, omitted
fields remain `None`, and field-qualified errors reject zero/fractional/
over-ceiling values.

- [ ] **Step 2: Run focused CRD tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator crd::gres --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator crd::kafka --lib --locked
```

- [ ] **Step 3: Add optional fields with existing serializers**

Queue fields use:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub client_dispatch_queue_capacity: Option<usize>,
```

Byte fields use:

```rust
#[serde(
    default,
    skip_serializing_if = "Option::is_none",
    with = "crabka_units::serde_units::human::option_byte_size"
)]
#[schemars(with = "Option<String>")]
pub client_frame_max: Option<ByteSize>,
```

Apply `reader_fetch_min` through
`RegistryPolicy::with_client_resource_policy`, retaining the policy's default
queue/frame values because those are overridden per consuming process.

- [ ] **Step 4: Extend effective compute validation**

Validate optional queue/frame/fetch values with:

```rust
ConnectionDispatchQueueCapacity::new(value)
ClientFrameMax::try_from(value)
FetchMinBytes::try_from(value)
```

Store typed optional values in `EffectiveGresComputePolicy`; do not add raw
byte or integer duplicates.

- [ ] **Step 5: Run focused tests and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator crd::gres --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator crd::kafka --lib --locked
git diff --check
git add crates/operator/src/crd/gres.rs crates/operator/src/crd/kafka.rs
git commit -m "feat(operator): add Gres client policy fields"
```

---

### Task 3: Render Gres Policy Exactly Once

**Files:**
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`

**Interfaces:**
- Consumes: typed optional fields from Task 2
- Produces:
  - activator flags for activator queue/frame and shared reader fetch minimum
  - compute flags for compute queue/frame, shared reader fetch minimum, FDW
    fetch minimum, and WAL-recovery fetch minimum

- [ ] **Step 1: Write failing exact-argument tests**

Assert configured values render as:

```text
--client-dispatch-queue-capacity 7
--client-frame-max 32768B
--registry-reader-fetch-min 4B
--fdw-fetch-min 2B
--wal-recovery-fetch-min 3B
```

Check every pair appears exactly once, omission emits none, activator never
receives FDW/WAL flags, and both single- and multi-range compute deployments
receive the correct process pair.

- [ ] **Step 2: Run controller tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator controller::gres --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator controller::gres_tenant --lib --locked
```

- [ ] **Step 3: Extend existing argument builders**

Keep `registry_policy_args`, `wal_consumer_admin_args`, and
`render_activator_deployment` as the only rendering seams. Append optional
arguments using typed getters and `ByteSize::human()` or explicit whole-byte
`B` strings. Do not create a generic argument framework.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator controller::gres --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator controller::gres_tenant --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --locked -- -D warnings
git diff --check
git add crates/operator/src/controller/gres.rs crates/operator/src/controller/gres_tenant.rs
git commit -m "feat(operator): render Gres client policy"
```

---

### Task 4: Wire KafkaNodePool Broker Policy

**Files:**
- Modify: `crates/operator/src/crd/kafka_node_pool.rs`
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

**Interfaces:**
- Produces optional `clientDispatchQueueCapacity` and `clientFrameMax` fields
  rendered as broker CLI flags

- [ ] **Step 1: Write failing CRD and pod-render tests**

Assert schema/serde behavior, validation errors, no flags when omitted, and
exact configured fragments:

```text
--client-dispatch-queue-capacity=7
--client-frame-max=32768B
```

Cover both metrics-disabled and metrics-enabled main scripts.

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator kafka_node_pool --lib --locked
```

- [ ] **Step 3: Add fields, validation, and minimal script rendering**

Add the same optional field shapes as Task 2. Validate before
`render_broker_container`. Extend `build_main_script` to accept the two typed
options and append only configured flags while preserving the existing
byte-for-byte constant when both are absent.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator kafka_node_pool --lib --locked
git diff --check
git add crates/operator/src/crd/kafka_node_pool.rs crates/operator/src/controller/kafka_node_pool.rs
git commit -m "feat(operator): render broker client policy"
```

---

### Task 5: Wire Gateway Policy

**Files:**
- Modify: `crates/operator/src/crd/grpc_gateway.rs`
- Modify: `crates/operator/src/controller/grpc_gateway.rs`

**Interfaces:**
- Adds optional queue/frame fields to `GatewayTuning`
- Renders existing gateway CLI flags from `gateway_args`

- [ ] **Step 1: Add failing serde, validation, and exact-argument tests**

Assert omission, queue minimum schema, frame string schema, invalid boundary
errors, and exactly one configured queue/frame flag in the Deployment.

- [ ] **Step 2: Run the focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator grpc_gateway --lib --locked
```

- [ ] **Step 3: Implement fields and render through existing seams**

Add fields to `GatewayTuning`, validate them in `validate_config`, and append
configured values in `gateway_args`. Render frame maximum as whole bytes with
`B`; do not inject environment variables.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator grpc_gateway --lib --locked
git diff --check
git add crates/operator/src/crd/grpc_gateway.rs crates/operator/src/controller/grpc_gateway.rs
git commit -m "feat(operator): render gateway client policy"
```

---

### Task 6: Wire Schema Registry Policy

**Files:**
- Modify: `crates/operator/src/crd/schema_registry.rs`
- Modify: `crates/operator/src/controller/schema_registry.rs`

**Interfaces:**
- Adds optional queue/frame fields to `SchemaRegistryRuntime`
- Renders existing Schema Registry CLI flags through `build_args_and_mounts`

- [ ] **Step 1: Add failing serde, validation, and argument tests**

Prove omission, schema shape, invalid boundaries, and exact configured output
from `build_args_and_mounts`.

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_registry --lib --locked
```

- [ ] **Step 3: Add fields, validation, and rendering**

Add fields to `SchemaRegistryRuntime`. Validate before
`build_args_and_mounts`, then extend its existing `push_runtime!` quantity
path to append `--client-dispatch-queue-capacity` and
`--client-frame-max=<bytes>B` only when configured.

- [ ] **Step 4: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_registry --lib --locked
git diff --check
git add crates/operator/src/crd/schema_registry.rs crates/operator/src/controller/schema_registry.rs
git commit -m "feat(operator): render registry client policy"
```

---

### Task 7: Regenerate Schemas, Audit, and Verify

**Files:**
- Modify: generated files under `deploy/crds/`
- Modify: `docs/configuration-audit.md`
- Modify: this plan's checkboxes

**Interfaces:**
- Proves checked-in OpenAPI schemas and all runtime render paths match the
  approved design

- [ ] **Step 1: Run the constructor and surface audit**

```bash
rg -n 'clientDispatchQueueCapacity|clientFrameMax|readerFetchMin|fdwFetchMin|walRecoveryFetchMin' \
  crates/operator deploy/crds
rg -n 'client-dispatch-queue-capacity|client-frame-max|registry-reader-fetch-min|fdw-fetch-min|wal-recovery-fetch-min' \
  crates/gres-activator crates/operator
```

Classify every production hit as schema, validation, or exact rendering.

- [ ] **Step 2: Regenerate CRDs twice and compare**

```bash
crd_first=$(mktemp -d)
crd_second=$(mktemp -d)
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_first"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_second"
diff -ru "$crd_first" "$crd_second"
cp "$crd_first"/* deploy/crds/
crd_verify=$(mktemp -d)
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_verify"
diff -ru deploy/crds "$crd_verify"
```

- [ ] **Step 3: Run full affected tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator -p crabka-operator --all-targets --locked
```

- [ ] **Step 4: Run workspace gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

- [ ] **Step 5: Re-run affected tests after final formatting**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator -p crabka-operator --all-targets --locked
```

- [ ] **Step 6: Update the audit and commit**

Record exact CRD paths, CLI/environment names, validation types, generated
schema evidence, and that the broader repository-wide hardcoded-value audit
remains active.

```bash
git add deploy/crds docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-client-resource-policy-crd-propagation.md
git commit -m "docs(config): close client policy CRDs"
```
