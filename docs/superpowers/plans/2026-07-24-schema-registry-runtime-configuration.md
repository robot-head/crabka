# Schema Registry Runtime Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose every Schema Registry deployment-policy value through validated CLI/environment inputs and the `SchemaRegistry` CRD while keeping protocol and persistence invariants fixed.

**Architecture:** `RegistryConfig` remains the single runtime owner. A nested `RegistryRuntimeConfig` carries service policy from Clap into election, `_schemas` topic creation, the store reader, and store defaults. The operator adds an optional typed `spec.runtime` plus service-specific health checks and renders the same flags.

**Tech Stack:** Rust 2024, `refined_type` 0.6, Clap, kube/schemars CRDs, Cargo nextest.

## Global Constraints

- Preserve current direct-process and operator-managed defaults.
- Configure only deployment policy; keep Kafka error codes, Schema Registry wire values, `_schemas` partition zero/count one, compacted-log semantics, idempotence, and `acks=all` fixed.
- New validated scalar inputs use `refined_type`; never use `unsafe_new`.
- Every process setting has a Clap flag backed by an environment variable.
- Every operator-managed process setting has a typed CRD field and is validated before child resources are rendered.
- Use existing service config and operator rendering paths; add no shared config crate or generic string map.
- Tests use `assert2`; add no Clippy suppressions.

## Runtime Field Table

| Field | Type | Direct/operator default | Constraint |
|---|---:|---:|---|
| `election_session_timeout_ms` | `i32` | `10000` | `>= 1` |
| `election_rebalance_timeout_ms` | `i32` | `30000` | `>= session` |
| `election_heartbeat_interval_ms` | `u64` | `3000` | `1..session` |
| `election_reconnect_backoff_ms` | `u64` | `500` | `>= 1` |
| `store_reader_retry_backoff_ms` | `u64` | `250` | `>= 1` |
| `store_reader_fetch_max_wait_ms` | `i32` | `500` | `>= 1` |
| `store_reader_fetch_max_bytes` | `i32` | `1048576` | `>= 1` |
| `schemas_topic_create_timeout_ms` | `i32` | `15000` | `>= 1` |
| `default_compatibility_level` | enum string | `BACKWARD` | existing compatibility enum |
| `default_mode` | enum string | `READWRITE` | `READWRITE`, `READONLY`, or `IMPORT` |

Direct-only existing setting: `admin_listen_addr`, default `0.0.0.0:9404`, gains `--admin-listen-addr` backed by `CRABKA_ADMIN_LISTEN_ADDR`. The operator has no admin Service, so a CRD field would be inert.

---

### Task 1: Add Validated Schema Registry Runtime Inputs

**Files:**

- Modify: `crates/schema-registry/Cargo.toml`
- Create: `crates/schema-registry/src/config_value.rs`
- Modify: `crates/schema-registry/src/lib.rs`
- Modify: `crates/schema-registry/src/config.rs`
- Modify: `crates/schema-registry/src/compat/mod.rs`
- Modify: `crates/schema-registry/src/bin/schema-registry.rs`
- Test: colocated modules in those files.

**Interfaces:**

- Produces: `PositiveMillis`, `PositiveI32`, `RegistryRuntimeConfig::default()`, and `RegistryRuntimeConfig::validate()`.
- Consumes: `refined_type::rule::{GreaterI32, GreaterU64}`.

- [ ] **Step 1: Write failing refined-boundary and default tests**

Add tests that require:

```rust
#[test]
fn runtime_scalar_boundaries_and_defaults() {
    assert2::check!(PositiveMillis::new(0).is_err());
    assert2::check!(PositiveMillis::new(1).is_ok());
    assert2::check!(PositiveI32::new(0).is_err());
    assert2::check!(PositiveI32::new(1).is_ok());
    assert2::assert!(
        RegistryRuntimeConfig::default()
            == RegistryRuntimeConfig {
                election_session_timeout_ms: 10_000,
                election_rebalance_timeout_ms: 30_000,
                election_heartbeat_interval_ms: 3_000,
                election_reconnect_backoff_ms: 500,
                store_reader_retry_backoff_ms: 250,
                store_reader_fetch_max_wait_ms: 500,
                store_reader_fetch_max_bytes: 1_048_576,
                schemas_topic_create_timeout_ms: 15_000,
                default_compatibility_level: "BACKWARD".into(),
                default_mode: "READWRITE".into(),
            }
    );
}

#[test]
fn runtime_relations_are_rejected() {
    let mut runtime = RegistryRuntimeConfig::default();
    runtime.election_heartbeat_interval_ms = 10_000;
    assert2::assert!(runtime.validate().is_err());
    runtime = RegistryRuntimeConfig::default();
    runtime.election_rebalance_timeout_ms = 9_999;
    assert2::assert!(runtime.validate().is_err());
}
```

- [ ] **Step 2: Run the tests to verify RED**

Run:

```bash
cargo test -p crabka-schema-registry runtime_scalar_boundaries_and_defaults
cargo test -p crabka-schema-registry runtime_relations_are_rejected
```

Expected: compilation fails because the input types and runtime config do not exist.

- [ ] **Step 3: Implement the two local refined wrappers**

`config_value.rs` must expose this API and implement `FromStr` by parsing the primitive and calling the indicated refined rule:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositiveMillis(u64);

impl PositiveMillis {
    pub fn new(value: u64) -> Result<Self, String>;
    #[must_use]
    pub const fn into_value(self) -> u64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositiveI32(i32);

impl PositiveI32 {
    pub fn new(value: i32) -> Result<Self, String>;
    #[must_use]
    pub const fn into_value(self) -> i32;
}
```

Use `GreaterU64<0>::new` and `GreaterI32<0>::new` internally. Add `refined_type.workspace = true` to the crate and export `pub mod config_value`.

- [ ] **Step 4: Add `RegistryRuntimeConfig`**

Add the exact fields from the table to:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRuntimeConfig { /* field table */ }
```

Implement the exact defaults and `validate()` rules:

```rust
if self.election_heartbeat_interval_ms
    >= u64::try_from(self.election_session_timeout_ms).unwrap_or(0)
{
    anyhow::bail!("election heartbeat interval must be below session timeout");
}
if self.election_session_timeout_ms > self.election_rebalance_timeout_ms {
    anyhow::bail!("election session timeout exceeds rebalance timeout");
}
if crate::compat::CompatibilityLevel::try_parse(&self.default_compatibility_level).is_none() {
    anyhow::bail!("invalid default compatibility level");
}
if !matches!(self.default_mode.as_str(), "READWRITE" | "READONLY" | "IMPORT") {
    anyhow::bail!("invalid default mode");
}
```

Add `pub runtime: RegistryRuntimeConfig` to `RegistryConfig`.

Add `CompatibilityLevel::try_parse(&str) -> Option<Self>` beside the existing parser, and implement `parse` as `try_parse(s).unwrap_or(Self::Backward)`. Use `try_parse` for runtime validation so an invalid configured default cannot silently become `BACKWARD`.

- [ ] **Step 5: Add CLI/environment inputs and precedence tests**

Add Option-backed fields to `Args` so explicit CLI/env values override defaults without defeating Clap precedence:

```rust
#[arg(long, env = "SCHEMA_REGISTRY_ELECTION_SESSION_TIMEOUT_MS")]
election_session_timeout_ms: Option<PositiveI32>,
```

Repeat for every field in the table using the exact `SCHEMA_REGISTRY_<UPPER_FIELD>` name. Use Clap value enums or existing parsers for compatibility and mode strings. Add:

```rust
#[arg(
    long,
    env = "CRABKA_ADMIN_LISTEN_ADDR",
    default_value = "0.0.0.0:9404"
)]
admin_listen_addr: std::net::SocketAddr,
```

Build `RegistryRuntimeConfig` from parsed overrides and `Default`, call `validate()`, place it on `RegistryConfig`, and pass `admin_listen_addr.to_string()` to `serve_admin_from_env`.

Convert the existing `schemas_topic_rf`, `acl_refresh_secs`, and present `bearer_jwks_refresh_ms` through the same positive refined wrappers. Add one table-driven test proving zero rejection, valid explicit values, environment override, CLI-over-env precedence, and all defaults.

- [ ] **Step 6: Run focused checks and commit**

Run:

```bash
cargo test -p crabka-schema-registry config_value
cargo test -p crabka-schema-registry runtime_
cargo test -p crabka-schema-registry --bin crabka-schema-registry
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
```

Commit only Task 1 files:

```bash
git commit -m "feat(schema-registry): add runtime inputs"
```

---

### Task 2: Route Runtime Policy to Production Consumers

**Files:**

- Modify: `crates/schema-registry/src/election/mod.rs`
- Modify: `crates/schema-registry/src/election/client.rs`
- Modify: `crates/schema-registry/src/kafkastore/reader.rs`
- Modify: `crates/schema-registry/src/kafkastore/topic.rs`
- Modify: `crates/schema-registry/src/kafkastore/mod.rs`
- Modify: `crates/schema-registry/src/store/mod.rs`
- Modify: every `RegistryConfig` fixture reported by `rg -l 'RegistryConfig \\{' crates/schema-registry`

**Interfaces:**

- Consumes: `RegistryConfig.runtime`.
- Produces: no local policy literals for the runtime table and configured store fallbacks.

- [ ] **Step 1: Write production-path tests that fail with the old constants**

Add narrow helpers used by production, with tests requiring:

```rust
assert2::assert!(
    election_policy(&configured_runtime)
        == ElectionPolicy {
            session_timeout_ms: 12_000,
            rebalance_timeout_ms: 40_000,
            heartbeat_interval: Duration::from_millis(2_000),
            reconnect_backoff: Duration::from_millis(750),
        }
);
assert2::assert!(
    reader_policy(&configured_runtime)
        == ReaderPolicy {
            retry_backoff: Duration::from_millis(333),
            fetch_max_wait_ms: 777,
            fetch_max_bytes: 2_097_152,
        }
);
assert2::assert!(schemas_topic_spec(&cfg).1 == 22_000);
```

The helpers must be called by the real election loop, reader task, and topic creation function; do not add test-only mirrors.

- [ ] **Step 2: Run focused tests to verify RED**

Run:

```bash
cargo test -p crabka-schema-registry election_policy
cargo test -p crabka-schema-registry reader_policy
cargo test -p crabka-schema-registry schemas_topic_spec
```

Expected: compilation fails until the helpers and production wiring exist.

- [ ] **Step 3: Replace election, reader, and topic literals**

Copy `RegistryRuntimeConfig` into `ElectionClient`, use it for both `JoinGroupRequest` fields, heartbeat sleep, and reconnect sleep. In the reader task, copy the three reader values before spawning and use them in every retry/fetch branch. Make `ensure_schemas_topic` pass the configured create timeout to `AdminClient::create_topics`.

Delete `SESSION_TIMEOUT_MS`, `REBALANCE_TIMEOUT_MS`, `HEARTBEAT_INTERVAL`, all four reader `250ms` literals, the fetch `500`/`1 << 20`, and the topic `15_000`.

- [ ] **Step 4: Configure initial compatibility and mode defaults**

Add default strings to `StoreState` separately from replayed overrides:

```rust
pub fn with_defaults(compatibility: String, mode: String) -> Self;
```

`global_compat()` and `global_mode()` return replayed values first and configured defaults second. `KafkaStore::start` initializes the state with `cfg.runtime` defaults before the reader begins. Tests must prove a configured initial value is returned and a replayed global record overrides it.

- [ ] **Step 5: Update fixtures, run focused checks, and commit**

Every existing `RegistryConfig` fixture gets `runtime: RegistryRuntimeConfig::default()`.

Run:

```bash
cargo test -p crabka-schema-registry election
cargo test -p crabka-schema-registry kafkastore
cargo test -p crabka-schema-registry store
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
```

Commit:

```bash
git commit -m "refactor(schema-registry): use runtime policy"
```

---

### Task 3: Expose Schema Registry Policy in the CRD

**Files:**

- Modify: `crates/operator/src/crd/schema_registry.rs`
- Modify: `crates/operator/src/controller/schema_registry.rs`
- Modify: `crates/operator/tests/reconcile_schema_registry.rs`
- Modify: `deploy/crds/crabka.io_schemaregistries.yaml`

**Interfaces:**

- Produces: `SchemaRegistrySpec.runtime`, `clientId`, and `healthChecks`.
- Consumes: the Task 1 flag names and constraints.

- [ ] **Step 1: Write failing CRD render and rejection tests**

Add a reconciliation test with nondefault values for all ten runtime fields, `clientId`, and:

```rust
health_checks: Some(SchemaRegistryHealthChecks {
    readiness_initial_delay_seconds: Some(3),
    readiness_period_seconds: Some(7),
    liveness_initial_delay_seconds: Some(9),
    liveness_period_seconds: Some(11),
})
```

Assert exact container flags and Probe timing values. Add invalid cases for zero positive fields, heartbeat/session/rebalance relationships, empty `clientId`, invalid compatibility/mode, nonpositive existing schemas RF/JWKS refresh/ACL refresh, and verify reason `SchemaRegistryConfigInvalid` with no Deployment rendered.

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p crabka-operator --test reconcile_schema_registry runtime_
```

Expected: compilation fails because the CRD types and fields do not exist.

- [ ] **Step 3: Add the typed CRD fields**

Add optional camel-case structs:

```rust
pub struct SchemaRegistryRuntime {
    pub election_session_timeout_ms: Option<i32>,
    pub election_rebalance_timeout_ms: Option<i32>,
    pub election_heartbeat_interval_ms: Option<u64>,
    pub election_reconnect_backoff_ms: Option<u64>,
    pub store_reader_retry_backoff_ms: Option<u64>,
    pub store_reader_fetch_max_wait_ms: Option<i32>,
    pub store_reader_fetch_max_bytes: Option<i32>,
    pub schemas_topic_create_timeout_ms: Option<i32>,
    pub default_compatibility_level: Option<String>,
    pub default_mode: Option<String>,
}

pub struct SchemaRegistryHealthChecks {
    pub readiness_initial_delay_seconds: Option<i32>,
    pub readiness_period_seconds: Option<i32>,
    pub liveness_initial_delay_seconds: Option<i32>,
    pub liveness_period_seconds: Option<i32>,
}
```

Annotate positive scalar fields with `#[schemars(range(min = 1))]`; initial delays use `min = 0`. Add `runtime`, `client_id`, and `health_checks` to `SchemaRegistrySpec`.

- [ ] **Step 4: Validate before rendering and emit flags**

Use `refined_type` directly in the operator for scalar checks and explicit relation/string checks. Reuse the existing reconciliation error/condition path; do not depend on the Schema Registry crate. Render present runtime fields as `--kebab-case=value`, `clientId` as `--client-id`, and health checks into the existing Kubernetes Probe objects.

Also validate existing `schemas_topic_replication_factor`, `jwks_refresh_ms`, and `acl_refresh_seconds` as positive before converting/rendering them.

- [ ] **Step 5: Regenerate, test, and commit**

Run:

```bash
cargo test -p crabka-operator --test reconcile_schema_registry
cargo test -p crabka-operator --lib crd::schema_registry
cargo run -p crabka-operator -- gen-crds /tmp/crabka-schema-registry-crds
diff -u deploy/crds/crabka.io_schemaregistries.yaml /tmp/crabka-schema-registry-crds/crabka.io_schemaregistries.yaml
cargo clippy -p crabka-operator --all-targets -- -D warnings
```

Commit:

```bash
git commit -m "feat(operator): expose registry tuning"
```

---

### Task 4: Close the Schema Registry Audit

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify only if a real gap is found: files from Tasks 1–3.

- [ ] **Step 1: Run the fresh semantic scan**

Run:

```bash
tools/audit-runtime-values.sh | rg '^crates/schema-registry/' > /tmp/crabka-schema-registry-runtime-values.txt
```

Classify every line. Fixed groups must include Kafka error codes, Schema Registry protocol/content-type/header/enums, `_schemas` ordered single-partition and compacted-log invariants, idempotence/acks durability, and test fixtures. Configure any remaining production policy before continuing.

- [ ] **Step 2: Run completion gates**

Run:

```bash
cargo +nightly fmt --all -- --check
cargo clippy -p crabka-schema-registry -p crabka-operator --all-targets -- -D warnings
cargo nextest run -p crabka-schema-registry -p crabka-operator
cargo run -p crabka-schema-registry -- --help | rg 'election-session|store-reader|schemas-topic-create|default-compatibility|admin-listen'
cargo run -p crabka-operator -- gen-crds /tmp/crabka-schema-registry-crds
diff -u deploy/crds/crabka.io_schemaregistries.yaml /tmp/crabka-schema-registry-crds/crabka.io_schemaregistries.yaml
git diff --check
```

- [ ] **Step 3: Record only Schema Registry completion and commit**

Update the ledger with the exact count, fixed classifications, and gate evidence. Do not claim the gateway, operator, or repository complete.

```bash
git commit -m "docs: close registry config audit"
```
