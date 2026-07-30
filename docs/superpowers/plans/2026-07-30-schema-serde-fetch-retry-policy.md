# Schema Serde Fetch Retry Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the schema cache's background fetch retry range through one validated UOM policy and the observability-demo and Gres deployment owners while preserving all existing defaults and retry semantics.

**Architecture:** `crabka-schema-serde` owns an opaque `SchemaFetchRetryPolicy` and applies it inside its existing retry-delay algorithm. Client Streams continues to consume `CacheConfig`; the observability demo constructs one policy for all three roles, while Gres threads the same policy through `KafkaFdw` and its compute CRD. The library never reads process-global environment variables.

**Tech Stack:** Rust, Tokio, Clap, `crabka-units`, Kube `JsonSchema`, generated CRDs.

## Global Constraints

- Preserve the initial retry default at `10ms` and maximum retry default at `1s`.
- Keep time values as UOM `Time` through every configuration boundary.
- Reject zero, negative, non-finite, or `std::time::Duration`-unrepresentable times and reject `initial_backoff > max_backoff`; equal bounds remain valid.
- Keep `SchemaCache::new` infallible and retain `CacheConfig::default()` compatibility.
- Keep Confluent media type and magic byte, the 64-reference traversal ceiling, exponential doubling, exponent cap `7`, deterministic zero-to-25-percent jitter, and terminal/transient error classification fixed.
- Add no generic retry framework, no library-global environment lookup, and no Kafka or Schema Registry CRD fields.
- Use the existing Client Streams `cache_config` builder input rather than adding duplicate Client Streams fields.
- Gres owns `CRABKA_GRES_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF` and `CRABKA_GRES_SCHEMA_FETCH_RETRY_MAX_BACKOFF`; the observability demo owns the corresponding `CRABKA_DEMO_*` variables.
- Preserve unrelated dirty-worktree changes and stage only each task's named hunks.
- Do not modify or stage the four protected untracked plans dated 2026-07-28.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not run `cargo clean` until the entire repository-wide configuration goal is complete.

---

### Task 1: Add the validated schema fetch retry policy

**Files:**
- Modify: `crates/schema-serde/Cargo.toml`
- Modify: `crates/schema-serde/src/cache.rs`
- Modify: `crates/schema-serde/src/lib.rs`
- Modify selected dependency hunk only: `Cargo.lock`

**Interfaces:**
- Produces: `DEFAULT_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF: Time`
- Produces: `DEFAULT_SCHEMA_FETCH_RETRY_MAX_BACKOFF: Time`
- Produces: `SchemaFetchRetryPolicy::new(Time, Time) -> Result<Self, String>`
- Produces: `SchemaFetchRetryPolicy::initial_backoff(self) -> Time`
- Produces: `SchemaFetchRetryPolicy::max_backoff(self) -> Time`
- Produces: `SchemaCache::fetch_retry_policy(&self) -> SchemaFetchRetryPolicy`
- Extends: `CacheConfig::fetch_retry_policy: SchemaFetchRetryPolicy`

- [x] **Step 1: Write failing default and validation tests**

Add `crabka_units::prelude::*` to the cache test module and write:

```rust
#[test]
fn fetch_retry_policy_defaults_preserve_behavior() {
    let policy = SchemaFetchRetryPolicy::default();
    check!(policy.initial_backoff() == millis(10));
    check!(policy.max_backoff() == secs(1));
    check!(CacheConfig::default().fetch_retry_policy == policy);
}

#[test]
fn fetch_retry_policy_accepts_equal_bounds() {
    let policy = SchemaFetchRetryPolicy::new(millis(37), millis(37)).unwrap();
    check!(policy.initial_backoff() == millis(37));
    check!(policy.max_backoff() == millis(37));
}

#[test]
fn fetch_retry_policy_rejects_invalid_values() {
    for (initial, maximum) in [
        (Time::ZERO, secs(1)),
        (millis(1), Time::ZERO),
        (Time::from_secs_f64(f64::INFINITY), secs(1)),
        (millis(1), Time::from_secs_f64(f64::INFINITY)),
        (millis(2), millis(1)),
    ] {
        assert!(SchemaFetchRetryPolicy::new(initial, maximum).is_err());
    }
}
```

- [x] **Step 2: Write failing retry-algorithm tests**

Change the private helper's intended signature to
`retry_delay(policy: SchemaFetchRetryPolicy, id: u32, attempt: u32)`.
Add:

```rust
#[test]
fn retry_delay_uses_custom_range_without_changing_jitter() {
    let policy = SchemaFetchRetryPolicy::new(millis(40), millis(100)).unwrap();
    let first = retry_delay(policy, 0, 1);
    let second = retry_delay(policy, 0, 2);
    let capped = retry_delay(policy, 0, 100);
    check!(first == millis(40).to_std());
    check!(second == millis(80).to_std());
    check!(capped == millis(100).to_std());

    let jittered = retry_delay(policy, 1, 1);
    assert!(jittered >= first);
    assert!(jittered <= policy.max_backoff().to_std());
}

#[test]
fn cache_retains_configured_fetch_retry_policy() {
    let policy = SchemaFetchRetryPolicy::new(millis(37), millis(91)).unwrap();
    let cache = SchemaCache::new(
        RegistryClient::new("http://unused"),
        CacheConfig {
            fetch_retry_policy: policy,
            ..CacheConfig::default()
        },
    );
    check!(cache.fetch_retry_policy() == policy);
}
```

- [x] **Step 3: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-schema-serde fetch_retry --all-features --locked
```

Expected: compilation fails because the policy, constants, config field, and
accessor do not exist.

- [x] **Step 4: Implement the minimal validated policy**

Add `crabka-units = { workspace = true }` to the crate. Replace the two private
`Duration` constants with:

```rust
pub const DEFAULT_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF: Time = millis(10);
pub const DEFAULT_SCHEMA_FETCH_RETRY_MAX_BACKOFF: Time = secs(1);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchemaFetchRetryPolicy {
    initial_backoff: Time,
    max_backoff: Time,
}
```

Implement `new` with one local validation helper:

```rust
fn positive_duration(field: &str, value: Time) -> Result<(), String> {
    let duration = std::time::Duration::try_from_secs_f64(value.secs_f64())
        .map_err(|error| format!("{field}: {error}"))?;
    if duration.is_zero() {
        return Err(format!("{field} must be positive"));
    }
    Ok(())
}
```

Validate both fields, then the ordering relation. Implement accessors and
`Default`. Add the policy to `CacheConfig::default`.

This is a composite policy with a cross-field invariant, not a scalar
validated newtype; do not add redundant wrapper types around `Time`.

- [x] **Step 5: Apply the policy to the existing retry helper**

Pass `self.config.fetch_retry_policy` into `retry_delay`. Convert the UOM
values to `Duration` only inside the helper:

```rust
let initial = policy.initial_backoff().to_std();
let maximum = policy.max_backoff().to_std();
```

Preserve the existing exponent, multiplier, jitter hash, percentage divisor,
checked arithmetic, and maximum clamps exactly. Add the cache accessor:

```rust
#[must_use]
pub fn fetch_retry_policy(&self) -> SchemaFetchRetryPolicy {
    self.config.fetch_retry_policy
}
```

Re-export the policy and default constants from `lib.rs`.

- [x] **Step 6: Run GREEN and the crate suite**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-schema-serde --all-targets --all-features --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-schema-serde --all-targets --all-features --locked -- -D warnings
```

- [x] **Step 7: Commit only the library policy**

Stage the three crate files and only the `crabka-schema-serde` dependency hunk
in `Cargo.lock`.

```bash
git commit -m "feat(schema): configure fetch retry range"
```

---

### Task 2: Wire the observability demo and existing Client Streams boundary

**Files:**
- Modify: `crates/client-streams/src/streams_app.rs`
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/schema_fetch_retry_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `SchemaFetchRetryPolicy::new(Time, Time) -> Result<Self, String>`
- Consumes: `CacheConfig::fetch_retry_policy`
- Consumes: `SchemaCache::fetch_retry_policy()`
- Produces environment variables:
  - `CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF`
  - `CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF`

- [x] **Step 1: Write the failing Client Streams propagation test**

In the existing `streams_app.rs` test module, build an app with:

```rust
let policy = crabka_schema_serde::SchemaFetchRetryPolicy::new(
    crabka_units::millis(37),
    crabka_units::millis(91),
)
.unwrap();
let app = StreamsApp::builder()
    .bootstrap("127.0.0.1:9092")
    .application_id("schema-retry")
    .schema_registry("http://127.0.0.1:8081")
    .cache_config(Some(crabka_schema_serde::CacheConfig {
        fetch_retry_policy: policy,
        ..Default::default()
    }))
    .build();
check!(app.cache.fetch_retry_policy() == policy);
```

No production Client Streams change is expected: this test proves the
existing `cache_config` boundary already carries the new policy.

- [x] **Step 2: Write failing observability-demo CLI tests**

Create `schema_fetch_retry_config.rs` with `std::process::Command`, matching
the existing configuration tests. Cover:

1. `--help` lists both flags exactly once.
2. `0ms` is rejected by Clap for either flag.
3. environment values `91ms` initial and `37ms` maximum fail before external
   I/O and name the inverted retry range, proving environment parsing.
4. valid environment values `37ms` and `91ms` plus a CLI initial override of
   `97ms` fail with `97ms` in the ordering error, proving CLI precedence.

Add unit tests beside `schema_fetch_retry_policy` for default, valid explicit,
and equal values. Unit tests cover successful custom construction without
starting a role or contacting Schema Registry.

- [x] **Step 3: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams cache_config_carries_schema_fetch_retry --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --test schema_fetch_retry_config --locked
```

Expected: the Client Streams test compiles after Task 1; the demo test fails
because the flags and environment variables do not exist.

- [x] **Step 4: Add and validate the demo configuration**

Add two `Option<Time>` Clap fields with `parse::positive_time`. Implement:

```rust
fn schema_fetch_retry_policy(
    cli: &Cli,
) -> std::io::Result<crabka_schema_serde::SchemaFetchRetryPolicy> {
    let defaults = crabka_schema_serde::SchemaFetchRetryPolicy::default();
    crabka_schema_serde::SchemaFetchRetryPolicy::new(
        cli.schema_fetch_retry_initial_backoff
            .unwrap_or_else(|| defaults.initial_backoff()),
        cli.schema_fetch_retry_max_backoff
            .unwrap_or_else(|| defaults.max_backoff()),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}
```

Resolve the policy once in `main`. Pass it into `order_serde` for producer and
consumer caches and into `run_stream`, where it is supplied through:

```rust
.cache_config(Some(CacheConfig {
    fetch_retry_policy,
    ..CacheConfig::default()
}))
```

Do not add role-specific validation: all three roles construct schema caches.

- [x] **Step 5: Wire deployment defaults**

Add both environment variables with `${NAME:-default}` expansion to each
produce, stream, and consume service in `demo/observability/docker-compose.yml`.
Defaults are `10ms` and `1s`.

Extend `observability_demo_config.rs` to assert each variable appears exactly
once in each role with those defaults.

- [x] **Step 6: Run GREEN and affected suites**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-streams -p observability-demo-app \
    --all-targets --locked -- -D warnings
```

- [x] **Step 7: Commit the demo owner**

```bash
git commit -m "feat(demo): configure schema fetch retry"
```

Stage only the named Client Streams, demo, test, and compose hunks.

---

### Task 3: Thread the policy through Gres and Kafka FDW

**Files:**
- Modify: `crates/gres-fdw/src/lib.rs`
- Modify: `crates/gres/src/lib.rs`

**Interfaces:**
- Consumes: `SchemaFetchRetryPolicy`
- Produces: `KafkaFdw::with_schema_fetch_retry_policy(Self, SchemaFetchRetryPolicy) -> Self`
- Produces: `KafkaFdw::schema_fetch_retry_policy(&self) -> SchemaFetchRetryPolicy`
- Produces Gres flags:
  - `--schema-fetch-retry-initial-backoff`
  - `--schema-fetch-retry-max-backoff`
- Produces Gres environment variables:
  - `CRABKA_GRES_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF`
  - `CRABKA_GRES_SCHEMA_FETCH_RETRY_MAX_BACKOFF`

- [x] **Step 1: Write failing FDW propagation tests**

Add a `schema_fetch_retry_policy` test beside the existing broker-DNS test:

```rust
let policy = SchemaFetchRetryPolicy::new(millis(37), millis(91)).unwrap();
let scanner = KafkaFdw::with_defaults(Some("broker:9092".into()))
    .with_schema_fetch_retry_policy(policy);
check!(scanner.schema_fetch_retry_policy() == policy);
```

Refactor `build_cache` to take `SchemaFetchRetryPolicy` in its intended
signature and add a unit test that asserts the resulting cache retains it.

- [x] **Step 2: Write failing Gres CLI/environment tests**

Follow `fdw_broker_dns_timeout_uses_default_environment_and_cli_precedence`.
Assert:

- defaults resolve to `10ms` and `1s`;
- environment resolves `37ms` and `91ms`;
- CLI `41ms` and `97ms` overrides environment;
- either `0ms` value is rejected by Clap;
- an inverted range is rejected by the effective-policy helper before role
  startup;
- `kafka_scanner` retains the exact custom policy.

- [x] **Step 3: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw schema_fetch_retry --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres schema_fetch_retry --lib --locked
```

Expected: compilation fails on the missing FDW field, methods, Gres flags, and
effective-policy helper.

- [x] **Step 4: Add the default-backed FDW policy**

Add `schema_fetch_retry_policy: SchemaFetchRetryPolicy` to `KafkaFdw`.
Initialize it in `Default` and `with_defaults`, implement the setter and
accessor, and pass it into each per-scan `CacheConfig`:

```rust
CacheConfig {
    fetch_retry_policy: self.schema_fetch_retry_policy,
    ..CacheConfig::default()
}
```

Re-export `SchemaFetchRetryPolicy` from `crabka-gres-fdw` so Gres can use the
type without adding a duplicate direct dependency.

- [x] **Step 5: Add the Gres CLI and effective policy**

Add two optional UOM fields to `ServeArgs`. Implement:

```rust
fn effective_schema_fetch_retry_policy(
    args: &ServeArgs,
) -> std::io::Result<crabka_gres_fdw::SchemaFetchRetryPolicy> {
    let defaults = crabka_gres_fdw::SchemaFetchRetryPolicy::default();
    crabka_gres_fdw::SchemaFetchRetryPolicy::new(
        args.schema_fetch_retry_initial_backoff
            .unwrap_or_else(|| defaults.initial_backoff()),
        args.schema_fetch_retry_max_backoff
            .unwrap_or_else(|| defaults.max_backoff()),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}
```

Extend `kafka_scanner`, `register_kafka_scanner`, and
`register_kafka_scanner_with_default_bootstrap` with the validated policy.
Resolve it before scanner registration. Keep the public no-argument
registration path default-backed.

- [x] **Step 6: Run GREEN and affected suites**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-fdw -p crabka-gres \
    --all-targets --locked -- -D warnings
```

- [x] **Step 7: Commit the Gres runtime owner**

```bash
git commit -m "feat(gres): configure schema fetch retry"
```

Stage only the schema-fetch retry hunks in the two dirty files.

---

### Task 4: Expose and render the Gres compute CRD policy

**Files:**
- Modify: `crates/operator/Cargo.toml`
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: `deploy/crds/crabka.io_greses.yaml`
- Modify selected dependency hunk only: `Cargo.lock`

**Interfaces:**
- Consumes: `SchemaFetchRetryPolicy::new(Time, Time)`
- Extends: `GresComputeSpec::schema_fetch_retry_initial_backoff: Option<Time>`
- Extends: `GresComputeSpec::schema_fetch_retry_max_backoff: Option<Time>`
- Extends: `EffectiveGresComputePolicy::schema_fetch_retry_policy`

- [x] **Step 1: Write failing CRD schema and validation tests**

Add tests beside the FDW DNS timeout coverage. Assert:

- both camelCase properties exist under `spec.compute`;
- both JSON schema types are `string`;
- neither field is required;
- omitted fields resolve to `10ms` and `1s`;
- explicit `37ms` and `91ms` values survive as exact UOM values;
- zero, non-finite, and inverted ranges fail with a
  `spec.compute.schemaFetchRetry...` path.

- [x] **Step 2: Write a failing rendered-workload test**

Extend the existing exact compute-argument test with:

```rust
schema_fetch_retry_initial_backoff: Some(millis(37)),
schema_fetch_retry_max_backoff: Some(millis(91)),
```

Assert each argument and value appears exactly once:

```text
--schema-fetch-retry-initial-backoff 37ms
--schema-fetch-retry-max-backoff 91ms
```

- [x] **Step 3: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_fetch_retry --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_fetch_retry --test reconcile_gres_tenant --locked
```

Expected: compilation fails because the CRD fields and effective policy do not
exist.

- [x] **Step 4: Add and validate the CRD fields**

Use the existing `option_time` Serde and `Option<String>` schema pattern.
During `GresComputeSpec::effective_policy`, overlay the optional values onto
`SchemaFetchRetryPolicy::default()` and call its authoritative constructor.
Store the validated policy on `EffectiveGresComputePolicy`.

Add `crabka-schema-serde = { version = "0.3.9", path = "../schema-serde" }`
as a regular operator dependency. Do not duplicate positivity or ordering
logic in the operator.

- [x] **Step 5: Render the two Gres arguments**

Add both flag/value pairs to the compute workload argument builder using
`Human`:

```rust
policy
    .schema_fetch_retry_policy
    .initial_backoff()
    .human()
    .to_string()
```

Repeat for the maximum. Do not render environment variables into the pod; the
operator owns the CRD-to-CLI boundary and the Gres binary independently owns
environment overrides.

- [x] **Step 6: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_fetch_retry --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator schema_fetch_retry --test reconcile_gres_tenant --locked
```

- [x] **Step 7: Regenerate only the Gres CRD safely**

Generate twice into exact temporary directories:

```bash
crd_tmp_a="$(mktemp -d /var/tmp/crabka-crd-a.XXXXXX)"
crd_tmp_b="$(mktemp -d /var/tmp/crabka-crd-b.XXXXXX)"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_tmp_a"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_tmp_b"
diff -u \
  "$crd_tmp_a/crabka.io_greses.yaml" \
  "$crd_tmp_b/crabka.io_greses.yaml"
```

After deterministic output is proven, replace only
`deploy/crds/crabka.io_greses.yaml`. Verify its diff contains exactly the
two new optional string properties and descriptions. Resolve both temporary
paths and remove them only if each begins with `/var/tmp/crabka-crd-`.

- [x] **Step 8: Run the operator all-target suite**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
```

- [x] **Step 9: Commit only the CRD owner**

```bash
git commit -m "feat(operator): expose schema fetch retry"
```

Stage only the schema-fetch retry hunks and the generated Gres CRD.
Include only the corresponding operator dependency hunk from `Cargo.lock`.

---

### Task 5: Close the audit slice and verify the repository boundary

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify checkboxes only: `docs/superpowers/plans/2026-07-30-schema-serde-fetch-retry-policy.md`

- [x] **Step 1: Update the schema-serde audit evidence**

Replace the pending design with:

- the two exact Rust, CLI, environment, and CRD names and preserved defaults;
- the observability-demo, Client Streams, Gres, FDW, and operator data flow;
- the fixed wire/reference/jitter behavior and why it remains fixed;
- exact test, formatting, Clippy, generated-CRD, and scanner counts.

- [x] **Step 2: Run every affected all-target suite**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-schema-serde --all-targets --all-features --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-streams --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-fdw --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
```

- [x] **Step 3: Format and run strict workspace Clippy**

```bash
cargo +nightly fmt --all
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
```

- [x] **Step 4: Verify generated schema and scanner evidence**

```bash
crd_verify_tmp="$(mktemp -d /var/tmp/crabka-crd-verify.XXXXXX)"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_verify_tmp"
diff -u \
  deploy/crds/crabka.io_greses.yaml \
  "$crd_verify_tmp/crabka.io_greses.yaml"

tools/audit-runtime-values.sh > /var/tmp/schema-serde-runtime-audit.txt
wc -l /var/tmp/schema-serde-runtime-audit.txt
cut -d: -f1 /var/tmp/schema-serde-runtime-audit.txt | sort -u | wc -l
rg '^crates/schema-serde/' /var/tmp/schema-serde-runtime-audit.txt \
  > /var/tmp/schema-serde-focused-audit.txt
wc -l /var/tmp/schema-serde-focused-audit.txt
cut -d: -f1 /var/tmp/schema-serde-focused-audit.txt | sort -u | wc -l
rg -n \
  'INITIAL_RETRY_DELAY|MAX_RETRY_DELAY|from_millis\\(10\\)|from_secs\\(1\\)|retry_delay\\(' \
  crates/schema-serde crates/client-streams crates/observability-demo-app \
  crates/gres-fdw crates/gres crates/operator
```

Resolve `crd_verify_tmp` and remove it only if it begins with
`/var/tmp/crabka-crd-verify.`.

- [x] **Step 5: Review scope and plan completeness**

```bash
git status --short
git diff --check
git diff --stat
git diff -- \
  crates/schema-serde crates/client-streams crates/observability-demo-app \
  crates/gres-fdw crates/gres crates/operator \
  deploy/crds/crabka.io_greses.yaml demo/observability/docker-compose.yml \
  docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-30-schema-serde-fetch-retry-policy.md
```

Confirm:

- both configured values remain UOM-backed end to end;
- invalid ranges cannot reach `SchemaCache`;
- all three demo roles and every Gres FDW scan receive the policy;
- Client Streams uses its existing `CacheConfig` boundary;
- fixed media type, magic byte, reference ceiling, exponent cap, and jitter are
  unchanged;
- no placeholder, test-only bypass, generic retry layer, or hidden environment
  read was added;
- unrelated dirty files and protected plans remain unstaged.

- [x] **Step 6: Commit the audit closure**

```bash
git commit -m "docs(config): close schema fetch retry audit"
```

Stage only the schema-serde audit section and this plan's completed
checkboxes. Do not run `cargo clean`; that remains the final action after every
repository owner is complete.
