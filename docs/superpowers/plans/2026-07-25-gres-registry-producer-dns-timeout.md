# Gres Registry Producer DNS Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the Gres registry producer DNS deadline through every
standalone registry CLI/environment surface and the Kafka-owned registry CRD
policy.

**Architecture:** Store the existing validated `ClientDnsTimeout` on
`RegistryPolicy`, forward it at the sole registry `Producer::builder()` call,
and extend the existing duplicated registry-option surfaces without adding a
new type or abstraction. Kubernetes operator control uses the effective policy
directly, while compute and activator deployments receive the same value as a
CLI argument.

**Tech Stack:** Rust 2024, Clap, `refined_type`, kube/schemars, serde, Tokio,
generated Kubernetes CRDs.

## Global Constraints

- Every Cargo command must set
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must use `--locked`; add no dependency.
- RED precedes production edits.
- Reuse `crabka_client_core::ClientDnsTimeout`; add no registry-specific
  timeout type, producer policy, or resolver abstraction.
- Default to `ClientDnsTimeout::default()` (10 seconds).
- Use exactly `--registry-producer-dns-timeout-ms`,
  `CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS`, and
  `spec.gresRegistry.producerDnsTimeoutMs`.
- CLI precedence is command line, environment, then the 10-second default.
- The CRD setting governs operator-internal registry control, Gres compute,
  and the activator.
- DNS remains independent from registry topic creation, reader, admin,
  TCP-connect, request, retry, and transaction behavior.
- Do not alter raw registry-reader resolution, registry admin connections, WAL
  producers, or unrelated producer deployments.
- Add no compatibility shim, source-text assertion, lint suppression, or
  speculative accessor.
- Preserve unrelated dirty and untracked files; stage only task-owned paths.

## Execution Preflight

- Confirm this checkout remains the linked `configuration_expose` worktree.
- Record `git status --short` and the exact Task 1 parent SHA.
- Run the affected-package baseline:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres-control -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked
```

- Use a fresh implementer per task and independent spec-compliance and quality
  reviewers. Keep each implementer available until both reviews pass.

---

### Task 1: Carry DNS policy to the registry producer

**Files:**
- Modify: `crates/gres-control/src/registry.rs`

**Interfaces:**
- Consumes:
  `crabka_client_core::ClientDnsTimeout`
- Produces:
  `RegistryPolicy::producer_dns_timeout(&self) -> ClientDnsTimeout`
- Produces:
  `RegistryPolicy::with_producer_dns_timeout_ms(self, u64) -> Result<Self, String>`
- Preserves:
  `RegistryPolicy::new(i32, i32, u64, i32, i32) -> Result<Self, String>`

- [ ] **Step 1: Add the failing policy test**

Add beside the existing registry policy tests:

```rust
#[test]
fn registry_policy_dns_timeout_defaults_and_replaces_exactly() {
    let defaults = RegistryPolicy::default();
    assert!(
        defaults.producer_dns_timeout()
            == crabka_client_core::ClientDnsTimeout::default()
    );

    let policy = defaults
        .with_producer_dns_timeout_ms(37)
        .expect("valid DNS timeout");
    assert!(policy.producer_dns_timeout().milliseconds() == 37);
    assert!(
        RegistryPolicy::default()
            .with_producer_dns_timeout_ms(0)
            .is_err()
    );
}
```

- [ ] **Step 2: Run RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control \
  registry_policy_dns_timeout_defaults_and_replaces_exactly --lib --locked
```

Expected: compilation fails because the accessor and consuming override do not
exist.

- [ ] **Step 3: Add the typed field and forwarding**

Extend the client-core import with `ClientDnsTimeout`.

Add to `RegistryPolicy`:

```rust
producer_dns_timeout: ClientDnsTimeout,
```

Initialize it in `RegistryPolicy::new`:

```rust
producer_dns_timeout: ClientDnsTimeout::default(),
```

Add the two exact methods:

```rust
/// DNS lookup deadline used by the registry producer.
#[must_use]
pub const fn producer_dns_timeout(&self) -> ClientDnsTimeout {
    self.producer_dns_timeout
}

/// Validate and replace the registry producer DNS lookup deadline.
///
/// # Errors
///
/// Returns an error when `milliseconds` is zero.
#[must_use]
pub fn with_producer_dns_timeout_ms(
    mut self,
    milliseconds: u64,
) -> Result<Self, String> {
    self.producer_dns_timeout =
        ClientDnsTimeout::new(Duration::from_millis(milliseconds))?;
    Ok(self)
}
```

Forward the typed value at the sole registry producer construction:

```rust
.dns_timeout(policy.producer_dns_timeout().duration())
```

Place it before `.enable_idempotence(true)`. Do not store a second timeout on
`Registry`.

- [ ] **Step 4: Run GREEN and crate gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control registry_policy_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-control --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 5: Commit and review**

```bash
git add crates/gres-control/src/registry.rs
git commit -m "feat(gres): configure registry producer DNS"
```

Require independent approval that the typed policy reaches the only registry
producer and that no reader/admin behavior changed.

---

### Task 2: Expose every standalone registry surface

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/cli/src/gres.rs`
- Modify: `crates/gres-activator/src/main.rs`
- Modify: `crates/gres-loadtest/src/main.rs`
- Modify: `crates/gres-loadtest/src/cluster.rs`

**Interfaces:**
- Consumes:
  `RegistryPolicy::with_producer_dns_timeout_ms(u64) -> Result<RegistryPolicy, String>`
- Produces on all four parsers:
  `--registry-producer-dns-timeout-ms`
- Produces on all four parsers:
  `CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS`
- Produces from the load-test child renderer:
  one `--registry-producer-dns-timeout-ms VALUE` pair

- [ ] **Step 1: Extend the existing failing parser tests**

In `crates/gres/src/lib.rs`, `crates/cli/src/gres.rs`, and
`crates/gres-loadtest/src/main.rs`, extend
`registry_policy_options_use_*`. In the activator, extend
`validated_input_rejects_invalid_cli_values`. Add:

```rust
"--registry-producer-dns-timeout-ms=0",
```

In each environment array, add:

```rust
("CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS", "37"),
```

For the activator's `temp_env` array, use:

```rust
(
    "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS",
    Some("37"),
),
```

Add this CLI override to each precedence case:

```rust
"--registry-producer-dns-timeout-ms=47",
```

Compare environment and CLI policies against:

```rust
let environment_policy =
    RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
        .expect("policy")
        .with_producer_dns_timeout_ms(37)
        .expect("environment DNS timeout");
let cli_policy =
    RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578)
        .expect("policy")
        .with_producer_dns_timeout_ms(47)
        .expect("CLI DNS timeout");
```

Keep each module's existing environment-isolation mechanism. Import
`ClientDnsTimeout` and `Duration` where they are not already in scope.

- [ ] **Step 2: Add the failing load-test child assertion**

Configure the existing load-test policy with a distinct value:

```rust
let policy = RegistryPolicy::new(3, 15_002, 252, 502, 1_048_578)
    .expect("policy")
    .with_producer_dns_timeout_ms(37)
    .expect("DNS timeout");
```

Then assert:

```rust
assert!(
    arg_value(&spawned_args, "--registry-producer-dns-timeout-ms")
        == Some("37")
);
```

- [ ] **Step 3: Run RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres -p crabka-cli -p crabka-gres-loadtest \
  registry_policy_options --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator validated_input_ --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-loadtest \
  node_specs_wire_topology_flags --lib --locked
```

Expected: parser tests fail because the flag does not exist, and the load-test
renderer omits it.

- [ ] **Step 4: Add the option to each existing `RegistryOptions`**

Add immediately after `fetch_partition_max_bytes` in all four parser structs:

```rust
#[arg(
    long = "registry-producer-dns-timeout-ms",
    env = "CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS"
)]
producer_dns_timeout_ms: Option<PositiveMillis>,
```

In each `RegistryOptions::policy`, resolve the absent value from the shared
typed default and attach it through the validated policy boundary:

```rust
let producer_dns_timeout_ms = self.producer_dns_timeout_ms.map_or_else(
    || {
        RegistryPolicy::default()
            .producer_dns_timeout()
            .milliseconds()
    },
    PositiveMillis::into_value,
);

RegistryPolicy::new(
    self.replication_factor.into_value(),
    self.topic_create_timeout_ms.into_value(),
    self.reader_retry_backoff_ms.into_value(),
    self.fetch_max_wait_ms.into_value(),
    self.fetch_partition_max_bytes.into_value(),
)
.expect("validated registry options")
.with_producer_dns_timeout_ms(producer_dns_timeout_ms)
.expect("validated registry producer DNS timeout")
```

Preserve the module's existing expectation wording where it differs. Do not
extract a cross-crate helper for four already-local Clap structs.

- [ ] **Step 5: Extend the load-test child renderer**

Change its fixed return size from 10 to 12 and append:

```rust
"--registry-producer-dns-timeout-ms".to_owned(),
policy.producer_dns_timeout().milliseconds().to_string(),
```

- [ ] **Step 6: Run GREEN and standalone gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres -p crabka-cli -p crabka-gres-loadtest \
  registry_policy_options --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-activator validated_input_ --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-loadtest \
  node_specs_wire_topology_flags --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres -p crabka-cli -p crabka-gres-activator \
  -p crabka-gres-loadtest --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy \
  -p crabka-gres -p crabka-cli -p crabka-gres-activator \
  -p crabka-gres-loadtest --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres --locked -- --help |
  rg -- '--registry-producer-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 7: Commit and review**

```bash
git add crates/gres/src/lib.rs crates/cli/src/gres.rs \
  crates/gres-activator/src/main.rs crates/gres-loadtest/src/main.rs \
  crates/gres-loadtest/src/cluster.rs
git commit -m "feat(gres): expose registry producer DNS"
```

Require independent approval of all four CLI/environment surfaces and the
load-test child propagation.

---

### Task 3: Expose the Kafka CRD and operator paths

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/context.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify generated: `deploy/crds/crabka.io_kafkas.yaml`

**Interfaces:**
- Produces:
  `GresRegistrySpec::producer_dns_timeout_ms: Option<u64>`
- Produces:
  exact malformed path `spec.gresRegistry.producerDnsTimeoutMs`
- Produces:
  one registry producer DNS argument in compute and activator containers
- Preserves:
  operator control cache equality through `RegistryPolicy`

- [ ] **Step 1: Add failing CRD tests**

In
`gres_registry_round_trips_and_defaults`, extend the existing registry
round-trip fixture:

```json
"producerDnsTimeoutMs":37
```

Compare it with:

```rust
let expected =
    crabka_gres_control::RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
        .expect("expected policy")
        .with_producer_dns_timeout_ms(37)
        .expect("DNS timeout");
```

In `gres_registry_schema_has_runtime_bounds`, add
`"producerDnsTimeoutMs"` to the schema-minimum loop. In
`gres_registry_rejects_zero_and_replication_overflow`, add this invalid case:

```rust
GresRegistrySpec {
    producer_dns_timeout_ms: Some(0),
    ..Default::default()
},
```

Require the effective conversion error:

```rust
let error = GresRegistrySpec {
    producer_dns_timeout_ms: Some(0),
    ..Default::default()
}
.policy()
.expect_err("zero DNS timeout");
assert!(error.starts_with(
    "spec.gresRegistry.producerDnsTimeoutMs:"
));
```

- [ ] **Step 2: Add failing operator propagation tests**

In `gres_control_cache_tracks_inputs_without_locking_during_build`, create a
policy that differs only by DNS:

```rust
let changed_dns = defaults
    .clone()
    .with_producer_dns_timeout_ms(37)
    .expect("DNS timeout");
```

Require `gres_control_for_with` to rebuild rather than reuse the cached
control for `changed_dns`.

In `activator_workload_renders_custom_policy`, configure the policy with 37
milliseconds and add this exact pair to the expected argument array:

```rust
"--registry-producer-dns-timeout-ms",
"37",
```

In `compute_workload_renders_custom_policy`, add:

```rust
["--registry-producer-dns-timeout-ms", "37"],
```

Configure its policy with the same typed override. Count the flag once in each
rendered container:

```rust
assert!(
    args.iter()
        .filter(|arg| arg.as_str() == "--registry-producer-dns-timeout-ms")
        .count()
        == 1
);
```

- [ ] **Step 3: Run RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator gres_registry --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  gres_control_cache_tracks_inputs_without_locking_during_build --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator activator_workload_ --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_workload_renders_custom_policy --lib --locked
```

Expected: compilation or assertions fail because the CRD field and rendered
arguments do not exist.

- [ ] **Step 4: Add the CRD field and effective conversion**

Add to `GresRegistrySpec`:

```rust
/// DNS lookup deadline for the registry producer.
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub producer_dns_timeout_ms: Option<u64>,
```

Resolve the default from the typed policy, build the base policy, then apply
the validated DNS override:

```rust
let producer_dns_timeout_ms = self.producer_dns_timeout_ms.unwrap_or_else(|| {
    crabka_gres_control::RegistryPolicy::default()
        .producer_dns_timeout()
        .milliseconds()
});
let policy = crabka_gres_control::RegistryPolicy::new(
    self.replication_factor.unwrap_or(1),
    self.topic_create_timeout_ms.unwrap_or(15_000),
    self.reader_retry_backoff_ms.unwrap_or(250),
    self.fetch_max_wait_ms.unwrap_or(500),
    self.fetch_partition_max_bytes.unwrap_or(1_048_576),
)
.map_err(|error| format!("spec.gresRegistry: {error}"))?;
policy
    .with_producer_dns_timeout_ms(producer_dns_timeout_ms)
    .map_err(|error| {
        format!("spec.gresRegistry.producerDnsTimeoutMs: {error}")
    })
```

At the reconciliation call site, remove its now-duplicate
`spec.gresRegistry:` wrapper and map the complete policy error directly to
`ReconcileError::Malformed`.

- [ ] **Step 5: Render the shared argument**

In both existing registry argument vectors, add:

```rust
"--registry-producer-dns-timeout-ms",
policy.producer_dns_timeout().milliseconds().to_string(),
```

For the JSON activator vector, use `registry_policy` as the variable name.
Add the pair once, adjacent to the other registry fields. Do not create
range-mode branches or a new renderer abstraction.

- [ ] **Step 6: Regenerate and compare all CRDs**

```bash
crd_a=$(mktemp -d)
crd_b=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator --locked -- gen-crds "$crd_a"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator --locked -- gen-crds "$crd_b"
test "$(find "$crd_a" -maxdepth 1 -type f | wc -l)" -eq 9
test "$(find "$crd_b" -maxdepth 1 -type f | wc -l)" -eq 9
diff -ru "$crd_a" "$crd_b"
cp "$crd_a"/*.yaml deploy/crds/
```

Expected: both fresh generations contain nine identical files; only
`deploy/crds/crabka.io_kafkas.yaml` differs from Task 2 HEAD. Remove the exact
temporary directories after comparison.

- [ ] **Step 7: Run GREEN and operator gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator gres_registry --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  gres_control_cache_tracks_inputs_without_locking_during_build --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator activator_workload_ --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_workload_renders_custom_policy --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 8: Commit and review**

```bash
git add crates/operator/src/crd/kafka.rs crates/operator/src/context.rs \
  crates/operator/src/controller/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_kafkas.yaml
git commit -m "feat(operator): expose registry producer DNS"
```

Require independent approval of CRD ownership, exact error naming, internal
control behavior, compute rendering, activator rendering, and generated
schema.

---

### Task 4: Audit, verify, publish, and continue

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the reviewed shared policy, standalone surfaces, and operator paths
- Produces: classified audit evidence, updated draft PR #904, and the next
  unresolved owner

- [ ] **Step 1: Run broad and focused audits**

```bash
tools/audit-runtime-values.sh
rg -n \
  "lookup_host|ClientDnsTimeout|DEFAULT_CLIENT_DNS_TIMEOUT|dns[_-]timeout|DnsTimeout|registry-producer-dns-timeout|producerDnsTimeoutMs" \
  crates deploy/crds docs/configuration-audit.md
```

Classify production, schema, test/harness, and prior-audit references. Confirm
one registry producer construction consumes the shared typed policy and every
current registry deployment surface owns an input.

- [ ] **Step 2: Record exact audit evidence**

Append `Gres Registry Producer DNS Timeout` to
`docs/configuration-audit.md`. Record:

- the exact CLI, environment, and CRD names;
- the 10-second validated default;
- the CRD → `RegistryPolicy` → operator control / rendered CLI →
  `Registry::connect_with_policy` → producer → client-core flow;
- scanner counts by classification;
- verification results;
- raw registry reader and registry admin DNS paths as still open; and
- the next coherent unresolved configuration owner.

Do not claim the repository-wide goal is complete.

- [ ] **Step 3: Commit the audit**

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record registry DNS audit"
```

- [ ] **Step 4: Obtain whole-slice independent review**

Review the Task 1 parent through Task 4 HEAD. Require:

- the existing `ClientDnsTimeout` is reused;
- the effective value reaches the sole registry producer;
- all four standalone parsers use exact naming and precedence;
- the load-test child receives the value;
- Kafka CRD ownership and minimum are exact;
- operator control, compute, and activator paths share one effective policy;
- raw reader/admin and unrelated producers remain unchanged; and
- only scoped committed files appear.

Resume the owning implementer for one coherent fix wave if needed, rerun
affected gates, and repeat review until every finding is addressed.

- [ ] **Step 5: Run fresh final verification**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres-control -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy \
  -p crabka-gres-control -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres --locked -- --help |
  rg -- '--registry-producer-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Generate two fresh nine-file CRD directories and require both to match each
other and `deploy/crds`.

- [ ] **Step 6: Verify scope and publish**

```bash
git status --short
git log --oneline 33808631..HEAD
git diff --stat 33808631..HEAD
git push origin configuration_expose
git rev-parse HEAD
git ls-remote origin refs/heads/configuration_expose
```

Confirm unrelated dirty/untracked files remain unstaged and unchanged. Use the
connected GitHub app to verify PR #904 remains open and draft and that its
`head_sha` equals local HEAD and the remote branch SHA.

- [ ] **Step 7: Continue the repository-wide goal**

Name the next coherent unresolved owner from the audit and begin its design
cycle. Do not mark the persistent goal complete unless a requirement-by-
requirement audit proves no hardcoded operational values remain.
