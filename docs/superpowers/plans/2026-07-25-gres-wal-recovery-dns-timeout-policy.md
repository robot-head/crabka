# Gres WAL Recovery DNS Timeout Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound raw committed-WAL DNS resolution with a validated timeout exposed through standalone Gres CLI/environment configuration and the Gres fleet CRD.

**Architecture:** Extend the existing `RecoveryReadPolicy` with one DNS duration and apply it at the sole raw WAL lookup boundary. Reuse the existing Gres policy assembly, `LiveRecoveryConfig` propagation, operator effective-policy validation, and shared compute argument renderer; do not introduce a resolver service or generic client policy.

**Tech Stack:** Rust, Tokio, clap, `refined_type`, kube/schemars, assert2, generated Kubernetes CRDs.

## Global Constraints

- Every Cargo command must set `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Use `refined_type` at validation boundaries; do not add a hand-written validation newtype.
- Use the approved 10,000 ms default as `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS`.
- DNS timeout, TCP connect timeout, and request timeout remain independent positive durations.
- The setting is `--wal-recovery-dns-timeout-ms` / `CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS` / `spec.compute.walRecoveryDnsTimeoutMs`.
- The CRD field is optional with minimum one; the operator renders the effective value for every single-range and multi-range compute.
- Do not change bootstrap ordering, first-address selection, security, fetch behavior, or unrelated DNS lookups.
- Do not add dependencies, compatibility shims, Clippy suppressions, source-text tests, or speculative abstractions.
- Use assert2 rather than Rust's built-in assertion macros.
- Preserve every unrelated dirty or untracked file and stage only task-owned paths.

## Batch Layout

- **Batch 1:** Task 1 establishes the substrate interface and lookup behavior.
- **Batch 2, parallel:** Task 2 wires standalone Gres while Task 3 wires the operator and generated CRD; their file sets do not overlap.
- **Batch 3:** Task 4 audits, performs the final cross-slice review and gates, pushes, and verifies draft PR #904.

---

### Task 1: Validate and enforce the raw WAL DNS deadline

**Files:**
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify: `crates/gres-substrate/src/lib.rs`

**Interfaces:**
- Produces: `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS: u64 = 10_000`
- Produces: `RecoveryReadPolicy::with_dns_timeout(u64) -> Result<Self, String>`
- Produces: `RecoveryReadPolicy::dns_timeout() -> Duration`
- Produces: the private `resolve_wal_addr` seam used only by `open_wal_connection`
- Preserves: the existing four-argument `RecoveryReadPolicy::new` and two-argument `with_timeouts`

- [ ] **Step 1: Add failing policy tests**

Extend `recovery_read_policy_owns_defaults` and add a focused validation/replacement test:

```rust
assert!(
    policy.dns_timeout()
        == Duration::from_millis(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS)
);

#[test]
fn recovery_read_policy_validates_and_replaces_dns_timeout() {
    assert!(RecoveryReadPolicy::default().with_dns_timeout(0).is_err());

    let policy = RecoveryReadPolicy::new(11, 22, 33, 44)
        .expect("valid policy")
        .with_timeouts(55, 66)
        .expect("valid connection timeouts")
        .with_dns_timeout(77)
        .expect("valid DNS timeout");

    assert!(policy.fetch_max_wait_ms() == 11);
    assert!(policy.connect_timeout() == Duration::from_millis(55));
    assert!(policy.request_timeout() == Duration::from_millis(66));
    assert!(policy.dns_timeout() == Duration::from_millis(77));
}
```

- [ ] **Step 2: Run RED for the policy interface**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate \
  recovery_read_policy_validates_and_replaces_dns_timeout --lib
```

Expected: compilation fails because the constant and policy methods do not exist.

- [ ] **Step 3: Add failing deterministic lookup tests**

Add tests for the wished-for private seam. Use `std::future::ready` for resolver results and a pending future under paused Tokio time:

```rust
#[tokio::test]
async fn wal_dns_lookup_returns_first_address_and_reports_resolver_failures() {
    let first: std::net::SocketAddr = "127.0.0.1:9092".parse().expect("first address");
    let second: std::net::SocketAddr = "127.0.0.2:9092".parse().expect("second address");
    let resolved = resolve_wal_addr(
        "broker:9092",
        Duration::from_millis(10),
        std::future::ready(Ok(vec![first, second].into_iter())),
    )
    .await
    .expect("resolved address");
    assert!(resolved == first);

    let error = resolve_wal_addr(
        "broker:9092",
        Duration::from_millis(10),
        std::future::ready(Err::<
            std::vec::IntoIter<std::net::SocketAddr>,
            _,
        >(std::io::Error::other("resolver failed"))),
    )
    .await
    .expect_err("resolver error");
    assert!(error.to_string().contains("DNS lookup broker:9092: resolver failed"));
}

#[tokio::test]
async fn wal_dns_lookup_rejects_an_empty_result() {
    let error = resolve_wal_addr(
        "broker:9092",
        Duration::from_millis(10),
        std::future::ready(Ok(Vec::<std::net::SocketAddr>::new().into_iter())),
    )
    .await
    .expect_err("empty resolution");

    assert!(error.to_string().contains("no addresses for broker:9092"));
}

#[tokio::test(start_paused = true)]
async fn wal_dns_lookup_stops_at_the_configured_timeout() {
    let error = resolve_wal_addr(
        "broker:9092",
        Duration::from_millis(37),
        std::future::pending::<
            std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>,
        >(),
    )
    .await
    .expect_err("DNS timeout");

    assert!(
        error
            .to_string()
            .contains("DNS lookup broker:9092 timed out after 37 ms")
    );
}
```

- [ ] **Step 4: Run RED for lookup behavior**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate wal_dns_lookup --lib
```

Expected: compilation fails because `resolve_wal_addr` does not exist.

- [ ] **Step 5: Implement the minimal validated policy**

Add the constant, field, builder, and accessor beside the existing recovery timeouts:

```rust
pub const DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS: u64 = 10_000;

pub struct RecoveryReadPolicy {
    fetch_max_wait_ms: i32,
    fetch_partition_max_bytes: i32,
    fetch_response_max_bytes: i32,
    empty_fetch_retries: usize,
    dns_timeout: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
}

// In RecoveryReadPolicy::new:
dns_timeout: validated_timeout(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS)?,

/// Replace the raw WAL DNS lookup timeout.
///
/// # Errors
///
/// Returns an error when the timeout is zero.
pub fn with_dns_timeout(mut self, dns_timeout_ms: u64) -> Result<Self, String> {
    self.dns_timeout = validated_timeout(dns_timeout_ms)?;
    Ok(self)
}

#[must_use]
pub const fn dns_timeout(self) -> Duration {
    self.dns_timeout
}
```

`validated_timeout` already uses `GreaterU64::<0>`; reuse it.

- [ ] **Step 6: Implement the minimal lookup deadline**

Add the narrow future seam and call it from `open_wal_connection`:

```rust
async fn resolve_wal_addr<I>(
    host_port: &str,
    timeout: Duration,
    lookup: impl std::future::Future<Output = std::io::Result<I>>,
) -> Result<std::net::SocketAddr, SubstrateError>
where
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(timeout, lookup)
        .await
        .map_err(|_| {
            SubstrateError::Unavailable(format!(
                "DNS lookup {host_port} timed out after {} ms",
                timeout.as_millis()
            ))
        })?
        .map_err(|error| {
            SubstrateError::Unavailable(format!("DNS lookup {host_port}: {error}"))
        })?;
    addrs
        .next()
        .ok_or_else(|| SubstrateError::Unavailable(format!("no addresses for {host_port}")))
}
```

Replace only the inline lookup:

```rust
let addr = resolve_wal_addr(
    host_port,
    read_policy.dns_timeout(),
    tokio::net::lookup_host(host_port),
)
.await?;
```

Keep `Connection::connect_with_options` and its TCP timeout unchanged.

- [ ] **Step 7: Re-export the substrate-owned default**

Add `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS` to the existing `recovery` re-export list in `crates/gres-substrate/src/lib.rs`.

- [ ] **Step 8: Run GREEN and substrate gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate wal_dns_lookup --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate recovery_read_policy --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-substrate --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 9: Commit and review**

Stage only:

```bash
git add crates/gres-substrate/src/recovery.rs crates/gres-substrate/src/lib.rs
git commit -m "feat(gres): bound WAL DNS lookup"
```

Obtain independent spec-compliance and quality approval. Resume this implementer for every fix until both reviews pass.

---

### Task 2: Expose standalone Gres CLI and environment policy

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/gres/tests/runtime.rs` only if compilation requires adding the new `ServeArgs` field to an explicit fixture

**Interfaces:**
- Consumes: `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS`
- Consumes: `RecoveryReadPolicy::with_dns_timeout`
- Produces: `ServeArgs::wal_recovery_dns_timeout_ms: Option<PositiveMillis>`
- Preserves: the single `SubstrateRuntimeConfig::live_recovery_config` propagation path

- [ ] **Step 1: Add the failing parser and propagation expectations**

Extend the existing WAL recovery policy tests rather than creating a parallel suite:

```rust
// Environment variable list:
"CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS",

// Default/environment assertion:
assert!(policy.dns_timeout() == Duration::from_millis(expected_dns_timeout_ms));

// CLI precedence input:
"--wal-recovery-dns-timeout-ms=30",

// CLI assertion:
assert!(policy.dns_timeout() == Duration::from_millis(30));

// Boundary and inert-use tables:
"--wal-recovery-dns-timeout-ms=0",
"--wal-recovery-dns-timeout-ms=1",
```

Extend `wal_recovery_read_policy_reaches_shared_recovery_config_helper` with a distinctive `with_dns_timeout(37)` and retain the whole-policy equality assertion.

- [ ] **Step 2: Run RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres \
  wal_recovery_read_policy_uses_defaults_environment_and_cli_precedence --lib
```

Expected: failure because clap does not recognize the flag and the assembled policy retains the default.

- [ ] **Step 3: Add the CLI/environment field**

Place the field immediately before the existing TCP connect timeout:

```rust
/// Timeout for resolving raw WAL recovery broker hostnames.
#[arg(
    long = "wal-recovery-dns-timeout-ms",
    env = "CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS",
    requires = "substrate_bootstrap"
)]
pub wal_recovery_dns_timeout_ms: Option<PositiveMillis>,
```

Add `wal_recovery_dns_timeout_ms` to the test-only environment-disabled clap command, every explicit `ServeArgs` fixture, hostile-environment cleanup, inert-use validation, and `set_wal_policy_option`. Update table lengths and index ranges exactly once.

- [ ] **Step 4: Assemble the effective policy**

Extend the existing builder chain without adding a new runtime field:

```rust
.and_then(|policy| {
    policy.with_dns_timeout(
        args.wal_recovery_dns_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS,
            PositiveMillis::into_value,
        ),
    )
})
.and_then(|policy| {
    policy.with_timeouts(
        args.wal_recovery_connect_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS,
            PositiveMillis::into_value,
        ),
        args.wal_recovery_request_timeout_ms.map_or(
            crabka_gres_substrate::DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS,
            PositiveMillis::into_value,
        ),
    )
})
```

Include the new option in `validate_wal_recovery_read_policy`, so programmatic inert use fails before listener or network I/O.

- [ ] **Step 5: Run GREEN and standalone gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres \
  wal_recovery_read_policy_uses_defaults_environment_and_cli_precedence --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres wal_recovery --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help |
  rg -- '--wal-recovery-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass and help contains the exact flag.

- [ ] **Step 6: Commit and review**

Stage only files changed by this task:

```bash
git add crates/gres/src/lib.rs crates/gres/tests/runtime.rs
git diff --cached --name-only
git commit -m "feat(gres): expose WAL DNS timeout"
```

If `crates/gres/tests/runtime.rs` did not change, omit it from `git add`. Obtain independent spec-compliance and quality approval and remediate every finding.

---

### Task 3: Expose fleet CRD policy and render it once

**Files:**
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify generated: `deploy/crds/crabka.io_greses.yaml`

**Interfaces:**
- Consumes: `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS`
- Produces: optional `GresComputeSpec::wal_recovery_dns_timeout_ms: Option<u64>`
- Produces: validated `EffectiveGresComputePolicy::wal_recovery_dns_timeout_ms: PositiveMillis`
- Produces: one `--wal-recovery-dns-timeout-ms VALUE` pair in the shared compute argument vector

- [ ] **Step 1: Add failing CRD and effective-policy tests**

Extend `compute_wal_recovery_policy_round_trips_validates_and_uses_substrate_defaults`:

```rust
let policy = GresComputeSpec {
    wal_recovery_dns_timeout_ms: Some(77),
    // existing distinctive recovery values
    ..GresComputeSpec::default()
};

assert!(
    properties["walRecoveryDnsTimeoutMs"]["minimum"].as_f64() == Some(1.0)
);
assert!(
    defaults.wal_recovery_dns_timeout_ms.into_value()
        == DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS
);

let error = GresComputeSpec {
    wal_recovery_dns_timeout_ms: Some(0),
    ..GresComputeSpec::default()
}
.effective_policy()
.expect_err("zero DNS timeout must fail");
assert!(error.contains("spec.compute.walRecoveryDnsTimeoutMs"));
```

- [ ] **Step 2: Add failing exact rendering expectations**

Extend `compute_wal_recovery_args_are_exact_in_single_and_multi_range_modes` so the ordered expected slice is:

```rust
[
    "--wal-recovery-fetch-max-wait-ms",
    expected[0],
    "--wal-recovery-fetch-partition-max-bytes",
    expected[1],
    "--wal-recovery-fetch-response-max-bytes",
    expected[2],
    "--wal-recovery-empty-fetch-retries",
    expected[3],
    "--wal-recovery-dns-timeout-ms",
    expected[4],
    "--wal-recovery-connect-timeout-ms",
    expected[5],
    "--wal-recovery-request-timeout-ms",
    expected[6],
]
```

Use default values `["100", "1048576", "52428800", "100", "10000", "10000", "30000"]` and override values `["11", "22", "33", "44", "77", "55", "66"]`.

- [ ] **Step 3: Run RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_wal_recovery_policy_round_trips_validates_and_uses_substrate_defaults --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_wal_recovery_args_are_exact_in_single_and_multi_range_modes --lib
```

Expected: compilation fails because the CRD/effective fields do not exist and rendering lacks the argument pair.

- [ ] **Step 4: Implement the CRD and validated effective field**

Import `DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS`, then add:

```rust
/// Timeout for resolving committed-WAL recovery broker hostnames.
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub wal_recovery_dns_timeout_ms: Option<u64>,
```

Add to `EffectiveGresComputePolicy`:

```rust
pub(crate) wal_recovery_dns_timeout_ms: PositiveMillis,
```

Resolve it in `effective_policy`:

```rust
wal_recovery_dns_timeout_ms: PositiveMillis::new(
    self.wal_recovery_dns_timeout_ms
        .unwrap_or(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS),
)
.map_err(|error| format!("spec.compute.walRecoveryDnsTimeoutMs: {error}"))?,
```

- [ ] **Step 5: Render the shared argument pair**

Insert immediately before the TCP connect timeout in the central compute vector:

```rust
"--wal-recovery-dns-timeout-ms".to_owned(),
compute_policy
    .wal_recovery_dns_timeout_ms
    .into_value()
    .to_string(),
```

Do not add single-range or multi-range branches.

- [ ] **Step 6: Regenerate and verify all CRDs**

Run:

```bash
crd_a=$(mktemp -d)
crd_b=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_a"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_b"
test "$(find "$crd_a" -maxdepth 1 -type f | wc -l)" -eq 9
test "$(find "$crd_b" -maxdepth 1 -type f | wc -l)" -eq 9
diff -ru "$crd_a" "$crd_b"
cp "$crd_a"/*.yaml deploy/crds/
```

Expected: both fresh generations contain nine identical files; only `deploy/crds/crabka.io_greses.yaml` changes from Task 1 HEAD.

- [ ] **Step 7: Run GREEN and operator gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_wal_recovery_policy_round_trips_validates_and_uses_substrate_defaults --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  compute_wal_recovery_args_are_exact_in_single_and_multi_range_modes --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 8: Commit and review**

Stage only:

```bash
git add crates/operator/src/crd/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_greses.yaml
git commit -m "feat(operator): expose WAL DNS timeout"
```

Obtain independent spec-compliance and quality approval and remediate every finding.

---

### Task 4: Audit, verify, publish, and continue

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the reviewed substrate, standalone, and operator implementation
- Produces: classified audit evidence, an updated draft PR #904, and the next coherent owner

- [ ] **Step 1: Re-run the broad scanner and focused DNS search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "lookup_host|DNS lookup|dns[_-]timeout|DnsTimeout|WAL_RECOVERY_DNS_TIMEOUT|wal-recovery-dns-timeout|walRecoveryDnsTimeout" \
  crates deploy/crds docs/configuration-audit.md
```

Classify every production raw-WAL match. Confirm that the only raw WAL `lookup_host` call is bounded by `RecoveryReadPolicy::dns_timeout`, and separately classify remaining client/admin/pool lookups without claiming they are covered.

- [ ] **Step 2: Record the audit**

Append `Gres WAL Recovery DNS Timeout Policy` to `docs/configuration-audit.md`, documenting:

- the named 10,000 ms default and positive `refined_type` validation;
- standalone CLI/environment and fleet CRD names;
- the exact `RecoveryReadPolicy` → `LiveRecoveryConfig` → `open_wal_connection` flow;
- deterministic paused-time deadline evidence;
- focused scanner counts split between production/schema and test/harness references;
- every verification command and result;
- the next coherent unresolved lookup owner.

Do not claim that all repository hardcoded values or DNS owners are complete.

- [ ] **Step 3: Commit the audit**

Run:

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record WAL DNS audit"
```

- [ ] **Step 4: Obtain a whole-slice independent review**

Review the complete diff from Task 1 parent through Task 4 HEAD against the approved design and this plan. Require:

- no unbounded raw WAL hostname lookup;
- no conflation of DNS and TCP connect timeouts;
- exact standalone and CRD naming;
- positive validation at every trust boundary;
- exact single-range and multi-range rendering;
- no unrelated DNS or resolver abstraction;
- no out-of-scope file changes.

Resume the owning implementer for one coherent fix wave, rerun scoped tests, and repeat review until every finding is explicitly addressed.

- [ ] **Step 5: Run fresh final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-gres-substrate -p crabka-gres -p crabka-operator \
  --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy \
  -p crabka-gres-substrate -p crabka-gres -p crabka-operator \
  --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help |
  rg -- '--wal-recovery-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Generate two fresh CRD directories again and require both nine-file directories to match each other and `deploy/crds`.

- [ ] **Step 6: Verify scope before publication**

Run:

```bash
git status --short
git log --oneline be8ebcbd..HEAD
git diff --stat be8ebcbd..HEAD
```

Confirm that only this slice's committed files appear in the range and that all pre-existing unrelated dirty/untracked files remain unstaged and unchanged.

- [ ] **Step 7: Push and verify draft PR #904**

Run:

```bash
git push origin configuration_expose
git rev-parse HEAD
git ls-remote origin refs/heads/configuration_expose
```

Use the connected GitHub app to verify that PR #904 remains open and draft, and that its `head_sha` exactly equals local HEAD and the remote branch SHA.

- [ ] **Step 8: Continue the repository-wide goal**

Name the next coherent unresolved owner from the focused audit and begin its design cycle. Do not mark the persistent repository-wide goal complete unless a requirement-by-requirement completion audit proves there are no remaining hardcoded operational values.
