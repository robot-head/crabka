# Gres WAL Producer DNS Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the generic client DNS deadline through the Gres WAL
producer's standalone CLI/environment and fleet CRD configuration paths.

**Architecture:** Add one validated DNS duration to the producer builder,
reuse `crabka_client_core::ClientDnsTimeout` throughout Gres runtime
propagation, and render one optional Gres CRD field into the existing compute
argument vector. Keep DNS separate from producer retry, TCP-connect, and
request policy.

**Tech Stack:** Rust 2024, Bon, Clap, `refined_type`, kube/schemars, serde,
Tokio, generated Kubernetes CRDs.

## Global Constraints

- Every Cargo command must set
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must use `--locked`; this slice adds no
  dependency.
- RED precedes production edits.
- Reuse `crabka_client_core::ClientDnsTimeout`; add no producer-specific
  validation type or resolver abstraction.
- Use the existing 10-second `DEFAULT_CLIENT_DNS_TIMEOUT`.
- The setting is `--wal-producer-dns-timeout-ms` /
  `CRABKA_GRES_WAL_PRODUCER_DNS_TIMEOUT_MS` /
  `spec.compute.walProducerDnsTimeoutMs`.
- CLI precedence is command line, then environment, then the 10-second
  default.
- Supplying the standalone setting without `--substrate-bootstrap` is invalid.
- DNS, TCP-connect, protocol-request, retry, and transaction timing remain
  independent.
- Do not alter consumer, streams, admin, non-Gres producer deployments, or
  generic resolver behavior.
- Add no dependencies, compatibility shims, source-text assertions, lint
  suppressions, or speculative public accessors.
- Preserve unrelated dirty and untracked files; stage only task-owned paths.

## Execution Preflight

- Confirm this checkout is already an isolated linked worktree and keep the
  current `configuration_expose` branch.
- Record `git status --short` and the exact Task 1 parent SHA.
- Run the affected four-package all-target test baseline before editing:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-client-producer -p crabka-gres-substrate \
  -p crabka-gres -p crabka-operator --all-targets --locked
```

- Use a fresh implementer per task and independent spec-compliance and quality
  reviewers. Keep each implementer available until its reviews pass.

---

### Task 1: Add the producer DNS input

**Files:**
- Modify: `crates/client-producer/src/builder.rs`

**Interfaces:**
- Consumes:
  `crabka_client_core::{ClientDnsTimeout, DEFAULT_CLIENT_DNS_TIMEOUT}`
- Produces: Bon builder setter
  `Producer::builder().dns_timeout(Duration)`
- Preserves: all existing producer defaults and public policy types

- [ ] **Step 1: Add failing producer-boundary tests**

Add focused tests beside
`producer_builder_rejects_retry_policy_before_connection_io`:

```rust
#[tokio::test]
async fn producer_builder_rejects_invalid_dns_timeout_before_connection_io() {
    for timeout in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::MAX,
    ] {
        let error = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .dns_timeout(timeout)
            .build()
            .await
            .expect_err("invalid DNS timeout must fail before connection I/O");
        assert2::assert!(matches!(
            error,
            ProducerError::InvalidConfig(message)
                if message.starts_with("client DNS timeout")
        ));
    }
}

#[tokio::test]
async fn producer_builder_accepts_a_distinct_dns_timeout() {
    let producer = Producer::builder()
        .bootstrap("127.0.0.1:1")
        .dns_timeout(Duration::from_millis(37))
        .enable_idempotence(false)
        .build()
        .await
        .expect("valid DNS timeout");
    producer.close().await.expect("close producer");
}
```

Use the producer's actual close signature if it differs; do not add an API
solely for this test.

- [ ] **Step 2: Run RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer \
  producer_builder_rejects_invalid_dns_timeout_before_connection_io --lib --locked
```

Expected: compilation fails because the Bon builder has no `dns_timeout`
setter.

- [ ] **Step 3: Implement the minimal producer forwarding**

Extend the existing client-core import:

```rust
use crabka_client_core::{
    Client, ClientDnsTimeout, ClientError, DEFAULT_CLIENT_DNS_TIMEOUT,
};
```

Add the builder input immediately before `request_timeout`:

```rust
#[builder(default = DEFAULT_CLIENT_DNS_TIMEOUT)] dns_timeout: Duration,
```

Validate it before any client construction:

```rust
let dns_timeout =
    ClientDnsTimeout::new(dns_timeout).map_err(ProducerError::InvalidConfig)?;
```

Forward it through the existing client builder:

```rust
.dns_timeout(dns_timeout.duration())
```

Do not store a duplicate field on `Producer`.

- [ ] **Step 4: Run GREEN and producer gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer producer_builder_ --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-producer --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 5: Commit and review**

Run:

```bash
git add crates/client-producer/src/builder.rs
git commit -m "feat(client): configure producer DNS timeout"
```

Obtain independent spec-compliance and quality approval. Resume the same
implementer for fixes until both reviews pass.

---

### Task 2: Carry standalone Gres policy to the WAL producer

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify: `crates/gres/tests/runtime.rs` only if an exhaustive
  `SubstrateRuntimeConfig` literal requires the new field

**Interfaces:**
- Consumes:
  `ClientDnsTimeout::new(Duration) -> Result<ClientDnsTimeout, String>`
- Produces:
  `ServeArgs::wal_producer_dns_timeout_ms: Option<PositiveMillis>`
- Produces:
  `LiveRecoveryConfig::with_producer_dns_timeout(ClientDnsTimeout) -> Self`
- Produces:
  `LiveRecoveryConfig::producer_dns_timeout(&self) -> ClientDnsTimeout`

- [ ] **Step 1: Add failing substrate propagation tests**

Add one test next to the existing producer-policy tests:

```rust
#[test]
fn producer_dns_timeout_defaults_replaces_and_reaches_builder() {
    let tenant = TenantName::parse("tenant-a").expect("tenant");
    let config =
        LiveRecoveryConfig::new("localhost:9092", tenant, RangeId::new(7), None);
    assert_eq!(
        config.producer_dns_timeout(),
        crabka_client_core::ClientDnsTimeout::default()
    );

    let replacement =
        crabka_client_core::ClientDnsTimeout::new(Duration::from_millis(37))
            .expect("valid DNS timeout");
    assert_eq!(
        config
            .with_producer_dns_timeout(replacement)
            .producer_dns_timeout(),
        replacement
    );
}
```

- [ ] **Step 2: Add failing standalone parser tests**

Create a focused child-process precedence test following
`wal_producer_flush_timeout_uses_defaults_environment_and_cli_precedence`.
It must assert:

```rust
// default
ClientDnsTimeout::default().milliseconds()

// environment
CRABKA_GRES_WAL_PRODUCER_DNS_TIMEOUT_MS=27

// CLI wins over environment
--wal-producer-dns-timeout-ms=37
```

Add zero and local-only cases:

```rust
Cli::try_parse_from([
    "crabka-gres",
    "--substrate-bootstrap=k:9092",
    "--tenant=t",
    "--wal-producer-dns-timeout-ms=0",
])
.expect_err("zero DNS timeout");

Cli::try_parse_from([
    "crabka-gres",
    "--wal-producer-dns-timeout-ms=1",
])
.expect_err("substrate bootstrap required");
```

Also set the field programmatically without substrate mode and require
`SubstrateRuntimeConfig::from_args` to reject it before listener or network
I/O.

- [ ] **Step 3: Run RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate producer_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres wal_producer_dns_timeout --lib --locked
```

Expected: compilation fails because the runtime field, config methods, and
Clap option do not exist.

- [ ] **Step 4: Add the runtime field and producer call**

Add to `LiveRecoveryConfig`:

```rust
producer_dns_timeout: crabka_client_core::ClientDnsTimeout,
```

Initialize it with `Default::default()`, add the exact builder/accessor from
the Interfaces block, and pass it at the sole WAL producer builder:

```rust
.dns_timeout(config.producer_dns_timeout().duration())
```

Do not add the value to `ProducerRetryPolicy`.

- [ ] **Step 5: Add the CLI/environment boundary**

Add immediately before `wal_producer_request_timeout_ms`:

```rust
/// Timeout for resolving WAL producer broker hostnames.
#[arg(
    long = "wal-producer-dns-timeout-ms",
    env = "CRABKA_GRES_WAL_PRODUCER_DNS_TIMEOUT_MS",
    requires = "substrate_bootstrap"
)]
pub wal_producer_dns_timeout_ms: Option<PositiveMillis>,
```

Add the field to the test-only environment-disabled Clap helper, every
exhaustive `ServeArgs` fixture, hostile-environment cleanup, the shared
WAL-option inert-use validator, and its programmatic option table.

Add the effective conversion:

```rust
fn effective_wal_producer_dns_timeout(
    args: &ServeArgs,
) -> std::io::Result<crabka_client_core::ClientDnsTimeout> {
    args.wal_producer_dns_timeout_ms.map_or_else(
        || Ok(crabka_client_core::ClientDnsTimeout::default()),
        |timeout| {
            crabka_client_core::ClientDnsTimeout::new(Duration::from_millis(
                timeout.into_value(),
            ))
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
            })
        },
    )
}
```

Store the result on `SubstrateRuntimeConfig`:

```rust
pub producer_dns_timeout: crabka_client_core::ClientDnsTimeout,
```

Construct it in `from_args`, then propagate it through the existing
`live_recovery_config` chain:

```rust
.with_producer_dns_timeout(self.producer_dns_timeout)
```

- [ ] **Step 6: Run GREEN and standalone gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate producer_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres wal_producer_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate -p crabka-gres --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-substrate -p crabka-gres \
  --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres --locked -- --help |
  rg -- '--wal-producer-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass and help contains the exact flag.

- [ ] **Step 7: Commit and review**

Stage only changed task files:

```bash
git add crates/gres/src/lib.rs crates/gres-substrate/src/recovery.rs
git add crates/gres/tests/runtime.rs
git diff --cached --name-only
git commit -m "feat(gres): expose producer DNS timeout"
```

Omit `crates/gres/tests/runtime.rs` if unchanged. Obtain independent
spec-compliance and quality approval and remediate every finding.

---

### Task 3: Expose the fleet CRD field

**Files:**
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify generated: `deploy/crds/crabka.io_greses.yaml`

**Interfaces:**
- Produces:
  `GresComputeSpec::wal_producer_dns_timeout_ms: Option<u64>`
- Produces:
  `EffectiveGresComputePolicy::wal_producer_dns_timeout: ClientDnsTimeout`
- Produces: one `--wal-producer-dns-timeout-ms VALUE` pair in every Gres
  compute deployment

- [ ] **Step 1: Add failing CRD policy tests**

Add a focused test next to
`wal_producer_flush_timeout_has_exact_schema_default_override_and_errors`:

```rust
#[test]
fn wal_producer_dns_timeout_has_exact_schema_default_override_and_error() {
    let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
    let field = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        ["properties"]["spec"]["properties"]["compute"]["properties"]
        ["walProducerDnsTimeoutMs"];
    assert!(field["type"] == "integer");
    assert!(field["format"] == "uint64");
    assert!(field["minimum"].as_f64() == Some(1.0));

    let default = GresComputeSpec::default()
        .effective_policy()
        .expect("default compute policy")
        .wal_producer_dns_timeout;
    assert!(default == crabka_client_core::ClientDnsTimeout::default());

    let configured = GresComputeSpec {
        wal_producer_dns_timeout_ms: Some(37),
        ..GresComputeSpec::default()
    }
    .effective_policy()
    .expect("configured compute policy")
    .wal_producer_dns_timeout;
    assert!(configured.milliseconds() == 37);

    let error = GresComputeSpec {
        wal_producer_dns_timeout_ms: Some(0),
        ..GresComputeSpec::default()
    }
    .effective_policy()
    .expect_err("zero DNS timeout");
    assert!(error.starts_with("spec.compute.walProducerDnsTimeoutMs:"));
}
```

- [ ] **Step 2: Add failing exact rendering tests**

Add one exact-once test following the flush-timeout test. Exercise both a
single-range and two-range deployment and require exactly one pair per
container:

```rust
let pair = ["--wal-producer-dns-timeout-ms", "37"];
assert!(
    args.windows(2).filter(|window| *window == pair).count() == 1,
    "got: {args:?}"
);
```

Also assert the default pair is `["--wal-producer-dns-timeout-ms", "10000"]`.

- [ ] **Step 3: Run RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator wal_producer_dns_timeout --lib --locked
```

Expected: compilation fails because the CRD and effective-policy fields do
not exist.

- [ ] **Step 4: Add the CRD and validated effective field**

Add immediately before the producer request timeout:

```rust
/// Timeout for resolving WAL producer broker hostnames.
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub wal_producer_dns_timeout_ms: Option<u64>,
```

Add to `EffectiveGresComputePolicy`:

```rust
pub(crate) wal_producer_dns_timeout: crabka_client_core::ClientDnsTimeout,
```

Resolve it without a second validation type:

```rust
wal_producer_dns_timeout: crabka_client_core::ClientDnsTimeout::new(
    Duration::from_millis(
        self.wal_producer_dns_timeout_ms.unwrap_or_else(|| {
            crabka_client_core::ClientDnsTimeout::default().milliseconds()
        }),
    ),
)
.map_err(|error| format!("spec.compute.walProducerDnsTimeoutMs: {error}"))?,
```

- [ ] **Step 5: Render one shared argument pair**

Add:

```rust
fn wal_producer_dns_args(
    timeout: crabka_client_core::ClientDnsTimeout,
) -> [String; 2] {
    [
        "--wal-producer-dns-timeout-ms".to_owned(),
        timeout.milliseconds().to_string(),
    ]
}
```

Extend the central argument vector once:

```rust
args.extend(wal_producer_dns_args(
    compute_policy.wal_producer_dns_timeout,
));
```

Do not add range-mode branches.

- [ ] **Step 6: Regenerate and compare all CRDs**

Run:

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
`deploy/crds/crabka.io_greses.yaml` differs from Task 2 HEAD.

- [ ] **Step 7: Run GREEN and operator gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator wal_producer_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 8: Commit and review**

Run:

```bash
git add crates/operator/src/crd/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_greses.yaml
git commit -m "feat(operator): expose producer DNS timeout"
```

Obtain independent spec-compliance and quality approval. Resume the same
implementer for fixes until both reviews pass.

---

### Task 4: Audit, verify, publish, and continue

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the reviewed producer, Gres, and operator implementation
- Produces: classified audit evidence, updated draft PR #904, and the next
  unresolved configuration owner

- [ ] **Step 1: Run the broad and focused audits**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "lookup_host|ClientDnsTimeout|DEFAULT_CLIENT_DNS_TIMEOUT|dns[_-]timeout|DnsTimeout|wal-producer-dns-timeout|walProducerDnsTimeout" \
  crates deploy/crds docs/configuration-audit.md
```

Classify producer/Gres production matches, schemas, and tests separately.
Confirm the Gres WAL producer has one CLI/environment/CRD owner and one live
consumer. List remaining producer deployments and higher-level
consumer/streams/admin owners without claiming they are covered.

- [ ] **Step 2: Record exact audit evidence**

Append `Gres WAL Producer DNS Timeout` to
`docs/configuration-audit.md`, documenting:

- the reused validated `ClientDnsTimeout` and 10-second default;
- standalone CLI/environment and CRD names;
- the exact CRD → operator argument → Gres runtime → `LiveRecoveryConfig` →
  producer → client-core flow;
- focused scanner counts split into production/schema and test/harness
  references;
- verification command results;
- the next coherent unresolved owner.

Do not claim the repository-wide goal is complete.

- [ ] **Step 3: Commit the audit**

Run:

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record producer DNS audit"
```

- [ ] **Step 4: Obtain whole-slice independent review**

Review the complete range from Task 1 parent through Task 4 HEAD against the
approved design and this plan. Require:

- the producer builder validates before I/O and forwards the exact duration;
- DNS remains independent from connect/request/retry policy;
- standalone naming and precedence are exact;
- CRD validation and single-/multi-range rendering are exact;
- the configured value reaches the only Gres WAL producer construction;
- no unrelated client or resolver changes;
- only scoped committed files appear.

Resume the owning implementer for one coherent fix wave, rerun affected gates,
and repeat review until every finding is explicitly addressed.

- [ ] **Step 5: Run fresh final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-client-producer -p crabka-gres-substrate \
  -p crabka-gres -p crabka-operator --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy \
  -p crabka-client-producer -p crabka-gres-substrate \
  -p crabka-gres -p crabka-operator --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres --locked -- --help |
  rg -- '--wal-producer-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Generate two fresh nine-file CRD directories again and require both to match
each other and `deploy/crds`.

- [ ] **Step 6: Verify scope and publish**

Run:

```bash
git status --short
git log --oneline b2410f5d..HEAD
git diff --stat b2410f5d..HEAD
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
requirement completion audit proves no hardcoded operational values remain.
