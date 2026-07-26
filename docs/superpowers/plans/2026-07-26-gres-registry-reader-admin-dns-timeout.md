# Gres Registry Reader/Admin DNS Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose one validated DNS deadline for Gres registry reader and admin
lookups through every standalone registry CLI/environment surface and the
Kafka-owned registry CRD policy.

**Architecture:** Make `AdminClient` honor the `ClientDnsTimeout` already
carried by `ConnectionOptions`, then store one reader/admin timeout on
`RegistryPolicy`. Registry reader resolution and admin connections consume
that typed value; existing standalone and operator surfaces carry the same
effective policy without a new type, dependency, or renderer abstraction.

**Tech Stack:** Rust 2024, Tokio, Clap, `refined_type`, kube/schemars, serde,
generated Kubernetes CRDs.

## Global Constraints

- Every Cargo command must set
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must use `--locked`; add no dependency.
- RED precedes production edits.
- Reuse `crabka_client_core::ClientDnsTimeout`; add no registry-specific
  timeout type or second reader/admin policy.
- Default to `ClientDnsTimeout::default()` (10 seconds).
- Use exactly `--registry-reader-admin-dns-timeout-ms`,
  `CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS`, and
  `spec.gresRegistry.readerAdminDnsTimeoutMs`.
- CLI precedence is command line, environment, then the 10-second default.
- The CRD setting governs operator-internal registry control, Gres compute,
  and the activator.
- Reader and admin DNS share one value; registry producer DNS remains
  independent and unchanged.
- Preserve existing reader retry backoff and admin error propagation.
- Do not configure TCP-connect, request, topic-create, fetch, retry,
  transaction, WAL, or unrelated DNS behavior.
- Add no compatibility shim, source-text assertion, lint suppression,
  dependency, cross-crate parser helper, or speculative accessor.
- Preserve unrelated dirty and untracked files; stage only task-owned paths.

## Execution Preflight

- Confirm this checkout remains the linked `configuration_expose` worktree.
- Record `git status --short` and the exact Task 1 parent SHA.
- Run the affected-package baseline:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-client-admin -p crabka-gres-control \
  -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked
```

- Use a fresh implementer per task and independent spec-compliance and quality
  review after each task.

---

### Task 1: Make AdminClient honor its DNS timeout

**Files:**
- Modify: `crates/client-admin/src/lib.rs`

**Interfaces:**
- Consumes:
  `crabka_client_core::ClientDnsTimeout`
- Produces:
  `AdminClient::connect_with_dns_timeout(&[String], ClientDnsTimeout) -> Result<AdminClient, AdminError>`
- Preserves:
  `AdminClient::connect_with_options(&[String], ConnectionOptions)`
- Preserves:
  bootstrap and controller reconnects through `AdminClient::connect_one`

- [ ] **Step 1: Add failing deadline and option tests**

Add a private lookup helper test beside the existing admin option tests. The
pending lookup makes Tokio's paused clock prove the exact deadline without
depending on external DNS:

```rust
#[tokio::test(start_paused = true)]
async fn dns_lookup_stops_at_connection_option_deadline() {
    let timeout = crabka_client_core::ClientDnsTimeout::new(
        Duration::from_millis(37),
    )
    .expect("positive timeout");
    let started = tokio::time::Instant::now();
    let pending = std::future::pending::<
        std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>,
    >();

    let result = lookup_first("broker.invalid:9092", timeout, pending).await;

    assert2::assert!(result.is_err());
    assert2::assert!(started.elapsed() == Duration::from_millis(37));
}
```

Add a separate live test beside `custom_options_are_observable_on_initial_dial`:

```rust
#[tokio::test]
async fn connect_with_dns_timeout_preserves_admin_defaults() {
    let live = ObservedAdminBroker::start(Duration::ZERO).await;
    let timeout = crabka_client_core::ClientDnsTimeout::new(
        Duration::from_millis(37),
    )
    .expect("positive timeout");
    let admin = AdminClient::connect_with_dns_timeout(
        &[live.addr.to_string()],
        timeout,
    )
    .await
    .expect("admin connects");

    assert2::assert!(admin.options.dns_timeout == timeout);
    assert2::assert!(admin.options.client_id == "crabka-operator");
    assert2::assert!(admin.options.connect_timeout == Duration::from_secs(5));
    assert2::assert!(admin.options.request_timeout == Duration::from_secs(30));
    live.stop();
}
```

- [ ] **Step 2: Run RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin \
  dns_lookup_stops_at_connection_option_deadline --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin \
  connect_with_dns_timeout --lib --locked
```

Expected: compilation fails because `lookup_first` and
`connect_with_dns_timeout` do not exist.

- [ ] **Step 3: Bound the shared lookup**

Add one private helper that accepts the lookup future so the real path and
paused-clock test exercise the same timeout:

```rust
async fn lookup_first<F, I>(
    host_port: &str,
    dns_timeout: crabka_client_core::ClientDnsTimeout,
    lookup: F,
) -> Result<std::net::SocketAddr, AdminError>
where
    F: std::future::Future<Output = std::io::Result<I>>,
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(dns_timeout.duration(), lookup)
        .await
        .map_err(|_| {
            AdminError::Protocol(format!(
                "DNS lookup {host_port} timed out after {} ms",
                dns_timeout.milliseconds(),
            ))
        })?
        .map_err(|error| {
            AdminError::Protocol(format!("DNS lookup {host_port}: {error}"))
        })?;
    addrs.next().ok_or_else(|| {
        AdminError::Protocol(format!("no addresses for {host_port}"))
    })
}
```

Replace the unbounded lookup in `connect_one` with:

```rust
let addr = lookup_first(
    host_port,
    opts.dns_timeout,
    tokio::net::lookup_host(host_port),
)
.await?;
```

Do not add a second timeout around `Connection::connect_with_options`; its
existing connect timeout remains authoritative.

- [ ] **Step 4: Add the narrow public entry point**

Add beside `connect_secured`:

```rust
/// Connect with the standard plaintext admin policy and a custom DNS deadline.
///
/// # Errors
/// Returns `AdminError::Connect { tried }` if no bootstrap address connects.
pub async fn connect_with_dns_timeout(
    bootstrap_addrs: &[String],
    dns_timeout: crabka_client_core::ClientDnsTimeout,
) -> Result<Self, AdminError> {
    let mut options = Self::opts(None);
    options.dns_timeout = dns_timeout;
    Self::connect_with_options(bootstrap_addrs, options).await
}
```

This preserves the existing admin client id and 5-second connect timeout. Do
not make `opts` public and do not create an admin policy type.

- [ ] **Step 5: Run GREEN and crate gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin dns_lookup --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin connect_with_dns_timeout --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-admin --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-admin --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 6: Commit and review**

```bash
git add crates/client-admin/src/lib.rs
git commit -m "fix(admin): honor DNS timeout"
```

Require independent approval that bootstrap, controller, and bootstrap-retry
lookups all route through the bounded function and that no connect/request
timeout behavior changed.

---

### Task 2: Carry reader/admin DNS policy through Registry

**Files:**
- Modify: `crates/gres-control/src/registry.rs`

**Interfaces:**
- Consumes:
  `AdminClient::connect_with_dns_timeout(&[String], ClientDnsTimeout)`
- Produces:
  `RegistryPolicy::reader_admin_dns_timeout(&self) -> ClientDnsTimeout`
- Produces:
  `RegistryPolicy::with_reader_admin_dns_timeout_ms(self, u64) -> Result<Self, String>`
- Preserves:
  `RegistryPolicy::new(i32, i32, u64, i32, i32) -> Result<Self, String>`

- [ ] **Step 1: Add failing policy tests**

Add beside the existing registry DNS policy test:

```rust
#[test]
fn registry_reader_admin_dns_defaults_and_replaces_exactly() {
    let defaults = RegistryPolicy::default();
    assert!(
        defaults.reader_admin_dns_timeout()
            == crabka_client_core::ClientDnsTimeout::default()
    );

    let policy = defaults
        .with_reader_admin_dns_timeout_ms(37)
        .expect("valid DNS timeout");
    assert!(policy.reader_admin_dns_timeout().milliseconds() == 37);
    assert!(
        RegistryPolicy::default()
            .with_reader_admin_dns_timeout_ms(0)
            .is_err()
    );
}
```

Add an async resolver test:

```rust
#[tokio::test]
async fn registry_bootstrap_resolver_accepts_typed_deadline() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(37))
        .expect("positive timeout");
    let addr = resolve_bootstrap_addr("127.0.0.1:9092", timeout)
        .await
        .expect("literal address");
    assert!(addr.port() == 9092);
}
```

- [ ] **Step 2: Run RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control \
  registry_reader_admin_dns --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control \
  registry_bootstrap_resolver_accepts_typed_deadline --lib --locked
```

Expected: compilation fails because the policy methods and async resolver
signature do not exist.

- [ ] **Step 3: Add the typed policy field**

Add to `RegistryPolicy`:

```rust
reader_admin_dns_timeout: ClientDnsTimeout,
```

Initialize it in `RegistryPolicy::new`:

```rust
reader_admin_dns_timeout: ClientDnsTimeout::default(),
```

Add:

```rust
/// DNS lookup deadline used by registry reader and admin paths.
#[must_use]
pub const fn reader_admin_dns_timeout(&self) -> ClientDnsTimeout {
    self.reader_admin_dns_timeout
}

/// Validate and replace the registry reader/admin DNS lookup deadline.
///
/// # Errors
///
/// Returns an error when `milliseconds` is zero.
#[must_use = "the validated policy must be used"]
pub fn with_reader_admin_dns_timeout_ms(
    mut self,
    milliseconds: u64,
) -> Result<Self, String> {
    self.reader_admin_dns_timeout =
        ClientDnsTimeout::new(Duration::from_millis(milliseconds))?;
    Ok(self)
}
```

Keep the existing producer field and methods unchanged.

- [ ] **Step 4: Make registry resolution async and bounded**

Replace the synchronous `ToSocketAddrs` helper with:

```rust
async fn resolve_bootstrap_addr(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
) -> Option<SocketAddr> {
    for entry in bootstrap.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
        let Ok(Ok(mut addrs)) = tokio::time::timeout(
            dns_timeout.duration(),
            tokio::net::lookup_host(entry),
        )
        .await
        else {
            continue;
        };
        if let Some(addr) = addrs.next() {
            return Some(addr);
        }
    }
    None
}
```

Remove the unused `ToSocketAddrs` import. Update registry refresh and the
background reader to call:

```rust
resolve_bootstrap_addr(
    &self.bootstrap,
    self.policy.reader_admin_dns_timeout(),
)
.await
```

and:

```rust
resolve_bootstrap_addr(
    &bootstrap,
    policy.reader_admin_dns_timeout(),
)
.await
```

Keep existing retry and error branches unchanged.

- [ ] **Step 5: Route both admin call sites through the policy**

In registry refresh and `ensure_compacted_single_partition_topic`, replace
`AdminClient::connect` with:

```rust
AdminClient::connect_with_dns_timeout(
    &bootstrap_addrs,
    policy.reader_admin_dns_timeout(),
)
.await?
```

For the `&self` refresh path use
`self.policy.reader_admin_dns_timeout()`. Do not alter topic-create timeout,
metadata requests, or retry behavior.

When creating direct reader/refresh `ConnectionOptions`, carry the same typed
value:

```rust
ConnectionOptions {
    dns_timeout: policy.reader_admin_dns_timeout(),
    client_id: "...".to_string(),
    ..Default::default()
}
```

Use `self.policy` in refresh. This preserves the existing connect and request
defaults.

- [ ] **Step 6: Run GREEN and crate gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control \
  registry_reader_admin_dns --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control \
  registry_bootstrap_resolver --lib --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-control --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 7: Commit and review**

```bash
git add crates/gres-control/src/registry.rs
git commit -m "feat(gres): configure registry reader DNS"
```

Require independent approval that one typed policy reaches refresh, reader,
topic creation, and metadata admin paths while producer DNS remains unchanged.

---

### Task 3: Expose every standalone registry surface

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/cli/src/gres.rs`
- Modify: `crates/gres-activator/src/main.rs`
- Modify: `crates/gres-loadtest/src/main.rs`
- Modify: `crates/gres-loadtest/src/cluster.rs`

**Interfaces:**
- Consumes:
  `RegistryPolicy::with_reader_admin_dns_timeout_ms(u64) -> Result<RegistryPolicy, String>`
- Produces on all four parsers:
  `--registry-reader-admin-dns-timeout-ms`
- Produces on all four parsers:
  `CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS`
- Produces from the load-test child renderer:
  one `--registry-reader-admin-dns-timeout-ms VALUE` pair

- [ ] **Step 1: Extend the existing failing parser tests**

In the four existing registry option test modules, add this zero case:

```rust
"--registry-reader-admin-dns-timeout-ms=0",
```

Add this environment value:

```rust
(
    "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS",
    "37",
),
```

Use `Some("37")` in the activator's `temp_env` array. Add this CLI override:

```rust
"--registry-reader-admin-dns-timeout-ms=47",
```

Extend the expected environment and CLI policies:

```rust
.with_reader_admin_dns_timeout_ms(37)
.expect("environment reader/admin DNS timeout")
```

and:

```rust
.with_reader_admin_dns_timeout_ms(47)
.expect("CLI reader/admin DNS timeout")
```

Keep each module's existing environment isolation.

- [ ] **Step 2: Add the failing load-test child assertion**

Extend the distinctive policy in the child argument test:

```rust
.with_reader_admin_dns_timeout_ms(37)
.expect("reader/admin DNS timeout")
```

Assert:

```rust
assert!(
    arg_value(
        &spawned_args,
        "--registry-reader-admin-dns-timeout-ms",
    ) == Some("37")
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

Expected: parsers reject the unknown flag or produce the old policy, and the
load-test child omits the argument.

- [ ] **Step 4: Add the option to each existing RegistryOptions**

Add immediately after the producer DNS option in all four parser structs:

```rust
#[arg(
    long = "registry-reader-admin-dns-timeout-ms",
    env = "CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS"
)]
reader_admin_dns_timeout_ms: Option<PositiveMillis>,
```

Use the module's existing positive-millisecond type alias where it differs.
In each `RegistryOptions::policy`, resolve absence from the typed default:

```rust
let reader_admin_dns_timeout_ms =
    self.reader_admin_dns_timeout_ms.map_or_else(
        || {
            RegistryPolicy::default()
                .reader_admin_dns_timeout()
                .milliseconds()
        },
        PositiveMillis::into_value,
    );
```

After constructing the base policy and applying producer DNS, attach:

```rust
.with_reader_admin_dns_timeout_ms(reader_admin_dns_timeout_ms)
.expect("validated registry reader/admin DNS timeout")
```

Do not extract a shared parser helper.

- [ ] **Step 5: Extend the load-test child renderer**

Increase its fixed return size from 12 to 14 and append:

```rust
"--registry-reader-admin-dns-timeout-ms".to_owned(),
policy.reader_admin_dns_timeout().milliseconds().to_string(),
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
  rg -- '--registry-reader-admin-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 7: Commit and review**

```bash
git add crates/gres/src/lib.rs crates/cli/src/gres.rs \
  crates/gres-activator/src/main.rs crates/gres-loadtest/src/main.rs \
  crates/gres-loadtest/src/cluster.rs
git commit -m "feat(gres): expose registry reader DNS"
```

Require independent approval of all four CLI/environment surfaces, exact
precedence, zero rejection, and load-test child propagation.

---

### Task 4: Expose Kafka CRD and operator paths

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/context.rs`
- Modify: `crates/operator/src/controller/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: `crates/operator/tests/reconcile_gres.rs` when exact arrays require it
- Modify generated: `deploy/crds/crabka.io_kafkas.yaml`

**Interfaces:**
- Produces:
  `GresRegistrySpec::reader_admin_dns_timeout_ms: Option<u64>`
- Produces:
  exact malformed path `spec.gresRegistry.readerAdminDnsTimeoutMs`
- Produces:
  one reader/admin DNS argument in compute and activator containers
- Preserves:
  operator control cache equality through `RegistryPolicy`

- [ ] **Step 1: Add failing CRD tests**

In `gres_registry_round_trips_and_defaults`, add:

```json
"readerAdminDnsTimeoutMs":37
```

Extend the expected policy:

```rust
.with_reader_admin_dns_timeout_ms(37)
.expect("reader/admin DNS timeout")
```

Add `"readerAdminDnsTimeoutMs"` to the schema-minimum loop. Add this invalid
case:

```rust
GresRegistrySpec {
    reader_admin_dns_timeout_ms: Some(0),
    ..Default::default()
},
```

Require:

```rust
let error = GresRegistrySpec {
    reader_admin_dns_timeout_ms: Some(0),
    ..Default::default()
}
.policy()
.expect_err("zero reader/admin DNS timeout");
assert!(error.starts_with(
    "spec.gresRegistry.readerAdminDnsTimeoutMs:"
));
```

- [ ] **Step 2: Add failing operator propagation tests**

In `gres_control_cache_tracks_inputs_without_locking_during_build`, create a
policy differing only by reader/admin DNS:

```rust
let changed_reader_admin_dns = defaults
    .clone()
    .with_reader_admin_dns_timeout_ms(37)
    .expect("reader/admin DNS timeout");
```

Require a rebuilt control for that policy.

Extend activator and compute custom-policy tests with:

```rust
"--registry-reader-admin-dns-timeout-ms",
"37",
```

and:

```rust
["--registry-reader-admin-dns-timeout-ms", "37"],
```

Configure their policy with the typed override. Count the flag exactly once:

```rust
assert!(
    args.iter()
        .filter(|arg| {
            arg.as_str() == "--registry-reader-admin-dns-timeout-ms"
        })
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
/// DNS lookup deadline for registry reader and admin paths.
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub reader_admin_dns_timeout_ms: Option<u64>,
```

Resolve the typed default:

```rust
let reader_admin_dns_timeout_ms =
    self.reader_admin_dns_timeout_ms.unwrap_or_else(|| {
        crabka_gres_control::RegistryPolicy::default()
            .reader_admin_dns_timeout()
            .milliseconds()
    });
```

After constructing the base policy and applying producer DNS, attach:

```rust
.with_reader_admin_dns_timeout_ms(reader_admin_dns_timeout_ms)
.map_err(|error| {
    format!("spec.gresRegistry.readerAdminDnsTimeoutMs: {error}")
})
```

Preserve the existing complete policy-error mapping at reconciliation call
sites.

- [ ] **Step 5: Render the shared argument**

In both existing registry argument vectors, add adjacent to producer DNS:

```rust
"--registry-reader-admin-dns-timeout-ms",
policy.reader_admin_dns_timeout().milliseconds().to_string(),
```

Use `registry_policy` where that is the existing variable. Update only exact
integration-test argument arrays that fail because the required default pair
is now rendered. Do not create a renderer helper or range-mode branch.

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

Expected: both generations contain nine identical files; only
`deploy/crds/crabka.io_kafkas.yaml` differs from Task 3 HEAD. Remove only the
two exact temporary directories after comparison.

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
  crates/operator/tests/reconcile_gres.rs \
  deploy/crds/crabka.io_kafkas.yaml
git commit -m "feat(operator): expose registry reader DNS"
```

If `crates/operator/tests/reconcile_gres.rs` did not change, omit it from
staging. Require independent approval of CRD ownership, exact error naming,
cache behavior, compute and activator rendering, and generated schema.

---

### Task 5: Audit, verify, publish, and continue

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes:
  reviewed admin boundary, registry policy, standalone surfaces, and operator paths
- Produces:
  classified audit evidence, updated draft PR #904, and the next unresolved owner

- [ ] **Step 1: Run broad and focused audits**

```bash
tools/audit-runtime-values.sh
rg -n \
  "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|registry-reader-admin-dns-timeout|readerAdminDnsTimeoutMs" \
  crates deploy/crds docs/configuration-audit.md
```

Classify production, schema, test/harness, and prior-audit references. Confirm
the registry's reader, refresh, topic-create, and metadata paths consume the
same typed policy. Confirm producer DNS remains separate.

- [ ] **Step 2: Record exact audit evidence**

Append `Gres Registry Reader/Admin DNS Timeout` to
`docs/configuration-audit.md`. Record:

- exact CLI, environment, and CRD names;
- the validated 10-second default;
- the CRD → `RegistryPolicy` → operator control/rendered CLI → registry
  reader/admin flow;
- the `AdminClient` root fix and reconnect coverage;
- scanner counts by classification;
- verification results;
- producer DNS as separately completed; and
- the next coherent unresolved configuration owner.

Do not claim the repository-wide goal is complete.

- [ ] **Step 3: Commit the audit**

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record registry reader DNS"
```

- [ ] **Step 4: Obtain whole-slice independent review**

Review the Task 1 parent through Task 5 HEAD. Require:

- `ClientDnsTimeout` is reused;
- `AdminClient` bounds every lookup through its carried option;
- one registry reader/admin value reaches refresh, background reader, topic
  creation, and metadata refresh;
- all four standalone parsers use exact naming and precedence;
- the load-test child receives the value;
- Kafka CRD ownership and minimum are exact;
- operator control, compute, and activator paths share one effective policy;
- producer and unrelated DNS paths remain unchanged; and
- only scoped committed files appear.

Address every Critical or Important finding, rerun affected gates, and repeat
review until clean.

- [ ] **Step 5: Run fresh final verification**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test \
  -p crabka-client-admin -p crabka-gres-control \
  -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy \
  -p crabka-client-admin -p crabka-gres-control \
  -p crabka-gres -p crabka-cli \
  -p crabka-gres-activator -p crabka-gres-loadtest \
  -p crabka-operator --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres --locked -- --help |
  rg -- '--registry-reader-admin-dns-timeout-ms'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Generate two fresh nine-file CRD directories and require both to match each
other and `deploy/crds`.

- [ ] **Step 6: Verify scope and publish**

```bash
git status --short
git log --oneline 5153ce6c..HEAD
git diff --stat 5153ce6c..HEAD
git push origin configuration_expose
git rev-parse HEAD
git ls-remote origin refs/heads/configuration_expose
```

Confirm unrelated dirty/untracked files remain unstaged and unchanged. Verify
through the connected GitHub app that PR #904 remains open and draft and that
its `head_sha` equals local HEAD and the remote branch SHA.

- [ ] **Step 7: Continue the repository-wide goal**

Name the next coherent unresolved owner from the audit and begin its design
cycle. Do not mark the persistent goal complete unless a requirement-by-
requirement audit proves no hardcoded operational values remain.
