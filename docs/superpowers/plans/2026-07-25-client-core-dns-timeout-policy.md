# Client Core DNS Timeout Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound every `crabka-client-core` bootstrap, reconnect, and advertised-broker DNS lookup with one validated, configurable per-lookup deadline.

**Architecture:** Add a `refined_type`-validated `ClientDnsTimeout` to `ConnectionOptions`, validate the client builder's raw `Duration` before I/O, and route the typed value through the existing bootstrap and pool paths. Reuse one private future seam around `tokio::time::timeout`; do not add a resolver trait or alter ordered fallback.

**Tech Stack:** Rust 2024, Tokio time/net, `refined_type`, Bon builders, `assert2`.

## Global Constraints

- Use one shared DNS timeout for initial bootstrap, reconnect, and advertised-broker resolution.
- Preserve the 10-second default and keep DNS independent from the existing TCP-connect and request deadlines.
- Accept only positive, whole-millisecond values representable as `u64` milliseconds; return `ClientError::InvalidConfig` before DNS or socket I/O.
- Preserve ordered per-entry fallback, `Disconnected` when no bootstrap entry resolves, and best-effort advertised-broker refresh.
- Name the existing 30-second connect and request defaults without changing their behavior.
- Do not add a resolver trait, global setting, compatibility shim, CRD field, CLI option, or dependency other than the workspace's existing `refined_type`.
- Use `assert2`, never Rust's plain assertion macros.
- Every Cargo command must set `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Preserve all pre-existing unrelated modified and untracked files.

---

### Task 1: Validated and Bounded Client DNS Policy

**Files:**
- Modify: `crates/client-core/Cargo.toml`
- Modify: `crates/client-core/src/error.rs`
- Modify: `crates/client-core/src/connection.rs`
- Modify: `crates/client-core/src/bootstrap.rs`
- Modify: `crates/client-core/src/pool.rs`
- Modify: `crates/client-core/src/client.rs`
- Modify: `crates/client-core/src/lib.rs`

**Interfaces:**
- Produces: `DEFAULT_CLIENT_DNS_TIMEOUT`, `DEFAULT_CLIENT_CONNECT_TIMEOUT`, and `DEFAULT_CLIENT_REQUEST_TIMEOUT`.
- Produces: `ClientDnsTimeout::new(Duration) -> Result<ClientDnsTimeout, String>`, `duration() -> Duration`, and `milliseconds() -> u64`.
- Extends: `ConnectionOptions { dns_timeout: ClientDnsTimeout, .. }`.
- Extends: `Client::builder().dns_timeout(Duration)`.
- Changes internally: `bootstrap::resolve(&str, ClientDnsTimeout)`.
- Produces internally: `bounded_lookup(ClientDnsTimeout, Future) -> Result<Future::Output, tokio::time::error::Elapsed>`.

- [ ] **Step 1: Record the baseline**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --all-targets
```

Expected: exit 0. Record every suite summary rather than inventing an aggregate when nested test processes interleave.

- [ ] **Step 2: Add failing validation and default tests**

Add `refined_type = { workspace = true }` to `crates/client-core/Cargo.toml`.

In `connection.rs`, add tests that require the new public constants and scalar:

```rust
#[test]
fn client_dns_timeout_validates_and_preserves_milliseconds() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(37)).expect("positive timeout");
    assert!(timeout.duration() == Duration::from_millis(37));
    assert!(timeout.milliseconds() == 37);
    assert!(ClientDnsTimeout::new(Duration::ZERO).is_err());
    assert!(ClientDnsTimeout::new(Duration::from_nanos(1)).is_err());
    assert!(ClientDnsTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1)).is_err());
}

#[test]
fn connection_options_own_named_defaults() {
    let options = ConnectionOptions::default();
    assert!(options.dns_timeout == ClientDnsTimeout::default());
    assert!(options.dns_timeout.duration() == DEFAULT_CLIENT_DNS_TIMEOUT);
    assert!(options.connect_timeout == DEFAULT_CLIENT_CONNECT_TIMEOUT);
    assert!(options.request_timeout == DEFAULT_CLIENT_REQUEST_TIMEOUT);
}
```

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core client_dns_timeout --lib
```

Expected: compilation fails because `ClientDnsTimeout` and the named constants do not exist.

- [ ] **Step 3: Implement the typed policy and validation error**

In `connection.rs`, define the named defaults and typed scalar using `refined_type::rule::MinMaxU128`:

```rust
pub const DEFAULT_CLIENT_DNS_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientDnsTimeout(Duration);

impl ClientDnsTimeout {
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("client DNS timeout: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("client DNS timeout: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err("client DNS timeout must be a whole number of milliseconds".to_owned());
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis()).expect("validated client DNS timeout fits u64")
    }
}

impl Default for ClientDnsTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_CLIENT_DNS_TIMEOUT).expect("default client DNS timeout is valid")
    }
}
```

Add `dns_timeout: ClientDnsTimeout` to `ConnectionOptions`, and use the three named constants in `Default`.

In `error.rs`, add:

```rust
#[error("invalid client configuration: {0}")]
InvalidConfig(String),
```

Re-export the scalar and constants from `lib.rs`.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core client_dns_timeout --lib
```

Expected: the scalar/default tests pass.

- [ ] **Step 4: Add failing deadline and fallback tests**

In `bootstrap.rs`, convert the touched tests to `assert2` and update them to pass `ClientDnsTimeout::default()`. Add paused-time coverage for the private future seam:

```rust
#[tokio::test(start_paused = true)]
async fn bounded_lookup_stops_at_the_configured_deadline() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(37)).expect("positive timeout");
    let started = tokio::time::Instant::now();
    let result = bounded_lookup(timeout, std::future::pending::<()>()).await;
    assert!(result.is_err());
    assert!(started.elapsed() == Duration::from_millis(37));
}
```

Add a fallback test using one malformed entry followed by a literal address:

```rust
#[tokio::test]
async fn resolve_skips_a_failed_entry_and_keeps_later_addresses() {
    let addrs = resolve(":,127.0.0.1:9093", ClientDnsTimeout::default())
        .await
        .expect("later address resolves");
    assert!(addrs.iter().any(|addr| addr.port() == 9093));
}
```

Add a builder test in `client.rs`:

```rust
#[tokio::test]
async fn zero_dns_timeout_is_rejected_before_resolution() {
    let result = Client::builder()
        .bootstrap("unused.invalid:9092")
        .dns_timeout(Duration::ZERO)
        .build()
        .await;
    assert!(matches!(result, Err(ClientError::InvalidConfig(_))));
}
```

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core bounded_lookup_stops_at_the_configured_deadline --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core zero_dns_timeout_is_rejected_before_resolution --lib
```

Expected: compilation fails because the seam and builder field are absent.

- [ ] **Step 5: Bound bootstrap and reconnect resolution**

In `bootstrap.rs`, add the minimal reusable seam:

```rust
pub(crate) async fn bounded_lookup<F>(
    timeout: ClientDnsTimeout,
    lookup: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    tokio::time::timeout(timeout.duration(), lookup).await
}
```

Change `resolve` to accept `ClientDnsTimeout` and wrap each `tokio::net::lookup_host(part)` future. Extend addresses only for `Ok(Ok(iter))`; log and skip both `Ok(Err(error))` and deadline expiry. Do not return early on one failed entry.

In `client.rs`, add:

```rust
#[builder(default = DEFAULT_CLIENT_DNS_TIMEOUT)]
dns_timeout: Duration,
```

Validate it before constructing `ConnectionOptions`:

```rust
let dns_timeout =
    ClientDnsTimeout::new(dns_timeout).map_err(ClientError::InvalidConfig)?;
```

Use `options.dns_timeout` for initial `bootstrap::resolve` and `self.options.dns_timeout` for reconnect. Remove the now-obsolete `#[allow(dead_code)]` from the stored options field.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core bootstrap --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core bounded_lookup_stops_at_the_configured_deadline --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core zero_dns_timeout_is_rejected_before_resolution --lib
```

Expected: all focused tests pass.

- [ ] **Step 6: Add failing advertised-broker policy tests**

In `pool.rs`, add a test pinning that the pool receives the typed option:

```rust
#[test]
fn pool_carries_the_configured_dns_timeout() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(41)).expect("positive timeout");
    let pool = BrokerPool::new(
        vec![],
        ConnectionOptions {
            dns_timeout: timeout,
            ..ConnectionOptions::default()
        },
    );
    assert!(pool.dns_timeout == timeout);
}
```

Add deterministic advertised-resolution deadline coverage against a private `first_resolved_addr` future seam:

```rust
#[tokio::test(start_paused = true)]
async fn advertised_broker_lookup_stops_at_the_configured_deadline() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(41)).expect("positive timeout");
    let started = tokio::time::Instant::now();
    let addr = first_resolved_addr(
        timeout,
        std::future::pending::<std::io::Result<std::vec::IntoIter<SocketAddr>>>(),
    )
    .await;
    assert!(addr.is_none());
    assert!(started.elapsed() == Duration::from_millis(41));
}
```

Keep `refresh_resolves_hostnames` and `refresh_skips_undialable_ports` as behavioral coverage for successful and skipped advertised addresses.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core pool_carries_the_configured_dns_timeout --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core advertised_broker_lookup_stops_at_the_configured_deadline --lib
```

Expected: compilation fails because `BrokerPool` does not store the policy and the advertised-resolution seam does not exist.

- [ ] **Step 7: Bound advertised-broker resolution**

Add `dns_timeout: ClientDnsTimeout` to `BrokerPool`. In `BrokerPool::new`, copy `options.dns_timeout` before moving the options into `TcpConnector`. Extend the private `with_connector` constructor with an explicit timeout and update its test-only callers to use `ClientDnsTimeout::default()`.

Add the private helper that directly owns advertised-address selection:

```rust
async fn first_resolved_addr<F, I>(timeout: ClientDnsTimeout, lookup: F) -> Option<SocketAddr>
where
    F: Future<Output = std::io::Result<I>>,
    I: Iterator<Item = SocketAddr>,
{
    bounded_lookup(timeout, lookup).await.ok()?.ok()?.next()
}
```

In `refresh_brokers`, call `first_resolved_addr(self.dns_timeout, tokio::net::lookup_host((b.host.as_str(), port)))` and insert the returned address when it is `Some`.

Keep the existing best-effort behavior: timeout and resolver failure both leave that broker absent without failing the metadata refresh.

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core pool_carries_the_configured_dns_timeout --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core advertised_broker_lookup_stops_at_the_configured_deadline --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core refresh_ --lib
```

Expected: all pool policy and resolution tests pass.

- [ ] **Step 8: Run task gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-core --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: every command exits 0. Stable rustfmt may print warnings for nightly-only repository settings; warnings are acceptable only when the command exits 0.

- [ ] **Step 9: Commit**

Stage only the seven task-owned files:

```bash
git add \
  crates/client-core/Cargo.toml \
  crates/client-core/src/error.rs \
  crates/client-core/src/connection.rs \
  crates/client-core/src/bootstrap.rs \
  crates/client-core/src/pool.rs \
  crates/client-core/src/client.rs \
  crates/client-core/src/lib.rs
git diff --cached --check
git commit -m "feat(client): bound DNS resolution"
```

### Task 2: Audit and Slice Verification

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the committed `ClientDnsTimeout` policy and all lookup call sites from Task 1.
- Produces: an evidence-backed audit entry and the next coherent unresolved configuration owner.

- [ ] **Step 1: Run the broad and focused scanners**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "lookup_host|ClientDnsTimeout|DEFAULT_CLIENT_(DNS|CONNECT|REQUEST)_TIMEOUT|dns_timeout|bounded_lookup" \
  crates docs/configuration-audit.md
```

Classify every focused match as production, test/harness, or prior audit. Confirm that both `crates/client-core/src/bootstrap.rs` and `crates/client-core/src/pool.rs` route `lookup_host` through the deadline seam. Do not claim other crates' DNS owners are complete.

- [ ] **Step 2: Update the audit**

Append one `Client Core DNS Timeout Policy` section to `docs/configuration-audit.md` recording:

- the 10-second shared default and positive whole-millisecond `refined_type` validation;
- initial bootstrap, reconnect, and advertised-broker propagation;
- independent DNS, TCP-connect, and request deadlines;
- preserved fallback and best-effort behavior;
- exact scanner counts and classifications;
- all fresh verification commands and results;
- higher-level producer/consumer/streams/admin deployment propagation as the next coherent owner;
- continued repository-wide audit scope.

- [ ] **Step 3: Run fresh final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-core --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-core --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 4: Commit**

```bash
git add docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(client): record DNS timeout audit"
```

### Task 3: Independent Review and Publication

**Files:**
- Review only: the complete implementation range from the parent of Task 1 through Task 2 HEAD.

**Interfaces:**
- Consumes: the approved design, this plan, task reports, committed diff, and scanner evidence.
- Produces: a clean independent review verdict and published draft PR head.

- [ ] **Step 1: Review the whole slice**

The reviewer must confirm:

- every client-core `lookup_host` is deadline-bounded;
- one typed policy covers bootstrap, reconnect, and advertised brokers;
- invalid values fail before I/O;
- DNS remains independent from TCP connect and request deadlines;
- ordered fallback and best-effort refresh behavior are unchanged;
- no resolver abstraction, caller propagation, CLI, CRD, or unrelated scope was added;
- tests are deterministic and the audit does not overclaim repository-wide completion.

- [ ] **Step 2: Apply at most one review fix wave**

If the reviewer reports findings, use one focused fix wave and one scoped rereview. Do not expand into higher-level producer/consumer deployment propagation in this slice.

- [ ] **Step 3: Re-run final verification**

Repeat Task 2 Step 3 after the final reviewed commit.

- [ ] **Step 4: Push and verify PR #904**

```bash
git push origin configuration_expose
git rev-parse HEAD
git ls-remote origin refs/heads/configuration_expose
```

Confirm that both SHAs match and that draft PR #904 is open with the same `head_sha`.
