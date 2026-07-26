# Client Streams Broker DNS Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply one validated broker DNS deadline to every Client Streams broker lookup and expose it through the observability demo's Stream role.

**Architecture:** Reuse `crabka_client_core::ClientDnsTimeout` as the only runtime policy value. Carry it through `StreamsApp`, `KafkaStreams`, broker I/O, and membership; centralize only the duplicated raw lookup and fetch `ConnectionOptions`. The demo parses positive milliseconds with `NonZeroU64`, validates role applicability before I/O, and forwards the typed value.

**Tech Stack:** Rust 2024, Tokio paused time, Bon builders, Clap derive/environment parsing, Docker Compose, `refined_type`-backed `ClientDnsTimeout`

## Global Constraints

- Preserve the exact 10,000-ms default.
- Use one `ClientDnsTimeout` for metadata, raw fetch, producer, offsets, join, and heartbeat DNS.
- Exact demo interfaces are `--streams-broker-dns-timeout-ms` and `CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS`.
- Precedence is CLI over environment over `ClientDnsTimeout::default()`.
- The demo setting is valid only with `--role stream` and must fail before telemetry or external I/O otherwise.
- Preserve bootstrap ordering, first-address selection, TCP/request defaults, ALO/EOS behavior, producer semantics, membership timing, fetch policy, TLS/SASL behavior, and schema-registry DNS behavior.
- Do not add a timeout newtype, policy struct, resolver trait, dependency, lockfile change, CRD field, or per-subclient setting.
- Use `std::num::NonZeroU64` directly at the demo CLI boundary; do not wrap it.
- Every Cargo command must set `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must pass `--locked`.
- Follow TDD: observe the intended failure before production implementation.
- Preserve and never stage unrelated dirty or untracked workspace files.

## File Map

- `crates/client-streams/src/runtime/io_broker.rs`: bounded raw lookup, fetch connection options, and ALO/EOS broker-client propagation.
- `crates/client-streams/src/membership/client.rs`: typed timeout on both membership clients.
- `crates/client-streams/src/runtime/app.rs`: one process value routed into broker I/O and membership.
- `crates/client-streams/src/streams_app.rs`: high-level builder storage/default/forwarding.
- `crates/observability-demo-app/src/main.rs`: CLI/environment parsing, role validation, and Stream-role forwarding.
- `crates/observability-demo-app/tests/streams_dns_config.rs`: subprocess proof of environment/CLI precedence and early rejection.
- `crates/observability-demo-app/tests/observability_demo_config.rs`: Compose pass-through scope.
- `demo/observability/docker-compose.yml`: Stream-role environment pass-through only.
- `docs/configuration-audit.md`: exact scanner evidence and remaining-owner classification.

---

### Task 1: Carry One Typed Timeout Through Client Streams

**Files:**
- Modify: `crates/client-streams/src/runtime/io_broker.rs`
- Modify: `crates/client-streams/src/membership/client.rs`
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/streams_app.rs`

**Interfaces:**
- Consumes:
  ```rust
  crabka_client_core::ClientDnsTimeout
  ClientDnsTimeout::default() // 10,000 ms
  ClientDnsTimeout::duration() -> Duration
  ```
- Produces:
  ```rust
  KafkaStreams::builder().broker_dns_timeout(ClientDnsTimeout)
  StreamsApp::builder().broker_dns_timeout(ClientDnsTimeout)
  StreamsMembership::builder().broker_dns_timeout(ClientDnsTimeout)

  io_broker::build(
      bootstrap: &str,
      group_id: &str,
      client_id: &str,
      broker_dns_timeout: ClientDnsTimeout,
  )

  io_broker::build_eos(
      bootstrap: &str,
      group_id: &str,
      client_id: &str,
      transactional_id: &str,
      broker_dns_timeout: ClientDnsTimeout,
  )
  ```

- [ ] **Step 1: Add failing raw-lookup and fetch-options tests**

In `runtime/io_broker.rs`, extend the existing test module imports with
`Duration`, `SocketAddr`, and `ClientDnsTimeout`. Add:

```rust
#[tokio::test(start_paused = true)]
async fn raw_lookup_stops_at_the_configured_deadline() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(37))
        .expect("positive timeout");
    let started = tokio::time::Instant::now();
    let error = lookup_first(
        "broker.example:9092",
        timeout,
        std::future::pending::<std::io::Result<std::vec::IntoIter<SocketAddr>>>(),
    )
    .await
    .expect_err("pending resolver must time out");

    assert2::assert!(started.elapsed() == Duration::from_millis(37));
    assert2::assert!(
        error.to_string()
            == "runtime error: DNS lookup broker.example:9092 timed out after 37 ms"
    );
}

#[tokio::test]
async fn raw_lookup_preserves_resolver_and_empty_result_context() {
    let timeout = ClientDnsTimeout::default();
    let resolver_error = lookup_first(
        "bad.example:9092",
        timeout,
        std::future::ready(Err::<std::vec::IntoIter<SocketAddr>, _>(
            std::io::Error::other("resolver failed"),
        )),
    )
    .await
    .expect_err("resolver error");
    assert2::assert!(
        resolver_error.to_string()
            == "runtime error: failed to resolve bootstrap bad.example:9092: resolver failed"
    );

    let empty = lookup_first(
        "empty.example:9092",
        timeout,
        std::future::ready(Ok(Vec::<SocketAddr>::new().into_iter())),
    )
    .await
    .expect_err("empty result");
    assert2::assert!(
        empty.to_string()
            == "runtime error: no addresses resolved for bootstrap: empty.example:9092"
    );
}

#[test]
fn fetch_connection_options_carry_the_typed_dns_timeout() {
    let timeout = ClientDnsTimeout::new(Duration::from_millis(41))
        .expect("positive timeout");
    let options = fetch_connection_options("streams-fetch", timeout);

    assert2::assert!(options.client_id == "streams-fetch");
    assert2::assert!(options.dns_timeout == timeout);
    assert2::assert!(
        options.connect_timeout == crabka_client_core::DEFAULT_CLIENT_CONNECT_TIMEOUT
    );
    assert2::assert!(
        options.request_timeout == crabka_client_core::DEFAULT_CLIENT_REQUEST_TIMEOUT
    );
}
```

- [ ] **Step 2: Add failing high-level default/override tests**

In `streams_app.rs`, extend the existing test module:

```rust
#[test]
fn broker_dns_timeout_uses_typed_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.broker_dns_timeout,
        crabka_client_core::ClientDnsTimeout::default()
    );

    let timeout = crabka_client_core::ClientDnsTimeout::new(
        std::time::Duration::from_millis(43),
    )
    .expect("positive timeout");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("override")
        .schema_registry("http://127.0.0.1:8081")
        .broker_dns_timeout(timeout)
        .build();
    assert_eq!(overridden.broker_dns_timeout, timeout);
}
```

- [ ] **Step 3: Run the focused tests and observe the missing interfaces**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  raw_lookup_stops_at_the_configured_deadline

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  broker_dns_timeout_uses_typed_default_and_override
```

Expected: compilation fails because `lookup_first`,
`fetch_connection_options`, and `StreamsApp::broker_dns_timeout` do not exist.

- [ ] **Step 4: Centralize and bound the raw lookup**

In `runtime/io_broker.rs`, import `ClientDnsTimeout`. Add above `build`:

```rust
async fn lookup_first<F, I>(
    bootstrap: &str,
    dns_timeout: ClientDnsTimeout,
    lookup: F,
) -> Result<std::net::SocketAddr, StreamsClientError>
where
    F: std::future::Future<Output = std::io::Result<I>>,
    I: Iterator<Item = std::net::SocketAddr>,
{
    let mut addrs = tokio::time::timeout(dns_timeout.duration(), lookup)
        .await
        .map_err(|_| {
            StreamsClientError::Runtime(format!(
                "DNS lookup {bootstrap} timed out after {} ms",
                dns_timeout.milliseconds(),
            ))
        })?
        .map_err(|error| {
            StreamsClientError::Runtime(format!(
                "failed to resolve bootstrap {bootstrap}: {error}"
            ))
        })?;
    addrs.next().ok_or_else(|| {
        StreamsClientError::Runtime(format!(
            "no addresses resolved for bootstrap: {bootstrap}"
        ))
    })
}

fn fetch_connection_options(
    client_id: &str,
    broker_dns_timeout: ClientDnsTimeout,
) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_owned(),
        dns_timeout: broker_dns_timeout,
        ..ConnectionOptions::default()
    }
}
```

Replace both direct `lookup_host` blocks with:

```rust
let addr = lookup_first(
    bootstrap,
    broker_dns_timeout,
    tokio::net::lookup_host(bootstrap),
)
.await?;
```

Construct both raw connections with:

```rust
Connection::connect_with_options(
    addr,
    fetch_connection_options(client_id, broker_dns_timeout),
)
.await?
```

- [ ] **Step 5: Pass the typed value through both broker-I/O modes**

Add `broker_dns_timeout: ClientDnsTimeout` to the end of `build` and
`build_eos`. In both functions, add:

```rust
.dns_timeout(broker_dns_timeout.duration())
```

to the metadata `Client`, producer, and offset `Client` builders. Do not alter
their client IDs, acks, idempotence, transaction ID, request settings, or
security behavior.

- [ ] **Step 6: Apply the same value to membership**

In `membership/client.rs`, import `ClientDnsTimeout`. Add to
`StreamsMembership::start`:

```rust
#[builder(default)]
broker_dns_timeout: ClientDnsTimeout,
```

Add this to both the initial client and coordinator client builders:

```rust
.dns_timeout(broker_dns_timeout.duration())
```

Keep the existing `security.clone()` forwarding on both paths.

- [ ] **Step 7: Route the value from `KafkaStreams`**

In `runtime/app.rs`, import `ClientDnsTimeout` and add to
`KafkaStreams::start`:

```rust
/// Deadline for each Kafka broker DNS lookup owned by this process.
#[builder(default)]
broker_dns_timeout: ClientDnsTimeout,
```

Pass it to both broker-I/O constructors:

```rust
io_broker::build(
    &bootstrap,
    &application_id,
    &application_id,
    broker_dns_timeout,
)
.await?
```

```rust
io_broker::build_eos(
    &bootstrap,
    &application_id,
    &application_id,
    &txn_id,
    broker_dns_timeout,
)
.await?
```

Pass it to membership:

```rust
.broker_dns_timeout(broker_dns_timeout)
```

- [ ] **Step 8: Store and forward the high-level app value**

In `streams_app.rs`, add:

```rust
broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
```

to `StreamsApp`, and add this parameter to `StreamsApp::new`:

```rust
/// Deadline for each Kafka broker DNS lookup owned by this process.
#[builder(default)]
broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
```

Store it in `Self`, then add:

```rust
.broker_dns_timeout(self.broker_dns_timeout)
```

to `run_built`'s `KafkaStreams` builder.

- [ ] **Step 9: Run focused and complete Client Streams gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  raw_lookup

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  fetch_connection_options_carry_the_typed_dns_timeout

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  broker_dns_timeout_uses_typed_default_and_override

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --all-targets --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
  -p crabka-client-streams --all-targets --locked -- -D warnings

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
```

Expected: all commands pass. Existing `KafkaStreams`, `StreamsApp`, and
`StreamsMembership` builder calls compile unchanged because the new input has
a default.

- [ ] **Step 10: Commit only Client Streams library changes**

```bash
git add -- \
  crates/client-streams/src/runtime/io_broker.rs \
  crates/client-streams/src/membership/client.rs \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/streams_app.rs
git diff --cached --check
git commit -m "feat(streams): bound broker DNS"
```

---

### Task 2: Expose the Demo Stream-Role Boundary

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/streams_dns_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes:
  ```rust
  StreamsApp::builder().broker_dns_timeout(ClientDnsTimeout)
  ```
- Produces:
  ```text
  --streams-broker-dns-timeout-ms
  CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS
  demo-stream Compose default/pass-through: 10000
  ```

- [ ] **Step 1: Add failing parser, validation, and forwarding tests**

At the bottom of `main.rs`, add a test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_broker_dns_timeout_uses_default_and_cli_override() {
        let defaults = Cli {
            role: Role::Stream,
            bootstrap: "127.0.0.1:9092".to_owned(),
            registry: "http://127.0.0.1:8081".to_owned(),
            input_topic: "orders".to_owned(),
            output_topic: "order-counts".to_owned(),
            orders_per_sec: 50,
            streams_broker_dns_timeout_ms: None,
        };
        assert_eq!(
            effective_streams_broker_dns_timeout(&defaults)
                .expect("typed default"),
            crabka_client_core::ClientDnsTimeout::default()
        );

        let overridden = Cli {
            streams_broker_dns_timeout_ms: std::num::NonZeroU64::new(37),
            ..defaults
        };
        assert_eq!(
            effective_streams_broker_dns_timeout(&overridden)
                .expect("typed override")
                .milliseconds(),
            37
        );
    }

    #[test]
    fn streams_broker_dns_timeout_rejects_zero_and_non_stream_roles() {
        Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "stream",
            "--streams-broker-dns-timeout-ms",
            "0",
        ])
        .expect_err("zero must fail in Clap");

        let produce = Cli::try_parse_from([
            "observability-demo-app",
            "--role",
            "produce",
            "--streams-broker-dns-timeout-ms",
            "37",
        ])
        .expect("parse before role validation");
        let error = effective_streams_broker_dns_timeout(&produce)
            .expect_err("Stream-only option");
        assert_eq!(
            error.to_string(),
            "--streams-broker-dns-timeout-ms (37 ms) is only valid with --role stream"
        );
    }
}
```

- [ ] **Step 2: Add failing subprocess precedence/help tests**

Create `tests/streams_dns_config.rs`:

```rust
use std::process::Command;

fn demo() -> Command {
    Command::new(env!("CARGO_BIN_EXE_observability-demo-app"))
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--streams-broker-dns-timeout-ms (37 ms) is only valid with --role stream"
    ));

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-broker-dns-timeout-ms",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--streams-broker-dns-timeout-ms (41 ms) is only valid with --role stream"
    ));
}

#[test]
fn zero_environment_value_is_rejected_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-broker-dns-timeout-ms")
            .count(),
        1
    );
}
```

- [ ] **Step 3: Add the failing Compose scope assertion**

In `observability_demo_config.rs`, add:

```rust
#[test]
fn streams_dns_timeout_is_configurable_only_on_the_stream_role() {
    let compose = docker_compose();
    let stream = compose_service_block(&compose, "demo-stream");
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS:-10000}\""
    ));
    for service in ["demo-produce", "demo-consume"] {
        assert2::assert!(
            !compose_service_block(&compose, service)
                .contains("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS")
        );
    }
}
```

- [ ] **Step 4: Run the focused tests and observe missing configuration**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --bin observability-demo-app --locked \
  streams_broker_dns_timeout

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test streams_dns_config --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test observability_demo_config --locked \
  streams_dns_timeout_is_configurable_only_on_the_stream_role
```

Expected: compilation fails because the CLI field and effective helper do not
exist; the Compose assertion also fails because the environment pass-through
is absent.

- [ ] **Step 5: Add the validated CLI/environment field**

Import `std::num::NonZeroU64` and `crabka_client_core::ClientDnsTimeout`. Add to
`Cli`:

```rust
/// Kafka Streams broker DNS timeout in milliseconds.
#[arg(
    long,
    env = "CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS"
)]
streams_broker_dns_timeout_ms: Option<NonZeroU64>,
```

Add:

```rust
fn effective_streams_broker_dns_timeout(
    cli: &Cli,
) -> std::io::Result<ClientDnsTimeout> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_broker_dns_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-broker-dns-timeout-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_broker_dns_timeout_ms.map_or_else(
        || Ok(ClientDnsTimeout::default()),
        |milliseconds| {
            ClientDnsTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
                })
        },
    )
}
```

Immediately after `Cli::parse()` and before telemetry initialization, resolve:

```rust
let streams_broker_dns_timeout =
    effective_streams_broker_dns_timeout(&cli)?;
```

- [ ] **Step 6: Forward only to the Stream role**

Change:

```rust
Role::Stream => run_stream(&cli, streams_broker_dns_timeout).await?,
```

Update the function:

```rust
async fn run_stream(
    cli: &Cli,
    broker_dns_timeout: ClientDnsTimeout,
) -> Result<(), BoxError> {
```

and add to the `StreamsApp` builder:

```rust
.broker_dns_timeout(broker_dns_timeout)
```

Do not apply this value to the demo's independent Produce or Consume clients.

- [ ] **Step 7: Add the Compose pass-through**

Under only the `demo-stream` service environment, add:

```yaml
CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS: "${CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS:-10000}"
```

Do not add it to `demo-produce`, `demo-consume`, or shared environment anchors.

- [ ] **Step 8: Run focused and complete demo gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --bin observability-demo-app --locked \
  streams_broker_dns_timeout

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test streams_dns_config --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test observability_demo_config --locked \
  streams_dns_timeout_is_configurable_only_on_the_stream_role

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --all-targets --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
  -p observability-demo-app --all-targets --locked -- -D warnings

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
  -p observability-demo-app --locked -- --help |
  rg -- '--streams-broker-dns-timeout-ms'

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
```

Expected: all tests and Clippy pass; help contains the exact flag once.

- [ ] **Step 9: Commit only the demo boundary**

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/streams_dns_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git diff --cached --check
git commit -m "feat(demo): expose Streams DNS timeout"
```

---

### Task 3: Audit Evidence, Whole-Slice Review, and Publication

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: Tasks 1-2 complete process path.
- Produces: an auditable closure record for Client Streams broker DNS and the
  next unresolved owner; it does not close the repository-wide goal.

- [ ] **Step 1: Run the runtime-value scanner**

```bash
tools/audit-runtime-values.sh
```

Record exact line and distinct-file totals from the current scanner stream.

- [ ] **Step 2: Classify the exact focused search**

```bash
rg -n \
  "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|streams-broker-dns-timeout|STREAMS_BROKER_DNS_TIMEOUT|broker_dns_timeout" \
  crates/client-streams crates/observability-demo-app demo/observability \
  docs/configuration-audit.md
```

Classify every match as production, demo deployment, test/harness, prior audit
evidence, completed downstream policy, or unresolved owner. Record exact line
and distinct-file totals. Confirm that both prior direct lookups are bounded
and name the next coherent unresolved owner from current evidence.

- [ ] **Step 3: Append the audit section**

Append `## Client Streams Broker DNS Timeout` to
`docs/configuration-audit.md`. Record:

- exact library, CLI, environment, and Compose names;
- the 10,000-ms default and CLI > environment > default precedence;
- the complete `StreamsApp -> KafkaStreams -> broker I/O / membership` flow;
- ALO/EOS, metadata, raw fetch, producer, offsets, join, and heartbeat coverage;
- validation and error behavior;
- preserved ordering, first-address, security, TCP/request, and protocol policy;
- scanner and focused-search totals;
- Task 1-2 verification evidence;
- other Client Streams values and the repository-wide goal remain open.

- [ ] **Step 4: Run fresh final verification on the exact head**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams \
  -p observability-demo-app \
  --all-targets --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
  -p crabka-client-streams \
  -p observability-demo-app \
  --all-targets --locked -- -D warnings

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
  -p observability-demo-app --locked -- --help |
  rg -- '--streams-broker-dns-timeout-ms'

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
```

Expected: all commands pass and help contains the exact flag once.

- [ ] **Step 5: Commit only audit evidence**

```bash
git add -- docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(streams): record broker DNS timeout"
```

- [ ] **Step 6: Freeze and review the complete implementation diff**

Freeze the diff from the plan commit through Task 3. Review against the
approved design:

- one typed value reaches every Streams-owned broker DNS path;
- both raw lookups have deterministic deadlines and contextual errors;
- exact public builder, demo CLI/environment, and Compose surfaces;
- CLI > environment > default precedence and Stream-only early validation;
- 10,000-ms default and existing builder compatibility;
- ALO/EOS, security, ordering, first-address, TCP/request, fetch, producer,
  membership, and schema-registry behavior remain intact;
- no new dependency, policy type, resolver trait, CRD, or unrelated tuning;
- tests prove deadline behavior and configuration propagation.

Resolve every Critical and Important finding, rerun affected gates, and repeat
review until clean. Fix convenient documentation-only Minor findings; ledger
any remaining non-blocking Minor finding explicitly.

- [ ] **Step 7: Publish to existing draft PR #904**

Confirm `git status -sb`, exact commits, exact file scope, `gh auth status`,
and branch `configuration_expose`. Push normally; do not force-push. Verify:

```bash
git rev-parse HEAD
git ls-remote origin refs/heads/configuration_expose
```

Require local HEAD, remote branch, and PR #904 `head_sha` to match. Require PR
#904 to remain open, draft, and mergeable.

- [ ] **Step 8: Continue the repository-wide audit**

Do not mark the persistent goal complete. Remove only this plan's exact SDD
artifact directory after publication, preserve the host-owned worktree, and
start a new design cycle for the next coherent unresolved operational owner
identified in Step 2.
