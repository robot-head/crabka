# Observability Demo Consumer Metadata Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the classic Consumer subscribed-topic metadata refresh interval through the observability demo's Consume-role CLI, environment, and Compose configuration.

**Architecture:** Reuse `ConsumerSubscriptionMetadataRefreshInterval` as the only validated policy type. Parse one optional nonzero millisecond value with Clap, resolve it before telemetry or I/O, and pass it only through `run_consume` to the existing raw Consumer builder setter.

**Tech Stack:** Rust 2024, Clap derive/environment parsing, `NonZeroU64`, the existing refined Client Consumer type, Docker Compose, Cargo, Clippy, rustfmt, ripgrep.

## Global Constraints

- Exact CLI name:
  `--consumer-subscription-metadata-refresh-interval-ms`.
- Exact environment name:
  `CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS`.
- Precedence is CLI over environment over
  `ConsumerSubscriptionMetadataRefreshInterval::default()`.
- Preserve the exact default of `5_000` milliseconds.
- Store the raw demo input as `Option<NonZeroU64>`.
- Resolve to `ConsumerSubscriptionMetadataRefreshInterval` before telemetry
  initialization, admin-server startup, DNS, or broker I/O.
- Reject an explicit value on Produce or Stream with `InvalidInput`; include
  the exact flag, supplied millisecond value, and required Consume role.
- Pass the typed value only through `run_consume`, then call
  `Consumer::builder().subscription_metadata_refresh_interval(value.duration())`.
- Configure only the `demo-consume` Compose service, using `${...:-5000}`.
- Add no CRD or operator field: the operator does not own this standalone demo
  process.
- Do not change the library type, default, validation, builder API, heartbeat
  wakeups, elapsed-time boundary, refresh failures, or rejoin behavior.
- Add no policy object, demo-specific newtype, dependency, Cargo feature, or
  lockfile change.
- Keep the separately queued `ShareAcquireMode::BatchOptimized` slice open.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Pass `--locked` to every lock-aware Cargo command.
- Follow TDD: observe the intended failure before production implementation.
- Preserve and never stage unrelated dirty or untracked workspace files.

## File Map

- `crates/observability-demo-app/src/main.rs`: CLI/environment input, typed
  resolver, early role validation, and Consume-role forwarding.
- `crates/observability-demo-app/tests/consumer_subscription_metadata_refresh_config.rs`:
  hermetic subprocess proof of environment use, CLI precedence, early role
  rejection, zero rejection, and help output.
- `crates/observability-demo-app/tests/observability_demo_config.rs`: Compose
  ownership and default assertion.
- `demo/observability/docker-compose.yml`: Consume-only environment
  pass-through.
- `docs/configuration-audit.md`: completed owner, exact scan evidence, and
  remaining adjacent policy.

---

### Task 1: Expose and route the Consume-role setting

**Files:**

- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/consumer_subscription_metadata_refresh_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**

- Consumes:
  `crabka_client_consumer::ConsumerSubscriptionMetadataRefreshInterval`.
- Produces:
  `Cli::consumer_subscription_metadata_refresh_interval_ms: Option<NonZeroU64>`.
- Produces:
  `effective_consumer_subscription_metadata_refresh_interval(&Cli) -> std::io::Result<ConsumerSubscriptionMetadataRefreshInterval>`.
- Produces:
  `--consumer-subscription-metadata-refresh-interval-ms`.
- Produces:
  `CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS`.
- Routes:
  `main -> run_consume -> Consumer::builder().subscription_metadata_refresh_interval(Duration)`.

- [ ] **Step 1: Add failing typed-resolver tests**

In the existing `main.rs` test module, add:

```rust
#[test]
fn consumer_subscription_metadata_refresh_uses_default_and_override() {
    let defaults = Cli::try_parse_from([
        "observability-demo-app",
        "--role",
        "consume",
    ])
    .expect("default CLI");
    assert_eq!(
        effective_consumer_subscription_metadata_refresh_interval(&defaults)
            .expect("typed default")
            .milliseconds(),
        5_000
    );

    let overridden = Cli::try_parse_from([
        "observability-demo-app",
        "--role",
        "consume",
        "--consumer-subscription-metadata-refresh-interval-ms",
        "37",
    ])
    .expect("override CLI");
    assert_eq!(
        effective_consumer_subscription_metadata_refresh_interval(&overridden)
            .expect("typed override")
            .milliseconds(),
        37
    );
}
```

This proves the omitted value stays at the typed `5_000` millisecond default
and a distinctive valid override reaches the resolver unchanged.

- [ ] **Step 2: Add failing hermetic subprocess tests**

Create
`crates/observability-demo-app/tests/consumer_subscription_metadata_refresh_config.rs`:

```rust
use std::process::Command;

fn demo() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command.env_clear();
    command
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS",
            "37",
        )
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--consumer-subscription-metadata-refresh-interval-ms (37 ms) is only valid with --role consume"
    ));

    let cli = demo()
        .args([
            "--role",
            "stream",
            "--consumer-subscription-metadata-refresh-interval-ms",
            "41",
        ])
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS",
            "37",
        )
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--consumer-subscription-metadata-refresh-interval-ms (41 ms) is only valid with --role consume"
    ));
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "consume"])
        .env(
            "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS",
            "0",
        )
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| {
                *token == "--consumer-subscription-metadata-refresh-interval-ms"
            })
            .count(),
        1
    );
}
```

- [ ] **Step 3: Add the failing Compose ownership assertion**

Append to `observability_demo_config.rs`:

```rust
#[test]
fn consumer_metadata_refresh_is_configurable_only_on_the_consume_role() {
    let compose = docker_compose();
    let consume = compose_service_block(&compose, "demo-consume");
    assert2::assert!(consume.contains(
        "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS: \"${CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:-5000}\""
    ));
    for service in ["demo-produce", "demo-stream"] {
        assert2::assert!(
            !compose_service_block(&compose, service).contains(
                "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS"
            )
        );
    }
}
```

- [ ] **Step 4: Run focused tests and verify the red state**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app consumer_subscription_metadata_refresh --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config consumer_metadata_refresh_is_configurable_only_on_the_consume_role --locked
```

Expected: the first command fails to compile because the resolver and CLI
field do not exist; the Compose test fails because `demo-consume` lacks the
environment variable.

- [ ] **Step 5: Add the option and early typed resolver**

Extend the existing Client Consumer import:

```rust
use crabka_client_consumer::{
    Consumer, ConsumerLeaveGroupTimeout, ConsumerRecord,
    ConsumerSubscriptionMetadataRefreshInterval,
};
```

Add immediately after `consumer_leave_group_timeout_ms` in `Cli`:

```rust
/// Classic Consumer subscribed-topic metadata refresh interval in milliseconds.
#[arg(
    long,
    env = "CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS"
)]
consumer_subscription_metadata_refresh_interval_ms: Option<NonZeroU64>,
```

Add beside `effective_consumer_leave_group_timeout`:

```rust
fn effective_consumer_subscription_metadata_refresh_interval(
    cli: &Cli,
) -> std::io::Result<ConsumerSubscriptionMetadataRefreshInterval> {
    if cli.role != Role::Consume
        && let Some(milliseconds) =
            cli.consumer_subscription_metadata_refresh_interval_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--consumer-subscription-metadata-refresh-interval-ms ({} ms) is only valid with --role consume",
                milliseconds.get(),
            ),
        ));
    }

    cli.consumer_subscription_metadata_refresh_interval_ms
        .map_or_else(
            || Ok(ConsumerSubscriptionMetadataRefreshInterval::default()),
            |milliseconds| {
                ConsumerSubscriptionMetadataRefreshInterval::new(
                    Duration::from_millis(milliseconds.get()),
                )
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
                })
            },
        )
}
```

Add this field to every existing direct `Cli` test literal:

```rust
consumer_subscription_metadata_refresh_interval_ms: None,
```

Immediately after resolving `consumer_leave_group_timeout` in `main`, resolve
the new typed value before telemetry:

```rust
let consumer_subscription_metadata_refresh_interval =
    effective_consumer_subscription_metadata_refresh_interval(&cli)?;
```

- [ ] **Step 6: Route the typed value only through Consume**

Change the Consume match arm to:

```rust
Role::Consume => {
    run_consume(
        &cli,
        &metrics,
        consumer_leave_group_timeout,
        consumer_subscription_metadata_refresh_interval,
    )
    .await?;
}
```

Extend `run_consume`:

```rust
async fn run_consume(
    cli: &Cli,
    metrics: &DemoMetrics,
    consumer_leave_group_timeout: ConsumerLeaveGroupTimeout,
    consumer_subscription_metadata_refresh_interval:
        ConsumerSubscriptionMetadataRefreshInterval,
) -> Result<(), BoxError> {
```

Add the existing raw builder setter after `leave_group_timeout`:

```rust
.subscription_metadata_refresh_interval(
    consumer_subscription_metadata_refresh_interval.duration(),
)
```

Do not pass the value to Produce or Stream.

- [ ] **Step 7: Add only the Consume Compose variable**

Under `demo-consume.environment`, immediately after the leave-group timeout,
add:

```yaml
CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS: "${CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:-5000}"
```

- [ ] **Step 8: Run focused green tests and package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app consumer_subscription_metadata_refresh --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test consumer_subscription_metadata_refresh_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config consumer_metadata_refresh_is_configurable_only_on_the_consume_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build -p observability-demo-app --bin observability-demo-app --locked
./target/debug/observability-demo-app --help | grep -o -- '--consumer-subscription-metadata-refresh-interval-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all focused and all-target tests pass; strict Clippy, formatting,
and diff hygiene pass; help count is exactly `1`; `Cargo.lock` has no diff.

- [ ] **Step 9: Commit Task 1**

Stage only the four Task 1 files:

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/consumer_subscription_metadata_refresh_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git diff --cached --check
git commit -m "feat(demo): expose metadata refresh"
```

Expected: the commit contains only the demo CLI/environment propagation,
Consume-only forwarding, Compose ownership, and focused tests.

---

### Task 2: Record the completed owner and verify the slice

**Files:**

- Modify: `docs/configuration-audit.md`

**Interfaces:**

- Consumes: the completed demo configuration flow from Task 1 and
  `tools/audit-runtime-values.sh`.
- Produces: an exclusive focused-search classification, explicit closure of
  the observability-demo owner, and the still-open
  `ShareAcquireMode::BatchOptimized` slice.

- [ ] **Step 1: Run the repository scanner and focused search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "consumer_subscription_metadata_refresh|ConsumerSubscriptionMetadataRefreshInterval|DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL|consumer-subscription-metadata-refresh-interval-ms|CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS|ShareAcquireMode|BatchOptimized" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_integration.rs \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

Record the exact scanner line/file totals. Classify every focused-search line
exactly once as classic Consumer production, demo production policy, demo
deployment, test or harness, prior audit, parked acquisition-mode policy, or
unresolved owner. Verify that category totals equal the focused-search total.

- [ ] **Step 2: Append the completed audit section**

Append `## Observability Demo Consumer Metadata Refresh` to
`docs/configuration-audit.md`. Include:

- the exact CLI and environment names;
- CLI-over-environment-over-typed-default precedence;
- the positive whole-millisecond input and exact `5_000` millisecond default;
- the pre-telemetry/pre-I/O role and typed-validation boundary;
- the exact `Cli -> resolver -> main -> run_consume -> Consumer::builder()`
  data flow;
- Compose ownership only on `demo-consume`, with `${...:-5000}`;
- why no CRD or operator field exists;
- unchanged library and runtime behavior;
- exact scanner and focused-search commands and exclusive classifications;
- verification results from Task 1; and
- `### Adjacent Pending Policy`, keeping other classic Consumer production
  owners and the `ShareAcquireMode::BatchOptimized` slice open.

Do not claim repository-wide completion.

- [ ] **Step 3: Run fresh final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build -p observability-demo-app --bin observability-demo-app --locked
./target/debug/observability-demo-app --help | grep -o -- '--consumer-subscription-metadata-refresh-interval-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all demo targets pass; strict Clippy, formatting, and diff hygiene
pass; help count is exactly `1`; `Cargo.lock` has no diff.

- [ ] **Step 4: Commit Task 2**

Stage only the audit:

```bash
git add -- docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(demo): record metadata refresh"
```

Expected: the commit contains only the completed owner audit and remaining
scope.

- [ ] **Step 5: Verify commit and workspace boundaries**

Run:

```bash
git diff --check HEAD~2..HEAD
git diff --stat HEAD~2..HEAD
git diff -- Cargo.lock
git status --short
```

Expected: the two implementation commits contain only the five planned files,
`Cargo.lock` has no diff, and every pre-existing unrelated dirty or untracked
file remains unstaged and unmodified by this plan.
