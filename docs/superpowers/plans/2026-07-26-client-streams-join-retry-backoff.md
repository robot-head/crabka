# Client Streams Join Retry Backoff Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed 200-ms Client Streams initial-join retry sleep with validated application configuration while preserving its current behavior by default.

**Architecture:** Add one public `StreamsJoinRetryBackoff` semantic type at the membership owner, retain raw-`Duration` compatibility at the public low-level builders, and validate before schema prewarm or broker I/O. Carry the typed value through `StreamsApp`; expose it only on the observability demo Stream role through Clap CLI/environment precedence and the Stream Compose service.

**Tech Stack:** Rust, `refined_type`, Bon builders, Tokio, Clap, Docker Compose YAML, Cargo tests, Clippy, rustfmt, ripgrep.

## Global Constraints

- Preserve the exact 200-ms default and fixed-delay retry behavior.
- Use `refined_type` for the new validated newtype.
- Accept only positive, exact whole-millisecond values representable as `u64`.
- Retain `Duration` inputs on public `KafkaStreams` and `StreamsMembership` builders.
- Validate `KafkaStreams` before broker construction and `StreamsMembership` before schema prewarm or broker construction.
- Retry only `COORDINATOR_LOAD_IN_PROGRESS`; add no jitter, exponential backoff, retry limit, or new retryable response.
- Use `--streams-join-retry-backoff-ms` and `CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`.
- Preserve CLI over environment over typed-default precedence.
- Resolve and validate demo configuration before telemetry or external I/O.
- Expose the deployment variable only on `demo-stream`, defaulting to `200`.
- Add no CRD: the operator does not own or render a Client Streams workload.
- Do not make the broker-provided heartbeat interval or its fixed 3-second invalid-response fallback tunable.
- Add no generic duration abstraction, retry-policy object, macro, profile, or cross-field rule.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate and route the Client Streams join retry backoff

**Files:**
- Modify: `crates/client-streams/src/membership/client.rs`
- Modify: `crates/client-streams/src/membership/mod.rs`
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/streams_app.rs`
- Modify: `crates/client-streams/src/lib.rs`

**Interfaces:**
- Produces: `pub const DEFAULT_STREAMS_JOIN_RETRY_BACKOFF: Duration`
- Produces: `pub struct StreamsJoinRetryBackoff(Duration)`
- Produces: `StreamsJoinRetryBackoff::new(Duration) -> Result<Self, String>`
- Produces: `StreamsJoinRetryBackoff::duration(self) -> Duration`
- Produces: `StreamsJoinRetryBackoff::milliseconds(self) -> u64`
- Produces: defaulted `join_retry_backoff: Duration` inputs on `KafkaStreams` and `StreamsMembership`
- Produces: defaulted typed `join_retry_backoff: StreamsJoinRetryBackoff` on `StreamsApp`
- Consumes: existing `COORDINATOR_LOAD_IN_PROGRESS` branch and existing `StreamsClientError::Runtime`

- [ ] **Step 1: Add failing membership type and retry-path tests**

In `membership/client.rs`, extend the test import and add:

```rust
use super::{
    StreamsJoinRetryBackoff, StreamsRebalanceTimeout, build_join_heartbeat,
    heartbeat_interval, join_retry_delay, map_error, should_emit_statuses,
};

#[test]
fn join_retry_backoff_uses_default_and_valid_override() {
    let default = StreamsJoinRetryBackoff::default();
    check!(default.duration() == Duration::from_millis(200));
    check!(default.milliseconds() == 200);

    let backoff = StreamsJoinRetryBackoff::new(Duration::from_millis(37))
        .expect("positive whole milliseconds");
    check!(backoff.duration() == Duration::from_millis(37));
    check!(backoff.milliseconds() == 37);
}

#[test]
fn join_retry_backoff_rejects_zero_and_fractional_milliseconds() {
    check!(StreamsJoinRetryBackoff::new(Duration::ZERO).is_err());
    check!(
        StreamsJoinRetryBackoff::new(
            Duration::from_millis(1) + Duration::from_nanos(1)
        )
        .is_err()
    );
}

#[test]
fn join_retry_path_uses_configured_backoff_only_while_coordinator_loads() {
    let backoff = StreamsJoinRetryBackoff::new(Duration::from_millis(37))
        .expect("positive whole milliseconds");
    check!(
        join_retry_delay(COORDINATOR_LOAD_IN_PROGRESS, backoff)
            == Some(Duration::from_millis(37))
    );
    check!(join_retry_delay(0, backoff).is_none());
    check!(join_retry_delay(15, backoff).is_none());
}
```

Import `COORDINATOR_LOAD_IN_PROGRESS` into the test module's `super` list so
the test uses the production response code.

- [ ] **Step 2: Add failing low-level and high-level ownership tests**

Extend `runtime/app.rs::tests::low_level_runtime_validation_names_the_invalid_field`
with a fourth `Duration` argument on every existing call. Use
`DEFAULT_STREAMS_JOIN_RETRY_BACKOFF` for valid cases, then add:

```rust
use super::{
    KafkaStreams, StreamsCommitInterval, StreamsPollInterval,
    validate_runtime_configuration,
};
use crate::{
    membership::DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
    topology::Topology,
};
```

```rust
let join_retry_error = validate_runtime_configuration(
    Duration::from_millis(200),
    Duration::from_secs(5),
    Duration::from_secs(30),
    Duration::ZERO,
)
.expect_err("zero join retry backoff");
assert2::assert!(
    join_retry_error
        .to_string()
        .contains("streams join retry backoff")
);
```

Import `KafkaStreams` and `crate::topology::Topology`, then add the direct
boundary test:

```rust
#[tokio::test]
async fn invalid_join_retry_backoff_fails_before_broker_lookup() {
    let mut topology = Topology::new();
    let source = topology.add_source::<String, String>("source", ["input"]);
    topology.add_sink("sink", "output", [&source]);
    let topology = topology.build("join-retry-validation").expect("topology");

    let error = KafkaStreams::builder()
        .bootstrap("invalid.invalid:9092")
        .application_id("join-retry-validation")
        .topology(topology)
        .join_retry_backoff(Duration::ZERO)
        .build()
        .await
        .err()
        .expect("invalid configuration");

    assert2::assert!(
        error
            .to_string()
            .contains("streams join retry backoff")
    );
}
```

Add this test to `streams_app.rs`:

```rust
#[test]
fn join_retry_backoff_uses_typed_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("join-retry-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.join_retry_backoff,
        crate::StreamsJoinRetryBackoff::default()
    );

    let backoff =
        crate::StreamsJoinRetryBackoff::new(std::time::Duration::from_millis(37))
            .expect("positive join retry backoff");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("join-retry-override")
        .schema_registry("http://127.0.0.1:8081")
        .join_retry_backoff(backoff)
        .build();
    assert_eq!(overridden.join_retry_backoff, backoff);
}
```

- [ ] **Step 3: Run the focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams join_retry_backoff --locked
```

Expected: compilation fails because `StreamsJoinRetryBackoff`,
`join_retry_delay`, the fourth validation input, and the `StreamsApp` field do
not exist.

- [ ] **Step 4: Implement the semantic type and exact retry-path seam**

In `membership/client.rs`, add beside `StreamsRebalanceTimeout`:

```rust
/// Default delay between Client Streams initial join retries.
pub const DEFAULT_STREAMS_JOIN_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Positive, whole-millisecond delay between Client Streams initial join retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsJoinRetryBackoff(Duration);

impl StreamsJoinRetryBackoff {
    /// Validate a Client Streams initial join retry backoff.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("streams join retry backoff: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("streams join retry backoff: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "streams join retry backoff must be a whole number of milliseconds".to_owned(),
            );
        }
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the validated duration no longer fits in `u64` milliseconds.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated streams join retry backoff fits u64")
    }
}

impl Default for StreamsJoinRetryBackoff {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_JOIN_RETRY_BACKOFF)
            .expect("default streams join retry backoff is valid")
    }
}

fn join_retry_delay(
    error_code: i16,
    backoff: StreamsJoinRetryBackoff,
) -> Option<Duration> {
    (error_code == COORDINATOR_LOAD_IN_PROGRESS).then(|| backoff.duration())
}
```

Add a defaulted `join_retry_backoff: Duration` input to
`StreamsMembership::start`, validate it immediately after the empty-group
check and before `schema_prewarm`, then replace the literal branch with:

```rust
if let Some(delay) = join_retry_delay(resp.error_code, join_retry_backoff) {
    tokio::time::sleep(delay).await;
    continue;
}
```

- [ ] **Step 5: Route validation through `KafkaStreams`**

In `runtime/app.rs`:

- import `DEFAULT_STREAMS_JOIN_RETRY_BACKOFF` and
  `StreamsJoinRetryBackoff`;
- add `join_retry_backoff: Duration` to `validate_runtime_configuration`;
- return `StreamsJoinRetryBackoff` as the fourth tuple element;
- construct it with a field-specific `StreamsClientError::Runtime`;
- add a defaulted raw-`Duration` `join_retry_backoff` builder input;
- pass it into early validation; and
- forward `join_retry_backoff.duration()` to `StreamsMembership`.

The validation result must have this shape:

```rust
let (poll_interval, commit_interval, rebalance_timeout, join_retry_backoff) =
    validate_runtime_configuration(
        poll_interval,
        commit_interval,
        rebalance_timeout,
        join_retry_backoff,
    )?;
```

- [ ] **Step 6: Route the typed value through `StreamsApp` and exports**

In `streams_app.rs`, add the import, stored field, defaulted typed builder
input, constructor assignment, and this forwarding call:

```rust
.join_retry_backoff(self.join_retry_backoff.duration())
```

Re-export `DEFAULT_STREAMS_JOIN_RETRY_BACKOFF` and
`StreamsJoinRetryBackoff` from `membership/mod.rs` and the crate root.

- [ ] **Step 7: Run focused GREEN and compatibility gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams join_retry --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams low_level_runtime_validation_names_the_invalid_field --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; the lockfile diff is empty. Existing raw
`Duration` builder callers compile unchanged.

- [ ] **Step 8: Commit Task 1**

Stage only the five Task 1 files and commit:

```bash
git add -- \
  crates/client-streams/src/membership/client.rs \
  crates/client-streams/src/membership/mod.rs \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/streams_app.rs \
  crates/client-streams/src/lib.rs
git commit -m "feat(streams): configure join retry backoff"
```

---

### Task 2: Expose the demo CLI, environment, and Compose setting

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/streams_join_retry_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `StreamsJoinRetryBackoff`
- Produces: `--streams-join-retry-backoff-ms`
- Produces: `CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`
- Produces: typed `StreamsJoinRetryBackoff` passed to `StreamsApp`

- [ ] **Step 1: Add failing hermetic subprocess tests**

Create `tests/streams_join_retry_config.rs`:

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
        .env("CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr).contains(
            "--streams-join-retry-backoff-ms (37 ms) is only valid with --role stream"
        )
    );

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-join-retry-backoff-ms",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr).contains(
            "--streams-join-retry-backoff-ms (41 ms) is only valid with --role stream"
        )
    );
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-join-retry-backoff-ms")
            .count(),
        1
    );
}
```

- [ ] **Step 2: Extend the failing Compose ownership test**

In `streams_runtime_policy_is_configurable_only_on_the_stream_role`, require:

```rust
assert2::assert!(stream.contains(
    "CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS: \"${CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS:-200}\""
));
```

Also assert Produce and Consume service blocks do not contain
`CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`.

- [ ] **Step 3: Run Task 2 tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_join_retry_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
```

Expected: subprocess tests fail because Clap does not know the option; the
Compose test fails because the Stream service lacks the variable.

- [ ] **Step 4: Add the early typed demo resolution**

In `main.rs`:

- import `StreamsJoinRetryBackoff`;
- add this optional `Cli` field:

```rust
/// Client Streams initial join retry backoff in milliseconds.
#[arg(long, env = "CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS")]
streams_join_retry_backoff_ms: Option<NonZeroU64>,
```

- add this resolver:

```rust
fn effective_streams_join_retry_backoff(
    cli: &Cli,
) -> std::io::Result<StreamsJoinRetryBackoff> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_join_retry_backoff_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-join-retry-backoff-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_join_retry_backoff_ms.map_or_else(
        || Ok(StreamsJoinRetryBackoff::default()),
        |milliseconds| {
            StreamsJoinRetryBackoff::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}
```

Resolve it beside the existing Stream settings before telemetry:

```rust
let streams_join_retry_backoff = effective_streams_join_retry_backoff(&cli)?;
```

Pass the typed value through `run_stream` and into:

```rust
.join_retry_backoff(streams_join_retry_backoff)
```

Update every literal `Cli` in unit tests with
`streams_join_retry_backoff_ms: None`. Add:

```rust
#[test]
fn streams_join_retry_backoff_uses_default_and_cli_override() {
    let defaults = Cli {
        role: Role::Stream,
        bootstrap: "127.0.0.1:9092".to_owned(),
        registry: "http://127.0.0.1:8081".to_owned(),
        input_topic: "orders".to_owned(),
        output_topic: "order-counts".to_owned(),
        orders_per_sec: 50,
        streams_broker_dns_timeout_ms: None,
        streams_poll_interval_ms: None,
        streams_commit_interval_ms: None,
        streams_rebalance_timeout_ms: None,
        streams_join_retry_backoff_ms: None,
    };
    assert_eq!(
        effective_streams_join_retry_backoff(&defaults).expect("typed default"),
        StreamsJoinRetryBackoff::default()
    );

    let overridden = Cli {
        streams_join_retry_backoff_ms: NonZeroU64::new(37),
        ..defaults
    };
    assert_eq!(
        effective_streams_join_retry_backoff(&overridden)
            .expect("typed override")
            .milliseconds(),
        37
    );
}
```

- [ ] **Step 5: Add the Stream-only Compose variable**

In the `demo-stream` environment block, add:

```yaml
CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS: "${CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS:-200}"
```

Do not add it to anchors or any other service.

- [ ] **Step 6: Run Task 2 GREEN and full demo gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_join_retry_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q -p observability-demo-app --locked -- --help
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; help contains the flag once and the
lockfile diff is empty.

- [ ] **Step 7: Commit Task 2**

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/streams_join_retry_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose Streams join retry"
```

---

### Task 3: Record audit closure and verify the slice

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: Tasks 1 and 2 committed behavior
- Produces: reproducible scanner totals, exclusive focused-reference classification, gate evidence, and one concrete next unresolved operational owner

- [ ] **Step 1: Capture the broad scanner**

Run:

```bash
tools/audit-runtime-values.sh > /tmp/client-streams-join-retry-runtime-audit.txt
wc -l /tmp/client-streams-join-retry-runtime-audit.txt
cut -d: -f1 /tmp/client-streams-join-retry-runtime-audit.txt | sort -u | wc -l
```

Record the exact line and distinct-file totals before appending the audit
section.

- [ ] **Step 2: Capture and classify every focused reference**

Run:

```bash
rg -n \
  "join_retry_backoff|StreamsJoinRetryBackoff|DEFAULT_STREAMS_JOIN_RETRY_BACKOFF|streams-join-retry-backoff|STREAMS_JOIN_RETRY_BACKOFF|COORDINATOR_LOAD_IN_PROGRESS" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md \
  > /tmp/client-streams-join-retry-focused.txt
wc -l /tmp/client-streams-join-retry-focused.txt
cut -d: -f1 /tmp/client-streams-join-retry-focused.txt | sort -u | wc -l
```

Classify every line exactly once as Client Streams production, demo policy,
demo deployment, test/harness, prior audit, or unresolved owner. Verify the
category sum equals the focused line count.

- [ ] **Step 3: Select the next real operational owner**

Inspect the remaining production candidates:

```bash
tools/audit-runtime-values.sh \
  | rg '^crates/client-streams/src/' \
  | rg -v \
    'join_retry_backoff|StreamsJoinRetryBackoff|DEFAULT_STREAMS_JOIN_RETRY_BACKOFF|COORDINATOR_LOAD_IN_PROGRESS|heartbeat_interval|from_secs\(3\)|dsl/names.rs|murmur3.rs|subscription.rs|_schema.rs|suppress_bufval.rs'
```

Name the first coherent runtime-owned operational setting supported by an
actual production consumer. Do not classify protocol identifiers, wire-format
sizes, algorithm constants, topology names, test values, or the broker-derived
heartbeat fallback as configuration.

- [ ] **Step 4: Append the audit section**

Append `## Client Streams Join Retry Backoff` to
`docs/configuration-audit.md`. Record:

- the public type, validation bounds, and 200-ms default;
- both low-level early-validation boundaries;
- the exact `StreamsApp` → `KafkaStreams` → `StreamsMembership` → retry-sleep
  flow;
- fixed-delay and response-code semantics;
- CLI/environment precedence and Stream-only Compose ownership;
- why there is no CRD;
- exact scanner and focused-search totals and exclusive classifications;
- Task 1, Task 2, and final verification results;
- unchanged protocol invariants; and
- the concrete next unresolved operational owner selected in Step 3.

- [ ] **Step 5: Run fresh combined completion gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Also run:

```bash
help=$(
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
    cargo run -q -p observability-demo-app --locked -- --help
)
test "$(
  grep -o -- '--streams-join-retry-backoff-ms' <<<"$help" | wc -l
)" -eq 1
```

Expected: all commands pass and `Cargo.lock` has no diff.

- [ ] **Step 6: Commit Task 3**

```bash
git add -- docs/configuration-audit.md
git commit -m "docs(streams): record join retry backoff"
```

- [ ] **Step 7: Review and publish**

Review the complete implementation range after this plan against the approved
design. Address only concrete correctness, compatibility, validation-order,
configuration-flow, test-isolation, or scope findings. Re-run the combined
completion gates after the final code change, preserve unrelated worktree
files, push `configuration_expose`, and verify that local HEAD, the remote
branch SHA, and pull request #904's head SHA match.
