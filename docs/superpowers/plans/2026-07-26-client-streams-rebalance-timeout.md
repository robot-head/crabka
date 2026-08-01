# Client Streams Rebalance Timeout Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the Client Streams rebalance timeout through validated application and demo configuration while preserving the 30-second default and rejecting values outside the Kafka wire range before external I/O.

**Architecture:** Add one public `StreamsRebalanceTimeout` refined newtype at the membership owner boundary. Keep the public `Duration` inputs on `KafkaStreams` and `StreamsMembership`, validate both entry points before external work, store the typed value in `StreamsApp`, and expose one Stream-role-only demo CLI/environment setting.

**Tech Stack:** Rust, `refined_type`, Bon builders, Clap environment parsing, Tokio, Docker Compose, Cargo tests, Clippy.

## Global Constraints

- Preserve the exact 30-second default.
- Accept only positive, whole-millisecond values no greater than `i32::MAX` milliseconds.
- Keep the existing public `Duration` inputs on `KafkaStreams` and `StreamsMembership`.
- Validate `KafkaStreams` before broker client construction.
- Validate `StreamsMembership` before schema prewarming or broker client construction.
- Keep the broker-provided heartbeat interval and fixed 3-second invalid-response fallback unchanged.
- Demo precedence is CLI over environment over the typed 30,000 ms default.
- Demo configuration is valid only for `--role stream`.
- Do not add a generic duration abstraction, macro, profile, cross-field rule, CRD, or unrelated timing configuration.
- Do not add dependencies or change `Cargo.lock`; `crabka-client-streams` already directly depends on workspace `refined_type`.
- Every Cargo command must set `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must use `--locked`.
- Preserve unrelated dirty and untracked files.

---

### Task 1: Validate and Route the Membership Rebalance Timeout

**Files:**
- Modify: `crates/client-streams/src/membership/client.rs:10-175`
- Modify: `crates/client-streams/src/membership/client.rs:311-370`
- Modify: `crates/client-streams/src/membership/coordinator.rs:403-440`
- Modify: `crates/client-streams/src/membership/mod.rs:9-12`
- Modify: `crates/client-streams/src/runtime/app.rs:14-190`
- Modify: `crates/client-streams/src/runtime/app.rs:229-236`
- Modify: `crates/client-streams/src/runtime/app.rs:432-474`
- Modify: `crates/client-streams/src/lib.rs:914-929`

**Interfaces:**
- Consumes: existing public `Duration` builder inputs and Kafka `StreamsGroupHeartbeatRequest::rebalance_timeout_ms: i32`.
- Produces: `pub const DEFAULT_STREAMS_REBALANCE_TIMEOUT: Duration`, `pub struct StreamsRebalanceTimeout(Duration)`, `StreamsRebalanceTimeout::new(Duration) -> Result<Self, String>`, `duration(self) -> Duration`, and `milliseconds(self) -> i32`.
- Produces: `KafkaStreams::builder().rebalance_timeout(Duration)` while retaining the existing `StreamsMembership::builder().rebalance_timeout(Duration)`.

- [ ] **Step 1: Write failing validated-type and membership request tests**

In `membership/client.rs`, import the new symbols into the test module and add:

```rust
#[test]
fn rebalance_timeout_uses_default_and_valid_override() {
    let default = StreamsRebalanceTimeout::default();
    check!(default.duration() == Duration::from_secs(30));
    check!(default.milliseconds() == 30_000);

    let timeout = StreamsRebalanceTimeout::new(Duration::from_millis(45_000))
        .expect("valid rebalance timeout");
    check!(timeout.duration() == Duration::from_secs(45));
    check!(timeout.milliseconds() == 45_000);
}

#[test]
fn rebalance_timeout_rejects_invalid_wire_values() {
    check!(StreamsRebalanceTimeout::new(Duration::ZERO).is_err());
    check!(
        StreamsRebalanceTimeout::new(Duration::from_millis(1) + Duration::from_nanos(1)).is_err()
    );
    check!(
        StreamsRebalanceTimeout::new(Duration::from_millis(
            u64::try_from(i32::MAX).expect("i32 max fits u64") + 1,
        ))
        .is_err()
    );
}
```

Keep `build_join_heartbeat_preserves_join_identity_and_topology` asserting
`req.rebalance_timeout_ms == 45_000`; this remains the initial-request
propagation proof.

In `membership/coordinator.rs`, add the subsequent-heartbeat proof:

```rust
#[tokio::test]
async fn heartbeat_uses_configured_rebalance_timeout() {
    let fake = FakeTransport::new(vec![ok_resp(9, vec![0])]);
    let sent = fake.sent_arc();
    let (mut state, _rx) = state_with(fake);
    state.rebalance_timeout_ms = 45_000;

    check!(matches!(heartbeat_once(&state, false).await, Outcome::Ok));
    check!(sent.lock().unwrap()[0].rebalance_timeout_ms == 45_000);
}
```

- [ ] **Step 2: Write the failing low-level runtime validation test**

Replace the runtime test helper import with
`validate_runtime_configuration`, then add the rebalance parameter to the
existing helper calls and assert its field-specific error:

```rust
#[test]
fn low_level_runtime_validation_names_the_invalid_field() {
    let poll_error = validate_runtime_configuration(
        Duration::ZERO,
        Duration::from_secs(5),
        Duration::from_secs(30),
    )
    .expect_err("zero poll interval");
    assert2::assert!(poll_error.to_string().contains("streams poll interval"));

    let commit_error = validate_runtime_configuration(
        Duration::from_millis(200),
        Duration::ZERO,
        Duration::from_secs(30),
    )
    .expect_err("zero commit interval");
    assert2::assert!(commit_error.to_string().contains("streams commit interval"));

    let rebalance_error = validate_runtime_configuration(
        Duration::from_millis(200),
        Duration::from_secs(5),
        Duration::from_millis(
            u64::try_from(i32::MAX).expect("i32 max fits u64") + 1,
        ),
    )
    .expect_err("rebalance timeout outside Kafka wire range");
    assert2::assert!(
        rebalance_error
            .to_string()
            .contains("streams rebalance timeout")
    );
}
```

- [ ] **Step 3: Run the focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams rebalance_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams low_level_runtime_validation_names_the_invalid_field --locked
```

Expected: compilation fails because `StreamsRebalanceTimeout`,
`DEFAULT_STREAMS_REBALANCE_TIMEOUT`, the runtime builder parameter, and
`validate_runtime_configuration` do not exist.

- [ ] **Step 4: Add the minimal refined newtype at the membership owner**

In `membership/client.rs`, import `refined_type::rule::MinMaxU128` and add:

```rust
/// Default Client Streams rebalance timeout.
pub const DEFAULT_STREAMS_REBALANCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Positive, whole-millisecond rebalance timeout representable on the Kafka wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsRebalanceTimeout(Duration);

impl StreamsRebalanceTimeout {
    /// Validate a Client Streams rebalance timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value greater
    /// than `i32::MAX` milliseconds.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds =
            MinMaxU128::<1, { i32::MAX as u128 }>::new(value.as_millis())
                .map_err(|error| format!("streams rebalance timeout: {error}"))?
                .into_value();
        let milliseconds = i32::try_from(milliseconds)
            .map_err(|error| format!("streams rebalance timeout: {error}"))?;
        let whole = Duration::from_millis(
            u64::try_from(milliseconds).expect("positive i32 milliseconds fit u64"),
        );
        if whole != value {
            return Err(
                "streams rebalance timeout must be a whole number of milliseconds".to_owned(),
            );
        }
        Ok(Self(value))
    }

    #[must_use]
    /// Return the validated duration.
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    /// Return the validated signed wire milliseconds.
    pub fn milliseconds(self) -> i32 {
        i32::try_from(self.0.as_millis())
            .expect("validated streams rebalance timeout fits i32")
    }
}

impl Default for StreamsRebalanceTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_REBALANCE_TIMEOUT)
            .expect("default streams rebalance timeout is valid")
    }
}
```

Change the existing membership builder default to
`DEFAULT_STREAMS_REBALANCE_TIMEOUT`. At the top of `start`, after the empty
group-id guard but before `schema_prewarm`, validate:

```rust
let rebalance_timeout =
    StreamsRebalanceTimeout::new(rebalance_timeout).map_err(StreamsClientError::Runtime)?;
```

Replace the silent conversion with:

```rust
let rebalance_timeout_ms = rebalance_timeout.milliseconds();
```

Do not change `heartbeat_interval`.

- [ ] **Step 5: Validate the low-level runtime before broker setup**

In `runtime/app.rs`, import the membership newtype and default. Rename
`validate_runtime_intervals` to `validate_runtime_configuration`, accept a
third `rebalance_timeout: Duration`, and return the three typed values:

```rust
fn validate_runtime_configuration(
    poll_interval: Duration,
    commit_interval: Duration,
    rebalance_timeout: Duration,
) -> Result<
    (
        StreamsPollInterval,
        StreamsCommitInterval,
        StreamsRebalanceTimeout,
    ),
    StreamsClientError,
> {
    let poll_interval =
        StreamsPollInterval::new(poll_interval).map_err(StreamsClientError::Runtime)?;
    let commit_interval =
        StreamsCommitInterval::new(commit_interval).map_err(StreamsClientError::Runtime)?;
    let rebalance_timeout =
        StreamsRebalanceTimeout::new(rebalance_timeout).map_err(StreamsClientError::Runtime)?;
    Ok((poll_interval, commit_interval, rebalance_timeout))
}
```

Add the compatible builder input:

```rust
#[builder(default = DEFAULT_STREAMS_REBALANCE_TIMEOUT)]
rebalance_timeout: Duration,
```

Call `validate_runtime_configuration` as the first operation in `start`,
before topology wrapping and `io_broker::build`, then forward:

```rust
.rebalance_timeout(rebalance_timeout.duration())
```

to `StreamsMembership::builder()`.

- [ ] **Step 6: Export the public type and default**

Export `StreamsRebalanceTimeout` and
`DEFAULT_STREAMS_REBALANCE_TIMEOUT` from `membership/mod.rs`, then from the
crate root. Do not move the type into the generic runtime interval code and do
not export it through `runtime/mod.rs`.

- [ ] **Step 7: Run focused and package verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams rebalance_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams low_level_runtime_validation_names_the_invalid_field --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
```

Expected: all tests and checks pass. Confirm `Cargo.lock` has no diff and
existing direct `StreamsMembership` and `KafkaStreams` callers still compile.

- [ ] **Step 8: Commit Task 1**

```bash
git add crates/client-streams/src/membership/client.rs \
  crates/client-streams/src/membership/coordinator.rs \
  crates/client-streams/src/membership/mod.rs \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/lib.rs
git commit -m "feat(streams): validate rebalance timeout"
```

---

### Task 2: Expose the Typed App and Demo Configuration

**Files:**
- Modify: `crates/client-streams/src/streams_app.rs:54-124`
- Modify: `crates/client-streams/src/streams_app.rs:171-190`
- Modify: `crates/client-streams/src/streams_app.rs:194-263`
- Modify: `crates/observability-demo-app/src/main.rs:13-180`
- Modify: `crates/observability-demo-app/src/main.rs:305-325`
- Modify: `crates/observability-demo-app/src/main.rs:480-610`
- Create: `crates/observability-demo-app/tests/streams_rebalance_timeout_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs:719-749`
- Modify: `demo/observability/docker-compose.yml:452-460`

**Interfaces:**
- Consumes: Task 1 `StreamsRebalanceTimeout`, its 30-second default, and `KafkaStreams::builder().rebalance_timeout(Duration)`.
- Produces: `StreamsApp::builder().rebalance_timeout(StreamsRebalanceTimeout)`.
- Produces: `--streams-rebalance-timeout-ms` and `CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS`.

- [ ] **Step 1: Write the failing `StreamsApp` ownership test**

Add to `streams_app.rs` tests:

```rust
#[test]
fn rebalance_timeout_uses_typed_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("rebalance-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.rebalance_timeout,
        crate::StreamsRebalanceTimeout::default()
    );

    let timeout =
        crate::StreamsRebalanceTimeout::new(std::time::Duration::from_millis(45_000))
            .expect("valid timeout");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("rebalance-override")
        .schema_registry("http://127.0.0.1:8081")
        .rebalance_timeout(timeout)
        .build();
    assert_eq!(overridden.rebalance_timeout, timeout);
}
```

- [ ] **Step 2: Write failing demo unit and subprocess tests**

Add the `Cli` field to test struct literals as `None`, then add unit tests for
the typed default, a 45,000 ms override, zero rejection, an `i32::MAX + 1`
error containing `streams rebalance timeout`, and Produce-role rejection
containing:

```text
--streams-rebalance-timeout-ms (45000 ms) is only valid with --role stream
```

Create `tests/streams_rebalance_timeout_config.rs` with an isolated command:

```rust
use std::process::Command;

fn demo() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command
        .env_remove("CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS")
        .env_remove("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS")
        .env_remove("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS")
        .env_remove("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS");
    command
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "37000")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr).contains(
            "--streams-rebalance-timeout-ms (37000 ms) is only valid with --role stream"
        )
    );

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-rebalance-timeout-ms",
            "41000",
        ])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "37000")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr).contains(
            "--streams-rebalance-timeout-ms (41000 ms) is only valid with --role stream"
        )
    );
}

#[test]
fn invalid_values_fail_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let overflow = demo()
        .args(["--role", "stream"])
        .env(
            "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS",
            "2147483648",
        )
        .output()
        .expect("run demo");
    assert!(!overflow.status.success());
    assert!(
        String::from_utf8_lossy(&overflow.stderr).contains("streams rebalance timeout")
    );

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-rebalance-timeout-ms")
            .count(),
        1
    );
}
```

- [ ] **Step 3: Write the failing Compose ownership test**

Rename `streams_runtime_cadence_is_configurable_only_on_the_stream_role` to
`streams_runtime_policy_is_configurable_only_on_the_stream_role`, then extend
it:

```rust
assert2::assert!(stream.contains(
    "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS:-30000}\""
));
for service in ["demo-produce", "demo-consume"] {
    assert2::assert!(
        !compose_service_block(&compose, service)
            .contains("CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS")
    );
}
```

- [ ] **Step 4: Run focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams rebalance_timeout_uses_typed_default_and_override --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_rebalance_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
```

Expected: the app field, demo option, effective-value function, forwarding,
and Compose variable are absent.

- [ ] **Step 5: Store and forward the typed application value**

Import `StreamsRebalanceTimeout` into `streams_app.rs`. Add:

```rust
rebalance_timeout: StreamsRebalanceTimeout,
```

to `StreamsApp`, add this builder parameter:

```rust
/// Timeout advertised for completing a Client Streams rebalance.
#[builder(default)]
rebalance_timeout: StreamsRebalanceTimeout,
```

store it in `Self`, and forward it in `run_built`:

```rust
.rebalance_timeout(self.rebalance_timeout.duration())
```

- [ ] **Step 6: Add the Stream-only demo boundary**

Import `StreamsRebalanceTimeout`, add this `Cli` field:

```rust
/// Client Streams rebalance timeout in milliseconds.
#[arg(long, env = "CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS")]
streams_rebalance_timeout_ms: Option<NonZeroU64>,
```

Add:

```rust
fn effective_streams_rebalance_timeout(
    cli: &Cli,
) -> std::io::Result<StreamsRebalanceTimeout> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_rebalance_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-rebalance-timeout-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_rebalance_timeout_ms.map_or_else(
        || Ok(StreamsRebalanceTimeout::default()),
        |milliseconds| {
            StreamsRebalanceTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}
```

Call it in `main` before telemetry initialization. Pass the typed result only
to `run_stream`, add it to that function's parameters, and forward:

```rust
.rebalance_timeout(streams_rebalance_timeout)
```

to `StreamsApp::builder()`.

- [ ] **Step 7: Configure only the demo Stream service**

Add under `demo-stream.environment`:

```yaml
CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS: "${CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS:-30000}"
```

Do not add it to anchors, Produce, Consume, or any other service.

- [ ] **Step 8: Run focused and package verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams rebalance_timeout_uses_typed_default_and_override --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_rebalance_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
```

Also run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p observability-demo-app --locked -- --help
```

Expected: every test and check passes; help contains the new flag once; no
non-Stream Compose service contains the environment variable; `Cargo.lock`
remains unchanged.

- [ ] **Step 9: Commit Task 2**

```bash
git add crates/client-streams/src/streams_app.rs \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/streams_rebalance_timeout_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose Streams rebalance timeout"
```

---

### Task 3: Close the Audit Slice and Run Final Gates

**Files:**
- Modify: `docs/configuration-audit.md:2066-2074`

**Interfaces:**
- Consumes: completed Task 1 and Task 2 production, test, and deployment paths.
- Produces: reproducible scanner evidence, exclusive focused-reference classification, and the next unresolved operational owner without reclassifying the heartbeat fallback.

- [ ] **Step 1: Capture repository-wide and focused audit evidence**

Before editing the audit, run:

```bash
tools/audit-runtime-values.sh > /tmp/client-streams-rebalance-runtime-audit.txt
wc -l /tmp/client-streams-rebalance-runtime-audit.txt
cut -d: -f1 /tmp/client-streams-rebalance-runtime-audit.txt | sort -u | wc -l

rg -n "rebalance_timeout|StreamsRebalanceTimeout|DEFAULT_STREAMS_REBALANCE_TIMEOUT|streams-rebalance-timeout|STREAMS_REBALANCE_TIMEOUT" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md \
  > /tmp/client-streams-rebalance-focused-audit.txt
wc -l /tmp/client-streams-rebalance-focused-audit.txt
cut -d: -f1 /tmp/client-streams-rebalance-focused-audit.txt | sort -u | wc -l
```

Read every focused match and classify each line exactly once as:

- Client Streams production;
- completed downstream demo policy;
- demo deployment;
- test or harness;
- prior audit text; or
- unresolved owner.

Verify the category sum equals the focused line count.

- [ ] **Step 2: Replace the pending-owner paragraph with completed evidence**

Append a concise Client Streams rebalance-timeout section to
`docs/configuration-audit.md` containing:

- the exact pre-append scanner line and file counts;
- the exact focused command, line count, file count, and exclusive category
  counts;
- the validated `1..=i32::MAX` whole-millisecond contract;
- the 30,000 ms default;
- the `StreamsApp` typed ownership and compatible raw-`Duration` boundaries;
- proof that validation precedes prewarm and broker I/O;
- initial and coordinator heartbeat propagation;
- exact demo CLI/environment names, precedence, and Stream-only Compose
  ownership;
- the Task 1, Task 2, and final verification results; and
- the next unresolved owner found by the scanner.

State explicitly that the broker-provided heartbeat interval and fixed
3-second invalid-response fallback remain defensive protocol behavior and are
not configuration policy. Do not nominate that fallback as the next owner.

- [ ] **Step 3: Run the combined final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p observability-demo-app --locked -- --help
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and strict checks pass; the help flag appears exactly once;
`Cargo.lock` has no diff; the broker heartbeat fallback is unchanged.

- [ ] **Step 4: Commit Task 3**

```bash
git add docs/configuration-audit.md
git commit -m "docs(streams): record rebalance timeout"
```

- [ ] **Step 5: Review the complete implementation range**

Run:

```bash
git log --oneline --decorate -4
git diff --stat HEAD~3..HEAD
git diff --check HEAD~3..HEAD
git status -sb
```

Review the three-task range against
`docs/superpowers/specs/2026-07-26-client-streams-rebalance-timeout-design.md`.
Reject publication if any invalid timeout can reach prewarm or broker I/O, if
either public raw-`Duration` builder breaks, if the demo setting leaks outside
the Stream role, if `Cargo.lock` changes, or if the heartbeat fallback changes.
