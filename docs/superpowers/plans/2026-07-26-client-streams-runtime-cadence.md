# Client Streams Runtime Cadence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose validated Client Streams poll and commit intervals through the high-level library and observability demo while preserving existing low-level callers and runtime behavior.

**Architecture:** Add two `refined_type`-validated semantic duration newtypes beside the `KafkaStreams` supervisor. `StreamsApp` and the demo use the typed values, while the existing `KafkaStreams` builder retains `Duration` inputs and validates them immediately before I/O. The demo exposes two Stream-only CLI/environment values and Compose passes them only to `demo-stream`.

**Tech Stack:** Rust 2024, Tokio intervals, Bon builders, `refined_type`, Clap derive/environment parsing, Docker Compose

## Global Constraints

- Preserve the exact 200-ms poll default and 5,000-ms commit default.
- Preserve Tokio's immediate first tick and the existing poll/commit select branches.
- Use separate `StreamsPollInterval` and `StreamsCommitInterval` newtypes backed by `refined_type`.
- Require positive whole milliseconds representable as `u64`.
- Keep `KafkaStreams::builder().poll_interval(Duration)` and `.commit_interval(Duration)` source compatible.
- Validate direct low-level durations before topology setup, DNS, or broker I/O.
- Exact demo interfaces are `--streams-poll-interval-ms`, `CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS`, `--streams-commit-interval-ms`, and `CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS`.
- Demo precedence is CLI over environment over the typed defaults.
- Demo cadence settings are valid only with `--role stream` and must fail before telemetry or external I/O otherwise.
- Compose exposes both values only on `demo-stream`, with defaults `200` and `5000`.
- Do not add a cadence struct, generic public interval type, macro, policy layer, CRD field, file configuration, or cross-field constraint.
- Do not configure membership retry, heartbeat, leave, test, punctuation, fetch, connect, or request timing in this slice.
- Add only the existing workspace `refined_type` dependency; do not change `Cargo.lock`.
- Every Cargo command must set `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Every lock-aware Cargo command must pass `--locked`.
- Follow TDD: observe the intended failure before production implementation.
- Preserve and never stage unrelated dirty or untracked workspace files.

## File Map

- `crates/client-streams/Cargo.toml`: direct workspace access to `refined_type`.
- `crates/client-streams/src/runtime/app.rs`: semantic interval types, low-level validation, unchanged Tokio timers.
- `crates/client-streams/src/runtime/mod.rs`: public runtime exports.
- `crates/client-streams/src/lib.rs`: crate-root exports.
- `crates/client-streams/src/streams_app.rs`: typed high-level storage, defaults, and forwarding.
- `crates/observability-demo-app/src/main.rs`: CLI/environment parsing, early role validation, and Stream-role forwarding.
- `crates/observability-demo-app/tests/streams_cadence_config.rs`: subprocess precedence, validation, and help proof.
- `crates/observability-demo-app/tests/observability_demo_config.rs`: Compose service scope.
- `demo/observability/docker-compose.yml`: Stream-role pass-through.
- `docs/configuration-audit.md`: scanner evidence, completed flow, and next owner.

---

### Task 1: Add Validated Library Cadence Types

**Files:**
- Modify: `crates/client-streams/Cargo.toml`
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/runtime/mod.rs`
- Modify: `crates/client-streams/src/lib.rs`
- Modify: `crates/client-streams/src/streams_app.rs`

**Interfaces:**
- Produces:
  ```rust
  pub const DEFAULT_STREAMS_POLL_INTERVAL: Duration = Duration::from_millis(200);
  pub const DEFAULT_STREAMS_COMMIT_INTERVAL: Duration = Duration::from_secs(5);

  pub struct StreamsPollInterval(Duration);
  impl StreamsPollInterval {
      pub fn new(value: Duration) -> Result<Self, String>;
      pub const fn duration(self) -> Duration;
      pub fn milliseconds(self) -> u64;
  }

  pub struct StreamsCommitInterval(Duration);
  impl StreamsCommitInterval {
      pub fn new(value: Duration) -> Result<Self, String>;
      pub const fn duration(self) -> Duration;
      pub fn milliseconds(self) -> u64;
  }

  StreamsApp::builder()
      .poll_interval(StreamsPollInterval)
      .commit_interval(StreamsCommitInterval)

  KafkaStreams::builder()
      .poll_interval(Duration)
      .commit_interval(Duration)
  ```
- Preserves: all existing `KafkaStreams` builder call signatures.

- [ ] **Step 1: Add failing semantic-type and low-level validation tests**

In `runtime/app.rs`, add a test module:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        StreamsCommitInterval, StreamsPollInterval, validate_runtime_intervals,
    };

    #[test]
    fn runtime_intervals_use_typed_defaults_and_valid_overrides() {
        let poll = StreamsPollInterval::default();
        let commit = StreamsCommitInterval::default();
        assert2::assert!(poll.milliseconds() == 200);
        assert2::assert!(commit.milliseconds() == 5_000);

        let poll = StreamsPollInterval::new(Duration::from_millis(37))
            .expect("positive whole milliseconds");
        let commit = StreamsCommitInterval::new(Duration::from_millis(41))
            .expect("positive whole milliseconds");
        assert2::assert!(poll.duration() == Duration::from_millis(37));
        assert2::assert!(commit.duration() == Duration::from_millis(41));
    }

    #[test]
    fn runtime_intervals_reject_zero_and_fractional_milliseconds() {
        assert2::assert!(StreamsPollInterval::new(Duration::ZERO).is_err());
        assert2::assert!(StreamsCommitInterval::new(Duration::ZERO).is_err());
        assert2::assert!(
            StreamsPollInterval::new(Duration::from_millis(1) + Duration::from_nanos(1))
                .is_err()
        );
        assert2::assert!(
            StreamsCommitInterval::new(
                Duration::from_millis(1) + Duration::from_nanos(1)
            )
            .is_err()
        );
    }

    #[test]
    fn low_level_runtime_validation_names_the_invalid_field() {
        let poll_error = validate_runtime_intervals(
            Duration::ZERO,
            Duration::from_secs(5),
        )
        .expect_err("zero poll interval");
        assert2::assert!(
            poll_error.to_string().contains("streams poll interval")
        );

        let commit_error = validate_runtime_intervals(
            Duration::from_millis(200),
            Duration::ZERO,
        )
        .expect_err("zero commit interval");
        assert2::assert!(
            commit_error.to_string().contains("streams commit interval")
        );
    }
}
```

- [ ] **Step 2: Add the failing high-level storage test**

In `streams_app.rs`, extend the existing test module:

```rust
#[test]
fn runtime_cadence_uses_typed_defaults_and_independent_overrides() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("cadence-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.poll_interval,
        crate::StreamsPollInterval::default()
    );
    assert_eq!(
        defaults.commit_interval,
        crate::StreamsCommitInterval::default()
    );

    let poll = crate::StreamsPollInterval::new(
        std::time::Duration::from_millis(37),
    )
    .expect("positive poll interval");
    let commit = crate::StreamsCommitInterval::new(
        std::time::Duration::from_millis(41),
    )
    .expect("positive commit interval");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("cadence-override")
        .schema_registry("http://127.0.0.1:8081")
        .poll_interval(poll)
        .commit_interval(commit)
        .build();
    assert_eq!(overridden.poll_interval, poll);
    assert_eq!(overridden.commit_interval, commit);
}
```

- [ ] **Step 3: Run focused tests and observe missing interfaces**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  runtime_intervals

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  runtime_cadence_uses_typed_defaults_and_independent_overrides
```

Expected: compilation fails because the two semantic types,
`validate_runtime_intervals`, and the two `StreamsApp` fields/builders do not
exist.

- [ ] **Step 4: Add the existing workspace dependency**

In `crates/client-streams/Cargo.toml`, add:

```toml
refined_type = { workspace = true }
```

Do not modify `Cargo.lock`.

- [ ] **Step 5: Implement the semantic types**

In `runtime/app.rs`, import `refined_type::rule::MinMaxU128` and add above
`KafkaStreams`:

```rust
/// Default delay between Client Streams processing polls.
pub const DEFAULT_STREAMS_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Default delay between Client Streams commit attempts.
pub const DEFAULT_STREAMS_COMMIT_INTERVAL: Duration = Duration::from_secs(5);

fn validate_positive_whole_milliseconds(
    field: &str,
    value: Duration,
) -> Result<u64, String> {
    let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(
        value.as_millis(),
    )
    .map_err(|error| format!("{field}: {error}"))?
    .into_value();
    let milliseconds = u64::try_from(milliseconds)
        .map_err(|error| format!("{field}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!(
            "{field} must be a whole number of milliseconds"
        ));
    }
    Ok(milliseconds)
}

/// Positive, whole-millisecond Client Streams processing poll interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsPollInterval(Duration);

impl StreamsPollInterval {
    /// Validate a processing poll interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds(
            "streams poll interval",
            value,
        )?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated streams poll interval fits u64")
    }
}

impl Default for StreamsPollInterval {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_POLL_INTERVAL)
            .expect("default streams poll interval is valid")
    }
}

/// Positive, whole-millisecond Client Streams commit interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsCommitInterval(Duration);

impl StreamsCommitInterval {
    /// Validate a commit interval.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds(
            "streams commit interval",
            value,
        )?;
        Ok(Self(value))
    }

    /// Return the validated duration.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    /// Return the validated whole milliseconds.
    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated streams commit interval fits u64")
    }
}

impl Default for StreamsCommitInterval {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_COMMIT_INTERVAL)
            .expect("default streams commit interval is valid")
    }
}
```

- [ ] **Step 6: Validate the compatible low-level inputs before I/O**

Add:

```rust
fn validate_runtime_intervals(
    poll_interval: Duration,
    commit_interval: Duration,
) -> Result<(StreamsPollInterval, StreamsCommitInterval), StreamsClientError> {
    let poll_interval = StreamsPollInterval::new(poll_interval)
        .map_err(StreamsClientError::Runtime)?;
    let commit_interval = StreamsCommitInterval::new(commit_interval)
        .map_err(StreamsClientError::Runtime)?;
    Ok((poll_interval, commit_interval))
}
```

Keep `KafkaStreams::start` parameters typed as `Duration`, but replace their
inline defaults with the named constants:

```rust
#[builder(default = DEFAULT_STREAMS_POLL_INTERVAL)]
poll_interval: Duration,
#[builder(default = DEFAULT_STREAMS_COMMIT_INTERVAL)]
commit_interval: Duration,
```

At the beginning of the function body, before `Arc::new(topology)` and all
broker setup, add:

```rust
let (poll_interval, commit_interval) =
    validate_runtime_intervals(poll_interval, commit_interval)?;
```

Construct the existing timers with:

```rust
let mut poll = tokio::time::interval(poll_interval.duration());
let mut commit = tokio::time::interval(commit_interval.duration());
```

Do not change the surrounding select loop or timer tick ordering.

- [ ] **Step 7: Export the semantic types and constants**

In `runtime/mod.rs`, replace the single app export with:

```rust
pub use app::{
    DEFAULT_STREAMS_COMMIT_INTERVAL, DEFAULT_STREAMS_POLL_INTERVAL,
    KafkaStreams, StreamsCommitInterval, StreamsPollInterval,
};
```

In `lib.rs`, extend the `runtime` export list with the two types and constants.

- [ ] **Step 8: Store and forward typed values from `StreamsApp`**

In `streams_app.rs`, import the two types with the existing runtime imports.
Add both fields to `StreamsApp`:

```rust
poll_interval: StreamsPollInterval,
commit_interval: StreamsCommitInterval,
```

Add defaulted typed inputs to `StreamsApp::new`:

```rust
/// Delay between Client Streams processing polls.
#[builder(default)]
poll_interval: StreamsPollInterval,
/// Delay between Client Streams commit attempts.
#[builder(default)]
commit_interval: StreamsCommitInterval,
```

Store them in `Self`. In `run_built`, forward:

```rust
.poll_interval(self.poll_interval.duration())
.commit_interval(self.commit_interval.duration())
```

- [ ] **Step 9: Run focused and complete Client Streams gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  runtime_intervals

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --lib --locked \
  runtime_cadence_uses_typed_defaults_and_independent_overrides

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p crabka-client-streams --all-targets --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
  -p crabka-client-streams --all-targets --locked -- -D warnings

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all commands pass, the existing minute-long `Duration` commit
interval tests compile unchanged, and `Cargo.lock` has no diff.

- [ ] **Step 10: Commit only the library change**

```bash
git add -- \
  crates/client-streams/Cargo.toml \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/runtime/mod.rs \
  crates/client-streams/src/lib.rs \
  crates/client-streams/src/streams_app.rs
git diff --cached --check
git commit -m "feat(streams): validate runtime cadence"
```

---

### Task 2: Expose the Demo Stream-Role Boundary

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/streams_cadence_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes:
  ```rust
  crabka_client_streams::{
      StreamsCommitInterval, StreamsPollInterval,
  }
  StreamsApp::builder()
      .poll_interval(StreamsPollInterval)
      .commit_interval(StreamsCommitInterval)
  ```
- Produces:
  ```text
  --streams-poll-interval-ms
  CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS
  --streams-commit-interval-ms
  CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS
  demo-stream Compose defaults: 200 and 5000
  ```

- [ ] **Step 1: Add failing parser, validation, and forwarding tests**

At the bottom of `main.rs`, extend the existing test module. Add the new fields
with `None` to every existing `Cli` literal, then add:

```rust
#[test]
fn streams_runtime_cadence_uses_defaults_and_independent_overrides() {
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
    };
    let (poll, commit) =
        effective_streams_runtime_cadence(&defaults).expect("typed defaults");
    assert_eq!(poll, crabka_client_streams::StreamsPollInterval::default());
    assert_eq!(
        commit,
        crabka_client_streams::StreamsCommitInterval::default()
    );

    let overridden = Cli {
        streams_poll_interval_ms: std::num::NonZeroU64::new(37),
        streams_commit_interval_ms: std::num::NonZeroU64::new(41),
        ..defaults
    };
    let (poll, commit) =
        effective_streams_runtime_cadence(&overridden).expect("typed overrides");
    assert_eq!(poll.milliseconds(), 37);
    assert_eq!(commit.milliseconds(), 41);
}

#[test]
fn streams_runtime_cadence_rejects_zero_and_non_stream_roles() {
    Cli::try_parse_from([
        "observability-demo-app",
        "--role",
        "stream",
        "--streams-poll-interval-ms",
        "0",
    ])
    .expect_err("zero poll interval");
    Cli::try_parse_from([
        "observability-demo-app",
        "--role",
        "stream",
        "--streams-commit-interval-ms",
        "0",
    ])
    .expect_err("zero commit interval");

    let produce = Cli::try_parse_from([
        "observability-demo-app",
        "--role",
        "produce",
        "--streams-poll-interval-ms",
        "37",
    ])
    .expect("parse before role validation");
    let error = effective_streams_runtime_cadence(&produce)
        .expect_err("Stream-only option");
    assert_eq!(
        error.to_string(),
        "--streams-poll-interval-ms (37 ms) is only valid with --role stream"
    );
}
```

- [ ] **Step 2: Add failing subprocess precedence and help tests**

Create `tests/streams_cadence_config.rs`:

```rust
use std::process::Command;

fn demo() -> Command {
    let mut command =
        Command::new(env!("CARGO_BIN_EXE_observability-demo-app"));
    command
        .env_remove("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS")
        .env_remove("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS");
    command
}

#[test]
fn environment_is_used_and_cli_wins_before_external_io() {
    let environment = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--streams-poll-interval-ms (37 ms) is only valid with --role stream"
    ));

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-poll-interval-ms",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--streams-poll-interval-ms (41 ms) is only valid with --role stream"
    ));

    let commit = demo()
        .args(["--role", "consume"])
        .env("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS", "43")
        .output()
        .expect("run demo");
    assert!(!commit.status.success());
    assert!(String::from_utf8_lossy(&commit.stderr).contains(
        "--streams-commit-interval-ms (43 ms) is only valid with --role stream"
    ));
}

#[test]
fn zero_values_are_rejected_and_help_lists_each_flag_once() {
    for (flag, environment) in [
        (
            "--streams-poll-interval-ms",
            "CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS",
        ),
        (
            "--streams-commit-interval-ms",
            "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS",
        ),
    ] {
        let zero = demo()
            .args(["--role", "stream"])
            .env(environment, "0")
            .output()
            .expect("run demo");
        assert!(!zero.status.success());
        assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

        let help = demo().arg("--help").output().expect("help");
        assert!(help.status.success());
        let help = String::from_utf8(help.stdout).expect("UTF-8 help");
        assert_eq!(
            help.split_whitespace()
                .filter(|token| *token == flag)
                .count(),
            1
        );
    }
}
```

- [ ] **Step 3: Add the failing Compose scope test**

In `observability_demo_config.rs`, add:

```rust
#[test]
fn streams_runtime_cadence_is_configurable_only_on_the_stream_role() {
    let compose = docker_compose();
    let stream = compose_service_block(&compose, "demo-stream");
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS: \"${CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS:-200}\""
    ));
    assert2::assert!(stream.contains(
        "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS: \"${CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS:-5000}\""
    ));
    for service in ["demo-produce", "demo-consume"] {
        let service = compose_service_block(&compose, service);
        assert2::assert!(
            !service.contains("CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS")
        );
        assert2::assert!(
            !service.contains("CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS")
        );
    }
}
```

- [ ] **Step 4: Run focused tests and observe missing configuration**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --bin observability-demo-app --locked \
  streams_runtime_cadence

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test streams_cadence_config --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test observability_demo_config --locked \
  streams_runtime_cadence_is_configurable_only_on_the_stream_role
```

Expected: compilation fails for the missing CLI fields and helper; the Compose
assertion fails because neither pass-through exists.

- [ ] **Step 5: Add the validated CLI/environment fields**

Import `StreamsCommitInterval` and `StreamsPollInterval` from
`crabka_client_streams`. Add to `Cli`:

```rust
/// Client Streams processing poll interval in milliseconds.
#[arg(long, env = "CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS")]
streams_poll_interval_ms: Option<NonZeroU64>,
/// Client Streams commit interval in milliseconds.
#[arg(long, env = "CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS")]
streams_commit_interval_ms: Option<NonZeroU64>,
```

Update every test `Cli` literal with these fields.

- [ ] **Step 6: Resolve both typed values before telemetry**

Add:

```rust
fn effective_streams_runtime_cadence(
    cli: &Cli,
) -> std::io::Result<(StreamsPollInterval, StreamsCommitInterval)> {
    if cli.role != Role::Stream {
        if let Some(milliseconds) = cli.streams_poll_interval_ms {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-poll-interval-ms ({} ms) is only valid with --role stream",
                    milliseconds.get(),
                ),
            ));
        }
        if let Some(milliseconds) = cli.streams_commit_interval_ms {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "--streams-commit-interval-ms ({} ms) is only valid with --role stream",
                    milliseconds.get(),
                ),
            ));
        }
    }

    let poll = cli.streams_poll_interval_ms.map_or_else(
        || Ok(StreamsPollInterval::default()),
        |milliseconds| {
            StreamsPollInterval::new(Duration::from_millis(
                milliseconds.get(),
            ))
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
            })
        },
    )?;
    let commit = cli.streams_commit_interval_ms.map_or_else(
        || Ok(StreamsCommitInterval::default()),
        |milliseconds| {
            StreamsCommitInterval::new(Duration::from_millis(
                milliseconds.get(),
            ))
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
            })
        },
    )?;
    Ok((poll, commit))
}
```

Immediately after `Cli::parse()` and the existing DNS resolution, before
telemetry initialization, add:

```rust
let (streams_poll_interval, streams_commit_interval) =
    effective_streams_runtime_cadence(&cli)?;
```

- [ ] **Step 7: Forward only through the Stream role**

Add both typed parameters to `run_stream`, pass them from the `Role::Stream`
match arm, and add to the `StreamsApp` builder:

```rust
.poll_interval(streams_poll_interval)
.commit_interval(streams_commit_interval)
```

Produce and Consume receive neither value.

- [ ] **Step 8: Add Stream-only Compose pass-through**

Under the existing `demo-stream.environment`, add:

```yaml
CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS: "${CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS:-200}"
CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS: "${CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS:-5000}"
```

Do not add either variable to `demo-produce`, `demo-consume`, or the shared
environment anchor.

- [ ] **Step 9: Run focused and complete demo gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --bin observability-demo-app --locked \
  streams_runtime_cadence

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test streams_cadence_config --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --test observability_demo_config --locked \
  streams_runtime_cadence_is_configurable_only_on_the_stream_role

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
  -p observability-demo-app --all-targets --locked

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
  -p observability-demo-app --all-targets --locked -- -D warnings

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
  -p observability-demo-app --locked -- --help |
  rg -- '--streams-(poll|commit)-interval-ms'

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all commands pass, help contains each exact flag once, and
`Cargo.lock` has no diff.

- [ ] **Step 10: Commit only the demo boundary**

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/streams_cadence_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git diff --cached --check
git commit -m "feat(demo): expose Streams cadence"
```

---

### Task 3: Audit Evidence, Whole-Slice Review, and Publication

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: Tasks 1-2 complete high-level and demo flow.
- Produces: an auditable closure record for Client Streams runtime cadence and
  the next unresolved owner; it does not close the repository-wide goal.

- [ ] **Step 1: Run the runtime-value scanner**

```bash
tools/audit-runtime-values.sh
```

Record exact line and distinct-file totals from the current scanner stream.

- [ ] **Step 2: Classify the exact focused search**

```bash
rg -n \
  "poll_interval|commit_interval|StreamsPollInterval|StreamsCommitInterval|DEFAULT_STREAMS_(POLL|COMMIT)_INTERVAL|streams-(poll|commit)-interval|STREAMS_(POLL|COMMIT)_INTERVAL" \
  crates/client-streams crates/observability-demo-app demo/observability \
  docs/configuration-audit.md
```

Classify every match as production, demo deployment, test/harness, prior audit
evidence, completed downstream policy, or unresolved owner. Record exact line
and distinct-file totals. Confirm that the 200-ms and 5,000-ms runtime values
now enter through validated policy and name the next coherent unresolved owner
from current evidence.

- [ ] **Step 3: Append the audit section**

Append `## Client Streams Runtime Cadence` to
`docs/configuration-audit.md`. Record:

- exact library types, builder fields, CLI, environment, and Compose names;
- exact 200-ms and 5,000-ms defaults and CLI > environment > default
  precedence;
- the complete demo -> `StreamsApp` -> compatible `KafkaStreams` builder ->
  immediate validation -> Tokio timer flow;
- positive whole-millisecond validation and early error behavior;
- preserved low-level source compatibility and timer semantics;
- scanner and focused-search totals and classification;
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
  rg -- '--streams-(poll|commit)-interval-ms'

CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all commands pass, help contains both exact flags once, and the
lockfile remains unchanged.

- [ ] **Step 5: Commit only audit evidence**

```bash
git add -- docs/configuration-audit.md
git diff --cached --check
git commit -m "docs(streams): record runtime cadence"
```

- [ ] **Step 6: Freeze and review the complete implementation diff**

Freeze the diff from the plan commit through Task 3. Review against the
approved design:

- both semantic newtypes use `refined_type` and preserve exact defaults;
- `StreamsApp` uses typed values and direct `KafkaStreams` `Duration` callers
  remain compatible;
- low-level validation occurs before topology setup and broker I/O;
- Tokio immediate ticks and select-loop behavior remain unchanged;
- exact demo CLI/environment/Compose names, precedence, defaults, and
  Stream-only early validation;
- no cadence struct, generic public type, macro, policy layer, cross-field
  constraint, CRD, lockfile change, or unrelated tuning;
- tests prove validation, compatibility, and configuration propagation.

Resolve every Critical and Important finding, rerun affected gates, and repeat
review until clean. Fix convenient documentation-only Minor findings; ledger
any remaining non-blocking Minor finding explicitly.

- [ ] **Step 7: Publish to existing draft PR #904**

Confirm `git status -sb`, exact commits, exact file scope, `gh auth status`, and
branch `configuration_expose`. Push normally; do not force-push. Verify:

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
