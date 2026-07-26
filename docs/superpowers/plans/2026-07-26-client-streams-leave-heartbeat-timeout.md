# Client Streams Leave-Heartbeat Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose and validate the Client Streams shutdown deadline for its final leave heartbeat while preserving the five-second default and best-effort behavior.

**Architecture:** Add one public membership timing newtype, validate raw `Duration` inputs at both public runtime entry points before external I/O, and carry the validated duration into the existing coordinator state. Expose the typed policy through `StreamsApp` and only the observability demo Stream role.

**Tech Stack:** Rust, `refined_type`, Tokio, Bon builders, Clap, Docker Compose YAML, Cargo tests, Clippy, rustfmt, ripgrep.

## Global Constraints

- Change only Client Streams; consumer and share-consumer leave deadlines remain
  unchanged.
- Preserve the exact default of five seconds.
- Reject zero, fractional milliseconds, and values above `u64::MAX`
  milliseconds.
- Use `refined_type::rule::MinMaxU128` for positive millisecond validation.
- Keep raw `Duration` builder setters on `StreamsMembership` and
  `KafkaStreams`.
- Validate `KafkaStreams` input before broker I/O and `StreamsMembership`
  input before schema prewarming or broker I/O.
- Store the typed value in `StreamsApp`.
- Preserve one best-effort leave heartbeat with `member_epoch = -1`; timeout,
  transport, and broker errors remain ignored.
- Use `--streams-leave-heartbeat-timeout-ms` and
  `CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS`.
- Preserve CLI over environment over typed-default precedence.
- Resolve demo configuration before telemetry initialization or external I/O.
- Expose the deployment variable only on `demo-stream`, defaulting to `5000`.
- Add no CRD, retry loop, disable switch, cross-client timeout abstraction, or
  new dependency.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate and propagate the library timeout

**Files:**
- Modify: `crates/client-streams/src/membership/client.rs`
- Modify: `crates/client-streams/src/membership/coordinator.rs`
- Modify: `crates/client-streams/src/membership/mod.rs`
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/streams_app.rs`
- Modify: `crates/client-streams/src/lib.rs`

**Interfaces:**
- Produces:
  `pub const DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5)`
- Produces: `pub struct StreamsLeaveHeartbeatTimeout(Duration)`
- Produces:
  `StreamsLeaveHeartbeatTimeout::new(Duration) -> Result<Self, String>`
- Produces: `StreamsLeaveHeartbeatTimeout::duration(self) -> Duration`
- Produces: `StreamsLeaveHeartbeatTimeout::milliseconds(self) -> u64`
- Consumes: raw `leave_heartbeat_timeout: Duration` builder inputs on
  `StreamsMembership` and `KafkaStreams`
- Produces: typed `leave_heartbeat_timeout: StreamsLeaveHeartbeatTimeout` on
  `StreamsApp`
- Carries: `leave_heartbeat_timeout: Duration` in `CoordinatorState`

- [ ] **Step 1: Add failing semantic-type tests**

In `crates/client-streams/src/membership/client.rs`, extend the test imports
with `StreamsLeaveHeartbeatTimeout`, then add:

```rust
#[test]
fn leave_heartbeat_timeout_uses_default_and_valid_override() {
    let default = StreamsLeaveHeartbeatTimeout::default();
    check!(default.duration() == Duration::from_secs(5));
    check!(default.milliseconds() == 5_000);

    let timeout = StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(37))
        .expect("positive whole milliseconds");
    check!(timeout.duration() == Duration::from_millis(37));
    check!(timeout.milliseconds() == 37);
}

#[test]
fn leave_heartbeat_timeout_validates_millisecond_boundaries() {
    check!(StreamsLeaveHeartbeatTimeout::new(Duration::ZERO).is_err());
    check!(
        StreamsLeaveHeartbeatTimeout::new(
            Duration::from_millis(1) + Duration::from_nanos(1)
        )
        .is_err()
    );
    check!(
        StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(u64::MAX))
            .is_ok()
    );
    check!(
        StreamsLeaveHeartbeatTimeout::new(Duration::from_secs(u64::MAX))
            .is_err()
    );
}
```

- [ ] **Step 2: Add a failing direct-membership pre-I/O test**

In the same test module, add a `SchemaPrewarm` test double and prove invalid
configuration is rejected before it is called:

```rust
struct CountingPrewarm(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait::async_trait]
impl super::SchemaPrewarm for CountingPrewarm {
    async fn prewarm(&self) -> Result<(), StreamsClientError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn invalid_leave_heartbeat_timeout_fails_before_prewarm_or_broker_lookup() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let topology = Arc::new(Topology::new().build("leave-validation").expect("topology"));

    let error = super::StreamsMembership::builder()
        .bootstrap("invalid.invalid:9092")
        .group_id("leave-validation")
        .topology(topology)
        .leave_heartbeat_timeout(Duration::ZERO)
        .schema_prewarm(Arc::new(CountingPrewarm(Arc::clone(&calls))))
        .build()
        .await
        .err()
        .expect("invalid configuration");

    check!(error.to_string().contains("streams leave heartbeat timeout"));
    check!(calls.load(std::sync::atomic::Ordering::Relaxed) == 0);
}
```

- [ ] **Step 3: Add a failing coordinator deadline test**

In `crates/client-streams/src/membership/coordinator.rs`, add a test transport
that completes ordinary heartbeats but never completes the leave:

```rust
struct HangingLeaveTransport;

#[async_trait::async_trait]
impl HeartbeatTransport for HangingLeaveTransport {
    async fn send_heartbeat(
        &self,
        req: StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
        if req.member_epoch == -1 {
            std::future::pending().await
        } else {
            Ok(ok_resp(7, vec![]))
        }
    }
}

#[tokio::test]
async fn run_loop_bounds_stalled_leave_with_configured_timeout() {
    let (mut state, _rx) = state_with(HangingLeaveTransport);
    state.leave_heartbeat_timeout = Duration::from_millis(37);
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), run(state, shutdown))
        .await
        .expect("configured leave timeout bounds shutdown");
}
```

Add `leave_heartbeat_timeout: DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT` to
`state_with` so every existing test uses the production default.

- [ ] **Step 4: Add failing low-level runtime and `StreamsApp` tests**

In `crates/client-streams/src/runtime/app.rs`, extend
`low_level_runtime_validation_names_the_invalid_field` with:

```rust
let leave_error = validate_runtime_configuration(
    Duration::from_millis(200),
    Duration::from_secs(5),
    Duration::from_secs(30),
    DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
    Duration::ZERO,
    DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES,
)
.expect_err("zero leave heartbeat timeout");
assert2::assert!(
    leave_error
        .to_string()
        .contains("streams leave heartbeat timeout")
);
```

Update every other direct call to `validate_runtime_configuration` with
`DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT`. Add:

```rust
#[tokio::test]
async fn invalid_leave_heartbeat_timeout_fails_before_broker_lookup() {
    let mut topology = Topology::new();
    let source = topology.add_source::<String, String>("source", ["input"]);
    topology.add_sink("sink", "output", [&source]);
    let topology = topology.build("leave-validation").expect("topology");

    let error = KafkaStreams::builder()
        .bootstrap("invalid.invalid:9092")
        .application_id("leave-validation")
        .topology(topology)
        .leave_heartbeat_timeout(Duration::ZERO)
        .build()
        .await
        .err()
        .expect("invalid configuration");

    assert2::assert!(
        error
            .to_string()
            .contains("streams leave heartbeat timeout")
    );
}
```

In `crates/client-streams/src/streams_app.rs`, add:

```rust
#[test]
fn leave_heartbeat_timeout_uses_typed_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("leave-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.leave_heartbeat_timeout,
        crate::StreamsLeaveHeartbeatTimeout::default()
    );

    let timeout =
        crate::StreamsLeaveHeartbeatTimeout::new(std::time::Duration::from_millis(37))
            .expect("positive timeout");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("leave-override")
        .schema_registry("http://127.0.0.1:8081")
        .leave_heartbeat_timeout(timeout)
        .build();
    assert_eq!(overridden.leave_heartbeat_timeout, timeout);
}
```

- [ ] **Step 5: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams leave_heartbeat_timeout --locked
```

Expected: compilation fails because the new type, constant, fields, and builder
setters do not exist.

- [ ] **Step 6: Implement the validated semantic type**

In `crates/client-streams/src/membership/client.rs`, add a private helper that
reuses the module's existing `MinMaxU128` validation:

```rust
fn validate_positive_whole_milliseconds(
    field: &str,
    value: Duration,
) -> Result<u64, String> {
    let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
        .map_err(|error| format!("{field}: {error}"))?
        .into_value();
    let milliseconds =
        u64::try_from(milliseconds).map_err(|error| format!("{field}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!("{field} must be a whole number of milliseconds"));
    }
    Ok(milliseconds)
}
```

Refactor `StreamsJoinRetryBackoff::new` to call this helper without changing
its accepted values or error text. Add:

```rust
/// Default deadline for the final Client Streams leave heartbeat.
pub const DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Positive, whole-millisecond deadline for the final leave heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsLeaveHeartbeatTimeout(Duration);

impl StreamsLeaveHeartbeatTimeout {
    /// Validate a final leave-heartbeat timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        validate_positive_whole_milliseconds(
            "streams leave heartbeat timeout",
            value,
        )?;
        Ok(Self(value))
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated streams leave heartbeat timeout fits u64")
    }
}

impl Default for StreamsLeaveHeartbeatTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT)
            .expect("default streams leave heartbeat timeout is valid")
    }
}
```

- [ ] **Step 7: Validate direct membership input and carry the duration**

Add to `StreamsMembership::start`:

```rust
#[builder(default = DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT)]
leave_heartbeat_timeout: Duration,
```

Construct `StreamsLeaveHeartbeatTimeout` immediately after the group-id check
and before schema prewarming:

```rust
let leave_heartbeat_timeout =
    StreamsLeaveHeartbeatTimeout::new(leave_heartbeat_timeout)
        .map_err(StreamsClientError::Runtime)?;
```

Add `leave_heartbeat_timeout: Duration` to `CoordinatorState`, initialize it
with `leave_heartbeat_timeout.duration()`, and replace:

```rust
tokio::time::timeout(Duration::from_secs(5), leave)
```

with:

```rust
tokio::time::timeout(state.leave_heartbeat_timeout, leave)
```

- [ ] **Step 8: Validate the low-level runtime before broker I/O**

Add `leave_heartbeat_timeout: Duration` to
`validate_runtime_configuration`, validate it with
`StreamsLeaveHeartbeatTimeout::new`, and return it before the cache-budget
tuple member.

Add this `KafkaStreams::start` builder input:

```rust
#[builder(default = DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT)]
leave_heartbeat_timeout: Duration,
```

Pass it into `validate_runtime_configuration`, bind the returned typed value,
and forward its duration to membership:

```rust
.leave_heartbeat_timeout(leave_heartbeat_timeout.duration())
```

- [ ] **Step 9: Add the typed `StreamsApp` setting and exports**

Add `leave_heartbeat_timeout: StreamsLeaveHeartbeatTimeout` to `StreamsApp`
and its constructor:

```rust
/// Deadline for the final Client Streams leave heartbeat during shutdown.
#[builder(default)]
leave_heartbeat_timeout: StreamsLeaveHeartbeatTimeout,
```

Forward it to `KafkaStreams` with:

```rust
.leave_heartbeat_timeout(self.leave_heartbeat_timeout.duration())
```

Re-export `DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT` and
`StreamsLeaveHeartbeatTimeout` from `membership/mod.rs` and the crate root.

- [ ] **Step 10: Run focused GREEN and package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams leave_heartbeat_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams run_loop_bounds_stalled_leave_with_configured_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams low_level_runtime_validation_names_the_invalid_field --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; `Cargo.lock` has no diff.

- [ ] **Step 11: Commit Task 1**

Stage only the six Task 1 files and commit:

```bash
git add -- \
  crates/client-streams/src/membership/client.rs \
  crates/client-streams/src/membership/coordinator.rs \
  crates/client-streams/src/membership/mod.rs \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/streams_app.rs \
  crates/client-streams/src/lib.rs
git commit -m "feat(streams): configure leave timeout"
```

---

### Task 2: Expose the demo CLI, environment, and Compose setting

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/streams_leave_heartbeat_timeout_config.rs`
- Modify:
  `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `StreamsLeaveHeartbeatTimeout`
- Produces: `--streams-leave-heartbeat-timeout-ms`
- Produces: `CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS`
- Produces: the typed timeout passed to
  `StreamsApp::leave_heartbeat_timeout(StreamsLeaveHeartbeatTimeout)`

- [ ] **Step 1: Add failing hermetic subprocess tests**

Create
`crates/observability-demo-app/tests/streams_leave_heartbeat_timeout_config.rs`:

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
        .env("CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--streams-leave-heartbeat-timeout-ms (37 ms) is only valid with --role stream"
    ));

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-leave-heartbeat-timeout-ms",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--streams-leave-heartbeat-timeout-ms (41 ms) is only valid with --role stream"
    ));
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-leave-heartbeat-timeout-ms")
            .count(),
        1
    );
}
```

- [ ] **Step 2: Add the failing Compose ownership assertion**

In
`crates/observability-demo-app/tests/observability_demo_config.rs`, extend
`streams_runtime_policy_is_configurable_only_on_the_stream_role`:

```rust
assert2::assert!(stream.contains(
    "CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS: \"${CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS:-5000}\""
));
```

Add inside the existing Produce/Consume loop:

```rust
assert2::assert!(
    !service.contains("CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS")
);
```

- [ ] **Step 3: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_leave_heartbeat_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
```

Expected: subprocess tests fail because Clap does not know the option; the
Compose test fails because the Stream service lacks the variable.

- [ ] **Step 4: Add the demo option and early typed resolver**

Import `StreamsLeaveHeartbeatTimeout` and add to `Cli`:

```rust
/// Client Streams final leave-heartbeat timeout in milliseconds.
#[arg(long, env = "CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS")]
streams_leave_heartbeat_timeout_ms: Option<NonZeroU64>,
```

Add:

```rust
fn effective_streams_leave_heartbeat_timeout(
    cli: &Cli,
) -> std::io::Result<StreamsLeaveHeartbeatTimeout> {
    if cli.role != Role::Stream
        && let Some(milliseconds) = cli.streams_leave_heartbeat_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-leave-heartbeat-timeout-ms ({} ms) is only valid with --role stream",
                milliseconds.get(),
            ),
        ));
    }

    cli.streams_leave_heartbeat_timeout_ms.map_or_else(
        || Ok(StreamsLeaveHeartbeatTimeout::default()),
        |milliseconds| {
            StreamsLeaveHeartbeatTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}
```

Call the resolver with the other Stream-only resolvers before telemetry
initialization. Add `streams_leave_heartbeat_timeout_ms: None` to every direct
`Cli` literal in the unit tests.

- [ ] **Step 5: Route the typed timeout to `StreamsApp`**

Add a `StreamsLeaveHeartbeatTimeout` parameter to `run_stream`, pass it from
`main`, and add:

```rust
.leave_heartbeat_timeout(streams_leave_heartbeat_timeout)
```

- [ ] **Step 6: Add only the Stream Compose variable**

In `demo/observability/docker-compose.yml`, add to `demo-stream`:

```yaml
CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS: "${CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS:-5000}"
```

Do not add it to `demo-produce`, `demo-consume`, or a shared anchor.

- [ ] **Step 7: Run focused GREEN and demo gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_leave_heartbeat_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--streams-leave-heartbeat-timeout-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; the help count is exactly `1`;
`Cargo.lock` has no diff.

- [ ] **Step 8: Commit Task 2**

Stage only the four Task 2 files and commit:

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/streams_leave_heartbeat_timeout_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose Streams leave timeout"
```

---

### Task 3: Record the completed owner and final verification

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: completed library, demo, and Compose behavior from Tasks 1-2
- Produces: exclusive focused-search classification and the next
  production-consumed configuration owner

- [ ] **Step 1: Run the repository scanner and focused owner search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "leave_heartbeat_timeout|StreamsLeaveHeartbeatTimeout|DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT|streams-leave-heartbeat-timeout-ms|STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS|member_epoch: -1" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

Record exact scanner line/file totals. Classify every focused line exactly once
as Client Streams production, demo policy, demo deployment, test or harness,
prior audit, or unresolved owner. Verify category totals equal the focused
search total.

- [ ] **Step 2: Append the completed audit section**

Append `## Client Streams Leave-Heartbeat Timeout` to
`docs/configuration-audit.md`. Include:

- the positive whole-millisecond range and exact five-second default;
- the raw low-level setters, typed `StreamsApp` setting, and both pre-I/O
  validation boundaries;
- the exact `StreamsApp -> KafkaStreams -> StreamsMembership ->
  CoordinatorState -> tokio::time::timeout` flow;
- unchanged best-effort leave semantics;
- demo CLI, environment, precedence, role restriction, and Compose owner;
- why no CRD exists;
- exact scanner and focused-search commands and classifications;
- verification results from Tasks 1-2; and
- `### Adjacent Pending Policy` naming the next scanner-visible value with a
  real production consumer.

Do not claim repository-wide completion. Keep consumer and share-consumer
five-second leave deadlines explicitly pending and separate.

- [ ] **Step 3: Run fresh combined final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--streams-leave-heartbeat-timeout-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; help count is exactly `1`; `Cargo.lock`
has no diff.

- [ ] **Step 4: Commit Task 3**

Stage only the audit and commit:

```bash
git add -- docs/configuration-audit.md
git commit -m "docs(streams): record leave timeout"
```

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git diff --stat e5c8f19a..HEAD
git diff --check e5c8f19a..HEAD
git diff e5c8f19a..HEAD -- Cargo.lock
git status --short
```

Confirm the range contains only the intended library, demo, Compose, audit, and
test files; `Cargo.lock` is unchanged; all pre-existing unrelated dirty and
untracked files remain unstaged.
