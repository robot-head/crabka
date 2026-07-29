# Client Consumer Leave-Group Timeout Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate and expose the classic Client Consumer deadline shared by failed-startup cleanup and normal coordinator shutdown while preserving the five-second default.

**Architecture:** Add one public refined timing type and a raw `Duration` builder input validated before startup side effects. Carry the validated duration through `StartConfig` into both existing leave paths, then expose it only on the observability demo Consume role.

**Tech Stack:** Rust, `refined_type`, Tokio, Bon builders, Clap, Docker Compose YAML, Cargo tests, Clippy, rustfmt, ripgrep.

## Global Constraints

- Change only classic `Consumer`; `ShareConsumer` remains unchanged.
- Preserve the exact default of five seconds.
- Reject zero, fractional milliseconds, and values above `u64::MAX`
  milliseconds.
- Use `refined_type::rule::MinMaxU128`.
- Add a raw `Duration` builder setter on `Consumer`, validated before the
  startup retry loop or network I/O.
- Use the same configured value for failed-startup cleanup and coordinator
  shutdown.
- Preserve one best-effort `LeaveGroup` per path; timeout, transport, and
  broker errors remain ignored.
- Use `--consumer-leave-group-timeout-ms` and
  `CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS`.
- Preserve CLI over environment over typed-default precedence.
- Resolve demo configuration before telemetry initialization or external I/O.
- Restrict the demo setting to the Consume role.
- Expose the deployment variable only on `demo-consume`, defaulting to `5000`.
- Add no CRD, retry loop, disable switch, shared cross-protocol timeout
  abstraction, or new external dependency.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate and propagate the classic Consumer timeout

**Files:**
- Modify: `crates/client-consumer/Cargo.toml`
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/coordinator.rs`
- Modify: `crates/client-consumer/src/lib.rs`

**Interfaces:**
- Produces:
  `pub const DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT: Duration = Duration::from_secs(5)`
- Produces: `pub struct ConsumerLeaveGroupTimeout(Duration)`
- Produces:
  `ConsumerLeaveGroupTimeout::new(Duration) -> Result<Self, String>`
- Produces: `ConsumerLeaveGroupTimeout::duration(self) -> Duration`
- Produces: `ConsumerLeaveGroupTimeout::milliseconds(self) -> u64`
- Produces: raw
  `Consumer::builder().leave_group_timeout(Duration)` with the typed default
- Carries: `leave_group_timeout: Duration` through `StartConfig` and
  `CoordinatorState`

- [ ] **Step 1: Add failing semantic-type boundary tests**

In the `consumer.rs` test module, import the new type and add:

```rust
#[test]
fn leave_group_timeout_uses_default_and_valid_override() {
    let default = ConsumerLeaveGroupTimeout::default();
    assert2::assert!(default.duration() == Duration::from_secs(5));
    assert2::assert!(default.milliseconds() == 5_000);

    let timeout = ConsumerLeaveGroupTimeout::new(Duration::from_millis(37))
        .expect("positive whole milliseconds");
    assert2::assert!(timeout.duration() == Duration::from_millis(37));
    assert2::assert!(timeout.milliseconds() == 37);
}

#[test]
fn leave_group_timeout_validates_millisecond_boundaries() {
    assert2::assert!(ConsumerLeaveGroupTimeout::new(Duration::ZERO).is_err());
    assert2::assert!(
        ConsumerLeaveGroupTimeout::new(
            Duration::from_millis(1) + Duration::from_nanos(1)
        )
        .is_err()
    );
    assert2::assert!(
        ConsumerLeaveGroupTimeout::new(Duration::from_millis(u64::MAX)).is_ok()
    );
    assert2::assert!(
        ConsumerLeaveGroupTimeout::new(Duration::from_secs(u64::MAX)).is_err()
    );
}
```

- [ ] **Step 2: Add a failing pre-network builder test**

Add:

```rust
#[tokio::test]
async fn invalid_leave_group_timeout_fails_before_broker_lookup() {
    let error = Consumer::builder()
        .bootstrap("invalid.invalid:9092")
        .group_id("leave-validation")
        .subscribe(["topic".to_owned()])
        .leave_group_timeout(Duration::ZERO)
        .build()
        .await
        .err()
        .expect("invalid configuration");

    assert2::assert!(
        error
            .to_string()
            .contains("consumer leave-group timeout")
    );
}
```

- [ ] **Step 3: Add failing configured startup-cleanup coverage**

Update `startup_member_cleanup_sends_leave_group` to pass a distinctive
`Duration::from_millis(37)` to `leave_startup_member`.

Add a second `MockBroker` test whose LeaveGroup callback records the request
but returns `None`, then prove the helper returns within an outer one-second
guard:

```rust
#[tokio::test]
async fn startup_member_cleanup_bounds_stalled_leave_with_configured_timeout() {
    let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_leave_in_mock = Arc::clone(&saw_leave);
    let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            Some(api_versions_for_startup_cleanup())
        } else if api_key == leave_group_request::API_KEY {
            saw_leave_in_mock.store(true, Ordering::SeqCst);
            None
        } else {
            None
        }
    })
    .await;

    let client = Client::builder()
        .bootstrap(mock.addr.to_string())
        .request_timeout(Duration::from_secs(1))
        .build()
        .await
        .expect("client");
    let coordinator_id = AtomicI32::new(0);

    tokio::time::timeout(
        Duration::from_secs(1),
        leave_startup_member(
            &client,
            &coordinator_id,
            "group-a",
            "member-a",
            None,
            Duration::from_millis(37),
        ),
    )
    .await
    .expect("configured leave deadline bounds cleanup");

    mock.stop();
    assert2::assert!(saw_leave.load(Ordering::SeqCst));
}
```

- [ ] **Step 4: Add failing coordinator-shutdown coverage**

In `coordinator.rs`'s `retry_tests` module, import `MockBroker`,
`crabka_protocol::Encode`, `api_versions_request`,
`api_versions_response::{ApiVersion, ApiVersionsResponse}`, and
`leave_group_request`. Add this local response helper:

```rust
fn api_versions_for_leave_group() -> Vec<u8> {
    let response = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: api_versions_request::API_KEY,
                min_version: 0,
                max_version: 3,
                ..Default::default()
            },
            ApiVersion {
                api_key: leave_group_request::API_KEY,
                min_version: 0,
                max_version: leave_group_request::MAX_VERSION,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buffer = bytes::BytesMut::new();
    response.encode(&mut buffer, 0).expect("encode API versions");
    buffer.to_vec()
}
```

Add a direct test of the shutdown helper. Construct every `CoordinatorState`
field explicitly so later field additions fail visibly rather than being
hidden by a generic fixture:

```rust
#[tokio::test]
async fn coordinator_leave_group_uses_configured_timeout() {
    let saw_leave = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_leave_in_mock = Arc::clone(&saw_leave);
    let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
        if api_key == api_versions_request::API_KEY {
            Some(api_versions_for_leave_group())
        } else if api_key == leave_group_request::API_KEY {
            saw_leave_in_mock.store(true, Ordering::SeqCst);
            None
        } else {
            None
        }
    })
    .await;
    let client = Client::builder()
        .bootstrap(mock.addr.to_string())
        .request_timeout(Duration::from_secs(1))
        .build()
        .await
        .expect("client");
    let state = CoordinatorState {
        client,
        group_id: "group-a".into(),
        coordinator_id: Arc::new(AtomicI32::new(0)),
        member_id: "member-a".into(),
        group_instance_id: None,
        generation_id: 1,
        current_generation: Arc::new(AtomicI32::new(1)),
        assignor: Assignor::Range,
        subscribed_topics: vec!["topic".into()],
        assigned: Arc::new(Mutex::new(Vec::new())),
        next_offsets: Arc::new(Mutex::new(HashMap::new())),
        positions: Arc::new(Mutex::new(HashMap::new())),
        topic_ids: Arc::new(Mutex::new(HashMap::new())),
        session_timeout: Duration::from_secs(45),
        rebalance_timeout: Duration::from_secs(60),
        heartbeat_interval: Duration::from_secs(3),
        leave_group_timeout: Duration::from_millis(37),
        auto_offset_reset: AutoOffsetReset::Latest,
        client_rack: None,
        initial_subscribed_counts: HashMap::new(),
    };

    tokio::time::timeout(Duration::from_secs(1), leave_group(&state))
    .await
    .expect("configured leave deadline bounds coordinator shutdown");
    mock.stop();
    assert2::assert!(saw_leave.load(Ordering::SeqCst));
}
```

This exercises the existing private helper directly; do not introduce a
generic transport abstraction solely for the test.

- [ ] **Step 5: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_group_timeout --locked
```

Expected: compilation fails because the type, constant, builder setter,
function argument, and coordinator field do not exist.

- [ ] **Step 6: Add the existing workspace dependency and semantic type**

In `crates/client-consumer/Cargo.toml`, add:

```toml
refined_type = { workspace = true }
```

In `consumer.rs`, import `refined_type::rule::MinMaxU128` and add:

```rust
/// Default deadline for classic Consumer best-effort group departure.
pub const DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Positive, whole-millisecond classic Consumer leave-group deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsumerLeaveGroupTimeout(Duration);

impl ConsumerLeaveGroupTimeout {
    /// Validate a leave-group timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for zero, fractional milliseconds, or a value whose
    /// milliseconds cannot be represented as `u64`.
    pub fn new(value: Duration) -> Result<Self, String> {
        let milliseconds = MinMaxU128::<1, { u64::MAX as u128 }>::new(value.as_millis())
            .map_err(|error| format!("consumer leave-group timeout: {error}"))?
            .into_value();
        let milliseconds = u64::try_from(milliseconds)
            .map_err(|error| format!("consumer leave-group timeout: {error}"))?;
        if Duration::from_millis(milliseconds) != value {
            return Err(
                "consumer leave-group timeout must be a whole number of milliseconds"
                    .to_owned(),
            );
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    pub fn milliseconds(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .expect("validated consumer leave-group timeout fits u64")
    }
}

impl Default for ConsumerLeaveGroupTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT)
            .expect("default consumer leave-group timeout is valid")
    }
}
```

- [ ] **Step 7: Validate once before startup side effects**

Add to `Consumer::start`:

```rust
#[builder(default = DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT)]
leave_group_timeout: Duration,
```

After all existing local argument checks and before constructing
`StartConfig`, add:

```rust
let leave_group_timeout =
    ConsumerLeaveGroupTimeout::new(leave_group_timeout)
        .map_err(ConsumerError::RebalanceFailed)?;
```

Add `leave_group_timeout: Duration` to `StartConfig` and initialize it with:

```rust
leave_group_timeout: leave_group_timeout.duration(),
```

- [ ] **Step 8: Route the policy through both leave paths**

Add `leave_group_timeout: Duration` to `leave_startup_member`, and replace its
hardcoded timeout with:

```rust
let _ = tokio::time::timeout(leave_group_timeout, send).await;
```

Before moving `finish_config` into `finish_startup`, copy:

```rust
let cleanup_leave_group_timeout = finish_config.leave_group_timeout;
```

In the failed-startup call, pass `cleanup_leave_group_timeout`.

Add `leave_group_timeout: Duration` to `CoordinatorState`. In
`spawn_consumer`, carry the field from `StartConfig` into the state. Replace
the coordinator's hardcoded timeout with:

```rust
let _ = tokio::time::timeout(state.leave_group_timeout, send).await;
```

Do not change request construction, coordinator routing, member-id ownership,
or error handling.

- [ ] **Step 9: Export the public policy**

In `lib.rs`, change the consumer export to:

```rust
pub use consumer::{
    Consumer, ConsumerLeaveGroupTimeout, ConsumerRecord,
    DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT, Header,
};
```

- [ ] **Step 10: Run focused GREEN and package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer leave_group_timeout --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer startup_member_cleanup --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; `Cargo.lock` has no diff.

- [ ] **Step 11: Commit Task 1**

Stage only the four Task 1 files:

```bash
git add -- \
  crates/client-consumer/Cargo.toml \
  crates/client-consumer/src/consumer.rs \
  crates/client-consumer/src/coordinator.rs \
  crates/client-consumer/src/lib.rs
git commit -m "feat(consumer): configure leave timeout"
```

---

### Task 2: Expose the demo Consume setting

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/consumer_leave_group_timeout_config.rs`
- Modify:
  `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `ConsumerLeaveGroupTimeout`
- Produces: `--consumer-leave-group-timeout-ms`
- Produces: `CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS`
- Produces: the validated duration passed to
  `Consumer::builder().leave_group_timeout(Duration)`

- [ ] **Step 1: Add failing hermetic subprocess tests**

Create
`crates/observability-demo-app/tests/consumer_leave_group_timeout_config.rs`:

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
        .env("CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--consumer-leave-group-timeout-ms (37 ms) is only valid with --role consume"
    ));

    let cli = demo()
        .args([
            "--role",
            "stream",
            "--consumer-leave-group-timeout-ms",
            "41",
        ])
        .env("CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--consumer-leave-group-timeout-ms (41 ms) is only valid with --role consume"
    ));
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "consume"])
        .env("CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid value '0'"));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--consumer-leave-group-timeout-ms")
            .count(),
        1
    );
}
```

- [ ] **Step 2: Add the failing Compose ownership assertion**

In `observability_demo_config.rs`, add:

```rust
#[test]
fn consumer_leave_timeout_is_configurable_only_on_the_consume_role() {
    let compose = docker_compose();
    let consume = compose_service_block(&compose, "demo-consume");
    assert2::assert!(consume.contains(
        "CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS: \"${CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS:-5000}\""
    ));
    for service in ["demo-produce", "demo-stream"] {
        assert2::assert!(
            !compose_service_block(&compose, service)
                .contains("CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS")
        );
    }
}
```

- [ ] **Step 3: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test consumer_leave_group_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config consumer_leave_timeout_is_configurable_only_on_the_consume_role --locked
```

Expected: subprocess tests fail because Clap does not know the option; the
Compose test fails because `demo-consume` lacks the variable.

- [ ] **Step 4: Add the option and early typed resolver**

Import `ConsumerLeaveGroupTimeout` and add to `Cli`:

```rust
/// Classic Consumer best-effort leave-group timeout in milliseconds.
#[arg(long, env = "CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS")]
consumer_leave_group_timeout_ms: Option<NonZeroU64>,
```

Add:

```rust
fn effective_consumer_leave_group_timeout(
    cli: &Cli,
) -> std::io::Result<ConsumerLeaveGroupTimeout> {
    if cli.role != Role::Consume
        && let Some(milliseconds) = cli.consumer_leave_group_timeout_ms
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--consumer-leave-group-timeout-ms ({} ms) is only valid with --role consume",
                milliseconds.get(),
            ),
        ));
    }

    cli.consumer_leave_group_timeout_ms.map_or_else(
        || Ok(ConsumerLeaveGroupTimeout::default()),
        |milliseconds| {
            ConsumerLeaveGroupTimeout::new(Duration::from_millis(milliseconds.get()))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}
```

Call the resolver before telemetry initialization. Add
`consumer_leave_group_timeout_ms: None` to every direct `Cli` test literal.

- [ ] **Step 5: Route the typed value to the Consume role**

Pass `ConsumerLeaveGroupTimeout` into `run_consume` and add:

```rust
.leave_group_timeout(consumer_leave_group_timeout.duration())
```

Do not pass it to Produce or Stream.

- [ ] **Step 6: Add only the Consume Compose variable**

Under `demo-consume.environment`, add:

```yaml
CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS: "${CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS:-5000}"
```

- [ ] **Step 7: Run focused GREEN and demo gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test consumer_leave_group_timeout_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config consumer_leave_timeout_is_configurable_only_on_the_consume_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--consumer-leave-group-timeout-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; help count is exactly `1`; `Cargo.lock`
has no diff.

- [ ] **Step 8: Commit Task 2**

```bash
git add -- \
  crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/consumer_leave_group_timeout_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose consumer leave timeout"
```

---

### Task 3: Record the completed owner and final verification

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: completed classic Consumer and demo behavior from Tasks 1-2
- Produces: exclusive focused-search classification and the next
  production-consumed configuration owner

- [ ] **Step 1: Run the repository scanner and focused owner search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "leave_group_timeout|ConsumerLeaveGroupTimeout|DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT|consumer-leave-group-timeout-ms|CONSUMER_LEAVE_GROUP_TIMEOUT_MS|leave_startup_member|leave_group\\(" \
  crates/client-consumer \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

Record exact scanner line/file totals. Classify every focused line exactly once
as classic Consumer production, ShareConsumer production, demo policy, demo
deployment, test or harness, prior audit, or unresolved owner. Verify category
totals equal the focused-search total.

- [ ] **Step 2: Append the completed audit section**

Append `## Client Consumer Leave-Group Timeout` to
`docs/configuration-audit.md`. Include:

- the positive whole-millisecond range and exact five-second default;
- the raw builder setter and pre-retry/pre-I/O validation boundary;
- the exact `Consumer::start -> StartConfig` branch into failed-startup
  cleanup and `CoordinatorState` shutdown;
- unchanged one-attempt best-effort semantics;
- demo CLI, environment, precedence, role restriction, and Compose owner;
- why no CRD exists;
- exact scanner and focused-search commands and classifications;
- verification results from Tasks 1-2; and
- `### Adjacent Pending Policy` naming the next scanner-visible value with a
  real production consumer.

Do not claim repository-wide completion. Keep the ShareConsumer five-second
leave-heartbeat deadline explicitly pending and separate.

- [ ] **Step 3: Run fresh combined final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-consumer -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-consumer -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--consumer-leave-group-timeout-ms' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; help count is exactly `1`; `Cargo.lock`
has no diff.

- [ ] **Step 4: Commit Task 3**

```bash
git add -- docs/configuration-audit.md
git commit -m "docs(consumer): record leave timeout"
```

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git diff --stat 20bd950c..HEAD
git diff --check 20bd950c..HEAD
git diff 20bd950c..HEAD -- Cargo.lock
git status --short
```

Confirm the range contains only the intended library, demo, Compose, audit,
plan, and test files; `Cargo.lock` is unchanged; all pre-existing unrelated
dirty and untracked files remain unstaged.
