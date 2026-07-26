# Client Streams Interactive Query Queue Capacity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two fixed 64-entry Client Streams interactive-query queues with one validated application setting while preserving current bounded-channel behavior by default.

**Architecture:** Add one public `StreamsInteractiveQueryQueueCapacity` semantic type beside the runtime owner and use it end-to-end in `StreamsApp` and `KafkaStreams`. Apply the same validated capacity to both v1 and v2 Tokio MPSC channels, then expose it only on the observability demo Stream role through Clap CLI/environment precedence and the Stream Compose service.

**Tech Stack:** Rust, `refined_type`, Bon builders, Tokio MPSC, Clap, Docker Compose YAML, Cargo tests, Clippy, rustfmt, ripgrep.

## Global Constraints

- Preserve the exact default capacity of `64` for both interactive-query queues.
- Use `refined_type::rule::GreaterUsize<0>` for the new validated newtype.
- Accept every positive `usize`; reject zero.
- Use one capacity for both `IqRequest` and `Iq2Request` channels.
- Keep bounded Tokio MPSC behavior, asynchronous backpressure, shutdown behavior, query dispatch, and response handling unchanged.
- Use typed `StreamsInteractiveQueryQueueCapacity` inputs on both public `StreamsApp` and `KafkaStreams` builders.
- Use `--streams-interactive-query-queue-capacity` and `CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`.
- Preserve CLI over environment over typed-default precedence.
- Resolve and validate demo configuration before telemetry or external I/O.
- Expose the deployment variable only on `demo-stream`, defaulting to `64`.
- Add no CRD: the operator does not own or render a Client Streams workload.
- Add no per-version settings, unbounded channel, dynamic resizing, enqueue timeout, drop policy, fairness change, queue metric, generic queue abstraction, macro, upper bound, or cross-field rule.
- Leave the test-only 16-entry interactive-query servicer channel unchanged.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate and route the shared queue capacity

**Files:**
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/runtime/mod.rs`
- Modify: `crates/client-streams/src/streams_app.rs`
- Modify: `crates/client-streams/src/lib.rs`

**Interfaces:**
- Produces: `pub const DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: usize`
- Produces: `pub struct StreamsInteractiveQueryQueueCapacity(usize)`
- Produces: `StreamsInteractiveQueryQueueCapacity::new(usize) -> Result<Self, String>`
- Produces: `StreamsInteractiveQueryQueueCapacity::capacity(self) -> usize`
- Produces: defaulted typed `interactive_query_queue_capacity` inputs on `KafkaStreams` and `StreamsApp`
- Produces: `interactive_query_queue_capacities(StreamsInteractiveQueryQueueCapacity) -> [usize; 2]`
- Consumes: existing `IqRequest` and `Iq2Request` channel construction

- [ ] **Step 1: Add failing semantic-type and two-channel tests**

In `runtime/app.rs`, extend the test import and add:

```rust
use super::{
    KafkaStreams, StreamsCommitInterval, StreamsInteractiveQueryQueueCapacity,
    StreamsPollInterval, interactive_query_queue_capacities,
    validate_runtime_configuration,
};

#[test]
fn interactive_query_queue_capacity_uses_default_and_valid_override() {
    let default = StreamsInteractiveQueryQueueCapacity::default();
    assert_eq!(default.capacity(), 64);

    let capacity =
        StreamsInteractiveQueryQueueCapacity::new(37).expect("positive queue capacity");
    assert_eq!(capacity.capacity(), 37);
}

#[test]
fn interactive_query_queue_capacity_rejects_zero() {
    let error = StreamsInteractiveQueryQueueCapacity::new(0)
        .expect_err("zero queue capacity");
    assert2::assert!(
        error.contains("streams interactive-query queue capacity")
    );
}

#[test]
fn interactive_query_queues_share_the_configured_capacity() {
    let capacity =
        StreamsInteractiveQueryQueueCapacity::new(37).expect("positive queue capacity");
    assert_eq!(interactive_query_queue_capacities(capacity), [37, 37]);
}
```

- [ ] **Step 2: Add the failing `StreamsApp` ownership test**

In `streams_app.rs`, add:

```rust
#[test]
fn interactive_query_queue_capacity_uses_typed_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("iq-capacity-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(
        defaults.interactive_query_queue_capacity,
        crate::StreamsInteractiveQueryQueueCapacity::default()
    );

    let capacity = crate::StreamsInteractiveQueryQueueCapacity::new(37)
        .expect("positive queue capacity");
    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("iq-capacity-override")
        .schema_registry("http://127.0.0.1:8081")
        .interactive_query_queue_capacity(capacity)
        .build();
    assert_eq!(overridden.interactive_query_queue_capacity, capacity);
}
```

- [ ] **Step 3: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams interactive_query_queue_capacity --locked
```

Expected: compilation fails because the semantic type, helper, builder inputs,
and `StreamsApp` field do not exist.

- [ ] **Step 4: Implement the validated semantic type and pure helper**

In `runtime/app.rs`, import `refined_type::rule::GreaterUsize` and add beside
the other public runtime configuration types:

```rust
/// Default capacity of each Client Streams interactive-query request queue.
pub const DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: usize = 64;

/// Positive capacity shared by the Client Streams interactive-query queues.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsInteractiveQueryQueueCapacity(usize);

impl StreamsInteractiveQueryQueueCapacity {
    /// Validate an interactive-query queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| {
                format!("streams interactive-query queue capacity: {error}")
            })
    }

    /// Return the validated capacity.
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.0
    }
}

impl Default for StreamsInteractiveQueryQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY)
            .expect("default streams interactive-query queue capacity is valid")
    }
}

fn interactive_query_queue_capacities(
    capacity: StreamsInteractiveQueryQueueCapacity,
) -> [usize; 2] {
    [capacity.capacity(); 2]
}
```

Do not add a reusable channel wrapper or generic capacity type.

- [ ] **Step 5: Apply the typed value to both `KafkaStreams` channels**

Add this defaulted input to `KafkaStreams::start`:

```rust
/// Capacity shared by the v1 and v2 interactive-query request queues.
#[builder(default)]
interactive_query_queue_capacity: StreamsInteractiveQueryQueueCapacity,
```

Replace the two literal channel capacities with:

```rust
let [iq_capacity, iq2_capacity] =
    interactive_query_queue_capacities(interactive_query_queue_capacity);
let (iq_tx, mut iq_rx) = mpsc::channel::<IqRequest>(iq_capacity);
let (iq2_tx, mut iq2_rx) = mpsc::channel::<Iq2Request>(iq2_capacity);
```

Do not alter either sender, receiver, `select!` branch, or query method.

- [ ] **Step 6: Route the typed value through `StreamsApp` and exports**

In `streams_app.rs`:

- import `StreamsInteractiveQueryQueueCapacity`;
- add a stored `interactive_query_queue_capacity` field;
- add a documented defaulted typed builder input;
- assign it in the constructor; and
- forward it in `run_built`:

```rust
.interactive_query_queue_capacity(self.interactive_query_queue_capacity)
```

Re-export `DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY` and
`StreamsInteractiveQueryQueueCapacity` from `runtime/mod.rs` and the crate
root.

- [ ] **Step 7: Run focused GREEN and package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams interactive_query_queue_capacity --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; both queues use the configured value and
the lockfile diff is empty.

- [ ] **Step 8: Commit Task 1**

Stage only the four Task 1 files and commit:

```bash
git add -- \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/runtime/mod.rs \
  crates/client-streams/src/streams_app.rs \
  crates/client-streams/src/lib.rs
git commit -m "feat(streams): configure query queue capacity"
```

---

### Task 2: Expose the demo CLI, environment, and Compose setting

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create: `crates/observability-demo-app/tests/streams_query_queue_config.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `StreamsInteractiveQueryQueueCapacity`
- Produces: `--streams-interactive-query-queue-capacity`
- Produces: `CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`
- Produces: typed `StreamsInteractiveQueryQueueCapacity` passed to `StreamsApp`

- [ ] **Step 1: Add failing hermetic subprocess tests**

Create `tests/streams_query_queue_config.rs`:

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
            "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY",
            "37",
        )
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(
        String::from_utf8_lossy(&environment.stderr).contains(
            "--streams-interactive-query-queue-capacity (37) is only valid with --role stream"
        )
    );

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-interactive-query-queue-capacity",
            "41",
        ])
        .env(
            "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY",
            "37",
        )
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(
        String::from_utf8_lossy(&cli.stderr).contains(
            "--streams-interactive-query-queue-capacity (41) is only valid with --role stream"
        )
    );
}

#[test]
fn zero_fails_early_and_help_lists_the_flag_once() {
    let zero = demo()
        .args(["--role", "stream"])
        .env(
            "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY",
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
            .filter(|token| *token == "--streams-interactive-query-queue-capacity")
            .count(),
        1
    );
}
```

- [ ] **Step 2: Extend the failing Compose ownership test**

In `streams_runtime_policy_is_configurable_only_on_the_stream_role`, require:

```rust
assert2::assert!(stream.contains(
    "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: \"${CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY:-64}\""
));
```

Also assert Produce and Consume service blocks do not contain
`CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`.

- [ ] **Step 3: Run Task 2 tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_query_queue_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
```

Expected: subprocess tests fail because Clap does not know the option; the
Compose test fails because the Stream service lacks the variable.

- [ ] **Step 4: Add the early typed demo resolution**

In `main.rs`, import `NonZeroUsize` and
`StreamsInteractiveQueryQueueCapacity`, then add:

```rust
/// Capacity shared by the Client Streams interactive-query request queues.
#[arg(
    long,
    env = "CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY"
)]
streams_interactive_query_queue_capacity: Option<NonZeroUsize>,
```

Add:

```rust
fn effective_streams_interactive_query_queue_capacity(
    cli: &Cli,
) -> std::io::Result<StreamsInteractiveQueryQueueCapacity> {
    if cli.role != Role::Stream
        && let Some(capacity) = cli.streams_interactive_query_queue_capacity
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-interactive-query-queue-capacity ({}) is only valid with --role stream",
                capacity.get(),
            ),
        ));
    }

    cli.streams_interactive_query_queue_capacity.map_or_else(
        || Ok(StreamsInteractiveQueryQueueCapacity::default()),
        |capacity| {
            StreamsInteractiveQueryQueueCapacity::new(capacity.get())
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
                })
        },
    )
}
```

Resolve it with the other Stream settings before telemetry:

```rust
let streams_interactive_query_queue_capacity =
    effective_streams_interactive_query_queue_capacity(&cli)?;
```

Pass the typed value through `run_stream` and into:

```rust
.interactive_query_queue_capacity(streams_interactive_query_queue_capacity)
```

Update every literal `Cli` in unit tests with
`streams_interactive_query_queue_capacity: None`. Add:

```rust
#[test]
fn streams_interactive_query_queue_capacity_uses_default_and_override() {
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
        streams_interactive_query_queue_capacity: None,
    };
    assert_eq!(
        effective_streams_interactive_query_queue_capacity(&defaults)
            .expect("typed default"),
        StreamsInteractiveQueryQueueCapacity::default()
    );

    let overridden = Cli {
        streams_interactive_query_queue_capacity: NonZeroUsize::new(37),
        ..defaults
    };
    assert_eq!(
        effective_streams_interactive_query_queue_capacity(&overridden)
            .expect("typed override")
            .capacity(),
        37
    );
}
```

- [ ] **Step 5: Add the Stream-only Compose variable**

In the `demo-stream` environment block, add:

```yaml
CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY: "${CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY:-64}"
```

Do not add it to anchors or any other service.

- [ ] **Step 6: Run Task 2 GREEN and full demo gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_query_queue_config --locked
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
  crates/observability-demo-app/tests/streams_query_queue_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose Streams query queue"
```

---

### Task 3: Record audit closure and verify the slice

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: Tasks 1 and 2 committed behavior
- Produces: reproducible scanner totals, exclusive focused-reference classification, final gate evidence, and one concrete next unresolved operational owner

- [ ] **Step 1: Capture the broad scanner**

Run:

```bash
tools/audit-runtime-values.sh > /tmp/client-streams-iq-queue-runtime-audit.txt
wc -l /tmp/client-streams-iq-queue-runtime-audit.txt
cut -d: -f1 /tmp/client-streams-iq-queue-runtime-audit.txt | sort -u | wc -l
```

Record the exact line and distinct-file totals before appending the audit
section.

- [ ] **Step 2: Capture and classify every focused reference**

Run:

```bash
rg -n \
  "interactive_query_queue_capacity|StreamsInteractiveQueryQueueCapacity|DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY|streams-interactive-query-queue-capacity|STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY|mpsc::channel::<Iq(Request|2Request)>\\(64\\)" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md \
  > /tmp/client-streams-iq-queue-focused.txt
wc -l /tmp/client-streams-iq-queue-focused.txt
cut -d: -f1 /tmp/client-streams-iq-queue-focused.txt | sort -u | wc -l
```

Classify every line exactly once as Client Streams production, demo policy,
demo deployment, test/harness, prior audit, or unresolved owner. Verify the
category sum equals the focused line count.

- [ ] **Step 3: Confirm the next real operational owner**

Inspect the remaining production candidates:

```bash
tools/audit-runtime-values.sh \
  | rg '^crates/client-streams/src/' \
  | rg -v \
    'interactive_query_queue_capacity|StreamsInteractiveQueryQueueCapacity|DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY|join_retry_backoff|StreamsJoinRetryBackoff|DEFAULT_STREAMS_JOIN_RETRY_BACKOFF|heartbeat_interval|from_secs\(3\)|dsl/names.rs|murmur3.rs|subscription.rs|_schema.rs|suppress_bufval.rs'
```

Confirm the first coherent runtime-owned operational setting with an actual
production consumer. The expected next candidate is the Client Streams
state-store record-cache byte budget, whose current 10 MiB default is passed
from `StreamsApp` through `KafkaStreams` into each `StreamThread`. If an earlier
candidate appears, trace its production consumer and record that instead.
Do not classify protocol identifiers, wire-format sizes, algorithm constants,
topology names, test values, the test-only 16-entry query channel, or the
broker-derived heartbeat fallback as configuration.

- [ ] **Step 4: Append the audit section**

Append `## Client Streams Interactive Query Queue Capacity` to
`docs/configuration-audit.md`. Record:

- the public type, positive-`usize` validation, and shared default of 64;
- typed ownership in both public builders;
- the exact `StreamsApp` to `KafkaStreams` to v1/v2 channel flow;
- unchanged bounded-channel backpressure, shutdown, dispatch, and response
  behavior;
- CLI/environment precedence and Stream-only Compose ownership;
- why there is no CRD;
- exact scanner and focused-search totals and exclusive classifications;
- Task 1, Task 2, and final verification results;
- the unchanged test-only 16-entry servicer channel; and
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
  grep -o -- '--streams-interactive-query-queue-capacity' <<<"$help" | wc -l
)" -eq 1
```

Expected: all commands pass and `Cargo.lock` has no diff.

- [ ] **Step 6: Commit Task 3**

```bash
git add -- docs/configuration-audit.md
git commit -m "docs(streams): record query queue capacity"
```

- [ ] **Step 7: Review and publish**

Review the complete implementation range after this plan against the approved
design. Address only concrete correctness, compatibility, configuration-flow,
test-isolation, or scope findings. Re-run the combined completion gates after
the final code change, preserve unrelated worktree files, push
`configuration_expose`, and verify that local HEAD, the remote branch SHA, and
pull request #904's head SHA match.
