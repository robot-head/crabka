# Client Streams State-Store Cache Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate and expose the existing Client Streams state-store record-cache byte budget while preserving the 10 MiB default, zero-as-disable behavior, and raw public builder APIs.

**Architecture:** Add one public `StreamsStateStoreCacheMaxBytes` semantic type beside the Client Streams runtime owner. Keep both existing `cache_max_bytes(i64)` builder setters, validate their value once in `KafkaStreams::start` before broker I/O, and pass the validated raw bytes through the unchanged cache pipeline. Expose the same policy only on the observability demo Stream role through Clap CLI/environment precedence and Compose.

**Tech Stack:** Rust, `refined_type`, Bon builders, Clap, Docker Compose YAML, Cargo tests, Clippy, rustfmt, ripgrep.

## Global Constraints

- Preserve the exact default of `10_485_760` bytes.
- Preserve zero as the explicit cache-disabled value.
- Reject negative values instead of coercing them to zero.
- Accept values through the largest `i64` representable as the target's
  `usize`; on 64-bit targets this is `i64::MAX`.
- Use `refined_type::rule::MinMaxI64` in the validated semantic type.
- Keep the public `StreamsApp::cache_max_bytes(i64)` and
  `KafkaStreams::cache_max_bytes(i64)` builder setters source-compatible.
- Validate in `KafkaStreams::start` before broker I/O or supervisor spawn.
- Keep cache accounting, eviction, flushing, per-task allocation, materialized
  store eligibility, and the downstream raw `i64` data flow unchanged.
- Use `--streams-state-store-cache-max-bytes` and
  `CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES`.
- Preserve CLI over environment over typed-default precedence.
- Resolve and validate demo configuration before telemetry or external I/O.
- Expose the deployment variable only on `demo-stream`, defaulting to
  `10_485_760`.
- Add no CRD: the operator does not own or render a Client Streams workload.
- Add no byte-unit parser, cache policy object, generic byte-size abstraction,
  dynamic resizing, or cache metric.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Do not modify `Cargo.lock`.
- Preserve all unrelated dirty and untracked files.

---

### Task 1: Validate the library cache budget

**Files:**
- Modify: `crates/client-streams/src/runtime/app.rs`
- Modify: `crates/client-streams/src/runtime/mod.rs`
- Modify: `crates/client-streams/src/streams_app.rs`
- Modify: `crates/client-streams/src/lib.rs`

**Interfaces:**
- Produces:
  `pub const DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES: i64 = 10_485_760`
- Produces:
  `pub const MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES: i64`
- Produces: `pub struct StreamsStateStoreCacheMaxBytes(i64)`
- Produces:
  `StreamsStateStoreCacheMaxBytes::new(i64) -> Result<Self, String>`
- Produces: `StreamsStateStoreCacheMaxBytes::bytes(self) -> i64`
- Consumes: existing raw `cache_max_bytes: i64` inputs on `StreamsApp` and
  `KafkaStreams`
- Preserves: `StreamThread::new(..., cache_max_bytes: i64)` and every
  downstream cache interface

- [ ] **Step 1: Add failing semantic-type boundary tests**

In `crates/client-streams/src/runtime/app.rs`, extend the test import with
`StreamsStateStoreCacheMaxBytes` and
`MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES`, then add:

```rust
#[test]
fn state_store_cache_max_bytes_uses_default_and_valid_overrides() {
    let default = StreamsStateStoreCacheMaxBytes::default();
    assert_eq!(default.bytes(), 10_485_760);
    assert_eq!(
        StreamsStateStoreCacheMaxBytes::new(0)
            .expect("zero disables caching")
            .bytes(),
        0
    );
    assert_eq!(
        StreamsStateStoreCacheMaxBytes::new(37)
            .expect("positive cache budget")
            .bytes(),
        37
    );
}

#[test]
fn state_store_cache_max_bytes_rejects_negative_values() {
    let error =
        StreamsStateStoreCacheMaxBytes::new(-1).expect_err("negative cache budget");
    assert2::assert!(error.contains("streams state-store cache max bytes"));
}

#[test]
fn state_store_cache_max_bytes_matches_target_boundaries() {
    let maximum =
        StreamsStateStoreCacheMaxBytes::new(MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES)
            .expect("target-supported maximum");
    assert_eq!(
        maximum.bytes(),
        MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES
    );

    if let Some(too_large) = MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES.checked_add(1) {
        StreamsStateStoreCacheMaxBytes::new(too_large)
            .expect_err("value above target-supported maximum");
    }
}
```

- [ ] **Step 2: Add the failing shared runtime-validation assertion**

Extend `validate_runtime_configuration` calls in
`low_level_runtime_validation_names_the_invalid_field` with the valid default
cache budget. Add this assertion:

```rust
let cache_error = validate_runtime_configuration(
    Duration::from_millis(200),
    Duration::from_secs(5),
    Duration::from_secs(30),
    DEFAULT_STREAMS_JOIN_RETRY_BACKOFF,
    -1,
)
.expect_err("negative cache budget");
assert2::assert!(
    cache_error
        .to_string()
        .contains("streams state-store cache max bytes")
);
```

Update every other direct call to `validate_runtime_configuration` in the test
module to pass `DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES`.

- [ ] **Step 3: Add a failing before-broker-I/O regression test**

In the same test module, add:

```rust
#[tokio::test]
async fn invalid_state_store_cache_budget_fails_before_broker_lookup() {
    let mut topology = Topology::new();
    let source = topology.add_source::<String, String>("source", ["input"]);
    topology.add_sink("sink", "output", [&source]);
    let topology = topology.build("cache-budget-validation").expect("topology");

    let error = KafkaStreams::builder()
        .bootstrap("invalid.invalid:9092")
        .application_id("cache-budget-validation")
        .topology(topology)
        .cache_max_bytes(-1)
        .build()
        .await
        .err()
        .expect("invalid configuration");

    assert2::assert!(
        error
            .to_string()
            .contains("streams state-store cache max bytes")
    );
}
```

- [ ] **Step 4: Add a failing `StreamsApp` compatibility test**

In `crates/client-streams/src/streams_app.rs`, add:

```rust
#[test]
fn state_store_cache_budget_preserves_raw_builder_default_and_override() {
    let defaults = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("cache-default")
        .schema_registry("http://127.0.0.1:8081")
        .build();
    assert_eq!(defaults.cache_max_bytes, 10_485_760);

    let overridden = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("cache-override")
        .schema_registry("http://127.0.0.1:8081")
        .cache_max_bytes(37)
        .build();
    assert_eq!(overridden.cache_max_bytes, 37);
}
```

- [ ] **Step 5: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams state_store_cache --locked
```

Expected: compilation fails because the new semantic type and constants do not
exist.

- [ ] **Step 6: Implement the minimal validated semantic type**

In `crates/client-streams/src/runtime/app.rs`, import
`refined_type::rule::MinMaxI64` and add beside the other runtime configuration
types:

```rust
/// Default Client Streams state-store record-cache budget in bytes.
pub const DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES: i64 = 10_485_760;

/// Largest cache budget representable by both the public `i64` API and the
/// target's internal `usize` cache accounting.
pub const MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES: i64 = if usize::BITS >= i64::BITS {
    i64::MAX
} else {
    usize::MAX as i64
};

/// Target-supported Client Streams state-store record-cache budget in bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamsStateStoreCacheMaxBytes(i64);

impl StreamsStateStoreCacheMaxBytes {
    /// Validate a state-store cache byte budget.
    ///
    /// Zero disables caching.
    ///
    /// # Errors
    ///
    /// Returns an error for negative values or values that cannot be represented
    /// by the target's internal cache accounting.
    pub fn new(value: i64) -> Result<Self, String> {
        MinMaxI64::<0, MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("streams state-store cache max bytes: {error}"))
    }

    /// Return the validated byte budget.
    #[must_use]
    pub const fn bytes(self) -> i64 {
        self.0
    }
}

impl Default for StreamsStateStoreCacheMaxBytes {
    fn default() -> Self {
        Self::new(DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES)
            .expect("default streams state-store cache max bytes is valid")
    }
}
```

Do not add a generic byte-size type or parsing helper.

- [ ] **Step 7: Validate once before runtime side effects**

Change `validate_runtime_configuration` to accept `cache_max_bytes: i64`,
construct `StreamsStateStoreCacheMaxBytes`, and return it as the fifth tuple
member:

```rust
let cache_max_bytes = StreamsStateStoreCacheMaxBytes::new(cache_max_bytes)
    .map_err(StreamsClientError::Runtime)?;
```

In `KafkaStreams::start`, replace the literal builder default with:

```rust
#[builder(default = DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES)]
cache_max_bytes: i64,
```

Pass `cache_max_bytes` into `validate_runtime_configuration`, bind the returned
typed value, and immediately recover its compatible raw representation:

```rust
let (
    poll_interval,
    commit_interval,
    rebalance_timeout,
    join_retry_backoff,
    cache_max_bytes,
) = validate_runtime_configuration(
    poll_interval,
    commit_interval,
    rebalance_timeout,
    join_retry_backoff,
    cache_max_bytes,
)?;
let cache_max_bytes = cache_max_bytes.bytes();
```

Leave `StreamThread::new`, `BuiltTopology::instantiate`, `Graph`, and
`ThreadCache` signatures unchanged.

- [ ] **Step 8: Reuse the default constant and export the public type**

In `crates/client-streams/src/streams_app.rs`, import
`DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES` and replace the literal builder
default with it. Keep the field and setter as `i64`.

Re-export `DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES`,
`MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES`, and
`StreamsStateStoreCacheMaxBytes` from `runtime/mod.rs` and the crate root.

- [ ] **Step 9: Run focused GREEN and package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams state_store_cache --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams low_level_runtime_validation_names_the_invalid_field --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
git diff --check
git diff -- Cargo.lock
```

Expected: all tests and checks pass; `Cargo.lock` has no diff.

- [ ] **Step 10: Commit Task 1**

Stage only the four Task 1 files and commit:

```bash
git add -- \
  crates/client-streams/src/runtime/app.rs \
  crates/client-streams/src/runtime/mod.rs \
  crates/client-streams/src/streams_app.rs \
  crates/client-streams/src/lib.rs
git commit -m "feat(streams): validate cache byte budget"
```

---

### Task 2: Expose the demo CLI, environment, and Compose setting

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/streams_state_store_cache_config.rs`
- Modify:
  `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `StreamsStateStoreCacheMaxBytes`
- Produces: `--streams-state-store-cache-max-bytes`
- Produces: `CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES`
- Produces: validated raw bytes passed to the existing
  `StreamsApp::cache_max_bytes(i64)` setter

- [ ] **Step 1: Add failing hermetic subprocess tests**

Create
`crates/observability-demo-app/tests/streams_state_store_cache_config.rs`:

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
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "37")
        .output()
        .expect("run demo");
    assert!(!environment.status.success());
    assert!(String::from_utf8_lossy(&environment.stderr).contains(
        "--streams-state-store-cache-max-bytes (37) is only valid with --role stream"
    ));

    let cli = demo()
        .args([
            "--role",
            "produce",
            "--streams-state-store-cache-max-bytes",
            "41",
        ])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "37")
        .output()
        .expect("run demo");
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr).contains(
        "--streams-state-store-cache-max-bytes (41) is only valid with --role stream"
    ));
}

#[test]
fn negative_fails_early_zero_is_parseable_and_help_lists_the_flag_once() {
    let negative = demo()
        .args(["--role", "stream"])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "-1")
        .output()
        .expect("run demo");
    assert!(!negative.status.success());
    assert!(
        String::from_utf8_lossy(&negative.stderr)
            .contains("streams state-store cache max bytes")
    );

    let zero = demo()
        .args(["--role", "produce"])
        .env("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES", "0")
        .output()
        .expect("run demo");
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains(
        "--streams-state-store-cache-max-bytes (0) is only valid with --role stream"
    ));

    let help = demo().arg("--help").output().expect("help");
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).expect("UTF-8 help");
    assert_eq!(
        help.split_whitespace()
            .filter(|token| *token == "--streams-state-store-cache-max-bytes")
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
    "CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES: \"${CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES:-10485760}\""
));
```

Add this assertion inside the existing Produce/Consume loop:

```rust
assert2::assert!(
    !service.contains("CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES")
);
```

- [ ] **Step 3: Run focused tests and record RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_state_store_cache_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
```

Expected: subprocess tests fail because Clap does not know the option; the
Compose test fails because the Stream service lacks the variable.

- [ ] **Step 4: Add the demo option and early typed resolver**

In `crates/observability-demo-app/src/main.rs`, import
`StreamsStateStoreCacheMaxBytes` and add to `Cli`:

```rust
/// Client Streams state-store record-cache budget in bytes; zero disables it.
#[arg(long, env = "CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES")]
streams_state_store_cache_max_bytes: Option<i64>,
```

Add:

```rust
fn effective_streams_state_store_cache_max_bytes(
    cli: &Cli,
) -> std::io::Result<StreamsStateStoreCacheMaxBytes> {
    if cli.role != Role::Stream
        && let Some(bytes) = cli.streams_state_store_cache_max_bytes
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "--streams-state-store-cache-max-bytes ({bytes}) is only valid with --role stream"
            ),
        ));
    }

    cli.streams_state_store_cache_max_bytes.map_or_else(
        || Ok(StreamsStateStoreCacheMaxBytes::default()),
        |bytes| {
            StreamsStateStoreCacheMaxBytes::new(bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        },
    )
}
```

Call the resolver with the other Stream-only resolvers before telemetry
initialization.

- [ ] **Step 5: Route the validated bytes to `StreamsApp`**

Add a `StreamsStateStoreCacheMaxBytes` parameter to `run_stream`, pass it from
`main`, and add this existing-builder call:

```rust
.cache_max_bytes(streams_state_store_cache_max_bytes.bytes())
```

Do not change the `StreamsApp` builder setter type.

- [ ] **Step 6: Add only the Stream Compose variable**

In `demo/observability/docker-compose.yml`, add to the `demo-stream`
environment:

```yaml
CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES: "${CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES:-10485760}"
```

Do not add it to `demo-produce`, `demo-consume`, or a shared anchor.

- [ ] **Step 7: Run focused GREEN and demo gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test streams_state_store_cache_config --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --test observability_demo_config streams_runtime_policy_is_configurable_only_on_the_stream_role --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--streams-state-store-cache-max-bytes' | wc -l
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
  crates/observability-demo-app/tests/streams_state_store_cache_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose cache byte budget"
```

---

### Task 3: Record the completed owner and final verification

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: the completed library, demo, and Compose behavior from Tasks 1-2
- Produces: an exclusive focused-search classification and the next
  production-consumed configuration owner

- [ ] **Step 1: Run the repository scanner and focused owner search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "cache_max_bytes|StreamsStateStoreCacheMaxBytes|DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES|MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES|streams-state-store-cache-max-bytes|STREAMS_STATE_STORE_CACHE_MAX_BYTES|statestore.cache.max.bytes" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

Record the exact scanner line/file totals and classify every focused line
exactly once as Client Streams production, demo policy, demo deployment, test
or harness, prior audit, or unresolved owner. Verify the category counts sum to
the focused-search total.

- [ ] **Step 2: Append the completed audit section**

Append `## Client Streams State-Store Cache Budget` to
`docs/configuration-audit.md`. Include:

- the validated range, 10 MiB default, and zero-as-disable semantics;
- the compatibility-preserving raw setters and single pre-I/O validation
  boundary;
- the exact `StreamsApp -> KafkaStreams -> StreamThread -> instantiate ->
  ThreadCache` flow;
- the demo CLI, environment, precedence, role restriction, and Compose owner;
- the reason no CRD exists;
- the exact scanner and focused-search commands and measured classifications;
- the verification results from all three tasks; and
- an `### Adjacent Pending Policy` naming the next scanner-visible value with a
  real production consumer while excluding protocol and test invariants.

Do not claim repository-wide completion.

- [ ] **Step 3: Run fresh combined final gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-streams -p observability-demo-app --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-streams -p observability-demo-app --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all -- --check
./target/debug/observability-demo-app --help | grep -o -- '--streams-state-store-cache-max-bytes' | wc -l
git diff --check
git diff -- Cargo.lock
```

Expected: every test and lint passes; the help count is exactly `1`; the
lockfile diff is empty.

- [ ] **Step 4: Commit Task 3**

Stage only the audit and commit:

```bash
git add -- docs/configuration-audit.md
git commit -m "docs(streams): record cache byte budget"
```

- [ ] **Step 5: Review the complete slice**

Run:

```bash
git diff --stat 0e2d1d6f..HEAD
git diff --check 0e2d1d6f..HEAD
git diff 0e2d1d6f..HEAD -- Cargo.lock
git status --short
```

Confirm the range contains only the intended library, demo, Compose, audit, and
test files; `Cargo.lock` is unchanged; all pre-existing unrelated dirty and
untracked files remain unstaged.
