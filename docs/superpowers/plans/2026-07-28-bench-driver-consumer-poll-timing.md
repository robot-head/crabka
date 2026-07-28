# Bench Driver Consumer Poll Timing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed 50-millisecond consumer poll wait and
100-millisecond poll-error sleep with two validated settings while preserving
the consumer loop.

**Architecture:** Parse one reusable positive-millisecond refined newtype at
the existing Clap boundary and store two role-named values in `DriverConfig`.
Copy both values through `ConsumerTask` to the sole poll and poll-error sleep
sites. Reuse the existing shell `envsubst` deployment path.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Bash, Kubernetes YAML.

## Global Constraints

- Preserve defaults: 50 ms poll timeout and 100 ms poll-error backoff.
- CLI overrides environment for both settings.
- Reject zero, malformed, negative, and primitive-overflow values.
- Preserve the consumer loop, stop checks, message processing, first-error
  recording, close behavior, client construction, and all other timing.
- Do not add a policy wrapper: the values have no cross-field invariant.
- Do not expose sampling or producer final-drain timing in this slice.
- Add no CRD; the benchmark launcher and Job template own this binary.
- `crabka-bench-driver` already directly depends on the workspace-pinned
  `refined_type`; do not change dependencies or `Cargo.lock`.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Preserve unrelated dirty and untracked files; stage only paths named by the
  current task.

---

## File Map

- `crates/bench-driver/src/workload.rs`: validated type, named defaults,
  configuration storage, task propagation, and runtime consumption.
- `crates/bench-driver/src/main.rs`: Clap inputs and CLI/environment tests.
- `bench/scripts/run-scenario.sh`: overrideable defaults and exports.
- `bench/manifests/driver/job-template.yaml`: driver environment wiring.
- `docs/configuration-audit.md`: evidence and next unresolved owner.

### Task 1: Expose and propagate consumer poll timing

**Files:**

- Modify: `crates/bench-driver/src/workload.rs`
- Modify: `crates/bench-driver/src/main.rs`

**Interfaces:**

- Produces: `pub const DEFAULT_CONSUMER_POLL_TIMEOUT_MS: u64 = 50`
- Produces: `pub const DEFAULT_CONSUMER_POLL_ERROR_BACKOFF_MS: u64 = 100`
- Produces: `pub struct ConsumerPollDurationMs(u64)`
- Produces: `FromStr`, `Display`, and `duration()`
- Produces: `DriverConfig::{consumer_poll_timeout, consumer_poll_error_backoff}`
- Consumes: existing `refined_type::rule::GreaterU64`

- [ ] **Step 1: Add failing validated-type tests**

In `crates/bench-driver/src/workload.rs`, add:

```rust
#[test]
fn consumer_poll_timing_defaults_preserve_behavior() {
    assert_eq!(
        default_consumer_poll_timeout().duration(),
        Duration::from_millis(50)
    );
    assert_eq!(
        default_consumer_poll_error_backoff().duration(),
        Duration::from_millis(100)
    );
}

#[test]
fn consumer_poll_duration_accepts_positive_minimum() {
    assert_eq!(
        ConsumerPollDurationMs::new(1)
            .expect("one millisecond is valid")
            .duration(),
        Duration::from_millis(1)
    );
}

#[test]
fn consumer_poll_duration_rejects_invalid_values() {
    for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
        assert!(
            invalid.parse::<ConsumerPollDurationMs>().is_err(),
            "{invalid:?} must be rejected"
        );
    }
}
```

- [ ] **Step 2: Add failing CLI/default/precedence tests**

In `crates/bench-driver/src/main.rs`, add:

```rust
#[test]
fn consumer_poll_timing_cli_defaults_preserve_behavior() {
    let cli = Cli::try_parse_from(required_args("crabka")).expect("poll defaults");

    assert_eq!(
        cli.consumer_poll_timeout_ms.duration(),
        Duration::from_millis(50)
    );
    assert_eq!(
        cli.consumer_poll_error_backoff_ms.duration(),
        Duration::from_millis(100)
    );
}

#[test]
fn consumer_poll_timing_rejects_invalid_cli_values() {
    for option in [
        "--consumer-poll-timeout-ms",
        "--consumer-poll-error-backoff-ms",
    ] {
        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            let mut args = required_args("crabka");
            args.extend([option, invalid]);
            assert!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
        }
    }
}
```

Add a child-process test that avoids process-global environment races:

```rust
#[test]
fn consumer_poll_timing_reads_environment_and_prefers_cli() {
    const CHILD: &str = "CRABKA_BENCH_CONSUMER_POLL_TIMING_CHILD";

    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(
            std::env::current_exe().expect("test executable"),
        )
        .args([
            "--exact",
            "tests::consumer_poll_timing_reads_environment_and_prefers_cli",
        ])
        .env(CHILD, "1")
        .env("BENCH_CONSUMER_POLL_TIMEOUT_MS", "11")
        .env("BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS", "12")
        .status()
        .expect("child test");
        assert!(status.success());
        return;
    }

    let from_env = Cli::try_parse_from(required_args("crabka")).expect("environment");
    assert_eq!(
        from_env.consumer_poll_timeout_ms.duration(),
        Duration::from_millis(11)
    );
    assert_eq!(
        from_env.consumer_poll_error_backoff_ms.duration(),
        Duration::from_millis(12)
    );

    let mut args = required_args("crabka");
    args.extend([
        "--consumer-poll-timeout-ms",
        "21",
        "--consumer-poll-error-backoff-ms",
        "22",
    ]);
    let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
    assert_eq!(
        from_cli.consumer_poll_timeout_ms.duration(),
        Duration::from_millis(21)
    );
    assert_eq!(
        from_cli.consumer_poll_error_backoff_ms.duration(),
        Duration::from_millis(22)
    );
}
```

- [ ] **Step 3: Run focused tests to verify RED**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver consumer_poll --locked
```

Expected: compilation fails because the newtype, defaults, and CLI fields do
not exist.

- [ ] **Step 4: Implement the minimal validated type and defaults**

In `crates/bench-driver/src/workload.rs`, add:

```rust
pub const DEFAULT_CONSUMER_POLL_TIMEOUT_MS: u64 = 50;
pub const DEFAULT_CONSUMER_POLL_ERROR_BACKOFF_MS: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerPollDurationMs(u64);

impl ConsumerPollDurationMs {
    pub fn new(value: u64) -> Result<Self, String> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
}

impl fmt::Display for ConsumerPollDurationMs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConsumerPollDurationMs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
```

Add role-specific default helpers:

```rust
#[must_use]
pub fn default_consumer_poll_timeout() -> ConsumerPollDurationMs {
    ConsumerPollDurationMs::new(DEFAULT_CONSUMER_POLL_TIMEOUT_MS)
        .expect("default consumer poll timeout is positive")
}

#[must_use]
pub fn default_consumer_poll_error_backoff() -> ConsumerPollDurationMs {
    ConsumerPollDurationMs::new(DEFAULT_CONSUMER_POLL_ERROR_BACKOFF_MS)
        .expect("default consumer poll-error backoff is positive")
}
```

Add standard `# Errors` documentation to `new` and accurate `# Panics`
documentation to both default helpers.

- [ ] **Step 5: Add typed CLI inputs**

Import `ConsumerPollDurationMs` in `crates/bench-driver/src/main.rs`. Add after
the consumer-build retry fields:

```rust
/// Consumer poll timeout, in milliseconds.
#[arg(
    long,
    env = "BENCH_CONSUMER_POLL_TIMEOUT_MS",
    default_value_t = workload::default_consumer_poll_timeout()
)]
consumer_poll_timeout_ms: ConsumerPollDurationMs,

/// Sleep after a consumer poll error, in milliseconds.
#[arg(
    long,
    env = "BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS",
    default_value_t = workload::default_consumer_poll_error_backoff()
)]
consumer_poll_error_backoff_ms: ConsumerPollDurationMs,
```

Store both values directly in `DriverConfig`; Clap completes their validation
before `Cli::parse()` returns.

- [ ] **Step 6: Propagate both values to the consumer loop**

Add these fields to `DriverConfig` and `ConsumerTask`:

```rust
consumer_poll_timeout: ConsumerPollDurationMs,
consumer_poll_error_backoff: ConsumerPollDurationMs,
```

Initialize the workload test helper with the two default helpers. Copy both
fields when spawning consumer tasks and destructure them in `run_consumer`.

Replace only:

```rust
consumer.poll(Duration::from_millis(50))
```

with:

```rust
consumer.poll(poll_timeout.duration())
```

and replace only:

```rust
tokio::time::sleep(Duration::from_millis(100))
```

with:

```rust
tokio::time::sleep(poll_error_backoff.duration())
```

- [ ] **Step 7: Run focused tests to verify GREEN**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver consumer_poll --locked
```

Expected: all focused type, default, parser, and precedence tests pass.

- [ ] **Step 8: Prove the single runtime flows**

```bash
test "$(rg -o 'consumer\\.poll\\(' crates/bench-driver/src/workload.rs | wc -l)" -eq 1
test "$(rg -o 'tokio::time::sleep\\(poll_error_backoff\\.duration\\(\\)\\)' \
  crates/bench-driver/src/workload.rs | wc -l)" -eq 1
if rg -n 'consumer\\.poll\\(Duration::from_millis\\(50\\)\\)|tokio::time::sleep\\(Duration::from_millis\\(100\\)\\)' \
  crates/bench-driver/src/workload.rs; then
  exit 1
fi
rg -n 'consumer_poll_(timeout|error_backoff)|ConsumerPollDurationMs' \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/workload.rs
```

Expected: one poll and one error-sleep site consume the typed settings; the
old literals are absent; and the focused search shows both complete flows.

- [ ] **Step 9: Run package gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-bench-driver --bin crabka-bench-driver --locked -- --help
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-poll-timeout-ms')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-poll-error-backoff-ms')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

- [ ] **Step 10: Review and commit the Rust implementation**

```bash
git diff -- crates/bench-driver/src/main.rs crates/bench-driver/src/workload.rs
git add crates/bench-driver/src/main.rs crates/bench-driver/src/workload.rs
git commit -m "feat(bench): expose consumer poll timing"
```

### Task 2: Wire poll timing through benchmark Jobs

**Files:**

- Modify: `bench/scripts/run-scenario.sh`
- Modify: `bench/manifests/driver/job-template.yaml`

- [ ] **Step 1: Add launcher defaults and exports**

Document both variables in the script header. Near the other consumer defaults
add:

```bash
: "${BENCH_CONSUMER_POLL_TIMEOUT_MS:=50}"
: "${BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS:=100}"
```

Export both before rendering the Job.

- [ ] **Step 2: Add Job environment entries**

Document both variables in the template header. Add:

```yaml
- name: BENCH_CONSUMER_POLL_TIMEOUT_MS
  value: "${BENCH_CONSUMER_POLL_TIMEOUT_MS}"
- name: BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS
  value: "${BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS}"
```

- [ ] **Step 3: Validate shell syntax and rendered values**

```bash
bash -n bench/scripts/run-scenario.sh
rg -n 'BENCH_CONSUMER_POLL_TIMEOUT_MS:=50|BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS:=100' \
  bench/scripts/run-scenario.sh
BENCH_CONSUMER_POLL_TIMEOUT_MS=50 \
BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS=100 \
  envsubst '$BENCH_CONSUMER_POLL_TIMEOUT_MS $BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_POLL_(TIMEOUT_MS|ERROR_BACKOFF_MS)'
BENCH_CONSUMER_POLL_TIMEOUT_MS=21 \
BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS=22 \
  envsubst '$BENCH_CONSUMER_POLL_TIMEOUT_MS $BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_POLL_(TIMEOUT_MS|ERROR_BACKOFF_MS)'
git diff --check
```

- [ ] **Step 4: Review and commit deployment wiring**

```bash
git diff -- bench/scripts/run-scenario.sh bench/manifests/driver/job-template.yaml
git add bench/scripts/run-scenario.sh bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire consumer poll timing"
```

### Task 3: Close the audit slice

**Files:**

- Modify: `docs/configuration-audit.md`

- [ ] **Step 1: Capture exact audit evidence**

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/'
rg -n 'consumer_poll_(timeout|error_backoff)|ConsumerPollDurationMs|DEFAULT_CONSUMER_POLL_(TIMEOUT|ERROR_BACKOFF)_MS|consumer-poll-(timeout|error-backoff)-ms|BENCH_CONSUMER_POLL_(TIMEOUT|ERROR_BACKOFF)_MS' \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
rg -n 'Duration::|_seconds|_millis|timeout|interval|backoff|capacity|limit' \
  crates/bench-driver/src \
  bench/scripts \
  bench/manifests
```

Record exact counts and mutually exclusive classifications. Inspect remaining
scanner output and name the next real unresolved operational owner; do not
classify dependency mechanics, invariants, tests, scenario inputs, or
configured defaults as unresolved.

- [ ] **Step 2: Append the audit section**

Append `## Bench Driver Consumer Poll Timing` with:

- the 50/100 defaults and both CLI/environment pairs;
- positive validation and CLI precedence;
- both complete task/runtime flows;
- preserved consumer-loop behavior;
- launcher and Job-template wiring and why no CRD exists;
- scanner and focused-search counts and classifications;
- verification evidence; and
- the next real unresolved repository owner.

- [ ] **Step 3: Re-run final verification**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
bash -n bench/scripts/run-scenario.sh
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-poll-timeout-ms')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-poll-error-backoff-ms')" -eq 1
BENCH_CONSUMER_POLL_TIMEOUT_MS=21 \
BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS=22 \
  envsubst '$BENCH_CONSUMER_POLL_TIMEOUT_MS $BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_POLL_(TIMEOUT_MS|ERROR_BACKOFF_MS)'
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

- [ ] **Step 4: Review and commit the audit**

```bash
git diff -- docs/configuration-audit.md
git add docs/configuration-audit.md
git commit -m "docs(audit): record consumer poll timing"
```

After the commit, inspect `git status --short` and confirm only the unrelated
untracked plans remain.
