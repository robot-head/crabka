# Bench Driver Consumer Build Retry Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed six-attempt, 100-to-2,000-millisecond
consumer-build retry policy with three validated settings while preserving the
retry loop.

**Architecture:** Parse two refined newtypes at the existing Clap boundary,
combine them into one relationally validated `ConsumerBuildRetryPolicy` before
scenario-file I/O, and copy that immutable policy through `DriverConfig` and
`ConsumerTask` to the sole backoff constructor. Reuse the existing shell
`envsubst` deployment path.

**Tech Stack:** Rust 2024, Clap, `refined_type`, `exponential-backoff`, Bash,
Kubernetes YAML.

## Global Constraints

- Preserve defaults: 6 attempts, 100 ms initial backoff, 2,000 ms maximum
  backoff.
- CLI overrides environment for all three settings.
- Reject zero, malformed, negative, and primitive-overflow values.
- Reject initial backoff above maximum before scenario-file or network I/O.
- Preserve retry-on-all-build-errors behavior, warning fields, attempt
  numbering, terminal error, client request timeout, TLS, polling, and
  poll-error backoff.
- Keep dependency growth factor 2 and jitter 0.3 fixed.
- Do not expose sampling, final-drain, request-timeout, or Prometheus policies
  in this slice.
- Add no CRD; the benchmark launcher and Job template own this binary.
- `crabka-bench-driver` already directly depends on `refined_type`; do not
  change dependencies or `Cargo.lock`.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Preserve unrelated dirty and untracked files; stage only paths named by the
  current task.

---

## File Map

- `crates/bench-driver/src/workload.rs`: validated types, policy, configuration
  storage, task propagation, and backoff construction.
- `crates/bench-driver/src/main.rs`: Clap inputs, early policy resolution, and
  CLI/environment tests.
- `bench/scripts/run-scenario.sh`: overrideable defaults and exports.
- `bench/manifests/driver/job-template.yaml`: driver environment wiring.
- `docs/configuration-audit.md`: evidence and next unresolved owner.

### Task 1: Expose and propagate the consumer-build retry policy

**Files:**

- Modify: `crates/bench-driver/src/workload.rs`
- Modify: `crates/bench-driver/src/main.rs`

**Interfaces:**

- Produces: `pub const DEFAULT_CONSUMER_BUILD_ATTEMPTS: u32 = 6`
- Produces:
  `pub const DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF_MS: u64 = 100`
- Produces:
  `pub const DEFAULT_CONSUMER_BUILD_MAX_BACKOFF_MS: u64 = 2_000`
- Produces: `pub struct ConsumerBuildAttempts(u32)`
- Produces: `pub struct ConsumerBuildBackoffMs(u64)`
- Produces: `pub struct ConsumerBuildRetryPolicy`
- Produces:
  `ConsumerBuildRetryPolicy::new(attempts, initial, max) -> Result<Self, String>`
- Produces: `FromStr`, `Display`, and typed accessors.
- Produces: `DriverConfig::consumer_build_retry_policy`
- Consumes: `refined_type::rule::{GreaterU32, GreaterU64}`

- [ ] **Step 1: Add failing validated-type and policy tests**

Replace the remaining `CONSUMER_BUILD_ATTEMPTS == 6` assertion in the
client-timeout default test with a separate retry-policy test in
`crates/bench-driver/src/workload.rs`:

```rust
#[test]
fn consumer_build_retry_defaults_preserve_policy() {
    let policy = ConsumerBuildRetryPolicy::default();

    assert_eq!(policy.attempts(), 6);
    assert_eq!(policy.initial_backoff(), Duration::from_millis(100));
    assert_eq!(policy.max_backoff(), Duration::from_millis(2_000));
}

#[test]
fn consumer_build_retry_accepts_positive_minimum_and_equal_backoffs() {
    let attempts = ConsumerBuildAttempts::new(1).expect("one attempt is valid");
    let one_ms = ConsumerBuildBackoffMs::new(1).expect("one millisecond is valid");
    let policy =
        ConsumerBuildRetryPolicy::new(attempts, one_ms, one_ms).expect("equal bounds are valid");

    assert_eq!(policy.attempts(), 1);
    assert_eq!(policy.initial_backoff(), Duration::from_millis(1));
    assert_eq!(policy.max_backoff(), Duration::from_millis(1));
}

#[test]
fn consumer_build_retry_rejects_invalid_primitive_values() {
    for invalid in ["0", "not-a-number", "-1", "4294967296"] {
        assert!(
            invalid.parse::<ConsumerBuildAttempts>().is_err(),
            "attempts {invalid:?} must be rejected"
        );
    }
    for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
        assert!(
            invalid.parse::<ConsumerBuildBackoffMs>().is_err(),
            "backoff {invalid:?} must be rejected"
        );
    }
}

#[test]
fn consumer_build_retry_rejects_inverted_backoff_range() {
    let attempts = ConsumerBuildAttempts::new(1).expect("valid attempts");
    let initial = ConsumerBuildBackoffMs::new(2).expect("valid initial");
    let max = ConsumerBuildBackoffMs::new(1).expect("valid maximum");

    assert!(ConsumerBuildRetryPolicy::new(attempts, initial, max).is_err());
}
```

- [ ] **Step 2: Add failing CLI/default/precedence tests**

In `crates/bench-driver/src/main.rs`, add:

```rust
#[test]
fn consumer_build_retry_cli_defaults_preserve_policy() {
    let cli = Cli::try_parse_from(required_args("crabka")).expect("retry defaults");
    let policy = resolve_consumer_build_retry_policy(&cli).expect("valid defaults");

    assert_eq!(policy.attempts(), 6);
    assert_eq!(policy.initial_backoff(), Duration::from_millis(100));
    assert_eq!(policy.max_backoff(), Duration::from_millis(2_000));
}

#[test]
fn consumer_build_retry_rejects_invalid_cli_values() {
    let cases = [
        ("--consumer-build-attempts", "0"),
        ("--consumer-build-attempts", "4294967296"),
        ("--consumer-build-initial-backoff-ms", "0"),
        (
            "--consumer-build-initial-backoff-ms",
            "18446744073709551616",
        ),
        ("--consumer-build-max-backoff-ms", "0"),
        ("--consumer-build-max-backoff-ms", "18446744073709551616"),
    ];
    for (option, invalid) in cases {
        let mut args = required_args("crabka");
        args.extend([option, invalid]);
        assert!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
    }
}

#[test]
fn consumer_build_retry_rejects_inverted_cli_range() {
    let mut args = required_args("crabka");
    args.extend([
        "--consumer-build-initial-backoff-ms",
        "2",
        "--consumer-build-max-backoff-ms",
        "1",
    ]);
    let cli = Cli::try_parse_from(args).expect("individual values are valid");

    assert!(resolve_consumer_build_retry_policy(&cli).is_err());
}
```

Add a child-process test that sets all three environment variables, asserts
them, supplies all three CLI flags, and proves the CLI values win:

```rust
#[test]
fn consumer_build_retry_reads_environment_and_prefers_cli() {
    const CHILD: &str = "CRABKA_BENCH_CONSUMER_BUILD_RETRY_CHILD";

    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(
            std::env::current_exe().expect("test executable"),
        )
        .args([
            "--exact",
            "tests::consumer_build_retry_reads_environment_and_prefers_cli",
        ])
        .env(CHILD, "1")
        .env("BENCH_CONSUMER_BUILD_ATTEMPTS", "2")
        .env("BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS", "11")
        .env("BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS", "12")
        .status()
        .expect("child test");
        assert!(status.success());
        return;
    }

    let from_env = Cli::try_parse_from(required_args("crabka")).expect("environment");
    let environment_policy =
        resolve_consumer_build_retry_policy(&from_env).expect("valid environment");
    assert_eq!(environment_policy.attempts(), 2);
    assert_eq!(environment_policy.initial_backoff(), Duration::from_millis(11));
    assert_eq!(environment_policy.max_backoff(), Duration::from_millis(12));

    let mut args = required_args("crabka");
    args.extend([
        "--consumer-build-attempts",
        "3",
        "--consumer-build-initial-backoff-ms",
        "21",
        "--consumer-build-max-backoff-ms",
        "22",
    ]);
    let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
    let cli_policy = resolve_consumer_build_retry_policy(&from_cli).expect("valid CLI");
    assert_eq!(cli_policy.attempts(), 3);
    assert_eq!(cli_policy.initial_backoff(), Duration::from_millis(21));
    assert_eq!(cli_policy.max_backoff(), Duration::from_millis(22));
}
```

- [ ] **Step 3: Run focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver consumer_build_retry --locked
```

Expected: compilation fails because the retry newtypes, policy, resolver, and
CLI fields do not exist.

- [ ] **Step 4: Implement the minimal validated types and policy**

In `crates/bench-driver/src/workload.rs`, extend the refined import:

```rust
use refined_type::rule::{GreaterU32, GreaterU64, MinMaxU64};
```

Replace `CONSUMER_BUILD_ATTEMPTS` with named defaults and add:

```rust
pub const DEFAULT_CONSUMER_BUILD_ATTEMPTS: u32 = 6;
pub const DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF_MS: u64 = 100;
pub const DEFAULT_CONSUMER_BUILD_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerBuildAttempts(u32);

impl ConsumerBuildAttempts {
    pub fn new(value: u32) -> Result<Self, String> {
        GreaterU32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> u32 {
        self.0
    }
}

impl Default for ConsumerBuildAttempts {
    fn default() -> Self {
        Self::new(DEFAULT_CONSUMER_BUILD_ATTEMPTS)
            .expect("default consumer-build attempts are positive")
    }
}

impl fmt::Display for ConsumerBuildAttempts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConsumerBuildAttempts {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerBuildBackoffMs(u64);

impl ConsumerBuildBackoffMs {
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

impl fmt::Display for ConsumerBuildBackoffMs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ConsumerBuildBackoffMs {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
```

Add default helpers for the two backoff roles, with accurate `# Panics`
documentation because they validate named constants with `expect`:

```rust
#[must_use]
pub fn default_consumer_build_initial_backoff() -> ConsumerBuildBackoffMs {
    ConsumerBuildBackoffMs::new(DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF_MS)
        .expect("default initial consumer-build backoff is positive")
}

#[must_use]
pub fn default_consumer_build_max_backoff() -> ConsumerBuildBackoffMs {
    ConsumerBuildBackoffMs::new(DEFAULT_CONSUMER_BUILD_MAX_BACKOFF_MS)
        .expect("default maximum consumer-build backoff is positive")
}
```

Add the policy:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerBuildRetryPolicy {
    attempts: ConsumerBuildAttempts,
    initial_backoff: ConsumerBuildBackoffMs,
    max_backoff: ConsumerBuildBackoffMs,
}

impl ConsumerBuildRetryPolicy {
    pub fn new(
        attempts: ConsumerBuildAttempts,
        initial_backoff: ConsumerBuildBackoffMs,
        max_backoff: ConsumerBuildBackoffMs,
    ) -> Result<Self, String> {
        if initial_backoff.duration() > max_backoff.duration() {
            return Err("consumer-build initial backoff exceeds maximum".to_owned());
        }
        Ok(Self {
            attempts,
            initial_backoff,
            max_backoff,
        })
    }

    #[must_use]
    pub const fn attempts(self) -> u32 {
        self.attempts.into_value()
    }

    #[must_use]
    pub const fn initial_backoff(self) -> Duration {
        self.initial_backoff.duration()
    }

    #[must_use]
    pub const fn max_backoff(self) -> Duration {
        self.max_backoff.duration()
    }
}

impl Default for ConsumerBuildRetryPolicy {
    fn default() -> Self {
        Self::new(
            ConsumerBuildAttempts::default(),
            default_consumer_build_initial_backoff(),
            default_consumer_build_max_backoff(),
        )
        .expect("default consumer-build retry range is ordered")
    }
}
```

Add standard `# Errors` documentation to public `new` methods and `# Panics`
documentation to both public default helpers so strict workspace lints pass.

- [ ] **Step 5: Add CLI inputs and early relational resolution**

Import the three types in `crates/bench-driver/src/main.rs`. Add after the
client request-timeout fields:

```rust
/// Maximum consumer build attempts.
#[arg(
    long,
    env = "BENCH_CONSUMER_BUILD_ATTEMPTS",
    default_value_t = ConsumerBuildAttempts::default()
)]
consumer_build_attempts: ConsumerBuildAttempts,

/// Initial consumer-build retry backoff, in milliseconds.
#[arg(
    long,
    env = "BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS",
    default_value_t = workload::default_consumer_build_initial_backoff()
)]
consumer_build_initial_backoff_ms: ConsumerBuildBackoffMs,

/// Maximum consumer-build retry backoff, in milliseconds.
#[arg(
    long,
    env = "BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS",
    default_value_t = workload::default_consumer_build_max_backoff()
)]
consumer_build_max_backoff_ms: ConsumerBuildBackoffMs,
```

Add:

```rust
fn resolve_consumer_build_retry_policy(cli: &Cli) -> Result<ConsumerBuildRetryPolicy> {
    ConsumerBuildRetryPolicy::new(
        cli.consumer_build_attempts,
        cli.consumer_build_initial_backoff_ms,
        cli.consumer_build_max_backoff_ms,
    )
    .map_err(anyhow::Error::msg)
}
```

Call this immediately after `Cli::parse()` and before
`tokio::fs::read_to_string`. Store the resulting policy in `DriverConfig`.

- [ ] **Step 6: Propagate the complete policy to the retry loop**

Add `consumer_build_retry_policy: ConsumerBuildRetryPolicy` to `DriverConfig`
and initialize the workload test helper with `ConsumerBuildRetryPolicy::default()`.

Add the same field to `ConsumerTask`, copy it when spawning, and pass it to
`build_consumer_with_retry`.

Change the backoff constructor only:

```rust
let backoff = exponential_backoff::Backoff::new(
    retry_policy.attempts(),
    retry_policy.initial_backoff(),
    Some(retry_policy.max_backoff()),
);
```

Do not change the loop body, warnings, errors, or any client builder input.

- [ ] **Step 7: Run focused tests to verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver consumer_build_retry --locked
```

Expected: all focused type, default, relational, parser, and precedence tests
pass.

- [ ] **Step 8: Prove validation ordering and the single runtime flow**

Run:

```bash
policy_line=$(sed -n '1,/^#\[cfg(test)\]/p' crates/bench-driver/src/main.rs \
  | rg -n 'resolve_consumer_build_retry_policy\(&cli\)' \
  | cut -d: -f1)
read_line=$(rg -n 'tokio::fs::read_to_string' crates/bench-driver/src/main.rs | cut -d: -f1)
test "$policy_line" -lt "$read_line"
test "$(rg -o 'exponential_backoff::Backoff::new' crates/bench-driver/src/workload.rs | wc -l)" -eq 1
if rg -n '^const CONSUMER_BUILD_ATTEMPTS' crates/bench-driver/src/workload.rs; then
  exit 1
fi
backoff_block=$(sed -n '/let backoff = exponential_backoff::Backoff::new(/,+4p' \
  crates/bench-driver/src/workload.rs)
rg -q 'retry_policy\.attempts()' <<<"$backoff_block"
rg -q 'retry_policy\.initial_backoff()' <<<"$backoff_block"
rg -q 'retry_policy\.max_backoff()' <<<"$backoff_block"
rg -n 'consumer_build_retry_policy|ConsumerBuildRetryPolicy|ConsumerBuildAttempts|ConsumerBuildBackoffMs' \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/workload.rs
```

Expected: relational validation precedes scenario-file I/O; one backoff
constructor consumes the policy; old hidden defaults are absent; and the
focused search shows the complete flow.

- [ ] **Step 9: Run package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-bench-driver --bin crabka-bench-driver --locked -- --help
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-attempts')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-initial-backoff-ms')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-max-backoff-ms')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Expected: tests and strict Clippy pass; help lists each flag once; formatting
and diff checks pass; `Cargo.lock` is unchanged.

- [ ] **Step 10: Review and commit the Rust implementation**

Inspect and commit only the Rust files:

```bash
git diff -- \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/workload.rs
git add \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/workload.rs
git commit -m "feat(bench): expose consumer build retry"
```

### Task 2: Wire the retry policy through benchmark Jobs

**Files:**

- Modify: `bench/scripts/run-scenario.sh`
- Modify: `bench/manifests/driver/job-template.yaml`

- [ ] **Step 1: Verify deployment wiring is absent**

Run:

```bash
if rg -n 'BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)' \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml; then
  exit 1
fi
```

Expected: no matches.

- [ ] **Step 2: Add launcher defaults and exports**

Document the three variables in the script header. Near the other defaults add:

```bash
: "${BENCH_CONSUMER_BUILD_ATTEMPTS:=6}"
: "${BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS:=100}"
: "${BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS:=2000}"
```

Export all three before rendering the Job.

- [ ] **Step 3: Add Job environment entries**

Document all three variables in the template header. Add after the client
request-timeout entries:

```yaml
- name: BENCH_CONSUMER_BUILD_ATTEMPTS
  value: "${BENCH_CONSUMER_BUILD_ATTEMPTS}"
- name: BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS
  value: "${BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS}"
- name: BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS
  value: "${BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS}"
```

- [ ] **Step 4: Validate shell syntax and rendered values**

Run:

```bash
bash -n bench/scripts/run-scenario.sh
rg -n 'BENCH_CONSUMER_BUILD_ATTEMPTS:=6|BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS:=100|BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS:=2000' \
  bench/scripts/run-scenario.sh
BENCH_CONSUMER_BUILD_ATTEMPTS=6 \
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS=100 \
BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS=2000 \
  envsubst '$BENCH_CONSUMER_BUILD_ATTEMPTS $BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS $BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)'
BENCH_CONSUMER_BUILD_ATTEMPTS=3 \
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS=21 \
BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS=22 \
  envsubst '$BENCH_CONSUMER_BUILD_ATTEMPTS $BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS $BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)'
git diff --check
```

Expected: shell syntax passes; source inspection shows 6/100/2000 defaults;
the renders contain defaults and explicit 3/21/22 overrides.

- [ ] **Step 5: Review and commit deployment wiring**

Inspect and commit only the deployment files:

```bash
git diff -- \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git add \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire consumer build retry"
```

### Task 3: Close the audit slice

**Files:**

- Modify: `docs/configuration-audit.md`

- [ ] **Step 1: Capture exact audit evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/'
rg -n 'consumer_build_retry_policy|ConsumerBuildRetryPolicy|ConsumerBuildAttempts|ConsumerBuildBackoffMs|DEFAULT_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)|consumer-build-(attempts|initial-backoff-ms|max-backoff-ms)|BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)' \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
rg -n 'Duration::|_seconds|_millis|timeout|interval|backoff|capacity|limit' \
  crates/bench-driver/src \
  bench/scripts \
  bench/manifests
```

Record exact total scanner line/file counts. Classify each bench-driver scanner
line and each focused-search line into mutually exclusive production flow,
deployment flow, test/harness, prior-audit, invariant, structural, or
unresolved-owner categories whose counts sum to the exact totals.

Inspect the remaining repository scanner output and name the next real
unresolved operational owner. Do not classify dependency mechanics,
protocol/format/state invariants, test fixtures, scenario inputs, or
already-configured defaults as unresolved.

- [ ] **Step 2: Append the audit section**

Append `## Bench Driver Consumer Build Retry` to
`docs/configuration-audit.md` with:

- the 6/100/2,000 defaults;
- all three flag/environment pairs and CLI precedence;
- positive and ordered-range validation before scenario-file I/O;
- the complete policy flow through task spawning;
- preserved retry loop, dependency factor/jitter, client, and polling behavior;
- launcher and Job-template wiring;
- why no CRD exists;
- scanner and focused-search counts and classifications;
- focused tests, package tests, strict Clippy, help, formatting, shell syntax,
  rendered manifests, diff, and unchanged-lock evidence;
- the next real unresolved repository owner.

- [ ] **Step 3: Re-run final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
bash -n bench/scripts/run-scenario.sh
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-attempts')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-initial-backoff-ms')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-build-max-backoff-ms')" -eq 1
BENCH_CONSUMER_BUILD_ATTEMPTS=3 \
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS=21 \
BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS=22 \
  envsubst '$BENCH_CONSUMER_BUILD_ATTEMPTS $BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS $BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)'
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

Expected: all gates pass; each flag appears once; rendered values are 3/21/22;
`Cargo.lock` is unchanged; scanner counts match the audit text.

- [ ] **Step 4: Review and commit the audit**

Inspect and stage only the audit:

```bash
git diff -- docs/configuration-audit.md
git add docs/configuration-audit.md
git commit -m "docs(audit): record consumer build retry"
```

After the commit, inspect `git status --short` and confirm only the user's
pre-existing unrelated untracked plans remain.
