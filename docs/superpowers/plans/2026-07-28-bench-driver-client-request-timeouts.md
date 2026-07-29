# Bench Driver Client Request Timeouts Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed producer and active-stack consumer request
timeouts with two positive, protocol-safe settings while preserving the
existing 2/5/30-second defaults.

**Architecture:** Parse one shared `ClientRequestTimeoutSeconds` type at the
existing Clap boundary. Resolve the optional consumer value against the active
stack, store concrete typed producer and consumer values in `DriverConfig`, and
copy them into each spawned task before converting to `Duration` at the client
builder boundaries. Reuse the existing shell `envsubst` deployment path.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Bash, Kubernetes YAML.

## Global Constraints

- Preserve defaults: producer 2 seconds, Crabka consumer 5 seconds, Kafka
  consumer 30 seconds.
- CLI overrides environment for both settings.
- Reject zero, malformed, negative, and values above 2,147,483 seconds before
  client construction or I/O.
- Use one active-stack consumer setting, not separate Crabka/Kafka knobs.
- Preserve producer send/failover behavior, consumer build retries, TLS,
  polling, error handling, and scenario behavior.
- Do not expose final-drain timing, retry attempts/backoff, polling/error
  backoff, sampling cadence, or Prometheus timing in this slice.
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

- `crates/bench-driver/src/workload.rs`: validated type, named defaults,
  `DriverConfig`, task propagation, and builder consumption.
- `crates/bench-driver/src/main.rs`: Clap inputs, active-stack resolution, and
  CLI/environment tests.
- `bench/scripts/run-scenario.sh`: producer default, stack-selected consumer
  default, and exports.
- `bench/manifests/driver/job-template.yaml`: driver environment wiring.
- `docs/configuration-audit.md`: evidence and next unresolved owner.

### Task 1: Expose and propagate client request timeouts

**Files:**

- Modify: `crates/bench-driver/src/workload.rs`
- Modify: `crates/bench-driver/src/main.rs`

**Interfaces:**

- Produces: `pub const MAX_CLIENT_REQUEST_TIMEOUT_SECONDS: u64 = 2_147_483`
- Produces:
  `pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT_SECONDS: u64 = 2`
- Produces:
  `pub const DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT_SECONDS: u64 = 5`
- Produces:
  `pub const DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT_SECONDS: u64 = 30`
- Produces: `pub struct ClientRequestTimeoutSeconds(u64)`
- Produces:
  `ClientRequestTimeoutSeconds::new(u64) -> Result<Self, String>`
- Produces: `ClientRequestTimeoutSeconds::duration(self) -> Duration`
- Produces: `FromStr` and `Display`
- Produces:
  `default_producer_request_timeout() -> ClientRequestTimeoutSeconds`
- Produces:
  `default_consumer_request_timeout(Stack) -> ClientRequestTimeoutSeconds`
- Produces:
  `DriverConfig::{producer_request_timeout_seconds,
  consumer_request_timeout_seconds}`
- Consumes: `refined_type::rule::MinMaxU64<1, 2_147_483>`

- [ ] **Step 1: Add failing validated-type and default tests**

Replace the existing `request_timeout_policy_bounds_producers_and_only_crabka_consumers`
test in `crates/bench-driver/src/workload.rs` with focused tests:

```rust
#[test]
fn client_request_timeout_defaults_preserve_policy() {
    assert_eq!(
        default_producer_request_timeout().duration(),
        Duration::from_secs(2)
    );
    assert_eq!(
        default_consumer_request_timeout(Stack::Crabka).duration(),
        Duration::from_secs(5)
    );
    assert_eq!(
        default_consumer_request_timeout(Stack::Kafka).duration(),
        Duration::from_secs(30)
    );
    assert2::assert!(CONSUMER_BUILD_ATTEMPTS == 6);
}

#[test]
fn client_request_timeout_accepts_protocol_bounds() {
    assert_eq!(
        ClientRequestTimeoutSeconds::new(1)
            .expect("one second is valid")
            .duration(),
        Duration::from_secs(1)
    );
    assert_eq!(
        ClientRequestTimeoutSeconds::new(MAX_CLIENT_REQUEST_TIMEOUT_SECONDS)
            .expect("maximum whole-second protocol timeout is valid")
            .duration(),
        Duration::from_secs(2_147_483)
    );
}

#[test]
fn client_request_timeout_rejects_invalid_values() {
    for invalid in ["0", "not-a-number", "-1", "2147484"] {
        assert!(
            invalid.parse::<ClientRequestTimeoutSeconds>().is_err(),
            "{invalid:?} must be rejected"
        );
    }
}
```

The existing attempts assertion stays only to prove this slice did not change
the adjacent retry policy.

- [ ] **Step 2: Add failing CLI/default/precedence tests**

Change the test helper in `crates/bench-driver/src/main.rs` to accept a stack:

```rust
fn required_args(stack: &'static str) -> Vec<&'static str> {
    vec![
        "crabka-bench-driver",
        "--scenario",
        "scenario.yaml",
        "--bootstrap",
        "broker:9092",
        "--stack",
        stack,
    ]
}
```

Update the existing Prometheus test to call `required_args("crabka")`.

Add tests for the new settings:

```rust
#[test]
fn client_request_timeout_defaults_follow_active_stack() {
    let crabka = Cli::try_parse_from(required_args("crabka")).expect("Crabka defaults");
    let kafka = Cli::try_parse_from(required_args("kafka")).expect("Kafka defaults");

    assert_eq!(
        crabka.producer_request_timeout_seconds.duration(),
        Duration::from_secs(2)
    );
    assert_eq!(
        resolve_consumer_request_timeout(
            Stack::Crabka,
            crabka.consumer_request_timeout_seconds,
        )
        .duration(),
        Duration::from_secs(5)
    );
    assert_eq!(
        resolve_consumer_request_timeout(
            Stack::Kafka,
            kafka.consumer_request_timeout_seconds,
        )
        .duration(),
        Duration::from_secs(30)
    );
}

#[test]
fn client_request_timeout_rejects_invalid_cli_values() {
    for option in [
        "--producer-request-timeout-seconds",
        "--consumer-request-timeout-seconds",
    ] {
        for invalid in ["0", "not-a-number", "-1", "2147484"] {
            let mut args = required_args("crabka");
            args.extend([option, invalid]);
            assert!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
        }
    }
}
```

Add one child-process test that sets both environment variables, asserts their
values, supplies both CLI flags, and asserts the CLI values win:

```rust
#[test]
fn client_request_timeouts_read_environment_and_prefer_cli() {
    const CHILD: &str = "CRABKA_BENCH_CLIENT_TIMEOUTS_CHILD";

    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(
            std::env::current_exe().expect("test executable"),
        )
        .args([
            "--exact",
            "tests::client_request_timeouts_read_environment_and_prefer_cli",
        ])
        .env(CHILD, "1")
        .env("BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS", "11")
        .env("BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS", "12")
        .status()
        .expect("child test");
        assert!(status.success());
        return;
    }

    let from_env = Cli::try_parse_from(required_args("crabka")).expect("environment");
    assert_eq!(
        from_env.producer_request_timeout_seconds.duration(),
        Duration::from_secs(11)
    );
    assert_eq!(
        resolve_consumer_request_timeout(
            Stack::Crabka,
            from_env.consumer_request_timeout_seconds,
        )
        .duration(),
        Duration::from_secs(12)
    );

    let mut args = required_args("crabka");
    args.extend([
        "--producer-request-timeout-seconds",
        "21",
        "--consumer-request-timeout-seconds",
        "22",
    ]);
    let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
    assert_eq!(
        from_cli.producer_request_timeout_seconds.duration(),
        Duration::from_secs(21)
    );
    assert_eq!(
        resolve_consumer_request_timeout(
            Stack::Crabka,
            from_cli.consumer_request_timeout_seconds,
        )
        .duration(),
        Duration::from_secs(22)
    );
}
```

This also proves an explicit consumer value replaces the active-stack default.

- [ ] **Step 3: Run focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver client_request_timeout --locked
```

Expected: compilation fails because the validated type, default functions,
resolution helper, and CLI fields do not exist.

- [ ] **Step 4: Implement the minimal validated type and defaults**

In `crates/bench-driver/src/workload.rs`, add `fmt` and `str::FromStr` to the
standard-library imports and import `refined_type::rule::MinMaxU64`.

Replace the old duration-returning timeout helpers with:

```rust
pub const MAX_CLIENT_REQUEST_TIMEOUT_SECONDS: u64 = 2_147_483;
pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT_SECONDS: u64 = 2;
pub const DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT_SECONDS: u64 = 5;
pub const DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT_SECONDS: u64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientRequestTimeoutSeconds(u64);

impl ClientRequestTimeoutSeconds {
    /// Validate a client request timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero or exceeds the largest
    /// whole-second Kafka protocol timeout.
    pub fn new(value: u64) -> Result<Self, String> {
        MinMaxU64::<1, 2_147_483>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}

impl fmt::Display for ClientRequestTimeoutSeconds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ClientRequestTimeoutSeconds {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Return the validated producer request-timeout default.
///
/// # Panics
///
/// Panics if the named default is not protocol-safe.
#[must_use]
pub fn default_producer_request_timeout() -> ClientRequestTimeoutSeconds {
    ClientRequestTimeoutSeconds::new(DEFAULT_PRODUCER_REQUEST_TIMEOUT_SECONDS)
        .expect("default producer request timeout is protocol-safe")
}

/// Return the validated consumer request-timeout default for `stack`.
///
/// # Panics
///
/// Panics if the selected named default is not protocol-safe.
#[must_use]
pub fn default_consumer_request_timeout(stack: Stack) -> ClientRequestTimeoutSeconds {
    let seconds = match stack {
        Stack::Crabka => DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT_SECONDS,
        Stack::Kafka => DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT_SECONDS,
    };
    ClientRequestTimeoutSeconds::new(seconds)
        .expect("default consumer request timeout is protocol-safe")
}
```

Use the named `MAX_CLIENT_REQUEST_TIMEOUT_SECONDS` constant in tests and
documentation, while retaining the literal const-generic bound required by
`MinMaxU64`.

- [ ] **Step 5: Add CLI inputs and active-stack resolution**

Import `ClientRequestTimeoutSeconds` in `crates/bench-driver/src/main.rs`.
Add after the Prometheus timeout:

```rust
/// Producer request timeout, in seconds.
#[arg(
    long,
    env = "BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS",
    default_value_t = workload::default_producer_request_timeout()
)]
producer_request_timeout_seconds: ClientRequestTimeoutSeconds,

/// Consumer request timeout, in seconds. Defaults to 5 for Crabka and 30
/// for Kafka.
#[arg(long, env = "BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS")]
consumer_request_timeout_seconds: Option<ClientRequestTimeoutSeconds>,
```

Add the pure resolution helper:

```rust
fn resolve_consumer_request_timeout(
    stack: Stack,
    configured: Option<ClientRequestTimeoutSeconds>,
) -> ClientRequestTimeoutSeconds {
    configured.unwrap_or_else(|| workload::default_consumer_request_timeout(stack))
}
```

Before constructing `DriverConfig`, resolve `stack` once and resolve the
consumer timeout from that stack. Store both concrete typed values in the
configuration.

- [ ] **Step 6: Propagate typed values through spawned tasks**

Add both typed fields to `DriverConfig` and initialize the workload test helper
with the named defaults.

Add `request_timeout: ClientRequestTimeoutSeconds` to `ProducerTask` and
`ConsumerTask`. Copy the appropriate configuration value when spawning each
task.

In `run_producer`, pass:

```rust
.request_timeout(request_timeout.duration())
```

Change `build_consumer_with_retry` to accept
`ClientRequestTimeoutSeconds`; pass the task's typed value into it and convert
inside each builder attempt:

```rust
.request_timeout(request_timeout.duration())
```

Remove `stack` from `ConsumerTask`; stack selection is complete before the
concrete configuration is spawned. Do not change retry construction, polling,
or any other builder option.

- [ ] **Step 7: Run focused tests to verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver client_request_timeout --locked
```

Expected: all focused type, default, parser, and precedence tests pass.

- [ ] **Step 8: Prove the production paths consume typed settings**

Run:

```bash
if sed -n '1,/^#\[cfg(test)\]/p' crates/bench-driver/src/workload.rs \
  | rg -n '\.request_timeout\(Duration::from_secs\((2|5|30)\)\)'; then
  exit 1
fi
test "$(rg -o 'request_timeout\.duration\(\)' crates/bench-driver/src/workload.rs | wc -l)" -eq 2
rg -n 'producer_request_timeout_seconds|consumer_request_timeout_seconds|ClientRequestTimeoutSeconds' \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/workload.rs
```

Expected: no hidden production duration literals remain, exactly two client
builder boundaries consume the typed duration, and the focused search shows
both complete flows.

- [ ] **Step 9: Run package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-bench-driver --bin crabka-bench-driver --locked -- --help
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--producer-request-timeout-seconds')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-request-timeout-seconds')" -eq 1
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
git commit -m "feat(bench): expose client timeouts"
```

### Task 2: Wire both timeouts through benchmark Jobs

**Files:**

- Modify: `bench/scripts/run-scenario.sh`
- Modify: `bench/manifests/driver/job-template.yaml`

- [ ] **Step 1: Verify deployment wiring is absent**

Run:

```bash
if rg -n 'BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS' \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml; then
  exit 1
fi
```

Expected: no matches.

- [ ] **Step 2: Add launcher defaults and exports**

Document both variables in the script header. Near the other defaults add:

```bash
: "${BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS:=2}"
if [[ "$STACK" == "crabka" ]]; then
  : "${BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS:=5}"
else
  : "${BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS:=30}"
fi
```

Export both variables before rendering the Job:

```bash
export BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS
export BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS
```

This leaves the script's existing stack-validation behavior unchanged.

- [ ] **Step 3: Add Job environment entries**

Document both variables in the template header. Add after the Prometheus
timeout entry:

```yaml
- name: BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS
  value: "${BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS}"
- name: BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS
  value: "${BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS}"
```

- [ ] **Step 4: Validate shell syntax, defaults, and rendered overrides**

Run:

```bash
bash -n bench/scripts/run-scenario.sh
rg -n 'BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS:=2|BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS:=(5|30)' \
  bench/scripts/run-scenario.sh
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS=2 \
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS=5 \
  envsubst '$BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS $BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS'
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS=2 \
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS=30 \
  envsubst '$BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS $BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS'
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS=7 \
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS=11 \
  envsubst '$BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS $BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS'
git diff --check
```

Expected: shell syntax passes; source inspection shows 2/5/30 defaults; the
renders contain Crabka 2/5, Kafka 2/30, and explicit override 7/11.

- [ ] **Step 5: Review and commit deployment wiring**

Inspect and commit only the deployment files:

```bash
git diff -- \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git add \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire client timeouts"
```

### Task 3: Close the audit slice

**Files:**

- Modify: `docs/configuration-audit.md`

- [ ] **Step 1: Capture exact audit evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/'
rg -n 'producer_request_timeout_seconds|consumer_request_timeout_seconds|ClientRequestTimeoutSeconds|DEFAULT_(PRODUCER|CRABKA_CONSUMER|KAFKA_CONSUMER)_REQUEST_TIMEOUT_SECONDS|producer-request-timeout-seconds|consumer-request-timeout-seconds|BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS' \
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
unresolved operational owner. Do not classify protocol/format/state
invariants, test fixtures, scenario inputs, or already-configured defaults as
unresolved.

- [ ] **Step 2: Append the audit section**

Append `## Bench Driver Client Request Timeouts` to
`docs/configuration-audit.md` with:

- the 2/5/30-second defaults;
- both flag/environment pairs and CLI precedence;
- positive protocol-safe validation;
- the producer and consumer value flows through task spawning;
- launcher and Job-template wiring;
- preserved retry, polling, TLS, failover, and scenario behavior;
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
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--producer-request-timeout-seconds')" -eq 1
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--consumer-request-timeout-seconds')" -eq 1
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS=7 \
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS=11 \
  envsubst '$BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS $BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS'
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

Expected: all gates pass; each flag appears once; the rendered values are 7
and 11; `Cargo.lock` is unchanged; scanner counts match the audit text.

- [ ] **Step 4: Review and commit the audit**

Inspect and stage only the audit:

```bash
git diff -- docs/configuration-audit.md
git add docs/configuration-audit.md
git commit -m "docs(audit): record bench client timeouts"
```

After the commit, inspect `git status --short` and confirm only the user's
pre-existing unrelated untracked plans remain.
