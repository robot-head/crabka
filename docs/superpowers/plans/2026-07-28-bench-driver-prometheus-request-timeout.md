# Bench Driver Prometheus Request Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed 15-second Prometheus HTTP request timeout with one
positive setting resolved from CLI, environment, or the existing default.

**Architecture:** Add a `PrometheusRequestTimeoutSeconds` newtype beside
`PromClient`, parse it at the existing Clap boundary, store it in
`DriverConfig`, and require it when constructing the reqwest client. Reuse the
existing shell `envsubst` path to pass the setting into benchmark Jobs.

**Tech Stack:** Rust 2024, Clap, `refined_type`, reqwest, Bash, Kubernetes YAML.

## Global Constraints

- Preserve the exact 15-second default.
- `--prometheus-request-timeout-seconds` overrides
  `BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS`.
- Reject zero, malformed, negative, and `u64` overflow values before
  Prometheus I/O.
- Preserve Prometheus queries, capture behavior, response parsing, notes, and
  errors.
- Add no CRD because the checked-in launcher and Job own this binary.
- Do not migrate unrelated benchmark settings.
- Add only the workspace-pinned `refined_type` dependency; dependency versions
  and transitive packages must not change.
- Run every Cargo command with
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use `--locked` for
  lock-aware commands.
- Preserve unrelated dirty and untracked files; stage only paths named by the
  current task.

---

## File Map

- `crates/bench-driver/Cargo.toml`: direct `refined_type` dependency.
- `crates/bench-driver/src/prom.rs`: validated timeout type and required
  `PromClient` constructor input.
- `crates/bench-driver/src/main.rs`: Clap input and CLI/environment precedence.
- `crates/bench-driver/src/workload.rs`: typed configuration storage and
  forwarding.
- `bench/scripts/run-scenario.sh`: overrideable deployment default and export.
- `bench/manifests/driver/job-template.yaml`: driver environment wiring.
- `Cargo.lock`: direct dependency entry only.
- `docs/configuration-audit.md`: evidence and next unresolved owner.

### Task 1: Expose and propagate the Prometheus timeout

**Files:**

- Modify: `crates/bench-driver/Cargo.toml`
- Modify: `crates/bench-driver/src/prom.rs`
- Modify: `crates/bench-driver/src/main.rs`
- Modify: `crates/bench-driver/src/workload.rs`
- Modify: `Cargo.lock`

**Interfaces:**

- Produces:
  `pub const DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS: u64`
- Produces: `pub struct PrometheusRequestTimeoutSeconds(u64)`
- Produces:
  `PrometheusRequestTimeoutSeconds::new(u64) -> Result<Self, String>`
- Produces: `PrometheusRequestTimeoutSeconds::duration(self) -> Duration`
- Produces: `FromStr`, `Display`, and `Default`
- Produces:
  `DriverConfig::prometheus_request_timeout_seconds:
  PrometheusRequestTimeoutSeconds`
- Changes:
  `PromClient::new(base_url, PrometheusRequestTimeoutSeconds) -> Result<Self>`
- Consumes: `refined_type::rule::GreaterU64<0>`

- [ ] **Step 1: Add failing typed-boundary tests**

In `crates/bench-driver/src/prom.rs`, add tests:

```rust
#[test]
fn prometheus_request_timeout_default_remains_fifteen_seconds() {
    assert_eq!(
        PrometheusRequestTimeoutSeconds::default().duration(),
        Duration::from_secs(15)
    );
}

#[test]
fn prometheus_request_timeout_accepts_one_second() {
    assert_eq!(
        PrometheusRequestTimeoutSeconds::new(1)
            .expect("one second is valid")
            .duration(),
        Duration::from_secs(1)
    );
}

#[test]
fn prometheus_request_timeout_rejects_invalid_values() {
    assert!(PrometheusRequestTimeoutSeconds::new(0).is_err());

    let overflow = format!("{}0", u64::MAX);
    for invalid in ["0", "not-a-number", "-1", overflow.as_str()] {
        assert!(
            invalid
                .parse::<PrometheusRequestTimeoutSeconds>()
                .is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn prometheus_request_timeout_constructs_prom_client() {
    let timeout = PrometheusRequestTimeoutSeconds::new(1).expect("valid timeout");

    assert!(PromClient::new("http://prometheus.example", timeout).is_ok());
}
```

These tests use hand-derived expected values and cover the complete validation
boundary. The required constructor argument is also enforced by every
production and test call at compile time.

- [ ] **Step 2: Add failing CLI precedence tests**

In a `#[cfg(test)] mod tests` in `crates/bench-driver/src/main.rs`, add a helper
that returns the required arguments:

```rust
fn required_args() -> Vec<&'static str> {
    vec![
        "crabka-bench-driver",
        "--scenario",
        "scenario.yaml",
        "--bootstrap",
        "broker:9092",
        "--stack",
        "crabka",
    ]
}
```

Add a child-process test so environment mutation is isolated:

```rust
#[test]
fn prometheus_request_timeout_environment_and_cli_precedence() {
    const CHILD: &str = "CRABKA_BENCH_PROMETHEUS_TIMEOUT_CHILD";

    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(
            std::env::current_exe().expect("test executable"),
        )
        .args([
            "--exact",
            "tests::prometheus_request_timeout_environment_and_cli_precedence",
        ])
        .env(CHILD, "1")
        .env("BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS", "32")
        .status()
        .expect("child test");
        assert!(status.success());
        return;
    }

    let from_env = Cli::try_parse_from(required_args()).expect("environment");
    assert_eq!(
        from_env.prometheus_request_timeout_seconds.duration(),
        Duration::from_secs(32)
    );

    let mut args = required_args();
    args.extend(["--prometheus-request-timeout-seconds", "64"]);
    let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
    assert_eq!(
        from_cli.prometheus_request_timeout_seconds.duration(),
        Duration::from_secs(64)
    );
}
```

Import `clap::Parser` and `std::time::Duration` in the test module as needed.

- [ ] **Step 3: Run focused tests to verify RED**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver prometheus_request_timeout --locked
```

Expected: compilation fails because the timeout type, CLI field, and new
constructor signature do not exist.

- [ ] **Step 4: Add the existing workspace dependency**

Add this in `crates/bench-driver/Cargo.toml`:

```toml
refined_type = { workspace = true }
```

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo check -p crabka-bench-driver --locked
```

If `--locked` reports that the lockfile needs an update, run exactly:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo check -p crabka-bench-driver
```

Then inspect `Cargo.lock`. The only allowed lock change is the addition of
`"refined_type"` to the existing `crabka-bench-driver` dependency list.

- [ ] **Step 5: Implement the minimal validated type**

In `crates/bench-driver/src/prom.rs`, extend the standard-library imports with
`fmt` and `str::FromStr`, and import `refined_type::rule::GreaterU64`.

Add before `PromClient`:

```rust
/// Default HTTP request timeout for Prometheus queries.
pub const DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS: u64 = 15;

/// A positive Prometheus HTTP request timeout in seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrometheusRequestTimeoutSeconds(u64);

impl PrometheusRequestTimeoutSeconds {
    /// Validate a Prometheus HTTP request timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> Result<Self, String> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated timeout.
    #[must_use]
    pub const fn duration(self) -> Duration {
        Duration::from_secs(self.0)
    }
}

impl Default for PrometheusRequestTimeoutSeconds {
    fn default() -> Self {
        Self::new(DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS)
            .expect("default Prometheus request timeout is positive")
    }
}

impl fmt::Display for PrometheusRequestTimeoutSeconds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for PrometheusRequestTimeoutSeconds {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
```

Change `PromClient::new` to require
`request_timeout: PrometheusRequestTimeoutSeconds` and replace the fixed
literal with:

```rust
.timeout(request_timeout.duration())
```

Do not add a compatibility constructor or hidden default.

- [ ] **Step 6: Add CLI input and typed configuration storage**

Import `PrometheusRequestTimeoutSeconds` in `crates/bench-driver/src/main.rs`
and add this `Cli` field after `prometheus`:

```rust
/// HTTP request timeout for Prometheus queries, in seconds.
#[arg(
    long,
    env = "BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS",
    default_value_t = PrometheusRequestTimeoutSeconds::default()
)]
prometheus_request_timeout_seconds: PrometheusRequestTimeoutSeconds,
```

Add the same typed field to `DriverConfig`, copy the parsed value in `main`,
and initialize the workload test helper with
`PrometheusRequestTimeoutSeconds::default()`.

In `capture_resources`, pass
`cfg.prometheus_request_timeout_seconds` as the second argument to
`PromClient::new`.

- [ ] **Step 7: Run focused tests to verify GREEN**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver prometheus_request_timeout --locked
```

Expected: all focused timeout tests pass.

- [ ] **Step 8: Prove the production flow has no hidden timeout**

Run:

```bash
if sed -n '1,/^#\[cfg(test)\]/p' crates/bench-driver/src/prom.rs \
  | rg -n 'Duration::from_secs\(15\)'; then
  exit 1
fi
rg -n 'prometheus_request_timeout_seconds|PrometheusRequestTimeoutSeconds|BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS' \
  crates/bench-driver/src
rg -n 'PromClient::new' crates/bench-driver/src
```

Expected: the old constructor literal is absent; the focused search shows the
single CLI-to-config-to-client flow; all `PromClient::new` calls supply the
typed timeout.

- [ ] **Step 9: Run package gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -p crabka-bench-driver --bin crabka-bench-driver --locked -- --help
target/debug/crabka-bench-driver --help | rg -c -- '--prometheus-request-timeout-seconds'
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

Expected: tests and strict Clippy pass; help lists the flag exactly once;
formatting and diff checks pass; the lock diff contains only the approved
direct dependency line.

- [ ] **Step 10: Review and commit the Rust implementation**

Inspect:

```bash
git diff -- \
  Cargo.lock \
  crates/bench-driver/Cargo.toml \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/prom.rs \
  crates/bench-driver/src/workload.rs
```

Stage and commit only those files:

```bash
git add \
  Cargo.lock \
  crates/bench-driver/Cargo.toml \
  crates/bench-driver/src/main.rs \
  crates/bench-driver/src/prom.rs \
  crates/bench-driver/src/workload.rs
git commit -m "feat(bench): expose Prometheus timeout"
```

### Task 2: Wire the timeout through benchmark Jobs

**Files:**

- Modify: `bench/scripts/run-scenario.sh`
- Modify: `bench/manifests/driver/job-template.yaml`

- [ ] **Step 1: Verify deployment wiring is absent**

Run:

```bash
if rg -n 'BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS' \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml; then
  exit 1
fi
```

Expected: no matches.

- [ ] **Step 2: Add the overrideable launcher default**

Document the variable in the `run-scenario.sh` header. Near the other
launcher defaults add:

```bash
: "${BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS:=15}"
```

Add it to the existing export block used before rendering the Job:

```bash
export BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS
```

Do not add a second rendering path or shell validation; the Rust argument
boundary remains the source of truth.

- [ ] **Step 3: Add the Job environment entry**

Document the variable in the Job template header. After
`BENCH_PROMETHEUS_URL`, add:

```yaml
- name: BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS
  value: "${BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS}"
```

- [ ] **Step 4: Validate shell syntax and rendered output**

Run:

```bash
bash -n bench/scripts/run-scenario.sh
BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS=7 \
  envsubst '$BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS'
git diff --check
```

Expected: shell syntax passes, the rendered entry has `value: "7"`, and diff
hygiene passes.

- [ ] **Step 5: Review and commit deployment wiring**

Inspect and commit only the two deployment files:

```bash
git diff -- \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git add \
  bench/scripts/run-scenario.sh \
  bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire Prometheus timeout"
```

### Task 3: Close the audit slice

**Files:**

- Modify: `docs/configuration-audit.md`

- [ ] **Step 1: Capture exact audit evidence**

Run:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/|^bench/'
rg -n 'prometheus_request_timeout_seconds|PrometheusRequestTimeoutSeconds|DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS|prometheus-request-timeout-seconds|BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS' \
  crates/bench-driver bench docs/configuration-audit.md
rg -n 'Duration::|_seconds|_millis|timeout|interval|backoff|capacity|limit' \
  crates/bench-driver/src bench/scripts bench/manifests
```

Record exact total scanner line/file counts. Classify each bench-driver and
benchmark-deployment scanner line, plus each focused-search line, into
mutually exclusive production flow, deployment flow, test/harness,
prior-audit, invariant, structural, or unresolved-owner categories whose
counts sum to the exact totals.

Inspect the remaining repository scanner output and name the next real
unresolved operational owner. Do not classify protocol constants, sentinels,
test fixtures, scenario inputs, Kubernetes invariants, or already-configured
defaults as unresolved.

- [ ] **Step 2: Append the audit section**

Append `## Bench Driver Prometheus Request Timeout` to
`docs/configuration-audit.md` with:

- the 15-second default;
- flag, environment variable, and CLI precedence;
- positive `u64` validation;
- the exact `Cli -> DriverConfig -> PromClient -> reqwest` flow;
- launcher and Job-template wiring;
- preserved Prometheus capture and error behavior;
- why no CRD exists;
- the exact approved lockfile change;
- scanner and focused-search counts and classifications;
- focused tests, package tests, strict Clippy, help, formatting, shell syntax,
  rendered manifest, and diff evidence;
- the next real unresolved repository owner.

- [ ] **Step 3: Re-run final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
bash -n bench/scripts/run-scenario.sh
BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS=7 \
  envsubst '$BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS'
target/debug/crabka-bench-driver --help | rg -c -- '--prometheus-request-timeout-seconds'
git diff --check
git diff -- Cargo.lock
tools/audit-runtime-values.sh
```

Expected: all gates pass; help lists the flag once; the rendered value is
`7`; the lockfile contains only the direct dependency addition; scanner counts
match the audit text.

- [ ] **Step 4: Review and commit the audit**

Inspect and stage only the audit:

```bash
git diff -- docs/configuration-audit.md
git add docs/configuration-audit.md
git commit -m "docs(audit): record bench Prometheus timeout"
```

After the commit, inspect `git status --short` and confirm only the user's
pre-existing unrelated changes and untracked plans remain.
