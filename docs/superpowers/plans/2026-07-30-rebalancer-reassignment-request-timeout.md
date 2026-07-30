# Rebalancer Reassignment Request Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Kafka broker-side reassignment request timeout explicit and configurable without changing its 60-second default.

**Architecture:** `crabka-rebalancer` owns one validated UOM newtype and stores it on `LiveClient`. Submit and cancel builders frame the value explicitly; the standalone binary and Helm chart provide the only deployment boundary because no CRD owns daemon transport policy.

**Tech Stack:** Rust, `crabka-units`, `refined_type`, Clap environment arguments, Helm.

## Global Constraints

- Preserve the 60-second default.
- Accept only finite, positive, whole-millisecond values within `1..=i32::MAX`.
- Use UOM `Time` at configuration boundaries and `refined_type` for newtype validation.
- Keep `LiveClient::new(Client)` source-compatible and default-backed.
- Do not change executor deadlines, polling, client I/O timeouts, request grouping, or Kafka response handling.
- Add no `KafkaRebalance` CRD field.
- Do not modify or stage the four protected untracked plans dated 2026-07-28.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

---

### Task 1: Validate and frame reassignment request timeout

**Files:**
- Modify: `crates/rebalancer/Cargo.toml`
- Modify: `crates/rebalancer/src/executor/client_impl.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `DEFAULT_REASSIGNMENT_REQUEST_TIMEOUT: Time`
- Produces: `ReassignmentRequestTimeout::new(Time) -> Result<Self, String>`
- Produces: `ReassignmentRequestTimeout::time(self) -> Time`
- Produces: `ReassignmentRequestTimeout::milliseconds(self) -> i32`
- Produces: `LiveClient::with_reassignment_request_timeout(Client, ReassignmentRequestTimeout) -> Self`

- [x] **Step 1: Write failing policy and request-builder tests**

Add tests in `client_impl.rs`:

```rust
#[test]
fn reassignment_request_timeout_validates_protocol_milliseconds() {
    let timeout = ReassignmentRequestTimeout::new(millis(37)).unwrap();
    assert2::assert!(timeout.time() == millis(37));
    assert2::assert!(timeout.milliseconds() == 37);
    assert2::assert!(
        ReassignmentRequestTimeout::default().milliseconds() == 60_000
    );
    for invalid in [
        Time::ZERO,
        Time::from_secs_f64(0.0005),
        Time::from_millis(i64::from(i32::MAX) + 1),
    ] {
        assert2::assert!(ReassignmentRequestTimeout::new(invalid).is_err());
    }
}

#[test]
fn reassignment_builders_frame_configured_timeout() {
    let timeout = ReassignmentRequestTimeout::new(millis(37)).unwrap();
    let submit = build_submit_reassignments_request(
        &[movement("orders", 0, vec![1], vec![2])],
        timeout,
    );
    let cancel =
        build_cancel_reassignments_request(&[("orders".into(), 0)], timeout);
    assert2::assert!(submit.timeout_ms == 37);
    assert2::assert!(cancel.timeout_ms == 37);
}
```

Update the existing submit/cancel expected-value tests to pass
`ReassignmentRequestTimeout::default()`.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer reassignment_request_timeout --locked
```

Expected: compilation fails because `ReassignmentRequestTimeout` and the
policy-aware builder parameters do not exist.

- [x] **Step 3: Implement the minimal validated newtype**

Add the existing workspace dependency:

```toml
refined_type = { workspace = true }
```

In `client_impl.rs`, add:

```rust
pub const DEFAULT_REASSIGNMENT_REQUEST_TIMEOUT: Time = secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReassignmentRequestTimeout(i32);

impl ReassignmentRequestTimeout {
    pub fn new(value: Time) -> Result<Self, String> {
        let milliseconds = value.millis_i64();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err(
                "reassignment request timeout must be a whole number of milliseconds".into(),
            );
        }
        let milliseconds = i32::try_from(milliseconds).map_err(|_| {
            "reassignment request timeout must be within 1..=i32::MAX milliseconds"
                .to_string()
        })?;
        refined_type::rule::GreaterI32::<0>::new(milliseconds)
            .map(refined_type::Refined::into_value)
            .map(Self)
            .map_err(|error| format!("reassignment request timeout: {error}"))
    }

    #[must_use]
    pub fn time(self) -> Time {
        Time::from_millis(i64::from(self.0))
    }

    #[must_use]
    pub const fn milliseconds(self) -> i32 {
        self.0
    }
}
```

Implement `Default` through `new(DEFAULT_REASSIGNMENT_REQUEST_TIMEOUT)`.

- [x] **Step 4: Thread the policy through request construction**

Change the submit, cancel, and shared builders to accept
`ReassignmentRequestTimeout`; construct the request with:

```rust
AlterPartitionReassignmentsRequest {
    timeout_ms: timeout.milliseconds(),
    topics,
    ..Default::default()
}
```

Store the timeout on `LiveClient`. Keep `new` default-backed and add:

```rust
pub fn with_reassignment_request_timeout(
    inner: Client,
    reassignment_request_timeout: ReassignmentRequestTimeout,
) -> Self
```

Use the stored value for submit and cancel.

- [x] **Step 5: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer reassignment_request_timeout --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer build_submit_reassignments_request --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer build_cancel_reassignments_request --locked
```

Expected: all focused tests pass.

### Task 2: Expose the timeout through CLI and environment

**Files:**
- Modify: `crates/rebalancer/Cargo.toml`
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`

**Interfaces:**
- Consumes: `ReassignmentRequestTimeout`
- Produces: `--reassignment-request-timeout`
- Produces: `CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT`

- [x] **Step 1: Write failing parser tests**

Add `temp-env = "0.3"` as a dev dependency and tests that use the repository's
standard environment lock:

```rust
#[test]
fn reassignment_request_timeout_defaults_and_accepts_cli() {
    let _guard = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("environment lock");
    temp_env::with_var(
        "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
        None::<&str>,
        || {
            let defaults = Args::try_parse_from([
                "crabka-rebalancer",
                "--bootstrap-servers",
                "127.0.0.1:9092",
            ])
            .unwrap();
            assert2::assert!(defaults.reassignment_request_timeout == secs(60));
        },
    );

    let custom = Args::try_parse_from([
        "crabka-rebalancer",
        "--bootstrap-servers",
        "127.0.0.1:9092",
        "--reassignment-request-timeout",
        "37ms",
    ])
    .unwrap();
    assert2::assert!(custom.reassignment_request_timeout == millis(37));
}

#[test]
fn reassignment_request_timeout_rejects_invalid_protocol_values() {
    assert2::assert!(
        Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--reassignment-request-timeout",
            "0s",
        ])
        .is_err()
    );
    for value in ["0.5ms", "2147483648ms"] {
        let args = Args::try_parse_from([
            "crabka-rebalancer",
            "--bootstrap-servers",
            "127.0.0.1:9092",
            "--reassignment-request-timeout",
            value,
        ])
        .unwrap();
        assert2::assert!(
            ReassignmentRequestTimeout::new(args.reassignment_request_timeout).is_err()
        );
    }
}

#[test]
fn reassignment_request_timeout_reads_environment() {
    let _guard = ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("environment lock");
    temp_env::with_var(
        "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
        Some("41ms"),
        || {
            let args = Args::try_parse_from([
                "crabka-rebalancer",
                "--bootstrap-servers",
                "127.0.0.1:9092",
            ])
            .unwrap();
            assert2::assert!(args.reassignment_request_timeout == millis(41));
        },
    );
}
```

Define `static ENV_LOCK: OnceLock<Mutex<()>>` in the test module.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer reassignment_request_timeout_defaults --locked
```

Expected: compilation fails because the argument does not exist.

- [x] **Step 3: Add the UOM argument and validate before construction**

Add:

```rust
#[arg(
    long,
    env = "CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT",
    default_value = "60s",
    value_parser = crabka_units::parse::positive_time
)]
reassignment_request_timeout: Time,
```

Before constructing `LiveClient`, validate once:

```rust
let reassignment_request_timeout =
    ReassignmentRequestTimeout::new(args.reassignment_request_timeout)
        .map_err(anyhow::Error::msg)?;
```

Construct the live client with
`LiveClient::with_reassignment_request_timeout(client.clone(), reassignment_request_timeout)`.

- [x] **Step 4: Run GREEN and the binary tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer reassignment_request_timeout --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer --bin crabka-rebalancer --locked
```

Expected: parser, validation, and existing binary tests pass.

### Task 3: Wire the Helm override and close the audit slice

**Files:**
- Modify: `charts/crabka-rebalancer/values.yaml`
- Modify: `charts/crabka-rebalancer/templates/deployment.yaml`
- Modify: `charts/crabka-rebalancer/tests/deployment_test.yaml`
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Produces: Helm value `reassignmentRequestTimeout`
- Produces: pod environment variable `CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT`

- [x] **Step 1: Write the failing Helm assertion**

Add:

```yaml
  - it: passes reassignment request timeout as a human duration
    set:
      reassignmentRequestTimeout: 37ms
    asserts:
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT
            value: 37ms
```

- [x] **Step 2: Run the RED gate**

```bash
helm unittest charts/crabka-rebalancer
```

Expected: the new assertion fails because the environment variable is absent.

- [x] **Step 3: Add the minimal Helm value and environment entry**

Add `reassignmentRequestTimeout: 60s` beside the existing reassignment values,
and render:

```yaml
- name: CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT
  value: {{ .Values.reassignmentRequestTimeout | quote }}
```

- [x] **Step 4: Update audit evidence**

Change the rebalancer scanner count to the current value, classify the
generated 60-second default as replaced by explicit request policy, and record
the exact CLI, environment, and Helm names. Keep all other unresolved
rebalancer policies Pending.

- [x] **Step 5: Run focused and repository gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-rebalancer --all-targets --locked
helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092
helm unittest charts/crabka-rebalancer
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

Expected: all tests, chart gates, Clippy, formatting, and diff hygiene pass.

- [ ] **Step 6: Run the requested cleanup after the entire repository goal**

Do not run this after the slice if additional goal work remains. After the
final repository-wide verification build:

```bash
cargo clean
```

Expected: Cargo removes the final build artifacts.
