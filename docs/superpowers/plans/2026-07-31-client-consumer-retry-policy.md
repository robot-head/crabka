# Client Consumer Retry Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the classic Consumer's hardcoded startup and coordinator
retry timing with one validated UOM-backed policy and expose it through the
observability demo Consume role.

**Architecture:** Add one public `ConsumerRetryPolicy` whose seven private
fields use one private validated whole-millisecond duration newtype. Carry the
policy through `Consumer::start`, `StartConfig`, and `CoordinatorState`; pass
its coordinator subset to the existing retry helpers. The standalone demo
parses the same UOM values and supplies the policy only to its Consume role.

**Tech Stack:** Rust, `bon`, Clap, `crabka-units`, `refined_type`, Tokio,
Docker Compose, Cargo.

## Global Constraints

- Preserve defaults: startup attempt `90s`, startup deadline `5m`, startup
  backoff `500ms` to `5s`, coordinator retry timeout `30s`, coordinator
  backoff `100ms` to `1s`.
- Preserve capped exponential doubling, retriable-error classification,
  protocol error codes, cancellation, and best-effort shutdown behavior.
- Every new time CLI/environment value uses UOM syntax; add no raw
  millisecond setting.
- Validate before network I/O: every time is finite, positive, and a whole
  number of milliseconds.
- Require startup attempt timeout at most startup deadline and each initial
  backoff at most its matching maximum.
- Use the existing workspace `refined_type` dependency; add no dependency.
- Explicit demo values are valid only for the Consume role.
- Add no CRD because the operator does not own the demo Consumer.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.

---

### Task 1: Add the Validated Policy and Startup Timing

**Files:**
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/lib.rs`

**Interfaces:**
- Produces:
  `ConsumerRetryPolicy::new(startup_attempt_timeout: Time,
  startup_deadline: Time, startup_initial_backoff: Time,
  startup_max_backoff: Time, coordinator_retry_timeout: Time,
  coordinator_initial_backoff: Time, coordinator_max_backoff: Time)
  -> Result<Self, String>`
- Produces: named `Time` getters for all seven fields and `Default`
- Produces:
  `Consumer::builder().retry_policy(ConsumerRetryPolicy::new(...).unwrap())`

- [ ] **Step 1: Write policy validation and startup-behavior tests**

In `consumer.rs` tests, add a default/override test that checks:

```rust
let policy = ConsumerRetryPolicy::default();
assert_eq!(policy.startup_attempt_timeout(), secs(90));
assert_eq!(policy.startup_deadline(), minutes(5));
assert_eq!(policy.startup_initial_backoff(), millis(500));
assert_eq!(policy.startup_max_backoff(), secs(5));
assert_eq!(policy.coordinator_retry_timeout(), secs(30));
assert_eq!(policy.coordinator_initial_backoff(), millis(100));
assert_eq!(policy.coordinator_max_backoff(), secs(1));
```

Add table-driven rejection for `0ms`, `0.5ms`, non-finite time, startup
attempt above deadline, startup initial backoff above maximum, and coordinator
initial backoff above maximum. Extend the paused-time startup retry test so a
non-default policy proves the attempt timeout, total deadline, and startup
backoff come from the builder value.

- [ ] **Step 2: Run the focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer consumer_retry_policy --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer configured_startup_retry_policy --lib --locked
```

Expected: `ConsumerRetryPolicy` and the builder input do not exist.

- [ ] **Step 3: Add the minimal validated types**

Add a private `RetryTime(Duration)` that validates through
`refined_type::rule::MinMaxU128<1, { u64::MAX as u128 }>` and rejects values
that do not round-trip through whole milliseconds. Add the public policy with
seven private `RetryTime` fields, the constructor above, named `Time` getters,
and the exact defaults from Global Constraints. Re-export
`ConsumerRetryPolicy` from `lib.rs`.

- [ ] **Step 4: Replace startup constants with policy getters**

Add

```rust
#[builder(default = ConsumerRetryPolicy::default())]
retry_policy: ConsumerRetryPolicy,
```

to `Consumer::start`, carry it in `StartConfig`, and replace
`CONSUMER_START_ATTEMPT_TIMEOUT`, `CONSUMER_START_DEADLINE`, the initial
`500ms`, and maximum `5s` with the four startup getters. Delete the two startup
constants.

- [ ] **Step 5: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
git add crates/client-consumer/src/consumer.rs crates/client-consumer/src/lib.rs
git commit -m "feat(consumer): expose retry policy"
```

---

### Task 2: Propagate Coordinator Retry Timing

**Files:**
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/coordinator.rs`
- Modify: `crates/client-consumer/src/commit.rs`

**Interfaces:**
- Consumes: `ConsumerRetryPolicy` and its three coordinator getters
- Produces:
  `CoordinatorRetryPolicy { timeout: Duration, initial_backoff: Duration,
  max_backoff: Duration }`
- Produces coordinator helpers that receive `CoordinatorRetryPolicy` instead
  of reading constants

- [ ] **Step 1: Write failing coordinator propagation tests**

Extend the existing retry-helper paused-time tests with a non-default typed
policy:

```rust
let retry = CoordinatorRetryPolicy {
    timeout: Duration::from_millis(35),
    initial_backoff: Duration::from_millis(5),
    max_backoff: Duration::from_millis(10),
};
```

Assert retry attempts occur at `0ms`, `5ms`, `15ms`, and `25ms`, then stop
after the `35ms` timeout. Add focused tests proving `find_coordinator`,
coordinator re-find, and offset commit receive the configured values rather
than the old `30s`, `100ms`, and `1s` constants.

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer configured_coordinator_retry_policy --lib --locked
```

Expected: `CoordinatorRetryPolicy` or the new helper parameters are absent.

- [ ] **Step 3: Thread one internal coordinator policy**

Define the private copyable `CoordinatorRetryPolicy` in `coordinator.rs`.
Construct it from the three validated `ConsumerRetryPolicy` getters while
building `StartConfig`, carry it into `CoordinatorState`, and pass it to
`find_coordinator`, `with_coordinator_retry`, and `with_coordinator_refind`.
Update `commit.rs` to use the policy supplied by its consumer/coordinator
caller. Delete `COORDINATOR_RETRY_TIMEOUT` and both local `MAX_BACKOFF`
constants; initialize and cap backoff from the policy.

- [ ] **Step 4: Audit all coordinator call sites**

```bash
rg -n 'COORDINATOR_RETRY_TIMEOUT|MAX_BACKOFF|with_coordinator_retry\\(|with_coordinator_refind\\(|find_coordinator\\(' \
  crates/client-consumer/src
```

Every production retry call must receive the typed policy. Remaining time
literals must be test inputs or unrelated poll/request behavior.

- [ ] **Step 5: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-consumer --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
git add crates/client-consumer/src/consumer.rs \
  crates/client-consumer/src/coordinator.rs crates/client-consumer/src/commit.rs
git commit -m "fix(consumer): propagate coordinator retry policy"
```

---

### Task 3: Expose the Demo Consume-Role Surface

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Modify: `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: `ConsumerRetryPolicy::new`
- Produces the seven CLI/environment pairs specified in the approved design
- Produces:
  `effective_consumer_retry_policy(&Cli) -> Result<ConsumerRetryPolicy, String>`

- [ ] **Step 1: Write failing parser, role, and Compose tests**

Add hermetic subprocess tests covering:

```text
--consumer-startup-attempt-timeout=11s
--consumer-startup-deadline=12s
--consumer-startup-initial-backoff=13ms
--consumer-startup-max-backoff=14ms
--consumer-coordinator-retry-timeout=15s
--consumer-coordinator-initial-backoff=16ms
--consumer-coordinator-max-backoff=17ms
```

Verify environment parsing, CLI-over-environment precedence, default policy,
zero/fractional/ordering rejection, and rejection on Produce and Stream roles.
Extend the Compose test to require all seven variables only under
`demo-consume`, with unit-bearing defaults matching Global Constraints.

- [ ] **Step 2: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app consumer_retry_policy --all-targets --locked
```

Expected: the CLI fields, environment bindings, or resolver are absent.

- [ ] **Step 3: Add the seven direct Clap fields**

Use `ByteSize`-style direct UOM parsing already established in this binary,
but with `Time`:

```rust
#[arg(
    long = "consumer-startup-attempt-timeout",
    env = "CRABKA_DEMO_CONSUMER_STARTUP_ATTEMPT_TIMEOUT",
    default_value = "90s"
)]
consumer_startup_attempt_timeout: Time,
```

Repeat with the exact names and defaults from the approved spec. Resolve once
through `ConsumerRetryPolicy::new`, reject explicit values for non-Consume
roles before telemetry initialization, and pass the policy to
`Consumer::builder().retry_policy(...)`.

- [ ] **Step 4: Add Compose ownership**

Under `demo-consume` only, add:

```yaml
CRABKA_DEMO_CONSUMER_STARTUP_ATTEMPT_TIMEOUT: "${CRABKA_DEMO_CONSUMER_STARTUP_ATTEMPT_TIMEOUT:-90s}"
CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE: "${CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE:-5m}"
CRABKA_DEMO_CONSUMER_STARTUP_INITIAL_BACKOFF: "${CRABKA_DEMO_CONSUMER_STARTUP_INITIAL_BACKOFF:-500ms}"
CRABKA_DEMO_CONSUMER_STARTUP_MAX_BACKOFF: "${CRABKA_DEMO_CONSUMER_STARTUP_MAX_BACKOFF:-5s}"
CRABKA_DEMO_CONSUMER_COORDINATOR_RETRY_TIMEOUT: "${CRABKA_DEMO_CONSUMER_COORDINATOR_RETRY_TIMEOUT:-30s}"
CRABKA_DEMO_CONSUMER_COORDINATOR_INITIAL_BACKOFF: "${CRABKA_DEMO_CONSUMER_COORDINATOR_INITIAL_BACKOFF:-100ms}"
CRABKA_DEMO_CONSUMER_COORDINATOR_MAX_BACKOFF: "${CRABKA_DEMO_CONSUMER_COORDINATOR_MAX_BACKOFF:-1s}"
```

- [ ] **Step 5: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
git add crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose consumer retry policy"
```

---

### Task 4: Audit and Verify the Slice

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-31-client-consumer-retry-policy.md`

**Interfaces:**
- Proves every old production constant is replaced by the typed policy and
  records that the broader repository audit remains active

- [ ] **Step 1: Run the focused ownership audit**

```bash
rg -n 'CONSUMER_START_ATTEMPT_TIMEOUT|CONSUMER_START_DEADLINE|COORDINATOR_RETRY_TIMEOUT|MAX_BACKOFF|ConsumerRetryPolicy|consumer-(startup|coordinator)-|CRABKA_DEMO_CONSUMER_(STARTUP|COORDINATOR)_' \
  crates/client-consumer crates/observability-demo-app demo/observability
```

Classify every production hit as a policy definition, validation, propagation,
or deployment input. There must be no unresolved old constant.

- [ ] **Step 2: Run affected all-target tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer -p observability-demo-app --all-targets --locked
```

- [ ] **Step 3: Run workspace gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo +nightly fmt --all
git diff --check
```

- [ ] **Step 4: Re-run affected tests after formatting**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-consumer -p observability-demo-app --all-targets --locked
```

- [ ] **Step 5: Update the audit and commit**

Append exact defaults, CLI/environment names, validation rules, live data
flow, test counts, and the focused audit classification. Check every completed
plan checkbox, then:

```bash
git add docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-client-consumer-retry-policy.md
git commit -m "docs(config): close consumer retry policy"
```

Do not run `cargo clean`; the user requested it only after the entire
repository-wide goal is complete.
