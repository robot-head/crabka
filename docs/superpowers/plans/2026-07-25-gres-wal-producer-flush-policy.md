# Gres WAL Producer Flush Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the producer's fixed flush polling budget with one validated
deadline exposed through the generic builder, Gres CLI/environment, and fleet
CRD.

**Architecture:** Add a small validated `ProducerFlushTimeout` scalar and keep
the builder's raw `Duration` input source-compatible. `Producer::flush`
registers its notification before checking drained state and waits against one
absolute deadline, eliminating the polling interval. Gres carries the typed
value through its existing recovery config, and the operator renders the same
effective value.

**Tech Stack:** Rust, Tokio `Notify`/paused time, `refined_type`, Clap
CLI/environment parsing, kube/schemars CRDs.

## Global Constraints

- Preserve the existing 50-second effective default.
- Accept whole milliseconds in `1..=2,147,483,647`; reject zero, fractions,
  and overflow before broker I/O.
- Expose only the total timeout. Do not expose or retain the 50-millisecond
  polling interval.
- Use `refined_type` at the validated scalar boundary.
- Add no dependency and no generic policy framework.
- Preserve unrelated dirty files and commit only scoped files.
- Every Cargo command uses
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Follow RED/GREEN/refactor and obtain independent spec and quality review for
  every implementation task.

---

### Task 1: Validate and honor the generic producer flush deadline

**Files:**
- Modify: `crates/client-producer/src/builder.rs`
- Modify: `crates/client-producer/src/producer.rs`
- Modify: `crates/client-producer/src/lib.rs`

**Interfaces:**
- Produces:
  `ProducerFlushTimeout::new(Duration) -> Result<ProducerFlushTimeout, String>`
- Produces: `ProducerFlushTimeout::{duration, milliseconds}`
- Produces: `DEFAULT_PRODUCER_FLUSH_TIMEOUT: Duration`
- Preserves: `Producer::builder().flush_timeout(Duration).build()`

- [ ] **Step 1: Add RED validation and builder tests**

In `builder.rs`, add focused tests that pin the exact default, a distinctive
11-millisecond value, zero, one nanosecond, and
`i32::MAX milliseconds + 1`. Extend the existing pre-I/O builder rejection
test with:

```rust
invalid!(
    flush_timeout,
    Duration::ZERO,
    "producer flush timeout"
);
```

The complete-value test must assert:

```rust
assert_eq!(
    (
        ProducerFlushTimeout::default().duration(),
        ProducerFlushTimeout::default().milliseconds(),
    ),
    (Duration::from_secs(50), 50_000),
);
```

- [ ] **Step 2: Run RED validation tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer \
  builder::security_arg_tests::producer_flush_timeout -- --nocapture
```

Expected: FAIL because `ProducerFlushTimeout` and the builder input do not
exist.

- [ ] **Step 3: Add the minimal validated scalar and builder wiring**

In `builder.rs`, add:

```rust
pub const DEFAULT_PRODUCER_FLUSH_TIMEOUT: Duration = Duration::from_secs(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerFlushTimeout(Duration);

impl ProducerFlushTimeout {
    pub fn new(value: Duration) -> Result<Self, String> {
        validated_protocol_duration(value, "producer flush timeout").map(Self)
    }

    #[must_use]
    pub const fn duration(self) -> Duration {
        self.0
    }

    #[must_use]
    pub fn milliseconds(self) -> i32 {
        protocol_milliseconds(self.0)
    }
}

impl Default for ProducerFlushTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_PRODUCER_FLUSH_TIMEOUT)
            .expect("default producer flush timeout is valid")
    }
}
```

Add the builder input:

```rust
#[builder(default = DEFAULT_PRODUCER_FLUSH_TIMEOUT)]
flush_timeout: Duration,
```

Validate it before client construction and store the validated scalar on
`Producer`. Re-export the scalar and default from `lib.rs`.

- [ ] **Step 4: Run GREEN validation tests**

Run the command from Step 2, then:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer \
  builder::security_arg_tests::producer_builder_rejects_flush_timeout_before_connection_io
```

Expected: PASS with field-specific pre-I/O rejection.

- [ ] **Step 5: Add RED deadline and missed-wakeup tests**

In `producer.rs`, use paused Tokio time and the existing mock producer fixture.
Add:

```rust
#[tokio::test(start_paused = true)]
async fn flush_times_out_at_the_configured_deadline()
```

Set `in_flight` to one, configure seven milliseconds, start `flush`, advance
six milliseconds and assert it is pending, then advance one millisecond and
assert `ProducerError::FlushTimeout`.

Add:

```rust
#[tokio::test]
async fn flush_does_not_miss_notification_during_state_check()
```

Hold one accumulator lock so `all_empty` blocks, start `flush`, clear the
accumulator and call `flush_notify.notify_waiters()` before releasing the
lock, then assert flush completes without waiting for another notification.
This test must fail against check-then-subscribe polling code when configured
with a short deadline.

- [ ] **Step 6: Run RED behavior tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer \
  producer::tests::flush_ -- --nocapture
```

Expected: at least the exact-deadline assertion FAILS because `flush` still
uses 50-millisecond polling and 1,000 iterations.

- [ ] **Step 7: Replace polling with one race-free deadline**

In `Producer::flush`, keep the force wake and replace the polling loop with:

```rust
let deadline = tokio::time::Instant::now()
    .checked_add(self.flush_timeout.duration())
    .ok_or(ProducerError::FlushTimeout)?;

tokio::time::timeout_at(deadline, async {
    loop {
        let notified = self.flush_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.all_empty().await && self.in_flight.load(Ordering::Acquire) == 0 {
            return;
        }
        notified.await;
    }
})
.await
.map_err(|_| ProducerError::FlushTimeout)
```

Do not retain an interval, attempt count, sleep, or ticker.

- [ ] **Step 8: Run generic producer gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-producer --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 9: Commit and review**

```bash
git add crates/client-producer/src/builder.rs \
  crates/client-producer/src/producer.rs \
  crates/client-producer/src/lib.rs
git commit -m "feat(producer): configure flush timeout"
```

Obtain independent spec and quality approval. Return every finding to this
task's implementer and commit remediations separately.

---

### Task 2: Carry the flush timeout through Gres CLI and runtime

**Files:**
- Modify: `crates/gres-substrate/src/recovery.rs`
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/gres/tests/runtime.rs`

**Interfaces:**
- Consumes: `ProducerFlushTimeout` and `DEFAULT_PRODUCER_FLUSH_TIMEOUT`
- Produces:
  `LiveRecoveryConfig::{with_producer_flush_timeout, producer_flush_timeout}`
- Produces: `ServeArgs::wal_producer_flush_timeout_ms`
- Produces: `SubstrateRuntimeConfig::producer_flush_timeout`

- [ ] **Step 1: Add RED Gres parser and propagation tests**

Add focused tests that prove:

- omitted input resolves to 50,000 milliseconds;
- environment input is accepted;
- CLI overrides environment with a distinctive value;
- zero, `2,147,483,648`, and explicit local-mode use fail;
- the hostile WAL environment matrix clears
  `CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS`;
- `LiveRecoveryConfig` defaults and replacement are exact;
- the sole WAL producer construction receives `.flush_timeout(...)`;
- `--help` contains `--wal-producer-flush-timeout-ms`.

- [ ] **Step 2: Run RED Gres tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres \
  tests::wal_producer_flush_timeout -- --nocapture
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate \
  producer_flush_timeout -- --nocapture
```

Expected: FAIL because the argument and recovery field do not exist.

- [ ] **Step 3: Add the minimal CLI/environment and runtime flow**

Add to `ServeArgs`:

```rust
#[arg(
    long = "wal-producer-flush-timeout-ms",
    env = "CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS",
    requires = "substrate_bootstrap"
)]
pub wal_producer_flush_timeout_ms: Option<PositiveMillis>,
```

Resolve it through:

```rust
let default_ms = u64::try_from(
    ProducerFlushTimeout::default().duration().as_millis(),
)
.expect("default producer flush timeout fits u64 milliseconds");
ProducerFlushTimeout::new(Duration::from_millis(
    args.wal_producer_flush_timeout_ms.map_or(
        default_ms,
        PositiveMillis::into_value,
    ),
))
```

Store the typed scalar in `SubstrateRuntimeConfig` and
`LiveRecoveryConfig`. Add one builder/getter pair matching the existing
producer retry and throughput methods. Apply:

```rust
.flush_timeout(config.producer_flush_timeout().duration())
```

at the sole WAL producer builder call. Include the new option in existing
pre-I/O validation and every direct `ServeArgs` fixture.

- [ ] **Step 4: Run GREEN and full Gres gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-substrate --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-substrate -p crabka-gres \
  --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 5: Commit and review**

```bash
git add crates/gres-substrate/src/recovery.rs \
  crates/gres/src/lib.rs crates/gres/tests/runtime.rs
git commit -m "feat(gres): expose WAL producer flush timeout"
```

Obtain independent spec and quality approval and remediate every finding.

---

### Task 3: Add the fleet CRD flush timeout

**Files:**
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: `deploy/crds/crabka.io_greses.yaml`

**Interfaces:**
- Consumes: `ProducerFlushTimeout`
- Produces: `GresComputeSpec::wal_producer_flush_timeout_ms: Option<u64>`
- Produces:
  `EffectiveGresComputePolicy::wal_producer_flush_timeout`
- Produces exact CLI pair for every substrate-backed compute deployment

- [ ] **Step 1: Add RED schema, validation, and rendering tests**

Extend focused operator tests to assert:

```rust
walProducerFlushTimeoutMs:
  type: integer
  format: uint64
  minimum: 1
  maximum: 2147483647
```

Pin the 50,000 default, a distinctive override, exact zero/overflow errors
beginning with
`spec.compute.walProducerFlushTimeoutMs: producer flush timeout:`, and exact
one-pair rendering in one single-range deployment and both deployments of a
genuine two-range layout.

- [ ] **Step 2: Run RED operator tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
  wal_producer_flush_timeout -- --nocapture
```

Expected: FAIL because the CRD field and renderer do not exist.

- [ ] **Step 3: Add the CRD field and effective value**

Add:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1, max = 2_147_483_647))]
pub wal_producer_flush_timeout_ms: Option<u64>,
```

Resolve it in `effective_policy` through `ProducerFlushTimeout::new`, mapping
errors to the exact camel-case field path. Store the typed value on
`EffectiveGresComputePolicy`.

Add a two-element argument helper:

```rust
fn wal_producer_flush_args(policy: ProducerFlushTimeout) -> [String; 2] {
    [
        "--wal-producer-flush-timeout-ms".to_owned(),
        policy.milliseconds().to_string(),
    ]
}
```

Extend the central compute argument assembly once so single-range and
multi-range deployments share the same rendering path.

- [ ] **Step 4: Regenerate and verify all CRDs**

Generate into two fresh temporary directories:

```bash
crd_a=$(mktemp -d)
crd_b=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_a"
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_b"
test "$(find "$crd_a" -maxdepth 1 -type f | wc -l)" -eq 9
diff -ru "$crd_a" "$crd_b"
cp "$crd_a"/*.yaml deploy/crds/
```

Expected: both generations contain nine identical files; after copying, only
the Gres CRD differs from Task 2 HEAD.

- [ ] **Step 5: Run GREEN and full operator gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Expected: all pass.

- [ ] **Step 6: Commit and review**

```bash
git add crates/operator/src/crd/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_greses.yaml
git commit -m "feat(operator): expose WAL flush timeout"
```

Obtain independent spec and quality approval and remediate every finding.

---

### Task 4: Audit, verify, and publish

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes the completed generic, Gres, and operator implementation
- Produces audit evidence and updates draft PR #904

- [ ] **Step 1: Re-run the repository scanner and focused search**

Run:

```bash
tools/audit-runtime-values.sh
rg -n \
  "flush_timeout|flush-timeout|FlushTimeout|Duration::from_millis\\(50\\)|for _ in 0\\.\\.1000" \
  crates/client-producer crates/gres-substrate crates/gres crates/operator deploy/crds
```

Classify every production match. The generic producer must contain one named
50-second default and no flush polling interval or attempt count.

- [ ] **Step 2: Update the audit ledger**

Add a `Gres WAL Producer Flush Policy` section documenting:

- the named 50-second default and exact validation bounds;
- the race-free subscribe-before-check deadline loop;
- the generic builder and live Gres CLI/environment/CRD flow;
- removal rather than exposure of the meaningless 50-millisecond poll;
- focused scanner counts and all verification evidence;
- the next coherent pending owner without claiming the repository-wide goal is
  complete.

- [ ] **Step 3: Commit the audit**

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record WAL flush audit"
```

- [ ] **Step 4: Run broad final verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-client-producer -p crabka-gres-substrate \
  -p crabka-gres -p crabka-operator --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-client-producer -p crabka-gres-substrate \
  -p crabka-gres -p crabka-operator --all-targets -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help |
  rg -- "--wal-producer-flush-timeout-ms"
cargo fmt --all -- --check
git diff --check
```

Regenerate all nine CRDs twice again and compare both directories exactly with
`deploy/crds`.

- [ ] **Step 5: Obtain final review and publish**

Obtain a fresh read-only review across the spec, plan, implementation, tests,
CRD, and audit. Remediate every finding through the original task implementer,
then rerun the relevant gates.

Push `configuration_expose`, verify the remote branch SHA equals local HEAD,
and verify draft PR #904 reports the same head SHA.

The repository-wide hardcoded-value goal remains active afterward.
