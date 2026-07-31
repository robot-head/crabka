# Observability Demo Consumer Timing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the classic Consumer's existing group and request timings
through the observability demo Consume role.

**Architecture:** Add four optional UOM `Time` fields to the demo CLI, resolve
their exact existing defaults before telemetry initialization, reject explicit
values on other roles, and forward them to the existing Consumer builder.
Add no library type, wrapper, dependency, or CRD.

**Tech Stack:** Rust, Clap, `crabka-units`, Docker Compose, Cargo.

## Global Constraints

- Preserve defaults: session `45s`, rebalance `1m`, heartbeat `3s`, request
  and connection `30s`.
- Use positive UOM `Time` parsing for all four inputs.
- Preserve fractional and large-time behavior from existing Consumer
  protocol-lowering helpers.
- Add no cross-field validation or new library API.
- Explicit values are valid only for Consume and fail before telemetry or I/O
  on Produce and Stream.
- Add Compose variables only to `demo-consume`.
- Add no CRD because the operator does not own the standalone demo Consumer.
- Preserve the four unrelated untracked plans dated `2026-07-28`.
- Run Cargo with
  `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not run `cargo clean`; reserve it for the completed repository-wide goal.

---

### Task 1: Expose Consume-Role Timing

**Files:**
- Modify: `crates/observability-demo-app/src/main.rs`
- Create:
  `crates/observability-demo-app/tests/consumer_timing_config.rs`
- Modify:
  `crates/observability-demo-app/tests/observability_demo_config.rs`
- Modify: `demo/observability/docker-compose.yml`

**Interfaces:**
- Consumes: existing `Consumer::builder()` setters
  `.session_timeout(Time)`, `.rebalance_timeout(Time)`,
  `.heartbeat_interval(Time)`, and `.request_timeout(Time)`
- Produces:
  `effective_consumer_timing(&Cli) ->
  std::io::Result<(Time, Time, Time, Time)>`

- [ ] **Step 1: Write failing resolver tests**

Add a unit test that parses the Consume role without explicit inputs and
asserts:

```rust
assert_eq!(
    effective_consumer_timing(&defaults).expect("default timing"),
    (secs(45), minutes(1), secs(3), secs(30))
);
```

Parse independent overrides:

```text
--consumer-session-timeout 46s
--consumer-rebalance-timeout 61s
--consumer-heartbeat-interval 4s
--consumer-request-timeout 31s
```

Assert the resolver returns `(secs(46), secs(61), secs(4), secs(31))`.

- [ ] **Step 2: Write failing subprocess and Compose tests**

Create a hermetic subprocess test using `env_clear()`. Set
`CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT=47s` on Produce and require:

```text
--consumer-session-timeout (47s) is only valid with --role consume
```

Then pass `--consumer-session-timeout 48s` on Stream with the environment
still set and require `48s` in stderr, proving CLI precedence. Pass `0ms` on
Consume and require Clap's invalid-value failure.

Extend the Compose contract to require only under `demo-consume`:

```yaml
CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT: "${CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT:-45s}"
CRABKA_DEMO_CONSUMER_REBALANCE_TIMEOUT: "${CRABKA_DEMO_CONSUMER_REBALANCE_TIMEOUT:-1m}"
CRABKA_DEMO_CONSUMER_HEARTBEAT_INTERVAL: "${CRABKA_DEMO_CONSUMER_HEARTBEAT_INTERVAL:-3s}"
CRABKA_DEMO_CONSUMER_REQUEST_TIMEOUT: "${CRABKA_DEMO_CONSUMER_REQUEST_TIMEOUT:-30s}"
```

- [ ] **Step 3: Run focused tests and confirm failure**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app consumer_timing --all-targets --locked
```

Expected: the CLI fields, resolver, and Compose values are absent.

- [ ] **Step 4: Implement the four direct inputs**

Add optional fields:

```rust
#[arg(
    long,
    env = "CRABKA_DEMO_CONSUMER_SESSION_TIMEOUT",
    value_parser = parse::positive_time
)]
consumer_session_timeout: Option<Time>,
```

Repeat for the exact rebalance, heartbeat, and request names from the approved
spec. The resolver checks the four `(flag, Option<Time>)` pairs in that order,
rejects the first explicit value on non-Consume roles, and returns:

```rust
(
    cli.consumer_session_timeout.unwrap_or_else(|| secs(45)),
    cli.consumer_rebalance_timeout
        .unwrap_or_else(|| minutes(1)),
    cli.consumer_heartbeat_interval
        .unwrap_or_else(|| secs(3)),
    cli.consumer_request_timeout
        .unwrap_or_else(|| secs(30)),
)
```

Resolve before telemetry. Pass the four values into `run_consume`, then:

```rust
Consumer::builder()
    .session_timeout(session_timeout)
    .rebalance_timeout(rebalance_timeout)
    .heartbeat_interval(heartbeat_interval)
    .request_timeout(request_timeout)
```

Add the four Compose variables only to `demo-consume`.

- [ ] **Step 5: Verify and commit**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p observability-demo-app --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
git add crates/observability-demo-app/src/main.rs \
  crates/observability-demo-app/tests/consumer_timing_config.rs \
  crates/observability-demo-app/tests/observability_demo_config.rs \
  demo/observability/docker-compose.yml
git commit -m "feat(demo): expose consumer timing"
```

---

### Task 2: Audit and Close the Slice

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify:
  `docs/superpowers/plans/2026-07-31-observability-demo-consumer-timing.md`

**Interfaces:**
- Records the direct demo-to-builder timing flow and preserves the broader
  repository audit as active

- [ ] **Step 1: Audit ownership**

```bash
rg -n \
  'consumer_(session_timeout|rebalance_timeout|heartbeat_interval|request_timeout)|consumer-(session-timeout|rebalance-timeout|heartbeat-interval|request-timeout)|CRABKA_DEMO_CONSUMER_(SESSION_TIMEOUT|REBALANCE_TIMEOUT|HEARTBEAT_INTERVAL|REQUEST_TIMEOUT)' \
  crates/observability-demo-app demo/observability
```

Every production hit must be a CLI/environment declaration, resolver,
forwarding call, or Compose input. Remaining hits must be tests.

- [ ] **Step 2: Run workspace gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

- [ ] **Step 3: Re-run demo tests after formatting**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p observability-demo-app --all-targets --locked
```

- [ ] **Step 4: Document and commit**

Append exact defaults, names, validation, runtime flow, test count, and audit
classification to `docs/configuration-audit.md`. Check every completed plan
checkbox, then:

```bash
git add docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-observability-demo-consumer-timing.md
git commit -m "docs(config): close demo consumer timing"
```
