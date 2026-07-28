# Gres Range-0 Follower Poll Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Gres's fixed 100 ms range-0 follower polling cadence with one validated CLI/environment setting and a typed fleet CRD field.

**Architecture:** `crabka-gres-control` owns the single compiled default. Gres resolves an optional `PositiveMillis` parser value into `SubstrateRuntimeConfig`, and the existing follower loop consumes that duration. The operator validates the same value in `GresComputeSpec` and emits it only with the existing multi-range arguments.

**Tech Stack:** Rust 2024, Clap, `refined_type` through `PositiveMillis`, Tokio, kube/schemars, serde, generated Kubernetes CRDs.

## Global Constraints

- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Reuse `PositiveMillis`; add no dependency, newtype, or one-field policy struct.
- Use exactly `--range0-follower-poll-interval-ms` and `CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS`.
- Keep the parser field optional; the one shared effective default is 100 ms.
- Reject zero and explicit standalone use without `--ranges`.
- Render the operator argument only in the existing range-control branch that renders `--ranges`.
- Do not change Kafka WAL fetch wait, empty-fetch retry count, connection timeouts, notification semantics, coordinator identity, or offset/protocol arithmetic.
- Preserve unrelated dirty files and commit only each task's scoped files.

---

### Task 1: Configure the Gres follower cadence

**Files:**

- Modify: `crates/gres-control/src/lib.rs`
- Modify test feature only: `crates/gres/Cargo.toml`
- Modify: `crates/gres/src/lib.rs`
- Modify constructor fallout only: `crates/gres/tests/runtime.rs`

**Interfaces:**

- Produces: `crabka_gres_control::DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS: u64`
- Produces: `ServeArgs::range0_follower_poll_interval_ms: Option<PositiveMillis>`
- Produces: `SubstrateRuntimeConfig::range0_follower_poll_interval: Duration`
- Produces: `async fn wait_for_range0_follower_refresh(&Notify, Duration)`

- [x] **Step 1: Add RED parser and effective-configuration tests**

Add a child-process parser test beside the checkpoint/local-vacuum parser
tests. It must scrub the environment for the default branch, inject it for the
environment branch, and prove CLI precedence:

```rust
#[test]
fn range0_follower_poll_interval_uses_default_environment_and_cli_precedence() {
    const CHILD: &str = "CRABKA_TEST_GRES_RANGE0_FOLLOWER_POLL_CHILD";
    const ENV: &str = "CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS";
    let base = [
        "crabka-gres",
        "--substrate-bootstrap=memory://",
        "--tenant=tenant-a",
        "--ranges=0,10",
    ];
    if std::env::var_os(CHILD).is_none() {
        for (mode, value) in [("default", None), ("environment", Some("17"))] {
            let mut child =
                std::process::Command::new(std::env::current_exe().expect("test exe"));
            child
                .args([
                    "--exact",
                    "tests::range0_follower_poll_interval_uses_default_environment_and_cli_precedence",
                ])
                .env(CHILD, mode);
            match value {
                Some(value) => {
                    child.env(ENV, value);
                }
                None => {
                    child.env_remove(ENV);
                }
            }
            assert!(child.status().expect("child test").success());
        }
        return;
    }

    let expected = if std::env::var(CHILD).as_deref() == Ok("environment") {
        17
    } else {
        DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS
    };
    let parsed = Cli::try_parse_from(base).expect("policy").serve;
    assert_eq!(
        SubstrateRuntimeConfig::from_args(&parsed)
            .expect("valid config")
            .expect("substrate config")
            .range0_follower_poll_interval,
        Duration::from_millis(expected)
    );

    let cli = Cli::try_parse_from(
        base.into_iter()
            .chain(["--range0-follower-poll-interval-ms=19"]),
    )
    .expect("CLI policy")
    .serve;
    assert_eq!(
        SubstrateRuntimeConfig::from_args(&cli)
            .expect("valid config")
            .expect("substrate config")
            .range0_follower_poll_interval,
        Duration::from_millis(19)
    );
}
```

Add boundary/mode assertions:

```rust
#[test]
fn range0_follower_poll_interval_rejects_zero_and_non_multirange_use() {
    assert!(
        Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--ranges=0,10",
            "--range0-follower-poll-interval-ms=0",
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            "--range0-follower-poll-interval-ms=1",
        ])
        .is_err()
    );

    let mut programmatic = Cli::try_parse_from(["crabka-gres"])
        .expect("defaults")
        .serve;
    programmatic.range0_follower_poll_interval_ms =
        Some(PositiveMillis::new(1).expect("positive"));
    let error = SubstrateRuntimeConfig::from_args(&programmatic)
        .expect_err("programmatic non-multirange configuration");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
```

- [x] **Step 2: Add a RED runtime cadence test**

Extract the existing wait into the wished-for production helper and test it
with paused Tokio time:

```rust
#[tokio::test(start_paused = true)]
async fn configured_range0_follower_poll_and_poke_control_refresh() {
    let poke = Arc::new(tokio::sync::Notify::new());
    let periodic_poke = Arc::clone(&poke);
    let periodic = tokio::spawn(async move {
        wait_for_range0_follower_refresh(&periodic_poke, Duration::from_millis(7)).await;
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(6)).await;
    assert!(!periodic.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    periodic.await.expect("periodic wake");

    let notified_poke = Arc::clone(&poke);
    let notified = tokio::spawn(async move {
        wait_for_range0_follower_refresh(&notified_poke, Duration::from_secs(60)).await;
    });
    tokio::task::yield_now().await;
    poke.notify_one();
    notified.await.expect("notification wake");
}
```

- [x] **Step 3: Enable deterministic paused-time testing**

Add Tokio's existing `test-util` feature to the Gres dev-dependency. This adds
no dependency and is required for `#[tokio::test(start_paused = true)]` and
`tokio::time::advance`:

```toml
tokio = { workspace = true, features = ["full", "test-util"] }
```

- [x] **Step 4: Run the RED tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres range0_follower_poll_interval --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres configured_range0_follower_poll_and_poke --lib
```

Expected: compilation fails because the shared default, parser/config fields,
and wait helper do not exist.

- [x] **Step 5: Add the shared default and optional parser field**

In `crates/gres-control/src/lib.rs`, add the only default owner:

```rust
/// Default periodic range-0 follower refresh cadence in milliseconds.
pub const DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS: u64 = 100;
```

Import it in Gres and add this field to `ServeArgs`:

```rust
/// Periodic range-0 follower refresh cadence in multi-range substrate mode.
#[arg(
    long = "range0-follower-poll-interval-ms",
    env = "CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS",
    requires = "ranges"
)]
pub range0_follower_poll_interval_ms: Option<PositiveMillis>,
```

- [x] **Step 6: Resolve and consume the effective duration**

Add the field to `SubstrateRuntimeConfig`:

```rust
/// Periodic refresh cadence for a remote range-0 follower.
pub range0_follower_poll_interval: Duration,
```

Before the early return for absent substrate bootstrap, reject programmatic
inert use. Then resolve the field in `SubstrateRuntimeConfig::from_args`:

```rust
if args.range0_follower_poll_interval_ms.is_some() && args.ranges.is_none() {
    return invalid_input("--range0-follower-poll-interval-ms requires --ranges");
}

range0_follower_poll_interval: Duration::from_millis(
    args.range0_follower_poll_interval_ms.map_or(
        DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
        PositiveMillis::into_value,
    ),
),
```

Update existing `SubstrateRuntimeConfig` literals with:

```rust
range0_follower_poll_interval: Duration::from_millis(
    DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS,
),
```

Add the production-used wait helper:

```rust
async fn wait_for_range0_follower_refresh(
    catalog_refresh_poke: &tokio::sync::Notify,
    poll_interval: Duration,
) {
    tokio::select! {
        () = catalog_refresh_poke.notified() => {}
        () = tokio::time::sleep(poll_interval) => {}
    }
}
```

Capture `config.range0_follower_poll_interval` before `tokio::spawn` and
replace the inline fixed sleep/select with:

```rust
wait_for_range0_follower_refresh(
    &catalog_refresh_poke,
    range0_follower_poll_interval,
)
.await;
```

- [x] **Step 7: Run focused and full Task 1 verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres range0_follower_poll_interval --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres configured_range0_follower_poll_and_poke --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-control -p crabka-gres \
    --all-targets --all-features -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help |
  rg -- '--range0-follower-poll-interval-ms'
cargo fmt --all -- --check
git diff --check
```

Expected: all tests and strict gates pass; help lists exactly the new flag.

- [x] **Step 8: Commit Task 1**

```bash
git add crates/gres-control/src/lib.rs crates/gres/Cargo.toml \
  crates/gres/src/lib.rs crates/gres/tests/runtime.rs
git commit -m "feat(gres): configure range0 follower polling"
```

---

### Task 2: Expose the cadence through the Gres CRD

**Files:**

- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify generated file: `deploy/crds/crabka.io_greses.yaml`

**Interfaces:**

- Consumes: `DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS`
- Produces: `GresComputeSpec::range0_follower_poll_interval_ms: Option<u64>`
- Produces: `EffectiveGresComputePolicy::range0_follower_poll_interval_ms: PositiveMillis`

- [x] **Step 1: Add RED CRD policy tests**

Extend `compute_checkpoint_lifecycle_policy_round_trips_and_has_exact_schema_bounds`
with:

```rust
range0_follower_poll_interval_ms: Some(1),
```

and include `"range0FollowerPollIntervalMs"` in the fields whose schema
minimum must equal one.

Extend the exact-default/boundary test:

```rust
assert!(
    defaults
        .range0_follower_poll_interval_ms
        .into_value()
        == DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS
);
```

Add this table entry:

```rust
(
    GresComputeSpec {
        range0_follower_poll_interval_ms: Some(0),
        ..GresComputeSpec::default()
    },
    "spec.compute.range0FollowerPollIntervalMs",
),
```

- [x] **Step 2: Add a RED renderer test**

Add the new flag to the single-range absence test:

```rust
assert!(
    !args
        .iter()
        .any(|arg| arg == "--range0-follower-poll-interval-ms")
);
```

Change `compute_workload_renders_custom_policy` to use these two valid ranges:

```rust
let ranges = [
    GresTenantRangeSpec {
        range_id: 0,
        end_key: Some(GresTenantRangeKey {
            table_id: 10,
            bucket: None,
            rowid: 0,
        }),
    },
    GresTenantRangeSpec {
        range_id: 1,
        end_key: None,
    },
];
```

Pass `range_control_enabled: true`. Set:

```rust
range0_follower_poll_interval_ms: Some(5_678),
```

and add the exact rendered pair:

```rust
["--range0-follower-poll-interval-ms", "5678"],
```

- [x] **Step 3: Run the RED operator tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
    compute_checkpoint_lifecycle_policy --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator compute_workload --lib
```

Expected: compilation fails because the CRD and effective-policy fields do not
exist.

- [x] **Step 4: Add the CRD and effective policy fields**

Import the shared default and add:

```rust
/// Periodic range-0 follower refresh cadence in milliseconds.
#[serde(default, skip_serializing_if = "Option::is_none")]
#[schemars(range(min = 1))]
pub range0_follower_poll_interval_ms: Option<u64>,
```

Add to `EffectiveGresComputePolicy`:

```rust
pub(crate) range0_follower_poll_interval_ms: PositiveMillis,
```

Resolve it in `effective_policy`:

```rust
range0_follower_poll_interval_ms: PositiveMillis::new(
    self.range0_follower_poll_interval_ms
        .unwrap_or(DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS),
)
.map_err(|error| format!("spec.compute.range0FollowerPollIntervalMs: {error}"))?,
```

- [x] **Step 5: Render only with range control**

Inside the existing `if config.range_control_enabled` argument extension, add:

```rust
"--range0-follower-poll-interval-ms".to_owned(),
config
    .compute_policy
    .range0_follower_poll_interval_ms
    .into_value()
    .to_string(),
```

Do not add an environment variable or render the flag outside that branch.

- [x] **Step 6: Regenerate and inspect CRDs**

First generate to a temporary directory:

```bash
crd_dir=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_dir"
diff -ru deploy/crds "$crd_dir"
```

Expected before updating: only
`crabka.io_greses.yaml` differs, adding the optional integer field with
`minimum: 1` and no schema default.

Then regenerate the checked-in CRDs:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds deploy/crds
```

- [x] **Step 7: Run focused and full Task 2 verification**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator \
    compute_checkpoint_lifecycle_policy --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator compute_workload --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check

crd_verify_dir=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_verify_dir"
diff -ru deploy/crds "$crd_verify_dir"
```

Expected: all tests and strict gates pass; all nine generated CRDs match
exactly.

- [x] **Step 8: Commit Task 2**

```bash
git add crates/operator/src/crd/gres.rs \
  crates/operator/src/controller/gres_tenant.rs \
  deploy/crds/crabka.io_greses.yaml
git commit -m "feat(operator): expose range0 follower polling"
```

---

### Task 3: Audit and close the follower-poll slice

**Files:**

- Modify: `docs/configuration-audit.md`
- Modify without committing unless already tracked for this workflow: `.superpowers/sdd/progress.md`

**Interfaces:**

- Consumes: Task 1 and Task 2 implementation commits
- Produces: durable classification and verification evidence for the next owner

- [x] **Step 1: Run the repository and focused scans**

```bash
tools/audit-runtime-values.sh > /tmp/crabka-gres-range0-follower-values.txt
rg -n \
  'range0.follower|range-0 follower|RANGE0_FOLLOWER|from_millis\\(100\\)' \
  crates/gres-control crates/gres crates/operator deploy/crds \
  > /tmp/crabka-gres-range0-follower-focused.txt
```

Classify every focused production numeric value as the shared default, live
configured consumer, fixed protocol/topology invariant, test/harness value, or
next-owner value. There must be no unexplained fixed 100 ms follower sleep.

- [x] **Step 2: Trace the complete live path**

Confirm by source inspection:

```text
GresComputeSpec
  -> EffectiveGresComputePolicy
  -> rendered multi-range Deployment argument
  -> ServeArgs
  -> SubstrateRuntimeConfig
  -> attach_range0_read_barrier
  -> wait_for_range0_follower_refresh
```

Also confirm CLI-over-environment precedence, zero rejection, explicit
non-multirange rejection, notification wake preservation, and absence from
single-range Deployment arguments.

- [x] **Step 3: Run final cross-crate gates**

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres-control --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres-control -p crabka-gres -p crabka-operator \
    --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check

crd_audit_dir=$(mktemp -d)
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-operator -- gen-crds "$crd_audit_dir"
diff -ru deploy/crds "$crd_audit_dir"
```

Expected: every test and strict gate passes, and all nine CRDs are exact.

- [x] **Step 4: Record the audit**

Append a `Gres Range-0 Follower Poll Policy` section to
`docs/configuration-audit.md` containing:

- the CLI, environment, CRD field, shared default, and validation;
- the exact live-consumer trace;
- why notification wakeup and topology/protocol values remain fixed;
- scanner/focused-search counts and classifications;
- test, Clippy, formatting, help, and CRD evidence;
- the next coherent owner: generic WAL recovery fetch/retry policy beginning
  with `FETCH_MAX_WAIT_MS` and `EMPTY_FETCH_RETRIES` in
  `crates/gres-substrate/src/recovery.rs`.

Update `.superpowers/sdd/progress.md` with the same concise completion evidence.

- [x] **Step 5: Commit Task 3**

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record follower poll audit"
```

Do not stage the progress ledger or unrelated dirty plans/reports.
