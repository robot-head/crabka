# Gres Local Vacuum Policy Implementation Plan

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Gres's hardcoded local-vacuum pacing and debt values with validated CLI/environment configuration that is rejected outside local runtime mode.

**Architecture:** A flattened `LocalVacuumOptions` parser group remains optional so explicit configuration can be distinguished from compiled local defaults. One validated `LocalVacuumPolicy` is constructed before runtime I/O, passed to the existing `VacuumPacer` and loop, and never rendered by the operator because substrate engines do not run local vacuum.

**Tech Stack:** Rust 2024, Clap, `refined_type`-backed Gres-control scalar types, Tokio, nextest.

## Global Constraints

- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Use existing `PositiveMillis`, `PositiveUsize`, and `NonZeroU64`; add no dependency or redundant validated wrapper.
- Keep all parser fields optional with environment bindings and no Clap defaults.
- Reject explicit local-vacuum inputs with `--substrate-bootstrap`; never accept an inert knob.
- Add no operator or CRD fields because operator compute engines use substrate mode.
- Keep adaptive factors, zero-delay hot mode, state transitions, statistic membership, and local-only enablement fixed.
- Preserve unrelated dirty files and commit only each task's scoped files.

---

### Task 1: Expose and wire the effective local policy

**Files:**

- Modify: `crates/gres/src/lib.rs`
- Modify constructor fallout only: `crates/gres/tests/runtime.rs`

**Interfaces:**

- Produces: `pub struct LocalVacuumOptions`
- Produces: `fn local_vacuum_policy(args: &ServeArgs) -> std::io::Result<Option<LocalVacuumPolicy>>`
- Produces: internal `LocalVacuumPolicy` consumed by `VacuumPacer` and the local loop

- [x] **Step 1: Add RED parser, environment, validation, and mode tests**

Add tests beside the existing registry/checkpoint parser tests:

```rust
#[test]
fn local_vacuum_options_are_absent_by_default_and_cli_overrides_environment() {
    let defaults = Cli::try_parse_from(["crabka-gres"]).expect("defaults").serve;
    assert_eq!(defaults.local_vacuum, LocalVacuumOptions::default());

    const CHILD: &str = "CRABKA_TEST_GRES_LOCAL_VACUUM_ENV_CHILD";
    let variables = [
        ("CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS", "11"),
        ("CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS", "12"),
        ("CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT", "13"),
        ("CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET", "14"),
        ("CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET", "15"),
        ("CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS", "16"),
        ("CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS", "17"),
        ("CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS", "18"),
    ];
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().expect("test exe"))
            .args([
                "--exact",
                "tests::local_vacuum_options_are_absent_by_default_and_cli_overrides_environment",
            ])
            .env(CHILD, "1")
            .envs(variables)
            .status()
            .expect("child test");
        assert!(status.success());
        return;
    }

    let environment = Cli::try_parse_from(["crabka-gres"])
        .expect("environment policy")
        .serve
        .local_vacuum;
    assert_eq!(environment.idle_interval_ms.map(PositiveMillis::into_value), Some(11));
    assert_eq!(environment.backoff_floor_ms.map(PositiveMillis::into_value), Some(12));
    assert_eq!(environment.hot_debt.map(NonZeroU64::get), Some(13));
    assert_eq!(environment.key_budget.map(PositiveUsize::into_value), Some(14));
    assert_eq!(environment.max_key_budget.map(PositiveUsize::into_value), Some(15));
    assert_eq!(environment.step_fast_ms.map(PositiveMillis::into_value), Some(16));
    assert_eq!(environment.step_slow_ms.map(PositiveMillis::into_value), Some(17));
    assert_eq!(environment.idle_after_ms.map(PositiveMillis::into_value), Some(18));

    let cli = Cli::try_parse_from([
        "crabka-gres",
        "--local-vacuum-idle-interval-ms", "21",
        "--local-vacuum-backoff-floor-ms", "22",
        "--local-vacuum-hot-debt", "23",
        "--local-vacuum-key-budget", "24",
        "--local-vacuum-max-key-budget", "25",
        "--local-vacuum-step-fast-ms", "26",
        "--local-vacuum-step-slow-ms", "27",
        "--local-vacuum-idle-after-ms", "28",
    ]).expect("CLI policy").serve.local_vacuum;
    assert_eq!(cli.idle_interval_ms.map(PositiveMillis::into_value), Some(21));
    assert_eq!(cli.backoff_floor_ms.map(PositiveMillis::into_value), Some(22));
    assert_eq!(cli.hot_debt.map(NonZeroU64::get), Some(23));
    assert_eq!(cli.key_budget.map(PositiveUsize::into_value), Some(24));
    assert_eq!(cli.max_key_budget.map(PositiveUsize::into_value), Some(25));
    assert_eq!(cli.step_fast_ms.map(PositiveMillis::into_value), Some(26));
    assert_eq!(cli.step_slow_ms.map(PositiveMillis::into_value), Some(27));
    assert_eq!(cli.idle_after_ms.map(PositiveMillis::into_value), Some(28));
}
```

Add table-driven zero rejection for all eight flags, relationship failures for
floor greater than idle, base greater than max, and fast greater than or equal
to slow, plus a table proving every explicit flag rejects with substrate mode:

```rust
#[test]
fn local_vacuum_policy_rejects_invalid_relationships_and_substrate_noops() {
    for option in [
        "--local-vacuum-idle-interval-ms=0",
        "--local-vacuum-backoff-floor-ms=0",
        "--local-vacuum-hot-debt=0",
        "--local-vacuum-key-budget=0",
        "--local-vacuum-max-key-budget=0",
        "--local-vacuum-step-fast-ms=0",
        "--local-vacuum-step-slow-ms=0",
        "--local-vacuum-idle-after-ms=0",
    ] {
        assert!(Cli::try_parse_from(["crabka-gres", option]).is_err());
    }

    for arguments in [
        ["--local-vacuum-idle-interval-ms", "10",
         "--local-vacuum-backoff-floor-ms", "11"].as_slice(),
        ["--local-vacuum-key-budget", "10",
         "--local-vacuum-max-key-budget", "9"].as_slice(),
        ["--local-vacuum-step-fast-ms", "10",
         "--local-vacuum-step-slow-ms", "10"].as_slice(),
    ] {
        let args = Cli::try_parse_from(
            std::iter::once("crabka-gres").chain(arguments.iter().copied())
        ).expect("scalar-valid arguments").serve;
        assert!(local_vacuum_policy(&args).is_err());
    }

    for option in [
        "--local-vacuum-idle-interval-ms=1",
        "--local-vacuum-backoff-floor-ms=1",
        "--local-vacuum-hot-debt=1",
        "--local-vacuum-key-budget=1",
        "--local-vacuum-max-key-budget=1",
        "--local-vacuum-step-fast-ms=1",
        "--local-vacuum-step-slow-ms=1",
        "--local-vacuum-idle-after-ms=1",
    ] {
        let args = Cli::try_parse_from([
            "crabka-gres",
            "--substrate-bootstrap=memory://",
            "--tenant=tenant-a",
            option,
        ]).expect("scalar-valid arguments").serve;
        assert!(local_vacuum_policy(&args).is_err());
    }
}
```

- [x] **Step 2: Run the RED tests**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres local_vacuum_options --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres local_vacuum_policy_rejects --lib
```

Expected: compilation fails because `LocalVacuumOptions`, its `ServeArgs`
field, and `local_vacuum_policy` do not exist.

- [x] **Step 3: Add the minimal optional parser group**

Add a flattened field to `ServeArgs`:

```rust
#[command(flatten)]
pub local_vacuum: LocalVacuumOptions,
```

Define the group with these exact bindings and no defaults:

```rust
#[derive(clap::Args, Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct LocalVacuumOptions {
    #[arg(long = "local-vacuum-idle-interval-ms",
          env = "CRABKA_GRES_LOCAL_VACUUM_IDLE_INTERVAL_MS")]
    idle_interval_ms: Option<PositiveMillis>,
    #[arg(long = "local-vacuum-backoff-floor-ms",
          env = "CRABKA_GRES_LOCAL_VACUUM_BACKOFF_FLOOR_MS")]
    backoff_floor_ms: Option<PositiveMillis>,
    #[arg(long = "local-vacuum-hot-debt",
          env = "CRABKA_GRES_LOCAL_VACUUM_HOT_DEBT")]
    hot_debt: Option<NonZeroU64>,
    #[arg(long = "local-vacuum-key-budget",
          env = "CRABKA_GRES_LOCAL_VACUUM_KEY_BUDGET")]
    key_budget: Option<PositiveUsize>,
    #[arg(long = "local-vacuum-max-key-budget",
          env = "CRABKA_GRES_LOCAL_VACUUM_MAX_KEY_BUDGET")]
    max_key_budget: Option<PositiveUsize>,
    #[arg(long = "local-vacuum-step-fast-ms",
          env = "CRABKA_GRES_LOCAL_VACUUM_STEP_FAST_MS")]
    step_fast_ms: Option<PositiveMillis>,
    #[arg(long = "local-vacuum-step-slow-ms",
          env = "CRABKA_GRES_LOCAL_VACUUM_STEP_SLOW_MS")]
    step_slow_ms: Option<PositiveMillis>,
    #[arg(long = "local-vacuum-idle-after-ms",
          env = "CRABKA_GRES_LOCAL_VACUUM_IDLE_AFTER_MS")]
    idle_after_ms: Option<PositiveMillis>,
}
```

Update only `ServeArgs` literals with
`local_vacuum: LocalVacuumOptions::default()`.

- [x] **Step 4: Construct and validate one effective policy**

Use one internal policy and one constructor:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalVacuumPolicy {
    idle_interval: Duration,
    backoff_floor: Duration,
    hot_debt: u64,
    key_budget: usize,
    max_key_budget: usize,
    step_fast: Duration,
    step_slow: Duration,
    idle_after: Duration,
}

const DEFAULT_LOCAL_VACUUM_IDLE_INTERVAL_MS: u64 = 2_000;
const DEFAULT_LOCAL_VACUUM_BACKOFF_FLOOR_MS: u64 = 25;
const DEFAULT_LOCAL_VACUUM_STEP_FAST_MS: u64 = 3;
const DEFAULT_LOCAL_VACUUM_STEP_SLOW_MS: u64 = 12;
const DEFAULT_LOCAL_VACUUM_IDLE_AFTER_MS: u64 = 1_000;

fn local_vacuum_policy(args: &ServeArgs) -> std::io::Result<Option<LocalVacuumPolicy>> {
    let options = args.local_vacuum;
    let requested = options != LocalVacuumOptions::default();
    if args.substrate_bootstrap.is_some() {
        return if requested {
            invalid_input("local vacuum options are incompatible with --substrate-bootstrap")
        } else {
            Ok(None)
        };
    }

    let key_budget = options.key_budget.map_or(
        crabka_pgexec::VACUUM_STEP_KEY_BUDGET,
        PositiveUsize::into_value,
    );
    let max_key_budget = match options.max_key_budget {
        Some(value) => value.into_value(),
        None => key_budget.checked_mul(4)
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "local vacuum default maximum key budget overflows usize",
            ))?,
    };
    let hot_debt = match options.hot_debt {
        Some(value) => value.get(),
        None => u64::try_from(key_budget).map_err(|_| std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "local vacuum key budget does not fit u64 debt accounting",
        ))?,
    };
    let idle_interval = Duration::from_millis(options.idle_interval_ms.map_or(
        DEFAULT_LOCAL_VACUUM_IDLE_INTERVAL_MS,
        PositiveMillis::into_value,
    ));
    let backoff_floor = Duration::from_millis(options.backoff_floor_ms.map_or(
        DEFAULT_LOCAL_VACUUM_BACKOFF_FLOOR_MS,
        PositiveMillis::into_value,
    ));
    let step_fast = Duration::from_millis(options.step_fast_ms.map_or(
        DEFAULT_LOCAL_VACUUM_STEP_FAST_MS,
        PositiveMillis::into_value,
    ));
    let step_slow = Duration::from_millis(options.step_slow_ms.map_or(
        DEFAULT_LOCAL_VACUUM_STEP_SLOW_MS,
        PositiveMillis::into_value,
    ));
    let idle_after = Duration::from_millis(options.idle_after_ms.map_or(
        DEFAULT_LOCAL_VACUUM_IDLE_AFTER_MS,
        PositiveMillis::into_value,
    ));
    if backoff_floor > idle_interval {
        return invalid_input("local vacuum backoff floor exceeds idle interval");
    }
    if key_budget > max_key_budget {
        return invalid_input("local vacuum key budget exceeds maximum key budget");
    }
    if step_fast >= step_slow {
        return invalid_input("local vacuum fast threshold must be below slow threshold");
    }
    Ok(Some(LocalVacuumPolicy {
        idle_interval,
        backoff_floor,
        hot_debt,
        key_budget,
        max_key_budget,
        step_fast,
        step_slow,
        idle_after,
    }))
}
```

Keep these five named defaults next to the policy constructor as their single
numeric owner. The runtime-wiring steps below remove the seven old operational
constants before this task commits.

- [x] **Step 5: Add RED custom-policy behavior tests**

Replace constant-based vacuum-pacing assertions with default-policy assertions,
then add one custom policy covering every consumer:

```rust
#[test]
fn custom_policy_controls_every_local_vacuum_decision() {
    let policy = LocalVacuumPolicy {
        idle_interval: Duration::from_millis(90),
        backoff_floor: Duration::from_millis(7),
        hot_debt: 20,
        key_budget: 10,
        max_key_budget: 40,
        step_fast: Duration::from_millis(2),
        step_slow: Duration::from_millis(8),
        idle_after: Duration::from_millis(30),
    };
    let mut pacer = VacuumPacer::new(policy);
    assert_eq!(pacer.pace(), VacuumPace { interval: policy.idle_interval, key_budget: 10 });

    let hot_fast = VacuumStepObservation {
        writes_since_step: 21,
        step_elapsed: Duration::from_millis(2),
        ..quiet_busy_step()
    };
    assert_eq!(pacer.observe(&hot_fast).key_budget, 20);
    pacer.pace.key_budget = 40;
    assert_eq!(pacer.observe(&hot_fast).key_budget, 40);

    let hot_slow = VacuumStepObservation {
        step_elapsed: Duration::from_millis(8),
        ..hot_fast
    };
    assert_eq!(pacer.observe(&hot_slow).key_budget, 20);
}
```

Also test that the caught-up interval starts at `7ms`, doubles toward `90ms`,
and never exceeds it. Add a focused helper assertion proving the configured
`idle_after` changes foreground-idle classification.

- [x] **Step 6: Run the pacing RED test**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres vacuum_pacing_tests --lib
```

Expected: compilation fails because `VacuumPacer::new` does not receive the
policy and the old constants still drive decisions.

- [x] **Step 7: Store the policy in `VacuumPacer`**

Change the pacer minimally:

```rust
struct VacuumPacer {
    policy: LocalVacuumPolicy,
    debt: u64,
    cycle_dirty: bool,
    store_settled: bool,
    pace: VacuumPace,
}

impl VacuumPacer {
    const fn new(policy: LocalVacuumPolicy) -> Self {
        Self {
            policy,
            debt: 0,
            cycle_dirty: false,
            store_settled: false,
            pace: VacuumPace {
                interval: policy.idle_interval,
                key_budget: policy.key_budget,
            },
        }
    }
}
```

Replace every old constant read in `observe` with `self.policy`. Reset cold
steps to `policy.key_budget`; clamp backoff to
`policy.backoff_floor..=policy.idle_interval`; compare step latency with
`policy.step_fast` and `policy.step_slow`; cap with `policy.max_key_budget`.
Use `saturating_mul(2)` for the configurable key budget.

- [x] **Step 8: Validate once and pass the policy into the loop**

At the beginning of `serve_listener_with_tenant_config_loader`, before TLS and
tenant loading, construct:

```rust
let local_vacuum_policy = local_vacuum_policy(&args)?;
```

This makes substrate no-op and cross-field errors fail before registry, Kafka,
checkpoint-store, or engine work.

Change the loop signature and spawn:

```rust
async fn run_local_vacuum_loop(
    engine: SqlEngine,
    activity: Arc<crabka_pgwire::server::ActivityTracker>,
    shutdown: CancellationToken,
    policy: LocalVacuumPolicy,
) {
    let mut pacer = VacuumPacer::new(policy);
    let mut last_version_puts = engine.committed_version_puts();
    let mut last_maintain = std::time::Instant::now();
    // The existing select/step loop remains unchanged except for reading
    // idle-after and maintenance cadence from `policy`.
}
```

Use `policy.idle_after` in `idle_window_elapsed` and
`policy.idle_interval` for the existing maintenance-rotation ceiling. At the
local-engine spawn, pass the `Some(policy)` produced before runtime I/O; keep
the substrate path unspawned.

- [x] **Step 9: Remove old constants and stale documentation links**

Delete all seven `LOCAL_VACUUM_*` constants. Update the existing comments to
name `LocalVacuumPolicy` fields rather than deleted constants. Keep factors
two/four and zero-delay state semantics fixed.

- [x] **Step 10: Run focused behavior and help checks**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres vacuum_pacing_tests --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres local_vacuum --lib
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -q -p crabka-gres -- --help \
  | rg 'local-vacuum-(idle-interval|backoff-floor|hot-debt|key-budget|max-key-budget|step-fast|step-slow|idle-after)'
```

Expected: all focused tests pass and help lists all eight flags.

- [x] **Step 11: Run constructor and full Gres gates**

Run:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check -p crabka-gres --all-targets
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-gres --no-fail-fast
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-gres --all-targets --all-features -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo fmt --all -- --check
git diff --check
```

Expected: every command exits zero.

- [x] **Step 12: Commit Task 1**

Stage only the two scoped Gres files and commit:

```bash
git add crates/gres/src/lib.rs crates/gres/tests/runtime.rs
git commit -m "feat(gres): configure local vacuum pacing"
```

### Task 2: Independent review and audit closure

**Files:**

- Modify after verified audit: `docs/configuration-audit.md`
- Modify tracking only: `docs/superpowers/plans/2026-07-24-gres-local-vacuum-policy.md`

**Interfaces:**

- Consumes: Task 1 commit and the repository runtime-value scanner
- Produces: reviewed classification and the next remaining runtime owner

- [x] **Step 1: Assign independent read-only reviews**

Have a fresh reviewer trace:

- all eight CLI/environment inputs through effective policy and live consumer;
- local-only spawn and substrate rejection;
- scalar and relational validation;
- absence of operator/CRD rendering;
- fixed algorithm values versus remaining operational candidates;
- test quality and unrelated diff scope.

Expected: findings include severity and `file:line`, or an explicit PASS.

- [x] **Step 2: Remediate every actionable finding**

Use a fresh implementer for each remediation wave. Require a regression test,
full affected gates, a task-only commit, and independent re-review before
continuing.

- [x] **Step 3: Run and classify the focused scanner**

Run:

```bash
tools/audit-runtime-values.sh > /tmp/crabka-gres-local-vacuum-values.txt
wc -l /tmp/crabka-gres-local-vacuum-values.txt
rg -n 'LOCAL_VACUUM|local.vacuum|vacuum.*(Duration|budget|debt|interval)' \
  crates/gres/src/lib.rs crates/pgexec/src/lib.rs
```

Classify every focused result as parser/effective default, test/harness,
fixed state-machine/derived arithmetic, or the next owner. No production hit
may remain unclassified.

- [x] **Step 4: Re-run final verification**

Run the full Gres suite, strict Clippy, formatting, help check, and
`git diff --check` again. Confirm `git diff -- deploy/crds` is empty because
the design intentionally has no CRD surface.

- [x] **Step 5: Record closure**

Append exact scanner counts, classifications, live-consumer traces, gate
results, and the next owner to `docs/configuration-audit.md`. Check every plan
item only after evidence exists, then commit only the audit document:

```bash
git add docs/configuration-audit.md
git commit -m "docs(gres): record local vacuum audit"
```
