# Metrics Service Cold-Store Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the metrics service cold-cache TTL and unbounded compatibility-query lookback through positive UOM CLI options backed by environment variables without changing defaults.

**Architecture:** `RefreshingMetricBlockStore` keeps its constructor and owns two `Time` fields initialized from named defaults. Direct builder setters inject values parsed by the standalone binary, and all three service roles apply them before constructing their Prometheus API state.

**Tech Stack:** Rust, Clap, `crabka-units`, Tokio, object_store, Cargo.

## Global Constraints

- Preserve defaults: cold-cache TTL `30s`, unbounded compatibility lookback `1h`.
- Keep both values as UOM `Time` through parsing and runtime use.
- Reject zero, negative, and non-finite values for both CLI/environment settings.
- Keep `RefreshingMetricBlockStore::new` source-compatible for library callers.
- Apply both settings to querier, query-frontend, and ruler startup.
- Change only the exact `i64::MIN..i64::MAX` compatibility sentinel; preserve explicit query ranges.
- Add no disable sentinel, policy aggregate, dependency, runtime file format, or CRD field.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not stage or edit the four protected untracked plans dated 2026-07-28.
- Do not run `cargo clean`; it remains the final repository-goal cleanup.

---

### Task 1: Make cold-store policies injectable

**Files:**
- Modify: `crates/metrics-service/src/lib.rs`

**Interfaces:**
- Produces: `DEFAULT_COLD_CACHE_TTL: Time = 30s` and `DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK: Time = 1h`.
- Produces: `RefreshingMetricBlockStore::with_cold_cache_ttl(self, Time) -> Self`.
- Produces: `RefreshingMetricBlockStore::with_unbounded_compatibility_lookback(self, Time) -> Self`.
- Changes private helper to `normalize_refresh_range(start_ms: i64, end_ms: i64, lookback: Time, now_ms: i64) -> (i64, i64)`.
- Preserves: `RefreshingMetricBlockStore::new` and public router-helper signatures.

- [x] **Step 1: Write failing policy tests**

Add these tests to the existing `lib.rs` test module:

```rust
#[test]
fn refreshing_blockstore_policy_defaults_and_overrides() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let defaults = super::RefreshingMetricBlockStore::new(
        Arc::clone(&object_store),
        url::Url::parse("memory:///").unwrap(),
        "metrics",
        crabka_promql::WalHead::new(),
    );
    check!(defaults.cold_cache_ttl == super::DEFAULT_COLD_CACHE_TTL);
    check!(
        defaults.unbounded_compatibility_lookback
            == super::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK
    );

    let configured = super::RefreshingMetricBlockStore::new(
        object_store,
        url::Url::parse("memory:///").unwrap(),
        "metrics",
        crabka_promql::WalHead::new(),
    )
    .with_cold_cache_ttl(secs(5))
    .with_unbounded_compatibility_lookback(minutes(10));
    check!(configured.cold_cache_ttl == secs(5));
    check!(configured.unbounded_compatibility_lookback == minutes(10));
}

#[test]
fn configured_lookback_normalizes_only_unbounded_range() {
    check!(
        super::normalize_refresh_range(i64::MIN, i64::MAX, minutes(10), 1_000_000)
            == (400_000, i64::MAX)
    );
    check!(super::normalize_refresh_range(100, 200, minutes(10), 1_000_000) == (100, 200));
}

#[test]
fn configured_cold_cache_ttl_controls_freshness() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let cold = crabka_promql::MetricBlockStore::new(crabka_blockstore::BlockStore::new(
        object_store,
        url::Url::parse("memory:///").unwrap(),
    ));
    let cached = super::CachedMetricBlockStore {
        cached_at: std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(2))
            .expect("two seconds before now is representable"),
        start_ms: 0,
        end_ms: 100,
        cold,
    };
    check!(cached.covers(0, 100, secs(3)));
    check!(!cached.covers(0, 100, secs(1)));
}
```

- [x] **Step 2: Run focused tests and verify red**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service refreshing_blockstore_policy_defaults_and_overrides --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service configured_lookback_normalizes_only_unbounded_range --locked
```

Expected: compilation fails because the policy fields, setters, and explicit normalization inputs do not exist.

- [x] **Step 3: Implement minimal library injection**

Make the existing constants public, add the two fields, and initialize them:

```rust
pub const DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK: Time = hours(1);
pub const DEFAULT_COLD_CACHE_TTL: Time = secs(30);

// RefreshingMetricBlockStore fields
cold_cache_ttl: Time,
unbounded_compatibility_lookback: Time,

// RefreshingMetricBlockStore::new
cold_cache_ttl: DEFAULT_COLD_CACHE_TTL,
unbounded_compatibility_lookback: DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK,
```

Add the builders:

```rust
#[must_use]
pub fn with_cold_cache_ttl(mut self, ttl: Time) -> Self {
    self.cold_cache_ttl = ttl;
    self
}

#[must_use]
pub fn with_unbounded_compatibility_lookback(mut self, lookback: Time) -> Self {
    self.unbounded_compatibility_lookback = lookback;
    self
}
```

Pass `self.cold_cache_ttl` to both `covers` calls. Replace range normalization with:

```rust
fn normalize_refresh_range(
    start_ms: i64,
    end_ms: i64,
    lookback: Time,
    now_ms: i64,
) -> (i64, i64) {
    if start_ms == i64::MIN && end_ms == i64::MAX {
        return (now_ms.saturating_sub(lookback.millis_i64()), i64::MAX);
    }
    (start_ms, end_ms)
}
```

Call it from `current_store` with `self.unbounded_compatibility_lookback` and `unix_time_ms()`.

- [x] **Step 4: Run focused library tests and verify green**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service refreshing_blockstore_policy --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service configured_lookback_normalizes_only_unbounded_range --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service configured_cold_cache_ttl_controls_freshness --locked
```

Expected: all selected tests pass.

- [x] **Step 5: Commit library injection**

```bash
git add -- crates/metrics-service/src/lib.rs
git commit -m "feat(metrics): inject cold-store policy"
```

---

### Task 2: Add CLI and environment wiring for every role

**Files:**
- Modify: `crates/metrics-service/src/main.rs`

**Interfaces:**
- Consumes: both named defaults and both `RefreshingMetricBlockStore` builders.
- Produces: `--cold-cache-ttl` / `CRABKA_METRICS_COLD_CACHE_TTL`.
- Produces: `--unbounded-compatibility-lookback` / `CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK`.
- Preserves: the flat CLI and all target startup signatures.

- [x] **Step 1: Write failing CLI and environment tests**

Add default, override, and invalid-value coverage:

```rust
#[test]
fn cold_store_policy_parses_defaults_overrides_and_boundaries() {
    let defaults =
        Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
    check!(defaults.cold_cache_ttl == crabka_metrics_service::DEFAULT_COLD_CACHE_TTL);
    check!(
        defaults.unbounded_compatibility_lookback
            == crabka_metrics_service::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK
    );

    let configured = Cli::try_parse_from([
        "crabka-metrics-service",
        "--target",
        "querier",
        "--cold-cache-ttl",
        "5s",
        "--unbounded-compatibility-lookback",
        "10m",
    ])
    .unwrap();
    check!(configured.cold_cache_ttl == secs(5));
    check!(configured.unbounded_compatibility_lookback == minutes(10));

    for args in [
        ["--cold-cache-ttl", "0s"],
        ["--cold-cache-ttl", "-1s"],
        ["--unbounded-compatibility-lookback", "0s"],
        ["--unbounded-compatibility-lookback", "-1s"],
    ] {
        assert2::assert!(
            Cli::try_parse_from([
                "crabka-metrics-service",
                "--target",
                "querier",
                args[0],
                args[1],
            ])
            .is_err()
        );
    }
}
```

Add the existing child-process pattern for environment precedence:

```rust
#[test]
fn cold_store_policy_reads_environment_and_prefers_cli() {
    const CHILD: &str = "CRABKA_METRICS_SERVICE_COLD_STORE_POLICY_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status =
            std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "tests::cold_store_policy_reads_environment_and_prefers_cli",
                ])
                .env(CHILD, "1")
                .env("CRABKA_METRICS_COLD_CACHE_TTL", "5s")
                .env("CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK", "10m")
                .status()
                .expect("child test");
        assert2::assert!(status.success());
        return;
    }

    let from_env =
        Cli::try_parse_from(["crabka-metrics-service", "--target", "querier"]).unwrap();
    check!(from_env.cold_cache_ttl == secs(5));
    check!(from_env.unbounded_compatibility_lookback == minutes(10));

    let from_cli = Cli::try_parse_from([
        "crabka-metrics-service",
        "--target",
        "querier",
        "--cold-cache-ttl",
        "7s",
        "--unbounded-compatibility-lookback",
        "20m",
    ])
    .unwrap();
    check!(from_cli.cold_cache_ttl == secs(7));
    check!(from_cli.unbounded_compatibility_lookback == minutes(20));
}
```

- [x] **Step 2: Run the focused binary test and verify red**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service --bin crabka-metrics-service \
  cold_store_policy_parses_defaults_overrides_and_boundaries --locked
```

Expected: compilation fails because both `Cli` fields do not exist.

- [x] **Step 3: Implement positive UOM fields and role wiring**

Add both `Cli` fields:

```rust
#[arg(
    long,
    env = "CRABKA_METRICS_COLD_CACHE_TTL",
    default_value = "30s",
    value_parser = parse::positive_time
)]
cold_cache_ttl: Time,
#[arg(
    long,
    env = "CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK",
    default_value = "1h",
    value_parser = parse::positive_time
)]
unbounded_compatibility_lookback: Time,
```

In `run_query_frontend`, `run_ruler`, and `run_querier`, extend the existing construction:

```rust
let metric_store = RefreshingMetricBlockStore::new(
    store,
    object_store_url.clone(),
    &cli.manifest_prefix,
    head,
)
.with_cold_cache_ttl(cli.cold_cache_ttl)
.with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
```

Retain each role's existing `Arc::clone`, `WalHead::new`, or `head` argument.

- [x] **Step 4: Run focused binary and library tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service --bin crabka-metrics-service cold_store_policy --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service --lib --locked
```

Expected: both binary policy tests and all library tests pass.

- [x] **Step 5: Commit CLI and environment wiring**

```bash
git add -- crates/metrics-service/src/main.rs
git commit -m "feat(metrics): configure cold-store policy"
```

---

### Task 3: Close the audit slice and verify

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-31-metrics-service-cold-store-policy.md`

**Interfaces:**
- Consumes: the completed library and binary configuration surface.
- Produces: audit evidence that both cold-store policies are no longer pending.

- [x] **Step 1: Run the complete focused suite**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics-service --all-targets --locked
```

Expected: every non-ignored target passes; Docker-only tests remain explicitly ignored.

- [x] **Step 2: Run repository verification gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

Expected: all commands exit successfully and Clippy emits no warnings.

- [x] **Step 3: Update the configuration audit**

Replace the pending metrics-service paragraph with a completed statement naming:

```text
CRABKA_METRICS_COLD_CACHE_TTL
CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK
```

State that defaults remain `30s` and `1h`, both remain positive UOM `Time`
values, zero and negative values are rejected, all three roles receive the
settings, and no CRD owns the standalone service. Record actual focused test
counts and repository verification gates.

- [x] **Step 4: Mark the plan complete and inspect the final diff**

Change every task checkbox to `[x]`, then run:

```bash
git diff --check
git status --short
git diff --stat HEAD~2
```

Expected: only the audit and this plan remain uncommitted; the four protected
2026-07-28 plans remain untracked and unchanged.

- [x] **Step 5: Commit audit closure**

```bash
git add -- docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-metrics-service-cold-store-policy.md
git commit -m "docs(config): close metrics cold-store policy"
```
