# Metrics Distributor Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the metrics distributor's HA failover timeout, ingestion-rate tenant bucket cap, and decompressed request cap through CLI options backed by environment variables without changing defaults.

**Architecture:** Keep policy ownership in the existing `HaTracker`, `IngestEnforcer`, and `DistributorState` paths. Add only the state setters needed to inject configured values, then parse the three values in the standalone `crabka-metrics` binary and apply them during distributor construction.

**Tech Stack:** Rust, Clap, `refined_type`, `crabka-units`, Tokio, Cargo.

## Global Constraints

- Preserve defaults: HA failover timeout `30s`, ingestion-rate bucket cap `100000`, decompressed request cap `32MiB`.
- Preserve HA semantics: negative disables takeover, zero permits immediate takeover, and positive values set the lease.
- Use a `refined_type` newtype to validate the positive dimensionless bucket cap.
- Keep the timeout and decompression cap as UOM `Time` and `ByteSize`.
- Require the decompression cap to be positive, whole-byte, and exactly representable at the `usize` request boundary.
- Keep library defaults and `IngestEnforcer::with_max_rate_buckets(0)` compatibility behavior unchanged.
- Add no CRD fields because no CRD owns the standalone metrics service.
- Add no policy aggregate, disable flag, runtime file format, or new dependency other than the already-workspace-managed `refined_type`.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not stage or edit the four protected untracked plans dated 2026-07-28.
- Do not run `cargo clean`; it remains the final repository-goal cleanup.

---

### Task 1: Inject distributor policies through existing library paths

**Files:**
- Modify: `crates/metrics/src/distributor/ha.rs`
- Modify: `crates/metrics/src/distributor/mod.rs`
- Modify: `crates/metrics/src/limits/enforce.rs`
- Modify: `crates/metrics/src/limits/mod.rs`
- Modify: `crates/metrics/src/lib.rs`

**Interfaces:**
- Produces: public `DEFAULT_MAX_RATE_BUCKETS: usize`.
- Produces: public `DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED: ByteSize`.
- Produces: `DistributorState::with_ha_failover_timeout(self, Time) -> Self`.
- Produces: `DistributorState::with_max_rate_buckets(self, usize) -> Self`.
- Produces: `HaTracker::elect_now_with_timeout(&self, tenant: &str, series: &[DecodedSeries], failover_timeout: Time) -> HaElection`.
- Preserves: `HaTracker::elect_now`, `HaTracker::elect`, `IngestEnforcer::with_max_rate_buckets`, and all default constructors.

- [ ] **Step 1: Write failing library tests**

In `distributor/ha.rs`, replace default-only `elect_now` test calls with an
explicit timeout and add a current-clock-independent test around `elect`:

```rust
#[test]
fn configured_failover_timeout_controls_takeover() {
    let tracker = || {
        let tracker = HaTracker::default();
        tracker.persist_elected(&HaElectionRecord {
            tenant: "tenant".to_owned(),
            cluster: "c1".to_owned(),
            replica: "r1".to_owned(),
            lease_timestamp_ms: 1_000,
        });
        tracker
    };
    let replacement = [series_with("c1", "r2")];

    check!(
        tracker().elect("tenant", &replacement, i64::MAX, secs(-1))
            == HaElection::Drop
    );
    check!(matches!(
        tracker().elect("tenant", &replacement, 1_001, Time::ZERO),
        HaElection::Elect(_)
    ));
    check!(
        tracker().elect("tenant", &replacement, 2_000, millis(999))
            == HaElection::Elect(HaElectionRecord {
                tenant: "tenant".to_owned(),
                cluster: "c1".to_owned(),
                replica: "r2".to_owned(),
                lease_timestamp_ms: 2_000,
            })
    );
}
```

In `distributor/mod.rs`, add a state-construction test:

```rust
#[test]
fn distributor_state_stores_configured_runtime_policy() {
    let sink = Arc::new(RecordingSink::default());
    let state = DistributorState::new(sink)
        .with_ha_failover_timeout(secs(-1))
        .with_max_rate_buckets(7)
        .with_max_decompressed(kibibytes(64));

    check!(state.ha_failover_timeout == secs(-1));
    check!(state.ingest_enforcer.max_rate_buckets() == 7);
    check!(state.max_decompressed == kibibytes(64));
}
```

Keep the existing `rate_bucket_map_stays_bounded` test in
`limits/enforce.rs`; change it to construct its cap from the newly public
default only where that helps verify the exported constant:

```rust
#[test]
fn default_rate_bucket_cap_is_preserved() {
    check!(DEFAULT_MAX_RATE_BUCKETS == 100_000);
}
```

- [ ] **Step 2: Run focused tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics configured_failover_timeout_controls_takeover \
  --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics distributor_state_stores_configured_runtime_policy \
  --locked
```

Expected: compilation fails because the state setters and stored timeout do
not exist.

- [ ] **Step 3: Implement minimal library injection**

In `limits/enforce.rs`, make the existing default constant public:

```rust
pub const DEFAULT_MAX_RATE_BUCKETS: usize = 100_000;
```

Re-export it from `limits/mod.rs` and `lib.rs` beside `IngestEnforcer`.

In `distributor/mod.rs`, name the existing byte default and add the timeout
field:

```rust
pub const DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED: ByteSize = mebibytes(32);

pub struct DistributorState {
    // existing fields
    ha_failover_timeout: Time,
    max_decompressed: ByteSize,
    // existing fields
}
```

Initialize both defaults in `DistributorState::new`:

```rust
ha_failover_timeout: DEFAULT_HA_FAILOVER_TIMEOUT,
max_decompressed: DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED,
```

Add direct setters:

```rust
#[must_use]
pub fn with_ha_failover_timeout(mut self, timeout: Time) -> Self {
    self.ha_failover_timeout = timeout;
    self
}

#[must_use]
pub fn with_max_rate_buckets(mut self, cap: usize) -> Self {
    self.ingest_enforcer = IngestEnforcer::with_max_rate_buckets(cap);
    self
}
```

Add a test-only observation method to `IngestEnforcer` so the state wiring is
proved without widening its production API:

```rust
#[cfg(test)]
pub(crate) const fn max_rate_buckets(&self) -> usize {
    self.max_rate_buckets
}
```

Keep `HaTracker::elect_now` unchanged and add an explicit-timeout sibling:

```rust
pub fn elect_now_with_timeout(
    &self,
    tenant: &str,
    series: &[DecodedSeries],
    failover_timeout: Time,
) -> HaElection {
    self.elect(tenant, series, now_ms(), failover_timeout)
}
```

At the sole production call in `apply_ha_election`, pass
`state.ha_failover_timeout` through `elect_now_with_timeout`. Existing library
callers of `elect_now` continue using `DEFAULT_HA_FAILOVER_TIMEOUT`.

- [ ] **Step 4: Run focused library tests and verify green**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics distributor::ha --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics rate_bucket_map_stays_bounded --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics distributor_state_stores_configured_runtime_policy \
  --locked
```

Expected: all selected tests pass, including negative timeout and bounded-map
behavior.

- [ ] **Step 5: Commit the library policy injection**

```bash
git add -- crates/metrics/src/distributor/ha.rs \
  crates/metrics/src/distributor/mod.rs crates/metrics/src/limits/enforce.rs \
  crates/metrics/src/limits/mod.rs crates/metrics/src/lib.rs
git commit -m "feat(metrics): inject distributor policy"
```

---

### Task 2: Add CLI and environment configuration

**Files:**
- Modify: `crates/metrics/Cargo.toml`
- Modify: `crates/metrics/src/bin/crabka-metrics.rs`

**Interfaces:**
- Consumes: `DEFAULT_HA_FAILOVER_TIMEOUT`, `DEFAULT_MAX_RATE_BUCKETS`, and `DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED`.
- Consumes: `DistributorState::with_ha_failover_timeout`, `DistributorState::with_max_rate_buckets`, and `DistributorState::with_max_decompressed`.
- Produces: CLI/environment options named in the approved design.
- Produces: binary-local `IngestRateBucketCap` validated by `GreaterUsize<0>`.

- [ ] **Step 1: Write failing CLI parsing tests**

Add one default/override/validation test:

```rust
#[test]
fn distributor_policy_parses_defaults_overrides_and_boundaries() {
    let defaults =
        Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
    check!(defaults.ha_failover_timeout == secs(30));
    check!(defaults.ingest_rate_bucket_cap == 100_000);
    check!(defaults.distributor_max_decompressed == mebibytes(32));

    let configured = Cli::try_parse_from([
        "crabka-metrics",
        "--target",
        "distributor",
        "--ha-failover-timeout",
        "-1s",
        "--ingest-rate-bucket-cap",
        "7",
        "--distributor-max-decompressed",
        "64KiB",
    ])
    .unwrap();
    check!(configured.ha_failover_timeout == secs(-1));
    check!(configured.ingest_rate_bucket_cap == 7);
    check!(configured.distributor_max_decompressed == kibibytes(64));

    for args in [
        ["--ingest-rate-bucket-cap", "0"],
        ["--distributor-max-decompressed", "0B"],
        ["--distributor-max-decompressed", "1.5B"],
    ] {
        let input = [
            "crabka-metrics",
            "--target",
            "distributor",
            args[0],
            args[1],
        ];
        assert!(Cli::try_parse_from(input).is_err());
    }
}
```

Add an environment precedence test using the existing child-process pattern,
because Clap environment reads can race other tests:

```rust
#[test]
fn distributor_policy_reads_environment_and_prefers_cli() {
    const CHILD: &str = "CRABKA_METRICS_DISTRIBUTOR_POLICY_CHILD";

    if std::env::var_os(CHILD).is_none() {
        let status =
            std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "tests::distributor_policy_reads_environment_and_prefers_cli",
                ])
                .env(CHILD, "1")
                .env("CRABKA_METRICS_HA_FAILOVER_TIMEOUT", "-1s")
                .env("CRABKA_METRICS_INGEST_RATE_BUCKET_CAP", "7")
                .env("CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED", "64KiB")
                .status()
                .expect("child test");
        assert!(status.success());
        return;
    }

    let from_env =
        Cli::try_parse_from(["crabka-metrics", "--target", "distributor"]).unwrap();
    check!(from_env.ha_failover_timeout == secs(-1));
    check!(from_env.ingest_rate_bucket_cap == 7);
    check!(from_env.distributor_max_decompressed == kibibytes(64));

    let from_cli = Cli::try_parse_from([
        "crabka-metrics",
        "--target",
        "distributor",
        "--ha-failover-timeout",
        "5s",
        "--ingest-rate-bucket-cap",
        "9",
        "--distributor-max-decompressed",
        "128KiB",
    ])
    .unwrap();
    check!(from_cli.ha_failover_timeout == secs(5));
    check!(from_cli.ingest_rate_bucket_cap == 9);
    check!(from_cli.distributor_max_decompressed == kibibytes(128));
}
```

- [ ] **Step 2: Run binary tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics --bin crabka-metrics \
  distributor_policy_parses_defaults_overrides_and_boundaries --locked
```

Expected: compilation fails because the three `Cli` fields do not exist.

- [ ] **Step 3: Implement validated CLI fields**

Add `refined_type = { workspace = true }` to `crates/metrics/Cargo.toml`.

Define the binary-local validated count:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IngestRateBucketCap(usize);

impl IngestRateBucketCap {
    fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("ingest rate bucket cap: {error}"))
    }

    #[must_use]
    const fn get(self) -> usize {
        self.0
    }
}

fn parse_ingest_rate_bucket_cap(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(IngestRateBucketCap::new)
        .map(IngestRateBucketCap::get)
}
```

Reuse the existing whole-byte parser pattern:

```rust
fn parse_distributor_max_decompressed(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    usize::try_from(size.bytes_u64())
        .map_err(|_| "size must fit the platform request boundary".to_owned())?;
    Ok(size)
}
```

Add the three fields to `Cli`:

```rust
#[arg(
    long,
    env = "CRABKA_METRICS_HA_FAILOVER_TIMEOUT",
    default_value = "30s",
    value_parser = parse::time
)]
ha_failover_timeout: Time,
#[arg(
    long,
    env = "CRABKA_METRICS_INGEST_RATE_BUCKET_CAP",
    default_value_t = DEFAULT_MAX_RATE_BUCKETS,
    value_parser = parse_ingest_rate_bucket_cap
)]
ingest_rate_bucket_cap: usize,
#[arg(
    long,
    env = "CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED",
    default_value = "32MiB",
    value_parser = parse_distributor_max_decompressed
)]
distributor_max_decompressed: ByteSize,
```

Import the three library defaults so default tests anchor the CLI values to
their owners. During `run_distributor`, extend the existing state builder:

```rust
.with_ha_failover_timeout(cli.ha_failover_timeout)
.with_max_rate_buckets(cli.ingest_rate_bucket_cap)
.with_max_decompressed(cli.distributor_max_decompressed)
```

- [ ] **Step 4: Run focused binary and library tests**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics --bin crabka-metrics distributor_policy --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics --lib --locked
```

Expected: all metrics binary policy tests and all metrics library tests pass.

- [ ] **Step 5: Commit CLI and environment wiring**

```bash
git add -- crates/metrics/Cargo.toml crates/metrics/src/bin/crabka-metrics.rs
git commit -m "feat(metrics): configure distributor policy"
```

---

### Task 3: Close the audit slice and verify

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-31-metrics-distributor-policy.md`

**Interfaces:**
- Consumes: the completed library and binary configuration surface.
- Produces: audit evidence that the three distributor policies are no longer pending.

- [ ] **Step 1: Run the complete focused suite**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-metrics --all-targets --locked
```

Expected: every non-ignored `crabka-metrics` target passes.

- [ ] **Step 2: Run repository verification gates**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

Expected: all commands exit successfully and Clippy emits no warnings.

- [ ] **Step 3: Update the configuration audit**

In the metrics distributor section of `docs/configuration-audit.md`, replace
the pending-design paragraph with a completed statement that names:

```text
CRABKA_METRICS_HA_FAILOVER_TIMEOUT
CRABKA_METRICS_INGEST_RATE_BUCKET_CAP
CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED
```

State that defaults remain `30s`, `100000`, and `32MiB`; timeout and byte
values remain UOM quantities; the count uses `refined_type`; negative timeout
still disables takeover; and no CRD owns the standalone service. Record that
the focused metrics suite, workspace check, strict Clippy, nightly formatting,
and diff hygiene passed.

- [ ] **Step 4: Mark this plan complete and inspect the final diff**

Change every checkbox in this plan to `[x]`, then run:

```bash
git diff --check
git status --short
git diff --stat HEAD~2
```

Expected: only the audit and this plan remain uncommitted; the four protected
2026-07-28 plan files remain untracked and unchanged.

- [ ] **Step 5: Commit audit closure**

```bash
git add -- docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-metrics-distributor-policy.md
git commit -m "docs(config): close metrics distributor policy"
```
