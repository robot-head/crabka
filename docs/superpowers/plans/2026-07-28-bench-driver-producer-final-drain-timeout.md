# Bench Driver Producer Final-Drain Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the fixed 10-second producer final-drain timeout with one
validated CLI/environment setting while preserving drain behavior.

**Architecture:** Parse a positive whole-second refined newtype at the existing
Clap boundary, copy it through `DriverConfig` and `ProducerTask`, and consume it
at the sole final-drain deadline. Reuse the benchmark launcher's existing
`envsubst` path.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Bash, Kubernetes YAML.

## Global Constraints

- Preserve the 10-second default.
- CLI overrides environment.
- Reject zero, malformed, negative, and primitive-overflow values.
- Preserve drain-loop checks, drop accounting, error text, flush, and close.
- Do not reuse the Kafka-protocol-bounded client request-timeout type.
- Do not expose sampling cadence in this slice.
- Add no CRD; the benchmark launcher and Job template own this binary.
- Add no dependency and do not change `Cargo.lock`.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `crates/bench-driver/src/workload.rs`
- `crates/bench-driver/src/main.rs`
- `bench/scripts/run-scenario.sh`
- `bench/manifests/driver/job-template.yaml`
- `docs/configuration-audit.md`

### Task 1: Expose and propagate the timeout

- [ ] Add failing workload tests:

```rust
#[test]
fn producer_final_drain_timeout_default_preserves_behavior() {
    assert_eq!(
        ProducerFinalDrainTimeoutSeconds::default().duration(),
        Duration::from_secs(10)
    );
}

#[test]
fn producer_final_drain_timeout_accepts_positive_minimum() {
    assert_eq!(
        ProducerFinalDrainTimeoutSeconds::new(1)
            .expect("one second is valid")
            .duration(),
        Duration::from_secs(1)
    );
}

#[test]
fn producer_final_drain_timeout_rejects_invalid_values() {
    for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
        assert!(
            invalid
                .parse::<ProducerFinalDrainTimeoutSeconds>()
                .is_err(),
            "{invalid:?} must be rejected"
        );
    }
}
```

- [ ] Add failing CLI tests for the 10-second default, invalid values, and a
  child-process environment/CLI precedence check using:

```text
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS=11
--producer-final-drain-timeout-seconds 21
```

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver producer_final_drain --locked
```

- [ ] In `workload.rs`, replace the fixed constant with:

```rust
pub const DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerFinalDrainTimeoutSeconds(u64);
```

Implement `new` with `GreaterU64<0>`, `duration`, `Default`, `Display`, and
`FromStr`. Add accurate `# Errors` documentation.

- [ ] Add the typed Clap field after the producer request timeout:

```rust
#[arg(
    long,
    env = "BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS",
    default_value_t = ProducerFinalDrainTimeoutSeconds::default()
)]
producer_final_drain_timeout_seconds: ProducerFinalDrainTimeoutSeconds,
```

- [ ] Add `producer_final_drain_timeout` to `DriverConfig` and `ProducerTask`.
  Copy it at task spawn and destructure it in `run_producer`. Remove the
  producer loop's redundant `sid` local and use `cfg.scenario_id` directly so
  the existing `run` function does not cross its strict line-count limit.

- [ ] Replace only:

```rust
Instant::now() + PRODUCER_FINAL_DRAIN_TIMEOUT
```

with:

```rust
Instant::now() + final_drain_timeout.duration()
```

- [ ] Initialize the workload test config with the typed default.

- [ ] Verify GREEN and the sole runtime flow:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver producer_final_drain --locked
test "$(rg -o 'Instant::now\\(\\) \\+ final_drain_timeout\\.duration\\(\\)' \
  crates/bench-driver/src/workload.rs | wc -l)" -eq 1
if rg -n '^const PRODUCER_FINAL_DRAIN_TIMEOUT' crates/bench-driver/src/workload.rs; then
  exit 1
fi
```

- [ ] Run package gates:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver --all-targets --locked
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-bench-driver --all-targets --locked -- -D warnings
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-bench-driver --bin crabka-bench-driver --locked -- --help
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--producer-final-drain-timeout-seconds')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

- [ ] Review and commit only the Rust files:

```bash
git add crates/bench-driver/src/main.rs crates/bench-driver/src/workload.rs
git commit -m "feat(bench): expose producer drain timeout"
```

### Task 2: Wire the timeout through benchmark Jobs

- [ ] Document, default, and export in `bench/scripts/run-scenario.sh`:

```bash
: "${BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS:=10}"
export BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS
```

- [ ] Document and add the Job environment entry:

```yaml
- name: BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS
  value: "${BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS}"
```

- [ ] Validate syntax and renders:

```bash
bash -n bench/scripts/run-scenario.sh
rg -n 'BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS:=10' \
  bench/scripts/run-scenario.sh
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS=10 \
  envsubst '$BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS'
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS=21 \
  envsubst '$BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS'
git diff --check
```

- [ ] Commit only deployment files:

```bash
git add bench/scripts/run-scenario.sh bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire producer drain timeout"
```

### Task 3: Close the audit slice

- [ ] Capture exact evidence:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/'
rg -n 'producer_final_drain_timeout|ProducerFinalDrainTimeoutSeconds|DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS|producer-final-drain-timeout-seconds|BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS' \
  crates/bench-driver bench docs/configuration-audit.md
```

Classify every bench-driver scanner and focused-search line into mutually
exclusive categories and identify the next real unresolved owner.

- [ ] Append `## Bench Driver Producer Final-Drain Timeout` to
  `docs/configuration-audit.md`, recording default, validation, precedence,
  value/deployment flows, preserved behavior, exact counts, gates, and the
  next unresolved owner.

- [ ] Re-run the package, Clippy, nightly format, help-entry, shell/render,
  diff, lockfile, and scanner gates.

- [ ] Commit only the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record producer drain timeout"
```

Confirm the worktree is clean after the commit.
