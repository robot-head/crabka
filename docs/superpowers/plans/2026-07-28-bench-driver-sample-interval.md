# Bench Driver Sample Interval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the fixed 2,000-millisecond sample interval with one
validated CLI/environment setting while preserving grid behavior.

**Architecture:** Parse a positive-millisecond refined newtype at the existing
Clap boundary, store it in `DriverConfig`, and consume it when constructing the
existing shared `Grid`. Reuse the benchmark launcher's `envsubst` path.

**Tech Stack:** Rust 2024, Clap, `refined_type`, Bash, Kubernetes YAML.

## Constraints

- Preserve the 2,000-millisecond default.
- CLI overrides environment.
- Reject zero, malformed, negative, and primitive-overflow values.
- Preserve interval-count ceiling, minimum-one bucket, clamping, task-local
  histograms, and merged output.
- Add no task fields; tasks already receive the complete `Grid`.
- Add no CRD, dependency, or `Cargo.lock` change.
- Run Cargo with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`; use
  `--locked` for lock-aware commands.
- Preserve unrelated work and stage only task paths.

## Files

- `crates/bench-driver/src/workload.rs`
- `crates/bench-driver/src/main.rs`
- `bench/scripts/run-scenario.sh`
- `bench/manifests/driver/job-template.yaml`
- `docs/configuration-audit.md`

### Task 1: Expose and consume the sample interval

- [ ] Add failing workload tests for:

```rust
SampleIntervalMs::default().milliseconds() == 2_000
SampleIntervalMs::new(1).expect("one millisecond").milliseconds() == 1
```

and rejection of `"0"`, `"not-a-number"`, `"-1"`, and
`"18446744073709551616"`.

- [ ] Add failing CLI tests for the default, invalid inputs, and a hermetic
  child-process precedence check:

```text
BENCH_SAMPLE_INTERVAL_MS=11
--sample-interval-ms 21
```

- [ ] Verify RED:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver sample_interval --locked
```

- [ ] Replace `SAMPLE_INTERVAL_MS` with:

```rust
pub const DEFAULT_SAMPLE_INTERVAL_MS: u64 = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleIntervalMs(u64);
```

Implement `new` with `GreaterU64<0>`, `milliseconds`, `Default`, `Display`, and
`FromStr`, with accurate `# Errors` documentation.

- [ ] Add the typed Clap field:

```rust
#[arg(
    long,
    env = "BENCH_SAMPLE_INTERVAL_MS",
    default_value_t = SampleIntervalMs::default()
)]
sample_interval_ms: SampleIntervalMs,
```

- [ ] Add `sample_interval: SampleIntervalMs` to `DriverConfig`, initialize it
  from the CLI and workload test config, and replace:

```rust
let interval_ms = SAMPLE_INTERVAL_MS;
```

with:

```rust
let interval_ms = cfg.sample_interval.milliseconds();
```

- [ ] Verify GREEN and the sole runtime flow:

```bash
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-bench-driver sample_interval --locked
test "$(rg -o 'cfg\\.sample_interval\\.milliseconds\\(\\)' \
  crates/bench-driver/src/workload.rs | wc -l)" -eq 1
if rg -n '^const SAMPLE_INTERVAL_MS' crates/bench-driver/src/workload.rs; then
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
test "$(target/debug/crabka-bench-driver --help | rg -c -- '--sample-interval-ms')" -eq 1
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo +nightly fmt --all
git diff --check
git diff -- Cargo.lock
```

- [ ] Commit only Rust files:

```bash
git add crates/bench-driver/src/main.rs crates/bench-driver/src/workload.rs
git commit -m "feat(bench): expose sample interval"
```

### Task 2: Wire the interval through benchmark Jobs

- [ ] Document, default, and export:

```bash
: "${BENCH_SAMPLE_INTERVAL_MS:=2000}"
export BENCH_SAMPLE_INTERVAL_MS
```

- [ ] Add the Job environment entry:

```yaml
- name: BENCH_SAMPLE_INTERVAL_MS
  value: "${BENCH_SAMPLE_INTERVAL_MS}"
```

- [ ] Validate syntax and 2,000/21 renders:

```bash
bash -n bench/scripts/run-scenario.sh
rg -n 'BENCH_SAMPLE_INTERVAL_MS:=2000' bench/scripts/run-scenario.sh
BENCH_SAMPLE_INTERVAL_MS=2000 \
  envsubst '$BENCH_SAMPLE_INTERVAL_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_SAMPLE_INTERVAL_MS'
BENCH_SAMPLE_INTERVAL_MS=21 \
  envsubst '$BENCH_SAMPLE_INTERVAL_MS' \
  < bench/manifests/driver/job-template.yaml \
  | rg -n -A1 'name: BENCH_SAMPLE_INTERVAL_MS'
git diff --check
```

- [ ] Commit only deployment files:

```bash
git add bench/scripts/run-scenario.sh bench/manifests/driver/job-template.yaml
git commit -m "feat(bench): wire sample interval"
```

### Task 3: Close the bench-driver audit

- [ ] Capture exact evidence:

```bash
tools/audit-runtime-values.sh
tools/audit-runtime-values.sh | rg '^crates/bench-driver/'
rg -n 'sample_interval|SampleIntervalMs|DEFAULT_SAMPLE_INTERVAL_MS|sample-interval-ms|BENCH_SAMPLE_INTERVAL_MS' \
  crates/bench-driver bench docs/configuration-audit.md
```

Classify every bench-driver scanner and focused-search line. Confirm remaining
bench-driver hits are configured values, tests/harness values, or genuine
protocol/format/state/mathematical/query invariants rather than unresolved
operational owners.

- [ ] Append `## Bench Driver Sample Interval` to
  `docs/configuration-audit.md`, recording the default, validation, precedence,
  runtime/deployment flow, preserved grid behavior, exact counts, gates, and
  bench-driver closure.

- [ ] Re-run all package, Clippy, nightly format, help, shell/render, diff,
  lockfile, and scanner gates.

- [ ] Commit only the audit:

```bash
git add docs/configuration-audit.md
git commit -m "docs(audit): record bench sample interval"
```

### Task 4: Select the next repository owner

- [ ] Inspect the complete scanner output outside `crates/bench-driver`.
- [ ] Exclude tests, fixtures, protocol/format/state invariants, dependency
  mechanics, and already-configured defaults.
- [ ] Name the next coherent unresolved operational owner and enter the design
  approval workflow before implementation.

Keep the unrelated producer final-drain plan untracked.
