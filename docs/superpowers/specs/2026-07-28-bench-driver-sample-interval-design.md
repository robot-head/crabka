# Bench Driver Sample Interval Design

## Goal

Replace the fixed bench-driver time-series sample interval with one validated
runtime setting while preserving the existing 2,000-millisecond default and
sampling behavior.

## Scope

This slice changes only the width of the shared time-series sampling grid.

It does not change scenario duration or warmup, producer or consumer behavior,
histogram accounting, report aggregation, Prometheus queries, or any other
timeout.

## Public Configuration

The binary accepts:

- `--sample-interval-ms`
- `BENCH_SAMPLE_INTERVAL_MS`

The command-line value wins when both sources are present. The existing default
remains 2,000 milliseconds.

The setting must be positive. Zero would make grid-index division invalid, so
malformed, negative, zero, and primitive-overflow values are rejected by Clap
before scenario-file or network I/O.

## Validated Type

Add `SampleIntervalMs(u64)`, validated with
`refined_type::rule::GreaterU64<0>`. It implements `FromStr` and `Display` for
Clap and exposes its validated milliseconds for the existing `Grid`.

Use a named constant for the existing default. The package already directly
depends on the workspace-pinned `refined_type`; this slice adds no dependency
and must not change `Cargo.lock`.

## Runtime Flow

Add the typed input to the private `Cli` and store it in `DriverConfig`.

The value flow is:

```text
CLI / environment / typed default
  -> DriverConfig::sample_interval
  -> run
  -> Grid::interval_ms
  -> producer and consumer sample buckets
```

The existing ceiling calculation for the number of intervals, minimum-one
bucket behavior, shared `Copy` grid, index clamping, local task histograms, and
merged output remain unchanged.

No producer or consumer task field is added because both already receive the
complete `Grid`.

## Deployment Wiring

Add the variable to the documented inputs and container environment in
`bench/manifests/driver/job-template.yaml`.

Add an overrideable 2,000-millisecond default and export to
`bench/scripts/run-scenario.sh`. Reuse the existing `envsubst` path.

No CRD or operator field is added because the benchmark launcher and Job
template own this binary.

## Tests

Test-first coverage will prove:

1. the default remains 2,000 milliseconds;
2. one millisecond is accepted;
3. zero, malformed, negative, and primitive-overflow values are rejected;
4. environment values are accepted and the command-line value overrides them;
5. the typed value reaches the sole grid construction path; and
6. rendered Jobs contain the default and an explicit override.

Completion gates are package all-target tests, strict Clippy, nightly
formatting, one help entry, shell syntax, rendered manifest inspection, diff
hygiene, and an unchanged `Cargo.lock`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts and verification evidence. Re-run the repository
scanner and select the next unresolved owner outside the bench driver.
