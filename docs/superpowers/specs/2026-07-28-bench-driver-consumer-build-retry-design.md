# Bench Driver Consumer Build Retry Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the fixed consumer-build retry policy in `crabka-bench-driver` with
three validated runtime settings while preserving the existing retry behavior
and defaults.

## Scope

This slice changes only the retry policy used when the benchmark driver builds
a `Consumer`.

It does not change consumer request timeouts, polling or poll-error backoff,
producer behavior, sampling cadence, final-drain timing, Prometheus timing,
scenario behavior, or the backoff dependency's growth factor and jitter.

## Public Configuration

The binary accepts:

- `--consumer-build-attempts`
- `BENCH_CONSUMER_BUILD_ATTEMPTS`
- `--consumer-build-initial-backoff-ms`
- `BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS`
- `--consumer-build-max-backoff-ms`
- `BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS`

The existing defaults remain exactly:

- 6 total build attempts;
- 100 milliseconds initial backoff;
- 2,000 milliseconds maximum backoff.

For each setting, the command-line value wins when both sources are present.
When neither is present, the compiled default is used.

Attempts and both backoffs must be positive. The initial backoff must not
exceed the maximum. Invalid values fail before scenario-file or network I/O.
These constraints prevent the backoff iterator's zero-attempt unsigned
underflow, a tight zero-delay retry loop, and `Duration::clamp` with an inverted
range.

## Validated Types

Add:

- `ConsumerBuildAttempts(u32)`, validated with
  `refined_type::rule::GreaterU32<0>`;
- `ConsumerBuildBackoffMs(u64)`, validated with
  `refined_type::rule::GreaterU64<0>`; and
- `ConsumerBuildRetryPolicy`, which stores the two newtypes and rejects an
  initial backoff above the maximum.

The newtypes implement `FromStr` and `Display` for Clap. The attempts type
exposes its validated `u32`; the backoff type exposes `Duration` only when
constructing `exponential_backoff::Backoff`.

Use named constants for all three existing defaults. The package already
directly depends on the workspace-pinned `refined_type`; this slice adds no
dependency and must not change `Cargo.lock`.

## Input Resolution

Add all three typed fields to the existing private Clap `Cli` parser with their
typed defaults.

Immediately after `Cli::parse()`, construct `ConsumerBuildRetryPolicy`. Return
a configuration error if the initial backoff exceeds the maximum. This ordering
ensures relational validation completes before reading the scenario file,
constructing clients, or performing network I/O.

Store the complete validated policy in `DriverConfig`; no raw retry value
crosses the configuration boundary.

## Runtime Flow

The exact value flow is:

```text
CLI / environment / typed defaults
  -> ConsumerBuildAttempts
     ConsumerBuildBackoffMs
  -> ConsumerBuildRetryPolicy
  -> DriverConfig::consumer_build_retry_policy
  -> ConsumerTask
  -> build_consumer_with_retry
  -> exponential_backoff::Backoff::new
```

Every consumer task uses the same immutable `Copy` policy. The retry loop,
attempt numbering, warnings, terminal error, and retry-on-all-build-errors
behavior remain unchanged.

## Dependency Defaults

`exponential_backoff::Backoff` continues to use its installed defaults:

- growth factor: 2;
- jitter: 0.3.

Those are dependency-level algorithm mechanics rather than application-owned
operational values in the current code. This slice does not add flags for them.

## Deployment Wiring

Add all three variables to the documented inputs and container environment in
`bench/manifests/driver/job-template.yaml`.

Add overrideable 6, 100, and 2,000 defaults and exports to
`bench/scripts/run-scenario.sh`. Reuse the existing `envsubst` rendering path.

No CRD or operator field is added because the benchmark launcher and Job
template own this binary.

## Tests

Test-first coverage will prove:

1. the defaults remain 6 attempts, 100 milliseconds initial, and 2,000
   milliseconds maximum;
2. one attempt and one millisecond are accepted;
3. zero, malformed, negative, and primitive-overflow values are rejected;
4. equal initial and maximum backoffs are accepted;
5. initial backoff above maximum is rejected before scenario-file I/O;
6. environment values are accepted and command-line values override them;
7. the validated policy reaches every consumer task and the sole backoff
   constructor; and
8. rendered Jobs contain defaults and explicit overrides.

Completion gates are the package's all-target tests, strict Clippy, nightly
formatting, one help entry per flag, shell syntax validation, rendered manifest
inspection, diff hygiene, and an unchanged `Cargo.lock`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, the complete value flow, verification evidence, and
the next real unresolved owner. Dependency jitter/factor, protocol constants,
test inputs, and the separate polling, sampling, final-drain, and request
timeout policies remain fixed rather than becoming part of this setting.
