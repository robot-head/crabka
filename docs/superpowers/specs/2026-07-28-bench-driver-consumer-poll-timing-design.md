# Bench Driver Consumer Poll Timing Design

## Goal

Replace the fixed consumer poll wait and poll-error sleep in
`crabka-bench-driver` with two validated runtime settings while preserving the
existing consumer loop and defaults.

## Scope

This slice changes only:

- the 50-millisecond timeout passed to `Consumer::poll`; and
- the 100-millisecond sleep after a poll error.

It does not change consumer construction or request timeouts, retry behavior,
TLS, message processing, sampling, producer behavior, or error reporting.

## Public Configuration

The binary accepts:

- `--consumer-poll-timeout-ms`
- `BENCH_CONSUMER_POLL_TIMEOUT_MS`
- `--consumer-poll-error-backoff-ms`
- `BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS`

The command-line value wins when both sources are present. The existing
defaults remain 50 milliseconds and 100 milliseconds respectively.

Both settings must be positive. Zero would turn either wait into a tight loop,
so malformed, negative, zero, and primitive-overflow values are rejected by
Clap before scenario-file or network I/O.

## Validated Type

Add one `ConsumerPollDurationMs(u64)` newtype validated with
`refined_type::rule::GreaterU64<0>`. It implements `FromStr` and `Display` for
Clap and exposes only a `Duration`.

Use the same value type for both settings; their `DriverConfig` field names
carry the distinct roles. There is no relationship between the values, so no
policy wrapper or relational validation is needed.

Use named constants for the existing defaults. The package already directly
depends on the workspace-pinned `refined_type`; this slice adds no dependency
and must not change `Cargo.lock`.

## Runtime Flow

Add both typed inputs to the existing private `Cli`, then store them in
`DriverConfig` and copy them into each `ConsumerTask`.

The value flows are:

```text
CLI / environment / typed default
  -> DriverConfig::consumer_poll_timeout
  -> ConsumerTask
  -> Consumer::poll

CLI / environment / typed default
  -> DriverConfig::consumer_poll_error_backoff
  -> ConsumerTask
  -> tokio::time::sleep after Err
```

The consumer loop, stop-state checks, message accounting, first-error
recording, close behavior, and all other timing remain unchanged.

## Deployment Wiring

Add both variables to the documented inputs and container environment in
`bench/manifests/driver/job-template.yaml`.

Add overrideable 50- and 100-millisecond defaults and exports to
`bench/scripts/run-scenario.sh`. Reuse the existing `envsubst` path.

No CRD or operator field is added because the benchmark launcher and Job
template own this binary.

## Tests

Test-first coverage will prove:

1. defaults remain 50 and 100 milliseconds;
2. one millisecond is accepted;
3. zero, malformed, negative, and primitive-overflow values are rejected;
4. environment values are accepted and command-line values override them;
5. both typed values reach every consumer task and the sole poll/error-sleep
   sites; and
6. rendered Jobs contain defaults and explicit overrides.

Completion gates are package all-target tests, strict Clippy, nightly
formatting, one help entry per flag, shell syntax, rendered manifest
inspection, diff hygiene, and an unchanged `Cargo.lock`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, verification evidence, and the next unresolved
owner. Sampling cadence and producer final-drain timing remain separate.
