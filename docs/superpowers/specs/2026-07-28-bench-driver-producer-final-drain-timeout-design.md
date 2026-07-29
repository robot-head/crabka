# Bench Driver Producer Final-Drain Timeout Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Replace the fixed producer final-drain timeout in `crabka-bench-driver` with
one validated runtime setting while preserving the existing drain behavior and
10-second default.

## Scope

This slice changes only the deadline used while waiting for outstanding
producer sends after the measurement window closes.

It does not change producer request timeouts, send behavior, drop accounting,
error text, flush or close behavior, consumer behavior, sampling, Prometheus
timing, or scenario inputs.

## Public Configuration

The binary accepts:

- `--producer-final-drain-timeout-seconds`
- `BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS`

The command-line value wins when both sources are present. The existing default
remains 10 seconds.

The setting must be positive. Zero would prevent any useful final drain, so
malformed, negative, zero, and primitive-overflow values are rejected by Clap
before scenario-file or network I/O.

## Validated Type

Add `ProducerFinalDrainTimeoutSeconds(u64)`, validated with
`refined_type::rule::GreaterU64<0>`. It implements `FromStr` and `Display` for
Clap and exposes only a `Duration`.

Do not reuse `ClientRequestTimeoutSeconds`: its upper bound exists for Kafka's
signed 32-bit millisecond protocol field, while this timeout is an in-process
Tokio deadline with no such protocol constraint.

Use a named constant for the existing default. The package already directly
depends on the workspace-pinned `refined_type`; this slice adds no dependency
and must not change `Cargo.lock`.

## Runtime Flow

Add the typed input to the existing private `Cli`, store it in `DriverConfig`,
and copy it into each `ProducerTask`.

The value flow is:

```text
CLI / environment / typed default
  -> DriverConfig::producer_final_drain_timeout
  -> ProducerTask
  -> Instant::now() + timeout
  -> timeout_at for outstanding sends
```

The loop deadline check, unresolved-send drop accounting, first-error
preservation, timeout error text, producer flush, and producer close remain
unchanged.

## Deployment Wiring

Add the variable to the documented inputs and container environment in
`bench/manifests/driver/job-template.yaml`.

Add an overrideable 10-second default and export to
`bench/scripts/run-scenario.sh`. Reuse the existing `envsubst` path.

No CRD or operator field is added because the benchmark launcher and Job
template own this binary.

## Tests

Test-first coverage will prove:

1. the default remains 10 seconds;
2. one second is accepted;
3. zero, malformed, negative, and primitive-overflow values are rejected;
4. environment values are accepted and the command-line value overrides them;
5. the typed value reaches every producer task and the sole final-drain
   deadline; and
6. rendered Jobs contain the default and an explicit override.

Completion gates are package all-target tests, strict Clippy, nightly
formatting, one help entry, shell syntax, rendered manifest inspection, diff
hygiene, and an unchanged `Cargo.lock`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, verification evidence, and the next unresolved
owner. Sampling cadence remains separate.
