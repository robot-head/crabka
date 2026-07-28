# Bench Driver Client Request Timeouts Design

## Goal

Replace the fixed producer and consumer request-timeout policy in
`crabka-bench-driver` with two validated runtime settings while preserving the
existing stack-specific defaults and client behavior.

## Scope

This slice changes only the request timeout passed to every benchmark
`Producer` builder and every benchmark `Consumer` builder, including consumer
build retries.

It does not change producer final-drain timing, consumer build attempts or
backoff, consumer polling or error backoff, sampling cadence, Prometheus
timeouts, client-library defaults, or scenario behavior.

## Public Configuration

The producer request timeout accepts:

- `--producer-request-timeout-seconds`
- `BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS`

Its default remains exactly 2 seconds.

The active stack's consumer request timeout accepts:

- `--consumer-request-timeout-seconds`
- `BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS`

When no consumer override is present, the default remains exactly 5 seconds
for a Crabka run and 30 seconds for a Kafka run. One active-stack setting avoids
exposing an irrelevant Kafka-specific option on a Crabka run or vice versa.

For both settings, the command-line value wins when both sources are present.
Zero, malformed, negative, and values whose whole-second duration exceeds
`i32::MAX` protocol milliseconds fail during argument parsing, before client
construction or network I/O.

## Validated Type

Add a public `ClientRequestTimeoutSeconds(u64)` newtype. Its constructor and
`FromStr` implementation validate with:

```text
refined_type::rule::MinMaxU64<1, 2_147_483>
```

The upper bound is the largest whole-second value representable by the Kafka
protocol's signed 32-bit millisecond timeout. The type exposes a `Duration`
only at the producer and consumer builder boundaries.

Use named constants for the existing defaults:

- `DEFAULT_PRODUCER_REQUEST_TIMEOUT_SECONDS = 2`
- `DEFAULT_CRABKA_CONSUMER_REQUEST_TIMEOUT_SECONDS = 5`
- `DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT_SECONDS = 30`

`crabka-bench-driver` already directly depends on the workspace-pinned
`refined_type`; this slice adds no dependency and must not change `Cargo.lock`.

## Input Resolution

Add both settings to the existing private Clap `Cli` parser.

The producer field uses its typed 2-second default directly. The consumer field
is optional at the parser boundary because its default depends on the parsed
stack. After parsing, resolve it to the supplied typed value or the named
Crabka/Kafka default, then store only a concrete validated value in
`DriverConfig`.

Clap provides command-line-over-environment precedence without custom
precedence code.

## Runtime Flow

The producer value flow is:

```text
CLI / environment / typed default
  -> ClientRequestTimeoutSeconds
  -> DriverConfig::producer_request_timeout_seconds
  -> Producer::builder
  -> request_timeout
```

The consumer value flow is:

```text
CLI / environment / active-stack typed default
  -> ClientRequestTimeoutSeconds
  -> DriverConfig::consumer_request_timeout_seconds
  -> every build_consumer_with_retry attempt
  -> Consumer::builder
  -> request_timeout
```

Remove the hidden `producer_request_timeout()` and
`consumer_request_timeout(stack)` duration helpers. Producer send behavior,
consumer retry behavior, TLS selection, error reporting, and failover
measurement remain unchanged.

## Deployment Wiring

Add both variables to the documented inputs and container environment in
`bench/manifests/driver/job-template.yaml`.

In `bench/scripts/run-scenario.sh`, provide an overrideable 2-second producer
default. If no consumer override is supplied, select 5 seconds for `crabka` and
30 seconds for `kafka`, using the already-validated `STACK` argument. Export
both nonempty values through the existing `envsubst` rendering path.

No CRD or operator field is added because the benchmark launcher and Job
template own this binary.

## Tests

Test-first coverage will prove:

1. the producer default remains 2 seconds;
2. the consumer defaults remain 5 seconds for Crabka and 30 seconds for Kafka;
3. one second and the maximum protocol-safe whole-second value are accepted;
4. zero, malformed, negative, and one-above-maximum values are rejected;
5. environment values are accepted and command-line values override them;
6. an explicit consumer override replaces either stack default;
7. `DriverConfig` forwards the typed values to the producer and consumer
   construction paths; and
8. rendered Crabka, Kafka, and explicit-override Jobs contain the expected
   values.

Completion gates are the package's all-target tests, strict Clippy, nightly
formatting, single-help-entry checks for both flags, shell syntax validation,
rendered manifest inspection, diff hygiene, and an unchanged `Cargo.lock`.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, both complete value flows, verification evidence,
and the next real unresolved owner. Protocol constants, test inputs, and the
separate retry, polling, sampling, and final-drain policies remain fixed rather
than becoming part of this setting.
