# Client Streams Join Retry Backoff Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Expose the fixed delay between Client Streams initial join attempts after
Kafka returns `COORDINATOR_LOAD_IN_PROGRESS`. Preserve the existing 200-ms
fixed-delay behavior by default while allowing operators to tune it.

This slice changes only that existing retry sleep. It does not add exponential
backoff, jitter, a retry limit, or retries for additional response codes.

## Validated Value

Add a public `StreamsJoinRetryBackoff` newtype backed by `refined_type`.
It accepts durations that are:

- positive;
- an exact whole number of milliseconds; and
- representable as `u64` milliseconds.

Its default is exactly 200 milliseconds. It exposes the validated `Duration`
and `u64` millisecond representation used by builders and external
configuration.

No generic duration abstraction, retry-policy struct, macro, or cross-field
policy is introduced.

## Ownership and Compatibility

`StreamsApp` owns and stores a typed `StreamsJoinRetryBackoff` beside its other
Client Streams settings.

The public `KafkaStreams` and `StreamsMembership` builders retain `Duration`
inputs and a 200-ms default. Both construct the semantic type before external
work:

- `KafkaStreams` validates before creating broker clients.
- `StreamsMembership` validates before schema prewarming or creating a broker
  client.

This preserves direct low-level builder call sites while ensuring invalid
values cannot reach the retry loop or external I/O. Repeated validation on the
nested path keeps both public entry points independently safe.

## Runtime Data Flow

The configured value flows through:

1. `StreamsApp::builder().join_retry_backoff(...)`;
2. `StreamsApp::run_built`;
3. `KafkaStreams::builder().join_retry_backoff(Duration)`;
4. `StreamsMembership::builder().join_retry_backoff(Duration)`; and
5. the initial join loop's sleep after
   `COORDINATOR_LOAD_IN_PROGRESS`.

A small pure retry-path helper used by the loop returns the configured delay
only for `COORDINATOR_LOAD_IN_PROGRESS`. This provides a deterministic test
seam for the exact value without changing retry behavior.

Successful responses and all other error codes continue directly to the
existing response mapper. The configured backoff has no relationship with the
rebalance timeout, broker-provided heartbeat interval, processing poll
interval, or commit interval.

## Observability Demo

The demo Stream role exposes:

- CLI: `--streams-join-retry-backoff-ms`
- environment: `CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`

Precedence is CLI over environment over the typed 200-ms default. Parsing uses
a nonzero integer millisecond value, then constructs
`StreamsJoinRetryBackoff` before telemetry initialization or external I/O.

Supplying the option or environment variable to Produce or Consume returns an
early role-specific error. `run_stream` accepts the typed value and forwards it
to `StreamsApp`.

Only the `demo-stream` Compose service receives
`CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`, with `${...:-200}` as its
deployment default.

There is no CRD field because the operator does not own or render a Client
Streams workload.

## Error Behavior

The public validated type and both raw-`Duration` compatibility boundaries
reject zero and fractional milliseconds. Errors identify the Client Streams
join retry backoff.

Clap rejects zero and malformed demo inputs. All demo resolution and role
validation occur before telemetry or external I/O.

## Verification

Focused tests prove:

- the typed default is 200 ms;
- a distinctive valid override preserves its exact value;
- zero and fractional milliseconds are rejected;
- the join loop's `COORDINATOR_LOAD_IN_PROGRESS` path uses the configured
  delay and other response codes do not retry through it;
- invalid direct `KafkaStreams` values fail before broker lookup;
- `StreamsApp` stores and forwards its typed default and override;
- demo environment values are used and CLI values win;
- invalid or non-Stream demo values fail before external I/O under a hermetic
  subprocess environment;
- help contains the new option exactly once; and
- Compose configures only `demo-stream`, defaulting to 200 ms.

Final gates run all Client Streams and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, nightly
formatting, and `git diff --check`. The runtime-value scanner and a focused
join-retry search are recorded in `docs/configuration-audit.md`.

## Out of Scope

- exponential backoff or jitter;
- a retry-count limit or retry deadline;
- retrying any response other than `COORDINATOR_LOAD_IN_PROGRESS`;
- broker-derived heartbeat timing or its fixed invalid-response fallback;
- CRD or operator configuration;
- generic retry or duration abstractions;
- relationships with other Client Streams timing values; and
- unrelated Client Streams or repository timing values.
