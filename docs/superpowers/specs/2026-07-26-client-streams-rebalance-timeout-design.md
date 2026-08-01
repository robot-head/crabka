# Client Streams Rebalance Timeout Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Expose the Client Streams rebalance timeout through the application-owned
configuration path while preserving the existing 30-second behavior by
default. Reject values that cannot be represented by the Kafka protocol
instead of silently replacing them with the default.

This slice configures only the client-provided rebalance timeout. The broker
continues to provide the heartbeat interval, and the existing 3-second fallback
for an invalid broker heartbeat response remains fixed defensive protocol
behavior.

## Validated Value

Add a public `StreamsRebalanceTimeout` newtype backed by `refined_type`.
It accepts durations that are:

- positive;
- an exact whole number of milliseconds; and
- no greater than `i32::MAX` milliseconds, matching the signed Kafka wire
  field.

Its default is exactly 30 seconds. It exposes the validated `Duration` and
`i32` millisecond representation needed by the existing builders and heartbeat
request construction.

No generic duration type, shared timing profile, macro, or cross-field timeout
policy is introduced.

## Ownership and Compatibility

`StreamsApp` owns and stores a typed `StreamsRebalanceTimeout`, alongside its
existing typed poll and commit intervals.

The public `KafkaStreams` and `StreamsMembership` builders retain their existing
`Duration` inputs and 30-second defaults so current callers remain source
compatible. Each compatibility boundary constructs `StreamsRebalanceTimeout`
before performing external work:

- `KafkaStreams` validates before creating broker clients.
- `StreamsMembership` validates before schema prewarming or creating a broker
  client.

Direct `StreamsMembership` callers and callers entering through
`KafkaStreams` therefore receive the same validation. The small repeated
validation on the nested runtime path is intentional: it keeps both public
entry points safe without adding another abstraction or breaking either
builder.

## Runtime Data Flow

The configured value flows through:

1. `StreamsApp::builder().rebalance_timeout(...)`;
2. `StreamsApp::run_built`;
3. `KafkaStreams::builder().rebalance_timeout(Duration)`;
4. `StreamsMembership::builder().rebalance_timeout(Duration)`; and
5. the signed millisecond field used by the initial join heartbeat and all
   subsequent coordinator heartbeats.

The current `i32::try_from(...).unwrap_or(30_000)` conversion is removed.
Invalid values return a field-specific `StreamsClientError::Runtime` before
prewarming, broker connection setup, or heartbeat transmission.

The broker-provided heartbeat interval, its scheduling, and the fixed 3-second
fallback for nonpositive broker values do not change.

## Observability Demo

The demo Stream role exposes:

- CLI: `--streams-rebalance-timeout-ms`
- environment:
  `CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS`

Precedence is CLI over environment over the typed 30,000 ms default. Parsing
uses a nonzero integer millisecond value, then constructs
`StreamsRebalanceTimeout` before telemetry initialization or external I/O.

Supplying the option or environment variable to Produce or Consume returns an
early role-specific error. `run_stream` accepts the typed value and forwards it
to `StreamsApp`.

Only the `demo-stream` Compose service receives
`CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS`, with `${...:-30000}` as its
deployment default.

## Error Behavior

The public validated type and both raw-`Duration` compatibility boundaries
reject:

- zero;
- fractional milliseconds; and
- values greater than `i32::MAX` milliseconds.

Errors identify the Streams rebalance timeout. No invalid input silently falls
back to 30 seconds.

Clap continues to reject zero and malformed demo inputs. Values that parse but
exceed the protocol range fail during typed construction, still before
telemetry or external I/O.

## Verification

Focused tests will prove:

- the typed default is 30,000 ms;
- valid overrides preserve their exact millisecond value;
- zero, fractional milliseconds, and `i32::MAX + 1` are rejected;
- the initial and subsequent heartbeat requests use the configured value;
- `StreamsApp` stores and forwards its typed default and override;
- demo environment values are used and CLI values win;
- invalid demo values fail before external I/O even under hostile unrelated
  environment variables;
- help contains the new option exactly once; and
- Compose configures only `demo-stream`, defaulting to 30,000 ms.

The final gate runs all Client Streams and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, formatting,
and `git diff --check`.

## Out of Scope

- making the broker-derived heartbeat interval configurable;
- changing or exposing its fixed 3-second invalid-response fallback;
- CRD or operator configuration;
- generic interval or timeout abstractions;
- configuration profiles or grouped cadence policies;
- cross-field relationships with poll, commit, heartbeat, session, or request
  timeouts; and
- unrelated Client Streams or broker timing values.
