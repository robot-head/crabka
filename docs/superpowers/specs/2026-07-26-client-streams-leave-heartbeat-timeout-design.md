# Client Streams Leave-Heartbeat Timeout Design

## Goal

Expose the existing five-second Client Streams shutdown deadline for its final
leave heartbeat. Preserve the current default and best-effort shutdown
semantics while allowing applications to bound graceful group departure for
their environment.

This slice changes only Client Streams. The consumer and share-consumer
coordinators have separate configuration surfaces and remain unchanged.

## Validated Value

Add a public `StreamsLeaveHeartbeatTimeout` semantic type beside the existing
Client Streams membership timing types. Its constructor uses
`refined_type::rule::MinMaxU128` to accept positive, whole-millisecond
durations representable as `u64` milliseconds. It exposes the validated
`Duration` through `duration()` and whole milliseconds through
`milliseconds()`.

`DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT` remains exactly five seconds. Zero,
fractional milliseconds, and durations above `u64::MAX` milliseconds are
rejected. Zero does not disable the leave attempt.

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`. The existing
positive whole-millisecond validation in the membership module is reused; no
generic timeout policy, cross-client abstraction, or new dependency is added.

## Compatibility and Runtime Data Flow

`StreamsMembership` and `KafkaStreams` expose raw `Duration` builder setters,
matching their existing timing APIs. Both default to
`DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT`.

`KafkaStreams::start` validates the value with its other runtime configuration
before creating broker clients. `StreamsMembership::start` also validates its
direct-builder input before schema prewarming or broker I/O. Invalid values
return `StreamsClientError::Runtime`.

`StreamsApp` stores a typed `StreamsLeaveHeartbeatTimeout`, defaults it, and
forwards its duration to `KafkaStreams`. The value then follows the existing
membership path:

`StreamsApp` -> `KafkaStreams` -> `StreamsMembership` -> `CoordinatorState`.

On shutdown, the coordinator uses the configured value in
`tokio::time::timeout` around the final heartbeat with `member_epoch = -1`.
Timeout, transport error, and broker error remain ignored because the leave is
best effort; `close()` continues once the deadline expires.

The new constant and semantic type are re-exported from `membership` and the
crate root.

## Observability Demo

The demo Stream role exposes:

- CLI: `--streams-leave-heartbeat-timeout-ms`
- environment: `CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS`

Precedence is CLI over environment over the typed five-second default. Clap
parses `NonZeroU64`; the resolver constructs
`StreamsLeaveHeartbeatTimeout` before telemetry initialization or external
I/O.

Supplying the option or environment variable to Produce or Consume returns an
early role-specific error. `run_stream` receives the typed value and passes it
to `StreamsApp`.

Only the `demo-stream` Compose service receives
`CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS`, with `${...:-5000}` as its
deployment default.

There is no CRD field because the operator does not own or render a Client
Streams workload.

## Verification

Focused tests prove:

- the typed default is exactly five seconds;
- a distinctive positive whole-millisecond override is preserved;
- zero and fractional milliseconds are rejected;
- the `u64` millisecond boundary is enforced;
- direct `StreamsMembership` and `KafkaStreams` invalid values fail before
  external I/O;
- the coordinator bounds a stalled leave heartbeat with the configured
  duration;
- `StreamsApp` uses the typed default and override;
- demo environment values are used and CLI values win;
- invalid or non-Stream demo values fail before external I/O;
- help contains the new option exactly once; and
- Compose configures only `demo-stream`, defaulting to `5000`.

Final gates run all Client Streams and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, nightly
formatting, and `git diff --check`. The runtime-value scanner and focused
leave-timeout search are recorded in `docs/configuration-audit.md`.

## Out of Scope

- consumer or share-consumer leave deadlines;
- disabling the leave heartbeat;
- retrying a failed or timed-out leave;
- changing the leave request or shutdown ordering;
- dynamic runtime reconfiguration;
- CRD or operator configuration;
- shared timeout abstractions across client crates; and
- unrelated Client Streams or repository operational values.
