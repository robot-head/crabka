# Client Consumer Leave-Group Timeout Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Expose the classic Client Consumer deadline used for best-effort group
departure. Preserve the existing five-second default while allowing
applications to bound failed-startup cleanup and normal shutdown for their
environment.

This slice changes only the classic `Consumer`. `ShareConsumer` has a separate
heartbeat protocol and remains unchanged.

## Validated Value

Add a public `ConsumerLeaveGroupTimeout` semantic type. Its constructor uses
`refined_type::rule::MinMaxU128` to accept positive, whole-millisecond
durations representable as `u64` milliseconds. It exposes the validated
`Duration` through `duration()` and whole milliseconds through
`milliseconds()`.

`DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT` remains exactly five seconds. Zero,
fractional milliseconds, and durations above `u64::MAX` milliseconds are
rejected. Zero does not disable either leave attempt.

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`.
`crabka-client-consumer` adds the existing workspace `refined_type`
dependency; no new external dependency or generic timeout abstraction is
introduced.

## Compatibility and Data Flow

`Consumer::builder()` adds a raw
`leave_group_timeout: Duration` setter, matching its existing timing inputs,
and defaults it to `DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT`.

`Consumer::start` validates the duration after its existing local argument
checks and before constructing `StartConfig`, entering the startup retry loop,
or performing network I/O. Invalid values return
`ConsumerError::RebalanceFailed` with a field-specific message.

The validated duration is carried as a raw `Duration` in the private
`StartConfig`. Both existing leave paths consume that same policy:

```text
Consumer::start
  -> StartConfig
     -> failed-startup leave_startup_member
     -> spawned CoordinatorState -> coordinator shutdown leave_group
```

`leave_startup_member` receives the configured duration explicitly.
`CoordinatorState` stores it for the live member, whose identifier may change
during rejoins.

Both paths still send one best-effort `LeaveGroup`. Timeout, transport, and
broker errors remain ignored so cleanup cannot replace the original startup
error or block `Consumer::close` indefinitely.

The constant and semantic type are re-exported from the crate root. No existing
builder setter changes type or meaning.

## Observability Demo

The demo Consume role exposes:

- CLI: `--consumer-leave-group-timeout-ms`
- environment: `CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS`

Precedence is CLI over environment over the typed five-second default. Clap
parses `NonZeroU64`; the resolver constructs `ConsumerLeaveGroupTimeout`
before telemetry initialization or external I/O.

Supplying the option or environment variable to Produce or Stream returns an
early role-specific error. `run_consume` receives the typed value and passes
its duration to the raw `Consumer` builder setter.

Only the `demo-consume` Compose service receives
`CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS`, with `${...:-5000}` as its
deployment default.

There is no CRD field because the operator does not own or render this
standalone demo Consumer process.

## Verification

Focused tests prove:

- the typed default is exactly five seconds;
- a distinctive positive whole-millisecond override is preserved;
- zero, fractional milliseconds, and values above the `u64` millisecond range
  are rejected;
- invalid builder input fails before network I/O;
- failed-startup cleanup uses the configured deadline;
- coordinator shutdown uses the configured deadline;
- both leave paths preserve one best-effort request;
- demo environment values are used and CLI values win;
- invalid or non-Consume demo values fail before external I/O;
- help contains the new option exactly once; and
- Compose configures only `demo-consume`, defaulting to `5000`.

Final gates run all Client Consumer and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, nightly
formatting, and `git diff --check`. The runtime-value scanner and focused
leave-group search are recorded in `docs/configuration-audit.md`.

## Out of Scope

- ShareConsumer leave-heartbeat configuration;
- disabling either classic Consumer leave attempt;
- retrying failed or timed-out leaves;
- changing startup retry, request timeout, session timeout, or shutdown
  ordering;
- dynamic runtime reconfiguration;
- CRD or operator configuration;
- shared timeout abstractions across client protocols; and
- unrelated repository operational values.
