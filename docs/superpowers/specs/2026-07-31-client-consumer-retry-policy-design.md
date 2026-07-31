# Client Consumer Retry Policy Design

## Goal

Replace the classic Consumer's hardcoded startup and coordinator retry timing
with one validated policy while preserving current behavior.

## Scope

`ConsumerRetryPolicy` owns seven UOM `Time` values:

| Field | Default |
|---|---:|
| `startup_attempt_timeout` | `90s` |
| `startup_deadline` | `5m` |
| `startup_initial_backoff` | `500ms` |
| `startup_max_backoff` | `5s` |
| `coordinator_retry_timeout` | `30s` |
| `coordinator_initial_backoff` | `100ms` |
| `coordinator_max_backoff` | `1s` |

The existing exponential doubling algorithm remains fixed. Retriable error
classification, coordinator error codes, best-effort shutdown semantics, and
retry cancellation behavior are not configuration.

## Validation

Construction validates every value before network I/O:

- every value is finite, positive, and a whole number of milliseconds;
- `startup_attempt_timeout <= startup_deadline`;
- `startup_initial_backoff <= startup_max_backoff`; and
- `coordinator_initial_backoff <= coordinator_max_backoff`.

Each timing is stored in a private validated newtype built with the existing
`refined_type` dependency. `ConsumerRetryPolicy` exposes named getters and
cannot contain invalid state.

## Library Data Flow

`Consumer::builder()` accepts `retry_policy: ConsumerRetryPolicy`, defaulting
to the values above. Policy construction and the rest of the Consumer input
are validated before the first connection.

Startup uses the four startup fields for its per-attempt timeout, wall-clock
deadline, and capped exponential backoff.

The validated policy is carried through `StartConfig` and
`CoordinatorState`. Coordinator discovery, re-find, commit, join, sync, and
heartbeat paths use the three coordinator fields instead of
`COORDINATOR_RETRY_TIMEOUT` and their local `100ms` / `1s` backoff values.
Shared retry helpers receive the typed coordinator policy rather than separate
raw durations.

## Deployment Owner

The observability demo Consume role exposes the seven fields:

| CLI | Environment |
|---|---|
| `--consumer-startup-attempt-timeout` | `CRABKA_DEMO_CONSUMER_STARTUP_ATTEMPT_TIMEOUT` |
| `--consumer-startup-deadline` | `CRABKA_DEMO_CONSUMER_STARTUP_DEADLINE` |
| `--consumer-startup-initial-backoff` | `CRABKA_DEMO_CONSUMER_STARTUP_INITIAL_BACKOFF` |
| `--consumer-startup-max-backoff` | `CRABKA_DEMO_CONSUMER_STARTUP_MAX_BACKOFF` |
| `--consumer-coordinator-retry-timeout` | `CRABKA_DEMO_CONSUMER_COORDINATOR_RETRY_TIMEOUT` |
| `--consumer-coordinator-initial-backoff` | `CRABKA_DEMO_CONSUMER_COORDINATOR_INITIAL_BACKOFF` |
| `--consumer-coordinator-max-backoff` | `CRABKA_DEMO_CONSUMER_COORDINATOR_MAX_BACKOFF` |

All values use unit-bearing syntax such as `90s` and `500ms`; no raw
millisecond options or environment variables are added. Explicit values are
rejected for non-Consume roles before telemetry or external I/O.

Only the `demo-consume` Compose service receives these environment variables.
No CRD is added because the operator does not own this standalone demo
Consumer.

## Compatibility

Omitted settings preserve the existing effective values and retry sequence.
The builder's default behavior, error surface, and wire requests remain
unchanged. This is configuration propagation only.

## Testing

Tests prove:

- default and non-default policy construction;
- zero, fractional-millisecond, non-finite, and invalid ordering rejection;
- configured startup attempt/deadline/backoff behavior;
- configured coordinator timeout/backoff propagation through discovery,
  re-find, and commit paths;
- demo CLI-over-environment-over-default precedence and role rejection;
- Compose ownership only by `demo-consume`; and
- all-target tests, strict Clippy, nightly formatting, and diff hygiene.

The repository-wide hardcoded operational-value audit remains active after
this slice.
