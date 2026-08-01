# Observability Demo Consumer Metadata Refresh Design

> **Historical configuration note:** This document records the pre-UOM interface
> at the time of implementation. Unit-suffixed names and primitive numeric
> examples below are historical, not the live contract; use current binary
> `--help`, generated CRDs, and unit-bearing values.

## Goal

Propagate the classic Consumer's validated subscribed-topic metadata refresh
interval through the standalone observability demo. Preserve the existing
five-second default and expose the value only on the demo's Consume role.

The Client Consumer library already owns validation and runtime behavior
through `ConsumerSubscriptionMetadataRefreshInterval`. This slice adds only
the demo deployment surface and forwards the resolved value to the existing
Consumer builder setter.

## Configuration Surface

The demo adds one optional whole-millisecond input:

- CLI: `--consumer-subscription-metadata-refresh-interval-ms`
- environment:
  `CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS`

`Cli` stores the input as `Option<NonZeroU64>`. Precedence remains Clap's
existing CLI-over-environment behavior. An omitted value resolves through
`ConsumerSubscriptionMetadataRefreshInterval::default()`, preserving exactly
`5_000` milliseconds.

The precise name distinguishes this subscribed-topic recovery cadence from
general metadata caching and request refresh behavior.

## Validation and Data Flow

Add
`effective_consumer_subscription_metadata_refresh_interval(&Cli)` beside the
existing classic Consumer leave-group resolver. It returns
`std::io::Result<ConsumerSubscriptionMetadataRefreshInterval>`.

The resolver:

1. rejects an explicitly supplied value unless `--role consume` was selected;
2. returns the typed library default when the input is absent; and
3. converts a supplied nonzero millisecond count to `Duration` and constructs
   the existing validated library type.

Resolution happens immediately after parsing and before telemetry
initialization, admin-server startup, DNS, or broker I/O. A wrong-role value
returns `InvalidInput` and names the exact flag, supplied millisecond value,
and required Consume role. Clap rejects zero or malformed values before the
resolver runs.

The typed value follows this path:

```text
Cli
  -> effective_consumer_subscription_metadata_refresh_interval
  -> main
  -> run_consume
  -> Consumer::builder()
       .subscription_metadata_refresh_interval(value.duration())
```

`run_produce` and `run_stream` do not receive the value. No combined consumer
policy object or duplicate demo-specific validated newtype is introduced.

## Deployment Ownership

Only the `demo-consume` service in
`demo/observability/docker-compose.yml` receives:

```text
CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:
  "${CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:-5000}"
```

`demo-produce` and `demo-stream` must not contain this environment variable.

There is no CRD field or operator rendering because the operator does not own
or deploy this standalone demo process.

## Compatibility

When the new input is omitted, the demo continues to use the library's exact
five-second default. The library type, default, validation range, Consumer
builder API, heartbeat-driven wakeups, inclusive elapsed-time boundary,
best-effort refresh failures, and rejoin behavior remain unchanged.

Existing Produce and Stream invocations are unchanged unless they explicitly
supply the consume-only setting, which now fails early instead of silently
accepting an unused value.

No dependency or Cargo feature changes are needed, and `Cargo.lock` must remain
unchanged.

## Verification

Focused resolver and subprocess tests prove:

- an omitted input resolves to the typed `5_000` millisecond default;
- an environment value is used;
- a CLI value takes precedence over the environment;
- explicit values on Produce and Stream fail before telemetry or external I/O;
- zero is rejected by Clap;
- help lists
  `--consumer-subscription-metadata-refresh-interval-ms` exactly once; and
- a distinctive valid Consume override reaches the typed resolver unchanged.

The existing Compose configuration test proves that only `demo-consume`
receives the environment variable and that its deployment default is `5000`.

Final gates run the observability demo all-target tests under the locked
dependency graph, strict all-target Clippy for the demo package, nightly
formatting, and `git diff --check`. The implementation records a fresh
runtime-value scan and focused exact search in `docs/configuration-audit.md`.

## Audit Handoff

The audit marks this observability-demo propagation owner complete without
claiming the classic Consumer's other production owners are configured. The
separately queued ShareConsumer acquire-mode slice remains open with the
approved `ShareAcquireMode::BatchOptimized` default, as does the broader
repository-wide hardcoded operational-value objective.

## Out of Scope

- changing the five-second default or library validation range;
- changing metadata refresh, heartbeat, rejoin, or error-handling behavior;
- adding an independent timer or dynamic runtime reconfiguration;
- propagating the setting through other classic Consumer owners;
- adding CRD or operator configuration;
- combining this value with the leave-group timeout in a new policy object;
- adding a demo-specific validated newtype; and
- implementing the queued ShareAcquireMode slice.
