# Client Streams Runtime Cadence Design

## Goal

Expose the two Client Streams supervisor cadences that already control record
polling and commits. Preserve the existing 200-ms poll default, 5,000-ms commit
default, immediate first ticks, and existing low-level builder call sites.

This closes only the high-level Client Streams runtime cadence pair. Membership
and protocol timing, other Client Streams operational values, and the
repository-wide hardcoded-value audit remain open.

## Current Problem

`KafkaStreams::start` accepts raw `Duration` values for `poll_interval` and
`commit_interval`, defaults them inline, and passes them directly to
`tokio::time::interval`. Zero therefore reaches a panic boundary, fractional
milliseconds have no supported external representation, and `StreamsApp`
cannot configure either value.

The observability demo is the only in-repository binary that starts
`StreamsApp`. Its Stream role cannot configure either cadence through CLI or
environment input.

## Configuration Types

`crabka-client-streams` adds two semantic newtypes:

- `StreamsPollInterval`
- `StreamsCommitInterval`

Both wrap `Duration` and use `refined_type::rule::MinMaxU128` to require a
positive value representable as whole `u64` milliseconds. Each exposes
`new(Duration)`, `duration()`, and `milliseconds()`, and implements `Default`
with its existing runtime value.

The types live beside their owner in `runtime/app.rs` and are publicly
re-exported from `runtime` and the crate root. The crate adds the already-used
workspace `refined_type` dependency; no external dependency or lockfile change
is required.

There is no shared cadence struct, generic interval wrapper, macro, or policy
layer. Polling and committing are independently meaningful settings with
different defaults and semantics.

## Compatibility Boundary

`StreamsApp::builder()` gains typed, defaulted inputs:

```text
poll_interval: StreamsPollInterval
commit_interval: StreamsCommitInterval
```

It stores and forwards both values.

`KafkaStreams::builder()` keeps its existing `Duration` inputs and defaults so
current low-level callers remain source compatible. At the start of
`KafkaStreams::start`, both durations are immediately parsed into their
semantic types before topology setup, DNS, or broker I/O. The supervisor then
uses the validated durations.

This keeps the public low-level compatibility surface while making its former
panic boundary an explicit `StreamsClientError`. High-level and demo callers
use the validated types directly.

## Demo Surface

The observability demo adds Stream-role-only inputs:

| Policy | CLI | Environment | Default |
| --- | --- | --- | --- |
| Poll interval | `--streams-poll-interval-ms` | `CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS` | `200` |
| Commit interval | `--streams-commit-interval-ms` | `CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS` | `5000` |

Each optional CLI field uses `std::num::NonZeroU64`. Clap provides
CLI-over-environment precedence; absence selects the corresponding typed
default. The demo converts supplied milliseconds to the semantic newtype and
forwards both values only to `StreamsApp`.

Supplying either option for Produce or Consume returns a field-specific error
before telemetry initialization, schema-registry access, DNS, or socket I/O.
The `demo-stream` Compose service passes through both variables with the
defaults above. No other demo service receives them.

There is no CRD field because the operator does not own or render a Client
Streams workload.

## Runtime Flow

```text
demo CLI / environment / typed defaults
  -> StreamsPollInterval + StreamsCommitInterval
  -> StreamsApp
  -> durations at the compatible KafkaStreams builder
  -> immediate refined validation in KafkaStreams::start
  -> existing Tokio poll and commit interval timers
```

The change preserves Tokio's current immediate first tick and the existing
`tokio::select!` branches. It does not alter assignment handling, record fetch
policy, commit behavior, ALO/EOS semantics, broker clients, membership, cache,
or shutdown.

The two values have no cross-field constraint. A poll interval does not define
a valid lower or upper bound for a commit interval, or vice versa.

## Validation and Errors

Both newtypes reject:

- zero;
- a fractional millisecond;
- a duration whose milliseconds cannot fit in `u64`.

Direct `KafkaStreams` callers receive a `StreamsClientError` before any
external I/O when either raw duration is invalid. The error names the invalid
poll or commit field. The demo rejects zero through Clap and maps any typed
conversion error to `InvalidInput`.

No unchecked conversion or panic is used for external input.

## Verification

Focused tests cover:

- both typed defaults;
- distinctive valid overrides;
- zero and fractional-millisecond rejection for both types;
- invalid direct `KafkaStreams` durations failing before broker lookup;
- `StreamsApp` storing the typed defaults and independent overrides;
- existing direct `KafkaStreams::builder().commit_interval(Duration)` callers
  continuing to compile;
- demo environment input and CLI-over-environment precedence for both fields;
- zero rejection and field-specific non-Stream-role rejection before I/O;
- both exact help flags appearing once;
- Compose pass-through existing only on `demo-stream`.

Final gates run all targets for `crabka-client-streams` and
`observability-demo-app`, strict Clippy, formatting, and `git diff --check`.
The runtime-value scanner and a focused cadence search are recorded in
`docs/configuration-audit.md`.

## Non-Goals

- A combined cadence profile or cross-field validation.
- Changing Tokio's immediate first tick.
- Changing the existing low-level `KafkaStreams` builder input types.
- Configuring membership retry sleeps, heartbeat timing, leave deadlines, test
  timeouts, punctuation, fetch wait, or broker request deadlines.
- Exposing Produce- or Consume-role cadence settings.
- A CRD, file, or foreign-server configuration surface.
- Any dependency other than the existing workspace `refined_type` crate.

## Audit Continuation

After verification, the audit classifies every focused cadence match and
records exact scanner totals. It names the next coherent unresolved owner from
current evidence without claiming the repository-wide goal is complete.
