# Client Streams State-Store Cache Budget Design

## Goal

Expose the existing Client Streams record-cache byte budget to standalone
applications and the observability demo. Preserve the Kafka-compatible default
of `10_485_760` bytes and the existing meaning of zero: caching is disabled.

This slice changes only how the existing `statestore.cache.max.bytes` value is
validated and supplied. It does not change cache accounting, eviction, flushing,
task allocation, or store eligibility.

## Validated Value

Add a public `StreamsStateStoreCacheMaxBytes` semantic type. Its constructor
uses `refined_type::rule::MinMaxI64` to accept the inclusive range from zero
through the largest `i64` that the target's `usize` can represent. It exposes
the validated `i64` through `bytes()` and defaults to exactly `10_485_760`.

Zero remains valid because it deliberately disables record caching. Negative
values are rejected instead of being silently coerced to zero. On 64-bit
targets the maximum is `i64::MAX`; the target-aware upper bound prevents the
existing `i64` to `usize` conversion from silently disabling caching on narrower
targets.

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`. No generic
byte-size type, parser, unit enum, cache policy object, or new dependency is
introduced.

## Compatibility and Runtime Data Flow

The public `StreamsApp::cache_max_bytes(i64)` and
`KafkaStreams::cache_max_bytes(i64)` builder setters remain unchanged for source
compatibility.

`KafkaStreams::start` constructs `StreamsStateStoreCacheMaxBytes` together with
the other runtime configuration before broker I/O. Invalid input therefore
returns `StreamsClientError::Runtime` without opening a connection or spawning
the supervisor.

The validated raw value then follows the existing path:

`KafkaStreams` -> `StreamThread` -> `BuiltTopology::instantiate` ->
per-task `ThreadCache`.

The existing `StreamsApp` builder keeps the raw value and forwards it to that
single validation boundary. This avoids duplicating validation while retaining
the established builder API. Callers that want eager validation can construct
`StreamsStateStoreCacheMaxBytes` and pass `bytes()` to either builder.

The default path remains exactly `10_485_760`; zero still skips cache wrapping;
positive values retain their exact byte count.

## Observability Demo

The demo Stream role exposes:

- CLI: `--streams-state-store-cache-max-bytes`
- environment: `CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES`

Precedence is CLI over environment over the typed default of `10_485_760`.
Clap parses an `i64`, then the resolver constructs
`StreamsStateStoreCacheMaxBytes` before telemetry initialization or external
I/O.

Supplying the option or environment variable to Produce or Consume returns an
early role-specific error. `run_stream` receives the validated value and passes
its raw bytes to the compatibility-preserving `StreamsApp` builder.

Only the `demo-stream` Compose service receives
`CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES`, with
`${...:-10485760}` as its deployment default.

There is no CRD field because the operator does not own or render a Client
Streams workload.

## Error Behavior

Negative library or demo values return a validation error identifying the
Client Streams state-store cache byte budget. Zero and every target-supported
positive `i64` are accepted without normalization.

Demo role validation and typed resolution occur before telemetry initialization
or external I/O. Malformed values are rejected by Clap.

## Verification

Focused tests prove:

- the typed default is exactly `10_485_760`;
- zero and a distinctive positive override preserve their exact values;
- negative one is rejected;
- the target-supported maximum is accepted and, where representable as `i64`,
  one greater is rejected;
- low-level runtime validation rejects a negative value before broker I/O;
- `StreamsApp` preserves its raw default and override compatibility;
- demo environment values are used and CLI values win;
- invalid or non-Stream demo values fail before external I/O;
- help contains the new option exactly once; and
- Compose configures only `demo-stream`, defaulting to `10_485_760`.

Final gates run all Client Streams and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, nightly
formatting, and `git diff --check`. The runtime-value scanner and focused cache
budget search are recorded in `docs/configuration-audit.md`.

## Out of Scope

- changing cache accounting, eviction, flushing, or per-task allocation;
- changing materialized-store cache eligibility;
- dynamic resizing or cache metrics;
- alternate byte units or human-size parsing;
- changing either existing raw builder setter;
- CRD or operator configuration;
- generic cache-policy or byte-size abstractions; and
- unrelated Client Streams or repository operational values.
