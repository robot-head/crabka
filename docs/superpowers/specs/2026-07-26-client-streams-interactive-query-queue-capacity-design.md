# Client Streams Interactive Query Queue Capacity Design

## Goal

Expose the fixed capacity of the two bounded channels that carry Client
Streams interactive-query requests to the runtime supervisor. Preserve the
existing capacity of 64 by default while allowing standalone Streams
applications to tune backpressure for their workload.

This slice changes only the capacity of the existing v1 and v2 request queues.
It does not change queue behavior, query execution, shutdown semantics, or
response handling.

## Validated Value

Add a public `StreamsInteractiveQueryQueueCapacity` newtype backed by
`refined_type::rule::GreaterUsize<0>`. It accepts any positive `usize`, exposes
the validated value through `capacity()`, and defaults to exactly 64.

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`. Its
constructor documents that zero is rejected.

No generic queue policy, channel abstraction, macro, upper bound, or
cross-field rule is introduced.

## Ownership and Runtime Data Flow

`StreamsApp` owns a typed `StreamsInteractiveQueryQueueCapacity` beside its
other runtime settings and forwards it to the public `KafkaStreams` builder.
`KafkaStreams` also accepts the typed value directly, so invalid capacity
cannot enter either public builder.

At startup, `KafkaStreams` obtains the two effective capacities from a small
pure helper and creates:

- the `IqRequest` channel used by v1 read-only store views; and
- the `Iq2Request` channel used by `KafkaStreams::query`.

Both channels receive the same configured value. Their receivers remain in
the existing supervisor `select!`, which dispatches requests to
`StreamThread::serve_iq` and `StreamThread::serve_iq2`.

The default path therefore preserves both existing `mpsc::channel(64)`
capacities exactly.

## Behavior

The queues remain bounded Tokio MPSC channels. When a queue is full, the
existing asynchronous send waits for capacity. Closed-channel and shutdown
behavior remain unchanged.

One setting controls both API generations because they feed the same
supervisor workload. There are no separate v1/v2 settings, dynamic resizing,
enqueue timeouts, drop policies, fairness changes, or new metrics.

## Observability Demo

The demo Stream role exposes:

- CLI: `--streams-interactive-query-queue-capacity`
- environment:
  `CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`

Precedence is CLI over environment over the typed default of 64. Clap parses a
`NonZeroUsize`, then the resolver constructs
`StreamsInteractiveQueryQueueCapacity` before telemetry initialization or
external I/O.

Supplying the option or environment variable to Produce or Consume returns an
early role-specific error. `run_stream` accepts the typed value and forwards
it to `StreamsApp`.

Only the `demo-stream` Compose service receives
`CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`, with `${...:-64}` as
its deployment default.

There is no CRD field because the operator does not own or render a Client
Streams workload.

## Error Behavior

`StreamsInteractiveQueryQueueCapacity::new(0)` returns a validation error
identifying the interactive-query queue capacity. Positive values retain their
exact `usize` representation.

Clap rejects zero and malformed demo inputs. Demo role validation and typed
resolution occur before telemetry initialization or external I/O.

## Verification

Focused tests prove:

- the typed default is 64;
- a distinctive positive override preserves its exact value;
- zero is rejected;
- the pure helper applies the configured value to both queue capacities;
- `StreamsApp` stores and forwards its typed default and override;
- demo environment values are used and CLI values win;
- invalid or non-Stream demo values fail before external I/O under a hermetic
  subprocess environment;
- help contains the new option exactly once; and
- Compose configures only `demo-stream`, defaulting to 64.

Final gates run all Client Streams and observability demo targets under the
locked dependency graph, strict Clippy for both affected packages, nightly
formatting, and `git diff --check`. The runtime-value scanner and a focused
interactive-query capacity search are recorded in
`docs/configuration-audit.md`, which also identifies the next real
production-consumed value.

## Out of Scope

- separate v1 and v2 queue capacities;
- unbounded channels or queue removal;
- dynamic resizing, drop behavior, enqueue deadlines, or fairness changes;
- queue-depth metrics;
- CRD or operator configuration;
- generic queue or channel abstractions;
- the test-only 16-entry interactive-query servicer channel; and
- unrelated Client Streams or repository operational values.
