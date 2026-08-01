# Client Consumer Subscription Metadata Refresh Design

## Goal

Expose the classic consumer's fixed five-second subscribed-topic metadata
refresh cadence as validated library configuration while preserving its exact
default and recovery behavior.

This cadence lets a running consumer discover that a subscribed topic has
appeared or gained partitions after the initial group join. It is separate
from broker heartbeats, request deadlines, general metadata caching, and
ShareConsumer behavior.

## Current Behavior

The coordinator loop wakes at the configured heartbeat interval. When it is
not already rejoining, it performs a metadata refresh after
`SUBSCRIPTION_METADATA_REFRESH = 5 seconds` has elapsed. A successful refresh
that shows a subscribed topic appearing or gaining partitions requests a
rejoin. Failed metadata requests are ignored and retried on a later heartbeat.

The known partition-count baseline advances only after a successful rejoin and
only by monotonic maximum. This prevents transient metadata under-reporting or
a newer pre-rejoin observation from suppressing later recovery.

## Validated Value

Add the public semantic type:

```text
ConsumerSubscriptionMetadataRefreshInterval(Duration)
```

Its constructor uses the existing `refined_type::rule::MinMaxU128` dependency.
It accepts positive, whole-millisecond durations from 1 millisecond through
`u64::MAX` milliseconds. It rejects zero, fractional milliseconds, and larger
durations.

The public default preserves current behavior exactly:

```text
DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL = 5 seconds
```

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`, implements
`Default`, and exposes `duration() -> Duration` and
`milliseconds() -> u64`.

Zero does not disable proactive metadata recovery. A disable switch would
introduce a failure mode in which a consumer that joined before its topic
existed could remain permanently unassigned.

## Public API and Data Flow

`Consumer::builder()` adds a raw `Duration` setter:

```text
subscription_metadata_refresh_interval
```

The raw setter follows the existing builder style. `Consumer::start` validates
the value after the existing subscription, group-id, group-instance-id, and
fetch-budget checks, but before `StartConfig` enters the retry loop and before
any `Client` construction or network I/O. Invalid values return
`ConsumerError::RebalanceFailed` with a consumer-specific setting name.

The validated raw `Duration` follows this path:

```text
Consumer::start
  -> StartConfig
  -> Consumer::start_once
  -> CoordinatorState
  -> coordinator::run elapsed-time gate
  -> subscribed_partition_counts
  -> rejoin when subscribed partition counts grow
```

Only `StartConfig` and `CoordinatorState` need the field. The returned
`Consumer` handle does not need to retain a duplicate because the coordinator
task exclusively owns the live cadence after startup.

Replace the fixed constant in the elapsed-time check with the configured state
field. A small private `subscription_metadata_refresh_due` predicate owns the
inclusive `elapsed >= interval` boundary so paused-time tests exercise the
actual production decision.

Export the default constant and semantic type from the crate root.

## Preserved Coordinator Semantics

This change does not alter:

- heartbeat cadence or missed-tick behavior;
- the rule that no metadata refresh runs while a rejoin is already pending;
- best-effort handling of metadata request failures;
- topic-appearance and partition-growth detection;
- monotonic partition-count merging;
- when the baseline advances after rejoin;
- assignment, offset, commit, leave-group, or shutdown behavior; or
- ShareConsumer.

The refresh can run only on heartbeat-loop wakeups. A configured interval
shorter than the heartbeat interval therefore does not create an independent
timer or promise sub-heartbeat precision.

## Compatibility and Error Handling

Existing callers that omit the new setter retain the exact five-second
elapsed-time threshold. No existing input changes type or meaning.

Validation is fail-fast and deterministic:

- zero is rejected;
- fractional milliseconds are rejected;
- values above `u64::MAX` milliseconds are rejected; and
- invalid configuration fails before bootstrap lookup or connection attempts.

No dependency or general-purpose cadence abstraction is added.

## Deployment Ownership

Unlike ShareConsumer, classic `Consumer` has multiple production owners in the
repository, including observability, metrics, traces, gRPC Gateway, profiles,
replicator, bench-driver, and service/demo binaries. Their configuration
surfaces are independent.

This library slice is a prerequisite, not deployment completion. It does not
read environment variables inside the library and does not invent one global
process setting. Subsequent owner-specific slices must:

- expose the interval through that process's command-line and environment
  configuration;
- add a CRD field and operator rendering where the process is operator-owned;
- forward the resolved value to every production `Consumer` construction in
  that owner; and
- preserve the shared five-second default.

The configuration audit must keep those deployment owners open rather than
claiming the repository-wide objective is complete. The first follow-up owner
is `observability-demo-app`, which already exposes the adjacent classic
Consumer leave-group timeout through CLI, environment, and Compose. That slice
adds the matching metadata-refresh setting only to the Consume role and its
`demo-consume` service. It adds no CRD because the operator does not own this
standalone demo Consumer.

## Verification

Focused tests prove:

- the semantic default is exactly 5,000 milliseconds;
- a distinctive positive override is preserved;
- zero and fractional milliseconds are rejected;
- `u64::MAX` milliseconds is accepted and larger durations are rejected;
- invalid builder input fails before broker lookup;
- the configured value reaches `CoordinatorState`; and
- the inclusive due predicate changes from false to true at the exact
  configured paused-time boundary.

Existing metadata-growth and monotonic-merge tests continue to prove recovery
semantics. Final gates run the complete `crabka-client-consumer` all-target
suite under the locked dependency graph, strict all-target Clippy, nightly
formatting, and `git diff --check`. `Cargo.lock` must remain unchanged.

The runtime-value scanner and focused exact search are recorded in
`docs/configuration-audit.md`. The audit names `observability-demo-app` as the
first production configuration owner to propagate next and retains the parked
ShareAcquireMode slice, whose approved default is now `BatchOptimized`.

## Out of Scope

- changing the five-second default;
- disabling proactive metadata recovery;
- introducing a timer independent of the heartbeat loop;
- changing heartbeat, session, rebalance, request, coordinator-retry, or
  startup-retry policy;
- general Kafka metadata cache policy;
- dynamic runtime reconfiguration;
- ShareConsumer metadata behavior;
- combining unrelated production configuration owners into this library
  change; and
- implementing the separately queued ShareAcquireMode slice.
