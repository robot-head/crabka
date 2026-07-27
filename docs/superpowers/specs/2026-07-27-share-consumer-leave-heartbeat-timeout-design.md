# ShareConsumer Leave-Heartbeat Timeout Design

## Goal

Expose the deadline that bounds `ShareConsumer`'s final best-effort
leave heartbeat. Preserve the existing five-second default and all current
shutdown behavior while allowing applications to choose the deadline through
the public builder.

This slice changes only `ShareConsumer`. Classic `Consumer` and Client Streams
have separate leave protocols and remain unchanged.

## Validated Value

Add a public `ShareConsumerLeaveHeartbeatTimeout` semantic type in the
ShareConsumer module. Its constructor uses
`refined_type::rule::MinMaxU128` to accept positive, whole-millisecond
durations representable as `u64` milliseconds. It exposes the validated
`Duration` through `duration()` and whole milliseconds through
`milliseconds()`.

`DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT` is exactly five seconds.
Zero, fractional milliseconds, and durations above `u64::MAX` milliseconds are
rejected. Zero does not disable the leave heartbeat.

The type derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`.
`crabka-client-consumer` already depends on the workspace `refined_type`
dependency. This slice adds no dependency and does not share a timeout type or
validator with another client protocol.

## Compatibility and Data Flow

`ShareConsumer::builder()` adds a raw
`leave_heartbeat_timeout: Duration` setter, matching its existing timing
inputs, and defaults it to
`DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT`.

`ShareConsumer::start` validates the duration after its existing subscription
and group-id checks and before constructing either `Client` or performing
network I/O. Invalid values return `ConsumerError::RebalanceFailed` with a
ShareConsumer-specific message.

The validated duration is carried as a raw `Duration` through the private
coordinator state:

```text
ShareConsumer::start
  -> ShareCoordinatorState
  -> coordinator observes shutdown
  -> tokio::time::timeout(configured timeout, final heartbeat)
```

The final request still uses `ShareGroupHeartbeat` with
`member_epoch = -1`. Shutdown still performs the existing final
acknowledgement flush before cancelling and awaiting the coordinator task.
The coordinator still sends exactly one leave heartbeat; timeout, transport,
and broker errors remain ignored. There is no retry or disable switch.

The constant and semantic type are exported through the `share` module and
re-exported from the crate root. Existing builder inputs and defaults retain
their types and meanings.

## Deployment Ownership

This slice is library-only. Repository-wide search finds no production binary,
operator resource, CRD, Kubernetes manifest, or observability-demo role that
constructs `ShareConsumer`; all external builder call sites are integration
tests.

Therefore this slice adds no command-line option, environment variable, demo
service, or CRD field. A future deployed process that owns a `ShareConsumer`
must expose this builder value through that process's existing configuration
surface. Adding a process solely to host this setting would create unrelated
product surface.

## Verification

Focused tests prove:

- the typed default is exactly five seconds;
- a distinctive positive whole-millisecond override is preserved;
- zero, fractional milliseconds, and values above the `u64` millisecond range
  are rejected;
- invalid builder input fails before broker lookup;
- coordinator shutdown uses the configured deadline against a stalled broker;
- the final request remains one `member_epoch = -1` heartbeat; and
- timeout, transport, and broker failures remain best-effort.

The stalled-broker regression test gives the mocked client a request timeout
longer than its outer guard, so replacing the configured deadline with the old
five-second literal fails deterministically.

Final gates run the complete `crabka-client-consumer` all-target suite under
the locked dependency graph, strict all-target Clippy, nightly formatting, and
`git diff --check`. `Cargo.lock` must remain unchanged. The runtime-value
scanner and focused ShareConsumer search are recorded in
`docs/configuration-audit.md`.

## Out of Scope

- classic Consumer or Client Streams leave configuration;
- command-line, environment, demo, CRD, or operator configuration without a
  production ShareConsumer owner;
- disabling the final leave heartbeat;
- retrying failed or timed-out leave heartbeats;
- changing final acknowledgement flushing or shutdown ordering;
- changing the broker-provided steady-state heartbeat interval;
- dynamic runtime reconfiguration;
- shared timeout abstractions across client protocols; and
- unrelated repository operational values.
