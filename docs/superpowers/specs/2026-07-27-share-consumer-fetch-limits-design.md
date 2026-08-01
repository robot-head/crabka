# ShareConsumer Fetch Limits Design

## Goal

Expose the live ShareFetch byte and record limits owned by `ShareConsumer`
while preserving the existing wire defaults. Remove the per-partition byte
constant because supported ShareFetch protocol versions never encode that
field.

This slice changes only `ShareConsumer`. Classic `Consumer`, Client Streams,
broker fetch policy, and the caller-supplied `poll(timeout)` remain unchanged.

## Protocol Boundary

The supported ShareFetch request versions are 1 and 2. In
`crates/protocol/schemas/ShareFetchRequest.json`,
`MinBytes`, `MaxBytes`, `MaxRecords`, and `BatchSize` are live in those
versions, but `PartitionMaxBytes` exists only in removed version 0.

Accordingly:

- `min_bytes`, `max_bytes`, and the record limit become configuration;
- the record limit populates both `max_records` and `batch_size`;
- `PARTITION_MAX_BYTES` is deleted rather than made tunable; and
- `FetchPartition::partition_max_bytes` is left at its generated zero default.

This matches Apache Kafka's ShareConsumer implementation:
[`ShareFetchConfig`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareFetchConfig.java)
owns minimum bytes, maximum bytes, and maximum poll records, and
[`ShareSessionHandler`](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareSessionHandler.java)
writes maximum poll records to both ShareFetch `MaxRecords` and `BatchSize`. A
separate batch-size setting would expose a wire detail that Kafka does not
treat as an independent client policy.

`poll(timeout)` continues to populate `MaxWaitMs`. It is already a per-call
control and does not need a second builder setting.

## Validated Values

Add these public semantic types beside `ShareConsumer`:

- `ShareConsumerFetchMinBytes`;
- `ShareConsumerFetchMaxBytes`; and
- `ShareConsumerFetchMaxRecords`.

Each wraps `i32`, derives `Clone`, `Copy`, `Debug`, `Eq`, and `PartialEq`, and
uses `refined_type::rule::GreaterI32<0>` in its constructor. Zero and negative
values are rejected. The byte types expose `bytes() -> i32`; the record type
exposes `records() -> i32`.

The public defaults preserve current behavior exactly:

```text
DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES = 1
DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES = 52_428_800
DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS = 500
```

`ShareConsumer::start` additionally rejects
`fetch_min_bytes > fetch_max_bytes`. A minimum larger than the response
maximum cannot be satisfied before timeout and is not a useful operating
policy.

## Public API and Data Flow

`ShareConsumer::builder()` adds raw `i32` setters:

```text
fetch_min_bytes
fetch_max_bytes
fetch_max_records
```

Raw setters preserve the existing builder style while the semantic types
provide reusable validation and documented defaults. Validation occurs after
the existing subscription and group-id checks, alongside leave-heartbeat
validation, and before either `Client` is constructed or network I/O begins.
Invalid values return `ConsumerError::RebalanceFailed` with
ShareConsumer-specific setting names.

The validated raw values are stored on `ShareConsumer` and used by every
subsequent `poll`:

```text
ShareConsumer::start
  -> ShareConsumer fetch fields
  -> ShareConsumer::poll
  -> build_share_fetch_request
     -> min_bytes
     -> max_bytes
     -> max_records
     -> batch_size = max_records
```

`build_share_fetch_topics` no longer accepts or stamps a per-partition byte
limit. Acknowledgement behavior, session epochs, assignment handling, record
decoding, and error propagation remain unchanged.

The constants and semantic types are exported from the `share` module and
re-exported from the crate root.

## Compatibility

Existing callers that omit the new setters send the same effective supported
wire values: minimum bytes 1, maximum bytes 50 MiB, and both record fields 500.
The removed per-partition assignment has no supported-wire effect because the
field is excluded from ShareFetch v1 and v2 encoding.

No existing builder input changes type or meaning. No dependency is added:
`crabka-client-consumer` already directly depends on the workspace
`refined_type` dependency.

## Deployment Ownership

This slice is library-only. Repository-wide search finds no production binary,
operator resource, CRD, Kubernetes manifest, or observability-demo role that
constructs `ShareConsumer`; external construction occurs only in integration
tests.

Therefore this slice adds no command-line option, environment variable, demo
service, or CRD field. A future deployed process that owns a `ShareConsumer`
must expose these builder settings through that process's existing
configuration surface. Creating a process only to host these settings would be
unrelated product surface.

## Verification

Focused tests prove:

- each typed default retains its exact current value;
- distinctive positive overrides are preserved;
- zero and negative values are rejected;
- `i32::MAX` is accepted;
- `fetch_min_bytes > fetch_max_bytes` fails before broker lookup;
- the builder stores all three validated overrides;
- a ShareFetch carries distinctive minimum bytes, maximum bytes, and record
  limit values;
- `batch_size` exactly equals the configured record limit; and
- fetch topics no longer stamp the version-0-only partition byte limit.

Final gates run the complete `crabka-client-consumer` all-target suite under
the locked dependency graph, strict all-target Clippy, nightly formatting, and
`git diff --check`. `Cargo.lock` must remain unchanged. The runtime-value
scanner and focused ShareConsumer search are recorded in
`docs/configuration-audit.md`, which also identifies the next unresolved
operational owner without claiming the repository-wide goal is complete.

## Out of Scope

- classic Consumer fetch configuration;
- a separate ShareFetch `BatchSize` setting;
- the removed ShareFetch v0 per-partition byte field;
- a second maximum-wait setting in addition to `poll(timeout)`;
- share acquire mode, acknowledgement mode, or isolation changes;
- command-line, environment, demo, CRD, or operator configuration without a
  production ShareConsumer owner;
- dynamic runtime reconfiguration;
- shared fetch-policy abstractions across client protocols; and
- unrelated ShareConsumer or broker behavior.
