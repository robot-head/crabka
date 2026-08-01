# Client Core Connection Resource Policy Design

Close the four remaining operational constants in `crabka-client-core` while
preserving current defaults and avoiding settings for fixed protocol behavior.

## Scope

This design covers:

- the per-connection request-dispatch queue capacity;
- the maximum accepted Kafka frame size;
- the minimum response size for `IsolatedFetch`; and
- the client id written into outbound SASL request headers.

The queue and frame policies apply to every connection created by
`crabka-client-core`. The fetch minimum applies only to callers that construct
`IsolatedFetch`. SASL receives no independent setting: it reuses the existing
connection client id.

Consumer and share-consumer fetch minima are outside this slice. They already
have independent consumer-owned fetch policies and builders; their literal
request encoding is implementation of those policies, not the
`IsolatedFetch` default being closed here. Broker-side frame limits and generated
Kafka protocol defaults are also separate owners.

## Validated Types and Defaults

`crabka-client-core` owns three public validated value types.

### `ConnectionDispatchQueueCapacity`

This is a `refined_type` newtype over `usize` with a strictly-positive
invariant. Its default is `64`, preserving the existing Tokio channel
capacity. It exposes only the integer needed by `mpsc::channel`.

There is no upper bound in client-core. Memory policy belongs to each
deployment owner, while positivity is the only universal correctness
requirement.

### `ClientFrameMax`

This is a positive, whole-byte UOM value. It accepts `ByteSize`, rejects
non-finite or fractional-byte values, and stores the validated byte count in a
representation accepted by both the length-delimited codec and SASL framing.

Its default is `100MiB`. A fixed, named `100MiB` security ceiling remains in
client-core and is not configurable. Requested values above that ceiling are
rejected during configuration validation. Keeping the ceiling fixed prevents a
configuration mistake from turning an accepted-frame setting into an
unbounded allocation primitive.

The configured value is inclusive policy: a frame whose encoded length is at
or below the configured maximum is accepted, and a larger frame is rejected
before allocating its payload buffer.

### `FetchMinBytes`

This is a positive, whole-byte UOM value representable by Kafka's signed
32-bit `min_bytes` field. It accepts `ByteSize`, rejects non-finite,
fractional, zero, negative, or out-of-range values, and stores the validated
`i32` byte count. Its default is `1B`.

`IsolatedFetch` stores this type, and request construction performs no further
policy decision: it copies the validated integer into `FetchRequest.min_bytes`.

All three types report configuration errors at construction. Raw CLI, CRD, and
builder inputs are validated before DNS, socket, or broker I/O.

## Client-Core Data Flow

`ConnectionOptions` gains `dispatch_queue_capacity` and `frame_max`, both with
typed defaults. `Client::builder()` accepts the corresponding raw values,
validates them once, and stores the typed policies in `ConnectionOptions`.
Direct `ConnectionOptions` construction must provide typed values, so invalid
state cannot reach a connection.

`Connection::from_stream` uses `dispatch_queue_capacity` when creating its
writer channel. Both framed reads and framed writes use `frame_max`.

Outbound SASL negotiation receives the configured `client_id` and `frame_max`
from `ConnectionOptions`. SASL request headers encode that client id instead of
the fixed `"crabka-client"` literal. SASL response length is checked against
the configured frame maximum and the fixed security ceiling before allocating
the response buffer. Empty or otherwise invalid client ids retain the existing
client-builder behavior; this slice does not add a second client-id policy.

The public zero-argument transport codec remains as a compatibility wrapper
using `ClientFrameMax::default()`. A typed codec constructor supplies the
configured value to production connections.

`IsolatedFetch` gains `fetch_min`, defaulted at every compatibility constructor
or higher-level builder that previously relied on the literal `1`. Existing
callers therefore retain byte-for-byte request behavior unless their owner
supplies an override.

## Higher-Level Library Propagation

Libraries receive typed policy and do not read environment variables.

- `crabka-client-producer` carries queue capacity and frame maximum through its
  builder to its main, transaction-coordinator, and group-coordinator clients.
- `crabka-client-admin` accepts a complete connection policy for callers that
  need overrides while its existing convenience constructors preserve all
  client-core defaults.
- `crabka-client-streams` carries one connection queue/frame policy to every
  membership, metadata, producer, and offset client. It separately carries one
  `FetchMinBytes` value to its `IsolatedFetch` runtime path.
- `crabka-gres-fdw`, `crabka-gres-substrate`, and `crabka-gres-control` carry
  their owner-specific typed queue/frame policies to connection or producer
  construction. Their scan, recovery, and registry-reader policies separately
  carry `FetchMinBytes`.
- Other libraries that construct `Client`, `Producer`, or `ConnectionOptions`
  accept the two connection policies from their deployment owner and forward
  them without reinterpretation.

Secondary clients created after startup must reuse the same owner policy.
Defaults are not silently reintroduced in coordinator, retry, recovery, or
reconnect paths.

## Deployment Ownership

Each deployed binary that constructs a client or producer exposes one
process-level connection policy pair using its own existing environment prefix:

- `crabka-bench-driver`;
- `crabka-broker`;
- `crabka-gres`;
- `crabka-grpc-gateway`;
- `crabka-metrics` and `crabka-metrics-service`;
- `crabka-observability-demo-app`;
- `crabka-profiles`;
- `crabka-rebalancer`;
- `crabka-replicator`;
- `crabka-schema-registry`; and
- `crabka-traces`.

The CLI suffixes are `client-dispatch-queue-capacity` and
`client-frame-max`. Environment names are the binary's established prefix plus
`CLIENT_DISPATCH_QUEUE_CAPACITY` and `CLIENT_FRAME_MAX`. For example, Profiles
uses `CRABKA_PROFILES_CLIENT_DISPATCH_QUEUE_CAPACITY` and
`CRABKA_PROFILES_CLIENT_FRAME_MAX`. UOM inputs use the repository's human
`ByteSize` syntax, so the default frame value is rendered as `100MiB`.

Only deployments that own an `IsolatedFetch` path expose a fetch-minimum:

- the observability demo Stream role exposes
  `--streams-fetch-min` / `CRABKA_DEMO_STREAMS_FETCH_MIN`;
- Gres FDW exposes `--fdw-fetch-min` / `CRABKA_GRES_FDW_FETCH_MIN`;
- Gres WAL recovery exposes
  `--wal-recovery-fetch-min` / `CRABKA_GRES_WAL_RECOVERY_FETCH_MIN`; and
- the Gres registry reader exposes
  `--registry-reader-fetch-min` /
  `CRABKA_GRES_REGISTRY_READER_FETCH_MIN`.

Role-restricted options fail startup when supplied to a role that cannot use
them, matching existing Gres and observability-demo validation.

### CRD Owners

Kafka and Gres CRDs expose policy only where they own the deployed client:

- Kafka process configuration gains `clientDispatchQueueCapacity` and
  `clientFrameMax`, rendered into the broker container's prefixed CLI
  arguments.
- Gres compute configuration gains `clientDispatchQueueCapacity`,
  `clientFrameMax`, `fdwFetchMin`, and `walRecoveryFetchMin`.
- Gres registry configuration gains `clientDispatchQueueCapacity`,
  `clientFrameMax`, and `readerFetchMin`.

Omitted fields render no argument and therefore preserve library defaults.
CRD schemas use integer validation for queue capacity and string UOM values for
dimensioned byte quantities. Queue capacity has a minimum of one. Byte
quantities are validated again by the same runtime types, with
`clientFrameMax` additionally capped at `100MiB`.

No CRD field is added for standalone deployments not owned by Kafka or Gres.
Their CLI/environment surface is the ownership boundary.

## Compatibility and Error Behavior

All omitted settings preserve the current values: queue capacity `64`, frame
maximum `100MiB`, and isolated-fetch minimum `1B`. Existing public convenience
constructors remain and select these defaults.

Invalid values fail deterministically during startup or builder construction
with the owning option name in the message. No invalid value is clamped.
Runtime oversized frames use the existing transport/protocol error path; SASL
oversize errors identify the announced length and configured maximum without
echoing credentials or payload data.

## Testing and Verification

Client-core unit tests pin each default, valid replacement, and rejection
boundary. Transport tests cover exact-limit and over-limit normal frames.
SASL tests prove that the configured client id reaches every request header,
that exact-limit responses are accepted, and that oversized announced lengths
fail before payload allocation. Fetch tests prove that the configured minimum
reaches the encoded request.

Each higher-level library has a focused propagation test, including secondary
client construction. Each deployment parser tests default, CLI override,
environment override, invalid input, and role restrictions where applicable.
Kafka and Gres tests cover CRD schema validation and rendered container
arguments.

The implementation gate is:

- affected-package all-target tests;
- CRD generation with no unstaged generated diff;
- Helm lint and chart tests when rendered arguments change;
- `cargo check --workspace --all-targets --locked`;
- `cargo clippy --workspace --all-targets --locked -- -D warnings`;
- `cargo +nightly fmt --all`;
- `git diff --check`; and
- a fresh runtime-value scan documenting closure of exactly these four
  client-core rows.
