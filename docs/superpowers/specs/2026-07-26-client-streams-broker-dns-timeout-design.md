# Client Streams Broker DNS Timeout Design

## Goal

Give one Kafka Streams process a validated broker DNS deadline and apply that
same value to every broker lookup it owns. Preserve the existing 10,000-ms
default and existing builder call sites.

This closes only Client Streams broker DNS. Other Client Streams operational
values and the repository-wide hardcoded-value audit remain open.

## Current Problem

`crabka-client-streams` creates several broker clients during startup:

- metadata and offset `Client`s;
- an idempotent or transactional `Producer`;
- join and heartbeat clients in `StreamsMembership`;
- a dedicated raw fetch `Connection`.

The clients and producer use their independent 10-second DNS defaults. The raw
fetch connection has two direct, unbounded `tokio::net::lookup_host` calls:
one in the at-least-once path and one in the exactly-once path. There is no
single process setting that keeps these paths consistent.

The observability demo is the only in-repository binary that starts
`StreamsApp`. Its Stream role has no CLI/environment setting for this policy.

## Configuration Surface

The library reuses the existing refined, validated
`crabka_client_core::ClientDnsTimeout`; it does not add another policy or
timeout newtype.

The following builders gain an optional typed input named
`broker_dns_timeout`, defaulting to `ClientDnsTimeout::default()`:

- `KafkaStreams::builder()`;
- `StreamsApp::builder()`;
- `StreamsMembership::builder()`.

Because the input has a builder default, existing callers remain source
compatible.

The observability demo adds:

- CLI: `--streams-broker-dns-timeout-ms`
- Environment: `CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS`
- Default: absent, resolved to `ClientDnsTimeout::default()` (10,000 ms)
- Validation: positive `u64` milliseconds
- Applicability: `--role stream` only

The demo parses the optional value as `std::num::NonZeroU64` and converts it
once to `ClientDnsTimeout`. No local validation wrapper or new dependency is
needed. CLI input takes precedence over environment input through Clap; an
absent value uses the typed default.

If the option is supplied for Produce or Consume, the process rejects it
before telemetry initialization, schema-registry access, DNS, or socket I/O.

There is no CRD field: the operator does not own or render a Client Streams
workload.

## Runtime Flow

```text
observability-demo CLI/environment or library builder
  -> ClientDnsTimeout
  -> StreamsApp
  -> KafkaStreams
     -> at-least-once or exactly-once broker I/O
        -> metadata Client
        -> bounded raw fetch lookup
        -> raw fetch ConnectionOptions
        -> Producer
        -> offset Client
     -> StreamsMembership
        -> join Client
        -> heartbeat Client
```

`StreamsApp::run_built` forwards its stored value to `KafkaStreams`.
`KafkaStreams::start` resolves no second default: it passes its one typed value
to `io_broker::build` or `io_broker::build_eos` and to
`StreamsMembership::builder()`.

Both broker-I/O constructors pass the value to:

- `Client::builder().dns_timeout(...)` for metadata;
- `Producer::builder().dns_timeout(...)`;
- `Client::builder().dns_timeout(...)` for offsets;
- the dedicated raw lookup;
- `ConnectionOptions::dns_timeout` for the resulting fetch connection.

`StreamsMembership::start` passes the same value to both its initial join
client and background coordinator/heartbeat client.

## Raw Lookup Boundary

`runtime/io_broker.rs` gains one private generic-future helper that resolves
the first address within a supplied `ClientDnsTimeout`. Both at-least-once and
exactly-once constructors call it.

This helper exists only to centralize the duplicated lookup and permit a
paused-clock test. It is not a resolver trait or public abstraction.

Behavior remains:

- the original bootstrap string is resolved;
- the first returned address is selected;
- resolver failures retain their underlying error;
- an empty result names the bootstrap address.

A deadline error names the bootstrap address and configured milliseconds.
The timeout applies only to DNS. Existing TCP-connect and request defaults are
unchanged.

## Validation and Errors

Library callers receive a `ClientDnsTimeout`, so invalid values cannot enter
the Streams runtime.

The demo trust boundary accepts only positive whole milliseconds via
`NonZeroU64`, then handles the `ClientDnsTimeout::new` result without an
unchecked conversion. Zero is rejected by Clap. Role misuse returns a
field-specific error before any external I/O.

The change does not alter:

- bootstrap ordering or first-address selection;
- at-least-once versus exactly-once behavior;
- producer idempotence or transactions;
- fetch isolation, wait, or byte limits;
- membership timing;
- TLS/SASL behavior;
- schema-registry DNS behavior.

## Verification

Focused tests cover:

- a pending raw lookup timing out at exactly 37 ms under Tokio's paused clock;
- raw resolver, empty-result, and deadline error context;
- the raw fetch `ConnectionOptions` carrying the supplied typed value;
- `StreamsApp` retaining the 10,000-ms default and a distinctive override;
- both at-least-once and exactly-once construction receiving the same typed
  value;
- membership join and heartbeat construction receiving the supplied value;
- demo zero rejection and Stream-only applicability;
- demo environment input, CLI-over-environment precedence, and typed default;
- the exact CLI help token appearing once.

Final gates run all targets for `crabka-client-streams` and
`observability-demo-app`, strict Clippy, formatting, and `git diff --check`.
The repository runtime-value scanner and a focused DNS search are recorded in
`docs/configuration-audit.md`.

## Non-Goals

- Separate DNS settings for metadata, fetch, producer, offsets, or membership.
- A resolver trait, shared runtime-policy struct, or new timeout type.
- Changes to client-core or producer DNS semantics.
- A foreign-server, file, or CRD configuration surface.
- Configuring unrelated demo Produce or Consume clients in this slice.
- Tuning Client Streams fetch, connect, request, polling, commit, cache, or
  membership values.

## Audit Continuation

After verification, the audit classifies every focused DNS match and records
the exact scanner totals. It names the next coherent unresolved owner from
current evidence without claiming the repository-wide goal is complete.
