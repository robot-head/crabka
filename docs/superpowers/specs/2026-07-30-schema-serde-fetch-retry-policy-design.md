# Schema Serde Fetch Retry Policy Design

## Goal

Expose the background writer-schema fetch retry range while preserving the
schema cache's current behavior and fixed wire/algorithm contracts.

The configurable policy is limited to:

| Value | Default |
| --- | --- |
| initial retry backoff | `10ms` |
| maximum retry backoff | `1s` |

The Confluent media type and magic byte, reference traversal ceiling,
exponential growth, exponent cap, and deterministic zero-to-25-percent jitter
remain fixed.

## Validated Library Policy

Add `SchemaFetchRetryPolicy` to `crabka-schema-serde`:

```rust
pub struct SchemaFetchRetryPolicy {
    initial_backoff: Time,
    max_backoff: Time,
}
```

`SchemaFetchRetryPolicy::new` accepts UOM `Time` values and rejects:

- zero, negative, non-finite, or `std::time::Duration`-unrepresentable values;
- an initial backoff greater than the maximum.

Equal bounds are valid. `Default` preserves `10ms` and `1s`. Read-only
accessors expose both values.

`CacheConfig` gains one public `fetch_retry_policy:
SchemaFetchRetryPolicy` field. Because the policy's fields are private, an
invalid retry range cannot be placed in a cache configuration.
`SchemaCache::new` remains infallible and source-compatible for callers using
`CacheConfig::default()`.

The retry-delay helper receives the validated policy. It uses the configured
initial and maximum values while preserving the existing exponent cap,
deterministic jitter calculation, monotonic delay, and maximum clamp.

No environment lookup occurs inside `crabka-schema-serde`.

## Owning Configuration Surfaces

### Observability demo application

Add UOM Clap options backed by:

- `CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF`;
- `CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF`.

The CLI names are:

- `--schema-fetch-retry-initial-backoff`;
- `--schema-fetch-retry-max-backoff`.

Startup constructs one validated policy and uses it for producer, consumer,
and Client Streams schema caches. Omitted values preserve the library
defaults.

### Client Streams

No duplicate Client Streams fields are added. `StreamsApp::builder` already
accepts `CacheConfig`; callers that own deployment configuration pass the
validated policy through that existing boundary.

### Gres FDW

Add the same two UOM options to the Gres compute CLI, backed by:

- `CRABKA_GRES_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF`;
- `CRABKA_GRES_SCHEMA_FETCH_RETRY_MAX_BACKOFF`.

Add optional camelCase UOM fields to `GresComputeSpec`:

- `schemaFetchRetryInitialBackoff`;
- `schemaFetchRetryMaxBackoff`.

The operator validates the effective pair and renders both Gres flags.
`KafkaFdw` stores the validated policy and applies it to every per-scan
`CacheConfig`. Existing `KafkaFdw` constructors remain default-backed; one
chainable policy setter carries custom configuration from Gres.

No Kafka CRD field is added because this policy belongs to schema consumers,
not brokers or the Schema Registry service.

## Error Handling

Invalid CLI or environment text is rejected by the existing UOM parser.
Positive-time and ordering errors are returned before role startup.
Invalid Gres CRD values produce the existing validation/status failure before
workload rendering.

Runtime registry failures retain their current classification:

- transient failures schedule another background fetch using the configured
  range;
- terminal not-found or malformed responses remain cached as unavailable;
- successful fetches clear retry state.

## Testing

The minimum behavioral coverage is:

1. library defaults, equal custom bounds, invalid times, and inverted ranges;
2. retry-delay propagation, deterministic jitter, monotonicity, and maximum
   clamping under custom bounds;
3. unchanged terminal/transient cache behavior;
4. observability demo CLI, environment, precedence, and all-role propagation;
5. Client Streams propagation through its existing `CacheConfig` argument;
6. Gres CLI/environment, FDW propagation, CRD schema/validation, and rendered
   arguments;
7. affected all-target suites, strict workspace Clippy, nightly formatting,
   scanner evidence, and diff hygiene.

No new dependency, generic retry framework, or network integration harness is
needed.
