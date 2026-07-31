# Metrics Distributor Policy Configuration

## Scope

Expose three existing distributor deployment policies through the standalone
metrics service while preserving current effective behavior:

| Policy | Existing effective default |
|---|---:|
| HA replica failover timeout | `30s` |
| ingestion-rate tenant bucket cap | `100000` |
| decompressed distributor request cap | `32MiB` |

The metrics service is not owned by a CRD, so these policies belong to its
CLI and environment surface. Library callers that construct
`DistributorState` or `IngestEnforcer` without overrides retain the same
defaults.

## Configuration Surface

The `crabka-metrics` binary adds:

| CLI | Environment | Default |
|---|---|---:|
| `--ha-failover-timeout` | `CRABKA_METRICS_HA_FAILOVER_TIMEOUT` | `30s` |
| `--ingest-rate-bucket-cap` | `CRABKA_METRICS_INGEST_RATE_BUCKET_CAP` | `100000` |
| `--distributor-max-decompressed` | `CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED` | `32MiB` |

All three options are accepted for every target, matching the binary's
existing flat CLI. Only the distributor consumes them.

## Types and Validation

The HA failover timeout remains a UOM `Time`. Existing semantics are
preserved:

- a negative value disables takeover;
- zero permits immediate takeover; and
- a positive value sets the elected-replica lease.

The ingestion-rate bucket cap is a positive `usize` validated with a
`refined_type` newtype at the CLI boundary. Zero is rejected instead of
relying on the library constructor's existing clamp.

The decompressed request cap remains a positive UOM `ByteSize`. It must
contain a whole number of bytes representable by the request processing
boundary. Invalid values fail during CLI parsing.

## Runtime Flow

```text
CLI / environment
  -> crabka-metrics distributor startup
  -> DistributorState
       -> HaTracker election timeout
       -> IngestEnforcer tenant bucket cap
       -> request decompression cap
```

`DistributorState` stores the configured failover timeout beside its existing
decompression cap. Its construction path selects the configured
`IngestEnforcer`. The HA decision path supplies the stored timeout to the
existing `HaTracker::elect` operation using the current wall clock.

No policy aggregate, disable flag, new dependency, runtime file format, or CRD
field is introduced.

## Errors

Invalid CLI or environment values fail before the service starts. Values are
not silently replaced, except that the existing library-only
`IngestEnforcer::with_max_rate_buckets(0)` behavior remains unchanged for API
compatibility.

## Verification

Tests cover:

- unchanged CLI defaults;
- CLI and environment overrides for all three policies;
- rejection of a zero bucket cap and invalid decompression caps;
- preservation of negative, zero, and positive HA timeout semantics;
- propagation from distributor startup into HA election, rate-bucket
  eviction, and decompression enforcement; and
- unchanged library defaults.

Closure requires focused metrics library and binary tests, workspace
all-target checking and strict Clippy, nightly formatting, and diff hygiene.
