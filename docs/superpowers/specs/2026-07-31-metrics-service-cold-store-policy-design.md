# Metrics Service Cold-Store Policy Configuration

## Scope

Expose two existing cold-store runtime policies through the standalone metrics
service while preserving current effective behavior:

| Policy | Existing effective default |
|---|---:|
| cold-manifest cache TTL | `30s` |
| unbounded compatibility-query lookback | `1h` |

The settings apply to querier, query-frontend, and ruler instances. The
metrics service is not owned by a CRD, so both policies belong to its CLI and
environment surface. Library callers that do not configure either value keep
the existing defaults.

## Configuration Surface

The `crabka-metrics-service` binary adds:

| CLI | Environment | Default |
|---|---|---:|
| `--cold-cache-ttl` | `CRABKA_METRICS_COLD_CACHE_TTL` | `30s` |
| `--unbounded-compatibility-lookback` | `CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK` | `1h` |

Both options remain on the binary's existing flat CLI and are accepted for
every target because every target constructs a `RefreshingMetricBlockStore`.

## Types and Validation

Both settings remain UOM `Time` values from parsing through the cold-store
boundary. They must be finite and strictly positive. Zero and negative values
fail during CLI or environment parsing.

The named library defaults remain `30s` and `1h`. No raw millisecond fields,
dimensionless duration newtypes, or alternate disable sentinel are added.

## Runtime Flow

```text
CLI / environment
  -> crabka-metrics-service target startup
  -> RefreshingMetricBlockStore
       -> cached cold-store freshness check
       -> unbounded compatibility-range normalization
```

`RefreshingMetricBlockStore::new` keeps its current signature and initializes
both policy fields from the named defaults. Two direct builder setters apply
configured values without breaking library callers. Querier,
query-frontend, and ruler startup call both setters.

The cache TTL replaces both uses of the fixed cache-freshness value before and
after the existing singleflight refresh lock. The compatibility lookback is
passed to range normalization and is used only for the exact
`i64::MIN..i64::MAX` sentinel range. Explicit query ranges remain unchanged.

No policy aggregate, new dependency, runtime file format, or CRD field is
introduced.

## Errors

Invalid CLI or environment values fail before the service starts. Values are
not clamped, silently replaced, or interpreted as cache-disable sentinels.

## Verification

Tests cover:

- unchanged CLI and library defaults;
- CLI and environment overrides with CLI precedence;
- rejection of zero and negative values for both settings;
- configured cache freshness and expiry behavior;
- configured normalization of the unbounded compatibility range;
- unchanged explicit query ranges; and
- propagation into querier, query-frontend, and ruler construction.

Closure requires all metrics-service targets, workspace all-target checking,
strict Clippy, nightly formatting, and diff hygiene.
