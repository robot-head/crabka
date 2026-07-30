# Pprof Debuginfod Resource Policy

## Scope

Expose the three deployment policies used by the optional debuginfod client:
downloaded-artifact size, connection timeout, and whole-request timeout.
Preserve the current 512-MiB, five-second, and ten-second defaults and keep
existing public constructors source-compatible.

The URL path algorithm, redirect prohibition, build-ID validation, object
parser guard, and capped streaming read remain fixed security behavior.

## Validated Configuration

`crabka-pprof` owns `DebuginfodConfig`:

```text
max_artifact_size: ByteSize
connect_timeout: Time
request_timeout: Time
```

The artifact size must be a positive whole-byte UOM value exactly
representable by the repository's `f64` quantities. Scalar positivity is
validated with `refined_type`. Both timeouts must be positive finite UOM
values, and the connect timeout must not exceed the whole-request timeout.

`DebuginfodResolver::new` remains default-backed.
`DebuginfodResolver::with_config` accepts explicit validated configuration.

## Profiles Propagation

The querier, query-frontend, and symbolizer roles all construct debuginfod
resolvers. Existing `crabka-profiles` helpers remain default-backed wrappers;
config-aware variants carry one `DebuginfodConfig` through each live path.

The binary validates the effective configuration once before role dispatch and
passes it to the applicable role.

## Deployment Configuration

The standalone Profiles binary exposes optional UOM overrides:

| CLI | Environment |
|---|---|
| `--debuginfod-max-artifact-size` | `CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE` |
| `--debuginfod-connect-timeout` | `CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT` |
| `--debuginfod-request-timeout` | `CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT` |

Absent overrides use `DebuginfodConfig::default`, avoiding duplicate default
literals at the CLI boundary.

The existing comma-delimited `--debuginfod-url` option gains
`CRABKA_PROFILES_DEBUGINFOD_URLS` backing. An empty URL list continues to
disable all debuginfod network access.

No CRD or Helm chart owns the standalone Profiles service.

## Verification

- configuration tests cover defaults, custom values, zero/non-finite values,
  fractional bytes, and connect/request ordering;
- resolver tests prove the explicit artifact cap reaches the capped read path;
- Profiles helper tests cover default and explicit configuration propagation;
- binary tests cover CLI defaults, overrides, environment values, URL
  environment splitting, and invalid relations;
- focused pprof/Profiles tests, strict workspace Clippy, nightly formatting,
  scanner count, and diff hygiene pass.
