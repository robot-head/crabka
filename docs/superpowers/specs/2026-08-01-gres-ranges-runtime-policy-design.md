# Gres Ranges Runtime Policy Design

## Goal

Expose the deployment-owned limits and pacing currently embedded in
`crabka-gres-ranges`, preserving all defaults and keeping protocol, format,
sentinel, and derived values fixed.

## Configuration ownership

`crabka-gres-ranges` owns one validated `RangeRuntimePolicy`. It contains UOM
`Time` and `ByteSize` values plus positive refined count/stride newtypes. It
does not depend on Clap or Kubernetes.

The existing `crabka-gres` `ServeArgs` surface accepts optional flags backed by
`CRABKA_GRES_RANGE_*` environment variables. `SubstrateRuntimeConfig` resolves
omissions to `RangeRuntimePolicy::default()` and carries the policy to the
existing tenant, transport, forwarder, barrier, and timestamp-oracle owners.

`GresComputeSpec` exposes matching optional unit-bearing fields. Its existing
effective-policy validation emits the corresponding CLI arguments; no new
configuration subtree or environment reader is needed.

## Policy

| Setting | Default |
|---|---:|
| RPC frame maximum | `1MiB` |
| RPC request timeout | `5s` |
| RPC server idle timeout | `1m` |
| RPC pool idle TTL | `5s` |
| RPC pool idle connections per endpoint | `32` |
| hosted remote-session idle retention | `1m` |
| hosted remote-session maximum | `1024` |
| range-0 wait timeout | `10s` |
| range-0 barrier reply budget | `4s` |
| cross-range lock-wait cap | `2s` |
| durable-inspection records | `4096` |
| durable-inspection bytes | `128KiB` |
| decision-release lag retries | `10` |
| decision-release retry backoff | `200ms` |
| timestamp-oracle heartbeat | `10ms` |
| logical persistence minimum interval | `100ms` |
| logical persistence base stride | `1024` |
| logical persistence maximum stride | `16777216` |
| HLC horizon headroom | `128ms` |

The SQL chunk target remains derived from the configured frame maximum and its
fixed encoding envelope. Internal topic names, range identifiers, format/wire
versions, hash constants, token widths, and timestamp encodings remain fixed.

## Validation

- All times and sizes are positive and finite.
- Counts and strides are positive refined values.
- Pool idle TTL is below server idle timeout.
- Barrier reply and lock-wait budgets are below the RPC request timeout.
- Logical base stride does not exceed logical maximum stride.
- The RPC frame maximum is larger than the fixed frame/SQL encoding envelope.
- Whole-number protocol boundaries are validated before conversion.

## Compatibility

Existing public constructors remain and delegate with the default policy.
Policy-aware constructors are added only at shared ownership seams. Tests and
external callers therefore retain current behavior without duplicating
defaults.
