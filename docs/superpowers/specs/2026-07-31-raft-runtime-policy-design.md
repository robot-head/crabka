# Raft Runtime Policy Configuration

## Scope

Expose four existing Raft runtime policies through the broker configuration
path while preserving current effective behavior:

| Policy | Existing effective default |
|---|---:|
| leader heartbeat cadence | election timeout divided by `3` |
| consecutive fetch-miss limit | `3` |
| command queue capacity | `256` |
| metadata Raft fetch maximum | `8MiB` |

The existing broker heartbeat option currently reaches `ControllerConfig` but
is dropped before the Raft engine. Omission must continue deriving the cadence
from the election timeout. An explicit heartbeat value must reach the engine
unchanged.

## Configuration Surface

The broker owns these CLI and environment pairs:

| CLI | Environment |
|---|---|
| existing `--controller-heartbeat-interval` | `CRABKA_CONTROLLER_HEARTBEAT_INTERVAL` |
| `--controller-fetch-miss-limit` | `CRABKA_CONTROLLER_FETCH_MISS_LIMIT` |
| `--metadata-raft-command-queue-capacity` | `CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY` |
| `--metadata-raft-fetch-max` | `CRABKA_METADATA_RAFT_FETCH_MAX` |

Runtime TOML uses matching snake-case keys. `Kafka.spec.brokerTuning` gains:

- `controllerFetchMissLimit`;
- `metadataRaftCommandQueueCapacity`; and
- `metadataRaftFetchMax`.

The existing `controllerHeartbeatInterval` field remains the heartbeat owner.
Omitted CRD fields render no runtime TOML entry.

## Types and Validation

The fetch-miss limit and command queue capacity are positive
`refined_type`-validated counts. Their defaults remain `3` and `256`.

The metadata Raft fetch maximum remains a UOM `ByteSize`. It must be positive,
contain a whole number of bytes, and fit the signed `i32` request field. Its
default remains `8MiB`.

An explicit controller heartbeat interval remains a positive UOM `Time` below
the controller election timeout. Configuration resolution retains whether the
value was explicitly supplied, including an explicit `500ms` equal to the
documented broker default.

No option is added for the fixed heartbeat divisor, snapshot maximum,
protocol fields, or internal timer mechanics.

## Runtime Flow

```text
CLI / environment / runtime TOML / Kafka CRD
  -> BrokerConfig validation
  -> ControllerConfig
  -> KraftConfig
  -> KraftController / Engine
```

`BrokerConfig` retains heartbeat explicitness. Broker-owned loops continue
using the existing resolved heartbeat value. The Raft boundary receives
`Option<Time>`:

- `None` derives election timeout divided by `3`;
- `Some(value)` uses the explicit value.

The fetch-miss limit replaces the fixed consecutive-miss comparison. The
queue capacity replaces the fixed Tokio command-channel capacity.

The metadata Raft fetch maximum replaces the coupled `8MiB` decoded-log read
constant for replication serving, committed-image application, and restart
replay. Replication serving performs one bounded read per response.
Committed-image application and restart replay repeat bounded reads until
their target offset, so lowering the budget cannot skip committed metadata.
Snapshot fetch keeps its separate existing maximum.

## Errors

Invalid values fail through existing broker configuration or Raft startup
errors before the service starts. Values are neither silently clamped nor
replaced with recovery fallbacks.

## Verification

Tests cover:

- defaults and explicit overrides through CLI, environment, runtime TOML, and
  the Kafka CRD;
- rejection of zero, fractional-byte, overflow, and invalid cross-field
  values;
- derived cadence for omission and exact cadence for explicit heartbeat;
- fetch-miss threshold and command queue capacity propagation;
- bounded replication responses;
- multi-chunk committed application and restart replay without skipped
  records;
- omitted CRD fields rendering no new configuration;
- generated CRD schemas and unchanged defaults.

Closure requires focused Raft, broker, and operator tests; generated-schema
verification; workspace all-target check and strict Clippy; nightly
formatting; and diff hygiene.
