# Kafka Client Resource Policy CRD Propagation Design

## Goal

Expose the already-implemented Kafka client dispatch-queue, frame-maximum, and
approved isolated-fetch-minimum settings through every existing CRD that owns
one of the affected deployed processes.

## Scope

This slice covers only operator-rendered workloads:

- broker pods owned by `KafkaNodePool`;
- the Gres registry process owned by `Kafka.spec.gresRegistry`;
- Gres compute processes owned by `Gres.spec.compute`;
- gateway pods owned by `KafkaGrpcGateway`; and
- registry pods owned by `SchemaRegistry`.

Standalone processes without an owning CRD keep their existing CLI and
environment surfaces. The operator's own cached admin clients remain
process-configured because one operator instance serves many clusters.

## CRD Shape

Add optional fields directly to each owner's existing policy structure. Do not
introduce a shared flattened CRD type.

| Owner | Fields |
|---|---|
| `KafkaNodePool.spec` | `clientDispatchQueueCapacity`, `clientFrameMax` |
| `Kafka.spec.gresRegistry` | `clientDispatchQueueCapacity`, `clientFrameMax`, `readerFetchMin` |
| `Gres.spec.compute` | `clientDispatchQueueCapacity`, `clientFrameMax`, `fdwFetchMin`, `walRecoveryFetchMin` |
| `KafkaGrpcGateway.spec.tuning` | `clientDispatchQueueCapacity`, `clientFrameMax` |
| `SchemaRegistry.spec.runtime` | `clientDispatchQueueCapacity`, `clientFrameMax` |

Every field is optional and omitted from serialization when absent. Queue
capacity is dimensionless and represented as `Option<usize>` with a schema
minimum of one. Frame and fetch values are `Option<ByteSize>`, use the existing
human UOM serializer, and appear as strings in generated OpenAPI schemas.

Omission preserves the process's current binary default. Setting only one
member of the queue/frame pair overrides only that member; the other retains
its binary default.

## Validation

CRD values are validated before workload rendering:

- queue capacity passes through
  `ConnectionDispatchQueueCapacity`;
- frame maximum passes through `ClientFrameMax`;
- fetch minima pass through `FetchMinBytes`.

These existing `refined_type`-backed newtypes remain the single validation
authority. The operator adds field-qualified context such as
`spec.tuning.clientFrameMax` to validation errors. It does not duplicate
numeric ceilings or add another validation abstraction.

The fixed `100MiB` client frame security ceiling remains non-configurable.
Dimensioned inputs must be positive whole-byte values.

## Rendering

Controllers append the existing process flags only when the corresponding CRD
field is present:

- `--client-dispatch-queue-capacity`;
- `--client-frame-max`;
- `--registry-reader-fetch-min` for the Gres registry;
- `--fdw-fetch-min` for Gres FDW-owning compute roles; and
- `--wal-recovery-fetch-min` for Gres WAL-recovery-owning compute roles.

Byte values are rendered with an explicit `B` suffix. Queue values use base-10
integers. Existing shared argument builders remain the single render point for
single- and multi-range Gres workloads.

Role-specific Gres values are never rendered into a process mode that rejects
them. A configured field that has no consuming compute role is rejected during
effective-policy validation rather than silently ignored.

## Compatibility and Failure Behavior

Existing CRs remain valid because every new field is optional. An omitted field
does not add a container argument and therefore preserves the deployed
binary's current default behavior.

Invalid configured values fail reconciliation before a workload is created or
updated. Errors name the CRD path and retain the underlying validated-newtype
reason. No environment variables are injected by the operator for these
fields; the CRD-to-CLI path is explicit and testable.

## Verification

Each owner requires focused tests proving:

- omitted fields deserialize and render no new flags;
- non-default values round-trip through serde and OpenAPI schema generation;
- zero queue, zero/fractional frame, over-ceiling frame, and zero fetch
  minima are rejected before rendering;
- each configured value appears exactly once in the rendered container;
- Gres role restrictions and both single- and multi-range paths are correct;
  and
- generated CRDs are deterministic and match `deploy/crds`.

After focused package tests, run workspace all-target check and strict Clippy,
nightly formatting, diff hygiene, and the operator CRD regeneration
comparison. Preserve the four unrelated untracked plans dated 2026-07-28.

## Out of Scope

This slice does not add a generic CRD client-policy abstraction, change public
library defaults, configure the fixed security ceiling, or close the broader
repository-wide hardcoded operational-value audit.
