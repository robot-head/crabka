# Replicator Runtime and Topic Policy Design

## Goal

Replace the replicator's production timing, retry, batching, and topic-creation
literals with validated process configuration while preserving existing
defaults and source topic topology.

## Configuration boundary

Add one `ReplicatorRuntimePolicy` flattened into the standalone binary's
existing Clap CLI. Every flag is backed by a `CRABKA_REPLICATOR_*` environment
variable, so Kubernetes can override the process without adding runtime policy
to the replicator's workload-definition YAML.

The policy owns these independent values:

| Setting | Default |
|---|---:|
| topic creation timeout | `10s` |
| source poll timeout | `500ms` |
| internal drain poll timeout | `500ms` |
| consecutive empty drain polls | `3` |
| worker build retry budget | `30s` |
| worker build initial backoff | `250ms` |
| worker build maximum backoff | `8s` |
| connect commit interval | `500ms` |
| connect maximum batch records | `500` |
| supervisor interval | `3s` |
| heartbeat interval | `1s` |
| checkpoint interval | `5s` |
| Kafka client DNS timeout | existing client default |
| Kafka client connect timeout | `5s` |
| Kafka client request timeout | `30s` |
| replicated data-topic replication factor | `1` |
| internal-topic replication factor | `1` |

Times are positive UOM `Time` values. Counts and batch records are positive
`NonZeroUsize` values. Kafka replication factors use a `refined_type`-validated
positive `i16` wrapper. Validation requires initial retry backoff not to exceed
its maximum and the retry budget not to be below the initial backoff.

The equal `500ms` defaults remain separate because source polling, internal
draining, and connect commits have different operational meaning.

## Runtime flow

Thread the policy through `FlowSupervisor`, its restart specifications,
`FlowWorkerParams`, `SourceConsumer`, `TargetSink`, checkpoint storage,
heartbeat/checkpoint tasks, and shared admin helpers. Existing convenience
entry points remain and delegate with the compatible default policy.

All Kafka admin connections created by the replicator use the policy's shared
DNS/connect/request timeouts. Topic creation uses the configured timeout and
the appropriate data or internal replication factor. Internal topics retain
one partition because their ordering and compatibility semantics require it.

## Source topology

The supervisor already receives partition metadata but currently discards it.
Retain each selected source topic's partition count, subscribe the source
consumer by name, and give the sink the topic-to-partition-count map. On first
target-topic creation, the sink uses the source count and configured data-topic
replication factor. Produced records continue to specify their original
partition, so this removes the current one-partition creation mismatch.

Partition count is not tunable: source topology is authoritative. Existing
target topics still rely on Kafka's already-exists behavior; this slice does
not invent automatic partition mutation.

## Compatibility and scope

- Every default matches current behavior.
- Replicator YAML remains cluster/flow/policy input only.
- MM2 topic names, Kafka error codes, record versions, provenance headers, and
  internal one-partition layouts remain fixed protocol semantics.
- Test-only polling and settle waits remain test controls.
- No CRD owns this standalone service, so no CRD field is added.

## Verification

Tests cover defaults, CLI-over-environment precedence, invalid scalar and
relational values, policy flow into each runtime owner, topic creation timeout
and replication factors, source partition-count preservation, and original
record partition production. Closure runs replicator all-target tests,
workspace check, strict Clippy, nightly formatting, and diff hygiene.
