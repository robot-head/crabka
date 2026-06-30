# Known issues

This file tracks deliberate deviations, beta caveats, and intentionally deferred
work that should not be rediscovered as surprising regressions. The feature
matrix in [README.md](README.md#feature-compatibility) remains the full status
source of truth.

## Beta / production-hardening caveat

Crabka is still pre-1.0 and has no production users or on-disk compatibility
guarantees. The Kafka wire protocol is treated as the compatibility contract;
internal metadata, local raft logs, and implementation-specific storage can
still change without migration shims while the project is greenfield.

Status: open until the project reaches a production-hardened 1.0 line.

## Kafka Streams client is not yet a full JVM replacement

`crabka-client-streams` implements the KIP-1071 streams membership path and a
Rust DSL/runtime for common stream-processing workloads, including state stores,
joins, windows, suppression, global tables, punctuators, interactive queries,
and exactly-once processing. It is still not a complete replacement for the JVM
Kafka Streams client library and should be treated as partial in the top-level
feature matrix.

Status: open. Track missing JVM Streams parity here rather than claiming the
entire Kafka Streams surface is complete.

## Kafka Connect and MirrorMaker equivalents are not implemented

The workspace includes a Schema Registry-compatible service and a gRPC /
Connect-RPC gateway, but it does not yet include Kafka Connect, MirrorMaker, or
their operator CRDs.

Status: open. These remain ecosystem gaps outside the broker core.

## Tiered-storage segment-data interop is narrower than metadata interop

Crabka's `__remote_log_metadata` records are now byte-exact with the JVM
`RemoteLogMetadataSerde`: the topic-backed `RemoteLogMetadataManager` serializes
through the same `AbstractApiMessageSerde` value envelope (frameVersion=1 +
apiKey + apiVersion, flexible message bodies) and the same
`RemoteLogSegmentMetadataRecord` / `RemoteLogSegmentMetadataUpdateRecord` /
`RemotePartitionDeleteMetadataRecord` schemas. This is verified byte-for-byte
against `mirror.gcr.io/apache/kafka:4.0.0` golden vectors
(`crates/remote-storage-topic/tests/jvm_serde_golden.rs`), so a mixed JVM +
Crabka cluster can share the internal metadata topic.

Sharing the *segment data* tier additionally requires both brokers to use the
same `RemoteStorageManager` object layout and producer-snapshot conventions.
Crabka's `RemoteStorageManager` path scheme, and its (currently absent)
producer-snapshot upload, are not yet validated against the JVM
`LocalTieredStorageManager`, so full segment-level interop in a mixed cluster is
not claimed.

Status: the metadata-topic record-format incompatibility is resolved. Open:
segment-data (RSM layout + producer-snapshot) interop validation.

## ZooKeeper mode and ZooKeeper-to-KRaft migration are out of scope

Crabka is KRaft-only. ZooKeeper-backed broker mode and ZK-to-KRaft migration
support are deliberate non-goals.

Status: intentional non-goal.
