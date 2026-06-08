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

## Mixed JVM + Crabka tiered-storage metadata topics are unsupported

Tiered storage uses the topic-backed `RemoteLogMetadataManager` and the
`__remote_log_metadata` internal topic when enabled, but Crabka's remote-log
metadata record format is not byte-compatible with the JVM
`RemoteLogMetadataSerde`. A real cluster should run one RLMM implementation for
that internal topic.

Status: intentional limitation. Crabka broker interoperability is validated at
the Kafka data/control protocol boundary, not by mixing RLMM implementations on
one metadata topic.

## ZooKeeper mode and ZooKeeper-to-KRaft migration are out of scope

Crabka is KRaft-only. ZooKeeper-backed broker mode and ZK-to-KRaft migration
support are deliberate non-goals.

Status: intentional non-goal.

## Captured-traffic corpus deviation from coverage acceptance criterion #9

The coverage meta-spec
(`docs/superpowers/specs/2026-05-11-crabka-protocol-coverage-design.md`)
acceptance criterion #9 requires a captured-traffic corpus entry per
`(api_key, version)` pair. Sub-plan 1d explicitly does not build the
corpus. Differential testing (default-fixture per pair on PR CI;
256 proptest per pair nightly) is the substitute.

Rationale: building ~1000 corpus entries via real broker captures
(high setup cost) or oracle-synthetic generation (which proves
nothing differential testing doesn't) is not worth the work for the
validation value it adds. The corpus remains useful for regression
reproduction; growth is deferred to a future maintenance task.

Status: open. Tracked here pending a future maintenance pass.
