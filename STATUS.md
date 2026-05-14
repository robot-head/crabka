# Crabka Protocol Foundation — Acceptance Gate Status

Generated: 2026-05-10

## Branch summary

- **Branch:** main
- **Total commits:** 29
- **Head commit:** a3bf25d ci: rust + clippy + fmt matrix and JVM differential job

## Test inventory

| Category       | Tests | Notes                                                                 |
|----------------|-------|-----------------------------------------------------------------------|
| Unit           | 33    | Primitives, error, codec, owned/borrowed message types, tagged fields |
| Proptest       | 3     | ApiVersionsRequest v3, ApiVersionsResponse v0 + v3 round-trips        |
| Differential   | 5     | 4 JVM byte-equality / decode-parity tests + oracle smoke test         |
| Corpus replay  | 1     | `corpus_round_trips` covering 1 hex entry                             |
| Snapshot       | 3     | Owned + borrowed codegen snapshots, snapshot_compiles smoke           |
| **Total**      | **45**|                                                                       |

## Acceptance gate — PASS

| # | Check                                         | Result |
|---|-----------------------------------------------|--------|
| 1 | `cargo fmt --check`                           | PASS   |
| 2 | `cargo clippy --workspace --all-targets`      | PASS   |
| 3 | Default tests (`cargo test --workspace`)      | PASS   |
| 4 | With JVM oracle (`--include-ignored`)         | PASS   |
| 5 | No drift (`regenerate.sh` + `git diff`)       | PASS   |
| 6 | ApiVersionsRequest v0 + v3 byte-equal JVM     | PASS   |
| 7 | ApiVersionsResponse v3 byte-equal + decode    | PASS   |
| 8 | Borrowed flavor exercised                     | PASS   |
| 9 | Corpus replay green (1 entry)                 | PASS   |
|10 | CI matrix ubuntu/macos/windows in ci.yml      | PASS   |
|11 | CONTRIBUTING.md: regenerate, oracle, version  | PASS   |

**Overall: PASS — all 11 acceptance items green.**

## Next step

Extend codegen and tests to the remaining ~99 Kafka message types via the
follow-up `crabka-protocol-coverage` plan.  The protocol-foundation
infrastructure (codegen pipeline, JVM oracle, differential harness, corpus
replay, CI matrix) is fully in place and proven correct against a live JVM
Kafka client for ApiVersions.

## Slice 11 — admin handlers (2026-05-14)

- 8 new handlers: AlterConfigs (33), IncrementalAlterConfigs (44),
  CreatePartitions (37), DeleteRecords (21), DescribeCluster (60),
  ListGroups (16), DescribeGroups (15), DeleteGroups (42).
- 1 new metadata record: V1TopicConfig.
- Topic-config whitelist with live propagation to `Log.config` via
  `Arc<RwLock<LogConfig>>` and a supervisor reconcile push.
- 5 new JVM acceptance tests covering `kafka-configs --alter`,
  `kafka-topics --alter --partitions`, `kafka-delete-records`,
  `kafka-consumer-groups --list/--describe`, `kafka-cluster cluster-id`.
- Side fixes: DescribeConfigs now projects topic_configs (not stub);
  OffsetFetch honors `topics: None` "fetch all" sentinel; flexible-body
  dispatch table + ApiVersions response register all slice-11 api_keys.
- Out of scope: Rust CLI, ACLs, quotas, partition reassignments,
  ElectLeaders, log compaction, broker-side recompression.
