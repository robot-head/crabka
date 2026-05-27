# Slice 45 — JBOD / multi-log-dir + DescribeLogDirs (KIP-113) — Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 45 — Crabka core: JBOD / multi-log-dir + DescribeLogDirs (KIP-113) (2026-05-23)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- AlterReplicaLogDirs move + future-log catch-up (closed by 2cba97a "Intra-broker log-dir reassignment (AlterReplicaLogDirs, KIP-113)")
- total_bytes / usable_bytes via statvfs
- kafka-reassign-partitions per-replica log_dirs
- Offline-dir / KAFKA_STORAGE_ERROR handling
- Operator JBOD surface is slice 46

---

Design: `docs/superpowers/specs/2026-05-23-crabka-jbod-multi-log-dir-45-design.md`

Scope: read + placement half of KIP-113. Intra-broker move
(`AlterReplicaLogDirs`) deferred to 45b. No protocol regeneration — the
`DescribeLogDirs` / `AlterReplicaLogDirs` types are already generated from
the Kafka 4.3.0 schemas.

## Task layout

### Batch 1 — config + placement primitives (independent files)

- **T1** `config.rs` + `bin/broker.rs` + `file_config.rs`: add
  `extra_log_dirs` field, `all_log_dirs()`, `--log-dirs` CLI
  (`CRABKA_EXTRA_LOG_DIRS`), TOML key + merge.
- **T2** `log_dir.rs`: `count_partitions`, `place_partition_dir`,
  `scan_all` + unit tests.

### Batch 2 — thread placement through materialization (depends on B1)

- **T3** `broker.rs` startup loop (`scan_all`), supervisor + disk-scanner
  wiring (`all_log_dirs()`).
- **T4** `replicator_supervisor.rs` (`materialize_partition(&[PathBuf])`,
  `log_dirs` field) + `replicator.rs` (`Config.log_dirs`,
  `ensure_local_partition`).
- **T5** `coordinator/bootstrap.rs` (offsets placement), `disk_scanner`
  (scan all dirs), handlers `create_topics` / `create_partitions` /
  `init_producer_id` / `delete_topics` (`all_log_dirs()`).

### Batch 3 — DescribeLogDirs (depends on B1)

- **T6** `handlers/describe_log_dirs.rs` (new) + `handlers/mod.rs` register
  (api 35) + `api_versions.rs` advertise + `network/dispatch.rs`
  `handler_body_flexible` arm.

### Batch 4 — tests (depends on B2 + B3)

- **T7** `tests/jbod.rs` (new) — placement spread + wire `DescribeLogDirs`.
- **T8** `jvm_acceptance.rs` — `kafka-log-dirs --describe` (`#[ignore]`).

## Acceptance

1. `cargo test -p crabka-broker` green (lib + integration, excl. ignored).
2. `cargo clippy --workspace --all-targets -D warnings` + `cargo fmt --check`.
3. New partitions spread across configured dirs by least-loaded placement;
   `__cluster_metadata` stays on the primary dir.
4. `kafka-log-dirs --describe` reports every configured dir with the
   partitions physically present (JVM acceptance, `--include-ignored`).
