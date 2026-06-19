# Slice 3, Tasks 4+5 — Audit Spool Config, Metrics, Recovery & Restart Test

## Summary

Tasks 4 and 5 were merged and implemented together. The broker crate was also non-compiling on entry because Task 3 had changed `AuditWriter::new` to take `AuditWriterParams` but `broker.rs` still used the old 6-positional-argument API; that wiring block was rewritten as part of this task.

## Changes

### `crates/broker/src/file_config.rs`
- Added `FileAuditSpoolConfig { dir: String, max_bytes: u64 }` with defaults `"audit-spool"` / 1 GiB.
- Added `spool: Option<FileAuditSpoolConfig>` to `FileAuditConfig`.
- `apply_to()` now writes `cfg.audit_spool_dir` and `cfg.audit_spool_max_bytes`.
- Unit test: `audit_spool_parses_and_defaults`.

### `crates/broker/src/config.rs`
- Added `audit_spool_dir: PathBuf` and `audit_spool_max_bytes: u64` to `BrokerConfig`.
- Both `Default` and `for_tests()` initialize to `"audit-spool"` / 1 GiB.

### `crates/broker/src/metrics.rs`
- Added 5 fields to `BrokerMetrics`: `audit_spool_depth` (Gauge), `audit_spool_bytes` (Gauge), `audit_records_spooled_total` (Counter), `audit_records_replayed_total` (Counter), `audit_records_dropped_total` (Counter).
- All 5 registered with prometheus-client and returned from `new()`.
- Unit test: `audit_spool_metrics_present`.

### `crates/broker/src/audit_recovery.rs` (new)
- `pub(crate) fn recover_from_partition_tail(partition: &Partition) -> Option<(u64, [u8; 32])>`
- Reads the last ≤256 offsets of the audit partition, skips checkpoint records, walks all chained records, and returns `(next_seq, chain_hash)` for the most-recent one.
- Uses `crabka_audit::chain::{chain_hash, from_hex32}` and the `HEADER_SEQ` / `HEADER_PREV_HASH` / `EVENT_CLASS_CHECKPOINT` constants from `crabka_audit`.

### `crates/broker/src/lib.rs`
- Added `pub(crate) mod audit_recovery;`.

### `crates/broker/src/broker.rs`
- Replaced old positional `AuditWriter::new(rx, sink, product, signer, n, every)` call with `AuditWriterParams { sink, product, signer, checkpoint_every_n, checkpoint_every, chain, spool, stats, replay_every }`.
- Chain recovery: prefers `spool.resume_point()`, falls back to `recover_from_partition_tail()`, falls back to `ChainState::new()`.
- Spool opened via `Spool::open(resolved_dir, max_bytes)`; relative dirs resolved under `config.log_dir`.
- Stats poller task spawned at 1 s interval: delta-increments counters, sets gauges.

### `crates/broker/tests/support/mod.rs`
- Added `start_with_dir(dir: &Path) -> (BrokerHandle, Client)`: auto-detects Rejoin mode by checking for `__cluster_metadata` directory.
- Added `audit_record_seqs(client: &Client) -> Vec<u64>`: fetches audit partition 0, skips checkpoint records, returns parsed `seq` headers in order.

### `crates/broker/tests/audit.rs`
- Added `audit_chain_continues_across_restart`: two-boot test using a shared tempdir; asserts seqs are contiguous, duplicate-free, and start from 0.

## Test Results

All 6 audit integration tests pass:
- `audit_topic_exists_after_startup`
- `broker_started_event_is_written_to_audit_topic`
- `successful_create_topics_is_audited`
- `signed_checkpoints_appear_on_audit_topic`
- `denied_operation_returns_cluster_authorization_failed`
- `audit_chain_continues_across_restart`

Workspace clippy (`--workspace --all-targets -D warnings`): clean.
`cargo +nightly fmt -p crabka-broker`: no changes.

## Design Decisions

- **Spool priority over partition tail**: the spool's `resume_point()` is authoritative when present because it is updated transactionally after each flush. Partition tail recovery is a fallback for a fresh spool on an existing data dir.
- **Restart detection**: presence of `__cluster_metadata` directory (written by KRaft at first boot) is a reliable signal that Rejoin mode is needed. This is the same heuristic used elsewhere in test helpers.
- **Stats poller delta pattern**: cumulative `AuditStats` values are compared against last-seen values; only the delta is added to Prometheus counters, preventing double-counting across poll intervals.
- **No backwards compat**: `FileAuditSpoolConfig` uses `serde(deny_unknown_fields)`; no migration code.
