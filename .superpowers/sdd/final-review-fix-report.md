# Final Review Fix Report

## Scope

- Fixed high finding: `Log::sync()` now makes newly-created active segment directory entries durable by fsyncing the log directory after initial segment creation, reset-created segments, truncate-created segments, or rollover-created segments.
- Fixed medium finding: the diskless data-path model now models `WalSync` as both WAL durability and fsync-gated client-facing HW release through `ReplicaState::recompute_hw_for_wal_durable`, while preserving `wal_acked` non-vacuous checks.

## TDD Evidence

### Log Directory Durability

Red:

```text
cargo test -p crabka-log sync_fsyncs_parent_dir -- --nocapture
FAILED log::tests::sync_fsyncs_parent_dir_for_initial_segment_creation
  [] == ["/tmp/.tmpErDEYU"]
FAILED log::tests::sync_fsyncs_parent_dir_after_segment_rollover
  [] == ["/tmp/.tmpDzad8T"]
```

Green:

```text
cargo test -p crabka-log sync_fsyncs_parent_dir -- --nocapture
test log::tests::sync_fsyncs_parent_dir_for_initial_segment_creation ... ok
test log::tests::sync_fsyncs_parent_dir_after_segment_rollover ... ok
test result: ok. 2 passed; 0 failed
```

Final affected log filter:

```text
cargo test -p crabka-log sync_ -- --nocapture
test log::tests::sync_fsyncs_parent_dir_for_initial_segment_creation ... ok
test log::tests::sync_fsyncs_parent_dir_after_segment_rollover ... ok
test log::tests::sync_persists_appended_records ... ok
test result: ok. 3 passed; 0 failed
```

### Diskless Model HW Release

Red:

```text
cargo test -p crabka-broker --lib data_diskless_wal_acked_never_lost -- --nocapture
Unexpected "diskless_hw_released_by_wal_sync" counterexample Path[6]:
- Produce
- WalSync
- Produce
- Produce
- Produce
- Die(0)
Last state: DpState { log: [[1, 1, 1, 1], [], []], hwm: 0, leader: 0, leader_epoch: 1, isr: 1, live: 0, committed: [], wal_acked: [1], lost: false }
```

Green:

```text
cargo test -p crabka-broker --lib data_diskless_wal_acked_never_lost -- --nocapture
[data_diskless_wal_acked_never_lost] unique=30 generated=161 depth=7
test data_path_model::data_diskless_wal_acked_never_lost ... ok
test result: ok. 1 passed; 0 failed
```

## Required Verification

```text
cargo test -p crabka-broker --lib data_clean -- --nocapture
[data_clean] unique=521626 generated=4884321 depth=35
test data_path_model::data_clean ... ok
test result: ok. 1 passed; 0 failed
```

```text
cargo test -p crabka-broker --lib data_unclean -- --nocapture
[data_unclean] unique=1255681 generated=11709489 depth=39
test data_path_model::data_unclean ... ok
test result: ok. 1 passed; 0 failed
```

```text
cargo +nightly fmt --check
```

Result: passed with no output.

## Notes

- Changes are narrow to `crates/log/src/log.rs`, `crates/broker/src/data_path_model.rs`, and this report.
- No wire path above `writer_tx` changed.
- Classic non-WAL writer path behavior is unchanged.
- Diskless model remains Slice 1 single-node local fsync: diskless init constrains ISR/live membership to the leader broker, and `WalSync` computes HW using a singleton leader ISR.
