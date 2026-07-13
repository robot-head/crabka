# Hash physical data path report

Status: TASK_IN_PROGRESS

## Timestamp hash identity slice

RED proved that a SQL `INSERT` into `SHARDED BY HASH (id) BUCKETS 16` persisted an ordinary timestamp version. After bucket-leading persistence was added, the physical committed tuple existed but SQL scans returned no rows. The root cause was the SQL cursor's legacy rowid range bounds: hash keys order by `(bucket,rowid)`, so `row_key(table,start)..row_key(table,end)` excluded bucket-prefixed keys.

GREEN computes the bucket from the catalog hash column and the single authoritative `pgkv::hash_bucket`, persists `HashPrimaryVersion`, scatters hash physical scans before applying rowid intervals, and reconstructs rows by decoding `(bucket,rowid)`. The pinned `int4(42)` corpus uses big-endian bytes and checks the exact physical key class, committed state, direct visibility, and SQL visibility.

Timestamp writes and durable descriptor operations now carry `Option<u32>` bucket identity. Descriptor v2 has a `TXD2` envelope and 22-byte operations: range id, table id, bucket tag plus bucket, rowid, and delete tag. Bucket tags and absent-bucket padding fail closed. Legacy unversioned 17-byte operations decode only as bucketless operations. Primary prewrite, later acknowledgements, descriptor authentication, commit/abort resolution, and recovery use the persisted bucket to rebuild the exact timestamp version key.

## Verification

- `cargo test -p crabka-pgexec --test transactions --no-fail-fast`: 37 passed.
- `cargo test -p crabka-pgexec timestamp_txn --lib --no-fail-fast`: 21 passed.
- `cargo check -p crabka-pgexec --all-targets`: passed.
- Changed pgexec files are rustfmt-clean and `git diff --check` is clean.
- `crates/gres-ranges/src/control.rs` was not edited or staged; its pre-existing formatting delta remains.

## Remaining work

This is not the complete Task 5 data path. xid/COPY/eval-plan-qual/index paths, checkpoint/restore/tail filtering, marker preservation, and the runtime midpoint split gate remain for later slices.

## UPDATE DELETE slice

Cross-bucket hash-key UPDATE now stages two bucket-distinct timestamp operations in one transaction: a delete tombstone in the old bucket and the replacement value in the new bucket. Same-rowid sidecar and prewrite reservation keys include the bucket only for hash writes, so old/new operations cannot collide while bucketless sharded keys remain byte-identical. DELETE derives and resolves the exact catalog hash bucket.

The production SQL regression moves a row across buckets, proves exactly one visible result, proves every physical row is `HashPrimaryVersion`, proves the old bucket is a committed delete, then deletes the moved row and proves the new bucket is a committed delete with no visible row.

Verification for commit `59092d16`:

- `cargo test -p crabka-pgexec --test transactions --no-fail-fast`: 38 passed.
- `cargo test -p crabka-pgexec timestamp_txn --lib --no-fail-fast`: 21 passed.
- `cargo check -p crabka-pgexec --all-targets`: passed.
- Changed-file `git diff --check`: passed.

COPY remains separate: both session COPY entry points currently reject sharded tables before the xid-only COPY executor.

## COPY timestamp slice

Autocommit `COPY FROM STDIN` now plans supported sharded tables as one `TimestampWritePlan` and commits the whole batch through the existing timestamp participant prewrite/commit/abort protocol. It reuses the established COPY text decoder, target resolution, defaults, NULL handling, and type coercion. Hash rows derive their bucket from the catalog hash column. Unsupported local indexes and unique global indexes fail closed; explicit sharded transactions retain their existing rejection. Unsharded COPY still executes the unchanged xid path.

The production protocol test copies the pinned integers 0 through 15, proves 16 physical `HashPrimaryVersion` keys and 16 SQL-visible rows, then submits a batch containing one valid row followed by a NULL hash key and proves the entire failing batch adds no physical version.

Verification for commit `2a93a781`:

- `cargo test -p crabka-pgexec copy_from_stdin --lib --no-fail-fast`: 4 passed.
- `cargo test -p crabka-pgexec --test transactions --no-fail-fast`: 38 passed.
- `cargo check -p crabka-pgexec --all-targets`: passed.
- Changed-file `git diff --check`: passed.
