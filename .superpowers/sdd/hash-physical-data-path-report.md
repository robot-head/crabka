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

This is not the complete Task 5 data path. UPDATE bucket moves, broader DELETE coverage, xid/COPY/eval-plan-qual/index paths, checkpoint/restore/tail filtering, marker preservation, and the runtime midpoint split gate remain for later slices.
