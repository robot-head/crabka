# crabka-pgmvcc

[![crates.io](https://img.shields.io/crates/v/crabka-pgmvcc.svg)](https://crates.io/crates/crabka-pgmvcc)
PostgreSQL-faithful MVCC for the Crabka Gres engine: xids, clog, snapshots, and `HeapTupleSatisfiesMVCC` visibility.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres. Chapter Gres
is a pure-Rust Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`. See the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

This crate ports the snapshot-isolation machinery faithfully from PostgreSQL: transaction ids, the clog (pg_xact) status store over the KV seam, xid-list `Snapshot`s, and tuple `(xmin, xmax)` visibility.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
