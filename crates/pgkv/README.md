# crabka-pgkv

[![crates.io](https://img.shields.io/crates/v/crabka-pgkv.svg)](https://crates.io/crates/crabka-pgkv)
Ordered key-value storage seam for the Crabka Gres engine with order-preserving key encoding and versioned row encoding.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres. Chapter Gres
is a pure-Rust Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`. See the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

This crate defines the `Kv` trait that the whole Gres engine consumes. The trait has `get`, `put`, `delete`, `scan_prefix`, `scan_range`, and `write_batch`, and `write_batch` is atomic and durable. The crate also supplies two local backends: `MemKv` is ephemeral, and `FjallKv` is a pure-Rust LSM. This trait is the permanent storage seam. Chapter Gres G-2 puts the Crabka substrate behind it and does not change the engine.

G-3 checkpoints use the `SnapshotKv` and `RestoreKv` extensions. A snapshot is a consistent, key-ordered stream of committed state. Restore consumes a strictly ascending stream into an empty store. `FjallKv` uses fjall snapshots and sorted ingestion for durable checkpoint restore. `MemKv` clones and batches pairs for deterministic tests.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
