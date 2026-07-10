# crabka-pgkv

[![crates.io](https://img.shields.io/crates/v/crabka-pgkv.svg)](https://crates.io/crates/crabka-pgkv)
[![docs.rs](https://docs.rs/crabka-pgkv/badge.svg)](https://docs.rs/crabka-pgkv)

Ordered key-value storage seam for the Crabka Gres engine with order-preserving key encoding and versioned row encoding.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

Defines the `Kv` trait (`get`/`put`/`delete`/`scan_prefix`/`scan_range`/`write_batch`, with `write_batch` atomic and durable) that the whole Gres engine consumes, plus two local backends: `MemKv` (ephemeral) and `FjallKv` (pure-Rust LSM). This trait is the permanent storage seam — Chapter Gres G-2 puts the Crabka substrate behind it without touching the engine.

G-3 checkpoints use the `SnapshotKv` and `RestoreKv` extensions. A snapshot is a consistent, key-ordered stream of committed state; restore consumes a strictly ascending stream into an empty store. `FjallKv` uses fjall snapshots and sorted ingestion for durable checkpoint restore, while `MemKv` clones and batches pairs for deterministic tests.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
