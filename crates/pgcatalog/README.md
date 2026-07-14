# crabka-pgcatalog

[![crates.io](https://img.shields.io/crates/v/crabka-pgcatalog.svg)](https://crates.io/crates/crabka-pgcatalog)
System catalog for the Crabka Gres engine: tables, columns, and FDW metadata as
a stateless view over the KV storage seam.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

OID-style table ids, column definitions, and foreign-data-wrapper metadata
(`FOREIGN DATA WRAPPER`/`SERVER`/`USER MAPPING`/`FOREIGN TABLE`), persisted
through the `Kv` trait with PostgreSQL error codes on CRUD.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
