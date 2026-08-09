# crabka-pgtypes

[![crates.io](https://img.shields.io/crates/v/crabka-pgtypes.svg)](https://crates.io/crates/crabka-pgtypes)
PostgreSQL value layer for the Crabka Gres engine: `Datum`, column types, text and binary wire encodings, casts, and operator semantics.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres. Chapter Gres
is a pure-Rust Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`. See the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

This crate implements the PostgreSQL-faithful value semantics that the Gres engine executes over: the `Datum` and `ColumnType` enums, numeric and date/time arithmetic, cast rules, operator semantics, and both wire encodings. The numeric arithmetic has arbitrary precision. The crate is the root of the engine crate graph and has no sibling dependencies.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
