# crabka-pgparser

[![crates.io](https://img.shields.io/crates/v/crabka-pgparser.svg)](https://crates.io/crates/crabka-pgparser)
Hand-written PostgreSQL SQL lexer and parser producing the Crabka Gres AST.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres. Chapter Gres
is a pure-Rust Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`. See the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

This crate is an original recursive-descent/Pratt parser with no third-party SQL
engine. It produces the `Statement` AST that the Gres executor consumes. The
optional `oracle` feature turns on a differential accept/reject test against
libpg_query for local verification. That feature is never a default and needs a
C build dependency.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
