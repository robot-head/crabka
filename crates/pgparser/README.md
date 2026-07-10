# crabka-pgparser

[![crates.io](https://img.shields.io/crates/v/crabka-pgparser.svg)](https://crates.io/crates/crabka-pgparser)
[![docs.rs](https://docs.rs/crabka-pgparser/badge.svg)](https://docs.rs/crabka-pgparser)

Hand-written PostgreSQL SQL lexer and parser producing the Crabka Gres AST.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

An original recursive-descent/Pratt parser (no third-party SQL engine) producing
the `Statement` AST the Gres executor consumes. The optional `oracle` feature
(never default; C build dep) enables a differential accept/reject test against
libpg_query for local verification.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
