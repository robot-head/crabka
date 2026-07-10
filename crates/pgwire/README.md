# crabka-pgwire

[![crates.io](https://img.shields.io/crates/v/crabka-pgwire.svg)](https://crates.io/crates/crabka-pgwire)
[![docs.rs](https://docs.rs/crabka-pgwire/badge.svg)](https://docs.rs/crabka-pgwire)

PostgreSQL v3 wire-protocol server: simple and extended query protocols, SCRAM-SHA-256, TLS, and CancelRequest.

Part of [Crabka](https://github.com/robot-head/crabka)'s Chapter Gres — a pure-Rust
Postgres-compatible engine vendored from
[crabgresql](https://github.com/robot-head/crabgresql) at `93f3d17`; see the
[chapter design](../../docs/superpowers/specs/2026-07-09-crabka-gres-chapter-design.md).

## Overview

A standalone pgwire server that any engine can sit behind via the `Engine`/`Session` traits: startup + SSLRequest negotiation (rustls), Trust and SCRAM-SHA-256 auth (RustCrypto, with anti-username-enumeration mock verifiers), Parse/Bind/Describe/Execute portals, per-column format codes, and CancelRequest semantics. Verified against tokio-postgres, sqlx, and recorded psql byte traces.

## License

Apache-2.0. Derived from [crabgresql](https://github.com/robot-head/crabgresql)
(PostgreSQL License); see [NOTICE](../../NOTICE).
