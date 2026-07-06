# crabka-connect-derive

[![Crates.io](https://img.shields.io/crates/v/crabka-connect-derive.svg)](https://crates.io/crates/crabka-connect-derive)
[![Docs.rs](https://docs.rs/crabka-connect-derive/badge.svg)](https://docs.rs/crabka-connect-derive)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Derive macros for `crabka-connect` connector configuration.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-connect-derive` provides `#[derive(ConnectorConfig)]`, the procedural
macro used by connector authors to declare ConfigDef-style schemas from Rust
structs. It generates both `ConnectorConfig::config_def()` and
`ConnectorConfig::from_resolved()` implementations for `crabka-connect`.

Most users get this macro through `crabka-connect`'s default `derive` feature:

```toml
crabka-connect = "0.3.8"
```

Use this crate directly only when you need the proc-macro dependency separate
from `crabka-connect`'s default feature set.

## Install

```sh
cargo add crabka-connect-derive
cargo add crabka-connect
```

For the usual connector-authoring path, prefer `crabka-connect` with its
default `derive` feature enabled:

```sh
cargo add crabka-connect
```

## Supported Attributes

- `#[config(required)]` - mark a field as required even if its Rust type could
  otherwise be optional.
- `#[config(secret)]` - mark `SecretString` or `Option<SecretString>` fields as
  secret-backed values.
- `#[config(default = ...)]` - set the ConfigDef default expression.
- `#[config(name = "...")]` - use a config key that differs from the field name.
- `#[config(crate = "path")]` - container attribute for renamed
  `crabka-connect` dependencies.

## Supported Field Types

The derive supports `String`, `bool`, signed and unsigned integer types,
`f32`, `f64`, `serde_json::Value`, `SecretString`, `std::time::Duration`,
`Vec<String>`, and `Option<T>` for supported `T`.

## Usage

```rust
use crabka_connect::{ConnectorConfig, SecretString};

#[derive(ConnectorConfig)]
struct PostgresSourceConfig {
    #[config(required)]
    database_url: String,

    #[config(secret)]
    password: SecretString,

    #[config(default = "public")]
    schema: String,

    #[config(name = "snapshot.mode")]
    snapshot_mode: Option<String>,
}
```

The generated implementation declares required, optional, secret, and defaulted
keys in a `ConfigDef`, then extracts typed values from a resolved config map.

## Boundaries

- Only structs with named fields are supported.
- Generic structs, tuple structs, and enums are rejected.
- `SecretString` and `Option<SecretString>` fields must use
  `#[config(secret)]`.
- Secret fields cannot declare defaults.
- Duplicate config keys are compile errors.

## Documentation

- [API documentation](https://docs.rs/crabka-connect-derive)
- [Connector SPI crate](https://crates.io/crates/crabka-connect)
- [Crabka repository](https://github.com/robot-head/crabka)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
