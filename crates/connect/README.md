# crabka-connect

[![Crates.io](https://img.shields.io/crates/v/crabka-connect.svg)](https://crates.io/crates/crabka-connect)
[![Docs.rs](https://docs.rs/crabka-connect/badge.svg)](https://docs.rs/crabka-connect)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Connector-framework SPI for Crabka sources, sinks, converters, and embedded
runtimes.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-connect` defines the traits and data model for connector authors. It is
the shared SPI for CDC sources, telemetry sinks, byte converters, typed schema
converters, and connector configuration. It also holds the single-process
runtime that pipes a source into a sink.

The crate is embeddable by design. It has no Kafka Connect worker protocol and
no REST management server. Build connectors programmatically and run them inside
the process that owns their lifecycle.

## Capabilities

- `Source<K, V>` polls records from an external system, checkpoints the read
  position, seeks on restart, and acknowledges committed checkpoints.
- `Sink<K, V>` writes batches, flushes them, and can put writes inside
  transactions.
- `ConnectRecord<K, V>` with optional key/value, timestamp, headers, and source
  offset metadata.
- `Converter<T>` converts between typed connector payloads and Kafka wire
  bytes.
- `ByteIdentity` for byte-for-byte passthrough.
- `SchemaConverter<T>` for Confluent-framed Avro, Protobuf, or JSON payloads
  through `crabka-schema-serde`.
- `ConnectorRuntime` for the sequential `poll -> put -> commit -> checkpoint ->
  acknowledge` loop.
- `ConfigDef`, `ConnectorConfig`, secret resolution, typed config extraction,
  and a default derive macro.

## Runtime Model

`ConnectorRuntime` owns one source and one sink. In each interval, the runtime
polls a bounded batch and writes it to the sink. It then commits or flushes the
sink and persists the source checkpoint. It acknowledges the source only after
the checkpoint is durable.

This order stops the source from moving ahead of the records that the sink has
committed. The included `InMemoryCheckpointStore` is useful for tests and
short-lived tools. A production connector should supply a durable
`CheckpointStore`.

## Install

```sh
cargo add crabka-connect
cargo add serde_json
```

For workspace development, use the path dependency from this repository.

## Usage

Define typed connector configuration with the default `derive` feature:

```rust,no_run
use crabka_connect::{ConfigDef, ConnectorConfig, EnvSecretResolver, SecretString};
use serde_json::json;

#[derive(ConnectorConfig)]
struct PostgresSourceConfig {
    #[config(required)]
    database_url: String,

    #[config(secret)]
    password: SecretString,

    #[config(default = "public")]
    schema: String,
}

# async fn build() -> crabka_connect::ConfigResult<PostgresSourceConfig> {
let raw = serde_json::Map::from_iter([
    ("database_url".to_string(), json!("postgres://localhost/app")),
    (
        "password".to_string(),
        json!({ "from": "env", "name": "POSTGRES_PASSWORD" }),
    ),
]);

let def: ConfigDef = PostgresSourceConfig::config_def();
let resolved = def.resolve(raw, &EnvSecretResolver).await?;
let config = PostgresSourceConfig::from_resolved(&resolved)?;
# Ok(config)
# }
```

Secret fields resolve through `SecretResolver` implementations. `Debug` and
`Display` redact them. The crate rejects literal secrets unless `ResolveOptions`
explicitly allows them.

## Cargo Features

- `derive` - enables and reexports `crabka-connect-derive::ConnectorConfig`.
  This feature is enabled by default.

## Documentation

- [API documentation](https://docs.rs/crabka-connect)
- [Derive macro crate](https://crates.io/crates/crabka-connect-derive)
- [Schema serdes](https://crates.io/crates/crabka-schema-serde)
- [Crabka repository](https://github.com/robot-head/crabka)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
