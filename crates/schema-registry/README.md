# crabka-schema-registry

[![Crates.io](https://img.shields.io/crates/v/crabka-schema-registry.svg)](https://crates.io/crates/crabka-schema-registry)
[![Docs.rs](https://docs.rs/crabka-schema-registry/badge.svg)](https://docs.rs/crabka-schema-registry)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Confluent Schema Registry-compatible REST service for Crabka (binary: crabka-schema-registry).

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-schema-registry
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run the Schema Registry-compatible REST service and register an Avro schema:

```bash
CRABKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
CRABKA_SCHEMA_REGISTRY_LISTEN_ADDR=127.0.0.1:8081 \
crabka-schema-registry

curl -X POST http://127.0.0.1:8081/subjects/orders-value/versions \
  -H 'content-type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"}'
```

## Documentation

Read the API documentation at [docs.rs/crabka-schema-registry](https://docs.rs/crabka-schema-registry). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
