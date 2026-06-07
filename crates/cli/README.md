# crabka-cli

[![Crates.io](https://img.shields.io/crates/v/crabka-cli.svg)](https://crates.io/crates/crabka-cli)
[![Docs.rs](https://docs.rs/crabka-cli/badge.svg)](https://docs.rs/crabka-cli)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Operator CLI for Crabka (binary: `crabka`).

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-cli = "0.3.2"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Format a broker log directory before first start, optionally seeding credentials:

```bash
crabka format \
  --cluster-id 00000000-0000-0000-0000-000000000001 \
  --node-id 1 \
  --log-dir /var/lib/crabka/data
```

## Documentation

API documentation is published on [docs.rs/crabka-cli](https://docs.rs/crabka-cli). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
