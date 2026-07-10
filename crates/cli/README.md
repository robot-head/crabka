# crabka-cli

[![Crates.io](https://img.shields.io/crates/v/crabka-cli.svg)](https://crates.io/crates/crabka-cli)
[![Docs.rs](https://docs.rs/crabka-cli/badge.svg)](https://docs.rs/crabka-cli)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Operator CLI for Crabka (binary: `crabka`).

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-cli
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

Manage Gres tenants through the control-plane registry:

```bash
crabka gres create-tenant \
  --bootstrap 127.0.0.1:9092 \
  --name tenant-a \
  --user app \
  --password-file ./tenant-a.password

crabka gres list --bootstrap 127.0.0.1:9092
crabka gres describe --bootstrap 127.0.0.1:9092 --name tenant-a
```

`create-tenant` writes a validated tenant record with a PostgreSQL SCRAM verifier
to the compacted `__gres_tenants` registry; command output redacts the verifier.
`suspend`, `resume`, and `delete` update the same tenant registry record. The
operator path mirrors tenant records into `__gres_cfg.<tenant>` for compute
startup.

Render PgDog files from the live registry for local or operator-adjacent testing:

```bash
crabka gres render-pgdog \
  --bootstrap 127.0.0.1:9092 \
  --out-dir ./pgdog
```

The render command writes `pgdog.toml` and `users.toml`. Pass `--activator host:port`
to route suspended tenants to an activator instead of omitting their backend route.

## Documentation

API documentation is published on [docs.rs/crabka-cli](https://docs.rs/crabka-cli). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
