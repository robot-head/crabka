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

Format a broker log directory before the first start. This command can also seed credentials:

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

`create-tenant` writes a validated tenant record to the compacted
`__gres_tenants` registry. The record holds a PostgreSQL SCRAM verifier, and the
command output redacts that verifier. `suspend`, `resume`, and `delete` update
the same tenant registry record. The operator path mirrors tenant records into
`__gres_cfg.<tenant>` for compute startup.

Render PgDog files from the live registry for local and operator-adjacent tests:

```bash
crabka gres render-pgdog \
  --bootstrap 127.0.0.1:9092 \
  --out-dir ./pgdog
```

The render command writes `pgdog.toml` and `users.toml`. Pass `--activator host:port`
to route suspended tenants to an activator. Without that flag, the command omits
the backend route of each suspended tenant.

## Documentation

Read the API documentation on [docs.rs/crabka-cli](https://docs.rs/crabka-cli). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
