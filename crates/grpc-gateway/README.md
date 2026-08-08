# crabka-grpc-gateway

[![Crates.io](https://img.shields.io/crates/v/crabka-grpc-gateway.svg)](https://crates.io/crates/crabka-grpc-gateway)
[![Docs.rs](https://docs.rs/crabka-grpc-gateway/badge.svg)](https://docs.rs/crabka-grpc-gateway)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

gRPC / Connect-RPC + HTTP gateway into Crabka (Kafka) topics.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-grpc-gateway
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Run the Connect-RPC gateway in front of a Crabka cluster:

```bash
CRABKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
CRABKA_GATEWAY_LISTEN_ADDR=127.0.0.1:9500 \
crabka-grpc-gateway

curl -f http://127.0.0.1:9500/healthz
```

## Documentation

The API documentation is on [docs.rs/crabka-grpc-gateway](https://docs.rs/crabka-grpc-gateway). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
