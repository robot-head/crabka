# crabka-operator

[![Crates.io](https://img.shields.io/crates/v/crabka-operator.svg)](https://crates.io/crates/crabka-operator)
[![Docs.rs](https://docs.rs/crabka-operator/badge.svg)](https://docs.rs/crabka-operator)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kubernetes operator for Crabka clusters.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-operator = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Generate CRDs locally, then run the operator in a Kubernetes cluster:

```bash
crabka-operator gen-crds ./target/crds
kubectl apply -f ./target/crds

WATCH_NAMESPACE=crabka-system crabka-operator run
```

## Documentation

API documentation is published on [docs.rs/crabka-operator](https://docs.rs/crabka-operator). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
