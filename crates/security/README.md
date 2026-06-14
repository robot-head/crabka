# crabka-security

[![Crates.io](https://img.shields.io/crates/v/crabka-security.svg)](https://crates.io/crates/crabka-security)
[![Docs.rs](https://docs.rs/crabka-security/badge.svg)](https://docs.rs/crabka-security)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

TLS, SASL, SCRAM, OAuth, and Kerberos security utilities for Crabka.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-security
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Build a SCRAM client exchange and produce the client-first message sent on the Kafka SASL channel:

```rust
use crabka_security::{SaslMechanism, ScramClientExchange};

let mut exchange = ScramClientExchange::new(
    "alice".to_string(),
    b"correct horse battery staple".to_vec(),
    SaslMechanism::ScramSha256,
);
let first = exchange.client_first().unwrap();
println!("send SCRAM client-first-message: {}", String::from_utf8_lossy(&first));
```

## Documentation

API documentation is published on [docs.rs/crabka-security](https://docs.rs/crabka-security). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
