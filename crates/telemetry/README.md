# crabka-telemetry

[![Crates.io](https://img.shields.io/crates/v/crabka-telemetry.svg)](https://crates.io/crates/crabka-telemetry)
[![Docs.rs](https://docs.rs/crabka-telemetry/badge.svg)](https://docs.rs/crabka-telemetry)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Generic OTLP distributed-tracing pipeline for Crabka services.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-telemetry
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Install stdout tracing plus optional OTLP export for a Crabka service process:

```rust,no_run
use crabka_telemetry::{init, OtlpConfig};

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let otlp = OtlpConfig::from_env(
    |key| std::env::var(key).ok(),
    "broker-1",
    env!("CARGO_PKG_VERSION"),
    "crabka-broker",
);
let guard = init(otlp, "info", "info", "crabka-broker")?;
tracing::info!("tracing is configured");
guard.shutdown();
# Ok(())
# }
```

## Documentation

Read the API documentation at [docs.rs/crabka-telemetry](https://docs.rs/crabka-telemetry). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
