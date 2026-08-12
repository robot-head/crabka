# crabka-broker

[![Crates.io](https://img.shields.io/crates/v/crabka-broker.svg)](https://crates.io/crates/crabka-broker)
[![Docs.rs](https://docs.rs/crabka-broker/badge.svg)](https://docs.rs/crabka-broker)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Single-node Apache Kafka-compatible broker (MVP).

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-broker
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Start a single-node broker with a local data directory:

```rust,no_run
use std::net::SocketAddr;
use crabka_broker::{Broker, BrokerConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let listen_addr: SocketAddr = "127.0.0.1:9092".parse()?;
let config = BrokerConfig {
    listen_addr,
    advertised_listener: listen_addr.to_string(),
    log_dir: "./target/crabka-data".into(),
    ..BrokerConfig::default()
};

let broker = Broker::start(config).await?;
tokio::signal::ctrl_c().await?;
broker.shutdown().await;
# Ok(())
# }
```

For OAUTHBEARER on inter-broker and controller-listener connections, enable
`OAUTHBEARER` on the corresponding SASL listeners and point the shared
outbound credentials at a token file:

```toml
controller_listener_protocol = "SaslPlaintext"

[inter_broker_credentials]
type = "oauth-bearer"
token_path = "/var/run/secrets/crabka/inter-broker-token"
```

The broker validates the file at startup and re-reads it for every new
connection, so replacing the mounted token rotates credentials without a
broker restart. Inbound validation uses the existing `[oauthbearer]` policy.

## Documentation

Read the API documentation on [docs.rs/crabka-broker](https://docs.rs/crabka-broker). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
