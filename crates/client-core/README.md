# crabka-client-core

[![Crates.io](https://img.shields.io/crates/v/crabka-client-core.svg)](https://crates.io/crates/crabka-client-core)
[![Docs.rs](https://docs.rs/crabka-client-core/badge.svg)](https://docs.rs/crabka-client-core)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Connection management and request dispatch for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-client-core = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Send a typed Kafka protocol request over a negotiated client connection:

```rust,no_run
use crabka_client_core::Client;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder()
    .bootstrap("127.0.0.1:9092")
    .client_id("metadata-prober")
    .build()
    .await?;

let response = client.send(ApiVersionsRequest::default()).await?;
println!("broker returned {} API keys", response.api_keys.len());
client.close();
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-client-core](https://docs.rs/crabka-client-core). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
