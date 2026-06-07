# crabka-client-admin

[![Crates.io](https://img.shields.io/crates/v/crabka-client-admin.svg)](https://crates.io/crates/crabka-client-admin)
[![Docs.rs](https://docs.rs/crabka-client-admin/badge.svg)](https://docs.rs/crabka-client-admin)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Operator-side admin client for Crabka clusters.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-client-admin = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Create a topic and fetch its metadata from the controller:

```rust,no_run
use std::collections::BTreeMap;
use crabka_client_admin::{AdminClient, CreateTopicSpec};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut admin = AdminClient::connect(&["127.0.0.1:9092".to_string()]).await?;
admin.create_topics(&[CreateTopicSpec {
    name: "orders".into(),
    partitions: 3,
    replicas: 1,
    configs: BTreeMap::new(),
}], 30_000).await?;

let metadata = admin.metadata(&["orders"]).await?;
println!("topics: {:?}", metadata.topics);
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-client-admin](https://docs.rs/crabka-client-admin). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
