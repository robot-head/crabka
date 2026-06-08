# crabka-metadata

[![Crates.io](https://img.shields.io/crates/v/crabka-metadata.svg)](https://crates.io/crates/crabka-metadata)
[![Docs.rs](https://docs.rs/crabka-metadata/badge.svg)](https://docs.rs/crabka-metadata)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Versioned metadata record types + immutable image for Crabka.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-metadata = "0.3.2"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Apply controller records to build an immutable metadata image:

```rust
use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
use uuid::Uuid;

let mut image = MetadataImage::new(Uuid::new_v4());
let topic_id = Uuid::new_v4();
image.apply(&MetadataRecord::V1Topic(TopicRecord {
    name: "orders".into(),
    topic_id,
    partitions: 3,
    replication_factor: 1,
}));

assert_eq!(image.topic("orders").unwrap().topic_id, topic_id);
```

## Documentation

API documentation is published on [docs.rs/crabka-metadata](https://docs.rs/crabka-metadata). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
