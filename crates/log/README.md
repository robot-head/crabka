# crabka-log

[![Crates.io](https://img.shields.io/crates/v/crabka-log.svg)](https://crates.io/crates/crabka-log)
[![Docs.rs](https://docs.rs/crabka-log/badge.svg)](https://docs.rs/crabka-log)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Byte-compatible reader/writer for Apache Kafka's on-disk log format.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-log = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Open a Kafka-compatible log directory, append a batch, and read it back:

```rust,no_run
use crabka_log::{Log, LogConfig};
use crabka_protocol::records::RecordBatch;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut log = Log::open("./target/orders-0", LogConfig::default())?;
let mut batch = RecordBatch::default();
// Fill the RecordBatch with records before appending in production code.
let base_offset = log.append(&mut batch)?;
let bytes = log.read(base_offset, 1024 * 1024)?;
println!("read {} bytes", bytes.bytes.len());
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-log](https://docs.rs/crabka-log). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
