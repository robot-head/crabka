# crabka-remote-storage

[![Crates.io](https://img.shields.io/crates/v/crabka-remote-storage.svg)](https://crates.io/crates/crabka-remote-storage)
[![Docs.rs](https://docs.rs/crabka-remote-storage/badge.svg)](https://docs.rs/crabka-remote-storage)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-405 tiered-storage SPI (RemoteStorageManager / RemoteLogMetadataManager) and reference implementations for Crabka.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-remote-storage
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Copy a closed log segment into the filesystem-backed remote tier and fetch its offset index:

```rust,no_run
use std::{collections::BTreeMap, path::PathBuf};
use bytes::Bytes;
use crabka_remote_storage::{
    IndexType, LocalTieredStorage, LogSegmentData, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};
use uuid::Uuid;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let storage = LocalTieredStorage::new(PathBuf::from("/var/lib/crabka-remote"));
let topic_partition = TopicIdPartition::new(Uuid::new_v4(), "orders", 0);
let segment_id = RemoteLogSegmentId::new(topic_partition, Uuid::new_v4());
let mut leader_epochs = BTreeMap::new();
leader_epochs.insert(0, 0);
let metadata = RemoteLogSegmentMetadata::new(
    segment_id, 0, 999, 1_713_000_000_000, 1, 1_713_000_000_000,
    1_048_576, RemoteLogSegmentState::CopySegmentStarted, leader_epochs,
)?;
let segment = LogSegmentData {
    log_segment: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.log"),
    offset_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.index"),
    time_index: PathBuf::from("/var/lib/crabka/orders-0/00000000000000000000.timeindex"),
    transaction_index: None,
    producer_snapshot_index: None,
    leader_epoch_index: Bytes::new(),
};
let _custom_metadata = storage.copy_log_segment_data(&metadata, &segment)?;
let _offset_index = storage.fetch_index(&metadata, IndexType::Offset)?;
# Ok(())
# }
```

## Documentation

Read the API documentation at [docs.rs/crabka-remote-storage](https://docs.rs/crabka-remote-storage). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
