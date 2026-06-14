//! Native columnar (polars `DataFrame`) topology. Edges carry `DataFrame`s;
//! a `BatchCodec` bridges Kafka records ↔ `DataFrame`. Operators, graph, driver,
//! and the broker runtime bridge are added by later tasks.

pub mod codec;

pub use codec::{BatchCodec, BatchError, BlobCodec, ConsumedRecord, ProduceRecord};
