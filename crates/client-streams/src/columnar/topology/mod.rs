//! Native columnar (polars `DataFrame`) topology. Edges carry `DataFrame`s;
//! a `BatchCodec` bridges Kafka records ↔ `DataFrame`. Operators, graph, driver,
//! and the broker runtime bridge are added by later tasks.

pub mod codec;
pub mod operator;
pub mod row_bridge;

pub use codec::{BatchCodec, BatchError, BlobCodec, ConsumedRecord, ProduceRecord, RowCodec};
pub use operator::{BuiltinOp, ColumnarContext, ColumnarProcessor};
pub use row_bridge::{JsonRowBridge, RowBridge};
