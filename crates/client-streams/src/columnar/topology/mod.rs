//! Native columnar topology over polars `DataFrame`s.
//!
//! Edges carry `DataFrame`s. A `BatchCodec` bridges Kafka records ↔
//! `DataFrame`. Later tasks add the operators, the graph, the driver, and the
//! broker runtime bridge.

pub mod codec;
pub mod driver;
pub mod graph;
pub mod operator;
pub mod row_bridge;
pub mod runtime;

pub use codec::{BatchCodec, BatchError, BlobCodec, ConsumedRecord, ProduceRecord, RowCodec};
pub use driver::ColumnarTestDriver;
pub use graph::{BuiltColumnarTopology, ColumnarNode, ColumnarTopology};
pub use operator::{BuiltinOp, ColumnarContext, ColumnarProcessor};
pub use row_bridge::{JsonRowBridge, RowBridge};
pub use runtime::run_partition_once;
