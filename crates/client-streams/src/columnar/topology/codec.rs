//! `BatchCodec`: bridges a per-partition batch of Kafka records ↔ a polars `DataFrame`.

use ::polars::prelude::*;
use bytes::Bytes;

/// Reserved metadata column names carried on every assembled `DataFrame` so the
/// sink codec can faithfully reconstruct Kafka records and the runtime can commit
/// offsets. Payload columns may not use these names.
pub const COL_KEY: &str = "__key";
pub const COL_TIMESTAMP: &str = "__timestamp";
pub const COL_PARTITION: &str = "__partition";
pub const COL_OFFSET: &str = "__offset";

/// All reserved column names, in `DataFrame` column order.
pub const RESERVED_COLUMNS: [&str; 4] = [COL_KEY, COL_TIMESTAMP, COL_PARTITION, COL_OFFSET];

/// One consumed Kafka record handed to a `BatchCodec::decode`.
#[derive(Debug, Clone)]
pub struct ConsumedRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: i64,
    pub partition: i32,
    pub offset: i64,
}

/// One record a `BatchCodec::encode` asks the runtime to produce.
#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: i64,
}

/// Failure assembling/decomposing a batch.
#[derive(Debug, thiserror::Error)]
#[error("batch codec error: {0}")]
pub struct BatchError(pub String);

/// Bridges a per-partition batch of records ↔ a polars `DataFrame`.
pub trait BatchCodec: Send + Sync + 'static {
    /// Assemble consumed records (in offset order) into one `DataFrame`, including
    /// the reserved metadata columns.
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError>;
    /// Decompose an output `DataFrame` into produce records.
    fn encode(&self, df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError>;
}

/// Returns `Err` if `df_columns` contains a name that collides with a reserved
/// metadata column. Shared by codecs (Tasks 6–7) and the topology builder (Task 9).
pub fn reject_reserved_payload_columns(df_columns: &[&str]) -> Result<(), BatchError> {
    for name in df_columns {
        if RESERVED_COLUMNS.contains(name) {
            return Err(BatchError(format!(
                "payload column `{name}` collides with a reserved metadata column"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn reserved_columns_are_distinct_and_underscored() {
        check!(RESERVED_COLUMNS.len() == 4);
        for c in RESERVED_COLUMNS {
            check!(c.starts_with("__"));
        }
    }

    #[test]
    fn reject_reserved_payload_columns_flags_collision() {
        check!(reject_reserved_payload_columns(&["id", "total"]).is_ok());
        check!(reject_reserved_payload_columns(&["id", "__key"]).is_err());
    }
}
