//! Writes columnar blocks to object storage as Parquet.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{Array, FixedSizeBinaryArray, Int64Array, UInt64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use object_store::{ObjectStore, path::Path};
use parquet::arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter};
use tracing::instrument;

use crate::{
    block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against},
    block_index::{BlockSchema, series_block_schema},
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

/// Columns used to summarize a block's time bounds and distinct identity keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryColumns {
    pub id_col: String,
    pub ts_col: String,
}

impl SummaryColumns {
    #[must_use]
    pub fn new(id_col: impl Into<String>, ts_col: impl Into<String>) -> Self {
        Self {
            id_col: id_col.into(),
            ts_col: ts_col.into(),
        }
    }

    #[must_use]
    pub fn series() -> Self {
        Self::new(COL_FINGERPRINT, COL_TIMESTAMP)
    }
}

/// Writes Parquet blocks to an object store.
pub struct BlockWriter {
    store: Arc<dyn ObjectStore>,
}

impl BlockWriter {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Write `batches` as a single Parquet block at `object_key`.
    ///
    /// Returns [`BlockMeta`] computed from the mandatory block columns.
    pub async fn write_block(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<BlockMeta> {
        self.write_block_with_decl(
            tenant,
            object_key,
            schema,
            batches,
            &series_block_schema(),
            SummaryColumns::series(),
        )
        .await
    }

    /// Write a block validated against a signal-specific schema declaration.
    ///
    /// Returns [`BlockMeta`] computed from the declared summary columns.
    #[instrument(
        skip_all,
        fields(tenant = %tenant, object_key = %object_key, batches = batches.len()),
        err
    )]
    pub async fn write_block_with_decl(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
        decl: &BlockSchema,
        summary: SummaryColumns,
    ) -> Result<BlockMeta> {
        validate_against(&schema, decl)?;
        validate_batch_schemas(&schema, batches)?;

        let (min_ts, max_ts, row_count, fingerprints) = summarize(batches, &summary)?;

        let path = Path::from(object_key);
        let object_writer = ParquetObjectWriter::new(self.store.clone(), path);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, None)?;
        for batch in batches {
            writer.write(batch).await?;
        }
        writer.close().await?;

        Ok(BlockMeta {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            fingerprints,
        })
    }
}

fn validate_batch_schemas(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    for (index, batch) in batches.iter().enumerate() {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "batch {index} schema does not match writer schema"
            )));
        }
    }
    Ok(())
}

fn summarize(
    batches: &[RecordBatch],
    summary: &SummaryColumns,
) -> Result<(i64, i64, usize, Vec<SeriesFingerprint>)> {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0_usize;
    let mut fps: BTreeSet<SeriesFingerprint> = BTreeSet::new();

    for batch in batches {
        row_count += batch.num_rows();

        let ts = batch
            .column_by_name(&summary.ts_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing timestamp column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlockStoreError::InvalidBlock("timestamp not Int64".into()))?;
        let id = batch
            .column_by_name(&summary.id_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing identity column".into()))?;
        // Only the series/logs/metrics (`UInt64` fingerprint) path populates
        // `BlockMeta.fingerprints`; trace (span) blocks key the identity column
        // as `FixedSizeBinary` and never read `fingerprints`, so we skip the
        // per-row FNV pass entirely for them. `FixedSizeBinary` is still an
        // accepted id-column type — we just don't fingerprint it.
        let id_u64 = id.as_any().downcast_ref::<UInt64Array>();
        if id_u64.is_none() && id.as_any().downcast_ref::<FixedSizeBinaryArray>().is_none() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be UInt64 or FixedSizeBinary",
                summary.id_col
            )));
        }

        for i in 0..batch.num_rows() {
            if !ts.is_null(i) {
                let v = ts.value(i);
                min_ts = min_ts.min(v);
                max_ts = max_ts.max(v);
            }
            if let Some(fp) = id_u64
                && !fp.is_null(i)
            {
                fps.insert(fp.value(i));
            }
        }
    }

    if row_count == 0 {
        return Err(BlockStoreError::InvalidBlock("empty block".into()));
    }

    Ok((min_ts, max_ts, row_count, fps.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use assert2::{assert, check};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};

    use super::*;

    fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        let fp = UInt64Array::from(vec![10_u64, 10, 20, 20]);
        let ts = Int64Array::from(vec![100_i64, 200, 300, 400]);
        let line = StringArray::from(vec!["a", "b", "c", "d"]);
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(fp), Arc::new(ts), Arc::new(line)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_block_persists_object_and_returns_meta() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = sample_batch(&schema);

        let meta = writer
            .write_block("tenant-a", "blocks/tenant-a/b1.parquet", schema, &[batch])
            .await
            .unwrap();

        let mut meta = meta;
        meta.fingerprints.sort_unstable();
        assert!(
            meta == BlockMeta {
                tenant: "tenant-a".to_string(),
                object_key: "blocks/tenant-a/b1.parquet".to_string(),
                min_ts: 100,
                max_ts: 400,
                row_count: 4,
                fingerprints: vec![10, 20],
            }
        );

        let head = store.head(&Path::from("blocks/tenant-a/b1.parquet")).await;
        assert!(head.is_ok());
    }

    fn span_summary_batch() -> RecordBatch {
        use arrow::array::FixedSizeBinaryArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("start_unix_nano", DataType::Int64, false),
        ]));
        let ids: Vec<[u8; 16]> = vec![[1_u8; 16], [2_u8; 16]];
        let trace_id =
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(<[u8; 16]>::as_slice)).unwrap();
        let ts = Int64Array::from(vec![100_i64, 200]);
        RecordBatch::try_new(schema, vec![Arc::new(trace_id), Arc::new(ts)]).unwrap()
    }

    #[test]
    fn summarize_skips_fingerprints_for_span_blocks() {
        // Span (FixedSizeBinary id) blocks never read `meta.fingerprints`, so
        // the per-row FNV pass should be skipped and the set left empty. Time
        // bounds and row count must still be summarized.
        let batch = span_summary_batch();
        let (min_ts, max_ts, row_count, fps) = summarize(
            &[batch],
            &SummaryColumns::new("trace_id", "start_unix_nano"),
        )
        .unwrap();
        check!(min_ts == 100);
        check!(max_ts == 200);
        check!(row_count == 2);
        check!(fps.is_empty());
    }

    #[test]
    fn summarize_still_fingerprints_series_blocks() {
        let schema = log_schema();
        let batch = sample_batch(&schema);
        let (_min, _max, _rows, mut fps) = summarize(&[batch], &SummaryColumns::series()).unwrap();
        fps.sort_unstable();
        assert!(fps == vec![10_u64, 20]);
    }

    #[tokio::test]
    async fn write_block_rejects_schema_without_mandatory_columns() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["x"]))])
                .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn write_block_rejects_batch_schema_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = log_schema();
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(Int64Array::from(vec![100_i64])),
                Arc::new(UInt64Array::from(vec![10_u64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;

        assert!(
            matches!(err, Err(BlockStoreError::InvalidBlock(message)) if message.contains("schema"))
        );
    }
}
