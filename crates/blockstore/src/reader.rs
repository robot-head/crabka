//! Reads Parquet blocks from object storage into Arrow record batches.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::error::Result;

/// Minimal row-group metadata used by query frontends to shard block scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupMeta {
    pub index: usize,
    pub compressed_bytes: u64,
}

/// Read every record batch from the Parquet block at `object_key`.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}

/// Read row-group sizes from Parquet metadata without scanning row data.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    Ok(builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| RowGroupMeta {
            index,
            compressed_bytes: u64::try_from(row_group.compressed_size()).unwrap_or(0),
        })
        .collect())
}

/// Read selected row groups from a Parquet block.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_row_groups(row_groups.to_vec())
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::arrow::async_writer::ParquetObjectWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::writer::BlockWriter;

    #[tokio::test]
    async fn write_then_read_round_trips_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        BlockWriter::new(store.clone())
            .write_block("t", "b.parquet", schema, &[batch])
            .await
            .unwrap();

        let out = read_block(store, "b.parquet").await.unwrap();
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);
    }

    #[tokio::test]
    async fn read_row_group_metadata_reports_every_group() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["first", "second"])),
            ],
        )
        .unwrap();

        // One row per row group → exactly two row groups.
        let object_writer = ParquetObjectWriter::new(store.clone(), Path::from("meta.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let meta = read_row_group_metadata(store, "meta.parquet")
            .await
            .unwrap();
        assert!(meta.len() == 2);
        assert!(meta[0].index == 0);
        assert!(meta[1].index == 1);
        assert!(meta[0].compressed_bytes > 0);
        assert!(meta[1].compressed_bytes > 0);
    }

    #[tokio::test]
    async fn read_block_row_groups_reads_only_selected_groups() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["first", "second"])),
            ],
        )
        .unwrap();

        let object_writer = ParquetObjectWriter::new(store.clone(), Path::from("rg.parquet"));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(props)).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let out = read_block_row_groups(store, "rg.parquet", &[1])
            .await
            .unwrap();

        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 1);
        let lines = out[0]
            .column_by_name("line")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(lines.value(0) == "second");
    }
}
