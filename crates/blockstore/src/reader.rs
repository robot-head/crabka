//! Reads Parquet blocks back from object storage into Arrow `RecordBatch`es.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

/// Maximum on-disk byte size of a Parquet block accepted by [`read_block`].
///
/// Blocks come from shared object storage and (per the threat model) may be
/// corrupt or maliciously oversized; streaming an unbounded Parquet file could
/// OOM the process. The block is `head()`ed first and rejected above this cap,
/// mirroring the profiles gunzip `max_decompressed` output cap. Defaults to
/// 1 GiB, well above a realistic compacted block.
pub const MAX_BLOCK_BYTES: u64 = 1024 * 1024 * 1024;

/// Minimal row-group metadata used by query frontends to shard block scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowGroupMeta {
    pub index: usize,
    pub compressed_bytes: u64,
}

/// Read every `RecordBatch` from the Parquet block at `object_key`.
///
/// The block is rejected with an error when its on-disk size exceeds
/// [`MAX_BLOCK_BYTES`], before any bytes are streamed.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    read_block_with_cap(store, object_key, MAX_BLOCK_BYTES).await
}

#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
async fn read_block_with_cap(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: u64,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    tracing::Span::current().record("size", meta.size);
    if meta.size > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {max_bytes} bytes",
            meta.size
        )));
    }
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    Ok(stream.try_collect::<Vec<_>>().await?)
}

/// Read row-group sizes from Parquet metadata without scanning row data.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    read_row_group_metadata_with_cap(store, object_key, MAX_BLOCK_BYTES).await
}

#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
async fn read_row_group_metadata_with_cap(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: u64,
) -> Result<Vec<RowGroupMeta>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    tracing::Span::current().record("size", meta.size);
    if meta.size > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {max_bytes} bytes",
            meta.size
        )));
    }
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
///
/// As with [`read_block`], the block is rejected when its on-disk size exceeds
/// [`MAX_BLOCK_BYTES`], before any bytes are streamed.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_with_cap(store, object_key, row_groups, MAX_BLOCK_BYTES).await
}

#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, row_groups = row_groups.len(), size = tracing::field::Empty),
    err
)]
async fn read_block_row_groups_with_cap(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: u64,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    tracing::Span::current().record("size", meta.size);
    if meta.size > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {max_bytes} bytes",
            meta.size
        )));
    }
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
    use assert2::{assert, check};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::arrow::async_writer::ParquetObjectWriter;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::writer::BlockWriter;

    #[test]
    fn max_block_bytes_is_one_gib() {
        assert!(MAX_BLOCK_BYTES == 1024 * 1024 * 1024);
        assert!(MAX_BLOCK_BYTES == 1_073_741_824);
    }

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
    async fn read_block_rejects_over_cap_block() {
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

        // A tiny cap stands in for the production cap so the test need not
        // materialize an over-cap block; the real block is well above 1 byte.
        let got = read_block_with_cap(store.clone(), "b.parquet", 1).await;
        assert!(got.is_err());

        // A cap exactly at the real size is accepted; only bytes above the cap
        // are rejected.
        let size = store.head(&Path::from("b.parquet")).await.unwrap().size;
        let out = read_block_with_cap(store, "b.parquet", size).await.unwrap();
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

        let meta = read_row_group_metadata(store.clone(), "meta.parquet")
            .await
            .unwrap();
        assert!(meta.len() == 2);
        for (i, want_index) in [(0, 0), (1, 1)] {
            check!(meta[i].index == want_index);
            check!(meta[i].compressed_bytes > 0);
        }

        let got = read_row_group_metadata_with_cap(store.clone(), "meta.parquet", 1).await;
        assert!(got.is_err());

        let size = store.head(&Path::from("meta.parquet")).await.unwrap().size;
        let meta = read_row_group_metadata_with_cap(store, "meta.parquet", size)
            .await
            .unwrap();
        assert!(meta.len() == 2);
        for (i, want_index) in [(0, 0), (1, 1)] {
            check!(meta[i].index == want_index);
            check!(meta[i].compressed_bytes > 0);
        }
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

        let got = read_block_row_groups_with_cap(store.clone(), "rg.parquet", &[1], 1).await;
        assert!(got.is_err());

        let size = store.head(&Path::from("rg.parquet")).await.unwrap().size;
        let out = read_block_row_groups_with_cap(store, "rg.parquet", &[1], size)
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
