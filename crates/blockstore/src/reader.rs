//! Reads Parquet blocks back from object storage into Arrow `RecordBatch`es.

use std::{fmt, str::FromStr, sync::Arc};

use arrow::record_batch::RecordBatch;
use crabka_units::prelude::*;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, async_reader::ParquetObjectReader};
use refined_type::rule::GreaterU64;
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

/// Maximum on-disk byte size of a Parquet block accepted by [`read_block`].
///
/// Blocks come from shared object storage and (per the threat model) may be
/// corrupt or maliciously oversized; streaming an unbounded Parquet file could
/// OOM the process. The block is `head()`ed first and rejected above this cap,
/// mirroring the profiles gunzip `max_decompressed` output cap. Defaults to
/// 1 GiB, well above a realistic compacted block.
pub const DEFAULT_BLOCK_READ_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Compatibility alias for the default Parquet block-read cap.
pub const MAX_BLOCK_BYTES: ByteSize = gibibytes(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockReadMaxBytes(u64);

impl BlockReadMaxBytes {
    /// Validate a maximum Parquet block-read size.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> std::result::Result<Self, String> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes(self.0)
    }
}

impl Default for BlockReadMaxBytes {
    fn default() -> Self {
        Self::new(DEFAULT_BLOCK_READ_MAX_BYTES)
            .expect("default Parquet block-read maximum is positive")
    }
}

impl fmt::Display for BlockReadMaxBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for BlockReadMaxBytes {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

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
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    read_block_with_max_bytes(store, object_key, BlockReadMaxBytes::default()).await
}

/// Read every `RecordBatch` with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_block_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: BlockReadMaxBytes,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let size = head_within_cap(&store, &path, object_key, max_bytes.size()).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    Ok(stream.try_collect::<Vec<_>>().await?)
}

async fn head_within_cap(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<u64> {
    let meta = store.head(path).await?;
    tracing::Span::current().record("size", meta.size);
    if ByteSize::from_bytes(meta.size) > max_bytes {
        return Err(BlockStoreError::InvalidBlock(format!(
            "block `{object_key}` is {} bytes, exceeds cap of {} bytes",
            meta.size,
            max_bytes.bytes_u64()
        )));
    }
    Ok(meta.size)
}

/// Read row-group sizes from Parquet metadata without scanning row data.
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    read_row_group_metadata_with_max_bytes(store, object_key, BlockReadMaxBytes::default()).await
}

/// Read row-group sizes with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, or persisted metadata is malformed.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_row_group_metadata_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: BlockReadMaxBytes,
) -> Result<Vec<RowGroupMeta>> {
    let path = Path::from(object_key);
    let size = head_within_cap(&store, &path, object_key, max_bytes.size()).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(size);
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
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_with_max_bytes(
        store,
        object_key,
        row_groups,
        BlockReadMaxBytes::default(),
    )
    .await
}

/// Read selected row groups with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, row_groups = row_groups.len(), size = tracing::field::Empty),
    err
)]
pub async fn read_block_row_groups_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: BlockReadMaxBytes,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let size = head_within_cap(&store, &path, object_key, max_bytes.size()).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(size);
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

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use parquet::{
        arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter},
        file::properties::WriterProperties,
    };

    use super::*;
    use crate::writer::BlockWriter;

    #[test]
    fn max_block_bytes_is_one_gib() {
        assert2::assert!(MAX_BLOCK_BYTES == gibibytes(1));
    }

    #[test]
    fn block_read_max_bytes_validates_and_preserves_default() {
        assert2::assert!(BlockReadMaxBytes::default().into_value() == 1024 * 1024 * 1024);
        assert2::assert!("1".parse::<BlockReadMaxBytes>().unwrap().into_value() == 1);
        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert2::assert!(invalid.parse::<BlockReadMaxBytes>().is_err());
        }
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
            .write_block("t", "b.parquet", schema, std::slice::from_ref(&batch))
            .await
            .unwrap();

        let out = read_block(store, "b.parquet").await.unwrap();
        assert2::assert!(out == vec![batch]);
    }

    #[tokio::test]
    async fn read_block_with_max_bytes_rejects_over_cap_block() {
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
        let got = read_block_with_max_bytes(
            store.clone(),
            "b.parquet",
            BlockReadMaxBytes::new(1).unwrap(),
        )
        .await;
        assert2::assert!(got.is_err());

        // A cap exactly at the real size is accepted; only bytes above the cap
        // are rejected.
        let size = store.head(&Path::from("b.parquet")).await.unwrap().size;
        let out =
            read_block_with_max_bytes(store, "b.parquet", BlockReadMaxBytes::new(size).unwrap())
                .await
                .unwrap();
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
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
        let project = |metadata: &[RowGroupMeta]| {
            metadata
                .iter()
                .map(|group| (group.index, group.compressed_bytes > 0))
                .collect::<Vec<_>>()
        };
        assert2::assert!(project(&meta) == vec![(0, true), (1, true)]);

        let got = read_row_group_metadata_with_max_bytes(
            store.clone(),
            "meta.parquet",
            BlockReadMaxBytes::new(1).unwrap(),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("meta.parquet")).await.unwrap().size;
        let meta = read_row_group_metadata_with_max_bytes(
            store,
            "meta.parquet",
            BlockReadMaxBytes::new(size).unwrap(),
        )
        .await
        .unwrap();
        assert2::assert!(project(&meta) == vec![(0, true), (1, true)]);
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

        let got = read_block_row_groups_with_max_bytes(
            store.clone(),
            "rg.parquet",
            &[1],
            BlockReadMaxBytes::new(1).unwrap(),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("rg.parquet")).await.unwrap().size;
        let out = read_block_row_groups_with_max_bytes(
            store,
            "rg.parquet",
            &[1],
            BlockReadMaxBytes::new(size).unwrap(),
        )
        .await
        .unwrap();

        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        let lines = out[0]
            .column_by_name("line")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert2::assert!(total == 1);
        assert2::assert!(lines.value(0) == "second");
    }
}
