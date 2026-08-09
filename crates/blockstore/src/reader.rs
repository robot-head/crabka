//! Reads Parquet blocks back from object storage into Arrow `RecordBatch`es.

use std::{ops::Range, sync::Arc};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use crabka_units::prelude::*;
use futures::{FutureExt, TryFutureExt, TryStreamExt, future::BoxFuture};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataSuffixFetch},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

/// Maximum on-disk byte size of a Parquet block accepted by [`read_block`].
///
/// Blocks come from shared object storage and, per the threat model, may be
/// corrupt or maliciously oversized. A stream of an unbounded Parquet file
/// could OOM the process. The reader `head()`s the block first and rejects it
/// above this cap. This mirrors the profiles gunzip `max_decompressed` output
/// cap. The default is 1 GiB, well above a realistic compacted block.
pub const DEFAULT_BLOCK_READ_MAX: ByteSize = gibibytes(1);

/// Minimal row-group metadata used by query frontends to shard block scans.
///
/// There is no `Eq`. [`ByteSize`] stores `f64`, so it is only `PartialEq`.
/// Nothing keys a map or a set on row-group metadata, so the derive is
/// unused.
#[derive(Clone, Debug, PartialEq)]
pub struct RowGroupMeta {
    pub index: usize,
    pub compressed: ByteSize,
}

/// Reads every `RecordBatch` from the Parquet block at `object_key`.
///
/// The reader rejects the block with an error when its on-disk size exceeds
/// [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block(store: Arc<dyn ObjectStore>, object_key: &str) -> Result<Vec<RecordBatch>> {
    read_block_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}

/// Reads every `RecordBatch` with a caller-supplied on-disk size limit.
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
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    Ok(stream.try_collect::<Vec<_>>().await?)
}

/// `head`s the block, rejects it above `max_bytes`, and hands back its on-disk
/// size for the Parquet reader.
///
/// The object store reports a raw `u64`, so the comparison is the one place
/// that lifts the size into a [`ByteSize`]. The rejection message still prints
/// whole bytes, so it reads the same for a caller-supplied cap and for the
/// default cap.
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

/// Reads row-group sizes from Parquet metadata and does not scan row data.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    read_row_group_metadata_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}

/// Reads row-group sizes with a caller-supplied on-disk size limit.
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
    max_bytes: ByteSize,
) -> Result<Vec<RowGroupMeta>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    Ok(builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| RowGroupMeta {
            index,
            compressed: ByteSize::from_bytes(
                u64::try_from(row_group.compressed_size()).unwrap_or(0),
            ),
        })
        .collect())
}

/// Reads selected row groups from a Parquet block.
///
/// As with [`read_block`], the reader rejects the block when its on-disk size
/// exceeds [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_with_max_bytes(store, object_key, row_groups, DEFAULT_BLOCK_READ_MAX)
        .await
}

/// Reads selected row groups with a caller-supplied on-disk size limit.
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
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_row_groups(row_groups.to_vec())
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}

#[derive(Clone, Debug)]
struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectStoreReader {
    fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_error)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_error)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let result = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(to_parquet_error)?;
            result.bytes().await.map_err(to_parquet_error)
        }
        .boxed()
    }
}

fn to_parquet_error(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use object_store::{ObjectStore, buffered::BufWriter, memory::InMemory, path::Path};
    use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};

    use super::*;
    use crate::writer::BlockWriter;

    #[test]
    fn max_block_bytes_is_one_gib() {
        assert2::assert!(DEFAULT_BLOCK_READ_MAX == gibibytes(1));
        assert2::assert!(DEFAULT_BLOCK_READ_MAX.bytes_u64() == 1024 * 1024 * 1024);
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
        let got =
            read_block_with_max_bytes(store.clone(), "b.parquet", ByteSize::from_bytes(1)).await;
        assert2::assert!(got.is_err());

        // A cap exactly at the real size is accepted; only bytes above the cap
        // are rejected.
        let size = store.head(&Path::from("b.parquet")).await.unwrap().size;
        let out = read_block_with_max_bytes(store, "b.parquet", ByteSize::from_bytes(size))
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
        let object_writer = BufWriter::new(store.clone(), Path::from("meta.parquet"));
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
                .map(|group| (group.index, group.compressed > ByteSize::ZERO))
                .collect::<Vec<_>>()
        };
        assert2::assert!(project(&meta) == vec![(0, true), (1, true)]);

        let got = read_row_group_metadata_with_max_bytes(
            store.clone(),
            "meta.parquet",
            ByteSize::from_bytes(1),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("meta.parquet")).await.unwrap().size;
        let meta = read_row_group_metadata_with_max_bytes(
            store,
            "meta.parquet",
            ByteSize::from_bytes(size),
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

        let object_writer = BufWriter::new(store.clone(), Path::from("rg.parquet"));
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
            ByteSize::from_bytes(1),
        )
        .await;
        assert2::assert!(got.is_err());

        let size = store.head(&Path::from("rg.parquet")).await.unwrap().size;
        let out = read_block_row_groups_with_max_bytes(
            store,
            "rg.parquet",
            &[1],
            ByteSize::from_bytes(size),
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
