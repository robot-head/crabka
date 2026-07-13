//! Shared columnar block-store primitives for Crabka observability signals.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{
            Array, Int64Array, MapArray, MapBuilder, RecordBatch, StringArray, StringBuilder,
            UInt64Array,
        },
        datatypes::{DataType, Field, Fields, Schema},
        error::ArrowError,
    },
    catalog::Session,
    datasource::{
        MemTable, TableProvider,
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        provider::TableProviderFilterPushDown,
    },
    error::DataFusionError,
    logical_expr::{Expr, TableType},
    parquet::{
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        errors::ParquetError,
    },
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use futures::StreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;
use xxhash_rust::xxh3::xxh3_64;

pub type Labels = BTreeMap<String, String>;
pub type StructuredMetadata = BTreeMap<String, String>;
pub type SeriesFingerprint = u64;

#[must_use]
pub fn labels<const N: usize>(items: [(&str, &str); N]) -> Labels {
    items
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[must_use]
pub fn series_fingerprint(labels: &Labels) -> SeriesFingerprint {
    let mut canonical = Vec::new();
    for (name, value) in labels {
        append_len_prefixed(&mut canonical, name);
        append_len_prefixed(&mut canonical, value);
    }
    xxh3_64(&canonical)
}

fn append_len_prefixed(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(value.len().to_string().as_bytes());
    bytes.push(b':');
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

#[derive(Clone, Debug)]
pub struct LabelPredicate {
    name: String,
    op: MatchOp,
    value: String,
    regex: Option<Regex>,
}

impl LabelPredicate {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, BlockStoreError> {
        let value = value.into();
        let regex = if matches!(op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            Some(
                Regex::new(&anchored_regex_pattern(&value)).map_err(|source| {
                    BlockStoreError::InvalidRegex {
                        pattern: value.clone(),
                        source,
                    }
                })?,
            )
        } else {
            None
        };
        Ok(Self {
            name: name.into(),
            op,
            value,
            regex,
        })
    }

    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        let candidate = labels.get(&self.name);
        match self.op {
            MatchOp::Equal => candidate == Some(&self.value),
            MatchOp::NotEqual => candidate != Some(&self.value),
            MatchOp::RegexEqual => self.regex_matches(candidate.map_or("", String::as_str)),
            MatchOp::RegexNotEqual => candidate.is_none_or(|value| !self.regex_matches(value)),
        }
    }

    fn exact_posting_key(&self) -> Option<(&str, &str)> {
        (self.op == MatchOp::Equal).then_some((&self.name, &self.value))
    }

    fn regex_matches(&self, value: &str) -> bool {
        self.regex
            .as_ref()
            .expect("regex predicate validated at construction")
            .is_match(value)
    }
}

fn anchored_regex_pattern(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelIndex {
    series: BTreeMap<String, BTreeMap<SeriesFingerprint, Labels>>,
    postings: BTreeMap<(String, String, String), BTreeSet<SeriesFingerprint>>,
}

impl LabelIndex {
    pub fn insert_series(
        &mut self,
        tenant: impl Into<String>,
        labels: Labels,
    ) -> SeriesFingerprint {
        let tenant = tenant.into();
        let fingerprint = series_fingerprint(&labels);
        for (name, value) in &labels {
            self.postings
                .entry((tenant.clone(), name.clone(), value.clone()))
                .or_default()
                .insert(fingerprint);
        }
        self.series
            .entry(tenant)
            .or_default()
            .insert(fingerprint, labels);
        fingerprint
    }

    #[must_use]
    pub fn match_series(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> BTreeSet<SeriesFingerprint> {
        let Some(series) = self.series.get(tenant) else {
            return BTreeSet::new();
        };
        let Some(candidates) = self.exact_candidates(tenant, predicates) else {
            return BTreeSet::new();
        };

        candidates
            .into_iter()
            .filter(|fingerprint| {
                series.get(fingerprint).is_some_and(|labels| {
                    predicates
                        .iter()
                        .filter(|predicate| predicate.op != MatchOp::Equal)
                        .all(|predicate| predicate.matches(labels))
                })
            })
            .collect()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> BTreeSet<String> {
        self.postings
            .keys()
            .filter(|(posting_tenant, _, _)| posting_tenant == tenant)
            .map(|(_, name, _)| name.clone())
            .collect()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, label_name: &str) -> BTreeSet<String> {
        self.postings
            .keys()
            .filter(|(posting_tenant, name, _)| posting_tenant == tenant && name == label_name)
            .map(|(_, _, value)| value.clone())
            .collect()
    }

    #[must_use]
    pub fn labels_for(&self, tenant: &str, fingerprint: SeriesFingerprint) -> Option<&Labels> {
        self.series.get(tenant)?.get(&fingerprint)
    }

    #[must_use]
    pub fn tenant_series(&self, tenant: &str) -> Vec<(SeriesFingerprint, Labels)> {
        self.series
            .get(tenant)
            .map(|series| {
                series
                    .iter()
                    .map(|(fingerprint, labels)| (*fingerprint, labels.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn exact_candidates(
        &self,
        tenant: &str,
        predicates: &[LabelPredicate],
    ) -> Option<BTreeSet<SeriesFingerprint>> {
        let mut matched: Option<BTreeSet<SeriesFingerprint>> = None;
        for predicate in predicates {
            let Some((name, value)) = predicate.exact_posting_key() else {
                continue;
            };
            let key = (tenant.to_string(), name.to_string(), value.to_string());
            let next = self.postings.get(&key)?;
            matched = Some(match matched {
                Some(current) => current.intersection(next).copied().collect(),
                None => next.clone(),
            });
        }

        Some(matched.unwrap_or_else(|| {
            self.series
                .get(tenant)
                .map_or_else(BTreeSet::new, |series| series.keys().copied().collect())
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TimeRange {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn new(start_ns: i64, end_ns: i64) -> Result<Self, BlockStoreError> {
        if start_ns > end_ns {
            return Err(BlockStoreError::InvalidTimeRange { start_ns, end_ns });
        }
        Ok(Self { start_ns, end_ns })
    }

    #[must_use]
    pub fn overlaps(self, other: Self) -> bool {
        self.start_ns <= other.end_ns && other.start_ns <= self.end_ns
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockKey {
    pub tenant: String,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub time_range: TimeRange,
}

impl BlockKey {
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        time_range: TimeRange,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            partition,
            first_offset,
            last_offset,
            time_range,
        }
    }

    #[must_use]
    pub fn object_key(&self) -> String {
        format!(
            "tenant={}/partition={}/offsets={}-{}/time={}-{}.parquet",
            self.tenant,
            self.partition,
            self.first_offset,
            self.last_offset,
            self.time_range.start_ns,
            self.time_range.end_ns
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockDescriptor {
    pub key: BlockKey,
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    #[serde(default)]
    pub size_bytes: u64,
}

impl BlockDescriptor {
    #[must_use]
    pub fn new(key: BlockKey, fingerprints: BTreeSet<SeriesFingerprint>) -> Self {
        Self::new_with_size(key, fingerprints, 0)
    }

    #[must_use]
    pub fn new_with_size(
        key: BlockKey,
        fingerprints: BTreeSet<SeriesFingerprint>,
        size_bytes: u64,
    ) -> Self {
        Self {
            key,
            fingerprints,
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRow {
    pub series_fingerprint: SeriesFingerprint,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: StructuredMetadata,
}

impl LogRow {
    #[must_use]
    pub fn new(
        series_fingerprint: SeriesFingerprint,
        timestamp_ns: i64,
        line: impl Into<String>,
        structured_metadata: StructuredMetadata,
    ) -> Self {
        Self {
            series_fingerprint,
            timestamp_ns,
            line: line.into(),
            structured_metadata,
        }
    }
}

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len()),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn write_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    rows.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    let path = block_path(root, key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema = log_block_schema();
    let batch = rows_to_batch(&rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let size_bytes = fs::metadata(&path)?.len();

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        size_bytes,
    ))
}

#[instrument(
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition, rows = rows.len(), size = tracing::field::Empty),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    mut rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    validate_rows(key, &rows)?;
    rows.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    let payload = encode_log_block(&rows)?;
    let size_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    tracing::Span::current().record("size", size_bytes);
    store
        .put(&log_block_object_path(prefix, key), payload.into())
        .await?;

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        size_bytes,
    ))
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn read_log_block(
    root: impl AsRef<Path>,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let file = File::open(block_path(root, key))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %key.tenant, partition = key.partition),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_block_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let bytes = store
        .get(&log_block_object_path(prefix, key))
        .await?
        .bytes()
        .await?;
    read_log_block_from_reader(bytes)
}

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks(
    ctx: &SessionContext,
    table_name: &str,
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new(root, blocks)?);
    ctx.register_table(table_name, table)?;
    Ok(())
}

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn register_log_blocks_from_object_store(
    ctx: &SessionContext,
    table_name: &str,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    blocks: &[BlockDescriptor],
) -> Result<(), BlockStoreError> {
    let table = Arc::new(LogBlockTableProvider::try_new_object_store(
        store, prefix, blocks,
    )?);
    ctx.register_table(table_name, table)?;
    Ok(())
}

#[derive(Debug)]
pub struct LogBlockTableProvider {
    schema: Arc<Schema>,
    planned_blocks: Vec<BlockDescriptor>,
    source: LogBlockTableSource,
}

#[derive(Debug)]
enum LogBlockTableSource {
    Local(Box<ListingTable>),
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}

impl LogBlockTableProvider {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new(
        root: impl AsRef<Path>,
        blocks: &[BlockDescriptor],
    ) -> Result<Self, BlockStoreError> {
        let schema = log_block_schema();
        let listing_table = planned_log_listing_table(root, blocks, Arc::clone(&schema))?;
        Ok(Self {
            schema,
            planned_blocks: blocks.to_vec(),
            source: LogBlockTableSource::Local(Box::new(listing_table)),
        })
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new_object_store(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        blocks: &[BlockDescriptor],
    ) -> Result<Self, BlockStoreError> {
        validate_planned_blocks(blocks)?;
        Ok(Self {
            schema: log_block_schema(),
            planned_blocks: blocks.to_vec(),
            source: LogBlockTableSource::ObjectStore { store, prefix },
        })
    }

    #[must_use]
    pub fn planned_blocks(&self) -> &[BlockDescriptor] {
        &self.planned_blocks
    }
}

#[async_trait]
impl TableProvider for LogBlockTableProvider {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        match &self.source {
            LogBlockTableSource::Local(listing_table) => {
                listing_table.scan(state, projection, filters, limit).await
            }
            LogBlockTableSource::ObjectStore { store, prefix } => {
                let mut partitions = Vec::with_capacity(self.planned_blocks.len());
                for block in &self.planned_blocks {
                    let rows = read_log_block_from_object_store(store.as_ref(), prefix, &block.key)
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    partitions.push(vec![
                        rows_to_batch(&rows, Arc::clone(&self.schema))
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    ]);
                }

                let table = MemTable::try_new(Arc::clone(&self.schema), partitions)?;
                table.scan(state, projection, filters, limit).await
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_references_only_pushdown_columns(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn filter_references_only_pushdown_columns(filter: &Expr) -> bool {
    let columns = filter.column_refs();
    !columns.is_empty()
        && columns.iter().all(|column| {
            matches!(
                column.name.as_str(),
                "series_fingerprint" | "timestamp_ns" | "line"
            )
        })
}

fn planned_log_listing_table(
    root: impl AsRef<Path>,
    blocks: &[BlockDescriptor],
    schema: Arc<Schema>,
) -> Result<ListingTable, BlockStoreError> {
    validate_planned_blocks(blocks)?;

    let table_paths = blocks
        .iter()
        .map(|block| {
            let path = block_path(root.as_ref(), &block.key);
            ListingTableUrl::parse(
                path.to_str()
                    .ok_or(BlockStoreError::NonUtf8BlockPath { path: path.clone() })?,
            )
            .map_err(BlockStoreError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let config = ListingTableConfig::new_with_multi_paths(table_paths)
        .with_listing_options(listing_options)
        .with_schema(schema);
    Ok(ListingTable::try_new(config)?)
}

fn validate_planned_blocks(blocks: &[BlockDescriptor]) -> Result<(), BlockStoreError> {
    if blocks.is_empty() {
        return Err(BlockStoreError::EmptyBlockScan);
    }
    Ok(())
}

const LOG_INDEX_MANIFEST_RELATIVE_PATH: &str = "index/logs/manifest.json";
const LOG_INDEX_MANIFEST_VERSION: u32 = 1;

#[instrument(skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn write_log_index_manifest(
    root: impl AsRef<Path>,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let path = log_index_manifest_path(root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let manifest = LogIndexManifest::from_indexes(label_index, block_index);
    serde_json::to_writer_pretty(File::create(path)?, &manifest)?;
    Ok(())
}

#[instrument(skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_log_index_manifest_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes(label_index, block_index);
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(&log_index_manifest_object_path(prefix), payload.into())
        .await?;
    Ok(())
}

#[instrument(skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_manifest_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes_for_tenant(tenant, label_index, block_index);
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(
            &log_tenant_index_manifest_object_path(prefix, tenant),
            payload.into(),
        )
        .await?;
    Ok(())
}

#[instrument(
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shard_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    let manifest = LogIndexManifest::from_indexes_for_tenant_shard(
        tenant,
        shard_range,
        label_index,
        block_index,
    );
    let payload = serde_json::to_vec_pretty(&manifest)?;
    store
        .put(
            &log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range),
            payload.into(),
        )
        .await?;
    Ok(())
}

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shards_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    for shard_range in shard_ranges {
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            *shard_range,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, shard_ranges).await
}

#[instrument(skip_all, fields(tenant = %tenant, shards = shard_ranges.len()), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shard_catalog_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
) -> Result<(), BlockStoreError> {
    let catalog = LogIndexShardCatalog::new(shard_ranges);
    let payload = serde_json::to_vec_pretty(&catalog)?;
    store
        .put(
            &log_tenant_index_shard_catalog_object_path(prefix, tenant),
            payload.into(),
        )
        .await?;
    Ok(())
}

#[instrument(level = "debug", skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn read_log_index_manifest(
    root: impl AsRef<Path>,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let manifest: LogIndexManifest =
        serde_json::from_reader(File::open(log_index_manifest_path(root))?)?;
    manifest.into_indexes()
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_manifest_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes_for_tenant(tenant)
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_shard_manifest_object_path(
            prefix,
            tenant,
            shard_range,
        ))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes_for_tenant(tenant)
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_shard_catalog_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let catalog: LogIndexShardCatalog = serde_json::from_slice(&bytes)?;
    catalog.into_shards()
}

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    collect_tenant_log_index_shard_ranges(
        shard_prefix.clone(),
        store.list(Some(&shard_prefix)),
        None,
    )
    .await
}

#[instrument(
    level = "debug",
    skip_all,
    fields(tenant = %tenant, start_ns = query_range.start_ns, end_ns = query_range.end_ns),
    err
)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let shard_prefix = log_tenant_index_shards_object_prefix(prefix, tenant);
    let offset = log_tenant_index_shard_list_offset_object_path(prefix, tenant, query_range);
    collect_tenant_log_index_shard_ranges(
        shard_prefix,
        store.list_with_offset(
            Some(&log_tenant_index_shards_object_prefix(prefix, tenant)),
            &offset,
        ),
        Some(query_range),
    )
    .await
}

async fn collect_tenant_log_index_shard_ranges(
    shard_prefix: ObjectPath,
    mut stream: futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>,
    filter_range: Option<TimeRange>,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let mut ranges = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if let Some(range) =
            parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &meta.location)
            && filter_range.is_none_or(|filter_range| range.overlaps(filter_range))
        {
            ranges.push(range);
        }
    }

    ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    ranges.dedup();
    Ok(ranges)
}

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_shards_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let mut shard_ranges = list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
        store,
        prefix,
        tenant,
        query_range,
    )
    .await?;
    if shard_ranges.is_empty() {
        shard_ranges =
            match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await
            {
                Ok(shard_ranges) => shard_ranges,
                Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                    Vec::new()
                }
                Err(error) => return Err(error),
            };
    }
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for shard_range in shard_ranges
        .into_iter()
        .filter(|shard_range| shard_range.overlaps(query_range))
    {
        let (label_index, block_index) =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;

        for (series_tenant, series) in label_index.series {
            for (_, labels) in series {
                merged_labels.insert_series(series_tenant.clone(), labels);
            }
        }
        for block in block_index.blocks {
            merged_blocks.entry(block.key.object_key()).or_insert(block);
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    Ok((merged_labels, block_index))
}

#[instrument(level = "debug", skip_all, err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_index_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes()
}

#[must_use]
pub fn log_index_manifest_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(LOG_INDEX_MANIFEST_RELATIVE_PATH)
}

#[must_use]
pub fn log_index_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    LOG_INDEX_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}

#[must_use]
pub fn log_tenant_index_manifest_object_path(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    log_index_manifest_object_path(&prefix.clone().join(format!("tenant={tenant}")))
}

#[must_use]
pub fn log_tenant_index_shard_catalog_object_path(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant).join("manifest.json")
}

#[must_use]
pub fn log_tenant_index_shards_object_prefix(prefix: &ObjectPath, tenant: &str) -> ObjectPath {
    prefix
        .clone()
        .join(format!("tenant={tenant}"))
        .join("index")
        .join("logs")
        .join("shards")
}

#[must_use]
pub fn log_tenant_index_shard_manifest_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant)
        .join(format!(
            "time={}-{}",
            shard_range.start_ns, shard_range.end_ns
        ))
        .join("manifest.json")
}

#[must_use]
pub fn log_tenant_index_shard_list_offset_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant).join(format!(
        "time={}",
        log_tenant_index_shard_list_offset_start_ns(query_range)
    ))
}

#[must_use]
pub fn log_tenant_index_shard_list_offset_start_ns(query_range: TimeRange) -> i64 {
    let query_width_ns = query_range
        .end_ns
        .saturating_sub(query_range.start_ns)
        .max(1);
    query_range.start_ns.saturating_sub(query_width_ns)
}

fn parse_log_tenant_index_shard_range_from_object_path(
    shard_prefix: &ObjectPath,
    location: &ObjectPath,
) -> Option<TimeRange> {
    let rest = location
        .as_ref()
        .strip_prefix(shard_prefix.as_ref())?
        .trim_start_matches('/');
    let mut parts = rest.split('/');
    let range_part = parts.next()?.strip_prefix("time=")?;
    if parts.next()? != "manifest.json" || parts.next().is_some() {
        return None;
    }

    for (index, _) in range_part.match_indices('-') {
        if index == 0 {
            continue;
        }
        let (start, end) = range_part.split_at(index);
        let end = &end[1..];
        if let (Ok(start_ns), Ok(end_ns)) = (start.parse::<i64>(), end.parse::<i64>())
            && let Ok(range) = TimeRange::new(start_ns, end_ns)
        {
            return Some(range);
        }
    }
    None
}

#[must_use]
pub fn log_block_object_path(prefix: &ObjectPath, key: &BlockKey) -> ObjectPath {
    key.object_key()
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}

#[must_use]
pub fn block_path(root: impl AsRef<Path>, key: &BlockKey) -> PathBuf {
    root.as_ref().join(key.object_key())
}

fn log_block_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("series_fingerprint", DataType::UInt64, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("line", DataType::Utf8, false),
        Field::new("structured_metadata", structured_metadata_type(), false),
    ]))
}

fn rows_to_batch(rows: &[LogRow], schema: Arc<Schema>) -> Result<RecordBatch, BlockStoreError> {
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.series_fingerprint)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.timestamp_ns).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(structured_metadata_array(rows)?),
        ],
    )?)
}

fn structured_metadata_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false,
    )
}

fn structured_metadata_array(rows: &[LogRow]) -> Result<MapArray, BlockStoreError> {
    let mut builder = MapBuilder::new(
        Some(datafusion::arrow::array::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));

    for row in rows {
        for (key, value) in &row.structured_metadata {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }

    Ok(builder.finish())
}

fn encode_log_block(rows: &[LogRow]) -> Result<Vec<u8>, BlockStoreError> {
    let schema = log_block_schema();
    let batch = rows_to_batch(rows, Arc::clone(&schema))?;
    let mut writer = ArrowWriter::try_new(Cursor::new(Vec::new()), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?.into_inner())
}

fn read_log_block_from_reader(
    reader: impl datafusion::parquet::file::reader::ChunkReader + 'static,
) -> Result<Vec<LogRow>, BlockStoreError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(reader)?.build()?;
    let mut rows = Vec::new();
    for batch in reader {
        rows.extend(batch_to_rows(&batch?)?);
    }
    Ok(rows)
}

fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<LogRow>, BlockStoreError> {
    let fingerprints = batch
        .column_by_name("series_fingerprint")
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "series_fingerprint",
            expected: "UInt64",
        })?;
    let timestamps = batch
        .column_by_name("timestamp_ns")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "timestamp_ns",
            expected: "Int64",
        })?;
    let lines = batch
        .column_by_name("line")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "line",
            expected: "Utf8",
        })?;
    let metadata = batch
        .column_by_name("structured_metadata")
        .and_then(|array| array.as_any().downcast_ref::<MapArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata",
            expected: "Map<Utf8, Utf8>",
        })?;

    (0..batch.num_rows())
        .map(|row| {
            Ok(LogRow::new(
                fingerprints.value(row),
                timestamps.value(row),
                lines.value(row),
                structured_metadata_value(metadata, row)?,
            ))
        })
        .collect()
}

fn structured_metadata_value(
    metadata: &MapArray,
    row: usize,
) -> Result<StructuredMetadata, BlockStoreError> {
    let entries = metadata.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata.key",
            expected: "Utf8",
        })?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(BlockStoreError::InvalidBlockColumn {
            column: "structured_metadata.value",
            expected: "Utf8",
        })?;

    Ok((0..entries.len())
        .map(|index| {
            (
                keys.value(index).to_string(),
                values.value(index).to_string(),
            )
        })
        .collect())
}

fn validate_rows(key: &BlockKey, rows: &[LogRow]) -> Result<(), BlockStoreError> {
    if let Some(row) = rows.iter().find(|row| {
        row.timestamp_ns < key.time_range.start_ns || row.timestamp_ns > key.time_range.end_ns
    }) {
        return Err(BlockStoreError::RowOutsideBlockTimeRange {
            timestamp_ns: row.timestamp_ns,
            start_ns: key.time_range.start_ns,
            end_ns: key.time_range.end_ns,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockIndex {
    blocks: Vec<BlockDescriptor>,
}

impl BlockIndex {
    pub fn insert(&mut self, block: BlockDescriptor) {
        self.blocks
            .retain(|existing| existing.key.object_key() != block.key.object_key());
        self.blocks.push(block);
        self.blocks
            .sort_by_cached_key(|block| block.key.object_key());
    }

    #[must_use]
    pub fn blocks(&self) -> &[BlockDescriptor] {
        &self.blocks
    }

    #[must_use]
    pub fn match_blocks(
        &self,
        tenant: &str,
        time_range: TimeRange,
        fingerprints: &[SeriesFingerprint],
    ) -> Vec<BlockDescriptor> {
        self.blocks
            .iter()
            .filter(|block| {
                block.key.tenant == tenant
                    && block.key.time_range.overlaps(time_range)
                    && (fingerprints.is_empty()
                        || fingerprints
                            .iter()
                            .any(|fingerprint| block.fingerprints.contains(fingerprint)))
            })
            .cloned()
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LogIndexManifest {
    format_version: u32,
    series: Vec<ManifestSeries>,
    blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogIndexShardCatalog {
    format_version: u32,
    shards: Vec<TimeRange>,
}

impl LogIndexShardCatalog {
    fn new(shard_ranges: &[TimeRange]) -> Self {
        let mut shards = shard_ranges.to_vec();
        shards.sort_by_key(|range| (range.start_ns, range.end_ns));
        shards.dedup();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            shards,
        }
    }

    fn into_shards(self) -> Result<Vec<TimeRange>, BlockStoreError> {
        if self.format_version != LOG_INDEX_MANIFEST_VERSION {
            return Err(BlockStoreError::InvalidManifestVersion {
                actual: self.format_version,
                expected: LOG_INDEX_MANIFEST_VERSION,
            });
        }
        Ok(self.shards)
    }
}

impl LogIndexManifest {
    fn from_indexes(label_index: &LabelIndex, block_index: &BlockIndex) -> Self {
        let series = label_index
            .series
            .iter()
            .flat_map(|(tenant, series)| {
                series
                    .iter()
                    .map(|(fingerprint, labels)| ManifestSeries {
                        tenant: tenant.clone(),
                        fingerprint: *fingerprint,
                        labels: labels.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks: block_index.blocks.clone(),
        }
    }

    fn from_indexes_for_tenant(
        tenant: &str,
        label_index: &LabelIndex,
        block_index: &BlockIndex,
    ) -> Self {
        let series = label_index
            .series
            .get(tenant)
            .into_iter()
            .flat_map(|series| {
                series.iter().map(|(fingerprint, labels)| ManifestSeries {
                    tenant: tenant.to_string(),
                    fingerprint: *fingerprint,
                    labels: labels.clone(),
                })
            })
            .collect();
        let blocks = block_index
            .blocks
            .iter()
            .filter(|block| block.key.tenant == tenant)
            .cloned()
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks,
        }
    }

    fn from_indexes_for_tenant_shard(
        tenant: &str,
        shard_range: TimeRange,
        label_index: &LabelIndex,
        block_index: &BlockIndex,
    ) -> Self {
        let blocks = block_index
            .blocks
            .iter()
            .filter(|block| {
                block.key.tenant == tenant && block.key.time_range.overlaps(shard_range)
            })
            .cloned()
            .collect::<Vec<_>>();
        let shard_fingerprints = blocks
            .iter()
            .flat_map(|block| block.fingerprints.iter().copied())
            .collect::<BTreeSet<_>>();
        let series = label_index
            .series
            .get(tenant)
            .into_iter()
            .flat_map(|series| {
                series
                    .iter()
                    .filter(|(fingerprint, _)| shard_fingerprints.contains(fingerprint))
                    .map(|(fingerprint, labels)| ManifestSeries {
                        tenant: tenant.to_string(),
                        fingerprint: *fingerprint,
                        labels: labels.clone(),
                    })
            })
            .collect();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series,
            blocks,
        }
    }

    fn into_indexes(self) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        self.into_indexes_filtered(None)
    }

    fn into_indexes_for_tenant(
        self,
        tenant: &str,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        self.into_indexes_filtered(Some(tenant))
    }

    fn into_indexes_filtered(
        self,
        tenant: Option<&str>,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        if self.format_version != LOG_INDEX_MANIFEST_VERSION {
            return Err(BlockStoreError::InvalidManifestVersion {
                actual: self.format_version,
                expected: LOG_INDEX_MANIFEST_VERSION,
            });
        }

        let mut label_index = LabelIndex::default();
        for series in self
            .series
            .into_iter()
            .filter(|series| tenant.is_none_or(|tenant| series.tenant == tenant))
        {
            let actual = label_index.insert_series(series.tenant, series.labels);
            if actual != series.fingerprint {
                return Err(BlockStoreError::ManifestFingerprintMismatch {
                    expected: series.fingerprint,
                    actual,
                });
            }
        }

        let mut block_index = BlockIndex::default();
        for block in self
            .blocks
            .into_iter()
            .filter(|block| tenant.is_none_or(|tenant| block.key.tenant == tenant))
        {
            block_index.insert(block);
        }

        Ok((label_index, block_index))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestSeries {
    tenant: String,
    fingerprint: SeriesFingerprint,
    labels: Labels,
}

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("no log blocks were supplied for DataFusion scan")]
    EmptyBlockScan,
    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        source: regex::Error,
    },
    #[error("invalid block column `{column}`: expected {expected}")]
    InvalidBlockColumn {
        column: &'static str,
        expected: &'static str,
    },
    #[error("invalid time range: start {start_ns} is after end {end_ns}")]
    InvalidTimeRange { start_ns: i64, end_ns: i64 },
    #[error("invalid log index manifest version {actual}; expected {expected}")]
    InvalidManifestVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("log index manifest fingerprint mismatch: expected {expected}, got {actual}")]
    ManifestFingerprintMismatch {
        expected: SeriesFingerprint,
        actual: SeriesFingerprint,
    },
    #[error("block path is not UTF-8: {path:?}")]
    NonUtf8BlockPath { path: PathBuf },
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error("row timestamp {timestamp_ns} is outside block time range {start_ns}-{end_ns}")]
    RowOutsideBlockTimeRange {
        timestamp_ns: i64,
        start_ns: i64,
        end_ns: i64,
    },
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use datafusion::prelude::{col, lit};
    use object_store::{local::LocalFileSystem, path::Path as ObjectPath};

    use super::*;

    #[test]
    fn labels_and_fingerprints_are_canonicalized_with_length_prefixes() {
        let label_set = labels([("service", "api"), ("env", "prod")]);
        let expected = Labels::from([
            ("env".to_string(), "prod".to_string()),
            ("service".to_string(), "api".to_string()),
        ]);

        assert2::assert!(label_set == expected);
        assert2::assert!(series_fingerprint(&expected) != 0);
        assert2::assert!(
            series_fingerprint(&labels([("a", "bc")]))
                != series_fingerprint(&labels([("ab", "c")]))
        );
    }

    #[test]
    fn label_predicates_match_exact_absent_and_anchored_regex_values() {
        let label_set = labels([("service", "api"), ("env", "prod")]);
        let cases = [
            ("exact match", "service", MatchOp::Equal, "api", true),
            ("exact mismatch", "service", MatchOp::Equal, "worker", false),
            (
                "not equal match",
                "service",
                MatchOp::NotEqual,
                "worker",
                true,
            ),
            (
                "absent not equal",
                "cluster",
                MatchOp::NotEqual,
                "east",
                true,
            ),
            (
                "anchored regex match",
                "service",
                MatchOp::RegexEqual,
                "api|worker",
                true,
            ),
            (
                "negative regex match",
                "service",
                MatchOp::RegexNotEqual,
                "api-.+",
                true,
            ),
            (
                "absent negative regex",
                "cluster",
                MatchOp::RegexNotEqual,
                "east",
                true,
            ),
            (
                "anchored regex mismatch",
                "service",
                MatchOp::RegexEqual,
                "p",
                false,
            ),
        ];

        for (_case, name, op, value, expected) in cases {
            assert2::assert!(
                LabelPredicate::new(name, op, value)
                    .unwrap()
                    .matches(&label_set)
                    == expected
            );
        }
        check!(LabelPredicate::new("service", MatchOp::RegexEqual, "[").is_err());
    }

    #[test]
    fn label_index_filters_by_tenant_exact_postings_and_residual_predicates() {
        let mut index = LabelIndex::default();
        let api_prod_labels = labels([("service", "api"), ("env", "prod"), ("region", "east")]);
        let api_stage_labels = labels([("service", "api"), ("env", "stage"), ("region", "west")]);
        let worker_prod_labels =
            labels([("service", "worker"), ("env", "prod"), ("region", "east")]);
        let other_tenant_labels =
            labels([("service", "api"), ("env", "prod"), ("region", "north")]);
        let api_prod = index.insert_series("tenant-a", api_prod_labels.clone());
        let api_stage = index.insert_series("tenant-a", api_stage_labels.clone());
        let worker_prod = index.insert_series("tenant-a", worker_prod_labels.clone());
        let other_tenant = index.insert_series("tenant-b", other_tenant_labels.clone());
        let mut expected_tenant_a_series = vec![
            (api_prod, api_prod_labels.clone()),
            (api_stage, api_stage_labels.clone()),
            (worker_prod, worker_prod_labels.clone()),
        ];
        expected_tenant_a_series.sort_by_key(|(fingerprint, _)| *fingerprint);

        assert2::assert!(index.labels_for("tenant-a", api_prod).cloned() == Some(api_prod_labels));
        assert2::assert!(index.labels_for("tenant-b", api_prod).cloned() == None);
        assert2::assert!(
            index.labels_for("tenant-b", other_tenant).cloned() == Some(other_tenant_labels)
        );
        assert2::assert!(
            index.label_names("tenant-a")
                == BTreeSet::from(["env".into(), "region".into(), "service".into()])
        );
        assert2::assert!(index.label_names("missing") == BTreeSet::new());
        assert2::assert!(
            index.label_values("tenant-a", "service")
                == BTreeSet::from(["api".into(), "worker".into()])
        );
        assert2::assert!(
            index.label_values("tenant-b", "service") == BTreeSet::from(["api".into()])
        );
        assert2::assert!(index.label_values("tenant-a", "missing") == BTreeSet::new());
        assert2::assert!(index.tenant_series("tenant-a") == expected_tenant_a_series);

        let exact_api_prod = [
            LabelPredicate::new("service", MatchOp::Equal, "api").unwrap(),
            LabelPredicate::new("env", MatchOp::Equal, "prod").unwrap(),
        ];
        let exact_and_residual = [
            LabelPredicate::new("service", MatchOp::Equal, "api").unwrap(),
            LabelPredicate::new("env", MatchOp::NotEqual, "prod").unwrap(),
            LabelPredicate::new("region", MatchOp::RegexEqual, "west|central").unwrap(),
        ];
        let no_exact_predicates = [
            LabelPredicate::new("service", MatchOp::RegexEqual, "api|worker").unwrap(),
            LabelPredicate::new("env", MatchOp::RegexNotEqual, "prod").unwrap(),
        ];
        let missing_exact = [LabelPredicate::new("service", MatchOp::Equal, "admin").unwrap()];
        let match_cases = [
            (
                "exact api prod",
                "tenant-a",
                exact_api_prod.as_slice(),
                BTreeSet::from([api_prod]),
            ),
            (
                "exact and residual",
                "tenant-a",
                exact_and_residual.as_slice(),
                BTreeSet::from([api_stage]),
            ),
            (
                "residual predicates only",
                "tenant-a",
                no_exact_predicates.as_slice(),
                BTreeSet::from([api_stage]),
            ),
            (
                "missing exact value",
                "tenant-a",
                missing_exact.as_slice(),
                BTreeSet::new(),
            ),
            (
                "missing tenant",
                "missing",
                exact_api_prod.as_slice(),
                BTreeSet::new(),
            ),
        ];

        for (_name, tenant, predicates, expected) in match_cases {
            assert2::assert!(index.match_series(tenant, predicates) == expected);
        }
    }

    #[test]
    fn time_ranges_and_block_keys_pin_boundary_semantics_and_paths() {
        let first = TimeRange::new(10, 20).unwrap();
        let key = BlockKey::new("tenant-a", 3, 42, 47, first);
        let root = Path::new("/tmp/log-blocks");
        let prefix = ObjectPath::from("observability/logs");
        let object_key = "tenant=tenant-a/partition=3/offsets=42-47/time=10-20.parquet";
        let overlap_cases = [
            ("touching boundary", TimeRange::new(20, 30).unwrap(), true),
            ("strictly before", TimeRange::new(0, 9).unwrap(), false),
            ("strictly after", TimeRange::new(21, 30).unwrap(), false),
        ];

        for (_name, other, expected) in overlap_cases {
            assert2::assert!(first.overlaps(other) == expected);
        }
        check!(TimeRange::new(21, 20).is_err());
        assert2::assert!(key.object_key() == object_key.to_string());
        assert2::assert!(block_path(root, &key) == root.join(object_key));
        assert2::assert!(
            log_block_object_path(&prefix, &key).to_string()
                == format!("observability/logs/{object_key}")
        );
    }

    #[test]
    fn log_block_round_trips_rows_metadata_and_rejects_out_of_range_rows() {
        let dir = tempfile::tempdir().unwrap();
        let api = series_fingerprint(&labels([("service", "api")]));
        let worker = series_fingerprint(&labels([("service", "worker")]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());
        let rows = vec![
            LogRow::new(worker, 150, "worker ok", metadata([("pod", "worker-0")])),
            LogRow::new(
                api,
                100,
                "api start",
                metadata([("pod", "api-0"), ("trace_id", "abc")]),
            ),
            LogRow::new(api, 199, "api stop", StructuredMetadata::new()),
        ];

        let descriptor = write_log_block(dir.path(), &key, rows.clone()).unwrap();
        let loaded_rows = read_log_block(dir.path(), &key).unwrap();

        check!(
            (descriptor.key.clone(), descriptor.fingerprints.clone())
                == (key.clone(), BTreeSet::from([api, worker]))
        );
        check!(descriptor.size_bytes > 0);
        check!(
            loaded_rows
                == vec![
                    LogRow::new(
                        api,
                        100,
                        "api start",
                        metadata([("pod", "api-0"), ("trace_id", "abc")]),
                    ),
                    LogRow::new(api, 199, "api stop", StructuredMetadata::new()),
                    LogRow::new(worker, 150, "worker ok", metadata([("pod", "worker-0")])),
                ]
        );

        for (name, timestamp_ns) in [("below range", 99), ("above range", 200)] {
            let rows = vec![LogRow::new(
                api,
                timestamp_ns,
                "out of range",
                StructuredMetadata::new(),
            )];
            check!(
                matches!(
                    write_log_block(dir.path(), &key, rows),
                    Err(BlockStoreError::RowOutsideBlockTimeRange { timestamp_ns: actual, .. })
                        if actual == timestamp_ns
                ),
                "case {name}"
            );
        }
    }

    #[tokio::test]
    async fn object_store_log_block_round_trips_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let api = series_fingerprint(&labels([("service", "api")]));
        let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap());

        let descriptor = write_log_block_to_object_store(
            &store,
            &prefix,
            &key,
            vec![LogRow::new(
                api,
                150,
                "api ok",
                metadata([("pod", "api-0")]),
            )],
        )
        .await
        .unwrap();
        let loaded_rows = read_log_block_from_object_store(&store, &prefix, &key)
            .await
            .unwrap();

        check!(descriptor.size_bytes > 0);
        check!(
            loaded_rows
                == vec![LogRow::new(
                    api,
                    150,
                    "api ok",
                    metadata([("pod", "api-0")])
                )]
        );
    }

    #[tokio::test]
    async fn object_store_log_index_manifests_round_trip_and_filter_by_tenant() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let fixture = log_index_fixture();

        write_log_index_manifest_to_object_store(
            &store,
            &prefix,
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let (loaded_labels, loaded_blocks) =
            read_log_index_manifest_from_object_store(&store, &prefix)
                .await
                .unwrap();
        check!(
            (loaded_labels, loaded_blocks)
                == (fixture.labels_index.clone(), fixture.block_index.clone())
        );

        write_tenant_log_index_manifest_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let (tenant_labels, tenant_blocks) =
            read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap();
        check!(
            (tenant_labels, tenant_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
        );
    }

    #[tokio::test]
    async fn object_store_log_index_shards_are_listed_filtered_and_merged() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let prefix = ObjectPath::from("observability");
        let fixture = log_index_fixture();

        write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            "tenant-a",
            &[
                TimeRange::new(100, 199).unwrap(),
                TimeRange::new(200, 299).unwrap(),
                TimeRange::new(400, 499).unwrap(),
                TimeRange::new(200, 299).unwrap(),
            ],
            &fixture.labels_index,
            &fixture.block_index,
        )
        .await
        .unwrap();
        let shard_ranges =
            read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap();
        check!(
            shard_ranges
                == vec![
                    TimeRange::new(100, 199).unwrap(),
                    TimeRange::new(200, 299).unwrap(),
                    TimeRange::new(400, 499).unwrap(),
                ]
        );
        check!(
            list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
                .await
                .unwrap()
                == shard_ranges
        );
        let listed_overlap =
            list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
                &store,
                &prefix,
                "tenant-a",
                TimeRange::new(250, 350).unwrap(),
            )
            .await
            .unwrap();
        check!(listed_overlap == vec![TimeRange::new(200, 299).unwrap()]);

        let (shard_labels, shard_blocks) = read_tenant_log_index_shards_from_object_store(
            &store,
            &prefix,
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
        )
        .await
        .unwrap();
        check!(
            (shard_labels, shard_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
        );
    }

    #[test]
    fn datafusion_provider_reports_filter_pushdown_and_planned_blocks() {
        let block = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([7]),
        );
        let provider = LogBlockTableProvider::try_new_object_store(
            Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
            ObjectPath::from("logs"),
            std::slice::from_ref(&block),
        )
        .unwrap();
        let timestamp_filter = col("timestamp_ns").gt_eq(lit(100_i64));
        let fingerprint_filter = col("series_fingerprint").eq(lit(7_u64));
        let line_filter = col("line").eq(lit("api ok"));
        let metadata_filter = col("structured_metadata").eq(lit("api ok"));
        let literal_filter = lit(true);
        let filter_cases = [
            (
                "timestamp",
                &timestamp_filter,
                TableProviderFilterPushDown::Inexact,
            ),
            (
                "fingerprint",
                &fingerprint_filter,
                TableProviderFilterPushDown::Inexact,
            ),
            ("line", &line_filter, TableProviderFilterPushDown::Inexact),
            (
                "metadata",
                &metadata_filter,
                TableProviderFilterPushDown::Unsupported,
            ),
            (
                "literal",
                &literal_filter,
                TableProviderFilterPushDown::Unsupported,
            ),
        ];

        check!(provider.planned_blocks() == std::slice::from_ref(&block));
        for (_name, filter, expected) in filter_cases {
            assert2::assert!(
                provider.supports_filters_pushdown(&[filter]).unwrap() == vec![expected]
            );
        }
        check!(
            LogBlockTableProvider::try_new_object_store(
                Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
                ObjectPath::from("logs"),
                &[],
            )
            .is_err()
        );
    }

    #[test]
    fn block_index_replaces_sorts_and_filters_blocks() {
        let first = BlockDescriptor::new_with_size(
            BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
            BTreeSet::from([2]),
            10,
        );
        let second = BlockDescriptor::new_with_size(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([1]),
            20,
        );
        let replacement_second =
            BlockDescriptor::new_with_size(second.key.clone(), BTreeSet::from([1, 3]), 30);
        let other_tenant = BlockDescriptor::new(
            BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([1]),
        );
        let mut index = BlockIndex::default();

        index.insert(first.clone());
        index.insert(second);
        index.insert(other_tenant.clone());
        index.insert(replacement_second.clone());

        let expected_all = vec![replacement_second.clone(), first.clone(), other_tenant];
        let match_cases = [
            (
                "replacement fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[1][..],
                vec![replacement_second.clone()],
            ),
            (
                "first fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[2][..],
                vec![first.clone()],
            ),
            (
                "all tenant blocks",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[][..],
                vec![replacement_second, first],
            ),
            (
                "missing tenant",
                "tenant-c",
                TimeRange::new(150, 250).unwrap(),
                &[1][..],
                vec![],
            ),
            (
                "outside time range",
                "tenant-a",
                TimeRange::new(300, 400).unwrap(),
                &[][..],
                vec![],
            ),
            (
                "missing fingerprint",
                "tenant-a",
                TimeRange::new(150, 250).unwrap(),
                &[99][..],
                vec![],
            ),
        ];

        check!(index.blocks() == expected_all.as_slice());
        for (name, tenant, time_range, fingerprints, expected) in match_cases {
            check!(
                index.match_blocks(tenant, time_range, fingerprints) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn manifest_conversions_filter_validate_versions_and_fingerprints() {
        let fixture = log_index_fixture();
        let expected_tenant_labels = expected_tenant_a_label_index();
        let expected_tenant_blocks =
            block_index_from([fixture.first.clone(), fixture.second.clone()]);
        let api = series_fingerprint(&labels([("service", "api")]));

        let full = LogIndexManifest::from_indexes(&fixture.labels_index, &fixture.block_index);
        let (full_labels, full_blocks) = full.into_indexes().unwrap();
        check!(
            (full_labels, full_blocks)
                == (fixture.labels_index.clone(), fixture.block_index.clone())
        );

        let tenant_manifest = LogIndexManifest::from_indexes_for_tenant(
            "tenant-a",
            &fixture.labels_index,
            &fixture.block_index,
        );
        let (tenant_labels, tenant_blocks) =
            tenant_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!((tenant_labels, tenant_blocks) == (expected_tenant_labels, expected_tenant_blocks));

        let shard_manifest = LogIndexManifest::from_indexes_for_tenant_shard(
            "tenant-a",
            TimeRange::new(150, 250).unwrap(),
            &fixture.labels_index,
            &fixture.block_index,
        );
        let (shard_labels, shard_blocks) =
            shard_manifest.into_indexes_for_tenant("tenant-a").unwrap();
        check!(
            (shard_labels, shard_blocks)
                == (
                    expected_tenant_a_label_index(),
                    block_index_from([fixture.first, fixture.second]),
                )
        );

        let bad_version = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION + 1,
            series: Vec::new(),
            blocks: Vec::new(),
        };
        check!(matches!(
            bad_version.into_indexes(),
            Err(BlockStoreError::InvalidManifestVersion { .. })
        ));
        let bad_fingerprint = LogIndexManifest {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            series: vec![ManifestSeries {
                tenant: "tenant-a".to_string(),
                fingerprint: api + 1,
                labels: labels([("service", "api")]),
            }],
            blocks: Vec::new(),
        };
        check!(matches!(
            bad_fingerprint.into_indexes(),
            Err(BlockStoreError::ManifestFingerprintMismatch { .. })
        ));
    }

    #[test]
    fn shard_catalog_and_path_helpers_sort_parse_and_validate_ranges() {
        let prefix = ObjectPath::from("observability");
        let shard_prefix = log_tenant_index_shards_object_prefix(&prefix, "tenant-a");
        let first = TimeRange::new(-10, 20).unwrap();
        let second = TimeRange::new(30, 40).unwrap();
        let catalog = LogIndexShardCatalog::new(&[second, first, second]);

        check!(catalog.into_shards().unwrap() == vec![first, second]);
        check!(matches!(
            (LogIndexShardCatalog {
                format_version: LOG_INDEX_MANIFEST_VERSION + 1,
                shards: vec![first],
            })
            .into_shards(),
            Err(BlockStoreError::InvalidManifestVersion { .. })
        ));
        let path_cases = [
            (
                "global manifest",
                log_index_manifest_object_path(&prefix),
                "observability/index/logs/manifest.json",
            ),
            (
                "tenant shard catalog",
                log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a"),
                "observability/tenant=tenant-a/index/logs/shards/manifest.json",
            ),
            (
                "tenant shard manifest",
                log_tenant_index_shard_manifest_object_path(&prefix, "tenant-a", first),
                "observability/tenant=tenant-a/index/logs/shards/time=-10-20/manifest.json",
            ),
            (
                "tenant shard list offset",
                log_tenant_index_shard_list_offset_object_path(
                    &prefix,
                    "tenant-a",
                    TimeRange::new(100, 199).unwrap(),
                ),
                "observability/tenant=tenant-a/index/logs/shards/time=1",
            ),
        ];

        for (_name, actual, expected) in path_cases {
            assert2::assert!(actual.to_string() == expected);
        }
        check!(
            log_tenant_index_shard_list_offset_start_ns(TimeRange::new(100, 100).unwrap()) == 99
        );
        let parse_cases = [
            (
                "valid shard manifest",
                shard_prefix
                    .clone()
                    .join("time=-10-20")
                    .join("manifest.json"),
                Some(first),
            ),
            (
                "reversed time range",
                shard_prefix
                    .clone()
                    .join("time=20-10")
                    .join("manifest.json"),
                None,
            ),
            (
                "wrong file name",
                shard_prefix.clone().join("time=10-20").join("data.json"),
                None,
            ),
            (
                "extra path component",
                shard_prefix
                    .clone()
                    .join("time=10-20")
                    .join("manifest.json")
                    .join("extra"),
                None,
            ),
            (
                "wrong prefix",
                ObjectPath::from("other/time=10-20/manifest.json"),
                None,
            ),
        ];

        for (name, location, expected) in parse_cases {
            check!(
                parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &location)
                    == expected,
                "case {name}"
            );
        }
    }

    struct LogIndexFixture {
        labels_index: LabelIndex,
        block_index: BlockIndex,
        first: BlockDescriptor,
        second: BlockDescriptor,
    }

    fn log_index_fixture() -> LogIndexFixture {
        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series("tenant-a", labels([("service", "api")]));
        let worker = labels_index.insert_series("tenant-a", labels([("service", "worker")]));
        let other = labels_index.insert_series("tenant-b", labels([("service", "api")]));
        let first = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([api]),
        );
        let second = BlockDescriptor::new(
            BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
            BTreeSet::from([worker]),
        );
        let other_block = BlockDescriptor::new(
            BlockKey::new("tenant-b", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
            BTreeSet::from([other]),
        );
        let mut block_index = BlockIndex::default();
        block_index.insert(first.clone());
        block_index.insert(second.clone());
        block_index.insert(other_block);

        LogIndexFixture {
            labels_index,
            block_index,
            first,
            second,
        }
    }

    fn expected_tenant_a_label_index() -> LabelIndex {
        let mut index = LabelIndex::default();
        index.insert_series("tenant-a", labels([("service", "api")]));
        index.insert_series("tenant-a", labels([("service", "worker")]));
        index
    }

    fn block_index_from<const N: usize>(blocks: [BlockDescriptor; N]) -> BlockIndex {
        let mut index = BlockIndex::default();
        for block in blocks {
            index.insert(block);
        }
        index
    }

    fn metadata<const N: usize>(items: [(&str, &str); N]) -> StructuredMetadata {
        items
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }
}
