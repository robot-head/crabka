//! Shared columnar block-store primitives for Crabka observability signals.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, Int64Array, MapArray, MapBuilder, RecordBatch, StringArray, StringBuilder, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::catalog::Session;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::provider::TableProviderFilterPushDown;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::errors::ParquetError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
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
                    predicates.iter().all(|predicate| predicate.matches(labels))
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
    store
        .put(&log_block_object_path(prefix, key), payload.into())
        .await?;

    Ok(BlockDescriptor::new_with_size(
        key.clone(),
        rows.iter().map(|row| row.series_fingerprint).collect(),
        size_bytes,
    ))
}

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

pub fn read_log_index_manifest(
    root: impl AsRef<Path>,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let manifest: LogIndexManifest =
        serde_json::from_reader(File::open(log_index_manifest_path(root))?)?;
    manifest.into_indexes()
}

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

pub async fn read_tenant_log_index_shards_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let shard_ranges =
        read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await?;
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
    prefix
        .clone()
        .join(format!("tenant={tenant}"))
        .join("index")
        .join("logs")
        .join("shards")
        .join("manifest.json")
}

#[must_use]
pub fn log_tenant_index_shard_manifest_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> ObjectPath {
    prefix
        .clone()
        .join(format!("tenant={tenant}"))
        .join("index")
        .join("logs")
        .join("shards")
        .join(format!(
            "time={}-{}",
            shard_range.start_ns, shard_range.end_ns
        ))
        .join("manifest.json")
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
