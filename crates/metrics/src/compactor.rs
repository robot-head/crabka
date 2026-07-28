//! Deterministic compactor core for metrics WAL records.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, MapBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use crabka_blockstore::{BlockMeta, BlockStoreError, BlockWriter};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord};
use crabka_ids::{Offset, PartitionIndex};
use crabka_telemetry::propagation::{TRACEPARENT, set_remote_parent};
use crabka_units::prelude::*;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::Instrument as _;

use crate::{
    NativeHistogram, encode_float_samples, encode_native_histograms,
    histogram::HistogramCodecError,
    schema::{exemplar_schema, metadata_schema},
    wal::{SamplePayload, WalError, WalExemplar, WalRecord},
};

/// One sorted float sample row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct FloatRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// One sorted native histogram row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogramRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub hist: NativeHistogram,
}

/// One sorted exemplar sidecar row.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub labels: Vec<(String, String)>,
}

/// One metric metadata row ready for indexing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRow {
    pub fingerprint: u64,
    pub metric_family_name: String,
    pub metric_type: String,
    pub help: String,
    pub unit: String,
}

/// Compacted rows for a single tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantCompactionRows {
    pub tenant: String,
    pub series_labels: BTreeMap<u64, crabka_blockstore::Labels>,
    pub float_rows: Vec<FloatRow>,
    pub histogram_rows: Vec<NativeHistogramRow>,
    pub exemplar_rows: Vec<ExemplarRow>,
    pub metadata_rows: Vec<MetadataRow>,
}

/// One series label set persisted in a compaction manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionSeriesLabels {
    pub fingerprint: u64,
    pub labels: crabka_blockstore::Labels,
}

/// Arrow batches produced from one tenant's compacted rows.
pub struct TenantBatches {
    pub float: Option<RecordBatch>,
    pub native_histograms: Option<RecordBatch>,
    pub exemplars: Option<RecordBatch>,
    pub metadata: Option<RecordBatch>,
}

/// Metric block payload kind used in deterministic object keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MetricBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
}

impl MetricBlockKind {
    const fn object_path(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::NativeHistograms => "native-histograms",
            Self::Exemplars => "exemplars",
            Self::Metadata => "metadata",
        }
    }
}

/// Deterministic object names for one compacted block and its index sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionObjectPlan {
    pub block_key: String,
    pub index_key: String,
    pub first_offset: i64,
    pub last_offset: i64,
    pub row_count: usize,
}

/// One persisted metric block and its committed index sidecar description.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactedBlockWrite {
    pub kind: MetricBlockKind,
    pub block_meta: BlockMeta,
    pub manifest: CompactionIndexManifest,
}

/// One encoded WAL record fetched by the compactor for a single topic partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionWalRecord {
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Vec<u8>,
}

/// Offset to commit for one compacted WAL partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset: the next offset after the last durable record.
    pub offset: Offset,
}

/// Result of processing one partition's compaction window.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionWindowResult {
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offset: Option<CompactionPartitionOffset>,
}

/// Result of processing a polled compaction batch across assigned partitions.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionBatchResult {
    pub partition_results: Vec<CompactionWindowResult>,
    pub writes: Vec<CompactedBlockWrite>,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}

/// Result of one compactor consumer poll and processing pass.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionPollResult {
    pub polled_records: usize,
    pub compacted_records: usize,
    pub batch: CompactionBatchResult,
}

/// Runtime knobs for the compactor polling loop.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionLoopConfig {
    pub wal_topic: String,
    pub poll_timeout: Time,
    /// Flush the accumulated buffer once this many WAL records are buffered.
    pub flush_max_rows: usize,
    /// Flush the accumulated buffer once its oldest record reaches this age.
    pub flush_max_age: Time,
}

/// Summary returned after a compactor loop exits.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionLoopResult {
    pub polls: usize,
    pub polled_records: usize,
    pub compacted_records: usize,
    pub writes: usize,
    pub committed_offsets: Vec<CompactionPartitionOffset>,
}

/// Counts of stale compacted objects removed by a retention sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionRetentionStats {
    pub manifests_scanned: usize,
    pub manifests_deleted: usize,
    pub blocks_deleted: usize,
}

/// Default number of buffered WAL records that triggers a compaction flush.
pub const DEFAULT_FLUSH_MAX_ROWS: usize = 50_000;

/// Default maximum age the oldest buffered WAL record may reach before a flush.
pub const DEFAULT_FLUSH_MAX_AGE: Time = minutes(1);

const COMPACTION_OBJECT_PREFIX: &str = "metrics";

/// Configuration for the metrics compactor role.
#[derive(Clone, Debug)]
pub struct MetricsCompactorConfig {
    pub bootstrap: String,
    pub group_id: String,
    pub client_id: String,
    pub wal_topic: String,
    pub poll_timeout: Time,
    pub auto_offset_reset: AutoOffsetReset,
    /// Flush the accumulated buffer once this many WAL records are buffered.
    pub flush_max_rows: usize,
    /// Flush the accumulated buffer once its oldest record reaches this age.
    pub flush_max_age: Time,
}

/// Runtime handles assembled for the compactor role.
pub struct MetricsCompactorRuntime {
    pub block_writer: BlockWriter,
    pub index_sink: ObjectStoreCompactionIndexSink,
    pub loop_config: CompactionLoopConfig,
}

/// Compaction index sidecar written next to a metric block object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactionIndexManifest {
    pub tenant: String,
    pub kind: MetricBlockKind,
    pub block_key: String,
    pub index_key: String,
    pub first_offset: i64,
    pub last_offset: i64,
    pub row_count: usize,
    pub min_ts: i64,
    pub max_ts: i64,
    pub fingerprints: Vec<u64>,
    pub series: Vec<CompactionSeriesLabels>,
}

/// Compaction index sidecar codec errors.
#[derive(Debug, thiserror::Error)]
pub enum CompactionIndexError {
    #[error("compaction index encode failed: {0}")]
    Encode(String),

    #[error("compaction index decode failed: {0}")]
    Decode(String),

    #[error("compaction index object-store write failed: {0}")]
    ObjectStore(String),
}

/// Errors raised while deleting compacted metric objects outside retention.
#[derive(Debug, thiserror::Error)]
pub enum CompactionRetentionError {
    #[error("compaction retention object-store operation failed: {0}")]
    ObjectStore(String),

    #[error("compaction retention manifest key mismatch: listed `{listed}`, manifest `{manifest}`")]
    ManifestKeyMismatch { listed: String, manifest: String },

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}

/// Errors raised while configuring the metrics compactor role.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorConfigError {
    #[error("metrics compactor config `{field}` must not be empty")]
    Empty { field: &'static str },

    #[error("metrics compactor poll_timeout must be non-zero")]
    ZeroPollTimeout,

    #[error("metrics compactor flush_max_rows must be non-zero")]
    ZeroFlushMaxRows,
}

/// Errors raised while writing compacted metric blocks.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWriteError {
    #[error(transparent)]
    Encode(#[from] HistogramCodecError),

    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),

    #[error(transparent)]
    Index(#[from] CompactionIndexError),
}

/// Errors raised while committing compactor WAL offsets.
#[derive(Debug, thiserror::Error)]
pub enum CompactionCommitError {
    #[error("compaction offset commit failed: {0}")]
    Commit(String),
}

/// Errors raised while adapting Kafka consumer records to compactor WAL records.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerRecordError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },
}

/// Errors raised by a consumer offset commit.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerCommitError {
    #[error("consumer commit failed: {0}")]
    Commit(String),
}

/// Errors raised by a consumer poll.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerPollError {
    #[error("consumer poll failed: {0}")]
    Poll(String),
}

/// Errors raised while processing a compaction WAL window.
#[derive(Debug, thiserror::Error)]
pub enum CompactionWindowError {
    #[error("compaction window spans multiple partitions: {first} and {second}")]
    MultiplePartitions {
        first: PartitionIndex,
        second: PartitionIndex,
    },

    #[error(transparent)]
    Wal(#[from] WalError),

    #[error(transparent)]
    Write(#[from] CompactionWriteError),

    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}

/// Errors raised by one compactor poll/process pass.
#[derive(Debug, thiserror::Error)]
pub enum CompactionPollError {
    #[error(transparent)]
    Poll(#[from] CompactionConsumerPollError),

    #[error(transparent)]
    ConsumerRecord(#[from] CompactionConsumerRecordError),

    #[error(transparent)]
    Window(#[from] CompactionWindowError),
}

/// Errors raised while constructing live compactor role dependencies.
#[derive(Debug, thiserror::Error)]
pub enum MetricsCompactorBuildError {
    #[error(transparent)]
    Config(#[from] MetricsCompactorConfigError),

    #[error("metrics compactor consumer build failed: {0}")]
    Consumer(String),
}

/// Sink for compaction index sidecars.
#[async_trait]
pub trait CompactionIndexSink: Send + Sync {
    async fn write_manifest(
        &self,
        manifest: &CompactionIndexManifest,
    ) -> Result<(), CompactionIndexError>;
}

/// Object-store backed compaction index sidecar sink.
pub struct ObjectStoreCompactionIndexSink {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCompactionIndexSink {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Read and decode a previously written compaction index manifest.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn read_manifest(
        &self,
        index_key: &str,
    ) -> Result<CompactionIndexManifest, CompactionIndexError> {
        let bytes = self
            .store
            .get(&Path::from(index_key))
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?;
        CompactionIndexManifest::decode(&bytes)
    }
}

#[async_trait]
impl CompactionIndexSink for ObjectStoreCompactionIndexSink {
    async fn write_manifest(
        &self,
        manifest: &CompactionIndexManifest,
    ) -> Result<(), CompactionIndexError> {
        let bytes = manifest.encode()?;
        self.store
            .put(
                &Path::from(manifest.index_key.clone()),
                PutPayload::from(bytes),
            )
            .await
            .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?;
        Ok(())
    }
}

/// Delete compacted metric blocks whose index manifest ends before the retention cutoff.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn enforce_compaction_retention(
    store: Arc<dyn ObjectStore>,
    now_ms: i64,
    retention: Time,
) -> Result<CompactionRetentionStats, CompactionRetentionError> {
    if retention <= Time::ZERO {
        return Ok(CompactionRetentionStats::default());
    }

    let cutoff_ms = now_ms.saturating_sub(retention.millis_i64());
    let mut objects = store
        .list(Some(&Path::from(COMPACTION_OBJECT_PREFIX)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let mut stats = CompactionRetentionStats::default();
    for object in objects {
        let key = object.location.as_ref();
        if !std::path::Path::new(key)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
        {
            continue;
        }
        stats.manifests_scanned += 1;
        let bytes = store
            .get(&object.location)
            .await
            .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| CompactionRetentionError::ObjectStore(error.to_string()))?;
        let manifest = CompactionIndexManifest::decode(&bytes)?;
        if manifest.index_key != key {
            return Err(CompactionRetentionError::ManifestKeyMismatch {
                listed: key.to_string(),
                manifest: manifest.index_key,
            });
        }
        if manifest.max_ts >= cutoff_ms {
            continue;
        }

        if delete_if_exists(&store, &Path::from(manifest.index_key.clone())).await? {
            stats.manifests_deleted += 1;
        }
        if delete_if_exists(&store, &Path::from(manifest.block_key.clone())).await? {
            stats.blocks_deleted += 1;
        }
    }

    Ok(stats)
}

async fn delete_if_exists(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
) -> Result<bool, CompactionRetentionError> {
    match store.delete(location).await {
        Ok(()) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(CompactionRetentionError::ObjectStore(error.to_string())),
    }
}

impl MetricsCompactorConfig {
    /// Configuration defaults for the metrics compactor role.
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            group_id: "crabka-metrics-compactor".to_string(),
            client_id: "crabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: secs(1),
            auto_offset_reset: AutoOffsetReset::Earliest,
            flush_max_rows: DEFAULT_FLUSH_MAX_ROWS,
            flush_max_age: DEFAULT_FLUSH_MAX_AGE,
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn validate(&self) -> Result<(), MetricsCompactorConfigError> {
        validate_non_empty("bootstrap", &self.bootstrap)?;
        validate_non_empty("group_id", &self.group_id)?;
        validate_non_empty("client_id", &self.client_id)?;
        validate_non_empty("wal_topic", &self.wal_topic)?;
        if self.poll_timeout <= Time::ZERO {
            return Err(MetricsCompactorConfigError::ZeroPollTimeout);
        }
        if self.flush_max_rows == 0 {
            return Err(MetricsCompactorConfigError::ZeroFlushMaxRows);
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn build_runtime(
        &self,
        store: Arc<dyn ObjectStore>,
    ) -> Result<MetricsCompactorRuntime, MetricsCompactorConfigError> {
        self.validate()?;
        Ok(MetricsCompactorRuntime {
            block_writer: BlockWriter::new(store.clone()),
            index_sink: ObjectStoreCompactionIndexSink::new(store),
            loop_config: CompactionLoopConfig {
                wal_topic: self.wal_topic.clone(),
                poll_timeout: self.poll_timeout,
                flush_max_rows: self.flush_max_rows,
                flush_max_age: self.flush_max_age,
            },
        })
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn build_consumer(&self) -> Result<Consumer, MetricsCompactorBuildError> {
        self.validate()?;
        Consumer::builder()
            .bootstrap(self.bootstrap.clone())
            .group_id(self.group_id.clone())
            .client_id(self.client_id.clone())
            .auto_offset_reset(self.auto_offset_reset)
            .subscribe([self.wal_topic.clone()])
            .build()
            .await
            .map_err(|error| consumer_build_error(&error))
    }
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), MetricsCompactorConfigError> {
    if value.is_empty() {
        Err(MetricsCompactorConfigError::Empty { field })
    } else {
        Ok(())
    }
}

fn consumer_build_error(error: &ConsumerError) -> MetricsCompactorBuildError {
    MetricsCompactorBuildError::Consumer(error.to_string())
}

/// Commits compacted WAL offsets after block and index writes are durable.
#[async_trait]
pub trait CompactionOffsetCommitter: Send + Sync {
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError>;
}

/// Minimal consumer commit surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerCommit: Send + Sync {
    async fn commit_sync(&self) -> Result<(), CompactionConsumerCommitError>;
}

/// Minimal mutable consumer commit surface for service loops that poll and commit
/// through the same handle.
#[async_trait]
pub trait CompactionConsumerCommitMut: Send {
    async fn commit_sync_mut(&mut self) -> Result<(), CompactionConsumerCommitError>;
}

/// Minimal consumer poll surface needed by the compactor loop.
#[async_trait]
pub trait CompactionConsumerPoll: Send {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError>;
}

#[async_trait]
impl CompactionConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, CompactionConsumerPollError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| CompactionConsumerPollError::Poll(error.to_string()))
    }
}

#[async_trait]
impl CompactionConsumerCommit for Consumer {
    async fn commit_sync(&self) -> Result<(), CompactionConsumerCommitError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}

#[async_trait]
impl CompactionConsumerCommitMut for Consumer {
    async fn commit_sync_mut(&mut self) -> Result<(), CompactionConsumerCommitError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| CompactionConsumerCommitError::Commit(error.to_string()))
    }
}

/// Adapter that commits the underlying consumer after durable compaction writes.
pub struct CompactionConsumerCommitter<'a, C: ?Sized> {
    consumer: &'a C,
}

impl<'a, C: ?Sized> CompactionConsumerCommitter<'a, C> {
    #[must_use]
    pub const fn new(consumer: &'a C) -> Self {
        Self { consumer }
    }
}

#[async_trait]
impl<C> CompactionOffsetCommitter for CompactionConsumerCommitter<'_, C>
where
    C: CompactionConsumerCommit + ?Sized,
{
    async fn commit_offsets(
        &self,
        offsets: &[CompactionPartitionOffset],
    ) -> Result<(), CompactionCommitError> {
        if offsets.is_empty() {
            return Ok(());
        }
        self.consumer
            .commit_sync()
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))
    }
}

impl CompactionIndexManifest {
    #[must_use]
    pub fn from_plan(
        tenant: impl Into<String>,
        kind: MetricBlockKind,
        plan: &CompactionObjectPlan,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            kind,
            block_key: plan.block_key.clone(),
            index_key: plan.index_key.clone(),
            first_offset: plan.first_offset,
            last_offset: plan.last_offset,
            row_count: plan.row_count,
            min_ts: 0,
            max_ts: 0,
            fingerprints: Vec::new(),
            series: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_block_meta(
        kind: MetricBlockKind,
        plan: &CompactionObjectPlan,
        meta: &BlockMeta,
        series: Vec<CompactionSeriesLabels>,
    ) -> Self {
        Self {
            tenant: meta.tenant.clone(),
            kind,
            block_key: plan.block_key.clone(),
            index_key: plan.index_key.clone(),
            first_offset: plan.first_offset,
            last_offset: plan.last_offset,
            row_count: meta.row_count,
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            fingerprints: meta.fingerprints.clone(),
            series,
        }
    }

    /// Encode via `serde-wincode`, matching the WAL record codec.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn encode(&self) -> Result<Vec<u8>, CompactionIndexError> {
        <serde_wincode::SerdeCompat<CompactionIndexManifest> as wincode::Serialize>::serialize(self)
            .map_err(|error| CompactionIndexError::Encode(error.to_string()))
    }

    /// Decode a [`CompactionIndexManifest`] from its `serde-wincode` bytes.
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn decode(bytes: &[u8]) -> Result<Self, CompactionIndexError> {
        <serde_wincode::SerdeCompat<CompactionIndexManifest> as wincode::Deserialize>::deserialize(
            bytes,
        )
        .map_err(|error| CompactionIndexError::Decode(error.to_string()))
    }
}

/// Deterministic object key for one tenant/kind/WAL offset compaction window.
///
/// The key is a pure function of `(tenant, kind, first_offset, last_offset)`, so
/// re-compacting the *same* offset range writes the *same* object key (an
/// idempotent overwrite). The accumulate-then-flush loop does NOT guarantee the
/// same range is re-formed after a crash-before-commit: the flushed window
/// depends on poll batching and the age timer, so a re-run may write the same
/// records under a *different* key. That is at-least-once delivery, not
/// byte-identical idempotency — but offset-overlapping duplicate blocks carry
/// identical `(series, ts, value)` rows and are deduplicated at query time by the
/// timestamp-keyed `PromQL` operator engine, so they do not double-count.
#[must_use]
pub fn compaction_object_key(
    tenant: &str,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> String {
    format!(
        "metrics/{}/{}/{:020}-{:020}.parquet",
        escape_object_path_segment(tenant),
        kind.object_path(),
        first_offset,
        last_offset
    )
}

/// Deterministic object key for one tenant/kind/WAL partition/offset window.
#[must_use]
pub fn compaction_partition_object_key(
    tenant: &str,
    kind: MetricBlockKind,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> String {
    format!(
        "metrics/{}/{}/partition={:010}/{:020}-{:020}.parquet",
        escape_object_path_segment(tenant),
        kind.object_path(),
        partition.get(),
        first_offset,
        last_offset
    )
}

/// Deterministic block and index object keys for one compaction window.
// cargo-mutants: covered by `compaction_object_plan_pairs_block_and_index_keys`.
#[cfg_attr(test, mutants::skip)]
#[must_use]
pub fn compaction_object_plan(
    tenant: &str,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let block_key = compaction_object_key(tenant, kind, first_offset, last_offset);
    let index_key = compaction_index_key(&block_key);
    CompactionObjectPlan {
        block_key,
        index_key,
        first_offset,
        last_offset,
        row_count: 0,
    }
}

/// Deterministic block and index object keys for one partition compaction window.
// cargo-mutants: this is a thin partition-key wrapper over covered key helpers.
#[cfg_attr(test, mutants::skip)]
#[must_use]
pub fn compaction_partition_object_plan(
    tenant: &str,
    kind: MetricBlockKind,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let block_key =
        compaction_partition_object_key(tenant, kind, partition, first_offset, last_offset);
    let index_key = compaction_index_key(&block_key);
    CompactionObjectPlan {
        block_key,
        index_key,
        first_offset,
        last_offset,
        row_count: 0,
    }
}

// cargo-mutants: suffix conversion is covered by object-plan and manifest tests.
#[cfg_attr(test, mutants::skip)]
fn compaction_index_key(block_key: &str) -> String {
    block_key.strip_suffix(".parquet").map_or_else(
        || format!("{block_key}.index"),
        |prefix| format!("{prefix}.index"),
    )
}

/// Convert polled consumer records from the metrics WAL topic into compactor inputs.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn compaction_wal_records_from_consumer_records(
    wal_topic: &str,
    records: &[ConsumerRecord],
) -> Result<Vec<CompactionWalRecord>, CompactionConsumerRecordError> {
    let mut out = Vec::new();
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        let value = record
            .value
            .as_ref()
            .ok_or(CompactionConsumerRecordError::MissingValue {
                partition: PartitionIndex(record.partition),
                offset: Offset(record.offset),
            })?;
        out.push(CompactionWalRecord {
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: value.to_vec(),
        });
    }
    Ok(out)
}

/// Deterministic object plan plus row-count evidence for one encoded block kind.
#[must_use]
pub fn compaction_object_plan_for_rows(
    rows: &TenantCompactionRows,
    kind: MetricBlockKind,
    first_offset: i64,
    last_offset: i64,
) -> CompactionObjectPlan {
    let mut plan = compaction_object_plan(&rows.tenant, kind, first_offset, last_offset);
    plan.row_count = match kind {
        MetricBlockKind::Float => rows.float_rows.len(),
        MetricBlockKind::NativeHistograms => rows.histogram_rows.len(),
        MetricBlockKind::Exemplars => rows.exemplar_rows.len(),
        MetricBlockKind::Metadata => rows.metadata_rows.len(),
    };
    plan
}

/// Write all non-empty block kinds for a compacted tenant window.
///
/// The block object is written before the corresponding index sidecar, so a
/// caller can safely commit WAL consumer offsets only after this function
/// returns successfully.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn write_compacted_tenant_blocks<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    write_compacted_tenant_blocks_with_partition(
        block_writer,
        index_sink,
        rows,
        None,
        first_offset,
        last_offset,
    )
    .await
}

/// Write all non-empty block kinds for a compacted tenant partition window.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn write_compacted_tenant_partition_blocks<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    write_compacted_tenant_blocks_with_partition(
        block_writer,
        index_sink,
        rows,
        Some(partition),
        first_offset,
        last_offset,
    )
    .await
}

async fn write_compacted_tenant_blocks_with_partition<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    partition: Option<PartitionIndex>,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    let batches = encode_tenant_batches(rows)?;
    let mut writes = Vec::new();

    if let Some(batch) = batches.float {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Float,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Float),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.native_histograms {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::NativeHistograms,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::NativeHistograms),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.exemplars {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Exemplars,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Exemplars),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.metadata {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Metadata,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Metadata),
                },
            )
            .await?,
        );
    }

    Ok(writes)
}

/// Process a polled compaction batch by partition, preserving per-partition commits.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn process_compaction_record_batch<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
) -> Result<CompactionBatchResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let mut by_partition = BTreeMap::<PartitionIndex, Vec<CompactionWalRecord>>::new();
    for record in records {
        by_partition
            .entry(record.partition)
            .or_default()
            .push(record.clone());
    }

    let mut partition_results = Vec::new();
    let mut writes = Vec::new();
    let mut committed_offsets = Vec::new();
    // Write every partition's block + index sidecar durably BEFORE committing any
    // offsets. The production committer (`CompactionConsumerCommitter`) commits
    // the whole assignment's offsets regardless of the per-partition offset
    // passed, so committing per-partition would advance partitions whose blocks
    // are not yet written; a later partition's write failure would then skip
    // those un-written records — silent data loss. One commit after all writes
    // only advances past fully-durable data; any write error returns before the
    // commit so the next poll re-reads from the last committed offset
    // (at-least-once).
    for partition_records in by_partition.into_values() {
        let result =
            write_compaction_partition_window(block_writer, index_sink, &partition_records).await?;
        writes.extend(result.writes.clone());
        if let Some(offset) = &result.committed_offset {
            committed_offsets.push(offset.clone());
        }
        partition_results.push(result);
    }

    if !committed_offsets.is_empty() {
        committer.commit_offsets(&committed_offsets).await?;
    }

    Ok(CompactionBatchResult {
        partition_results,
        writes,
        committed_offsets,
    })
}

/// Poll the metrics WAL consumer once, compact returned records, and commit on success.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_once<P, S, C>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    wal_topic: &str,
    timeout: Time,
) -> Result<CompactionPollResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let records = poller.poll(timeout).await?;
    let polled_records = records.len();
    let wal_records = compaction_wal_records_from_consumer_records(wal_topic, &records)?;
    let compacted_records = wal_records.len();
    let batch =
        process_compaction_record_batch(block_writer, index_sink, committer, &wal_records).await?;

    Ok(CompactionPollResult {
        polled_records,
        compacted_records,
        batch,
    })
}

/// Monotonic clock used by the compaction loop to age the accumulation buffer.
///
/// Abstracted so flush-by-age can be driven deterministically in tests without
/// real wall-clock waits.
pub trait CompactionClock {
    fn now(&self) -> std::time::Instant;
}

/// Real monotonic clock backed by [`std::time::Instant::now`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemCompactionClock;

impl CompactionClock for SystemCompactionClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

/// Accumulates polled WAL records across polls so the compactor can flush one
/// larger block per threshold instead of one tiny block per poll.
///
/// The buffer retains each record's consumer offset/partition metadata, so the
/// flush keys blocks by the buffered offset range exactly as a single-poll write
/// would. The oldest record's arrival time anchors the age-based flush deadline.
struct CompactionBuffer {
    records: Vec<CompactionWalRecord>,
    oldest_arrival: Option<std::time::Instant>,
}

impl CompactionBuffer {
    const fn new() -> Self {
        Self {
            records: Vec::new(),
            oldest_arrival: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Append newly polled records, anchoring the age deadline at the first
    /// record to enter an empty buffer.
    fn extend(&mut self, records: Vec<CompactionWalRecord>, now: std::time::Instant) {
        if records.is_empty() {
            return;
        }
        if self.oldest_arrival.is_none() {
            self.oldest_arrival = Some(now);
        }
        self.records.extend(records);
    }

    /// Whether the buffer should flush now given the configured thresholds.
    fn should_flush(&self, config: &CompactionLoopConfig, now: std::time::Instant) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.records.len() >= config.flush_max_rows {
            return true;
        }
        self.oldest_arrival
            .is_some_and(|anchor| now.duration_since(anchor).as_time() >= config.flush_max_age)
    }

    /// Take all buffered records, resetting the buffer to empty.
    fn take(&mut self) -> Vec<CompactionWalRecord> {
        self.oldest_arrival = None;
        std::mem::take(&mut self.records)
    }
}

/// Write one block from the buffered records and commit their offsets, folding
/// the result into the running loop summary.
///
/// CORRECTNESS: `process_compaction_record_batch` writes every partition's block
/// and index sidecar durably *before* committing any offsets, then commits once
/// after all writes succeed — so offsets are advanced only after the accumulated
/// blocks are durable. The buffer is emptied by the caller via `take` *before*
/// this call, so a write/commit error leaves the buffer empty and the next poll
/// re-reads from the last committed offset — at-least-once.
async fn flush_buffer<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
    summary: &mut CompactionLoopResult,
) -> Result<Vec<CompactionPartitionOffset>, CompactionPollError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let batch =
        process_compaction_record_batch(block_writer, index_sink, committer, records).await?;
    summary.writes += batch.writes.len();
    summary
        .committed_offsets
        .extend(batch.committed_offsets.iter().cloned());
    Ok(batch.committed_offsets)
}

/// Run the compactor polling loop until `should_stop` returns true, using the
/// real monotonic clock for flush-by-age.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_loop<P, S, C, Stop>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    config: CompactionLoopConfig,
    should_stop: Stop,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
{
    run_compactor_loop_with_clock(
        poller,
        block_writer,
        index_sink,
        committer,
        config,
        should_stop,
        &SystemCompactionClock,
    )
    .await
}

/// Accumulate-then-flush compactor loop with an injectable clock.
///
/// Each poll appends to an in-memory buffer instead of writing a block. A block
/// is written (and offsets committed) only when the buffer reaches
/// `flush_max_rows` or its oldest record reaches `flush_max_age`, or when
/// `should_stop` fires (shutdown), at which point the remaining buffer is
/// flushed so no records are dropped. The `CompactionPollResult` handed to
/// `should_stop` reports this poll's `polled_records`/`compacted_records`; its
/// `batch` reflects only the writes/commits that occurred this iteration (empty
/// while buffering, populated on a flush).
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_loop_with_clock<P, S, C, Stop, Clock>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    config: CompactionLoopConfig,
    mut should_stop: Stop,
    clock: &Clock,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
    Clock: CompactionClock + ?Sized,
{
    let mut summary = CompactionLoopResult::default();
    let mut buffer = CompactionBuffer::new();
    loop {
        let records = poller.poll(config.poll_timeout).await?;
        let polled_records = records.len();
        let wal_records =
            compaction_wal_records_from_consumer_records(&config.wal_topic, &records)?;
        let compacted_records = wal_records.len();

        let now = clock.now();
        buffer.extend(wal_records, now);

        let mut iteration_offsets = Vec::new();
        if buffer.should_flush(&config, now) {
            let buffered = buffer.take();
            iteration_offsets =
                flush_buffer(block_writer, index_sink, committer, &buffered, &mut summary).await?;
        }

        summary.polls += 1;
        summary.polled_records += polled_records;
        summary.compacted_records += compacted_records;

        let result = CompactionPollResult {
            polled_records,
            compacted_records,
            batch: CompactionBatchResult {
                partition_results: Vec::new(),
                writes: Vec::new(),
                committed_offsets: iteration_offsets,
            },
        };

        if should_stop(&result) {
            // Shutdown: flush whatever is still buffered so no records are lost.
            let buffered = buffer.take();
            flush_buffer(block_writer, index_sink, committer, &buffered, &mut summary).await?;
            break;
        }
    }
    Ok(summary)
}

/// Run the compactor polling loop using a single consumer handle for poll and
/// commit, using the real monotonic clock for flush-by-age.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_consumer_loop<C, S, Stop>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    config: CompactionLoopConfig,
    should_stop: Stop,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
{
    run_compactor_consumer_loop_with_clock(
        consumer,
        block_writer,
        index_sink,
        config,
        should_stop,
        &SystemCompactionClock,
    )
    .await
}

/// Build the per-poll-batch `metrics_compaction` consumer span, joining it to the
/// producer trace carried on a WAL record's `traceparent` header (if any).
///
/// ONE span per poll batch (not per record). `set_remote_parent` is a no-op when
/// no polled record carries a valid trace context, so the span is always safe to
/// build. The first record carrying a `traceparent` header anchors the parent.
fn compaction_batch_span(records: &[ConsumerRecord], wal_records: usize) -> tracing::Span {
    let span = tracing::info_span!(
        "metrics_compaction",
        otel.kind = "consumer",
        crabka.wal.records = wal_records,
    );
    if let Some(record) = records.iter().find(|record| {
        record
            .headers
            .iter()
            .any(|header| header.key == TRACEPARENT)
    }) {
        set_remote_parent(
            &span,
            record.headers.iter().map(|header| {
                (
                    header.key.as_str(),
                    header.value.as_deref().unwrap_or(&[][..]),
                )
            }),
        );
    }
    span
}

/// Accumulate-then-flush single-consumer compactor loop with an injectable clock.
///
/// Mirrors [`run_compactor_loop_with_clock`] but polls and commits through one
/// mutable consumer handle (`process_compaction_record_batch_with_consumer`),
/// which likewise writes the block durably before committing offsets.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_compactor_consumer_loop_with_clock<C, S, Stop, Clock>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    config: CompactionLoopConfig,
    mut should_stop: Stop,
    clock: &Clock,
) -> Result<CompactionLoopResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
    Stop: FnMut(&CompactionPollResult) -> bool,
    Clock: CompactionClock + ?Sized,
{
    let mut summary = CompactionLoopResult::default();
    let mut buffer = CompactionBuffer::new();
    loop {
        let records = consumer.poll(config.poll_timeout).await?;
        let polled_records = records.len();
        let wal_records =
            compaction_wal_records_from_consumer_records(&config.wal_topic, &records)?;
        let compacted_records = wal_records.len();
        // ONE consumer span per poll batch, parented on the producer trace carried
        // in a polled WAL record's `traceparent` header. Built once per batch and
        // run over the flush so the compaction block/index writes join the ingest
        // trace; a batch that only buffers (no flush this iteration) does no
        // compaction work and correctly carries no span.
        let span = compaction_batch_span(&records, compacted_records);

        let now = clock.now();
        buffer.extend(wal_records, now);

        let mut iteration_offsets = Vec::new();
        if buffer.should_flush(&config, now) {
            let buffered = buffer.take();
            iteration_offsets = flush_buffer_with_consumer(
                block_writer,
                index_sink,
                consumer,
                &buffered,
                &mut summary,
            )
            .instrument(span)
            .await?;
        } else {
            drop(span);
        }

        summary.polls += 1;
        summary.polled_records += polled_records;
        summary.compacted_records += compacted_records;

        let result = CompactionPollResult {
            polled_records,
            compacted_records,
            batch: CompactionBatchResult {
                partition_results: Vec::new(),
                writes: Vec::new(),
                committed_offsets: iteration_offsets,
            },
        };

        if should_stop(&result) {
            // Shutdown: flush whatever is still buffered so no records are lost.
            let buffered = buffer.take();
            flush_buffer_with_consumer(block_writer, index_sink, consumer, &buffered, &mut summary)
                .await?;
            break;
        }
    }
    Ok(summary)
}

/// Write one block from buffered records and commit through the consumer handle,
/// folding the result into the running summary and returning the offsets committed
/// this flush.
///
/// CORRECTNESS: `process_compaction_record_batch_with_consumer` writes the block
/// and index sidecar durably before `commit_sync_mut`, so offsets are committed
/// only after the accumulated block is durable.
async fn flush_buffer_with_consumer<C, S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    consumer: &mut C,
    records: &[CompactionWalRecord],
    summary: &mut CompactionLoopResult,
) -> Result<Vec<CompactionPartitionOffset>, CompactionPollError>
where
    C: CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let batch =
        process_compaction_record_batch_with_consumer(block_writer, index_sink, consumer, records)
            .await?;
    summary.writes += batch.writes.len();
    summary
        .committed_offsets
        .extend(batch.committed_offsets.iter().cloned());
    Ok(batch.committed_offsets)
}

/// Poll, compact, and commit once using a single mutable consumer handle.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_consumer_once<C, S>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    wal_topic: &str,
    timeout: Time,
) -> Result<CompactionPollResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    let polled_records = records.len();
    let wal_records = compaction_wal_records_from_consumer_records(wal_topic, &records)?;
    let compacted_records = wal_records.len();
    let batch = process_compaction_record_batch_with_consumer(
        block_writer,
        index_sink,
        consumer,
        &wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records,
        compacted_records,
        batch,
    })
}

/// Decode, compact, write, and commit one assigned WAL partition window.
///
/// A successful return means all decoded records in the window are represented
/// by durable block/index writes and the partition offset has been committed to
/// the next offset. Empty windows are a no-op.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn process_compaction_partition_window<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
) -> Result<CompactionWindowResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let result = write_compaction_partition_window(block_writer, index_sink, records).await?;
    if let Some(committed_offset) = result.committed_offset.clone() {
        committer
            .commit_offsets(std::slice::from_ref(&committed_offset))
            .await?;
    }
    Ok(result)
}

async fn process_compaction_record_batch_with_consumer<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    consumer: &mut C,
    records: &[CompactionWalRecord],
) -> Result<CompactionBatchResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionConsumerCommitMut + ?Sized,
{
    let mut by_partition = BTreeMap::<PartitionIndex, Vec<CompactionWalRecord>>::new();
    for record in records {
        by_partition
            .entry(record.partition)
            .or_default()
            .push(record.clone());
    }

    let mut partition_results = Vec::new();
    let mut writes = Vec::new();
    let mut committed_offsets = Vec::new();
    // Write every partition's block durably BEFORE committing any offsets.
    // `commit_sync` advances the whole assignment's offsets (a whole-snapshot
    // commit, see `Consumer::commit_sync`), so committing inside the loop would
    // advance partitions whose blocks have not yet been written; a later
    // partition's write failure would then skip those un-written records on the
    // next run — silent data loss. Writing all partitions first means the single
    // commit below only advances past fully-durable data, and any write error
    // returns before the commit so the next poll re-reads (at-least-once).
    for partition_records in by_partition.into_values() {
        let result =
            write_compaction_partition_window(block_writer, index_sink, &partition_records).await?;
        writes.extend(result.writes.clone());
        if let Some(offset) = &result.committed_offset {
            committed_offsets.push(offset.clone());
        }
        partition_results.push(result);
    }

    if !committed_offsets.is_empty() {
        consumer
            .commit_sync_mut()
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))?;
    }

    Ok(CompactionBatchResult {
        partition_results,
        writes,
        committed_offsets,
    })
}

async fn write_compaction_partition_window<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    records: &[CompactionWalRecord],
) -> Result<CompactionWindowResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
{
    let Some(first_record) = records.first() else {
        return Ok(CompactionWindowResult {
            writes: Vec::new(),
            committed_offset: None,
        });
    };
    let partition = first_record.partition;
    let mut first_offset = first_record.offset;
    let mut last_offset = first_record.offset;
    let mut wal_records = Vec::with_capacity(records.len());

    for record in records {
        if record.partition != partition {
            return Err(CompactionWindowError::MultiplePartitions {
                first: partition,
                second: record.partition,
            });
        }
        first_offset = first_offset.min(record.offset);
        last_offset = last_offset.max(record.offset);
        wal_records.push(WalRecord::decode(&record.value)?);
    }

    let mut writes = Vec::new();
    for rows in compact_wal_records(&wal_records) {
        writes.extend(
            write_compacted_tenant_partition_blocks(
                block_writer,
                index_sink,
                &rows,
                partition,
                first_offset.0,
                last_offset.0,
            )
            .await?,
        );
    }

    let committed_offset = CompactionPartitionOffset {
        partition,
        offset: last_offset + 1,
    };

    Ok(CompactionWindowResult {
        writes,
        committed_offset: Some(committed_offset),
    })
}

struct CompactedBlockRequest<'a> {
    tenant: &'a str,
    kind: MetricBlockKind,
    partition: Option<PartitionIndex>,
    first_offset: i64,
    last_offset: i64,
    batch: RecordBatch,
    series: Vec<CompactionSeriesLabels>,
}

async fn write_compacted_block<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    request: CompactedBlockRequest<'_>,
) -> Result<CompactedBlockWrite, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    let mut plan = request.partition.map_or_else(
        || {
            compaction_object_plan(
                request.tenant,
                request.kind,
                request.first_offset,
                request.last_offset,
            )
        },
        |partition| {
            compaction_partition_object_plan(
                request.tenant,
                request.kind,
                partition,
                request.first_offset,
                request.last_offset,
            )
        },
    );
    plan.row_count = request.batch.num_rows();
    let block_meta = block_writer
        .write_block(
            request.tenant,
            &plan.block_key,
            request.batch.schema(),
            &[request.batch],
        )
        .await?;
    let manifest =
        CompactionIndexManifest::from_block_meta(request.kind, &plan, &block_meta, request.series);
    index_sink.write_manifest(&manifest).await?;

    Ok(CompactedBlockWrite {
        kind: request.kind,
        block_meta,
        manifest,
    })
}

/// Percent-escape a tenant id for use as a single object-store path segment.
///
/// `.` is allowed as an interior character (tenant ids legitimately contain
/// dots), but a tenant that is *exactly* `.` or `..` would form a path-traversal
/// segment in the object key. Tenants are validated upstream, so this is defense
/// in depth: a whole-segment `.`/`..` has its dots percent-escaped so the
/// resulting segment can never be a relative-path component. The `kind` and
/// offset segments are formatted separately and never pass through here.
fn escape_object_path_segment(value: &str) -> String {
    // Reject a tenant segment that is exactly `.` or `..` by escaping the dots,
    // which cannot otherwise be produced (an escaped dot is `%2E`, not `.`).
    if value == "." || value == ".." {
        return value.bytes().map(|_| "%2E").collect();
    }
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(&mut out, "%{byte:02X}").expect("write to String");
        }
    }
    out
}

/// Group WAL records by tenant and sort rows by `(fingerprint, timestamp)`.
#[must_use]
pub fn compact_wal_records(records: &[WalRecord]) -> Vec<TenantCompactionRows> {
    let mut tenants = BTreeMap::<String, TenantCompactionRows>::new();
    for record in records {
        let fingerprint = record.series_fingerprint();
        let rows = tenants
            .entry(record.tenant.clone())
            .or_insert_with(|| TenantCompactionRows {
                tenant: record.tenant.clone(),
                series_labels: BTreeMap::new(),
                float_rows: Vec::new(),
                histogram_rows: Vec::new(),
                exemplar_rows: Vec::new(),
                metadata_rows: Vec::new(),
            });
        rows.series_labels
            .entry(fingerprint)
            .or_insert_with(|| record.labels());

        match &record.payload {
            SamplePayload::Float {
                timestamp_ms,
                value,
                ..
            } => rows.float_rows.push(FloatRow {
                fingerprint,
                timestamp_ms: *timestamp_ms,
                value: *value,
            }),
            SamplePayload::Hist { timestamp_ms, hist } => {
                rows.histogram_rows.push(NativeHistogramRow {
                    fingerprint,
                    timestamp_ms: *timestamp_ms,
                    hist: hist.clone(),
                });
            }
            SamplePayload::Metadata {
                metric_family_name,
                metric_type,
                help,
                unit,
            } => rows.metadata_rows.push(MetadataRow {
                fingerprint,
                metric_family_name: metric_family_name.clone(),
                metric_type: metric_type.clone(),
                help: help.clone(),
                unit: unit.clone(),
            }),
            SamplePayload::Exemplars => {}
        }

        rows.exemplar_rows.extend(
            record
                .exemplars
                .iter()
                .map(|exemplar| exemplar_row(fingerprint, exemplar)),
        );
    }

    let mut out = tenants.into_values().collect::<Vec<_>>();
    for rows in &mut out {
        rows.float_rows
            .sort_by_key(|row| (row.fingerprint, row.timestamp_ms));
        rows.histogram_rows
            .sort_by_key(|row| (row.fingerprint, row.timestamp_ms));
        rows.exemplar_rows
            .sort_by_key(|row| (row.fingerprint, row.timestamp_ms));
        rows.metadata_rows.sort_by(|left, right| {
            (
                left.metric_family_name.as_str(),
                left.fingerprint,
                left.metric_type.as_str(),
                left.help.as_str(),
                left.unit.as_str(),
            )
                .cmp(&(
                    right.metric_family_name.as_str(),
                    right.fingerprint,
                    right.metric_type.as_str(),
                    right.help.as_str(),
                    right.unit.as_str(),
                ))
        });
    }
    out
}

fn series_labels_for_kind(
    rows: &TenantCompactionRows,
    kind: MetricBlockKind,
) -> Vec<CompactionSeriesLabels> {
    let fingerprints = match kind {
        MetricBlockKind::Float => rows
            .float_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::NativeHistograms => rows
            .histogram_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::Exemplars => rows
            .exemplar_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::Metadata => rows
            .metadata_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
    };

    fingerprints
        .into_iter()
        .filter_map(|fingerprint| {
            rows.series_labels
                .get(&fingerprint)
                .cloned()
                .map(|labels| CompactionSeriesLabels {
                    fingerprint,
                    labels,
                })
        })
        .collect()
}

/// Encode one tenant's sorted rows into Arrow batches for the block writer.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_tenant_batches(
    rows: &TenantCompactionRows,
) -> Result<TenantBatches, HistogramCodecError> {
    let float = if rows.float_rows.is_empty() {
        None
    } else {
        let tuples = rows
            .float_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms, row.value))
            .collect::<Vec<_>>();
        Some(encode_float_samples(&tuples)?)
    };

    let native_histograms = if rows.histogram_rows.is_empty() {
        None
    } else {
        let tuples = rows
            .histogram_rows
            .iter()
            .map(|row| (row.fingerprint, row.timestamp_ms, row.hist.clone()))
            .collect::<Vec<_>>();
        Some(encode_native_histograms(&tuples)?)
    };

    let exemplars = if rows.exemplar_rows.is_empty() {
        None
    } else {
        Some(encode_exemplar_rows(&rows.exemplar_rows)?)
    };

    let metadata = if rows.metadata_rows.is_empty() {
        None
    } else {
        Some(encode_metadata_rows(&rows.metadata_rows)?)
    };

    Ok(TenantBatches {
        float,
        native_histograms,
        exemplars,
        metadata,
    })
}

fn encode_metadata_rows(rows: &[MetadataRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut names = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut helps = StringBuilder::new();
    let mut units = StringBuilder::new();

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(0);
        names.append_value(&row.metric_family_name);
        types.append_value(&row.metric_type);
        helps.append_value(&row.help);
        units.append_value(&row.unit);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(names.finish()),
        Arc::new(types.finish()),
        Arc::new(helps.finish()),
        Arc::new(units.finish()),
    ];

    Ok(RecordBatch::try_new(metadata_schema(), columns)?)
}

fn encode_exemplar_rows(rows: &[ExemplarRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    let mut trace_ids = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut labels = MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false));

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(row.timestamp_ms);
        values.append_value(row.value);
        match &row.trace_id {
            Some(trace_id) => trace_ids.append_value(trace_id),
            None => trace_ids.append_null(),
        }
        match &row.span_id {
            Some(span_id) => span_ids.append_value(span_id),
            None => span_ids.append_null(),
        }
        for (name, value) in &row.labels {
            labels.keys().append_value(name);
            labels.values().append_value(value);
        }
        labels.append(true)?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(trace_ids.finish()),
        Arc::new(span_ids.finish()),
        Arc::new(labels.finish()),
    ];

    Ok(RecordBatch::try_new(exemplar_schema(), columns)?)
}

fn exemplar_row(fingerprint: u64, exemplar: &WalExemplar) -> ExemplarRow {
    let mut trace_id = None;
    let mut span_id = None;
    let mut labels = Vec::new();

    for (name, value) in &exemplar.labels {
        match name.as_str() {
            "trace_id" => trace_id = Some(value.clone()),
            "span_id" => span_id = Some(value.clone()),
            _ => labels.push((name.clone(), value.clone())),
        }
    }

    ExemplarRow {
        fingerprint,
        timestamp_ms: exemplar.timestamp_ms,
        value: exemplar.value,
        trace_id,
        span_id,
        labels,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use assert2::{assert, check};
    use async_trait::async_trait;
    use crabka_blockstore::Labels;
    use crabka_units::prelude::*;
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::{compact_wal_records, encode_tenant_batches};
    use crate::{
        BucketSpan, FloatRow, NativeHistogram, ResetHint,
        distributor::wal_records_from_series,
        wal::{SamplePayload, WalExemplar, WalRecord},
        wire::{DecodedExemplar, DecodedSample, DecodedSeries},
    };

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    fn float_record(tenant: &str, metric: &str, job: &str, timestamp_ms: i64) -> WalRecord {
        WalRecord {
            tenant: tenant.to_string(),
            labels: vec![
                ("__name__".into(), metric.into()),
                ("job".into(), job.into()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        }
    }

    fn hist() -> NativeHistogram {
        NativeHistogram {
            schema: 1,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 3.0,
            sum: 6.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: Some(10),
        }
    }

    #[test]
    fn compact_wal_records_groups_by_tenant_and_sorts_rows() {
        let a_late = float_record("tenant-a", "up", "api", 30);
        let a_early = float_record("tenant-a", "up", "api", 10);
        let b_row = float_record("tenant-b", "up", "api", 20);

        let compacted = compact_wal_records(&[a_late.clone(), b_row.clone(), a_early.clone()]);

        check!(compacted.len() == 2);
        check!(compacted[0].tenant == "tenant-a");
        check!(compacted[1].tenant == "tenant-b");
        check!(compacted[0].float_rows.len() == 2);
        check!(compacted[0].float_rows[0].timestamp_ms == 10);
        check!(compacted[0].float_rows[1].timestamp_ms == 30);
        check!(compacted[0].float_rows[0].fingerprint == a_early.series_fingerprint());
        check!(compacted[1].float_rows[0].fingerprint == b_row.series_fingerprint());
    }

    #[test]
    fn compaction_object_keys_are_deterministic_by_tenant_kind_and_offsets() {
        let cases = [
            (
                super::MetricBlockKind::Float,
                "metrics/tenant%2Fa/float/00000000000000000042-00000000000000000099.parquet",
            ),
            (
                super::MetricBlockKind::NativeHistograms,
                "metrics/tenant%2Fa/native-histograms/00000000000000000042-00000000000000000099.parquet",
            ),
            (
                super::MetricBlockKind::Exemplars,
                "metrics/tenant%2Fa/exemplars/00000000000000000042-00000000000000000099.parquet",
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                super::compaction_object_key("tenant/a", kind, 42, 99),
                expected,
                "kind {kind:?}"
            );
        }
    }

    #[test]
    fn tenant_dot_segments_cannot_form_path_traversal() {
        // Defense in depth: a tenant of exactly "." or ".." must not survive as
        // a relative-path component in the object key.
        assert!(super::escape_object_path_segment(".") == "%2E");
        assert!(super::escape_object_path_segment("..") == "%2E%2E");
        let key = super::compaction_object_key("..", super::MetricBlockKind::Float, 42, 99);
        assert!(key == "metrics/%2E%2E/float/00000000000000000042-00000000000000000099.parquet");
        // Interior dots in a legitimate tenant id are still allowed verbatim.
        assert!(super::escape_object_path_segment("a.b") == "a.b");
    }

    #[test]
    fn compaction_object_plan_pairs_block_and_index_keys() {
        let plan = super::compaction_object_plan("tenant/a", super::MetricBlockKind::Float, 42, 99);

        assert!(
            plan.block_key
                == "metrics/tenant%2Fa/float/00000000000000000042-00000000000000000099.parquet"
        );
        assert!(
            plan.index_key
                == "metrics/tenant%2Fa/float/00000000000000000042-00000000000000000099.index"
        );
    }

    #[test]
    fn compaction_object_plan_records_offset_window_and_row_count() {
        let compacted = compact_wal_records(&[
            float_record("tenant-a", "up", "api", 10),
            float_record("tenant-a", "up", "api", 20),
        ]);

        let plan = super::compaction_object_plan_for_rows(
            &compacted[0],
            super::MetricBlockKind::Float,
            42,
            99,
        );

        assert_eq!(
            plan,
            super::CompactionObjectPlan {
                block_key:
                    "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                        .to_string(),
                index_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                    .to_string(),
                first_offset: 42,
                last_offset: 99,
                row_count: 2,
            }
        );
    }

    #[test]
    fn compaction_index_manifest_round_trips() {
        let plan = super::CompactionObjectPlan {
            block_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                .to_string(),
            index_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                .to_string(),
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
        };

        let block_meta = crabka_blockstore::BlockMeta {
            tenant: "tenant-a".to_string(),
            object_key: plan.block_key.clone(),
            min_ts: 1_000,
            max_ts: 2_000,
            row_count: 2,
            fingerprints: vec![7, 9],
        };
        let manifest = super::CompactionIndexManifest::from_block_meta(
            super::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![super::CompactionSeriesLabels {
                fingerprint: 7,
                labels: labels(&[("__name__", "up")]),
            }],
        );
        let encoded = manifest.encode().expect("encode manifest");
        let decoded = super::CompactionIndexManifest::decode(&encoded).expect("decode manifest");

        assert_eq!(decoded, manifest);
        assert_eq!(
            decoded,
            super::CompactionIndexManifest {
                tenant: "tenant-a".to_string(),
                kind: super::MetricBlockKind::Float,
                block_key:
                    "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                        .to_string(),
                index_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                    .to_string(),
                first_offset: 42,
                last_offset: 99,
                row_count: 2,
                min_ts: 1_000,
                max_ts: 2_000,
                fingerprints: vec![7, 9],
                series: vec![super::CompactionSeriesLabels {
                    fingerprint: 7,
                    labels: labels(&[("__name__", "up")]),
                }],
            }
        );
    }

    #[tokio::test]
    async fn object_store_compaction_index_sink_writes_encoded_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = super::ObjectStoreCompactionIndexSink::new(object_store.clone());
        let plan = super::CompactionObjectPlan {
            block_key: "metrics/tenant-a/float/partition=0000000003/00000000000000000042-00000000000000000099.parquet"
                .to_string(),
            index_key: "metrics/tenant-a/float/partition=0000000003/00000000000000000042-00000000000000000099.index"
                .to_string(),
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
        };
        let manifest = super::CompactionIndexManifest::from_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            &plan,
        );

        super::CompactionIndexSink::write_manifest(&sink, &manifest)
            .await
            .expect("write manifest");
        let bytes = object_store
            .get(&object_store::path::Path::from(manifest.index_key.clone()))
            .await
            .expect("get manifest")
            .bytes()
            .await
            .expect("manifest bytes");
        let decoded =
            super::CompactionIndexManifest::decode(&bytes).expect("decode persisted manifest");

        assert!(decoded == manifest);
    }

    #[tokio::test]
    async fn retention_deletes_blocks_and_indexes_older_than_cutoff() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store.clone());
        let sink = super::ObjectStoreCompactionIndexSink::new(object_store.clone());

        let old_plan = super::compaction_partition_object_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            super::PartitionIndex(0),
            1,
            2,
        );
        let old_meta = block_writer
            .write_block(
                "tenant-a",
                &old_plan.block_key,
                crate::float_sample_schema(),
                &[crate::encode_float_samples(&[(1, 1_000, 1.0)]).expect("encode old float")],
            )
            .await
            .expect("write old block");
        let old = super::CompactionIndexManifest::from_block_meta(
            super::MetricBlockKind::Float,
            &old_plan,
            &old_meta,
            vec![super::CompactionSeriesLabels {
                fingerprint: 1,
                labels: labels(&[("__name__", "up"), ("job", "old")]),
            }],
        );
        super::CompactionIndexSink::write_manifest(&sink, &old)
            .await
            .expect("write old manifest");

        let fresh_plan = super::compaction_partition_object_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            super::PartitionIndex(0),
            3,
            4,
        );
        let fresh_meta = block_writer
            .write_block(
                "tenant-a",
                &fresh_plan.block_key,
                crate::float_sample_schema(),
                &[crate::encode_float_samples(&[(2, 10_000, 1.0)]).expect("encode fresh float")],
            )
            .await
            .expect("write fresh block");
        let fresh = super::CompactionIndexManifest::from_block_meta(
            super::MetricBlockKind::Float,
            &fresh_plan,
            &fresh_meta,
            vec![super::CompactionSeriesLabels {
                fingerprint: 2,
                labels: labels(&[("__name__", "up"), ("job", "fresh")]),
            }],
        );
        super::CompactionIndexSink::write_manifest(&sink, &fresh)
            .await
            .expect("write fresh manifest");

        let stats = super::enforce_compaction_retention(object_store.clone(), 10_000, secs(5))
            .await
            .expect("enforce retention");

        assert_eq!(
            stats,
            super::CompactionRetentionStats {
                manifests_scanned: 2,
                manifests_deleted: 1,
                blocks_deleted: 1,
            }
        );
        check!(
            object_store
                .head(&object_store::path::Path::from(old.index_key.clone()))
                .await
                .is_err()
        );
        check!(
            object_store
                .head(&object_store::path::Path::from(old.block_key.clone()))
                .await
                .is_err()
        );
        check!(
            object_store
                .head(&object_store::path::Path::from(fresh.index_key.clone()))
                .await
                .is_ok()
        );
        check!(
            object_store
                .head(&object_store::path::Path::from(fresh.block_key.clone()))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn zero_and_negative_retention_windows_sweep_nothing() {
        // The retention window is an extent, so "no window configured" is any
        // non-positive extent — the sweep must not treat it as "delete all".
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for retention in [Time::ZERO, Time::from_millis(-1)] {
            let stats =
                super::enforce_compaction_retention(object_store.clone(), 10_000, retention)
                    .await
                    .expect("enforce retention");
            assert!(stats == super::CompactionRetentionStats::default());
        }
    }

    #[tokio::test]
    async fn retention_rejects_manifest_with_mismatched_index_key() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let listed_index_key = "metrics/tenant-a/float/mismatch.index";
        let manifest_index_key = "metrics/tenant-a/float/actual.index";
        let manifest = super::CompactionIndexManifest {
            tenant: "tenant-a".to_string(),
            kind: super::MetricBlockKind::Float,
            block_key: "metrics/tenant-a/float/block.parquet".to_string(),
            index_key: manifest_index_key.to_string(),
            first_offset: 0,
            last_offset: 1,
            row_count: 1,
            min_ts: 1_000,
            max_ts: 1_000,
            fingerprints: vec![1],
            series: Vec::new(),
        };
        object_store
            .put(
                &object_store::path::Path::from(listed_index_key),
                object_store::PutPayload::from(manifest.encode().expect("encode manifest")),
            )
            .await
            .expect("write mismatched manifest");

        let error = super::enforce_compaction_retention(object_store.clone(), 10_000, secs(5))
            .await
            .expect_err("mismatched manifest should fail");

        assert!(matches!(
            error,
            super::CompactionRetentionError::ManifestKeyMismatch { listed, manifest }
                if listed == listed_index_key && manifest == manifest_index_key
        ));
        assert!(
            object_store
                .head(&object_store::path::Path::from(listed_index_key))
                .await
                .is_ok()
        );
    }

    #[test]
    fn metrics_compactor_config_validates_required_consumer_fields() {
        let cfg = super::MetricsCompactorConfig {
            bootstrap: String::new(),
            group_id: "metrics-compactor".to_string(),
            client_id: "crabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: millis(500),
            auto_offset_reset: crabka_client_consumer::AutoOffsetReset::Earliest,
            flush_max_rows: super::DEFAULT_FLUSH_MAX_ROWS,
            flush_max_age: super::DEFAULT_FLUSH_MAX_AGE,
        };

        let err = cfg.validate().expect_err("empty bootstrap should fail");
        assert!(format!("{err}").contains("bootstrap"));
    }

    #[tokio::test]
    async fn metrics_compactor_config_builds_runtime_with_shared_object_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cfg = super::MetricsCompactorConfig {
            bootstrap: "127.0.0.1:9092".to_string(),
            group_id: "metrics-compactor".to_string(),
            client_id: "crabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: millis(250),
            auto_offset_reset: crabka_client_consumer::AutoOffsetReset::Earliest,
            flush_max_rows: 12_345,
            flush_max_age: secs(7),
        };

        let runtime = cfg
            .build_runtime(object_store.clone())
            .expect("build runtime");
        assert_eq!(
            runtime.loop_config,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(250),
                flush_max_rows: 12_345,
                flush_max_age: secs(7),
            }
        );

        let manifest = super::CompactionIndexManifest::from_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            &super::compaction_partition_object_plan(
                "tenant-a",
                super::MetricBlockKind::Float,
                super::PartitionIndex(0),
                10,
                10,
            ),
        );
        super::CompactionIndexSink::write_manifest(&runtime.index_sink, &manifest)
            .await
            .expect("write manifest through runtime sink");
        let bytes = object_store
            .get(&object_store::path::Path::from(manifest.index_key.clone()))
            .await
            .expect("get manifest")
            .bytes()
            .await
            .expect("manifest bytes");
        assert!(super::CompactionIndexManifest::decode(&bytes).expect("decode") == manifest);
    }

    #[derive(Default)]
    struct RecordingIndexSink {
        manifests: Mutex<Vec<super::CompactionIndexManifest>>,
    }

    #[async_trait]
    impl super::CompactionIndexSink for RecordingIndexSink {
        async fn write_manifest(
            &self,
            manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            self.manifests
                .lock()
                .expect("manifest lock")
                .push(manifest.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_compacted_tenant_blocks_writes_block_before_index_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store.clone());
        let sink = RecordingIndexSink::default();
        let rows = super::TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels: BTreeMap::from([(7, labels(&[("__name__", "up")]))]),
            float_rows: vec![
                FloatRow {
                    fingerprint: 7,
                    timestamp_ms: 100,
                    value: 1.0,
                },
                FloatRow {
                    fingerprint: 7,
                    timestamp_ms: 200,
                    value: 2.0,
                },
            ],
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
        };

        let writes = super::write_compacted_tenant_blocks(&block_writer, &sink, &rows, 42, 99)
            .await
            .expect("write compacted blocks");

        check!(writes.len() == 1);
        check!(writes[0].kind == super::MetricBlockKind::Float);
        check!(writes[0].block_meta.row_count == 2);
        let persisted = crabka_blockstore::read_block(object_store, &writes[0].manifest.block_key)
            .await
            .expect("read persisted block");
        assert!(persisted.len() == 1);
        assert!(persisted[0].num_rows() == 2);

        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests.as_slice() == [writes[0].manifest.clone()]);
        check!(manifests[0].tenant == "tenant-a");
        check!(manifests[0].kind == super::MetricBlockKind::Float);
        check!(manifests[0].first_offset == 42);
        check!(manifests[0].last_offset == 99);
        check!(manifests[0].row_count == 2);
    }

    #[tokio::test]
    async fn write_compacted_tenant_blocks_persists_metadata_only_rows() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store.clone());
        let sink = RecordingIndexSink::default();
        let rows = super::TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels: BTreeMap::from([(7, labels(&[("__name__", "http_requests_total")]))]),
            float_rows: Vec::new(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: vec![super::MetadataRow {
                fingerprint: 7,
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }],
        };

        let writes = super::write_compacted_tenant_blocks(&block_writer, &sink, &rows, 42, 99)
            .await
            .expect("write compacted blocks");

        check!(writes.len() == 1);
        check!(writes[0].kind == super::MetricBlockKind::Metadata);
        check!(writes[0].block_meta.row_count == 1);
        let persisted = crabka_blockstore::read_block(object_store, &writes[0].manifest.block_key)
            .await
            .expect("read persisted metadata block");
        assert!(persisted.len() == 1);
        assert!(persisted[0].num_rows() == 1);

        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests.as_slice() == [writes[0].manifest.clone()]);
        check!(manifests[0].tenant == "tenant-a");
        check!(manifests[0].kind == super::MetricBlockKind::Metadata);
        check!(manifests[0].first_offset == 42);
        check!(manifests[0].last_offset == 99);
        check!(manifests[0].row_count == 1);
    }

    #[derive(Default)]
    struct RecordingOffsetCommitter {
        commits: Mutex<Vec<super::CompactionPartitionOffset>>,
    }

    #[async_trait]
    impl super::CompactionOffsetCommitter for RecordingOffsetCommitter {
        async fn commit_offsets(
            &self,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionCommitError> {
            self.commits
                .lock()
                .expect("commit lock")
                .extend_from_slice(offsets);
            Ok(())
        }
    }

    #[tokio::test]
    async fn process_compaction_partition_window_commits_after_blocks_and_indexes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let committer = RecordingOffsetCommitter::default();
        let first = float_record("tenant-a", "up", "api", 100);
        let second = float_record("tenant-a", "up", "api", 200);
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(3),
                offset: super::Offset(42),
                value: first.encode().expect("encode first"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(3),
                offset: super::Offset(43),
                value: second.encode().expect("encode second"),
            },
        ];

        let result =
            super::process_compaction_partition_window(&block_writer, &sink, &committer, &records)
                .await
                .expect("process compaction window");

        check!(
            result.committed_offset
                == Some(super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(3),
                    offset: super::Offset(44),
                })
        );
        check!(result.writes.len() == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
        check!(
            committer.commits.lock().expect("commit lock").as_slice()
                == [super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(3),
                    offset: super::Offset(44),
                }]
        );
    }

    /// Index sink that succeeds for the first `ok_before_failure` manifest writes
    /// and then fails, to model one partition's block write succeeding before a
    /// later partition's write fails mid-batch.
    struct FailAfterIndexSink {
        ok_before_failure: usize,
        calls: Mutex<usize>,
    }

    impl FailAfterIndexSink {
        fn new(ok_before_failure: usize) -> Self {
            Self {
                ok_before_failure,
                calls: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl super::CompactionIndexSink for FailAfterIndexSink {
        async fn write_manifest(
            &self,
            _manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            let mut calls = self.calls.lock().expect("calls lock");
            if *calls >= self.ok_before_failure {
                return Err(super::CompactionIndexError::ObjectStore(
                    "injected index write failure".to_string(),
                ));
            }
            *calls += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn process_compaction_record_batch_does_not_commit_when_a_later_partition_write_fails() {
        // Two partitions processed in order (0, then 1). Partition 0's block +
        // index write succeeds; partition 1's index write fails. Because the
        // commit advances the WHOLE assignment's offsets, committing per-partition
        // would have advanced partition 1's offset past records whose block was
        // never written — silent data loss. The fix writes all partitions first
        // and commits once, so a mid-batch failure must leave NOTHING committed
        // and the next poll re-reads from the last committed offset (at-least-once).
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        // Float-only records => exactly one block (one index manifest) per
        // partition, so `ok_before_failure = 1` lets partition 0 through and fails
        // partition 1.
        let sink = FailAfterIndexSink::new(1);
        let committer = RecordingOffsetCommitter::default();
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(0),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode p0"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(1),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 200)
                    .encode()
                    .expect("encode p1"),
            },
        ];

        let result =
            super::process_compaction_record_batch(&block_writer, &sink, &committer, &records)
                .await;

        assert!(result.is_err());
        assert!(committer.commits.lock().expect("commit lock").is_empty());
    }

    #[tokio::test]
    async fn process_compaction_record_batch_groups_partitions_and_uses_distinct_block_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let committer = RecordingOffsetCommitter::default();
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(0),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode p0"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(1),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 200)
                    .encode()
                    .expect("encode p1"),
            },
        ];

        let result =
            super::process_compaction_record_batch(&block_writer, &sink, &committer, &records)
                .await
                .expect("process compaction batch");

        check!(result.partition_results.len() == 2);
        check!(result.writes.len() == 2);
        check!(result.writes[0].manifest.block_key != result.writes[1].manifest.block_key);
        check!(
            result.committed_offsets
                == vec![
                    super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(0),
                        offset: super::Offset(43),
                    },
                    super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(1),
                        offset: super::Offset(43),
                    },
                ]
        );
        check!(
            committer.commits.lock().expect("commit lock").as_slice()
                == result.committed_offsets.as_slice()
        );
    }

    #[test]
    fn compaction_wal_records_from_consumer_records_filters_topic_and_requires_values() {
        let wal_record = float_record("tenant-a", "up", "api", 100);
        let records = vec![
            crabka_client_consumer::ConsumerRecord {
                topic: crate::WAL_TOPIC.to_string(),
                partition: 2,
                offset: 10,
                leader_epoch: -1,
                timestamp: 100,
                key: None,
                value: Some(bytes::Bytes::from(wal_record.encode().expect("encode wal"))),
                headers: Vec::new(),
            },
            crabka_client_consumer::ConsumerRecord {
                topic: "unrelated".to_string(),
                partition: 2,
                offset: 11,
                leader_epoch: -1,
                timestamp: 101,
                key: None,
                value: Some(bytes::Bytes::from_static(b"ignored")),
                headers: Vec::new(),
            },
        ];

        let converted =
            super::compaction_wal_records_from_consumer_records(crate::WAL_TOPIC, &records)
                .expect("convert consumer records");

        assert!(
            converted
                == vec![super::CompactionWalRecord {
                    partition: super::PartitionIndex(2),
                    offset: super::Offset(10),
                    value: wal_record.encode().expect("encode expected"),
                }]
        );

        let missing_value = vec![crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 3,
            offset: 12,
            leader_epoch: -1,
            timestamp: 102,
            key: None,
            value: None,
            headers: Vec::new(),
        }];
        let err =
            super::compaction_wal_records_from_consumer_records(crate::WAL_TOPIC, &missing_value)
                .expect_err("missing value should fail");
        assert!(matches!(
            err,
            super::CompactionConsumerRecordError::MissingValue {
                partition: super::PartitionIndex(3),
                offset: super::Offset(12)
            }
        ));
    }

    #[derive(Default)]
    struct RecordingCommitSync {
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl super::CompactionConsumerCommit for RecordingCommitSync {
        async fn commit_sync(&self) -> Result<(), super::CompactionConsumerCommitError> {
            *self.calls.lock().expect("commit calls lock") += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn compaction_consumer_committer_calls_commit_sync_once() {
        let sync = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&sync);

        super::CompactionOffsetCommitter::commit_offsets(
            &committer,
            &[super::CompactionPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(11),
            }],
        )
        .await
        .expect("commit offsets");

        assert!(*sync.calls.lock().expect("commit calls lock") == 1);
    }

    struct StaticPoller {
        records: Vec<crabka_client_consumer::ConsumerRecord>,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for StaticPoller {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<crabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            Ok(std::mem::take(&mut self.records))
        }
    }

    #[tokio::test]
    async fn poll_compactor_once_converts_processes_and_commits_records() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit);
        let wal_record = float_record("tenant-a", "up", "api", 100);
        let mut poller = StaticPoller {
            records: vec![crabka_client_consumer::ConsumerRecord {
                topic: crate::WAL_TOPIC.to_string(),
                partition: 4,
                offset: 21,
                leader_epoch: -1,
                timestamp: 100,
                key: None,
                value: Some(bytes::Bytes::from(wal_record.encode().expect("encode wal"))),
                headers: Vec::new(),
            }],
        };

        let result = super::poll_compactor_once(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            crate::WAL_TOPIC,
            millis(1),
        )
        .await
        .expect("poll compactor once");

        check!(result.polled_records == 1);
        check!(result.compacted_records == 1);
        check!(result.batch.writes.len() == 1);
        check!(
            result.batch.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(4),
                    offset: super::Offset(22),
                }]
        );
        check!(*commit.calls.lock().expect("commit calls lock") == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
    }

    struct QueuePoller {
        batches: Vec<Vec<crabka_client_consumer::ConsumerRecord>>,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for QueuePoller {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<crabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            if self.batches.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(self.batches.remove(0))
            }
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_accumulates_across_polls_and_flushes_once_on_stop() {
        // Two below-threshold polls must accumulate into ONE block (not one per
        // poll) and commit offsets only at the single shutdown flush.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit);
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };
        let mut stop_after_empty =
            |result: &super::CompactionPollResult| result.polled_records == 0;

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                // High row threshold and long age so neither below-threshold
                // poll triggers a mid-loop flush; only the shutdown flush writes.
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            &mut stop_after_empty,
        )
        .await
        .expect("run compactor loop");

        // ONE block written for the whole buffer, not one per poll; a single
        // commit at flush through the last buffered record (offset 11 -> commit 12).
        assert!(
            result
                == super::CompactionLoopResult {
                    polls: 3,
                    polled_records: 2,
                    compacted_records: 2,
                    writes: 1,
                    committed_offsets: vec![super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(0),
                        offset: super::Offset(12),
                    }],
                }
        );
        check!(*commit.calls.lock().expect("commit calls lock") == 1);
        assert!(sink.manifests.lock().expect("manifest lock").len() == 1);
        // The single block spans the full buffered offset range [10, 11].
        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
        check!(manifests[0].row_count == 2);
    }

    #[tokio::test]
    async fn run_compactor_loop_flushes_when_row_threshold_reached() {
        // Crossing flush_max_rows must flush mid-loop without waiting for stop.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit);
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        // Two records per poll; flush_max_rows == 2 flushes on the first poll.
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100), make_record(11, 200)]],
        };
        // Stop once the buffer has flushed (a committed offset surfaced) or polls drain.
        let mut stop_after_empty =
            |result: &super::CompactionPollResult| result.polled_records == 0;

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 2,
                flush_max_age: hours(1),
            },
            &mut stop_after_empty,
        )
        .await
        .expect("run compactor loop");

        // One block flushed by the row threshold on the first poll; the empty
        // second poll triggers stop with an already-empty buffer (no extra write).
        check!(result.writes == 1);
        check!(
            result.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(12),
                }]
        );
        check!(*commit.calls.lock().expect("commit calls lock") == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
    }

    struct FixedClock {
        now: std::sync::Mutex<std::time::Instant>,
    }

    impl FixedClock {
        fn new(start: std::time::Instant) -> Self {
            Self {
                now: std::sync::Mutex::new(start),
            }
        }

        fn advance(&self, delta: std::time::Duration) {
            let mut guard = self.now.lock().expect("clock lock");
            *guard += delta;
        }
    }

    impl super::CompactionClock for FixedClock {
        fn now(&self) -> std::time::Instant {
            *self.now.lock().expect("clock lock")
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_age_flush_uses_injected_clock() {
        // With a finite age, the buffer flushes only after the clock advances past it.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit);
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let clock = std::sync::Arc::new(FixedClock::new(std::time::Instant::now()));
        let advance_clock = std::sync::Arc::clone(&clock);
        // Poll 1 buffers offset 10; poll 2 buffers offset 11; poll 3 is empty.
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };
        // Advance the clock past flush_max_age once both records are buffered (after 2 polls).
        let mut polls = 0_usize;
        let mut stop_after_three = move |result: &super::CompactionPollResult| {
            polls += 1;
            if polls == 2 {
                advance_clock.advance(std::time::Duration::from_mins(2));
            }
            result.polled_records == 0
        };

        let result = super::run_compactor_loop_with_clock(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: minutes(1),
            },
            &mut stop_after_three,
            clock.as_ref(),
        )
        .await
        .expect("run compactor loop with clock");

        // Both records land in one age-triggered block; commit through offset 11 -> 12.
        check!(result.writes == 1);
        check!(
            result.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(12),
                }]
        );
        check!(*commit.calls.lock().expect("commit calls lock") == 1);
        let manifests = sink.manifests.lock().expect("manifest lock");
        assert!(manifests.len() == 1);
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
    }

    #[tokio::test]
    async fn run_compactor_consumer_loop_uses_one_consumer_for_poll_and_commit() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![make_record(10, 100)], Vec::new()],
            commit_calls: 0,
        };

        let result = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
        )
        .await
        .expect("run compactor consumer loop");

        check!(result.polls == 2);
        check!(result.polled_records == 1);
        // Buffered for one poll, then flushed once on the empty-poll shutdown.
        check!(result.writes == 1);
        check!(consumer.commit_calls == 1);
    }

    #[tokio::test]
    async fn run_compactor_consumer_loop_accumulates_multiple_polls_into_one_block() {
        // Two below-threshold polls accumulate into ONE block and commit once.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
            commit_calls: 0,
        };

        let result = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
        )
        .await
        .expect("run compactor consumer loop");

        check!(result.polls == 3);
        check!(result.polled_records == 2);
        // Single block + single commit for the whole two-poll buffer.
        check!(result.writes == 1);
        check!(consumer.commit_calls == 1);
        let manifests = sink.manifests.lock().expect("manifest lock");
        assert!(manifests.len() == 1);
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
        check!(manifests[0].row_count == 2);
    }

    /// Index sink that appends an ordered event marker shared with a committer,
    /// so a test can assert block/index writes precede the offset commit.
    struct OrderingIndexSink {
        store: Arc<dyn ObjectStore>,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl super::CompactionIndexSink for OrderingIndexSink {
        async fn write_manifest(
            &self,
            manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            // The block object is written before this sink runs, so assert it is
            // already durable when the index manifest lands.
            let head = self
                .store
                .head(&object_store::path::Path::from(manifest.block_key.clone()))
                .await;
            assert!(
                head.is_ok(),
                "block object must exist before index manifest"
            );
            self.events
                .lock()
                .expect("events lock")
                .push(format!("index:{}", manifest.block_key));
            Ok(())
        }
    }

    /// Committer that asserts the buffered block object is durable, then records
    /// the commit event after the block/index writes.
    struct OrderingCommitter {
        store: Arc<dyn ObjectStore>,
        events: Arc<Mutex<Vec<String>>>,
        block_key: String,
    }

    #[async_trait]
    impl super::CompactionOffsetCommitter for OrderingCommitter {
        async fn commit_offsets(
            &self,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionCommitError> {
            // Commits must only happen after the block is durably written.
            let head = self
                .store
                .head(&object_store::path::Path::from(self.block_key.clone()))
                .await;
            assert!(head.is_ok(), "block must be durable before offset commit");
            for offset in offsets {
                self.events
                    .lock()
                    .expect("events lock")
                    .push(format!("commit:{}", offset.offset));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_commits_offsets_only_after_durable_block_write() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = crabka_blockstore::BlockWriter::new(object_store.clone());
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let block_key = super::compaction_partition_object_key(
            "tenant-a",
            super::MetricBlockKind::Float,
            super::PartitionIndex(0),
            10,
            11,
        );
        let sink = OrderingIndexSink {
            store: object_store.clone(),
            events: Arc::clone(&events),
        };
        let committer = OrderingCommitter {
            store: object_store.clone(),
            events: Arc::clone(&events),
            block_key: block_key.clone(),
        };
        let make_record = |offset, timestamp| crabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
        )
        .await
        .expect("run compactor loop");

        assert!(result.writes == 1);
        // Index manifest write (which only runs after the durable block put) must
        // precede the offset commit in the recorded event order.
        let recorded = events.lock().expect("events lock").clone();
        assert!(recorded == vec![format!("index:{block_key}"), "commit:12".to_string()]);
    }

    struct PollAndCommit {
        batches: Vec<Vec<crabka_client_consumer::ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for PollAndCommit {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<crabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            if self.batches.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(self.batches.remove(0))
            }
        }
    }

    #[async_trait]
    impl super::CompactionConsumerCommit for PollAndCommit {
        async fn commit_sync(&self) -> Result<(), super::CompactionConsumerCommitError> {
            Err(super::CompactionConsumerCommitError::Commit(
                "immutable commit path should not be used by this adapter test".into(),
            ))
        }
    }

    #[async_trait]
    impl super::CompactionConsumerCommitMut for PollAndCommit {
        async fn commit_sync_mut(&mut self) -> Result<(), super::CompactionConsumerCommitError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    #[test]
    fn compact_wal_records_extracts_histograms_and_exemplars() {
        let mut record = float_record("tenant-a", "request_duration_seconds", "api", 20);
        record.exemplars = vec![WalExemplar {
            labels: vec![
                ("trace_id".into(), "abc".into()),
                ("span_id".into(), "def".into()),
                ("kind".into(), "slow".into()),
            ],
            value: 2.0,
            timestamp_ms: 19,
        }];
        let hist_record = WalRecord {
            tenant: "tenant-a".into(),
            labels: record.labels.clone(),
            payload: SamplePayload::Hist {
                timestamp_ms: 21,
                hist: hist(),
            },
            exemplars: Vec::new(),
        };

        let compacted = compact_wal_records(&[record.clone(), hist_record]);

        assert!(compacted.len() == 1);
        assert!(compacted[0].histogram_rows.len() == 1);
        check!(compacted[0].histogram_rows[0].timestamp_ms == 21);
        assert!(compacted[0].exemplar_rows.len() == 1);
        check!(compacted[0].exemplar_rows[0].fingerprint == record.series_fingerprint());
        check!(compacted[0].exemplar_rows[0].trace_id.as_deref() == Some("abc"));
        check!(compacted[0].exemplar_rows[0].span_id.as_deref() == Some("def"));
        check!(compacted[0].exemplar_rows[0].labels == vec![("kind".into(), "slow".into())]);
    }

    #[test]
    fn compact_wal_records_does_not_duplicate_series_exemplars_per_sample() {
        let labels = crabka_blockstore::Labels::from_iter([
            ("__name__".to_string(), "http_requests_total".to_string()),
            ("job".to_string(), "api".to_string()),
        ]);
        let exemplar_labels =
            crabka_blockstore::Labels::from_iter([("trace_id".to_string(), "abc".to_string())]);
        let records = wal_records_from_series(
            "tenant-a",
            &[DecodedSeries {
                labels: labels.clone(),
                samples: vec![DecodedSample::new(20, 2.0), DecodedSample::new(30, 3.0)],
                histograms: Vec::new(),
                exemplars: vec![DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 19,
                    value: 2.0,
                }],
                metadata: None,
            }],
        );

        let compacted = compact_wal_records(&records);

        assert!(compacted.len() == 1);
        check!(compacted[0].float_rows.len() == 2);
        assert!(compacted[0].exemplar_rows.len() == 1);
        check!(compacted[0].exemplar_rows[0].fingerprint == labels.fingerprint());
        check!(compacted[0].exemplar_rows[0].trace_id.as_deref() == Some("abc"));
    }

    #[test]
    fn compact_wal_records_extracts_metric_metadata() {
        let record = WalRecord {
            tenant: "tenant-a".into(),
            labels: vec![("__name__".into(), "http_requests_total".into())],
            payload: SamplePayload::Metadata {
                metric_family_name: "http_requests_total".into(),
                metric_type: "counter".into(),
                help: "Total HTTP requests.".into(),
                unit: "requests".into(),
            },
            exemplars: Vec::new(),
        };

        let compacted = compact_wal_records(std::slice::from_ref(&record));

        assert!(compacted.len() == 1);
        assert!(
            compacted[0].metadata_rows
                == vec![super::MetadataRow {
                    fingerprint: record.series_fingerprint(),
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }]
        );
    }

    #[test]
    fn metadata_index_queries_tenant_metric_metadata() {
        let rows = compact_wal_records(&[
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "counter".into(),
                    help: "Total HTTP requests.".into(),
                    unit: "requests".into(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "counter".into(),
                    help: "Total HTTP requests.".into(),
                    unit: "requests".into(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "up".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "up".into(),
                    metric_type: "gauge".into(),
                    help: "Target health.".into(),
                    unit: String::new(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-b".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "gauge".into(),
                    help: "Wrong tenant.".into(),
                    unit: String::new(),
                },
                exemplars: Vec::new(),
            },
        ]);

        let index = crate::MetadataIndex::from_compaction_rows(&rows);
        let tenant_a_all = index.metadata("tenant-a", None);
        let tenant_a_http = index.metadata("tenant-a", Some("http_requests_total"));

        assert!(tenant_a_all.len() == 2);
        check!(tenant_a_all[0].metric_family_name == "http_requests_total");
        check!(tenant_a_all[1].metric_family_name == "up");
        assert!(tenant_a_http.len() == 1);
        check!(tenant_a_http[0].metric_type == "counter");
        check!(tenant_a_http[0].help == "Total HTTP requests.");
        check!(index.metadata("tenant-b", Some("http_requests_total"))[0].metric_type == "gauge");
    }

    #[test]
    fn encode_tenant_batches_builds_float_and_histogram_batches() {
        let compacted = compact_wal_records(&[
            float_record("tenant-a", "up", "api", 10),
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "latency".into())],
                payload: SamplePayload::Hist {
                    timestamp_ms: 20,
                    hist: hist(),
                },
                exemplars: Vec::new(),
            },
        ]);

        let batches = encode_tenant_batches(&compacted[0]).unwrap();

        assert!(batches.float.as_ref().unwrap().num_rows() == 1);
        assert!(batches.native_histograms.as_ref().unwrap().num_rows() == 1);
    }

    #[test]
    fn encode_tenant_batches_builds_exemplar_sidecar_batch() {
        let mut record = float_record("tenant-a", "request_duration_seconds", "api", 20);
        record.exemplars = vec![WalExemplar {
            labels: vec![
                ("trace_id".into(), "abc".into()),
                ("span_id".into(), "def".into()),
                ("kind".into(), "slow".into()),
            ],
            value: 2.0,
            timestamp_ms: 19,
        }];
        let compacted = compact_wal_records(std::slice::from_ref(&record));

        let batches = encode_tenant_batches(&compacted[0]).unwrap();

        let batch = batches.exemplars.as_ref().expect("exemplar sidecar");
        assert!(batch.num_rows() == 1);
        assert!(batch.schema() == crate::exemplar_schema());

        let trace_ids = batch
            .column_by_name("trace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let span_ids = batch
            .column_by_name("span_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let labels = batch
            .column_by_name("labels")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        let label_entries = labels.value(0);

        check!(trace_ids.value(0) == "abc");
        check!(span_ids.value(0) == "def");
        assert!(label_entries.column(0).len() == 1);
        check!(
            label_entries
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                == "kind"
        );
        check!(
            label_entries
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                == "slow"
        );
    }
}
