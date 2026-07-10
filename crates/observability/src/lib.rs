//! Role-selectable service skeleton for Crabka observability.

pub mod ids;
pub mod metrics;

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    future::{Future, IntoFuture, pending},
    io::{ErrorKind, Read as _},
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Extension, Router,
    body::Bytes,
    extract::{
        Path, RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{
        HeaderMap, StatusCode,
        header::{ACCEPT, CONTENT_ENCODING, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, ValueEnum};
use crabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LogBlockIndex as BlockIndex,
    LogBlockStoreError as BlockStoreError, LogLabels as Labels, LogRow,
    LogSeriesFingerprint as SeriesFingerprint, TimeRange, read_log_block,
    read_log_block_from_object_store, read_log_index_manifest,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store, register_log_blocks,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};
use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, AdminClient, AdminError, PatternType, PermissionType,
    ResourceType,
};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError};
use crabka_client_producer::{
    Acks, Header as ProducerHeader, Producer, ProducerError, ProducerRecord,
};
use crabka_logql::{
    ComparisonOp, FieldFilter, FieldFilterExpression, FieldFilterLogicOp, FieldValue,
    LabelFormatValue, LabelSelectionMatcher, LabelSelectionSet, LineFilterOp, LogfmtParserConfig,
    MatchOp, MetricBinaryArithmetic, MetricBinaryComparison, MetricBinarySet, MetricBinarySetOp,
    MetricLabelJoin, MetricQuery, MetricScalarArithmetic, MetricScalarArithmeticOp,
    MetricScalarComparison, MetricVectorGroupModifier, MetricVectorMatching, ParseError,
    ParserStage, PipelineStage, PlanError, Quantile, RangeAggregation, StreamPlan, StreamQuery,
    UNWRAP_SAMPLE_VALUE_LABEL, UnwrapConversion, VectorAggregation, VectorAggregationOp,
    VectorGrouping, parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query, plan_stream_query,
};
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Float64Array, Int64Array, MapArray, StringArray,
            TimestampNanosecondArray, UInt64Array,
            builder::{MapBuilder, StringBuilder},
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    prelude::SessionContext,
};
use flate2::read::{DeflateDecoder, GzDecoder};
use futures_util::{StreamExt as _, TryStreamExt as _};
pub use ids::{Offset, PartitionIndex};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, parse_url_opts, path::Path as ObjectPath,
};
use opentelemetry_proto::tonic::{
    collector::logs::v1::{
        ExportLogsServiceRequest as ProtoExportLogsServiceRequest,
        ExportLogsServiceResponse as ProtoExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    common::v1::{
        AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue, any_value as proto_any_value,
    },
    logs::v1::LogRecord as ProtoLogRecord,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use prost::Message as _;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snap::raw::Decoder as SnappyDecoder;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;
use url::Url;

use crate::metrics::ServiceMetrics;

const LOKI_REJECT_OLD_SAMPLES_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const LOKI_CREATION_GRACE_PERIOD: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Role {
    Distributor,
    Compactor,
    Querier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum QuerierIndexSource {
    LocalManifest,
    TenantObjectStoreManifest,
    TenantObjectStoreShards,
}

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(name = "crabka-observability")]
pub struct ServiceConfig {
    #[arg(long, value_enum)]
    pub target: Role,

    #[arg(long, default_value = "127.0.0.1:3100")]
    pub listen_addr: SocketAddr,

    #[arg(long)]
    pub object_store_url: Option<String>,

    #[arg(long)]
    pub wal_bootstrap_server: Option<String>,

    #[arg(long, default_value = "__crabka_observability_logs_wal")]
    pub wal_topic: String,

    #[arg(long, default_value = "crabka-observability-compactor")]
    pub wal_group_id: String,

    #[arg(long, default_value = ".")]
    pub data_root: PathBuf,

    #[arg(long, value_enum, default_value = "local-manifest")]
    pub querier_index_source: QuerierIndexSource,

    #[arg(long)]
    pub tenant: Option<String>,

    #[arg(long)]
    pub index_prefix: Option<String>,

    #[arg(long)]
    pub query_start_ns: Option<i64>,

    #[arg(long)]
    pub query_end_ns: Option<i64>,

    #[arg(long)]
    pub max_query_range_ns: Option<i64>,

    #[arg(long)]
    pub max_query_series: Option<usize>,

    #[arg(long)]
    pub max_query_bytes: Option<u64>,

    #[arg(long)]
    pub max_query_length: Option<usize>,

    #[arg(long)]
    pub max_ingest_body_bytes: Option<usize>,

    #[arg(long)]
    pub wal_append_timeout_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ServiceConfigError {
    #[error("WAL sink is required for distributor service startup")]
    MissingWalSink,
    #[error("WAL consumer is required for compactor service startup")]
    MissingWalConsumer,
    #[error("missing --wal-bootstrap-server for WAL-backed service startup")]
    MissingWalBootstrapServer,
    #[error("object store is required for object-store querier index sources")]
    MissingObjectStore,
    #[error("missing --index-prefix for compactor service startup")]
    MissingCompactorIndexPrefix,
    #[error("missing --tenant for querier index source {index_source:?}")]
    MissingTenant { index_source: QuerierIndexSource },
    #[error("missing --index-prefix for querier index source {index_source:?}")]
    MissingIndexPrefix { index_source: QuerierIndexSource },
    #[error("missing --query-start-ns for querier index source tenant-object-store-shards")]
    MissingQueryStartNs,
    #[error("missing --query-end-ns for querier index source tenant-object-store-shards")]
    MissingQueryEndNs,
    #[error("invalid --object-store-url {url}: {reason}")]
    InvalidObjectStoreUrl { url: String, reason: String },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    Rules(#[from] LokiRuleStoreError),
}

#[derive(Debug, Error)]
pub enum ServiceRuntimeError {
    #[error(transparent)]
    Config(#[from] ServiceConfigError),
    #[error(transparent)]
    Admin(#[from] AdminError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error(transparent)]
    Compactor(#[from] CompactorRunError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceStatus {
    pub role: Role,
}

#[derive(Clone, Default)]
pub struct ServiceDependencies {
    wal_sink: Option<Arc<dyn LogWalSink>>,
    wal_consumer: Option<Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>>,
    ingest_limiter: Option<Arc<dyn LogIngestLimiter>>,
    query_authorizer: Option<Arc<dyn LogQueryAuthorizer>>,
    hot_tail: Option<HotTailDependency>,
    compaction_frontier: Option<SharedCompactionFrontier>,
    delete_requests: Option<SharedLogDeleteRequests>,
    /// Querier-only: params for the hot-tail WAL consumer that is connected
    /// asynchronously after HTTP serving begins (FIX B2).
    deferred_wal_consumer_connect: Option<DeferredWalConsumerConnect>,
    /// Shared RED-metrics bundle threaded into the per-role state. `None` in
    /// tests that don't care about metrics (each state then gets a fresh
    /// bundle); the binary constructs one bundle and threads it through here so
    /// the `:9404` exporter and the handlers share the same registry.
    metrics: Option<ServiceMetrics>,
}

#[derive(Clone)]
struct HotTailDependency {
    source: Arc<dyn LogHotTail>,
    frontier: CompactionFrontierSource,
}

/// Parameters needed to connect a [`KafkaLogWalConsumer`] in the background.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DeferredWalConsumerConnect {
    bootstrap: String,
    group_id: String,
    topic: String,
}

#[derive(Clone, Default)]
pub struct SharedLogDeleteRequests {
    inner: Arc<Mutex<CompactorDeleteRequests>>,
    storage_path: Option<Arc<PathBuf>>,
}

#[derive(Debug, Error)]
pub enum LogDeleteRequestStoreError {
    #[error("delete request store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("delete request store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum LokiRuleStoreError {
    #[error("Loki rule store I/O error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Loki rule store JSON error for {path:?}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub enum ActiveLogDeleteFilterError {
    #[error(transparent)]
    Store(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("stored delete request query {query:?} failed to parse: {source}")]
    Parse { query: String, source: ParseError },
}

impl ServiceDependencies {
    /// Thread a shared RED-metrics bundle into the service so the per-role
    /// state (distributor / querier) increments the same registry the `:9404`
    /// exporter serves.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_wal_sink(mut self, sink: impl LogWalSink) -> Self {
        self.wal_sink = Some(Arc::new(sink));
        self
    }

    #[must_use]
    pub fn with_wal_consumer(mut self, consumer: impl LogWalConsumer) -> Self {
        self.wal_consumer = Some(Arc::new(tokio::sync::Mutex::new(Box::new(consumer))));
        self
    }

    #[must_use]
    pub fn with_ingest_limiter(mut self, limiter: impl LogIngestLimiter) -> Self {
        self.ingest_limiter = Some(Arc::new(limiter));
        self
    }

    #[must_use]
    pub fn with_query_authorizer(mut self, authorizer: impl LogQueryAuthorizer) -> Self {
        self.query_authorizer = Some(Arc::new(authorizer));
        self
    }

    #[must_use]
    pub fn with_delete_requests(mut self, requests: SharedLogDeleteRequests) -> Self {
        self.delete_requests = Some(requests);
        self
    }

    #[must_use]
    pub fn with_compaction_frontier(mut self, frontier: SharedCompactionFrontier) -> Self {
        self.compaction_frontier = Some(frontier);
        self
    }

    #[must_use]
    pub fn with_hot_tail(self, source: impl LogHotTail, compacted_through_ns: i64) -> Self {
        self.with_hot_tail_frontier(source, CompactionFrontier::new(compacted_through_ns))
    }

    #[must_use]
    pub fn with_hot_tail_frontier(
        mut self,
        source: impl LogHotTail,
        frontier: CompactionFrontier,
    ) -> Self {
        self.hot_tail = Some(HotTailDependency {
            source: Arc::new(source),
            frontier: CompactionFrontierSource::Snapshot(frontier),
        });
        self
    }

    #[must_use]
    pub fn with_hot_tail_shared_frontier(
        mut self,
        source: impl LogHotTail,
        frontier: SharedCompactionFrontier,
    ) -> Self {
        self.hot_tail = Some(HotTailDependency {
            source: Arc::new(source),
            frontier: CompactionFrontierSource::Shared(frontier),
        });
        self
    }

    #[must_use]
    fn with_deferred_wal_consumer_connect(
        mut self,
        bootstrap: String,
        group_id: String,
        topic: String,
    ) -> Self {
        self.deferred_wal_consumer_connect = Some(DeferredWalConsumerConnect {
            bootstrap,
            group_id,
            topic,
        });
        self
    }
}

#[must_use]
pub fn run(config: ServiceConfig) -> Result<ServiceStatus, Infallible> {
    let ServiceConfig {
        target,
        listen_addr: _listen_addr,
        object_store_url: _object_store_url,
        wal_bootstrap_server: _wal_bootstrap_server,
        wal_topic: _wal_topic,
        wal_group_id: _wal_group_id,
        data_root: _data_root,
        querier_index_source: _querier_index_source,
        tenant: _tenant,
        index_prefix: _index_prefix,
        query_start_ns: _query_start_ns,
        query_end_ns: _query_end_ns,
        max_query_range_ns: _max_query_range_ns,
        max_query_series: _max_query_series,
        max_query_bytes: _max_query_bytes,
        max_query_length: _max_query_length,
        max_ingest_body_bytes: _max_ingest_body_bytes,
        wal_append_timeout_ms: _wal_append_timeout_ms,
    } = config;

    Ok(ServiceStatus { role: target })
}

const WAL_CONNECT_STARTUP_DEADLINE: Duration = Duration::from_secs(120);
const WAL_CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
const COMPACTION_FRONTIER_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const DYNAMIC_INDEX_CACHE_TTL: Duration = Duration::from_secs(5);
const DYNAMIC_INDEX_SHARD_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const DYNAMIC_INDEX_SHARD_FETCH_CONCURRENCY: usize = 32;
const OBJECT_STORE_STREAM_BLOCK_FETCH_CONCURRENCY: usize = 8;
const LOG_COMPACTION_ACCUMULATION_WINDOW: Duration = Duration::from_secs(2);
const LOG_COMPACTION_ACCUMULATION_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const LOG_COMPACTION_MAX_RECORDS_PER_BATCH: usize = 4096;

fn build_compactor_configured_object_store(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    if object_store.is_some() {
        return Ok(None);
    }

    build_configured_object_store(config)
}

fn compactor_object_store<'a>(
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<(&'a dyn ObjectStore, Option<&'a ObjectPath>), ServiceConfigError> {
    if let Some(store) = object_store {
        return Ok((store, None));
    }

    let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
    Ok((
        configured_store.store.as_ref(),
        Some(&configured_store.prefix),
    ))
}

#[cfg_attr(test, mutants::skip)]
async fn connect_with_startup_retry<T, E, F, Fut>(
    what: &str,
    deadline: Duration,
    mut make: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Every attempt is bounded by WAL_CONNECT_ATTEMPT_TIMEOUT — including the
    // final one after the deadline.  The previous implementation called an
    // un-timed `make().await` when the deadline expired inside the timeout arm,
    // which could itself hang forever.  Instead we track the last `Err` value
    // and return it on deadline expiry so we never call make() without a timer.
    let start = tokio::time::Instant::now();
    let mut backoff = Duration::from_millis(200);
    let mut last_err: Option<E> = None;
    loop {
        match tokio::time::timeout(WAL_CONNECT_ATTEMPT_TIMEOUT, make()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(error)) => {
                if start.elapsed() >= deadline {
                    return Err(error);
                }
                eprintln!(
                    "[crabka-observability] WAL dependency {what} connect failed during broker warmup; retrying: {error}"
                );
                last_err = Some(error);
            }
            Err(_elapsed) => {
                if start.elapsed() >= deadline {
                    if let Some(e) = last_err {
                        // Return the last real error we saw rather than an un-timed attempt.
                        return Err(e);
                    }
                    // All attempts so far only timed out (no Err variant captured).
                    // Do one final timed attempt; whatever it returns is the answer.
                    return match tokio::time::timeout(WAL_CONNECT_ATTEMPT_TIMEOUT, make()).await {
                        Ok(r) => r,
                        // Still timing out after the deadline — treat as a persistent
                        // hang; update last_err on next loop iteration and eventually
                        // we'll return Err from the Ok(Err) deadline arm.  For now,
                        // sleep briefly and let the loop expire naturally.
                        Err(_) => {
                            eprintln!(
                                "[crabka-observability] WAL dependency {what} connect timed out repeatedly; giving up"
                            );
                            sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                    };
                }
                eprintln!(
                    "[crabka-observability] WAL dependency {what} connect timed out during broker warmup; retrying"
                );
            }
        }
        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, Duration::from_secs(2));
    }
}

pub async fn build_service_dependencies(
    config: &ServiceConfig,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    match config.target {
        Role::Distributor => {
            let bootstrap = config
                .wal_bootstrap_server
                .as_deref()
                .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
            let bootstrap_owned = bootstrap.to_string();
            let topic = config.wal_topic.clone();
            let sink = connect_with_startup_retry("wal-sink", WAL_CONNECT_STARTUP_DEADLINE, || {
                let b = bootstrap_owned.clone();
                let t = topic.clone();
                async move { KafkaLogWalSink::connect(&b, t).await }
            })
            .await?;
            let bootstrap_owned2 = bootstrap.to_string();
            let topic2 = config.wal_topic.clone();
            let limiter =
                connect_with_startup_retry("ingest-limiter", WAL_CONNECT_STARTUP_DEADLINE, || {
                    let b = bootstrap_owned2.clone();
                    let t = topic2.clone();
                    async move { BrokerBackedIngestLimiter::connect(&b, t).await }
                })
                .await?;
            Ok(ServiceDependencies::default()
                .with_wal_sink(sink)
                .with_ingest_limiter(limiter))
        }
        Role::Compactor => {
            let bootstrap = config
                .wal_bootstrap_server
                .as_deref()
                .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
            let group_id = config.wal_group_id.clone();
            let topic = config.wal_topic.clone();
            // `Consumer::start` (called by `KafkaLogWalConsumer::connect`) now
            // retries internally with per-attempt timeouts, so there is no need
            // to double-wrap it in `connect_with_startup_retry`.  Call directly.
            let consumer = KafkaLogWalConsumer::connect(bootstrap, group_id, topic).await?;
            Ok(ServiceDependencies::default().with_wal_consumer(consumer))
        }
        Role::Querier => {
            // Validate configuration eagerly so misconfiguration fails fast (the
            // `querier_dependencies_require_wal_bootstrap_server` test relies on this).
            let bootstrap = config
                .wal_bootstrap_server
                .as_deref()
                .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
            // The actual Kafka connects happen asynchronously inside build_service_router so
            // that the querier's HTTP port binds immediately (FIX B2). Store the params for
            // later use.
            Ok(
                ServiceDependencies::default().with_deferred_wal_consumer_connect(
                    bootstrap.to_string(),
                    config.wal_group_id.clone(),
                    config.wal_topic.clone(),
                ),
            )
        }
    }
}

pub async fn run_compactor_once(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<BlockDescriptor>, ServiceRuntimeError> {
    let configured_store = build_compactor_configured_object_store(config, object_store)?;
    let (store, object_store_prefix) =
        compactor_object_store(object_store, configured_store.as_ref())?;
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    let prefix = effective_object_store_prefix(object_store_prefix, index_prefix);
    let compaction_frontier = dependencies.compaction_frontier.unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
    load_existing_compaction_frontier(store, &prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    let mut consumer = consumer.lock().await;

    let mut tenant_indexes = TenantCompactionIndexCache::new();
    let descriptors = materialize_deletes_then_compact_next_kafka_wal_batch(
        store,
        &prefix,
        consumer.as_mut(),
        Duration::from_millis(500),
        &delete_requests,
        &mut tenant_indexes,
    )
    .await?;
    for descriptor in &descriptors {
        advance_and_persist_compaction_frontier(store, &prefix, &compaction_frontier, descriptor)
            .await?;
    }
    Ok(descriptors.into_iter().next())
}

pub async fn run_compactor_until_idle(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
    let configured_store = build_compactor_configured_object_store(config, object_store)?;
    let (store, object_store_prefix) =
        compactor_object_store(object_store, configured_store.as_ref())?;
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    let prefix = effective_object_store_prefix(object_store_prefix, index_prefix);
    let compaction_frontier = dependencies.compaction_frontier.unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
    load_existing_compaction_frontier(store, &prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    let mut consumer = consumer.lock().await;
    let mut descriptors = Vec::new();
    let mut tenant_indexes = TenantCompactionIndexCache::new();

    loop {
        let batch_descriptors = materialize_deletes_then_compact_next_kafka_wal_batch(
            store,
            &prefix,
            consumer.as_mut(),
            Duration::from_millis(500),
            &delete_requests,
            &mut tenant_indexes,
        )
        .await?;
        if batch_descriptors.is_empty() {
            break;
        }
        for descriptor in batch_descriptors {
            advance_and_persist_compaction_frontier(
                store,
                &prefix,
                &compaction_frontier,
                &descriptor,
            )
            .await?;
            descriptors.push(descriptor);
        }
    }

    Ok(descriptors)
}

#[cfg_attr(test, mutants::skip)]
pub async fn run_compactor_until_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    shutdown: impl Future<Output = ()>,
) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
    let configured_store = build_compactor_configured_object_store(config, object_store)?;
    let (store, object_store_prefix) =
        compactor_object_store(object_store, configured_store.as_ref())?;
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    let prefix = effective_object_store_prefix(object_store_prefix, index_prefix);
    let compaction_frontier = dependencies.compaction_frontier.unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
    load_existing_compaction_frontier(store, &prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    let mut consumer = consumer.lock().await;
    let mut descriptors = Vec::new();
    let mut object_store_retry_backoff = Duration::from_millis(10);
    let mut tenant_indexes = TenantCompactionIndexCache::new();
    let mut pending_compaction_records: Option<Vec<KafkaWalRecord>> = None;
    // Shared RED-metrics bundle for the `:9404` exporter; `None` in tests that
    // don't wire metrics, so block-written accounting is a no-op there.
    let metrics = dependencies.metrics.clone();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(descriptors),
            () = sleep(Duration::ZERO) => {}
        }

        let batch_result = match materialize_log_deletes_before_compaction(
            store,
            &prefix,
            &delete_requests,
            &mut tenant_indexes,
        )
        .await
        {
            Ok(()) => {
                let records = match pending_compaction_records.take() {
                    Some(records) => records,
                    None => {
                        match poll_accumulated_log_compaction_records(
                            consumer.as_mut(),
                            Duration::from_millis(500),
                        )
                        .await
                        {
                            Ok(records) => records,
                            Err(error) => return Err(CompactorRunError::from(error).into()),
                        }
                    }
                };
                if records.is_empty() {
                    Ok(Vec::new())
                } else {
                    let retry_records = records.clone();
                    match compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
                        store,
                        &prefix,
                        consumer.as_mut(),
                        records,
                        &delete_requests,
                        &mut tenant_indexes,
                    )
                    .await
                    {
                        Ok(batch_descriptors) => Ok(batch_descriptors),
                        Err(error) => {
                            pending_compaction_records = Some(retry_records);
                            Err(error)
                        }
                    }
                }
            }
            Err(error) => Err(error),
        };
        let batch_descriptors = match batch_result {
            Ok(batch_descriptors) => {
                object_store_retry_backoff = Duration::from_millis(10);
                batch_descriptors
            }
            Err(error) if compactor_run_error_is_object_store(&error) => {
                tenant_indexes.clear();
                tokio::select! {
                    () = &mut shutdown => return Ok(descriptors),
                    () = sleep(object_store_retry_backoff) => {}
                }
                object_store_retry_backoff =
                    next_compactor_object_store_backoff(object_store_retry_backoff);
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if batch_descriptors.is_empty() {
            tokio::select! {
                () = &mut shutdown => return Ok(descriptors),
                () = sleep(Duration::from_millis(10)) => {}
            }
        } else {
            for descriptor in batch_descriptors {
                loop {
                    match advance_and_persist_compaction_frontier(
                        store,
                        &prefix,
                        &compaction_frontier,
                        &descriptor,
                    )
                    .await
                    {
                        Ok(()) => {
                            object_store_retry_backoff = Duration::from_millis(10);
                            // One durable log block persisted to object storage.
                            if let Some(metrics) = &metrics {
                                metrics.record_block_written();
                            }
                            break;
                        }
                        Err(error) if compactor_run_error_is_object_store(&error) => {
                            tokio::select! {
                                () = &mut shutdown => return Err(error.into()),
                                () = sleep(object_store_retry_backoff) => {}
                            }
                            object_store_retry_backoff =
                                next_compactor_object_store_backoff(object_store_retry_backoff);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                descriptors.push(descriptor);
            }
        }
    }
}

fn next_compactor_object_store_backoff(current: Duration) -> Duration {
    std::cmp::min(current * 2, Duration::from_millis(500))
}

fn compactor_run_error_is_object_store(error: &CompactorRunError) -> bool {
    match error {
        CompactorRunError::Wal(KafkaWalCompactionError::Compaction(error))
        | CompactorRunError::Compaction(error) => compaction_error_is_object_store(error),
        CompactorRunError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(_)) => true,
        CompactorRunError::Wal(KafkaWalCompactionError::Decode(_))
        | CompactorRunError::Decode(_)
        | CompactorRunError::Consumer(_)
        | CompactorRunError::Frontier(
            CompactionFrontierStoreError::InvalidVersion { .. }
            | CompactionFrontierStoreError::Json(_),
        )
        | CompactorRunError::DeleteFilter(_)
        | CompactorRunError::MissingSeriesLabels { .. }
        | CompactorRunError::MissingCommitPosition => false,
    }
}

fn compaction_error_is_object_store(error: &CompactionError) -> bool {
    match error {
        CompactionError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactionError::EmptyWalBatch
        | CompactionError::AllRowsDeleted
        | CompactionError::MissingWalPosition { .. }
        | CompactionError::MixedTenant { .. }
        | CompactionError::MixedPartition { .. }
        | CompactionError::Commit(_) => false,
    }
}

fn block_store_error_is_object_store(error: &BlockStoreError) -> bool {
    matches!(error, BlockStoreError::ObjectStore(_))
}

async fn load_existing_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(loaded) => frontier.replace(loaded),
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn shared_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<SharedCompactionFrontier, CompactionFrontierStoreError> {
    let frontier = SharedCompactionFrontier::default();
    load_existing_compaction_frontier(store, prefix, &frontier).await?;
    Ok(frontier)
}

async fn refresh_compaction_frontier_and_prune(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    hot_tail: &BufferedLogHotTail,
) -> Result<usize, CompactionFrontierStoreError> {
    let updated = match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(updated) => updated,
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => return Ok(0),
        Err(error) => return Err(error),
    };
    frontier.replace(updated.clone());
    Ok(hot_tail.prune_compacted(&updated))
}

#[cfg_attr(test, mutants::skip)]
fn spawn_compaction_frontier_refresher(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    frontier: SharedCompactionFrontier,
    hot_tail: BufferedLogHotTail,
    token: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                () = sleep(COMPACTION_FRONTIER_REFRESH_INTERVAL) => {}
            }

            if let Err(error) =
                refresh_compaction_frontier_and_prune(store.as_ref(), &prefix, &frontier, &hot_tail)
                    .await
            {
                eprintln!("[crabka-observability] compaction frontier refresh failed: {error}");
            }
        }
    });
}

async fn advance_and_persist_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    frontier.advance_partition_offset(WalPosition {
        partition: PartitionIndex(descriptor.key.partition),
        offset: Offset(descriptor.key.last_offset),
    });
    write_compaction_frontier_to_object_store(store, prefix, &frontier.snapshot()).await?;
    Ok(())
}

async fn materialize_deletes_then_compact_next_kafka_wal_batch(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Duration,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    materialize_log_deletes_before_compaction(store, prefix, delete_requests, tenant_indexes)
        .await?;
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
    )
    .await
}

async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Duration,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .await
}

async fn materialize_log_deletes_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<(), CompactorRunError> {
    if !active_log_delete_tenants(delete_requests)?.is_empty() {
        materialize_delete_requests_in_existing_object_store_blocks(store, prefix, delete_requests)
            .await?;
        tenant_indexes.clear();
    }
    Ok(())
}

async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // ONE consumer span per poll batch: stitch it onto the ingest trace via the
    // `traceparent` a producer injected in `build_kafka_wal_record`. The first
    // record carrying a trace context is representative of the batch.
    let span = tracing::info_span!(
        "logs_compaction",
        otel.kind = "consumer",
        crabka.wal.records = records.len(),
    );
    if let Some(parent) = records
        .iter()
        .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
    {
        crabka_telemetry::propagation::set_remote_parent(
            &span,
            parent
                .headers
                .iter()
                .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
        );
    }

    compact_polled_kafka_wal_records_inner(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .instrument(span)
    .await
}

async fn compact_polled_kafka_wal_records_inner(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let mut descriptors = Vec::new();
    let mut commit_positions: BTreeMap<PartitionIndex, Offset> = BTreeMap::new();

    for chunk in wal_compaction_chunks(decoded) {
        let tenant = chunk
            .first()
            .ok_or(CompactionError::EmptyWalBatch)?
            .tenant
            .clone();
        let (label_index, block_index) = tenant_indexes
            .entry(tenant.clone())
            .or_insert_with(|| (LabelIndex::default(), BlockIndex::default()));
        let mut committer = LastCompactedPosition::default();
        let time_range = wal_record_time_range(&chunk)?;
        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, &tenant, time_range)?;
        let descriptor = compact_wal_records_to_object_store_with_delete_filters_and_index_output(
            store,
            prefix,
            label_index,
            block_index,
            &mut committer,
            chunk,
            &delete_filters,
            LogCompactionIndexOutput::ShardManifests,
        )
        .await?;
        let position = committer
            .position
            .ok_or(CompactorRunError::MissingCommitPosition)?;
        commit_positions
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
        if let Some(descriptor) = descriptor {
            descriptors.push(descriptor);
        }
    }

    for (partition, offset) in commit_positions {
        consumer
            .commit_compacted(WalPosition { partition, offset })
            .await?;
    }

    Ok(descriptors)
}

async fn poll_accumulated_log_compaction_records(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    initial_timeout: Duration,
) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
    let mut records = consumer.poll(initial_timeout).await?;
    if records.is_empty() || records.len() >= LOG_COMPACTION_MAX_RECORDS_PER_BATCH {
        return Ok(records);
    }

    let deadline = Instant::now() + LOG_COMPACTION_ACCUMULATION_WINDOW;
    while records.len() < LOG_COMPACTION_MAX_RECORDS_PER_BATCH {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        let poll_timeout = std::cmp::min(remaining, LOG_COMPACTION_ACCUMULATION_POLL_TIMEOUT);
        let next = consumer.poll(poll_timeout).await?;
        if next.is_empty() {
            break;
        }
        records.extend(next);
    }

    Ok(records)
}

async fn materialize_delete_requests_in_existing_object_store_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    for tenant in active_log_delete_tenants(delete_requests)? {
        let mut materialized_blocks: BTreeMap<String, Option<BlockDescriptor>> = BTreeMap::new();

        match read_tenant_log_index_manifest_from_object_store(store, prefix, &tenant).await {
            Ok((label_index, block_index)) => {
                if let Some((next_label_index, next_block_index)) =
                    materialize_delete_requests_in_object_store_block_index(
                        store,
                        prefix,
                        &tenant,
                        &label_index,
                        &block_index,
                        delete_requests,
                        &mut materialized_blocks,
                    )
                    .await?
                {
                    write_tenant_log_index_manifest_to_object_store(
                        store,
                        prefix,
                        &tenant,
                        &next_label_index,
                        &next_block_index,
                    )
                    .await?;
                }
            }
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {}
            Err(error) => return Err(error.into()),
        }

        let shard_ranges = match read_tenant_log_index_shard_ranges_from_object_store(
            store, prefix, &tenant,
        )
        .await
        {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        for shard_range in shard_ranges {
            let (label_index, block_index) =
                read_tenant_log_index_shard_from_object_store(store, prefix, &tenant, shard_range)
                    .await?;
            if let Some((next_label_index, next_block_index)) =
                materialize_delete_requests_in_object_store_block_index(
                    store,
                    prefix,
                    &tenant,
                    &label_index,
                    &block_index,
                    delete_requests,
                    &mut materialized_blocks,
                )
                .await?
            {
                write_tenant_log_index_shard_to_object_store(
                    store,
                    prefix,
                    &tenant,
                    shard_range,
                    &next_label_index,
                    &next_block_index,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
fn materialize_delete_requests_in_existing_local_manifest_blocks(
    root: &FsPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    let (label_index, block_index) = match read_log_index_manifest(root) {
        Ok(indexes) => indexes,
        Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let active_tenants = active_log_delete_tenants(delete_requests)?;
    if active_tenants.is_empty() {
        return Ok(());
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let tenant = &block.key.tenant;
        let delete_filters = if active_tenants.contains(tenant) {
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?
        } else {
            Vec::new()
        };
        let mut descriptor = block.clone();

        if !delete_filters.is_empty() {
            let rows = read_log_block(root, &block.key)?;
            let original_len = rows.len();
            let mut kept_rows = Vec::with_capacity(original_len);
            for row in rows {
                let labels = label_index
                    .labels_for(tenant, row.series_fingerprint)
                    .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                        tenant: tenant.clone(),
                        fingerprint: row.series_fingerprint,
                    })?;
                if is_deleted_log_entry(
                    &delete_filters,
                    labels,
                    &row.line,
                    &row.structured_metadata,
                    row.timestamp_ns,
                ) {
                    continue;
                }
                kept_rows.push(row);
            }

            if kept_rows.len() != original_len {
                changed = true;
                if kept_rows.is_empty() {
                    continue;
                }
                descriptor = write_log_block(root, &block.key, kept_rows)?;
            }
        }

        insert_descriptor_labels(&mut next_label_index, &label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    if changed {
        write_log_index_manifest(root, &next_label_index, &next_block_index)?;
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
async fn materialize_delete_requests_in_object_store_block_index(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    delete_requests: &SharedLogDeleteRequests,
    materialized_blocks: &mut BTreeMap<String, Option<BlockDescriptor>>,
) -> Result<Option<(LabelIndex, BlockIndex)>, CompactorRunError> {
    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let object_key = block.key.object_key();
        if let Some(materialized) = materialized_blocks.get(&object_key) {
            match materialized {
                Some(descriptor) => {
                    if descriptor != block {
                        changed = true;
                    }
                    insert_descriptor_labels(
                        &mut next_label_index,
                        label_index,
                        tenant,
                        descriptor,
                    )?;
                    next_block_index.insert(descriptor.clone());
                }
                None => {
                    changed = true;
                }
            }
            continue;
        }

        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?;
        let mut descriptor = block.clone();

        if delete_filters.is_empty() {
            insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
            next_block_index.insert(descriptor);
            continue;
        }

        let rows = read_log_block_from_object_store(store, prefix, &block.key).await?;
        let original_len = rows.len();
        let mut kept_rows = Vec::with_capacity(original_len);
        for row in rows {
            let labels = label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            kept_rows.push(row);
        }

        if kept_rows.len() != original_len {
            changed = true;
            if kept_rows.is_empty() {
                materialized_blocks.insert(object_key, None);
                continue;
            }
            descriptor =
                write_log_block_to_object_store(store, prefix, &block.key, kept_rows).await?;
            materialized_blocks.insert(object_key, Some(descriptor.clone()));
        }

        insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    Ok(changed.then_some((next_label_index, next_block_index)))
}

fn active_log_delete_tenants(
    delete_requests: &SharedLogDeleteRequests,
) -> Result<BTreeSet<String>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .map(|request| request.tenant.clone())
        .collect())
}

fn insert_descriptor_labels(
    target: &mut LabelIndex,
    source: &LabelIndex,
    tenant: &str,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    for fingerprint in &descriptor.fingerprints {
        let labels = source.labels_for(tenant, *fingerprint).ok_or_else(|| {
            CompactorRunError::MissingSeriesLabels {
                tenant: tenant.to_string(),
                fingerprint: *fingerprint,
            }
        })?;
        target.insert_series(tenant.to_string(), labels.clone());
    }
    Ok(())
}

fn wal_compaction_chunks(records: Vec<WalLogRecord>) -> Vec<Vec<WalLogRecord>> {
    let mut chunks: Vec<Vec<WalLogRecord>> = Vec::new();
    for record in records {
        let Some(position) = record.position else {
            chunks.push(vec![record]);
            continue;
        };
        if let Some(chunk) = chunks.last_mut()
            && chunk.first().is_some_and(|first| {
                first.tenant == record.tenant
                    && first.position.is_some_and(|first_position| {
                        first_position.partition == position.partition
                    })
            })
        {
            chunk.push(record);
        } else {
            chunks.push(vec![record]);
        }
    }
    chunks
}

fn wal_record_time_range(records: &[WalLogRecord]) -> Result<TimeRange, CompactionError> {
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    for record in records.iter().skip(1) {
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
    }
    Ok(TimeRange::new(start_ns, end_ns)?)
}

type TenantCompactionIndexCache = BTreeMap<String, (LabelIndex, BlockIndex)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogCompactionIndexOutput {
    FullManifestAndShardCatalog,
    ShardManifests,
}

pub async fn compact_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        key,
        label_index,
        block_index,
        rows,
        LogCompactionIndexOutput::FullManifestAndShardCatalog,
    )
    .await
}

async fn compact_log_block_to_object_store_with_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
    index_output: LogCompactionIndexOutput,
) -> Result<BlockDescriptor, BlockStoreError> {
    let descriptor = write_log_block_to_object_store(store, prefix, key, rows).await?;
    block_index.insert(descriptor.clone());
    write_tenant_compaction_indexes_to_object_store(
        store,
        prefix,
        &key.tenant,
        &descriptor,
        label_index,
        block_index,
        index_output,
    )
    .await?;
    Ok(descriptor)
}

async fn write_tenant_compaction_indexes_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    new_descriptor: &BlockDescriptor,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    index_output: LogCompactionIndexOutput,
) -> Result<(), BlockStoreError> {
    if index_output == LogCompactionIndexOutput::ShardManifests {
        let mut shard_block_index = BlockIndex::default();
        shard_block_index.insert(new_descriptor.clone());
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            new_descriptor.key.time_range,
            label_index,
            &shard_block_index,
        )
        .await?;
        return Ok(());
    }

    if index_output == LogCompactionIndexOutput::FullManifestAndShardCatalog {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_to_object_store(
        store,
        prefix,
        tenant,
        new_descriptor.key.time_range,
        label_index,
        block_index,
    )
    .await?;

    let mut shard_ranges =
        match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error),
        };
    if !shard_ranges.contains(&new_descriptor.key.time_range) {
        shard_ranges.push(new_descriptor.key.time_range);
    }
    shard_ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, &shard_ranges).await
}

pub trait CompactionOffsetCommitter {
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError>;
}

#[derive(Debug, Error)]
#[error("offset commit failed")]
pub struct CompactionCommitError;

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("cannot compact an empty WAL batch")]
    EmptyWalBatch,
    #[error("cannot compact WAL batch after all rows were deleted")]
    AllRowsDeleted,
    #[error("missing WAL position for record at timestamp {timestamp_ns}")]
    MissingWalPosition { timestamp_ns: i64 },
    #[error("cannot compact mixed-tenant WAL batch: expected {expected}, got {actual}")]
    MixedTenant { expected: String, actual: String },
    #[error("cannot compact mixed-partition WAL batch: expected {expected}, got {actual}")]
    MixedPartition { expected: i32, actual: i32 },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}

#[derive(Debug, Error)]
pub enum KafkaWalCompactionError {
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
}

#[derive(Debug, Error)]
pub enum CompactorRunError {
    #[error(transparent)]
    Wal(#[from] KafkaWalCompactionError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("compacted WAL batch did not report a commit position")]
    MissingCommitPosition,
}

#[derive(Debug, Error)]
pub enum CompactionFrontierStoreError {
    #[error("invalid compaction frontier manifest version {actual}; expected {expected}")]
    InvalidVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

pub async fn compact_next_kafka_wal_batch_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Duration,
) -> Result<Option<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    if records.is_empty() {
        return Ok(None);
    }

    let mut committer = LastCompactedPosition::default();
    let descriptor = compact_kafka_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        &mut committer,
        records,
    )
    .await?;
    let position = committer
        .position
        .ok_or(CompactorRunError::MissingCommitPosition)?;
    consumer.commit_compacted(position).await?;

    Ok(Some(descriptor))
}

#[derive(Default)]
struct LastCompactedPosition {
    position: Option<WalPosition>,
}

impl CompactionOffsetCommitter for LastCompactedPosition {
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError> {
        self.position = Some(position);
        Ok(())
    }
}

pub async fn compact_kafka_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<KafkaWalRecord>,
) -> Result<BlockDescriptor, KafkaWalCompactionError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(compact_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        decoded,
    )
    .await?)
}

pub async fn compact_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
) -> Result<BlockDescriptor, CompactionError> {
    compact_wal_records_to_object_store_with_delete_filters_and_index_output(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        records,
        &[],
        LogCompactionIndexOutput::FullManifestAndShardCatalog,
    )
    .await?
    .ok_or(CompactionError::AllRowsDeleted)
}

async fn compact_wal_records_to_object_store_with_delete_filters_and_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
    delete_filters: &[ActiveLogDeleteFilter],
    index_output: LogCompactionIndexOutput,
) -> Result<Option<BlockDescriptor>, CompactionError> {
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let tenant = first.tenant.clone();
    let first_position = first.position.ok_or(CompactionError::MissingWalPosition {
        timestamp_ns: first.timestamp_ns,
    })?;
    let partition = first_position.partition;
    let mut first_offset = first_position.offset;
    let mut last_offset = first_position.offset;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    let mut staged_label_index = label_index.clone();
    let mut rows = Vec::with_capacity(records.len());

    for record in records {
        if record.tenant != tenant {
            return Err(CompactionError::MixedTenant {
                expected: tenant,
                actual: record.tenant,
            });
        }
        let position = record.position.ok_or(CompactionError::MissingWalPosition {
            timestamp_ns: record.timestamp_ns,
        })?;
        if position.partition != partition {
            return Err(CompactionError::MixedPartition {
                expected: partition.get(),
                actual: position.partition.get(),
            });
        }

        first_offset = first_offset.min(position.offset);
        last_offset = last_offset.max(position.offset);
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
        if is_deleted_log_entry(
            delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) {
            continue;
        }
        let fingerprint = staged_label_index.insert_series(&tenant, record.labels);
        rows.push(LogRow::new(
            fingerprint,
            record.timestamp_ns,
            record.line,
            record.structured_metadata,
        ));
    }

    if rows.is_empty() {
        committer.commit_compacted(WalPosition {
            partition,
            offset: last_offset,
        })?;
        return Ok(None);
    }

    let key = BlockKey::new(
        tenant,
        partition.get(),
        first_offset.get(),
        last_offset.get(),
        TimeRange::new(start_ns, end_ns)?,
    );
    let mut staged_block_index = block_index.clone();
    let descriptor = compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        &key,
        &staged_label_index,
        &mut staged_block_index,
        rows,
        index_output,
    )
    .await?;

    committer.commit_compacted(WalPosition {
        partition,
        offset: last_offset,
    })?;
    *label_index = staged_label_index;
    *block_index = staged_block_index;

    Ok(Some(descriptor))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalPosition {
    pub partition: PartitionIndex,
    pub offset: Offset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalLogRecord {
    pub tenant: String,
    pub labels: Labels,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: BTreeMap<String, String>,
    pub position: Option<WalPosition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalRecord {
    pub value: Vec<u8>,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<KafkaWalHeader>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionFrontier {
    pub compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl CompactionFrontier {
    #[must_use]
    pub fn new(compacted_through_ns: i64) -> Self {
        Self {
            compacted_through_ns,
            partition_offsets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_partition_offset(mut self, partition: PartitionIndex, offset: Offset) -> Self {
        self.partition_offsets.insert(partition, offset);
        self
    }

    pub fn advance_partition_offset(&mut self, position: WalPosition) {
        self.partition_offsets
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
    }

    fn is_compacted(&self, record: &WalLogRecord) -> bool {
        if let Some(position) = record.position
            && self
                .partition_offsets
                .get(&position.partition)
                .is_some_and(|offset| position.offset <= *offset)
        {
            return true;
        }

        record.timestamp_ns <= self.compacted_through_ns
    }
}

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    frontier: Arc<Mutex<CompactionFrontier>>,
}

impl SharedCompactionFrontier {
    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(frontier)),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> CompactionFrontier {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
    }

    pub fn advance_partition_offset(&self, position: WalPosition) {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .advance_partition_offset(position);
    }

    pub fn replace(&self, frontier: CompactionFrontier) {
        *self.frontier.lock().expect("frontier mutex poisoned") = frontier;
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}

const COMPACTION_FRONTIER_MANIFEST_VERSION: u32 = 1;
const COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH: &str = "index/logs/compaction-frontier.json";

#[derive(Deserialize, Serialize)]
struct CompactionFrontierManifest {
    version: u32,
    compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl From<&CompactionFrontier> for CompactionFrontierManifest {
    fn from(frontier: &CompactionFrontier) -> Self {
        Self {
            version: COMPACTION_FRONTIER_MANIFEST_VERSION,
            compacted_through_ns: frontier.compacted_through_ns,
            partition_offsets: frontier.partition_offsets.clone(),
        }
    }
}

impl TryFrom<CompactionFrontierManifest> for CompactionFrontier {
    type Error = CompactionFrontierStoreError;

    fn try_from(manifest: CompactionFrontierManifest) -> Result<Self, Self::Error> {
        if manifest.version != COMPACTION_FRONTIER_MANIFEST_VERSION {
            return Err(CompactionFrontierStoreError::InvalidVersion {
                actual: manifest.version,
                expected: COMPACTION_FRONTIER_MANIFEST_VERSION,
            });
        }

        Ok(Self {
            compacted_through_ns: manifest.compacted_through_ns,
            partition_offsets: manifest.partition_offsets,
        })
    }
}

pub async fn write_compaction_frontier_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &CompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    let payload = serde_json::to_vec_pretty(&CompactionFrontierManifest::from(frontier))?;
    store
        .put(
            &compaction_frontier_manifest_object_path(prefix),
            payload.into(),
        )
        .await?;
    Ok(())
}

pub async fn read_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<CompactionFrontier, CompactionFrontierStoreError> {
    let bytes = store
        .get(&compaction_frontier_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: CompactionFrontierManifest = serde_json::from_slice(&bytes)?;
    manifest.try_into()
}

fn compaction_frontier_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}

#[derive(Clone, Debug)]
enum CompactionFrontierSource {
    Snapshot(CompactionFrontier),
    Shared(SharedCompactionFrontier),
}

impl CompactionFrontierSource {
    fn snapshot(&self) -> CompactionFrontier {
        match self {
            Self::Snapshot(frontier) => frontier.clone(),
            Self::Shared(frontier) => frontier.snapshot(),
        }
    }
}

struct ConfiguredObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

type CompactionFrontierRefreshSource = (Arc<dyn ObjectStore>, ObjectPath);

#[cfg_attr(test, mutants::skip)]
fn build_configured_object_store(
    config: &ServiceConfig,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    let Some(raw_url) = config.object_store_url.as_deref() else {
        return Ok(None);
    };

    match Url::parse(raw_url) {
        Ok(url) if url.scheme() == "file" => {
            let path =
                url.to_file_path()
                    .map_err(|()| ServiceConfigError::InvalidObjectStoreUrl {
                        url: raw_url.to_string(),
                        reason: "file URL must map to a local filesystem path".to_string(),
                    })?;
            Ok(Some(ConfiguredObjectStore {
                store: Arc::new(LocalFileSystem::new_with_prefix(path)?),
                prefix: ObjectPath::from(""),
            }))
        }
        Ok(url) => {
            let (store, prefix) = parse_url_opts(&url, std::env::vars())?;
            Ok(Some(ConfiguredObjectStore {
                store: Arc::from(store),
                prefix,
            }))
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Ok(Some(ConfiguredObjectStore {
            store: Arc::new(LocalFileSystem::new_with_prefix(raw_url)?),
            prefix: ObjectPath::from(""),
        })),
        Err(error) => Err(ServiceConfigError::InvalidObjectStoreUrl {
            url: raw_url.to_string(),
            reason: error.to_string(),
        }),
    }
}

async fn load_querier_shared_compaction_frontier(
    config: &ServiceConfig,
    configured_store: Option<&ConfiguredObjectStore>,
    object_store: Option<&dyn ObjectStore>,
) -> Result<
    (
        Option<SharedCompactionFrontier>,
        Option<CompactionFrontierRefreshSource>,
    ),
    ServiceConfigError,
> {
    if let Some(configured_store) = configured_store
        && let Some(prefix) = querier_object_store_prefix(config, Some(&configured_store.prefix))?
    {
        let frontier =
            shared_compaction_frontier_from_object_store(configured_store.store.as_ref(), &prefix)
                .await?;
        return Ok((
            Some(frontier),
            Some((configured_store.store.clone(), prefix)),
        ));
    }

    if let Some(store) = object_store
        && let Some(prefix) = querier_object_store_prefix(config, None)?
    {
        return Ok((
            Some(shared_compaction_frontier_from_object_store(store, &prefix).await?),
            None,
        ));
    }

    Ok((None, None))
}

fn compactor_delete_requests_for_config(
    config: &ServiceConfig,
    provided: Option<SharedLogDeleteRequests>,
) -> Result<SharedLogDeleteRequests, LogDeleteRequestStoreError> {
    match provided {
        Some(delete_requests) => Ok(delete_requests),
        None => SharedLogDeleteRequests::from_data_root(&config.data_root),
    }
}

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;
}

#[async_trait]
pub trait LogIngestLimiter: Send + Sync + 'static {
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError>;
}

#[async_trait]
pub trait LogQueryAuthorizer: Send + Sync + 'static {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError>;
}

pub trait LogHotTail: Send + Sync + 'static {
    fn records(&self) -> Vec<WalLogRecord>;

    /// Return the hot-tail records whose `timestamp_ns` falls within the inclusive
    /// window `[start_ns, end_ns]`.
    ///
    /// Callers re-apply their exact per-record time bound downstream, so this may
    /// return a *superset* of the in-window records (e.g. records sharing a coarse
    /// time bucket with the window edges); it MUST NOT drop any record whose
    /// timestamp lies in `[start_ns, end_ns]`. The default implementation simply
    /// filters [`LogHotTail::records`], preserving its order; implementations that
    /// maintain a time index (see [`BufferedLogHotTail`]) override this to avoid a
    /// full-buffer scan.
    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .collect()
    }
}

#[async_trait]
pub trait LogWalConsumer: Send + 'static {
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<KafkaWalRecord>, WalConsumerError>;

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryWalSink {
    records: Arc<Mutex<Vec<WalLogRecord>>>,
}

impl InMemoryWalSink {
    #[must_use]
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.records.lock().expect("wal sink lock poisoned").clone()
    }
}

#[async_trait]
impl LogWalSink for InMemoryWalSink {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        self.records
            .lock()
            .expect("wal sink lock poisoned")
            .push(record);
        Ok(())
    }
}

impl LogHotTail for InMemoryWalSink {
    fn records(&self) -> Vec<WalLogRecord> {
        InMemoryWalSink::records(self)
    }
}

#[derive(Clone, Debug, Default)]
struct AllowAllIngestLimiter;

#[async_trait]
impl LogIngestLimiter for AllowAllIngestLimiter {
    async fn check(
        &self,
        _tenant: &str,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}

struct BrokerBackedQueryAuthorizer {
    admin: tokio::sync::Mutex<AdminClient>,
    wal_topic: String,
}

impl BrokerBackedQueryAuthorizer {
    async fn connect(bootstrap: &str, wal_topic: String) -> Result<Self, AdminError> {
        let admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
        })
    }
}

#[async_trait]
impl LogQueryAuthorizer for BrokerBackedQueryAuthorizer {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let acls = {
            let mut admin = self.admin.lock().await;
            admin
                .describe_acls(&AclEntryFilter::default())
                .await
                .map_err(|error| QueryAuthorizationError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                })?
        };
        check_tenant_wal_read_acl(tenant, &self.wal_topic, &acls)
    }
}

/// A [`LogQueryAuthorizer`] whose underlying implementation can be swapped after construction.
///
/// Used by the querier to start with a permissive allow-all policy while the real
/// [`BrokerBackedQueryAuthorizer`] connects asynchronously in the background (FIX B2).
struct SwappableQueryAuthorizer {
    inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
}

impl SwappableQueryAuthorizer {
    /// Create a new swappable authorizer starting with `AllowAllQueryAuthorizer`.
    fn new() -> (Self, Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>) {
        let inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>> =
            Arc::new(tokio::sync::RwLock::new(Arc::new(AllowAllQueryAuthorizer)));
        (
            Self {
                inner: inner.clone(),
            },
            inner,
        )
    }
}

#[async_trait]
impl LogQueryAuthorizer for SwappableQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let authorizer = self.inner.read().await.clone();
        authorizer.check(tenant).await
    }
}

const PRODUCER_BYTE_RATE_QUOTA_KEY: &str = "producer_byte_rate";

struct BrokerBackedIngestLimiter {
    admin: tokio::sync::Mutex<AdminClient>,
    wal_topic: String,
    buckets: Mutex<BTreeMap<String, IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    async fn connect(bootstrap: &str, wal_topic: String) -> Result<Self, AdminError> {
        let admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
            buckets: Mutex::new(BTreeMap::new()),
        })
    }
}

#[async_trait]
impl LogIngestLimiter for BrokerBackedIngestLimiter {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError> {
        let (acls, quota) = {
            let mut admin = self.admin.lock().await;
            let acls = admin
                .describe_acls(&AclEntryFilter::default())
                .await
                .map_err(|error| IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                })?;
            let quota = admin.describe_user_quotas(tenant).await.map_err(|error| {
                IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                }
            })?;
            (acls, quota)
        };
        check_tenant_wal_write_acl(tenant, &self.wal_topic, &acls)?;

        let Some(rate) = quota.get(PRODUCER_BYTE_RATE_QUOTA_KEY).copied() else {
            return Ok(());
        };
        if !rate.is_finite() || rate <= 0.0 {
            return Ok(());
        }

        let bytes = ingest_quota_bytes(records);
        if bytes == 0 {
            return Ok(());
        }

        let mut buckets = self.buckets.lock().expect("ingest quota lock poisoned");
        let bucket = buckets
            .entry(tenant.to_string())
            .or_insert_with(|| IngestQuotaBucket::new(rate));
        bucket.update_rate(rate);
        if bucket.consume(bytes as f64) {
            return Ok(());
        }

        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: format!(
                "{PRODUCER_BYTE_RATE_QUOTA_KEY} quota {rate:.0} bytes/s exceeded by {bytes} byte ingest batch"
            ),
        })
    }
}

fn check_tenant_wal_write_acl(
    tenant: &str,
    wal_topic: &str,
    acls: &[AclEntry],
) -> Result<(), IngestLimitError> {
    if acls.is_empty() {
        return Ok(());
    }

    let principal = format!("User:{tenant}");
    let mut allowed = false;
    for acl in acls {
        if !acl_matches_tenant_wal_write(acl, &principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(IngestLimitError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant write ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant write ACL for WAL topic `{wal_topic}`"),
        })
    }
}

fn acl_matches_tenant_wal_write(acl: &AclEntry, principal: &str, wal_topic: &str) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Write)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

fn check_tenant_wal_read_acl(
    tenant: &str,
    wal_topic: &str,
    acls: &[AclEntry],
) -> Result<(), QueryAuthorizationError> {
    if acls.is_empty() {
        return Ok(());
    }

    let principal = format!("User:{tenant}");
    let mut allowed = false;
    for acl in acls {
        if !acl_matches_tenant_wal_read(acl, &principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(QueryAuthorizationError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant read ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant read ACL for WAL topic `{wal_topic}`"),
        })
    }
}

fn acl_matches_tenant_wal_read(acl: &AclEntry, principal: &str, wal_topic: &str) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Read)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

fn matches_acl_topic_pattern(acl: &AclEntry, wal_topic: &str) -> bool {
    match acl.pattern_type {
        PatternType::Literal => acl.resource_name == wal_topic || acl.resource_name == "*",
        PatternType::Prefixed => wal_topic.starts_with(&acl.resource_name),
    }
}

#[derive(Debug)]
struct IngestQuotaBucket {
    rate_per_second: f64,
    available: f64,
    updated_at: Instant,
}

impl IngestQuotaBucket {
    fn new(rate_per_second: f64) -> Self {
        Self {
            rate_per_second,
            available: rate_per_second,
            updated_at: Instant::now(),
        }
    }

    fn update_rate(&mut self, rate_per_second: f64) {
        self.refill();
        self.rate_per_second = rate_per_second;
        if self.available > self.capacity() {
            self.available = self.capacity();
        }
    }

    fn consume(&mut self, bytes: f64) -> bool {
        self.refill();
        if bytes > self.available {
            return false;
        }
        self.available -= bytes;
        true
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.updated_at);
        self.updated_at = now;
        self.available =
            (self.available + elapsed.as_secs_f64() * self.rate_per_second).min(self.capacity());
    }

    fn capacity(&self) -> f64 {
        self.rate_per_second
    }
}

fn ingest_quota_bytes(records: &[WalLogRecord]) -> usize {
    records
        .iter()
        .map(|record| {
            record.tenant.len()
                + record.line.len()
                + std::mem::size_of_val(&record.timestamp_ns)
                + record
                    .labels
                    .iter()
                    .map(|(name, value)| name.len() + value.len())
                    .sum::<usize>()
                + record
                    .structured_metadata
                    .iter()
                    .map(|(name, value)| name.len() + value.len())
                    .sum::<usize>()
        })
        .sum()
}

/// Width of a hot-tail time bucket, in nanoseconds (one minute).
///
/// Coarse enough that a wide retention window holds few buckets, fine enough that
/// a typical query window (minutes to hours) only touches the buckets it overlaps.
const HOT_TAIL_BUCKET_NS: i64 = 60_000_000_000;

/// Map a record timestamp to its bucket key. Uses [`i64::div_euclid`] so negative
/// timestamps (pre-epoch) still bucket monotonically and the bucket containing a
/// given timestamp is unambiguous.
fn hot_tail_bucket_key(timestamp_ns: i64) -> i64 {
    timestamp_ns.div_euclid(HOT_TAIL_BUCKET_NS)
}

/// Buffer holding polled hot-tail records.
///
/// Records arrive from Kafka polling in NO timestamp order, so the buffer keeps two
/// views of the same data:
///
/// * `records` — the append-ordered log. [`records`](Self::records) clones this whole
///   list; it backs the WebSocket tail path, which indexes into the buffer by arrival
///   order and must see every record.
/// * `buckets` — a per-minute time index mapping each [`hot_tail_bucket_key`] to the
///   positions in `records` whose timestamps land in that bucket. Out-of-order arrivals
///   simply land in the correct bucket; no global sort is ever needed.
///
/// [`records_in_range`](Self::records_in_range) walks only the buckets overlapping the
/// query window, so a 30-minute query over a buffer holding hours of logs touches only
/// the window's records instead of scanning the entire buffer.
#[derive(Debug, Default)]
struct HotTailBuffer {
    records: Vec<WalLogRecord>,
    buckets: BTreeMap<i64, Vec<usize>>,
}

impl HotTailBuffer {
    fn push(&mut self, record: WalLogRecord) {
        let index = self.records.len();
        let bucket = hot_tail_bucket_key(record.timestamp_ns);
        self.records.push(record);
        self.buckets.entry(bucket).or_default().push(index);
    }

    fn prune_compacted(&mut self, frontier: &CompactionFrontier) -> usize {
        let before = self.records.len();
        if before == 0 {
            return 0;
        }

        let old_records = std::mem::take(&mut self.records);
        self.records = old_records
            .into_iter()
            .filter(|record| !frontier.is_compacted(record))
            .collect();
        let pruned = before - self.records.len();
        if pruned > 0 {
            self.records.shrink_to_fit();
            self.rebuild_buckets();
        }
        pruned
    }

    fn rebuild_buckets(&mut self) {
        self.buckets.clear();
        for (index, record) in self.records.iter().enumerate() {
            self.buckets
                .entry(hot_tail_bucket_key(record.timestamp_ns))
                .or_default()
                .push(index);
        }
        for indices in self.buckets.values_mut() {
            indices.shrink_to_fit();
        }
    }

    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        if start_ns > end_ns {
            return Vec::new();
        }
        let start_bucket = hot_tail_bucket_key(start_ns);
        let end_bucket = hot_tail_bucket_key(end_ns);
        let mut matches: Vec<usize> = Vec::new();
        for (_bucket, indices) in self.buckets.range(start_bucket..=end_bucket) {
            for &index in indices {
                let record = &self.records[index];
                if record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns {
                    matches.push(index);
                }
            }
        }
        // Restore append order so the windowed slice matches a full-buffer scan
        // exactly (downstream collects into a BTreeMap and re-sorts, so order is not
        // load-bearing, but matching the full-scan order keeps the two paths trivially
        // equivalent for testing and reasoning).
        matches.sort_unstable();
        matches
            .into_iter()
            .map(|index| self.records[index].clone())
            .collect()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BufferedLogHotTail {
    buffer: Arc<Mutex<HotTailBuffer>>,
}

impl BufferedLogHotTail {
    #[must_use]
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records
            .clone()
    }

    #[must_use]
    pub fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records_in_range(start_ns, end_ns)
    }

    pub fn append_records(&self, records: Vec<WalLogRecord>) {
        let mut buffer = self.buffer.lock().expect("hot tail buffer lock poisoned");
        for record in records {
            buffer.push(record);
        }
    }

    #[must_use]
    pub fn prune_compacted(&self, frontier: &CompactionFrontier) -> usize {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .prune_compacted(frontier)
    }
}

impl LogHotTail for BufferedLogHotTail {
    fn records(&self) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records(self)
    }

    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records_in_range(self, start_ns, end_ns)
    }
}

#[derive(Clone)]
pub struct KafkaLogWalSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaLogWalSink {
    #[must_use]
    pub fn new(producer: Producer, topic: impl Into<String>) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: topic.into(),
        }
    }

    #[cfg_attr(test, mutants::skip)]
    pub async fn connect(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ProducerError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .client_id("crabka-observability-distributor")
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self::new(producer, topic))
    }
}

#[async_trait]
impl LogWalSink for KafkaLogWalSink {
    #[cfg_attr(test, mutants::skip)]
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        let delivery = self
            .producer
            .send(build_kafka_wal_record(&self.topic, &record)?)
            .await;
        delivery
            .await
            .map_err(|_| WalSinkError::DeliveryCanceled)??;
        Ok(())
    }
}

pub struct KafkaLogWalConsumer {
    consumer: Consumer,
}

impl KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    pub async fn connect(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ConsumerError> {
        let topic = topic.into();
        let consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id("crabka-observability-compactor")
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(vec![topic])
            .build()
            .await?;
        Ok(Self { consumer })
    }

    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn close(self) {
        let _ = self.consumer.close().await;
    }
}

#[async_trait]
impl LogWalConsumer for KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    async fn poll(&mut self, timeout: Duration) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        self.consumer
            .poll(timeout)
            .await?
            .into_iter()
            .map(|record| {
                let value = record
                    .value
                    .ok_or_else(|| WalConsumerError::MissingValue {
                        topic: record.topic.clone(),
                        partition: record.partition,
                        offset: record.offset,
                    })?
                    .to_vec();
                Ok(KafkaWalRecord {
                    value,
                    partition: PartitionIndex(record.partition),
                    offset: Offset(record.offset),
                    timestamp_ms: Some(record.timestamp),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|header| KafkaWalHeader {
                            key: header.key,
                            value: header.value.map(|value| value.to_vec()),
                        })
                        .collect(),
                })
            })
            .collect()
    }

    #[cfg_attr(test, mutants::skip)]
    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        self.consumer.commit_sync().await?;
        Ok(())
    }
}

pub fn build_kafka_wal_record(
    topic: impl Into<String>,
    record: &WalLogRecord,
) -> Result<ProducerRecord, WalSinkError> {
    let fingerprint = series_fingerprint(&record.labels);
    let mut headers = vec![
        ProducerHeader {
            key: "crabka-wal-record-type".to_string(),
            value: Some(Bytes::from_static(b"log")),
        },
        ProducerHeader {
            key: "crabka-tenant".to_string(),
            value: Some(Bytes::from(record.tenant.clone())),
        },
    ];
    // Inject the current span's W3C trace context (`traceparent`/`tracestate`)
    // so the compactor can stitch its consume/compaction span onto the ingest
    // trace. Additive: the record body is unchanged, and this is a no-op when
    // there is no active/sampled span.
    for (key, value) in crabka_telemetry::propagation::current_trace_headers() {
        headers.push(ProducerHeader {
            key,
            value: Some(Bytes::from(value.into_bytes())),
        });
    }
    Ok(ProducerRecord {
        topic: topic.into(),
        partition: None,
        key: Some(Bytes::from(format!("{}:{fingerprint}", record.tenant))),
        value: Some(Bytes::from(serde_json::to_vec(record)?)),
        headers,
        timestamp_ms: Some(record.timestamp_ns / 1_000_000),
    })
}

pub fn decode_kafka_wal_record(
    value: &[u8],
    partition: PartitionIndex,
    offset: Offset,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let mut record: WalLogRecord = serde_json::from_slice(value)?;
    record.position = Some(WalPosition { partition, offset });
    Ok(record)
}

pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}

pub async fn poll_log_hot_tail_once(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Duration,
) -> Result<usize, HotTailPollError> {
    poll_log_hot_tail_once_with_frontier(consumer, hot_tail, timeout, None).await
}

async fn poll_log_hot_tail_once_with_frontier(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Duration,
    frontier: Option<&SharedCompactionFrontier>,
) -> Result<usize, HotTailPollError> {
    let batch = consumer.poll(timeout).await?;
    let records = batch
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let decoded = records.len();
    hot_tail.append_records(records);
    if let Some(frontier) = frontier {
        let _ = hot_tail.prune_compacted(&frontier.snapshot());
    }
    Ok(decoded)
}

#[cfg_attr(test, mutants::skip)]
fn spawn_log_hot_tail_poller(
    consumer: Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>,
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
) {
    tokio::spawn(async move {
        loop {
            let result = {
                let mut consumer = consumer.lock().await;
                poll_log_hot_tail_once_with_frontier(
                    consumer.as_mut(),
                    &hot_tail,
                    Duration::from_millis(50),
                    frontier.as_ref(),
                )
                .await
            };
            let should_back_off = match result {
                Ok(decoded) => decoded == 0,
                Err(_) => true,
            };
            if should_back_off {
                sleep(Duration::from_millis(50)).await;
            }
        }
    });
}

/// Spawn a background task that retries `KafkaLogWalConsumer::connect` until it succeeds,
/// then runs the hot-tail poll loop.  On a cold boot the Kafka broker may not be ready yet;
/// retrying here lets the querier serve its HTTP port immediately (FIX B2).
///
/// Cancelling `token` causes the poll loop to exit and calls `consumer.close()` to send
/// LeaveGroup, removing the consumer from the broker's group immediately on graceful shutdown.
#[cfg_attr(test, mutants::skip)]
fn spawn_wal_hot_tail_connect_and_poll(
    bootstrap: String,
    group_id: String,
    topic: String,
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consumer = loop {
            tokio::select! {
                () = token.cancelled() => return,
                result = KafkaLogWalConsumer::connect(&bootstrap, group_id.clone(), topic.clone()) => {
                    match result {
                        Ok(c) => break c,
                        Err(error) => {
                            eprintln!(
                                "[crabka-observability] querier WAL consumer connect failed; retrying: {error}"
                            );
                            tokio::select! {
                                () = token.cancelled() => return,
                                () = sleep(Duration::from_millis(500)) => {}
                            }
                        }
                    }
                }
            }
        };
        loop {
            let result = tokio::select! {
                () = token.cancelled() => break,
                result = poll_log_hot_tail_once_with_frontier(&mut consumer, &hot_tail, Duration::from_millis(50), frontier.as_ref()) => result,
            };
            let should_back_off = match result {
                Ok(decoded) => decoded == 0,
                Err(_) => true,
            };
            if should_back_off {
                tokio::select! {
                    () = token.cancelled() => break,
                    () = sleep(Duration::from_millis(50)) => {}
                }
            }
        }
        consumer.close().await;
    })
}

/// Spawn a background task that retries `BrokerBackedQueryAuthorizer::connect` until it succeeds,
/// then swaps the provided `slot` from AllowAll to the real broker-backed authorizer (FIX B2).
#[cfg_attr(test, mutants::skip)]
fn spawn_query_authorizer_connect(
    bootstrap: String,
    topic: String,
    slot: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
) {
    tokio::spawn(async move {
        let authorizer = loop {
            match BrokerBackedQueryAuthorizer::connect(&bootstrap, topic.clone()).await {
                Ok(a) => break a,
                Err(error) => {
                    eprintln!(
                        "[crabka-observability] querier authorizer connect failed; retrying: {error}"
                    );
                    sleep(Duration::from_millis(500)).await;
                }
            }
        };
        let mut guard = slot.write().await;
        *guard = Arc::new(authorizer);
        eprintln!(
            "[crabka-observability] querier query authorizer promoted to broker-backed ACL checks"
        );
    });
}

fn has_native_kafka_log_headers(headers: &[KafkaWalHeader]) -> bool {
    headers.iter().any(|header| {
        header.key == "crabka-log-timestamp-ns"
            || header.key.starts_with("crabka-log-label-")
            || (header.key == "crabka-wal-record-type"
                && header
                    .value
                    .as_deref()
                    .is_some_and(|value| value == b"log-line"))
    })
}

fn decode_native_kafka_log_record(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let tenant = required_kafka_header_utf8(&record.headers, "crabka-tenant")?;
    let timestamp_ns = if let Some(value) =
        optional_kafka_header_utf8(&record.headers, "crabka-log-timestamp-ns")?
    {
        let timestamp_ns =
            value
                .parse()
                .map_err(|source| WalRecordDecodeError::InvalidNativeTimestamp {
                    value: value.clone(),
                    source,
                })?;
        validate_native_timestamp_ns(timestamp_ns, value)?
    } else {
        let timestamp_ms =
            record
                .timestamp_ms
                .ok_or_else(|| WalRecordDecodeError::MissingNativeHeader {
                    name: "crabka-log-timestamp-ns".to_string(),
                })?;
        native_timestamp_ms_to_ns(timestamp_ms)?
    };
    let labels = kafka_headers_with_prefix(&record.headers, "crabka-log-label-", |name| {
        WalRecordDecodeError::DuplicateNativeLabelName { name }
    })?;
    if labels.is_empty() {
        return Err(WalRecordDecodeError::MissingNativeLabels);
    }
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(WalRecordDecodeError::InvalidNativeLabelName { name: name.clone() });
    }
    let structured_metadata =
        kafka_headers_with_prefix(&record.headers, "crabka-log-metadata-", |name| {
            WalRecordDecodeError::DuplicateNativeMetadataName { name }
        })?;
    if let Some(name) = structured_metadata
        .keys()
        .find(|name| !is_loki_label_name(name))
    {
        return Err(WalRecordDecodeError::InvalidNativeMetadataName { name: name.clone() });
    }
    let line = String::from_utf8(record.value)
        .map_err(|_| WalRecordDecodeError::InvalidNativeLogLineUtf8)?;

    Ok(WalLogRecord {
        tenant,
        labels,
        timestamp_ns,
        line,
        structured_metadata,
        position: Some(WalPosition {
            partition: record.partition,
            offset: record.offset,
        }),
    })
}

fn required_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<String, WalRecordDecodeError> {
    optional_kafka_header_utf8(headers, name)?.ok_or_else(|| {
        WalRecordDecodeError::MissingNativeHeader {
            name: name.to_string(),
        }
    })
}

fn optional_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<Option<String>, WalRecordDecodeError> {
    let Some(header) = headers.iter().find(|header| header.key == name) else {
        return Ok(None);
    };
    let value =
        header
            .value
            .as_ref()
            .ok_or_else(|| WalRecordDecodeError::MissingNativeHeaderValue {
                name: name.to_string(),
            })?;
    String::from_utf8(value.clone()).map(Some).map_err(|_| {
        WalRecordDecodeError::InvalidNativeHeaderUtf8 {
            name: name.to_string(),
        }
    })
}

fn native_timestamp_ms_to_ns(timestamp_ms: i64) -> Result<i64, WalRecordDecodeError> {
    let converted_ns = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        WalRecordDecodeError::InvalidNativeTimestampValue {
            value: timestamp_ms.to_string(),
        }
    })?;
    validate_native_timestamp_ns(converted_ns, timestamp_ms.to_string())
}

fn validate_native_timestamp_ns(
    timestamp_ns: i64,
    value: String,
) -> Result<i64, WalRecordDecodeError> {
    if timestamp_ns < 0 {
        Err(WalRecordDecodeError::InvalidNativeTimestampValue { value })
    } else {
        Ok(timestamp_ns)
    }
}

fn kafka_headers_with_prefix(
    headers: &[KafkaWalHeader],
    prefix: &str,
    duplicate_error: impl Fn(String) -> WalRecordDecodeError,
) -> Result<BTreeMap<String, String>, WalRecordDecodeError> {
    let mut values = BTreeMap::new();
    for header in headers {
        let Some(name) = header.key.strip_prefix(prefix) else {
            continue;
        };
        let value = header.value.as_ref().ok_or_else(|| {
            WalRecordDecodeError::MissingNativeHeaderValue {
                name: header.key.clone(),
            }
        })?;
        let value = String::from_utf8(value.clone()).map_err(|_| {
            WalRecordDecodeError::InvalidNativeHeaderUtf8 {
                name: header.key.clone(),
            }
        })?;
        let name = name.to_string();
        if values.insert(name.clone(), value).is_some() {
            return Err(duplicate_error(name));
        }
    }
    Ok(values)
}

#[derive(Debug, Error)]
pub enum WalSinkError {
    #[error("wal sink append failed")]
    Append,
    #[error("wal record serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("wal producer failed: {0}")]
    Producer(#[from] ProducerError),
    #[error("wal producer delivery channel closed")]
    DeliveryCanceled,
}

#[derive(Debug, Error)]
pub enum IngestLimitError {
    #[error("ingest unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("ingest quota exceeded for tenant `{tenant}`: {reason}")]
    RateLimited { tenant: String, reason: String },
    #[error("ingest quota check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}

#[derive(Debug, Error)]
pub enum QueryAuthorizationError {
    #[error("query unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("query authorization check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}

#[derive(Debug, Error)]
pub enum WalConsumerError {
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error("WAL consumer record {topic}-{partition}@{offset} did not include a value")]
    MissingValue {
        topic: String,
        partition: i32,
        offset: i64,
    },
}

#[derive(Debug, Error)]
pub enum HotTailPollError {
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
}

#[derive(Debug, Error)]
pub enum WalRecordDecodeError {
    #[error("wal record deserialization failed: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("native Kafka log record is missing header {name}")]
    MissingNativeHeader { name: String },
    #[error("native Kafka log record header {name} has no value")]
    MissingNativeHeaderValue { name: String },
    #[error("native Kafka log record header {name} is not UTF-8")]
    InvalidNativeHeaderUtf8 { name: String },
    #[error("native Kafka log record timestamp `{value}` is invalid: {source}")]
    InvalidNativeTimestamp {
        value: String,
        source: std::num::ParseIntError,
    },
    #[error("invalid native Kafka timestamp `{value}`")]
    InvalidNativeTimestampValue { value: String },
    #[error("native Kafka log record value is not UTF-8")]
    InvalidNativeLogLineUtf8,
    #[error("native Kafka log record did not include any crabka-log-label-* headers")]
    MissingNativeLabels,
    #[error("invalid native Kafka label name {name}")]
    InvalidNativeLabelName { name: String },
    #[error("invalid native Kafka metadata name {name}")]
    InvalidNativeMetadataName { name: String },
    #[error("duplicate native Kafka label name {name}")]
    DuplicateNativeLabelName { name: String },
    #[error("duplicate native Kafka metadata name {name}")]
    DuplicateNativeMetadataName { name: String },
}

#[derive(Clone)]
pub struct DistributorState {
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    prepare_shutdown: Arc<AtomicBool>,
    max_ingest_body_bytes: Option<usize>,
    wal_append_timeout: Option<Duration>,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
    metrics: ServiceMetrics,
}

pub fn distributor_router(sink: impl LogWalSink) -> Router {
    distributor_router_with_sink(
        Arc::new(sink),
        Arc::new(AllowAllIngestLimiter),
        None,
        None,
        None,
        None,
        ServiceMetrics::new(),
    )
}

#[derive(Clone, Copy)]
struct RoleOps {
    target: &'static str,
    ring_component: &'static str,
    role_ring_path: Option<&'static str>,
}

const DISTRIBUTOR_OPS: RoleOps = RoleOps {
    target: "distributor",
    ring_component: "crabka-distributor",
    role_ring_path: Some("/distributor/ring"),
};

const QUERIER_OPS: RoleOps = RoleOps {
    target: "querier",
    ring_component: "crabka-querier",
    role_ring_path: None,
};

const COMPACTOR_OPS: RoleOps = RoleOps {
    target: "compactor",
    ring_component: "crabka-compactor",
    role_ring_path: Some("/compactor/ring"),
};

fn with_role_ops_routes<S>(mut router: Router<S>, ops: RoleOps) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router = router
        .route("/ready", get(ready))
        .route("/log_level", get(log_level).post(log_level_post))
        .route("/metrics", get(role_metrics))
        .route("/config", get(role_config))
        .route("/services", get(role_services))
        .route("/memberlist", get(memberlist_status))
        .route("/ring", get(role_ring))
        .route("/loki/api/v1/status/buildinfo", get(build_info));
    if let Some(path) = ops.role_ring_path {
        router = router.route(path, get(role_ring));
    }
    router.layer(Extension(ops))
}

fn distributor_router_with_sink(
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    max_ingest_body_bytes: Option<usize>,
    wal_append_timeout: Option<Duration>,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
    metrics: ServiceMetrics,
) -> Router {
    let grpc_logs_service = OtlpGrpcLogsService {
        sink: Arc::clone(&sink),
        ingest_limiter: Arc::clone(&ingest_limiter),
        wal_append_timeout,
        metrics: metrics.clone(),
    };

    with_role_ops_routes(Router::new(), DISTRIBUTOR_OPS)
        .route("/flush", post(flush_ingester_chunks))
        .route(
            "/ingester/prepare_shutdown",
            get(get_prepare_shutdown)
                .post(set_prepare_shutdown)
                .delete(unset_prepare_shutdown),
        )
        .route(
            "/ingester/shutdown",
            get(shutdown_ingester).post(shutdown_ingester),
        )
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route("/loki/api/v1/push", post(push_logs))
        .route("/api/prom/push", post(push_logs))
        .route("/v1/logs", post(push_otlp_logs))
        .route("/otlp/v1/logs", post(push_otlp_logs))
        .route_service(
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            LogsServiceServer::new(grpc_logs_service),
        )
        .with_state(DistributorState {
            sink,
            ingest_limiter,
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            max_ingest_body_bytes,
            wal_append_timeout,
            reject_old_samples_max_age,
            creation_grace_period,
            metrics,
        })
}

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    wal_append_timeout: Option<Duration>,
    metrics: ServiceMetrics,
}

pub fn otlp_grpc_logs_service(sink: impl LogWalSink) -> OtlpGrpcLogsService {
    otlp_grpc_logs_service_with_limiter(sink, AllowAllIngestLimiter)
}

pub fn otlp_grpc_logs_service_with_limiter(
    sink: impl LogWalSink,
    ingest_limiter: impl LogIngestLimiter,
) -> OtlpGrpcLogsService {
    OtlpGrpcLogsService {
        sink: Arc::new(sink),
        ingest_limiter: Arc::new(ingest_limiter),
        wal_append_timeout: None,
        metrics: ServiceMetrics::new(),
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcLogsService {
    async fn export(
        &self,
        request: tonic::Request<ProtoExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ProtoExportLogsServiceResponse>, tonic::Status> {
        let (metadata, _, payload) = request.into_parts();
        let tenant = grpc_tenant(&metadata)?;
        let records = normalize_otlp_proto_logs_for_tenant(tenant, payload, None, None)
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        let state = DistributorState {
            sink: Arc::clone(&self.sink),
            ingest_limiter: Arc::clone(&self.ingest_limiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            max_ingest_body_bytes: None,
            wal_append_timeout: self.wal_append_timeout,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
            metrics: self.metrics.clone(),
        };
        append_distributor_wal_records(&state, records)
            .await
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        Ok(tonic::Response::new(ProtoExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[derive(Debug, Deserialize)]
struct LokiPushRequest {
    #[serde(default)]
    streams: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct LokiTypedPushRequest {
    streams: Vec<LokiPushStream>,
}

#[derive(Debug, Deserialize)]
struct LokiPushStream {
    #[serde(default)]
    stream: Option<Labels>,
    #[serde(default)]
    values: Option<Vec<Value>>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<LokiProtoStream>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoStream {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    parsed: Vec<LokiProtoLabelPair>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogsRequest {
    resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceLogs {
    resource: Option<OtlpResource>,
    scope_logs: Vec<OtlpScopeLogs>,
}

#[derive(Debug, Deserialize)]
struct OtlpResource {
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeLogs {
    scope: Option<OtlpScope>,
    log_records: Vec<OtlpLogRecord>,
}

#[derive(Debug, Deserialize)]
struct OtlpScope {
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogRecord {
    time_unix_nano: Value,
    #[serde(default)]
    severity_number: Option<Value>,
    #[serde(default)]
    severity_text: Option<String>,
    body: Option<OtlpAnyValue>,
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpKeyValue {
    key: String,
    value: OtlpAnyValue,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum OtlpAnyValue {
    #[serde(rename = "stringValue")]
    String(String),
    #[serde(rename = "boolValue")]
    Bool(bool),
    #[serde(rename = "intValue")]
    Int(Value),
    #[serde(rename = "doubleValue")]
    Double(Value),
    #[serde(rename = "bytesValue")]
    Bytes(String),
    #[serde(rename = "arrayValue")]
    Array(OtlpArrayValue),
    #[serde(rename = "kvlistValue")]
    Kvlist(OtlpKeyValueList),
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpArrayValue {
    values: Option<Vec<OtlpAnyValue>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpKeyValueList {
    values: Option<Vec<OtlpKeyValue>>,
}

/// Tenant for an ingest request from `X-Scope-OrgID`, falling back to
/// `"unknown"` when the header is missing, non-UTF-8, or empty. Used only to
/// label the ingest span / per-tenant metric — the WAL records carry their own
/// per-record tenant, so a permissive fallback here never affects storage.
fn ingest_tenant(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|t| !t.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

async fn push_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let bytes = body.len() as u64;
    let tenant = ingest_tenant(&headers);
    // ONE server span per push request (not per log line): wraps the whole
    // ingest body so the produce-side WAL append (which injects `traceparent`)
    // and downstream compaction stitch onto this trace. `crabka.ingest.lines`
    // is unknown until normalization, so it is recorded on the span below.
    let span = tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__crabka_observability_logs_wal",
        crabka.tenant = %tenant,
        crabka.ingest.lines = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    async move {
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body.len()) {
            return record_ingest_response(&state, error.into_response(), bytes, 0, start);
        }
        let resp = match normalize_loki_http_push(
            &headers,
            &body,
            state.reject_old_samples_max_age,
            state.creation_grace_period,
        ) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("crabka.ingest.lines", items);
                state.metrics.record_ingest_lines(&tenant, items);
                let resp = match append_distributor_wal_records(&state, records).await {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => error.into_response(),
                };
                return record_ingest_response(&state, resp, bytes, items, start);
            }
            Err(error) => error.into_response(),
        };
        record_ingest_response(&state, resp, bytes, 0, start)
    }
    .instrument(span)
    .await
}

async fn push_otlp_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let bytes = body.len() as u64;
    let tenant = ingest_tenant(&headers);
    // ONE server span per OTLP push request, mirroring the Loki push handler:
    // the OTLP emit path feeds the same log WAL, so instrumenting it keeps the
    // produce→compaction trace intact for OTLP-emitted logs too.
    let span = tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__crabka_observability_logs_wal",
        crabka.tenant = %tenant,
        crabka.ingest.lines = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    async move {
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body.len()) {
            return record_ingest_response(&state, error.into_response(), bytes, 0, start);
        }
        let resp = match normalize_otlp_http_logs(
            &headers,
            &body,
            state.reject_old_samples_max_age,
            state.creation_grace_period,
        ) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("crabka.ingest.lines", items);
                state.metrics.record_ingest_lines(&tenant, items);
                let resp = match append_distributor_wal_records(&state, records).await {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => {
                        // Surface why an accepted OTLP log batch failed to persist
                        // (WAL append errors are otherwise opaque to the client).
                        tracing::debug!(error = %error, "OTLP logs: WAL append failed");
                        error.into_response()
                    }
                };
                return record_ingest_response(&state, resp, bytes, items, start);
            }
            Err(error) => {
                // Surface why an OTLP log push was rejected at decode/normalize
                // (content-type, encoding, and size pinpoint client misconfig).
                tracing::debug!(
                    error = %error,
                    content_type = ?headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()),
                    content_encoding = ?headers.get(CONTENT_ENCODING).and_then(|v| v.to_str().ok()),
                    bytes,
                    "OTLP logs: decode/normalize rejected the request"
                );
                otlp_http_error_response(error)
            }
        };
        record_ingest_response(&state, resp, bytes, 0, start)
    }
    .instrument(span)
    .await
}

/// Record one push-handler ingest outcome from the response status and return
/// the response unchanged. `ok` is true for any 2xx; the WAL/produce failure
/// counter is bumped separately at the [`append_distributor_wal_records`] error
/// site, so a 4xx validation/quota reject here does not inflate it.
fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    bytes: u64,
    items: u64,
    start: Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, bytes, items, start.elapsed().as_secs_f64());
    resp
}

fn otlp_http_error_response(error: DistributorError) -> Response {
    if matches!(
        error,
        DistributorError::TimestampTooOld { .. } | DistributorError::TimestampTooNew { .. }
    ) {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/x-protobuf")],
            encode_otlp_status_message(&error.to_string()),
        )
            .into_response();
    }

    error.into_response()
}

fn encode_otlp_status_message(message: &str) -> Vec<u8> {
    let message = message.trim_end_matches('\n').as_bytes();
    let mut body = vec![0x12];
    encode_varint(message.len() as u64, &mut body);
    body.extend_from_slice(message);
    body
}

fn encode_varint(mut value: u64, body: &mut Vec<u8>) {
    while value >= 0x80 {
        body.push((value as u8) | 0x80);
        value >>= 7;
    }
    body.push(value as u8);
}

fn validate_ingest_body_limit(
    state: &DistributorState,
    body_bytes: usize,
) -> Result<(), DistributorError> {
    let Some(max_bytes) = state.max_ingest_body_bytes else {
        return Ok(());
    };
    if body_bytes > max_bytes {
        return Err(DistributorError::IngestBodyTooLarge {
            body_bytes,
            max_bytes,
        });
    }
    Ok(())
}

async fn append_wal_records(
    sink: &dyn LogWalSink,
    records: Vec<WalLogRecord>,
) -> Result<(), WalSinkError> {
    for record in records {
        sink.append(record).await?;
    }
    Ok(())
}

async fn append_distributor_wal_records(
    state: &DistributorState,
    records: Vec<WalLogRecord>,
) -> Result<(), DistributorError> {
    // A quota/rate-limit reject is a 4xx client error, NOT a WAL-append
    // failure, so it must not bump the WAL failure counter.
    check_ingest_quota(state.ingest_limiter.as_ref(), &records).await?;
    let result = if let Some(timeout) = state.wal_append_timeout {
        match tokio::time::timeout(timeout, append_wal_records(state.sink.as_ref(), records)).await
        {
            Ok(inner) => inner.map_err(DistributorError::from),
            Err(_) => Err(DistributorError::WalAppendTimeout),
        }
    } else {
        append_wal_records(state.sink.as_ref(), records)
            .await
            .map_err(DistributorError::from)
    };
    // Bump the WAL/produce append-failure counter only at the actual append
    // error site (timeout or sink error), never on a 4xx validation/quota
    // reject handled above or upstream.
    if result.is_err() {
        state.metrics.record_wal_append_failure();
    }
    result
}

async fn check_ingest_quota(
    limiter: &dyn LogIngestLimiter,
    records: &[WalLogRecord],
) -> Result<(), DistributorError> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    limiter
        .check(&first.tenant, records)
        .await
        .map_err(DistributorError::IngestQuota)
}

fn normalize_loki_http_push(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let body = decode_loki_http_body(headers, body)?;
    if is_loki_json_content_type(headers)? {
        let raw_payload: Value =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        if raw_payload.is_null() {
            return Err(DistributorError::NoValidStreams);
        }
        if !raw_payload.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_payload_parse_error(&body),
            ));
        }
        let payload =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        let payload = validate_loki_json_push_stream_objects(payload, &body)?;
        validate_loki_json_push_value_arrays(&payload, &body)?;
        validate_loki_json_push_timestamp_types(&payload, &body)?;
        validate_loki_json_structured_metadata_value_types(&payload, &body)?;
        normalize_loki_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    } else {
        let decompressed = SnappyDecoder::new()
            .decompress_vec(&body)
            .map_err(DistributorError::LokiSnappyDecode)?;
        let payload = LokiProtoPushRequest::decode(decompressed.as_slice())
            .map_err(DistributorError::LokiDecode)?;
        normalize_loki_proto_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    }
}

fn validate_loki_json_push_stream_objects(
    payload: LokiPushRequest,
    body: &[u8],
) -> Result<LokiTypedPushRequest, DistributorError> {
    let Some(streams) = payload.streams else {
        return Err(DistributorError::NoValidStreams);
    };
    let Some(raw_streams) = streams.as_array() else {
        return Err(DistributorError::InvalidJsonPushValueSyntax(
            loki_json_push_streams_parse_error(body, &streams),
        ));
    };
    if raw_streams.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }
    let mut streams = Vec::with_capacity(raw_streams.len());
    for stream in raw_streams {
        if !stream.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_stream_parse_error(body, stream),
            ));
        }
        if let Some(labels) = stream.get("stream")
            && !labels.is_object() {
                return Err(DistributorError::InvalidJsonPushValueSyntax(
                    loki_json_push_labels_field_parse_error(body),
                ));
            }
        if let Some(values) = stream.get("values")
            && !values.is_array() && !values.is_null() {
                return Err(DistributorError::InvalidJsonPushValueSyntax(
                    loki_json_push_values_field_parse_error(body, values),
                ));
            }
        let stream = serde_json::from_value(stream.clone())
            .map_err(|_| DistributorError::InvalidPushPayload)?;
        streams.push(stream);
    }

    Ok(LokiTypedPushRequest { streams })
}

fn validate_loki_json_push_value_arrays(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            if !value.is_array() {
                return Err(DistributorError::InvalidJsonPushValueSyntax(
                    loki_json_push_value_parse_error(body, value),
                ));
            }
        }
    }

    Ok(())
}

fn validate_loki_json_push_timestamp_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(timestamp) = value.get(0) else {
                continue;
            };
            if !timestamp.is_string() {
                return Err(DistributorError::InvalidJsonTimestampSyntax(
                    loki_json_timestamp_value_parse_error(body, timestamp, value.get(1)),
                ));
            }
        }
    }

    Ok(())
}

fn loki_json_push_value_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(10));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(30));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_payload_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let value_start = body
        .char_indices()
        .find_map(|(index, char)| (!char.is_whitespace()).then_some(index))
        .unwrap_or(body.len());
    let found = body[value_start..].chars().next().unwrap_or('\0');
    let context_start = previous_char_boundary(&body, value_start);
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 11));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start);

    format!(
        "readObjectStart: expect {{ or n, but found {found}, error found in #1 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_values_field_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(37));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_stream_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(4));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(12));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_labels_field_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let context = loki_decode_error_context(&body, body.len().saturating_sub(12));
    let bigger_context = loki_decode_error_context(&body, body.len().saturating_sub(52));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_streams_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context_start = previous_char_boundary(&body, value_start.saturating_sub(9));
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 20));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(11));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found \", error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn validate_loki_json_structured_metadata_value_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(metadata_value) = value.get(2) else {
                continue;
            };
            let Value::Object(metadata) = metadata_value else {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_object_parse_error(body, metadata_value),
                ));
            };
            if let Some((name, value)) = metadata.iter().find(|(_, value)| !value.is_string()) {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_value_parse_error(body, name, value),
                ));
            }
        }
    }

    Ok(())
}

fn loki_structured_metadata_object_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_structured_metadata_value_parse_error(body: &[u8], name: &str, value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let key = quote_logql_string(name);
    let needle = format!("{key}:{}", value);
    let value_start = body
        .find(&needle)
        .map(|offset| offset + key.len() + 1)
        .unwrap_or_else(|| body.find(&value.to_string()).unwrap_or(body.len()));
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_decode_error_context(body: &str, start: usize) -> &str {
    let start = previous_char_boundary(body, start.min(body.len()));
    let end = previous_char_boundary(body, body.len().min(start + 80));
    &body[start..end]
}

fn previous_char_boundary(value: &str, mut offset: usize) -> usize {
    while !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn decode_loki_http_body(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, DistributorError> {
    let Some(encoding) = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(body.to_vec());
    };
    let encoding = encoding.trim();

    if encoding.is_empty() || encoding.eq_ignore_ascii_case("snappy") {
        return Ok(body.to_vec());
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoder = GzDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiGzipDecode)?;
        return Ok(decompressed);
    } else if encoding.eq_ignore_ascii_case("deflate") {
        let mut decoder = DeflateDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiDeflateDecode)?;
        return Ok(decompressed);
    }

    Err(DistributorError::UnsupportedLokiContentEncoding(
        encoding.to_string(),
    ))
}

fn normalize_otlp_http_logs(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    // OTLP/HTTP clients (e.g. the OpenTelemetry SDK's otlphttp exporter, which
    // defaults to gzip) honour Content-Encoding just like the Loki push path, so
    // decompress before decode. Without this, a gzip body reaches the protobuf
    // decoder as raw deflate stream bytes and fails to parse.
    let body = decode_loki_http_body(headers, body)?;
    let body = body.as_slice();

    if is_protobuf_content_type(headers) {
        let payload =
            ProtoExportLogsServiceRequest::decode(body).map_err(DistributorError::OtlpDecode)?;
        return normalize_otlp_proto_logs(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        );
    }

    let payload = serde_json::from_slice(body).map_err(|_| DistributorError::InvalidOtlpPayload)?;
    normalize_otlp_logs(
        headers,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}

fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    content_type.split(';').next().is_some_and(|content_type| {
        matches!(
            content_type.trim(),
            "application/x-protobuf" | "application/protobuf"
        )
    })
}

fn is_loki_json_content_type(headers: &HeaderMap) -> Result<bool, DistributorError> {
    let Some(content_type) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(false);
    };
    let content_type = content_type.trim();
    if content_type.is_empty() {
        return Ok(false);
    }

    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(DistributorError::InvalidLokiContentType(
            content_type.to_string(),
        ));
    }

    let mut parameters = parts.peekable();
    while let Some(parameter) = parameters.next() {
        let parameter = parameter.trim();
        if parameter.is_empty() && parameters.peek().is_none() {
            continue;
        }
        let Some((name, value)) = parameter.split_once('=') else {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        };
        if name.trim().is_empty() || value.trim().is_empty() {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        }
    }

    Ok(media_type.eq_ignore_ascii_case("application/json"))
}

fn normalize_loki_push(
    headers: &HeaderMap,
    payload: LokiTypedPushRequest,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let Some(original_stream_labels) = stream.stream else {
            continue;
        };
        validate_loki_stream_labels(&original_stream_labels)?;
        let mut stream_labels = original_stream_labels.clone();
        discover_service_name_label(&mut stream_labels);

        let Some(values) = stream.values else {
            continue;
        };
        for value in values {
            let Some(value) = value.as_array() else {
                return Err(DistributorError::InvalidPushValue);
            };
            let zero_timestamp;
            let (timestamp, line, metadata, is_empty_value) = match value.as_slice() {
                [timestamp] => (timestamp, "", [].as_slice(), false),
                [timestamp, line, metadata @ ..] => (
                    timestamp,
                    line.as_str().ok_or_else(|| {
                        DistributorError::InvalidJsonLineSyntax(loki_json_line_parse_error(
                            &original_stream_labels,
                            timestamp.as_str().unwrap_or_default(),
                            line,
                        ))
                    })?,
                    metadata,
                    false,
                ),
                [] => {
                    zero_timestamp = Value::String("0".to_string());
                    (&zero_timestamp, "", [].as_slice(), true)
                }
            };
            let timestamp = timestamp
                .as_str()
                .ok_or(DistributorError::InvalidTimestamp)?;
            let timestamp_ns = timestamp.parse().map_err(|_| {
                DistributorError::InvalidJsonTimestampSyntax(loki_json_timestamp_parse_error(
                    timestamp, line,
                ))
            })?;
            let timestamp_ns = validate_ingest_timestamp_ns(timestamp_ns)?;
            if is_empty_value {
                validate_loki_empty_json_value_timestamp_window(
                    &stream_labels,
                    reject_old_samples_max_age,
                )?;
            }
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, line);

            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: parse_structured_metadata(metadata.first())?,
                position: None,
            });
        }
    }

    Ok(records)
}

fn validate_loki_empty_json_value_timestamp_window(
    stream_labels: &Labels,
    max_age: Option<Duration>,
) -> Result<(), DistributorError> {
    let Some(max_age) = max_age else {
        return Ok(());
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns()
        .saturating_sub(i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX));
    Err(DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    })
}

fn normalize_loki_proto_push(
    headers: &HeaderMap,
    payload: LokiProtoPushRequest,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let mut stream_labels = parse_loki_proto_labels(&stream.labels)?;
        validate_loki_stream_labels(&stream_labels)?;
        discover_service_name_label(&mut stream_labels);

        for entry in stream.entries {
            let timestamp_ns = if let Some(timestamp) = entry.timestamp.as_ref() {
                loki_proto_timestamp_ns(Some(timestamp))?
            } else {
                return Err(loki_missing_proto_timestamp_error(
                    &stream_labels,
                    reject_old_samples_max_age,
                ));
            };
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, &entry.line);
            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: entry.line,
                structured_metadata: loki_proto_label_pairs_to_labels(&entry.structured_metadata)?,
                position: None,
            });
        }
    }

    if records.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }

    Ok(records)
}

fn loki_push_entry_labels(stream_labels: &Labels, line: &str) -> Labels {
    let mut labels = stream_labels.clone();
    discover_detected_level_label(&mut labels, line);
    labels
}

fn normalize_otlp_logs(
    headers: &HeaderMap,
    payload: OtlpLogsRequest,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = otlp_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.as_deref()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(otlp_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.attributes.as_deref()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = otlp_timestamp_ns(&log_record.time_unix_nano)?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(otlp_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: otlp_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

fn loki_json_timestamp_parse_error(timestamp: &str, line: &str) -> String {
    let found_context = timestamp
        .char_indices()
        .nth(9)
        .map(|(offset, _)| &timestamp[offset..])
        .unwrap_or(timestamp);
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}\"]]}}]}}|..., bigger context ...|s\":[[\"{timestamp}\",\"{line}\"]]}}]}}|...\n"
    )
}

fn loki_json_timestamp_value_parse_error(
    body: &[u8],
    timestamp: &Value,
    line: Option<&Value>,
) -> String {
    let body = String::from_utf8_lossy(body);
    let timestamp_text = timestamp.to_string();
    let value_start = body.find(&timestamp_text).unwrap_or(body.len());
    let found_context = line
        .and_then(Value::as_str)
        .map(|line| {
            let start = line
                .char_indices()
                .nth(line.chars().count().saturating_sub(6))
                .map(|(offset, _)| offset)
                .unwrap_or(0);
            format!("{}\"]]}}]}}", &line[start..])
        })
        .unwrap_or_else(|| {
            loki_decode_error_context(&body, value_start.saturating_add(10)).to_string()
        });
    let context_prefix_len = if timestamp.is_array() {
        10
    } else if timestamp.is_object() {
        4
    } else {
        9
    };
    let bigger_context =
        loki_decode_error_context(&body, value_start.saturating_sub(context_prefix_len));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_line_parse_error(stream_labels: &Labels, timestamp: &str, line: &Value) -> String {
    let line = line.to_string();
    let found_context = format!(
        "{}\",{}]]}}]}}",
        timestamp
            .char_indices()
            .nth(timestamp.chars().count().saturating_sub(2))
            .map(|(offset, _)| &timestamp[offset..])
            .unwrap_or(timestamp),
        line
    );
    let labels = serde_json::to_string(stream_labels).unwrap_or_else(|_| "{}".to_string());
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|ream\":{labels},\"values\":[[\"{timestamp}\",{line}]]}}]}}|...\n"
    )
}

fn validate_loki_stream_labels(labels: &Labels) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    Ok(())
}

fn loki_push_label_parse_error(labels: &Labels, invalid_name: &str) -> String {
    let rendered = loki_label_set(labels);
    let name_start = rendered.find(invalid_name).unwrap_or(1);
    let invalid_offset = invalid_name
        .char_indices()
        .find_map(|(offset, value)| {
            (!is_loki_label_name_char(value, offset == 0)).then_some(offset)
        })
        .unwrap_or(0);
    let column = name_start + invalid_offset + 1;
    let unexpected = invalid_name[invalid_offset..].chars().next().unwrap_or('}');
    format!(
        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{unexpected}'\n"
    )
}

fn loki_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}

fn is_loki_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    is_loki_label_name_char(first, true) && chars.all(|value| is_loki_label_name_char(value, false))
}

fn is_loki_label_name_char(value: char, first: bool) -> bool {
    value == '_' || value.is_ascii_alphabetic() || (!first && value.is_ascii_digit())
}

fn parse_loki_proto_labels(labels: &str) -> Result<Labels, DistributorError> {
    let labels = labels.trim();
    if labels.is_empty() || labels == "{}" {
        return Ok(Labels::new());
    }

    let query = parse_query(labels).map_err(|_| {
        loki_proto_label_parse_error(labels)
            .map(DistributorError::InvalidPushLabelSyntax)
            .unwrap_or(DistributorError::InvalidPushLabels)
    })?;
    if !query.pipeline.is_empty() {
        return Err(DistributorError::InvalidPushLabels);
    }

    let mut labels = Labels::new();
    let mut rendered_labels = Vec::new();
    for matcher in query.matchers {
        if matcher.op != MatchOp::Equal {
            return Err(DistributorError::InvalidPushLabels);
        }
        rendered_labels.push(format!(
            "{}={}",
            matcher.name,
            quote_logql_string(&matcher.value)
        ));
        if labels.contains_key(&matcher.name) {
            let mut discovered_labels = labels.clone();
            discover_service_name_label(&mut discovered_labels);
            if !rendered_labels
                .iter()
                .any(|label| label.starts_with("service_name="))
                && let Some(service_name) = discovered_labels.get("service_name")
            {
                rendered_labels.push(format!("service_name={}", quote_logql_string(service_name)));
            }
            return Err(DistributorError::InvalidPushLabelSyntax(format!(
                "stream '{{{}}}' has duplicate label name: '{}'\n",
                rendered_labels.join(", "),
                matcher.name
            )));
        }
        labels.insert(matcher.name, matcher.value);
    }

    Ok(labels)
}

fn loki_proto_label_parse_error(labels: &str) -> Option<String> {
    let labels = labels.trim();
    let mut chars = labels.char_indices();
    if chars.next()? != (0, '{') {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut expecting_name = true;
    let mut first_name_char = true;

    for (offset, value) in chars {
        if in_string {
            if escaped {
                escaped = false;
            } else if value == '\\' {
                escaped = true;
            } else if value == '"' {
                in_string = false;
            }
            continue;
        }

        match value {
            '"' => in_string = true,
            ',' => {
                expecting_name = true;
                first_name_char = true;
            }
            '=' => {
                expecting_name = false;
                first_name_char = true;
            }
            '}' => break,
            value if expecting_name && value.is_whitespace() => {}
            value if expecting_name => {
                if !is_loki_label_name_char(value, first_name_char) {
                    let column = labels[..offset].chars().count() + 1;
                    return Some(format!(
                        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{value}'\n"
                    ));
                }
                first_name_char = false;
            }
            _ => {}
        }
    }

    None
}

fn loki_proto_timestamp_ns(
    timestamp: Option<&LokiProtoTimestamp>,
) -> Result<i64, DistributorError> {
    let timestamp = timestamp.ok_or(DistributorError::InvalidTimestamp)?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(DistributorError::InvalidTimestamp);
    }

    timestamp
        .seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds_ns| seconds_ns.checked_add(i64::from(timestamp.nanos)))
        .ok_or(DistributorError::InvalidTimestamp)
}

fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Option<Duration>,
) -> DistributorError {
    let max_age = max_age.unwrap_or(LOKI_REJECT_OLD_SAMPLES_MAX_AGE);
    let oldest_acceptable_timestamp_ns = current_unix_time_ns()
        .saturating_sub(i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX));
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}

fn loki_proto_label_pairs_to_labels(
    labels: &[LokiProtoLabelPair],
) -> Result<Labels, DistributorError> {
    let mut labels_by_name = Labels::new();
    for label in labels {
        labels_by_name.insert(label.name.clone(), label.value.clone());
    }
    Ok(labels_by_name)
}

fn normalize_otlp_proto_logs(
    headers: &HeaderMap,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?;
    normalize_otlp_proto_logs_for_tenant(
        tenant,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}

fn normalize_otlp_proto_logs_for_tenant(
    tenant: &str,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = proto_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .map(|resource| resource.attributes.as_slice()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(proto_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .map(|scope| scope.attributes.as_slice()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(proto_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: proto_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        if labels
            .insert(name, otlp_value_to_string(&attribute.value))
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        let value = attribute
            .value
            .as_ref()
            .map(proto_value_to_string)
            .unwrap_or_default();
        if labels.insert(name, value).is_some() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

fn proto_log_record_structured_metadata(
    log_record: &ProtoLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = proto_attributes_to_labels(Some(log_record.attributes.as_slice()))?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        (log_record.severity_number != 0).then(|| log_record.severity_number.to_string()),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        (!log_record.severity_text.is_empty()).then(|| log_record.severity_text.clone()),
    )?;
    insert_proto_trace_context_metadata(&mut metadata, "trace_id", &log_record.trace_id);
    insert_proto_trace_context_metadata(&mut metadata, "span_id", &log_record.span_id);
    Ok(metadata)
}

fn otlp_log_record_structured_metadata(
    log_record: &OtlpLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = otlp_attributes_to_labels(log_record.attributes.as_deref())?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        log_record
            .severity_number
            .as_ref()
            .map(otlp_severity_number_to_string)
            .transpose()?,
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        log_record
            .severity_text
            .as_ref()
            .filter(|severity_text| !severity_text.is_empty())
            .cloned(),
    )?;
    Ok(metadata)
}

fn insert_metadata_if_absent(
    metadata: &mut Labels,
    name: &str,
    value: Option<String>,
) -> Result<(), DistributorError> {
    let Some(value) = value else {
        return Ok(());
    };
    if metadata.insert(name.to_string(), value).is_some() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}

fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), hex_string(value));
    }
}

fn normalize_otlp_attribute_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        normalized.insert(0, '_');
    }
    normalized
}

fn discover_service_name_label(labels: &mut Labels) {
    if labels.contains_key("service_name") {
        return;
    }

    let service_name = SERVICE_NAME_DISCOVERY_LABELS
        .iter()
        .filter_map(|name| labels.get(*name))
        .find(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "unknown_service".to_string());
    labels.insert("service_name".to_string(), service_name);
}

fn discover_detected_level_label(labels: &mut Labels, line: &str) {
    if labels.contains_key("detected_level")
        || labels.contains_key("level")
        || labels.contains_key("severity")
        || labels.contains_key("severity_text")
    {
        return;
    }

    let level = detect_log_level(line);
    if let Some(level) = level {
        labels.insert("detected_level".to_string(), level.to_string());
    }
}

fn detect_log_level(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    for level in [
        "critical", "crit", "fatal", "error", "warn", "warning", "info", "debug", "trace",
    ] {
        if contains_log_level_token(&line, level) {
            return Some(match level {
                "crit" => "critical",
                "warning" => "warn",
                level => level,
            });
        }
    }
    None
}

fn contains_log_level_token(line: &str, level: &str) -> bool {
    line.match_indices(level).any(|(start, _)| {
        let end = start + level.len();
        let before = start
            .checked_sub(1)
            .and_then(|index| line.as_bytes().get(index))
            .copied();
        let after = line.as_bytes().get(end).copied();
        !before.is_some_and(is_log_level_word_byte) && !after.is_some_and(is_log_level_word_byte)
    })
}

fn is_log_level_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

const SERVICE_NAME_DISCOVERY_LABELS: &[&str] = &[
    "service",
    "app",
    "application",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "component",
    "workload",
    "job",
];

fn proto_timestamp_ns(
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
) -> Result<i64, DistributorError> {
    let timestamp = if time_unix_nano == 0 {
        observed_time_unix_nano
    } else {
        time_unix_nano
    };
    i64::try_from(timestamp).map_err(|_| DistributorError::InvalidTimestamp)
}

fn otlp_timestamp_ns(timestamp: &Value) -> Result<i64, DistributorError> {
    let timestamp_ns = match timestamp {
        Value::String(timestamp) => timestamp
            .parse()
            .map_err(|_| DistributorError::InvalidTimestamp),
        Value::Number(timestamp) => timestamp.as_i64().ok_or(DistributorError::InvalidTimestamp),
        _ => Err(DistributorError::InvalidTimestamp),
    }?;
    validate_ingest_timestamp_ns(timestamp_ns)
}

fn validate_ingest_timestamp_ns(timestamp_ns: i64) -> Result<i64, DistributorError> {
    if timestamp_ns < 0 {
        Err(DistributorError::InvalidTimestamp)
    } else {
        Ok(timestamp_ns)
    }
}

fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Duration>,
    creation_grace_period: Option<Duration>,
) -> Result<(), DistributorError> {
    let now_ns = current_unix_time_ns();
    if let Some(max_age) = max_age {
        let oldest_acceptable_timestamp_ns =
            now_ns.saturating_sub(i64::try_from(max_age.as_nanos()).unwrap_or(i64::MAX));
        if timestamp_ns < oldest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooOld {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
                oldest_acceptable_timestamp_ns,
            });
        }
    }
    if let Some(creation_grace_period) = creation_grace_period {
        let newest_acceptable_timestamp_ns = now_ns
            .saturating_add(i64::try_from(creation_grace_period.as_nanos()).unwrap_or(i64::MAX));
        if timestamp_ns > newest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooNew {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
            });
        }
    }
    Ok(())
}

fn loki_stale_sample_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{values}}}")
}

fn rfc3339_seconds(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return seconds.to_string();
    };
    let date = timestamp.date();
    let time = timestamp.time();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

fn otlp_severity_number_to_string(value: &Value) -> Result<String, DistributorError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => Err(DistributorError::InvalidOtlpPayload),
    }
}

fn otlp_value_to_string(value: &OtlpAnyValue) -> String {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => value.clone(),
        OtlpAnyValue::Bool(value) => value.to_string(),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => metadata_value_to_string(value),
        OtlpAnyValue::Array(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP array values serialize to JSON"),
        OtlpAnyValue::Kvlist(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP key-value lists serialize to JSON"),
    }
}

fn otlp_value_to_json(value: &OtlpAnyValue) -> Value {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => Value::String(value.clone()),
        OtlpAnyValue::Bool(value) => Value::Bool(*value),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => value.clone(),
        OtlpAnyValue::Array(value) => Value::Array(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect(),
        ),
        OtlpAnyValue::Kvlist(value) => Value::Object(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect(),
        ),
    }
}

fn proto_value_to_string(value: &ProtoAnyValue) -> String {
    value
        .value
        .as_ref()
        .map(proto_any_value_to_string)
        .unwrap_or_default()
}

fn proto_any_value_to_string(value: &proto_any_value::Value) -> String {
    match value {
        proto_any_value::Value::StringValue(value) => value.clone(),
        proto_any_value::Value::BoolValue(value) => value.to_string(),
        proto_any_value::Value::IntValue(value) => value.to_string(),
        proto_any_value::Value::DoubleValue(value) => value.to_string(),
        proto_any_value::Value::BytesValue(value) => hex_string(value),
        proto_any_value::Value::ArrayValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(proto_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP protobuf array values serialize to JSON"),
        proto_any_value::Value::KvlistValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.clone(),
                        attribute
                            .value
                            .as_ref()
                            .map_or(Value::Null, proto_value_to_json),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP protobuf key-value lists serialize to JSON"),
        proto_any_value::Value::StringValueStrindex(value) => value.to_string(),
    }
}

fn proto_value_to_json(value: &ProtoAnyValue) -> Value {
    match value.value.as_ref() {
        Some(proto_any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(proto_any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(proto_any_value::Value::IntValue(value)) => Value::Number((*value).into()),
        Some(proto_any_value::Value::DoubleValue(value)) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        Some(proto_any_value::Value::BytesValue(value)) => Value::String(hex_string(value)),
        Some(proto_any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(proto_value_to_json).collect())
        }
        Some(proto_any_value::Value::KvlistValue(value)) => Value::Object(
            value
                .values
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.clone(),
                        attribute
                            .value
                            .as_ref()
                            .map_or(Value::Null, proto_value_to_json),
                    )
                })
                .collect(),
        ),
        Some(proto_any_value::Value::StringValueStrindex(value)) => Value::Number((*value).into()),
        None => Value::Null,
    }
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn parse_structured_metadata(
    metadata: Option<&Value>,
) -> Result<BTreeMap<String, String>, DistributorError> {
    let Some(metadata) = metadata else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(metadata) = metadata else {
        return Err(DistributorError::InvalidStructuredMetadata);
    };

    metadata
        .iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .ok_or(DistributorError::InvalidStructuredMetadata)?;
            Ok((name.clone(), value.to_string()))
        })
        .collect::<Result<BTreeMap<_, _>, DistributorError>>()
}

fn metadata_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

#[derive(Clone)]
pub struct QuerierState {
    root: PathBuf,
    label_index: LabelIndex,
    block_index: BlockIndex,
    cold_store: Option<ColdObjectStoreState>,
    dynamic_index: Option<DynamicIndexSource>,
    dynamic_index_cache: DynamicIndexCache,
    hot_tail: Option<HotTailState>,
    delete_requests: Option<SharedLogDeleteRequests>,
    rules: SharedLokiRules,
    alert_states: SharedPrometheusAlertStates,
    query_authorizer: Arc<dyn LogQueryAuthorizer>,
    max_query_range_ns: Option<i64>,
    max_query_series: Option<usize>,
    max_query_bytes: Option<u64>,
    max_query_length: Option<usize>,
    /// Shared RED-metrics bundle. `None` for test routers that don't wire
    /// metrics; the binary threads a shared bundle in via [`QuerierState::with_metrics`].
    metrics: Option<ServiceMetrics>,
}

type LokiRuleGroupsByName = BTreeMap<String, serde_yaml::Value>;
type LokiRuleNamespaces = BTreeMap<String, LokiRuleGroupsByName>;
type LokiRuleTenants = BTreeMap<String, LokiRuleNamespaces>;

#[derive(Clone, Default)]
struct SharedLokiRules {
    tenants: Arc<Mutex<LokiRuleTenants>>,
    storage_path: Option<Arc<PathBuf>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PrometheusAlertKey {
    tenant: String,
    alert_name: String,
    query: String,
    labels: Labels,
}

#[derive(Clone, Debug)]
struct PrometheusAlertRuntimeState {
    active_at: i64,
    last_active_at: i64,
    value: String,
}

#[derive(Clone, Default)]
struct SharedPrometheusAlertStates {
    alerts: Arc<Mutex<BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>>>,
}

impl SharedPrometheusAlertStates {
    fn clear_tenant(&self, tenant: &str) {
        self.alerts
            .lock()
            .expect("Prometheus alert state lock poisoned")
            .retain(|key, _| key.tenant != tenant);
    }
}

#[derive(Clone)]
struct ColdObjectStoreState {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

#[derive(Clone)]
enum DynamicIndexSource {
    TenantObjectStoreManifest {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
    TenantObjectStoreShards {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}

#[derive(Clone, Default)]
struct DynamicIndexCache {
    entries: Arc<Mutex<BTreeMap<DynamicIndexCacheKey, CachedDynamicIndex>>>,
    shard_ranges: Arc<Mutex<BTreeMap<DynamicShardRangesCacheKey, CachedShardRanges>>>,
    shard_indexes: Arc<Mutex<BTreeMap<DynamicShardIndexCacheKey, CachedDynamicIndex>>>,
}

impl DynamicIndexCache {
    fn clear(&self) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .clear();
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .clear();
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .clear();
    }

    fn get(&self, key: &DynamicIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .entries
            .lock()
            .expect("dynamic index cache lock poisoned");
        let entry = entries.get(key)?;
        if entry.loaded_at.elapsed() > DYNAMIC_INDEX_CACHE_TTL {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    fn insert(&self, key: DynamicIndexCacheKey, label_index: LabelIndex, block_index: BlockIndex) {
        self.entries
            .lock()
            .expect("dynamic index cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }

    fn get_shard_ranges(
        &self,
        key: &DynamicShardRangesCacheKey,
        required_from_ns: i64,
    ) -> Option<Vec<TimeRange>> {
        let mut ranges = self
            .shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned");
        let entry = ranges.get(key)?;
        if entry.loaded_at.elapsed() > DYNAMIC_INDEX_CACHE_TTL
            || entry.listed_from_ns > required_from_ns
        {
            ranges.remove(key);
            return None;
        }
        Some(entry.ranges.clone())
    }

    fn insert_shard_ranges(
        &self,
        key: DynamicShardRangesCacheKey,
        listed_from_ns: i64,
        ranges: Vec<TimeRange>,
    ) {
        self.shard_ranges
            .lock()
            .expect("dynamic index shard range cache lock poisoned")
            .insert(
                key,
                CachedShardRanges {
                    loaded_at: Instant::now(),
                    listed_from_ns,
                    ranges,
                },
            );
    }

    fn get_shard_index(&self, key: &DynamicShardIndexCacheKey) -> Option<(LabelIndex, BlockIndex)> {
        let mut entries = self
            .shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned");
        let entry = entries.get(key)?;
        if entry.loaded_at.elapsed() > DYNAMIC_INDEX_SHARD_CACHE_TTL {
            entries.remove(key);
            return None;
        }
        Some((entry.label_index.clone(), entry.block_index.clone()))
    }

    fn insert_shard_index(
        &self,
        key: DynamicShardIndexCacheKey,
        label_index: LabelIndex,
        block_index: BlockIndex,
    ) {
        self.shard_indexes
            .lock()
            .expect("dynamic index shard cache lock poisoned")
            .insert(
                key,
                CachedDynamicIndex {
                    loaded_at: Instant::now(),
                    label_index,
                    block_index,
                },
            );
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DynamicIndexCacheKey {
    TenantManifest {
        tenant: String,
    },
    TenantShards {
        tenant: String,
        start_ns: i64,
        end_ns: i64,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DynamicShardRangesCacheKey {
    tenant: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DynamicShardIndexCacheKey {
    tenant: String,
    start_ns: i64,
    end_ns: i64,
}

#[derive(Clone)]
struct CachedDynamicIndex {
    loaded_at: Instant,
    label_index: LabelIndex,
    block_index: BlockIndex,
}

#[derive(Clone)]
struct CachedShardRanges {
    loaded_at: Instant,
    listed_from_ns: i64,
    ranges: Vec<TimeRange>,
}

fn merge_tenant_shard_indexes(
    tenant: &str,
    indexes: impl IntoIterator<Item = (LabelIndex, BlockIndex)>,
) -> (LabelIndex, BlockIndex) {
    let mut merged_labels = LabelIndex::default();
    let mut merged_blocks = BTreeMap::new();

    for (label_index, block_index) in indexes {
        for (_, labels) in label_index.tenant_series(tenant) {
            merged_labels.insert_series(tenant.to_string(), labels);
        }
        for block in block_index.blocks() {
            merged_blocks
                .entry(block.key.object_key())
                .or_insert_with(|| block.clone());
        }
    }

    let mut block_index = BlockIndex::default();
    for block in merged_blocks.into_values() {
        block_index.insert(block);
    }

    (merged_labels, block_index)
}

#[derive(Clone)]
struct HotTailState {
    source: Arc<dyn LogHotTail>,
    frontier: CompactionFrontierSource,
}

impl QuerierState {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, label_index: LabelIndex, block_index: BlockIndex) -> Self {
        Self {
            root: root.into(),
            label_index,
            block_index,
            cold_store: None,
            dynamic_index: None,
            dynamic_index_cache: DynamicIndexCache::default(),
            hot_tail: None,
            delete_requests: None,
            rules: SharedLokiRules::default(),
            alert_states: SharedPrometheusAlertStates::default(),
            query_authorizer: Arc::new(AllowAllQueryAuthorizer),
            max_query_range_ns: None,
            max_query_series: None,
            max_query_bytes: None,
            max_query_length: None,
            metrics: None,
        }
    }

    /// Thread a shared RED-metrics bundle so each querier handler records its
    /// per-route request count and latency on the same registry the `:9404`
    /// exporter serves. A no-op when left unset (test routers).
    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Record one querier request outcome (per-route count + latency) on the
    /// shared bundle. No-op when metrics are not wired (test routers).
    fn record_query(&self, route: &str, ok: bool, start: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(route, ok, start.elapsed().as_secs_f64());
        }
    }

    #[must_use]
    pub fn with_max_query_range_ns(mut self, max_query_range_ns: i64) -> Self {
        self.max_query_range_ns = Some(max_query_range_ns);
        self
    }

    #[must_use]
    pub fn with_max_query_series(mut self, max_query_series: usize) -> Self {
        self.max_query_series = Some(max_query_series);
        self
    }

    #[must_use]
    pub fn with_max_query_bytes(mut self, max_query_bytes: u64) -> Self {
        self.max_query_bytes = Some(max_query_bytes);
        self
    }

    #[must_use]
    pub fn with_max_query_length(mut self, max_query_length: usize) -> Self {
        self.max_query_length = Some(max_query_length);
        self
    }

    #[must_use]
    pub fn with_query_authorizer(mut self, authorizer: impl LogQueryAuthorizer) -> Self {
        self.query_authorizer = Arc::new(authorizer);
        self
    }

    fn with_query_authorizer_source(mut self, authorizer: Arc<dyn LogQueryAuthorizer>) -> Self {
        self.query_authorizer = authorizer;
        self
    }

    #[must_use]
    pub fn with_hot_tail(self, source: impl LogHotTail, compacted_through_ns: i64) -> Self {
        self.with_hot_tail_frontier(source, CompactionFrontier::new(compacted_through_ns))
    }

    #[must_use]
    pub fn with_hot_tail_frontier(
        self,
        source: impl LogHotTail,
        frontier: CompactionFrontier,
    ) -> Self {
        self.with_hot_tail_source(
            Arc::new(source),
            CompactionFrontierSource::Snapshot(frontier),
        )
    }

    #[must_use]
    pub fn with_hot_tail_shared_frontier(
        self,
        source: impl LogHotTail,
        frontier: SharedCompactionFrontier,
    ) -> Self {
        self.with_hot_tail_source(Arc::new(source), CompactionFrontierSource::Shared(frontier))
    }

    fn with_hot_tail_source(
        mut self,
        source: Arc<dyn LogHotTail>,
        frontier: CompactionFrontierSource,
    ) -> Self {
        self.hot_tail = Some(HotTailState { source, frontier });
        self
    }

    fn with_delete_requests(mut self, requests: SharedLogDeleteRequests) -> Self {
        self.delete_requests = Some(requests);
        self
    }

    fn with_rules(mut self, rules: SharedLokiRules) -> Self {
        self.rules = rules;
        self
    }

    fn with_cold_object_store_source(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.cold_store = Some(ColdObjectStoreState { store, prefix });
        self
    }

    fn with_dynamic_tenant_object_store_manifest(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.dynamic_index_cache.clear();
        self.dynamic_index = Some(DynamicIndexSource::TenantObjectStoreManifest { store, prefix });
        self
    }

    fn with_dynamic_tenant_object_store_shards(
        mut self,
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    ) -> Self {
        self.dynamic_index_cache.clear();
        self.dynamic_index = Some(DynamicIndexSource::TenantObjectStoreShards { store, prefix });
        self
    }

    async fn with_request_tenant_index(
        &self,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let Some(dynamic_index) = &self.dynamic_index else {
            return Ok(self.clone());
        };

        match dynamic_index {
            DynamicIndexSource::TenantObjectStoreManifest { store, prefix } => {
                let cache_key = DynamicIndexCacheKey::TenantManifest {
                    tenant: tenant.to_string(),
                };
                if let Some((label_index, block_index)) = self.dynamic_index_cache.get(&cache_key) {
                    let mut state = self.clone();
                    state.label_index = label_index;
                    state.block_index = block_index;
                    return Ok(state);
                }
                let (label_index, block_index) =
                    match read_tenant_log_index_manifest_from_object_store(
                        store.as_ref(),
                        prefix,
                        tenant,
                    )
                    .await
                    {
                        Ok(indexes) => indexes,
                        Err(BlockStoreError::ObjectStore(object_store::Error::NotFound {
                            ..
                        })) => {
                            return Ok(self.clone());
                        }
                        Err(error) => return Err(error),
                    };
                self.dynamic_index_cache.insert(
                    cache_key,
                    label_index.clone(),
                    block_index.clone(),
                );
                let mut state = self.clone();
                state.label_index = label_index;
                state.block_index = block_index;
                Ok(state)
            }
            DynamicIndexSource::TenantObjectStoreShards { store, prefix } => {
                let cache_key = DynamicIndexCacheKey::TenantShards {
                    tenant: tenant.to_string(),
                    start_ns: query_range.start_ns,
                    end_ns: query_range.end_ns,
                };
                if let Some((label_index, block_index)) = self.dynamic_index_cache.get(&cache_key) {
                    let mut state = self.clone();
                    state.label_index = label_index;
                    state.block_index = block_index;
                    return Ok(state);
                }
                let (label_index, block_index) = self
                    .cached_tenant_shard_indexes(store.as_ref(), prefix, tenant, query_range)
                    .await?;
                self.dynamic_index_cache.insert(
                    cache_key,
                    label_index.clone(),
                    block_index.clone(),
                );
                let mut state = self.clone();
                state.label_index = label_index;
                state.block_index = block_index;
                Ok(state)
            }
        }
    }

    async fn cached_tenant_shard_ranges(
        &self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Vec<TimeRange>, BlockStoreError> {
        let required_from_ns =
            crabka_blockstore::log_tenant_index_shard_list_offset_start_ns(query_range);
        let cache_key = DynamicShardRangesCacheKey {
            tenant: tenant.to_string(),
        };
        if let Some(ranges) = self
            .dynamic_index_cache
            .get_shard_ranges(&cache_key, required_from_ns)
        {
            return Ok(ranges);
        }

        let mut shard_ranges =
            crabka_blockstore::list_tenant_log_index_shard_ranges_overlapping_query_from_object_store(
                store,
                prefix,
                tenant,
                query_range,
            )
            .await?;
        if shard_ranges.is_empty() {
            shard_ranges =
                match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant)
                    .await
                {
                    Ok(shard_ranges) => shard_ranges,
                    Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                        Vec::new()
                    }
                    Err(error) => return Err(error),
                };
        }

        self.dynamic_index_cache.insert_shard_ranges(
            cache_key,
            required_from_ns,
            shard_ranges.clone(),
        );
        Ok(shard_ranges)
    }

    async fn cached_tenant_shard_indexes(
        &self,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
        let shard_ranges = self
            .cached_tenant_shard_ranges(store, prefix, tenant, query_range)
            .await?;
        let mut indexes = Vec::new();
        let mut misses = Vec::new();

        for shard_range in shard_ranges
            .into_iter()
            .filter(|shard_range| shard_range.overlaps(query_range))
        {
            let cache_key = DynamicShardIndexCacheKey {
                tenant: tenant.to_string(),
                start_ns: shard_range.start_ns,
                end_ns: shard_range.end_ns,
            };
            if let Some(index) = self.dynamic_index_cache.get_shard_index(&cache_key) {
                indexes.push(index);
            } else {
                misses.push(shard_range);
            }
        }

        let fetched = futures_util::stream::iter(misses)
            .map(|shard_range| async move {
                let (label_index, block_index) = read_tenant_log_index_shard_from_object_store(
                    store,
                    prefix,
                    tenant,
                    shard_range,
                )
                .await?;
                Ok::<_, BlockStoreError>((shard_range, label_index, block_index))
            })
            .buffer_unordered(DYNAMIC_INDEX_SHARD_FETCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        for (shard_range, label_index, block_index) in fetched {
            let cache_key = DynamicShardIndexCacheKey {
                tenant: tenant.to_string(),
                start_ns: shard_range.start_ns,
                end_ns: shard_range.end_ns,
            };
            self.dynamic_index_cache.insert_shard_index(
                cache_key,
                label_index.clone(),
                block_index.clone(),
            );
            indexes.push((label_index, block_index));
        }

        Ok(merge_tenant_shard_indexes(tenant, indexes))
    }

    pub fn from_manifest(root: impl Into<PathBuf>) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) = read_log_index_manifest(&root)?;
        Ok(Self::new(root, label_index, block_index))
    }

    pub async fn from_tenant_object_store(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_manifest_from_object_store(store, prefix, tenant).await?;
        Ok(Self::new(root, label_index, block_index))
    }

    pub async fn from_tenant_object_store_shard(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        shard_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_shard_from_object_store(store, prefix, tenant, shard_range)
                .await?;
        Ok(Self::new(root, label_index, block_index))
    }

    pub async fn from_tenant_object_store_shards(
        root: impl Into<PathBuf>,
        store: &dyn ObjectStore,
        prefix: &ObjectPath,
        tenant: &str,
        query_range: TimeRange,
    ) -> Result<Self, BlockStoreError> {
        let root = root.into();
        let (label_index, block_index) =
            read_tenant_log_index_shards_from_object_store(store, prefix, tenant, query_range)
                .await?;
        Ok(Self::new(root, label_index, block_index))
    }
}

pub async fn build_querier_state(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<QuerierState, ServiceConfigError> {
    build_querier_state_with_object_store_prefix(config, object_store, None).await
}

async fn build_querier_state_with_object_store_prefix(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<QuerierState, ServiceConfigError> {
    let state = match config.querier_index_source {
        QuerierIndexSource::LocalManifest => QuerierState::from_manifest(config.data_root.clone())?,
        QuerierIndexSource::TenantObjectStoreManifest => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            QuerierState::from_tenant_object_store(config.data_root.clone(), store, &prefix, tenant)
                .await?
        }
        QuerierIndexSource::TenantObjectStoreShards => {
            let (store, tenant, prefix) =
                querier_object_store_inputs(config, object_store, object_store_prefix)?;
            let start_ns = config
                .query_start_ns
                .ok_or(ServiceConfigError::MissingQueryStartNs)?;
            let end_ns = config
                .query_end_ns
                .ok_or(ServiceConfigError::MissingQueryEndNs)?;

            QuerierState::from_tenant_object_store_shards(
                config.data_root.clone(),
                store,
                &prefix,
                tenant,
                TimeRange::new(start_ns, end_ns)?,
            )
            .await?
        }
    };

    let state = if let Some(max_query_range_ns) = config.max_query_range_ns {
        state.with_max_query_range_ns(max_query_range_ns)
    } else {
        state
    };

    let state = if let Some(max_query_series) = config.max_query_series {
        state.with_max_query_series(max_query_series)
    } else {
        state
    };

    let state = if let Some(max_query_bytes) = config.max_query_bytes {
        state.with_max_query_bytes(max_query_bytes)
    } else {
        state
    };

    Ok(if let Some(max_query_length) = config.max_query_length {
        state.with_max_query_length(max_query_length)
    } else {
        state
    })
}

fn querier_object_store_inputs<'a>(
    config: &'a ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<(&'a dyn ObjectStore, &'a str, ObjectPath), ServiceConfigError> {
    let store = object_store.ok_or(ServiceConfigError::MissingObjectStore)?;
    let tenant = config
        .tenant
        .as_deref()
        .ok_or(ServiceConfigError::MissingTenant {
            index_source: config.querier_index_source,
        })?;
    let prefix = querier_object_store_prefix(config, object_store_prefix)?.ok_or(
        ServiceConfigError::MissingIndexPrefix {
            index_source: config.querier_index_source,
        },
    )?;

    Ok((store, tenant, prefix))
}

fn querier_object_store_prefix(
    config: &ServiceConfig,
    object_store_prefix: Option<&ObjectPath>,
) -> Result<Option<ObjectPath>, ServiceConfigError> {
    match config.querier_index_source {
        QuerierIndexSource::LocalManifest => Ok(None),
        QuerierIndexSource::TenantObjectStoreManifest
        | QuerierIndexSource::TenantObjectStoreShards => {
            let prefix =
                config
                    .index_prefix
                    .as_deref()
                    .ok_or(ServiceConfigError::MissingIndexPrefix {
                        index_source: config.querier_index_source,
                    })?;
            Ok(Some(effective_object_store_prefix(
                object_store_prefix,
                prefix,
            )))
        }
    }
}

fn effective_object_store_prefix(base: Option<&ObjectPath>, index_prefix: &str) -> ObjectPath {
    let index_prefix = index_prefix.trim_matches('/');
    let Some(base) = base else {
        return ObjectPath::from(index_prefix);
    };
    let base = base.as_ref().trim_matches('/');

    match (base.is_empty(), index_prefix.is_empty()) {
        (true, true) => ObjectPath::from(""),
        (true, false) => ObjectPath::from(index_prefix),
        (false, true) => ObjectPath::from(base),
        (false, false) => ObjectPath::from(format!("{base}/{index_prefix}")),
    }
}

async fn build_configured_querier_state(
    config: &ServiceConfig,
    configured_store: &ConfiguredObjectStore,
) -> Result<QuerierState, ServiceConfigError> {
    if config.tenant.is_none()
        && matches!(
            config.querier_index_source,
            QuerierIndexSource::TenantObjectStoreManifest
                | QuerierIndexSource::TenantObjectStoreShards
        )
    {
        let prefix = querier_object_store_prefix(config, Some(&configured_store.prefix))?.ok_or(
            ServiceConfigError::MissingIndexPrefix {
                index_source: config.querier_index_source,
            },
        )?;
        let state = QuerierState::new(
            config.data_root.clone(),
            LabelIndex::default(),
            BlockIndex::default(),
        );
        return Ok(match config.querier_index_source {
            QuerierIndexSource::TenantObjectStoreManifest => state
                .with_dynamic_tenant_object_store_manifest(
                    Arc::clone(&configured_store.store),
                    prefix,
                ),
            QuerierIndexSource::TenantObjectStoreShards => state
                .with_dynamic_tenant_object_store_shards(
                    Arc::clone(&configured_store.store),
                    prefix,
                ),
            QuerierIndexSource::LocalManifest => state,
        });
    }

    build_querier_state_with_object_store_prefix(
        config,
        Some(configured_store.store.as_ref()),
        Some(&configured_store.prefix),
    )
    .await
}

pub async fn build_service_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Router, ServiceConfigError> {
    build_service_router_with_shutdown(config, dependencies, object_store, CancellationToken::new())
        .await
        .map(|(router, _)| router)
}

async fn build_service_router_with_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
) -> Result<(Router, Option<JoinHandle<()>>), ServiceConfigError> {
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    match config.target {
        Role::Distributor => {
            let sink = dependencies
                .wal_sink
                .ok_or(ServiceConfigError::MissingWalSink)?;
            let ingest_limiter = dependencies
                .ingest_limiter
                .unwrap_or_else(|| Arc::new(AllowAllIngestLimiter));
            Ok((
                distributor_router_with_sink(
                    sink,
                    ingest_limiter,
                    config.max_ingest_body_bytes,
                    config.wal_append_timeout_ms.map(Duration::from_millis),
                    Some(LOKI_REJECT_OLD_SAMPLES_MAX_AGE),
                    Some(LOKI_CREATION_GRACE_PERIOD),
                    metrics,
                ),
                None,
            ))
        }
        Role::Querier => {
            let mut wal_handle: Option<JoinHandle<()>> = None;
            let configured_store = if object_store.is_none() {
                build_configured_object_store(config)?
            } else {
                None
            };
            let mut state = if let Some(configured_store) = configured_store.as_ref() {
                build_configured_querier_state(config, configured_store).await?
            } else {
                build_querier_state(config, object_store).await?
            };
            if let Some(configured_store) = configured_store.as_ref()
                && let Some(prefix) =
                    querier_object_store_prefix(config, Some(&configured_store.prefix))?
            {
                state = state.with_cold_object_store_source(configured_store.store.clone(), prefix);
            }
            if let Some(query_authorizer) = dependencies.query_authorizer {
                state = state.with_query_authorizer_source(query_authorizer);
            }
            let delete_requests = if let Some(delete_requests) = dependencies.delete_requests {
                delete_requests
            } else {
                SharedLogDeleteRequests::from_data_root(&config.data_root)?
            };
            state = state.with_delete_requests(delete_requests);
            state = state.with_rules(SharedLokiRules::from_data_root(&config.data_root)?);
            if let Some(hot_tail) = dependencies.hot_tail {
                state = state.with_hot_tail_source(hot_tail.source, hot_tail.frontier);
            } else if let Some(wal_consumer) = dependencies.wal_consumer {
                // Pre-connected consumer supplied directly (e.g. by tests).
                let hot_tail = BufferedLogHotTail::default();
                let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
                    config,
                    configured_store.as_ref(),
                    object_store,
                )
                .await?;
                if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source)
                {
                    spawn_compaction_frontier_refresher(
                        store,
                        prefix,
                        frontier,
                        hot_tail.clone(),
                        token.clone(),
                    );
                }
                spawn_log_hot_tail_poller(wal_consumer, hot_tail.clone(), frontier.clone());
                if let Some(frontier) = frontier {
                    state = state.with_hot_tail_shared_frontier(hot_tail, frontier);
                } else {
                    state = state.with_hot_tail(hot_tail, i64::MIN);
                }
            } else if let Some(deferred) = dependencies.deferred_wal_consumer_connect {
                // Deferred connect: the consumer and authorizer connect asynchronously so the
                // querier's HTTP port binds without waiting for the broker to be ready (FIX B2).
                let hot_tail = BufferedLogHotTail::default();
                let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
                    config,
                    configured_store.as_ref(),
                    object_store,
                )
                .await?;
                if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source)
                {
                    spawn_compaction_frontier_refresher(
                        store,
                        prefix,
                        frontier,
                        hot_tail.clone(),
                        token.clone(),
                    );
                }

                // Spawn the consumer connect + poll loop in a background task.
                wal_handle = Some(spawn_wal_hot_tail_connect_and_poll(
                    deferred.bootstrap.clone(),
                    deferred.group_id,
                    deferred.topic.clone(),
                    hot_tail.clone(),
                    frontier.clone(),
                    token,
                ));

                // Install a swappable authorizer that starts as AllowAll and gets promoted
                // to BrokerBackedQueryAuthorizer once connected in the background.
                let (swappable, slot) = SwappableQueryAuthorizer::new();
                spawn_query_authorizer_connect(deferred.bootstrap, deferred.topic, slot);
                state = state.with_query_authorizer(swappable);

                if let Some(frontier) = frontier {
                    state = state.with_hot_tail_shared_frontier(hot_tail, frontier);
                } else {
                    state = state.with_hot_tail(hot_tail, i64::MIN);
                }
            }
            state = state.with_metrics(metrics);
            Ok((loki_router(state), wal_handle))
        }
        Role::Compactor => {
            let delete_requests =
                compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
            Ok((compactor_router_with_delete_requests(delete_requests), None))
        }
    }
}

pub async fn serve_service(
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    serve_service_listener(listener, config, dependencies, object_store).await
}

pub async fn serve_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    if config.target == Role::Compactor {
        return serve_compactor_service_listener(listener, config, dependencies, object_store)
            .await;
    }

    let token = CancellationToken::new();
    let token_sig = token.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        token_sig.cancel();
    });
    let token_srv = token.clone();
    let (app, wal_handle) =
        build_service_router_with_shutdown(&config, dependencies, object_store, token.clone())
            .await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .await?;
    token.cancel();
    if let Some(h) = wal_handle {
        let _ = h.await;
    }
    Ok(())
}

async fn serve_compactor_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let delete_requests =
        compactor_delete_requests_for_config(&config, dependencies.delete_requests.clone())?;
    let app = compactor_router_with_delete_requests(delete_requests.clone());
    let dependencies = dependencies.with_delete_requests(delete_requests);
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = http_shutdown_rx.await;
        })
        .into_future();
    let compactor = run_compactor_until_shutdown(&config, dependencies, object_store, pending());
    tokio::pin!(server);
    tokio::pin!(compactor);

    tokio::select! {
        result = &mut server => {
            result?;
            Ok(())
        }
        result = &mut compactor => {
            let _ = http_shutdown_tx.send(());
            result?;
            Ok(())
        }
    }
}

pub fn loki_router(state: QuerierState) -> Router {
    with_role_ops_routes(Router::new(), QUERIER_OPS)
        .route("/loki/api/v1/rules", get(loki_rules))
        .route(
            "/loki/api/v1/rules/{namespace}",
            get(loki_rule_namespace)
                .post(create_loki_rule_group)
                .delete(delete_loki_rule_namespace),
        )
        .route(
            "/loki/api/v1/rules/{namespace}/{group_name}",
            get(loki_rule_group).delete(delete_loki_rule_group),
        )
        .route("/prometheus/api/v1/rules", get(prometheus_rules))
        .route("/prometheus/api/v1/alerts", get(prometheus_alerts))
        .route("/ruler/ring", get(ruler_ring))
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route("/loki/api/v1/patterns", get(patterns).post(patterns_post))
        .route(
            "/loki/api/v1/detected_fields",
            get(detected_fields).post(detected_fields_post),
        )
        .route(
            "/loki/api/v1/detected_labels",
            get(detected_labels).post(detected_labels_post),
        )
        .route(
            "/loki/api/v1/detected_field/{name}/values",
            get(detected_field_values).post(detected_field_values_post),
        )
        .route("/loki/api/v1/query", get(query).post(query_post))
        .route(
            "/loki/api/v1/query_range",
            get(query_range).post(query_range_post),
        )
        .route(
            "/loki/api/v1/labels",
            get(label_names).post(label_names_post),
        )
        .route(
            "/loki/api/v1/label",
            get(label_names).post(label_names_post),
        )
        .route(
            "/loki/api/v1/label/{name}/values",
            get(label_values).post(label_values_post),
        )
        .route("/loki/api/v1/series", get(series).post(series_post))
        .route(
            "/api/prom/query",
            get(api_prom_query).post(api_prom_query_post),
        )
        .route(
            "/api/prom/query_range",
            get(api_prom_query_range).post(api_prom_query_range_post),
        )
        .route("/api/prom/rules", get(loki_rules))
        .route("/api/prom/alerts", get(loki_page_not_found))
        .route("/scheduler/ring", get(scheduler_ring))
        .route(
            "/api/prom/rules/{namespace}",
            get(loki_rule_namespace)
                .post(create_loki_rule_group)
                .delete(delete_loki_rule_namespace),
        )
        .route(
            "/api/prom/rules/{namespace}/{group_name}",
            get(loki_rule_group).delete(delete_loki_rule_group),
        )
        .route("/api/prom/tail", get(tail))
        .route(
            "/api/prom/label",
            get(api_prom_label_names).post(api_prom_label_names_post),
        )
        .route(
            "/api/prom/label/{name}/values",
            get(api_prom_label_values).post(api_prom_label_values_post),
        )
        .route(
            "/api/prom/series",
            get(api_prom_series).post(api_prom_series_post),
        )
        .route(
            "/loki/api/v1/index/stats",
            get(index_stats).post(index_stats_post),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(index_volume).post(index_volume_post),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(index_volume_range).post(index_volume_range_post),
        )
        .route("/loki/api/v1/tail", get(tail))
        .with_state(state)
}

fn compactor_router_with_delete_requests(delete_requests: SharedLogDeleteRequests) -> Router {
    let delete_state = CompactorDeleteState { delete_requests };
    with_role_ops_routes(Router::new(), COMPACTOR_OPS)
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/loki/api/v1/delete",
            get(list_delete_requests)
                .post(create_delete_request)
                .put(create_delete_request)
                .delete(cancel_delete_request),
        )
        .with_state(delete_state)
}

async fn ready() -> Response {
    (StatusCode::OK, "ready\n").into_response()
}

async fn flush_ingester_chunks() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

async fn get_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    let status = if state.prepare_shutdown.load(AtomicOrdering::SeqCst) {
        "set"
    } else {
        "unset"
    };
    text_response(StatusCode::OK, status)
}

async fn set_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(true, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}

async fn unset_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(false, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}

async fn shutdown_ingester() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

async fn log_level() -> Response {
    json_response(
        StatusCode::OK,
        &json!({ "message": "Current log level is info" }),
    )
}

async fn log_level_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let body_query = match form_body_query(&body) {
        Ok(body_query) => body_query,
        Err(error) => return error.into_response(),
    };
    let raw_params = match (raw_query.as_deref(), body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => raw_query.to_owned(),
        (Some(raw_query), false) if !raw_query.is_empty() => format!("{body_query}&{raw_query}"),
        _ => body_query,
    };
    match parse_log_level_param(Some(&raw_params)) {
        Ok(level) => json_response(
            StatusCode::OK,
            &json!({
                "status": "success",
                "message": format!("Log level set to {level}"),
            }),
        ),
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            value,
        }) => log_level_failed_response(format!("unrecognized log level \"{value}\"")),
        Err(HttpQueryError::MissingQueryParameter("log_level")) => {
            log_level_failed_response("unrecognized log level \"\"".to_owned())
        }
        Err(error) => error.into_response(),
    }
}

fn log_level_failed_response(message: String) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        &json!({
            "status": "failed",
            "message": message,
        }),
    )
}

fn parse_log_level_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("log_level"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "log_level" {
            let level = decode_form_component(value)?;
            return match level.as_str() {
                "debug" | "info" | "warn" | "error" => Ok(level),
                _ => Err(HttpQueryError::InvalidQueryParameter {
                    name: "log_level",
                    value: level,
                }),
            };
        }
    }
    Err(HttpQueryError::MissingQueryParameter("log_level"))
}

async fn role_config(
    Extension(ops): Extension<RoleOps>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    status_config(ops.target, raw_query.as_deref())
}

fn status_config(_target: &'static str, raw_query: Option<&str>) -> Response {
    match query_param_value(raw_query, "mode").as_deref() {
        Some("diff") => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain; charset=utf-8")],
                "unsupported type <nil>\n",
            )
                .into_response();
        }
        Some("defaults") => {
            return (
                StatusCode::OK,
                [("content-type", "application/yaml; charset=utf-8")],
                "target: all\nauth_enabled: true\n",
            )
                .into_response();
        }
        _ => {}
    }

    (
        StatusCode::OK,
        [("content-type", "application/yaml; charset=utf-8")],
        "target: all\n",
    )
        .into_response()
}

fn query_param_value(raw_query: Option<&str>, name: &str) -> Option<String> {
    let raw_query = raw_query?;
    for pair in raw_query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key).ok()? == name {
            return decode_form_component(value).ok();
        }
    }
    None
}

async fn role_services(Extension(ops): Extension<RoleOps>) -> Response {
    status_services(ops.target)
}

fn status_services(_name: &'static str) -> Response {
    text_response(
        StatusCode::OK,
        "query-scheduler => Running\n\
         ingester-querier => Running\n\
         query-frontend => Running\n\
         server => Running\n\
         querier => Running\n\
         rule-evaluator => Running\n\
         memberlist-kv => Running\n\
         query-frontend-tripperware => Running\n\
         analytics => Running\n\
         ruler => Running\n\
         cache-generation-loader => Running\n\
         store => Running\n\
         ring => Running\n\
         ingester => Running\n\
         compactor => Running\n\
         distributor => Running\n\
         query-scheduler-ring => Running\n",
    )
}

async fn memberlist_status() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "This instance doesn't use memberlist.",
    )
        .into_response()
}

async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response {
    status_metrics(ops.target)
}

async fn scheduler_ring() -> Response {
    ring_status_page("crabka-scheduler")
}

async fn ruler_ring() -> Response {
    ruler_status_page()
}

async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response {
    ring_status_page(ops.ring_component)
}

#[derive(Clone, Default)]
struct CompactorDeleteState {
    delete_requests: SharedLogDeleteRequests,
}

#[derive(Default, Deserialize, Serialize)]
struct CompactorDeleteRequests {
    next_id: u64,
    requests: Vec<CompactorDeleteRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CompactorDeleteRequest {
    tenant: String,
    request_id: String,
    query: String,
    start_time: i64,
    end_time: i64,
    status: String,
    created_at: i64,
}

#[derive(Clone)]
struct ActiveLogDeleteFilter {
    time_range: TimeRange,
    query: StreamQuery,
}

#[derive(Serialize)]
struct CompactorDeleteRequestResponse {
    request_id: String,
    start_time: i64,
    end_time: i64,
    query: String,
    status: String,
    created_at: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct CreateDeleteRequestParams {
    query: String,
    start_time: i64,
    end_time: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct ListDeleteRequestsParams {
    start_time: Option<i64>,
    end_time: Option<i64>,
}

impl SharedLogDeleteRequests {
    fn from_data_root(root: impl AsRef<FsPath>) -> Result<Self, LogDeleteRequestStoreError> {
        let path = log_delete_requests_path(root.as_ref());
        Ok(Self {
            inner: Arc::new(Mutex::new(read_log_delete_requests(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    fn persist(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = self.inner.lock().expect("compactor delete state poisoned");
        write_log_delete_requests(path, &requests)
    }

    fn refresh(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = read_log_delete_requests(path)?;
        *self.inner.lock().expect("compactor delete state poisoned") = requests;
        Ok(())
    }
}

fn log_delete_requests_path(root: &FsPath) -> PathBuf {
    root.join("log-delete-requests.json")
}

fn read_log_delete_requests(
    path: &FsPath,
) -> Result<CompactorDeleteRequests, LogDeleteRequestStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == ErrorKind::NotFound => {
            return Ok(CompactorDeleteRequests::default());
        }
        Err(source) => {
            return Err(LogDeleteRequestStoreError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| LogDeleteRequestStoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn write_log_delete_requests(
    path: &FsPath,
    requests: &CompactorDeleteRequests,
) -> Result<(), LogDeleteRequestStoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| LogDeleteRequestStoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let tmp_path = path.with_file_name(".log-delete-requests.json.tmp");
    let payload =
        serde_json::to_vec_pretty(requests).map_err(|source| LogDeleteRequestStoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
    std::fs::write(&tmp_path, payload).map_err(|source| LogDeleteRequestStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    std::fs::rename(&tmp_path, path).map_err(|source| LogDeleteRequestStoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}

impl SharedLokiRules {
    fn from_data_root(root: impl AsRef<FsPath>) -> Result<Self, LokiRuleStoreError> {
        let path = loki_ruler_rules_path(root.as_ref());
        Ok(Self {
            tenants: Arc::new(Mutex::new(read_loki_rule_tenants(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    fn persist_snapshot(&self, tenants: &LokiRuleTenants) -> Result<(), LokiRuleStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        write_loki_rule_tenants(path, tenants)
    }
}

fn loki_ruler_rules_path(root: &FsPath) -> PathBuf {
    root.join("loki-ruler-rules.json")
}

fn read_loki_rule_tenants(path: &FsPath) -> Result<LokiRuleTenants, LokiRuleStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == ErrorKind::NotFound => return Ok(LokiRuleTenants::new()),
        Err(source) => {
            return Err(LokiRuleStoreError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| LokiRuleStoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}

fn write_loki_rule_tenants(
    path: &FsPath,
    tenants: &LokiRuleTenants,
) -> Result<(), LokiRuleStoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| LokiRuleStoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let tmp_path = path.with_file_name(".loki-ruler-rules.json.tmp");
    let payload =
        serde_json::to_vec_pretty(tenants).map_err(|source| LokiRuleStoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
    std::fs::write(&tmp_path, payload).map_err(|source| LokiRuleStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    std::fs::rename(&tmp_path, path).map_err(|source| LokiRuleStoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}

async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    match execute_create_delete_request(state, headers, raw_query.as_deref(), &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

fn execute_create_delete_request(
    state: CompactorDeleteState,
    headers: HeaderMap,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(&headers)?.to_string();
    let raw_params = request_query_or_form_body(raw_query, body)?;
    let params = parse_create_delete_request_params(Some(raw_params.as_str()))?;
    parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;

    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests.next_id += 1;
    let request_id = format!("delete-{}", requests.next_id);
    requests.requests.push(CompactorDeleteRequest {
        tenant,
        request_id,
        query: params.query,
        start_time: params.start_time,
        end_time: params.end_time,
        status: "received".to_string(),
        created_at: current_unix_time_ns() / 1_000_000_000,
    });
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}

async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_list_delete_requests(state, headers, raw_query.as_deref()) {
        Ok(requests) => json_response(StatusCode::OK, &json!(requests)),
        Err(error) => error.into_response(),
    }
}

fn execute_list_delete_requests(
    state: CompactorDeleteState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<Vec<CompactorDeleteRequestResponse>, HttpQueryError> {
    let tenant = tenant(&headers)?;
    let params = parse_list_delete_requests_params(raw_query)?;
    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant)
        .filter(|request| delete_request_overlaps_filter(request, &params))
        .map(|request| CompactorDeleteRequestResponse {
            request_id: request.request_id.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            query: request.query.clone(),
            status: request.status.clone(),
            created_at: request.created_at,
        })
        .collect())
}

async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_cancel_delete_request(state, headers, raw_query.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

fn execute_cancel_delete_request(
    state: CompactorDeleteState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(&headers)?.to_string();
    let request_id = parse_cancel_delete_request_params(raw_query)?;
    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .retain(|request| request.tenant != tenant || request.request_id != request_id);
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}

fn request_query_or_form_body(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    match raw_query {
        Some(raw_query) if !raw_query.is_empty() => Ok(raw_query.to_string()),
        _ if !body.is_empty() => form_body_query(body),
        _ => Err(HttpQueryError::MissingQueryParameter("query")),
    }
}

fn parse_create_delete_request_params(
    raw_query: Option<&str>,
) -> Result<CreateDeleteRequestParams, HttpQueryError> {
    let mut query = None;
    let mut start_time = None;
    let mut end_time = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(raw_query, &["query", "start", "end", "max_interval"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;
        match key.as_str() {
            "query" => query = Some(value),
            "start" => start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?),
            "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
            "max_interval" => {
                parse_loki_duration_query_param("max_interval", &value)?;
            }
            _ => {}
        }
    }

    let start_time = start_time.ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end_time = end_time.unwrap_or_else(|| current_unix_time_ns() / 1_000_000_000);
    if end_time < start_time {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "end",
            value: "end must be greater than or equal to start".to_string(),
        });
    }

    Ok(CreateDeleteRequestParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start_time,
        end_time,
    })
}

fn parse_list_delete_requests_params(
    raw_query: Option<&str>,
) -> Result<ListDeleteRequestsParams, HttpQueryError> {
    let mut start_time = None;
    let mut end_time = None;
    if let Some(raw_query) = raw_query {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_form_component(key)?;
            let value = decode_form_component(value)?;
            match key.as_str() {
                "start" => {
                    start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?);
                }
                "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
                _ => {}
            }
        }
    }
    if start_time.is_some() != end_time.is_some() {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "start",
            value: "start and end must be provided together".to_string(),
        });
    }
    Ok(ListDeleteRequestsParams {
        start_time,
        end_time,
    })
}

fn parse_cancel_delete_request_params(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let mut request_id = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("request_id"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;
        match key.as_str() {
            "request_id" => request_id = Some(value),
            "force" => match value.as_str() {
                "true" | "false" => {}
                _ => {
                    return Err(HttpQueryError::InvalidQueryParameter {
                        name: "force",
                        value,
                    });
                }
            },
            _ => {}
        }
    }
    request_id.ok_or(HttpQueryError::MissingQueryParameter("request_id"))
}

fn parse_loki_delete_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        return Ok(seconds);
    }
    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns / 1_000_000_000);
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(|timestamp| timestamp.unix_timestamp())
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}

fn delete_request_overlaps_filter(
    request: &CompactorDeleteRequest,
    params: &ListDeleteRequestsParams,
) -> bool {
    match (params.start_time, params.end_time) {
        (Some(start_time), Some(end_time)) => {
            request.end_time >= start_time && request.start_time <= end_time
        }
        _ => true,
    }
}

fn active_log_delete_filters(
    state: &QuerierState,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, HttpQueryError> {
    let Some(delete_requests) = &state.delete_requests else {
        return Ok(Vec::new());
    };
    Ok(active_log_delete_filters_from_requests(
        delete_requests,
        tenant,
        query_range,
    )?)
}

fn active_log_delete_filters_from_requests(
    delete_requests: &SharedLogDeleteRequests,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant)
        .filter_map(|request| {
            delete_request_time_range(request)
                .ok()
                .filter(|range| ranges_overlap(*range, query_range))
                .map(|range| (request, range))
        })
        .map(|(request, time_range)| {
            let query = parse_query(&request.query).map_err(|source| {
                ActiveLogDeleteFilterError::Parse {
                    query: request.query.clone(),
                    source,
                }
            })?;
            Ok(ActiveLogDeleteFilter { time_range, query })
        })
        .collect()
}

fn delete_request_time_range(
    request: &CompactorDeleteRequest,
) -> Result<TimeRange, ActiveLogDeleteFilterError> {
    let start_ns =
        request
            .start_time
            .checked_mul(1_000_000_000)
            .ok_or(BlockStoreError::InvalidTimeRange {
                start_ns: request.start_time,
                end_ns: request.end_time,
            })?;
    let end_ns = request
        .end_time
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(999_999_999))
        .ok_or(BlockStoreError::InvalidTimeRange {
            start_ns: request.start_time,
            end_ns: request.end_time,
        })?;
    TimeRange::new(start_ns, end_ns).map_err(ActiveLogDeleteFilterError::from)
}

fn ranges_overlap(left: TimeRange, right: TimeRange) -> bool {
    left.end_ns >= right.start_ns && left.start_ns <= right.end_ns
}

fn ring_status_page(instance: &'static str) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><html><head><title>Ring Status</title></head>\
         <body><h1>Ring Status</h1>\
         <table><thead><tr><th>Instance</th><th>State</th></tr></thead>\
         <tbody><tr><td>{instance}</td><td>ACTIVE</td></tr></tbody>\
         </table></body></html>"
        ),
    )
        .into_response()
}

fn ruler_status_page() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        "<!doctype html><html><head><title>Cortex Ruler Status</title></head>\
         <body><h1>Cortex Ruler Status</h1></body></html>",
    )
        .into_response()
}

async fn loki_rules(State(state): State<QuerierState>, headers: HeaderMap) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(namespaces) = rules.get(&tenant).map(loki_rule_namespace_response) else {
        return missing_loki_rule_directory_response(&tenant);
    };
    loki_yaml_response(StatusCode::OK, &namespaces)
}

async fn loki_page_not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "404 page not found\n")
}

fn missing_loki_rule_directory_response(tenant: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "unable to read rule dir /loki/rules/{tenant}: open /loki/rules/{tenant}: no such file or directory\n"
        ),
    )
}

async fn loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    if !rules.contains_key(&tenant) {
        return missing_loki_rule_namespace_response(&tenant, &namespace);
    }
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
    };
    loki_yaml_response(
        StatusCode::OK,
        &groups.values().cloned().collect::<Vec<_>>(),
    )
}

fn missing_loki_rule_namespace_response(tenant: &str, namespace: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "error parsing /loki/rules/{tenant}/{namespace}: /loki/rules/{tenant}/{namespace}: open /loki/rules/{tenant}/{namespace}: no such file or directory\n"
        ),
    )
}

async fn create_loki_rule_group(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let Ok(rule_group) = parse_loki_rule_group(&body) else {
        return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n");
    };
    let name = match loki_rule_group_name(&rule_group) {
        Some(name) => name.to_string(),
        None => return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n"),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        rules
            .entry(tenant.clone())
            .or_default()
            .entry(namespace)
            .or_default()
            .insert(name, rule_group);
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(&tenant) else {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        };
        if namespaces.remove(&namespace).is_none() {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        }
        if namespaces.is_empty() {
            rules.remove(&tenant);
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

async fn loki_rule_group(
    State(state): State<QuerierState>,
    Path((namespace, group_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        );
    };
    let Some(group) = groups.get(&group_name) else {
        return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
    };
    loki_yaml_response(StatusCode::OK, group)
}

async fn delete_loki_rule_group(
    State(state): State<QuerierState>,
    Path((namespace, group_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(&tenant) else {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        };
        let Some(groups) = namespaces.get_mut(&namespace) else {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        };
        if groups.remove(&group_name).is_none() {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        }
        if groups.is_empty() {
            namespaces.remove(&namespace);
        }
        if namespaces.is_empty() {
            rules.remove(&tenant);
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

fn loki_ruler_tenant(headers: &HeaderMap) -> Result<String, HttpQueryError> {
    match headers.get("X-Scope-OrgID") {
        Some(value) => {
            let tenant = value.to_str().map_err(|_| HttpQueryError::InvalidTenant)?;
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant.to_string())
            }
        }
        None => Ok("fake".to_string()),
    }
}

fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    namespaces
        .iter()
        .map(|(namespace, groups)| (namespace.clone(), groups.values().cloned().collect()))
        .collect()
}

fn parse_loki_rule_group(body: &[u8]) -> Result<serde_yaml::Value, ()> {
    let rule_group = serde_yaml::from_slice(body).map_err(|_| ())?;
    validate_loki_rule_group(&rule_group)?;
    Ok(rule_group)
}

fn loki_rule_group_name(rule_group: &serde_yaml::Value) -> Option<&str> {
    let serde_yaml::Value::Mapping(fields) = rule_group else {
        return None;
    };
    fields
        .get(serde_yaml::Value::String("name".to_string()))
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
}

fn validate_loki_rule_group(rule_group: &serde_yaml::Value) -> Result<(), ()> {
    let fields = loki_yaml_mapping(rule_group).ok_or(())?;
    if loki_rule_group_name(rule_group).is_none() {
        return Err(());
    }
    let rules = fields
        .get(serde_yaml_key("rules"))
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or(())?;
    for rule in rules {
        validate_loki_rule(rule)?;
    }
    Ok(())
}

fn validate_loki_rule(rule: &serde_yaml::Value) -> Result<(), ()> {
    let fields = loki_yaml_mapping(rule).ok_or(())?;
    yaml_string_field(fields, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or(())?;
    let is_alert = yaml_string_field(fields, "alert").is_some_and(|name| !name.is_empty());
    let is_record = yaml_string_field(fields, "record").is_some_and(|name| !name.is_empty());
    if is_alert == is_record {
        return Err(());
    }
    Ok(())
}

fn loki_yaml_response(status: StatusCode, value: &impl Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(body) => (
            status,
            [("content-type", "application/yaml; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(source) => text_response(StatusCode::INTERNAL_SERVER_ERROR, &source.to_string()),
    }
}

async fn prometheus_rules(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let filters = match PrometheusRulesFilters::parse(raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
    let namespaces = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned")
        .get(&tenant)
        .cloned();
    let page = match namespaces {
        Some(namespaces) => {
            match prometheus_rule_groups_response(
                &state,
                &tenant,
                &namespaces,
                &filters,
                evaluation_time,
            )
            .await
            {
                Ok(page) => page,
                Err(error) => return error.into_response(),
            }
        }
        None => match filters.page_groups(Vec::new()) {
            Ok(page) => page,
            Err(error) => return error.into_response(),
        },
    };
    let mut data = json!({
        "groups": page.groups
    });
    if let Some(token) = page.next_token {
        data["groupNextToken"] = json!(token);
    }
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": data,
            "errorType": "",
            "error": "",
        }),
    )
}

async fn prometheus_alerts(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let filters = match PrometheusRulesFilters::parse(raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
    let namespaces = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned")
        .get(&tenant)
        .cloned();
    let alerts = match namespaces {
        Some(namespaces) => {
            match prometheus_alerts_response(&state, &tenant, &namespaces, evaluation_time).await {
                Ok(alerts) => alerts,
                Err(error) => return error.into_response(),
            }
        }
        None => Vec::new(),
    };
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": {
                "alerts": alerts
            },
            "errorType": "",
            "error": "",
        }),
    )
}

#[derive(Default)]
struct PrometheusRulesFilters {
    rule_kind: Option<&'static str>,
    rule_names: BTreeSet<String>,
    rule_groups: BTreeSet<String>,
    files: BTreeSet<String>,
    label_selectors: Vec<StreamQuery>,
    group_limit: Option<usize>,
    group_next_token: Option<String>,
    exclude_alerts: bool,
    evaluation_time: Option<i64>,
}

impl PrometheusRulesFilters {
    fn parse(raw_query: Option<&str>) -> Result<Self, HttpQueryError> {
        let mut filters = Self::default();
        let Some(raw_query) = raw_query else {
            return Ok(filters);
        };
        for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
            match key.as_ref() {
                "type" if value == "alert" => filters.rule_kind = Some("alerting"),
                "type" if value == "record" => filters.rule_kind = Some("recording"),
                "exclude_alerts" if value == "true" => filters.exclude_alerts = true,
                "time" if !value.is_empty() => {
                    filters.evaluation_time =
                        Some(parse_loki_timestamp_query_param("time", &value)?);
                }
                "rule_name" | "rule_name[]" if !value.is_empty() => {
                    filters.rule_names.insert(value.into_owned());
                }
                "rule_group" | "rule_group[]" if !value.is_empty() => {
                    filters.rule_groups.insert(value.into_owned());
                }
                "file" | "file[]" if !value.is_empty() => {
                    filters.files.insert(value.into_owned());
                }
                "group_limit" if !value.is_empty() => {
                    filters.group_limit = Some(parse_usize_query_param("group_limit", &value)?);
                }
                "group_next_token" if !value.is_empty() => {
                    filters.group_next_token = Some(value.into_owned());
                }
                "match" | "match[]" if !value.is_empty() => {
                    let selector = value.into_owned();
                    filters
                        .label_selectors
                        .push(parse_query(&selector).map_err(|source| {
                            HttpQueryError::LokiParse {
                                query: selector.clone(),
                                source,
                            }
                        })?);
                }
                _ => {}
            }
        }
        if filters.group_next_token.is_some() && filters.group_limit.is_none() {
            return Err(HttpQueryError::MissingQueryParameter("group_limit"));
        }
        Ok(filters)
    }

    fn has_rule_filter(&self) -> bool {
        self.rule_kind.is_some() || !self.rule_names.is_empty() || !self.label_selectors.is_empty()
    }

    fn matches_rule(&self, rule: &Value, source_rule: &serde_yaml::Value) -> bool {
        if self
            .rule_kind
            .is_some_and(|kind| rule.get("type").and_then(Value::as_str) != Some(kind))
        {
            return false;
        }
        if !self.rule_names.is_empty()
            && !rule
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| self.rule_names.contains(name))
        {
            return false;
        }
        self.matches_rule_labels(source_rule)
    }

    fn matches_rule_labels(&self, source_rule: &serde_yaml::Value) -> bool {
        if self.label_selectors.is_empty() {
            return true;
        }
        let labels = loki_yaml_mapping(source_rule)
            .map(|fields| yaml_string_labels_field(fields, "labels"))
            .unwrap_or_default();
        self.label_selectors.iter().any(|selector| {
            selector
                .matchers
                .iter()
                .all(|matcher| matcher.matches(&labels))
        })
    }

    fn page_groups(
        &self,
        groups: Vec<PrometheusRuleGroupResponse>,
    ) -> Result<PrometheusRulesPage, HttpQueryError> {
        let start_index = match &self.group_next_token {
            Some(token) => groups
                .iter()
                .position(|group| group.token == *token)
                .map(|index| index + 1)
                .ok_or_else(|| HttpQueryError::InvalidQueryParameter {
                    name: "group_next_token",
                    value: token.clone(),
                })?,
            None => 0,
        };
        let Some(limit) = self.group_limit else {
            return Ok(PrometheusRulesPage {
                groups: groups
                    .into_iter()
                    .skip(start_index)
                    .map(|group| group.value)
                    .collect(),
                next_token: None,
            });
        };
        let next_token = (groups.len() > start_index.saturating_add(limit) && limit > 0)
            .then(|| groups[start_index + limit - 1].token.clone());
        Ok(PrometheusRulesPage {
            groups: groups
                .into_iter()
                .skip(start_index)
                .take(limit)
                .map(|group| group.value)
                .collect(),
            next_token,
        })
    }
}

#[derive(Default)]
struct PrometheusRulesPage {
    groups: Vec<Value>,
    next_token: Option<String>,
}

struct PrometheusRuleGroupResponse {
    token: String,
    value: Value,
}

async fn prometheus_rule_groups_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<PrometheusRulesPage, HttpQueryError> {
    let mut response_groups = Vec::new();
    for (namespace, groups) in namespaces {
        if !filters.files.is_empty() && !filters.files.contains(namespace) {
            continue;
        }
        for group in groups.values() {
            let Some(name) = loki_rule_group_name(group) else {
                continue;
            };
            if !filters.rule_groups.is_empty() && !filters.rule_groups.contains(name) {
                continue;
            }
            let rules =
                prometheus_rules_for_group(state, tenant, group, filters, evaluation_time).await?;
            if filters.has_rule_filter() && rules.is_empty() {
                continue;
            }
            response_groups.push(PrometheusRuleGroupResponse {
                token: prometheus_rule_group_page_token(namespace, name),
                value: json!({
                    "name": name,
                    "file": namespace,
                    "interval": prometheus_rule_group_interval_seconds(group),
                    "limit": 0,
                    "rules": rules,
                }),
            });
        }
    }
    filters.page_groups(response_groups)
}

fn prometheus_rule_group_page_token(namespace: &str, group_name: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{namespace}\n{group_name}"))
}

async fn prometheus_rules_for_group(
    state: &QuerierState,
    tenant: &str,
    group: &serde_yaml::Value,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut response_rules = Vec::new();
    let Some(rules) = loki_yaml_mapping(group)
        .and_then(|fields| fields.get(serde_yaml_key("rules")))
        .and_then(serde_yaml::Value::as_sequence)
    else {
        return Ok(response_rules);
    };
    for source_rule in rules {
        let Some(mut rule) = prometheus_rule_response(source_rule) else {
            continue;
        };
        if !filters.matches_rule(&rule, source_rule) {
            continue;
        }
        if !filters.exclude_alerts && rule.get("type").and_then(Value::as_str) == Some("alerting") {
            let alerts =
                prometheus_alerts_for_rule(state, tenant, source_rule, evaluation_time).await?;
            rule["alerts"] = json!(alerts);
        }
        response_rules.push(rule);
    }
    Ok(response_rules)
}

fn prometheus_rule_response(rule: &serde_yaml::Value) -> Option<Value> {
    let fields = loki_yaml_mapping(rule)?;
    let query = yaml_string_field(fields, "expr")?;
    if let Some(name) = yaml_string_field(fields, "alert") {
        let mut rule = json!({
            "type": "alerting",
            "name": name,
            "query": query,
            "duration": yaml_duration_seconds_field(fields, "for").unwrap_or(0),
            "labels": yaml_string_map_field(fields, "labels"),
            "annotations": yaml_string_map_field(fields, "annotations"),
            "alerts": [],
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        remove_empty_object_field(&mut rule, "annotations");
        return Some(rule);
    }
    yaml_string_field(fields, "record").map(|name| {
        let mut rule = json!({
            "type": "recording",
            "name": name,
            "query": query,
            "labels": yaml_string_map_field(fields, "labels"),
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        rule
    })
}

fn prometheus_rule_group_interval_seconds(group: &serde_yaml::Value) -> i64 {
    loki_yaml_mapping(group)
        .and_then(|fields| yaml_duration_seconds_field(fields, "interval"))
        .unwrap_or(0)
}

fn yaml_duration_seconds_field(fields: &serde_yaml::Mapping, name: &'static str) -> Option<i64> {
    yaml_duration_ns_field(fields, name)
        .and_then(|duration_ns| duration_ns.checked_div(1_000_000_000))
}

fn yaml_duration_ns_field(fields: &serde_yaml::Mapping, name: &'static str) -> Option<i64> {
    let duration = yaml_string_field(fields, name)?;
    parse_prometheus_duration(duration)
}

fn yaml_string_field<'a>(fields: &'a serde_yaml::Mapping, name: &'static str) -> Option<&'a str> {
    fields
        .get(serde_yaml_key(name))
        .and_then(serde_yaml::Value::as_str)
}

fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
    let values = fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), json!(value.as_str()?)))
                })
                .collect::<serde_json::Map<_, _>>()
        })
        .unwrap_or_default();
    Value::Object(values)
}

fn yaml_string_template_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), value.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn yaml_string_labels_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), value.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn expand_prometheus_alert_template(template: &str, labels: &Labels, value: &str) -> String {
    let mut expanded = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        expanded.push_str(&remaining[..start]);
        let action_start = start + "{{".len();
        let action = &remaining[action_start..];
        let Some(end) = action.find("}}") else {
            expanded.push_str(&remaining[start..]);
            return expanded;
        };
        let expression = action[..end].trim();
        if expression == "$value" {
            expanded.push_str(value);
        } else if let Some(name) = expression.strip_prefix("$labels.") {
            if let Some(label_value) = labels.get(name) {
                expanded.push_str(label_value);
            } else {
                expanded.push_str("{{");
                expanded.push_str(&action[..end]);
                expanded.push_str("}}");
            }
        } else {
            expanded.push_str("{{");
            expanded.push_str(&action[..end]);
            expanded.push_str("}}");
        }
        remaining = &action[end + "}}".len()..];
    }
    expanded.push_str(remaining);
    expanded
}

fn prometheus_alert_template_map(templates: &Labels, labels: &Labels, value: &str) -> Value {
    Value::Object(
        templates
            .iter()
            .map(|(key, template)| {
                (
                    key.clone(),
                    json!(expand_prometheus_alert_template(template, labels, value)),
                )
            })
            .collect(),
    )
}

fn loki_yaml_mapping(value: &serde_yaml::Value) -> Option<&serde_yaml::Mapping> {
    match value {
        serde_yaml::Value::Mapping(fields) => Some(fields),
        _ => None,
    }
}

fn serde_yaml_key(value: &'static str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_string())
}

fn remove_empty_object_field(value: &mut Value, field: &'static str) {
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    if fields
        .get(field)
        .and_then(Value::as_object)
        .is_some_and(serde_json::Map::is_empty)
    {
        fields.remove(field);
    }
}

async fn prometheus_alerts_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut alerts = Vec::new();
    for groups in namespaces.values() {
        for group in groups.values() {
            let Some(rules) = loki_yaml_mapping(group)
                .and_then(|fields| fields.get(serde_yaml_key("rules")))
                .and_then(serde_yaml::Value::as_sequence)
            else {
                continue;
            };
            for rule in rules {
                alerts.extend(
                    prometheus_alerts_for_rule(state, tenant, rule, evaluation_time).await?,
                );
            }
        }
    }
    Ok(alerts)
}

async fn prometheus_alerts_for_rule(
    state: &QuerierState,
    tenant: &str,
    rule: &serde_yaml::Value,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let Some(fields) = loki_yaml_mapping(rule) else {
        return Ok(Vec::new());
    };
    let Some(alert_name) = yaml_string_field(fields, "alert") else {
        return Ok(Vec::new());
    };
    let Some(query) = yaml_string_field(fields, "expr") else {
        return Ok(Vec::new());
    };
    let params = QueryParams {
        query: query.to_string(),
        time: Some(evaluation_time),
        start: None,
        end: None,
        since: None,
        step: None,
        interval: None,
        limit: None,
        direction: None,
        delay_for: None,
    };
    let result = execute_http_query_for_tenant(state, tenant, &params, QueryKind::Instant).await?;
    Ok(prometheus_alerts_from_query_result(
        &state.alert_states,
        tenant,
        alert_name,
        fields,
        query,
        evaluation_time,
        &result,
    ))
}

fn prometheus_alerts_from_query_result(
    alert_states: &SharedPrometheusAlertStates,
    tenant: &str,
    alert_name: &str,
    fields: &serde_yaml::Mapping,
    query: &str,
    evaluation_time: i64,
    result: &Value,
) -> Vec<Value> {
    let hold_duration_ns = yaml_duration_ns_field(fields, "for").unwrap_or(0);
    let keep_firing_for_ns = yaml_duration_ns_field(fields, "keep_firing_for").unwrap_or(0);
    let annotation_templates = yaml_string_template_map_field(fields, "annotations");
    let rule_label_templates = yaml_string_template_map_field(fields, "labels");
    let samples = result
        .pointer("/data/result")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sample| {
            let value = sample
                .get("value")
                .and_then(Value::as_array)
                .and_then(|value| value.get(1))
                .and_then(Value::as_str)?;
            let mut labels = BTreeMap::new();
            if let Some(metric) = sample.get("metric").and_then(Value::as_object) {
                for (key, value) in metric {
                    if let Some(value) = value.as_str() {
                        labels.insert(key.clone(), value.to_string());
                    }
                }
            }
            for (key, template) in &rule_label_templates {
                labels.insert(
                    key.clone(),
                    expand_prometheus_alert_template(template, &labels, value),
                );
            }
            labels.insert("alertname".to_string(), alert_name.to_string());
            Some((labels, value.to_string()))
        })
        .collect::<Vec<_>>();

    let mut states = alert_states
        .alerts
        .lock()
        .expect("Prometheus alert state lock poisoned");
    let mut active_keys = BTreeSet::new();
    let mut alerts = samples
        .into_iter()
        .map(|(labels, value)| {
            let key = PrometheusAlertKey {
                tenant: tenant.to_string(),
                alert_name: alert_name.to_string(),
                query: query.to_string(),
                labels: labels.clone(),
            };
            let alert = states
                .entry(key.clone())
                .or_insert_with(|| PrometheusAlertRuntimeState {
                    active_at: evaluation_time,
                    last_active_at: evaluation_time,
                    value: value.clone(),
            });
            alert.last_active_at = evaluation_time;
            alert.value.clone_from(&value);
            let state = if evaluation_time.saturating_sub(alert.active_at) >= hold_duration_ns {
                "firing"
            } else {
                "pending"
            };
            active_keys.insert(key);
            json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(&annotation_templates, &labels, &value),
                "labels": labels,
                "state": state,
                "value": value,
            })
        })
        .collect::<Vec<_>>();

    let (retained_alerts, retained_keys) = retained_prometheus_alerts(
        &states,
        &PrometheusRetainedAlertParams {
            tenant,
            alert_name,
            query,
            evaluation_time,
            hold_duration_ns,
            keep_firing_for_ns,
            active_keys: &active_keys,
            annotation_templates: &annotation_templates,
        },
    );
    alerts.extend(retained_alerts);

    states.retain(|key, _| {
        key.tenant != tenant
            || key.alert_name != alert_name
            || key.query != query
            || active_keys.contains(key)
            || retained_keys.contains(key)
    });
    alerts
}

struct PrometheusRetainedAlertParams<'a> {
    tenant: &'a str,
    alert_name: &'a str,
    query: &'a str,
    evaluation_time: i64,
    hold_duration_ns: i64,
    keep_firing_for_ns: i64,
    active_keys: &'a BTreeSet<PrometheusAlertKey>,
    annotation_templates: &'a Labels,
}

fn retained_prometheus_alerts(
    states: &BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>,
    params: &PrometheusRetainedAlertParams<'_>,
) -> (Vec<Value>, BTreeSet<PrometheusAlertKey>) {
    let mut retained_alerts = Vec::new();
    let mut retained_keys = BTreeSet::new();
    for (key, alert) in states {
        if !prometheus_alert_key_matches_rule(key, params) {
            continue;
        }
        let was_firing =
            alert.last_active_at.saturating_sub(alert.active_at) >= params.hold_duration_ns;
        let within_keep_firing = params.evaluation_time.saturating_sub(alert.last_active_at)
            <= params.keep_firing_for_ns;
        if was_firing && within_keep_firing {
            retained_keys.insert(key.clone());
            retained_alerts.push(json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(
                    params.annotation_templates,
                    &key.labels,
                    &alert.value,
                ),
                "labels": key.labels,
                "state": "firing",
                "value": alert.value,
            }));
        }
    }
    (retained_alerts, retained_keys)
}

fn prometheus_alert_key_matches_rule(
    key: &PrometheusAlertKey,
    params: &PrometheusRetainedAlertParams<'_>,
) -> bool {
    key.tenant == params.tenant
        && key.alert_name == params.alert_name
        && key.query == params.query
        && !params.active_keys.contains(key)
}

fn prometheus_active_at(timestamp_ns: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ns))
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn status_metrics(component: &'static str) -> Response {
    let compactor_running = usize::from(component == "compactor");
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        format!(
            "# HELP loki_build_info A metric with a constant '1' value labeled by version, revision, branch, goversion from which loki was built, and the goos and goarch for the build.\n\
             # TYPE loki_build_info gauge\n\
             loki_build_info{{branch=\"unknown\",goarch=\"unknown\",goos=\"unknown\",goversion=\"unknown\",revision=\"unknown\",tags=\"\",version=\"{}\"}} 1\n\
             # HELP loki_boltdb_shipper_compactor_running Value will be 1 if compactor is currently running on this instance\n\
             # TYPE loki_boltdb_shipper_compactor_running gauge\n\
             loki_boltdb_shipper_compactor_running {compactor_running}\n\
             # HELP crabka_observability_service_up Whether the observability service is running.\n\
             # TYPE crabka_observability_service_up gauge\n\
             crabka_observability_service_up{{component=\"{component}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        ),
    )
        .into_response()
}

async fn build_info() -> Response {
    let value = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "unknown",
        "branch": "unknown",
        "buildDate": "",
        "buildUser": "crabka",
        "goVersion": "not-go",
    });
    json_response(StatusCode::OK, &value)
}

#[derive(Debug)]
struct QueryParams {
    query: String,
    time: Option<i64>,
    start: Option<i64>,
    end: Option<i64>,
    since: Option<i64>,
    step: Option<i64>,
    interval: Option<i64>,
    limit: Option<usize>,
    direction: Option<String>,
    delay_for: Option<i64>,
}

#[derive(Debug, Default)]
struct SeriesParams {
    matchers: Vec<String>,
    start: Option<i64>,
    end: Option<i64>,
    since: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VolumeKind {
    Instant,
    Range,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VolumeAggregateBy {
    Series,
    Labels,
}

#[derive(Debug)]
struct VolumeParams {
    query: String,
    start: i64,
    end: i64,
    step: Option<i64>,
    limit: usize,
    target_labels: Option<Vec<String>>,
    aggregate_by: VolumeAggregateBy,
}

#[derive(Debug)]
struct DetectedFieldsParams {
    query: String,
    start: i64,
    end: i64,
    limit: usize,
    line_limit: usize,
}

#[derive(Debug)]
struct DetectedLabelsParams {
    query: Option<String>,
    start: i64,
    end: i64,
    limit: usize,
}

#[derive(Debug)]
struct PatternsParams {
    query: String,
    start: i64,
    end: i64,
    step: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DetectedFieldType {
    Boolean,
    Int,
    Float,
    Duration,
    Bytes,
    String,
}

impl DetectedFieldType {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::String, _) | (_, Self::String) => Self::String,
            (Self::Bytes, Self::Bytes) => Self::Bytes,
            (Self::Duration, Self::Duration) => Self::Duration,
            (Self::Float, _) | (_, Self::Float) => Self::Float,
            (Self::Int, Self::Int) => Self::Int,
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            _ => Self::String,
        }
    }

    fn as_loki_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Float => "float",
            Self::Duration => "duration",
            Self::Bytes => "bytes",
            Self::String => "string",
        }
    }
}

#[derive(Debug)]
struct DetectedFieldStats {
    ty: DetectedFieldType,
    values: BTreeSet<String>,
    parsers: BTreeSet<&'static str>,
}

impl DetectedFieldStats {
    fn new(ty: DetectedFieldType, value: String, parser: &'static str) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::from([parser]),
        }
    }

    fn new_generated(ty: DetectedFieldType, value: String) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::new(),
        }
    }

    fn add(&mut self, ty: DetectedFieldType, value: String, parser: &'static str) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
        self.parsers.insert(parser);
    }

    fn add_generated(&mut self, ty: DetectedFieldType, value: String) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
    }

    fn parsers_json(self) -> Value {
        if self.parsers.is_empty() {
            Value::Null
        } else {
            json!(self.parsers.into_iter().collect::<Vec<_>>())
        }
    }
}

async fn query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Instant,
    )
    .await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}

async fn query_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => {
            let resp = error.into_response();
            state.record_query("query", resp.status().is_success(), start);
            return resp;
        }
    };
    let resp = handle_query(state.clone(), headers, Some(&raw_query), QueryKind::Instant).await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}

async fn api_prom_query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query(state, headers, raw_query.as_deref()).await
}

async fn api_prom_query_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    handle_api_prom_query(state, headers, Some(&raw_query)).await
}

async fn api_prom_query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query_range(state, headers, raw_query.as_deref()).await
}

async fn api_prom_query_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    handle_api_prom_query_range(state, headers, Some(&raw_query)).await
}

async fn query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Range,
    )
    .await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}

async fn query_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => {
            let resp = error.into_response();
            state.record_query("query_range", resp.status().is_success(), start);
            return resp;
        }
    };
    let resp = handle_query(state.clone(), headers, Some(&raw_query), QueryKind::Range).await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}

async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match execute_format_query(raw_query.as_deref()) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

async fn format_query_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_format_query(Some(&raw_query)) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

async fn patterns(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_patterns_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn patterns_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_patterns_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_fields(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_fields_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_fields_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_fields_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_labels(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_labels_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_labels_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_labels_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_field_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_field_values_query(&state, &headers, &name, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_field_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_field_values_query(&state, &headers, &name, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn label_names(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_names_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("labels", resp.status().is_success(), start);
    resp
}

async fn label_names_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn label_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_values_query(&state, &headers, &name, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("label_values", resp.status().is_success(), start);
    resp
}

async fn label_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_label_values_query(&state, &headers, &name, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_series_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("series", resp.status().is_success(), start);
    resp
}

async fn series_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_names(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_names_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_series_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn index_stats(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_stats_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_stats", resp.status().is_success(), start);
    resp
}

async fn index_stats_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_stats_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_volume_query(
        &state,
        &headers,
        raw_query.as_deref(),
        VolumeKind::Instant,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_volume", resp.status().is_success(), start);
    resp
}

async fn index_volume_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(&state, &headers, Some(&raw_query), VolumeKind::Instant).await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_index_volume_query(&state, &headers, raw_query.as_deref(), VolumeKind::Range)
        .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(&state, &headers, Some(&raw_query), VolumeKind::Range).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn tail(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = match parse_query_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match prepare_http_tail(&state, &headers, &params).await {
        Ok(tail) => ws
            .on_upgrade(move |socket| send_tail_stream(socket, tail))
            .into_response(),
        Err(error) => error.into_response(),
    }
}

async fn handle_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
) -> Response {
    let wants_parquet = wants_loki_parquet(&headers);
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, kind).await {
        Ok(value) if wants_parquet => match loki_parquet_response(&value) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn handle_api_prom_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, QueryKind::Instant).await {
        Ok(value) => api_prom_streams_only_response(value),
        Err(error) => error.into_response(),
    }
}

async fn handle_api_prom_query_range(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, QueryKind::Range).await {
        Ok(value) => api_prom_streams_only_response(value),
        Err(error) => error.into_response(),
    }
}

fn api_prom_streams_only_response(value: Value) -> Response {
    if value.pointer("/data/resultType").and_then(Value::as_str) == Some("streams") {
        json_response(StatusCode::OK, &value)
    } else {
        text_response(
            StatusCode::BAD_REQUEST,
            "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
        )
    }
}

async fn execute_http_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let tenants = authorized_tenants(state, headers).await?;
    if tenants.len() > 1 {
        return execute_http_multi_tenant_query(state, &tenants, &params, kind).await;
    }
    execute_http_query_for_tenant(state, &tenants[0], &params, kind).await
}

async fn execute_http_multi_tenant_query(
    state: &QuerierState,
    tenants: &[String],
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let time_range = time_range(params, kind)?;
        validate_loki_range_query_range_limit(kind, time_range)?;
        validate_loki_query_range_resolution(params, kind, time_range)?;
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => {
                let step_ns = params
                    .step
                    .unwrap_or_else(|| default_metric_range_step(time_range));
                if step_ns <= 0 {
                    return Err(HttpQueryError::InvalidStep);
                }
                loki_range_vector_response(time_range, step_ns, result)
            }
        };
        return Ok(add_loki_query_stats(value));
    }

    let mut merged = None;
    for tenant in tenants {
        let response = execute_http_query_for_tenant(state, tenant, params, kind).await?;
        match &mut merged {
            Some(merged) => merge_loki_query_response(merged, response),
            None => merged = Some(response),
        }
    }
    Ok(merged.unwrap_or_else(|| {
        add_loki_query_stats(loki_success_value(json!({
            "resultType": "streams",
            "result": []
        })))
    }))
}

async fn execute_http_query_for_tenant(
    state: &QuerierState,
    tenant: &str,
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let time_range = time_range(params, kind)?;
    validate_loki_range_query_range_limit(kind, time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    validate_loki_query_range_resolution(params, kind, time_range)?;
    let limit = params.limit;
    let direction = loki_direction(params.direction.as_deref())?;
    let interval = params.interval;
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => {
                let step_ns = params
                    .step
                    .unwrap_or_else(|| default_metric_range_step(time_range));
                if step_ns <= 0 {
                    return Err(HttpQueryError::InvalidStep);
                }
                loki_range_vector_response(time_range, step_ns, result)
            }
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Some(sort) = parse_sort_vector_expression(&params.query) {
        return execute_http_sort_vector_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            sort,
            &params.query,
        )
        .await;
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            label_replace.query.clone(),
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            &params.query,
        )?;
        return Ok(value);
    }
    if let Some(binary) = parse_label_replace_metric_binary_expression(&params.query) {
        return execute_http_label_replace_metric_binary_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            binary,
            &params.query,
        )
        .await;
    }
    if let Some(label_replace) = parse_label_replace_expression(&params.query) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            &label_replace.query,
            &params.query,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            &params.query,
        )?;
        return Ok(value);
    }
    if let Ok(label_join) = parse_metric_label_join_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            label_join.query.clone(),
        )
        .await?;
        apply_label_join_to_loki_result(&mut value, &label_join);
        return Ok(value);
    }
    if let Some(inner_query) = strip_outer_parenthesized_expression(&params.query) {
        return execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            inner_query,
            &params.query,
        )
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(&params.query) {
        return execute_http_metric_vector_arithmetic_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(&params.query) {
        return execute_http_metric_vector_comparison_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(&params.query) {
        return execute_http_metric_vector_set_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
            &params.query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(&params.query) {
        return execute_http_metric_binary_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(&params.query) {
        return execute_http_metric_binary_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(&params.query) {
        return execute_http_metric_binary_set_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(&params.query) {
        return execute_http_metric_scalar_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(&params.query) {
        return execute_http_metric_scalar_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    let value = if let Ok(query) = parse_metric_query(&params.query) {
        execute_http_metric_query(state, tenant, time_range, params.step, kind, query).await?
    } else {
        execute_http_stream_query(
            state,
            &params.query,
            tenant,
            time_range,
            direction,
            limit,
            interval,
            if matches!(kind, QueryKind::Range) {
                Some(time_range.end_ns)
            } else {
                None
            },
        )
        .await
        .map_err(|error| match error {
            HttpQueryError::Parse(source) => HttpQueryError::LokiParse {
                query: params.query.clone(),
                source,
            },
            error => error,
        })?
    };

    Ok(add_loki_query_stats(value))
}

#[derive(Clone, Debug, PartialEq)]
struct LabelReplaceExpression {
    query: String,
    destination_label: String,
    replacement: String,
    source_label: String,
    pattern: String,
}

struct SortVectorExpression {
    query: String,
    descending: bool,
}

enum LabelReplaceMetricBinaryExpression {
    Arithmetic {
        left: String,
        op: MetricScalarArithmeticOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Comparison {
        left: String,
        op: ComparisonOp,
        bool_modifier: bool,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
    Set {
        left: String,
        op: MetricBinarySetOp,
        matching: Option<MetricVectorMatching>,
        right: String,
    },
}

struct MetricVectorArithmeticExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: MetricScalarArithmeticOp,
    matching: Option<MetricVectorMatching>,
}

struct MetricVectorComparisonExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<MetricVectorMatching>,
}

struct MetricVectorSetExpression {
    metric_query: String,
    vector_query: String,
    vector_on_left: bool,
    op: MetricBinarySetOp,
    matching: Option<MetricVectorMatching>,
}

fn parse_label_replace_expression(query: &str) -> Option<LabelReplaceExpression> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    let [
        inner_query,
        destination_label,
        replacement,
        source_label,
        pattern,
    ] = arguments.as_slice()
    else {
        return None;
    };

    Some(LabelReplaceExpression {
        query: inner_query.to_string(),
        destination_label: parse_logql_string_argument(destination_label)?,
        replacement: parse_logql_string_argument(replacement)?,
        source_label: parse_logql_string_argument(source_label)?,
        pattern: parse_logql_string_argument(pattern)?,
    })
}

fn parse_sort_vector_expression(query: &str) -> Option<SortVectorExpression> {
    for (function_name, descending) in [("sort", false), ("sort_desc", true)] {
        let Some(arguments) = split_logql_function_arguments(query, function_name) else {
            continue;
        };
        let [inner_query] = arguments.as_slice() else {
            return None;
        };
        return Some(SortVectorExpression {
            query: inner_query.to_string(),
            descending,
        });
    }

    None
}

fn strip_outer_parenthesized_expression(query: &str) -> Option<&str> {
    let trimmed = query.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter = None;
    let mut escaped = false;
    for (index, ch) in trimmed.char_indices() {
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote_delimiter = None;
            }
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 && index != trimmed.len() - ch.len_utf8() {
                    return None;
                }
            }
            _ => {}
        }
    }

    if depth == 0 {
        Some(trimmed[1..trimmed.len() - 1].trim())
    } else {
        None
    }
}

fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some((left, operator, right)) = split_top_level_arithmetic_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
                left: left.to_string(),
                op: parse_metric_arithmetic_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_comparison_query(query) {
        let right = right.trim_start();
        let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
            (true, rest.trim_start())
        } else {
            (false, right)
        };
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Comparison {
                left: left.to_string(),
                op: parse_metric_comparison_operator(operator)?,
                bool_modifier,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_set_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, false)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Set {
                left: left.to_string(),
                op: parse_metric_set_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    None
}

fn parse_metric_vector_arithmetic_expression(
    query: &str,
) -> Option<MetricVectorArithmeticExpression> {
    let (left, operator, right) = split_top_level_arithmetic_query(query)?;
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorArithmeticExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        (true, false) => Some(MetricVectorArithmeticExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        _ => None,
    }
}

fn parse_metric_vector_comparison_expression(
    query: &str,
) -> Option<MetricVectorComparisonExpression> {
    let (left, operator, right) = split_top_level_comparison_query(query)?;
    let right = right.trim_start();
    let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right)
    };
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorComparisonExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        (true, false) => Some(MetricVectorComparisonExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        _ => None,
    }
}

fn parse_metric_vector_set_expression(query: &str) -> Option<MetricVectorSetExpression> {
    let (left, operator, right) = split_top_level_set_query(query)?;
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, false)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorSetExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_set_operator(operator)?,
            matching,
        }),
        (true, false) => Some(MetricVectorSetExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_set_operator(operator)?,
            matching,
        }),
        _ => None,
    }
}

fn parse_leading_metric_vector_matching_modifier(
    query: &str,
    allow_group_modifier: bool,
) -> Option<(Option<MetricVectorMatching>, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let (labels, rest) = parse_leading_label_list(rest.trim_start())?;
        let (group, rest) = parse_leading_metric_vector_group_modifier(rest.trim_start())?;
        if group.is_some() && !allow_group_modifier {
            return None;
        }
        let matching = match modifier {
            "on" => MetricVectorMatching::On { labels, group },
            "ignoring" => MetricVectorMatching::Ignoring { labels, group },
            _ => unreachable!("modifier loop only produces known modifiers"),
        };
        return Some((Some(matching), rest));
    }

    Some((None, query))
}

fn parse_leading_label_list(query: &str) -> Option<(Vec<String>, &str)> {
    let inner = query.strip_prefix('(')?;
    let labels_end = inner.find(')')?;
    let labels_text = &inner[..labels_end];
    let labels = if labels_text.trim().is_empty() {
        Vec::new()
    } else {
        labels_text
            .split(',')
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
            .collect()
    };
    Some((labels, &inner[labels_end + 1..]))
}

fn parse_leading_metric_vector_group_modifier(
    query: &str,
) -> Option<(Option<MetricVectorGroupModifier>, &str)> {
    for modifier in ["group_left", "group_right"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let rest = rest.trim_start();
        let (labels, rest) = if rest.starts_with('(') {
            parse_leading_label_list(rest)?
        } else {
            (Vec::new(), rest)
        };
        let group = match modifier {
            "group_left" => MetricVectorGroupModifier::Left(labels),
            "group_right" => MetricVectorGroupModifier::Right(labels),
            _ => unreachable!("modifier loop only produces known group modifiers"),
        };
        return Some((Some(group), rest));
    }

    Some((None, query))
}

fn parse_metric_arithmetic_operator(operator: &str) -> Option<MetricScalarArithmeticOp> {
    match operator {
        "+" => Some(MetricScalarArithmeticOp::Add),
        "-" => Some(MetricScalarArithmeticOp::Subtract),
        "*" => Some(MetricScalarArithmeticOp::Multiply),
        "/" => Some(MetricScalarArithmeticOp::Divide),
        "%" => Some(MetricScalarArithmeticOp::Modulo),
        "^" => Some(MetricScalarArithmeticOp::Power),
        _ => None,
    }
}

fn parse_metric_comparison_operator(operator: &str) -> Option<ComparisonOp> {
    match operator {
        "==" => Some(ComparisonOp::Equal),
        "!=" => Some(ComparisonOp::NotEqual),
        ">" => Some(ComparisonOp::Greater),
        ">=" => Some(ComparisonOp::GreaterEqual),
        "<" => Some(ComparisonOp::Less),
        "<=" => Some(ComparisonOp::LessEqual),
        _ => None,
    }
}

fn parse_metric_set_operator(operator: &str) -> Option<MetricBinarySetOp> {
    match operator {
        "and" => Some(MetricBinarySetOp::And),
        "or" => Some(MetricBinarySetOp::Or),
        "unless" => Some(MetricBinarySetOp::Unless),
        _ => None,
    }
}

fn loki_instant_scalar_or_vector_response(
    timestamp_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let timestamp = unix_ns_string_to_loki_seconds(&timestamp_ns.to_string());
    match result {
        ScalarVectorExpressionResult::Scalar { sample } => loki_success_value(json!({
            "resultType": "scalar",
            "result": [timestamp, sample]
        })),
        ScalarVectorExpressionResult::Vector { sample, metric } => {
            let timestamp = json!(timestamp_ns);
            let result = sample.map_or_else(Vec::new, |sample| {
                vec![json!({
                    "metric": metric,
                    "value": [
                        timestamp,
                        sample
                    ]
                })]
            });
            loki_success_value(json!({
                "resultType": "vector",
                "result": result
            }))
        }
    }
}

fn loki_range_vector_response(
    time_range: TimeRange,
    step_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let (sample, metric) = match result {
        ScalarVectorExpressionResult::Scalar { sample } => (Some(sample), BTreeMap::new()),
        ScalarVectorExpressionResult::Vector { sample, metric } => (sample, metric),
    };
    let result = sample.map_or_else(Vec::new, |sample| {
        vec![json!({
            "metric": metric,
            "values": eval_times(time_range, step_ns)
                .into_iter()
                .map(|timestamp_ns| {
                    json!([
                        unix_ns_string_to_loki_seconds(&timestamp_ns.to_string()),
                        sample
                    ])
                })
                .collect::<Vec<_>>()
        })]
    });
    loki_success_value(json!({
        "resultType": "matrix",
        "result": result
    }))
}

#[derive(Clone)]
enum ScalarVectorExpressionResult {
    Scalar {
        sample: String,
    },
    Vector {
        sample: Option<String>,
        metric: BTreeMap<String, String>,
    },
}

fn scalar_vector_expression_result(query: &str) -> Option<ScalarVectorExpressionResult> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let mut parser = VectorScalarExpressionParser::new(&query);
    let result = parser.parse_result()?;
    if parser.is_finished() {
        Some(result)
    } else {
        None
    }
}

fn scalar_vector_query_is_vector(query: &str) -> bool {
    matches!(
        scalar_vector_expression_result(query),
        Some(ScalarVectorExpressionResult::Vector { .. })
    )
}

fn reject_signed_vector_function_literal(query: &str) -> Result<(), HttpQueryError> {
    scalar_vector_plain_parse_error(query)
        .map(HttpQueryError::LokiPlainParse)
        .map_or(Ok(()), Err)
}

fn scalar_vector_plain_parse_error(query: &str) -> Option<String> {
    signed_vector_function_literal_error(query)
        .or_else(|| unspaced_vector_set_operator_error(query))
}

fn signed_vector_function_literal_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is always on a char boundary");
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += ch.len_utf8();
            continue;
        }
        if query[index..].starts_with("vector(") {
            let mut sign_index = index + "vector(".len();
            while let Some(next) = query[sign_index..].chars().next() {
                if !next.is_whitespace() {
                    break;
                }
                sign_index += next.len_utf8();
            }
            if let Some(sign @ ('+' | '-')) = query[sign_index..].chars().next() {
                let column = query[..sign_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected {sign}, expecting NUMBER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}

fn unspaced_vector_set_operator_error(query: &str) -> Option<String> {
    if !could_be_scalar_vector_expression(query) {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;
    while index < query.len() {
        let ch = query[index..]
            .chars()
            .next()
            .expect("index is always on a char boundary");
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            index += ch.len_utf8();
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += ch.len_utf8();
            continue;
        }
        if ch == ')' {
            let next_index = index + ch.len_utf8();
            if ["and", "or", "unless"]
                .iter()
                .any(|operator| query[next_index..].starts_with(operator))
            {
                let column = query[..next_index].chars().count() + 1;
                return Some(format!(
                    "parse error at line 1, col {column}: syntax error: unexpected IDENTIFIER"
                ));
            }
        }
        index += ch.len_utf8();
    }
    None
}

fn could_be_scalar_vector_expression(query: &str) -> bool {
    let trimmed = query.trim_start();
    let Some(first) = trimmed.chars().next() else {
        return false;
    };
    if first.is_ascii_digit() || matches!(first, '+' | '-' | '.' | '(') {
        return true;
    }
    if first.is_ascii_alphabetic() || first == '_' {
        let ident_len = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(char::len_utf8)
            .sum::<usize>();
        return matches!(
            &trimmed[..ident_len],
            "vector" | "label_replace" | "label_join"
        );
    }
    false
}

fn apply_label_replace_to_loki_result(
    value: &mut Value,
    destination_label: &str,
    replacement: &str,
    source_label: &str,
    pattern: &str,
    query: &str,
) -> Result<(), HttpQueryError> {
    let regex = Regex::new(pattern).map_err(|error| HttpQueryError::LokiParse {
        query: query.to_string(),
        source: ParseError::Syntax {
            message: error.to_string(),
            position: 0,
        },
    })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let source_value = metric
            .get(source_label)
            .and_then(Value::as_str)
            .unwrap_or("");
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(replacement, &mut destination_value);
            metric.insert(destination_label.to_string(), json!(destination_value));
        }
    }
    Ok(())
}

fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let joined = label_join
            .source_labels
            .iter()
            .map(|label| metric.get(label).and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(&label_join.separator);
        metric.insert(label_join.destination_label.clone(), json!(joined));
    }
}

struct VectorScalarExpressionParser<'a> {
    input: &'a str,
    position: usize,
    vector_terms: usize,
}

impl<'a> VectorScalarExpressionParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            position: 0,
            vector_terms: 0,
        }
    }

    fn parse_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        if self.input[self.position..].starts_with("label_replace(") {
            return self.parse_label_replace_result();
        }
        if self.input[self.position..].starts_with("label_join(") {
            return self.parse_label_join_result();
        }

        let left_vector_terms = self.vector_terms;
        let left = self.parse_expression()?;
        let left_contains_vector = self.vector_terms > left_vector_terms;
        if let Some(operator) = self.parse_set_operator() {
            let right_vector_terms = self.vector_terms;
            let _right = self.parse_expression()?;
            let right_contains_vector = self.vector_terms > right_vector_terms;
            if !left_contains_vector || !right_contains_vector {
                return None;
            }
            let sample = match operator {
                ScalarSetOp::And | ScalarSetOp::Or => Some(left.format()),
                ScalarSetOp::Unless => None,
            };
            return Some(ScalarVectorExpressionResult::Vector {
                sample,
                metric: BTreeMap::new(),
            });
        }

        let Some(operator) = self.parse_comparison_operator() else {
            return Some(if self.vector_terms > 0 {
                ScalarVectorExpressionResult::Vector {
                    sample: Some(left.format()),
                    metric: BTreeMap::new(),
                }
            } else {
                ScalarVectorExpressionResult::Scalar {
                    sample: left.format(),
                }
            });
        };

        let bool_modifier = self.consume_keyword("bool");
        let left_vector_terms = self.vector_terms;
        let has_matching_modifier = self.consume_vector_matching_modifier()?;
        let right_vector_terms = self.vector_terms;
        let right = self.parse_expression()?;
        self.validate_vector_matching_modifier(
            has_matching_modifier,
            left_vector_terms,
            right_vector_terms,
        )?;
        let comparison_matches = left.compare(operator, right)?;
        if self.vector_terms == 0 {
            if bool_modifier {
                return None;
            }
            return Some(ScalarVectorExpressionResult::Scalar {
                sample: if comparison_matches { "1" } else { "0" }.to_string(),
            });
        }
        let sample = if bool_modifier {
            Some(if comparison_matches { "1" } else { "0" }.to_string())
        } else if comparison_matches {
            Some(left.format())
        } else {
            None
        };
        Some(ScalarVectorExpressionResult::Vector {
            sample,
            metric: BTreeMap::new(),
        })
    }

    fn parse_label_replace_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_replace");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let replacement = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let source_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let pattern = self.parse_string_literal()?;
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let regex = Regex::new(&pattern).ok()?;
        let source_value = metric.get(&source_label).map_or("", String::as_str);
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(&replacement, &mut destination_value);
            metric.insert(destination_label, destination_value);
        }

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    fn parse_label_join_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_join");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let separator = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let mut source_labels = vec![self.parse_string_literal()?];
        while self.consume(',') {
            source_labels.push(self.parse_string_literal()?);
        }
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let joined = source_labels
            .iter()
            .map(|label| metric.get(label).map_or("", String::as_str))
            .collect::<Vec<_>>()
            .join(&separator);
        metric.insert(destination_label, joined);

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    fn parse_expression(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_product()?;
        loop {
            if self.consume('+') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.add(right)?;
            } else if self.consume('-') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.subtract(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    fn parse_product(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_power()?;
        loop {
            if self.consume('*') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.multiply(right)?;
            } else if self.consume('/') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.divide(right)?;
            } else if self.consume('%') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.modulo(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    fn parse_power(&mut self) -> Option<ScalarSample> {
        let sample = self.parse_primary()?;
        if self.consume('^') {
            let left_vector_terms = self.vector_terms;
            let has_matching_modifier = self.consume_vector_matching_modifier()?;
            let right_vector_terms = self.vector_terms;
            let right = self.parse_power()?;
            self.validate_vector_matching_modifier(
                has_matching_modifier,
                left_vector_terms,
                right_vector_terms,
            )?;
            sample.power(right)
        } else {
            Some(sample)
        }
    }

    fn parse_primary(&mut self) -> Option<ScalarSample> {
        if self.consume('(') {
            let sample = self.parse_expression()?;
            return self.consume(')').then_some(sample);
        }

        self.parse_vector_scalar()
            .or_else(|| self.parse_scalar_literal())
    }

    fn parse_comparison_operator(&mut self) -> Option<ScalarComparisonOp> {
        for (operator, op) in [
            (">=", ScalarComparisonOp::GreaterOrEqual),
            ("<=", ScalarComparisonOp::LessOrEqual),
            ("==", ScalarComparisonOp::Equal),
            ("!=", ScalarComparisonOp::NotEqual),
            (">", ScalarComparisonOp::Greater),
            ("<", ScalarComparisonOp::Less),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    fn parse_set_operator(&mut self) -> Option<ScalarSetOp> {
        for (operator, op) in [
            ("unless", ScalarSetOp::Unless),
            ("and", ScalarSetOp::And),
            ("or", ScalarSetOp::Or),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    fn consume_vector_matching_modifier(&mut self) -> Option<bool> {
        if self.consume_keyword("on") || self.consume_keyword("ignoring") {
            self.consume_label_list()?;
            self.consume_group_modifier()?;
            Some(true)
        } else {
            Some(false)
        }
    }

    fn consume_group_modifier(&mut self) -> Option<()> {
        if !(self.consume_keyword("group_left") || self.consume_keyword("group_right")) {
            return Some(());
        }
        if self.input[self.position..].starts_with('(') {
            self.consume_label_list()?;
        }
        Some(())
    }

    fn consume_label_list(&mut self) -> Option<()> {
        self.consume('(').then_some(())?;
        if self.consume(')') {
            return Some(());
        }

        loop {
            self.consume_label_name()?;
            if self.consume(')') {
                return Some(());
            }
            self.consume(',').then_some(())?;
        }
    }

    fn consume_label_name(&mut self) -> Option<()> {
        let bytes = self.input.as_bytes();
        let first = *bytes.get(self.position)?;
        if !matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_') {
            return None;
        }

        self.position += 1;
        while matches!(
            bytes.get(self.position),
            Some(b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
        ) {
            self.position += 1;
        }
        Some(())
    }

    fn validate_vector_matching_modifier(
        &self,
        has_matching_modifier: bool,
        left_vector_terms: usize,
        right_vector_terms: usize,
    ) -> Option<()> {
        if !has_matching_modifier {
            return Some(());
        }

        let left_contains_vector = left_vector_terms > 0;
        let right_contains_vector = self.vector_terms > right_vector_terms;
        (left_contains_vector && right_contains_vector).then_some(())
    }

    fn parse_vector_scalar(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let scalar = rest.strip_prefix("vector(")?;
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        self.position += "vector(".len() + scalar_end + 1;
        let sample = parse_scalar_sample(scalar_text)?;
        self.vector_terms += 1;
        Some(sample)
    }

    fn parse_scalar_literal(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let literal_len = scalar_literal_len(rest)?;
        let sample = parse_scalar_sample(&rest[..literal_len])?;
        self.position += literal_len;
        Some(sample)
    }

    fn parse_string_literal(&mut self) -> Option<String> {
        self.consume('"').then_some(())?;
        let mut value = String::new();
        while self.position < self.input.len() {
            let ch = self.input[self.position..].chars().next()?;
            self.position += ch.len_utf8();
            match ch {
                '"' => return Some(value),
                '\\' => {
                    let escaped = self.input[self.position..].chars().next()?;
                    self.position += escaped.len_utf8();
                    value.push(match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        other => other,
                    });
                }
                other => value.push(other),
            }
        }
        None
    }

    fn consume(&mut self, operator: char) -> bool {
        if self.input[self.position..].starts_with(operator) {
            self.position += operator.len_utf8();
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if self.input[self.position..].starts_with(keyword) {
            self.position += keyword.len();
            true
        } else {
            false
        }
    }

    fn is_finished(&self) -> bool {
        self.position == self.input.len()
    }
}

#[derive(Clone, Copy)]
enum ScalarSetOp {
    And,
    Or,
    Unless,
}

fn scalar_literal_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut position = 0;
    if matches!(bytes.get(position), Some(b'+') | Some(b'-')) {
        position += 1;
    }

    let whole_start = position;
    while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
        position += 1;
    }
    let whole_digits = position > whole_start;

    let mut fractional_digits = false;
    if matches!(bytes.get(position), Some(b'.')) {
        position += 1;
        let fractional_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        fractional_digits = position > fractional_start;
    }

    if !whole_digits && !fractional_digits {
        return None;
    }

    if matches!(bytes.get(position), Some(b'e') | Some(b'E')) {
        position += 1;
        if matches!(bytes.get(position), Some(b'+') | Some(b'-')) {
            position += 1;
        }
        let exponent_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        if position == exponent_start {
            return None;
        }
    }

    Some(position)
}

#[derive(Clone, Copy)]
enum ScalarComparisonOp {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}

#[derive(Clone, Copy)]
struct ScalarSample {
    numerator: i128,
    denominator: u128,
}

impl ScalarSample {
    fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self {
                numerator: 0,
                denominator: 1,
            };
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).unwrap_or(i128::MAX),
            denominator: denominator / divisor,
        }
    }

    fn add(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_add(right?)?, denominator))
    }

    fn subtract(self, other: Self) -> Option<Self> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?);
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?);
        let denominator = self.denominator.checked_mul(other.denominator)?;
        Some(Self::new(left?.checked_sub(right?)?, denominator))
    }

    fn multiply(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.numerator.checked_mul(other.numerator)?,
            self.denominator.checked_mul(other.denominator)?,
        ))
    }

    fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
    }

    fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    fn compare(self, operator: ScalarComparisonOp, other: Self) -> Option<bool> {
        let left = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let right = other
            .numerator
            .checked_mul(i128::try_from(self.denominator).ok()?)?;
        Some(match operator {
            ScalarComparisonOp::Equal => left == right,
            ScalarComparisonOp::NotEqual => left != right,
            ScalarComparisonOp::Greater => left > right,
            ScalarComparisonOp::GreaterOrEqual => left >= right,
            ScalarComparisonOp::Less => left < right,
            ScalarComparisonOp::LessOrEqual => left <= right,
        })
    }

    fn to_f64(self) -> Option<f64> {
        let value = self.numerator as f64 / self.denominator as f64;
        value.is_finite().then_some(value)
    }

    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE as f64).round();
        if scaled < i128::MIN as f64 || scaled > i128::MAX as f64 {
            return None;
        }
        Some(Self::new(scaled as i128, METRIC_DECIMAL_SCALE))
    }

    fn format(self) -> String {
        let negative = self.numerator < 0;
        let numerator = self.numerator.unsigned_abs();
        let whole = numerator / self.denominator;
        let mut remainder = numerator % self.denominator;
        let sign = if negative { "-" } else { "" };
        if remainder == 0 {
            return format!("{sign}{whole}");
        }

        let mut decimals = String::new();
        while remainder != 0 && decimals.len() < 9 {
            remainder *= 10;
            let digit =
                u8::try_from(remainder / self.denominator).expect("decimal digit is less than 10");
            decimals.push(char::from(b'0' + digit));
            remainder %= self.denominator;
        }
        while decimals.ends_with('0') {
            decimals.pop();
        }
        format!("{sign}{whole}.{decimals}")
    }

    fn format_fixed_six(self) -> String {
        format!("{:.6}", self.numerator as f64 / self.denominator as f64)
    }
}

fn parse_scalar_sample(value: &str) -> Option<ScalarSample> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(ScalarSample::new(numerator, denominator))
}

fn gcd_signed(left: i128, right: u128) -> u128 {
    let mut left = left.unsigned_abs();
    let mut right = right;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn validate_query_range_limit(
    state: &QuerierState,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    let Some(max_query_range_ns) = state.max_query_range_ns else {
        return Ok(());
    };
    let query_range_ns = time_range.end_ns.checked_sub(time_range.start_ns).ok_or(
        HttpQueryError::QueryRangeTooLarge {
            range_ns: i64::MAX,
            max_range_ns: max_query_range_ns,
        },
    )?;
    if query_range_ns > max_query_range_ns {
        return Err(HttpQueryError::QueryRangeTooLarge {
            range_ns: query_range_ns,
            max_range_ns: max_query_range_ns,
        });
    }
    Ok(())
}

fn validate_loki_volume_query_range_limit(time_range: TimeRange) -> Result<(), HttpQueryError> {
    let query_range_ns = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .ok_or_else(|| HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(i64::MAX),
        })?;
    if query_range_ns > LOKI_VOLUME_MAX_QUERY_RANGE_NS {
        return Err(HttpQueryError::LokiQueryRangeTooLarge {
            query_length: format_loki_query_length(query_range_ns),
        });
    }
    Ok(())
}

fn validate_loki_range_query_range_limit(
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if matches!(kind, QueryKind::Range) {
        validate_loki_volume_query_range_limit(time_range)?;
    }
    Ok(())
}

fn validate_loki_query_range_resolution(
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if !matches!(kind, QueryKind::Range) {
        return Ok(());
    }
    let step_ns = params
        .step
        .unwrap_or_else(|| default_metric_range_step(time_range));
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    let query_range_ns = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .ok_or(HttpQueryError::QueryResolutionTooHigh)?;
    if query_range_ns / step_ns > LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS {
        return Err(HttpQueryError::QueryResolutionTooHigh);
    }
    Ok(())
}

fn format_loki_query_length(range_ns: i64) -> String {
    let total_seconds = range_ns.max(0) / 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;

    format!("{hours}h{minutes}m{seconds}s")
}

fn validate_query_length_limit(state: &QuerierState, query: &str) -> Result<(), HttpQueryError> {
    let Some(max_query_length) = state.max_query_length else {
        return Ok(());
    };
    let query_length = query.len();
    if query_length > max_query_length {
        return Err(HttpQueryError::QueryLengthTooLarge {
            query_length,
            max_query_length,
        });
    }
    Ok(())
}

async fn execute_http_metric_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: MetricQuery,
) -> Result<Value, HttpQueryError> {
    if metric_query_uses_approx_topk(&query) {
        return Err(HttpQueryError::ApproxTopKDisabled);
    }
    if metric_query_uses_count_values(&query) {
        return Err(HttpQueryError::CountValuesQuery);
    }
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                time_range,
                step_ns,
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }
    let response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            eval_range,
            1,
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

async fn execute_http_metric_expression_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: &str,
    full_query: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(sort) = parse_sort_vector_expression(query) {
        return Box::pin(execute_http_sort_vector_expression(
            state, tenant, time_range, step, kind, sort, full_query,
        ))
        .await;
    }
    if split_logql_function_arguments(query, "label_join").is_none()
        && let Some(result) = scalar_vector_expression_result(query)
    {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => {
                let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
                if step_ns <= 0 {
                    return Err(HttpQueryError::InvalidStep);
                }
                loki_range_vector_response(time_range, step_ns, result)
            }
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            label_replace.query.clone(),
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(label_replace) = parse_label_replace_expression(query) {
        let mut value = Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            full_query,
        ))
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(inner_query) = strip_outer_parenthesized_expression(query) {
        return Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            inner_query,
            full_query,
        ))
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(query) {
        return execute_http_metric_vector_arithmetic_expression(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(query) {
        return execute_http_metric_vector_comparison_expression(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(query) {
        return execute_http_metric_vector_set_expression(
            state, tenant, time_range, step, kind, set, full_query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(query) {
        return execute_http_metric_binary_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(query) {
        return execute_http_metric_binary_comparison_query(
            state, tenant, time_range, step, kind, comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(query) {
        return execute_http_metric_binary_set_query(state, tenant, time_range, step, kind, set)
            .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(query) {
        return execute_http_metric_scalar_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(query) {
        return execute_http_metric_scalar_comparison_query(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    let query = parse_metric_query(query).map_err(|source| HttpQueryError::LokiParse {
        query: full_query.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}

async fn execute_http_label_replace_metric_binary_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    binary: LabelReplaceMetricBinaryExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    match binary {
        LabelReplaceMetricBinaryExpression::Arithmetic {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_arithmetic_to_loki_result(&mut left, &right, op, matching.as_ref());
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Comparison {
            left,
            op,
            bool_modifier,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_comparison_to_loki_result(
                &mut left,
                &right,
                op,
                bool_modifier,
                matching.as_ref(),
            );
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Set {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_set_to_loki_result(&mut left, &right, op, matching.as_ref());
            Ok(left)
        }
    }
}

async fn execute_http_metric_binary_operand(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    operand: &str,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(label_replace) = parse_label_replace_expression(operand) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            query_text,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            query_text,
        )?;
        return Ok(value);
    }
    if scalar_vector_query_is_vector(operand) {
        return execute_http_scalar_vector_expression_result(
            operand, time_range, step, kind, query_text,
        );
    }

    let query = parse_metric_query(operand).map_err(|source| HttpQueryError::LokiParse {
        query: query_text.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}

async fn execute_http_sort_vector_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    sort: SortVectorExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let mut value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &sort.query,
        query_text,
    ))
    .await?;
    sort_loki_vector_result(&mut value, sort.descending);
    Ok(value)
}

async fn execute_http_metric_vector_arithmetic_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricVectorArithmeticExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &arithmetic.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &arithmetic.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if arithmetic.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &metric_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &vector_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        Ok(value)
    }
}

async fn execute_http_metric_vector_comparison_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricVectorComparisonExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &comparison.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &comparison.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if comparison.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &metric_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &vector_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        Ok(value)
    }
}

async fn execute_http_metric_vector_set_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricVectorSetExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &set.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &set.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if set.vector_on_left {
        let mut value = vector_value;
        if matches!(kind, QueryKind::Instant) {
            normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        }
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &metric_value,
            set.op,
            set.matching.as_ref(),
        );
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &vector_value,
            set.op,
            set.matching.as_ref(),
        );
        Ok(value)
    }
}

fn normalize_loki_vector_sample_timestamps_to_seconds(value: &mut Value) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(sample) = series.get_mut("value").and_then(Value::as_array_mut) else {
            continue;
        };
        let Some(timestamp) = sample.get_mut(0) else {
            continue;
        };
        *timestamp = match timestamp {
            Value::Number(number) => {
                let seconds = unix_ns_string_to_loki_seconds(&number.to_string());
                json!(seconds)
            }
            Value::String(text) => json!(unix_ns_string_to_loki_seconds(text)),
            _ => continue,
        };
    }
}

fn execute_http_scalar_vector_expression_result(
    query: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let vector_result =
        scalar_vector_expression_result(query).ok_or_else(|| HttpQueryError::LokiParse {
            query: query_text.to_string(),
            source: ParseError::Syntax {
                message: "expected vector expression".to_string(),
                position: 0,
            },
        })?;
    let value = match kind {
        QueryKind::Instant => {
            loki_instant_scalar_or_vector_response(time_range.end_ns, vector_result)
        }
        QueryKind::Range => {
            let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
            if step_ns <= 0 {
                return Err(HttpQueryError::InvalidStep);
            }
            loki_range_vector_response(time_range, step_ns, vector_result)
        }
    };
    Ok(add_loki_query_stats(value))
}

fn retain_metric_binary_on_labels(value: &mut Value, matching: Option<&MetricVectorMatching>) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        metric.retain(|label, _| labels.contains(label));
    }
}

fn sort_loki_vector_result(value: &mut Value, descending: bool) {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector") {
        return;
    }
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    results.sort_by(|left, right| {
        let ordering = match (
            loki_vector_sample_value(left),
            loki_vector_sample_value(right),
        ) {
            (Some(left), Some(right)) => left.cmp_value(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

fn loki_vector_sample_value(sample: &Value) -> Option<MetricValue> {
    sample
        .pointer("/value/1")
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
}

fn metric_query_uses_approx_topk(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
}

fn metric_query_uses_count_values(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::CountValues(_)))
}

async fn execute_http_metric_binary_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricBinaryArithmetic,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, arithmetic.right).await?;
    apply_metric_binary_arithmetic_to_loki_result(
        &mut left,
        &right,
        arithmetic.op,
        arithmetic.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricBinaryComparison,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        comparison.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, comparison.right).await?;
    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        comparison.op,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_set_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricBinarySet,
) -> Result<Value, HttpQueryError> {
    let mut left =
        execute_http_metric_query(state, tenant, time_range, step, kind, set.left.clone()).await?;
    let right = execute_http_metric_query(state, tenant, time_range, step, kind, set.right).await?;
    apply_metric_binary_set_to_loki_result(&mut left, &right, set.op, set.matching.as_ref());
    Ok(left)
}

async fn execute_http_metric_scalar_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricScalarComparison,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = comparison.query.clone();
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let mut response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                time_range,
                step_ns,
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }

    let mut response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            eval_range,
            1,
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

async fn execute_http_metric_scalar_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricScalarArithmetic,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = arithmetic.query.clone();
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let mut response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                time_range,
                step_ns,
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }

    let mut response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            eval_range,
            1,
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_arithmetic_group_right_to_results(
            left_results,
            right_results,
            op,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_arithmetic_to_series(&mut left_results[index], right_series, op) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn apply_metric_binary_arithmetic_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

fn apply_metric_binary_arithmetic_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample(&mut left_values[index], right_sample, op) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample(left_sample, right_sample, op)
}

fn apply_metric_binary_arithmetic_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample_operands(output_sample, left_sample, &right_sample, op)
}

fn matching_metric_binary_sample<'a>(
    left_sample: &Value,
    right_values: &'a [Value],
) -> Option<&'a Value> {
    right_values
        .iter()
        .find(|right_sample| metric_binary_sample_timestamps_match(left_sample, right_sample))
}

fn metric_binary_sample_timestamps_match(left_sample: &Value, right_sample: &Value) -> bool {
    match (
        metric_binary_sample_timestamp_ns_candidates(left_sample),
        metric_binary_sample_timestamp_ns_candidates(right_sample),
    ) {
        (Some(left), Some(right)) => left
            .iter()
            .any(|left_timestamp| right.contains(left_timestamp)),
        (None, None) => {
            left_sample.as_array().and_then(|sample| sample.first())
                == right_sample.as_array().and_then(|sample| sample.first())
        }
        _ => false,
    }
}

fn metric_binary_sample_timestamp_ns_candidates(sample: &Value) -> Option<Vec<i64>> {
    let timestamp = sample.as_array()?.first()?;
    if let Some(timestamp) = timestamp.as_i64() {
        return Some(metric_binary_integer_timestamp_ns_candidates(timestamp));
    }
    if let Some(timestamp) = timestamp.as_u64() {
        return i64::try_from(timestamp)
            .ok()
            .map(metric_binary_integer_timestamp_ns_candidates);
    }
    if let Some(timestamp) = timestamp.as_f64() {
        let timestamp = timestamp * 1_000_000_000.0;
        if timestamp.is_finite() && timestamp >= i64::MIN as f64 && timestamp <= i64::MAX as f64 {
            return Some(vec![timestamp.round() as i64]);
        }
    }
    if let Some(timestamp) = timestamp.as_str() {
        let mut candidates = Vec::new();
        if let Some(timestamp) = parse_decimal_seconds_timestamp(timestamp) {
            candidates.push(timestamp);
        }
        if let Ok(timestamp) = timestamp.parse::<i64>() {
            candidates.extend(metric_binary_integer_timestamp_ns_candidates(timestamp));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if !candidates.is_empty() {
            return Some(candidates);
        }
    }
    None
}

fn metric_binary_integer_timestamp_ns_candidates(timestamp: i64) -> Vec<i64> {
    let mut candidates = vec![timestamp];
    if let Some(seconds_timestamp) = timestamp.checked_mul(1_000_000_000) {
        candidates.push(seconds_timestamp);
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn apply_metric_binary_arithmetic_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_arithmetic_to_sample_operands(left_sample, &original_left, right_sample, op)
}

fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(left_value, op, right_value, false) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_comparison_group_right_to_results(
            left_results,
            right_results,
            op,
            bool_modifier,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_comparison_to_series(
            &mut left_results[index],
            right_series,
            op,
            bool_modifier,
        ) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn apply_metric_binary_comparison_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
            bool_modifier,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

fn apply_metric_binary_comparison_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample(
                &mut left_values[index],
                right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample(left_sample, right_sample, op, bool_modifier)
}

fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample_operands(
        output_sample,
        left_sample,
        &right_sample,
        op,
        bool_modifier,
    )
}

fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        &original_left,
        right_sample,
        op,
        bool_modifier,
    )
}

fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(left_value, op, right_value, false);
    if bool_modifier {
        if let Some(value) = output_values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        if matches
            && let (Some(output), Some(left)) = (output_values.get_mut(1), left_values.get(1)) {
                *output = left.clone();
            }
        matches
    }
}

fn apply_metric_binary_set_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricBinarySetOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        if matches!(op, MetricBinarySetOp::And) {
            left_results.clear();
        }
        return;
    };

    if matches!(op, MetricBinarySetOp::Or) {
        let left_label_sets = left_results
            .iter()
            .filter_map(metric_series_labels)
            .map(|labels| metric_vector_matching_key(&labels, matching))
            .collect::<BTreeSet<_>>();
        for right_series in right_results {
            let Some(right_labels) = metric_series_labels(right_series) else {
                continue;
            };
            let right_key = metric_vector_matching_key(&right_labels, matching);
            if !left_label_sets.contains(&right_key) {
                left_results.push(right_series.clone());
            }
        }
        sort_loki_metric_results_by_labels(left_results);
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let right_series = right_results.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == left_key)
        });
        let keep = match (op, right_series) {
            (MetricBinarySetOp::And, Some(right_series)) => {
                apply_metric_binary_set_to_series(&mut left_results[index], right_series, op)
            }
            (MetricBinarySetOp::And, None) => false,
            (MetricBinarySetOp::Unless, Some(right_series)) => {
                apply_metric_binary_set_to_series(&mut left_results[index], right_series, op)
            }
            (MetricBinarySetOp::Unless, None) => true,
            (MetricBinarySetOp::Or, _) => true,
        };
        if keep {
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn metric_series_labels(series: &Value) -> Option<Labels> {
    series.get("metric").and_then(json_object_to_labels)
}

fn sort_loki_metric_results_by_labels(results: &mut [Value]) {
    results.sort_by_key(metric_series_labels);
}

fn metric_vector_matching_key(labels: &Labels, matching: Option<&MetricVectorMatching>) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

fn metric_vector_group_modifier(
    matching: Option<&MetricVectorMatching>,
) -> Option<&MetricVectorGroupModifier> {
    match matching {
        Some(MetricVectorMatching::On { group, .. })
        | Some(MetricVectorMatching::Ignoring { group, .. }) => group.as_ref(),
        None => None,
    }
}

fn include_metric_group_labels(
    output_series: &mut Value,
    source_series: &Value,
    labels: &[String],
) {
    if labels.is_empty() {
        return;
    }
    let Some(source_metric) = source_series.get("metric").and_then(Value::as_object) else {
        return;
    };
    let Some(output_metric) = output_series
        .get_mut("metric")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for label in labels {
        output_metric.remove(label);
        if let Some(value) = source_metric.get(label).and_then(Value::as_str) {
            output_metric.insert(label.clone(), json!(value));
        }
    }
}

fn apply_metric_binary_set_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricBinarySetOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let right_values = right_series.get("values").and_then(Value::as_array);
        let mut index = 0;
        while index < left_values.len() {
            let matched = right_values
                .and_then(|right_values| {
                    matching_metric_binary_sample(&left_values[index], right_values)
                })
                .is_some();
            if metric_binary_set_keeps_sample(op, matched) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    let matched = right_series
        .get("value")
        .is_some_and(|right_sample| metric_samples_share_timestamp(left_sample, right_sample));
    metric_binary_set_keeps_sample(op, matched)
}

fn metric_binary_set_keeps_sample(op: MetricBinarySetOp, matched: bool) -> bool {
    match op {
        MetricBinarySetOp::And => matched,
        MetricBinarySetOp::Or => true,
        MetricBinarySetOp::Unless => !matched,
    }
}

fn metric_samples_share_timestamp(left_sample: &Value, right_sample: &Value) -> bool {
    metric_binary_sample_timestamps_match(left_sample, right_sample)
}

fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&arithmetic.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_arithmetic_to_series(
            &mut results[index],
            arithmetic.op,
            scalar,
            arithmetic.scalar_on_left,
        ) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_arithmetic_to_sample(
                &mut values[index],
                op,
                scalar,
                scalar_on_left,
            ) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_arithmetic_to_sample(sample, op, scalar, scalar_on_left)
}

fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(sample_value, op, scalar, scalar_on_left)
    else {
        return false;
    };
    if let Some(value) = values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

fn metric_scalar_arithmetic_value(
    sample: MetricValue,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> Option<MetricValue> {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}

fn apply_metric_scalar_comparison_to_loki_result(
    value: &mut Value,
    comparison: &MetricScalarComparison,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&comparison.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_comparison_to_series(&mut results[index], comparison, scalar) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

fn apply_metric_scalar_comparison_to_series(
    series: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_comparison_to_sample(&mut values[index], comparison, scalar) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_comparison_to_sample(sample, comparison, scalar)
}

fn apply_metric_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(
        sample_value,
        comparison.op,
        scalar,
        comparison.scalar_on_left,
    );
    if comparison.bool_modifier {
        if let Some(value) = values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        matches
    }
}

fn metric_scalar_comparison_matches(
    sample: MetricValue,
    op: ComparisonOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    let ordering = left.cmp_value(right);
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}

fn default_metric_range_step(time_range: TimeRange) -> i64 {
    time_range.end_ns.saturating_sub(time_range.start_ns).max(1)
}

async fn execute_http_metric_range_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    time_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        return execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            time_range,
            step_ns,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from);
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        return execute_metric_query_range_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            time_range,
            step_ns,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from);
    }
    execute_metric_query_range_with_deletes(
        &state.root,
        plan,
        query,
        &state.label_index,
        time_range,
        step_ns,
        delete_filters,
    )
    .await
    .map_err(HttpQueryError::from)
}

async fn execute_http_metric_instant_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    let response = if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    } else if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        execute_metric_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    } else {
        execute_metric_query_with_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    };

    Ok(loki_vector_response_from_matrix(response))
}

async fn execute_http_stream_query(
    state: &QuerierState,
    query: &str,
    tenant: &str,
    time_range: TimeRange,
    direction: LokiDirection,
    limit: Option<usize>,
    interval: Option<i64>,
    end_exclusive: Option<i64>,
) -> Result<Value, HttpQueryError> {
    validate_loki_interval(interval)?;
    let query = parse_query(query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            &plan,
            &state.label_index,
            &records,
            &frontier,
            &delete_filters,
            StreamScanOptions::from_stream_options(direction, limit, interval, end_exclusive),
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response =
            apply_loki_stream_options(scan.value, direction, limit, interval, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_blocks_with_hot_tail(
            response,
            &scan.scanned_blocks,
            &plan,
            &records,
            &frontier,
        ));
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        let response = execute_stream_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            &plan,
            &state.label_index,
            &records,
            &frontier,
            &delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response =
            apply_loki_stream_options(response, direction, limit, interval, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_plan_with_hot_tail(
            response, &plan, &records, &frontier,
        ));
    }
    let response =
        execute_stream_query_with_deletes(&state.root, &plan, &state.label_index, &delete_filters)
            .await
            .map_err(HttpQueryError::from)?;
    let response = apply_loki_stream_options(response, direction, limit, interval, end_exclusive);
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
}

fn validate_loki_interval(interval: Option<i64>) -> Result<(), HttpQueryError> {
    if let Some(interval_ns) = interval
        && interval_ns < 0
    {
        return Err(HttpQueryError::InvalidInterval);
    }
    Ok(())
}

async fn execute_index_stats_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let time_range = TimeRange::new(start, end).map_err(HttpQueryError::from)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let entries = count_index_stats_entries(&state, &plan).await?;
    let bytes = plan
        .blocks
        .iter()
        .map(|block| block.size_bytes)
        .try_fold(0_u64, u64::checked_add)
        .unwrap_or(u64::MAX);
    let streams = plan
        .blocks
        .iter()
        .flat_map(|block| block.fingerprints.iter())
        .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
        .copied()
        .collect::<BTreeSet<_>>()
        .len();

    Ok(json!({
        "streams": u64::try_from(streams).unwrap_or(u64::MAX),
        "chunks": u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX),
        "entries": entries,
        "bytes": bytes,
    }))
}

async fn count_index_stats_entries(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<u64, HttpQueryError> {
    let mut entries = 0_u64;
    for block in &plan.blocks {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
        };
        let matching_entries = rows
            .into_iter()
            .filter(|row| {
                plan.fingerprints.contains(&row.series_fingerprint)
                    && plan.time_range.start_ns <= row.timestamp_ns
                    && row.timestamp_ns <= plan.time_range.end_ns
            })
            .count();
        entries = entries.saturating_add(u64::try_from(matching_entries).unwrap_or(u64::MAX));
    }
    Ok(entries)
}

async fn execute_patterns_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_patterns_params(raw_query)?;
    if params.step <= 0 {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "step",
            value: params.step.to_string(),
        });
    }

    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
        };
        for row in rows {
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns >= plan.time_range.end_ns
            {
                continue;
            }
            let labels = state
                .label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or(QueryError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            if !plan
                .query
                .matches_with_fields(labels, &row.line, &row.structured_metadata)
            {
                continue;
            }
            let bucket = sample_time_bucket(row.timestamp_ns, params.start, params.step);
            *patterns
                .entry(log_line_pattern(&row.line))
                .or_default()
                .entry(bucket)
                .or_default() += 1;
        }
    }

    let data = patterns
        .into_iter()
        .map(|(pattern, samples)| {
            json!({
                "pattern": pattern,
                "samples": samples
                    .into_iter()
                    .map(|(timestamp_ns, count)| json!([timestamp_ns / 1_000_000_000, count]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    Ok(loki_success_value(data))
}

fn log_line_pattern(line: &str) -> String {
    // Crabka services (and every JSON-emitting collector) log compact objects
    // like `{"timestamp":"…","severity":"INFO","message":"connection opened"}`.
    // Whitespace tokenization mangles those — the quoted values contain spaces
    // and the `:` separator is invisible to the logfmt `key=value` splitter — so
    // every distinct timestamp became its own pattern. Templatize JSON lines
    // structurally instead, keeping keys and constant values while collapsing
    // variable values (timestamps, ids, numbers) to `<_>`.
    if let Some(pattern) = json_log_pattern(line) {
        return pattern;
    }
    line.split_whitespace()
        .map(log_pattern_token)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Templatize a single-object JSON log line, returning `None` for anything that
/// is not a JSON object (so the caller falls back to whitespace/logfmt mining).
fn json_log_pattern(line: &str) -> Option<String> {
    // `from_str` already rejects non-objects and non-JSON, so there is no
    // cheap pre-check guard here: a leading-`{` fast path would be a pure
    // performance optimization with no behavior of its own to test.
    let Value::Object(map) = serde_json::from_str::<Value>(line.trim()).ok()? else {
        return None;
    };
    let templatized = Value::Object(
        map.iter()
            .map(|(key, value)| (key.clone(), json_value_pattern(value)))
            .collect(),
    );
    serde_json::to_string(&templatized).ok()
}

/// Replace variable JSON leaf values with the `<_>` placeholder while preserving
/// object/array structure and constant (low-entropy) values.
fn json_value_pattern(value: &Value) -> Value {
    match value {
        // Numbers are always high-cardinality dimensions (offsets, durations,
        // counts), so collapse them; booleans and null are constants worth
        // keeping as discriminators.
        Value::Number(_) => Value::String("<_>".to_string()),
        Value::Null | Value::Bool(_) => value.clone(),
        Value::String(text) => Value::String(templatize_text(text)),
        Value::Array(items) => Value::Array(items.iter().map(json_value_pattern).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), json_value_pattern(value)))
                .collect(),
        ),
    }
}

/// Templatize the variable whitespace-delimited tokens inside a free-text value
/// (e.g. an embedded request id or timestamp in a `message` field), leaving the
/// constant words intact so distinct messages stay distinct patterns.
fn templatize_text(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            if pattern_value_is_variable(token) {
                "<_>".to_string()
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn log_pattern_token(token: &str) -> String {
    let Some((key, value)) = token.split_once('=') else {
        return if pattern_value_is_variable(token) {
            "<_>".to_string()
        } else {
            token.to_string()
        };
    };
    if key.is_empty() || value.is_empty() {
        return token.to_string();
    }
    if pattern_value_is_variable(value) {
        format!("{key}=<_>")
    } else {
        token.to_string()
    }
}

/// Whether a token looks like variable (per-line-varying) data that should be
/// templatized to `<_>` rather than kept as a constant part of the pattern.
///
/// Timestamps and other numeric-leading values are caught by the leading-digit
/// and float checks; identifiers that begin with a letter (UUIDs, trace/span
/// hashes, opaque high-entropy tokens) need the explicit shape checks so they do
/// not each become their own pattern.
fn pattern_value_is_variable(value: &str) -> bool {
    let value = value.trim_matches('"');
    if value.is_empty() {
        return false;
    }
    value.as_bytes().first().is_some_and(u8::is_ascii_digit)
        || value.parse::<f64>().is_ok()
        || is_uuid(value)
        || is_hex_id(value)
        || is_high_entropy_id(value)
}

/// Canonical `8-4-4-4-12` hex UUID, regardless of leading character.
fn is_uuid(value: &str) -> bool {
    let mut groups = value.split('-');
    let shaped = [8usize, 4, 4, 4, 12].into_iter().all(|len| {
        groups
            .next()
            .is_some_and(|group| group.len() == len && group.bytes().all(|b| b.is_ascii_hexdigit()))
    });
    shaped && groups.next().is_none()
}

/// A long pure-hex string (trace/span id, digest, dash-less UUID). The length
/// floor keeps short hex-looking words (`face`, `cafe`) from being templatized.
fn is_hex_id(value: &str) -> bool {
    value.len() >= 16 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A long opaque identifier (session token, base62 id, api key): purely
/// alphanumeric with both letters and digits. Requiring a digit avoids
/// templatizing long lowercase or mixed-case words, and the punctuation
/// exclusion keeps module paths and file locations intact.
fn is_high_entropy_id(value: &str) -> bool {
    value.len() >= 16
        && value.bytes().all(|b| b.is_ascii_alphanumeric())
        && value.bytes().any(|b| b.is_ascii_digit())
        && value.bytes().any(|b| b.is_ascii_alphabetic())
}

async fn execute_detected_fields_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|(label, stats)| {
            let ty = stats.ty.as_loki_str();
            let cardinality = stats.values.len();
            let parsers = stats.parsers_json();
            json!({
                "label": label,
                "type": ty,
                "cardinality": cardinality,
                "parsers": parsers,
            })
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "fields": fields,
        "limit": limit,
    }))
}

async fn execute_detected_labels_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_labels_params(raw_query)?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    if let Some(query) = &params.query {
        validate_query_length_limit(state, query)?;
    }
    let series_params = SeriesParams {
        matchers: params.query.into_iter().collect(),
        start: Some(params.start),
        end: Some(params.end),
        since: None,
    };
    let label_sets = series_data(state, headers, &series_params).await?;
    let mut values_by_label = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in label_sets {
        for (name, value) in labels {
            values_by_label.entry(name).or_default().insert(value);
        }
    }
    if values_by_label.is_empty() {
        return Ok(json!({}));
    }

    let detected_labels = values_by_label
        .into_iter()
        .take(params.limit)
        .map(|(label, values)| {
            json!({
                "label": label,
                "cardinality": values.len(),
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "detectedLabels": detected_labels,
    }))
}

async fn execute_detected_field_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let values = fields
        .get(name)
        .map(|stats| stats.values.iter().take(limit).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if values.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "values": values,
        "limit": limit,
    }))
}

async fn collect_detected_fields(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
        };
        for row in rows {
            if scanned_lines >= params.line_limit {
                break;
            }
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns > plan.time_range.end_ns
            {
                continue;
            }
            let labels = state
                .label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or(QueryError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            if !plan
                .query
                .matches_with_fields(labels, &row.line, &row.structured_metadata)
            {
                continue;
            }
            scanned_lines += 1;
            detect_detected_level_field(&mut fields, labels, &row.line);
            detect_structured_metadata_fields(&mut fields, &row.structured_metadata);
            detect_json_fields(&mut fields, &row.line);
            detect_logfmt_fields(&mut fields, &row.line);
        }
    }

    Ok(fields)
}

fn detect_detected_level_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    labels: &Labels,
    line: &str,
) {
    if !should_insert_unknown_detected_level(labels) {
        return;
    }
    let level = detect_log_level(line).unwrap_or("unknown");
    add_generated_detected_field(
        fields,
        "detected_level",
        level.to_string(),
        DetectedFieldType::String,
    );
}

fn detect_structured_metadata_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    metadata: &Labels,
) {
    for (name, value) in metadata {
        add_detected_field(
            fields,
            name,
            value.clone(),
            field_type_from_str(value),
            "structured_metadata",
        );
    }
}

fn detect_json_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return;
    };
    for (name, json_value) in object {
        let Some(value) = detected_json_value_string(&json_value) else {
            continue;
        };
        add_detected_field(
            fields,
            &name,
            value,
            field_type_from_json(&json_value),
            "json",
        );
    }
}

fn detect_logfmt_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    for (name, value) in parse_logfmt_pairs(line) {
        let ty = field_type_from_str(&value);
        add_detected_field(fields, &name, value, ty, "logfmt");
    }
}

fn add_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
    parser: &'static str,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add(ty, value.clone(), parser))
        .or_insert_with(|| DetectedFieldStats::new(ty, value, parser));
}

fn add_generated_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add_generated(ty, value.clone()))
        .or_insert_with(|| DetectedFieldStats::new_generated(ty, value));
}

fn detected_json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn field_type_from_json(value: &Value) -> DetectedFieldType {
    match value {
        Value::Bool(_) => DetectedFieldType::Boolean,
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                DetectedFieldType::Int
            } else {
                DetectedFieldType::Float
            }
        }
        Value::String(value) => field_type_from_str(value),
        Value::Null | Value::Array(_) | Value::Object(_) => DetectedFieldType::String,
    }
}

fn field_type_from_str(value: &str) -> DetectedFieldType {
    let normalized = value.to_ascii_lowercase();
    if matches!(normalized.as_str(), "true" | "false") {
        return DetectedFieldType::Boolean;
    }
    if value.parse::<i64>().is_ok() {
        return DetectedFieldType::Int;
    }
    if value.parse::<f64>().is_ok() {
        return DetectedFieldType::Float;
    }
    if is_prometheus_duration_literal(value) {
        return DetectedFieldType::Duration;
    }
    if is_bytes_literal(value) {
        return DetectedFieldType::Bytes;
    }
    DetectedFieldType::String
}

fn is_prometheus_duration_literal(value: &str) -> bool {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut seen_units = 0_u16;

    while pos < value.len() {
        let value_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == value_start {
            return false;
        }

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let Some((unit_order, unit_bit)) = detected_duration_unit(&value[unit_start..pos]) else {
            return false;
        };
        if seen_units & unit_bit != 0 {
            return false;
        }
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return false;
        }

        seen_units |= unit_bit;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    parsed_chunk
}

fn detected_duration_unit(unit: &str) -> Option<(u8, u16)> {
    match unit {
        "y" => Some((0, 1 << 0)),
        "w" => Some((1, 1 << 1)),
        "d" => Some((2, 1 << 2)),
        "h" => Some((3, 1 << 3)),
        "m" => Some((4, 1 << 4)),
        "s" => Some((5, 1 << 5)),
        "ms" => Some((6, 1 << 6)),
        "us" => Some((7, 1 << 7)),
        "ns" => Some((8, 1 << 8)),
        _ => None,
    }
}

fn is_bytes_literal(value: &str) -> bool {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    if unit_start == value.len() {
        return false;
    }
    let Ok(amount) = value[..unit_start].parse::<f64>() else {
        return false;
    };
    amount.is_finite() && amount >= 0.0 && detected_bytes_unit(&value[unit_start..]).is_some()
}

fn detected_bytes_unit(unit: &str) -> Option<()> {
    match unit {
        "B" | "kB" | "KB" | "MB" | "GB" | "TB" | "KiB" | "MiB" | "GiB" | "TiB" => Some(()),
        _ => None,
    }
}

fn parse_logfmt_pairs(line: &str) -> Vec<(String, String)> {
    let bytes = line.as_bytes();
    let mut pairs = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key_start = index;
        while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'=' {
            index += 1;
        }
        if key_start == index || index >= bytes.len() || bytes[index] != b'=' {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            continue;
        }
        let key = &line[key_start..index];
        index += 1;
        let value = if index < bytes.len() && bytes[index] == b'"' {
            index += 1;
            let mut value = String::new();
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if index + 1 < bytes.len() => {
                        index += 1;
                        value.push(bytes[index] as char);
                        index += 1;
                    }
                    b'"' => {
                        index += 1;
                        break;
                    }
                    byte => {
                        value.push(byte as char);
                        index += 1;
                    }
                }
            }
            value
        } else {
            let value_start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            line[value_start..index].to_string()
        };
        pairs.push((key.to_string(), value));
    }
    pairs
}

async fn execute_index_volume_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let volumes = index_volume_samples(&state, tenant, &plan, &params);
    let response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            loki_volume_vector_response(volumes, params.end, params.limit)
        }
    };
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
}

fn index_volume_samples(
    state: &QuerierState,
    tenant: &str,
    plan: &StreamPlan,
    params: &VolumeParams,
) -> BTreeMap<Labels, BTreeMap<i64, u64>> {
    let mut volumes = BTreeMap::<Labels, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let matching_fingerprints = block
            .fingerprints
            .iter()
            .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
            .copied()
            .collect::<Vec<_>>();
        if matching_fingerprints.is_empty() {
            continue;
        }

        let sample_time = block.key.time_range.start_ns.max(plan.time_range.start_ns);
        for fingerprint in matching_fingerprints {
            let Some(labels) = state.label_index.labels_for(tenant, fingerprint) else {
                continue;
            };
            for metric in volume_metrics_for_labels(labels, params) {
                let samples = volumes.entry(metric).or_default();
                let sample = samples.entry(sample_time).or_default();
                *sample = sample.saturating_add(block.size_bytes);
            }
        }
    }
    volumes
}

fn volume_metrics_for_labels(labels: &Labels, params: &VolumeParams) -> Vec<Labels> {
    match params.aggregate_by {
        VolumeAggregateBy::Series => {
            let labels = if let Some(target_labels) = &params.target_labels {
                project_labels(labels, target_labels)
            } else {
                labels.clone()
            };
            vec![labels]
        }
        VolumeAggregateBy::Labels => match &params.target_labels {
            Some(target_labels) => target_labels
                .iter()
                .filter(|name| labels.contains_key(*name))
                .map(|name| BTreeMap::from([(name.clone(), String::new())]))
                .collect(),
            None => labels
                .keys()
                .map(|name| BTreeMap::from([(name.clone(), String::new())]))
                .collect(),
        },
    }
}

fn project_labels(labels: &Labels, target_labels: &[String]) -> Labels {
    target_labels
        .iter()
        .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
        .collect()
}

fn loki_volume_vector_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    timestamp: i64,
    limit: usize,
) -> Value {
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|(metric, samples)| {
            let value = samples.values().copied().fold(0_u64, u64::saturating_add);
            json!({
                "metric": metric,
                "value": [timestamp, value.to_string()],
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "vector",
        "result": result,
    }))
}

fn limit_volume_series(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Vec<(Labels, BTreeMap<i64, u64>)> {
    volumes.into_iter().take(limit).collect()
}

fn sample_time_bucket(sample_time: i64, start: i64, step: i64) -> i64 {
    if sample_time <= start {
        return start;
    }
    let offset = sample_time - start;
    start + (offset / step) * step
}

fn form_body_query(body: &Bytes) -> Result<String, HttpQueryError> {
    String::from_utf8(body.to_vec()).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

fn post_query_params(raw_query: Option<&str>, body: &Bytes) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{raw_query}&{body_query}"))
        }
        _ => Ok(body_query),
    }
}

fn post_query_params_body_first(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    match (raw_query, body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => Ok(raw_query.to_owned()),
        (Some(raw_query), false) if !raw_query.is_empty() => {
            Ok(format!("{body_query}&{raw_query}"))
        }
        _ => Ok(body_query),
    }
}

fn execute_format_query(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let query = parse_format_query_param(raw_query)?;
    format_logql_query(&query)
}

fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::LokiFormatMissingQuery);
    };
    for pair in split_query_param_pairs(raw_query, &["query"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "query" {
            return decode_form_component(value);
        }
    }
    Err(HttpQueryError::LokiFormatMissingQuery)
}

fn format_logql_query(query: &str) -> Result<String, HttpQueryError> {
    if let Some(error) = scalar_vector_plain_parse_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }
    if let Some(error) = label_join_format_query_error(query) {
        return Err(HttpQueryError::LokiFormatPlainParse(error));
    }

    match parse_query(query) {
        Ok(query) => Ok(format_stream_query(&query)),
        Err(stream_error) => {
            if let Some(formatted) = format_scalar_vector_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_arithmetic(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_comparison(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_binary_set(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_arithmetic_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_comparison_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_binary_set_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_comparison_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_vector_set_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_metric_label_replace_query(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
                Ok(formatted)
            } else if let Some(formatted) = format_sort_vector_expression(query) {
                Ok(formatted)
            } else if let Ok(metric_query) = parse_metric_query(query) {
                Ok(format_metric_query(&metric_query).unwrap_or_else(|| query.trim().to_string()))
            } else if parse_metric_label_join_query(query).is_ok()
                || parse_metric_label_replace_query(query).is_ok()
                || parse_metric_binary_arithmetic_query(query).is_ok()
                || parse_metric_binary_comparison_query(query).is_ok()
                || parse_metric_binary_set_query(query).is_ok()
                || parse_metric_scalar_arithmetic_query(query).is_ok()
                || parse_metric_scalar_comparison_query(query).is_ok()
            {
                Ok(query.trim().to_string())
            } else if scalar_vector_expression_result(query).is_some() {
                Ok(query.trim().to_string())
            } else {
                Err(HttpQueryError::LokiFormatParse {
                    query: query.to_string(),
                    source: stream_error,
                })
            }
        }
    }
}

fn label_join_format_query_error(query: &str) -> Option<String> {
    query
        .trim_start()
        .starts_with("label_join")
        .then(|| "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string())
}

fn format_label_replace_metric_binary_arithmetic(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, bool_modifier, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}

fn format_binary_operator_line(
    operator: &str,
    bool_modifier: bool,
    modifiers: Option<FormattedVectorBinaryModifiers>,
) -> String {
    let mut formatted = operator.to_string();
    if bool_modifier {
        formatted.push_str(" bool");
    }
    if let Some(modifiers) = modifiers {
        formatted.push(' ');
        formatted.push_str(&modifiers.text);
    }
    formatted
}

fn format_label_replace_metric_binary_expression(
    left_text: &str,
    operator: &str,
    right_text: &str,
) -> Option<String> {
    let (left, left_is_label_replace) = format_label_replace_metric_binary_operand(left_text)?;
    let (right, right_is_label_replace) = format_label_replace_metric_binary_operand(right_text)?;
    if !left_is_label_replace && !right_is_label_replace {
        return None;
    }
    Some(format!(
        "{}\n{operator}\n{}",
        indent_logql_lines(&left, "  "),
        indent_logql_lines(&right, "  "),
    ))
}

fn format_label_replace_metric_binary_operand(query: &str) -> Option<(String, bool)> {
    if let Some(formatted) = format_metric_label_replace_query(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_vector_function_text(query) {
        return Some((formatted, false));
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_simple_metric_query(&query))
        .map(|formatted| (formatted, false))
}

fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = if let (Some(left), Some(right)) = (
        parse_metric_query(left_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
        format_vector_function_text(right_text.trim()),
    ) {
        (left, right)
    } else if let (Some(left), Some(right)) = (
        format_vector_function_text(left_text.trim()),
        parse_metric_query(right_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
    ) {
        (left, right)
    } else {
        return None;
    };

    Some(format_metric_vector_binary_expression(
        &left, operator, modifiers, &right,
    ))
}

fn format_metric_vector_binary_expression(
    left: &str,
    operator: &str,
    modifiers: Option<FormattedVectorBinaryModifiers>,
    right: &str,
) -> String {
    match modifiers {
        Some(modifiers) => format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        ),
        None => format!("({left} {operator} {right})"),
    }
}

fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_arithmetic_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let arithmetic = parse_metric_binary_arithmetic_query(query).ok()?;
    let left = format_metric_query(&arithmetic.left)?;
    let right = format_metric_query(&arithmetic.right)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        arithmetic.matching.as_ref(),
        &right,
    ))
}

fn format_metric_binary_comparison_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let right_text = right_text
        .strip_prefix("bool")
        .map(str::trim_start)
        .unwrap_or(right_text);
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let comparison = parse_metric_binary_comparison_query(query).ok()?;
    let left = format_metric_query(&comparison.left)?;
    let right = format_metric_query(&comparison.right)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    Some(format_metric_binary_expression(
        &left,
        operator,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
        &right,
    ))
}

fn format_metric_binary_set_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_set_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let set = parse_metric_binary_set_query(query).ok()?;
    let left = format_metric_query(&set.left)?;
    let right = format_metric_query(&set.right)?;
    let operator = format_metric_binary_set_operator(set.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        set.matching.as_ref(),
        &right,
    ))
}

fn format_metric_binary_expression(
    left: &str,
    operator: &str,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    right: &str,
) -> String {
    let bool_text = if bool_modifier { " bool" } else { "" };
    let Some(matching) = matching else {
        return format!("({left} {operator}{bool_text} {right})");
    };
    let matching = format_metric_vector_matching(matching);
    if matching.has_group {
        return format!(
            "  {left}\n{operator}{bool_text} {}\n  {right}",
            matching.text
        );
    }
    format!("({left} {operator}{bool_text} {}  {right})", matching.text)
}

struct FormattedMetricVectorMatching {
    text: String,
    has_group: bool,
}

fn format_metric_vector_matching(matching: &MetricVectorMatching) -> FormattedMetricVectorMatching {
    match matching {
        MetricVectorMatching::On { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("on", labels, group.as_ref()),
            has_group: group.is_some(),
        },
        MetricVectorMatching::Ignoring { labels, group } => FormattedMetricVectorMatching {
            text: format_metric_vector_matching_text("ignoring", labels, group.as_ref()),
            has_group: group.is_some(),
        },
    }
}

fn format_metric_vector_matching_text(
    modifier: &str,
    labels: &[String],
    group: Option<&MetricVectorGroupModifier>,
) -> String {
    let mut text = format!("{modifier} ({})", labels.join(","));
    if let Some(group) = group {
        text.push(' ');
        text.push_str(&format_metric_vector_group_modifier(group));
    }
    text
}

fn format_metric_vector_group_modifier(group: &MetricVectorGroupModifier) -> String {
    match group {
        MetricVectorGroupModifier::Left(labels) => {
            format_metric_vector_group_modifier_text("group_left", labels)
        }
        MetricVectorGroupModifier::Right(labels) => {
            format_metric_vector_group_modifier_text("group_right", labels)
        }
    }
}

fn format_metric_vector_group_modifier_text(modifier: &str, labels: &[String]) -> String {
    if labels.is_empty() {
        modifier.to_string()
    } else {
        format!("{modifier} ({})", labels.join(","))
    }
}

fn format_metric_binary_set_operator(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}

fn split_leading_vector_binary_modifiers(
    query: &str,
) -> (Option<FormattedVectorBinaryModifiers>, &str) {
    let Some((matching_modifier, rest)) = split_leading_vector_matching_modifier(query) else {
        return (None, query.trim_start());
    };
    let (group_modifier, rest) = split_leading_vector_group_modifier(rest);
    (
        Some(match group_modifier {
            Some(group_modifier) => FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            None => FormattedVectorBinaryModifiers {
                text: matching_modifier,
                right_separator: "  ",
            },
        }),
        rest.trim_start(),
    )
}

fn split_leading_vector_matching_modifier(query: &str) -> Option<(String, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let labels = rest.trim_start().strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels_text = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels_text})"),
                &labels[labels_end + 1..],
            ));
        }
    }
    None
}

fn split_leading_vector_group_modifier(query: &str) -> (Option<String>, &str) {
    let query = query.trim_start();
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query.strip_prefix(modifier) {
            let rest = rest.trim_start();
            let Some(labels) = rest.strip_prefix('(') else {
                return (Some(modifier.to_string()), rest);
            };
            let Some(labels_end) = labels.find(')') else {
                return (None, query);
            };
            let labels_text = &labels[..labels_end];
            let modifier_text = if labels_text.is_empty() {
                modifier.to_string()
            } else {
                format!("{modifier} ({labels_text})")
            };
            return (Some(modifier_text), &labels[labels_end + 1..]);
        }
    }
    (None, query)
}

fn format_metric_vector_set_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = if let (Some(left), Some(right)) = (
        parse_metric_query(left_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
        format_vector_function_text(right_text.trim()),
    ) {
        (left, right)
    } else if let (Some(left), Some(right)) = (
        format_vector_function_text(left_text.trim()),
        parse_metric_query(right_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
    ) {
        (left, right)
    } else {
        return None;
    };

    Some(format_metric_vector_binary_expression(
        &left, operator, modifiers, &right,
    ))
}

fn format_metric_scalar_arithmetic_expression(query: &str) -> Option<String> {
    let arithmetic = parse_metric_scalar_arithmetic_query(query).ok()?;
    let metric = format_simple_metric_query(&arithmetic.query)?;
    let scalar = format_scalar_text(&arithmetic.scalar)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(if arithmetic.scalar_on_left {
        format!("({scalar} {operator} {metric})")
    } else {
        format!("({metric} {operator} {scalar})")
    })
}

fn format_metric_scalar_comparison_expression(query: &str) -> Option<String> {
    let comparison = parse_metric_scalar_comparison_query(query).ok()?;
    let metric = format_simple_metric_query(&comparison.query)?;
    let scalar = format_scalar_text(&comparison.scalar)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    let bool_modifier = if comparison.bool_modifier {
        " bool"
    } else {
        ""
    };
    Some(if comparison.scalar_on_left {
        format!("({scalar} {operator}{bool_modifier} {metric})")
    } else {
        format!("({metric} {operator}{bool_modifier} {scalar})")
    })
}

fn format_scalar_text(scalar: &str) -> Option<String> {
    Some(parse_scalar_sample(scalar)?.format())
}

fn format_metric_scalar_arithmetic_operator(op: MetricScalarArithmeticOp) -> &'static str {
    match op {
        MetricScalarArithmeticOp::Add => "+",
        MetricScalarArithmeticOp::Subtract => "-",
        MetricScalarArithmeticOp::Multiply => "*",
        MetricScalarArithmeticOp::Divide => "/",
        MetricScalarArithmeticOp::Modulo => "%",
        MetricScalarArithmeticOp::Power => "^",
    }
}

fn format_metric_scalar_comparison_operator(op: ComparisonOp) -> Option<&'static str> {
    match op {
        ComparisonOp::Equal => Some("=="),
        ComparisonOp::NotEqual => Some("!="),
        ComparisonOp::Greater => Some(">"),
        ComparisonOp::GreaterEqual => Some(">="),
        ComparisonOp::Less => Some("<"),
        ComparisonOp::LessEqual => Some("<="),
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => None,
    }
}

fn format_metric_label_replace_query(query: &str) -> Option<String> {
    let label_replace = parse_metric_label_replace_query(query).ok()?;
    let metric = format_metric_query(&label_replace.query)?;
    Some(format!(
        "label_replace({metric},{},{},{},{})",
        format_logql_quoted_string(&label_replace.destination_label),
        format_logql_quoted_string(&label_replace.replacement),
        format_logql_quoted_string(&label_replace.source_label),
        format_logql_quoted_string(&label_replace.pattern),
    ))
}

fn format_label_replace_metric_scalar_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_metric_scalar_vector_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace({vector},{},{},{},{})",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

fn format_metric_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    None
}

fn format_label_replace_metric_vector_expression(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_mixed_metric_vector_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace(\n  {vector},\n  {},\n  {},\n  {},\n  {}\n)",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

fn format_mixed_metric_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_sort_vector_expression(query) {
        return Some(formatted);
    }
    None
}

fn format_sort_vector_expression(query: &str) -> Option<String> {
    for function in ["sort", "sort_desc"] {
        let Some(arguments) = split_logql_function_arguments(query, function) else {
            continue;
        };
        if arguments.len() != 1 {
            return None;
        }
        let inner = format_loki_vector_expression(arguments[0].trim())?;
        if inner.contains('\n') {
            return Some(format!(
                "{function}(\n{}\n)",
                indent_logql_lines(&inner, "  ")
            ));
        }
        return Some(format!("{function}({inner})"));
    }
    None
}

fn format_loki_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_scalar_vector_expression(query) {
        return Some(formatted);
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_metric_query(&query))
}

fn indent_logql_lines(value: &str, prefix: &str) -> String {
    value
        .lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_logql_quoted_string(value: &str) -> String {
    let mut formatted = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => formatted.push_str("\\\\"),
            '"' => formatted.push_str("\\\""),
            '\n' => formatted.push_str("\\n"),
            '\r' => formatted.push_str("\\r"),
            '\t' => formatted.push_str("\\t"),
            other => formatted.push(other),
        }
    }
    formatted.push('"');
    formatted
}

fn split_top_level_set_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            ch if parens == 0 && brackets == 0 && braces == 0 && ch.is_ascii_alphabetic() => {
                for operator in ["unless", "and", "or"] {
                    if query[index..].starts_with(operator)
                        && has_word_boundary(query, index, operator.len())
                    {
                        return Some((
                            &query[..index],
                            operator,
                            query[index + operator.len()..].trim_start(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn has_word_boundary(query: &str, index: usize, len: usize) -> bool {
    query[..index]
        .chars()
        .next_back()
        .is_none_or(char::is_whitespace)
        && query[index + len..]
            .chars()
            .next()
            .is_none_or(char::is_whitespace)
}

fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = if let (Some(left), Some(right)) = (
        parse_metric_query(left_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
        format_vector_function_text(right_text.trim()),
    ) {
        (left, right)
    } else if let (Some(left), Some(right)) = (
        format_vector_function_text(left_text.trim()),
        parse_metric_query(right_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
    ) {
        (left, right)
    } else {
        return None;
    };

    match (bool_modifier, modifiers) {
        (true, Some(modifiers)) => Some(format!(
            "({left} {operator} bool {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (true, None) => Some(format!("({left} {operator} bool {right})")),
        (false, Some(modifiers)) => Some(format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (false, None) => Some(format!("({left} {operator} {right})")),
    }
}

fn split_top_level_comparison_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            '>' | '<' | '=' | '!' if parens == 0 && brackets == 0 && braces == 0 => {
                for operator in [">=", "<=", "==", "!=", ">", "<"] {
                    if query[index..].starts_with(operator) {
                        return Some((
                            &query[..index],
                            operator,
                            query[index + operator.len()..].trim_start(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_arithmetic_query(query: &str) -> Option<(&str, &'static str, &str)> {
    let mut parens = 0_i32;
    let mut brackets = 0_i32;
    let mut braces = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '{' => braces += 1,
            '}' => braces -= 1,
            '+' | '-' | '*' | '/' | '%' | '^' if parens == 0 && brackets == 0 && braces == 0 => {
                let right = query[index + ch.len_utf8()..].trim_start();
                return Some((
                    &query[..index],
                    match ch {
                        '+' => "+",
                        '-' => "-",
                        '*' => "*",
                        '/' => "/",
                        '%' => "%",
                        '^' => "^",
                        _ => unreachable!(),
                    },
                    right,
                ));
            }
            _ => {}
        }
    }
    None
}

fn format_simple_metric_query(query: &MetricQuery) -> Option<String> {
    if query.vector_aggregation.is_some() || query.range_grouping.is_some() {
        return None;
    }
    format_metric_range_aggregation_query(query)
}

fn format_metric_query(query: &MetricQuery) -> Option<String> {
    let mut formatted = format_metric_range_aggregation_query(query)?;
    if let Some(grouping) = &query.range_grouping {
        formatted = format!("{formatted} {}", format_vector_grouping(grouping));
    }
    if let Some(vector_aggregation) = &query.vector_aggregation {
        formatted = format_vector_aggregation_query(vector_aggregation, &formatted)?;
    }
    Some(formatted)
}

fn format_metric_range_aggregation_query(query: &MetricQuery) -> Option<String> {
    let range = format_metric_range_selector(query)?;
    if let RangeAggregation::QuantileOverTime(quantile) = query.aggregation {
        return Some(format!(
            "quantile_over_time({},{range})",
            format_quantile(quantile),
        ));
    }
    Some(format!(
        "{}({range})",
        format_range_aggregation_name(&query.aggregation)?,
    ))
}

fn format_metric_range_selector(query: &MetricQuery) -> Option<String> {
    let range = format_loki_duration_ns(query.range_ns.0)?;
    let offset = if query.offset_ns.0 == 0 {
        String::new()
    } else {
        let sign = if query.offset_ns.0 < 0 { "-" } else { "" };
        let duration = format_loki_offset_duration_ns(query.offset_ns.0.checked_abs()?)?;
        format!(" offset {sign}{duration}")
    };
    Some(format!(
        "{}[{range}]{offset}",
        format_stream_query(&query.stream)
    ))
}

fn format_vector_aggregation_query(aggregation: &VectorAggregation, inner: &str) -> Option<String> {
    let grouping = aggregation
        .grouping
        .as_ref()
        .map(|grouping| format!(" {}", format_vector_grouping(grouping)))
        .unwrap_or_default();
    match &aggregation.op {
        VectorAggregationOp::Sum => Some(format!("sum{grouping}({inner})")),
        VectorAggregationOp::Count => Some(format!("count{grouping}({inner})")),
        VectorAggregationOp::Min => Some(format!("min{grouping}({inner})")),
        VectorAggregationOp::Max => Some(format!("max{grouping}({inner})")),
        VectorAggregationOp::Avg => Some(format!("avg{grouping}({inner})")),
        VectorAggregationOp::Stddev => Some(format!("stddev{grouping}({inner})")),
        VectorAggregationOp::Stdvar => Some(format!("stdvar{grouping}({inner})")),
        VectorAggregationOp::TopK(limit) => Some(format!("topk{grouping}({limit},{inner})")),
        VectorAggregationOp::BottomK(limit) => Some(format!("bottomk{grouping}({limit},{inner})")),
        VectorAggregationOp::ApproxTopK(limit) if aggregation.grouping.is_none() => {
            Some(format!("approx_topk({limit},{inner})"))
        }
        VectorAggregationOp::Sort if aggregation.grouping.is_none() => {
            Some(format!("sort({inner})"))
        }
        VectorAggregationOp::SortDesc if aggregation.grouping.is_none() => {
            Some(format!("sort_desc({inner})"))
        }
        VectorAggregationOp::CountValues(_)
        | VectorAggregationOp::ApproxTopK(_)
        | VectorAggregationOp::Sort
        | VectorAggregationOp::SortDesc => None,
    }
}

fn format_vector_grouping(grouping: &VectorGrouping) -> String {
    match grouping {
        VectorGrouping::By(labels) => format!("by ({})", labels.join(",")),
        VectorGrouping::Without(labels) => format!("without ({})", labels.join(",")),
    }
}

fn format_loki_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let mut formatted = String::new();
    for (unit_ns, suffix) in [
        (3_600_000_000_000_i64, "h"),
        (60_000_000_000_i64, "m"),
        (1_000_000_000_i64, "s"),
        (1_000_000_i64, "ms"),
        (1_000_i64, "us"),
        (1_i64, "ns"),
    ] {
        if remaining >= unit_ns {
            let value = remaining / unit_ns;
            remaining %= unit_ns;
            formatted.push_str(&format!("{value}{suffix}"));
        }
    }
    Some(formatted)
}

fn format_loki_offset_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    const HOUR_NS: i64 = 3_600_000_000_000;
    const MINUTE_NS: i64 = 60_000_000_000;
    const SECOND_NS: i64 = 1_000_000_000;
    const MILLISECOND_NS: i64 = 1_000_000;
    const MICROSECOND_NS: i64 = 1_000;

    let mut remaining = duration_ns;
    let hours = remaining / HOUR_NS;
    remaining %= HOUR_NS;
    let minutes = remaining / MINUTE_NS;
    remaining %= MINUTE_NS;

    if hours > 0 {
        return Some(format!(
            "{hours}h{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if minutes > 0 {
        return Some(format!(
            "{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if remaining >= SECOND_NS {
        return Some(format_loki_offset_seconds(remaining));
    }
    if remaining >= MILLISECOND_NS {
        return Some(format_loki_decimal_unit(remaining, MILLISECOND_NS, 6, "ms"));
    }
    if remaining >= MICROSECOND_NS {
        return Some(format_loki_decimal_unit(
            remaining,
            MICROSECOND_NS,
            3,
            "\u{00b5}s",
        ));
    }
    Some(format!("{remaining}ns"))
}

fn format_loki_offset_seconds(duration_ns: i64) -> String {
    format_loki_decimal_unit(duration_ns, 1_000_000_000, 9, "s")
}

fn format_loki_decimal_unit(duration_ns: i64, unit_ns: i64, width: usize, suffix: &str) -> String {
    let whole = duration_ns / unit_ns;
    let fractional_ns = duration_ns % unit_ns;
    if fractional_ns == 0 {
        return format!("{whole}{suffix}");
    }

    let mut fraction = format!("{fractional_ns:0width$}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    format!("{whole}.{fraction}{suffix}")
}

fn format_quantile(quantile: Quantile) -> String {
    ScalarSample::new(
        i128::from(quantile.numerator.0),
        u128::from(quantile.denominator.0),
    )
    .format()
}

fn format_range_aggregation_name(aggregation: &RangeAggregation) -> Option<&'static str> {
    match aggregation {
        RangeAggregation::CountOverTime => Some("count_over_time"),
        RangeAggregation::Rate => Some("rate"),
        RangeAggregation::RateCounter => Some("rate_counter"),
        RangeAggregation::BytesRate => Some("bytes_rate"),
        RangeAggregation::BytesOverTime => Some("bytes_over_time"),
        RangeAggregation::AbsentOverTime => Some("absent_over_time"),
        RangeAggregation::PresentOverTime => Some("present_over_time"),
        RangeAggregation::SumOverTime => Some("sum_over_time"),
        RangeAggregation::AvgOverTime => Some("avg_over_time"),
        RangeAggregation::StdvarOverTime => Some("stdvar_over_time"),
        RangeAggregation::StddevOverTime => Some("stddev_over_time"),
        RangeAggregation::MinOverTime => Some("min_over_time"),
        RangeAggregation::MaxOverTime => Some("max_over_time"),
        RangeAggregation::FirstOverTime => Some("first_over_time"),
        RangeAggregation::LastOverTime => Some("last_over_time"),
        RangeAggregation::QuantileOverTime(_) => None,
    }
}

fn format_vector_function_text(query: &str) -> Option<String> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let (formatted, end) = parse_formatted_vector_function(&query, 0)?;
    (end == query.len()).then_some(formatted)
}

fn format_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(scalar) = query
        .strip_prefix("vector(")
        .and_then(|query| query.strip_suffix(')'))
    {
        if scalar.starts_with(['+', '-']) {
            return None;
        }
        if let Some(sample) = parse_scalar_sample(scalar) {
            return Some(format!("vector({})", sample.format_fixed_six()));
        }
    }
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    match scalar_vector_expression_result(&query)? {
        ScalarVectorExpressionResult::Scalar { sample } => Some(sample),
        ScalarVectorExpressionResult::Vector { .. } => None,
    }
}

fn format_vector_label_replace_function(query: &str) -> Option<String> {
    let arguments = split_logql_function_arguments(query, "label_replace")?;
    if arguments.len() != 5 {
        return None;
    }
    let vector = format_vector_only_expression(arguments[0].trim())?;
    Some(format!(
        "label_replace({vector},{},{},{},{})",
        format_logql_quoted_string(&parse_logql_string_argument(arguments[1].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[2].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[3].trim())?),
        format_logql_quoted_string(&parse_logql_string_argument(arguments[4].trim())?),
    ))
}

fn format_vector_only_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    None
}

fn split_logql_function_arguments<'a>(query: &'a str, name: &str) -> Option<Vec<&'a str>> {
    let query = query.trim();
    let rest = query.strip_prefix(name)?.trim_start();
    let rest = rest.strip_prefix('(')?;
    let mut arguments = Vec::new();
    let mut start = 0;
    let mut parens = 0_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in rest.char_indices() {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' if parens > 0 => parens -= 1,
            ',' if parens == 0 => {
                arguments.push(rest[start..index].trim());
                start = index + ch.len_utf8();
            }
            ')' => {
                arguments.push(rest[start..index].trim());
                if rest[index + ch.len_utf8()..].trim().is_empty() {
                    return Some(arguments);
                }
                return None;
            }
            _ => {}
        }
    }
    None
}

fn parse_logql_string_argument(argument: &str) -> Option<String> {
    if let Some(inner) = argument
        .strip_prefix('`')
        .and_then(|argument| argument.strip_suffix('`'))
    {
        return Some(inner.to_string());
    }

    let inner = argument
        .strip_prefix('"')
        .and_then(|argument| argument.strip_suffix('"'))?;
    let mut parsed = String::new();
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            parsed.push(match chars.next()? {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\\' => '\\',
                other => other,
            });
        } else {
            parsed.push(ch);
        }
    }
    Some(parsed)
}

fn format_vector_set_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    for operator in ["unless", "and", "or"] {
        if let Some(rest) = query[position..].strip_prefix(operator) {
            let mut right_position = query.len() - rest.len();
            let modifiers = if let Some((modifiers, next_position)) =
                parse_vector_binary_modifiers(query, right_position)
            {
                right_position = next_position;
                Some(modifiers)
            } else {
                None
            };
            let (right, end) = parse_formatted_vector_function(query, right_position)?;
            if end == query.len() {
                return Some(match modifiers {
                    Some(modifiers) => format!(
                        "({left} {operator} {}{}{right})",
                        modifiers.text, modifiers.right_separator
                    ),
                    None => format!("({left} {operator} {right})"),
                });
            }
        }
    }
    None
}

fn format_vector_comparison_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_comparison_operator(query, position)?;
    let bool_modifier = query[right_position..].starts_with("bool");
    if bool_modifier {
        right_position += "bool".len();
    }
    let modifiers = if let Some((modifiers, next_position)) =
        parse_vector_binary_modifiers(query, right_position)
    {
        right_position = next_position;
        Some(modifiers)
    } else {
        None
    };
    let (right, end) = parse_formatted_vector_function(query, right_position)?;
    if end != query.len() {
        return None;
    }
    match (bool_modifier, modifiers) {
        (true, Some(modifiers)) => Some(format!(
            "({left} {operator} bool {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (true, None) => Some(format!("({left} {operator} bool {right})")),
        (false, Some(modifiers)) => Some(format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (false, None) => Some(format!("({left} {operator} {right})")),
    }
}

fn parse_vector_comparison_operator(query: &str, position: usize) -> Option<(&'static str, usize)> {
    for operator in [">=", "<=", "==", "!=", ">", "<"] {
        if query[position..].starts_with(operator) {
            return Some((operator, position + operator.len()));
        }
    }
    None
}

fn format_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_arithmetic_operator(query, position)?;
    let modifiers = if let Some((modifiers, next_position)) =
        parse_vector_binary_modifiers(query, right_position)
    {
        right_position = next_position;
        Some(modifiers)
    } else {
        None
    };
    let (right, end) = parse_formatted_vector_function(query, right_position)?;
    if end == query.len() {
        Some(match modifiers {
            Some(modifiers) => format!(
                "({left} {operator} {}{}{right})",
                modifiers.text, modifiers.right_separator
            ),
            None => format!("({left} {operator} {right})"),
        })
    } else {
        None
    }
}

struct FormattedVectorBinaryModifiers {
    text: String,
    right_separator: &'static str,
}

fn parse_vector_binary_modifiers(
    query: &str,
    position: usize,
) -> Option<(FormattedVectorBinaryModifiers, usize)> {
    let (matching_modifier, position) = parse_vector_matching_modifier(query, position)?;
    if let Some((group_modifier, position)) = parse_vector_group_modifier(query, position) {
        return Some((
            FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            position,
        ));
    }
    Some((
        FormattedVectorBinaryModifiers {
            text: matching_modifier,
            right_separator: "  ",
        },
        position,
    ))
}

fn parse_vector_matching_modifier(query: &str, position: usize) -> Option<(String, usize)> {
    for modifier in ["on", "ignoring"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let labels = rest.strip_prefix('(')?;
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}

fn parse_vector_group_modifier(query: &str, position: usize) -> Option<(String, usize)> {
    for modifier in ["group_left", "group_right"] {
        if let Some(rest) = query[position..].strip_prefix(modifier) {
            let Some(labels) = rest.strip_prefix('(') else {
                return Some((modifier.to_string(), position + modifier.len()));
            };
            let labels_end = labels.find(')')?;
            let labels = &labels[..labels_end];
            if labels.is_empty() {
                return Some((modifier.to_string(), position + modifier.len() + 2));
            }
            return Some((
                format!("{modifier} ({labels})"),
                position + modifier.len() + 1 + labels_end + 1,
            ));
        }
    }
    None
}

fn parse_vector_arithmetic_operator(query: &str, position: usize) -> Option<(&'static str, usize)> {
    for (raw, formatted) in [
        ("+", "+"),
        ("-", "-"),
        ("*", "*"),
        ("/", "/"),
        ("%", "%"),
        ("^", "^"),
    ] {
        if query[position..].starts_with(raw) {
            return Some((formatted, position + raw.len()));
        }
    }
    None
}

fn parse_formatted_vector_function(query: &str, position: usize) -> Option<(String, usize)> {
    if let Some(scalar) = query[position..].strip_prefix("vector(") {
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        let sample = parse_scalar_sample(scalar_text)?.format_fixed_six();
        return Some((
            format!("vector({sample})"),
            position + "vector(".len() + scalar_end + 1,
        ));
    }

    let call_end = find_logql_function_call_end(query, position, "label_replace")?;
    let formatted = format_vector_label_replace_function(&query[position..call_end])?;
    Some((formatted, call_end))
}

fn find_logql_function_call_end(query: &str, position: usize, name: &str) -> Option<usize> {
    let rest = &query[position..];
    let rest = rest.strip_prefix(name)?;
    let mut chars = rest.char_indices();
    let (_, first) = chars.next()?;
    if first != '(' {
        return None;
    }

    let mut parens = 1_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in chars {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => {
                parens -= 1;
                if parens == 0 {
                    return Some(position + name.len() + index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

fn format_stream_query(query: &StreamQuery) -> String {
    let mut formatted = format!(
        "{{{}}}",
        query
            .matchers
            .iter()
            .map(format_label_matcher)
            .collect::<Vec<_>>()
            .join(",")
    );
    for stage in &query.pipeline {
        if matches!(stage, PipelineStage::LineFilter(_)) {
            formatted.push(' ');
        } else {
            formatted.push_str(" | ");
        }
        formatted.push_str(&format_pipeline_stage(stage));
    }
    formatted
}

fn format_label_matcher(matcher: &crabka_logql::LabelMatcher) -> String {
    format!(
        "{}{}{}",
        matcher.name,
        match matcher.op {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::RegexEqual => "=~",
            MatchOp::RegexNotEqual => "!~",
        },
        quote_logql_string(&matcher.value)
    )
}

fn format_pipeline_stage(stage: &PipelineStage) -> String {
    match stage {
        PipelineStage::LineFilter(filter) => {
            let value = if filter.is_ip_matcher() {
                format!("ip({})", quote_logql_string(&filter.pattern))
            } else {
                quote_logql_string(&filter.pattern)
            };
            format!(
                "{} {value}",
                match filter.op {
                    LineFilterOp::Contains => "|=",
                    LineFilterOp::NotContains => "!=",
                    LineFilterOp::Regex => "|~",
                    LineFilterOp::NotRegex => "!~",
                    LineFilterOp::Pattern => "|>",
                    LineFilterOp::NotPattern => "!>",
                }
            )
        }
        PipelineStage::Decolorize => "decolorize".to_string(),
        PipelineStage::Parser(ParserStage::Json) => "json".to_string(),
        PipelineStage::Parser(ParserStage::JsonSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    format!(
                        "{}={}",
                        extraction.destination(),
                        quote_logql_string(extraction.expression())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("json {extractions}")
        }
        PipelineStage::Parser(ParserStage::Logfmt) => "logfmt".to_string(),
        PipelineStage::Parser(ParserStage::LogfmtConfigured(config)) => {
            format!("logfmt{}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::LogfmtSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    if extraction.destination() == extraction.source() {
                        extraction.destination().to_string()
                    } else {
                        format!(
                            "{}={}",
                            extraction.destination(),
                            quote_logql_string(extraction.source())
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("logfmt{} {extractions}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::Unpack) => "unpack".to_string(),
        PipelineStage::Parser(ParserStage::Pattern(pattern)) => {
            format!("pattern {}", quote_logql_string(pattern.pattern()))
        }
        PipelineStage::Parser(ParserStage::Regexp(parser)) => {
            format!("regexp {}", quote_logql_string(parser.pattern()))
        }
        PipelineStage::LineFormat(format) => {
            format!("line_format {}", quote_logql_string(format.template()))
        }
        PipelineStage::LabelFormat(format) => {
            let assignments = format
                .assignments()
                .iter()
                .map(|assignment| {
                    let value = match assignment.value() {
                        LabelFormatValue::Rename(source) => source.clone(),
                        LabelFormatValue::Template(template) => {
                            quote_logql_string(template.template())
                        }
                    };
                    format!("{}={value}", assignment.destination())
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("label_format {assignments}")
        }
        PipelineStage::DropLabels(selections) => {
            format!("drop {}", format_label_selection_set(selections))
        }
        PipelineStage::KeepLabels(selections) => {
            format!("keep {}", format_label_selection_set(selections))
        }
        PipelineStage::Unwrap(unwrap) => match unwrap.conversion() {
            UnwrapConversion::Raw => format!("unwrap {}", unwrap.label()),
            UnwrapConversion::Bytes => format!("unwrap bytes({})", unwrap.label()),
            UnwrapConversion::Duration => format!("unwrap duration({})", unwrap.label()),
        },
        PipelineStage::FieldFilter(filter) => format_field_filter(filter),
        PipelineStage::FieldFilterChain(chain) => {
            let mut formatted = format_field_filter(chain.first());
            for (op, filter) in chain.rest() {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter(filter));
            }
            formatted
        }
        PipelineStage::FieldFilterExpression(expression) => {
            format_field_filter_expression(expression)
        }
    }
}

fn format_logfmt_parser_flags(config: &LogfmtParserConfig) -> String {
    let mut flags = Vec::new();
    if config.keep_empty() {
        flags.push("--keep-empty");
    }
    if config.strict() {
        flags.push("--strict");
    }
    if flags.is_empty() {
        String::new()
    } else {
        format!(" {}", flags.join(" "))
    }
}

fn format_label_selection_set(selections: &LabelSelectionSet) -> String {
    selections
        .selections()
        .iter()
        .map(|selection| {
            let Some(matcher) = selection.matcher() else {
                return selection.name_str().to_string();
            };
            match matcher {
                LabelSelectionMatcher::Equal(value) => {
                    format!("{}={}", selection.name_str(), quote_logql_string(value))
                }
                LabelSelectionMatcher::Regex(pattern) => {
                    format!("{}=~{}", selection.name_str(), quote_logql_string(pattern))
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_field_filter(filter: &FieldFilter) -> String {
    format!(
        "{}{}{}",
        filter.name,
        match filter.op {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::RegexEqual => "=~",
            ComparisonOp::RegexNotEqual => "!~",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterEqual => ">=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessEqual => "<=",
        },
        match &filter.value {
            FieldValue::Number(value) => value.to_string(),
            FieldValue::Duration(value) => format!("{value}ns"),
            FieldValue::Bytes(value) => format!("{value}B"),
            FieldValue::String(value) => quote_logql_string(value),
            FieldValue::Ip(value) => format!("ip({})", quote_logql_string(value.pattern())),
        }
    )
}

fn format_field_filter_expression(expression: &FieldFilterExpression) -> String {
    match expression {
        FieldFilterExpression::Filter(filter) => format_field_filter(filter),
        FieldFilterExpression::Group(expression) => {
            format!("({})", format_field_filter_expression(expression))
        }
        FieldFilterExpression::Chain { first, rest } => {
            let mut formatted = format_field_filter_expression(first);
            for (op, expression) in rest {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter_expression(expression));
            }
            formatted
        }
    }
}

fn quote_logql_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization cannot fail")
}

fn validate_query_series_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_series) = state.max_query_series else {
        return Ok(());
    };
    let series = plan.fingerprints.len();
    if series > max_query_series {
        return Err(HttpQueryError::QuerySeriesTooLarge {
            series,
            max_series: max_query_series,
        });
    }
    Ok(())
}

fn validate_query_bytes_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_bytes) = state.max_query_bytes else {
        return Ok(());
    };
    let planned_bytes = plan
        .blocks
        .iter()
        .map(|block| block.size_bytes)
        .try_fold(0_u64, u64::checked_add)
        .unwrap_or(u64::MAX);
    if planned_bytes > max_query_bytes {
        return Err(HttpQueryError::QueryBytesTooLarge {
            planned_bytes,
            max_bytes: max_query_bytes,
        });
    }
    Ok(())
}

/// Snapshot the hot-tail records overlapping `time_range`, plus the compaction frontier.
///
/// `time_range` must be the planned scan range (`plan.time_range`): every hot-tail
/// query path re-applies the exact per-record time bound downstream, and that bound is
/// always contained within the plan's scan range (the stream query range, or
/// [`metric_scan_range`] for metric queries). Pruning to the plan range therefore drops
/// only records the downstream filter would reject anyway, so results are identical to a
/// full-buffer scan while a narrow window avoids touching the whole retained buffer.
fn hot_tail_snapshot(
    state: &QuerierState,
    time_range: TimeRange,
) -> (Vec<WalLogRecord>, CompactionFrontier) {
    state.hot_tail.as_ref().map_or(
        (Vec::new(), CompactionFrontier::new(i64::MAX)),
        |hot_tail| {
            (
                hot_tail
                    .source
                    .records_in_range(time_range.start_ns, time_range.end_ns),
                hot_tail.frontier.snapshot(),
            )
        },
    )
}

struct TailStream {
    plan: StreamPlan,
    source: Option<Arc<dyn LogHotTail>>,
    frontier: CompactionFrontierSource,
    delete_filters: Vec<ActiveLogDeleteFilter>,
    limit: Option<usize>,
    delay_for: i64,
}

async fn prepare_http_tail(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &QueryParams,
) -> Result<TailStream, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = optional_start_end_range(params.start, params.since, params.end)?;
    let delay_for = params.delay_for.unwrap_or(0);
    validate_loki_tail_delay_for(delay_for)?;
    validate_query_length_limit(state, &params.query)?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let delete_filters = active_log_delete_filters(state, tenant, time_range)?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    let (source, frontier) = state.hot_tail.as_ref().map_or(
        (
            None,
            CompactionFrontierSource::Snapshot(CompactionFrontier::new(i64::MAX)),
        ),
        |hot_tail| (Some(hot_tail.source.clone()), hot_tail.frontier.clone()),
    );

    Ok(TailStream {
        plan,
        source,
        frontier,
        delete_filters,
        limit: Some(params.limit.unwrap_or(LOKI_DEFAULT_TAIL_LIMIT)),
        delay_for,
    })
}

async fn send_tail_stream(mut socket: WebSocket, tail: TailStream) {
    let Some(source) = tail.source else {
        let _ = send_tail_frame(&mut socket, json!({ "streams": [] })).await;
        return;
    };
    let mut sent_records = 0;

    loop {
        let records = source.records();
        if records.len() < sent_records {
            sent_records = 0;
        }
        if records.len() > sent_records {
            let eligible = eligible_tail_record_count(&records[sent_records..], tail.delay_for);
            if eligible > 0 {
                let eligible_end = sent_records + eligible;
                let frontier = tail.frontier.snapshot();
                let frame = execute_tail_query_with_frontier_and_deletes(
                    &tail.plan,
                    &records[sent_records..eligible_end],
                    &frontier,
                    &tail.delete_filters,
                );
                sent_records = eligible_end;
                let frame = apply_loki_tail_frame_limit(frame, tail.limit);
                if !tail_frame_is_empty(&frame) && !send_tail_frame(&mut socket, frame).await {
                    return;
                }
            }
        }

        sleep(Duration::from_millis(50)).await;
    }
}

fn eligible_tail_record_count(records: &[WalLogRecord], delay_for: i64) -> usize {
    if delay_for <= 0 {
        return records.len();
    }

    let cutoff = current_unix_time_ns().saturating_sub(delay_for);
    records
        .iter()
        .take_while(|record| record.timestamp_ns <= cutoff)
        .count()
}

async fn send_tail_frame(socket: &mut WebSocket, frame: Value) -> bool {
    socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .is_ok()
}

fn tail_frame_is_empty(frame: &Value) -> bool {
    frame
        .get("streams")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty)
}

fn apply_loki_tail_frame_limit(mut frame: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return frame;
    };
    let Some(streams) = frame.get_mut("streams").and_then(Value::as_array_mut) else {
        return frame;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        if remaining == 0 {
            values.clear();
            continue;
        }
        if values.len() > remaining {
            values.truncate(remaining);
        }
        remaining = remaining.saturating_sub(values.len());
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    frame
}

async fn execute_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_names_data(state, headers, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}

async fn execute_api_prom_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let values = label_names_data(state, headers, params).await?;
    Ok(if values.is_empty() {
        json_response(StatusCode::OK, &json!({}))
    } else {
        json_response(
            StatusCode::OK,
            &json!({
                "values": values,
            }),
        )
    })
}

async fn label_names_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut names = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        names.extend(labels.keys().cloned());
    }

    Ok(names.into_iter().collect::<Vec<_>>())
}

async fn execute_label_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_values_data(state, headers, name, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}

async fn label_values_data(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut values = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        if let Some(value) = labels.get(name) {
            values.insert(value.clone());
        }
    }

    Ok(values.into_iter().collect::<Vec<_>>())
}

fn metadata_time_range(params: &SeriesParams) -> Result<Option<TimeRange>, HttpQueryError> {
    if params.start.is_none() && params.end.is_none() && params.since.is_none() {
        return Ok(None);
    }

    let end = if params.start.is_none() && params.since.is_some() && params.end.is_none() {
        Some(current_unix_time_ns())
    } else {
        params.end
    };
    optional_start_end_range(params.start, params.since, end).map(Some)
}

fn metadata_index_range(params: &SeriesParams) -> Result<TimeRange, HttpQueryError> {
    let Some(time_range) = metadata_time_range(params)? else {
        let end_ns = current_unix_time_ns();
        return TimeRange::new(
            end_ns.saturating_sub(LOKI_METADATA_DEFAULT_INDEX_RANGE_NS),
            end_ns,
        )
        .map_err(HttpQueryError::from);
    };
    validate_loki_volume_query_range_limit(time_range)?;
    Ok(time_range)
}

async fn metadata_label_sets(
    state: &QuerierState,
    tenant: &str,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let time_range = metadata_time_range(params)?;
    let time_fingerprints = if let Some(time_range) = time_range {
        Some(metadata_fingerprints_in_time_range(state, tenant, time_range).await?)
    } else {
        None
    };

    let selectors = metadata_selectors(params)?;
    let mut label_sets = BTreeSet::new();

    for (fingerprint, labels) in state.label_index.tenant_series(tenant) {
        if time_fingerprints
            .as_ref()
            .is_none_or(|fingerprints| fingerprints.contains(&fingerprint))
            && metadata_labels_match_selectors(&labels, &selectors)
        {
            label_sets.insert(metadata_visible_labels(&labels));
        }
    }

    if let Some(hot_tail) = &state.hot_tail {
        // Prune to the metadata window when one is supplied; the per-record bound below
        // (`< start_ns || > end_ns`) is re-applied, so the windowed records are exactly
        // the records a full scan would have kept. With no time range, scan everything.
        let records = match time_range {
            Some(range) => hot_tail
                .source
                .records_in_range(range.start_ns, range.end_ns),
            None => hot_tail.source.records(),
        };
        let frontier = hot_tail.frontier.snapshot();
        for record in records {
            if record.tenant != tenant || frontier.is_compacted(&record) {
                continue;
            }
            if time_range.is_some_and(|range| {
                record.timestamp_ns < range.start_ns || record.timestamp_ns > range.end_ns
            }) {
                continue;
            }
            if metadata_labels_match_selectors(&record.labels, &selectors) {
                label_sets.insert(metadata_visible_labels(&record.labels));
            }
        }
    }

    Ok(label_sets.into_iter().collect())
}

async fn metadata_fingerprints_in_time_range(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
) -> Result<BTreeSet<SeriesFingerprint>, HttpQueryError> {
    let mut fingerprints = BTreeSet::new();
    for block in state.block_index.match_blocks(tenant, time_range, &[]) {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            match read_log_block(&state.root, &block.key) {
                Ok(rows) => rows,
                Err(BlockStoreError::Io(source)) if source.kind() == ErrorKind::NotFound => {
                    fingerprints.extend(block.fingerprints);
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        };
        fingerprints.extend(rows.into_iter().filter_map(|row| {
            (time_range.start_ns <= row.timestamp_ns && row.timestamp_ns <= time_range.end_ns)
                .then_some(row.series_fingerprint)
        }));
    }
    Ok(fingerprints)
}

fn metadata_visible_labels(labels: &Labels) -> Labels {
    let mut labels = labels.clone();
    labels.remove("detected_level");
    labels
}

fn metadata_labels_match_selectors(
    labels: &Labels,
    selectors: &[crabka_logql::StreamQuery],
) -> bool {
    if selectors.is_empty() {
        return true;
    }

    selectors.iter().any(|selector| {
        selector
            .matchers
            .iter()
            .all(|matcher| matcher.matches(labels))
    })
}

fn metadata_selectors(
    params: &SeriesParams,
) -> Result<Vec<crabka_logql::StreamQuery>, HttpQueryError> {
    params
        .matchers
        .iter()
        .map(|matcher| {
            parse_query(matcher).map_err(|source| HttpQueryError::LokiParse {
                query: matcher.clone(),
                source,
            })
        })
        .collect()
}

async fn execute_series_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(series_data(state, headers, params).await?))
}

async fn execute_api_prom_series_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(series_data(state, headers, params).await?))
}

async fn series_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    metadata_label_sets(&state, tenant, params).await
}

fn parse_series_params(raw_query: Option<&str>) -> Result<SeriesParams, HttpQueryError> {
    let mut params = SeriesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };

    for pair in split_query_param_pairs(
        raw_query,
        &["match[]", "match%5B%5D", "query", "start", "end", "since"],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "match[]" | "query" => params.matchers.push(value),
            "start" if params.start.is_none() => {
                params.start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if params.end.is_none() => {
                params.end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if params.since.is_none() => {
                params.since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            _ => {}
        }
    }

    Ok(params)
}

fn parse_query_params(raw_query: Option<&str>) -> Result<QueryParams, HttpQueryError> {
    let mut query = None;
    let mut time = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
    let mut step = None;
    let mut interval = None;
    let mut limit = None;
    let mut direction = None;
    let mut delay_for = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "time",
            "start",
            "end",
            "since",
            "step",
            "interval",
            "limit",
            "direction",
            "delay_for",
        ],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "time" if time.is_none() => {
                time = Some(parse_loki_timestamp_query_param("time", &value)?);
            }
            "start" if start.is_none() => {
                start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if end.is_none() => {
                end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if since.is_none() => {
                since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &value)?);
            }
            "interval" if interval.is_none() => {
                interval = Some(parse_loki_duration_query_param("interval", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "direction" if direction.is_none() => direction = Some(value),
            "delay_for" if delay_for.is_none() => {
                delay_for = Some(parse_loki_tail_delay_for_query_param(&value)?);
            }
            _ => {}
        }
    }

    Ok(QueryParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        time,
        start,
        end,
        since,
        step,
        interval,
        limit,
        direction,
        delay_for,
    })
}

fn split_query_param_pairs<'a>(raw_query: &'a str, known_keys: &[&str]) -> Vec<&'a str> {
    let mut pairs = Vec::new();
    let mut pair_start = 0;
    for (index, byte) in raw_query.bytes().enumerate() {
        if byte == b'&'
            && known_keys.iter().any(|key| {
                raw_query[index + 1..]
                    .strip_prefix(key)
                    .is_some_and(|rest| rest.starts_with('='))
            })
        {
            if pair_start != index {
                pairs.push(&raw_query[pair_start..index]);
            }
            pair_start = index + 1;
        }
    }
    if pair_start < raw_query.len() {
        pairs.push(&raw_query[pair_start..]);
    }
    pairs
}

fn parse_volume_params(raw_query: Option<&str>) -> Result<VolumeParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let mut limit = None;
    let mut target_labels = None;
    let mut aggregate_by = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "start",
            "end",
            "step",
            "limit",
            "targetLabels",
            "aggregateBy",
        ],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "start" if start.is_none() => {
                start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if end.is_none() => {
                end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "targetLabels" if target_labels.is_none() => {
                target_labels = Some(
                    value
                        .split(',')
                        .filter(|label| !label.is_empty())
                        .map(ToString::to_string)
                        .collect(),
                );
            }
            "aggregateBy" if aggregate_by.is_none() => {
                aggregate_by = Some(match value.as_str() {
                    "series" => VolumeAggregateBy::Series,
                    "labels" => VolumeAggregateBy::Labels,
                    _ => return Err(HttpQueryError::InvalidVolumeAggregation),
                });
            }
            _ => {}
        }
    }

    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start.unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE_NS));

    Ok(VolumeParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start,
        end,
        step,
        limit: limit.unwrap_or(100),
        target_labels,
        aggregate_by: aggregate_by.unwrap_or(VolumeAggregateBy::Series),
    })
}

fn parse_detected_fields_params(
    raw_query: Option<&str>,
) -> Result<DetectedFieldsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
    let mut step = None;
    let mut limit = None;
    let mut line_limit = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(
        raw_query,
        &[
            "query",
            "start",
            "end",
            "since",
            "step",
            "limit",
            "field_limit",
            "line_limit",
        ],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" if query.is_none() => query = Some(value),
            "start" if start.is_none() => {
                start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if end.is_none() => {
                end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if since.is_none() => {
                since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            "step" if step.is_none() => {
                step = Some(parse_loki_duration_query_param("step", &value)?);
            }
            "limit" if limit.is_none() => limit = Some(parse_usize_query_param("limit", &value)?),
            "field_limit" if limit.is_none() => {
                limit = Some(parse_usize_query_param("field_limit", &value)?);
            }
            "line_limit" if line_limit.is_none() => {
                line_limit = Some(parse_usize_query_param("line_limit", &value)?);
            }
            _ => {}
        }
    }

    if let Some(step) = step
        && step <= 0
    {
        return Err(HttpQueryError::InvalidStep);
    }
    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start_or_since(start, since, Some(end))?
        .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE_NS));

    Ok(DetectedFieldsParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start,
        end,
        limit: limit.unwrap_or(1000),
        line_limit: line_limit.unwrap_or(100),
    })
}

fn parse_detected_labels_params(
    raw_query: Option<&str>,
) -> Result<DetectedLabelsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut since = None;
    let mut limit = None;

    if let Some(raw_query) = raw_query {
        for pair in split_query_param_pairs(
            raw_query,
            &[
                "query",
                "start",
                "end",
                "since",
                "limit",
                "field_limit",
                "step",
            ],
        ) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_form_component(key)?;
            let value = decode_form_component(value)?;

            match key.as_str() {
                // Grafana's Logs Drilldown sends `detected_labels?query=` (empty)
                // on load to discover all labels. Treat an empty/blank query as
                // "match all streams" (None) — the same as Loki — instead of
                // parsing "" as a stream selector, which fails with
                // `parse error: syntax error: unexpected $end, expecting '{'`.
                // `execute_detected_labels_query` already maps `None` to no
                // matchers (all series).
                "query" if query.is_none() && !value.trim().is_empty() => query = Some(value),
                "start" if start.is_none() => {
                    start = Some(parse_loki_timestamp_query_param("start", &value)?);
                }
                "end" if end.is_none() => {
                    end = Some(parse_loki_timestamp_query_param("end", &value)?);
                }
                "since" if since.is_none() => {
                    since = Some(parse_loki_duration_query_param("since", &value)?);
                }
                "limit" | "field_limit" if limit.is_none() => {
                    limit = parse_usize_query_param("limit", &value).ok().or(limit);
                }
                _ => {}
            }
        }
    }

    let end = end.unwrap_or_else(current_unix_time_ns);
    let start = start_or_since(start, since, Some(end))?
        .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE_NS));

    Ok(DetectedLabelsParams {
        query,
        start,
        end,
        limit: limit.unwrap_or(1000),
    })
}

fn parse_patterns_params(raw_query: Option<&str>) -> Result<PatternsParams, HttpQueryError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(raw_query, &["query", "start", "end", "step"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "query" => query = Some(value),
            "start" => start = Some(parse_loki_timestamp_query_param("start", &value)?),
            "end" => end = Some(parse_loki_timestamp_query_param("end", &value)?),
            "step" => step = Some(parse_loki_duration_query_param("step", &value)?),
            _ => {}
        }
    }

    Ok(PatternsParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start: start.ok_or(HttpQueryError::MissingQueryParameter("start"))?,
        end: end.ok_or(HttpQueryError::MissingQueryParameter("end"))?,
        step: step.unwrap_or(1_000_000_000),
    })
}

fn parse_loki_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(timestamp_ns) = value.parse::<i64>() {
        return Ok(timestamp_ns);
    }

    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns);
    }

    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .and_then(|timestamp| i64::try_from(timestamp.unix_timestamp_nanos()).ok())
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}

fn parse_loki_duration_query_param(name: &'static str, value: &str) -> Result<i64, HttpQueryError> {
    let duration = if let Ok(seconds) = value.parse::<i64>() {
        seconds.checked_mul(1_000_000_000).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })?
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        duration_ns
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            if name == "since" {
                HttpQueryError::InvalidSinceQueryParameter {
                    value: value.to_string(),
                }
            } else {
                HttpQueryError::InvalidDurationQueryParameter {
                    value: value.to_string(),
                }
            }
        })?
    };

    if name == "since" && duration <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: value.to_string(),
        });
    }

    Ok(duration)
}

fn parse_loki_tail_delay_for_query_param(value: &str) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        seconds
            .checked_mul(1_000_000_000)
            .ok_or_else(|| HttpQueryError::InvalidQueryParameter {
                name: "delay_for",
                value: value.to_string(),
            })
    } else if let Some(duration_ns) = parse_decimal_seconds_timestamp(value) {
        Ok(duration_ns)
    } else {
        parse_prometheus_duration(value).ok_or_else(|| {
            HttpQueryError::InvalidDurationQueryParameter {
                value: value.to_string(),
            }
        })
    }
}

fn validate_loki_tail_delay_for(delay_for: i64) -> Result<(), HttpQueryError> {
    if !(0..=LOKI_MAX_TAIL_DELAY_NS).contains(&delay_for) {
        return Err(HttpQueryError::TailDelayForTooLarge);
    }

    Ok(())
}

fn parse_prometheus_duration(value: &str) -> Option<i64> {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut seen_units = 0_u16;
    let mut total_ns = 0_i128;

    while pos < value.len() {
        let amount_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == amount_start {
            return None;
        }
        let amount = value[amount_start..pos].parse::<i128>().ok()?;

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let (unit_order, unit_bit, multiplier) = prometheus_duration_unit(&value[unit_start..pos])?;
        if seen_units & unit_bit != 0 {
            return None;
        }
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        seen_units |= unit_bit;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}

fn prometheus_duration_unit(unit: &str) -> Option<(u8, u16, i128)> {
    match unit {
        "y" => Some((0, 1 << 0, 31_536_000_000_000_000)),
        "w" => Some((1, 1 << 1, 604_800_000_000_000)),
        "d" => Some((2, 1 << 2, 86_400_000_000_000)),
        "h" => Some((3, 1 << 3, 3_600_000_000_000)),
        "m" => Some((4, 1 << 4, 60_000_000_000)),
        "s" => Some((5, 1 << 5, 1_000_000_000)),
        "ms" => Some((6, 1 << 6, 1_000_000)),
        "us" => Some((7, 1 << 7, 1_000)),
        "ns" => Some((8, 1 << 8, 1)),
        _ => None,
    }
}

fn parse_decimal_seconds_timestamp(value: &str) -> Option<i64> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let (seconds, fraction) = unsigned.split_once('.')?;
    if seconds.is_empty() && fraction.is_empty() {
        return None;
    }
    if !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let seconds = if seconds.is_empty() {
        0
    } else {
        seconds.parse::<i128>().ok()?
    };
    let mut fraction_ns = 0_i128;
    let mut scale = 100_000_000_i128;
    for digit in fraction.bytes().take(9) {
        fraction_ns += i128::from(digit - b'0') * scale;
        scale /= 10;
    }

    let timestamp_ns = seconds
        .checked_mul(1_000_000_000)?
        .checked_add(fraction_ns)?;
    let timestamp_ns = if negative {
        timestamp_ns.checked_neg()?
    } else {
        timestamp_ns
    };
    i64::try_from(timestamp_ns).ok()
}

fn parse_usize_query_param(name: &'static str, value: &str) -> Result<usize, HttpQueryError> {
    if name == "limit" {
        let limit = value
            .parse::<i64>()
            .map_err(|_| HttpQueryError::InvalidLimit(value.to_string()))?;
        if limit <= 0 {
            return Err(HttpQueryError::LimitNotPositive);
        }
        return usize::try_from(limit).map_err(|_| HttpQueryError::InvalidLimit(value.to_string()));
    }

    value
        .parse()
        .map_err(|_| HttpQueryError::InvalidQueryParameter {
            name,
            value: value.to_string(),
        })
}

fn decode_form_component(component: &str) -> Result<String, HttpQueryError> {
    let mut bytes = Vec::with_capacity(component.len());
    let mut iter = component.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                let low = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                bytes.push(high << 4 | low);
            }
            _ => bytes.push(byte),
        }
    }

    String::from_utf8(bytes).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn grpc_tenant(metadata: &tonic::metadata::MetadataMap) -> Result<&str, tonic::Status> {
    metadata
        .get("x-scope-orgid")
        .ok_or_else(|| tonic::Status::invalid_argument("missing tenant header"))?
        .to_str()
        .map_err(|_| tonic::Status::invalid_argument("invalid tenant header"))
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(tonic::Status::invalid_argument("invalid tenant header"))
            } else {
                Ok(tenant)
            }
        })
}

async fn authorized_tenant<'a>(
    state: &QuerierState,
    headers: &'a HeaderMap,
) -> Result<&'a str, HttpQueryError> {
    let tenant = tenant(headers)?;
    state.query_authorizer.check(tenant).await?;
    Ok(tenant)
}

async fn authorized_tenants(
    state: &QuerierState,
    headers: &HeaderMap,
) -> Result<Vec<String>, HttpQueryError> {
    let header = tenant(headers)?;
    let tenants = header
        .split('|')
        .map(str::trim)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tenants.iter().any(String::is_empty) {
        return Err(HttpQueryError::InvalidTenant);
    }
    for tenant in &tenants {
        state.query_authorizer.check(tenant).await?;
    }
    Ok(tenants)
}

fn tenant(headers: &HeaderMap) -> Result<&str, HttpQueryError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(HttpQueryError::MissingTenant)?
        .to_str()
        .map_err(|_| HttpQueryError::InvalidTenant)
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant)
            }
        })
}

fn time_range(params: &QueryParams, kind: QueryKind) -> Result<TimeRange, HttpQueryError> {
    match kind {
        QueryKind::Instant => {
            if let Some(time) = params.time {
                TimeRange::new(time, time).map_err(HttpQueryError::from)
            } else {
                optional_start_end_range(params.start, params.since, params.end)
            }
        }
        QueryKind::Range => {
            let end = params.end.unwrap_or_else(current_unix_time_ns);
            let start = start_or_since(params.start, params.since, Some(end))?
                .unwrap_or_else(|| end.saturating_sub(LOKI_DEFAULT_QUERY_RANGE_NS));
            TimeRange::new(start, end).map_err(HttpQueryError::from)
        }
    }
}

const LOKI_DEFAULT_QUERY_RANGE_NS: i64 = 3_600_000_000_000;
const LOKI_METADATA_DEFAULT_INDEX_RANGE_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
const LOKI_DEFAULT_TAIL_LIMIT: usize = 100;
const LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS: i64 = 11_000;
const LOKI_MAX_TAIL_DELAY_NS: i64 = 5_000_000_000;
const LOKI_VOLUME_MAX_QUERY_RANGE_NS: i64 = 2_595_600_000_000_000;

fn current_unix_time_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}

fn optional_start_end_range(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<TimeRange, HttpQueryError> {
    let start = start_or_since(start, since, end)?.unwrap_or(i64::MIN);
    TimeRange::new(start, end.unwrap_or(i64::MAX)).map_err(HttpQueryError::from)
}

fn start_or_since(
    start: Option<i64>,
    since: Option<i64>,
    end: Option<i64>,
) -> Result<Option<i64>, HttpQueryError> {
    if start.is_some() {
        return Ok(start);
    }
    let Some(since) = since else {
        return Ok(None);
    };
    if since <= 0 {
        return Err(HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        });
    }
    let Some(end) = end else {
        return Ok(None);
    };
    end.checked_sub(since)
        .map(Some)
        .ok_or_else(|| HttpQueryError::InvalidSinceQueryParameter {
            value: since.to_string(),
        })
}

#[derive(Clone, Copy)]
enum QueryKind {
    Instant,
    Range,
}

#[derive(Clone, Copy)]
enum LokiDirection {
    Forward,
    Backward,
}

fn loki_direction(direction: Option<&str>) -> Result<LokiDirection, HttpQueryError> {
    match direction {
        None | Some("backward") => Ok(LokiDirection::Backward),
        Some("forward") => Ok(LokiDirection::Forward),
        Some(value) => Err(HttpQueryError::InvalidDirection(value.to_string())),
    }
}

pub async fn execute_stream_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_with_deletes(root, plan, label_index, &[]).await
}

async fn execute_stream_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
        delete_filters,
    )
    .await
}

pub async fn execute_stream_query_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
        &[],
    )
    .await
}

pub async fn execute_stream_query_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_stream_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

async fn execute_stream_query_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
        let sql = stream_plan_scan_sql(plan);
        let batches = ctx.sql(&sql).await?.collect().await?;
        append_matching_log_batches(&mut streams, plan, label_index, &batches, delete_filters)?;
    }

    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    Ok(loki_streams_response(streams))
}

pub async fn execute_stream_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_stream_query_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
        &[],
    )
    .await
}

struct ObjectStoreStreamScan {
    value: Value,
    scanned_blocks: Vec<BlockDescriptor>,
}

#[derive(Clone, Copy)]
struct StreamScanOptions {
    direction: LokiDirection,
    limit: Option<usize>,
    end_exclusive: Option<i64>,
    allow_limit_short_circuit: bool,
}

impl StreamScanOptions {
    fn exhaustive() -> Self {
        Self {
            direction: LokiDirection::Forward,
            limit: None,
            end_exclusive: None,
            allow_limit_short_circuit: false,
        }
    }

    fn from_stream_options(
        direction: LokiDirection,
        limit: Option<usize>,
        interval: Option<i64>,
        end_exclusive: Option<i64>,
    ) -> Self {
        Self {
            direction,
            limit,
            end_exclusive,
            allow_limit_short_circuit: limit.is_some() && interval.is_none(),
        }
    }

    fn reached_limit(self, streams: &BTreeMap<Labels, Vec<[String; 2]>>) -> bool {
        self.allow_limit_short_circuit
            && self
                .limit
                .is_some_and(|limit| count_stream_map_lines(streams, self.end_exclusive) >= limit)
    }

    fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit {
            return OBJECT_STORE_STREAM_BLOCK_FETCH_CONCURRENCY;
        }
        self.limit
            .map(|limit| OBJECT_STORE_STREAM_BLOCK_FETCH_CONCURRENCY.min(limit.max(1)))
            .unwrap_or(OBJECT_STORE_STREAM_BLOCK_FETCH_CONCURRENCY)
    }
}

fn count_stream_map_lines(
    streams: &BTreeMap<Labels, Vec<[String; 2]>>,
    end_exclusive: Option<i64>,
) -> usize {
    streams
        .values()
        .map(|values| {
            values
                .iter()
                .filter(|entry| {
                    end_exclusive.is_none_or(|end_exclusive| {
                        entry[0]
                            .parse::<i64>()
                            .map_or(true, |timestamp| timestamp < end_exclusive)
                    })
                })
                .count()
        })
        .fold(0_usize, usize::saturating_add)
}

fn object_store_stream_blocks_in_scan_order(
    blocks: &[BlockDescriptor],
    direction: LokiDirection,
) -> Vec<&BlockDescriptor> {
    let mut blocks = blocks.iter().collect::<Vec<_>>();
    match direction {
        LokiDirection::Forward => {
            blocks.sort_by_key(|block| {
                (
                    block.key.time_range.start_ns,
                    block.key.time_range.end_ns,
                    block.key.partition,
                    block.key.first_offset,
                )
            });
        }
        LokiDirection::Backward => {
            blocks.sort_by_key(|block| {
                (
                    std::cmp::Reverse(block.key.time_range.end_ns),
                    std::cmp::Reverse(block.key.time_range.start_ns),
                    std::cmp::Reverse(block.key.partition),
                    std::cmp::Reverse(block.key.last_offset),
                )
            });
        }
    }
    blocks
}

#[must_use]
pub fn stream_plan_scan_sql(plan: &StreamPlan) -> String {
    stream_plan_scan_sql_for_time_range(plan, plan.time_range)
}

#[must_use]
pub fn metric_plan_scan_sql(
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<String, QueryError> {
    let scan_range = metric_scan_range(query, eval_range)?;
    Ok(stream_plan_scan_sql_for_time_range(plan, scan_range))
}

fn metric_scan_range(query: &MetricQuery, eval_range: TimeRange) -> Result<TimeRange, QueryError> {
    let scan_end_ns = eval_range.end_ns.saturating_sub(query.offset_ns.0);
    let scan_start_ns = eval_range
        .start_ns
        .saturating_sub(query.offset_ns.0)
        .saturating_sub(query.range_ns.0);
    Ok(TimeRange::new(scan_start_ns, scan_end_ns)?)
}

fn stream_plan_scan_sql_for_time_range(plan: &StreamPlan, time_range: TimeRange) -> String {
    let mut predicates = vec![format!(
        "timestamp_ns >= {} and timestamp_ns <= {}",
        time_range.start_ns, time_range.end_ns
    )];
    if !plan.fingerprints.is_empty() {
        let fingerprints = plan
            .fingerprints
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!("series_fingerprint in ({fingerprints})"));
    }
    predicates.extend(literal_line_filter_sql_predicates(&plan.query.pipeline));
    format!(
        "select series_fingerprint, timestamp_ns, line, structured_metadata \
         from logs \
         where {} \
         order by series_fingerprint, timestamp_ns",
        predicates.join(" and ")
    )
}

fn literal_line_filter_sql_predicates(pipeline: &[PipelineStage]) -> Vec<String> {
    let mut predicates = Vec::new();
    for stage in pipeline {
        if stage.mutates_line() {
            break;
        }
        if let Some(predicate) = {
            let PipelineStage::LineFilter(filter) = stage else {
                continue;
            };
            if filter.is_ip_matcher() {
                continue;
            }
            match filter.op {
                LineFilterOp::Contains => Some(format!(
                    "line like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::NotContains => Some(format!(
                    "line not like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::Regex
                | LineFilterOp::NotRegex
                | LineFilterOp::Pattern
                | LineFilterOp::NotPattern => None,
            }
        } {
            predicates.push(predicate);
        }
    }
    predicates
}

fn sql_like_pattern_literal(value: &str) -> String {
    sql_string_literal(value)
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

async fn execute_stream_query_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    Ok(
        execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            store,
            prefix,
            plan,
            label_index,
            hot_tail,
            frontier,
            delete_filters,
            StreamScanOptions::exhaustive(),
        )
        .await?
        .value,
    )
}

#[allow(clippy::too_many_arguments)]
async fn execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    options: StreamScanOptions,
) -> Result<ObjectStoreStreamScan, QueryError> {
    if plan.blocks.is_empty() || plan.fingerprints.is_empty() {
        let mut streams = BTreeMap::new();
        for record in hot_tail {
            append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
        }
        sort_loki_stream_values(&mut streams);
        return Ok(ObjectStoreStreamScan {
            value: loki_streams_response(streams),
            scanned_blocks: Vec::new(),
        });
    }

    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut scanned_blocks = Vec::new();

    if matches!(options.direction, LokiDirection::Backward) {
        for record in hot_tail {
            append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
        }
    }

    if !options.reached_limit(&streams) {
        let ordered_blocks =
            object_store_stream_blocks_in_scan_order(&plan.blocks, options.direction);
        for block_batch in ordered_blocks.chunks(options.block_fetch_concurrency()) {
            if options.reached_limit(&streams) {
                break;
            }
            let results = futures_util::future::join_all(block_batch.iter().map(|block| {
                let store = Arc::clone(&store);
                let block = *block;
                async move {
                    let result =
                        collect_object_store_stream_log_batches(store, prefix, block, plan).await;
                    (block, result)
                }
            }))
            .await;

            for (block, result) in results {
                scanned_blocks.push(block.clone());
                let Ok(batches) = result else {
                    warnings.push(format!("failed to read block {}", block.key.object_key()));
                    continue;
                };
                append_matching_log_batches(
                    &mut streams,
                    plan,
                    label_index,
                    &batches,
                    delete_filters,
                )?;
            }
        }
    }

    if matches!(options.direction, LokiDirection::Forward) && !options.reached_limit(&streams) {
        for record in hot_tail {
            append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
        }
    }
    sort_loki_stream_values(&mut streams);

    Ok(ObjectStoreStreamScan {
        value: loki_streams_response_with_warnings(streams, &warnings),
        scanned_blocks,
    })
}

async fn collect_object_store_stream_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
) -> Result<Vec<RecordBatch>, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(
        &ctx,
        "logs",
        store,
        prefix.clone(),
        std::slice::from_ref(block),
    )?;
    Ok(ctx
        .sql(&stream_plan_scan_sql(plan))
        .await?
        .collect()
        .await?)
}

async fn collect_object_store_metric_log_batches(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    block: &BlockDescriptor,
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<Vec<RecordBatch>, QueryError> {
    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(
        &ctx,
        "logs",
        store,
        prefix.clone(),
        std::slice::from_ref(block),
    )?;
    Ok(ctx
        .sql(&metric_plan_scan_sql(plan, query, eval_range)?)
        .await?
        .collect()
        .await?)
}

fn append_matching_log_batches(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    batches: &[RecordBatch],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_log_row(
                streams,
                plan,
                label_index,
                fingerprints.value(row),
                timestamps.value(row),
                lines.value(row),
                &structured_metadata,
                delete_filters,
            )?;
        }
    }
    Ok(())
}

#[must_use]
pub fn execute_tail_query(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Value {
    execute_tail_query_with_frontier(
        plan,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
}

#[must_use]
pub fn execute_tail_query_with_frontier(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    execute_tail_query_with_frontier_and_deletes(plan, hot_tail, frontier, &[])
}

fn execute_tail_query_with_frontier_and_deletes(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Value {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    json!({
        "streams": streams
            .into_iter()
            .map(|(stream, values)| json!({
                "stream": stream,
                "values": values,
            }))
            .collect::<Vec<_>>()
    })
}

pub async fn execute_metric_query(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_metric_query_with_deletes(root, plan, query, label_index, &[]).await
}

async fn execute_metric_query_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        1,
        delete_filters,
    )
    .await
}

pub async fn execute_metric_query_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_with_hot_tail_frontier(
        root,
        plan,
        query,
        label_index,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
    .await
}

pub async fn execute_metric_query_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

async fn execute_metric_query_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        1,
        hot_tail,
        frontier,
        delete_filters,
    )
    .await
}

pub async fn execute_metric_query_range(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        &[],
    )
    .await
}

async fn execute_metric_query_range_with_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        &[],
        &CompactionFrontier::new(i64::MAX),
        delete_filters,
    )
    .await
}

pub async fn execute_metric_query_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        query,
        label_index,
        &[],
        &CompactionFrontier::new(i64::MAX),
    )
    .await
}

async fn execute_metric_query_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

async fn execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    let eval_range = TimeRange::new(plan.time_range.end_ns, plan.time_range.end_ns)?;
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        eval_range,
        1,
        hot_tail,
        frontier,
        delete_filters,
    )
    .await
}

pub async fn execute_metric_query_range_from_object_store(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier(
        store,
        prefix,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        &[],
        &CompactionFrontier::new(i64::MAX),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_metric_query_range_with_hot_tail(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_metric_query_range_with_hot_tail_frontier(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_range_with_hot_tail_frontier_and_deletes(
        root,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_metric_query_range_with_hot_tail_frontier_and_deletes(
    root: impl AsRef<FsPath>,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    if step_ns <= 0 {
        return Err(QueryError::InvalidStep(step_ns));
    }

    let eval_times = eval_times(eval_range, step_ns);
    let mut samples = BTreeMap::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        let ctx = SessionContext::new();
        register_log_blocks(&ctx, "logs", root, &plan.blocks)?;
        let sql = metric_plan_scan_sql(plan, query, eval_range)?;
        let batches = ctx.sql(&sql).await?.collect().await?;
        samples = metric_samples_from_batches(
            &batches,
            plan,
            query,
            label_index,
            &eval_times,
            delete_filters,
        )?;
    }

    for record in hot_tail {
        append_matching_hot_metric_record(
            &mut samples,
            plan,
            query,
            record,
            frontier,
            &eval_times,
            query.range_ns.0,
            delete_filters,
        )?;
    }
    apply_absent_over_time(&mut samples, query, &eval_times);

    Ok(loki_matrix_response(format_metric_samples(samples, query)))
}

#[allow(clippy::too_many_arguments)]
async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Result<Value, QueryError> {
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
        store,
        prefix,
        plan,
        query,
        label_index,
        eval_range,
        step_ns,
        hot_tail,
        frontier,
        &[],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
    store: Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_range: TimeRange,
    step_ns: i64,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, QueryError> {
    if step_ns <= 0 {
        return Err(QueryError::InvalidStep(step_ns));
    }

    let eval_times = eval_times(eval_range, step_ns);
    let mut samples = BTreeMap::new();
    let mut warnings = Vec::new();

    if !plan.blocks.is_empty() && !plan.fingerprints.is_empty() {
        for block in &plan.blocks {
            let Ok(batches) = collect_object_store_metric_log_batches(
                Arc::clone(&store),
                prefix,
                block,
                plan,
                query,
                eval_range,
            )
            .await
            else {
                warnings.push(format!("failed to read block {}", block.key.object_key()));
                continue;
            };
            let block_samples = metric_samples_from_batches(
                &batches,
                plan,
                query,
                label_index,
                &eval_times,
                delete_filters,
            )?;
            merge_metric_samples(&mut samples, block_samples);
        }
    }

    for record in hot_tail {
        append_matching_hot_metric_record(
            &mut samples,
            plan,
            query,
            record,
            frontier,
            &eval_times,
            query.range_ns.0,
            delete_filters,
        )?;
    }
    apply_absent_over_time(&mut samples, query, &eval_times);

    Ok(loki_matrix_response_with_warnings(
        format_metric_samples(samples, query),
        &warnings,
    ))
}

type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;
type FormattedMetricSeries = Vec<(Labels, Vec<[String; 2]>)>;

fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for (labels, values) in block_samples {
        let target = samples.entry(labels).or_default();
        for (timestamp_ns, value) in values {
            let sample = target.entry(timestamp_ns).or_default();
            sample.merge(value);
        }
    }
}

fn apply_absent_over_time(samples: &mut MetricSamples, query: &MetricQuery, eval_times: &[i64]) {
    if !matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return;
    }

    let mut absent_values = BTreeMap::new();
    for eval_time_ns in eval_times {
        let has_sample = samples.values().any(|values| {
            values
                .get(eval_time_ns)
                .is_some_and(MetricSampleState::has_samples)
        });
        if !has_sample {
            let mut sample = MetricSampleState::default();
            sample.record(*eval_time_ns, MetricValue::integer(1));
            absent_values.insert(*eval_time_ns, sample);
        }
    }

    samples.clear();
    if !absent_values.is_empty() {
        samples.insert(absent_metric_labels(query), absent_values);
    }
}

fn absent_metric_labels(query: &MetricQuery) -> Labels {
    query
        .stream
        .matchers
        .iter()
        .filter(|matcher| matcher.op == MatchOp::Equal)
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<Labels>()
}

fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_metric_row(
                &mut samples,
                plan,
                label_index,
                query,
                QueryRow {
                    fingerprint: fingerprints.value(row),
                    timestamp_ns: timestamps.value(row),
                    line: lines.value(row),
                    structured_metadata: &structured_metadata,
                },
                eval_times,
                query.range_ns.0,
                delete_filters,
            )?;
        }
    }

    Ok(samples)
}

fn format_metric_samples(samples: MetricSamples, query: &MetricQuery) -> FormattedMetricSeries {
    let samples = if let Some(grouping) = &query.range_grouping {
        group_range_samples(samples, grouping)
    } else {
        samples
    };

    if let Some(vector_aggregation) = &query.vector_aggregation {
        let mut series = aggregate_vector_samples(samples, query, vector_aggregation)
            .into_iter()
            .map(|(labels, values)| {
                (
                    labels,
                    values
                        .into_iter()
                        .map(|(time, value)| [time.to_string(), format_metric_value(value)])
                        .collect(),
                )
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| {
                        [
                            time.to_string(),
                            format_metric_value(range_sample_value(value, query)),
                        ]
                    })
                    .collect(),
            )
        })
        .collect()
}

fn sort_formatted_vector_samples(series: &mut FormattedMetricSeries, op: &VectorAggregationOp) {
    match op {
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            series.sort_by(|left, right| {
                let left_value = left
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let right_value = right
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let value_order = match op {
                    VectorAggregationOp::Sort => left_value.cmp_value(right_value),
                    VectorAggregationOp::SortDesc => right_value.cmp_value(left_value),
                    _ => Ordering::Equal,
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
        }
        _ => {}
    }
}

fn group_range_samples(samples: MetricSamples, grouping: &VectorGrouping) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for (time, value) in values {
            grouped_values.entry(time).or_default().merge(value);
        }
    }

    grouped
}

fn aggregate_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    vector_aggregation: &VectorAggregation,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    match &vector_aggregation.op {
        VectorAggregationOp::TopK(limit) | VectorAggregationOp::ApproxTopK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Largest,
            );
        }
        VectorAggregationOp::BottomK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Smallest,
            );
        }
        VectorAggregationOp::CountValues(label) => {
            return count_values_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                label,
            );
        }
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            return select_all_vector_samples(samples, query);
        }
        _ => {}
    }

    let mut states: BTreeMap<Labels, BTreeMap<i64, VectorAggregationState>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, vector_aggregation.grouping.as_ref());
        for (time, value) in values {
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(value, query));
        }
    }

    states
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, state)| (time, state.finish(&vector_aggregation.op)))
                    .collect(),
            )
        })
        .collect()
}

fn count_values_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    value_label: &str,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut counted: BTreeMap<Labels, BTreeMap<i64, u64>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            let value = range_sample_value(value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    counted
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, count)| (time, MetricValue::integer(count)))
                    .collect(),
            )
        })
        .collect()
}

fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| (time, range_sample_value(value, query)))
                    .collect(),
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
enum VectorSelection {
    Largest,
    Smallest,
}

fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<(Labels, MetricValue)>>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push((labels.clone(), range_sample_value(value, query)));
        }
    }

    let mut selected = BTreeMap::new();
    for (_grouped_labels, values) in groups {
        for (time, mut candidates) in values {
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.1.cmp_value(left.1),
                    VectorSelection::Smallest => left.1.cmp_value(right.1),
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
            for (labels, value) in candidates.into_iter().take(limit as usize) {
                selected
                    .entry(labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, value);
            }
        }
    }

    selected
}

fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(VectorGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => Labels::new(),
    }
}

fn range_sample_value(value: MetricSampleState, query: &MetricQuery) -> MetricValue {
    match query.aggregation {
        RangeAggregation::CountOverTime
        | RangeAggregation::BytesOverTime
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::SumOverTime => value.sum,
        RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::Rate | RangeAggregation::BytesRate => {
            rate_metric_value(value.sum, query.range_ns.0)
        }
        RangeAggregation::RateCounter => {
            rate_metric_value(value.counter_increase(), query.range_ns.0)
        }
        RangeAggregation::AvgOverTime => value.average(),
        RangeAggregation::StdvarOverTime => value.stdvar(),
        RangeAggregation::StddevOverTime => value.stddev(),
        RangeAggregation::QuantileOverTime(quantile) => value.quantile(quantile),
        RangeAggregation::MinOverTime => value.min.unwrap_or_else(MetricValue::zero),
        RangeAggregation::MaxOverTime => value.max.unwrap_or_else(MetricValue::zero),
        RangeAggregation::FirstOverTime => value
            .first
            .map(|(_, value)| value)
            .unwrap_or_else(MetricValue::zero),
        RangeAggregation::LastOverTime => value
            .last
            .map(|(_, value)| value)
            .unwrap_or_else(MetricValue::zero),
    }
}

fn is_unwrapped_metric_query(query: &MetricQuery) -> bool {
    query
        .stream
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetricValue {
    numerator: i128,
    denominator: u128,
}

const METRIC_DECIMAL_SCALE: u128 = 1_000_000_000;

impl MetricValue {
    fn zero() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    fn integer(value: u64) -> Self {
        Self::new(i128::from(value), 1)
    }

    fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self::zero();
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).expect("gcd fits in i128"),
            denominator: denominator / divisor,
        }
    }

    fn add(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                + other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    fn subtract(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                - other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    fn multiply(self, other: Self) -> Self {
        Self::new(
            self.numerator * other.numerator,
            self.denominator * other.denominator,
        )
    }

    fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
    }

    fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }
        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    fn saturating_sub(self, other: Self) -> Self {
        if self.cmp_value(other) == Ordering::Less {
            Self::zero()
        } else {
            Self::new(
                self.numerator
                    * i128::try_from(other.denominator).expect("denominator fits in i128")
                    - other.numerator
                        * i128::try_from(self.denominator).expect("denominator fits in i128"),
                self.denominator * other.denominator,
            )
        }
    }

    fn divide_by(self, divisor: u64) -> Self {
        if divisor == 0 {
            Self::zero()
        } else {
            Self::new(self.numerator, self.denominator * u128::from(divisor))
        }
    }

    fn sqrt(self) -> Self {
        let value = (self.numerator as f64 / self.denominator as f64).sqrt();
        if !value.is_finite() || value <= 0.0 {
            return Self::zero();
        }

        Self::new(
            (value * METRIC_DECIMAL_SCALE as f64).floor() as i128,
            METRIC_DECIMAL_SCALE,
        )
    }

    fn cmp_value(self, other: Self) -> Ordering {
        (self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")).cmp(
            &(other.numerator
                * i128::try_from(self.denominator).expect("denominator fits in i128")),
        )
    }

    fn to_f64(self) -> Option<f64> {
        let value = self.numerator as f64 / self.denominator as f64;
        value.is_finite().then_some(value)
    }

    fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE as f64).round();
        if scaled < i128::MIN as f64 || scaled > i128::MAX as f64 {
            return None;
        }
        Some(Self::new(scaled as i128, METRIC_DECIMAL_SCALE))
    }
}

impl Default for MetricValue {
    fn default() -> Self {
        Self::zero()
    }
}

#[derive(Clone, Debug, Default)]
struct MetricSampleState {
    count: u64,
    sum: MetricValue,
    sum_squares: MetricValue,
    min: Option<MetricValue>,
    max: Option<MetricValue>,
    first: Option<(i64, MetricValue)>,
    last: Option<(i64, MetricValue)>,
    values: Vec<MetricValue>,
    values_by_time: BTreeMap<i64, MetricValue>,
}

impl MetricSampleState {
    fn has_samples(&self) -> bool {
        self.count > 0
    }

    fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
        self.count += 1;
        self.sum = self.sum.add(value);
        self.sum_squares = self.sum_squares.add(value.multiply(value));
        self.min = Some(self.min.map_or(value, |min| {
            if value.cmp_value(min) == Ordering::Less {
                value
            } else {
                min
            }
        }));
        self.max = Some(self.max.map_or(value, |max| {
            if value.cmp_value(max) == Ordering::Greater {
                value
            } else {
                max
            }
        }));
        self.first = Some(self.first.map_or((timestamp_ns, value), |first| {
            if timestamp_ns < first.0 {
                (timestamp_ns, value)
            } else {
                first
            }
        }));
        self.last = Some(self.last.map_or((timestamp_ns, value), |last| {
            if timestamp_ns > last.0 {
                (timestamp_ns, value)
            } else {
                last
            }
        }));
        self.values.push(value);
        self.values_by_time
            .entry(timestamp_ns)
            .and_modify(|current| *current = (*current).add(value))
            .or_insert(value);
    }

    fn merge(&mut self, other: Self) {
        self.count = self.count.saturating_add(other.count);
        self.sum = self.sum.add(other.sum);
        self.sum_squares = self.sum_squares.add(other.sum_squares);
        if let Some(min) = other.min {
            self.min = Some(self.min.map_or(min, |current| {
                if min.cmp_value(current) == Ordering::Less {
                    min
                } else {
                    current
                }
            }));
        }
        if let Some(max) = other.max {
            self.max = Some(self.max.map_or(max, |current| {
                if max.cmp_value(current) == Ordering::Greater {
                    max
                } else {
                    current
                }
            }));
        }
        if let Some(first) = other.first {
            self.first =
                Some(self.first.map_or(
                    first,
                    |current| {
                        if first.0 < current.0 { first } else { current }
                    },
                ));
        }
        if let Some(last) = other.last {
            self.last =
                Some(self.last.map_or(
                    last,
                    |current| {
                        if last.0 > current.0 { last } else { current }
                    },
                ));
        }
        self.values.extend(other.values);
        for (timestamp_ns, value) in other.values_by_time {
            self.values_by_time
                .entry(timestamp_ns)
                .and_modify(|current| *current = (*current).add(value))
                .or_insert(value);
        }
    }

    fn average(self) -> MetricValue {
        self.sum.divide_by(self.count)
    }

    fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }

    fn stddev(self) -> MetricValue {
        self.stdvar().sqrt()
    }

    fn quantile(mut self, quantile: Quantile) -> MetricValue {
        if self.values.is_empty() {
            return MetricValue::zero();
        }
        self.values.sort_by(|left, right| left.cmp_value(*right));
        if self.values.len() == 1 {
            return self.values[0];
        }

        let scaled_rank =
            u128::from(quantile.numerator.0) * u128::try_from(self.values.len() - 1).unwrap();
        let denominator = u128::from(quantile.denominator.0);
        let lower_index = usize::try_from(scaled_rank / denominator).unwrap();
        let rank_remainder = scaled_rank % denominator;
        if rank_remainder == 0 {
            return self.values[lower_index];
        }

        let upper_index = lower_index + 1;
        let fraction = MetricValue::new(
            i128::try_from(rank_remainder).expect("quantile rank remainder fits in i128"),
            denominator,
        );
        self.values[lower_index].add(
            self.values[upper_index]
                .saturating_sub(self.values[lower_index])
                .multiply(fraction),
        )
    }

    fn counter_increase(self) -> MetricValue {
        let mut values = self.values_by_time.into_values();
        let Some(mut previous) = values.next() else {
            return MetricValue::zero();
        };
        let mut increase = MetricValue::zero();
        for value in values {
            increase = if value.cmp_value(previous) == Ordering::Less {
                increase.add(value)
            } else {
                increase.add(value.saturating_sub(previous))
            };
            previous = value;
        }
        increase
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct VectorAggregationState {
    count: u64,
    sum: MetricValue,
    sum_squares: MetricValue,
    min: Option<MetricValue>,
    max: Option<MetricValue>,
}

impl VectorAggregationState {
    fn record(&mut self, value: MetricValue) {
        self.count += 1;
        self.sum = self.sum.add(value);
        self.sum_squares = self.sum_squares.add(value.multiply(value));
        self.min = Some(self.min.map_or(value, |min| {
            if value.cmp_value(min) == Ordering::Less {
                value
            } else {
                min
            }
        }));
        self.max = Some(self.max.map_or(value, |max| {
            if value.cmp_value(max) == Ordering::Greater {
                value
            } else {
                max
            }
        }));
    }

    fn finish(self, op: &VectorAggregationOp) -> MetricValue {
        match op {
            VectorAggregationOp::Sum => self.sum,
            VectorAggregationOp::Count => MetricValue::integer(self.count),
            VectorAggregationOp::Min => self.min.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Max => self.max.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Avg => self.sum.divide_by(self.count),
            VectorAggregationOp::Stddev => self.stdvar().sqrt(),
            VectorAggregationOp::Stdvar => self.stdvar(),
            VectorAggregationOp::TopK(_)
            | VectorAggregationOp::BottomK(_)
            | VectorAggregationOp::ApproxTopK(_)
            | VectorAggregationOp::CountValues(_)
            | VectorAggregationOp::Sort
            | VectorAggregationOp::SortDesc => {
                unreachable!("selection aggregations are handled before reduction")
            }
        }
    }

    fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }
}

fn format_metric_value(value: MetricValue) -> String {
    let negative = value.numerator < 0;
    let numerator = value.numerator.unsigned_abs();
    let whole = numerator / value.denominator;
    let mut remainder = numerator % value.denominator;
    let sign = if negative { "-" } else { "" };
    if remainder == 0 {
        return format!("{sign}{whole}");
    }

    let mut decimals = String::new();
    while remainder != 0 && decimals.len() < 9 {
        remainder *= 10;
        let digit =
            u8::try_from(remainder / value.denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= value.denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{sign}{whole}.{decimals}")
}

fn rate_metric_value(value: MetricValue, range_ns: i64) -> MetricValue {
    let denominator = u128::from(range_ns.unsigned_abs());
    if denominator == 0 {
        return MetricValue::zero();
    }

    MetricValue::new(
        value.numerator * 1_000_000_000,
        value.denominator * denominator,
    )
}

fn eval_times(range: TimeRange, step_ns: i64) -> Vec<i64> {
    let mut times = Vec::new();
    let mut time = range.start_ns;
    while time <= range.end_ns {
        times.push(time);
        let Some(next) = time.checked_add(step_ns) else {
            break;
        };
        if next <= time {
            break;
        }
        time = next;
    }
    times
}

fn append_matching_log_row(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    fingerprint: SeriesFingerprint,
    timestamp_ns: i64,
    line: &str,
    structured_metadata: &Labels,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    if timestamp_ns < plan.time_range.start_ns
        || timestamp_ns > plan.time_range.end_ns
        || !plan.fingerprints.contains(&fingerprint)
    {
        return Ok(());
    }

    let labels = label_index.labels_for(&plan.tenant, fingerprint).ok_or(
        QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint,
        },
    )?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        line,
        structured_metadata,
        timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((stream_labels, current_line)) =
        matching_loki_stream_entry(&plan.query, labels, line, structured_metadata, timestamp_ns)
    {
        streams
            .entry(stream_labels)
            .or_default()
            .push([timestamp_ns.to_string(), current_line]);
    }

    Ok(())
}

fn append_matching_hot_log_record(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) {
    if record.tenant != plan.tenant
        || frontier.is_compacted(record)
        || record.timestamp_ns < plan.time_range.start_ns
        || record.timestamp_ns > plan.time_range.end_ns
    {
        return;
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return;
    }

    if let Some((stream_labels, current_line)) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        streams
            .entry(stream_labels)
            .or_default()
            .push([record.timestamp_ns.to_string(), current_line]);
    }
}

fn is_deleted_log_entry(
    delete_filters: &[ActiveLogDeleteFilter],
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> bool {
    delete_filters.iter().any(|filter| {
        timestamp_ns >= filter.time_range.start_ns
            && timestamp_ns <= filter.time_range.end_ns
            && filter
                .query
                .matches_with_fields(labels, line, structured_metadata)
    })
}

fn matching_loki_stream_entry(
    query: &StreamQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Option<(Labels, String)> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);
    if should_insert_unknown_detected_level_for_stream_query(query, &stream_labels) {
        stream_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Some((stream_labels, evaluation.line))
}

fn matching_loki_metric_sample(
    query: &MetricQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Result<Option<(Labels, String, Option<MetricValue>)>, QueryError> {
    let evaluation =
        query
            .stream
            .evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns);
    let Some(evaluation) = evaluation else {
        return Ok(None);
    };
    if let Some(error) = evaluation
        .fields
        .get("__error__")
        .filter(|error| !error.is_empty())
    {
        return Err(QueryError::MetricPipelineError {
            error: error.clone(),
            details: evaluation.fields.get("__error_details__").cloned(),
        });
    }
    let mut metric_labels = evaluation.fields;
    let unwrap_sample = metric_labels
        .remove(UNWRAP_SAMPLE_VALUE_LABEL)
        .and_then(|value| parse_metric_sample_value(&value));
    for stage in &query.stream.pipeline {
        if let PipelineStage::Unwrap(unwrap) = stage {
            metric_labels.remove(unwrap.label());
        }
    }
    if should_insert_unknown_detected_level_for_stream_query(&query.stream, &metric_labels) {
        metric_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Ok(Some((metric_labels, evaluation.line, unwrap_sample)))
}

fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(numerator, denominator))
}

fn parse_decimal_sample_literal(value: &str) -> Option<(i128, u128)> {
    if value.is_empty() {
        return None;
    }
    let (negative, value) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if value.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            (
                &value[..index],
                parse_decimal_sample_exponent(exponent_text)?,
            )
        }
        None => (value, 0),
    };
    if mantissa.is_empty() {
        return None;
    }

    let (whole, fractional) = match mantissa.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let denominator = i128::try_from(denominator).ok()?;
    let numerator = i128::try_from(numerator).ok()?;
    Some((
        if negative { -numerator } else { numerator },
        u128::try_from(denominator).ok()?,
    ))
}

fn parse_decimal_sample_exponent(value: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    value.parse::<i32>().ok()
}

fn should_insert_unknown_detected_level(labels: &Labels) -> bool {
    !labels.contains_key("detected_level")
        && !labels.contains_key("level")
        && !labels.contains_key("severity")
        && !labels.contains_key("severity_text")
}

fn should_insert_unknown_detected_level_for_stream_query(
    query: &StreamQuery,
    labels: &Labels,
) -> bool {
    should_insert_unknown_detected_level(labels)
        && !query
            .pipeline
            .iter()
            .any(|stage| matches!(stage, PipelineStage::KeepLabels(_)))
}

fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<[String; 2]>>) {
    for values in streams.values_mut() {
        values.sort_by_key(|[timestamp, _]| timestamp.parse::<i64>().unwrap_or(i64::MAX));
    }
}

fn structured_metadata_value(metadata: &MapArray, row: usize) -> Result<Labels, QueryError> {
    let entries = metadata.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
            column: "structured_metadata.key",
            expected: "Utf8",
        })?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
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

fn append_matching_metric_row(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    query: &MetricQuery,
    row: QueryRow<'_>,
    eval_times: &[i64],
    range_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    if !plan.fingerprints.contains(&row.fingerprint) {
        return Ok(());
    }

    let labels = label_index
        .labels_for(&plan.tenant, row.fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint: row.fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    )? {
        let samples = samples.entry(metric_labels).or_default();
        let is_unwrapped = is_unwrapped_metric_query(query);
        let value = match query.aggregation {
            RangeAggregation::Rate if is_unwrapped => unwrap_sample.unwrap_or_default(),
            RangeAggregation::CountOverTime
            | RangeAggregation::Rate
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::PresentOverTime => MetricValue::integer(1),
            RangeAggregation::BytesRate | RangeAggregation::BytesOverTime => {
                MetricValue::integer(current_line.len() as u64)
            }
            RangeAggregation::RateCounter
            | RangeAggregation::SumOverTime
            | RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime => unwrap_sample.unwrap_or_default(),
        };
        for eval_time_ns in eval_times {
            let window_end_ns = eval_time_ns.saturating_sub(query.offset_ns.0);
            if row.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && row.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(row.timestamp_ns, value);
            }
        }
    }

    Ok(())
}

fn append_matching_hot_metric_record(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    query: &MetricQuery,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    eval_times: &[i64],
    range_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    if record.tenant != plan.tenant || frontier.is_compacted(record) {
        return Ok(());
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return Ok(());
    }

    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    )? {
        let samples = samples.entry(metric_labels).or_default();
        let is_unwrapped = is_unwrapped_metric_query(query);
        let value = match query.aggregation {
            RangeAggregation::Rate if is_unwrapped => unwrap_sample.unwrap_or_default(),
            RangeAggregation::CountOverTime
            | RangeAggregation::Rate
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::PresentOverTime => MetricValue::integer(1),
            RangeAggregation::BytesRate | RangeAggregation::BytesOverTime => {
                MetricValue::integer(current_line.len() as u64)
            }
            RangeAggregation::RateCounter
            | RangeAggregation::SumOverTime
            | RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime => unwrap_sample.unwrap_or_default(),
        };
        for eval_time_ns in eval_times {
            let window_end_ns = eval_time_ns.saturating_sub(query.offset_ns.0);
            if record.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && record.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(record.timestamp_ns, value);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct QueryRow<'a> {
    fingerprint: SeriesFingerprint,
    timestamp_ns: i64,
    line: &'a str,
    structured_metadata: &'a Labels,
}

fn loki_streams_response(streams: BTreeMap<Labels, Vec<[String; 2]>>) -> Value {
    loki_streams_response_with_warnings(streams, &[])
}

fn loki_streams_response_with_warnings(
    streams: BTreeMap<Labels, Vec<[String; 2]>>,
    warnings: &[String],
) -> Value {
    let result = streams
        .into_iter()
        .map(|(stream, values)| {
            json!({
                "stream": stream,
                "values": values,
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "streams",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}

fn loki_matrix_response(series: FormattedMetricSeries) -> Value {
    loki_matrix_response_with_warnings(series, &[])
}

fn loki_matrix_response_with_warnings(series: FormattedMetricSeries, warnings: &[String]) -> Value {
    let result = series
        .into_iter()
        .map(|(metric, values)| {
            json!({
                "metric": metric,
                "values": values
                    .into_iter()
                    .map(loki_metric_sample)
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "matrix",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}

fn loki_metric_sample([timestamp_ns, value]: [String; 2]) -> Value {
    json!([unix_ns_string_to_loki_seconds(&timestamp_ns), value])
}

fn unix_ns_string_to_loki_seconds(timestamp_ns: &str) -> Value {
    let timestamp_ns = timestamp_ns.parse::<u64>().unwrap_or_default();
    let seconds = timestamp_ns / 1_000_000_000;
    let nanos = timestamp_ns % 1_000_000_000;
    if nanos == 0 {
        json!(seconds)
    } else {
        json!(Duration::from_nanos(timestamp_ns).as_secs_f64())
    }
}

fn loki_vector_response_from_matrix(mut value: Value) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("matrix") {
        return value;
    }

    value["data"]["resultType"] = json!("vector");
    if let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    {
        for result in results {
            if let Some(values) = result.get_mut("values").and_then(Value::as_array_mut) {
                let value_sample = values.pop().unwrap_or_else(|| json!([]));
                result["value"] = value_sample;
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("values");
            }
        }
    }

    value
}

fn apply_loki_stream_options(
    mut value: Value,
    direction: LokiDirection,
    limit: Option<usize>,
    interval: Option<i64>,
    end_exclusive: Option<i64>,
) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
    }

    apply_loki_stream_end_bound(&mut value, end_exclusive);
    apply_loki_stream_interval(&mut value, interval);

    if matches!(direction, LokiDirection::Backward)
        && let Some(streams) = value
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        for stream in streams {
            if let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) {
                values.reverse();
            }
        }
    }

    apply_loki_stream_limit(value, limit)
}

fn apply_loki_stream_end_bound(value: &mut Value, end_exclusive: Option<i64>) {
    let Some(end_exclusive) = end_exclusive else {
        return;
    };
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        values.retain(|entry| {
            entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
                .is_none_or(|timestamp| timestamp < end_exclusive)
        });
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}

fn apply_loki_stream_interval(value: &mut Value, interval: Option<i64>) {
    let Some(interval) = interval else {
        return;
    };
    if interval == 0 {
        return;
    }
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        let mut next_timestamp = None;
        values.retain(|entry| {
            let Some(timestamp) = entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
            else {
                return true;
            };
            match next_timestamp {
                Some(next) if timestamp < next => false,
                _ => {
                    next_timestamp = timestamp.checked_add(interval);
                    true
                }
            }
        });
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}

fn apply_loki_stream_limit(mut value: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return value;
    };
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
    }

    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return value;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        if remaining == 0 {
            values.clear();
            continue;
        }
        if values.len() > remaining {
            values.truncate(remaining);
            remaining = 0;
        } else {
            remaining -= values.len();
        }
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    value
}

const LOKI_PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

fn wants_loki_parquet(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.split(',').any(accept_part_allows_loki_parquet))
}

fn accept_part_allows_loki_parquet(part: &str) -> bool {
    let mut pieces = part.trim().split(';');
    let Some(mime) = pieces.next() else {
        return false;
    };
    if !mime.trim().eq_ignore_ascii_case(LOKI_PARQUET_CONTENT_TYPE) {
        return false;
    }

    !pieces.any(accept_parameter_is_zero_quality)
}

fn accept_parameter_is_zero_quality(parameter: &str) -> bool {
    let Some((name, value)) = parameter.trim().split_once('=') else {
        return false;
    };
    if !name.trim().eq_ignore_ascii_case("q") {
        return false;
    }

    value
        .trim()
        .parse::<f32>()
        .is_ok_and(|quality| quality <= 0.0)
}

fn loki_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    match value.pointer("/data/resultType").and_then(Value::as_str) {
        Some("streams") => loki_streams_parquet_response(value),
        Some("matrix") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Matrix),
        Some("vector") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Vector),
        _ => Err(HttpQueryError::LokiParquet(
            "only stream and metric query results can be encoded as parquet",
        )),
    }
}

fn loki_streams_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing stream result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut lines = Vec::new();
    for stream in results {
        let labels = loki_parquet_labels(stream.get("stream"), "stream labels")?;
        let values = stream
            .get("values")
            .and_then(Value::as_array)
            .ok_or(HttpQueryError::LokiParquet("missing stream values array"))?;
        for entry in values {
            let entry = entry
                .as_array()
                .ok_or(HttpQueryError::LokiParquet("stream value is not an array"))?;
            let timestamp = entry
                .first()
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet(
                    "stream timestamp is not a string",
                ))?
                .parse::<i64>()
                .map_err(|_| HttpQueryError::LokiParquet("stream timestamp is not an integer"))?;
            let line = entry
                .get(1)
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet("stream line is not a string"))?;
            timestamps.push(timestamp);
            label_sets.push(labels.clone());
            lines.push(line.to_string());
        }
    }

    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(&label_sets)?;
    let line_array = StringArray::from(lines);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", timestamp_data_type, false),
        Field::new("labels", labels_array.data_type().clone(), false),
        Field::new("line", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
            Arc::new(line_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}

#[derive(Clone, Copy)]
enum LokiMetricParquetKind {
    Matrix,
    Vector,
}

fn loki_metrics_parquet_response(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing metric result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut values = Vec::new();
    for series in results {
        let labels = loki_parquet_labels(series.get("metric"), "metric labels")?;
        match kind {
            LokiMetricParquetKind::Matrix => {
                let samples = series
                    .get("values")
                    .and_then(Value::as_array)
                    .ok_or(HttpQueryError::LokiParquet("missing matrix values array"))?;
                for sample in samples {
                    let (timestamp_ns, value) = loki_parquet_metric_sample(sample, kind)?;
                    timestamps.push(timestamp_ns);
                    label_sets.push(labels.clone());
                    values.push(value);
                }
            }
            LokiMetricParquetKind::Vector => {
                let sample = series
                    .get("value")
                    .ok_or(HttpQueryError::LokiParquet("missing vector value"))?;
                let (timestamp_ns, value) = loki_parquet_metric_sample(sample, kind)?;
                timestamps.push(timestamp_ns);
                label_sets.push(labels.clone());
                values.push(value);
            }
        }
    }

    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(&label_sets)?;
    let value_array = Float64Array::from(values);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", timestamp_data_type, false),
        Field::new("labels", labels_array.data_type().clone(), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
            Arc::new(value_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}

fn loki_parquet_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
) -> Result<(i64, f64), HttpQueryError> {
    let sample = sample
        .as_array()
        .ok_or(HttpQueryError::LokiParquet("metric sample is not an array"))?;
    let timestamp_ns = loki_parquet_metric_timestamp_ns(
        sample
            .first()
            .ok_or(HttpQueryError::LokiParquet("missing metric timestamp"))?,
        kind,
    )?;
    let value = sample
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
        .and_then(MetricValue::to_f64)
        .ok_or(HttpQueryError::LokiParquet("metric value is not numeric"))?;
    Ok((timestamp_ns, value))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "Loki metric JSON can carry seconds floats; Parquet timestamps are integer nanoseconds"
)]
fn loki_parquet_metric_timestamp_ns(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<i64, HttpQueryError> {
    if matches!(kind, LokiMetricParquetKind::Vector)
        && let Some(timestamp_ns) = value.as_i64() {
            return Ok(timestamp_ns);
        }

    if let Some(seconds) = value.as_i64() {
        return seconds
            .checked_mul(1_000_000_000)
            .ok_or(HttpQueryError::LokiParquet(
                "metric timestamp is out of range",
            ));
    }
    let seconds = value.as_f64().ok_or(HttpQueryError::LokiParquet(
        "metric timestamp is not numeric",
    ))?;
    let timestamp_ns = (seconds * 1_000_000_000.0).round();
    if !timestamp_ns.is_finite() || timestamp_ns < i64::MIN as f64 || timestamp_ns > i64::MAX as f64
    {
        return Err(HttpQueryError::LokiParquet(
            "metric timestamp is out of range",
        ));
    }
    Ok(timestamp_ns as i64)
}

fn loki_parquet_labels(
    labels: Option<&Value>,
    field: &'static str,
) -> Result<Vec<(String, String)>, HttpQueryError> {
    let labels = labels
        .and_then(Value::as_object)
        .ok_or(HttpQueryError::LokiParquet(field))?;
    labels
        .iter()
        .map(|(key, value)| {
            value.as_str().map_or_else(
                || Err(HttpQueryError::LokiParquet("label value is not a string")),
                |value| Ok((key.clone(), value.to_string())),
            )
        })
        .collect()
}

fn loki_parquet_label_array(
    label_sets: &[Vec<(String, String)>],
) -> Result<MapArray, HttpQueryError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for labels in label_sets {
        for (key, value) in labels {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }
    Ok(builder.finish())
}

fn loki_parquet_batch_response(batch: &RecordBatch) -> Result<Response, HttpQueryError> {
    let mut body = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut body, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    Ok((
        StatusCode::OK,
        [("content-type", LOKI_PARQUET_CONTENT_TYPE)],
        body,
    )
        .into_response())
}

fn loki_success(data: impl serde::Serialize) -> Response {
    json_response(StatusCode::OK, &loki_success_value(data))
}

fn loki_sparse_success() -> Response {
    json_response(StatusCode::OK, &json!({ "status": "success" }))
}

fn loki_success_value(data: impl serde::Serialize) -> Value {
    json!({
        "status": "success",
        "data": data,
    })
}

fn add_loki_query_stats(mut value: Value) -> Value {
    if value
        .pointer("/data/stats")
        .and_then(Value::as_object)
        .is_none()
    {
        value["data"]["stats"] = loki_query_stats();
    }
    value
}

fn merge_loki_query_response(target: &mut Value, source: Value) {
    if let Some(source_result) = source
        .pointer("/data/result")
        .and_then(Value::as_array)
        .cloned()
        && let Some(target_result) = target
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        target_result.extend(source_result);
    }

    if let Some(source_stats) = source.pointer("/data/stats") {
        merge_loki_query_stats(&mut target["data"]["stats"], source_stats);
    }

    if let Some(source_warnings) = source.get("warnings").and_then(Value::as_array).cloned() {
        let warnings = target
            .as_object_mut()
            .expect("Loki response is an object")
            .entry("warnings")
            .or_insert_with(|| json!([]));
        if let Some(target_warnings) = warnings.as_array_mut() {
            target_warnings.extend(source_warnings);
        }
    }
}

fn merge_loki_query_stats(target: &mut Value, source: &Value) {
    for pointer in [
        "/ingester/compressedBytes",
        "/ingester/decompressedBytes",
        "/ingester/decompressedLines",
        "/ingester/headChunkBytes",
        "/ingester/headChunkLines",
        "/ingester/totalBatches",
        "/ingester/totalChunksMatched",
        "/ingester/totalDuplicates",
        "/ingester/totalLinesSent",
        "/ingester/totalReached",
        "/store/compressedBytes",
        "/store/decompressedBytes",
        "/store/decompressedLines",
        "/store/totalChunksRef",
        "/store/totalChunksDownloaded",
        "/store/totalDuplicates",
        "/summary/totalBytesProcessed",
        "/summary/totalLinesProcessed",
    ] {
        add_loki_query_stat_field(target, source, pointer);
    }
}

fn add_loki_query_stat_field(target: &mut Value, source: &Value, pointer: &str) {
    let Some(addend) = source.pointer(pointer).and_then(Value::as_u64) else {
        return;
    };
    let Some(current) = target.pointer_mut(pointer) else {
        return;
    };
    let total = current.as_u64().unwrap_or_default().saturating_add(addend);
    *current = json!(total);
}

fn add_loki_query_stats_for_stream_plan(mut value: Value, plan: &StreamPlan) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let mut stats = loki_query_stats();
    let (store_lines, ingester_lines) = if chunks == 0 { (0, lines) } else { (lines, 0) };
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

fn add_loki_query_stats_for_stream_plan_with_hot_tail(
    value: Value,
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    add_loki_query_stats_for_stream_blocks_with_hot_tail(
        value,
        &plan.blocks,
        plan,
        hot_tail,
        frontier,
    )
}

fn add_loki_query_stats_for_stream_blocks_with_hot_tail(
    mut value: Value,
    blocks: &[BlockDescriptor],
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    let bytes = planned_block_bytes_for_blocks(blocks);
    let chunks = u64::try_from(blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let ingester_lines = count_loki_stream_result_hot_tail_lines(&value, plan, hot_tail, frontier);
    let store_lines = lines.saturating_sub(ingester_lines);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

fn add_loki_query_stats_for_metric_plan(
    mut value: Value,
    plan: &StreamPlan,
    query: &MetricQuery,
) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let samples = count_loki_metric_result_scan_lines(&value, query);
    let mut stats = loki_query_stats();
    let (store_lines, ingester_lines) = if chunks == 0 {
        (0, samples)
    } else {
        (samples, 0)
    };
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

#[allow(clippy::too_many_arguments)]
fn add_loki_query_stats_for_metric_plan_with_hot_tail(
    mut value: Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    eval_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let samples = count_loki_metric_result_scan_lines(&value, query);
    let ingester_samples = count_loki_metric_result_hot_tail_samples(
        &value,
        plan,
        query,
        hot_tail,
        frontier,
        eval_range,
        step_ns,
        delete_filters,
    );
    let store_samples = samples.saturating_sub(ingester_samples);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_samples, ingester_samples, chunks);
    value["data"]["stats"] = stats;
    value
}

fn populate_loki_query_scan_stats(
    stats: &mut Value,
    bytes: u64,
    store_lines: u64,
    ingester_lines: u64,
    chunks: u64,
) {
    if ingester_lines > 0 {
        stats["ingester"]["decompressedLines"] = json!(ingester_lines);
        stats["ingester"]["totalLinesSent"] = json!(ingester_lines);
    }
    if chunks > 0 {
        stats["store"]["compressedBytes"] = json!(bytes);
        stats["store"]["decompressedBytes"] = json!(bytes);
        stats["store"]["decompressedLines"] = json!(store_lines);
        stats["store"]["totalChunksRef"] = json!(chunks);
        stats["store"]["totalChunksDownloaded"] = json!(chunks);
    }
    stats["summary"]["totalBytesProcessed"] = json!(bytes);
    stats["summary"]["totalLinesProcessed"] = json!(store_lines.saturating_add(ingester_lines));
}

fn planned_block_bytes(plan: &StreamPlan) -> u64 {
    planned_block_bytes_for_blocks(&plan.blocks)
}

fn planned_block_bytes_for_blocks(blocks: &[BlockDescriptor]) -> u64 {
    blocks
        .iter()
        .map(|block| block.size_bytes)
        .try_fold(0_u64, u64::checked_add)
        .unwrap_or(u64::MAX)
}

fn count_loki_stream_result_lines(value: &Value) -> u64 {
    value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .map(|streams| {
            streams
                .iter()
                .filter_map(|stream| stream.get("values").and_then(Value::as_array))
                .map(|values| u64::try_from(values.len()).unwrap_or(u64::MAX))
                .fold(0_u64, u64::saturating_add)
        })
        .unwrap_or(0)
}

fn count_loki_stream_result_hot_tail_lines(
    value: &Value,
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> u64 {
    let mut hot_counts: BTreeMap<(Labels, String, String), u64> = BTreeMap::new();
    for record in hot_tail {
        if record.tenant != plan.tenant
            || frontier.is_compacted(record)
            || record.timestamp_ns < plan.time_range.start_ns
            || record.timestamp_ns > plan.time_range.end_ns
        {
            continue;
        }
        let Some((stream_labels, current_line)) = matching_loki_stream_entry(
            &plan.query,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) else {
            continue;
        };
        let key = (stream_labels, record.timestamp_ns.to_string(), current_line);
        hot_counts
            .entry(key)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    let Some(streams) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    let mut matched = 0_u64;
    for stream in streams {
        let Some(labels) = stream.get("stream").and_then(json_object_to_labels) else {
            continue;
        };
        let Some(values) = stream.get("values").and_then(Value::as_array) else {
            continue;
        };
        for value in values {
            let Some(pair) = value.as_array() else {
                continue;
            };
            let (Some(timestamp), Some(line)) = (
                pair.first().and_then(Value::as_str),
                pair.get(1).and_then(Value::as_str),
            ) else {
                continue;
            };
            let key = (labels.clone(), timestamp.to_string(), line.to_string());
            let Some(count) = hot_counts.get_mut(&key) else {
                continue;
            };
            if *count == 0 {
                continue;
            }
            *count -= 1;
            matched = matched.saturating_add(1);
        }
    }
    matched
}

fn json_object_to_labels(value: &Value) -> Option<Labels> {
    value.as_object().map(|object| {
        object
            .iter()
            .filter_map(|(name, value)| {
                value
                    .as_str()
                    .map(|value| (name.clone(), value.to_string()))
            })
            .collect()
    })
}

fn count_loki_metric_result_samples(value: &Value) -> u64 {
    let Some(results) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    results
        .iter()
        .map(|result| {
            if let Some(values) = result.get("values").and_then(Value::as_array) {
                u64::try_from(values.len()).unwrap_or(u64::MAX)
            } else if result.get("value").is_some() {
                1
            } else {
                0
            }
        })
        .fold(0_u64, u64::saturating_add)
}

fn count_loki_metric_result_scan_lines(value: &Value, query: &MetricQuery) -> u64 {
    if matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return 0;
    }
    count_loki_metric_result_samples(value)
}

#[allow(clippy::too_many_arguments)]
fn count_loki_metric_result_hot_tail_samples(
    value: &Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    eval_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> u64 {
    if matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return 0;
    }

    let eval_times = eval_times(eval_range, step_ns);
    let mut hot_samples = BTreeMap::new();
    for record in hot_tail {
        append_matching_hot_metric_record(
            &mut hot_samples,
            plan,
            query,
            record,
            frontier,
            &eval_times,
            query.range_ns.0,
            delete_filters,
        )
        .ok();
    }

    let mut hot_counts: BTreeMap<(Labels, String), u64> = BTreeMap::new();
    for (labels, values) in format_metric_samples(hot_samples, query) {
        for [timestamp_ns, _] in values {
            let key = (
                labels.clone(),
                unix_ns_string_to_loki_seconds(&timestamp_ns).to_string(),
            );
            hot_counts
                .entry(key)
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
        }
    }

    let Some(results) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    let mut matched = 0_u64;
    for result in results {
        let Some(labels) = result.get("metric").and_then(json_object_to_labels) else {
            continue;
        };
        if let Some(values) = result.get("values").and_then(Value::as_array) {
            for sample in values {
                if consume_hot_metric_sample(&mut hot_counts, &labels, sample) {
                    matched = matched.saturating_add(1);
                }
            }
        } else if let Some(sample) = result.get("value")
            && consume_hot_metric_sample(&mut hot_counts, &labels, sample)
        {
            matched = matched.saturating_add(1);
        }
    }
    matched
}

fn consume_hot_metric_sample(
    hot_counts: &mut BTreeMap<(Labels, String), u64>,
    labels: &Labels,
    sample: &Value,
) -> bool {
    let Some(timestamp_key) = loki_metric_sample_timestamp_key(sample) else {
        return false;
    };
    let key = (labels.clone(), timestamp_key);
    let Some(count) = hot_counts.get_mut(&key) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}

fn loki_metric_sample_timestamp_key(sample: &Value) -> Option<String> {
    sample
        .as_array()
        .and_then(|sample| sample.first())
        .map(Value::to_string)
}

fn loki_query_stats() -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": 0,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": 0,
            "totalChunksDownloaded": 0,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": 0,
            "totalLinesProcessed": 0
        }
    })
}

fn loki_error(status: StatusCode, error_type: &'static str, error: &str) -> Response {
    let value = json!({
        "status": "error",
        "errorType": error_type,
        "error": error,
        "data": null,
    });
    json_response(status, &value)
}

fn loki_format_query_invalid_response(status: StatusCode, error: &str) -> Response {
    let error = serde_json::to_string(error).expect("string serialization cannot fail");
    (
        status,
        [("content-type", "application/json")],
        format!("{{\"status\":\"invalid-query\",\"error\":{error}}}\n"),
    )
        .into_response()
}

fn loki_parse_error(status: StatusCode, query: &str, source: &ParseError) -> Response {
    text_response(status, &loki_parse_error_text(query, source))
}

fn loki_parse_error_text(query: &str, source: &ParseError) -> String {
    match source {
        ParseError::Syntax { message, position } => {
            let unexpected = unexpected_logql_token(query, *position);
            let prefix = format!(
                "parse error at line {}, col {}: syntax error: unexpected {}",
                line_number(query, *position),
                column_number(query, *position),
                unexpected
            );
            if should_omit_expected_logql_token(message, &unexpected) {
                prefix
            } else {
                format!("{prefix}, expecting {}", expected_logql_token(message))
            }
        }
        ParseError::InvalidRegex { pattern, source } => {
            format!("parse error: invalid regex `{pattern}`: {source}")
        }
    }
}

fn line_number(query: &str, position: usize) -> usize {
    query[..position.min(query.len())]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

fn column_number(query: &str, position: usize) -> usize {
    let prefix = &query[..position.min(query.len())];
    prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, line)| line)
        .chars()
        .count()
        + 1
}

fn unexpected_logql_token(query: &str, position: usize) -> String {
    let rest = &query[position.min(query.len())..];
    let Some(token) = rest.chars().next() else {
        return "$end".to_string();
    };
    if token == '_' || token.is_ascii_alphabetic() {
        return "IDENTIFIER".to_string();
    }
    token.to_string()
}

fn should_omit_expected_logql_token(message: &str, unexpected: &str) -> bool {
    message == "expected '{'" && unexpected == "IDENTIFIER"
}

fn expected_logql_token(message: &str) -> String {
    match message {
        "expected '\"'" | "expected closing quote" => "STRING".to_string(),
        "expected label matcher operator" => "ASSIGN, EQ, NEQ, RE, NRE".to_string(),
        "expected label name" => "IDENTIFIER".to_string(),
        "expected end of query" => "$end".to_string(),
        _ => message
            .strip_prefix("expected ")
            .unwrap_or(message)
            .to_string(),
    }
}

fn text_response(status: StatusCode, value: &str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        value.to_string(),
    )
        .into_response()
}

fn json_response(status: StatusCode, value: &Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        value.to_string(),
    )
        .into_response()
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("invalid query column `{column}`: expected {expected}")]
    InvalidColumn {
        column: &'static str,
        expected: &'static str,
    },
    #[error("invalid metric query step {0}")]
    InvalidStep(i64),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("metric query contains pipeline error `{error}`")]
    MetricPipelineError {
        error: String,
        details: Option<String>,
    },
    #[error(transparent)]
    StructuredMetadata(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
enum DistributorError {
    #[error("empty stream labels")]
    EmptyStreamLabels,
    #[error("invalid OTLP attribute")]
    InvalidOtlpAttribute,
    #[error("invalid OTLP payload")]
    InvalidOtlpPayload,
    #[error("ingest body {body_bytes} bytes exceeds configured limit {max_bytes}")]
    IngestBodyTooLarge { body_bytes: usize, max_bytes: usize },
    #[error(transparent)]
    IngestQuota(#[from] IngestLimitError),
    #[error("invalid Loki push value")]
    InvalidPushValue,
    #[error("invalid Loki push labels")]
    InvalidPushLabels,
    #[error("{0}")]
    InvalidPushLabelSyntax(String),
    #[error("{0}")]
    InvalidJsonPushValueSyntax(String),
    #[error("{0}")]
    InvalidJsonLineSyntax(String),
    #[error("{0}")]
    InvalidJsonTimestampSyntax(String),
    #[error("invalid Loki push payload")]
    InvalidPushPayload,
    #[error("error at least one valid stream is required for ingestion\n")]
    NoValidStreams,
    #[error("invalid structured metadata")]
    InvalidStructuredMetadata,
    #[error("{0}")]
    InvalidStructuredMetadataSyntax(String),
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error(
        "entry for stream '{stream}' has timestamp too old: {timestamp}, oldest acceptable timestamp is: {oldest}\n",
        timestamp = rfc3339_seconds(*timestamp_ns),
        oldest = rfc3339_seconds(*oldest_acceptable_timestamp_ns),
    )]
    TimestampTooOld {
        stream: String,
        timestamp_ns: i64,
        oldest_acceptable_timestamp_ns: i64,
    },
    #[error(
        "entry for stream '{stream}' has timestamp too old: {timestamp}, oldest acceptable timestamp is: {oldest}\n",
        oldest = rfc3339_seconds(*oldest_acceptable_timestamp_ns),
    )]
    TimestampTooOldString {
        stream: String,
        timestamp: &'static str,
        oldest_acceptable_timestamp_ns: i64,
    },
    #[error(
        "entry for stream '{stream}' has timestamp too new: {timestamp}\n",
        timestamp = rfc3339_seconds(*timestamp_ns),
    )]
    TimestampTooNew { stream: String, timestamp_ns: i64 },
    #[error(transparent)]
    Http(#[from] HttpQueryError),
    #[error("invalid Loki protobuf payload: {0}")]
    LokiDecode(prost::DecodeError),
    #[error("invalid snappy-compressed Loki protobuf payload: {0}")]
    LokiSnappyDecode(snap::Error),
    #[error("invalid gzip-compressed Loki payload: {0}")]
    LokiGzipDecode(std::io::Error),
    #[error("invalid deflate-compressed Loki payload: {0}")]
    LokiDeflateDecode(std::io::Error),
    #[error("Content-Encoding {0:?} not supported")]
    UnsupportedLokiContentEncoding(String),
    #[error("invalid media type {0:?}")]
    InvalidLokiContentType(String),
    #[error("invalid OTLP protobuf payload: {0}")]
    OtlpDecode(prost::DecodeError),
    #[error("wal append timed out")]
    WalAppendTimeout,
    #[error(transparent)]
    WalSink(#[from] WalSinkError),
}

impl IntoResponse for DistributorError {
    fn into_response(self) -> Response {
        if let Self::LokiGzipDecode(source) = &self {
            return text_response(
                StatusCode::BAD_REQUEST,
                &loki_gzip_decode_error_text(source),
            );
        }
        if let Self::LokiDeflateDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "EOF\n");
        }
        if let Self::UnsupportedLokiContentEncoding(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, &format!("{self}\n"));
        }
        if let Self::LokiSnappyDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "snappy: corrupt input\n");
        }
        if let Self::LokiDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "unexpected EOF\n");
        }

        let status = match &self {
            Self::IngestBodyTooLarge { .. }
            | Self::IngestQuota(IngestLimitError::RateLimited { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::IngestQuota(IngestLimitError::Unauthorized { .. }) => StatusCode::FORBIDDEN,
            Self::IngestQuota(IngestLimitError::Unavailable { .. }) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::NoValidStreams => StatusCode::UNPROCESSABLE_ENTITY,
            Self::WalAppendTimeout | Self::WalSink(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::EmptyStreamLabels
            | Self::InvalidOtlpAttribute
            | Self::InvalidOtlpPayload
            | Self::InvalidPushLabels
            | Self::InvalidJsonPushValueSyntax(_)
            | Self::InvalidJsonLineSyntax(_)
            | Self::InvalidJsonTimestampSyntax(_)
            | Self::InvalidPushLabelSyntax(_)
            | Self::InvalidPushPayload
            | Self::InvalidPushValue
            | Self::InvalidStructuredMetadata
            | Self::InvalidStructuredMetadataSyntax(_)
            | Self::InvalidTimestamp
            | Self::TimestampTooOld { .. }
            | Self::TimestampTooOldString { .. }
            | Self::TimestampTooNew { .. }
            | Self::Http(_)
            | Self::LokiDecode(_)
            | Self::LokiDeflateDecode(_)
            | Self::LokiGzipDecode(_)
            | Self::LokiSnappyDecode(_)
            | Self::InvalidLokiContentType(_)
            | Self::UnsupportedLokiContentEncoding(_)
            | Self::OtlpDecode(_) => StatusCode::BAD_REQUEST,
        };
        if matches!(
            &self,
            Self::InvalidPushLabelSyntax(_)
                | Self::InvalidJsonLineSyntax(_)
                | Self::InvalidJsonPushValueSyntax(_)
                | Self::InvalidJsonTimestampSyntax(_)
                | Self::InvalidStructuredMetadataSyntax(_)
                | Self::NoValidStreams
                | Self::TimestampTooOld { .. }
                | Self::TimestampTooOldString { .. }
                | Self::TimestampTooNew { .. }
        ) {
            return text_response(status, &self.to_string());
        }
        let error_type = match status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            _ => "server_error",
        };
        loki_error(status, error_type, &self.to_string())
    }
}

fn loki_gzip_decode_error_text(source: &std::io::Error) -> String {
    let source = source.to_string();
    let message = match source.as_str() {
        "unexpected end of file" => "unexpected EOF",
        other => other,
    };
    format!("{message}\n")
}

fn distributor_error_to_grpc_status(error: &DistributorError) -> tonic::Status {
    let message = error.to_string();
    match error {
        DistributorError::IngestBodyTooLarge { .. }
        | DistributorError::IngestQuota(IngestLimitError::RateLimited { .. }) => {
            tonic::Status::resource_exhausted(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unauthorized { .. }) => {
            tonic::Status::permission_denied(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unavailable { .. }) => {
            tonic::Status::unavailable(message)
        }
        DistributorError::WalAppendTimeout | DistributorError::WalSink(_) => {
            tonic::Status::unavailable(message)
        }
        DistributorError::EmptyStreamLabels
        | DistributorError::InvalidOtlpAttribute
        | DistributorError::InvalidOtlpPayload
        | DistributorError::InvalidPushLabels
        | DistributorError::InvalidJsonLineSyntax(_)
        | DistributorError::InvalidJsonTimestampSyntax(_)
        | DistributorError::InvalidPushLabelSyntax(_)
        | DistributorError::InvalidPushPayload
        | DistributorError::InvalidPushValue
        | DistributorError::NoValidStreams
        | DistributorError::InvalidJsonPushValueSyntax(_)
        | DistributorError::InvalidStructuredMetadata
        | DistributorError::InvalidStructuredMetadataSyntax(_)
        | DistributorError::InvalidTimestamp
        | DistributorError::TimestampTooOld { .. }
        | DistributorError::TimestampTooOldString { .. }
        | DistributorError::TimestampTooNew { .. }
        | DistributorError::Http(_)
        | DistributorError::LokiDecode(_)
        | DistributorError::LokiDeflateDecode(_)
        | DistributorError::LokiGzipDecode(_)
        | DistributorError::LokiSnappyDecode(_)
        | DistributorError::InvalidLokiContentType(_)
        | DistributorError::UnsupportedLokiContentEncoding(_)
        | DistributorError::OtlpDecode(_) => tonic::Status::invalid_argument(message),
    }
}

#[derive(Debug, Error)]
enum HttpQueryError {
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("invalid percent-encoded query parameter")]
    InvalidPercentEncoding,
    #[error("invalid direction '{0}'")]
    InvalidDirection(String),
    #[error("strconv.Atoi: parsing \"{0}\": invalid syntax")]
    InvalidLimit(String),
    #[error("limit must be a positive value")]
    LimitNotPositive,
    #[error(
        "zero or negative query resolution step widths are not accepted. Try a positive integer"
    )]
    InvalidStep,
    #[error("invalid query parameter `{name}` value `{value}`")]
    InvalidQueryParameter { name: &'static str, value: String },
    #[error("delay_for can't be greater than 5")]
    TailDelayForTooLarge,
    #[error("cannot parse \"{value}\" to a valid duration")]
    InvalidDurationQueryParameter { value: String },
    #[error("interval must be >= 0")]
    InvalidInterval,
    #[error("invalid aggregation option")]
    InvalidVolumeAggregation,
    #[error("could not parse 'since' parameter: not a valid duration string: \"{value}\"")]
    InvalidSinceQueryParameter { value: String },
    #[error(
        "could not parse '{name}' parameter: strconv.ParseInt: parsing \"{value}\": invalid syntax"
    )]
    InvalidTimestampQueryParameter { name: &'static str, value: String },
    #[error("invalid tenant header")]
    InvalidTenant,
    #[error("missing query parameter `{0}`")]
    MissingQueryParameter(&'static str),
    #[error("missing X-Scope-OrgID header")]
    MissingTenant,
    #[error("query range {range_ns}ns exceeds configured limit {max_range_ns}ns")]
    QueryRangeTooLarge { range_ns: i64, max_range_ns: i64 },
    #[error("the query time range exceeds the limit (query length: {query_length}, limit: 30d1h)")]
    LokiQueryRangeTooLarge { query_length: String },
    #[error(
        "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
    )]
    QueryResolutionTooHigh,
    #[error("query planned {planned_bytes} bytes, exceeding configured limit {max_bytes}")]
    QueryBytesTooLarge { planned_bytes: u64, max_bytes: u64 },
    #[error("query length {query_length} bytes exceeds configured limit {max_query_length}")]
    QueryLengthTooLarge {
        query_length: usize,
        max_query_length: usize,
    },
    #[error("query matched {series} series, exceeding configured limit {max_series}")]
    QuerySeriesTooLarge { series: usize, max_series: usize },
    #[error("approx_topk is not enabled. See -limits.shard_aggregations")]
    ApproxTopKDisabled,
    #[error("parse error at line 1, col 1: syntax error: unexpected IDENTIFIER")]
    CountValuesQuery,
    #[error("{0}")]
    LokiPlainParse(String),
    #[error("{0}")]
    LokiFormatPlainParse(String),
    #[error(transparent)]
    QueryAuthorization(#[from] QueryAuthorizationError),
    #[error("{source}")]
    LokiParse { query: String, source: ParseError },
    #[error("{source}")]
    LokiFormatParse { query: String, source: ParseError },
    #[error("missing query parameter `query`")]
    LokiFormatMissingQuery,
    #[error("cannot encode Loki query result as parquet: {0}")]
    LokiParquet(&'static str),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    Rules(#[from] LokiRuleStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
}

impl IntoResponse for HttpQueryError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BlockStore(_)
            | Self::InvalidPercentEncoding
            | Self::InvalidDirection(_)
            | Self::InvalidLimit(_)
            | Self::LimitNotPositive
            | Self::InvalidStep
            | Self::InvalidQueryParameter { .. }
            | Self::TailDelayForTooLarge
            | Self::InvalidDurationQueryParameter { .. }
            | Self::InvalidInterval
            | Self::InvalidVolumeAggregation
            | Self::InvalidSinceQueryParameter { .. }
            | Self::InvalidTimestampQueryParameter { .. }
            | Self::InvalidTenant
            | Self::MissingQueryParameter(_)
            | Self::MissingTenant
            | Self::QueryRangeTooLarge { .. }
            | Self::LokiQueryRangeTooLarge { .. }
            | Self::QueryResolutionTooHigh
            | Self::QueryBytesTooLarge { .. }
            | Self::QueryLengthTooLarge { .. }
            | Self::QuerySeriesTooLarge { .. }
            | Self::LokiPlainParse(_)
            | Self::CountValuesQuery
            | Self::Plan(_) => StatusCode::BAD_REQUEST,
            Self::ApproxTopKDisabled => StatusCode::INTERNAL_SERVER_ERROR,
            Self::QueryAuthorization(QueryAuthorizationError::Unauthorized { .. }) => {
                StatusCode::FORBIDDEN
            }
            Self::Query(QueryError::MetricPipelineError { .. }) => StatusCode::BAD_REQUEST,
            Self::Arrow(_)
            | Self::QueryAuthorization(QueryAuthorizationError::Unavailable { .. })
            | Self::Query(_)
            | Self::DeleteRequests(_)
            | Self::Rules(_)
            | Self::DeleteFilter(_)
            | Self::LokiParquet(_)
            | Self::Parquet(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::LokiParse { query, source } => {
                return loki_parse_error(StatusCode::BAD_REQUEST, query, source);
            }
            Self::LokiFormatParse { query, source } => {
                return loki_format_query_invalid_response(
                    StatusCode::BAD_REQUEST,
                    &loki_parse_error_text(query, source),
                );
            }
            Self::LokiFormatPlainParse(error) => {
                return loki_format_query_invalid_response(StatusCode::BAD_REQUEST, error);
            }
            Self::LokiFormatMissingQuery => {
                return loki_format_query_invalid_response(
                    StatusCode::BAD_REQUEST,
                    "parse error : syntax error: unexpected $end",
                );
            }
            Self::Parse(_) => StatusCode::BAD_REQUEST,
        };
        let error_type = match status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::FORBIDDEN => "forbidden",
            _ => "server_error",
        };
        if matches!(
            self,
            Self::InvalidDirection(_)
                | Self::InvalidLimit(_)
                | Self::LimitNotPositive
                | Self::InvalidStep
                | Self::TailDelayForTooLarge
                | Self::InvalidDurationQueryParameter { .. }
                | Self::InvalidInterval
                | Self::InvalidVolumeAggregation
                | Self::InvalidSinceQueryParameter { .. }
                | Self::InvalidTimestampQueryParameter { .. }
                | Self::LokiQueryRangeTooLarge { .. }
                | Self::QueryResolutionTooHigh
                | Self::LokiPlainParse(_)
                | Self::ApproxTopKDisabled
                | Self::CountValuesQuery
        ) {
            return text_response(status, &self.to_string());
        }
        if matches!(self, Self::MissingQueryParameter("query")) {
            return text_response(status, "parse error : syntax error: unexpected $end");
        }
        loki_error(status, error_type, &self.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// `ingest_tenant` returns a present non-empty `X-Scope-OrgID` verbatim,
    /// but falls back to `"unknown"` when the header is missing or empty.
    #[test]
    fn ingest_tenant_reads_header_or_falls_back() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert2::assert!(ingest_tenant(&present) == "acme");

        let missing = HeaderMap::new();
        assert2::assert!(ingest_tenant(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert2::assert!(ingest_tenant(&empty) == "unknown");
    }

    #[derive(Clone)]
    struct RecordingObjectStore {
        inner: Arc<object_store::memory::InMemory>,
        put_paths: Arc<Mutex<Vec<String>>>,
        get_paths: Arc<Mutex<Vec<String>>>,
        list_prefixes: Arc<Mutex<Vec<String>>>,
        list_offsets: Arc<Mutex<Vec<String>>>,
        get_delay: Duration,
        active_gets: Arc<std::sync::atomic::AtomicUsize>,
        max_active_gets: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RecordingObjectStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                put_paths: Arc::new(Mutex::new(Vec::new())),
                get_paths: Arc::new(Mutex::new(Vec::new())),
                list_prefixes: Arc::new(Mutex::new(Vec::new())),
                list_offsets: Arc::new(Mutex::new(Vec::new())),
                get_delay: Duration::ZERO,
                active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn with_get_delay(mut self, get_delay: Duration) -> Self {
            self.get_delay = get_delay;
            self
        }

        fn clear_recorded_paths(&self) {
            self.put_paths.lock().unwrap().clear();
            self.get_paths.lock().unwrap().clear();
            self.list_prefixes.lock().unwrap().clear();
            self.list_offsets.lock().unwrap().clear();
        }

        fn clear_put_paths(&self) {
            self.put_paths.lock().unwrap().clear();
        }

        fn put_paths(&self) -> Vec<String> {
            self.put_paths.lock().unwrap().clone()
        }

        fn get_paths(&self) -> Vec<String> {
            self.get_paths.lock().unwrap().clone()
        }

        fn list_prefixes(&self) -> Vec<String> {
            self.list_prefixes.lock().unwrap().clone()
        }

        fn list_offsets(&self) -> Vec<String> {
            self.list_offsets.lock().unwrap().clone()
        }

        fn max_active_gets(&self) -> usize {
            self.max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn record_get_start(&self) {
            let active = self
                .active_gets
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let mut current = self
                .max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst);
            while active > current {
                match self.max_active_gets.compare_exchange(
                    current,
                    active,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }

        fn record_get_end(&self) {
            self.active_gets
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl std::fmt::Debug for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    impl std::fmt::Display for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.put_paths.lock().unwrap().push(location.to_string());
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.get_paths.lock().unwrap().push(location.to_string());
            self.record_get_start();
            if !self.get_delay.is_zero() {
                sleep(self.get_delay).await;
            }
            let result = self.inner.get_opts(location, options).await;
            self.record_get_end();
            result
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&object_store::path::Path>,
            offset: &object_store::path::Path,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.list_offsets.lock().unwrap().push(offset.to_string());
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn compactor_configured_object_store_builds_when_not_injected() {
        let object_store_dir = tempfile::tempdir().unwrap();
        let object_store_url = Url::from_directory_path(object_store_dir.path())
            .expect("temporary directory should be representable as a file URL")
            .to_string();
        let config = ServiceConfig::parse_from([
            "crabka-observability",
            "--target",
            "compactor",
            "--object-store-url",
            &object_store_url,
        ]);

        let configured_store = build_compactor_configured_object_store(&config, None)
            .expect("valid object-store URL should configure a compactor store");

        assert2::assert!(configured_store.is_some());
    }

    /// The OTLP/HTTP logs handler must decompress `Content-Encoding: gzip`
    /// before protobuf-decoding. The OpenTelemetry SDK's `otlphttp` exporter
    /// (what the demo's Alloy uses) gzips by default, so a regression here means
    /// every emitted log line silently fails to decode and no logs are ingested.
    #[test]
    fn normalize_otlp_http_logs_decodes_gzip_identically_to_identity() {
        use std::io::Write as _;

        use opentelemetry_proto::tonic::{
            logs::v1::{ResourceLogs, ScopeLogs},
            resource::v1::Resource,
        };

        let request = ProtoExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![ProtoKeyValue {
                        key: "service.name".to_string(),
                        value: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "checkout".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        body: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "hello world".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let raw = request.encode_to_vec();

        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "demo".parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        // Identity (no Content-Encoding) decodes to a single record.
        let identity = normalize_otlp_http_logs(&headers, &raw, None, None)
            .expect("uncompressed OTLP proto logs should decode");
        assert2::assert!(identity.len() == 1);
        assert2::assert!(identity[0].line.as_str() == "hello world");

        // The gzip-compressed body must decode to exactly the same records.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut gz_headers = headers.clone();
        gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let from_gzip = normalize_otlp_http_logs(&gz_headers, &gzipped, None, None)
            .expect("gzip-compressed OTLP proto logs should decode");
        assert2::assert!(from_gzip == identity);
    }

    fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
        WalLogRecord {
            tenant: "tenant".to_string(),
            labels: BTreeMap::from([("app".to_string(), app.to_string())]),
            timestamp_ns,
            line: format!("line@{timestamp_ns}"),
            structured_metadata: BTreeMap::new(),
            position: None,
        }
    }

    /// Brute-force oracle: the records a full linear scan keeps for an inclusive window.
    fn brute_force_in_range(
        records: &[WalLogRecord],
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<WalLogRecord> {
        records
            .iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }

    /// The time-bucketed `records_in_range` MUST return exactly the same records (and
    /// therefore the same label/field sets) as a full-buffer scan, for any inclusive
    /// `[start, end]`, even though records are appended in NO timestamp order. This is the
    /// soundness guarantee that lets the query paths prune to the window instead of
    /// scanning the whole retained buffer.
    #[tokio::test]
    async fn hot_tail_records_in_range_matches_full_scan_under_out_of_order_inserts() {
        let hot_tail = BufferedLogHotTail::default();

        // Timestamps deliberately out of order and spread across many one-minute buckets,
        // with duplicates at the same instant and records straddling bucket boundaries.
        const BUCKET: i64 = HOT_TAIL_BUCKET_NS;
        let timestamps = [
            5 * BUCKET + 10,
            BUCKET - 1, // last ns of bucket 0
            3 * BUCKET,
            BUCKET,          // first ns of bucket 1
            5 * BUCKET + 10, // duplicate timestamp
            0,
            7 * BUCKET + 42,
            2 * BUCKET + 999,
            3 * BUCKET, // duplicate timestamp in a different append position
            4 * BUCKET - 1,
            -BUCKET + 5, // a pre-epoch (negative) timestamp
            6 * BUCKET,
        ];
        let apps = ["api", "web", "db"];
        let records: Vec<WalLogRecord> = timestamps
            .iter()
            .enumerate()
            .map(|(i, &ts)| hot_tail_test_record(ts, apps[i % apps.len()]))
            .collect();

        // Append one at a time to exercise incremental bucket insertion of out-of-order data.
        for record in &records {
            hot_tail.append_records(vec![record.clone()]);
        }

        // `records()` must still return the full append-ordered buffer (the tail path
        // depends on this).
        assert2::assert!(hot_tail.records() == records);

        // Probe a wide set of windows: exact bucket edges, sub-bucket slivers, windows
        // spanning many buckets, empty windows, and windows entirely outside the data.
        let min_ts = *timestamps.iter().min().unwrap();
        let max_ts = *timestamps.iter().max().unwrap();
        let mut probes: Vec<(i64, i64)> = Vec::new();
        // Walk window starts at a coarse quarter-bucket stride from below the earliest
        // record to above the latest, pairing each with several spans.
        let stride = BUCKET / 4;
        let mut start = min_ts - 2 * BUCKET;
        while start <= max_ts + 2 * BUCKET {
            for span in [0_i64, 1, BUCKET - 1, BUCKET, BUCKET + 1, 3 * BUCKET] {
                probes.push((start, start + span));
            }
            start += stride;
        }
        // Add exact per-record point windows and tight windows around each timestamp.
        for &ts in &timestamps {
            probes.push((ts, ts));
            probes.push((ts - 1, ts));
            probes.push((ts, ts + 1));
            probes.push((ts + 1, ts + 1));
        }

        for (start, end) in probes {
            if start > end {
                // Mirror the guard: an inverted window yields nothing.
                assert2::assert!(hot_tail.records_in_range(start, end).is_empty());
                continue;
            }
            let expected = brute_force_in_range(&records, start, end);
            let actual = hot_tail.records_in_range(start, end);
            assert2::assert!(actual == expected);

            // The label sets a query would derive must be identical too (records are the
            // sole input to label/field extraction).
            let expected_labels: BTreeSet<Labels> =
                expected.iter().map(|r| r.labels.clone()).collect();
            let actual_labels: BTreeSet<Labels> = actual.iter().map(|r| r.labels.clone()).collect();
            assert2::assert!(actual_labels == expected_labels);
        }

        // The trait-object path the querier actually uses must agree with the inherent method.
        let dyn_tail: Arc<dyn LogHotTail> = Arc::new(hot_tail.clone());
        let window = (2 * BUCKET, 6 * BUCKET);
        assert2::assert!(
            dyn_tail.records_in_range(window.0, window.1)
                == hot_tail.records_in_range(window.0, window.1)
        );

        // The default trait impl (used by other LogHotTail implementors, e.g. the
        // in-memory sink) falls back to filtering the full buffer and must also agree.
        let in_memory = InMemoryWalSink::default();
        for record in &records {
            LogWalSink::append(&in_memory, record.clone())
                .await
                .unwrap();
        }
        let in_memory_dyn: Arc<dyn LogHotTail> = Arc::new(in_memory);
        assert2::assert!(
            in_memory_dyn.records_in_range(window.0, window.1)
                == brute_force_in_range(&records, window.0, window.1)
        );
    }

    #[test]
    fn hot_tail_prune_compacted_rebuilds_records_and_time_index() {
        let hot_tail = BufferedLogHotTail::default();
        const BUCKET: i64 = HOT_TAIL_BUCKET_NS;

        let mut compacted_by_offset = hot_tail_test_record(4 * BUCKET, "offset-old");
        compacted_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(7),
        });
        let mut kept_by_offset = hot_tail_test_record(3 * BUCKET, "offset-new");
        kept_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(8),
        });
        let compacted_by_time = hot_tail_test_record(2 * BUCKET, "time-old");
        let kept_by_time = hot_tail_test_record(5 * BUCKET, "time-new");
        let expected = vec![kept_by_offset.clone(), kept_by_time.clone()];

        hot_tail.append_records(vec![
            compacted_by_offset,
            kept_by_offset,
            compacted_by_time,
            kept_by_time,
        ]);

        let frontier =
            CompactionFrontier::new(2 * BUCKET).with_partition_offset(PartitionIndex(0), Offset(7));

        assert2::assert!(hot_tail.prune_compacted(&frontier) == 2);
        assert2::assert!(hot_tail.records() == expected.clone());
        assert2::assert!(hot_tail.records_in_range(0, 6 * BUCKET) == expected);
        assert2::assert!(hot_tail.records_in_range(2 * BUCKET, 2 * BUCKET).is_empty());
        assert2::assert!(hot_tail.records_in_range(4 * BUCKET, 4 * BUCKET).is_empty());
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_prunes_hot_tail_from_object_store() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::default();
        let hot_tail = BufferedLogHotTail::default();
        let compacted = hot_tail_test_record(1_000, "old");
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![compacted, fresh.clone()]);
        write_compaction_frontier_to_object_store(&store, &prefix, &CompactionFrontier::new(2_000))
            .await
            .unwrap();

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert2::assert!(pruned == 1);
        assert2::assert!(frontier.snapshot() == CompactionFrontier::new(2_000));
        assert2::assert!(hot_tail.records() == vec![fresh]);
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_treats_absent_manifest_as_empty() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(123));
        let hot_tail = BufferedLogHotTail::default();
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![fresh.clone()]);

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert2::assert!(pruned == 0);
        assert2::assert!(frontier.snapshot() == CompactionFrontier::new(123));
        assert2::assert!(hot_tail.records() == vec![fresh]);
    }

    #[tokio::test]
    async fn appending_log_index_shard_does_not_rewrite_historical_shards_or_full_manifest() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability");
        let tenant = "tenant-a";
        let old_range_a = TimeRange::new(100, 199).unwrap();
        let old_range_b = TimeRange::new(200, 299).unwrap();
        let new_range = TimeRange::new(300, 399).unwrap();
        let mut labels_index = LabelIndex::default();
        let api =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
        let worker =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "worker".into())]));
        let admin =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "admin".into())]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 10, 19, old_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 20, 29, old_range_b),
            BTreeSet::from([worker]),
        ));
        crabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_range_a, old_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();

        let new_descriptor = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 30, 39, new_range),
            BTreeSet::from([admin]),
        );
        block_index.insert(new_descriptor.clone());
        store.clear_put_paths();

        write_tenant_compaction_indexes_to_object_store(
            &store,
            &prefix,
            tenant,
            &new_descriptor,
            &labels_index,
            &block_index,
            LogCompactionIndexOutput::ShardManifests,
        )
        .await
        .unwrap();

        // Exactly one PUT is allowed: the new shard manifest. The global
        // tenant manifest, the shard catalog, and the old shard manifests
        // must not be rewritten.
        let put_paths = store.put_paths();
        assert2::assert!(
            put_paths
                == vec![
                    crabka_blockstore::log_tenant_index_shard_manifest_object_path(
                        &prefix, tenant, new_range
                    )
                    .to_string()
                ]
        );
    }

    #[test]
    fn detected_labels_empty_query_is_match_all() {
        // Grafana's Logs Drilldown loads `detected_labels?query=` with an empty
        // query to discover every label. An empty/blank query must parse to
        // `None` (match all streams), not be handed to the LogQL parser — which
        // rejects "" with `syntax error: unexpected $end, expecting '{'`.
        for raw in ["query=", "query=%20", "query=%20%20"] {
            let params = parse_detected_labels_params(Some(raw)).unwrap();
            assert2::assert!(params.query.is_none());
        }
        // A real stream selector is still preserved.
        let params = parse_detected_labels_params(Some("query=%7Bapp%3D%22api%22%7D")).unwrap();
        assert2::assert!(params.query.as_deref() == Some(r#"{app="api"}"#));
    }

    #[test]
    fn instant_synthetic_vector_uses_raw_loki_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Vector {
                sample: Some("1".to_string()),
                metric: BTreeMap::new(),
            },
        );

        assert2::assert!(response["data"]["result"][0]["value"][0] == json!(4_000_000_000i64));
    }

    #[test]
    fn instant_scalar_expression_keeps_loki_seconds_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Scalar {
                sample: "2".to_string(),
            },
        );

        assert2::assert!(response["data"]["result"][0] == json!(4));
    }

    #[test]
    fn formats_loki_numeric_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[1000000000,"non-string push timestamp"]]}]}"#;
        let timestamp = json!(1000000000);
        let line = json!("non-string push timestamp");

        assert2::assert!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line))
                == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|alues\":[[1000000000,\"non-string push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_object_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[{"ts":"1000000000"},"object push timestamp"]]}]}"#;
        let timestamp = json!({"ts": "1000000000"});
        let line = json!("object push timestamp");

        assert2::assert!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line))
                == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_array_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[["1000000000"],"array push timestamp"]]}]}"#;
        let timestamp = json!(["1000000000"]);
        let line = json!("array push timestamp");

        assert2::assert!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line))
                == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
        );
    }

    // --- FIX B1 tests ---

    /// A TenantObjectStoreManifest source backed by an empty in-memory store (no manifest present)
    /// must return Ok with an empty (self-clone) index rather than propagating NotFound as an error.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_manifest(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert2::assert!(result.is_ok());
        let returned = result.unwrap();
        assert2::assert!(returned.block_index.blocks().is_empty());
    }

    /// Same check for the TenantObjectStoreShards variant.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_shards() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_shards(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert2::assert!(result.is_ok());
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_range = TimeRange::new(0, 100).unwrap();
        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, crabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        let shard_range = TimeRange::new(10, 19).unwrap();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range),
            BTreeSet::from([api]),
        ));
        crabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert2::assert!(
            first.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()])
        );
        assert2::assert!(
            second.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()])
        );

        let shard_prefix =
            crabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest = crabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest)
            .count();

        assert2::assert!(list_count == 1);
        assert2::assert!(shard_get_count == 1);
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let first_query_range = TimeRange::new(0, 100).unwrap();
        let moving_query_range = TimeRange::new(5, 105).unwrap();
        let shard_range_a = TimeRange::new(10, 19).unwrap();
        let shard_range_b = TimeRange::new(80, 89).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, crabka_blockstore::labels([("app", "api")]));
        let worker =
            labels_index.insert_series(tenant, crabka_blockstore::labels([("app", "worker")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 44, 45, shard_range_b),
            BTreeSet::from([worker]),
        ));
        crabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range_a, shard_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, first_query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, moving_query_range)
            .await
            .unwrap();

        for (_name, state) in [("first range", &first), ("moving range", &second)] {
            assert2::assert!(
                state.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()])
            );
            assert2::assert!(state.block_index.blocks().len() == 2);
        }

        let shard_prefix =
            crabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest_a = crabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_a,
        )
        .to_string();
        let shard_manifest_b = crabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_b,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count_a = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_a)
            .count();
        let shard_get_count_b = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_b)
            .count();

        check!(list_count == 1, "shard prefix should be listed once");
        check!(
            shard_get_count_a == 1,
            "shard manifest A should be fetched once"
        );
        check!(
            shard_get_count_b == 1,
            "shard manifest B should be fetched once"
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_lists_shards_from_query_window_offset() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_start = 1_700_000_000_000_000_000;
        let query_end = query_start + 300_000_000_000;
        let query_range = TimeRange::new(query_start, query_end).unwrap();
        let old_shard_range =
            TimeRange::new(query_start - 600_000_000_000, query_start - 599_000_000_000).unwrap();
        let matching_shard_range = TimeRange::new(query_start + 10, query_start + 20).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, crabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 40, 41, old_shard_range),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
            BTreeSet::from([api]),
        ));
        crabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_shard_range, matching_shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let state = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert2::assert!(
            state.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()])
        );
        let expected_offset =
            crabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant)
                .join(format!("time={}", query_start - (query_end - query_start)))
                .to_string();
        assert2::assert!(store.list_offsets().contains(&expected_offset));
    }

    #[test]
    fn metadata_index_range_defaults_empty_metadata_requests_to_recent_window() {
        const SIX_HOURS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
        let before = current_unix_time_ns();
        let range = metadata_index_range(&SeriesParams::default()).unwrap();
        let after = current_unix_time_ns();

        check!(
            range.start_ns >= before - SIX_HOURS_NS,
            "default metadata index start should be within Loki's default recent window"
        );
        check!(
            range.end_ns <= after,
            "default metadata index end should be now-ish, got {} after {}",
            range.end_ns,
            after
        );
        check!(
            range.end_ns - range.start_ns <= SIX_HOURS_NS,
            "default metadata index range should not be all time"
        );
    }

    #[tokio::test]
    async fn object_store_stream_query_batches_cold_block_reads() {
        let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let mut label_index = LabelIndex::default();
        let api = label_index.insert_series(tenant, crabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();

        for block_id in 0_i64..4 {
            let start_ns = block_id * 10;
            let end_ns = start_ns + 9;
            let block = write_log_block_to_object_store(
                &store,
                &prefix,
                &BlockKey::new(
                    tenant,
                    0,
                    start_ns,
                    end_ns,
                    TimeRange::new(start_ns, end_ns).unwrap(),
                ),
                vec![LogRow::new(
                    api,
                    end_ns,
                    format!("api error {block_id}"),
                    BTreeMap::new(),
                )],
            )
            .await
            .unwrap();
            block_index.insert(block);
        }

        let plan = plan_stream_query(
            tenant,
            TimeRange::new(0, 39).unwrap(),
            parse_query(r#"{app="api"} |= "error""#).unwrap(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::new(store.clone()),
            &prefix,
            &plan,
            &label_index,
            &[],
            &CompactionFrontier::new(i64::MAX),
            &[],
            StreamScanOptions::from_stream_options(LokiDirection::Forward, Some(100), None, None),
        )
        .await
        .unwrap();

        assert2::assert!(scan.scanned_blocks.len() == 4);
        assert2::assert!(store.max_active_gets() > 1);
    }

    // --- FIX B3 tests ---

    /// connect_with_startup_retry returns Ok immediately when the closure succeeds on the first try.
    #[tokio::test]
    async fn connect_with_startup_retry_succeeds_on_first_try() {
        let result: Result<u32, String> =
            connect_with_startup_retry("test", Duration::from_secs(5), || async {
                Ok::<u32, String>(42)
            })
            .await;

        assert2::assert!(result.unwrap() == 42);
    }

    /// connect_with_startup_retry retries on failure and returns Ok when a later attempt succeeds.
    #[tokio::test]
    async fn connect_with_startup_retry_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering as AO};
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = counter.clone();

        let result: Result<u32, String> =
            connect_with_startup_retry("test", Duration::from_secs(10), move || {
                let c = counter2.clone();
                async move {
                    let n = c.fetch_add(1, AO::SeqCst);
                    if n < 2 {
                        Err(format!("not ready yet (attempt {})", n))
                    } else {
                        Ok(99u32)
                    }
                }
            })
            .await;

        assert2::assert!(result.unwrap() == 99);
        assert2::assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    }

    /// connect_with_startup_retry returns the error after the deadline is exceeded.
    #[tokio::test]
    async fn connect_with_startup_retry_gives_up_after_deadline() {
        let result: Result<u32, String> = connect_with_startup_retry(
            "test",
            Duration::from_millis(50), // very short deadline
            || async { Err::<u32, String>("always fails".to_string()) },
        )
        .await;

        assert2::assert!(result.unwrap_err() == "always fails");
    }

    fn acl_entry(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: PatternType,
        principal: &str,
        operation: AclOperation,
        permission_type: PermissionType,
    ) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: resource_name.to_string(),
            pattern_type,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type,
        }
    }

    #[test]
    fn service_duration_constants_are_exact() {
        assert2::assert!(LOKI_REJECT_OLD_SAMPLES_MAX_AGE == Duration::from_secs(604_800));
        assert2::assert!(LOKI_CREATION_GRACE_PERIOD == Duration::from_secs(600));
    }

    #[test]
    fn service_dependencies_builder_methods_preserve_existing_fields() {
        #[derive(Clone)]
        struct TestLimiter;
        #[async_trait]
        impl LogIngestLimiter for TestLimiter {
            async fn check(
                &self,
                _tenant: &str,
                _records: &[WalLogRecord],
            ) -> Result<(), IngestLimitError> {
                Ok(())
            }
        }

        #[derive(Clone)]
        struct TestAuthorizer;
        #[async_trait]
        impl LogQueryAuthorizer for TestAuthorizer {
            async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
                Ok(())
            }
        }

        let metrics = ServiceMetrics::new();
        let frontier = SharedCompactionFrontier::default();
        let deps = ServiceDependencies::default()
            .with_metrics(metrics.clone())
            .with_wal_sink(InMemoryWalSink::default())
            .with_ingest_limiter(TestLimiter)
            .with_query_authorizer(TestAuthorizer)
            .with_hot_tail_shared_frontier(BufferedLogHotTail::default(), frontier.clone())
            .with_deferred_wal_consumer_connect(
                "broker:9092".to_string(),
                "group".to_string(),
                "topic".to_string(),
            );

        assert2::assert!(deps.metrics.is_some());
        assert2::assert!(deps.wal_sink.is_some());
        assert2::assert!(deps.ingest_limiter.is_some());
        assert2::assert!(deps.query_authorizer.is_some());
        assert2::assert!(deps.hot_tail.is_some());
        assert2::assert!(deps.deferred_wal_consumer_connect.is_some());
        check!(Arc::ptr_eq(
            &deps.metrics.as_ref().unwrap().registry,
            &metrics.registry
        ));
        match deps.hot_tail.as_ref().unwrap().frontier.clone() {
            CompactionFrontierSource::Shared(actual) => {
                assert2::assert!(actual.snapshot() == frontier.snapshot());
            }
            CompactionFrontierSource::Snapshot(_) => panic!("expected shared frontier"),
        }
        assert2::assert!(
            deps.deferred_wal_consumer_connect.as_ref()
                == Some(&DeferredWalConsumerConnect {
                    bootstrap: "broker:9092".to_string(),
                    group_id: "group".to_string(),
                    topic: "topic".to_string(),
                })
        );
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        assert2::assert!(
            next_compactor_object_store_backoff(Duration::from_millis(10))
                == Duration::from_millis(20)
        );
        assert2::assert!(
            next_compactor_object_store_backoff(Duration::from_millis(300))
                == Duration::from_millis(500)
        );
        assert2::assert!(
            next_compactor_object_store_backoff(Duration::from_millis(500))
                == Duration::from_millis(500)
        );
    }

    #[test]
    fn acl_helpers_require_topic_operation_principal_and_pattern() {
        let allow_write = acl_entry(
            ResourceType::Topic,
            "__crabka_observability_logs_wal",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::Write,
            PermissionType::Allow,
        );
        let allow_read = acl_entry(
            ResourceType::Topic,
            "__crabka_",
            PatternType::Prefixed,
            "User:*",
            AclOperation::Read,
            PermissionType::Allow,
        );
        let deny_write = acl_entry(
            ResourceType::Topic,
            "*",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::All,
            PermissionType::Deny,
        );

        for (entry, topic, want) in [
            (&allow_write, "__crabka_observability_logs_wal", true),
            (&allow_read, "__crabka_observability_logs_wal", true),
            (&allow_read, "other-topic", false),
        ] {
            check!(
                matches_acl_topic_pattern(entry, topic) == want,
                "pattern={} topic={topic}",
                entry.resource_name
            );
        }
        check!(acl_matches_tenant_wal_write(
            &allow_write,
            "User:tenant-a",
            "__crabka_observability_logs_wal"
        ));
        check!(acl_matches_tenant_wal_read(
            &allow_read,
            "User:tenant-a",
            "__crabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_write(
            &allow_read,
            "User:tenant-a",
            "__crabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_read(
            &allow_write,
            "User:tenant-a",
            "__crabka_observability_logs_wal"
        ));
        check!(!acl_matches_tenant_wal_write(
            &acl_entry(
                ResourceType::Group,
                "__crabka_observability_logs_wal",
                PatternType::Literal,
                "User:tenant-a",
                AclOperation::Write,
                PermissionType::Allow,
            ),
            "User:tenant-a",
            "__crabka_observability_logs_wal",
        ));
        check!(
            check_tenant_wal_write_acl(
                "tenant-a",
                "__crabka_observability_logs_wal",
                std::slice::from_ref(&allow_write)
            )
            .is_ok()
        );
        check!(
            check_tenant_wal_read_acl(
                "tenant-a",
                "__crabka_observability_logs_wal",
                std::slice::from_ref(&allow_read)
            )
            .is_ok()
        );
        check!(
            check_tenant_wal_write_acl(
                "tenant-a",
                "__crabka_observability_logs_wal",
                &[deny_write]
            )
            .is_err()
        );
        check!(
            check_tenant_wal_read_acl(
                "tenant-a",
                "__crabka_observability_logs_wal",
                &[allow_write]
            )
            .is_err()
        );
    }

    #[test]
    fn ingest_quota_bucket_and_byte_accounting_are_precise() {
        let record = WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([
                ("app".to_string(), "api".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]),
            timestamp_ns: 42,
            line: "hello".to_string(),
            structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            position: None,
        };
        let expected_bytes = "tenant-a".len()
            + "hello".len()
            + std::mem::size_of::<i64>()
            + "app".len()
            + "api".len()
            + "env".len()
            + "prod".len()
            + "trace_id".len()
            + "abc".len();
        assert2::assert!(ingest_quota_bytes(&[record]) == expected_bytes);

        let mut bucket = IngestQuotaBucket::new(10.0);
        check!((bucket.capacity() - 10.0).abs() < f64::EPSILON);
        check!(bucket.consume(10.0));
        check!(!bucket.consume(0.1));
        bucket.update_rate(5.0);
        assert2::assert!(bucket.available <= 5.0);
        bucket.available = 4.0;
        bucket.update_rate(20.0);
        assert2::assert!(bucket.available >= 4.0);
        assert2::assert!(bucket.consume(4.0));
    }

    #[test]
    fn hot_tail_bucket_key_uses_euclidean_minutes() {
        assert2::assert!(hot_tail_bucket_key(0) == 0);
        assert2::assert!(hot_tail_bucket_key(HOT_TAIL_BUCKET_NS - 1) == 0);
        assert2::assert!(hot_tail_bucket_key(HOT_TAIL_BUCKET_NS) == 1);
        assert2::assert!(hot_tail_bucket_key(-1) == -1);
        assert2::assert!(hot_tail_bucket_key(-HOT_TAIL_BUCKET_NS) == -1);
        assert2::assert!(hot_tail_bucket_key(-HOT_TAIL_BUCKET_NS - 1) == -2);
    }

    #[test]
    fn native_header_detection_requires_native_log_shape() {
        for (key, value, want) in [
            ("crabka-wal-record-type", Some(&b"log-line"[..]), true),
            ("crabka-log-timestamp-ns", Some(&b"1"[..]), true),
            ("crabka-log-label-app", Some(&b"api"[..]), true),
            ("crabka-wal-record-type", Some(&b"log"[..]), false),
            ("other", None, false),
        ] {
            let header = KafkaWalHeader {
                key: key.to_string(),
                value: value.map(<[u8]>::to_vec),
            };
            assert2::assert!(has_native_kafka_log_headers(&[header]) == want);
        }
    }

    #[test]
    fn varint_encoding_and_ingest_limits_pin_boundaries() {
        let mut body = Vec::new();
        encode_varint(0, &mut body);
        encode_varint(127, &mut body);
        encode_varint(128, &mut body);
        encode_varint(300, &mut body);
        assert2::assert!(body == vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

        let state = DistributorState {
            sink: Arc::new(InMemoryWalSink::default()),
            ingest_limiter: Arc::new(AllowAllIngestLimiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            metrics: ServiceMetrics::new(),
            max_ingest_body_bytes: Some(5),
            wal_append_timeout: None,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
        };
        assert2::assert!(validate_ingest_body_limit(&state, 5).is_ok());
        assert2::assert!(validate_ingest_body_limit(&state, 6).is_err());
    }

    #[test]
    fn loki_content_type_and_body_decoding_accept_only_expected_forms() {
        let mut headers = HeaderMap::new();
        assert2::assert!(decode_loki_http_body(&headers, b"raw").unwrap() == b"raw");
        headers.insert(CONTENT_ENCODING, "snappy".parse().unwrap());
        assert2::assert!(decode_loki_http_body(&headers, b"raw").unwrap() == b"raw");
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"raw").unwrap();
        assert2::assert!(
            decode_loki_http_body(&headers, &encoder.finish().unwrap()).unwrap() == b"raw"
        );
        headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
        assert2::assert!(decode_loki_http_body(&headers, b"raw").is_err());

        fn loki_content_type(value: &str) -> HeaderMap {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, value.parse().unwrap());
            headers
        }

        for (value, want) in [
            ("application/json", Some(true)),
            ("Application/JSON; charset=utf-8", Some(true)),
            ("application/x-protobuf", Some(false)),
            ("application/json; charset", None),
            ("application/json; charset=", None),
        ] {
            assert2::assert!(is_loki_json_content_type(&loki_content_type(value)).ok() == want);
        }
    }

    #[test]
    fn loki_error_contexts_respect_utf8_boundaries_and_offsets() {
        let body = "{\"streams\":\"not-array\"}";
        assert2::assert!(
            loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
                .contains("|{\"streams\":\"not-array\"}|")
        );

        let structured =
            br#"{"streams":[{"stream":{"app":"api"},"values":[["1","line",{"ok":true}]]}]}"#;
        let error = loki_structured_metadata_value_parse_error(structured, "ok", &json!(true));
        assert2::assert!(error.contains("ok\":true"));

        let text = "ab\u{20ac}cd";
        assert2::assert!(previous_char_boundary(text, 4) == 2);
        assert2::assert!(previous_char_boundary(text, text.len()) == text.len());
    }

    #[test]
    fn loki_label_and_level_helpers_pin_boundaries() {
        let rendered_labels = BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        assert2::assert!(loki_label_set(&rendered_labels) == r#"{app="api",env="prod"}"#);
        check!(loki_push_label_parse_error(&rendered_labels, "bad-name").contains("1:5"));
        check!(
            loki_proto_label_parse_error(r#"{9bad="x"}"#)
                .unwrap()
                .contains("1:2")
        );
        check!(
            loki_proto_label_parse_error(r#"{app="api",9bad="x"}"#)
                .unwrap()
                .contains("1:12")
        );

        let mut detected = BTreeMap::from([("app".to_string(), "api".to_string())]);
        discover_detected_level_label(&mut detected, "api ERROR happened");
        assert2::assert!(detected.get("detected_level").map(String::as_str) == Some("error"));
        let mut explicit = BTreeMap::from([("level".to_string(), "custom".to_string())]);
        discover_detected_level_label(&mut explicit, "api error happened");
        assert2::assert!(!explicit.contains_key("detected_level"));
        for (line, want) in [
            ("error happened", true),
            ("happened error", true),
            ("terror", false),
            ("error_code", false),
        ] {
            assert2::assert!(contains_log_level_token(line, "error") == want);
        }
        for (byte, want) in [(b'a', true), (b'1', true), (b'_', true), (b'-', false)] {
            assert2::assert!(is_log_level_word_byte(byte) == want);
        }
    }

    #[test]
    fn timestamp_and_value_conversions_cover_json_and_proto_shapes() {
        assert2::assert!(otlp_timestamp_ns(&json!("123")).unwrap() == 123);
        assert2::assert!(otlp_timestamp_ns(&json!(456)).unwrap() == 456);
        assert2::assert!(otlp_timestamp_ns(&json!(-1)).is_err());
        assert2::assert!(otlp_severity_number_to_string(&json!("INFO")).unwrap() == "INFO");
        assert2::assert!(otlp_severity_number_to_string(&json!(9)).unwrap() == "9");

        let otlp_value = OtlpAnyValue::Kvlist(OtlpKeyValueList {
            values: Some(vec![
                OtlpKeyValue {
                    key: "ok".to_string(),
                    value: OtlpAnyValue::Bool(true),
                },
                OtlpKeyValue {
                    key: "items".to_string(),
                    value: OtlpAnyValue::Array(OtlpArrayValue {
                        values: Some(vec![OtlpAnyValue::String("a".to_string())]),
                    }),
                },
            ]),
        });
        assert2::assert!(otlp_value_to_json(&otlp_value) == json!({"items": ["a"], "ok": true}));

        let proto_value = ProtoAnyValue {
            value: Some(proto_any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![ProtoKeyValue {
                        key: "answer".to_string(),
                        value: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::IntValue(42)),
                        }),
                        ..Default::default()
                    }],
                },
            )),
        };
        assert2::assert!(proto_value_to_json(&proto_value) == json!({"answer": 42}));
    }

    #[test]
    fn delete_request_query_parsing_and_overlap_boundaries() {
        let params = parse_create_delete_request_params(Some(
            "query=%7Bapp%3D%22api%22%7D&start=10&end=20&max_interval=1h",
        ))
        .unwrap();
        assert2::assert!(
            params
                == CreateDeleteRequestParams {
                    query: r#"{app="api"}"#.to_string(),
                    start_time: 10,
                    end_time: 20,
                }
        );
        assert2::assert!(
            parse_create_delete_request_params(Some("query=x&start=20&end=10")).is_err()
        );

        let list = parse_list_delete_requests_params(Some("start=10&end=20")).unwrap();
        assert2::assert!(
            list == ListDeleteRequestsParams {
                start_time: Some(10),
                end_time: Some(20),
            }
        );
        assert2::assert!(parse_list_delete_requests_params(Some("start=10")).is_err());
        assert2::assert!(
            parse_cancel_delete_request_params(Some("request_id=delete-1&force=true")).unwrap()
                == "delete-1"
        );
        assert2::assert!(
            parse_cancel_delete_request_params(Some("request_id=delete-1&force=maybe")).is_err()
        );
        assert2::assert!(parse_loki_delete_timestamp_query_param("start", "1.5").unwrap() == 1);

        let request = CompactorDeleteRequest {
            tenant: "tenant-a".to_string(),
            request_id: "delete-1".to_string(),
            query: r#"{app="api"}"#.to_string(),
            start_time: 10,
            end_time: 20,
            status: "received".to_string(),
            created_at: 1,
        };
        for (filter, want) in [
            (list, true),
            (
                ListDeleteRequestsParams {
                    start_time: Some(20),
                    end_time: Some(30),
                },
                true,
            ),
            (
                ListDeleteRequestsParams {
                    start_time: Some(21),
                    end_time: Some(30),
                },
                false,
            ),
        ] {
            assert2::assert!(delete_request_overlaps_filter(&request, &filter) == want);
        }
        for (right, want) in [
            (TimeRange::new(20, 30).unwrap(), true),
            (TimeRange::new(21, 30).unwrap(), false),
        ] {
            assert2::assert!(ranges_overlap(TimeRange::new(10, 20).unwrap(), right) == want);
        }
    }

    #[test]
    fn prometheus_rules_filters_parse_all_supported_axes() {
        let filters = PrometheusRulesFilters::parse(Some(
            "type=alert&exclude_alerts=true&time=10&rule_name=HighError&rule_group=api&file=rules.yaml&group_limit=2&group_next_token=next&match=%7Bapp%3D%22api%22%7D",
        ))
        .unwrap();
        assert2::assert!(filters.rule_kind == Some("alerting"));
        assert2::assert!(filters.exclude_alerts);
        assert2::assert!(filters.evaluation_time.is_some());
        assert2::assert!(filters.rule_names.contains("HighError"));
        assert2::assert!(filters.rule_groups.contains("api"));
        assert2::assert!(filters.files.contains("rules.yaml"));
        assert2::assert!(filters.group_limit == Some(2));
        assert2::assert!(filters.group_next_token.as_deref() == Some("next"));
        assert2::assert!(filters.label_selectors.len() == 1);
        assert2::assert!(filters.has_rule_filter());

        let recording = PrometheusRulesFilters::parse(Some("type=record")).unwrap();
        assert2::assert!(recording.rule_kind == Some("recording"));
        assert2::assert!(PrometheusRulesFilters::parse(Some("group_next_token=next")).is_err());
        assert2::assert!(
            !PrometheusRulesFilters::parse(Some(""))
                .unwrap()
                .has_rule_filter()
        );
    }

    #[test]
    fn json_log_lines_collapse_to_a_single_templated_pattern() {
        // Two Crabka-shaped JSON log lines differing only by timestamp must mine
        // to one pattern with the timestamp templatized and every constant kept.
        let first = r#"{"timestamp":"2026-07-01T04:19:26.1238077Z","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened"}"#;
        let second = r#"{"timestamp":"2026-07-01T04:19:27.9981001Z","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened"}"#;
        assert2::assert!(log_line_pattern(first) == log_line_pattern(second));
        assert2::assert!(
            log_line_pattern(first)
                == r#"{"timestamp":"<_>","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened"}"#
        );
    }

    #[test]
    fn json_log_pattern_templatizes_ids_and_numbers_but_keeps_constants() {
        let pattern = log_line_pattern(
            r#"{"severity":"INFO","request_id":"550e8400-e29b-41d4-a716-446655440000","trace":"4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b","offset":12345,"sasl":false,"listener":"PLAIN"}"#,
        );
        assert2::assert!(
            pattern
                == r#"{"severity":"INFO","request_id":"<_>","trace":"<_>","offset":"<_>","sasl":false,"listener":"PLAIN"}"#
        );
    }

    #[test]
    fn json_message_field_templatizes_embedded_variables() {
        assert2::assert!(
            log_line_pattern(r#"{"message":"processed request 550e8400e29b41d4a716 in 42ms"}"#)
                == r#"{"message":"processed request <_> in <_>"}"#
        );
    }

    #[test]
    fn non_json_lines_still_use_logfmt_mining() {
        assert2::assert!(
            log_line_pattern("status=500 user=100 route=/checkout")
                == "status=<_> user=<_> route=/checkout"
        );
        // A line that merely starts with `{` but is not valid JSON falls back.
        assert2::assert!(log_line_pattern("{not json ts=1") == "{not json ts=<_>");
    }

    #[test]
    fn pattern_value_variable_classification() {
        // Variable: timestamps, floats, UUIDs, long hex ids, opaque tokens.
        assert2::assert!(pattern_value_is_variable("2026-07-01T04:19:26.1238077Z"));
        assert2::assert!(pattern_value_is_variable("42.5"));
        assert2::assert!(pattern_value_is_variable(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        assert2::assert!(pattern_value_is_variable(
            "4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b"
        ));
        assert2::assert!(pattern_value_is_variable("AKIAIOSFODNN7EXAMPLE"));
        assert2::assert!(pattern_value_is_variable("\"2026-07-01T04:19:26Z\""));
        // Sole-reason coverage: each value below is variable via exactly one
        // classifier, so every branch of the `||` chain (and the shape checks
        // inside `is_uuid`/`is_hex_id`) is independently exercised.
        assert2::assert!(pattern_value_is_variable("-42.5")); // negative float: only the f64 parse
        assert2::assert!(pattern_value_is_variable(
            "f47ac10b-58cc-4372-a567-0e02b2c3d479" // letter-led UUID: only is_uuid
        ));
        assert2::assert!(pattern_value_is_variable("abcdefabcdefabcd")); // 16 hex letters, no digit: only is_hex_id
        // UUID *layout* but non-hex groups must not be accepted as a UUID (guards
        // the `len == n && all-hex` check inside is_uuid).
        assert2::assert!(!pattern_value_is_variable(
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        ));
        // Constant: levels, module paths, file:line callers, short words.
        assert2::assert!(!pattern_value_is_variable("INFO"));
        assert2::assert!(!pattern_value_is_variable(
            "crabka_broker::network::dispatch"
        ));
        assert2::assert!(!pattern_value_is_variable("grpc_logging.go:66"));
        assert2::assert!(!pattern_value_is_variable("/cortex.Ingester/Push"));
        assert2::assert!(!pattern_value_is_variable("cafe"));
        assert2::assert!(!pattern_value_is_variable("authenticationToken"));
    }

    /// A negative range offset MUST render with a leading `-` sign, and a positive
    /// offset MUST NOT. This pins the `offset_ns.0 < 0` sign branch in
    /// `format_metric_range_selector`: replacing `<` with `==` (never true here,
    /// since the `== 0` case is handled by the outer guard) would drop the sign and
    /// emit a positive offset for a query that asked to look *forward* in time.
    #[test]
    fn format_metric_range_selector_signs_negative_offset() {
        let negative = parse_metric_query("count_over_time({app=\"x\"}[5m] offset -3m)").unwrap();
        let positive = parse_metric_query("count_over_time({app=\"x\"}[5m] offset 3m)").unwrap();

        let negative_selector =
            format_metric_range_selector(&negative).expect("negative offset selector");
        let positive_selector =
            format_metric_range_selector(&positive).expect("positive offset selector");

        // The negative offset carries the sign; the positive one does not.
        check!(negative_selector.contains(" offset -"));
        check!(!positive_selector.contains(" offset -"));
        // The two differ ONLY by the sign character.
        check!(negative_selector == positive_selector.replace(" offset ", " offset -"));
    }

    /// `count_loki_metric_result_hot_tail_samples` counts matched ingester samples and
    /// returns 0 when there is nothing to match: an `absent_over_time` query short-
    /// circuits to 0, and a query whose response JSON has no `data.result` array also
    /// yields 0. Replacing the whole body with a constant `1` (the mutant) would report
    /// a phantom ingester sample and skew the store/ingester scan-stat split.
    #[test]
    fn count_loki_metric_result_hot_tail_samples_returns_zero_when_nothing_matches() {
        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 300_000_000_000).unwrap(),
            query: StreamQuery {
                matchers: Vec::new(),
                pipeline: Vec::new(),
            },
            fingerprints: BTreeSet::new(),
            blocks: Vec::new(),
        };
        let frontier = CompactionFrontier::new(0);
        let eval_range = TimeRange::new(0, 300_000_000_000).unwrap();
        let step_ns = 60_000_000_000;

        // `absent_over_time` short-circuits to 0 regardless of the response body.
        let absent_query = parse_metric_query("absent_over_time({app=\"x\"}[5m])").unwrap();
        let absent = count_loki_metric_result_hot_tail_samples(
            &json!({ "data": { "result": [] } }),
            &plan,
            &absent_query,
            &[],
            &frontier,
            eval_range,
            step_ns,
            &[],
        );
        check!(absent == 0);

        // A non-absent query with an empty hot tail and a response lacking any
        // `data.result` array matches nothing and returns 0.
        let count_query = parse_metric_query("count_over_time({app=\"x\"}[5m])").unwrap();
        let none = count_loki_metric_result_hot_tail_samples(
            &json!({}),
            &plan,
            &count_query,
            &[],
            &frontier,
            eval_range,
            step_ns,
            &[],
        );
        check!(none == 0);
    }
}
