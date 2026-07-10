//! Block-builder helpers for turning WAL span records into span blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use arrow::compute::concat_batches;
use crabka_blockstore::{
    BlockMeta, BlockWriter, PromotedSpanAttr, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom,
    SummaryColumns, TraceBlockStats, TraceIndex, span_block_decl,
    span_block_schema_with_promoted_attrs,
};
use crabka_client_consumer::{Consumer, ConsumerRecord};
use object_store::ObjectStore;
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    error::TracesError,
    ids::{MaxOffset, MinOffset, WindowStartNs},
    metrics::ServiceMetrics,
    span::{AttrValue, Span, batch::span_batch_with_promoted_attrs},
    wal::SpanRecord,
};

/// W3C trace-context header key carried on WAL records by the distributor's
/// ingest span; used to continue the same distributed trace on the consume side.
const TRACEPARENT_HEADER: &str = "traceparent";

/// Minimal WAL-consumer poll surface the block-builder loop drives.
///
/// Narrowing `run` to this trait (rather than the concrete
/// [`crabka_client_consumer::Consumer`]) lets the offset-commit invariants be
/// driven by a scripted fake in tests. The record type matches what
/// [`decode_consumer_records`] consumes so the loop body is unchanged.
#[async_trait::async_trait]
pub trait WalConsumerPoll: Send {
    async fn poll(&mut self, window: Duration) -> Result<Vec<ConsumerRecord>, TracesError>;
}

/// Minimal WAL-consumer commit surface the block-builder loop drives.
///
/// Kept separate from [`WalConsumerPoll`] so the commit-only invariant
/// (commit happens strictly after a durable flush) is expressible as its own
/// recorded call in tests.
#[async_trait::async_trait]
pub trait WalConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), TracesError>;
}

#[async_trait::async_trait]
impl WalConsumerPoll for Consumer {
    async fn poll(&mut self, window: Duration) -> Result<Vec<ConsumerRecord>, TracesError> {
        Consumer::poll(self, window)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))
    }
}

/// Decoded records from one Kafka partition and their inclusive offset range.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionWindow {
    pub offset_range: (i64, i64),
    pub records: Vec<SpanRecord>,
}

/// Default number of buffered span records that triggers a flush.
pub const DEFAULT_FLUSH_MAX_RECORDS: usize = 50_000;

/// Default maximum age of the oldest buffered span record before a flush.
///
/// In a cold-only deployment (no querier live tier, e.g. the demo) the
/// block-builder is the only path that makes spans queryable, so this age also
/// bounds how stale recent-trace search / trace-by-id can be. It is kept short
/// (10s) so freshness stays close to the per-poll behaviour while the
/// `flush_max_records` cap still batches bursty traffic into larger blocks
/// (the proliferation case). Deployments that attach a querier live tier can
/// raise it to batch more aggressively without a freshness cost.
pub const DEFAULT_FLUSH_MAX_AGE: Duration = Duration::from_secs(10);

/// Runtime settings for the block-builder loop.
#[derive(Clone, Debug)]
pub struct BlockBuilderConfig {
    pub object_key_prefix: String,
    pub index_key: String,
    pub window: Duration,
    pub promoted_attrs: Vec<PromotedSpanAttr>,
    /// Flush the accumulated buffer once this many span records are buffered.
    pub flush_max_records: usize,
    /// Flush the accumulated buffer once the oldest buffered record reaches this age.
    pub flush_max_age: Duration,
}

struct BlockBuildOptions<'a> {
    object_key_prefix: &'a str,
    promoted_attrs: &'a [PromotedSpanAttr],
}

/// Accumulates decoded [`PartitionWindow`]s across multiple WAL polls so the
/// block-builder can flush one larger block per partition instead of a tiny
/// block per poll.
///
/// Successive polls are merged by partition: their records are appended and the
/// inclusive offset range is widened to span every buffered record, so the block
/// object key is a pure function of the buffered offset range. When a
/// crash-and-reprocess re-forms the *same* buffer (same records, same flush
/// boundary) the key is identical and the re-run overwrites it idempotently.
/// Flush boundaries are timing-dependent (record count / age), so this is
/// at-least-once delivery rather than a guarantee of byte-identical keys across
/// every recovery.
#[derive(Debug, Default)]
pub struct FlushAccumulator {
    windows: BTreeMap<i32, PartitionWindow>,
    record_count: usize,
    oldest_record_at: Option<Instant>,
}

impl FlushAccumulator {
    /// Create an empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of span records currently buffered across all partitions.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Whether the buffer holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Merge one poll's decoded windows into the buffer.
    ///
    /// Records are appended per partition and the inclusive offset range is
    /// widened to cover both the buffered and incoming records. `now` stamps the
    /// arrival time used for age-based flushing; it is only recorded the first
    /// time records enter an otherwise-empty buffer so the age tracks the
    /// *oldest* buffered record.
    pub fn merge(&mut self, windows: BTreeMap<i32, PartitionWindow>, now: Instant) {
        for (partition, incoming) in windows {
            if incoming.records.is_empty() {
                continue;
            }
            if self.oldest_record_at.is_none() {
                self.oldest_record_at = Some(now);
            }
            self.record_count += incoming.records.len();
            self.windows
                .entry(partition)
                .and_modify(|buffered| {
                    buffered.offset_range.0 = buffered.offset_range.0.min(incoming.offset_range.0);
                    buffered.offset_range.1 = buffered.offset_range.1.max(incoming.offset_range.1);
                    buffered.records.extend(incoming.records.iter().cloned());
                })
                .or_insert(incoming);
        }
    }

    /// Whether the buffered records should be flushed now.
    ///
    /// True once either the record-count threshold is reached or the oldest
    /// buffered record has aged past `flush_max_age`. Always false when empty.
    #[must_use]
    pub fn should_flush(&self, config: &BlockBuilderConfig, now: Instant) -> bool {
        if self.record_count == 0 {
            return false;
        }
        if self.record_count >= config.flush_max_records {
            return true;
        }
        match self.oldest_record_at {
            Some(oldest) => now.saturating_duration_since(oldest) >= config.flush_max_age,
            None => false,
        }
    }

    /// Drain the buffered windows, resetting the accumulator to empty.
    #[must_use]
    pub fn take(&mut self) -> BTreeMap<i32, PartitionWindow> {
        self.record_count = 0;
        self.oldest_record_at = None;
        std::mem::take(&mut self.windows)
    }
}

/// Deterministic object key for one block-builder flush window.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: MinOffset,
    max_offset: MaxOffset,
    window_start_ns: WindowStartNs,
) -> String {
    let (min_offset, max_offset, window_start_ns) = (min_offset.0, max_offset.0, window_start_ns.0);
    format!(
        "traces/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet"
    )
}

/// Apply an optional object-store prefix to a raw traces object key.
#[must_use]
pub fn prefixed_object_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let key = key.trim_start_matches('/');
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}/{key}")
    }
}

/// Group records by tenant and trace id, sorting each trace for stable DFS input.
#[must_use]
pub fn group_by_trace(records: &[SpanRecord]) -> BTreeMap<(String, [u8; 16]), Vec<Span>> {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }
    for spans in grouped.values_mut() {
        spans.sort_by_key(|span| (span.start_ns, span.span_id));
    }
    grouped
}

/// Decode Kafka consumer records into per-partition span windows.
///
/// Tombstones / records without values are ignored and do not extend the
/// inclusive offset range.
pub fn decode_consumer_records(
    records: &[ConsumerRecord],
) -> Result<BTreeMap<i32, PartitionWindow>, TracesError> {
    let mut windows = BTreeMap::<i32, PartitionWindow>::new();
    for record in records {
        let Some(value) = &record.value else {
            continue;
        };
        let decoded = SpanRecord::decode(value)?;
        let window = windows.entry(record.partition).or_insert(PartitionWindow {
            offset_range: (record.offset, record.offset),
            records: Vec::new(),
        });
        window.offset_range.0 = window.offset_range.0.min(record.offset);
        window.offset_range.1 = window.offset_range.1.max(record.offset);
        window.records.push(decoded);
    }
    Ok(windows)
}

/// Build and write one span block for `tenant` from the supplied WAL records.
pub async fn build_blocks(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_promoted_attrs(writer, index, tenant, partition, records, offset_range, &[])
        .await
}

/// Build and write one span block with an object-store prefix applied to its key.
pub async fn build_blocks_with_prefix(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix,
            promoted_attrs: &[],
        },
    )
    .await
}

pub async fn build_blocks_with_promoted_attrs(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix: "",
            promoted_attrs,
        },
    )
    .await
}

async fn build_blocks_with_options(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
    options: BlockBuildOptions<'_>,
) -> Result<Vec<BlockMeta>, TracesError> {
    let grouped = group_by_trace(records);
    let mut batches = Vec::new();
    let mut traces = Vec::new();
    let mut tag_names = BTreeSet::new();
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut window_start_ns = i64::MAX;

    for ((record_tenant, trace_id), spans) in grouped {
        if record_tenant != tenant {
            continue;
        }
        window_start_ns =
            window_start_ns.min(spans.iter().map(|span| span.start_ns).min().unwrap_or(0));
        collect_tags(&spans, &mut tag_names, &mut tag_values);
        traces.push(trace_id);
        batches.push(span_batch_with_promoted_attrs(
            &spans,
            options.promoted_attrs,
        )?);
    }

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = span_block_schema_with_promoted_attrs(options.promoted_attrs);
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let key = object_key(
        tenant,
        partition,
        MinOffset(offset_range.0),
        MaxOffset(offset_range.1),
        WindowStartNs(window_start_ns),
    );
    let key = prefixed_object_key(options.object_key_prefix, &key);
    let meta = writer
        .write_block_with_decl(
            tenant,
            &key,
            schema,
            &[concatenated],
            &span_block_decl(),
            SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
        )
        .await
        .map_err(|err| TracesError::Block(err.to_string()))?;

    let mut bloom = ShardedTraceBloom::with_tempo_defaults(traces.len());
    for trace_id in traces {
        bloom.insert(&trace_id);
    }
    index.add_trace_block(
        tenant,
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom,
            tag_names,
            tag_values,
        },
    );

    Ok(vec![meta])
}

/// Consume WAL records, write span blocks, save the trace index, then commit offsets.
///
/// To avoid block proliferation, decoded windows are accumulated across polls
/// and merged per partition; a single larger block per partition is flushed only
/// once [`BlockBuilderConfig::flush_max_records`] records are buffered or the
/// oldest buffered record reaches [`BlockBuilderConfig::flush_max_age`]. WAL
/// offsets are committed only after the merged block(s) are durably written, and
/// the remaining buffer is drained on shutdown so no spans are lost.
pub async fn run<C>(
    mut consumer: C,
    writer: BlockWriter,
    index: Arc<Mutex<TraceIndex>>,
    object_store: Arc<dyn ObjectStore>,
    config: BlockBuilderConfig,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), TracesError>
where
    C: WalConsumerPoll + WalConsumerCommit,
{
    let mut accumulator = FlushAccumulator::new();
    while !shutdown.is_cancelled() {
        let records = consumer.poll(config.window).await?;
        let windows = decode_consumer_records(&records)?;

        // One consume span per NON-EMPTY poll batch (NOT per record). Parent it
        // to the distributor's ingest span via the W3C trace context carried on
        // any consumed record so the block-build continues the same distributed
        // trace; a no-op when no record carries a `traceparent`. Empty polls run
        // outside the span so age-based flushing is still re-checked without
        // emitting a span per idle round.
        let build_span = (!windows.is_empty()).then(|| {
            let span = tracing::info_span!(
                "traces_block_build",
                otel.kind = "consumer",
                crabka.wal.records = records.len(),
            );
            set_remote_parent_from_records(&span, &records);
            span
        });

        let iteration = async {
            if windows.is_empty() {
                // `poll` normally long-polls for `config.window`, so an empty
                // round already cost a full window. But when every assigned
                // leader hits a transient transport error (e.g. the demo's flaky
                // Docker DNS) `poll` returns `Ok(vec![])` immediately — without
                // this backoff the loop would busy-spin a core. A short sleep
                // bounds that to a trickle.
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else {
                accumulator.merge(windows, Instant::now());
            }

            // Flush + commit only when a threshold is reached; a low-traffic
            // stream still flushes within `flush_max_age` because every poll
            // re-checks the age of the oldest buffered record (the empty-poll
            // backoff above bounds the re-check interval). Committing only after
            // `flush_partition_windows` returns `Ok` keeps WAL offsets behind the
            // durable block(s).
            if accumulator.should_flush(&config, Instant::now()) {
                flush_and_commit(
                    &mut consumer,
                    &writer,
                    &index,
                    &object_store,
                    &config,
                    &metrics,
                    &mut accumulator,
                )
                .await?;
            }
            Ok::<(), TracesError>(())
        };

        match build_span {
            Some(span) => iteration.instrument(span).await?,
            None => iteration.await?,
        }
    }

    // Drain the remaining buffer on shutdown so buffered spans are not lost.
    if !accumulator.is_empty() {
        flush_and_commit(
            &mut consumer,
            &writer,
            &index,
            &object_store,
            &config,
            &metrics,
            &mut accumulator,
        )
        .await?;
    }
    Ok(())
}

/// Make `span` a child of the distributed trace carried on any consumed record's
/// `traceparent` header. Uses the first record that carries one; a no-op when
/// none do (e.g. records produced by an unsampled ingest request).
fn set_remote_parent_from_records(span: &tracing::Span, records: &[ConsumerRecord]) {
    let Some(record) = records
        .iter()
        .find(|record| record.headers.iter().any(|h| h.key == TRACEPARENT_HEADER))
    else {
        return;
    };
    crabka_telemetry::propagation::set_remote_parent(
        span,
        record
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
}

/// Flush the accumulated buffer to durable blocks, then commit WAL offsets.
///
/// The accumulator is drained first so a flush error leaves nothing
/// double-counted, and `commit_sync` runs only after `flush_partition_windows`
/// reports the block(s) and trace index are durably written.
async fn flush_and_commit<C>(
    consumer: &mut C,
    writer: &BlockWriter,
    index: &Arc<Mutex<TraceIndex>>,
    object_store: &Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    metrics: &ServiceMetrics,
    accumulator: &mut FlushAccumulator,
) -> Result<(), TracesError>
where
    C: WalConsumerCommit,
{
    let windows = accumulator.take();
    let blocks = {
        let mut guard = index.lock().await;
        flush_partition_windows(
            writer,
            &mut guard,
            Arc::clone(object_store),
            config,
            windows,
        )
        .await?
    };
    // One counter tick per durably-written span block (post-flush, pre-commit).
    for _ in 0..blocks {
        metrics.record_block_flushed();
    }
    consumer.commit_sync().await
}

/// Flush decoded partition windows and durably save the trace index, returning
/// the number of span blocks durably written.
///
/// The caller should commit WAL offsets only after this returns `Ok(_)`.
pub async fn flush_partition_windows(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_store: Arc<dyn ObjectStore>,
    config: &BlockBuilderConfig,
    windows: BTreeMap<i32, PartitionWindow>,
) -> Result<usize, TracesError> {
    let mut blocks_written = 0usize;
    for (partition, partition_window) in windows {
        for tenant in tenants_in_records(&partition_window.records) {
            let metas = build_blocks_with_options(
                writer,
                index,
                &tenant,
                partition,
                &partition_window.records,
                partition_window.offset_range,
                BlockBuildOptions {
                    object_key_prefix: &config.object_key_prefix,
                    promoted_attrs: &config.promoted_attrs,
                },
            )
            .await?;
            blocks_written += metas.len();
        }
    }
    index
        .save_latest_snapshot(&object_store, &config.index_key)
        .await
        .map(|_| blocks_written)
        .map_err(|err| TracesError::Block(err.to_string()))
}

fn collect_tags(
    spans: &[Span],
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) {
    for span in spans {
        for attr in span.resource_attrs.iter().chain(&span.span_attrs) {
            tag_names.insert(attr.key.clone());
            tag_values
                .entry(attr.key.clone())
                .or_default()
                .insert(attr_value_string(&attr.value));
        }
        for event in &span.events {
            insert_tag_value(tag_names, tag_values, "event:name", event.name.clone());
            insert_tag_value(
                tag_names,
                tag_values,
                "event:timeSinceStart",
                event
                    .time_unix_nano
                    .saturating_sub(span.start_ns)
                    .to_string(),
            );
            for attr in &event.attrs {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    &attr.key,
                    attr_value_string(&attr.value),
                );
            }
        }
        for link in &span.links {
            insert_tag_value(
                tag_names,
                tag_values,
                "link:traceID",
                hex::encode(link.trace_id),
            );
            insert_tag_value(
                tag_names,
                tag_values,
                "link:spanID",
                hex::encode(link.span_id),
            );
            for attr in &link.attrs {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    &attr.key,
                    attr_value_string(&attr.value),
                );
            }
        }
        if !span.instrumentation_scope.is_empty() {
            insert_tag_value(
                tag_names,
                tag_values,
                "instrumentation:name",
                span.instrumentation_scope.clone(),
            );
        }
        if !span.instrumentation_version.is_empty() {
            insert_tag_value(
                tag_names,
                tag_values,
                "instrumentation:version",
                span.instrumentation_version.clone(),
            );
        }
    }
}

fn insert_tag_value(
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
    tag: &str,
    value: String,
) {
    tag_names.insert(tag.to_string());
    tag_values.entry(tag.to_string()).or_default().insert(value);
}

fn tenants_in_records(records: &[SpanRecord]) -> BTreeSet<String> {
    records.iter().map(|record| record.tenant.clone()).collect()
}

fn attr_value_string(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Double(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
        AttrValue::Bytes(value) => hex::encode(value),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::span::{EventRecord, KeyValue, LinkRecord, SpanKind, StatusCode};

    /// `set_remote_parent_from_records` must re-parent the span into the trace
    /// carried on the FIRST record whose header key equals `TRACEPARENT_HEADER`.
    ///
    /// Guards two mutants: replacing the whole function with `()` (the span would
    /// keep its own fresh trace id, not the header's), and flipping the header-key
    /// comparison from `==` to `!=` (the non-traceparent record would be matched
    /// first and its garbage/absent context would fail to re-parent the span).
    /// The non-traceparent record is deliberately placed BEFORE the traceparent
    /// one so the `==`/`!=` distinction is observable.
    #[test]
    fn set_remote_parent_from_records_reparents_into_header_trace() {
        use opentelemetry::trace::{TraceContextExt as _, TraceId, TracerProvider as _};
        use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("blockbuilder-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            fn record(headers: Vec<crabka_client_consumer::Header>) -> ConsumerRecord {
                ConsumerRecord {
                    topic: "wal".into(),
                    partition: 0,
                    offset: 0,
                    leader_epoch: 0,
                    timestamp: 0,
                    key: None,
                    value: None,
                    headers,
                }
            }

            let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            let records = vec![
                // A record WITHOUT the traceparent header comes first: with the
                // `==`→`!=` mutant, `find` would (wrongly) select this one and
                // extract no valid context, so the trace-id assertion would fail.
                record(vec![crabka_client_consumer::Header {
                    key: "other".into(),
                    value: Some(bytes::Bytes::from_static(b"x")),
                }]),
                // The record actually carrying the producer's W3C trace context.
                record(vec![crabka_client_consumer::Header {
                    key: TRACEPARENT_HEADER.into(),
                    value: Some(bytes::Bytes::from(traceparent.as_bytes().to_vec())),
                }]),
            ];

            let span = tracing::info_span!("t");
            set_remote_parent_from_records(&span, &records);

            // The span now belongs to the producer's trace (shares its trace id).
            // A no-op mutant leaves the span in its own fresh trace, so this fails.
            let sc = span.context().span().span_context().clone();
            assert!(
                sc.trace_id() == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
            );
        });
    }

    fn span() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 100,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: Vec::new(),
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: Vec::new(),
            }],
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        }
    }

    #[test]
    fn collect_tags_indexes_event_and_link_intrinsics() {
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span()], &mut tag_names, &mut tag_values);

        assert_eq!(
            tag_names,
            BTreeSet::from([
                "event:name".to_string(),
                "event:timeSinceStart".to_string(),
                "link:spanID".to_string(),
                "link:traceID".to_string(),
                "service.name".to_string(),
            ])
        );
        assert_eq!(
            tag_values,
            BTreeMap::from([
                (
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()])
                ),
                (
                    "event:timeSinceStart".to_string(),
                    BTreeSet::from(["50".to_string()])
                ),
                (
                    "link:spanID".to_string(),
                    BTreeSet::from(["0808080808080808".to_string()])
                ),
                (
                    "link:traceID".to_string(),
                    BTreeSet::from(["09090909090909090909090909090909".to_string()])
                ),
                (
                    "service.name".to_string(),
                    BTreeSet::from(["api".to_string()])
                ),
            ])
        );
    }

    #[test]
    fn collect_tags_indexes_event_and_link_attributes() {
        let mut span = span();
        span.events[0].attrs = vec![KeyValue {
            key: "cache.key".into(),
            value: AttrValue::Str("users".into()),
        }];
        span.links[0].attrs = vec![KeyValue {
            key: "link.kind".into(),
            value: AttrValue::Str("retry".into()),
        }];
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span], &mut tag_names, &mut tag_values);

        assert_eq!(
            tag_names,
            BTreeSet::from([
                "cache.key".to_string(),
                "event:name".to_string(),
                "event:timeSinceStart".to_string(),
                "link.kind".to_string(),
                "link:spanID".to_string(),
                "link:traceID".to_string(),
                "service.name".to_string(),
            ])
        );
        assert_eq!(
            tag_values,
            BTreeMap::from([
                (
                    "cache.key".to_string(),
                    BTreeSet::from(["users".to_string()])
                ),
                (
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()])
                ),
                (
                    "event:timeSinceStart".to_string(),
                    BTreeSet::from(["50".to_string()])
                ),
                (
                    "link.kind".to_string(),
                    BTreeSet::from(["retry".to_string()])
                ),
                (
                    "link:spanID".to_string(),
                    BTreeSet::from(["0808080808080808".to_string()])
                ),
                (
                    "link:traceID".to_string(),
                    BTreeSet::from(["09090909090909090909090909090909".to_string()])
                ),
                (
                    "service.name".to_string(),
                    BTreeSet::from(["api".to_string()])
                ),
            ])
        );
    }

    #[test]
    fn collect_tags_indexes_instrumentation_intrinsics() {
        let mut span = span();
        span.instrumentation_scope = "otel-rust".into();
        span.instrumentation_version = "1.2.3".into();
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span], &mut tag_names, &mut tag_values);

        assert_eq!(
            tag_names,
            BTreeSet::from([
                "event:name".to_string(),
                "event:timeSinceStart".to_string(),
                "instrumentation:name".to_string(),
                "instrumentation:version".to_string(),
                "link:spanID".to_string(),
                "link:traceID".to_string(),
                "service.name".to_string(),
            ])
        );
        assert_eq!(
            tag_values,
            BTreeMap::from([
                (
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()])
                ),
                (
                    "event:timeSinceStart".to_string(),
                    BTreeSet::from(["50".to_string()])
                ),
                (
                    "instrumentation:name".to_string(),
                    BTreeSet::from(["otel-rust".to_string()])
                ),
                (
                    "instrumentation:version".to_string(),
                    BTreeSet::from(["1.2.3".to_string()])
                ),
                (
                    "link:spanID".to_string(),
                    BTreeSet::from(["0808080808080808".to_string()])
                ),
                (
                    "link:traceID".to_string(),
                    BTreeSet::from(["09090909090909090909090909090909".to_string()])
                ),
                (
                    "service.name".to_string(),
                    BTreeSet::from(["api".to_string()])
                ),
            ])
        );
    }
}
