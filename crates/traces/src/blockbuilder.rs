//! Block-builder helpers for turning WAL span records into span blocks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use crabka_blockstore::{
    BlockMeta, BlockWriter, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SummaryColumns,
    TraceBlockStats, TraceIndex, span_block_decl, span_block_schema,
};
use crabka_client_consumer::{Consumer, ConsumerRecord};
use object_store::ObjectStore;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::TracesError;
use crate::span::{AttrValue, Span, batch::span_batch};
use crate::wal::SpanRecord;

/// Decoded records from one Kafka partition and their inclusive offset range.
#[derive(Clone, Debug, PartialEq)]
pub struct PartitionWindow {
    pub offset_range: (i64, i64),
    pub records: Vec<SpanRecord>,
}

/// Runtime settings for the block-builder loop.
#[derive(Clone, Debug)]
pub struct BlockBuilderConfig {
    pub object_key_prefix: String,
    pub index_key: String,
    pub window: Duration,
}

/// Deterministic object key for one block-builder flush window.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    window_start_ns: i64,
) -> String {
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
    build_blocks_with_prefix(writer, index, "", tenant, partition, records, offset_range).await
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
        batches.push(span_batch(&spans)?);
    }

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = span_block_schema();
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let key = object_key(
        tenant,
        partition,
        offset_range.0,
        offset_range.1,
        window_start_ns,
    );
    let key = prefixed_object_key(object_key_prefix, &key);
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
pub async fn run(
    mut consumer: Consumer,
    writer: BlockWriter,
    index: Arc<Mutex<TraceIndex>>,
    object_store: Arc<dyn ObjectStore>,
    config: BlockBuilderConfig,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(config.window)
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        let windows = decode_consumer_records(&records)?;
        if windows.is_empty() {
            continue;
        }

        {
            let mut guard = index.lock().await;
            for (partition, partition_window) in windows {
                for tenant in tenants_in_records(&partition_window.records) {
                    build_blocks_with_prefix(
                        &writer,
                        &mut guard,
                        &config.object_key_prefix,
                        &tenant,
                        partition,
                        &partition_window.records,
                        partition_window.offset_range,
                    )
                    .await?;
                }
            }
            guard
                .save(&object_store, &config.index_key)
                .await
                .map_err(|err| TracesError::Block(err.to_string()))?;
        }

        consumer
            .commit_sync()
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
    }
    Ok(())
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

        assert!(tag_names.contains("event:name"));
        assert!(tag_names.contains("event:timeSinceStart"));
        assert!(tag_names.contains("link:traceID"));
        assert!(tag_names.contains("link:spanID"));
        assert!(tag_values["event:name"].contains("exception"));
        assert!(tag_values["event:timeSinceStart"].contains("50"));
        assert!(tag_values["link:traceID"].contains("09090909090909090909090909090909"));
        assert!(tag_values["link:spanID"].contains("0808080808080808"));
    }
}
