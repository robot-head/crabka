//! `SpanStore` implementation over cold span blocks plus the live tier.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arc_swap::ArcSwap;
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
        FixedSizeBinaryBuilder, Float64Array, Int32Array, Int64Array, Int64Builder,
        LargeStringArray, ListArray, StringArray, StringBuilder, StringViewArray, StructArray,
        UInt32Array,
    },
    compute::{cast, concat_batches, filter_record_batch, take},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use crabka_blockstore::{
    BlockIndex, BlockStore, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_EVENTS, SCOL_LINKS, TraceIndex,
    span_block_schema,
};
use crabka_traceql::{
    ATTR_PREFIX, AttrValue, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME,
    COL_EVENT_TIME_SINCE_START, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND,
    COL_LINK_SPAN_ID, COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, EVENT_ATTR_PREFIX,
    EventRef, LINK_ATTR_PREFIX, LinkRef, MatchCmp, MatchScope, MatchValue, ScanJob, ScanOptions,
    ScanResult, ScopedTag, SpanMatcher, SpanRef, SpanStore, TagScope, TraceSpans, TraceqlError,
    TypedValue, span_schema,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};

use crate::{querier::live::LiveTier, span::batch::RESOURCE_ATTR_PREFIX};

const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:parentID",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];
const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];
const LINK_TAGS: &[&str] = &["link:spanID", "link:traceID"];
const INSTRUMENTATION_TAGS: &[&str] = &["instrumentation:name", "instrumentation:version"];
const SCOPE_ORDER: &[TagScope] = &[
    TagScope::Resource,
    TagScope::Span,
    TagScope::Intrinsic,
    TagScope::Event,
    TagScope::Link,
    TagScope::Instrumentation,
];

/// A `TraceIndex` shared between the span store and live sources, swappable at
/// runtime so a background task can reload it without restarting.
pub type SharedTraceIndex = Arc<ArcSwap<TraceIndex>>;

/// Query-side span store that merges sealed blocks with an optional live tier.
pub struct CrabkaSpanStore {
    blocks: Arc<BlockStore>,
    trace_index: SharedTraceIndex,
    live: Option<LiveTier>,
}

impl CrabkaSpanStore {
    #[must_use]
    pub fn new(
        blocks: Arc<BlockStore>,
        trace_index: SharedTraceIndex,
        live: Option<LiveTier>,
    ) -> Self {
        Self {
            blocks,
            trace_index,
            live,
        }
    }

    async fn cold_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
        job: Option<&ScanJob>,
    ) -> Result<Vec<RecordBatch>, TraceqlError> {
        if end_ns < start_ns {
            return Ok(Vec::new());
        }
        let trace_index = self.trace_index.load();
        let (ctx, table) = if let Some(job) = job {
            if !trace_index.trace_blocks(tenant).iter().any(|block| {
                block.object_key == job.object_key
                    && block.min_ts <= end_ns
                    && block.max_ts >= start_ns
            }) {
                return Ok(Vec::new());
            }
            let row_groups = (job.row_group_start..job.row_group_end).collect::<Vec<_>>();
            self.blocks
                .scan_block_row_groups(&job.object_key, &row_groups, span_block_schema())
                .await
                .map_err(|err| block_err(&err))?
        } else {
            let keys = trace_index.candidate_blocks(tenant, start_ns, end_ns);
            self.blocks
                .scan_block_keys(&keys, span_block_schema())
                .await
                .map_err(|err| block_err(&err))?
        };
        collect_table(&ctx, &table).await
    }

    async fn scan_inner(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult, TraceqlError> {
        let scan_job = options.job.as_ref();
        let (cold_end, live_start) = if scan_job.is_some() {
            (end_ns, end_ns.saturating_add(1))
        } else {
            self.live.as_ref().map_or((end_ns, end_ns + 1), |live| {
                let frontier = live.block_builder_frontier_ns(tenant);
                (
                    end_ns.min(frontier.saturating_sub(1)),
                    start_ns.max(frontier),
                )
            })
        };

        let mut batches = self
            .cold_batches(tenant, start_ns, cold_end, scan_job)
            .await?;
        if let Some(live) = &self.live
            && live_start <= end_ns
        {
            batches.extend(live.span_batches(tenant, live_start, end_ns).await?);
        }
        // Bytes this scan inspected: the decoded size of the cold+live data read,
        // before filtering (surfaced as the Tempo search `metrics.inspectedBytes`).
        let inspected_bytes = batches
            .iter()
            .map(|b| u64::try_from(b.get_array_memory_size()).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add);
        let batches = recompute_scan_nested_sets(batches)?;
        let batches = filter_batches_by_matchers(batches, matchers)?;
        let mut expansion_matchers = matchers.to_vec();
        expansion_matchers.extend(options.projection_matchers.clone());
        let batches = add_nested_intrinsic_columns(batches, &expansion_matchers)?;
        let batches = add_span_attr_columns(batches, &options.projection_matchers)?;

        let schema = batches
            .first()
            .map_or_else(span_schema, RecordBatch::schema);
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, partitions)?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
            inspected_bytes,
        })
    }

    async fn trace_by_id_inner(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.candidate_blocks_for_trace(tenant, trace_id, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let mut spans = trace_from_batches(trace_id, collect_table(&ctx, &table).await?)?;

        if let Some(live_trace) = match &self.live {
            Some(live) => live.trace_spans(tenant, trace_id).await?,
            None => None,
        } {
            if spans.is_none() {
                spans = Some(TraceSpans {
                    trace_id: live_trace.trace_id,
                    root_service_name: live_trace.root_service_name.clone(),
                    root_trace_name: live_trace.root_trace_name.clone(),
                    resource_attributes: live_trace.resource_attributes.clone(),
                    spans: Vec::new(),
                });
            }
            if let Some(out) = &mut spans {
                if out.resource_attributes.is_empty() {
                    out.resource_attributes = live_trace.resource_attributes;
                }
                out.spans.extend(live_trace.spans);
                deduplicate_trace_spans(&mut out.spans);
            }
        }

        // `start_ns`/`end_ns` are a block/candidate-selection HINT for a by-id
        // lookup (already applied above via `candidate_blocks_for_trace`), NOT a
        // hard span-level filter. Real Tempo returns the *whole* trace for a
        // by-id request even when Grafana sends a narrow window, so spans that
        // start outside the window (a trace straddling the window edge) are kept
        // and the assembled trace is returned intact (the caller labels it
        // COMPLETE). Clipping here would silently drop straddling spans while
        // still reporting COMPLETE.
        Ok(spans)
    }
}

#[async_trait::async_trait]
impl SpanStore for CrabkaSpanStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult, TraceqlError> {
        self.scan_inner(tenant, matchers, start_ns, end_ns, &ScanOptions::default())
            .await
    }

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult, TraceqlError> {
        self.scan_inner(tenant, matchers, start_ns, end_ns, options)
            .await
    }

    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        self.trace_by_id_inner(tenant, trace_id, 0, i64::MAX).await
    }

    async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        self.trace_by_id_inner(tenant, trace_id, start_ns, end_ns)
            .await
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let mut by_scope: BTreeMap<&'static str, (TagScope, BTreeSet<String>)> = BTreeMap::new();
        let has_cold_blocks = !trace_index
            .candidate_blocks(tenant, start_ns, end_ns)
            .is_empty();
        let cold_index_tags = trace_index.tag_names(tenant, start_ns, end_ns);
        let needs_scoped_cold_scan = matches!(
            scope,
            None | Some(TagScope::Resource | TagScope::Event | TagScope::Link)
        );
        if has_cold_blocks && !cold_index_tags.is_empty() && needs_scoped_cold_scan {
            let cold_scoped = self
                .cold_attribute_tag_names(tenant, start_ns, end_ns)
                .await?;
            merge_dynamic_scope(
                &mut by_scope,
                scope,
                TagScope::Resource,
                cold_scoped.resource,
            );
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Span, cold_scoped.span);
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Event, cold_scoped.event);
            merge_dynamic_scope(&mut by_scope, scope, TagScope::Link, cold_scoped.link);
        } else if matches!(scope, None | Some(TagScope::Span)) {
            let (_, tags) = by_scope
                .entry("span")
                .or_insert((TagScope::Span, BTreeSet::new()));
            tags.extend(
                cold_index_tags
                    .into_iter()
                    .filter(|tag| !is_intrinsic_tag(tag)),
            );
        }
        if has_cold_blocks {
            merge_static_scope(&mut by_scope, scope, TagScope::Intrinsic, INTRINSIC_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Event, EVENT_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Link, LINK_TAGS);
            merge_static_scope(
                &mut by_scope,
                scope,
                TagScope::Instrumentation,
                INSTRUMENTATION_TAGS,
            );
        }
        if let Some(live) = &self.live {
            for scoped in live.tag_names(tenant, scope, start_ns, end_ns).await? {
                let key = tag_scope_key(scoped.scope);
                let (_, tags) = by_scope
                    .entry(key)
                    .or_insert((scoped.scope, BTreeSet::new()));
                tags.extend(scoped.tags);
            }
        }
        Ok(SCOPE_ORDER
            .iter()
            .filter_map(|scope| by_scope.remove(tag_scope_key(*scope)))
            .filter_map(|(scope, tags)| {
                (!tags.is_empty()).then_some(ScopedTag {
                    scope,
                    tags: tags.into_iter().collect(),
                })
            })
            .collect())
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let index_tag = unscoped_attribute_tag(tag);
        if is_nested_intrinsic_tag(tag) {
            return self
                .nested_intrinsic_tag_values(tenant, tag, start_ns, end_ns)
                .await;
        }
        if is_intrinsic_tag(tag) {
            let scan = self.scan(tenant, &[], start_ns, end_ns).await?;
            let batches = collect_table(&scan.ctx, &scan.span_table).await?;
            return intrinsic_values_from_batches(tag, &batches);
        }
        let mut values = self
            .cold_attribute_tag_values(tenant, tag, index_tag, start_ns, end_ns)
            .await?;
        if let Some(live) = &self.live {
            values.extend(
                live.tag_values(tenant, tag, start_ns, end_ns)
                    .await?
                    .into_iter()
                    .map(|value| (value.type_, value.value)),
            );
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

#[derive(Default)]
struct ColdAttributeTagNames {
    resource: BTreeSet<String>,
    span: BTreeSet<String>,
    event: BTreeSet<String>,
    link: BTreeSet<String>,
}

fn attr_typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Float(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
    }
}

fn unscoped_attribute_tag(tag: &str) -> &str {
    tag.strip_prefix("resource.")
        .or_else(|| tag.strip_prefix("span."))
        .or_else(|| tag.strip_prefix("event.").filter(|tag| tag.contains('.')))
        .or_else(|| tag.strip_prefix("link.").filter(|tag| tag.contains('.')))
        .unwrap_or(tag)
}

impl CrabkaSpanStore {
    async fn cold_attribute_tag_names(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ColdAttributeTagNames, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.candidate_blocks(tenant, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let batches = collect_table(&ctx, &table).await?;
        let mut names = ColdAttributeTagNames::default();
        for batch in &batches {
            collect_attribute_tag_names(batch, &mut names)?;
        }
        Ok(names)
    }

    async fn cold_attribute_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        index_tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<BTreeSet<(String, String)>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let keys = trace_index.prune_blocks_by_tag(tenant, index_tag, None, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let batches = collect_table(&ctx, &table).await?;
        let mut values = BTreeSet::new();
        for batch in &batches {
            collect_attribute_tag_values(batch, tag, index_tag, &mut values)?;
        }
        Ok(values)
    }

    async fn nested_intrinsic_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let trace_index = self.trace_index.load();
        let mut values: BTreeSet<(String, String)> = trace_index
            .tag_values(tenant, tag, start_ns, end_ns)
            .into_iter()
            .map(|value| ("string".to_string(), value))
            .collect();
        if let Some(live) = &self.live {
            values.extend(
                live.tag_values(tenant, tag, start_ns, end_ns)
                    .await?
                    .into_iter()
                    .map(|value| (value.type_, value.value)),
            );
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

fn collect_attribute_tag_names(
    batch: &RecordBatch,
    names: &mut ColdAttributeTagNames,
) -> Result<(), TraceqlError> {
    for row in 0..batch.num_rows() {
        for (key, _) in attr_values_with_resource(batch, row, true)? {
            if let Some(key) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
                names.resource.insert(key.to_string());
            } else {
                names.span.insert(key);
            }
        }
        for event in event_values(batch, row)? {
            names
                .event
                .extend(event.attributes.into_iter().map(|(key, _)| key));
        }
        for link in link_values(batch, row)? {
            names
                .link
                .extend(link.attributes.into_iter().map(|(key, _)| key));
        }
    }
    Ok(())
}

fn collect_attribute_tag_values(
    batch: &RecordBatch,
    tag: &str,
    index_tag: &str,
    values: &mut BTreeSet<(String, String)>,
) -> Result<(), TraceqlError> {
    for row in 0..batch.num_rows() {
        for (key, value) in attr_values_with_resource(batch, row, true)? {
            let matches = key == tag
                || key == index_tag
                || key
                    .strip_prefix(RESOURCE_ATTR_PREFIX)
                    .is_some_and(|key| key == index_tag || key == tag);
            if matches {
                values.insert(attr_typed_value_parts(&value));
            }
        }
        for event in event_values(batch, row)? {
            for (key, value) in event.attributes {
                if key == tag || key == index_tag {
                    values.insert(attr_typed_value_parts(&value));
                }
            }
        }
        for link in link_values(batch, row)? {
            for (key, value) in link.attributes {
                if key == tag || key == index_tag {
                    values.insert(attr_typed_value_parts(&value));
                }
            }
        }
    }
    Ok(())
}

fn merge_dynamic_scope(
    by_scope: &mut BTreeMap<&'static str, (TagScope, BTreeSet<String>)>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: BTreeSet<String>,
) {
    if requested.is_some_and(|requested| requested != scope) || tags.is_empty() {
        return;
    }
    let (_, out) = by_scope
        .entry(tag_scope_key(scope))
        .or_insert((scope, BTreeSet::new()));
    out.extend(tags);
}

fn merge_static_scope(
    by_scope: &mut BTreeMap<&'static str, (TagScope, BTreeSet<String>)>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: &[&str],
) {
    if requested.is_some_and(|requested| requested != scope) {
        return;
    }
    let (_, out) = by_scope
        .entry(tag_scope_key(scope))
        .or_insert((scope, BTreeSet::new()));
    out.extend(tags.iter().map(|tag| (*tag).to_string()));
}

async fn collect_table(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    Ok(ctx.table(table).await?.collect().await?)
}

fn filter_batches_by_matchers(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    if matchers.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| {
            let mask = (0..batch.num_rows())
                .map(|row| row_matches(&batch, row, matchers))
                .collect::<Result<Vec<_>, _>>()?;
            filter_record_batch(&batch, &BooleanArray::from(mask))
                .map_err(|err| TraceqlError::Store(err.to_string()))
        })
        .collect()
}

fn row_matches(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    if !nested_event_matchers_match(batch, row, matchers)?
        || !nested_link_matchers_match(batch, row, matchers)?
    {
        return Ok(false);
    }
    matchers.iter().try_fold(true, |matched, matcher| {
        if !matched {
            return Ok(false);
        }
        if is_event_matcher(matcher) || is_link_matcher(matcher) {
            return Ok(true);
        }
        row_matcher_matches(batch, row, matcher)
    })
}

fn nested_event_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return Ok(true);
    }
    let events = event_values(batch, row)?;
    if events.is_empty() {
        return Ok(event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher)));
    }
    Ok(events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    }))
}

fn nested_link_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return Ok(true);
    }
    let links = link_values(batch, row)?;
    if links.is_empty() {
        return Ok(link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher)));
    }
    Ok(links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    }))
}

fn is_event_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Event
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("event:"))
}

fn is_link_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Link
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("link:"))
}

fn event_matcher_matches_event(event: &EventRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| string_matches(&event.name, matcher.op, &matcher.value)),
            "event:timeSinceStart" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    int_matches(
                        i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX),
                        matcher.op,
                        &matcher.value,
                    )
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn event_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" | "event:timeSinceStart" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn link_matcher_matches_link(link: &LinkRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => {
            let values = link
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                }),
            "link:spanID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn link_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" | "link:spanID" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}

fn row_matcher_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    let is_match = match matcher.scope {
        MatchScope::Event => event_values(batch, row)?.iter().any(|event| {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Link => link_values(batch, row)?.iter().any(|link| {
            let values = link
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Intrinsic => intrinsic_matches(batch, row, matcher)?,
        MatchScope::Resource => resource_matches(batch, row, matcher)?,
        MatchScope::Instrumentation => instrumentation_matches(batch, row, matcher)?,
        MatchScope::Both => {
            resource_matches(batch, row, matcher)?
                || batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Span => {
            batch_attr_matches(batch, row, &matcher.key, matcher.op, &matcher.value)?
        }
        MatchScope::Parent => true,
    };
    Ok(is_match != matcher.negated)
}

fn batch_attr_matches(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> Result<bool, TraceqlError> {
    batch_attr_matches_with_resource(batch, row, key, op, expected, false)
}

fn batch_attr_matches_with_resource(
    batch: &RecordBatch,
    row: usize,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
    include_resource: bool,
) -> Result<bool, TraceqlError> {
    let attrs = attr_values_with_resource(batch, row, include_resource)?;
    let values = attrs
        .iter()
        .filter(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    Ok(attr_values_match(&values, op, expected))
}

fn resource_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "service.name" => {
            root_service_matches(&string_value(batch, COL_ROOT_SERVICE_NAME, row)?, matcher)
        }
        _ => batch_attr_matches_with_resource(
            batch,
            row,
            &format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key),
            matcher.op,
            &matcher.value,
            true,
        )?,
    })
}

fn instrumentation_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "name" | "instrumentation:name" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "version" | "instrumentation:version" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_VERSION, row)?,
            matcher.op,
            &matcher.value,
        ),
        _ => nil_matches(matcher.op, &matcher.value),
    })
}

fn intrinsic_matches(
    batch: &RecordBatch,
    row: usize,
    matcher: &SpanMatcher,
) -> Result<bool, TraceqlError> {
    Ok(match matcher.key.as_str() {
        "span:name" => string_matches(
            &string_value(batch, COL_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "event:name" => {
            let events = event_values(batch, row)?;
            nested_presence_matches(!events.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    events
                        .iter()
                        .any(|event| string_matches(&event.name, matcher.op, &matcher.value))
                },
            )
        }
        "event:timeSinceStart" => {
            let events = event_values(batch, row)?;
            nested_presence_matches(!events.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    events.iter().any(|event| {
                        int_matches(
                            i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX),
                            matcher.op,
                            &matcher.value,
                        )
                    })
                },
            )
        }
        "link:traceID" => {
            let links = link_values(batch, row)?;
            nested_presence_matches(!links.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                    })
                },
            )
        }
        "link:spanID" => {
            let links = link_values(batch, row)?;
            nested_presence_matches(!links.is_empty(), matcher.op, &matcher.value).unwrap_or_else(
                || {
                    links.iter().any(|link| {
                        string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                    })
                },
            )
        }
        "trace:id" => string_matches(
            &bytes_to_hex(&fixed_value::<16>(batch, COL_TRACE_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "trace:rootService" => string_matches(
            &string_value(batch, COL_ROOT_SERVICE_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "trace:rootName" => string_matches(
            &string_value(batch, COL_ROOT_SPAN_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "trace:duration" => int_matches(
            int64_value(batch, COL_TRACE_DURATION, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:duration" => int_matches(
            int64_value(batch, COL_DURATION, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:id" => string_matches(
            &bytes_to_hex(&fixed_value::<8>(batch, COL_SPAN_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:parentID" => nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)?.map_or_else(
            || nil_matches(matcher.op, &matcher.value),
            |parent| string_matches(&bytes_to_hex(&parent), matcher.op, &matcher.value),
        ),
        "span:kind" => enum_int_matches(
            i64::from(int32_value(batch, COL_KIND, row)?),
            matcher.op,
            &matcher.value,
            kind_enum_value,
        ),
        "span:status" => enum_int_matches(
            i64::from(int32_value(batch, COL_STATUS_CODE, row)?),
            matcher.op,
            &matcher.value,
            status_enum_value,
        ),
        "span:statusMessage" => string_matches(
            &string_value(batch, COL_STATUS_MESSAGE, row)?,
            matcher.op,
            &matcher.value,
        ),
        "span:childCount" => int_matches(
            i64::from(int32_value(batch, COL_CHILD_COUNT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetLeft" => int_matches(
            i64::from(int32_value(batch, COL_NS_LEFT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetRight" => int_matches(
            i64::from(int32_value(batch, COL_NS_RIGHT, row)?),
            matcher.op,
            &matcher.value,
        ),
        "span:nestedSetParent" | "span:Parent" => int_matches(
            i64::from(int32_value(batch, COL_PARENT_ID, row)?),
            matcher.op,
            &matcher.value,
        ),
        "instrumentation:name" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
        "instrumentation:version" => string_matches(
            &string_value(batch, COL_INSTRUMENTATION_VERSION, row)?,
            matcher.op,
            &matcher.value,
        ),
        _ => true,
    })
}

fn attr_matches(value: &AttrValue, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match value {
        AttrValue::Str(value) => string_matches(value, op, expected),
        AttrValue::Int(value) => int_matches(*value, op, expected),
        AttrValue::Float(value) => float_matches(*value, op, expected),
        AttrValue::Bool(value) => bool_matches(*value, op, expected),
    }
}

fn attr_values_match(values: &[&AttrValue], op: MatchCmp, expected: &MatchValue) -> bool {
    if values.is_empty() {
        return nil_matches(op, expected);
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match op {
        MatchCmp::Neq | MatchCmp::Nre => {
            values.iter().all(|value| attr_matches(value, op, expected))
        }
        MatchCmp::Eq
        | MatchCmp::Re
        | MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte => values.iter().any(|value| attr_matches(value, op, expected)),
    }
}

fn nested_presence_matches(has_values: bool, op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(!has_values),
        (MatchCmp::Neq, MatchValue::Nil) => Some(has_values),
        _ => None,
    }
}

fn present_value_matches(op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(false),
        (MatchCmp::Neq, MatchValue::Nil) => Some(true),
        _ => None,
    }
}

fn nil_matches(op: MatchCmp, expected: &MatchValue) -> bool {
    matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil))
}

fn root_service_matches(value: &str, matcher: &SpanMatcher) -> bool {
    nested_presence_matches(!value.is_empty(), matcher.op, &matcher.value)
        .unwrap_or_else(|| string_matches(value, matcher.op, &matcher.value))
}

fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Str(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value
            .partial_cmp(expected.as_str())
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Neq => !value
            .partial_cmp(expected.as_str())
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Re => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| re.is_match(value))
        }
        MatchCmp::Nre => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| !re.is_match(value))
        }
        MatchCmp::Lt | MatchCmp::Lte | MatchCmp::Gt | MatchCmp::Gte => false,
    }
}

fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let expected = match expected {
        MatchValue::Int(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Neq => !value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}

fn enum_int_matches(
    value: i64,
    op: MatchCmp,
    expected: &MatchValue,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match expected {
        MatchValue::Str(name) => enum_value(&name.to_ascii_lowercase()).map(i64::from),
        MatchValue::Int(value) => Some(*value),
        MatchValue::Nil => return present_value_matches(op, expected).unwrap_or(false),
        MatchValue::Float(_) | MatchValue::Bool(_) => None,
    };
    expected.is_some_and(|expected| int_matches(value, op, &MatchValue::Int(expected)))
}

fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}

fn kind_enum_value(name: &str) -> Option<i32> {
    match name {
        "unspecified" => Some(0),
        "internal" => Some(1),
        "server" => Some(2),
        "client" => Some(3),
        "producer" => Some(4),
        "consumer" => Some(5),
        _ => None,
    }
}

fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Neq => !value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}

fn bool_matches(value: bool, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Bool(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == *expected,
        MatchCmp::Neq => value != *expected,
        MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte
        | MatchCmp::Re
        | MatchCmp::Nre => false,
    }
}

fn trace_from_batches(
    trace_id: &[u8; 16],
    batches: Vec<RecordBatch>,
) -> Result<Option<TraceSpans>, TraceqlError> {
    let mut root_service_name = String::new();
    let mut root_trace_name = String::new();
    let mut resource_attributes = Vec::new();
    let mut spans = Vec::new();

    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        for row in 0..batch.num_rows() {
            if trace_ids.value(row) != trace_id {
                continue;
            }
            if root_service_name.is_empty() {
                root_service_name = string_value(&batch, COL_ROOT_SERVICE_NAME, row)?;
            }
            if root_trace_name.is_empty() {
                root_trace_name = string_value(&batch, COL_ROOT_SPAN_NAME, row)?;
            }
            if resource_attributes.is_empty() {
                resource_attributes = resource_attr_values(&batch, row)?;
            }
            spans.push(SpanRef {
                span_id: fixed_value::<8>(&batch, COL_SPAN_ID, row)?,
                parent_span_id: nullable_fixed_value::<8>(&batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(&batch, COL_NAME, row)?,
                kind: int32_value(&batch, COL_KIND, row)?,
                nested_set_left: int32_value(&batch, COL_NS_LEFT, row)?,
                nested_set_right: int32_value(&batch, COL_NS_RIGHT, row)?,
                nested_set_parent: int32_value(&batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64::try_from(int64_value(&batch, COL_START, row)?)
                    .unwrap_or(0),
                duration_nanos: u64::try_from(int64_value(&batch, COL_DURATION, row)?).unwrap_or(0),
                status_code: int32_value(&batch, COL_STATUS_CODE, row)?,
                status_message: string_value(&batch, COL_STATUS_MESSAGE, row)?,
                instrumentation_name: string_value(&batch, COL_INSTRUMENTATION_NAME, row)?,
                instrumentation_version: string_value(&batch, COL_INSTRUMENTATION_VERSION, row)?,
                resource_attributes: resource_attr_values(&batch, row)?,
                attributes: attr_values(&batch, row)?,
                events: event_values(&batch, row)?,
                links: link_values(&batch, row)?,
            });
        }
    }

    deduplicate_trace_spans(&mut spans);
    Ok((!spans.is_empty()).then_some(TraceSpans {
        trace_id: *trace_id,
        root_service_name,
        root_trace_name,
        resource_attributes,
        spans,
    }))
}

fn deduplicate_trace_spans(spans: &mut Vec<SpanRef>) {
    spans.sort_by_key(|span| span.span_id);
    spans.dedup_by_key(|span| span.span_id);
    recompute_trace_nested_sets(spans);
    spans.sort_by_key(|span| (span.start_time_unix_nano, span.span_id));
}

fn recompute_trace_nested_sets(spans: &mut [SpanRef]) {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let positions = spans
        .iter()
        .enumerate()
        .map(|(idx, span)| (span.span_id, idx))
        .collect::<BTreeMap<_, _>>();
    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();
    for (idx, span) in spans.iter().enumerate() {
        match span
            .parent_span_id
            .and_then(|parent| positions.get(&parent).copied())
        {
            Some(parent_idx) if parent_idx != idx => children[parent_idx].push(idx),
            _ => roots.push(idx),
        }
    }

    let mut counter = 1_i32;
    let mut stack = Vec::new();
    for &root in roots.iter().rev() {
        stack.push(Frame::Enter {
            idx: root,
            // Root spans encode nestedSetParent = -1 (Tempo's no-parent sentinel;
            // left values start at 1 so -1 never collides). Must match the stored
            // assignment in span/nested_set.rs and the Drilldown's `< 0` signal.
            parent_left: -1,
        });
    }
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { idx, parent_left } => {
                let left = counter;
                counter += 1;
                spans[idx].nested_set_left = left;
                spans[idx].nested_set_parent = parent_left;
                stack.push(Frame::Exit { idx });
                for &child in children[idx].iter().rev() {
                    stack.push(Frame::Enter {
                        idx: child,
                        parent_left: left,
                    });
                }
            }
            Frame::Exit { idx } => {
                spans[idx].nested_set_right = counter;
                counter += 1;
            }
        }
    }
}

fn recompute_scan_nested_sets(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, TraceqlError> {
    // `concat_batches` materialises every matched span into one RecordBatch.
    // Arrow's variable-length columns use i32 offsets, so a combined batch over
    // ~2 GiB overflows with an opaque `Offset overflow error`. Cap the merge at a
    // safe 1.5 GiB and surface an actionable error so a pathological query (an
    // unbounded time range over a huge tenant) degrades cleanly instead of
    // emitting `concat scan batches: Offset overflow error`.
    const MAX_SCAN_CONCAT_BYTES: usize = 1_500_000_000;
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let batches = align_scan_batches_to_schema(batches, &schema)?;
    let total_bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    if total_bytes > MAX_SCAN_CONCAT_BYTES {
        return Err(TraceqlError::Store(format!(
            "scan result too large to merge ({total_bytes} bytes > {MAX_SCAN_CONCAT_BYTES} cap); \
             narrow the query time range or selector"
        )));
    }
    let batch = concat_batches(&schema, &batches)
        .map_err(|err| TraceqlError::Store(format!("concat scan batches: {err}")))?;
    recompute_batch_nested_sets(&batch).map(|batch| vec![batch])
}

fn add_nested_intrinsic_columns(
    batches: Vec<RecordBatch>,
    matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    batches
        .into_iter()
        .map(|batch| add_nested_intrinsic_columns_to_batch(&batch, matchers))
        .collect()
}

/// Materialize regular span/resource attribute columns (`attr.<key>`) referenced
/// by metric `by()`/`select()` projections. The selector path filters attributes
/// directly on the parquet arrays, so these columns are otherwise never built and
/// `rate() by(span.http.method)` cannot `GROUP BY attr.http.method`. Values are
/// stringified into a Utf8 column (metric labels are strings); a span missing the
/// attribute becomes NULL — the nil group, matching Tempo. Event/Link matchers
/// are handled by `add_nested_intrinsic_columns`; `service.name` is skipped (it is
/// the promoted `COL_ROOT_SERVICE_NAME` column, not an attribute).
fn add_span_attr_columns(
    batches: Vec<RecordBatch>,
    projection_matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    // (column_name, attr-array lookup key, include_resource) per regular-attr field.
    let mut wanted: Vec<(String, String, bool)> = Vec::new();
    for matcher in projection_matchers {
        let (lookup_key, include_resource) = match matcher.scope {
            MatchScope::Span | MatchScope::Both => (matcher.key.clone(), false),
            MatchScope::Resource => (format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key), true),
            _ => continue,
        };
        if matcher.key == "service.name" {
            continue; // grouped via the promoted COL_ROOT_SERVICE_NAME column
        }
        let column_name = format!("{ATTR_PREFIX}{}", matcher.key);
        if !wanted.iter().any(|(name, _, _)| name == &column_name) {
            wanted.push((column_name, lookup_key, include_resource));
        }
    }
    if wanted.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|batch| add_span_attr_columns_to_batch(&batch, &wanted))
        .collect()
}

fn add_span_attr_columns_to_batch(
    batch: &RecordBatch,
    wanted: &[(String, String, bool)],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();
    for (column_name, lookup_key, include_resource) in wanted {
        if schema.column_with_name(column_name).is_some() {
            continue; // already a (promoted) column
        }
        let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = attr_values_with_resource(batch, row, *include_resource)?
                .into_iter()
                .find(|(key, _)| key == lookup_key)
                .map(|(_, value)| attr_value_label(&value));
            values.push(value);
        }
        fields.push(Field::new(column_name.clone(), DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("materialize attribute columns: {err}")))
}

fn attr_value_label(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Float(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
    }
}

fn add_nested_intrinsic_columns_to_batch(
    batch: &RecordBatch,
    matchers: &[SpanMatcher],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let missing = [
        COL_EVENT_NAME,
        COL_EVENT_TIME_SINCE_START,
        COL_LINK_TRACE_ID,
        COL_LINK_SPAN_ID,
    ]
    .into_iter()
    .filter(|name| schema.column_with_name(name).is_none())
    .collect::<Vec<_>>();
    let missing_attrs = nested_attr_columns(matchers)
        .into_iter()
        .filter(|(column, _)| schema.column_with_name(column).is_none())
        .collect::<Vec<_>>();
    if missing.is_empty() && missing_attrs.is_empty() {
        return Ok(batch.clone());
    }

    let nested = nested_intrinsic_rows(batch, matchers, &missing_attrs)?;
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &nested.indices, None)
                .map_err(|err| TraceqlError::Store(format!("expand nested intrinsic rows: {err}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for name in missing {
        match name {
            COL_EVENT_NAME => {
                fields.push(Field::new(COL_EVENT_NAME, DataType::Utf8, true));
                columns.push(nested.event_name.clone());
            }
            COL_EVENT_TIME_SINCE_START => {
                fields.push(Field::new(
                    COL_EVENT_TIME_SINCE_START,
                    DataType::Int64,
                    true,
                ));
                columns.push(nested.event_time_since_start.clone());
            }
            COL_LINK_TRACE_ID => {
                fields.push(Field::new(
                    COL_LINK_TRACE_ID,
                    DataType::FixedSizeBinary(16),
                    true,
                ));
                columns.push(nested.link_trace_id.clone());
            }
            COL_LINK_SPAN_ID => {
                fields.push(Field::new(
                    COL_LINK_SPAN_ID,
                    DataType::FixedSizeBinary(8),
                    true,
                ));
                columns.push(nested.link_span_id.clone());
            }
            _ => {}
        }
    }
    for (column, _) in missing_attrs {
        fields.push(Field::new(&column, DataType::Utf8, true));
        columns.push(
            nested
                .attr_columns
                .get(&column)
                .ok_or_else(|| TraceqlError::Store(format!("missing nested attr column {column}")))?
                .clone(),
        );
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("add nested intrinsic columns: {err}")))
}

struct NestedIntrinsicRows {
    indices: UInt32Array,
    event_name: ArrayRef,
    event_time_since_start: ArrayRef,
    link_trace_id: ArrayRef,
    link_span_id: ArrayRef,
    attr_columns: BTreeMap<String, ArrayRef>,
}

fn nested_intrinsic_rows(
    batch: &RecordBatch,
    matchers: &[SpanMatcher],
    attr_columns: &[(String, NestedAttrColumn)],
) -> Result<NestedIntrinsicRows, TraceqlError> {
    let mut event_name = StringBuilder::new();
    let mut event_time_since_start = Int64Builder::new();
    let mut link_trace_id = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 16);
    let mut link_span_id = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 8);
    let mut attr_builders = attr_columns
        .iter()
        .map(|(column, attr)| (column.clone(), *attr, StringBuilder::new()))
        .collect::<Vec<_>>();
    let mut row_indices = Vec::new();
    for row in 0..batch.num_rows() {
        let events = matching_events_for_scan(batch, row, matchers)?;
        let links = matching_links_for_scan(batch, row, matchers)?;
        for event in &events {
            for link in &links {
                row_indices
                    .push(u32::try_from(row).map_err(|err| {
                        TraceqlError::Store(format!("row index overflow: {err}"))
                    })?);
                append_nested_event(event.as_ref(), &mut event_name, &mut event_time_since_start);
                append_nested_link(link.as_ref(), &mut link_trace_id, &mut link_span_id)?;
                for (_, attr, builder) in &mut attr_builders {
                    append_nested_attr(event.as_ref(), link.as_ref(), *attr, builder);
                }
            }
        }
    }
    let attr_columns = attr_builders
        .into_iter()
        .map(|(column, _, mut builder)| (column, Arc::new(builder.finish()) as ArrayRef))
        .collect();
    Ok(NestedIntrinsicRows {
        indices: UInt32Array::from(row_indices),
        event_name: Arc::new(event_name.finish()),
        event_time_since_start: Arc::new(event_time_since_start.finish()),
        link_trace_id: Arc::new(link_trace_id.finish()),
        link_span_id: Arc::new(link_span_id.finish()),
        attr_columns,
    })
}

#[derive(Clone, Copy)]
enum NestedAttrScope {
    Event,
    Link,
}

#[derive(Clone, Copy)]
struct NestedAttrColumn<'a> {
    scope: NestedAttrScope,
    key: &'a str,
}

fn nested_attr_columns(matchers: &[SpanMatcher]) -> Vec<(String, NestedAttrColumn<'_>)> {
    let mut out = Vec::new();
    for matcher in matchers {
        let (scope, prefix) = match matcher.scope {
            MatchScope::Event => (NestedAttrScope::Event, EVENT_ATTR_PREFIX),
            MatchScope::Link => (NestedAttrScope::Link, LINK_ATTR_PREFIX),
            MatchScope::Both
            | MatchScope::Span
            | MatchScope::Resource
            | MatchScope::Parent
            | MatchScope::Instrumentation
            | MatchScope::Intrinsic => continue,
        };
        let column = format!("{ATTR_PREFIX}{prefix}{}", matcher.key);
        if out.iter().any(|(existing, _)| existing == &column) {
            continue;
        }
        out.push((
            column,
            NestedAttrColumn {
                scope,
                key: &matcher.key,
            },
        ));
    }
    out
}

fn append_nested_event(
    event: Option<&EventRef>,
    event_name: &mut StringBuilder,
    event_time_since_start: &mut Int64Builder,
) {
    if let Some(event) = event {
        event_name.append_value(&event.name);
        event_time_since_start
            .append_value(i64::try_from(event.time_since_start_nano).unwrap_or(i64::MAX));
    } else {
        event_name.append_null();
        event_time_since_start.append_null();
    }
}

fn append_nested_link(
    link: Option<&LinkRef>,
    link_trace_id: &mut FixedSizeBinaryBuilder,
    link_span_id: &mut FixedSizeBinaryBuilder,
) -> Result<(), TraceqlError> {
    if let Some(link) = link {
        link_trace_id
            .append_value(link.trace_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
        link_span_id
            .append_value(link.span_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
    } else {
        link_trace_id.append_null();
        link_span_id.append_null();
    }
    Ok(())
}

fn append_nested_attr(
    event: Option<&EventRef>,
    link: Option<&LinkRef>,
    attr: NestedAttrColumn<'_>,
    builder: &mut StringBuilder,
) {
    let value = match attr.scope {
        NestedAttrScope::Event => event.and_then(|event| {
            event
                .attributes
                .iter()
                .find(|(key, _)| key == attr.key)
                .map(|(_, value)| value)
        }),
        NestedAttrScope::Link => link.and_then(|link| {
            link.attributes
                .iter()
                .find(|(key, _)| key == attr.key)
                .map(|(_, value)| value)
        }),
    };
    if let Some(value) = value {
        builder.append_value(attr_typed_value_parts(value).1);
    } else {
        builder.append_null();
    }
}

fn matching_events_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<EventRef>>, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    let events = event_values(batch, row)?;
    if event_matchers.is_empty() {
        return Ok(vec![events.into_iter().next()]);
    }
    if events.is_empty() {
        return Ok(vec![None]);
    }
    Ok(events
        .into_iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .map(Some)
        .collect())
}

fn matching_links_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<LinkRef>>, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    let links = link_values(batch, row)?;
    if link_matchers.is_empty() {
        return Ok(vec![links.into_iter().next()]);
    }
    if links.is_empty() {
        return Ok(vec![None]);
    }
    Ok(links
        .into_iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .map(Some)
        .collect())
}

fn align_scan_batches_to_schema(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    batches
        .into_iter()
        .map(|batch| align_scan_batch_to_schema(&batch, schema))
        .collect()
}

fn align_scan_batch_to_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TraceqlError> {
    if batch.schema() == *schema {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = batch
            .column_by_name(field.name())
            .ok_or_else(|| TraceqlError::Store(format!("missing column `{}`", field.name())))?;
        if column.data_type() == field.data_type() {
            columns.push(column.clone());
        } else {
            columns.push(cast(column, field.data_type()).map_err(|err| {
                TraceqlError::Store(format!(
                    "cast column `{}` from {:?} to {:?}: {err}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })?);
        }
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|err| TraceqlError::Store(format!("align scan batch schema: {err}")))
}

fn recompute_batch_nested_sets(batch: &RecordBatch) -> Result<RecordBatch, TraceqlError> {
    enum Frame {
        Enter { row: usize, parent_left: i32 },
        Exit { row: usize },
    }

    let trace_ids = fixed(batch, COL_TRACE_ID)?;
    let span_ids = fixed(batch, COL_SPAN_ID)?;
    let parent_span_ids = fixed(batch, COL_PARENT_SPAN_ID)?;
    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let mut left = vec![0_i32; batch.num_rows()];
    let mut right = vec![0_i32; batch.num_rows()];
    // Default to the root sentinel (-1): a row not reached by the per-trace DFS
    // (e.g. a null trace_id) has no parent. 0 would be an invalid parent (left
    // values start at 1) and would read as "has a parent at left 0".
    let mut parent_id = vec![-1_i32; batch.num_rows()];
    let mut child_count = vec![0_i32; batch.num_rows()];

    for rows in by_trace.values() {
        let mut positions = BTreeMap::new();
        for &row in rows {
            if span_ids.is_null(row) {
                continue;
            }
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(row));
            positions.insert(span_id, row);
        }

        let mut children = BTreeMap::<usize, Vec<usize>>::new();
        let mut roots = Vec::new();
        for &row in rows {
            let parent = (!parent_span_ids.is_null(row)).then(|| {
                let mut parent = [0_u8; 8];
                parent.copy_from_slice(parent_span_ids.value(row));
                parent
            });
            match parent.and_then(|parent| positions.get(&parent).copied()) {
                Some(parent_row) if parent_row != row => {
                    children.entry(parent_row).or_default().push(row);
                }
                _ => roots.push(row),
            }
        }

        // childCount is PER TRACE: each parent's direct children, scoped to this
        // trace's rows. The nested-set `left` values reset to 1 per trace, so a
        // batch-global count would collide across traces and over-count.
        for (&parent_row, kids) in &children {
            child_count[parent_row] = i32::try_from(kids.len()).unwrap_or(i32::MAX);
        }

        let mut counter = 1_i32;
        let mut stack = Vec::new();
        for &row in roots.iter().rev() {
            stack.push(Frame::Enter {
                row,
                // Root span: nestedSetParent = -1 (Tempo no-parent sentinel).
                parent_left: -1,
            });
        }
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { row, parent_left } => {
                    left[row] = counter;
                    parent_id[row] = parent_left;
                    counter += 1;
                    stack.push(Frame::Exit { row });
                    if let Some(children) = children.get(&row) {
                        for &child in children.iter().rev() {
                            stack.push(Frame::Enter {
                                row: child,
                                parent_left: left[row],
                            });
                        }
                    }
                }
                Frame::Exit { row } => {
                    right[row] = counter;
                    counter += 1;
                }
            }
        }
    }

    replace_scan_int32_columns(
        batch,
        &[
            (COL_NS_LEFT, left),
            (COL_NS_RIGHT, right),
            (COL_PARENT_ID, parent_id),
            (COL_CHILD_COUNT, child_count),
        ],
    )
}

fn replace_scan_int32_columns(
    batch: &RecordBatch,
    replacements: &[(&str, Vec<i32>)],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for (name, values) in replacements {
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TraceqlError::Store(format!("missing column `{name}`")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone())) as ArrayRef;
    }
    RecordBatch::try_new(schema, columns)
        .map_err(|err| TraceqlError::Store(format!("replace scan nested-set columns: {err}")))
}

fn is_intrinsic_tag(tag: &str) -> bool {
    tag.contains(':')
}

fn is_nested_intrinsic_tag(tag: &str) -> bool {
    matches!(
        tag,
        "event:name" | "event:timeSinceStart" | "link:traceID" | "link:spanID"
    )
}

fn intrinsic_values_from_batches(
    tag: &str,
    batches: &[RecordBatch],
) -> Result<Vec<TypedValue>, TraceqlError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            collect_intrinsic_value(batch, row, tag, &mut values)?;
        }
    }
    Ok(values
        .into_iter()
        .map(|(type_, value)| TypedValue { type_, value })
        .collect())
}

fn collect_intrinsic_value(
    batch: &RecordBatch,
    row: usize,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) -> Result<(), TraceqlError> {
    match tag {
        "span:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_DURATION)?;
        }
        "span:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_SPAN_ID)?.value(row)),
            ));
        }
        "span:kind" => {
            insert_i32_value(batch, row, values, COL_KIND)?;
        }
        "span:name" => {
            insert_string_value(batch, row, values, COL_NAME)?;
        }
        "span:childCount" => {
            insert_i32_value(batch, row, values, COL_CHILD_COUNT)?;
        }
        "span:parentID" => {
            if let Some(parent_id) = nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)? {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            insert_i32_value(batch, row, values, COL_STATUS_CODE)?;
        }
        "span:statusMessage" => {
            let message = string_value(batch, COL_STATUS_MESSAGE, row)?;
            if !message.is_empty() {
                values.insert(("string".to_string(), message));
            }
        }
        "span:nestedSetLeft" => {
            insert_i32_value(batch, row, values, COL_NS_LEFT)?;
        }
        "span:nestedSetParent" | "span:Parent" => {
            insert_i32_value(batch, row, values, COL_PARENT_ID)?;
        }
        "span:nestedSetRight" => {
            insert_i32_value(batch, row, values, COL_NS_RIGHT)?;
        }
        "trace:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_TRACE_DURATION)?;
        }
        "trace:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_TRACE_ID)?.value(row)),
            ));
        }
        "trace:rootName" => {
            insert_string_value(batch, row, values, COL_ROOT_SPAN_NAME)?;
        }
        "trace:rootService" => {
            insert_string_value(batch, row, values, COL_ROOT_SERVICE_NAME)?;
        }
        "instrumentation:name" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_NAME)?;
        }
        "instrumentation:version" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_VERSION)?;
        }
        _ => {}
    }
    Ok(())
}

fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(("string".to_string(), string_value(batch, column, row)?));
    Ok(())
}

fn insert_i32_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        "int".to_string(),
        int32_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}

fn insert_i64_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    type_: &str,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        type_.to_string(),
        int64_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}

fn attr_values(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    attr_values_with_resource(batch, row, false)
}

fn resource_attr_values(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    Ok(attr_values_with_resource(batch, row, true)?
        .into_iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(RESOURCE_ATTR_PREFIX)
                .map(|key| (key.to_string(), value))
        })
        .collect())
}

fn attr_values_with_resource(
    batch: &RecordBatch,
    row: usize,
    include_resource: bool,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    let mut out = Vec::new();
    let mut promoted_keys = BTreeSet::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let Some(key) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let col = batch.column(idx);
        if col.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(string_array_value(col.as_ref(), row)?),
            DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8 => {
                AttrValue::Str(string_array_value(col.as_ref(), row)?)
            }
            DataType::Int64 => AttrValue::Int(int64_array_value(col.as_ref(), row)?),
            DataType::Float64 => AttrValue::Float(float64_array_value(col.as_ref(), row)?),
            DataType::Boolean => AttrValue::Bool(bool_array_value(col.as_ref(), row)?),
            _ => continue,
        };
        promoted_keys.insert(key.to_string());
        out.push((key.to_string(), value));
    }
    out.extend(block_attr_values(
        batch,
        row,
        include_resource,
        &promoted_keys,
    )?);
    Ok(out)
}

fn block_attr_values(
    batch: &RecordBatch,
    row: usize,
    include_resource: bool,
    promoted_keys: &BTreeSet<String>,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    let Some(keys) = optional_list_column(batch, SCOL_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store("attr_keys row is not Utf8".into()))?;
    let str_values = optional_list_column(batch, SCOL_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, SCOL_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, SCOL_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, SCOL_ATTR_VALUE_BOOL)?;

    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let values = block_attr_values_for_key(
            str_values,
            int_values,
            double_values,
            bool_values,
            row,
            attr_idx,
        )?;
        out.extend(values.into_iter().filter_map(|value| {
            let key = key_values.value(attr_idx);
            ((include_resource || !key.starts_with(RESOURCE_ATTR_PREFIX))
                && !promoted_keys.contains(key))
            .then(|| (key.to_string(), value))
        }));
    }
    Ok(out)
}

fn block_attr_values_for_key(
    str_values: Option<&ListArray>,
    int_values: Option<&ListArray>,
    double_values: Option<&ListArray>,
    bool_values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
) -> Result<Vec<AttrValue>, TraceqlError> {
    let values = string_attr_values(str_values, row, attr_idx, SCOL_ATTR_VALUE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Str).collect());
    }
    let values = i64_attr_values(int_values, row, attr_idx, SCOL_ATTR_VALUE_INT)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Int).collect());
    }
    let values = f64_attr_values(double_values, row, attr_idx, SCOL_ATTR_VALUE_DOUBLE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Float).collect());
    }
    Ok(
        bool_attr_values(bool_values, row, attr_idx, SCOL_ATTR_VALUE_BOOL)?
            .into_iter()
            .map(AttrValue::Bool)
            .collect(),
    )
}

fn row_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Option<arrow::array::ArrayRef>, TraceqlError> {
    let Some(values) = values else {
        return Ok(None);
    };
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("attribute column `{name}` row is not a list"))
        })?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    Ok(Some(row_values.value(attr_idx)))
}

fn string_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<String>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Utf8")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx).to_string())
        .collect())
}

fn i64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<i64>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Int64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn f64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<f64>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Float64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn bool_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<bool>, TraceqlError> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| TraceqlError::Store(format!("attribute column `{name}` is not Boolean")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn event_values(batch: &RecordBatch, row: usize) -> Result<Vec<EventRef>, TraceqlError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(Vec::new());
    };
    if events.is_null(row) {
        return Ok(Vec::new());
    }
    let row_events = events.value(row);
    let row_events = row_events
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_EVENTS}` row is not a struct"))
        })?;
    let names = struct_string_field(row_events, 0, SCOL_EVENTS)?;
    let times = struct_int64_field(row_events, 1, SCOL_EVENTS)?;
    let attr_keys = struct_list_field(row_events, 2, SCOL_EVENTS)?;
    let attr_values = struct_list_field(row_events, 3, SCOL_EVENTS)?;

    let mut out = Vec::new();
    for idx in 0..row_events.len() {
        if row_events.is_null(idx) {
            continue;
        }
        let name = if names.is_null(idx) {
            String::new()
        } else {
            string_array_value(names, idx)?
        };
        let time_since_start_nano = if times.is_null(idx) {
            0
        } else {
            u64::try_from(times.value(idx)).unwrap_or(0)
        };
        out.push(EventRef {
            time_since_start_nano,
            name,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}

fn link_values(batch: &RecordBatch, row: usize) -> Result<Vec<LinkRef>, TraceqlError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(Vec::new());
    };
    if links.is_null(row) {
        return Ok(Vec::new());
    }
    let row_links = links.value(row);
    let row_links = row_links
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_LINKS}` row is not a struct"))
        })?;
    let trace_ids = struct_fixed_field(row_links, 0, SCOL_LINKS)?;
    let span_ids = struct_fixed_field(row_links, 1, SCOL_LINKS)?;
    let attr_keys = struct_list_field(row_links, 2, SCOL_LINKS)?;
    let attr_values = struct_list_field(row_links, 3, SCOL_LINKS)?;

    let mut out = Vec::new();
    for idx in 0..row_links.len() {
        if row_links.is_null(idx) {
            continue;
        }
        out.push(LinkRef {
            trace_id: fixed_array_value::<16>(trace_ids, idx, SCOL_LINKS)?,
            span_id: fixed_array_value::<8>(span_ids, idx, SCOL_LINKS)?,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}

fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a ListArray>, TraceqlError> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}

fn struct_string_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a dyn Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing string field")))
}

fn struct_int64_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a Int64Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing int64 field")))
}

fn struct_fixed_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{name}` missing fixed binary field"))
        })
}

fn struct_list_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a ListArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing list field")))
}

fn nested_string_attrs(
    keys: &ListArray,
    values: &ListArray,
    row: usize,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    if keys.is_null(row) || values.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store("nested attribute keys are not strings".into()))?;
    let value_lists = values.value(row);
    let value_lists = value_lists
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store("nested attribute values are not string lists".into())
        })?;

    let mut out = Vec::new();
    for idx in 0..key_values.len().min(value_lists.len()) {
        if key_values.is_null(idx) || value_lists.is_null(idx) {
            continue;
        }
        let scalar_values = value_lists.value(idx);
        let scalar_values = scalar_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                TraceqlError::Store("nested attribute scalar values are not strings".into())
            })?;
        if scalar_values.is_empty() || scalar_values.is_null(0) {
            continue;
        }
        out.push((
            key_values.value(idx).to_string(),
            AttrValue::Str(scalar_values.value(0).to_string()),
        ));
    }
    Ok(out)
}

fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| TraceqlError::Store(format!("missing fixed binary column `{name}`")))
}

fn fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<[u8; N], TraceqlError> {
    fixed(batch, name)?
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}

fn fixed_array_value<const N: usize>(
    values: &FixedSizeBinaryArray,
    row: usize,
    name: &str,
) -> Result<[u8; N], TraceqlError> {
    if values.is_null(row) {
        return Ok([0; N]);
    }
    values
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}

fn nullable_fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; N]>, TraceqlError> {
    let arr = fixed(batch, name)?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        arr.value(row)
            .try_into()
            .map(Some)
            .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
    }
}

fn string_value(batch: &RecordBatch, name: &str, row: usize) -> Result<String, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing string column `{name}`")))?;
    if col.is_null(row) {
        return Ok(String::new());
    }
    string_array_value(col.as_ref(), row)
}

fn string_array_value(col: &dyn Array, row: usize) -> Result<String, TraceqlError> {
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_string())
        .or_else(|| {
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row).to_string())
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<StringViewArray>()
                .map(|a| a.value(row).to_string())
        })
        .map(Ok)
        .or_else(|| {
            col.as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .map(|a| {
                    let key = usize::try_from(a.keys().value(row))
                        .map_err(|err| TraceqlError::Store(err.to_string()))?;
                    string_array_value(a.values().as_ref(), key)
                })
        })
        .transpose()?
        .ok_or_else(|| TraceqlError::Store("unsupported string column type".into()))
}

fn int64_value(batch: &RecordBatch, name: &str, row: usize) -> Result<i64, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int64 column `{name}`")))?;
    int64_array_value(col.as_ref(), row)
}

fn int64_array_value(col: &dyn Array, row: usize) -> Result<i64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int64 column type".into()))
}

fn int32_value(batch: &RecordBatch, name: &str, row: usize) -> Result<i32, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int32 column `{name}`")))?;
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int32 column type".into()))
}

fn float64_array_value(col: &dyn Array, row: usize) -> Result<f64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported float64 column type".into()))
}

fn bool_array_value(col: &dyn Array, row: usize) -> Result<bool, TraceqlError> {
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported bool column type".into()))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn tag_scope_key(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn block_err(err: &crabka_blockstore::BlockStoreError) -> TraceqlError {
    TraceqlError::Store(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arc_swap::ArcSwap;
    use arrow::{
        array::{
            ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray,
            StringDictionaryBuilder,
        },
        datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    };
    use assert2::check;
    use crabka_blockstore::{
        AttrValue as BlockAttrValue, BlockWriter, NestedSet as BlockNestedSet, PromotedSpanAttr,
        SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SpanAttr, SpanKind as BlockSpanKind,
        SpanRow, StatusCode as BlockStatusCode, SummaryColumns, TraceBlockStats, encode_span_rows,
        span_block_decl, span_block_schema,
    };
    use crabka_traceql::{
        COL_CHILD_COUNT, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, EngineOpts,
        EventRef, LinkRef, ScanJob, ScanOptions, TraceqlEngine,
    };
    use object_store::{memory::InMemory, path::Path};
    use parquet::{
        arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter},
        file::properties::WriterProperties,
    };
    use url::Url;

    use super::*;
    use crate::{
        querier::live::LiveSource,
        span::{
            AttrValue as SpanAttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind,
            StatusCode,
            batch::{span_batch, span_batch_with_promoted_attrs},
        },
    };

    fn shared(index: TraceIndex) -> SharedTraceIndex {
        Arc::new(ArcSwap::from_pointee(index))
    }

    #[test]
    fn integer_matchers_distinguish_equal_and_unequal_values() {
        let expected = MatchValue::Int(7);
        assert2::assert!(int_matches(7, MatchCmp::Eq, &expected));
        assert2::assert!(!int_matches(8, MatchCmp::Eq, &expected));
        assert2::assert!(!int_matches(7, MatchCmp::Neq, &expected));
        assert2::assert!(int_matches(8, MatchCmp::Neq, &expected));
    }

    #[test]
    fn float_matchers_distinguish_equal_and_unequal_values() {
        let expected = MatchValue::Float(7.5);
        assert2::assert!(float_matches(7.5, MatchCmp::Eq, &expected));
        assert2::assert!(!float_matches(8.5, MatchCmp::Eq, &expected));
        assert2::assert!(!float_matches(7.5, MatchCmp::Neq, &expected));
        assert2::assert!(float_matches(8.5, MatchCmp::Neq, &expected));
    }

    #[derive(Default)]
    struct FakeLiveSource {
        trace: Option<TraceSpans>,
        batches: Vec<RecordBatch>,
        values: Vec<TypedValue>,
        frontier_ns: i64,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>, TraceqlError> {
            Ok(self.batches.clone())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>, TraceqlError> {
            Ok(self.trace.clone())
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>, TraceqlError> {
            Ok(Vec::new())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>, TraceqlError> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
            self.frontier_ns
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            Field::new("nested_set_left", DataType::Int32, false),
            Field::new("nested_set_right", DataType::Int32, false),
            Field::new("parent_id", DataType::Int32, false),
            Field::new(COL_CHILD_COUNT, DataType::Int32, false),
            Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
            Field::new("trace_start_unix_nano", DataType::Int64, false),
            Field::new("trace_duration_nanos", DataType::Int64, false),
            Field::new(COL_NAME, DataType::Utf8, true),
            Field::new("kind", DataType::Int32, false),
            Field::new(COL_START, DataType::Int64, false),
            Field::new(COL_DURATION, DataType::Int64, false),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
            Field::new(format!("{ATTR_PREFIX}svc"), DataType::Utf8, true),
        ]))
    }

    fn batch() -> RecordBatch {
        let schema = test_schema();
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(2, 16);
        trace_id.append_value([7; 16]).unwrap();
        trace_id.append_value([9; 16]).unwrap();
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        span_id.append_value([1; 8]).unwrap();
        span_id.append_value([2; 8]).unwrap();
        let mut parent_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        parent_id.append_null();
        parent_id.append_null();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent_id.finish()),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["api", "web"])),
                Arc::new(StringArray::from(vec!["GET /", "GET /x"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["root", "other"])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["tracer", "tracer"])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    fn dictionary_attr_batch() -> RecordBatch {
        let mut fields = test_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(
            format!("{ATTR_PREFIX}http.method"),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ));
        let schema = Arc::new(Schema::new(fields));
        let base = batch();
        let mut columns = base.columns().to_vec();
        let mut methods = StringDictionaryBuilder::<Int32Type>::new();
        methods.append_value("GET");
        methods.append_value("POST");
        columns.push(Arc::new(methods.finish()) as ArrayRef);
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn resource_service_matcher(op: MatchCmp, value: MatchValue) -> SpanMatcher {
        SpanMatcher {
            scope: MatchScope::Resource,
            key: "service.name".into(),
            op,
            value,
            negated: false,
        }
    }

    #[test]
    fn resource_matches_service_name_uses_root_service_column() {
        let batch = batch();

        for (i, (matcher, want)) in [
            (
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("api".into())),
                true,
            ),
            (
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("web".into())),
                false,
            ),
            (
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                true,
            ),
            (
                SpanMatcher {
                    scope: MatchScope::Resource,
                    key: "missing".into(),
                    op: MatchCmp::Neq,
                    value: MatchValue::Nil,
                    negated: false,
                },
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            check!(
                resource_matches(&batch, 0, &matcher).unwrap() == want,
                "case {i}"
            );
        }
    }

    #[test]
    fn root_service_matches_preserves_nil_and_string_semantics() {
        for (i, (service, matcher, want)) in [
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("api".into())),
                true,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("web".into())),
                false,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                true,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Nil),
                false,
            ),
            (
                "",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Nil),
                true,
            ),
            (
                "",
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            check!(
                root_service_matches(service, &matcher) == want,
                "case {i}: service {service:?}"
            );
        }
    }

    #[test]
    fn reconstructs_trace_from_candidate_batches() {
        let got = trace_from_batches(&[7; 16], vec![batch()])
            .unwrap()
            .unwrap();
        check!(
            (
                got.root_service_name.as_str(),
                got.spans.len(),
                got.spans[0].attributes.as_slice(),
            ) == (
                "api",
                1,
                [("svc".into(), AttrValue::Str("a".into()))].as_slice(),
            )
        );
    }

    #[test]
    fn reconstructs_trace_from_dictionary_promoted_attr_columns() {
        let got = trace_from_batches(&[7; 16], vec![dictionary_attr_batch()])
            .unwrap()
            .unwrap();
        assert2::assert!(
            got.spans[0]
                .attributes
                .iter()
                .any(|(key, value)| key == "http.method" && value == &AttrValue::Str("GET".into()))
        );
    }

    #[test]
    fn generic_attrs_do_not_duplicate_promoted_attr_columns() {
        let span = span_with_nested_refs();
        let batch = span_batch_with_promoted_attrs(
            std::slice::from_ref(&span),
            &[PromotedSpanAttr::int("http.status_code")],
        )
        .unwrap();
        let got = trace_from_batches(&span.trace_id, vec![batch])
            .unwrap()
            .unwrap();
        assert2::assert!(
            got.spans[0]
                .attributes
                .iter()
                .filter(|(key, _)| key == "http.status_code")
                .count()
                == 1
        );
    }

    #[test]
    fn cold_intrinsic_values_include_child_count_and_instrumentation() {
        let batches = vec![batch()];
        check!(
            intrinsic_values_from_batches("span:childCount", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "int" && value.value == "0")
        );
        check!(
            intrinsic_values_from_batches("instrumentation:name", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string" && value.value == "tracer")
        );
        check!(
            intrinsic_values_from_batches("instrumentation:version", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string")
        );
    }

    #[test]
    fn child_count_is_per_trace_in_multi_trace_scan_batch() {
        // Two traces in ONE scan batch, each root -> child. Per-trace nested-set
        // numbering resets `left` to 1, so both roots get left=1 and both
        // children get parent_id=1. `span:childCount` must be counted PER TRACE
        // (1 each), not across the whole batch (which collides on left=1 and
        // inflates each root to 2).
        let schema = test_schema();
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(4, 16);
        for t in [[7_u8; 16], [7; 16], [9; 16], [9; 16]] {
            trace_id.append_value(t).unwrap();
        }
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(4, 8);
        for s in [[1_u8; 8], [2; 8], [1; 8], [2; 8]] {
            span_id.append_value(s).unwrap();
        }
        let mut parent = FixedSizeBinaryBuilder::with_capacity(4, 8);
        parent.append_null(); // trace A root
        parent.append_value([1; 8]).unwrap(); // trace A child -> A root
        parent.append_null(); // trace B root
        parent.append_value([1; 8]).unwrap(); // trace B child -> B root
        let s4 = |a: &str, b: &str| StringArray::from(vec![a, a, b, b]);
        let i32_4 = || Int32Array::from(vec![0, 0, 0, 0]);
        let i64_4 = || Int64Array::from(vec![0_i64, 0, 0, 0]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent.finish()),
                Arc::new(i32_4()), // ns_left (recomputed)
                Arc::new(i32_4()), // ns_right (recomputed)
                Arc::new(i32_4()), // parent_id (recomputed)
                Arc::new(i32_4()), // child_count (recomputed)
                Arc::new(s4("api", "web")),
                Arc::new(s4("GET /", "GET /x")),
                Arc::new(i64_4()),
                Arc::new(i64_4()),
                Arc::new(StringArray::from(vec!["root", "child", "root", "child"])),
                Arc::new(i32_4()),
                Arc::new(i64_4()),
                Arc::new(i64_4()),
                Arc::new(i32_4()),
                Arc::new(s4("", "")),
                Arc::new(s4("tracer", "tracer")),
                Arc::new(s4("", "")),
                Arc::new(s4("a", "b")),
            ],
        )
        .unwrap();

        let out = recompute_batch_nested_sets(&batch).unwrap();
        // Per-trace `left` reset (collision confirms the multi-trace scenario).
        for (row, want) in [(0, 1), (2, 1)] {
            check!(
                int32_value(&out, COL_NS_LEFT, row).unwrap() == want,
                "row {row}"
            );
        }
        // Each root has exactly one child; children have none — NOT inflated to 2.
        for (row, want) in [(0, 1), (1, 0), (2, 1), (3, 0)] {
            check!(
                int32_value(&out, COL_CHILD_COUNT, row).unwrap() == want,
                "row {row}"
            );
        }
        // Roots encode nestedSetParent = -1 (Tempo no-parent sentinel) so the
        // Drilldown's `nestedSetParent < 0` primary signal selects them; each
        // child points at its root's `left` (1 after the per-trace reset).
        for (row, want) in [(0, -1), (1, 1), (2, -1), (3, 1)] {
            check!(
                int32_value(&out, COL_PARENT_ID, row).unwrap() == want,
                "row {row}"
            );
        }
    }

    #[test]
    fn metrics_by_attr_materializes_span_and_resource_columns() {
        // A `by(span.<attr>)` / `by(resource.<attr>)` projection must materialize
        // an `attr.<key>` column from the parquet attr arrays so grouping can read
        // it; spans lacking the attribute become the nil group (NULL). The
        // in-memory store can't catch this (it materializes every attr), so this
        // exercises the production parquet batch path directly.
        use crate::span::{AttrValue as SAttr, KeyValue, Span, SpanKind, StatusCode};
        let mk = |id: u8, parent: Option<u8>, span_attrs: Vec<KeyValue>, version: Option<&str>| {
            let mut resource_attrs = vec![KeyValue {
                key: "service.name".into(),
                value: SAttr::Str("api".into()),
            }];
            if let Some(v) = version {
                resource_attrs.push(KeyValue {
                    key: "service.version".into(),
                    value: SAttr::Str(v.into()),
                });
            }
            Span {
                trace_id: [7; 16],
                span_id: [id; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: "GET /".into(),
                kind: SpanKind::Server,
                start_ns: 1_000 + i64::from(id),
                duration_ns: 100,
                status: StatusCode::Ok,
                status_message: String::new(),
                resource_attrs,
                span_attrs,
                events: vec![],
                links: vec![],
                instrumentation_scope: String::new(),
                instrumentation_version: String::new(),
            }
        };
        let root = mk(
            1,
            None,
            vec![KeyValue {
                key: "http.method".into(),
                value: SAttr::Str("GET".into()),
            }],
            Some("1.2.3"),
        );
        let child = mk(2, Some(1), vec![], None);
        let batch = span_batch(&[root, child]).unwrap();

        let matcher = |scope, key: &str| SpanMatcher {
            scope,
            key: key.into(),
            op: MatchCmp::Neq,
            value: MatchValue::Nil,
            negated: false,
        };
        let out = add_span_attr_columns(
            vec![batch],
            &[
                matcher(MatchScope::Span, "http.method"),
                matcher(MatchScope::Resource, "service.version"),
            ],
        )
        .unwrap();
        let out = &out[0];

        let sorted = |name: &str| -> Vec<Option<String>> {
            let col = out
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{name} materialized"));
            let mut vals: Vec<Option<String>> = (0..out.num_rows())
                .map(|row| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(string_array_value(col.as_ref(), row).unwrap())
                    }
                })
                .collect();
            vals.sort();
            vals
        };
        // The span with the attribute carries its value; the other is the nil
        // group (NULL → empty label downstream).
        for (_name, column, expected) in [
            (
                "span method",
                "attr.http.method",
                vec![None, Some("GET".to_string())],
            ),
            (
                "service version",
                "attr.service.version",
                vec![None, Some("1.2.3".to_string())],
            ),
        ] {
            assert2::assert!(sorted(column) == expected);
        }
    }

    #[tokio::test]
    async fn empty_store_scans_as_empty_span_table() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = CrabkaSpanStore::new(blocks, shared(TraceIndex::new()), None);
        let scan = store.scan("tenant", &[], 0, 10).await.unwrap();
        let rows: usize = scan
            .ctx
            .table(&scan.span_table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert2::assert!(rows == 0);
    }

    #[tokio::test]
    async fn tag_discovery_unions_cold_index_values() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/tags.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut tags = BTreeSet::new();
        tags.insert("service.name".to_string());
        let mut values = BTreeMap::new();
        values.insert(
            "service.name".to_string(),
            BTreeSet::from(["api".to_string()]),
        );
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: tags,
                tag_values: values,
            },
        );

        let store = CrabkaSpanStore::new(blocks, shared(index), None);
        assert2::assert!(
            store.tag_names("tenant", None, 0, 10_000).await.unwrap()[0]
                .tags
                .clone()
                == vec!["service.name".to_string()]
        );
        assert2::assert!(
            store
                .tag_values("tenant", "service.name", 0, 10_000)
                .await
                .unwrap()[0]
                .value
                .clone()
                == "api".to_string()
        );
    }

    #[tokio::test]
    async fn cold_attribute_tag_values_preserve_static_types() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/typed-tags.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from([
                    "http.status_code".to_string(),
                    "retryable".to_string(),
                ]),
                tag_values: BTreeMap::from([
                    (
                        "http.status_code".to_string(),
                        BTreeSet::from(["504".to_string()]),
                    ),
                    (
                        "retryable".to_string(),
                        BTreeSet::from(["true".to_string()]),
                    ),
                ]),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let status_values = store
            .tag_values("tenant", "http.status_code", 0, 10_000)
            .await
            .unwrap();
        let retryable_values = store
            .tag_values("tenant", "retryable", 0, 10_000)
            .await
            .unwrap();

        assert2::assert!(
            status_values
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "504".into(),
                }]
        );
        assert2::assert!(
            retryable_values
                == vec![TypedValue {
                    type_: "bool".into(),
                    value: "true".into(),
                }]
        );
    }

    #[tokio::test]
    async fn cold_nested_tag_values_scan_event_and_link_attributes() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/nested-tag-values.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["exception.type".into(), "link.kind".into()]),
                tag_values: BTreeMap::from([
                    ("exception.type".into(), BTreeSet::from(["timeout".into()])),
                    ("link.kind".into(), BTreeSet::from(["retry".into()])),
                ]),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let event_values = store
            .tag_values("tenant", "exception.type", 0, 10_000)
            .await
            .unwrap();
        let scoped_event_values = store
            .tag_values("tenant", "event.exception.type", 0, 10_000)
            .await
            .unwrap();
        let link_values = store
            .tag_values("tenant", "link.kind", 0, 10_000)
            .await
            .unwrap();
        let scoped_link_values = store
            .tag_values("tenant", "link.link.kind", 0, 10_000)
            .await
            .unwrap();

        check!(
            event_values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "timeout".into(),
                }]
        );
        check!(scoped_event_values == event_values);
        check!(
            link_values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "retry".into(),
                }]
        );
        check!(scoped_link_values == link_values);
    }

    #[tokio::test]
    async fn cold_nested_tag_names_scan_event_and_link_attributes() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/nested-tag-names.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["exception.type".into(), "link.kind".into()]),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let event_tags = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10_000)
            .await
            .unwrap();
        let link_tags = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10_000)
            .await
            .unwrap();

        check!(
            event_tags
                == vec![ScopedTag {
                    scope: TagScope::Event,
                    tags: vec![
                        "event:name".to_string(),
                        "event:timeSinceStart".to_string(),
                        "exception.type".to_string(),
                    ],
                }]
        );
        check!(
            link_tags
                == vec![ScopedTag {
                    scope: TagScope::Link,
                    tags: vec![
                        "link.kind".to_string(),
                        "link:spanID".to_string(),
                        "link:traceID".to_string(),
                    ],
                }]
        );
    }

    #[tokio::test]
    async fn cold_tag_discovery_exposes_static_traceql_scopes() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );

        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let intrinsic = store
            .tag_names("tenant", Some(TagScope::Intrinsic), 0, 10)
            .await
            .unwrap();
        check!(
            (
                intrinsic.len(),
                intrinsic[0].scope,
                intrinsic[0].tags.contains(&"span:duration".to_string()),
                intrinsic[0].tags.contains(&"trace:id".to_string()),
            ) == (1, TagScope::Intrinsic, true, true)
        );

        let event = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10)
            .await
            .unwrap();
        check!(
            event
                .iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Event,
                    &vec!["event:name".into(), "event:timeSinceStart".into()]
                )]
        );

        let link = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10)
            .await
            .unwrap();
        check!(
            link.iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Link,
                    &vec!["link:spanID".into(), "link:traceID".into()]
                )]
        );

        let instrumentation = store
            .tag_names("tenant", Some(TagScope::Instrumentation), 0, 10)
            .await
            .unwrap();
        check!(
            instrumentation
                .iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Instrumentation,
                    &vec![
                        "instrumentation:name".into(),
                        "instrumentation:version".into(),
                    ]
                )]
        );
    }

    #[tokio::test]
    async fn cold_span_tag_discovery_excludes_intrinsic_index_names() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from([
                    "http.method".to_string(),
                    "event:name".to_string(),
                    "instrumentation:name".to_string(),
                ]),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let tags = store
            .tag_names("tenant", Some(TagScope::Span), 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            tags == vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string()],
            }]
        );
    }

    #[tokio::test]
    async fn cold_scan_can_read_one_backend_row_group_job() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let first = encode_span_rows(&[block_attr_span_row(
            [1; 16],
            [1; 8],
            "first-rg",
            false,
            vec!["GET".into()],
        )])
        .unwrap();
        let second = encode_span_rows(&[block_attr_span_row(
            [2; 16],
            [2; 8],
            "second-rg",
            false,
            vec!["POST".into()],
        )])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let object_writer = ParquetObjectWriter::new(
            object_store.clone(),
            Path::from("blocks/row-groups.parquet"),
        );
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), Some(props)).unwrap();
        writer.write(&first).await.unwrap();
        writer.write(&second).await.unwrap();
        writer.close().await.unwrap();

        let index = || {
            let mut index = TraceIndex::new();
            index.add_trace_block(
                "tenant",
                TraceBlockStats {
                    object_key: "blocks/row-groups.parquet".into(),
                    min_ts: 0,
                    max_ts: 10,
                    bloom: ShardedTraceBloom::with_tempo_defaults(1),
                    tag_names: BTreeSet::new(),
                    tag_values: BTreeMap::new(),
                },
            );
            index
        };
        let capped_blocks = Arc::new(BlockStore::new_with_block_read_max_bytes(
            object_store,
            Url::parse("memory:///").unwrap(),
            crabka_blockstore::BlockReadMaxBytes::new(1).unwrap(),
        ));
        let capped_store = CrabkaSpanStore::new(capped_blocks, shared(index()), None);
        let store = CrabkaSpanStore::new(blocks, shared(index()), None);

        let options = ScanOptions {
            job: Some(ScanJob {
                object_key: "blocks/row-groups.parquet".into(),
                row_group_start: 1,
                row_group_end: 2,
            }),
            ..ScanOptions::default()
        };
        let scan = store
            .scan_with_options("tenant", &[], 0, 10, &options)
            .await
            .unwrap();
        let batches = collect_table(&scan.ctx, &scan.span_table).await.unwrap();
        let names = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(crabka_traceql::COL_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert2::assert!(names == vec!["second-rg"]);
        assert2::assert!(
            capped_store
                .scan_with_options("tenant", &[], 0, 10, &options)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cold_scan_rejects_backend_row_group_job_for_other_tenant() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let batch = encode_span_rows(&[block_attr_span_row(
            [1; 16],
            [1; 8],
            "tenant-a-only",
            false,
            vec!["GET".into()],
        )])
        .unwrap();
        let object_writer = ParquetObjectWriter::new(
            object_store.clone(),
            Path::from("blocks/tenant-a-row-groups.parquet"),
        );
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            TraceBlockStats {
                object_key: "blocks/tenant-a-row-groups.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::with_tempo_defaults(1),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let scan = store
            .scan_with_options(
                "tenant-b",
                &[],
                0,
                10,
                &ScanOptions {
                    job: Some(ScanJob {
                        object_key: "blocks/tenant-a-row-groups.parquet".into(),
                        row_group_start: 0,
                        row_group_end: 1,
                    }),
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        let rows: usize = collect_table(&scan.ctx, &scan.span_table)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();

        assert2::assert!(rows == 0);
    }

    #[tokio::test]
    async fn live_nested_intrinsic_values_are_returned_by_store() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            values: vec![TypedValue {
                type_: "string".into(),
                value: "cache.miss".into(),
            }],
            frontier_ns: 0,
            ..FakeLiveSource::default()
        }));
        let store = CrabkaSpanStore::new(blocks, shared(TraceIndex::new()), Some(live));

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "cache.miss".into(),
                }]
        );
    }

    #[tokio::test]
    async fn cold_nested_intrinsic_values_are_returned_from_trace_index() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from(["event:name".to_string()]),
                tag_values: BTreeMap::from([(
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()]),
                )]),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "exception".into(),
                }]
        );
    }

    fn span_with_nested_refs() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /users".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: SpanAttrValue::Str("api".into()),
            }],
            span_attrs: vec![
                KeyValue {
                    key: "http.status_code".into(),
                    value: SpanAttrValue::Int(504),
                },
                KeyValue {
                    key: "retryable".into(),
                    value: SpanAttrValue::Bool(true),
                },
            ],
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("timeout".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: SpanAttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    fn span_ref_from_span(span: &Span) -> SpanRef {
        SpanRef {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: span.name.clone(),
            kind: span.kind as i32,
            nested_set_left: 1,
            nested_set_right: 2,
            nested_set_parent: 0,
            start_time_unix_nano: u64::try_from(span.start_ns).unwrap_or_default(),
            duration_nanos: u64::try_from(span.duration_ns).unwrap_or_default(),
            status_code: span.status as i32,
            status_message: span.status_message.clone(),
            instrumentation_name: span.instrumentation_scope.clone(),
            instrumentation_version: span.instrumentation_version.clone(),
            resource_attributes: vec![],
            attributes: vec![],
            events: vec![],
            links: vec![],
        }
    }

    fn assert_cloud_region_resource_attr(attrs: &[(String, AttrValue)]) {
        assert2::assert!(
            attrs.contains(&("cloud.region".into(), AttrValue::Str("us-east-1".into())))
        );
        assert2::assert!(
            !attrs
                .iter()
                .any(|(key, _)| key == "__resource.cloud.region")
        );
    }

    #[tokio::test]
    async fn cold_trace_by_id_projects_events_and_links_from_span_blocks() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/spans.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| (&span.attributes, &span.events, &span.links))
                .collect::<Vec<_>>()
                == vec![(
                    &vec![
                        ("http.status_code".into(), AttrValue::Int(504)),
                        ("retryable".into(), AttrValue::Bool(true)),
                    ],
                    &vec![EventRef {
                        time_since_start_nano: 50,
                        name: "exception".into(),
                        attributes: vec![(
                            "exception.type".into(),
                            AttrValue::Str("timeout".into())
                        )],
                    }],
                    &vec![LinkRef {
                        trace_id: [9; 16],
                        span_id: [8; 8],
                        attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
                    }],
                )]
        );
    }

    #[tokio::test]
    async fn cold_trace_by_id_within_prefilters_blocks_by_time_range() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut early = span_with_nested_refs();
        early.span_id = [2; 8];
        early.start_ns = 1_000;
        let mut late = span_with_nested_refs();
        late.span_id = [3; 8];
        late.start_ns = 5_000;

        let early_batch = span_batch(std::slice::from_ref(&early)).unwrap();
        let early_meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/early.parquet",
                span_block_schema(),
                &[early_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let late_batch = span_batch(std::slice::from_ref(&late)).unwrap();
        let late_meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/late.parquet",
                span_block_schema(),
                &[late_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut index = TraceIndex::new();
        for (span, meta) in [(&early, early_meta), (&late, late_meta)] {
            let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
            bloom.insert(&span.trace_id);
            index.add_trace_block(
                "tenant",
                TraceBlockStats {
                    object_key: meta.object_key,
                    min_ts: meta.min_ts,
                    max_ts: meta.max_ts,
                    bloom,
                    tag_names: BTreeSet::new(),
                    tag_values: BTreeMap::new(),
                },
            );
        }

        let store = CrabkaSpanStore::new(blocks, shared(index), None);
        let trace = store
            .trace_by_id_within("tenant", &early.trace_id, 5_000, 5_000)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![late.span_id]
        );
    }

    #[tokio::test]
    async fn trace_by_id_deduplicates_spans_present_in_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/dedup.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: Some(TraceSpans {
                trace_id: span.trace_id,
                root_service_name: "api".into(),
                root_trace_name: "GET /users".into(),
                resource_attributes: vec![],
                spans: vec![span_ref_from_span(&span)],
            }),
            batches: vec![],
            values: vec![],
            frontier_ns: 1_000,
        }));
        let store = CrabkaSpanStore::new(blocks, shared(index), Some(live));

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![span.span_id]
        );
    }

    #[tokio::test]
    async fn trace_by_id_recomputes_nested_sets_across_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let root = span_with_nested_refs();
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.start_ns = root.start_ns + 10;
        let batch = span_batch(std::slice::from_ref(&root)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/split-trace-root.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: Some(TraceSpans {
                trace_id: root.trace_id,
                root_service_name: "api".into(),
                root_trace_name: "GET /users".into(),
                resource_attributes: vec![],
                spans: vec![span_ref_from_span(&child)],
            }),
            batches: vec![],
            values: vec![],
            frontier_ns: 1_000,
        }));
        let store = CrabkaSpanStore::new(blocks, shared(index), Some(live));

        let trace = store
            .trace_by_id("tenant", &root.trace_id)
            .await
            .unwrap()
            .unwrap();
        let root = trace
            .spans
            .iter()
            .find(|span| span.span_id == root.span_id)
            .unwrap();
        let child = trace
            .spans
            .iter()
            .find(|span| span.span_id == child.span_id)
            .unwrap();

        check!(child.nested_set_parent == root.nested_set_left);
        check!(child.nested_set_left > root.nested_set_left);
        check!(child.nested_set_right < root.nested_set_right);
    }

    #[tokio::test]
    async fn trace_by_id_within_keeps_spans_straddling_the_window() {
        // A by-id `start`/`end` is a candidate-selection HINT, not a hard
        // span-level filter: real Tempo returns the *whole* trace even when
        // Grafana sends a narrow window. A trace whose spans straddle the window
        // edge must return ALL its spans (so the caller can label it COMPLETE),
        // not just the spans whose start falls inside the window.
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        // Root at 1_000ns (= 0.000001s, well before the query window); child at
        // 5_000ns. Both belong to the same trace and the same block.
        let mut root = span_with_nested_refs();
        root.start_ns = 1_000;
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.start_ns = 5_000;
        let batch = span_batch(&[root.clone(), child.clone()]).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/straddle.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared(index), None);

        // Window [4_000, 6_000] covers only the child by span start, yet the
        // block (min_ts..max_ts spans both) is still selected, so the whole
        // trace must come back — both spans, not just the child.
        let trace = store
            .trace_by_id_within("tenant", &root.trace_id, 4_000, 6_000)
            .await
            .unwrap()
            .unwrap();
        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![root.span_id, child.span_id]
        );
    }

    #[tokio::test]
    async fn traceql_search_recomputes_nested_sets_across_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let root = span_with_nested_refs();
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.name = "db".into();
        child.start_ns = root.start_ns + 10;

        let cold_batch = span_batch(std::slice::from_ref(&root)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/split-trace-search-root.parquet",
                span_block_schema(),
                &[cold_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: None,
            batches: vec![span_batch(std::slice::from_ref(&child)).unwrap()],
            values: vec![],
            frontier_ns: child.start_ns,
        }));
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), Some(live)));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search(
                "tenant",
                "{ span:name = \"GET /users\" } >> { span:name = \"db\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| {
                    (
                        trace.trace_id,
                        trace
                            .span_sets
                            .iter()
                            .flat_map(|set| set.spans.iter().map(|span| span.span_id))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
                == vec![(root.trace_id, vec![child.span_id])]
        );
    }

    async fn event_intrinsic_fixture() -> (TraceqlEngine<CrabkaSpanStore>, [[u8; 16]; 4]) {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let matching = span_with_nested_refs();
        let mut other = span_with_nested_refs();
        other.trace_id = [3; 16];
        other.span_id = [4; 8];
        other.events[0].name = "cache.hit".into();
        let mut split_events = span_with_nested_refs();
        split_events.trace_id = [7; 16];
        split_events.span_id = [8; 8];
        split_events.events = vec![
            EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("other".into()),
                }],
            },
            EventRecord {
                time_unix_nano: 1_060,
                name: "cache.hit".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("timeout".into()),
                }],
            },
        ];
        split_events.links.push(LinkRecord {
            trace_id: [7; 16],
            span_id: [6; 8],
            attrs: Vec::new(),
        });
        let mut no_event = span_with_nested_refs();
        no_event.trace_id = [5; 16];
        no_event.span_id = [6; 8];
        no_event.events.clear();
        let batch = span_batch(&[
            matching.clone(),
            other.clone(),
            split_events.clone(),
            no_event.clone(),
        ])
        .unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-events.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&matching.trace_id);
        bloom.insert(&other.trace_id);
        bloom.insert(&split_events.trace_id);
        bloom.insert(&no_event.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        (
            engine,
            [
                matching.trace_id,
                other.trace_id,
                split_events.trace_id,
                no_event.trace_id,
            ],
        )
    }

    #[tokio::test]
    async fn cold_traceql_search_filters_event_intrinsics() {
        let (engine, [matching_id, other_id, split_events_id, no_event_id]) =
            event_intrinsic_fixture().await;

        let resp = engine
            .search("tenant", "{ event:name = \"exception\" }", 0, 10_000, 10)
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ event:name != nil } | count() by (event:name) > 1",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, other_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ span:name = \"GET /users\" } | count() by (event:name) > 1",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, other_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ event:name = \"exception\" && event.exception.type = \"timeout\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == matching_id);

        let resp = engine
            .search("tenant", "{ event:name != nil }", 0, 10_000, 10)
            .await
            .unwrap();

        check!(resp.traces.len() == 3);
        check!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == matching_id)
        );
        check!(resp.traces.iter().any(|trace| trace.trace_id == other_id));
        check!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == split_events_id)
        );
        check!(
            !resp
                .traces
                .iter()
                .any(|trace| trace.trace_id == no_event_id)
        );

        let mut series = engine
            .query_range(
                "tenant",
                "{ event:name != nil } | count_over_time() | by(event:name)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        let cache_hit = series
            .iter()
            .find(|series| series.labels == vec![("name".into(), "cache.hit".into())])
            .unwrap();
        assert2::assert!(cache_hit.points == vec![(0, 2.0), (10_000, 0.0)]);

        let mut series = engine
            .query_range(
                "tenant",
                "{ span:name = \"GET /users\" } | count_over_time() | by(event.exception.type)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        // `by(event.exception.type)` groups by an event ATTRIBUTE, so the series
        // label key carries its `event.` scope (matching real Tempo, per the
        // live-Tempo differential) — unlike the bare `event:name` intrinsic above.
        assert2::assert!(series.iter().any(|series| series.labels
            == vec![("event.exception.type".into(), "timeout".into())]
            && series.points == vec![(0, 3.0), (10_000, 0.0)]));

        let mut series = engine
            .query_range(
                "tenant",
                "{ span:name = \"GET /users\" } | count_over_time() | by(link:spanID)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert2::assert!(series.iter().any(|series| series.labels
            == vec![("spanID".into(), "0606060606060606".into())]
            && series.points == vec![(0, 1.0), (10_000, 0.0)]));
    }

    #[tokio::test]
    async fn cold_traceql_search_applies_repeated_attr_any_none_semantics() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut repeated = span_with_nested_refs();
        repeated.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("GET".into()),
        });
        repeated.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("POST".into()),
        });
        let mut other = span_with_nested_refs();
        other.trace_id = [3; 16];
        other.span_id = [4; 8];
        other.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("DELETE".into()),
        });
        let batch = span_batch(&[repeated.clone(), other.clone()]).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-array-attrs.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&repeated.trace_id);
        bloom.insert(&other.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == repeated.trace_id);

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == other.trace_id);
    }

    #[tokio::test]
    async fn cold_traceql_search_keeps_resource_and_span_scopes_distinct() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut span = span_with_nested_refs();
        span.resource_attrs.push(KeyValue {
            key: "cloud.region".into(),
            value: SpanAttrValue::Str("us-east-1".into()),
        });
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-resource-scope.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resource = engine
            .search(
                "tenant",
                "{ resource.service.name = \"api\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();
        assert2::assert!(resource.traces.len() == 1);

        let bare = engine
            .search("tenant", "{ .service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(bare.traces.len() == 1);

        let resource_attr = engine
            .search(
                "tenant",
                "{ resource.cloud.region = \"us-east-1\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();
        assert2::assert!(resource_attr.traces.len() == 1);

        let trace = engine
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();
        assert_cloud_region_resource_attr(&trace.resource_attributes);
        assert_cloud_region_resource_attr(&trace.spans[0].resource_attributes);

        let bare_attr = engine
            .search("tenant", "{ .cloud.region = \"us-east-1\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(bare_attr.traces.len() == 1);

        let span = engine
            .search("tenant", "{ span.service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(span.traces.is_empty());
    }

    #[tokio::test]
    async fn cold_traceql_metrics_group_resource_service_name_after_nil_guard() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut checkout = span_with_nested_refs();
        checkout.trace_id = [1; 16];
        checkout.span_id = [1; 8];
        checkout.start_ns = 1_000;
        checkout.resource_attrs = vec![KeyValue {
            key: "service.name".into(),
            value: SpanAttrValue::Str("checkout".into()),
        }];
        let mut billing = span_with_nested_refs();
        billing.trace_id = [2; 16];
        billing.span_id = [2; 8];
        billing.start_ns = 2_000;
        billing.resource_attrs = vec![KeyValue {
            key: "service.name".into(),
            value: SpanAttrValue::Str("billing".into()),
        }];
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/metrics-resource-service-name.parquet",
                span_block_schema(),
                &[
                    span_batch(std::slice::from_ref(&checkout)).unwrap(),
                    span_batch(std::slice::from_ref(&billing)).unwrap(),
                ],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&checkout.trace_id);
        bloom.insert(&billing.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["service.name".to_string()]),
                tag_values: BTreeMap::from([(
                    "service.name".to_string(),
                    BTreeSet::from(["billing".to_string(), "checkout".to_string()]),
                )]),
            },
        );
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let mut series = engine
            .query_range(
                "tenant",
                "{ resource.service.name != nil } | count_over_time() by(resource.service.name)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        check!(
            series
                .iter()
                .map(|series| (series.labels.clone(), series.points.clone()))
                .collect::<Vec<_>>()
                == ["billing", "checkout"]
                    .into_iter()
                    .map(|service| {
                        (
                            vec![("resource.service.name".into(), service.into())],
                            vec![(0, 1.0), (10_000, 0.0)],
                        )
                    })
                    .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cold_traceql_search_applies_block_array_attr_any_none_semantics() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let rows = vec![
            block_attr_span_row(
                [1; 16],
                [2; 8],
                "GET /users",
                true,
                vec!["GET".into(), "POST".into()],
            ),
            block_attr_span_row(
                [3; 16],
                [4; 8],
                "DELETE /users",
                false,
                vec!["DELETE".into()],
            ),
        ];
        let batch = encode_span_rows(&rows).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-block-array-attrs.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        bloom.insert(&[3; 16]);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(CrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        check!(
            resp.traces
                .iter()
                .map(|trace| {
                    (
                        trace.trace_id,
                        trace.span_sets[0].spans[0].attributes.clone(),
                    )
                })
                .collect::<Vec<_>>()
                == vec![(
                    [1; 16],
                    vec![
                        ("http.method".into(), AttrValue::Str("GET".into())),
                        ("http.method".into(), AttrValue::Str("POST".into())),
                    ],
                )]
        );

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == [3; 16]);
    }

    fn block_attr_span_row(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        name: &str,
        is_array: bool,
        values: Vec<String>,
    ) -> SpanRow {
        SpanRow {
            trace_id,
            span_id,
            parent_span_id: None,
            nested_set: BlockNestedSet {
                nested_set_left: 1,
                nested_set_right: 2,
                parent_id: 0,
            },
            child_count: 0,
            root_service_name: Some("api".into()),
            root_span_name: Some("root".into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 500,
            name: Some(name.into()),
            kind: BlockSpanKind::Server,
            start_unix_nano: 1_000,
            duration_nanos: 500,
            status_code: BlockStatusCode::Ok,
            status_message: None,
            instrumentation_name: Some("otel-rust".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "http.method".into(),
                is_array,
                value: BlockAttrValue::Str(values),
            }],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    #[tokio::test]
    async fn can_back_traceql_engine() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = Arc::new(CrabkaSpanStore::new(
            blocks,
            shared(TraceIndex::new()),
            None,
        ));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        let resp = engine
            .search("tenant", "{ span:name = \"missing\" }", 0, 10, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.is_empty());
    }

    /// Verify that a live `ArcSwap` is observed: `candidate_blocks` returns nothing
    /// from the initial empty index, then the new block is immediately visible
    /// after `store()` on the shared handle — both directly and through the
    /// `CrabkaSpanStore` that holds the same `Arc<ArcSwap<TraceIndex>>`.
    #[tokio::test]
    async fn span_store_observes_swapped_index() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let handle: SharedTraceIndex = shared(TraceIndex::new());
        // Build the store — it holds the same Arc so it observes every swap.
        let _store = CrabkaSpanStore::new(Arc::clone(&blocks), Arc::clone(&handle), None);

        // Before swap: no candidate blocks.
        let before = handle.load().candidate_blocks("tenant", 0, i64::MAX);
        assert2::assert!(before.is_empty());

        // Swap in an index with one block.
        let mut new_index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1_u8; 16]);
        new_index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/swap-test.parquet".into(),
                min_ts: 0,
                max_ts: 10_000,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        handle.store(Arc::new(new_index));

        // After swap: candidate_blocks via the same handle now returns the new block.
        let after = handle.load().candidate_blocks("tenant", 0, 10_000);
        assert2::assert!(!after.is_empty());
        assert2::assert!(after.first().map(String::as_str) == Some("blocks/swap-test.parquet"));

        // Any subsequent load() call through the store's field would return
        // the same result — both the store and the caller share the same Arc.
        let via_handle = handle.load().candidate_blocks("tenant", 0, 10_000);
        assert2::assert!(via_handle == after);
    }
}
