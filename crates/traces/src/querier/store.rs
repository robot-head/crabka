//! `SpanStore` implementation over cold span blocks plus the live tier.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    LargeStringArray, ListArray, StringArray, StringViewArray, StructArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockIndex, BlockStore, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_EVENTS, SCOL_LINKS, TraceIndex,
    span_block_schema,
};
use crabka_traceql::{
    ATTR_PREFIX, AttrValue, COL_CHILD_COUNT, COL_DURATION, COL_INSTRUMENTATION_NAME,
    COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, EventRef, LinkRef,
    MatchCmp, MatchScope, MatchValue, ScanResult, ScopedTag, SpanMatcher, SpanRef, SpanStore,
    TagScope, TraceSpans, TraceqlError, TypedValue, span_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::querier::live::LiveTier;
use crate::span::batch::RESOURCE_ATTR_PREFIX;

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

/// Query-side span store that merges sealed blocks with an optional live tier.
pub struct CrabkaSpanStore {
    blocks: Arc<BlockStore>,
    trace_index: Arc<TraceIndex>,
    live: Option<LiveTier>,
}

impl CrabkaSpanStore {
    #[must_use]
    pub fn new(
        blocks: Arc<BlockStore>,
        trace_index: Arc<TraceIndex>,
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
    ) -> Result<Vec<RecordBatch>, TraceqlError> {
        if end_ns < start_ns {
            return Ok(Vec::new());
        }
        let keys = self.trace_index.candidate_blocks(tenant, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        collect_table(&ctx, &table).await
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
        let (cold_end, live_start) = self.live.as_ref().map_or((end_ns, end_ns + 1), |live| {
            let frontier = live.block_builder_frontier_ns(tenant);
            (
                end_ns.min(frontier.saturating_sub(1)),
                start_ns.max(frontier),
            )
        });

        let mut batches = self.cold_batches(tenant, start_ns, cold_end).await?;
        if let Some(live) = &self.live
            && live_start <= end_ns
        {
            batches.extend(live.span_batches(tenant, live_start, end_ns).await?);
        }
        let batches = filter_batches_by_matchers(batches, matchers)?;

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
        })
    }

    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        let keys = self
            .trace_index
            .candidate_blocks_for_trace(tenant, trace_id, 0, i64::MAX);
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
                out.spans.sort_by_key(|span| span.start_time_unix_nano);
            }
        }

        Ok(spans)
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        let mut by_scope: BTreeMap<&'static str, (TagScope, BTreeSet<String>)> = BTreeMap::new();
        let has_cold_blocks = !self
            .trace_index
            .candidate_blocks(tenant, start_ns, end_ns)
            .is_empty();
        if matches!(scope, None | Some(TagScope::Span)) {
            by_scope.insert("span", (TagScope::Span, BTreeSet::new()));
            for tag in self.trace_index.tag_names(tenant, start_ns, end_ns) {
                if !is_intrinsic_tag(&tag) {
                    by_scope.get_mut("span").expect("span scope").1.insert(tag);
                }
            }
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
        let mut values: BTreeSet<(String, String)> = self
            .trace_index
            .tag_values(tenant, index_tag, start_ns, end_ns)
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

fn unscoped_attribute_tag(tag: &str) -> &str {
    tag.strip_prefix("resource.")
        .or_else(|| tag.strip_prefix("span."))
        .unwrap_or(tag)
}

impl CrabkaSpanStore {
    async fn nested_intrinsic_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let mut values: BTreeSet<(String, String)> = self
            .trace_index
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
        "service.name" => string_matches(
            &string_value(batch, COL_ROOT_SERVICE_NAME, row)?,
            matcher.op,
            &matcher.value,
        ),
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

#[allow(clippy::too_many_lines)]
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

fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Str(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
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
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
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

#[allow(clippy::float_cmp)]
fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
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

    spans.sort_by_key(|span| span.start_time_unix_nano);
    Ok((!spans.is_empty()).then_some(TraceSpans {
        trace_id: *trace_id,
        root_service_name,
        root_trace_name,
        resource_attributes,
        spans,
    }))
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
            DataType::Int64 => AttrValue::Int(int64_array_value(col.as_ref(), row)?),
            DataType::Float64 => AttrValue::Float(float64_array_value(col.as_ref(), row)?),
            DataType::Boolean => AttrValue::Bool(bool_array_value(col.as_ref(), row)?),
            _ => continue,
        };
        out.push((key.to_string(), value));
    }
    out.extend(block_attr_values(batch, row, include_resource)?);
    Ok(out)
}

fn block_attr_values(
    batch: &RecordBatch,
    row: usize,
    include_resource: bool,
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
            (include_resource || !key.starts_with(RESOURCE_ATTR_PREFIX))
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

    use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use assert2::assert;
    use crabka_blockstore::{
        AttrValue as BlockAttrValue, BlockWriter, NestedSet as BlockNestedSet, SCOL_START_NANO,
        SCOL_TRACE_ID, ShardedTraceBloom, SpanAttr, SpanKind as BlockSpanKind, SpanRow,
        StatusCode as BlockStatusCode, SummaryColumns, TraceBlockStats, encode_span_rows,
        span_block_decl,
    };
    use crabka_traceql::{
        COL_CHILD_COUNT, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, EngineOpts,
        EventRef, LinkRef, TraceqlEngine,
    };
    use object_store::memory::InMemory;
    use url::Url;

    use crate::querier::live::LiveSource;
    use crate::span::{
        AttrValue as SpanAttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode,
        batch::span_batch,
    };

    use super::*;

    #[derive(Default)]
    struct FakeLiveSource {
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
            Ok(Vec::new())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>, TraceqlError> {
            Ok(None)
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

    #[test]
    fn reconstructs_trace_from_candidate_batches() {
        let got = trace_from_batches(&[7; 16], vec![batch()])
            .unwrap()
            .unwrap();
        assert!(got.root_service_name == "api");
        assert!(got.spans.len() == 1);
        assert!(got.spans[0].attributes == vec![("svc".into(), AttrValue::Str("a".into()))]);
    }

    #[test]
    fn cold_intrinsic_values_include_child_count_and_instrumentation() {
        let batches = vec![batch()];
        assert!(
            intrinsic_values_from_batches("span:childCount", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "int" && value.value == "0")
        );
        assert!(
            intrinsic_values_from_batches("instrumentation:name", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string" && value.value == "tracer")
        );
        assert!(
            intrinsic_values_from_batches("instrumentation:version", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string")
        );
    }

    #[tokio::test]
    async fn empty_store_scans_as_empty_span_table() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = CrabkaSpanStore::new(blocks, Arc::new(TraceIndex::new()), None);
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
        assert!(rows == 0);
    }

    #[tokio::test]
    async fn tag_discovery_unions_cold_index_values() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
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
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: tags,
                tag_values: values,
            },
        );

        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);
        assert!(
            store.tag_names("tenant", None, 0, 10).await.unwrap()[0].tags == vec!["service.name"]
        );
        assert!(
            store
                .tag_values("tenant", "service.name", 0, 10)
                .await
                .unwrap()[0]
                .value
                == "api"
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

        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let intrinsic = store
            .tag_names("tenant", Some(TagScope::Intrinsic), 0, 10)
            .await
            .unwrap();
        assert!(intrinsic.len() == 1);
        assert!(intrinsic[0].scope == TagScope::Intrinsic);
        assert!(intrinsic[0].tags.contains(&"span:duration".to_string()));
        assert!(intrinsic[0].tags.contains(&"trace:id".to_string()));

        let event = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10)
            .await
            .unwrap();
        assert!(event.len() == 1);
        assert!(event[0].tags == vec!["event:name", "event:timeSinceStart"]);

        let link = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10)
            .await
            .unwrap();
        assert!(link.len() == 1);
        assert!(link[0].tags == vec!["link:spanID", "link:traceID"]);

        let instrumentation = store
            .tag_names("tenant", Some(TagScope::Instrumentation), 0, 10)
            .await
            .unwrap();
        assert!(instrumentation.len() == 1);
        assert!(instrumentation[0].tags == vec!["instrumentation:name", "instrumentation:version"]);
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
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let tags = store
            .tag_names("tenant", Some(TagScope::Span), 0, 10)
            .await
            .unwrap();

        assert!(tags.len() == 1);
        assert!(tags[0].scope == TagScope::Span);
        assert!(tags[0].tags == vec!["http.method"]);
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
        }));
        let store = CrabkaSpanStore::new(blocks, Arc::new(TraceIndex::new()), Some(live));

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert!(
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
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert!(
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

    fn assert_cloud_region_resource_attr(attrs: &[(String, AttrValue)]) {
        assert!(attrs.contains(&("cloud.region".into(), AttrValue::Str("us-east-1".into()))));
        assert!(
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
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        assert!(trace.spans.len() == 1);
        assert!(
            trace.spans[0].attributes
                == vec![
                    ("http.status_code".into(), AttrValue::Int(504)),
                    ("retryable".into(), AttrValue::Bool(true)),
                ]
        );
        assert!(
            trace.spans[0].events
                == vec![EventRef {
                    time_since_start_nano: 50,
                    name: "exception".into(),
                    attributes: vec![("exception.type".into(), AttrValue::Str("timeout".into()))],
                }]
        );
        assert!(
            trace.spans[0].links
                == vec![LinkRef {
                    trace_id: [9; 16],
                    span_id: [8; 8],
                    attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
                }]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn cold_traceql_search_filters_event_intrinsics() {
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
        let store = Arc::new(CrabkaSpanStore::new(blocks, Arc::new(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ event:name = \"exception\" }", 0, 10_000, 10)
            .await
            .unwrap();

        assert!(resp.traces.len() == 2);
        assert!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == matching.trace_id)
        );
        assert!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == split_events.trace_id)
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

        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].trace_id == matching.trace_id);

        let resp = engine
            .search("tenant", "{ event:name != nil }", 0, 10_000, 10)
            .await
            .unwrap();

        assert!(resp.traces.len() == 3);
        assert!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == matching.trace_id)
        );
        assert!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == other.trace_id)
        );
        assert!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == split_events.trace_id)
        );
        assert!(
            !resp
                .traces
                .iter()
                .any(|trace| trace.trace_id == no_event.trace_id)
        );
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
        let store = Arc::new(CrabkaSpanStore::new(blocks, Arc::new(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].trace_id == repeated.trace_id);

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].trace_id == other.trace_id);
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
        let store = Arc::new(CrabkaSpanStore::new(blocks, Arc::new(index), None));
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
        assert!(resource.traces.len() == 1);

        let bare = engine
            .search("tenant", "{ .service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(bare.traces.len() == 1);

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
        assert!(resource_attr.traces.len() == 1);

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
        assert!(bare_attr.traces.len() == 1);

        let span = engine
            .search("tenant", "{ span.service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(span.traces.is_empty());
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
        let store = Arc::new(CrabkaSpanStore::new(blocks, Arc::new(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].trace_id == [1; 16]);
        assert!(
            resp.traces[0].span_sets[0].spans[0].attributes
                == vec![
                    ("http.method".into(), AttrValue::Str("GET".into())),
                    ("http.method".into(), AttrValue::Str("POST".into())),
                ]
        );

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].trace_id == [3; 16]);
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
            Arc::new(TraceIndex::new()),
            None,
        ));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        let resp = engine
            .search("tenant", "{ span:name = \"missing\" }", 0, 10, 10)
            .await
            .unwrap();
        assert!(resp.traces.is_empty());
    }
}
