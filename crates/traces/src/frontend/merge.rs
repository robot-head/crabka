//! Merge per-job search/by-id/tag partials back into one Tempo response,
//! honoring `limit` (max traces) and `spss` (max spans per spanSet), and
//! accumulating the job-accounting `metrics{}` block.
//!
//! The search merge currency is the **typed serde edge model**
//! ([`crate::frontend::wire`]), not raw `serde_json::Value`. Reunion is keyed by
//! `traceID` so a trace split across blocks / hot+cold reassembles, with
//! span-level dedup (by `spanID`) for the late-span overlap case, including
//! cross-block `matched`-count accumulation, all over typed structs.

use std::collections::BTreeSet;

use crabka_traceql::{ScopedTag, TagScope, TypedValue};

use crate::frontend::{
    backend::{SearchPartial, TagNamesPartial, TagValuesPartial, TracePartial},
    wire::{Metrics, SearchResponseJson, SpanSetJson, TraceByIdResponseJson, TraceJson},
};

/// The v2 by-id status: a fully-returned trace is `COMPLETE`; one exceeding the
/// max trace size is `PARTIAL` (returned with an explanatory message, not an
/// error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceStatus {
    Complete,
    Partial,
}

impl TraceStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            TraceStatus::Complete => "COMPLETE",
            TraceStatus::Partial => "PARTIAL",
        }
    }
}

/// Merge search partials: reunion by `traceID`, accumulate metrics, then apply
/// `limit` (newest-first) and `spss` (per-spanSet span cap, `matched`
/// preserved). Returns the merged `SearchResponseJson` ready to serialize.
#[must_use]
pub fn merge_search(partials: Vec<SearchPartial>, limit: usize, spss: usize) -> SearchResponseJson {
    let mut merged: Vec<TraceJson> = Vec::new();
    let mut metrics = Metrics::default();

    for p in partials {
        metrics.add(&p.metrics);
        for trace in p.traces {
            merge_trace(&mut merged, trace);
        }
    }

    apply_search_limits(&mut merged, limit, spss);
    SearchResponseJson {
        traces: merged,
        metrics,
    }
}

/// Fold one trace into the merged set: append when new, else reunion its
/// spanSets into the existing same-`traceID` trace.
fn merge_trace(merged: &mut Vec<TraceJson>, trace: TraceJson) {
    let Some(existing) = merged.iter_mut().find(|t| t.trace_id == trace.trace_id) else {
        merged.push(trace);
        return;
    };
    // Earliest start wins (newest-first ordering uses startTimeUnixNano).
    if parse_nanos(&trace.start_time_unix_nano) < parse_nanos(&existing.start_time_unix_nano) {
        existing
            .start_time_unix_nano
            .clone_from(&trace.start_time_unix_nano);
    }
    existing.duration_ms = existing.duration_ms.max(trace.duration_ms);
    if existing.root_service_name.is_empty() {
        existing
            .root_service_name
            .clone_from(&trace.root_service_name);
    }
    if existing.root_trace_name.is_empty() {
        existing.root_trace_name.clone_from(&trace.root_trace_name);
    }
    merge_span_sets(&mut existing.span_sets, trace.span_sets);
}

/// Reunion spanSets across blocks: dedupe spans by `spanID` into the first
/// spanSet, accumulating each spanSet's true `matched` count (cross-shard the
/// match count is additive — ported from the legacy `merge_span_sets`).
fn merge_span_sets(existing: &mut Vec<SpanSetJson>, incoming: Vec<SpanSetJson>) {
    for span_set in incoming {
        let Some(first) = existing.first_mut() else {
            existing.push(span_set);
            continue;
        };
        // `matched` is additive across shards, but only for *distinct* matches:
        // a span already present (a late-span / overlap duplicate) must not be
        // counted twice. Subtract the already-seen *returned* spans from this
        // set's reported `matched` before folding it in.
        //
        // Crucially we fold `matched` for EVERY set rather than skipping a set
        // whose returned spans all happen to be duplicates: under per-shard spss
        // truncation a set's returned spans are only a subset of what it matched,
        // so an overlapping returned subset does NOT make the set a pure
        // duplicate — its non-returned matches (`matched - duplicates`) are still
        // new and would otherwise be lost (an undercount).
        let duplicates = span_set
            .spans
            .iter()
            .filter(|s| first.spans.iter().any(|e| e.span_id == s.span_id))
            .count();
        let new_matches = span_set
            .matched
            .saturating_sub(u32::try_from(duplicates).unwrap_or(u32::MAX));
        first.matched = first.matched.saturating_add(new_matches);
        for span in span_set.spans {
            if !first.spans.iter().any(|s| s.span_id == span.span_id) {
                first.spans.push(span);
            }
        }
    }
}

/// Apply Tempo's post-merge `limit`/`spss` truncation: order traces newest-first
/// by `startTimeUnixNano`, keep at most `limit`, then cap each kept trace's
/// spanSets' `spans` to `spss` (preserving each spanSet's `matched` count).
fn apply_search_limits(traces: &mut Vec<TraceJson>, limit: usize, spss: usize) {
    traces.sort_by(|a, b| {
        parse_nanos(&b.start_time_unix_nano).cmp(&parse_nanos(&a.start_time_unix_nano))
    });
    if limit > 0 {
        traces.truncate(limit);
    }
    if spss > 0 {
        for trace in traces.iter_mut() {
            for ss in &mut trace.span_sets {
                if ss.spans.len() > spss {
                    ss.spans.truncate(spss);
                }
            }
        }
    }
}

fn parse_nanos(s: &str) -> i128 {
    s.parse().unwrap_or(i128::MIN)
}

/// Assemble one trace from per-querier by-id partials: union `resourceSpans`,
/// dedupe spans by `spanId`, accumulate metrics, and flag `Partial` when the
/// assembled trace exceeds `max_trace_bytes` (or any partial reported `PARTIAL`).
///
/// Returns `None` when no querier returned the trace.
#[must_use]
pub fn assemble_trace(
    partials: Vec<TracePartial>,
    max_trace_bytes: u64,
) -> (Option<TraceByIdResponseJson>, Metrics, TraceStatus) {
    let mut metrics = Metrics::default();
    let mut acc: Option<TraceByIdResponseJson> = None;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut any_partial = false;

    for p in partials {
        metrics.add(&p.metrics);
        if p.trace.status.eq_ignore_ascii_case("PARTIAL") {
            any_partial = true;
        }
        if p.trace.is_empty() {
            continue;
        }
        if let Some(existing) = &mut acc {
            union_trace_bodies(existing, p.trace, &mut seen);
        } else {
            seed_seen(&p.trace, &mut seen);
            acc = Some(p.trace);
        }
    }

    let status = match &acc {
        Some(t) if any_partial || t.approx_size_bytes() > max_trace_bytes => TraceStatus::Partial,
        _ => TraceStatus::Complete,
    };
    (acc, metrics, status)
}

/// Record every spanId in `trace` so later unions can dedup against it.
fn seed_seen(trace: &TraceByIdResponseJson, seen: &mut BTreeSet<String>) {
    for rs in &trace.trace.resource_spans {
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                seen.insert(span.span_id.clone());
            }
        }
    }
}

/// Union another querier's by-id body into the accumulator, deduping spans by
/// `spanId` (appending new resourceSpans/scopeSpans only as needed).
fn union_trace_bodies(
    acc: &mut TraceByIdResponseJson,
    other: TraceByIdResponseJson,
    seen: &mut BTreeSet<String>,
) {
    for mut rs in other.trace.resource_spans {
        for ss in &mut rs.scope_spans {
            // GAP6 (documented-as-acceptable): dedup is global across resources
            // (one `seen` set), not keyed on `(resource, spanId)`. This matches
            // OTLP's invariant that a span id is unique within a trace, so the
            // same span returned by multiple queriers (each reassembles the whole
            // trace) dedups correctly. The only case it mishandles is *malformed*
            // input that reuses a span id across resources — then the second
            // occurrence is dropped. Keying on `(resource, spanId)` would require
            // serializing each resource `Value` per span (not free) to defend a
            // spec-violating input, so we keep the cheaper global dedup.
            ss.spans.retain(|span| seen.insert(span.span_id.clone()));
        }
        rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        if !rs.scope_spans.is_empty() {
            // Merge into an existing resourceSpans group with an equal resource,
            // else append a new group.
            //
            // GAP4 (documented-as-acceptable): grouping is by raw
            // `serde_json::Value` equality, so the *same logical resource* with a
            // different attribute ordering would form two sibling groups. A
            // correct canonicalization is NOT cheap here: OTLP arrays are
            // semantically ordered in general, and only the `attributes` array is
            // order-insensitive — sorting it blindly would require structural
            // OTLP knowledge this typed-`Value` mirror deliberately doesn't have.
            // In practice every querier renders a resource through the same
            // `attrs_json` code path with a deterministic key order, so the same
            // logical resource serializes identically across queriers and matches
            // exactly. Duplicated groups would only cosmetically split a resource;
            // no span is dropped or duplicated.
            if let Some(existing) = acc
                .trace
                .resource_spans
                .iter_mut()
                .find(|e| e.resource == rs.resource)
            {
                merge_scope_spans(existing, rs.scope_spans);
            } else {
                acc.trace.resource_spans.push(rs);
            }
        }
    }
}

fn merge_scope_spans(
    existing: &mut crate::frontend::wire::ResourceSpansJson,
    incoming: Vec<crate::frontend::wire::ScopeSpansJson>,
) {
    for ss in incoming {
        if let Some(group) = existing
            .scope_spans
            .iter_mut()
            .find(|e| e.scope == ss.scope)
        {
            group.spans.extend(ss.spans);
        } else {
            existing.scope_spans.push(ss);
        }
    }
}

/// Total span count of a typed by-id body (helper for callers/tests).
#[must_use]
pub fn assembled_span_count(trace: &TraceByIdResponseJson) -> usize {
    trace.span_count()
}

/// Union scoped tag names across jobs, dedup + sort per scope; accumulate
/// metrics.
#[must_use]
pub fn merge_tag_names(partials: Vec<TagNamesPartial>) -> (Vec<ScopedTag>, Metrics) {
    let mut metrics = Metrics::default();
    // Keyed on a stable scope discriminant so the merged scopes have a
    // deterministic order without requiring `Ord` on `TagScope`.
    let mut by_scope: std::collections::BTreeMap<&'static str, (TagScope, BTreeSet<String>)> =
        std::collections::BTreeMap::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for st in partial.tags {
            let key = scope_key(st.scope);
            let entry = by_scope
                .entry(key)
                .or_insert_with(|| (st.scope, BTreeSet::new()));
            entry.1.extend(st.tags);
        }
    }

    let merged = by_scope
        .into_values()
        .map(|(scope, set)| ScopedTag {
            scope,
            tags: set.into_iter().collect(),
        })
        .collect();
    (merged, metrics)
}

/// Union typed tag values across jobs, dedup `(type, value)` pairs; accumulate
/// metrics.
#[must_use]
pub fn merge_tag_values(partials: Vec<TagValuesPartial>) -> (Vec<TypedValue>, Metrics) {
    let mut metrics = Metrics::default();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out = Vec::new();

    for partial in partials {
        metrics.add(&partial.metrics);
        for v in partial.values {
            if seen.insert((v.type_.clone(), v.value.clone())) {
                out.push(v);
            }
        }
    }
    out.sort_by(|a, b| (&a.type_, &a.value).cmp(&(&b.type_, &b.value)));
    (out, metrics)
}

/// Stable string discriminant for a `TagScope` (ordering + dedup key).
fn scope_key(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

// Re-export the metric-series merge helpers (separate module for clarity).
pub use crate::frontend::metrics_merge::{
    MetricSample, MetricSeries, limit_exemplars, merge_metric_series,
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::frontend::wire::{
        OtlpSpanJson, ResourceSpansJson, ScopeSpansJson, SpanJson, TraceEnvelopeJson,
    };

    fn span(id: &str, start: u64, dur: u64) -> SpanJson {
        SpanJson {
            span_id: id.to_string(),
            start_time_unix_nano: start.to_string(),
            duration_nanos: dur.to_string(),
            attributes: vec![],
        }
    }

    fn trace(tid: &str, svc: &str, start: u64, spans: Vec<SpanJson>) -> TraceJson {
        let matched = u32::try_from(spans.len()).unwrap();
        TraceJson {
            trace_id: tid.to_string(),
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: start.to_string(),
            duration_ms: 1,
            span_sets: vec![SpanSetJson { spans, matched }],
        }
    }

    fn partial(traces: Vec<TraceJson>, bytes: u64) -> SearchPartial {
        SearchPartial {
            traces,
            metrics: Metrics {
                total_jobs: 1,
                completed_jobs: 1,
                inspected_bytes: bytes,
                inspected_traces: 1,
                ..Metrics::default()
            },
        }
    }

    #[test]
    fn same_trace_across_blocks_reunions_spans() {
        let p0 = partial(
            vec![trace("01", "checkout", 10, vec![span("01", 10, 5)])],
            100,
        );
        let p1 = partial(
            vec![trace("01", "checkout", 8, vec![span("02", 8, 9)])],
            200,
        );
        let resp = merge_search(vec![p0, p1], 20, 10);
        assert2::assert!(
            resp == SearchResponseJson {
                traces: vec![TraceJson {
                    trace_id: "01".to_string(),
                    root_service_name: "checkout".to_string(),
                    root_trace_name: "GET /".to_string(),
                    start_time_unix_nano: "8".to_string(),
                    duration_ms: 1,
                    span_sets: vec![SpanSetJson {
                        spans: vec![span("01", 10, 5), span("02", 8, 9)],
                        matched: 2,
                    }],
                }],
                metrics: Metrics {
                    total_jobs: 2,
                    completed_jobs: 2,
                    total_blocks: 0,
                    inspected_traces: 2,
                    inspected_bytes: 300,
                    inspected_spans: 0,
                },
            }
        );
    }

    #[test]
    fn duplicate_span_across_blocks_is_deduped() {
        let p0 = partial(vec![trace("01", "s", 10, vec![span("07", 10, 5)])], 50);
        let p1 = partial(vec![trace("01", "s", 10, vec![span("07", 10, 5)])], 50);
        let resp = merge_search(vec![p0, p1], 20, 10);
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 1);
    }

    #[test]
    fn partial_overlap_does_not_double_count_matched() {
        // Shard 0: spans 01,02 (matched 2). Shard 1: spans 02(dup),03 (matched 2).
        // Merged distinct spans = 01,02,03; matched = 2 + (2 - 1 dup) = 3, not 4.
        let p0 = partial(
            vec![trace(
                "01",
                "s",
                10,
                vec![span("01", 10, 5), span("02", 11, 5)],
            )],
            50,
        );
        let p1 = partial(
            vec![trace(
                "01",
                "s",
                10,
                vec![span("02", 11, 5), span("03", 12, 5)],
            )],
            50,
        );
        let resp = merge_search(vec![p0, p1], 20, 10);
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 3);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 3);
    }

    #[test]
    fn truncated_overlap_subset_still_folds_its_matched_count() {
        // Per-shard spss truncation: shard 0 returned only spans 01,02 but
        // matched 5; shard 1 returned only span 02 (a subset that happens to
        // overlap shard 0's returned spans) but matched 3. Shard 1 is NOT a pure
        // duplicate — its returned span is truncated, so its non-returned matches
        // are still new. Merged matched = 5 + (3 - 1 returned dup) = 7, not 5.
        let mut p0 = trace("01", "s", 10, vec![span("01", 10, 5), span("02", 11, 5)]);
        p0.span_sets[0].matched = 5;
        let mut p1 = trace("01", "s", 10, vec![span("02", 11, 5)]);
        p1.span_sets[0].matched = 3;
        let resp = merge_search(vec![partial(vec![p0], 50), partial(vec![p1], 50)], 20, 10);
        assert2::assert!(resp.traces.len() == 1);
        // Distinct returned spans are still 01,02 (02 deduped).
        let total_spans: usize = resp.traces[0]
            .span_sets
            .iter()
            .map(|ss| ss.spans.len())
            .sum();
        assert2::assert!(total_spans == 2);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 7);
    }

    #[test]
    fn cross_block_matched_count_accumulates() {
        // Two shards each contribute a distinct span; the merged spanSet's
        // matched is the sum (legacy semantics).
        let p0 = partial(vec![trace("01", "s", 10, vec![span("01", 10, 5)])], 50);
        let p1 = partial(vec![trace("01", "s", 10, vec![span("02", 10, 5)])], 50);
        let resp = merge_search(vec![p0, p1], 20, 10);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 2);
    }

    #[test]
    fn limit_caps_trace_count_newest_first() {
        let p = partial(
            vec![
                trace("01", "a", 100, vec![span("01", 100, 1)]),
                trace("02", "b", 300, vec![span("02", 300, 1)]),
                trace("03", "c", 200, vec![span("03", 200, 1)]),
            ],
            10,
        );
        let resp = merge_search(vec![p], 2, 10);
        assert2::assert!(
            resp.traces
                == vec![
                    trace("02", "b", 300, vec![span("02", 300, 1)]),
                    trace("03", "c", 200, vec![span("03", 200, 1)]),
                ]
        );
    }

    #[test]
    fn spss_caps_spans_but_matched_is_true_count() {
        let spans = vec![
            span("01", 1, 1),
            span("02", 2, 1),
            span("03", 3, 1),
            span("04", 4, 1),
        ];
        let p = partial(vec![trace("01", "a", 1, spans)], 10);
        let resp = merge_search(vec![p], 20, 2);
        assert2::assert!(resp.traces[0].span_sets[0].spans.len() == 2);
        assert2::assert!(resp.traces[0].span_sets[0].matched == 4);
    }

    fn otlp_span(id: &str) -> OtlpSpanJson {
        OtlpSpanJson {
            span_id: id.to_string(),
            rest: serde_json::Map::new(),
        }
    }

    fn by_id_body(span_ids: &[&str], status: &str) -> TraceByIdResponseJson {
        TraceByIdResponseJson {
            trace: TraceEnvelopeJson {
                resource_spans: vec![ResourceSpansJson {
                    resource: serde_json::Value::Null,
                    scope_spans: vec![ScopeSpansJson {
                        scope: serde_json::Value::Null,
                        spans: span_ids.iter().map(|id| otlp_span(id)).collect(),
                    }],
                }],
            },
            status: status.to_string(),
            message: String::new(),
        }
    }

    fn by_id_partial(body: TraceByIdResponseJson, bytes: u64) -> TracePartial {
        TracePartial {
            trace: body,
            metrics: Metrics {
                completed_jobs: 1,
                inspected_bytes: bytes,
                ..Metrics::default()
            },
        }
    }

    #[test]
    fn assemble_returns_none_when_no_querier_has_it() {
        let p0 = by_id_partial(TraceByIdResponseJson::default(), 5);
        let p1 = by_id_partial(TraceByIdResponseJson::default(), 5);
        assert2::assert!(
            assemble_trace(vec![p0, p1], 1_000_000)
                == (
                    None,
                    Metrics {
                        total_jobs: 0,
                        completed_jobs: 2,
                        total_blocks: 0,
                        inspected_traces: 0,
                        inspected_bytes: 10,
                        inspected_spans: 0,
                    },
                    TraceStatus::Complete,
                )
        );
    }

    #[test]
    fn assemble_unions_spans_across_queriers_and_dedupes() {
        // querier A holds spans 1,2; querier B holds spans 2,3 (2 overlaps).
        let p0 = by_id_partial(by_id_body(&["01", "02"], "COMPLETE"), 100);
        let p1 = by_id_partial(by_id_body(&["02", "03"], "COMPLETE"), 100);
        let (trace, metrics, status) = assemble_trace(vec![p0, p1], 1_000_000);
        let trace = trace.unwrap();
        check!(assembled_span_count(&trace) == 3);
        check!(
            metrics
                == Metrics {
                    total_jobs: 0,
                    completed_jobs: 2,
                    total_blocks: 0,
                    inspected_traces: 0,
                    inspected_bytes: 200,
                    inspected_spans: 0,
                }
        );
        check!(status == TraceStatus::Complete);
    }

    #[test]
    fn assemble_flags_partial_over_byte_budget() {
        let p0 = by_id_partial(by_id_body(&["01", "02", "03"], "COMPLETE"), 100);
        let (trace, _m, status) = assemble_trace(vec![p0], 1);
        assert2::assert!(trace.is_some());
        assert2::assert!(matches!(status, TraceStatus::Partial));
    }

    #[test]
    fn assemble_propagates_querier_partial_status() {
        let p0 = by_id_partial(by_id_body(&["01"], "PARTIAL"), 100);
        let (_t, _m, status) = assemble_trace(vec![p0], 1_000_000);
        assert2::assert!(matches!(status, TraceStatus::Partial));
    }

    fn tag_metrics(bytes: u64) -> Metrics {
        Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            inspected_bytes: bytes,
            ..Metrics::default()
        }
    }

    #[test]
    fn tag_names_union_dedupes_per_scope() {
        let a = TagNamesPartial {
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string()],
            }],
            metrics: tag_metrics(10),
        };
        let b = TagNamesPartial {
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string(), "http.status_code".to_string()],
            }],
            metrics: tag_metrics(20),
        };
        assert2::assert!(
            merge_tag_names(vec![a, b])
                == (
                    vec![ScopedTag {
                        scope: TagScope::Span,
                        tags: vec!["http.method".to_string(), "http.status_code".to_string()],
                    }],
                    Metrics {
                        total_jobs: 2,
                        completed_jobs: 2,
                        total_blocks: 0,
                        inspected_traces: 0,
                        inspected_bytes: 30,
                        inspected_spans: 0,
                    },
                )
        );
    }

    #[test]
    fn tag_values_union_dedupes_pairs() {
        let a = TagValuesPartial {
            values: vec![TypedValue {
                type_: "string".to_string(),
                value: "GET".to_string(),
            }],
            metrics: tag_metrics(1),
        };
        let b = TagValuesPartial {
            values: vec![
                TypedValue {
                    type_: "string".to_string(),
                    value: "GET".to_string(),
                },
                TypedValue {
                    type_: "string".to_string(),
                    value: "POST".to_string(),
                },
            ],
            metrics: tag_metrics(1),
        };
        let (merged, _) = merge_tag_values(vec![a, b]);
        // sorted by (type, value).
        assert2::assert!(
            merged
                == vec![
                    TypedValue {
                        type_: "string".to_string(),
                        value: "GET".to_string(),
                    },
                    TypedValue {
                        type_: "string".to_string(),
                        value: "POST".to_string(),
                    },
                ]
        );
    }
}
