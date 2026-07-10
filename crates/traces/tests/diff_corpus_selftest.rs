#[path = "support/diff_corpus.rs"]
mod diff_corpus;

use assert2::check;
use diff_corpus::*;
use serde_json::json;

#[test]
fn normalize_search_is_order_and_metrics_insensitive() {
    let a = json!({
        "traces": [
            {"traceID": "02", "rootServiceName": "s", "rootTraceName": "b", "spanSets": []},
            {"traceID": "01", "rootServiceName": "s", "rootTraceName": "a", "spanSets": []}
        ],
        "metrics": {"inspectedTraces": 7, "inspectedBytes": 999}
    });
    let b = json!({
        "traces": [
            {"traceID": "01", "rootServiceName": "s", "rootTraceName": "a", "spanSets": []},
            {"traceID": "02", "rootServiceName": "s", "rootTraceName": "b", "spanSets": []}
        ],
        "metrics": {"inspectedTraces": 3, "inspectedBytes": 12}
    });

    check!(normalize_search(&a) == normalize_search(&b));
}

#[test]
fn real_search_difference_is_detected() {
    let a = json!({"traces": [{"traceID": "01", "rootTraceName": "a", "spanSets": []}]});
    let b = json!({"traces": [{"traceID": "01", "rootTraceName": "DIFFERENT", "spanSets": []}]});

    check!(normalize_search(&a) != normalize_search(&b));
}

#[test]
fn corpus_is_nonempty_and_covers_key_operators() {
    let queries = search_corpus();

    check!(queries.iter().any(|case| case.traceql.contains(">>")));
    check!(queries.iter().any(|case| case.traceql.contains('~')));
    check!(queries.iter().any(|case| case.traceql.contains("count(")));
    assert2::assert!(!seed_dataset().is_empty());
    assert2::assert!(!by_id_corpus().is_empty());
}

#[test]
fn normalize_trace_sorts_nested_spans_and_attributes() {
    let a = json!({
        "status": "COMPLETE",
        "trace": {"resourceSpans": [{
            "resource": {"attributes": [
                {"key": "z", "value": {"stringValue": "last"}},
                {"key": "a", "value": {"stringValue": "first"}}
            ]},
            "scopeSpans": [{"scope": {"name": "scope"}, "spans": [
                {"spanID": "02", "attributes": [{"key": "b"}, {"key": "a"}]},
                {"spanID": "01", "attributes": []}
            ]}]
        }]}
    });
    let b = json!({
        "status": "COMPLETE",
        "trace": {"resourceSpans": [{
            "resource": {"attributes": [
                {"key": "a", "value": {"stringValue": "first"}},
                {"key": "z", "value": {"stringValue": "last"}}
            ]},
            "scopeSpans": [{"scope": {"name": "scope"}, "spans": [
                {"spanID": "01", "attributes": []},
                {"spanID": "02", "attributes": [{"key": "a"}, {"key": "b"}]}
            ]}]
        }]}
    });

    check!(normalize_trace(&a) == normalize_trace(&b));
    assert_trace_query_equal("trace-normalize", &a, &b);
}

#[test]
fn otlp_payload_uses_seed_dataset() {
    let seed = seed_dataset();
    let payload = to_otlp(&seed);

    assert2::assert!(payload.resource_spans.len() == seed.len());
    assert2::assert!(payload.resource_spans[0].scope_spans[0].spans.len() == seed[0].spans.len());
}
