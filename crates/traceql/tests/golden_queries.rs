use std::sync::Arc;

use assert2::assert;
use crabka_traceql::{
    AttrValue, EngineOpts, InMemorySpanStore, InputSpan, SearchResponse, TraceqlEngine,
};

fn span(
    trace: u8,
    id: u8,
    parent: Option<u8>,
    name: &str,
    duration_nanos: i64,
    attrs: Vec<(&str, AttrValue)>,
) -> InputSpan {
    InputSpan {
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        kind: 0,
        start_unix_nano: 1_000 + i64::from(id),
        duration_nanos,
        status_code: 0,
        status_message: String::new(),
        instrumentation_name: String::new(),
        instrumentation_version: String::new(),
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn engine() -> TraceqlEngine<InMemorySpanStore> {
    let mut store = InMemorySpanStore::new();
    store.push_trace(
        "t",
        "svc-a",
        "root-a",
        vec![
            span(
                1,
                1,
                None,
                "root-a",
                100,
                vec![
                    ("svc", AttrValue::Str("a".into())),
                    ("a", AttrValue::Int(1)),
                    ("http.method", AttrValue::Str("GET".into())),
                    ("name", AttrValue::Str("post-root".into())),
                ],
            ),
            span(
                1,
                2,
                Some(1),
                "child-x",
                200,
                vec![
                    ("svc", AttrValue::Str("b".into())),
                    ("b", AttrValue::Int(2)),
                ],
            ),
            span(
                1,
                4,
                Some(2),
                "grand-y",
                80,
                vec![("svc", AttrValue::Str("c".into()))],
            ),
            span(
                1,
                3,
                Some(1),
                "child-z",
                220,
                vec![("svc", AttrValue::Str("b".into()))],
            ),
        ],
    );
    store.push_trace(
        "t",
        "svc-x",
        "root-x",
        vec![span(
            2,
            1,
            None,
            "both",
            50,
            vec![
                ("svc", AttrValue::Str("x".into())),
                ("a", AttrValue::Int(1)),
                ("b", AttrValue::Int(2)),
                ("name", AttrValue::Str("xpost".into())),
            ],
        )],
    );
    store.push_trace(
        "t",
        "svc-d",
        "root-d",
        vec![
            span(
                3,
                1,
                None,
                "root-d",
                100,
                vec![("svc", AttrValue::Str("a".into()))],
            ),
            span(
                3,
                2,
                Some(1),
                "child-d",
                100,
                vec![("svc", AttrValue::Str("d".into()))],
            ),
        ],
    );
    TraceqlEngine::new(Arc::new(store), EngineOpts::default())
}

async fn query(q: &str) -> SearchResponse {
    engine().search("t", q, 0, 10_000, 20).await.unwrap()
}

fn trace_ids(resp: &SearchResponse) -> Vec<u8> {
    resp.traces.iter().map(|t| t.trace_id[0]).collect()
}

fn span_ids(resp: &SearchResponse) -> Vec<u8> {
    let mut ids = Vec::new();
    for trace in &resp.traces {
        for set in &trace.span_sets {
            for span in &set.spans {
                ids.push(span.span_id[0]);
            }
        }
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn selector_queries_match_hand_computed_traces() {
    assert!(trace_ids(&query("{ .http.method = \"GET\" }").await) == vec![1]);
    assert!(trace_ids(&query("{ span:duration > 150 }").await) == vec![1]);
    assert!(trace_ids(&query("{ .name =~ \"po.*\" }").await) == vec![1]);
}

#[tokio::test]
async fn single_span_and_differs_from_inter_brace_and() {
    assert!(trace_ids(&query("{ .a = 1 && .b = 2 }").await) == vec![2]);
    assert!(trace_ids(&query("{ .a = 1 } && { .b = 2 }").await) == vec![1, 2]);
}

#[tokio::test]
async fn structural_operators_return_right_hand_spans() {
    assert!(span_ids(&query("{ .svc = \"a\" } >> { .svc = \"c\" }").await) == vec![4]);
    assert!(span_ids(&query("{ .svc = \"c\" } << { .svc = \"a\" }").await) == vec![1]);
    assert!(span_ids(&query("{ .svc = \"a\" } > { .svc = \"b\" }").await) == vec![2, 3]);
    assert!(span_ids(&query("{ .svc = \"c\" } < { .svc = \"b\" }").await) == vec![2]);
    assert!(span_ids(&query("{ .svc = \"b\" } ~ { .svc = \"b\" }").await) == vec![2, 3]);
}

#[tokio::test]
async fn structural_join_is_trace_isolated() {
    let resp = query("{ .svc = \"a\" } >> { .svc = \"d\" }").await;
    assert!(trace_ids(&resp) == vec![3]);
    assert!(span_ids(&resp) == vec![2]);
}

#[tokio::test]
async fn pipeline_count_filter_matches_trace_cardinality() {
    assert!(trace_ids(&query("{ .svc = \"b\" } | count() > 1").await) == vec![1]);
    assert!(trace_ids(&query("{ .svc = \"b\" } | count() > 5").await).is_empty());
}

#[tokio::test]
async fn trace_by_id_returns_known_trace() {
    let engine = engine();
    let got = engine.trace_by_id("t", &[1; 16]).await.unwrap().unwrap();
    assert!(got.spans.len() == 4);
    assert!(engine.trace_by_id("t", &[9; 16]).await.unwrap().is_none());
}

#[test]
fn golden_query_corpus_passes() {
    let corpus_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/testdata/traceql");
    let report = crabka_traceql::testkit::run_corpus_dir(corpus_dir);
    println!("{}", report.to_text());

    let failing = report
        .cases
        .iter()
        .filter(|case| !case.passed)
        .collect::<Vec<_>>();

    assert!(
        failing.is_empty(),
        "traceql golden corpus failures: {failing:?}"
    );
}
