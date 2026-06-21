//! File-backed `TraceQL` conformance testkit.

use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::{AttrValue, EngineOpts, InMemorySpanStore, InputSpan, SearchResponse, TraceqlEngine};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseResult {
    pub name: String,
    pub passed: bool,
    pub passed_assertions: usize,
    pub total_assertions: usize,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub cases: Vec<CaseResult>,
}

impl Report {
    pub fn write_to(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_text())
    }

    #[must_use]
    pub fn to_text(&self) -> String {
        let passed = self.cases.iter().filter(|case| case.passed).count();
        let total = self.cases.len();
        let mut out = format!("TraceQL conformance: {passed}/{total} cases passed\n");
        for case in &self.cases {
            let status = if case.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(
                out,
                "{status} {} ({}/{}) {}",
                case.name, case.passed_assertions, case.total_assertions, case.message
            );
        }
        out
    }
}

#[must_use]
pub fn run_corpus_dir(dir: impl AsRef<Path>) -> Report {
    let dir = dir.as_ref();
    let mut files = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "case"))
            .collect::<Vec<_>>(),
        Err(err) => {
            return Report {
                cases: vec![CaseResult {
                    name: dir.display().to_string(),
                    passed: false,
                    passed_assertions: 0,
                    total_assertions: 1,
                    message: format!("failed to read corpus dir: {err}"),
                }],
            };
        }
    };
    files.sort();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("traceql conformance runtime");
    let engine = engine();
    let mut cases = Vec::new();
    for file in files {
        let rel = file_name(&file);
        match fs::read_to_string(&file) {
            Ok(contents) => {
                cases.extend(
                    parse_cases(&rel, &contents)
                        .into_iter()
                        .map(|case| rt.block_on(async { run_case(&engine, case).await })),
                );
            }
            Err(err) => cases.push(CaseResult {
                name: rel,
                passed: false,
                passed_assertions: 0,
                total_assertions: 1,
                message: format!("failed to read case file: {err}"),
            }),
        }
    }

    Report { cases }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

async fn run_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    match case.kind.as_str() {
        "search" => run_search_case(engine, case).await,
        "metrics" => run_metrics_case(engine, case).await,
        "trace_by_id" => run_trace_by_id_case(engine, case).await,
        other => CaseResult {
            name: case.name,
            passed: false,
            passed_assertions: 0,
            total_assertions: 1,
            message: format!("unknown case kind `{other}`"),
        },
    }
}

async fn run_search_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 2,
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.search("t", &query, 0, 10_000, 20).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };

    let expected_trace_ids = parse_u8_list(case.expect_trace_ids.as_deref());
    let actual_trace_ids = trace_ids(&response);
    if actual_trace_ids == expected_trace_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "trace ids expected {expected_trace_ids:?}, got {actual_trace_ids:?}; "
        );
    }

    let expected_span_ids = parse_u8_list(case.expect_span_ids.as_deref());
    let actual_span_ids = span_ids(&response);
    if actual_span_ids == expected_span_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "span ids expected {expected_span_ids:?}, got {actual_span_ids:?}; "
        );
    }

    result.passed = result.passed_assertions == result.total_assertions;
    result
}

async fn run_metrics_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1,
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.query_range("t", &query, 0, 10_000, 10_000).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_series_count.unwrap_or(0);
    let actual = response.series.len();
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("series count expected {expected}, got {actual}");
    }
    result
}

async fn run_trace_by_id_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1,
        message: String::new(),
    };
    let Some(trace_id) = case.trace_id else {
        result.message = "missing trace_id".into();
        return result;
    };
    let response = match engine.trace_by_id("t", &[trace_id; 16]).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_span_count.unwrap_or(0);
    let actual = response.map_or(0, |trace| trace.spans.len());
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("span count expected {expected}, got {actual}");
    }
    result
}

#[derive(Default)]
struct Case {
    name: String,
    kind: String,
    query: Option<String>,
    trace_id: Option<u8>,
    expect_trace_ids: Option<String>,
    expect_span_ids: Option<String>,
    expect_series_count: Option<usize>,
    expect_span_count: Option<usize>,
}

fn parse_cases(file: &str, contents: &str) -> Vec<Case> {
    contents
        .split("\n---")
        .enumerate()
        .filter_map(|(idx, block)| {
            let mut case = Case {
                name: format!("{file}#{}", idx + 1),
                kind: "search".into(),
                ..Case::default()
            };
            for line in block.lines().map(str::trim) {
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let Some((key, value)) = line.split_once(':') else {
                    continue;
                };
                let value = value.trim();
                match key.trim() {
                    "name" => case.name = format!("{file}:{value}"),
                    "kind" => case.kind = value.to_string(),
                    "query" => case.query = Some(value.to_string()),
                    "trace_id" => case.trace_id = value.parse().ok(),
                    "expect_trace_ids" => case.expect_trace_ids = Some(value.to_string()),
                    "expect_span_ids" => case.expect_span_ids = Some(value.to_string()),
                    "expect_series_count" => case.expect_series_count = value.parse().ok(),
                    "expect_span_count" => case.expect_span_count = value.parse().ok(),
                    _ => {}
                }
            }
            (!block.trim().is_empty()).then_some(case)
        })
        .collect()
}

fn parse_u8_list(value: Option<&str>) -> Vec<u8> {
    value
        .unwrap_or_default()
        .split(',')
        .filter_map(|item| {
            let item = item.trim();
            (!item.is_empty()).then(|| item.parse().ok()).flatten()
        })
        .collect()
}

fn trace_ids(resp: &SearchResponse) -> Vec<u8> {
    resp.traces.iter().map(|trace| trace.trace_id[0]).collect()
}

fn span_ids(resp: &SearchResponse) -> Vec<u8> {
    let mut ids = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .flat_map(|set| set.spans.iter())
        .map(|span| span.span_id[0])
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
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
