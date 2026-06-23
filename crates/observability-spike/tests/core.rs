use assert2::check;
use crabka_observability_spike::{
    LabelIndex, LogEntry, LogSelector, labels, loki_streams_response, parse_logql,
    series_fingerprint,
};
use serde_json::json;

#[test]
fn fingerprint_is_stable_for_label_order() {
    let a = labels([("app", "api"), ("env", "prod")]);
    let b = labels([("env", "prod"), ("app", "api")]);

    check!(series_fingerprint(&a) == series_fingerprint(&b));
}

#[test]
fn label_index_prunes_to_matching_series() {
    let mut index = LabelIndex::default();
    let api = labels([("app", "api"), ("env", "prod")]);
    let worker = labels([("app", "worker"), ("env", "prod")]);
    let api_fp = series_fingerprint(&api);

    index.insert_series(api.clone());
    index.insert_series(worker);

    let matched = index.match_series(&LogSelector::new(labels([("app", "api")])));

    check!(matched == [api_fp].into());
}

#[test]
fn label_index_serves_label_metadata() {
    let mut index = LabelIndex::default();
    index.insert_series(labels([("app", "api"), ("env", "prod")]));
    index.insert_series(labels([("app", "worker"), ("env", "prod")]));

    check!(index.label_names() == ["app".to_string(), "env".to_string()].into());
    check!(index.label_values("app") == ["api".to_string(), "worker".to_string()].into());
    check!(index.label_values("missing").is_empty());
}

#[test]
fn parsed_logql_supports_matchers_and_line_filters() {
    let selector = parse_logql(r#"{app="api", env!="dev"} |= "error""#).unwrap();

    check!(selector.matches(
        &LogEntry::new(10, labels([("app", "api"), ("env", "prod")]), "error: boom"),
        0,
        20,
    ));
    check!(!selector.matches(
        &LogEntry::new(10, labels([("app", "api"), ("env", "dev")]), "error: boom"),
        0,
        20,
    ));
    check!(!selector.matches(
        &LogEntry::new(10, labels([("app", "api"), ("env", "prod")]), "healthy"),
        0,
        20,
    ));
}

#[test]
fn parsed_logql_supports_regex_matchers_and_negative_line_filters() {
    let selector = parse_logql(r#"{app=~"api|worker", env!~"dev|test"} !~ "debug""#).unwrap();

    check!(selector.matches(
        &LogEntry::new(10, labels([("app", "worker"), ("env", "prod")]), "error"),
        0,
        20,
    ));
    check!(!selector.matches(
        &LogEntry::new(10, labels([("app", "scheduler"), ("env", "prod")]), "error"),
        0,
        20,
    ));
    check!(!selector.matches(
        &LogEntry::new(10, labels([("app", "worker"), ("env", "test")]), "error"),
        0,
        20,
    ));
    check!(!selector.matches(
        &LogEntry::new(
            10,
            labels([("app", "worker"), ("env", "prod")]),
            "debug noise"
        ),
        0,
        20,
    ));
}

#[test]
fn invalid_logql_reports_a_parse_error() {
    let error = parse_logql(r#"{app="api" |= "missing brace""#).unwrap_err();

    check!(error.to_string().contains("expected"));
}

#[test]
fn loki_response_groups_lines_by_stream() {
    let entries = vec![
        LogEntry::new(10, labels([("app", "api")]), "ok"),
        LogEntry::new(20, labels([("app", "api")]), "error: boom"),
        LogEntry::new(15, labels([("app", "worker")]), "error: hidden"),
    ];

    let response = loki_streams_response(
        &entries,
        &LogSelector::new(labels([("app", "api")])).contains("error"),
        0,
        30,
    );

    check!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"app": "api"},
                            "values": [["20", "error: boom"]]
                        }
                    ]
                }
            })
    );
}
