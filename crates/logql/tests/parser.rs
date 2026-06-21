use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use assert2::check;
use crabka_logql::{
    ComparisonOp, FieldFilter, FieldValue, JsonExtraction, JsonParserConfig, LabelFormat,
    LabelFormatAssignment, LabelMatcher, LabelSelection, LabelSelectionSet, LineFilter,
    LineFilterOp, LineFormat, LogfmtExtraction, LogfmtParserConfig, MatchOp, MetricQuery,
    ParserStage, PatternParser, PipelineStage, Quantile, RangeAggregation, RegexpParser,
    StreamQuery, UnwrapExpression, VectorAggregation, VectorAggregationOp, VectorGrouping,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query,
};

#[test]
fn parses_selector_with_all_matcher_ops() {
    let query =
        parse_query(r#"{app="api", env!="dev", pod=~"api-[0-9]+", zone!~"test|stage"}"#).unwrap();

    check!(
        query
            == StreamQuery {
                matchers: vec![
                    LabelMatcher::new("app", MatchOp::Equal, "api").unwrap(),
                    LabelMatcher::new("env", MatchOp::NotEqual, "dev").unwrap(),
                    LabelMatcher::new("pod", MatchOp::RegexEqual, "api-[0-9]+").unwrap(),
                    LabelMatcher::new("zone", MatchOp::RegexNotEqual, "test|stage").unwrap(),
                ],
                pipeline: vec![],
            }
    );
}

#[test]
fn rejects_selectors_without_a_non_empty_compatible_matcher() {
    for query in [
        r#"{}"#,
        r#"{env=""}"#,
        r#"{env!="prod"}"#,
        r#"{env=~".*"}"#,
        r#"{env!~"prod"}"#,
        r#"{env=~".*", zone!="prod"}"#,
    ] {
        check!(
            parse_query(query).is_err(),
            "query should be rejected: {query}"
        );
    }
}

#[test]
fn accepts_selectors_with_at_least_one_non_empty_compatible_matcher() {
    for query in [
        r#"{env="prod"}"#,
        r#"{env!=""}"#,
        r#"{env=~".+"}"#,
        r#"{env!~".*"}"#,
        r#"{app="api", env=~".*"}"#,
        r#"{app=~"api|worker", env!="prod"}"#,
    ] {
        check!(
            parse_query(query).is_ok(),
            "query should be accepted: {query}"
        );
    }
}

#[test]
fn parses_multiple_line_filters_in_order() {
    let query =
        parse_query(r#"{app="api"} |= "error" != "debug" |~ "status=[45][0-9][0-9]""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Contains, "error").unwrap()
                ),
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::NotContains, "debug").unwrap()
                ),
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Regex, "status=[45][0-9][0-9]").unwrap()
                ),
            ]
    );
}

#[test]
fn parses_decolorize_stage() {
    let query = parse_query(r#"{app="api"} | decolorize |= "status=500""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Decolorize,
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Contains, "status=500").unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_decolorize_before_later_line_filters() {
    let query = parse_query(r#"{app="api"} | decolorize |= "status=500" !~ `\x1b\[`"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);
    let evaluation = query
        .evaluate_with_fields(
            &labels,
            "\u{1b}[31mlevel=error status=500\u{1b}[0m",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.line == "level=error status=500");
}

#[test]
fn query_evaluator_applies_pattern_line_filters() {
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);
    let line = r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=debug traceID=abc msg="POST /push.v1.PusherService/Push (200) 12ms""#;
    let query = parse_query(
        r#"{app="api"} |> `<_> caller=http.go:194 level=debug <_> msg="POST /push.v1.PusherService/Push <_>`"#,
    )
    .unwrap();

    check!(query.matches(&labels, line));
    check!(!query.matches(
        &labels,
        r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=info msg="POST /push.v1.PusherService/Push (200) 12ms""#
    ));

    let query = parse_query(
        r#"{app="api"} !> `<_> caller=http.go:194 level=debug <_> msg="POST /push.v1.PusherService/Push <_>`"#,
    )
    .unwrap();

    check!(!query.matches(&labels, line));
    check!(query.matches(
        &labels,
        r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=info msg="POST /push.v1.PusherService/Push (200) 12ms""#
    ));
}

#[test]
fn query_evaluator_ignores_logql_comments_outside_strings() {
    let query = parse_query(
        r#"
            {app="api"} # selector comment
            |= "error # literal"
            # disabled stage: != "error"
            | logfmt # parser comment
            | status >= 500 # filter comment
        "#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "status=500 msg=\"error # literal\""));
    check!(!query.matches(&labels, "status=500 msg=\"error\""));
    check!(!query.matches(&labels, "status=200 msg=\"error # literal\""));
}

#[test]
fn decodes_common_escapes_in_quoted_strings() {
    let query =
        parse_query(r#"{app="api\nprod"} |= "line\tone" | logfmt | msg = "hello\"there""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api\nprod".to_string())]);

    check!(query.matches(&labels, "line\tone msg=\"hello\\\"there\""));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "api\\nprod".to_string())]),
        "line\tone msg=\"hello\\\"there\""
    ));
    check!(!query.matches(&labels, "line\\tone msg=\"hello\\\"there\""));
}

#[test]
fn query_evaluator_applies_matchers_and_pipeline() {
    let query = parse_query(r#"{app="api", env!="dev"} |= "error" !~ "debug""#).unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    check!(query.matches(&labels, "error status=500"));
    check!(!query.matches(&labels, "debug error status=500"));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "error"
    ));
}

#[test]
fn query_evaluator_treats_empty_compatible_regex_matcher_as_matching_absent_label() {
    let query = parse_query(r#"{app="api", env=~".*"}"#).unwrap();

    check!(query.matches(
        &BTreeMap::from([("app".to_string(), "api".to_string())]),
        "api line"
    ));
    check!(query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]),
        "api line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "worker line"
    ));
}

#[test]
fn query_evaluator_anchors_regex_label_matchers() {
    let query = parse_query(r#"{app=~"api|worker"}"#).unwrap();

    check!(query.matches(
        &BTreeMap::from([("app".to_string(), "api".to_string())]),
        "api line"
    ));
    check!(query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "worker line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "myapi".to_string())]),
        "prefixed api line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "api-v2".to_string())]),
        "suffixed api line"
    ));
}

#[test]
fn query_evaluator_applies_field_filter_to_original_labels() {
    let query = parse_query(r#"{app="api"} | env = "prod""#).unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    check!(query.matches(&labels, "api error"));
    check!(!query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "dev".to_string()),
        ]),
        "api error"
    ));
}

#[test]
fn query_evaluator_suffixes_json_fields_that_collide_with_original_labels() {
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let query =
        parse_query(r#"{app="api"} | json | env = "prod" | env_extracted = "dev""#).unwrap();
    check!(query.matches(&labels, r#"{"env":"dev"}"#));

    let query = parse_query(r#"{app="api"} | json | env = "dev""#).unwrap();
    check!(!query.matches(&labels, r#"{"env":"dev"}"#));
}

#[test]
fn query_evaluator_suffixes_logfmt_fields_that_collide_with_original_labels() {
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let query =
        parse_query(r#"{app="api"} | logfmt | env = "prod" | env_extracted = "dev""#).unwrap();
    check!(query.matches(&labels, "env=dev"));

    let query = parse_query(r#"{app="api"} | logfmt | env = "dev""#).unwrap();
    check!(!query.matches(&labels, "env=dev"));
}

#[test]
fn parses_json_parser_stage_and_numeric_field_filter() {
    let query = parse_query(r#"{app="api"} | json | status >= 500"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Json),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_json_parser_stage_and_field_filter() {
    let query = parse_query(r#"{app="api"} | json | status >= 500"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"{"status":500,"message":"boom"}"#));
    check!(!query.matches(&labels, r#"{"status":200,"message":"ok"}"#));
    check!(!query.matches(&labels, "not json"));
}

#[test]
fn query_evaluator_exposes_json_parser_error_fields() {
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let query = parse_query(r#"{app="api"} | json | __error__ = "JSONParserErr""#).unwrap();
    check!(query.matches(&labels, "not json"));
    check!(!query.matches(&labels, r#"{"status":500}"#));

    let query = parse_query(r#"{app="api"} | json | __error__ = """#).unwrap();
    check!(!query.matches(&labels, "not json"));
    check!(query.matches(&labels, r#"{"status":500}"#));
}

#[test]
fn parses_selected_json_parser_stage() {
    let query = parse_query(
        r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/1""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::JsonSelected(
                    JsonParserConfig::new(vec![
                        JsonExtraction::new("first_server", "servers[0]").unwrap(),
                        JsonExtraction::new("ua", r#"request.headers["User-Agent"]"#).unwrap(),
                    ])
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "ua",
                    ComparisonOp::Equal,
                    FieldValue::String("Agent/1".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_selected_json_extracts_paths_and_arrays() {
    let query = parse_query(
        r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/1""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"{"servers":["10.0.0.1"],"request":{"headers":{"User-Agent":"Agent/1"},"method":"GET"},"status":500}"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("first_server") == Some(&"10.0.0.1".to_string()));
    check!(evaluation.fields.get("ua") == Some(&"Agent/1".to_string()));
    check!(!evaluation.fields.contains_key("request_method"));
    check!(!evaluation.fields.contains_key("status"));
}

#[test]
fn parses_unpack_parser_stage() {
    let query = parse_query(r#"{app="api"} | unpack | pod = "pod-3223f""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Unpack),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "pod",
                    ComparisonOp::Equal,
                    FieldValue::String("pod-3223f".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_unpack_parser_and_replaced_line_filters() {
    let query = parse_query(
        r#"{app="api"} | unpack |= "original log message" != "container" | pod = "pod-3223f""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"container original log message"}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"other log message"}"#
    ));
}

#[test]
fn parses_line_format_stage() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}`"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::LineFormat(LineFormat::new("{{.msg}} {{.status}}").unwrap()),
            ]
    );
}

#[test]
fn query_evaluator_applies_line_format_before_later_line_filters() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api error 500" != "status=""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"status=500 msg="api error""#));
    check!(!query.matches(&labels, r#"status=200 msg="api error""#));
}

#[test]
fn query_evaluator_line_format_can_reference_current_line() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{__line__}} method={{.method}}` |= "raw method=GET""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"raw method=GET"#));
    check!(!query.matches(&labels, r#"raw method=POST"#));
}

#[test]
fn query_evaluator_line_format_can_reference_current_timestamp() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ __timestamp__ | unixEpochNanos }} {{ __timestamp__ | unixEpochMillis }} {{ __timestamp__ | unixEpoch }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "1234567890 1234 1");
}

#[test]
fn query_evaluator_line_format_exposes_line_and_timestamp_aliases() {
    let query =
        parse_query(r#"{app="api"} | line_format `{{ line }} {{ timestamp | unixEpochNanos }}`"#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw line", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "raw line 1234567890");
}

#[test]
fn query_evaluator_line_format_formats_current_timestamp_with_date_helper() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ __timestamp__ | date "2006-01-02T15:04:05.00Z-07:00" }} {{ __timestamp__ | date "2006-01-02" }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "1970-01-01T00:00:01.23Z+00:00 1970-01-01");
}

#[test]
fn query_evaluator_line_format_converts_epoch_strings_with_unix_to_time_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .day | unixToTime | date "2006-01-02" }} {{ .seconds | unixToTime | date "2006-01-02T15:04:05" }} {{ .millis | unixToTime | unixEpoch }} {{ .micros | unixToTime | unixEpochMillis }} {{ .nanos | unixToTime | unixEpochNanos }} {{ .invalid | unixToTime | date "2006" }}` |= "2023-01-16 2023-03-23T13:13:35 1679577215 1679577215000 1679577215000000000 ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"day=19373 seconds=1679577215 millis=1679577215000 micros=1679577215000000 nanos=1679577215000000000 invalid=soon"#
    ));
    check!(!query.matches(
        &labels,
        r#"day=19373 seconds=1679587215 millis=1679577215000 micros=1679577215000000 nanos=1679577215000000000 invalid=soon"#
    ));
}

#[test]
fn query_evaluator_line_format_parses_dates_with_to_date_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .day | toDate "2006-01-02" | unixEpoch }} {{ .stamp | toDateInZone "2006-01-02T15:04:05.999999999Z" "UTC" | unixEpochNanos }} {{ .day | toDateInZone "2006-01-02" "America/New_York" | unixEpoch }} {{ .bad | toDateInZone "2006-01-02" "UTC" | unixEpoch }}` |= "1635811200 1635867930123456789 1635825600 ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"day=2021-11-02 stamp=2021-11-02T15:45:30.123456789Z bad=soon"#
    ));
    check!(!query.matches(
        &labels,
        r#"day=2021-11-03 stamp=2021-11-02T15:45:30.123456789Z bad=soon"#
    ));
}

#[test]
fn query_evaluator_line_format_exposes_now_template_helper() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ now | unixEpochNanos }} {{ now | unixEpochMillis }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);
    let before = current_unix_epoch_nanos();

    let evaluation = query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();

    let after = current_unix_epoch_nanos();
    let parts = evaluation.line.split_whitespace().collect::<Vec<_>>();
    check!(parts.len() == 2);
    let nanos = parts[0].parse::<u128>().unwrap();
    let millis = parts[1].parse::<u128>().unwrap();
    check!(nanos >= before);
    check!(nanos <= after);
    check!(millis >= before / 1_000_000);
    check!(millis <= after / 1_000_000);
}

#[test]
fn query_evaluator_line_format_ranges_over_from_json_arrays() {
    let query = parse_query(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $q := fromJson .queries }}{{ $q.query }}={{ $q.duration }};{{ end }}` |= "rate=30;sum=15;""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":30},{"query":"sum","duration":15}]}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":20},{"query":"sum","duration":15}]}"#
    ));
}

#[test]
fn query_evaluator_line_format_ranges_with_current_dot_over_from_json_arrays() {
    let query = parse_query(
        r#"{app="api"} | json queries="queries" | line_format `{{ range fromJson .queries }}{{ .query }}={{ .duration }};{{ end }}` |= "rate=30;sum=15;""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":30},{"query":"sum","duration":15}]}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":20},{"query":"sum","duration":15}]}"#
    ));
}

#[test]
fn query_evaluator_line_format_ranges_with_index_and_value_variables() {
    let query = parse_query(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $i, $q := fromJson .queries }}{{ $i }}:{{ $q.query }}={{ $q.duration }};{{ end }}` |= "0:rate=30;1:sum=15;""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":30},{"query":"sum","duration":15}]}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":20},{"query":"sum","duration":15}]}"#
    ));
}

#[test]
fn query_evaluator_line_format_ranges_over_from_json_objects() {
    let query = parse_query(
        r#"{app="api"} | json durations="durations" | line_format `{{ range $name, $duration := fromJson .durations }}{{ $name }}={{ $duration }};{{ end }}` |= "rate=30;sum=15;""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"{"durations":{"rate":30,"sum":15}}"#));
    check!(!query.matches(&labels, r#"{"durations":{"rate":20,"sum":15}}"#));
}

#[test]
fn query_evaluator_line_format_uses_range_else_for_empty_from_json_arrays() {
    let query = parse_query(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $q := fromJson .queries }}{{ $q.query }};{{ else }}none{{ end }}` |= "none""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"{"queries":[]}"#));
    check!(!query.matches(&labels, r#"{"queries":[{"query":"rate","duration":30}]}"#));
}

#[test]
fn query_evaluator_line_format_applies_go_template_index_and_slice_helpers() {
    let query = parse_query(
        r#"{app="api"} | json payload="payload" | line_format `{{ index (fromJson .payload) "servers" 1 "name" }}|{{ index (fromJson .payload) "status" }}|{{ slice "abcdef" 1 4 }}|{{ slice (index (fromJson .payload) "servers") 0 1 }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let result = query
        .evaluate_with_fields(
            &labels,
            r#"{"payload":{"servers":[{"name":"api"},{"name":"worker"}],"status":200}}"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(result.line == r#"worker|200|bcd|[{"name":"api"}]"#);
}

#[test]
fn query_evaluator_line_format_applies_integer_math_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ add 3 2 5 }} {{ sub 5 2 }} {{ mul 5 2 3 }} {{ div 10 2 }} {{ mod 10 3 }} {{ max 1 2 3 }} {{ min 1 2 3 }} {{ .count | int | add 2 }} {{ .bad | int }}` |= "10 3 30 5 1 3 1 10 ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"count=8 bad=soon"#));
    check!(!query.matches(&labels, r#"count=7 bad=soon"#));
}

#[test]
fn query_evaluator_line_format_applies_float_math_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ addf 3.5 2 5 }} {{ subf 5.5 2 1.5 }} {{ mulf 5.5 2 2.5 }} {{ divf 10 2 4 }} {{ maxf 1 2.5 3 }} {{ minf 1.5 2.5 3 }} {{ ceil 123.001 }} {{ floor 123.9999 }} {{ round 123.555555 3 }} {{ .ratio | float64 | addf 1.25 }} {{ .bad | float64 }}` |= "10.5 2 27.5 1.25 3 1.5 124 123 123.556 4.75 ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"ratio=3.5 bad=soon"#));
    check!(!query.matches(&labels, r#"ratio=2.5 bad=soon"#));
}

#[test]
fn query_evaluator_line_format_applies_template_string_pipelines() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .path | replace "/" "_" | upper | trunc 6 }} {{ __line__ | lower }}` |= "_CHECK status=500 path=/checkout msg=api_error""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"status=500 path=/checkout msg=API_ERROR"#));
    check!(!query.matches(&labels, r#"status=500 path=/health msg=API_ERROR"#));
}

#[test]
fn query_evaluator_line_format_applies_additional_template_string_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .raw | trim | trimPrefix "/" | trimSuffix "/" | title }} {{ .raw | trimAll " /" }} {{ .path | substr 1 10 }} {{ .path | substr 5 -1 }} {{ .path | substr -1 4 }} {{ .query | urlencode }} {{ .encoded | urldecode }}` |= "Checkout checkout api/items items /api a%3D1%20b%3Dtwo a=1 b=two""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"raw=" /checkout/ " path=/api/items query="a=1 b=two" encoded="a%3D1%20b%3Dtwo""#
    ));
    check!(!query.matches(
        &labels,
        r#"raw=" /health/ " path=/api/items query="a=1 b=two" encoded="a%3D1%20b%3Dtwo""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_base64_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .raw | b64enc }} {{ .encoded | b64dec }} {{ .invalid | b64dec }}` |= "aGVsbG8= hello ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"raw=hello encoded="aGVsbG8=" invalid="not-base64!""#
    ));
    check!(!query.matches(
        &labels,
        r#"raw=hello encoded="d29ybGQ=" invalid="not-base64!""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_measurement_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .latency | duration }} {{ .latency | duration_seconds }} {{ .size | bytes }} {{ .invalid | bytes }}` |= "90 90 1572864 ""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"latency=1m30s size=1.5MiB invalid=soon"#));
    check!(!query.matches(&labels, r#"latency=250ms size=1.5MiB invalid=soon"#));
}

#[test]
fn query_evaluator_line_format_applies_printf_template_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ printf "The IP address was %s" .remote_addr }}|{{ printf "%-5.5s" .request_method }}|{{ printf "%15.15s" .client_host }}|{{ .route | printf "[%s]" }}` |= "The IP address was 192.168.1.1|GET  |long-example.in|[/checkout]""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"remote_addr=192.168.1.1 request_method=GET client_host=long-example.internal route=/checkout"#
    ));
    check!(!query.matches(
        &labels,
        r#"remote_addr=192.168.1.1 request_method=POST client_host=long-example.internal route=/checkout"#
    ));
}

#[test]
fn query_evaluator_line_format_applies_go_template_print_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ print "status=" 500 " method=" .method }}|{{ println "status" 500 }}|{{ urlquery "a=1 b=two&x=/api" }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let result = query
        .evaluate_with_fields(&labels, r#"method=GET"#, &BTreeMap::new())
        .unwrap();

    check!(result.line == "status=500 method=GET|status 500\n|a%3D1+b%3Dtwo%26x%3D%2Fapi");
}

#[test]
fn query_evaluator_line_format_applies_go_template_escape_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ html .html }}|{{ js .script }}|{{ "<x>" | html }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let result = query
        .evaluate_with_fields(
            &labels,
            r#"html="<a&b>\"'" script="line\n\"quote\" <tag> &=""#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(
        result.line
            == r#"&lt;a&amp;b&gt;&#34;&#39;|line\u000A\"quote\" \u003Ctag\u003E \u0026\u003D|&lt;x&gt;"#
    );
}

#[test]
fn query_evaluator_line_format_applies_logical_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ contains "timeout" .msg }} {{ .path | hasPrefix "/api" }} {{ .path | hasSuffix "items" }} {{ .method | eq "GET" }}` |= "true true true true""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"method=GET path=/api/items msg="request timeout""#
    ));
    check!(!query.matches(
        &labels,
        r#"method=POST path=/api/items msg="request timeout""#
    ));
    check!(!query.matches(&labels, r#"method=GET path=/health msg="request timeout""#));
}

#[test]
fn query_evaluator_line_format_applies_ne_template_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ ne .method "POST" }} {{ .status | ne "500" }}` |= "true true""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"method=GET status=200"#));
    check!(!query.matches(&labels, r#"method=POST status=200"#));
    check!(!query.matches(&labels, r#"method=GET status=500"#));
}

#[test]
fn query_evaluator_line_format_applies_ordering_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if gt .status 499 }}server{{ else }}ok{{ end }} {{ ge .status 500 }} {{ lt 2 10 }} {{ le .status 500 }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let server = query
        .evaluate_with_fields(&labels, r#"status=500"#, &BTreeMap::new())
        .unwrap();
    let ok = query
        .evaluate_with_fields(&labels, r#"status=200"#, &BTreeMap::new())
        .unwrap();

    check!(server.line == "server true true true");
    check!(ok.line == "ok false true true");
}

#[test]
fn query_evaluator_line_format_applies_len_template_helper() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `len={{ len .msg }}` |= "len=15""#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"msg="template helper""#));
    check!(!query.matches(&labels, r#"msg="tiny""#));
}

#[test]
fn query_evaluator_line_format_applies_conditional_template_blocks() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if contains "timeout" .msg }}timeout{{ else if eq "GET" .method }}read{{ else }}other{{ end }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let timeout = query
        .evaluate_with_fields(
            &labels,
            r#"method=POST msg="request timeout""#,
            &BTreeMap::new(),
        )
        .unwrap();
    let read = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();
    let other = query
        .evaluate_with_fields(&labels, r#"method=POST msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(timeout.line == "timeout");
    check!(read.line == "read");
    check!(other.line == "other");
}

#[test]
fn query_evaluator_line_format_applies_json_template_truthiness() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ if fromJson "[]" }}array{{ else }}empty-array{{ end }}|{{ if fromJson "{}" }}object{{ else }}empty-object{{ end }}|{{ if fromJson "null" }}null{{ else }}empty-null{{ end }}|{{ if fromJson "false" }}bool{{ else }}empty-bool{{ end }}|{{ if fromJson "0" }}number{{ else }}empty-number{{ end }}|{{ with fromJson "{\"method\":\"GET\"}" }}{{ .method }}{{ else }}missing{{ end }}|{{ not (fromJson "[]") }}|{{ or (fromJson "[]") (fromJson "{\"x\":1}") }}|{{ and (fromJson "{\"x\":1}") (fromJson "0") }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let result = query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();

    check!(
        result.line
            == "empty-array|empty-object|empty-null|empty-bool|empty-number|GET|true|true|false"
    );
}

#[test]
fn query_evaluator_line_format_applies_with_template_blocks() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ with .method }}method={{ . }}{{ else }}missing{{ end }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let present = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();
    let absent = query
        .evaluate_with_fields(&labels, r#"msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(present.line == "method=GET");
    check!(absent.line == "missing");
}

#[test]
fn query_evaluator_line_format_applies_template_variable_assignments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ $method := .method }}{{ $status := .status }}{{ $method }} {{ $status | printf "status=%s" }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let output = query
        .evaluate_with_fields(&labels, r#"method=GET status=500"#, &BTreeMap::new())
        .unwrap();

    check!(output.line == "GET status=500");
}

#[test]
fn query_evaluator_line_format_applies_template_trim_markers() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `left {{- .method -}} right`"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let output = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(output.line == "leftGETright");
}

#[test]
fn query_evaluator_line_format_ignores_template_comments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `before{{/* hidden */}}after {{ .method }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let output = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(output.line == "beforeafter GET");
}

#[test]
fn query_evaluator_line_format_applies_boolean_template_combinators() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if and (contains "timeout" .msg) (hasPrefix "/api" .path) }}route-timeout{{ else if or (eq "POST" .method) (not (hasSuffix "ok" .msg)) }}attention{{ else }}other{{ end }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let route_timeout = query
        .evaluate_with_fields(
            &labels,
            r#"method=GET path=/api/items msg=request_timeout"#,
            &BTreeMap::new(),
        )
        .unwrap();
    let attention = query
        .evaluate_with_fields(
            &labels,
            r#"method=POST path=/health msg=ok"#,
            &BTreeMap::new(),
        )
        .unwrap();
    let other = query
        .evaluate_with_fields(
            &labels,
            r#"method=GET path=/health msg=ok"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(route_timeout.line == "route-timeout");
    check!(attention.line == "attention");
    check!(other.line == "other");
}

#[test]
fn query_evaluator_line_format_applies_spacing_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ alignLeft 5 .short }}|{{ alignLeft 5 .long }}|{{ alignRight 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}|{{ .multi | indent 2 }}|{{ .multi | nindent 2 }}` |= "hi   |hello|   hi|world|xxx|  alpha\n  beta|\n  alpha\n  beta""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"short=hi long=hello-world mark=x multi="alpha\nbeta""#
    ));
    check!(!query.matches(
        &labels,
        r#"short=hi long=hello-world mark=y multi="alpha\nbeta""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_regex_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ count "o" .word }}|{{ .word | count "o" }}|{{ regexReplaceAll "(f)(o+)" .word "${1}a" }}|{{ .word | regexReplaceAllLiteral "(f)(o+)" "${1}a" }}` |= "2|2|fa|${1}a""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"word=foo"#));
    check!(!query.matches(&labels, r#"word=bar"#));
}

#[test]
fn parses_label_format_stage_with_rename_and_template_assignments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | label_format route=path, summary="{{.method}} {{.status}}""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::LabelFormat(
                    LabelFormat::new(vec![
                        LabelFormatAssignment::rename("route", "path").unwrap(),
                        LabelFormatAssignment::template("summary", "{{.method}} {{.status}}")
                            .unwrap(),
                    ])
                    .unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_label_format_to_later_filters_and_labels() {
    let query = parse_query(
        r#"{app="api",env="prod"} | logfmt | label_format namespace=env, summary="{{.method}} {{.status}}" | namespace = "prod" | summary = "GET 500""#,
    )
    .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"method=GET status=500 path=/api"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("namespace") == Some(&"prod".to_string()));
    check!(evaluation.fields.get("summary") == Some(&"GET 500".to_string()));
    check!(!evaluation.fields.contains_key("env"));
}

#[test]
fn query_evaluator_label_format_applies_template_default_and_upper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | label_format method=`{{ .method | default "get" | upper }}` | method = "GET""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let missing_method = query
        .evaluate_with_fields(&labels, r#"status=500 path=/checkout"#, &BTreeMap::new())
        .unwrap();
    let present_method = query.evaluate_with_fields(
        &labels,
        r#"method=post status=500 path=/checkout"#,
        &BTreeMap::new(),
    );

    check!(missing_method.fields.get("method") == Some(&"GET".to_string()));
    check!(present_method.is_none());
}

#[test]
fn parses_parameterized_logfmt_parser_stage() {
    let query =
        parse_query(r#"{app="api"} | logfmt host, fwd_ip="fwd" | fwd_ip = "124.133.124.161""#)
            .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::LogfmtSelected(
                    LogfmtParserConfig::new(vec![
                        LogfmtExtraction::same("host").unwrap(),
                        LogfmtExtraction::rename("fwd_ip", "fwd").unwrap(),
                    ])
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "fwd_ip",
                    ComparisonOp::Equal,
                    FieldValue::String("124.133.124.161".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_parameterized_logfmt_extracts_only_requested_fields() {
    let query = parse_query(
        r#"{app="api"} | logfmt host, fwd_ip="fwd" | host = "grafana.net" | fwd_ip = "124.133.124.161""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"at=info method=GET path=/ host=grafana.net fwd="124.133.124.161" status=200"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("fwd_ip") == Some(&"124.133.124.161".to_string()));
    check!(!evaluation.fields.contains_key("method"));
    check!(!evaluation.fields.contains_key("status"));
}

#[test]
fn query_evaluator_parameterized_logfmt_keeps_missing_requested_fields_as_empty() {
    let query = parse_query(r#"{app="api"} | logfmt status, message="msg""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"duration=25ms msg="api typed parser ok""#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("status") == Some(&String::new()));
    check!(evaluation.fields.get("message") == Some(&"api typed parser ok".to_string()));
}

#[test]
fn query_evaluator_numeric_field_filter_keeps_invalid_present_values_as_label_filter_errors() {
    let query = parse_query(r#"{app="api"} | logfmt status | status >= 500"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"duration=25ms msg="api typed parser ok""#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("status") == Some(&String::new()));
    check!(evaluation.fields.get("__error__") == Some(&"LabelFilterErr".to_string()));
    check!(
        evaluation.fields.get("__error_details__")
            == Some(&r#"strconv.ParseFloat: parsing "": invalid syntax"#.to_string())
    );
}

#[test]
fn query_evaluator_logfmt_keep_empty_keeps_standalone_keys() {
    let query =
        parse_query(r#"{app="api"} | logfmt --keep-empty | empty = "" | host = "grafana.net""#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, r#"host=grafana.net empty"#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("empty") == Some(&String::new()));
}

#[test]
fn query_evaluator_field_filter_matches_missing_string_label_as_empty() {
    let query = parse_query(r#"{app="api"} | logfmt | empty = """#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, r#"host=grafana.net"#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(!evaluation.fields.contains_key("empty"));

    let non_empty_query = parse_query(r#"{app="api"} | logfmt | empty != """#).unwrap();
    check!(
        non_empty_query
            .evaluate_with_fields(&labels, r#"host=grafana.net"#, &BTreeMap::new())
            .is_none()
    );
}

#[test]
fn query_evaluator_logfmt_strict_marks_malformed_tokens_as_errors() {
    let query =
        parse_query(r#"{app="api"} | logfmt --strict | __error__ = "LogfmtParserErr""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, "host=grafana.net =broken", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("__error__") == Some(&"LogfmtParserErr".to_string()));
}

#[test]
fn query_evaluator_logfmt_strict_ignores_standalone_keys_without_keep_empty() {
    let query = parse_query(r#"{app="api"} | logfmt --strict | __error__ = """#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"host=grafana.net empty status=204"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("status") == Some(&"204".to_string()));
    check!(!evaluation.fields.contains_key("empty"));
    check!(!evaluation.fields.contains_key("__error__"));
}

#[test]
fn query_evaluator_logfmt_sanitizes_ansi_prefixed_field_names() {
    let query = parse_query(r#"{app="api"} | logfmt"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("_31mstatus") == Some(&"503".to_string()));
    check!(!evaluation.fields.contains_key("\u{1b}[31mstatus"));
}

#[test]
fn query_evaluator_logfmt_strict_reports_loki_syntax_error_details() {
    let query =
        parse_query(r#"{app="api"} | logfmt --strict | __error__ = "LogfmtParserErr""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, r#"status=500 msg="unterminated"#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__error__") == Some(&"LogfmtParserErr".to_string()));
    check!(
        evaluation.fields.get("__error_details__")
            == Some(&"logfmt syntax error at pos 29 : unterminated quoted value".to_string())
    );
}

#[test]
fn parses_drop_and_keep_label_expression_stages() {
    let query = parse_query(
        r#"{app="api"} | logfmt | drop level, app=~"debug-.*" | keep method, status="500""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::DropLabels(
                    LabelSelectionSet::new(vec![
                        LabelSelection::name("level").unwrap(),
                        LabelSelection::regex("app", "debug-.*").unwrap(),
                    ])
                    .unwrap()
                ),
                PipelineStage::KeepLabels(
                    LabelSelectionSet::new(vec![
                        LabelSelection::name("method").unwrap(),
                        LabelSelection::equal("status", "500").unwrap(),
                    ])
                    .unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_drop_and_keep_to_later_filters_and_labels() {
    let query = parse_query(
        r#"{app="api",env="prod"} | logfmt | drop env, level="debug" | keep app, method, status="500" | method = "GET""#,
    )
    .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
        ("__error__".to_string(), "ParserErr".to_string()),
    ]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"method=GET status=500 level=debug path=/api"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("app") == Some(&"api".to_string()));
    check!(evaluation.fields.get("method") == Some(&"GET".to_string()));
    check!(evaluation.fields.get("status") == Some(&"500".to_string()));
    check!(evaluation.fields.get("__error__") == Some(&"ParserErr".to_string()));
    check!(!evaluation.fields.contains_key("env"));
    check!(!evaluation.fields.contains_key("level"));
    check!(!evaluation.fields.contains_key("path"));
}

#[test]
fn query_evaluator_accepts_decimal_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=1.5", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__crabka_unwrap_sample_value__") == Some(&"1.5".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_accepts_signed_decimal_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=-1.5", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__crabka_unwrap_sample_value__") == Some(&"-1.5".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_accepts_scientific_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=-2.5e-1", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__crabka_unwrap_sample_value__") == Some(&"-0.25".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_flattens_nested_json_fields() {
    let query =
        parse_query(r#"{app="api"} | json | request_method = "GET" | response_status >= 500"#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(
        &labels,
        r#"{"request":{"method":"GET"},"response":{"status":500}}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"request":{"method":"POST"},"response":{"status":500}}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"request":{"method":"GET"},"response":{"status":200}}"#
    ));
}

#[test]
fn query_evaluator_sanitizes_json_field_names_and_skips_arrays() {
    let query = parse_query(
        r#"{app="api"} | json | request_headers_User_Agent = "curl/7.68.0" | servers = "ignored""#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(!query.matches(
        &labels,
        r#"{"request":{"headers":{"User-Agent":"curl/7.68.0"}},"servers":["10.0.0.1"]}"#
    ));

    let query =
        parse_query(r#"{app="api"} | json | request_headers_User_Agent = "curl/7.68.0""#).unwrap();
    check!(query.matches(
        &labels,
        r#"{"request":{"headers":{"User-Agent":"curl/7.68.0"}},"servers":["10.0.0.1"]}"#
    ));
}

#[test]
fn parses_pattern_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | status >= 500"#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Pattern(
                    PatternParser::new("<method> <path> (<status>) <duration>").unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_pattern_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | method = "POST" | status >= 500"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "POST /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "GET /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "POST /api/prom/query_range (200) 1.5s"));
    check!(!query.matches(&labels, "not a matching line"));
}

#[test]
fn query_evaluator_applies_unanchored_pattern_parser_and_collision_suffixes() {
    let query =
        parse_query(r#"{app="api",method="GET"} | pattern `<_> method=<method> status=<status>` | method = "GET" | method_extracted = "POST" | status = "500""#)
            .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("method".to_string(), "GET".to_string()),
    ]);

    check!(query.matches(&labels, "prefix method=POST status=500"));
    check!(!query.matches(&labels, "prefix method=POST status=200"));
}

#[test]
fn query_evaluator_exposes_pattern_parser_error_fields() {
    let query =
        parse_query(r#"{app="api"} | pattern `<method> <path>` | __error__ = "PatternParserErr""#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "too-few"));
    check!(!query.matches(&labels, "GET /ready"));

    let query = parse_query(r#"{app="api"} | pattern `<method> <path>` | __error__ = """#).unwrap();
    check!(!query.matches(&labels, "too-few"));
    check!(query.matches(&labels, "GET /ready"));
}

#[test]
fn parses_regexp_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | status >= 500"#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Regexp(
                    RegexpParser::new(
                        r"(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)"
                    )
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_regexp_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | method = "POST" | status >= 500"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "POST /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "GET /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "POST /api/prom/query_range (200) 1.5s"));
    check!(!query.matches(&labels, "not a matching line"));
}

#[test]
fn query_evaluator_suffixes_regexp_captures_that_collide_with_original_labels() {
    let query =
        parse_query(r#"{app="api",method="GET"} | regexp `method=(?P<method>\w+) status=(?P<status>\d+)` | method = "GET" | method_extracted = "POST" | status = "500""#)
            .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("method".to_string(), "GET".to_string()),
    ]);

    check!(query.matches(&labels, "prefix method=POST status=500 suffix"));
    check!(!query.matches(&labels, "prefix method=POST status=200 suffix"));
}

#[test]
fn query_evaluator_exposes_regexp_parser_error_fields() {
    let query =
        parse_query(r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+)` | __error__ = "RegexpParserErr""#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "too-few"));
    check!(!query.matches(&labels, "GET /ready"));

    let query =
        parse_query(r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+)` | __error__ = """#)
            .unwrap();
    check!(!query.matches(&labels, "too-few"));
    check!(query.matches(&labels, "GET /ready"));
}

#[test]
fn rejects_regexp_parser_without_named_capture() {
    check!(parse_query(r#"{app="api"} | regexp `\w+ \S+`"#).is_err());
}

#[test]
fn parses_logfmt_parser_stage_and_string_field_filter() {
    let query = parse_query(r#"{app="api"} | logfmt | msg = "api error""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "msg",
                    ComparisonOp::Equal,
                    FieldValue::String("api error".to_string())
                )),
            ]
    );
}

#[test]
fn parses_backtick_string_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | msg = `api error` | path =~ `/api/.+`"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "msg",
                    ComparisonOp::Equal,
                    FieldValue::String("api error".to_string())
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "path",
                    ComparisonOp::RegexEqual,
                    FieldValue::String("/api/.+".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_backtick_string_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | msg = `api error` | path =~ `/api/.+`"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"msg="api error" path=/api/search"#));
    check!(!query.matches(&labels, r#"msg="api ok" path=/api/search"#));
    check!(!query.matches(&labels, r#"msg="api error" path=/ready"#));
}

#[test]
fn query_evaluator_applies_ip_line_filters_to_complete_ip_tokens() {
    let query = parse_query(r#"{app="api"} |= ip("192.168.4.0/24")"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "client=192.168.4.20 status=200"));
    check!(!query.matches(&labels, "client=192.168.5.20 status=200"));

    let query = parse_query(r#"{app="api"} != ip("3.180.71.3")"#).unwrap();

    check!(query.matches(&labels, "client=93.180.71.3 status=200"));
    check!(!query.matches(&labels, "client=3.180.71.3 status=200"));
}

#[test]
fn query_evaluator_applies_ip_label_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | remote_addr = ip("192.168.4.5-192.168.4.20")"#)
            .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "remote_addr=192.168.4.12"));
    check!(!query.matches(&labels, "remote_addr=192.168.4.21"));

    let query = parse_query(r#"{app="api"} | logfmt | remote_addr != ip("192.168.4.2")"#).unwrap();

    check!(query.matches(&labels, "remote_addr=192.168.4.12"));
    check!(!query.matches(&labels, "remote_addr=192.168.4.2"));
}

#[test]
fn parses_regex_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "method",
                    ComparisonOp::RegexEqual,
                    FieldValue::String("GET|POST".to_string())
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "path",
                    ComparisonOp::RegexNotEqual,
                    FieldValue::String("/health.*".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_logfmt_parser_stage_and_field_filters() {
    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 | msg = "api error""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, r#"status=500 msg="api error" trace=abc"#));
    check!(!query.matches(&labels, r#"status=200 msg="api ok" trace=abc"#));
    check!(!query.matches(&labels, "plain line"));
}

#[test]
fn parses_duration_and_bytes_field_filters() {
    check!(
        parse_query(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 1.5MiB"#).is_ok()
    );
}

#[test]
fn query_evaluator_applies_duration_and_bytes_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "duration=25ms bytes_consumed=21MB"));
    check!(!query.matches(&labels, "duration=10ms bytes_consumed=21MB"));
    check!(!query.matches(&labels, "duration=25ms bytes_consumed=19MB"));
    check!(!query.matches(&labels, "duration=oops bytes_consumed=21MB"));
}

#[test]
fn query_evaluator_applies_and_or_field_filter_chains() {
    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 or level = "warn""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "status=500 level=info"));
    check!(query.matches(&labels, "status=200 level=warn"));
    check!(!query.matches(&labels, "status=200 level=info"));

    let query =
        parse_query(r#"{app="api"} | logfmt | status >= 500 and path !~ "/health.*""#).unwrap();

    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));
}

#[test]
fn query_evaluator_applies_parenthesized_field_filter_chains() {
    let query = parse_query(
        r#"{app="api"} | logfmt | duration >= 20ms or (method = "GET" and size <= 20KB)"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "duration=10ms method=GET size=10KB"));
    check!(query.matches(&labels, "duration=25ms method=POST size=40KB"));
    check!(!query.matches(&labels, "duration=10ms method=GET size=30KB"));

    let flat_query = parse_query(
        r#"{app="api"} | logfmt | duration >= 20ms or method = "GET" and size <= 20KB"#,
    )
    .unwrap();

    check!(!flat_query.matches(&labels, "duration=25ms method=POST size=40KB"));
}

#[test]
fn query_evaluator_treats_comma_and_adjacent_field_filters_as_and() {
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let query =
        parse_query(r#"{app="api"} | logfmt | status >= 500, path !~ "/health.*""#).unwrap();
    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));

    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 path !~ "/health.*""#).unwrap();
    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));
}

#[test]
fn query_evaluator_skips_unterminated_logfmt_quoted_field() {
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    let query = parse_query(r#"{app="api"} | logfmt | msg = "unterminated""#).unwrap();
    check!(!query.matches(&labels, r#"status=500 msg="unterminated"#));

    let query = parse_query(r#"{app="api"} | logfmt | status >= 500"#).unwrap();
    check!(query.matches(&labels, r#"status=500 msg="unterminated"#));
}

#[test]
fn query_evaluator_applies_regex_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api".to_string())]);

    check!(query.matches(&labels, "method=GET path=/checkout"));
    check!(query.matches(&labels, "method=POST path=/checkout"));
    check!(!query.matches(&labels, "method=DELETE path=/checkout"));
    check!(!query.matches(&labels, "method=GET path=/healthz"));
}

#[test]
fn parses_count_over_time_metric_query() {
    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [30s])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_metric_range_selector_before_pipeline() {
    let query = parse_metric_query(
        r#"count_over_time({app="api"}[30s] |= "error" | logfmt | status >= 500)"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error" | logfmt | status >= 500"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_label_replace_metric_query() {
    let query = parse_metric_label_replace_query(
        r#"label_replace(count_over_time({app="api"} |= "error" [30s]), "service", "$1-api", "app", "(.*)")"#,
    )
    .unwrap();

    check!(query.destination_label == "service");
    check!(query.replacement == "$1-api");
    check!(query.source_label == "app");
    check!(query.pattern == "(.*)");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_label_join_metric_query() {
    let query = parse_metric_label_join_query(
        r#"label_join(count_over_time({app="api"} |= "error" [30s]), "joined", "/", "app", "env", "missing")"#,
    )
    .unwrap();

    check!(query.destination_label == "joined");
    check!(query.separator == "/");
    check!(query.source_labels == vec!["app", "env", "missing"]);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_metric_scalar_comparison_query() {
    let query = parse_metric_scalar_comparison_query(
        r#"count_over_time({app="api"} |= "error" [30s]) > bool 1.5e0"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.scalar == "1.5e0");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_scalar_metric_comparison_query() {
    let query = parse_metric_scalar_comparison_query(
        r#"2 > bool count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.scalar == "2");
    check!(query.scalar_on_left);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_metric_scalar_arithmetic_query() {
    let query = parse_metric_scalar_arithmetic_query(
        r#"count_over_time({app="api"} |= "error" [30s]) * 2.5"#,
    )
    .unwrap();

    check!(query.op == crabka_logql::MetricScalarArithmeticOp::Multiply);
    check!(query.scalar == "2.5");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_scalar_metric_arithmetic_query() {
    let query = parse_metric_scalar_arithmetic_query(
        r#"2 - count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == crabka_logql::MetricScalarArithmeticOp::Subtract);
    check!(query.scalar == "2");
    check!(query.scalar_on_left);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == 30_000_000_000);
}

#[test]
fn parses_metric_binary_arithmetic_query() {
    let query = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == crabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == 30_000_000_000);
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == 30_000_000_000);
}

#[test]
fn parses_metric_binary_arithmetic_matching_modifier() {
    let query = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"}[30s]) / ignoring(app) count_over_time({app="worker"}[30s])"#,
    )
    .unwrap();

    check!(
        query.matching
            == Some(crabka_logql::MetricVectorMatching::Ignoring {
                labels: vec!["app".to_string()],
                group: None,
            })
    );
    check!(query.op == crabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
}

#[test]
fn parses_metric_binary_arithmetic_group_modifier() {
    let query = parse_metric_binary_arithmetic_query(
        r#"sum by (app, env) (count_over_time({env="prod"}[30s])) / on(env) group_left(status) sum by (env, status) (count_over_time({env="prod"}[30s]))"#,
    )
    .unwrap();

    check!(
        query.matching
            == Some(crabka_logql::MetricVectorMatching::On {
                labels: vec!["env".to_string()],
                group: Some(crabka_logql::MetricVectorGroupModifier::Left(vec![
                    "status".to_string()
                ])),
            })
    );
    check!(query.op == crabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.vector_aggregation.is_some());
    check!(query.right.vector_aggregation.is_some());
}

#[test]
fn parses_metric_binary_comparison_query() {
    let query = parse_metric_binary_comparison_query(
        r#"count_over_time({app="api"}[30s]) > bool count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == 30_000_000_000);
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == 30_000_000_000);
}

#[test]
fn parses_metric_binary_set_query() {
    let query = parse_metric_binary_set_query(
        r#"count_over_time({app="api"}[30s]) and count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == crabka_logql::MetricBinarySetOp::And);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == 30_000_000_000);
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == 30_000_000_000);
}

#[test]
fn parses_rate_metric_query() {
    let query = parse_metric_query(r#"rate({app="api"} |= "error" [2m])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::Rate,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 120_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_rate_counter_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"rate_counter({app="api"} | logfmt | unwrap requests | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::RateCounter);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap requests | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_bytes_over_time_metric_query() {
    let query = parse_metric_query(r#"bytes_over_time({app="api"} |= "error" [1h])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::BytesOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 3_600_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_bytes_rate_metric_query() {
    let query = parse_metric_query(r#"bytes_rate({app="api"} |= "error" [2m])"#).unwrap();

    check!(format!("{:?}", query.aggregation) == "BytesRate");
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == 120_000_000_000);
}

#[test]
fn parses_sum_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::new("cost").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_sum_over_time_unwrap_bytes_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap bytes(size) | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::bytes("size").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_sum_over_time_unwrap_duration_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap duration(latency) | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::duration("latency").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_sum_over_time_unwrap_duration_seconds_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap duration_seconds(latency) | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::duration("latency").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_avg_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::AvgOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_avg_over_time_unwrap_metric_query_with_range_grouping() {
    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s]) by (app)"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::AvgOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
    check!(query.range_grouping == Some(VectorGrouping::By(vec!["app".to_string()])));
}

#[test]
fn parses_stdvar_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"stdvar_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::StdvarOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_stddev_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"stddev_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::StddevOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_quantile_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"quantile_over_time(0.75, {app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query.aggregation
            == RangeAggregation::QuantileOverTime(Quantile {
                numerator: 3,
                denominator: 4,
            })
    );
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_absent_over_time_metric_query() {
    let query = parse_metric_query(r#"absent_over_time({app="api",env="prod"} [30s])"#).unwrap();

    check!(query.aggregation == RangeAggregation::AbsentOverTime);
    check!(query.stream == parse_query(r#"{app="api",env="prod"}"#).unwrap());
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_present_over_time_metric_query() {
    let query = parse_metric_query(r#"present_over_time({app="api"} |= "error" [30s])"#).unwrap();

    check!(query.aggregation == RangeAggregation::PresentOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_min_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"min_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::MinOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_max_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"max_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::MaxOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_first_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"first_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::FirstOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_last_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"last_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::LastOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == 30_000_000_000);
}

#[test]
fn parses_compound_prometheus_duration_metric_query() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [1h30m15s250ms])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 5_415_250_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_metric_query_with_range_offset() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [10s] offset 5m)"#).unwrap();

    check!(query.aggregation == RangeAggregation::CountOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == 10_000_000_000);
    check!(query.offset_ns == 300_000_000_000);
}

#[test]
fn parses_metric_query_with_negative_range_offset() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [10s] offset -5m)"#).unwrap();

    check!(query.aggregation == RangeAggregation::CountOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == 10_000_000_000);
    check!(query.offset_ns == -300_000_000_000);
}

#[test]
fn rejects_metric_query_with_offset_after_range_aggregation() {
    check!(parse_metric_query(r#"count_over_time({app="api"} [10s]) offset 5m"#).is_err());
}

#[test]
fn rejects_out_of_order_or_repeated_prometheus_duration_units() {
    check!(parse_metric_query(r#"count_over_time({app="api"} [30m1h])"#).is_err());
    check!(parse_metric_query(r#"count_over_time({app="api"} [1h30m15m])"#).is_err());
}

#[test]
fn parses_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"sum by (env, status) (rate({app="api"} |= "error" [5m]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::Rate,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Sum,
                    grouping: Some(VectorGrouping::By(vec![
                        "env".to_string(),
                        "status".to_string(),
                    ])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 300_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_vector_aggregation_metric_query_with_trailing_grouping() {
    let query =
        parse_metric_query(r#"sum(rate({app="api"} |= "error" [5m])) by (env, status)"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::Rate,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Sum,
                    grouping: Some(VectorGrouping::By(vec![
                        "env".to_string(),
                        "status".to_string(),
                    ])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: 300_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_vector_aggregation_without_metric_query() {
    let query =
        parse_metric_query(r#"avg without (pod) (bytes_over_time({app="api", env="prod"} [30s]))"#)
            .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::BytesOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Avg,
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api", env="prod"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_stddev_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"stddev by (env) (count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Stddev,
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_stdvar_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"stdvar(count_over_time({app="api"} [30s])) without (pod)"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Stdvar,
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_count_values_vector_aggregation_metric_query() {
    let query = parse_metric_query(
        r#"count_values by (env) ("events", count_over_time({app="api"} [30s]))"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::CountValues("events".to_string()),
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_topk_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"topk by (env) (2, count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::TopK(2),
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_approx_topk_metric_query() {
    let query =
        parse_metric_query(r#"approx_topk(2, count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::ApproxTopK(2),
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn rejects_approx_topk_with_grouping() {
    check!(
        parse_metric_query(r#"approx_topk by (env) (2, count_over_time({app="api"} [30s]))"#)
            .is_err()
    );
}

#[test]
fn parses_bottomk_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"bottomk(2, count_over_time({app="api"} [30s])) without (pod)"#)
            .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::BottomK(2),
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_sort_vector_aggregation_metric_query() {
    let query = parse_metric_query(r#"sort(count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Sort,
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn parses_sort_desc_vector_aggregation_metric_query() {
    let query = parse_metric_query(r#"sort_desc(count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::SortDesc,
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: 30_000_000_000,
                offset_ns: 0,
            }
    );
}

#[test]
fn invalid_regex_reports_parse_error() {
    let error = parse_query(r#"{app=~"["}"#).unwrap_err();

    check!(error.to_string().contains("invalid regex"));
}

#[test]
fn invalid_syntax_reports_expected_token() {
    let error = parse_query(r#"{app="api" |= "error""#).unwrap_err();

    check!(error.to_string().contains("expected"));
}

fn current_unix_epoch_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos()
}
