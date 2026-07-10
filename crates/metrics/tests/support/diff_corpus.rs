use std::cmp::Ordering;

use serde_json::{Map, Value, json};

const FLOAT_EPSILON: f64 = 1e-6;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SeedPoint {
    pub metric: &'static str,
    pub labels: &'static [(&'static str, &'static str)],
    pub samples: &'static [(i64, f64)],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCase {
    pub name: &'static str,
    pub promql: &'static str,
    pub kind: QueryKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryKind {
    Instant { time: i64 },
    Range { start: i64, end: i64, step: i64 },
}

#[must_use]
pub fn seed_dataset() -> Vec<SeedPoint> {
    vec![
        SeedPoint {
            metric: "up",
            labels: &[("job", "api"), ("instance", "a")],
            samples: &[(0, 1.0), (15_000, 1.0), (30_000, 1.0), (45_000, 1.0)],
        },
        SeedPoint {
            metric: "http_requests_total",
            labels: &[("job", "api"), ("method", "GET"), ("code", "200")],
            samples: &[(0, 0.0), (15_000, 30.0), (30_000, 75.0), (45_000, 120.0)],
        },
        SeedPoint {
            metric: "http_requests_total",
            labels: &[("job", "api"), ("method", "POST"), ("code", "500")],
            samples: &[(0, 0.0), (15_000, 3.0), (30_000, 5.0), (45_000, 8.0)],
        },
        SeedPoint {
            metric: "cpu_temperature_celsius",
            labels: &[("job", "node"), ("instance", "a")],
            samples: &[(0, 40.0), (15_000, 42.0), (30_000, 41.5), (45_000, 43.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "0.5")],
            samples: &[(0, 0.0), (15_000, 10.0), (30_000, 25.0), (45_000, 40.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "1")],
            samples: &[(0, 0.0), (15_000, 20.0), (30_000, 45.0), (45_000, 70.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_bucket",
            labels: &[("job", "api"), ("le", "+Inf")],
            samples: &[(0, 0.0), (15_000, 25.0), (30_000, 55.0), (45_000, 90.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_sum",
            labels: &[("job", "api")],
            samples: &[(0, 0.0), (15_000, 12.0), (30_000, 30.0), (45_000, 60.0)],
        },
        SeedPoint {
            metric: "http_request_duration_seconds_count",
            labels: &[("job", "api")],
            samples: &[(0, 0.0), (15_000, 25.0), (30_000, 55.0), (45_000, 90.0)],
        },
        SeedPoint {
            metric: "native_histogram_marker",
            labels: &[("job", "api")],
            samples: &[(0, 1.0), (15_000, 1.0), (30_000, 1.0), (45_000, 1.0)],
        },
    ]
}

#[must_use]
pub fn query_corpus() -> Vec<QueryCase> {
    vec![
        QueryCase {
            name: "counter_rate_by_method",
            promql: "sum by (method) (rate(http_requests_total[30s]))",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "classic_histogram_quantile",
            promql: "histogram_quantile(0.9, sum by (le) (rate(http_request_duration_seconds_bucket[30s])))",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "counter_increase",
            promql: "increase(http_requests_total[45s])",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "binary_on_group_left",
            promql: "http_requests_total{method=\"GET\"} / on (job) group_left cpu_temperature_celsius",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "topk_gauge",
            promql: "topk(1, cpu_temperature_celsius)",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "over_time",
            promql: "avg_over_time(cpu_temperature_celsius[45s])",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "at_offset",
            promql: "up @ 30000 offset 15s",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "subquery",
            promql: "max_over_time(rate(http_requests_total[30s])[45s:15s])",
            kind: QueryKind::Instant { time: 45_000 },
        },
        QueryCase {
            name: "range_rate",
            promql: "rate(http_requests_total[30s])",
            kind: QueryKind::Range {
                start: 15_000,
                end: 45_000,
                step: 15_000,
            },
        },
    ]
}

#[must_use]
pub fn normalize(response: &Value) -> Value {
    normalize_value(response)
}

pub fn assert_query_equal(_name: &str, left: &Value, right: &Value) {
    let left = normalize(left);
    let right = normalize(right);
    assert2::assert!(left == right);
}

fn normalize_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(normalize_value).collect()),
        Value::Object(object) => normalize_object(object),
        Value::String(value) => normalize_string(value),
        other => other.clone(),
    }
}

fn normalize_object(object: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    for (key, value) in object {
        if is_volatile_field(key) {
            continue;
        }
        out.insert(key.clone(), normalize_value(value));
    }

    if let Some(Value::Array(result)) = out.get_mut("result") {
        result.sort_by(compare_series_result);
    }

    Value::Object(out)
}

fn normalize_string(value: &str) -> Value {
    match value {
        "NaN" | "+Inf" | "-Inf" => Value::String(value.to_string()),
        _ => value
            .parse::<f64>()
            .map_or_else(|_| Value::String(value.to_string()), rounded_float_string),
    }
}

fn rounded_float_string(value: f64) -> Value {
    if value.is_finite() {
        Value::String(format!(
            "{:.6}",
            (value / FLOAT_EPSILON).round() * FLOAT_EPSILON
        ))
    } else if value.is_nan() {
        Value::String("NaN".to_string())
    } else if value.is_sign_positive() {
        Value::String("+Inf".to_string())
    } else {
        Value::String("-Inf".to_string())
    }
}

fn is_volatile_field(key: &str) -> bool {
    matches!(key, "warnings" | "infos" | "stats")
}

fn compare_series_result(left: &Value, right: &Value) -> Ordering {
    series_sort_key(left).cmp(&series_sort_key(right))
}

fn series_sort_key(value: &Value) -> String {
    let labels = value
        .get("metric")
        .and_then(Value::as_object)
        .map(|metric| {
            metric
                .iter()
                .map(|(name, value)| format!("{name}={}", value.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let sample = value
        .get("value")
        .or_else(|| value.get("values"))
        .map_or_else(String::new, Value::to_string);
    format!("{labels}|{sample}")
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

#[allow(dead_code)]
fn _native_histogram_shape_marker() -> Value {
    json!({
        "schema": 0,
        "count": "1",
        "sum": "1",
        "zeroCount": "0",
        "positiveSpans": [],
        "negativeSpans": []
    })
}
