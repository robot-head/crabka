use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;
use crabka_blockstore::LabelMatcher;
use crabka_metrics::{QueryEnforcer, validate_tenant};
use promql_parser::parser::Expr;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use super::{ApiError, PrometheusApiState};
use crate::{
    MetricStore, PromqlError, QueryResult,
    engine::{MAX_RESOLUTION_POINTS, label_matcher_sets},
    parse_promql,
};

#[derive(Debug, Default)]
pub(super) struct DiscoveryParams {
    pub(super) matches: Vec<String>,
    pub(super) start: Option<String>,
    pub(super) end: Option<String>,
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Default)]
pub(super) struct CardinalityParams {
    pub(super) selector: Option<String>,
    pub(super) label_names: Vec<String>,
    pub(super) limit: Option<usize>,
}

pub(super) fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ApiError> {
    let tenant = headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|tenant| !tenant.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("missing X-Scope-OrgID tenant header"))?;
    validate_tenant(&tenant).map_err(ApiError::bad_data)?;
    Ok(tenant)
}

pub(super) fn optional_timestamp_ms(value: Option<&str>) -> Result<i64, ApiError> {
    match value {
        Some(value) => timestamp_ms(value),
        None => unix_now_ms(),
    }
}

pub(super) fn unix_now_ms() -> Result<i64, ApiError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::internal(format!("system time before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ApiError::internal("system time exceeds supported timestamp range"))
}

pub(super) fn timestamp_ms(value: &str) -> Result<i64, ApiError> {
    seconds_to_ms(value)
        .or_else(|()| rfc3339_to_ms(value))
        .map_err(|()| ApiError::bad_data("invalid timestamp"))
}

pub(super) fn duration_ms(value: &str) -> Result<i64, ApiError> {
    let step_ms = seconds_to_ms(value)
        .or_else(|()| prometheus_duration_ms(value).ok_or(()))
        .map_err(|()| ApiError::bad_data("invalid duration"))?;
    if step_ms <= 0 {
        return Err(ApiError::bad_data("duration must be positive"));
    }
    Ok(step_ms)
}

pub(super) fn validate_timestamp_range(start_ms: i64, end_ms: i64) -> Result<(), ApiError> {
    if end_ms < start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok(())
}

/// Reject a range query whose resolution exceeds the per-timeseries point cap.
///
/// Prometheus enforces this unconditionally (independent of any configured
/// per-tenant limit) in `web/api/v1/api.go`: it rejects when
/// `(end - start) / step > maxResolution` (integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]). The error message and the
/// comma-formatted bound are matched byte-for-byte so Prometheus/Grafana clients
/// that string-match on it behave identically. `step_ms` is already validated
/// positive by [`duration_ms`].
pub(super) fn check_range_resolution(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<(), ApiError> {
    if step_ms <= 0 {
        return Ok(());
    }
    let intervals = end_ms.saturating_sub(start_ms) / step_ms;
    if intervals > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX) {
        return Err(ApiError::bad_data(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)",
        ));
    }
    Ok(())
}

pub(super) fn enforce_selected_series_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    selected: usize,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_series_count(
        limits.for_tenant(tenant),
        u64::try_from(selected).unwrap_or(u64::MAX),
    )
    .map_err(ApiError::from)
}

pub(super) fn enforce_sample_count<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    processed: u64,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_sample_count(limits.for_tenant(tenant), processed).map_err(ApiError::from)
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "Prometheus HTTP timestamps are decimal Unix seconds; internal evaluation uses millisecond integers."
)]
fn seconds_to_ms(value: &str) -> Result<i64, ()> {
    let seconds = value.parse::<f64>().map_err(|_| ())?;
    Ok((seconds * 1000.0).round() as i64)
}

fn prometheus_duration_ms(value: &str) -> Option<i64> {
    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = value.as_bytes();

    while index < bytes.len() {
        let amount_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if amount_start == index {
            return None;
        }
        let amount = value[amount_start..index].parse::<i64>().ok()?;

        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &value[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        total_ms = total_ms.checked_add(amount.checked_mul(multiplier)?)?;
    }

    Some(total_ms)
}

fn rfc3339_to_ms(value: &str) -> Result<i64, ()> {
    let time = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
    i64::try_from(time.unix_timestamp_nanos() / 1_000_000).map_err(|_| ())
}

pub(super) fn parse_discovery_params(raw_query: Option<&str>) -> Result<DiscoveryParams, ApiError> {
    let mut params = DiscoveryParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "match[]" => params.matches.push(value.into_owned()),
            "start" => params.start = Some(value.into_owned()),
            "end" => params.end = Some(value.into_owned()),
            "limit" => {
                params.limit = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid limit parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}

pub(super) fn parse_discovery_form(body: &[u8]) -> Result<DiscoveryParams, ApiError> {
    parse_discovery_params(std::str::from_utf8(body).ok())
}

pub(super) fn parse_limit_parameter(value: &str) -> Result<usize, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data("invalid limit parameter"))
}

pub(super) fn parse_cardinality_params(
    raw_query: Option<&str>,
) -> Result<CardinalityParams, ApiError> {
    let mut params = CardinalityParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "selector" => params.selector = Some(value.into_owned()),
            "label_names[]" => params.label_names.push(value.into_owned()),
            "count_method" => match value.as_ref() {
                "inmemory" | "active" => {}
                _ => return Err(ApiError::bad_data("invalid count_method parameter")),
            },
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}

pub(super) fn parse_cardinality_form(body: &[u8]) -> Result<CardinalityParams, ApiError> {
    parse_cardinality_params(std::str::from_utf8(body).ok())
}

pub(super) fn required_form_param(value: Option<String>, name: &str) -> Result<String, ApiError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_data(format!("missing {name} parameter")))
}

pub(super) fn apply_limit<T>(values: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit.filter(|limit| *limit > 0) {
        values.truncate(limit);
    }
}

pub(super) fn apply_result_limit(result: &mut QueryResult, limit: Option<usize>) {
    match result {
        QueryResult::InstantVector(samples) => apply_limit(samples, limit),
        QueryResult::RangeMatrix(series) => apply_limit(series, limit),
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => {}
    }
}

pub(super) struct DiscoveryWindow {
    pub(super) start_ms: i64,
    pub(super) end_ms: i64,
}

pub(super) fn discovery_window(params: &DiscoveryParams) -> Result<DiscoveryWindow, ApiError> {
    let start_ms = match params.start.as_deref() {
        Some(start) => timestamp_ms(start)?,
        None => 0,
    };
    let end_ms = match params.end.as_deref() {
        Some(end) => timestamp_ms(end)?,
        None => i64::MAX,
    };
    validate_timestamp_range(start_ms, end_ms)?;
    Ok(DiscoveryWindow { start_ms, end_ms })
}

pub(super) fn discovery_matchers(
    params: &DiscoveryParams,
) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    if params.matches.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    let mut out = Vec::new();
    for selector in &params.matches {
        out.extend(selector_matchers(selector).map_err(ApiError::from)?);
    }
    Ok(out)
}

pub(super) fn selector_matchers(selector: &str) -> Result<Vec<Vec<LabelMatcher>>, PromqlError> {
    match parse_promql(selector)? {
        Expr::VectorSelector(selector) => Ok(label_matcher_sets(&selector)),
        Expr::MatrixSelector(selector) => Ok(label_matcher_sets(&selector.vs)),
        other => Err(PromqlError::Plan(format!(
            "metadata matcher must be a vector selector, got {other}"
        ))),
    }
}
