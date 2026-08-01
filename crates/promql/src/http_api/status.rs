use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use crabka_units::fmt::Human as _;
use serde::Deserialize;
use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ApiError, PrometheusApiState, apply_limit, parse_limit_parameter, success_data_response,
    tenant_from_headers,
};
use crate::{
    MetricStore,
    store::{NamedTsdbStat, TsdbBlock, TsdbStats},
};

#[derive(Debug, Default, Deserialize)]
struct TsdbStatusParams {
    limit: Option<usize>,
}

pub(super) async fn build_info() -> Response {
    success_data_response(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "",
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": "",
    }))
}

pub(super) async fn status_config() -> Response {
    success_data_response(json!({
        "yaml": "global:\n  scrape_interval: 1m\n",
    }))
}

pub(super) async fn status_flags<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    success_data_response(json!({
        "log.level": "info",
        "query.lookback-delta": state.engine_opts.lookback_delta.human().to_string(),
        "query.max-concurrency": state.max_concurrent_queries.to_string(),
        "storage.tsdb.retention.time": "15d",
    }))
}

pub(super) async fn runtime_info<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let tsdb_stats = match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb_stats) => tsdb_stats,
        Err(error) => return ApiError::from(error).into_response(),
    };

    success_data_response(json!({
        "startTime": unix_time_string(state.start_time),
        "CWD": std::env::current_dir()
            .ok()
            .and_then(|path| path.into_os_string().into_string().ok())
            .unwrap_or_default(),
        "hostname": "",
        "serverTime": unix_time_string(SystemTime::now()),
        "reloadConfigSuccess": true,
        "lastConfigTime": unix_time_string(state.start_time),
        "timeSeriesCount": tsdb_stats.head_stats.num_series,
        "corruptionCount": 0,
        "goroutineCount": 0,
        "GOMAXPROCS": 0,
        "GOGC": "",
        "GODEBUG": "",
        "storageRetention": "",
    }))
}

pub(super) async fn tsdb_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_tsdb_status_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb) => success_data_response(tsdb_status_json(tsdb, params.limit)),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_blocks(&tenant).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn wal_replay_status() -> Response {
    success_data_response(json!({
        "min": 0,
        "max": 0,
        "current": 0,
        "state": "done",
    }))
}

pub(super) async fn alertmanagers() -> Response {
    success_data_response(json!({
        "activeAlertmanagers": [],
        "droppedAlertmanagers": [],
    }))
}

pub(super) async fn targets() -> Response {
    success_data_response(json!({
        "activeTargets": [],
        "droppedTargets": [],
        "droppedTargetCounts": {},
    }))
}

pub(super) async fn scrape_pools() -> Response {
    success_data_response(json!([]))
}

fn parse_tsdb_status_params(raw_query: Option<&str>) -> Result<TsdbStatusParams, ApiError> {
    let mut params = TsdbStatusParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        if name == "limit" {
            params.limit = Some(parse_limit_parameter(&value)?);
        }
    }
    Ok(params)
}

fn tsdb_status_json(stats: TsdbStats, limit: Option<usize>) -> Value {
    json!({
        "headStats": {
            "numSeries": stats.head_stats.num_series,
            "chunkCount": stats.head_stats.num_chunks,
            "numSamples": stats.head_stats.num_samples,
            "minTime": stats.head_stats.min_time,
            "maxTime": stats.head_stats.max_time,
        },
        "seriesCountByMetricName": named_tsdb_stats_json(stats.series_count_by_metric_name, limit),
        "labelValueCountByLabelName": named_tsdb_stats_json(stats.label_value_count_by_label_name, limit),
        "memoryInBytesByLabelName": named_tsdb_stats_json(stats.memory_in_bytes_by_label_name, limit),
        "seriesCountByLabelValuePair": named_tsdb_stats_json(stats.series_count_by_label_value_pair, limit),
    })
}

fn tsdb_blocks_json(mut blocks: Vec<TsdbBlock>) -> Vec<Value> {
    blocks.sort_by(|left, right| {
        left.min_time
            .cmp(&right.min_time)
            .then_with(|| left.max_time.cmp(&right.max_time))
            .then_with(|| left.id.cmp(&right.id))
    });
    blocks
        .into_iter()
        .map(|block| {
            json!({
                "ulid": block.id,
                "minTime": block.min_time,
                "maxTime": block.max_time,
                "stats": {
                    "numSamples": block.num_samples,
                    "numSeries": block.num_series,
                    "numChunks": block.num_series,
                },
            })
        })
        .collect()
}

fn named_tsdb_stats_json(mut stats: Vec<NamedTsdbStat>, limit: Option<usize>) -> Vec<Value> {
    apply_limit(&mut stats, limit);
    stats
        .into_iter()
        .map(|stat| {
            json!({
                "name": stat.name,
                "value": stat.value,
            })
        })
        .collect()
}

fn unix_time_string(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_string(),
        |duration| duration.as_secs().to_string(),
    )
}
