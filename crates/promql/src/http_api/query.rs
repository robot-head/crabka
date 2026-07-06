use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use crabka_metrics::QueryEnforcer;
use serde::Deserialize;
use url::form_urlencoded;

use super::{
    ApiError, PrometheusApiState, acquire_query_permit, apply_result_limit, check_range_resolution,
    duration_ms, exemplar_key, exemplars_json, optional_timestamp_ms, parse_limit_parameter,
    record_query_response, required_form_param, selector_matchers, success_data_response,
    success_response, tenant_from_headers, timestamp_ms, unix_now_ms, validate_timestamp_range,
};
use crate::{
    MetricStore,
    query_frontend::{FrontendRangeRequest, execute_range_query_frontend},
};

#[derive(Debug, Deserialize)]
struct InstantQueryParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RangeQueryParams {
    query: String,
    start: String,
    end: String,
    step: String,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ExemplarsQueryParams {
    query: String,
    start: String,
    end: String,
}

pub(super) async fn query<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match instant_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_inner(state, headers, params).await
}

pub(super) async fn query_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match instant_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_inner(state, headers, params).await
}

async fn query_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: InstantQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let _query_permit = acquire_query_permit(&state).await;
    // Held across dispatch so `active_queries` reflects queries admitted past
    // the concurrency gate and now executing; decremented on drop.
    let _active = state.active_query_guard();
    let response = query_dispatch(&state, &headers, params).await;
    record_query_response(&state, "query", &response, started);
    response
}

async fn query_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: InstantQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let time_ms = match optional_timestamp_ms(params.time.as_deref()) {
        Ok(time_ms) => time_ms,
        Err(error) => return error.into_response(),
    };

    let engine = state.engine_for_tenant(&tenant);
    // Time the pure engine eval (parse+plan+execute), excluding param decode,
    // permit wait, and response encoding — that whole-handler span is already
    // covered by `query_duration{route}`.
    let eval_started = std::time::Instant::now();
    let outcome = engine.query_instant(&tenant, &params.query, time_ms).await;
    state.record_eval(
        "instant",
        outcome.is_ok(),
        eval_started.elapsed().as_secs_f64(),
    );
    match outcome {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn query_range<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match range_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_range_inner(state, headers, params).await
}

pub(super) async fn query_range_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match range_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_range_inner(state, headers, params).await
}

async fn query_range_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: RangeQueryParams,
) -> Response {
    let started = std::time::Instant::now();
    let _query_permit = acquire_query_permit(&state).await;
    let _active = state.active_query_guard();
    let response = query_range_dispatch(&state, &headers, params).await;
    record_query_response(&state, "query_range", &response, started);
    response
}

async fn query_range_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: RangeQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }
    let step_ms = match duration_ms(&params.step) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = check_range_resolution(start_ms, end_ms, step_ms) {
        return error.into_response();
    }
    if let Some(limits) = &state.query_limits {
        let now_ms = match unix_now_ms() {
            Ok(now_ms) => now_ms,
            Err(error) => return error.into_response(),
        };
        if let Err(error) =
            QueryEnforcer::check_range(limits.for_tenant(&tenant), start_ms, end_ms, now_ms)
        {
            return ApiError::from(error).into_response();
        }
    }

    // Time the pure range eval (through the frontend cache/split when enabled),
    // labelled `type="range"`; the whole-handler span stays on
    // `query_duration{route="query_range"}`.
    let eval_started = std::time::Instant::now();
    let result = if let Some(frontend) = &state.query_frontend {
        let engine = state.engine_for_tenant(&tenant);
        execute_range_query_frontend(
            &engine,
            frontend.cache.as_ref(),
            &FrontendRangeRequest {
                tenant: tenant.clone(),
                query: params.query.clone(),
                start_ms,
                end_ms,
                step_ms,
                opts: frontend.opts,
            },
        )
        .await
    } else {
        state
            .engine_for_tenant(&tenant)
            .query_range(&tenant, &params.query, start_ms, end_ms, step_ms)
            .await
    };
    state.record_eval(
        "range",
        result.is_ok(),
        eval_started.elapsed().as_secs_f64(),
    );

    match result {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn query_exemplars<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match exemplars_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes())
        {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_exemplars_inner(state, headers, params).await
}

pub(super) async fn query_exemplars_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match exemplars_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_exemplars_inner(state, headers, params).await
}

async fn query_exemplars_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: ExemplarsQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match selector_matchers(&params.query) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(start_ms) => start_ms,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(end_ms) => end_ms,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .exemplars(&tenant, &matchers, start_ms, end_ms)
            .await
        {
            Ok(exemplars) => {
                for exemplar in exemplars {
                    by_key.insert(exemplar_key(&exemplar), exemplar);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    success_data_response(exemplars_json(by_key.into_values().collect()))
}

fn instant_query_params_from_form(body: &[u8]) -> Result<InstantQueryParams, ApiError> {
    let mut query = None;
    let mut time = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "time" => time = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(InstantQueryParams {
        query: required_form_param(query, "query")?,
        time,
        limit,
    })
}

fn range_query_params_from_form(body: &[u8]) -> Result<RangeQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            "step" => step = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(RangeQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
        step: required_form_param(step, "step")?,
        limit,
    })
}

fn exemplars_query_params_from_form(body: &[u8]) -> Result<ExemplarsQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(ExemplarsQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
    })
}
