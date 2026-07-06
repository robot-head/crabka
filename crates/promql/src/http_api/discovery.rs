use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};

use super::{
    ApiError, DiscoveryParams, PrometheusApiState, apply_limit, discovery_matchers,
    discovery_window, enforce_selected_series_limit, labels_json, labels_key, parse_discovery_form,
    parse_discovery_params, record_query_response, success_data_response, tenant_from_headers,
};
use crate::MetricStore;

pub(super) async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, params).await
}

pub(super) async fn series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, params).await
}

async fn series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = series_dispatch(&state, &headers, params).await;
    record_query_response(&state, "series", &response, started);
    response
}

async fn series_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .series(&tenant, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(series) => {
                for labels in series {
                    by_key.insert(labels_key(&labels), labels);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut series = by_key
        .into_values()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    if let Err(error) = enforce_selected_series_limit(state, &tenant, series.len()) {
        return error.into_response();
    }
    apply_limit(&mut series, params.limit);
    success_data_response(series)
}

pub(super) async fn labels<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}

pub(super) async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}

async fn labels_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = labels_dispatch(&state, &headers, params).await;
    record_query_response(&state, "labels", &response, started);
    response
}

async fn labels_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut names = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_names(&tenant, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(label_names) => {
                for name in label_names {
                    names.insert(name.clone(), name);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut names = names.into_values().collect::<Vec<_>>();
    apply_limit(&mut names, params.limit);
    success_data_response(names)
}

pub(super) async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}

pub(super) async fn label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}

async fn label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    name: String,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = label_values_dispatch(&state, &headers, name, params).await;
    record_query_response(&state, "label_values", &response, started);
    response
}

async fn label_values_dispatch<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    headers: &HeaderMap,
    name: String,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut values = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_values(&tenant, &name, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(label_values) => {
                for value in label_values {
                    values.insert(value.clone(), value);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut values = values.into_values().collect::<Vec<_>>();
    apply_limit(&mut values, params.limit);
    success_data_response(values)
}
