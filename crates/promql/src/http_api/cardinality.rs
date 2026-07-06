use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json,
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use crabka_blockstore::Labels;

use super::{
    ApiError, CardinalityParams, PrometheusApiState, active_series_response, apply_limit,
    cardinality_label_names_response, cardinality_label_values_response,
    enforce_selected_series_limit, labels_key, parse_cardinality_form, parse_cardinality_params,
    selector_matchers, tenant_from_headers,
};
use crate::MetricStore;

pub(super) async fn cardinality_label_names<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_names_inner(state, headers, params).await
}

pub(super) async fn cardinality_label_names_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_names_inner(state, headers, params).await
}

async fn cardinality_label_names_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, &tenant, &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    Json(cardinality_label_names_response(&series, params.limit)).into_response()
}

pub(super) async fn cardinality_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}

pub(super) async fn cardinality_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}

async fn cardinality_label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, &tenant, &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    Json(cardinality_label_values_response(
        &series,
        &params.label_names,
        params.limit,
    ))
    .into_response()
}

/// Resolve the series set a cardinality request operates on: the selector match
/// when a `selector` is provided, otherwise every active series for the tenant.
async fn cardinality_series<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    if params.selector.is_some() {
        cardinality_series_for_params(state, tenant, params).await
    } else {
        state
            .store
            .cardinality_active_series(tenant)
            .await
            .map_err(ApiError::from)
    }
}

async fn cardinality_series_for_params<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    let selector = params.selector.as_deref().unwrap_or_default();
    let matcher_sets = selector_matchers(selector).map_err(ApiError::from)?;
    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        let series = state
            .store
            .series(tenant, &matchers, i64::MIN, i64::MAX)
            .await
            .map_err(ApiError::from)?;
        for labels in series {
            by_key.insert(labels_key(&labels), labels);
        }
    }
    Ok(by_key.into_values().collect())
}

pub(super) async fn cardinality_active_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, params).await
}

pub(super) async fn cardinality_active_series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, params).await
}

async fn cardinality_active_series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = cardinality_series(&state, &tenant, &params).await;
    match series {
        Ok(mut series) => {
            if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
                return error.into_response();
            }
            apply_limit(&mut series, params.limit);
            Json(active_series_response(series)).into_response()
        }
        Err(error) => error.into_response(),
    }
}
