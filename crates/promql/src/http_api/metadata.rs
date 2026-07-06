use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ApiError, PrometheusApiState, apply_limit, parse_limit_parameter, success_data_response,
    tenant_from_headers,
};
use crate::{MetricStore, store::MetadataRecord};

#[derive(Debug, Default, Deserialize)]
struct MetadataParams {
    metric: Option<String>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
}

pub(super) async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(&tenant, params.metric.as_deref())
        .await
    {
        Ok(mut metadata) => {
            apply_limit(&mut metadata, params.limit);
            success_data_response(metadata_json(metadata, params.limit_per_metric))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(&tenant, params.metric.as_deref())
        .await
    {
        Ok(mut metadata) => {
            apply_limit(&mut metadata, params.limit);
            success_data_response(target_metadata_json(metadata))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

fn parse_metadata_params(raw_query: Option<&str>) -> Result<MetadataParams, ApiError> {
    let mut params = MetadataParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "metric" => params.metric = Some(value.into_owned()),
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            "limit_per_metric" => params.limit_per_metric = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}

fn metadata_json(
    metadata: Vec<MetadataRecord>,
    limit_per_metric: Option<usize>,
) -> BTreeMap<String, Vec<Value>> {
    let mut by_metric = BTreeMap::<String, Vec<Value>>::new();
    for record in metadata {
        let entries = by_metric.entry(record.metric_family_name).or_default();
        if limit_per_metric == Some(0) || limit_per_metric.is_none_or(|limit| entries.len() < limit)
        {
            entries.push(json!({
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            }));
        }
    }
    by_metric
}

fn target_metadata_json(metadata: Vec<MetadataRecord>) -> Vec<Value> {
    metadata
        .into_iter()
        .map(|record| {
            json!({
                "target": {},
                "metric": record.metric_family_name,
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            })
        })
        .collect()
}
