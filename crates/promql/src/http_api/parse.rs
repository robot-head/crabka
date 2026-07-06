use axum::{
    body::Bytes,
    extract::RawQuery,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use url::form_urlencoded;

use super::{ApiError, required_form_param, success_data_response};
use crate::parse_promql;

#[derive(Debug, Deserialize)]
struct ParseQueryParams {
    query: String,
}

pub(super) async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

pub(super) async fn format_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

fn format_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => success_data_response(expr.to_string()),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(super) async fn parse_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

pub(super) async fn parse_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

fn parse_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => match serde_json::to_value(expr) {
            Ok(value) => success_data_response(value),
            Err(error) => ApiError::internal(format!("PromQL AST serialization failed: {error}"))
                .into_response(),
        },
        Err(error) => ApiError::from(error).into_response(),
    }
}

fn parse_query_params(body: &[u8]) -> Result<ParseQueryParams, ApiError> {
    let mut query = None;
    for (name, value) in form_urlencoded::parse(body) {
        if name == "query" {
            query = Some(value.into_owned());
        }
    }
    Ok(ParseQueryParams {
        query: required_form_param(query, "query")?,
    })
}
