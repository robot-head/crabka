use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use crabka_blockstore::Labels;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use super::{
    AlertStateKey, ApiError, PrometheusApiState, RulesParams,
    alert_templates::{expand_alert_mapping_json, expand_alert_template, labels_from_map},
    sample_string, success_data_response, tenant_from_headers,
};
use crate::{MetricStore, PromqlError, QueryResult, SampleValue, parse_promql};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuleTypeFilter {
    Any,
    Alert,
    Record,
}

impl RuleTypeFilter {
    fn from_param(value: Option<&str>) -> Self {
        match value {
            Some("alert") => Self::Alert,
            Some("record") => Self::Record,
            _ => Self::Any,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RuleRenderOptions {
    type_filter: RuleTypeFilter,
    exclude_alerts: bool,
}

pub(super) async fn rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_rules_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let groups = match prometheus_rule_groups_json(
        &state,
        &tenant,
        rules,
        RuleRenderOptions {
            type_filter: RuleTypeFilter::from_param(params.rule_type.as_deref()),
            exclude_alerts: params.exclude_alerts.unwrap_or(false),
        },
    )
    .await
    {
        Ok(groups) => groups,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "groups": groups,
    }))
}

pub(super) async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let alerts = match prometheus_alerts_json(&state, &tenant, rules).await {
        Ok(alerts) => alerts,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "alerts": alerts,
    }))
}

pub(super) async fn ruler_config_rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let rules = rules
        .into_iter()
        .map(|(namespace, groups)| (namespace, groups.into_values().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();
    yaml_response(StatusCode::OK, &rules)
}

pub(super) async fn ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let groups = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match groups {
        Some(groups) => yaml_response(StatusCode::OK, &groups.into_values().collect::<Vec<_>>()),
        None => ApiError::not_found("rule namespace not found").into_response(),
    }
}

pub(super) async fn ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let group = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .and_then(|groups| groups.get(&group_name))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match group {
        Some(group) => yaml_response(StatusCode::OK, &group),
        None => ApiError::not_found("rule group not found").into_response(),
    }
}

pub(super) async fn set_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_yaml_content_type(&headers) {
        return error.into_response();
    }
    let group: serde_yaml::Value = match serde_yaml::from_slice(&body) {
        Ok(group) => group,
        Err(error) => {
            return ApiError::bad_data(format!("rule group YAML decode failed: {error}"))
                .into_response();
        }
    };
    let group_name = match rule_group_name(&group) {
        Ok(name) => name,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_rule_group(&group) {
        return error.into_response();
    }

    match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules
                .entry(tenant)
                .or_default()
                .entry(namespace)
                .or_default()
                .insert(group_name, group);
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

pub(super) async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant)
                && let Some(groups) = namespaces.get_mut(&namespace)
            {
                groups.remove(&group_name);
                if groups.is_empty() {
                    namespaces.remove(&namespace);
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

pub(super) async fn delete_ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant) {
                namespaces.remove(&namespace);
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

fn require_yaml_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    match content_type {
        "application/yaml" | "application/x-yaml" | "text/yaml" => Ok(()),
        _ => Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "ruler config requires application/yaml".into(),
        }),
    }
}

fn rule_group_name(group: &serde_yaml::Value) -> Result<String, ApiError> {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain a non-empty name"))
}

fn validate_rule_group(group: &serde_yaml::Value) -> Result<(), ApiError> {
    let rules = group
        .get("rules")
        .and_then(serde_yaml::Value::as_sequence)
        .filter(|rules| !rules.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain at least one rule"))?;
    for rule in rules {
        validate_rule(rule)?;
    }
    Ok(())
}

fn validate_rule(rule: &serde_yaml::Value) -> Result<(), ApiError> {
    let has_record = yaml_optional_string(rule, "record").is_some();
    let has_alert = yaml_optional_string(rule, "alert").is_some();
    match (has_record, has_alert) {
        (true, true) | (false, false) => {
            return Err(ApiError::bad_data(
                "rule must contain exactly one of record or alert",
            ));
        }
        _ => {}
    }
    let expr = yaml_optional_string(rule, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule must contain a non-empty expr"))?;
    parse_promql(&expr)
        .map(|_| ())
        .map_err(|error| ApiError::bad_data(format!("rule PromQL expression is invalid: {error}")))
}

fn yaml_response(status: StatusCode, value: &impl serde::Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(yaml) => (status, [(header::CONTENT_TYPE, "application/yaml")], yaml).into_response(),
        Err(error) => ApiError::internal(format!("YAML encode failed: {error}")).into_response(),
    }
}

fn parse_rules_params(raw_query: Option<&str>) -> Result<RulesParams, ApiError> {
    let mut params = RulesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "type" => match value.as_ref() {
                "alert" | "record" => params.rule_type = Some(value.into_owned()),
                _ => return Err(ApiError::bad_data("invalid type parameter")),
            },
            "exclude_alerts" => {
                params.exclude_alerts = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid exclude_alerts parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}

async fn prometheus_rule_groups_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let mut groups = Vec::new();
    for (namespace, namespace_groups) in rules {
        for group in namespace_groups.into_values() {
            let rules = prometheus_rules_json(state, tenant, &group, options).await?;
            if rules.is_empty() {
                continue;
            }
            let group_name = yaml_string(&group, "name");
            let last_evaluation = state
                .ruler_group_last_eval_ms(tenant, &namespace, &group_name)
                .map_or_else(|| zero_evaluation_time().to_string(), rfc3339_time_string);
            groups.push(json!({
                "name": group_name,
                "file": namespace,
                "interval": duration_seconds_from_yaml(&group, "interval"),
                "lastEvaluation": last_evaluation,
                "evaluationTime": 0.0,
                "lastError": "",
                "limit": 0,
                "rules": rules,
            }));
        }
    }
    Ok(groups)
}

async fn prometheus_rules_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    group: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for rule in rules {
        if let Some(rule_json) = prometheus_rule_json(state, tenant, rule, options).await? {
            out.push(rule_json);
        }
    }
    Ok(out)
}

async fn prometheus_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rule: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Option<Value>, PromqlError> {
    if let Some(name) = yaml_optional_string(rule, "record") {
        if options.type_filter == RuleTypeFilter::Alert {
            return Ok(None);
        }
        return Ok(Some(json!({
            "evaluationTime": 0.0,
            "health": "ok",
            "lastError": "",
            "lastEvaluation": zero_evaluation_time(),
            "name": name,
            "query": yaml_string(rule, "expr"),
            "type": "recording",
        })));
    }
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(None);
    };
    if options.type_filter == RuleTypeFilter::Record {
        return Ok(None);
    }
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let alert_eval = prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await;
    let (health, last_error, alerts) = match alert_eval {
        Ok(alerts) => ("ok", String::new(), alerts),
        Err(error) => ("err", error.to_string(), Vec::new()),
    };
    let mut rule_json = json!({
        "annotations": yaml_mapping_json(rule, "annotations"),
        "duration": duration_seconds_from_yaml(rule, "for"),
        "evaluationTime": 0.0,
        "health": health,
        "lastError": last_error,
        "lastEvaluation": rfc3339_time_string(eval_time_ms),
        "labels": yaml_mapping_json(rule, "labels"),
        "name": name,
        "query": yaml_string(rule, "expr"),
        "type": "alerting",
    });
    if !options.exclude_alerts {
        rule_json["alerts"] = json!(alerts);
    }
    Ok(Some(rule_json))
}

async fn prometheus_alerts_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
) -> Result<Vec<Value>, PromqlError> {
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let mut alerts = Vec::new();
    for namespace_groups in rules.into_values() {
        for group in namespace_groups.into_values() {
            if let Some(group_rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) {
                for rule in group_rules {
                    alerts.extend(
                        prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await?,
                    );
                }
            }
        }
    }
    Ok(alerts)
}

async fn prometheus_alerts_for_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<Vec<Value>, PromqlError> {
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(Vec::new());
    };
    let query = yaml_string(rule, "expr");
    let result = state
        .engine
        .query_instant(tenant, &query, eval_time_ms)
        .await?;
    let QueryResult::InstantVector(samples) = result else {
        return Ok(Vec::new());
    };
    let duration_seconds = duration_seconds_from_yaml(rule, "for");
    let duration_ms = duration_seconds.saturating_mul(1000);
    let rule_id = format!("{name}\n{query}");
    let mut evaluated = Vec::new();
    let mut active_keys = BTreeSet::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let labels = alert_labels_map(&sample.labels, rule, &name);
        let key = AlertStateKey {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        active_keys.insert(key.clone());
        evaluated.push((key, labels, value));
    }

    let mut alert_states = state
        .ruler_alerts
        .write()
        .map_err(|_| PromqlError::Exec("ruler alert state lock poisoned".into()))?;
    alert_states.retain(|key, _| {
        (key.tenant != tenant || key.rule_id != rule_id) || active_keys.contains(key)
    });

    let mut alerts = Vec::new();
    for (key, labels, value) in evaluated {
        let active_at_ms = *alert_states.entry(key).or_insert(eval_time_ms);
        let alert_state = if duration_ms == 0
            || u64::try_from(eval_time_ms.saturating_sub(active_at_ms))
                .is_ok_and(|active_ms| active_ms >= duration_ms)
        {
            "firing"
        } else {
            "pending"
        };
        let template_labels = labels_from_map(&labels);
        let annotations = expand_alert_mapping_json(
            &yaml_mapping_json(rule, "annotations"),
            value,
            &template_labels,
        );
        let expanded_labels = labels
            .into_iter()
            .map(|(name, label_value)| {
                let expanded = expand_alert_template(&label_value, value, &template_labels);
                (name, expanded)
            })
            .collect::<BTreeMap<_, _>>();
        alerts.push(json!({
            "activeAt": rfc3339_time_string(active_at_ms),
            "annotations": annotations,
            "duration": duration_seconds,
            "labels": labels_map_json(expanded_labels),
            "name": name,
            "query": query,
            "state": alert_state,
            "value": sample_string(value),
        }));
    }
    Ok(alerts)
}

fn zero_evaluation_time() -> &'static str {
    "0001-01-01T00:00:00Z"
}

fn rfc3339_time_string(ts_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_ms) * 1_000_000).map_or_else(
        |_| zero_evaluation_time().to_string(),
        |time| {
            time.format(&Rfc3339)
                .unwrap_or_else(|_| zero_evaluation_time().to_string())
        },
    )
}

fn alert_labels_map(
    sample_labels: &Labels,
    rule: &serde_yaml::Value,
    alert_name: &str,
) -> BTreeMap<String, String> {
    let mut labels = sample_labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("alertname".to_string(), alert_name.to_string());
    if let Value::Object(rule_labels) = yaml_mapping_json(rule, "labels") {
        labels.extend(
            rule_labels
                .into_iter()
                .filter_map(|(name, value)| Some((name, value.as_str()?.to_string()))),
        );
    }
    labels
}

fn labels_map_json(labels: BTreeMap<String, String>) -> Value {
    Value::Object(
        labels
            .into_iter()
            .map(|(name, value)| (name, Value::String(value)))
            .collect(),
    )
}

fn yaml_string(value: &serde_yaml::Value, key: &str) -> String {
    yaml_optional_string(value, key).unwrap_or_default()
}

fn yaml_optional_string(value: &serde_yaml::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
}

fn yaml_mapping_json(value: &serde_yaml::Value, key: &str) -> Value {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map_or_else(
            || json!({}),
            |mapping| {
                let object = mapping
                    .iter()
                    .filter_map(|(name, value)| {
                        Some((
                            name.as_str()?.to_string(),
                            Value::String(value.as_str().unwrap_or_default().to_string()),
                        ))
                    })
                    .collect::<Map<_, _>>();
                Value::Object(object)
            },
        )
}

fn duration_seconds_from_yaml(value: &serde_yaml::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .and_then(parse_duration_seconds)
        .unwrap_or(0)
}

fn parse_duration_seconds(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Some(seconds) = value.strip_suffix('s') {
        return seconds.parse().ok();
    }
    if let Some(minutes) = value.strip_suffix('m') {
        return minutes.parse::<u64>().ok().map(|minutes| minutes * 60);
    }
    if let Some(hours) = value.strip_suffix('h') {
        return hours.parse::<u64>().ok().map(|hours| hours * 60 * 60);
    }
    value.parse().ok()
}
