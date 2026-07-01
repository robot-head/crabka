//! `DescribeClientQuotas` (`api_key` 48, KIP-13/124).

use bytes::Bytes;
use crabka_metadata::{EntityKey, ResourceType};
use crabka_protocol::Encode;
use crabka_protocol::owned::describe_client_quotas_request::{
    ComponentData, DescribeClientQuotasRequest,
};
use crabka_protocol::owned::describe_client_quotas_response::{
    DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::CLUSTER_AUTHORIZATION_FAILED;

const MATCH_TYPE_EXACT: i8 = 0;
const MATCH_TYPE_DEFAULT: i8 = 1;
const MATCH_TYPE_ANY: i8 = 2;

#[allow(clippy::unused_async)]
#[tracing::instrument(
    name = "handle_describe_client_quotas",
    level = "info",
    skip_all,
    fields(api = "DescribeClientQuotas"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeClientQuotasRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Describe,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-client-quotas denied".into()),
            entries: None,
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let mut entries: Vec<EntryData> = Vec::new();
    for (stored_key, configs) in image.client_quotas() {
        if !entity_matches_filter(stored_key, &req.components, req.strict) {
            continue;
        }
        entries.push(EntryData {
            entity: stored_key
                .iter()
                .map(|(t, n)| EntityData {
                    entity_type: t.clone(),
                    entity_name: n.clone(),
                    ..Default::default()
                })
                .collect(),
            values: configs
                .iter()
                .map(|(k, v)| ValueData {
                    key: k.clone(),
                    value: *v,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
    }

    let resp = DescribeClientQuotasResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        entries: Some(entries),
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

pub(crate) fn entity_matches_filter(
    stored: &EntityKey,
    components: &[ComponentData],
    strict: bool,
) -> bool {
    if strict && stored.len() != components.len() {
        return false;
    }
    for comp in components {
        let Some(stored_entity) = stored.iter().find(|(t, _)| t == &comp.entity_type) else {
            return false;
        };
        let ok = match comp.match_type {
            MATCH_TYPE_EXACT => stored_entity.1.as_deref() == comp.match_.as_deref(),
            MATCH_TYPE_DEFAULT => stored_entity.1.is_none(),
            MATCH_TYPE_ANY => true,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!("encode DescribeClientQuotas: {e}"))
    })?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn comp(entity_type: &str, match_type: i8, m: Option<&str>) -> ComponentData {
        ComponentData {
            entity_type: entity_type.into(),
            match_type,
            match_: m.map(Into::into),
            ..Default::default()
        }
    }

    fn key(parts: Vec<(&str, Option<&str>)>) -> EntityKey {
        parts
            .into_iter()
            .map(|(t, n)| (t.into(), n.map(Into::into)))
            .collect()
    }

    #[test]
    fn strict_exact_match_filters_correctly() {
        let stored = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, true));
        assert!(!entity_matches_filter(&stored, &filter[..0], true)); // strict: type-count mismatch
    }

    #[test]
    fn non_strict_filter_returns_supersets() {
        // Stored has (user, client-id); filter only mentions user.
        let stored = key(vec![("client-id", Some("app1")), ("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))];
        assert!(entity_matches_filter(&stored, &filter, false));
        assert!(!entity_matches_filter(&stored, &filter, true)); // strict rejects superset
    }

    #[test]
    fn default_match_type_filters_by_none_entity_name() {
        let stored_default = key(vec![("user", None)]);
        let stored_named = key(vec![("user", Some("alice"))]);
        let filter = vec![comp("user", MATCH_TYPE_DEFAULT, None)];
        assert!(entity_matches_filter(&stored_default, &filter, true));
        assert!(!entity_matches_filter(&stored_named, &filter, true));
    }

    #[test]
    fn any_match_type_returns_all_names_of_type() {
        let stored1 = key(vec![("user", Some("alice"))]);
        let stored2 = key(vec![("user", None)]);
        let filter = vec![comp("user", MATCH_TYPE_ANY, None)];
        assert!(entity_matches_filter(&stored1, &filter, true));
        assert!(entity_matches_filter(&stored2, &filter, true));
    }
}
