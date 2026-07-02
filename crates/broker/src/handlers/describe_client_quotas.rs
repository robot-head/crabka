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
    use assert2::check;
    use crabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};
    use crabka_protocol::Decode;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::broker::{Broker, BrokerHandle};
    use crate::config::BrokerConfig;

    const VERSION: i16 = 1;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

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

    fn request(components: Vec<ComponentData>, strict: bool) -> DescribeClientQuotasRequest {
        DescribeClientQuotasRequest {
            components,
            strict,
            ..Default::default()
        }
    }

    fn decode_response(bytes: &Bytes) -> DescribeClientQuotasResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            DescribeClientQuotasResponse::decode(&mut cur, VERSION).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn seed_quota(
        handle: &BrokerHandle,
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: entity
                    .into_iter()
                    .map(|(entity_type, entity_name)| QuotaEntity {
                        entity_type: entity_type.into(),
                        entity_name: entity_name.map(Into::into),
                    })
                    .collect(),
                config_key: key.into(),
                config_value: Some(value),
            })])
            .await
            .expect("seed quota");
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

    #[tokio::test]
    async fn denied_response_preserves_error_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))], true),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-client-quotas denied".into()),
            entries: None,
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_matching_quota_entry_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_quota(
            &broker_handle,
            vec![("client-id", Some("app-1")), ("user", Some("alice"))],
            "producer_byte_rate",
            2048.0,
        )
        .await;
        seed_quota(
            &broker_handle,
            vec![("user", Some("bob"))],
            "consumer_byte_rate",
            512.0,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("alice"))], false),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes);

        check!(resp.throttle_time_ms == 0, "{resp:?}");
        check!(resp.error_code == 0, "{resp:?}");
        check!(resp.error_message == None, "{resp:?}");
        let entries = resp.entries.expect("entries");
        assert!(entries.len() == 1, "{entries:?}");
        let entry = &entries[0];
        assert!(entry.entity.len() == 2, "{entry:?}");
        let by_type: std::collections::HashMap<_, _> = entry
            .entity
            .iter()
            .map(|e| (e.entity_type.as_str(), e.entity_name.as_deref()))
            .collect();
        check!(
            by_type.get("client-id") == Some(&Some("app-1")),
            "{entry:?}"
        );
        check!(by_type.get("user") == Some(&Some("alice")), "{entry:?}");
        check!(entry.values.len() == 1, "{entry:?}");
        check!(
            entry.values[0].key.as_str() == "producer_byte_rate",
            "{entry:?}"
        );
        check!(
            (entry.values[0].value - 2048.0).abs() < f64::EPSILON,
            "{entry:?}"
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_empty_match_uses_some_empty_entries() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            request(vec![comp("user", MATCH_TYPE_EXACT, Some("missing"))], true),
            &ctx,
            VERSION,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes);

        let expected = DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            entries: Some(Vec::new()),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
